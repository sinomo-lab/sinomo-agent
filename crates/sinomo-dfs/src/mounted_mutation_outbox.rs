//! Crash-safe ownership handoff for mutations issued by host filesystem mounts.
//!
//! Built-in and external host-filesystem adapters execute against a process-local
//! [`crate::NetworkedDfsService`]. A mutation can therefore commit metadata or
//! create repair work without passing through the daemon's normal served-message
//! boundary. This outbox brackets that mutation with an fsynced ambiguity marker
//! and retains it until the main daemon has durably accepted the exact work (or
//! recovery has completed). The operation and host-session locks are evidence:
//! recovery never infers that an external mount died merely because a daemon
//! generation changed.

use fs2::FileExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_core::Id;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use sinomo_core::storage::durable_file::{
    self, DurableAtomicFileOptions, create_private_directory, ensure_private_directory,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

#[cfg(windows)]
use std::os::windows::fs::MetadataExt as _;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::{DfsCommittedWork, DfsError, DfsRepairAttemptRecord};

pub const MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION: u16 = 1;
pub const MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS: usize = 64;
pub const MOUNTED_SIFS_MUTATION_OUTBOX_MAX_REPAIR_RECORDS: usize = 64;
pub const MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES: usize = 512;
pub const MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES: usize = 512 * 1024;

// One status/recovery scan must stay bounded even when an attacker or a crash
// leaves a wide directory tree behind. This covers every directory entry
// yielded by the scan, including staging/retirement children and repeated
// session proof work. The deadline is monotonic; it complements rather than
// replaces the exact entry-work bound.
const MOUNTED_SIFS_MUTATION_OUTBOX_MAX_DIRECTORY_SCAN_ENTRIES: usize =
    MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS * 8;
const MOUNTED_SIFS_MUTATION_OUTBOX_CLEANUP_SCAN_ENTRIES: usize =
    MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS * 2;
const MOUNTED_SIFS_MUTATION_OUTBOX_OPERATION_SCAN_ENTRIES: usize =
    MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS * 3;
const MOUNTED_SIFS_MUTATION_OUTBOX_SESSION_SCAN_ENTRIES: usize =
    MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS * 3;
const MOUNTED_SIFS_MUTATION_OUTBOX_DIRECTORY_SCAN_TIMEOUT: Duration = Duration::from_secs(1);
const MOUNTED_SIFS_MUTATION_OUTBOX_LOCK_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);
const MOUNTED_SIFS_MUTATION_OUTBOX_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(5);

const OUTBOX_DIRECTORY: &str = "mounted-mutation-outbox";
const SESSIONS_DIRECTORY: &str = "sessions";
const PREPARING_SESSIONS_DIRECTORY: &str = "preparing-sessions";
const OPERATIONS_DIRECTORY: &str = "operations";
const PREPARING_OPERATIONS_DIRECTORY: &str = "preparing-operations";
const RETIRED_SESSIONS_DIRECTORY: &str = "retired-sessions";
const RETIRED_OPERATIONS_DIRECTORY: &str = "retired-operations";
const STATE_FILE: &str = "state.sobj";
const LEASE_FILE: &str = "lease.lock";
const STATE_LOCK_FILE: &str = "state.lock";
const STORE_LOCK_FILE: &str = "outbox.lock";
const DAEMON_COMMITTED_WORK_LATCH_FILE: &str = "daemon-committed-work-latch.sobj";
const CLEANUP_AUTHORIZATION_FILE: &str = "cleanup-authorized.sobj";
const CREATION_TOKEN_FILE: &str = "creation-token.sobj";

pub type MountedSifsHostMountSessionId = Id;
pub type MountedSifsMutationMarkerId = Id;
pub type MountedSifsMutationNonce = Id;
pub type MountedSifsMutationArmRequestId = Id;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MountedSifsMutationAdapter {
    Fuse,
    #[serde(alias = "cloud-files")]
    ExternalProvider,
    Nfs,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MountedSifsMutationOrigin {
    pub adapter: MountedSifsMutationAdapter,
    pub mount_session_id: MountedSifsHostMountSessionId,
    pub mount_session_nonce: MountedSifsMutationNonce,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stable_operation_id: Option<String>,
    pub operation: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MountedSifsMutationCapturedWork {
    pub committed_work: DfsCommittedWork,
    pub committed_repair_records: Vec<DfsRepairAttemptRecord>,
}

impl MountedSifsMutationCapturedWork {
    pub fn new(
        committed_work: DfsCommittedWork,
        committed_repair_records: Vec<DfsRepairAttemptRecord>,
    ) -> Result<Self, DfsError> {
        let captured = Self {
            committed_work,
            committed_repair_records,
        };
        validate_captured_work(&captured)?;
        Ok(captured)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MountedSifsMutationErrorObservation {
    pub captured_work: MountedSifsMutationCapturedWork,
    pub error_code: String,
}

impl MountedSifsMutationErrorObservation {
    pub fn new(
        captured_work: MountedSifsMutationCapturedWork,
        error_code: impl Into<String>,
    ) -> Result<Self, DfsError> {
        let observation = Self {
            captured_work,
            error_code: error_code.into(),
        };
        validate_error_observation(&observation)?;
        Ok(observation)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MountedSifsMutationRecoveryReceipt {
    pub repair_queue_durable: bool,
    pub metadata_publish_durably_owned: bool,
    pub maintenance_durably_owned: bool,
    pub metadata_replica_progress_durably_owned: bool,
    pub full_recovery_completed: bool,
    pub completed_at_unix_nanos: u64,
}

/// Constant-size, monotonic daemon ownership for committed work captured by
/// direct mounted mutations. Bits are never cleared: every daemon generation
/// can replay the snapshot once during startup without a lost-wake window.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(default)]
pub struct MountedSifsMutationCommittedWorkLatch {
    pub schema_version: u16,
    pub generation: u64,
    pub committed_work: DfsCommittedWork,
    pub updated_at_unix_nanos: u64,
}

impl Default for MountedSifsMutationCommittedWorkLatch {
    fn default() -> Self {
        Self {
            schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
            generation: 0,
            committed_work: DfsCommittedWork::default(),
            updated_at_unix_nanos: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MountedSifsMutationCorruptRecoveryReceipt {
    pub schema_version: u16,
    pub marker_id: MountedSifsMutationMarkerId,
    pub recovery_receipt: MountedSifsMutationRecoveryReceipt,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "reason", rename_all = "kebab-case")]
enum MountedSifsMutationCleanupAuthorization {
    Terminal {
        marker: MountedSifsMutationOutboxMarker,
    },
    CorruptFullRecovery {
        receipt: MountedSifsMutationCorruptRecoveryReceipt,
    },
    UnpublishedMarker {
        marker_id: MountedSifsMutationMarkerId,
        authorized_at_unix_nanos: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct MountedSifsMutationCreationToken {
    schema_version: u16,
    marker_id: MountedSifsMutationMarkerId,
    marker_nonce: MountedSifsMutationNonce,
    created_at_unix_nanos: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "phase", rename_all = "kebab-case")]
pub enum MountedSifsMutationOutboxPhase {
    Prepared,
    Armed {
        arm_request_id: MountedSifsMutationArmRequestId,
    },
    Captured {
        arm_request_id: MountedSifsMutationArmRequestId,
        captured_work: MountedSifsMutationCapturedWork,
    },
    UnknownAfterError {
        arm_request_id: MountedSifsMutationArmRequestId,
        observation: MountedSifsMutationErrorObservation,
    },
    ProvenNoWrite {
        arm_request_id: MountedSifsMutationArmRequestId,
        proof_code: String,
    },
    DaemonAccepted {
        arm_request_id: MountedSifsMutationArmRequestId,
        captured_work: MountedSifsMutationCapturedWork,
        requires_full_recovery: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_observation: Option<MountedSifsMutationErrorObservation>,
    },
    AbortedUnstarted,
    Terminal {
        recovery_receipt: MountedSifsMutationRecoveryReceipt,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MountedSifsMutationOutboxMarker {
    pub schema_version: u16,
    pub marker_id: MountedSifsMutationMarkerId,
    pub marker_nonce: MountedSifsMutationNonce,
    pub origin: MountedSifsMutationOrigin,
    pub prepared_at_unix_nanos: u64,
    pub updated_at_unix_nanos: u64,
    pub phase: MountedSifsMutationOutboxPhase,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MountedSifsHostMountSessionPhase {
    Active,
    Closed,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MountedSifsHostMountSessionMarker {
    pub schema_version: u16,
    pub session_id: MountedSifsHostMountSessionId,
    pub session_nonce: MountedSifsMutationNonce,
    pub adapter: MountedSifsMutationAdapter,
    pub mount_identity: String,
    pub started_at_unix_nanos: u64,
    pub updated_at_unix_nanos: u64,
    pub phase: MountedSifsHostMountSessionPhase,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum MountedSifsMutationRecoveryKind {
    Exact,
    Full,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MountedSifsMutationOutboxStatus {
    pub total_marker_count: usize,
    pub pending_marker_count: usize,
    pub live_marker_count: usize,
    pub exact_recovery_marker_count: usize,
    pub full_recovery_marker_count: usize,
    pub deferred_live_session_marker_count: usize,
    pub unproven_session_marker_count: usize,
    pub malformed_marker_count: usize,
    pub terminal_marker_count: usize,
    pub marker_limit_exceeded: bool,
    /// The shared directory entry/deadline budget expired before the complete
    /// outbox tree was classified. Only independently safe Exact candidates or
    /// Full/corrupt candidates backed by a complete session-death proof may be
    /// returned in this state; callers must still fail closed for admission and
    /// schedule another bounded scan.
    pub directory_scan_limit_exceeded: bool,
}

impl MountedSifsMutationOutboxStatus {
    pub fn requires_full_recovery(&self) -> bool {
        self.full_recovery_marker_count > 0
            || self.unproven_session_marker_count > 0
            || self.malformed_marker_count > 0
            || self.marker_limit_exceeded
            || self.directory_scan_limit_exceeded
    }
}

#[derive(Clone, Debug)]
pub struct MountedSifsMutationOutboxStore {
    root: PathBuf,
}

/// Raw, owner-validated evidence used only by an explicit external-provider
/// cursor-only recovery token. Constructing this value never creates a path,
/// opens the normal self-healing store, repairs state, prunes history, or
/// creates a lock file.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct MountedSifsMutationOutboxCursorRecoveryInspection {
    pub outbox_present: bool,
    pub raw_files: BTreeMap<String, Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_work_latch: Option<MountedSifsMutationCommittedWorkLatch>,
}

#[derive(Debug)]
pub struct MountedSifsMutationOutboxCursorRecoveryGuard {
    inspection: MountedSifsMutationOutboxCursorRecoveryInspection,
    _store_lock: Option<CursorRecoveryStoreLock>,
}

impl MountedSifsMutationOutboxCursorRecoveryGuard {
    pub fn inspection(&self) -> &MountedSifsMutationOutboxCursorRecoveryInspection {
        &self.inspection
    }

    /// Accepts the exact token-bound owner image or the one monotonic change
    /// this owner permits while cursor recovery runs: committed mounted DFS
    /// work may advance the constant-size daemon latch. The latch never clears
    /// bits, and every ownership-advancing write increments its generation, so
    /// this does not admit a replacement or ambiguous operation tree.
    pub fn matches_original_or_committed_work_successor(
        &self,
        original_raw_files: &BTreeMap<String, Vec<u8>>,
    ) -> Result<bool, DfsError> {
        if &self.inspection.raw_files == original_raw_files {
            return Ok(true);
        }
        if original_raw_files.len() != 2
            || original_raw_files.get(STORE_LOCK_FILE).map(Vec::as_slice) != Some(&[])
            || self
                .inspection
                .raw_files
                .get(STORE_LOCK_FILE)
                .map(Vec::as_slice)
                != Some(&[])
        {
            return Ok(false);
        }
        let Some(original_bytes) = original_raw_files.get(DAEMON_COMMITTED_WORK_LATCH_FILE) else {
            return Ok(false);
        };
        let original: MountedSifsMutationCommittedWorkLatch =
            sinomo_object_serde::from_bytes(original_bytes).map_err(|source| {
                outbox_error(format!(
                    "cannot decode token-bound mounted-mutation committed-work latch: {source}"
                ))
            })?;
        if original.schema_version != MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
            || (original.generation == 0
                && original != MountedSifsMutationCommittedWorkLatch::default())
            || (original.generation != 0 && original.updated_at_unix_nanos == 0)
            || sinomo_object_serde::to_bytes(&original, 1, LengthMode::U32).map_err(|source| {
                outbox_error(format!(
                    "cannot re-encode token-bound mounted-mutation committed-work latch: {source}"
                ))
            })? != *original_bytes
        {
            return Ok(false);
        }
        let Some(current) = self.inspection.committed_work_latch else {
            return Ok(false);
        };
        Ok(current.generation > original.generation
            && committed_work_contains(current.committed_work, original.committed_work))
    }
}

fn committed_work_contains(current: DfsCommittedWork, original: DfsCommittedWork) -> bool {
    (!original.publish_metadata || current.publish_metadata)
        && (!original.repair_maintenance || current.repair_maintenance)
        && (!original.namespace_maintenance || current.namespace_maintenance)
        && (!original.metadata_replica_progress || current.metadata_replica_progress)
        && (!original.metadata_materialization || current.metadata_materialization)
}

/// Inspect the outbox in the only state compatible with adopting cursor-only
/// external-provider history: no session, operation, staging, or retired evidence
/// exists. A pre-existing store lock is acquired without `create` and held by
/// the returned guard for the complete snapshot/effect fence; its bytes are
/// included in the evidence.
pub fn inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(
    data_dir: &SinomoDataDir,
) -> Result<MountedSifsMutationOutboxCursorRecoveryGuard, DfsError> {
    let dfs_root = data_dir.dfs_dir();
    if let Ok(metadata) = fs::symlink_metadata(&dfs_root)
        && (!metadata.file_type().is_dir() || cursor_recovery_metadata_is_reparse(&metadata))
    {
        return Err(outbox_error(format!(
            "DFS root {} is not a plain directory",
            dfs_root.display()
        )));
    }
    let root = dfs_root.join(OUTBOX_DIRECTORY);
    let root_metadata = match fs::symlink_metadata(&root) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(MountedSifsMutationOutboxCursorRecoveryGuard {
                inspection: MountedSifsMutationOutboxCursorRecoveryInspection::default(),
                _store_lock: None,
            });
        }
        Err(source) => return Err(lock_error(&root, source)),
    };
    if !root_metadata.file_type().is_dir() || cursor_recovery_metadata_is_reparse(&root_metadata) {
        return Err(outbox_error(format!(
            "mounted-mutation outbox {} is not a plain directory",
            root.display()
        )));
    }

    let lock_path = root.join(STORE_LOCK_FILE);
    let lock_metadata = fs::symlink_metadata(&lock_path).map_err(|source| {
        outbox_error(format!(
            "mounted-mutation outbox has no pre-existing store lock {}: {source}",
            lock_path.display()
        ))
    })?;
    if !lock_metadata.file_type().is_file() || cursor_recovery_metadata_is_reparse(&lock_metadata) {
        return Err(outbox_error(
            "mounted-mutation outbox store lock is not a plain file",
        ));
    }
    let store_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&lock_path)
        .map_err(|source| lock_error(&lock_path, source))?;
    store_lock.try_lock_exclusive().map_err(|source| {
        outbox_error(format!(
            "mounted-mutation outbox is not globally quiescent: {source}"
        ))
    })?;
    tracing::info!(
        target: "sinomo.dfs.mounted_mutation.lock",
        lock_path = %lock_path.display(),
        thread_name = std::thread::current().name(),
        "acquired mounted-mutation cursor-recovery store lock"
    );
    let mut store_lock = CursorRecoveryStoreLock {
        file: store_lock,
        path: lock_path.clone(),
    };

    let expected_directories = BTreeSet::from([
        SESSIONS_DIRECTORY.to_string(),
        PREPARING_SESSIONS_DIRECTORY.to_string(),
        OPERATIONS_DIRECTORY.to_string(),
        PREPARING_OPERATIONS_DIRECTORY.to_string(),
        RETIRED_SESSIONS_DIRECTORY.to_string(),
        RETIRED_OPERATIONS_DIRECTORY.to_string(),
    ]);
    let expected_files = BTreeSet::from([
        STORE_LOCK_FILE.to_string(),
        DAEMON_COMMITTED_WORK_LATCH_FILE.to_string(),
    ]);
    let mut observed_directories = BTreeSet::new();
    let mut observed_files = BTreeSet::new();
    for entry in fs::read_dir(&root).map_err(|source| lock_error(&root, source))? {
        let entry = entry.map_err(|source| lock_error(&root, source))?;
        let path = entry.path();
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| outbox_error("mounted-mutation outbox contains a non-UTF-8 entry"))?;
        let metadata = fs::symlink_metadata(&path).map_err(|source| lock_error(&path, source))?;
        if cursor_recovery_metadata_is_reparse(&metadata) {
            return Err(outbox_error(format!(
                "mounted-mutation outbox contains reparse/symlink evidence {}",
                path.display()
            )));
        }
        if metadata.file_type().is_dir() && expected_directories.contains(&name) {
            if fs::read_dir(&path)
                .map_err(|source| lock_error(&path, source))?
                .next()
                .transpose()
                .map_err(|source| lock_error(&path, source))?
                .is_some()
            {
                return Err(outbox_error(format!(
                    "mounted-mutation outbox recovery requires empty `{name}` evidence"
                )));
            }
            observed_directories.insert(name);
        } else if metadata.file_type().is_file() && expected_files.contains(&name) {
            observed_files.insert(name);
        } else {
            return Err(outbox_error(format!(
                "unexpected mounted-mutation outbox recovery entry {}",
                path.display()
            )));
        }
    }
    if observed_directories != expected_directories {
        return Err(outbox_error(
            "mounted-mutation outbox recovery topology is missing a canonical directory",
        ));
    }
    if !observed_files.contains(STORE_LOCK_FILE) {
        return Err(outbox_error(
            "mounted-mutation outbox recovery requires its pre-existing store lock",
        ));
    }

    // Read the lock evidence through the handle which owns the byte-range
    // lock. On Windows an exclusive LockFileEx range also blocks reads through
    // a second handle in this process, so reopening via `fs::read` would make
    // a successful recovery fence reject itself with ERROR_LOCK_VIOLATION.
    store_lock
        .file
        .seek(SeekFrom::Start(0))
        .map_err(|source| lock_error(&lock_path, source))?;
    let mut lock_bytes = Vec::new();
    store_lock
        .file
        .read_to_end(&mut lock_bytes)
        .map_err(|source| lock_error(&lock_path, source))?;
    if !lock_bytes.is_empty() {
        return Err(outbox_error(
            "mounted-mutation outbox lock contains unexpected state bytes",
        ));
    }
    let mut raw_files = BTreeMap::from([(STORE_LOCK_FILE.to_string(), lock_bytes)]);
    let committed_work_latch = if observed_files.contains(DAEMON_COMMITTED_WORK_LATCH_FILE) {
        let path = root.join(DAEMON_COMMITTED_WORK_LATCH_FILE);
        let bytes = fs::read(&path).map_err(|source| lock_error(&path, source))?;
        if bytes.len() > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES {
            return Err(outbox_error(
                "mounted-mutation committed-work latch exceeds its bound",
            ));
        }
        let latch: MountedSifsMutationCommittedWorkLatch = sinomo_object_serde::from_bytes(&bytes)
            .map_err(|source| {
                outbox_error(format!(
                    "cannot decode mounted-mutation committed-work latch: {source}"
                ))
            })?;
        if latch.schema_version != MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
            || (latch.generation == 0 && latch != MountedSifsMutationCommittedWorkLatch::default())
            || (latch.generation != 0 && latch.updated_at_unix_nanos == 0)
            || sinomo_object_serde::to_bytes(&latch, 1, LengthMode::U32).map_err(|source| {
                outbox_error(format!(
                    "cannot re-encode mounted-mutation committed-work latch: {source}"
                ))
            })? != bytes
        {
            return Err(outbox_error(
                "mounted-mutation committed-work latch is not monotonic and valid",
            ));
        }
        raw_files.insert(DAEMON_COMMITTED_WORK_LATCH_FILE.to_string(), bytes);
        Some(latch)
    } else {
        return Err(outbox_error(
            "mounted-mutation outbox is missing its committed-work latch; absence is possible lost wake ownership",
        ));
    };

    Ok(MountedSifsMutationOutboxCursorRecoveryGuard {
        inspection: MountedSifsMutationOutboxCursorRecoveryInspection {
            outbox_present: true,
            raw_files,
            committed_work_latch,
        },
        _store_lock: Some(store_lock),
    })
}

#[derive(Debug)]
struct CursorRecoveryStoreLock {
    file: File,
    path: PathBuf,
}

impl Drop for CursorRecoveryStoreLock {
    fn drop(&mut self) {
        match FileExt::unlock(&self.file) {
            Ok(()) => tracing::info!(
                target: "sinomo.dfs.mounted_mutation.lock",
                lock_path = %self.path.display(),
                thread_name = std::thread::current().name(),
                "released mounted-mutation cursor-recovery store lock"
            ),
            Err(source) => tracing::warn!(
                target: "sinomo.dfs.mounted_mutation.lock",
                lock_path = %self.path.display(),
                error = %source,
                "failed to release mounted-mutation cursor-recovery store lock"
            ),
        }
    }
}

#[cfg(windows)]
fn cursor_recovery_metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn cursor_recovery_metadata_is_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[derive(Debug)]
pub struct MountedSifsHostMountSessionLease {
    store: MountedSifsMutationOutboxStore,
    marker: MountedSifsHostMountSessionMarker,
    lease: Mutex<Option<HeldLeaseLock>>,
}

impl MountedSifsHostMountSessionLease {
    pub fn marker(&self) -> &MountedSifsHostMountSessionMarker {
        &self.marker
    }

    pub fn origin(
        &self,
        stable_operation_id: Option<String>,
        operation: impl Into<String>,
    ) -> Result<MountedSifsMutationOrigin, DfsError> {
        let origin = MountedSifsMutationOrigin {
            adapter: self.marker.adapter,
            mount_session_id: self.marker.session_id.clone(),
            mount_session_nonce: self.marker.session_nonce.clone(),
            stable_operation_id,
            operation: operation.into(),
        };
        validate_origin(&origin)?;
        Ok(origin)
    }

    /// Persist an orderly session close before releasing its external-liveness
    /// proof. This shared form lets a fail-closed adapter retire recovery
    /// ownership even when its host worker remains stuck behind detached kernel
    /// references. The adapter must fence new service entry and serialize this
    /// call after its last in-flight operation lease.
    pub fn close_clean_shared(&self) -> Result<(), DfsError> {
        let mut lease = self
            .lease
            .lock()
            .map_err(|_| outbox_error("host-mount session lease is poisoned"))?;
        if lease.is_none() {
            return Ok(());
        }
        let mut marker = self.marker.clone();
        marker.phase = MountedSifsHostMountSessionPhase::Closed;
        marker.updated_at_unix_nanos = now_unix_nanos();
        self.store.write_session_marker(&marker)?;
        // Release only after the Closed marker is durable. Session scans still
        // require both Closed and a dead OS/process lease before recovery.
        lease.take();
        Ok(())
    }

    pub fn close_clean(self) -> Result<(), DfsError> {
        self.close_clean_shared()
    }
}

#[derive(Debug)]
pub struct MountedSifsMutationLease<'session> {
    store: MountedSifsMutationOutboxStore,
    marker: MountedSifsMutationOutboxMarker,
    _lease: HeldLeaseLock,
    _session: PhantomData<&'session MountedSifsHostMountSessionLease>,
}

impl MountedSifsMutationLease<'_> {
    pub fn marker(&self) -> &MountedSifsMutationOutboxMarker {
        &self.marker
    }

    /// Persist the exact successful mutation delta before asking the daemon to
    /// accept it. This transition is legal only after the daemon's durable Arm
    /// acknowledgement.
    pub fn record_captured_success(
        &mut self,
        arm_request_id: &MountedSifsMutationArmRequestId,
        captured_work: MountedSifsMutationCapturedWork,
    ) -> Result<(), DfsError> {
        validate_captured_work(&captured_work)?;
        self.transition_from_armed(arm_request_id, |marker| {
            marker.phase = MountedSifsMutationOutboxPhase::Captured {
                arm_request_id: arm_request_id.clone(),
                captured_work,
            };
            Ok(())
        })
    }

    /// Persist an ambiguous error. An ordinary `Err` must use this transition;
    /// an empty successful capture is never a substitute for positive no-write
    /// proof.
    pub fn record_unknown_after_error(
        &mut self,
        arm_request_id: &MountedSifsMutationArmRequestId,
        observation: MountedSifsMutationErrorObservation,
    ) -> Result<(), DfsError> {
        validate_error_observation(&observation)?;
        self.transition_from_armed(arm_request_id, |marker| {
            marker.phase = MountedSifsMutationOutboxPhase::UnknownAfterError {
                arm_request_id: arm_request_id.clone(),
                observation,
            };
            Ok(())
        })
    }

    /// Persist the adapter's positive proof that the armed operation did not
    /// write. The daemon may attest only this exact stored proof; it does not
    /// author no-write evidence on the adapter's behalf.
    pub fn record_proven_no_write(
        &mut self,
        arm_request_id: &MountedSifsMutationArmRequestId,
        proof_code: impl Into<String>,
    ) -> Result<(), DfsError> {
        let proof_code = proof_code.into();
        validate_label("proof_code", &proof_code, false)?;
        self.transition_from_armed(arm_request_id, |marker| {
            marker.phase = MountedSifsMutationOutboxPhase::ProvenNoWrite {
                arm_request_id: arm_request_id.clone(),
                proof_code,
            };
            Ok(())
        })
    }

    /// Release the operation proof only after observing a daemon ACK backed by
    /// one of the durable post-mutation phases.
    pub fn release_after_daemon_ack(self) -> Result<(), DfsError> {
        let current = self
            .store
            .marker(&self.marker.marker_id)?
            .ok_or_else(|| outbox_error("mounted-mutation marker disappeared before daemon ACK"))?;
        verify_marker_identity(&current, &self.marker.marker_id, &self.marker.marker_nonce)?;
        match current.phase {
            MountedSifsMutationOutboxPhase::DaemonAccepted { .. }
            | MountedSifsMutationOutboxPhase::ProvenNoWrite { .. }
            | MountedSifsMutationOutboxPhase::AbortedUnstarted
            | MountedSifsMutationOutboxPhase::Terminal { .. } => Ok(()),
            _ => Err(outbox_error(format!(
                "mounted-mutation marker {} has no durable daemon ACK",
                current.marker_id
            ))),
        }
    }

    fn transition_from_armed(
        &mut self,
        arm_request_id: &MountedSifsMutationArmRequestId,
        transition: impl FnOnce(&mut MountedSifsMutationOutboxMarker) -> Result<(), DfsError>,
    ) -> Result<(), DfsError> {
        let marker_id = self.marker.marker_id.clone();
        let marker_nonce = self.marker.marker_nonce.clone();
        let updated = self.store.mutate_operation_marker(&marker_id, |marker| {
            verify_marker_identity(marker, &marker_id, &marker_nonce)?;
            require_matching_armed(marker, arm_request_id)?;
            transition(marker)
        })?;
        self.marker = updated;
        Ok(())
    }
}

#[derive(Debug)]
pub struct MountedSifsMutationRecoveryClaim {
    store: MountedSifsMutationOutboxStore,
    marker: MountedSifsMutationOutboxMarker,
    recovery_kind: MountedSifsMutationRecoveryKind,
    _lease: HeldLeaseLock,
}

#[derive(Debug)]
pub struct MountedSifsMutationCorruptRecoveryClaim {
    store: MountedSifsMutationOutboxStore,
    marker_id: MountedSifsMutationMarkerId,
    _lease: HeldLeaseLock,
}

impl MountedSifsMutationCorruptRecoveryClaim {
    pub fn marker_id(&self) -> &MountedSifsMutationMarkerId {
        &self.marker_id
    }

    /// Persist a full-recovery receipt before retiring the undecodable state.
    /// Retirement is an atomic directory move; any later deletion is outside
    /// the active classifier namespace and is therefore crash-idempotent.
    pub fn terminalize(
        self,
        recovery_receipt: MountedSifsMutationRecoveryReceipt,
    ) -> Result<(), DfsError> {
        validate_full_recovery_receipt(&recovery_receipt)?;
        let store = self.store.clone();
        let marker_id = self.marker_id.clone();
        let directory = store.operation_directory(&marker_id);
        let _store_lock = lock_file_exclusive(&store.root.join(STORE_LOCK_FILE))?;
        // A corrupt marker has lost the exact work provenance. Conservatively
        // retain every edge before authorizing removal of the ambiguity.
        store.retain_daemon_committed_work_unlocked(all_committed_work())?;
        let state_lock_path = directory.join(STATE_LOCK_FILE);
        let state_lock = lock_file_exclusive(&state_lock_path)?;
        let receipt = MountedSifsMutationCorruptRecoveryReceipt {
            schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
            marker_id: self.marker_id.clone(),
            recovery_receipt,
        };
        write_bounded_sobj(
            &directory.join(CLEANUP_AUTHORIZATION_FILE),
            &MountedSifsMutationCleanupAuthorization::CorruptFullRecovery { receipt },
        )?;
        drop(state_lock);
        let MountedSifsMutationCorruptRecoveryClaim { _lease: lease, .. } = self;
        drop(lease);
        store.retire_operation_directory(&directory, &marker_id)
    }
}

impl MountedSifsMutationRecoveryClaim {
    pub fn marker(&self) -> &MountedSifsMutationOutboxMarker {
        &self.marker
    }

    pub fn recovery_kind(&self) -> MountedSifsMutationRecoveryKind {
        self.recovery_kind
    }

    /// Terminalize only after the daemon presents evidence for every durable
    /// owner/pass implied by the marker. A notification or spawned task cannot
    /// satisfy any receipt field.
    pub fn terminalize(
        self,
        recovery_receipt: MountedSifsMutationRecoveryReceipt,
    ) -> Result<(), DfsError> {
        validate_recovery_receipt(&self.marker, self.recovery_kind, &recovery_receipt)?;
        self.store
            .terminalize_recovery_claim(&self.marker, self.recovery_kind, recovery_receipt)
    }
}

impl MountedSifsMutationOutboxStore {
    pub fn new(data_dir: &SinomoDataDir) -> Result<Self, DfsError> {
        data_dir
            .ensure_root_layout()
            .map_err(|source| outbox_error(source.to_string()))?;
        let root = data_dir.dfs_dir().join(OUTBOX_DIRECTORY);
        let outbox_preexisting = root.exists();
        ensure_private_directory(&root).map_err(durable_error)?;
        ensure_private_directory(&root.join(SESSIONS_DIRECTORY)).map_err(durable_error)?;
        ensure_private_directory(&root.join(PREPARING_SESSIONS_DIRECTORY))
            .map_err(durable_error)?;
        ensure_private_directory(&root.join(OPERATIONS_DIRECTORY)).map_err(durable_error)?;
        ensure_private_directory(&root.join(PREPARING_OPERATIONS_DIRECTORY))
            .map_err(durable_error)?;
        ensure_private_directory(&root.join(RETIRED_SESSIONS_DIRECTORY)).map_err(durable_error)?;
        ensure_private_directory(&root.join(RETIRED_OPERATIONS_DIRECTORY))
            .map_err(durable_error)?;
        let store = Self { root };
        let _store_lock = lock_file_exclusive(&store.root.join(STORE_LOCK_FILE))?;
        if !store.daemon_committed_work_latch_path().exists() {
            let initial = if outbox_preexisting {
                // Absence in an existing outbox may be loss after acceptance.
                // Retaining all bits is conservative and bounded.
                MountedSifsMutationCommittedWorkLatch {
                    generation: 1,
                    committed_work: all_committed_work(),
                    updated_at_unix_nanos: now_unix_nanos(),
                    ..MountedSifsMutationCommittedWorkLatch::default()
                }
            } else {
                MountedSifsMutationCommittedWorkLatch::default()
            };
            store.write_daemon_committed_work_latch_unlocked(&initial)?;
        }
        // Decode failure is fail-safe but not a permanent mount blocker: the
        // latch is only coalesced wake ownership, so self-heal by retaining all
        // work bits and let daemon startup replay them.
        let _ = store.daemon_committed_work_latch_unlocked()?;
        let _ = store.scan_unlocked()?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Read the monotonic daemon-owned committed-work latch. The latch has no
    /// clear operation; callers replay its coalesced bits once per daemon
    /// startup and may also react to a newer generation in-process.
    pub fn daemon_committed_work_latch(
        &self,
    ) -> Result<MountedSifsMutationCommittedWorkLatch, DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        self.daemon_committed_work_latch_unlocked()
    }

    /// Durably OR daemon-owned committed work into the constant-size latch.
    /// This is primarily for recovery paths with no live mounted caller; live
    /// CommitApplied/CommitUnknown acceptance performs the same write
    /// automatically before publishing `DaemonAccepted`.
    pub fn retain_daemon_committed_work(
        &self,
        committed_work: DfsCommittedWork,
    ) -> Result<MountedSifsMutationCommittedWorkLatch, DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        self.retain_daemon_committed_work_unlocked(committed_work)
    }

    pub fn open_host_mount_session(
        &self,
        adapter: MountedSifsMutationAdapter,
        mount_identity: impl Into<String>,
    ) -> Result<MountedSifsHostMountSessionLease, DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        let mut scan_budget = MountedSifsMutationDirectoryScanBudget::production();
        let status = self.scan_unlocked_with_budget(&mut scan_budget)?.status;
        if status.pending_marker_count != 0
            || status.malformed_marker_count != 0
            || status.marker_limit_exceeded
            || status.directory_scan_limit_exceeded
        {
            return Err(outbox_error(
                "mounted-mutation recovery must drain before a new host-mount session opens",
            ));
        }
        let mut session_phase_remaining = MOUNTED_SIFS_MUTATION_OUTBOX_SESSION_SCAN_ENTRIES;
        if self.all_host_mount_sessions_proven_dead_with_budget(
            &mut scan_budget,
            &mut session_phase_remaining,
        )? != HostMountSessionsDeathProof::AllDead
        {
            return Err(outbox_error(
                "another mounted-mutation host session is live or unproven",
            ));
        }
        let mount_identity = mount_identity.into();
        validate_label("mount_identity", &mount_identity, false)?;
        let session_id = Id::generate();
        let session_nonce = Id::generate();
        let directory = self.session_directory(&session_id);
        let preparing_directory = self
            .root
            .join(PREPARING_SESSIONS_DIRECTORY)
            .join(session_id.as_str());
        create_private_directory(&preparing_directory).map_err(durable_error)?;
        let lease = HeldLeaseLock::acquire_new(&preparing_directory.join(LEASE_FILE))?;
        let now = now_unix_nanos();
        let marker = MountedSifsHostMountSessionMarker {
            schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
            session_id,
            session_nonce,
            adapter,
            mount_identity,
            started_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            phase: MountedSifsHostMountSessionPhase::Active,
        };
        self.write_session_marker_in_directory(&preparing_directory, &marker)?;
        // The store lock serializes publication with every production scan.
        // Close the staging lease before the directory rename because Windows
        // rejects renaming a directory while a child handle is open, then
        // reacquire the published lease before exposing the session. A crash
        // before the rename leaves dead private staging; a crash after it
        // leaves a complete, unreferenced session with a dead lease which the
        // next store-locked scan can retire without fabricating recovery work.
        let lease = publish_host_mount_session_directory(&preparing_directory, &directory, lease)?;
        Ok(MountedSifsHostMountSessionLease {
            store: self.clone(),
            marker,
            lease: Mutex::new(Some(lease)),
        })
    }

    /// Fsync a Prepared ambiguity marker while the host-session and operation
    /// leases are held. The adapter must not enter its mutation before a later
    /// daemon [`Self::arm_live`] call has also been fsynced and acknowledged.
    pub fn prepare<'session>(
        &self,
        session: &'session MountedSifsHostMountSessionLease,
        stable_operation_id: Option<String>,
        operation: impl Into<String>,
    ) -> Result<MountedSifsMutationLease<'session>, DfsError> {
        let session_lease_is_held = session
            .lease
            .lock()
            .map_err(|_| outbox_error("host-mount session lease is poisoned"))?
            .as_ref()
            .is_some_and(HeldLeaseLock::is_held);
        if session.store.root != self.root
            || session.marker.phase != MountedSifsHostMountSessionPhase::Active
            || !session_lease_is_held
        {
            return Err(outbox_error(
                "host-mount session is not live in this outbox",
            ));
        }
        let origin = session.origin(stable_operation_id, operation)?;
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        let status = self.status_unlocked()?;
        if status.directory_scan_limit_exceeded
            || status.marker_limit_exceeded
            || status.malformed_marker_count != 0
        {
            return Err(outbox_error(
                "mounted-mutation outbox classification must complete before preparing new work",
            ));
        }
        if status.total_marker_count >= MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
            return Err(outbox_error(format!(
                "mounted-mutation outbox active marker limit {} reached",
                MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS
            )));
        }

        let marker_id = Id::generate();
        let marker_nonce = Id::generate();
        let directory = self.operation_directory(&marker_id);
        let preparing_directory = self
            .root
            .join(PREPARING_OPERATIONS_DIRECTORY)
            .join(marker_id.as_str());
        create_private_directory(&preparing_directory).map_err(durable_error)?;
        let now = now_unix_nanos();
        write_bounded_sobj(
            &preparing_directory.join(CREATION_TOKEN_FILE),
            &MountedSifsMutationCreationToken {
                schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
                marker_id: marker_id.clone(),
                marker_nonce: marker_nonce.clone(),
                created_at_unix_nanos: now,
            },
        )?;
        retire_directory(&preparing_directory, &directory)?;
        let lease = HeldLeaseLock::acquire_new(&directory.join(LEASE_FILE))?;
        let marker = MountedSifsMutationOutboxMarker {
            schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
            marker_id,
            marker_nonce,
            origin,
            prepared_at_unix_nanos: now,
            updated_at_unix_nanos: now,
            phase: MountedSifsMutationOutboxPhase::Prepared,
        };
        self.write_operation_marker(&marker)?;
        // Once Prepared is fsynced, absence of state must never be mistaken for
        // a pre-publication crash. Durably consume the one-shot creation token
        // before returning the lease to code which can request Arm.
        remove_file_durable(&directory.join(CREATION_TOKEN_FILE))?;
        Ok(MountedSifsMutationLease {
            store: self.clone(),
            marker,
            _lease: lease,
            _session: PhantomData,
        })
    }

    /// Daemon-owned authoritative Arm transition. Repeating the same request
    /// identity is response-retry safe; a different identity conflicts.
    pub fn arm_live(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        self.require_live_operation_and_session(marker_id, marker_nonce)?;
        self.mutate_operation_marker(marker_id, |marker| {
            verify_marker_identity(marker, marker_id, marker_nonce)?;
            match &marker.phase {
                MountedSifsMutationOutboxPhase::Prepared => {
                    marker.phase = MountedSifsMutationOutboxPhase::Armed {
                        arm_request_id: arm_request_id.clone(),
                    };
                }
                MountedSifsMutationOutboxPhase::Armed {
                    arm_request_id: current,
                } if current == arm_request_id => {}
                MountedSifsMutationOutboxPhase::Armed { .. } => {
                    return Err(outbox_error(
                        "mounted-mutation marker is armed by another request",
                    ));
                }
                _ => {
                    return Err(outbox_error(
                        "mounted-mutation Arm is invalid after mutation capture",
                    ));
                }
            }
            Ok(())
        })
    }

    /// Daemon transition after it has durably appended every exact repair
    /// record and accepted the remaining committed-work bits.
    pub fn accept_captured_live(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        captured_work: &MountedSifsMutationCapturedWork,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        validate_captured_work(captured_work)?;
        self.accept_live_with_durable_work(
            marker_id,
            marker_nonce,
            captured_work.committed_work,
            |marker| match &marker.phase {
                MountedSifsMutationOutboxPhase::Captured {
                    arm_request_id: current,
                    captured_work: current_work,
                } if current == arm_request_id && current_work == captured_work => {
                    Ok(Some(MountedSifsMutationOutboxPhase::DaemonAccepted {
                        arm_request_id: arm_request_id.clone(),
                        captured_work: captured_work.clone(),
                        requires_full_recovery: false,
                        error_observation: None,
                    }))
                }
                MountedSifsMutationOutboxPhase::DaemonAccepted {
                    arm_request_id: current,
                    captured_work: current_work,
                    requires_full_recovery: false,
                    error_observation: None,
                } if current == arm_request_id && current_work == captured_work => Ok(None),
                _ => Err(outbox_error(
                    "mounted-mutation accepted work does not match the fsynced capture",
                )),
            },
        )
    }

    /// Daemon transition for an error without no-write proof. Exact captured
    /// work is retained, but the marker remains a full-recovery gate.
    pub fn accept_unknown_live(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        observation: &MountedSifsMutationErrorObservation,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        validate_error_observation(observation)?;
        self.accept_live_with_durable_work(
            marker_id,
            marker_nonce,
            observation.captured_work.committed_work,
            |marker| match &marker.phase {
                MountedSifsMutationOutboxPhase::UnknownAfterError {
                    arm_request_id: current,
                    observation: current_observation,
                } if current == arm_request_id && current_observation == observation => {
                    Ok(Some(MountedSifsMutationOutboxPhase::DaemonAccepted {
                        arm_request_id: arm_request_id.clone(),
                        captured_work: observation.captured_work.clone(),
                        requires_full_recovery: true,
                        error_observation: Some(observation.clone()),
                    }))
                }
                MountedSifsMutationOutboxPhase::DaemonAccepted {
                    arm_request_id: current,
                    captured_work,
                    requires_full_recovery: true,
                    error_observation: Some(current_observation),
                } if current == arm_request_id
                    && captured_work == &observation.captured_work
                    && current_observation == observation =>
                {
                    Ok(None)
                }
                _ => Err(outbox_error(
                    "mounted-mutation unknown observation does not match the fsynced error",
                )),
            },
        )
    }

    pub fn prove_no_write_live(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        arm_request_id: &MountedSifsMutationArmRequestId,
        proof_code: &str,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        validate_label("proof_code", proof_code, false)?;
        self.require_live_operation_and_session(marker_id, marker_nonce)?;
        self.mutate_operation_marker(marker_id, |marker| {
            verify_marker_identity(marker, marker_id, marker_nonce)?;
            match &marker.phase {
                MountedSifsMutationOutboxPhase::ProvenNoWrite {
                    arm_request_id: current,
                    proof_code: current_proof,
                } if current == arm_request_id && current_proof == proof_code => {}
                _ => {
                    return Err(outbox_error("mounted-mutation daemon no-write attestation does not match the fsynced adapter proof"));
                }
            }
            Ok(())
        })
    }

    /// Idempotently abort only a marker which was never Armed. A caller which
    /// lost an Arm response cannot erase the daemon's fsynced ambiguity fence.
    pub fn abort_unstarted_live(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        self.require_live_operation_and_session(marker_id, marker_nonce)?;
        self.mutate_operation_marker(marker_id, |marker| {
            verify_marker_identity(marker, marker_id, marker_nonce)?;
            match marker.phase {
                MountedSifsMutationOutboxPhase::Prepared => {
                    marker.phase = MountedSifsMutationOutboxPhase::AbortedUnstarted;
                }
                MountedSifsMutationOutboxPhase::AbortedUnstarted => {}
                _ => {
                    return Err(outbox_error(
                        "mounted-mutation AbortUnstarted is invalid after Arm",
                    ));
                }
            }
            Ok(())
        })
    }

    pub fn marker(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
    ) -> Result<Option<MountedSifsMutationOutboxMarker>, DfsError> {
        let path = self.operation_state_path(marker_id);
        if !path.exists() {
            return Ok(None);
        }
        let marker: MountedSifsMutationOutboxMarker = read_bounded_sobj(&path)?;
        validate_marker(&marker)?;
        if &marker.marker_id != marker_id {
            return Err(outbox_error(
                "mounted-mutation marker does not match its operation directory",
            ));
        }
        Ok(Some(marker))
    }

    pub fn status(&self) -> Result<MountedSifsMutationOutboxStatus, DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        self.scan_unlocked().map(|scan| scan.status)
    }

    /// Deterministic bounded snapshot for marker-specific live re-probes. The
    /// returned IDs include live/deferred markers; recovery still requires an
    /// authoritative lock/session claim.
    pub fn pending_marker_ids(&self) -> Result<Vec<MountedSifsMutationMarkerId>, DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        self.scan_unlocked().map(|scan| scan.pending_marker_ids)
    }

    /// Candidates which appeared recoverable during a bounded scan.
    /// [`Self::try_claim_recovery`] rechecks every lock and marker afterward.
    pub fn recovery_candidate_marker_ids(
        &self,
    ) -> Result<Vec<MountedSifsMutationMarkerId>, DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        self.scan_unlocked()
            .map(|scan| scan.recovery_candidate_marker_ids)
    }

    /// Claim an orphan only after acquiring and retaining its operation lock.
    /// Full recovery additionally requires positive proof that the referenced
    /// external host-mount session is no longer live.
    pub fn try_claim_recovery(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
    ) -> Result<Option<MountedSifsMutationRecoveryClaim>, DfsError> {
        if !self.operation_directory_is_plain_directory(marker_id)? {
            return Ok(None);
        }
        let Some(lease) = HeldLeaseLock::try_acquire_existing(
            &self.operation_directory(marker_id).join(LEASE_FILE),
        )?
        else {
            return Ok(None);
        };
        let Some(marker) = self.marker(marker_id)? else {
            return Ok(None);
        };
        verify_marker_identity(&marker, marker_id, &marker.marker_nonce)?;
        let recovery_kind = match marker_recovery_kind(&marker.phase) {
            Some(MountedSifsMutationRecoveryKind::Exact) => MountedSifsMutationRecoveryKind::Exact,
            Some(MountedSifsMutationRecoveryKind::Full) => {
                if !self.all_host_mount_sessions_proven_dead()? {
                    return Ok(None);
                }
                MountedSifsMutationRecoveryKind::Full
            }
            None => return Ok(None),
        };
        // Re-read after every lock/liveness decision while the operation claim
        // remains held. A recovery pass never acts on a pre-claim snapshot.
        let current = self
            .marker(marker_id)?
            .ok_or_else(|| outbox_error("mounted-mutation marker disappeared during claim"))?;
        if current != marker {
            return Err(outbox_error(
                "mounted-mutation marker changed while recovery was claiming it",
            ));
        }
        Ok(Some(MountedSifsMutationRecoveryClaim {
            store: self.clone(),
            marker,
            recovery_kind,
            _lease: lease,
        }))
    }

    /// Claim an undecodable marker only when its operation lease is dead and
    /// every host-mount session is positively closed/dead. This is deliberately
    /// broader than exact recovery because the corrupt state has lost its
    /// origin and nonce.
    pub fn try_claim_corrupt_recovery(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
    ) -> Result<Option<MountedSifsMutationCorruptRecoveryClaim>, DfsError> {
        let directory = self.operation_directory(marker_id);
        if !self.operation_directory_is_plain_directory(marker_id)? {
            return Ok(None);
        }
        let Some(lease) = HeldLeaseLock::try_acquire_existing(&directory.join(LEASE_FILE))? else {
            return Ok(None);
        };
        let state_path = directory.join(STATE_FILE);
        let state_is_corrupt = if state_path.is_file() {
            read_bounded_sobj::<MountedSifsMutationOutboxMarker>(&state_path).map_or(
                true,
                |marker| {
                    validate_marker(&marker).is_err()
                        || directory != self.operation_directory(&marker.marker_id)
                },
            )
        } else {
            !self.valid_creation_token(&directory, marker_id)
        };
        if !state_is_corrupt || !self.all_host_mount_sessions_proven_dead()? {
            return Ok(None);
        }
        // Recheck the corrupt state after the global session proof while the
        // operation claim remains held.
        let still_corrupt = if state_path.is_file() {
            read_bounded_sobj::<MountedSifsMutationOutboxMarker>(&state_path).map_or(
                true,
                |marker| {
                    validate_marker(&marker).is_err()
                        || directory != self.operation_directory(&marker.marker_id)
                },
            )
        } else {
            !self.valid_creation_token(&directory, marker_id)
        };
        if !still_corrupt {
            return Ok(None);
        }
        Ok(Some(MountedSifsMutationCorruptRecoveryClaim {
            store: self.clone(),
            marker_id: marker_id.clone(),
            _lease: lease,
        }))
    }

    fn status_unlocked(&self) -> Result<MountedSifsMutationOutboxStatus, DfsError> {
        self.scan_unlocked().map(|scan| scan.status)
    }

    fn scan_unlocked(&self) -> Result<MountedSifsMutationBoundedScan, DfsError> {
        let mut budget = MountedSifsMutationDirectoryScanBudget::production();
        self.scan_unlocked_with_budget(&mut budget)
    }

    fn scan_unlocked_with_budget(
        &self,
        budget: &mut MountedSifsMutationDirectoryScanBudget,
    ) -> Result<MountedSifsMutationBoundedScan, DfsError> {
        let mut status = MountedSifsMutationOutboxStatus::default();
        let mut cleanup_phase_remaining = MOUNTED_SIFS_MUTATION_OUTBOX_CLEANUP_SCAN_ENTRIES;
        let mut operation_phase_remaining = MOUNTED_SIFS_MUTATION_OUTBOX_OPERATION_SCAN_ENTRIES;
        let mut session_phase_remaining = MOUNTED_SIFS_MUTATION_OUTBOX_SESSION_SCAN_ENTRIES;
        // Keep only the lexicographically first `max + 1` pending entries.
        // Terminal history is pruned while streaming and never consumes the
        // pending bound or hides newer work.
        let mut pending = BTreeMap::<PathBuf, Option<MountedSifsMutationOutboxMarker>>::new();
        let mut referenced_sessions = BTreeSet::new();
        let operations = self.root.join(OPERATIONS_DIRECTORY);
        let mut operation_scan_complete = false;
        if !budget.limit_reached() && operation_phase_remaining > 0 {
            let mut operation_entries =
                fs::read_dir(&operations).map_err(|source| lock_error(&operations, source))?;
            loop {
                let entry = match budget.next_entry(
                    &mut operation_phase_remaining,
                    &operations,
                    &mut operation_entries,
                )? {
                    MountedSifsMutationDirectoryScanStep::Entry(entry) => entry,
                    MountedSifsMutationDirectoryScanStep::Complete => {
                        operation_scan_complete = true;
                        break;
                    }
                    MountedSifsMutationDirectoryScanStep::LimitExceeded => {
                        break;
                    }
                };
                let directory = entry.path();
                let file_type = entry
                    .file_type()
                    .map_err(|source| lock_error(&directory, source))?;
                if !file_type.is_dir() {
                    // The authoritative operations namespace contains only
                    // private marker directories. A file, symlink, or other
                    // entry has lost the state/lease shape needed for exact
                    // classification and must never disappear from readiness.
                    status.total_marker_count += 1;
                    status.pending_marker_count += 1;
                    status.malformed_marker_count += 1;
                    continue;
                }
                let state_path = directory.join(STATE_FILE);
                let marker: MountedSifsMutationOutboxMarker = match read_bounded_sobj(&state_path) {
                    Ok(marker)
                        if validate_marker(&marker).is_ok()
                            && directory == self.operation_directory(&marker.marker_id) =>
                    {
                        marker
                    }
                    Ok(_) | Err(_) => {
                        if self.try_prune_completed_corrupt_operation(&directory)? {
                            continue;
                        }
                        if !state_path.exists()
                            && self.try_prune_state_less_operation(&directory)?
                        {
                            continue;
                        }
                        status.total_marker_count += 1;
                        status.pending_marker_count += 1;
                        status.malformed_marker_count += 1;
                        pending.insert(directory, None);
                        if pending.len() > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS + 1 {
                            pending.pop_last();
                        }
                        continue;
                    }
                };
                if directory.join(CREATION_TOKEN_FILE).is_file() {
                    remove_file_durable(&directory.join(CREATION_TOKEN_FILE))?;
                }
                if is_terminal_phase(&marker.phase) {
                    if self.try_prune_terminal_operation(&directory, &marker)? {
                        continue;
                    }
                    status.total_marker_count += 1;
                    status.terminal_marker_count += 1;
                    continue;
                }
                status.total_marker_count += 1;
                status.pending_marker_count += 1;
                if referenced_sessions.len() <= MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
                    referenced_sessions.insert(marker.origin.mount_session_id.clone());
                }
                pending.insert(directory, Some(marker));
                if pending.len() > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS + 1 {
                    pending.pop_last();
                }
            }
        }
        if !operation_scan_complete {
            status.directory_scan_limit_exceeded = true;
        }
        status.marker_limit_exceeded =
            status.pending_marker_count > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS;

        // A staging session is not authoritative yet, but a live or partially
        // inspected staging lease can still publish into `sessions` after this
        // snapshot. Complete that bounded proof before authorizing any Full or
        // corrupt recovery. Exact operation candidates do not depend on it.
        let preparing_sessions_complete =
            self.prune_preparing_session_directories(budget, &mut session_phase_remaining)?;
        if !preparing_sessions_complete {
            status.directory_scan_limit_exceeded = true;
        }
        let mut full_recovery_sessions_dead = (!preparing_sessions_complete)
            .then_some(HostMountSessionsDeathProof::ScanLimitExceeded);

        let mut pending_marker_ids = Vec::new();
        let mut recovery_candidate_marker_ids = Vec::new();
        for (directory, marker) in pending
            .into_iter()
            .take(MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS)
        {
            let lease_path = directory.join(LEASE_FILE);
            let Some(marker) = marker else {
                let Some(marker_id) = directory
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| Id::new(name.to_string()).ok())
                else {
                    continue;
                };
                pending_marker_ids.push(marker_id.clone());
                match lock_liveness(&lease_path)? {
                    LeaseLiveness::Live => status.live_marker_count += 1,
                    LeaseLiveness::Unproven => status.unproven_session_marker_count += 1,
                    LeaseLiveness::Dead => {
                        let sessions_dead = match full_recovery_sessions_dead {
                            Some(value) => value,
                            None => {
                                let value = self.all_host_mount_sessions_proven_dead_with_budget(
                                    budget,
                                    &mut session_phase_remaining,
                                )?;
                                full_recovery_sessions_dead = Some(value);
                                value
                            }
                        };
                        match sessions_dead {
                            HostMountSessionsDeathProof::AllDead => {
                                status.full_recovery_marker_count += 1;
                                recovery_candidate_marker_ids.push(marker_id);
                            }
                            HostMountSessionsDeathProof::NotDeadOrUnproven => {
                                status.unproven_session_marker_count += 1;
                            }
                            HostMountSessionsDeathProof::ScanLimitExceeded => {
                                status.directory_scan_limit_exceeded = true;
                                status.unproven_session_marker_count += 1;
                            }
                        }
                    }
                }
                continue;
            };
            pending_marker_ids.push(marker.marker_id.clone());
            match lock_liveness(&lease_path)? {
                LeaseLiveness::Live => status.live_marker_count += 1,
                LeaseLiveness::Unproven => status.unproven_session_marker_count += 1,
                LeaseLiveness::Dead => match marker_recovery_kind(&marker.phase) {
                    Some(MountedSifsMutationRecoveryKind::Exact) => {
                        status.exact_recovery_marker_count += 1;
                        recovery_candidate_marker_ids.push(marker.marker_id.clone());
                    }
                    Some(MountedSifsMutationRecoveryKind::Full) => {
                        let sessions_dead = match full_recovery_sessions_dead {
                            Some(value) => value,
                            None => {
                                let value = self.all_host_mount_sessions_proven_dead_with_budget(
                                    budget,
                                    &mut session_phase_remaining,
                                )?;
                                full_recovery_sessions_dead = Some(value);
                                value
                            }
                        };
                        match sessions_dead {
                            HostMountSessionsDeathProof::AllDead => {
                                status.full_recovery_marker_count += 1;
                                recovery_candidate_marker_ids.push(marker.marker_id.clone());
                            }
                            HostMountSessionsDeathProof::NotDeadOrUnproven => {
                                if self.session_liveness(&marker.origin)? == LeaseLiveness::Live {
                                    status.deferred_live_session_marker_count += 1;
                                } else {
                                    status.unproven_session_marker_count += 1;
                                }
                            }
                            HostMountSessionsDeathProof::ScanLimitExceeded => {
                                status.directory_scan_limit_exceeded = true;
                                status.unproven_session_marker_count += 1;
                            }
                        }
                    }
                    None => status.terminal_marker_count += 1,
                },
            }
        }

        // A missing/undecodable or omitted origin cannot prove a session is
        // unreferenced, so session cleanup fails closed in those cases.
        if operation_scan_complete
            && !status.marker_limit_exceeded
            && status.malformed_marker_count == 0
        {
            if !self.prune_unreferenced_sessions(
                &referenced_sessions,
                budget,
                &mut session_phase_remaining,
            )? {
                status.directory_scan_limit_exceeded = true;
            }
        }
        // Retirement and unpublished-operation staging are cleanup-only
        // namespaces. They run last on their own quota so a wide cleanup tree
        // or slow deletion cannot suppress already-proven recovery candidates.
        if !self.prune_optional_directories(budget, &mut cleanup_phase_remaining)? {
            status.directory_scan_limit_exceeded = true;
        }
        Ok(MountedSifsMutationBoundedScan {
            status,
            pending_marker_ids,
            recovery_candidate_marker_ids,
        })
    }

    fn require_live_operation_and_session(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        let marker = self
            .marker(marker_id)?
            .ok_or_else(|| outbox_error(format!("unknown mounted-mutation marker {marker_id}")))?;
        verify_marker_identity(&marker, marker_id, marker_nonce)?;
        if lock_liveness(&self.operation_directory(marker_id).join(LEASE_FILE))?
            != LeaseLiveness::Live
        {
            return Err(outbox_error("mounted-mutation operation lease is not live"));
        }
        if self.session_liveness(&marker.origin)? != LeaseLiveness::Live {
            return Err(outbox_error(
                "mounted-mutation host-mount session is not live",
            ));
        }
        Ok(marker)
    }

    fn session_liveness(
        &self,
        origin: &MountedSifsMutationOrigin,
    ) -> Result<LeaseLiveness, DfsError> {
        let directory = self.session_directory(&origin.mount_session_id);
        let state_path = self.session_state_path(&origin.mount_session_id);
        let marker: MountedSifsHostMountSessionMarker = match read_bounded_sobj(&state_path) {
            Ok(marker) => marker,
            Err(_) => return Ok(LeaseLiveness::Unproven),
        };
        if validate_session_marker(&marker).is_err()
            || directory != self.session_directory(&marker.session_id)
            || marker.session_id != origin.mount_session_id
            || marker.session_nonce != origin.mount_session_nonce
            || marker.adapter != origin.adapter
        {
            return Ok(LeaseLiveness::Unproven);
        }
        // A Closed marker records intent to close, not proof that every
        // adapter thread has relinquished the session. Only the OS/process
        // lease can prove death; this also closes the write-Closed-before-Drop
        // race in `close_clean`.
        lock_liveness(&directory.join(LEASE_FILE))
    }

    fn all_host_mount_sessions_proven_dead(&self) -> Result<bool, DfsError> {
        let mut budget = MountedSifsMutationDirectoryScanBudget::production();
        let mut session_phase_remaining = MOUNTED_SIFS_MUTATION_OUTBOX_SESSION_SCAN_ENTRIES;
        if !self.prune_preparing_session_directories(&mut budget, &mut session_phase_remaining)? {
            return Ok(false);
        }
        self.all_host_mount_sessions_proven_dead_with_budget(
            &mut budget,
            &mut session_phase_remaining,
        )
        .map(|proof| proof == HostMountSessionsDeathProof::AllDead)
    }

    fn all_host_mount_sessions_proven_dead_with_budget(
        &self,
        budget: &mut MountedSifsMutationDirectoryScanBudget,
        session_phase_remaining: &mut usize,
    ) -> Result<HostMountSessionsDeathProof, DfsError> {
        let sessions = self.root.join(SESSIONS_DIRECTORY);
        if budget.limit_reached() || *session_phase_remaining == 0 {
            return Ok(HostMountSessionsDeathProof::ScanLimitExceeded);
        }
        let mut entries =
            fs::read_dir(&sessions).map_err(|source| lock_error(&sessions, source))?;
        loop {
            let entry = match budget.next_entry(session_phase_remaining, &sessions, &mut entries)? {
                MountedSifsMutationDirectoryScanStep::Entry(entry) => entry,
                MountedSifsMutationDirectoryScanStep::Complete => {
                    return Ok(HostMountSessionsDeathProof::AllDead);
                }
                MountedSifsMutationDirectoryScanStep::LimitExceeded => {
                    return Ok(HostMountSessionsDeathProof::ScanLimitExceeded);
                }
            };
            let directory = entry.path();
            if !entry
                .file_type()
                .map_err(|source| lock_error(&directory, source))?
                .is_dir()
            {
                return Ok(HostMountSessionsDeathProof::NotDeadOrUnproven);
            }
            let marker: MountedSifsHostMountSessionMarker =
                match read_bounded_sobj(&directory.join(STATE_FILE)) {
                    Ok(marker) => marker,
                    Err(_) => return Ok(HostMountSessionsDeathProof::NotDeadOrUnproven),
                };
            if validate_session_marker(&marker).is_err()
                || directory != self.session_directory(&marker.session_id)
            {
                return Ok(HostMountSessionsDeathProof::NotDeadOrUnproven);
            }
            if lock_liveness(&directory.join(LEASE_FILE))? != LeaseLiveness::Dead {
                return Ok(HostMountSessionsDeathProof::NotDeadOrUnproven);
            }
        }
    }

    fn valid_creation_token(
        &self,
        directory: &Path,
        marker_id: &MountedSifsMutationMarkerId,
    ) -> bool {
        read_bounded_sobj::<MountedSifsMutationCreationToken>(&directory.join(CREATION_TOKEN_FILE))
            .is_ok_and(|token| {
                token.schema_version == MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
                    && token.created_at_unix_nanos > 0
                    && &token.marker_id == marker_id
                    && directory == self.operation_directory(marker_id)
            })
    }

    fn try_prune_completed_corrupt_operation(&self, directory: &Path) -> Result<bool, DfsError> {
        let authorization: MountedSifsMutationCleanupAuthorization =
            match read_bounded_sobj(&directory.join(CLEANUP_AUTHORIZATION_FILE)) {
                Ok(authorization) => authorization,
                Err(_) => return Ok(false),
            };
        let marker_id = match authorization {
            MountedSifsMutationCleanupAuthorization::CorruptFullRecovery { receipt }
                if receipt.schema_version == MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
                    && validate_full_recovery_receipt(&receipt.recovery_receipt).is_ok() =>
            {
                receipt.marker_id
            }
            MountedSifsMutationCleanupAuthorization::Terminal { marker }
                if is_terminal_phase(&marker.phase)
                    && read_bounded_sobj::<MountedSifsMutationOutboxMarker>(
                        &directory.join(STATE_FILE),
                    )
                    .is_ok_and(|current| current == marker) =>
            {
                marker.marker_id
            }
            MountedSifsMutationCleanupAuthorization::UnpublishedMarker {
                marker_id,
                authorized_at_unix_nanos,
            } if authorized_at_unix_nanos > 0 && !directory.join(STATE_FILE).exists() => marker_id,
            _ => return Ok(false),
        };
        if directory != self.operation_directory(&marker_id) {
            return Ok(false);
        }
        let lease_path = directory.join(LEASE_FILE);
        let lease = if lease_path.is_file() {
            let Some(lease) = HeldLeaseLock::try_acquire_existing(&lease_path)? else {
                return Ok(false);
            };
            Some(lease)
        } else {
            None
        };
        drop(lease);
        self.retire_operation_directory(directory, &marker_id)?;
        Ok(true)
    }

    fn try_prune_terminal_operation(
        &self,
        directory: &Path,
        expected: &MountedSifsMutationOutboxMarker,
    ) -> Result<bool, DfsError> {
        if directory != self.operation_directory(&expected.marker_id) {
            return Ok(false);
        }
        let Some(lease) = HeldLeaseLock::try_acquire_existing(&directory.join(LEASE_FILE))? else {
            // A terminal marker whose adapter has not received the ACK keeps
            // its live lease and remains retryable.
            return Ok(false);
        };
        let state_lock_path = directory.join(STATE_LOCK_FILE);
        let state_lock = lock_file_exclusive(&state_lock_path)?;
        let current: MountedSifsMutationOutboxMarker =
            match read_bounded_sobj(&directory.join(STATE_FILE)) {
                Ok(marker) => marker,
                Err(_) => return Ok(false),
            };
        if &current != expected || !is_terminal_phase(&current.phase) {
            return Ok(false);
        }
        write_bounded_sobj(
            &directory.join(CLEANUP_AUTHORIZATION_FILE),
            &MountedSifsMutationCleanupAuthorization::Terminal {
                marker: current.clone(),
            },
        )?;
        drop(state_lock);
        drop(lease);
        self.retire_operation_directory(directory, &current.marker_id)?;
        Ok(true)
    }

    fn try_prune_state_less_operation(&self, directory: &Path) -> Result<bool, DfsError> {
        let state_path = directory.join(STATE_FILE);
        if state_path.exists() {
            return Ok(false);
        }
        if directory.join(CLEANUP_AUTHORIZATION_FILE).exists() {
            return self.try_prune_completed_corrupt_operation(directory);
        }
        let creation_token: MountedSifsMutationCreationToken =
            match read_bounded_sobj(&directory.join(CREATION_TOKEN_FILE)) {
                Ok(token) => token,
                Err(_) => return Ok(false),
            };
        let marker_id = creation_token.marker_id.clone();
        if creation_token.schema_version != MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
            || creation_token.created_at_unix_nanos == 0
            || directory != self.operation_directory(&marker_id)
        {
            return Ok(false);
        }
        let lease_path = directory.join(LEASE_FILE);
        let lease = if lease_path.is_file() {
            let Some(lease) = HeldLeaseLock::try_acquire_existing(&lease_path)? else {
                return Ok(false);
            };
            Some(lease)
        } else {
            None
        };
        let state_lock_path = directory.join(STATE_LOCK_FILE);
        let state_lock = lock_file_exclusive(&state_lock_path)?;
        if state_path.exists() {
            return Ok(false);
        }
        let current_token: MountedSifsMutationCreationToken =
            read_bounded_sobj(&directory.join(CREATION_TOKEN_FILE))?;
        if current_token != creation_token {
            return Ok(false);
        }
        write_bounded_sobj(
            &directory.join(CLEANUP_AUTHORIZATION_FILE),
            &MountedSifsMutationCleanupAuthorization::UnpublishedMarker {
                marker_id: marker_id.clone(),
                authorized_at_unix_nanos: now_unix_nanos(),
            },
        )?;
        drop(state_lock);
        drop(lease);
        self.retire_operation_directory(directory, &marker_id)?;
        Ok(true)
    }

    fn prune_unreferenced_sessions(
        &self,
        referenced_sessions: &BTreeSet<MountedSifsHostMountSessionId>,
        budget: &mut MountedSifsMutationDirectoryScanBudget,
        session_phase_remaining: &mut usize,
    ) -> Result<bool, DfsError> {
        let sessions = self.root.join(SESSIONS_DIRECTORY);
        if budget.limit_reached() || *session_phase_remaining == 0 {
            return Ok(false);
        }
        let mut entries =
            fs::read_dir(&sessions).map_err(|source| lock_error(&sessions, source))?;
        loop {
            let entry = match budget.next_entry(session_phase_remaining, &sessions, &mut entries)? {
                MountedSifsMutationDirectoryScanStep::Entry(entry) => entry,
                MountedSifsMutationDirectoryScanStep::Complete => return Ok(true),
                MountedSifsMutationDirectoryScanStep::LimitExceeded => return Ok(false),
            };
            let directory = entry.path();
            if !entry
                .file_type()
                .map_err(|source| lock_error(&directory, source))?
                .is_dir()
            {
                continue;
            }
            let state_path = directory.join(STATE_FILE);
            let marker: MountedSifsHostMountSessionMarker = match read_bounded_sobj(&state_path) {
                Ok(marker)
                    if validate_session_marker(&marker).is_ok()
                        && directory == self.session_directory(&marker.session_id) =>
                {
                    marker
                }
                Err(_) => continue,
                Ok(_) => continue,
            };
            if referenced_sessions.contains(&marker.session_id) {
                continue;
            }
            let Some(lease) = HeldLeaseLock::try_acquire_existing(&directory.join(LEASE_FILE))?
            else {
                continue;
            };
            let state_lock_path = directory.join(STATE_LOCK_FILE);
            let state_lock = lock_file_exclusive(&state_lock_path)?;
            let current: MountedSifsHostMountSessionMarker = match read_bounded_sobj(&state_path) {
                Ok(current)
                    if validate_session_marker(&current).is_ok()
                        && directory == self.session_directory(&current.session_id) =>
                {
                    current
                }
                Err(_) => continue,
                Ok(_) => continue,
            };
            if current != marker || referenced_sessions.contains(&current.session_id) {
                continue;
            }
            drop(state_lock);
            drop(lease);
            self.retire_session_directory(&directory, &current.session_id)?;
        }
    }

    fn retire_operation_directory(
        &self,
        directory: &Path,
        marker_id: &MountedSifsMutationMarkerId,
    ) -> Result<(), DfsError> {
        retire_directory(
            directory,
            &self
                .root
                .join(RETIRED_OPERATIONS_DIRECTORY)
                .join(marker_id.as_str()),
        )
    }

    fn retire_session_directory(
        &self,
        directory: &Path,
        session_id: &MountedSifsHostMountSessionId,
    ) -> Result<(), DfsError> {
        retire_directory(
            directory,
            &self
                .root
                .join(RETIRED_SESSIONS_DIRECTORY)
                .join(session_id.as_str()),
        )
    }

    fn prune_optional_directories(
        &self,
        budget: &mut MountedSifsMutationDirectoryScanBudget,
        cleanup_phase_remaining: &mut usize,
    ) -> Result<bool, DfsError> {
        for root in [
            self.root.join(PREPARING_OPERATIONS_DIRECTORY),
            self.root.join(RETIRED_OPERATIONS_DIRECTORY),
            self.root.join(RETIRED_SESSIONS_DIRECTORY),
        ] {
            if !prune_retired_directory_contents(&root, budget, cleanup_phase_remaining)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Staging session directories are never authoritative: adapters cannot
    /// prepare a mutation until the complete directory has been atomically
    /// renamed into `sessions`. A dead or never-created staging lease therefore
    /// proves that every file in this exact staging directory is safe to
    /// remove. Live/unreadable leases fail closed.
    fn prune_preparing_session_directories(
        &self,
        budget: &mut MountedSifsMutationDirectoryScanBudget,
        cleanup_phase_remaining: &mut usize,
    ) -> Result<bool, DfsError> {
        let root = self.root.join(PREPARING_SESSIONS_DIRECTORY);
        if budget.limit_reached() || *cleanup_phase_remaining == 0 {
            return Ok(false);
        }
        let mut entries = fs::read_dir(&root).map_err(|source| lock_error(&root, source))?;
        loop {
            let entry = match budget.next_entry(cleanup_phase_remaining, &root, &mut entries)? {
                MountedSifsMutationDirectoryScanStep::Entry(entry) => entry,
                MountedSifsMutationDirectoryScanStep::Complete => return Ok(true),
                MountedSifsMutationDirectoryScanStep::LimitExceeded => return Ok(false),
            };
            let directory = entry.path();
            if !entry
                .file_type()
                .map_err(|source| lock_error(&directory, source))?
                .is_dir()
            {
                return Err(outbox_error(format!(
                    "unexpected mounted-mutation preparing-session entry {}",
                    directory.display()
                )));
            }
            let Some(directory_name) = directory.file_name().and_then(|name| name.to_str()) else {
                return Err(outbox_error(
                    "mounted-mutation preparing-session name is not UTF-8",
                ));
            };
            Id::new(directory_name.to_string()).map_err(|source| {
                outbox_error(format!(
                    "invalid mounted-mutation preparing-session id {directory_name}: {source}"
                ))
            })?;

            let mut children = BTreeSet::new();
            if budget.limit_reached() || *cleanup_phase_remaining == 0 {
                return Ok(false);
            }
            let mut child_entries =
                fs::read_dir(&directory).map_err(|source| lock_error(&directory, source))?;
            loop {
                let child = match budget.next_entry(
                    cleanup_phase_remaining,
                    &directory,
                    &mut child_entries,
                )? {
                    MountedSifsMutationDirectoryScanStep::Entry(child) => child,
                    MountedSifsMutationDirectoryScanStep::Complete => break,
                    MountedSifsMutationDirectoryScanStep::LimitExceeded => return Ok(false),
                };
                let child_path = child.path();
                if !child
                    .file_type()
                    .map_err(|source| lock_error(&child_path, source))?
                    .is_file()
                {
                    return Err(outbox_error(format!(
                        "unexpected mounted-mutation preparing-session child {}",
                        child_path.display()
                    )));
                }
                let Some(name) = child.file_name().to_str().map(str::to_owned) else {
                    return Err(outbox_error(
                        "mounted-mutation preparing-session child name is not UTF-8",
                    ));
                };
                if !matches!(name.as_str(), LEASE_FILE | STATE_FILE | STATE_LOCK_FILE) {
                    return Err(outbox_error(format!(
                        "unexpected mounted-mutation preparing-session file {name}"
                    )));
                }
                children.insert(name);
            }

            let lease_path = directory.join(LEASE_FILE);
            let lease = if children.contains(LEASE_FILE) {
                let Some(lease) = HeldLeaseLock::try_acquire_existing(&lease_path)? else {
                    return Err(outbox_error(format!(
                        "mounted-mutation preparing session {} is still live",
                        directory.display()
                    )));
                };
                Some(lease)
            } else {
                None
            };
            drop(lease);
            for name in [STATE_FILE, STATE_LOCK_FILE, LEASE_FILE] {
                if children.contains(name) {
                    remove_file_durable(&directory.join(name))?;
                }
            }
            remove_empty_directory_durable(&directory)?;
        }
    }

    fn daemon_committed_work_latch_unlocked(
        &self,
    ) -> Result<MountedSifsMutationCommittedWorkLatch, DfsError> {
        let path = self.daemon_committed_work_latch_path();
        let decoded = read_bounded_sobj::<MountedSifsMutationCommittedWorkLatch>(&path);
        match decoded {
            Ok(latch)
                if latch.schema_version == MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
                    && (latch.generation == 0 || latch.updated_at_unix_nanos != 0) =>
            {
                Ok(latch)
            }
            Ok(_) | Err(_) => {
                // This file carries only coalesced wake ownership. On loss or
                // corruption, scheduling every bounded maintenance owner is a
                // safe self-heal and avoids a permanent mount STOP.
                let healed = MountedSifsMutationCommittedWorkLatch {
                    generation: 1,
                    committed_work: all_committed_work(),
                    updated_at_unix_nanos: now_unix_nanos(),
                    ..MountedSifsMutationCommittedWorkLatch::default()
                };
                self.write_daemon_committed_work_latch_unlocked(&healed)?;
                Ok(healed)
            }
        }
    }

    fn retain_daemon_committed_work_unlocked(
        &self,
        committed_work: DfsCommittedWork,
    ) -> Result<MountedSifsMutationCommittedWorkLatch, DfsError> {
        let mut latch = self.daemon_committed_work_latch_unlocked()?;
        let retained_before = latch.committed_work;
        latch.committed_work.merge(committed_work);
        if latch.committed_work == retained_before {
            return Ok(latch);
        }
        latch.generation = latch.generation.checked_add(1).ok_or_else(|| {
            outbox_error("mounted-mutation committed-work latch generation overflow")
        })?;
        latch.updated_at_unix_nanos = now_unix_nanos();
        self.write_daemon_committed_work_latch_unlocked(&latch)?;
        Ok(latch)
    }

    fn write_daemon_committed_work_latch_unlocked(
        &self,
        latch: &MountedSifsMutationCommittedWorkLatch,
    ) -> Result<(), DfsError> {
        if latch.schema_version != MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
            || (latch.generation != 0 && latch.updated_at_unix_nanos == 0)
        {
            return Err(outbox_error(
                "mounted-mutation committed-work latch is invalid",
            ));
        }
        write_bounded_sobj(&self.daemon_committed_work_latch_path(), latch)
    }

    fn accept_live_with_durable_work(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        marker_nonce: &MountedSifsMutationNonce,
        committed_work: DfsCommittedWork,
        transition: impl FnOnce(
            &MountedSifsMutationOutboxMarker,
        ) -> Result<Option<MountedSifsMutationOutboxPhase>, DfsError>,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        let directory = self.operation_directory(marker_id);
        if lock_liveness(&directory.join(LEASE_FILE))? != LeaseLiveness::Live {
            return Err(outbox_error("mounted-mutation operation lease is not live"));
        }
        let _state_lock = lock_file_exclusive(&directory.join(STATE_LOCK_FILE))?;
        let mut marker: MountedSifsMutationOutboxMarker =
            read_bounded_sobj(&directory.join(STATE_FILE))?;
        verify_marker_identity(&marker, marker_id, marker_nonce)?;
        if self.session_liveness(&marker.origin)? != LeaseLiveness::Live {
            return Err(outbox_error(
                "mounted-mutation host-mount session is not live",
            ));
        }
        let Some(next_phase) = transition(&marker)? else {
            return Ok(marker);
        };
        // This fsync is the authoritative durable owner. If the following
        // marker write fails or the process crashes, startup still replays the
        // coalesced work; the live caller can retry the exact marker/report.
        self.retain_daemon_committed_work_unlocked(committed_work)?;
        marker.phase = next_phase;
        marker.updated_at_unix_nanos = now_unix_nanos();
        self.write_operation_marker_unlocked(&marker)?;
        Ok(marker)
    }

    fn terminalize_recovery_claim(
        &self,
        expected: &MountedSifsMutationOutboxMarker,
        recovery_kind: MountedSifsMutationRecoveryKind,
        recovery_receipt: MountedSifsMutationRecoveryReceipt,
    ) -> Result<(), DfsError> {
        let _store_lock = lock_file_exclusive(&self.root.join(STORE_LOCK_FILE))?;
        let directory = self.operation_directory(&expected.marker_id);
        let _state_lock = lock_file_exclusive(&directory.join(STATE_LOCK_FILE))?;
        let mut current: MountedSifsMutationOutboxMarker =
            read_bounded_sobj(&directory.join(STATE_FILE))?;
        if &current != expected || marker_recovery_kind(&current.phase) != Some(recovery_kind) {
            return Err(outbox_error(
                "mounted-mutation marker changed before recovery terminalization",
            ));
        }
        let retained_work = if recovery_kind == MountedSifsMutationRecoveryKind::Full {
            all_committed_work()
        } else {
            captured_work_for_phase(&current.phase)
                .map(|work| work.committed_work)
                .unwrap_or_default()
        };
        self.retain_daemon_committed_work_unlocked(retained_work)?;
        current.phase = MountedSifsMutationOutboxPhase::Terminal { recovery_receipt };
        current.updated_at_unix_nanos = now_unix_nanos();
        self.write_operation_marker_unlocked(&current)
    }

    fn mutate_operation_marker(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
        mutation: impl FnOnce(&mut MountedSifsMutationOutboxMarker) -> Result<(), DfsError>,
    ) -> Result<MountedSifsMutationOutboxMarker, DfsError> {
        let directory = self.operation_directory(marker_id);
        let _state_lock = lock_file_exclusive(&directory.join(STATE_LOCK_FILE))?;
        let mut marker: MountedSifsMutationOutboxMarker =
            read_bounded_sobj(&directory.join(STATE_FILE))?;
        mutation(&mut marker)?;
        marker.updated_at_unix_nanos = now_unix_nanos();
        self.write_operation_marker_unlocked(&marker)?;
        Ok(marker)
    }

    fn write_operation_marker(
        &self,
        marker: &MountedSifsMutationOutboxMarker,
    ) -> Result<(), DfsError> {
        let directory = self.operation_directory(&marker.marker_id);
        let _state_lock = lock_file_exclusive(&directory.join(STATE_LOCK_FILE))?;
        self.write_operation_marker_unlocked(marker)
    }

    fn write_operation_marker_unlocked(
        &self,
        marker: &MountedSifsMutationOutboxMarker,
    ) -> Result<(), DfsError> {
        validate_marker(marker)?;
        write_bounded_sobj(&self.operation_state_path(&marker.marker_id), marker)
    }

    fn write_session_marker(
        &self,
        marker: &MountedSifsHostMountSessionMarker,
    ) -> Result<(), DfsError> {
        let directory = self.session_directory(&marker.session_id);
        self.write_session_marker_in_directory(&directory, marker)
    }

    fn write_session_marker_in_directory(
        &self,
        directory: &Path,
        marker: &MountedSifsHostMountSessionMarker,
    ) -> Result<(), DfsError> {
        validate_session_marker(marker)?;
        let _state_lock = lock_file_exclusive(&directory.join(STATE_LOCK_FILE))?;
        write_bounded_sobj(&directory.join(STATE_FILE), marker)
    }

    fn operation_directory(&self, marker_id: &MountedSifsMutationMarkerId) -> PathBuf {
        self.root
            .join(OPERATIONS_DIRECTORY)
            .join(marker_id.as_str())
    }

    fn operation_directory_is_plain_directory(
        &self,
        marker_id: &MountedSifsMutationMarkerId,
    ) -> Result<bool, DfsError> {
        let directory = self.operation_directory(marker_id);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) => Ok(metadata.file_type().is_dir()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(source) => Err(lock_error(&directory, source)),
        }
    }

    fn operation_state_path(&self, marker_id: &MountedSifsMutationMarkerId) -> PathBuf {
        self.operation_directory(marker_id).join(STATE_FILE)
    }

    fn session_directory(&self, session_id: &MountedSifsHostMountSessionId) -> PathBuf {
        self.root.join(SESSIONS_DIRECTORY).join(session_id.as_str())
    }

    fn session_state_path(&self, session_id: &MountedSifsHostMountSessionId) -> PathBuf {
        self.session_directory(session_id).join(STATE_FILE)
    }

    fn daemon_committed_work_latch_path(&self) -> PathBuf {
        self.root.join(DAEMON_COMMITTED_WORK_LATCH_FILE)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LeaseLiveness {
    Live,
    Dead,
    Unproven,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HostMountSessionsDeathProof {
    AllDead,
    NotDeadOrUnproven,
    ScanLimitExceeded,
}

#[derive(Debug)]
struct MountedSifsMutationDirectoryScanBudget {
    remaining_entries: usize,
    visited_entries: usize,
    deadline: Instant,
}

#[derive(Debug)]
enum MountedSifsMutationDirectoryScanStep {
    Entry(fs::DirEntry),
    Complete,
    LimitExceeded,
}

impl MountedSifsMutationDirectoryScanBudget {
    fn production() -> Self {
        Self::with_limits(
            MOUNTED_SIFS_MUTATION_OUTBOX_MAX_DIRECTORY_SCAN_ENTRIES,
            MOUNTED_SIFS_MUTATION_OUTBOX_DIRECTORY_SCAN_TIMEOUT,
        )
    }

    fn with_limits(max_entries: usize, timeout: Duration) -> Self {
        let started = Instant::now();
        Self {
            remaining_entries: max_entries,
            visited_entries: 0,
            deadline: started.checked_add(timeout).unwrap_or(started),
        }
    }

    fn limit_reached(&self) -> bool {
        self.remaining_entries == 0 || Instant::now() >= self.deadline
    }

    fn next_entry(
        &mut self,
        phase_remaining_entries: &mut usize,
        directory: &Path,
        entries: &mut fs::ReadDir,
    ) -> Result<MountedSifsMutationDirectoryScanStep, DfsError> {
        if self.limit_reached() || *phase_remaining_entries == 0 {
            return Ok(MountedSifsMutationDirectoryScanStep::LimitExceeded);
        }
        let Some(entry) = entries.next() else {
            return Ok(MountedSifsMutationDirectoryScanStep::Complete);
        };
        let entry = entry.map_err(|source| lock_error(directory, source))?;
        self.remaining_entries -= 1;
        *phase_remaining_entries -= 1;
        self.visited_entries += 1;
        // The final unit in either the global or phase allocation is a charged
        // lookahead. It distinguishes a complete supported-size directory from
        // an oversized one without processing more entry work than the bound.
        if self.remaining_entries == 0
            || *phase_remaining_entries == 0
            || Instant::now() >= self.deadline
        {
            return Ok(MountedSifsMutationDirectoryScanStep::LimitExceeded);
        }
        Ok(MountedSifsMutationDirectoryScanStep::Entry(entry))
    }
}

#[derive(Debug)]
struct HeldLeaseLock {
    file: File,
    path: PathBuf,
    held: bool,
}

#[derive(Debug)]
struct MountedSifsMutationBoundedScan {
    status: MountedSifsMutationOutboxStatus,
    pending_marker_ids: Vec<MountedSifsMutationMarkerId>,
    recovery_candidate_marker_ids: Vec<MountedSifsMutationMarkerId>,
}

impl HeldLeaseLock {
    fn acquire_new(path: &Path) -> Result<Self, DfsError> {
        register_process_lock(path)?;
        let result = (|| {
            let file = open_lock_file(path)?;
            FileExt::try_lock_exclusive(&file).map_err(|source| lock_error(path, source))?;
            Ok(Self {
                file,
                path: path.to_path_buf(),
                held: true,
            })
        })();
        if result.is_err() {
            unregister_process_lock(path);
        }
        result
    }

    fn try_acquire_existing(path: &Path) -> Result<Option<Self>, DfsError> {
        if !path.is_file() || !try_register_process_lock(path)? {
            return Ok(None);
        }
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(source) => {
                unregister_process_lock(path);
                if source.kind() == io::ErrorKind::NotFound {
                    return Ok(None);
                }
                return Err(lock_error(path, source));
            }
        };
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(Self {
                file,
                path: path.to_path_buf(),
                held: true,
            })),
            Err(source) if fs2_lock_is_contended(&source) => {
                unregister_process_lock(path);
                Ok(None)
            }
            Err(source) => {
                unregister_process_lock(path);
                Err(lock_error(path, source))
            }
        }
    }

    fn is_held(&self) -> bool {
        self.held
    }
}

impl Drop for HeldLeaseLock {
    fn drop(&mut self) {
        if self.held {
            let _ = FileExt::unlock(&self.file);
            unregister_process_lock(&self.path);
            self.held = false;
        }
    }
}

fn process_locks() -> &'static Mutex<BTreeSet<PathBuf>> {
    static LOCKS: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

fn register_process_lock(path: &Path) -> Result<(), DfsError> {
    if try_register_process_lock(path)? {
        Ok(())
    } else {
        Err(outbox_error(format!(
            "mounted-mutation lease is already held in this process: {}",
            path.display()
        )))
    }
}

fn try_register_process_lock(path: &Path) -> Result<bool, DfsError> {
    let mut locks = process_locks()
        .lock()
        .map_err(|_| outbox_error("mounted-mutation process lock registry is poisoned"))?;
    Ok(locks.insert(path.to_path_buf()))
}

fn unregister_process_lock(path: &Path) {
    if let Ok(mut locks) = process_locks().lock() {
        locks.remove(path);
    }
}

fn process_lock_is_held(path: &Path) -> Result<bool, DfsError> {
    let locks = process_locks()
        .lock()
        .map_err(|_| outbox_error("mounted-mutation process lock registry is poisoned"))?;
    Ok(locks.contains(path))
}

fn lock_liveness(path: &Path) -> Result<LeaseLiveness, DfsError> {
    if process_lock_is_held(path)? {
        return Ok(LeaseLiveness::Live);
    }
    let file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            return Ok(LeaseLiveness::Unproven);
        }
        Err(_) => return Ok(LeaseLiveness::Unproven),
    };
    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            let _ = FileExt::unlock(&file);
            Ok(LeaseLiveness::Dead)
        }
        Err(source) if fs2_lock_is_contended(&source) => Ok(LeaseLiveness::Live),
        Err(_) => Ok(LeaseLiveness::Unproven),
    }
}

/// `fs2` exposes the platform's canonical nonblocking-lock contention error.
/// Windows reports `ERROR_LOCK_VIOLATION` (33), which Rust does not classify as
/// [`io::ErrorKind::WouldBlock`]. Compare the raw platform code when available
/// so DLL/EXE boundaries can prove lease liveness without broadening every
/// `Uncategorized` error into a contention result.
fn fs2_lock_is_contended(source: &io::Error) -> bool {
    let contended = fs2::lock_contended_error();
    match (source.raw_os_error(), contended.raw_os_error()) {
        (Some(source), Some(contended)) => source == contended,
        _ => source.kind() == contended.kind(),
    }
}

#[track_caller]
fn lock_file_exclusive(path: &Path) -> Result<ExclusiveFileLock, DfsError> {
    lock_file_exclusive_with_timeout(path, MOUNTED_SIFS_MUTATION_OUTBOX_LOCK_ACQUIRE_TIMEOUT)
}

#[track_caller]
fn lock_file_exclusive_with_timeout(
    path: &Path,
    timeout: Duration,
) -> Result<ExclusiveFileLock, DfsError> {
    let caller = std::panic::Location::caller();
    let file = open_lock_file(path)?;
    let started = Instant::now();
    let deadline = started.checked_add(timeout).unwrap_or(started);
    loop {
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                if path.file_name().and_then(|name| name.to_str()) == Some(STORE_LOCK_FILE) {
                    tracing::info!(
                        target: "sinomo.dfs.mounted_mutation.lock",
                        lock_path = %path.display(),
                        caller_file = caller.file(),
                        caller_line = caller.line(),
                        thread_name = std::thread::current().name(),
                        "acquired mounted-mutation global store lock"
                    );
                }
                return Ok(ExclusiveFileLock {
                    file,
                    path: path.to_path_buf(),
                    caller_file: caller.file(),
                    caller_line: caller.line(),
                });
            }
            Err(source) if fs2_lock_is_contended(&source) => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(outbox_error(format!(
                        "timed out acquiring mounted-mutation outbox lock {} after {} ms",
                        path.display(),
                        timeout.as_millis()
                    )));
                }
                std::thread::sleep(
                    deadline
                        .saturating_duration_since(now)
                        .min(MOUNTED_SIFS_MUTATION_OUTBOX_LOCK_RETRY_INTERVAL),
                );
            }
            Err(source) => return Err(lock_error(path, source)),
        }
    }
}

#[derive(Debug)]
struct ExclusiveFileLock {
    file: File,
    path: PathBuf,
    caller_file: &'static str,
    caller_line: u32,
}

impl Drop for ExclusiveFileLock {
    fn drop(&mut self) {
        // `fs2` recommends explicitly unlocking instead of relying on closing
        // the file handle because close-time lock release differs by platform.
        // This is especially important on Windows where the main executable
        // and an external provider DLL independently open `outbox.lock`.
        match FileExt::unlock(&self.file) {
            Ok(()) => {
                if self.path.file_name().and_then(|name| name.to_str()) == Some(STORE_LOCK_FILE) {
                    tracing::info!(
                        target: "sinomo.dfs.mounted_mutation.lock",
                        lock_path = %self.path.display(),
                        caller_file = self.caller_file,
                        caller_line = self.caller_line,
                        thread_name = std::thread::current().name(),
                        "released mounted-mutation global store lock"
                    );
                }
            }
            Err(source) => tracing::warn!(
                target: "sinomo.dfs.mounted_mutation.lock",
                lock_path = %self.path.display(),
                caller_file = self.caller_file,
                caller_line = self.caller_line,
                error = %source,
                "failed to release mounted-mutation file lock"
            ),
        }
    }
}

fn open_lock_file(path: &Path) -> Result<File, DfsError> {
    let parent = path
        .parent()
        .ok_or_else(|| outbox_error("mounted-mutation lock has no parent"))?;
    match OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(file) => {
            durable_file::sync_directory(parent).map_err(durable_error)?;
            Ok(file)
        }
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| lock_error(path, source)),
        Err(source) => Err(lock_error(path, source)),
    }
}

fn write_bounded_sobj<T: Serialize>(path: &Path, value: &T) -> Result<(), DfsError> {
    let bytes = sinomo_object_serde::to_bytes(value, 1, LengthMode::U32)
        .map_err(|source| outbox_error(format!("cannot encode {}: {source}", path.display())))?;
    if bytes.len() > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES {
        return Err(outbox_error(format!(
            "mounted-mutation state {} is {} bytes; max is {}",
            path.display(),
            bytes.len(),
            MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES
        )));
    }
    durable_file::write_file_atomic(
        path,
        &bytes,
        DurableAtomicFileOptions::new("state.sobj", "mounted-mutation-state").sync_all(),
    )
    .map_err(durable_error)
}

fn read_bounded_sobj<T: DeserializeOwned>(path: &Path) -> Result<T, DfsError> {
    let metadata = fs::metadata(path).map_err(|source| lock_error(path, source))?;
    if metadata.len() > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES as u64 {
        return Err(outbox_error(format!(
            "mounted-mutation state {} exceeds {} bytes",
            path.display(),
            MOUNTED_SIFS_MUTATION_OUTBOX_MAX_STATE_BYTES
        )));
    }
    let bytes = fs::read(path).map_err(|source| lock_error(path, source))?;
    sinomo_object_serde::from_bytes(&bytes)
        .map_err(|source| outbox_error(format!("cannot decode {}: {source}", path.display())))
}

fn remove_file_durable(path: &Path) -> Result<(), DfsError> {
    durable_file::remove_file_if_exists_and_sync_parent(path)
        .map(|_| ())
        .map_err(durable_error)
}

fn remove_empty_directory_durable(path: &Path) -> Result<(), DfsError> {
    durable_file::remove_empty_dir_if_exists_and_sync_parent(path)
        .map(|_| ())
        .map_err(durable_error)
}

fn retire_directory(source: &Path, destination: &Path) -> Result<(), DfsError> {
    let source_parent = source
        .parent()
        .ok_or_else(|| outbox_error("mounted-mutation retirement source has no parent"))?;
    let destination_parent = destination
        .parent()
        .ok_or_else(|| outbox_error("mounted-mutation retirement destination has no parent"))?;
    fs::rename(source, destination).map_err(|source| lock_error(destination, source))?;
    durable_file::sync_directory(source_parent).map_err(durable_error)?;
    if destination_parent != source_parent {
        durable_file::sync_directory(destination_parent).map_err(durable_error)?;
    }
    Ok(())
}

fn publish_host_mount_session_directory(
    preparing_directory: &Path,
    published_directory: &Path,
    staged_lease: HeldLeaseLock,
) -> Result<HeldLeaseLock, DfsError> {
    publish_host_mount_session_directory_with_cuts(
        preparing_directory,
        published_directory,
        staged_lease,
        |_| Ok(()),
        |_| Ok(()),
    )
}

fn publish_host_mount_session_directory_with_cuts<BeforeRename, BeforeReacquire>(
    preparing_directory: &Path,
    published_directory: &Path,
    staged_lease: HeldLeaseLock,
    before_rename: BeforeRename,
    before_reacquire: BeforeReacquire,
) -> Result<HeldLeaseLock, DfsError>
where
    BeforeRename: FnOnce(&Path) -> Result<(), DfsError>,
    BeforeReacquire: FnOnce(&Path) -> Result<(), DfsError>,
{
    let preparing_lease_path = preparing_directory.join(LEASE_FILE);
    if staged_lease.path != preparing_lease_path || !staged_lease.is_held() {
        return Err(outbox_error(
            "mounted-mutation session publication requires its live staging lease",
        ));
    }

    // Dropping also closes the child file handle after unlocking it. This must
    // happen before `fs::rename` on Windows; `outbox.lock` remains held by the
    // caller and prevents a cooperating scan from entering this short gap.
    drop(staged_lease);
    if process_lock_is_held(&preparing_lease_path)? {
        return Err(outbox_error(format!(
            "mounted-mutation staging lease remained registered before publication: {}",
            preparing_lease_path.display()
        )));
    }
    before_rename(&preparing_lease_path)?;
    retire_directory(preparing_directory, published_directory)?;

    let published_lease_path = published_directory.join(LEASE_FILE);
    before_reacquire(&published_lease_path)?;
    HeldLeaseLock::try_acquire_existing(&published_lease_path)?.ok_or_else(|| {
        outbox_error(format!(
            "mounted-mutation published session lease is unavailable: {}",
            published_lease_path.display()
        ))
    })
}

fn prune_retired_directory_contents(
    root: &Path,
    budget: &mut MountedSifsMutationDirectoryScanBudget,
    cleanup_phase_remaining: &mut usize,
) -> Result<bool, DfsError> {
    if budget.limit_reached() || *cleanup_phase_remaining == 0 {
        return Ok(false);
    }
    let mut entries = fs::read_dir(root).map_err(|source| lock_error(root, source))?;
    loop {
        let entry = match budget.next_entry(cleanup_phase_remaining, root, &mut entries)? {
            MountedSifsMutationDirectoryScanStep::Entry(entry) => entry,
            MountedSifsMutationDirectoryScanStep::Complete => return Ok(true),
            MountedSifsMutationDirectoryScanStep::LimitExceeded => return Ok(false),
        };
        let directory = entry.path();
        if !entry
            .file_type()
            .map_err(|source| lock_error(&directory, source))?
            .is_dir()
        {
            continue;
        }
        if budget.limit_reached() || *cleanup_phase_remaining == 0 {
            return Ok(false);
        }
        let mut child_entries =
            fs::read_dir(&directory).map_err(|source| lock_error(&directory, source))?;
        loop {
            let child =
                match budget.next_entry(cleanup_phase_remaining, &directory, &mut child_entries)? {
                    MountedSifsMutationDirectoryScanStep::Entry(child) => child,
                    MountedSifsMutationDirectoryScanStep::Complete => break,
                    MountedSifsMutationDirectoryScanStep::LimitExceeded => return Ok(false),
                };
            let child_path = child.path();
            if child
                .file_type()
                .map_err(|source| lock_error(&child_path, source))?
                .is_file()
            {
                remove_file_durable(&child_path)?;
            }
        }
        remove_empty_directory_durable(&directory)?;
    }
}

fn validate_marker(marker: &MountedSifsMutationOutboxMarker) -> Result<(), DfsError> {
    if marker.schema_version != MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION {
        return Err(outbox_error(format!(
            "unsupported mounted-mutation marker schema {}",
            marker.schema_version
        )));
    }
    validate_origin(&marker.origin)?;
    match &marker.phase {
        MountedSifsMutationOutboxPhase::Captured { captured_work, .. } => {
            validate_captured_work(captured_work)?
        }
        MountedSifsMutationOutboxPhase::DaemonAccepted {
            captured_work,
            requires_full_recovery,
            error_observation,
            ..
        } => {
            validate_captured_work(captured_work)?;
            match (*requires_full_recovery, error_observation) {
                (false, None) => {}
                (true, Some(observation)) if &observation.captured_work == captured_work => {
                    validate_error_observation(observation)?;
                }
                _ => {
                    return Err(outbox_error(
                        "mounted-mutation accepted phase has inconsistent error evidence",
                    ));
                }
            }
        }
        MountedSifsMutationOutboxPhase::UnknownAfterError { observation, .. } => {
            validate_error_observation(observation)?
        }
        MountedSifsMutationOutboxPhase::ProvenNoWrite { proof_code, .. } => {
            validate_label("proof_code", proof_code, false)?
        }
        _ => {}
    }
    Ok(())
}

fn validate_session_marker(marker: &MountedSifsHostMountSessionMarker) -> Result<(), DfsError> {
    if marker.schema_version != MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION {
        return Err(outbox_error(format!(
            "unsupported mounted-mutation host-session schema {}",
            marker.schema_version
        )));
    }
    validate_label("mount_identity", &marker.mount_identity, false)?;
    if marker.started_at_unix_nanos == 0
        || marker.updated_at_unix_nanos < marker.started_at_unix_nanos
    {
        return Err(outbox_error(
            "mounted-mutation host-session timestamps are invalid",
        ));
    }
    Ok(())
}

fn validate_origin(origin: &MountedSifsMutationOrigin) -> Result<(), DfsError> {
    if let Some(operation_id) = &origin.stable_operation_id {
        validate_label("stable_operation_id", operation_id, false)?;
    }
    validate_label("operation", &origin.operation, false)
}

fn validate_captured_work(work: &MountedSifsMutationCapturedWork) -> Result<(), DfsError> {
    if work.committed_repair_records.len() > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_REPAIR_RECORDS {
        return Err(outbox_error(format!(
            "mounted-mutation captured {} repair records; max is {}",
            work.committed_repair_records.len(),
            MOUNTED_SIFS_MUTATION_OUTBOX_MAX_REPAIR_RECORDS
        )));
    }
    Ok(())
}

fn validate_error_observation(
    observation: &MountedSifsMutationErrorObservation,
) -> Result<(), DfsError> {
    validate_captured_work(&observation.captured_work)?;
    validate_label("error_code", &observation.error_code, false)
}

fn validate_label(field: &str, value: &str, allow_empty: bool) -> Result<(), DfsError> {
    if !allow_empty && value.is_empty() {
        return Err(outbox_error(format!(
            "mounted-mutation {field} cannot be empty"
        )));
    }
    if value.len() > MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES {
        return Err(outbox_error(format!(
            "mounted-mutation {field} is {} bytes; max is {}",
            value.len(),
            MOUNTED_SIFS_MUTATION_OUTBOX_MAX_LABEL_BYTES
        )));
    }
    Ok(())
}

fn require_matching_armed(
    marker: &MountedSifsMutationOutboxMarker,
    arm_request_id: &MountedSifsMutationArmRequestId,
) -> Result<(), DfsError> {
    match &marker.phase {
        MountedSifsMutationOutboxPhase::Armed {
            arm_request_id: current,
        } if current == arm_request_id => Ok(()),
        _ => Err(outbox_error(
            "mounted-mutation capture requires the matching durable Arm",
        )),
    }
}

fn verify_marker_identity(
    marker: &MountedSifsMutationOutboxMarker,
    marker_id: &MountedSifsMutationMarkerId,
    marker_nonce: &MountedSifsMutationNonce,
) -> Result<(), DfsError> {
    if marker.schema_version != MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION
        || &marker.marker_id != marker_id
        || &marker.marker_nonce != marker_nonce
    {
        return Err(outbox_error(
            "mounted-mutation marker identity or schema does not match",
        ));
    }
    Ok(())
}

fn marker_recovery_kind(
    phase: &MountedSifsMutationOutboxPhase,
) -> Option<MountedSifsMutationRecoveryKind> {
    match phase {
        MountedSifsMutationOutboxPhase::Captured { .. } => {
            Some(MountedSifsMutationRecoveryKind::Exact)
        }
        MountedSifsMutationOutboxPhase::Prepared
        | MountedSifsMutationOutboxPhase::Armed { .. }
        | MountedSifsMutationOutboxPhase::UnknownAfterError { .. }
        | MountedSifsMutationOutboxPhase::DaemonAccepted {
            requires_full_recovery: true,
            ..
        }
        | MountedSifsMutationOutboxPhase::DaemonAccepted {
            requires_full_recovery: false,
            error_observation: Some(_),
            ..
        } => Some(MountedSifsMutationRecoveryKind::Full),
        MountedSifsMutationOutboxPhase::ProvenNoWrite { .. }
        | MountedSifsMutationOutboxPhase::DaemonAccepted {
            requires_full_recovery: false,
            error_observation: None,
            ..
        }
        | MountedSifsMutationOutboxPhase::AbortedUnstarted
        | MountedSifsMutationOutboxPhase::Terminal { .. } => None,
    }
}

fn is_terminal_phase(phase: &MountedSifsMutationOutboxPhase) -> bool {
    match phase {
        MountedSifsMutationOutboxPhase::ProvenNoWrite { .. }
        | MountedSifsMutationOutboxPhase::AbortedUnstarted
        | MountedSifsMutationOutboxPhase::Terminal { .. }
        | MountedSifsMutationOutboxPhase::DaemonAccepted {
            requires_full_recovery: false,
            error_observation: None,
            ..
        } => true,
        _ => false,
    }
}

fn captured_work_for_phase(
    phase: &MountedSifsMutationOutboxPhase,
) -> Option<&MountedSifsMutationCapturedWork> {
    match phase {
        MountedSifsMutationOutboxPhase::Captured { captured_work, .. }
        | MountedSifsMutationOutboxPhase::DaemonAccepted { captured_work, .. } => {
            Some(captured_work)
        }
        MountedSifsMutationOutboxPhase::UnknownAfterError { observation, .. } => {
            Some(&observation.captured_work)
        }
        _ => None,
    }
}

fn all_committed_work() -> DfsCommittedWork {
    DfsCommittedWork {
        publish_metadata: true,
        repair_maintenance: true,
        namespace_maintenance: true,
        metadata_replica_progress: true,
        metadata_materialization: true,
    }
}

fn validate_recovery_receipt(
    marker: &MountedSifsMutationOutboxMarker,
    recovery_kind: MountedSifsMutationRecoveryKind,
    receipt: &MountedSifsMutationRecoveryReceipt,
) -> Result<(), DfsError> {
    if receipt.completed_at_unix_nanos == 0 {
        return Err(outbox_error(
            "mounted-mutation recovery receipt has no completion time",
        ));
    }
    let captured_work = captured_work_for_phase(&marker.phase);
    if let Some(work) = captured_work {
        if !work.committed_repair_records.is_empty() && !receipt.repair_queue_durable {
            return Err(outbox_error(
                "mounted-mutation recovery has not durably owned its repair queue delta",
            ));
        }
        if work.committed_work.publish_metadata && !receipt.metadata_publish_durably_owned {
            return Err(outbox_error(
                "mounted-mutation recovery has no durable metadata-publication owner",
            ));
        }
        if (work.committed_work.repair_maintenance || work.committed_work.namespace_maintenance)
            && !receipt.maintenance_durably_owned
        {
            return Err(outbox_error(
                "mounted-mutation recovery has no durable maintenance owner",
            ));
        }
        if (work.committed_work.metadata_replica_progress
            || work.committed_work.metadata_materialization)
            && !receipt.metadata_replica_progress_durably_owned
        {
            return Err(outbox_error(
                "mounted-mutation recovery has no durable metadata-replica-progress owner",
            ));
        }
    }
    if recovery_kind == MountedSifsMutationRecoveryKind::Full {
        validate_full_recovery_receipt(receipt)?;
    }
    Ok(())
}

fn validate_full_recovery_receipt(
    receipt: &MountedSifsMutationRecoveryReceipt,
) -> Result<(), DfsError> {
    if receipt.completed_at_unix_nanos == 0
        || !receipt.repair_queue_durable
        || !receipt.full_recovery_completed
        || !receipt.metadata_publish_durably_owned
        || !receipt.maintenance_durably_owned
        || !receipt.metadata_replica_progress_durably_owned
    {
        return Err(outbox_error(
            "mounted-mutation ambiguity requires durable full-recovery owners and a completed full pass",
        ));
    }
    Ok(())
}

fn now_unix_nanos() -> u64 {
    crate::metadata_records::dfs_now_unix_nanos()
}

fn durable_error(source: durable_file::DurableFileError) -> DfsError {
    outbox_error(format!(
        "mounted-mutation durable file {}: {}",
        source.path(),
        source.detail()
    ))
}

fn lock_error(path: &Path, source: io::Error) -> DfsError {
    outbox_error(format!(
        "mounted-mutation outbox I/O at {}: {source}",
        path.display()
    ))
}

fn outbox_error(message: impl Into<String>) -> DfsError {
    DfsError::Metadata {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn store(label: &str) -> (sinomo_testdir::TestDir, MountedSifsMutationOutboxStore) {
        let root = sinomo_testdir::TestDir::new(label);
        let data_dir = SinomoDataDir::new(root.path());
        let store = MountedSifsMutationOutboxStore::new(&data_dir).expect("open outbox");
        (root, store)
    }

    fn stage_host_mount_session_for_publication(
        store: &MountedSifsMutationOutboxStore,
        adapter: MountedSifsMutationAdapter,
        mount_identity: &str,
    ) -> (
        MountedSifsHostMountSessionId,
        PathBuf,
        PathBuf,
        HeldLeaseLock,
    ) {
        let session_id = Id::generate();
        let preparing_directory = store
            .root
            .join(PREPARING_SESSIONS_DIRECTORY)
            .join(session_id.as_str());
        let published_directory = store.session_directory(&session_id);
        create_private_directory(&preparing_directory).expect("preparing session directory");
        let staged_lease = HeldLeaseLock::acquire_new(&preparing_directory.join(LEASE_FILE))
            .expect("staging lease");
        let now = now_unix_nanos();
        store
            .write_session_marker_in_directory(
                &preparing_directory,
                &MountedSifsHostMountSessionMarker {
                    schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
                    session_id: session_id.clone(),
                    session_nonce: Id::generate(),
                    adapter,
                    mount_identity: mount_identity.to_string(),
                    started_at_unix_nanos: now,
                    updated_at_unix_nanos: now,
                    phase: MountedSifsHostMountSessionPhase::Active,
                },
            )
            .expect("complete staged state");
        (
            session_id,
            preparing_directory,
            published_directory,
            staged_lease,
        )
    }

    fn arm(
        store: &MountedSifsMutationOutboxStore,
        mutation: &MountedSifsMutationLease<'_>,
    ) -> MountedSifsMutationArmRequestId {
        let arm_request_id = Id::generate();
        store
            .arm_live(
                &mutation.marker.marker_id,
                &mutation.marker.marker_nonce,
                &arm_request_id,
            )
            .expect("durably arm");
        arm_request_id
    }

    fn full_recovery_receipt() -> MountedSifsMutationRecoveryReceipt {
        MountedSifsMutationRecoveryReceipt {
            repair_queue_durable: true,
            metadata_publish_durably_owned: true,
            maintenance_durably_owned: true,
            metadata_replica_progress_durably_owned: true,
            full_recovery_completed: true,
            completed_at_unix_nanos: now_unix_nanos(),
        }
    }

    fn assert_all_work_is_retained(work: DfsCommittedWork) {
        assert!(work.publish_metadata);
        assert!(work.repair_maintenance);
        assert!(work.namespace_maintenance);
        assert!(work.metadata_replica_progress);
        assert!(work.metadata_materialization);
    }

    #[test]
    fn cursor_recovery_inspection_is_byte_read_only_for_default_and_nonzero_latches() {
        let (root, store) = store("mounted-mutation-cursor-recovery-read-only");
        let data_dir = SinomoDataDir::new(root.path());

        let default_before = snapshot_tree(store.root());
        let default_guard = inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir)
            .expect("inspect canonical default outbox");
        assert!(default_guard.inspection().outbox_present);
        assert_eq!(
            default_guard.inspection().committed_work_latch,
            Some(MountedSifsMutationCommittedWorkLatch::default())
        );
        assert_eq!(default_guard.inspection().raw_files.len(), 2);
        drop(default_guard);
        assert_eq!(snapshot_tree(store.root()), default_before);

        let retained = store
            .retain_daemon_committed_work(DfsCommittedWork {
                publish_metadata: true,
                ..DfsCommittedWork::default()
            })
            .expect("retain daemon work");
        assert!(retained.generation > 0);
        assert!(retained.updated_at_unix_nanos > 0);
        let retained_before = snapshot_tree(store.root());
        let retained_guard = inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir)
            .expect("inspect canonical nonzero latch");
        assert_eq!(
            retained_guard.inspection().committed_work_latch,
            Some(retained)
        );
        assert_eq!(
            retained_guard
                .inspection()
                .raw_files
                .get(DAEMON_COMMITTED_WORK_LATCH_FILE),
            retained_before
                .get(DAEMON_COMMITTED_WORK_LATCH_FILE)
                .and_then(Option::as_ref)
        );
        drop(retained_guard);
        assert_eq!(snapshot_tree(store.root()), retained_before);
    }

    #[test]
    fn retaining_already_owned_daemon_work_is_byte_read_only() {
        let (_root, store) = store("mounted-mutation-duplicate-daemon-work");
        let work = DfsCommittedWork {
            publish_metadata: true,
            namespace_maintenance: true,
            ..DfsCommittedWork::default()
        };
        let retained = store
            .retain_daemon_committed_work(work)
            .expect("retain daemon work");
        let before = snapshot_tree(store.root());

        let duplicate = store
            .retain_daemon_committed_work(work)
            .expect("retain duplicate daemon work");

        assert_eq!(duplicate, retained);
        assert_eq!(snapshot_tree(store.root()), before);
    }

    #[test]
    fn cursor_recovery_accepts_only_a_monotonic_committed_work_latch_successor() {
        let (root, store) = store("mounted-mutation-cursor-recovery-latch-successor");
        let data_dir = SinomoDataDir::new(root.path());
        let original = inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir)
            .expect("inspect original outbox");
        let original_raw = original.inspection().raw_files.clone();
        drop(original);

        let retained = store
            .retain_daemon_committed_work(DfsCommittedWork {
                publish_metadata: true,
                namespace_maintenance: true,
                ..DfsCommittedWork::default()
            })
            .expect("retain committed work successor");
        let successor = inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir)
            .expect("inspect committed work successor");
        assert!(
            successor
                .matches_original_or_committed_work_successor(&original_raw)
                .expect("classify monotonic successor")
        );
        drop(successor);

        let regressed = MountedSifsMutationCommittedWorkLatch {
            generation: retained.generation + 1,
            committed_work: DfsCommittedWork::default(),
            updated_at_unix_nanos: now_unix_nanos(),
            ..MountedSifsMutationCommittedWorkLatch::default()
        };
        store
            .write_daemon_committed_work_latch_unlocked(&regressed)
            .expect("write canonical regressed latch evidence");
        let rejected = inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir)
            .expect("strictly inspect regressed latch evidence");
        assert!(
            !rejected
                .matches_original_or_committed_work_successor(&BTreeMap::from([
                    (STORE_LOCK_FILE.to_string(), Vec::new()),
                    (
                        DAEMON_COMMITTED_WORK_LATCH_FILE.to_string(),
                        sinomo_object_serde::to_bytes(&retained, 1, LengthMode::U32)
                            .expect("encode retained predecessor"),
                    ),
                ]),)
                .expect("reject regressed successor")
        );
    }

    #[test]
    fn cursor_recovery_inspection_rejects_history_and_malformed_latch_without_mutation() {
        let (history_root, history_store) = store("mounted-mutation-cursor-recovery-history");
        let data_dir = SinomoDataDir::new(history_root.path());
        let retained_path = history_store
            .root
            .join(OPERATIONS_DIRECTORY)
            .join("retained-evidence");
        fs::write(&retained_path, b"operation evidence").expect("write retained evidence");
        let history_before = snapshot_tree(history_store.root());
        assert!(inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir).is_err());
        assert_eq!(snapshot_tree(history_store.root()), history_before);

        let (root, store) = store("mounted-mutation-cursor-recovery-invalid-latch");
        let data_dir = SinomoDataDir::new(root.path());
        let invalid = MountedSifsMutationCommittedWorkLatch {
            committed_work: DfsCommittedWork {
                publish_metadata: true,
                ..DfsCommittedWork::default()
            },
            ..MountedSifsMutationCommittedWorkLatch::default()
        };
        store
            .write_daemon_committed_work_latch_unlocked(&invalid)
            .expect("write canonical but invalid generation-zero latch");
        let invalid_before = snapshot_tree(store.root());
        assert!(inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir).is_err());
        assert_eq!(snapshot_tree(store.root()), invalid_before);
    }

    #[test]
    fn exclusive_store_and_state_locks_have_deterministic_bounded_waits() {
        let (_root, store) = store("mounted-mutation-bounded-exclusive-locks");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mutation = store
            .prepare(&session, Some("lock-timeout".into()), "create")
            .expect("prepare");
        let marker_id = mutation.marker.marker_id.clone();

        let store_lock_path = store.root.join(STORE_LOCK_FILE);
        let held_store_lock = lock_file_exclusive(&store_lock_path).expect("hold store lock");
        let started = Instant::now();
        assert!(
            lock_file_exclusive_with_timeout(&store_lock_path, Duration::ZERO).is_err(),
            "a contended store lock must time out instead of blocking"
        );
        assert!(started.elapsed() < Duration::from_millis(100));
        drop(held_store_lock);
        drop(
            lock_file_exclusive_with_timeout(&store_lock_path, Duration::ZERO)
                .expect("store lock succeeds after release"),
        );

        let state_lock_path = store.operation_directory(&marker_id).join(STATE_LOCK_FILE);
        let held_state_lock = lock_file_exclusive(&state_lock_path).expect("hold state lock");
        let before = store.marker(&marker_id).expect("marker before timeout");
        let started = Instant::now();
        assert!(
            lock_file_exclusive_with_timeout(&state_lock_path, Duration::ZERO).is_err(),
            "a contended state lock must time out instead of blocking"
        );
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(
            store.marker(&marker_id).expect("marker after timeout"),
            before,
            "timed-out lock acquisition cannot mutate marker state"
        );
        drop(held_state_lock);
        drop(
            lock_file_exclusive_with_timeout(&state_lock_path, Duration::ZERO)
                .expect("state lock succeeds after release"),
        );
    }

    #[test]
    fn external_fs2_lock_contention_is_live_and_bounded() {
        let root = sinomo_testdir::TestDir::new("mounted-mutation-external-lock-contention");
        let lock_path = root.join("external.lock");
        let held = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("create external lock");
        FileExt::try_lock_exclusive(&held).expect("hold external lock");

        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
            .expect("open external lock probe");
        let contention = FileExt::try_lock_exclusive(&probe).expect_err("lock is contended");
        assert!(
            fs2_lock_is_contended(&contention),
            "the platform's canonical fs2 contention error must be recognized"
        );
        assert_eq!(
            lock_liveness(&lock_path).expect("external lock liveness"),
            LeaseLiveness::Live
        );
        assert!(
            HeldLeaseLock::try_acquire_existing(&lock_path)
                .expect("try external lock")
                .is_none(),
            "a contended external lease must remain owned by its current holder"
        );
        let timeout = lock_file_exclusive_with_timeout(&lock_path, Duration::ZERO)
            .expect_err("contended lock must time out");
        assert!(
            timeout
                .to_string()
                .contains("timed out acquiring mounted-mutation outbox lock"),
            "contention must take the bounded retry path: {timeout}"
        );

        drop(probe);
        drop(held);
        assert_eq!(
            lock_liveness(&lock_path).expect("released external lock liveness"),
            LeaseLiveness::Dead
        );
        drop(
            HeldLeaseLock::try_acquire_existing(&lock_path)
                .expect("acquire released external lock")
                .expect("released external lease is available"),
        );
    }

    #[test]
    fn prepared_marker_is_fsynced_before_arm_and_arm_retry_is_idempotent() {
        let (_root, store) = store("mounted-mutation-arm");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mutation = store
            .prepare(&session, Some("create-1".into()), "create")
            .expect("prepare");
        assert!(matches!(
            mutation.marker.phase,
            MountedSifsMutationOutboxPhase::Prepared
        ));

        let arm_request_id = arm(&store, &mutation);
        let retried = store
            .arm_live(
                &mutation.marker.marker_id,
                &mutation.marker.marker_nonce,
                &arm_request_id,
            )
            .expect("retry same Arm");
        assert!(matches!(
            retried.phase,
            MountedSifsMutationOutboxPhase::Armed { arm_request_id: current }
                if current == arm_request_id
        ));
        assert!(
            store
                .arm_live(
                    &mutation.marker.marker_id,
                    &mutation.marker.marker_nonce,
                    &Id::generate(),
                )
                .is_err()
        );
        assert!(
            store
                .abort_unstarted_live(&mutation.marker.marker_id, &mutation.marker.marker_nonce)
                .is_err(),
            "lost Arm response cannot be erased by AbortUnstarted"
        );
    }

    #[test]
    fn error_capture_remains_full_recovery_gated() {
        let (_root, store) = store("mounted-mutation-unknown");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("session");
        let mut mutation = store
            .prepare(&session, Some("rename-1".into()), "rename")
            .expect("prepare");
        let arm_request_id = arm(&store, &mutation);
        let observation = MountedSifsMutationErrorObservation::new(
            MountedSifsMutationCapturedWork::default(),
            "possibly-applied",
        )
        .expect("observation");
        mutation
            .record_unknown_after_error(&arm_request_id, observation.clone())
            .expect("persist unknown");
        let accepted = store
            .accept_unknown_live(
                &mutation.marker.marker_id,
                &mutation.marker.marker_nonce,
                &arm_request_id,
                &observation,
            )
            .expect("accept unknown");
        assert!(matches!(
            accepted.phase,
            MountedSifsMutationOutboxPhase::DaemonAccepted {
                requires_full_recovery: true,
                ..
            }
        ));
    }

    #[test]
    fn orphan_full_recovery_waits_for_external_session_death() {
        let (_root, store) = store("mounted-mutation-session-proof");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let marker_id = {
            let mutation = store
                .prepare(&session, Some("unlink-1".into()), "unlink")
                .expect("prepare");
            mutation.marker.marker_id.clone()
        };
        assert!(
            store
                .try_claim_recovery(&marker_id)
                .expect("claim")
                .is_none(),
            "operation orphan alone cannot override a live external mount session"
        );
        drop(session);
        let claim = store
            .try_claim_recovery(&marker_id)
            .expect("claim after session death")
            .expect("full recovery claim");
        assert_eq!(claim.recovery_kind(), MountedSifsMutationRecoveryKind::Full);
    }

    #[test]
    fn shared_session_close_waits_for_the_inflight_operation_lease() {
        let (_root, store) = store("mounted-mutation-shared-session-close");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mutation = store
            .prepare(&session, Some("create-before-fatal".into()), "create")
            .expect("prepare");
        let marker_id = mutation.marker.marker_id.clone();
        arm(&store, &mutation);

        session
            .close_clean_shared()
            .expect("durably close shared session");
        assert!(
            store
                .try_claim_recovery(&marker_id)
                .expect("claim while operation is live")
                .is_none(),
            "closing the session must not overtake the in-flight operation lease"
        );

        drop(mutation);
        let claim = store
            .try_claim_recovery(&marker_id)
            .expect("claim after operation release")
            .expect("full recovery claim");
        assert_eq!(claim.recovery_kind(), MountedSifsMutationRecoveryKind::Full);
        drop(claim);
        assert!(
            store
                .prepare(&session, Some("late-create".into()), "create")
                .is_err(),
            "a retained closed session must never admit another operation"
        );
    }

    #[test]
    fn exact_capture_can_recover_without_waiting_for_session_exit() {
        let (_root, store) = store("mounted-mutation-exact-claim");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let marker_id = {
            let mut mutation = store
                .prepare(&session, Some("mkdir-1".into()), "mkdir")
                .expect("prepare");
            let arm_request_id = arm(&store, &mutation);
            mutation
                .record_captured_success(
                    &arm_request_id,
                    MountedSifsMutationCapturedWork::new(
                        DfsCommittedWork {
                            publish_metadata: true,
                            ..DfsCommittedWork::default()
                        },
                        Vec::new(),
                    )
                    .expect("captured work"),
                )
                .expect("capture");
            mutation.marker.marker_id.clone()
        };
        let claim = store
            .try_claim_recovery(&marker_id)
            .expect("claim")
            .expect("exact recovery claim");
        assert_eq!(
            claim.recovery_kind(),
            MountedSifsMutationRecoveryKind::Exact
        );
        assert!(
            claim
                .terminalize(MountedSifsMutationRecoveryReceipt {
                    completed_at_unix_nanos: now_unix_nanos(),
                    ..MountedSifsMutationRecoveryReceipt::default()
                })
                .is_err(),
            "a wake alone is not durable metadata-publication ownership"
        );
        store
            .try_claim_recovery(&marker_id)
            .expect("retry exact claim")
            .expect("retry exact recovery claim")
            .terminalize(MountedSifsMutationRecoveryReceipt {
                metadata_publish_durably_owned: true,
                completed_at_unix_nanos: now_unix_nanos(),
                ..MountedSifsMutationRecoveryReceipt::default()
            })
            .expect("exact receipt with durable owner");
        assert!(
            store
                .daemon_committed_work_latch()
                .expect("recovery latch")
                .committed_work
                .publish_metadata
        );
    }

    #[test]
    fn daemon_accept_latches_work_before_retryable_terminal_ack() {
        let (_root, store) = store("mounted-mutation-terminal-receipt");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("session");
        let mut mutation = store
            .prepare(&session, Some("write-1".into()), "write")
            .expect("prepare");
        let marker_id = mutation.marker.marker_id.clone();
        let marker_nonce = mutation.marker.marker_nonce.clone();
        let arm_request_id = arm(&store, &mutation);
        let captured = MountedSifsMutationCapturedWork::new(
            DfsCommittedWork {
                publish_metadata: true,
                namespace_maintenance: true,
                ..DfsCommittedWork::default()
            },
            Vec::new(),
        )
        .expect("captured");
        mutation
            .record_captured_success(&arm_request_id, captured.clone())
            .expect("capture");
        store
            .accept_captured_live(&marker_id, &marker_nonce, &arm_request_id, &captured)
            .expect("daemon accept");
        let latched = store
            .daemon_committed_work_latch()
            .expect("durable work latch");
        assert_eq!(latched.generation, 1);
        assert!(latched.committed_work.publish_metadata);
        assert!(latched.committed_work.namespace_maintenance);

        store
            .accept_captured_live(&marker_id, &marker_nonce, &arm_request_id, &captured)
            .expect("lost-response retry");
        assert_eq!(
            store
                .daemon_committed_work_latch()
                .expect("retry latch")
                .generation,
            latched.generation,
            "an exact response retry must not create a second ownership generation"
        );
        let live = store.status().expect("live accepted status");
        assert_eq!(live.pending_marker_count, 0);
        assert_eq!(live.terminal_marker_count, 1);
        assert!(store.operation_directory(&marker_id).is_dir());

        mutation
            .release_after_daemon_ack()
            .expect("release after durable ACK");
        assert_eq!(store.status().expect("pruned status").total_marker_count, 0);
        assert!(!store.operation_directory(&marker_id).exists());
    }

    #[test]
    fn separate_store_arm_is_observed_atomically_by_adapter_capture() {
        let (root, store) = store("mounted-mutation-separate-store-arm");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mut mutation = store
            .prepare(&session, Some("create-separate".into()), "create")
            .expect("prepare");
        let daemon_store = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("separate daemon store");
        let arm_request_id = arm(&daemon_store, &mutation);
        mutation
            .record_captured_success(&arm_request_id, MountedSifsMutationCapturedWork::default())
            .expect("adapter rereads daemon Arm before capture");
        assert!(matches!(
            daemon_store
                .marker(&mutation.marker.marker_id)
                .expect("marker")
                .expect("present")
                .phase,
            MountedSifsMutationOutboxPhase::Captured { .. }
        ));
    }

    #[test]
    fn exact_unknown_retry_retains_and_compares_error_evidence() {
        let (_root, store) = store("mounted-mutation-exact-unknown-evidence");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("session");
        let mut mutation = store
            .prepare(&session, Some("rename-evidence".into()), "rename")
            .expect("prepare");
        let arm_request_id = arm(&store, &mutation);
        let observation = MountedSifsMutationErrorObservation::new(
            MountedSifsMutationCapturedWork::default(),
            "possibly-applied",
        )
        .expect("observation");
        mutation
            .record_unknown_after_error(&arm_request_id, observation.clone())
            .expect("persist unknown");
        store
            .accept_unknown_live(
                &mutation.marker.marker_id,
                &mutation.marker.marker_nonce,
                &arm_request_id,
                &observation,
            )
            .expect("first accept");
        let different = MountedSifsMutationErrorObservation::new(
            MountedSifsMutationCapturedWork::default(),
            "different-error",
        )
        .expect("different observation");
        assert!(
            store
                .accept_unknown_live(
                    &mutation.marker.marker_id,
                    &mutation.marker.marker_nonce,
                    &arm_request_id,
                    &different,
                )
                .is_err(),
            "a response retry cannot substitute different error evidence"
        );
    }

    #[test]
    fn live_terminal_is_retained_until_ack_releases_operation_lease() {
        let (_root, store) = store("mounted-mutation-live-terminal");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mut mutation = store
            .prepare(&session, Some("noop-1".into()), "setattr")
            .expect("prepare");
        let marker_id = mutation.marker.marker_id.clone();
        let marker_nonce = mutation.marker.marker_nonce.clone();
        let arm_request_id = arm(&store, &mutation);
        mutation
            .record_proven_no_write(&arm_request_id, "validated-before-dispatch")
            .expect("persist proof");
        store
            .prove_no_write_live(
                &marker_id,
                &marker_nonce,
                &arm_request_id,
                "validated-before-dispatch",
            )
            .expect("daemon attest proof");
        let live = store.status().expect("live status");
        assert_eq!(live.terminal_marker_count, 1);
        assert!(store.operation_directory(&marker_id).is_dir());

        mutation
            .release_after_daemon_ack()
            .expect("release after ACK");
        let drained = store.status().expect("drained status");
        assert_eq!(drained.total_marker_count, 0);
        assert!(!store.operation_directory(&marker_id).exists());
    }

    #[test]
    fn pending_recovery_and_live_session_gate_new_mount_sessions() {
        let (root, store) = store("mounted-mutation-session-gates");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("first session");
        let second_store = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("second store");
        assert!(
            second_store
                .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
                .is_err(),
            "single-session ownership is enforced inside the outbox"
        );
        let mutation = store
            .prepare(&session, Some("armed-before-restart".into()), "unlink")
            .expect("prepare");
        let marker_id = mutation.marker.marker_id.clone();
        arm(&store, &mutation);
        drop(mutation);
        drop(session);
        assert!(
            second_store
                .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/restarted")
                .is_err(),
            "orphan ambiguity must drain before restart"
        );
        second_store
            .try_claim_recovery(&marker_id)
            .expect("claim")
            .expect("full claim")
            .terminalize(full_recovery_receipt())
            .expect("full recovery");
        second_store.status().expect("retire recovered marker");
        second_store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/restarted")
            .expect("session opens after recovery");
    }

    #[test]
    fn closed_session_marker_does_not_prove_death_until_lease_release() {
        let (root, store) = store("mounted-mutation-closed-session-race");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mut closed = session.marker().clone();
        closed.phase = MountedSifsHostMountSessionPhase::Closed;
        closed.updated_at_unix_nanos = now_unix_nanos().max(closed.started_at_unix_nanos);
        store
            .write_session_marker(&closed)
            .expect("persist close intent while lease is live");

        let origin = session.origin(None, "closed-race").expect("origin");
        assert_eq!(
            store.session_liveness(&origin).expect("liveness"),
            LeaseLiveness::Live,
            "a Closed state is not an OS/process lease-death proof"
        );
        assert!(
            !store
                .all_host_mount_sessions_proven_dead()
                .expect("global liveness")
        );
        let second_store = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("second store");
        assert!(
            second_store
                .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
                .is_err(),
            "session admission cannot pass between Closed persistence and lease Drop"
        );

        drop(session);
        assert!(
            store
                .all_host_mount_sessions_proven_dead()
                .expect("released liveness")
        );
        store.status().expect("prune released session");
        store
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("new session after actual lease death");
    }

    #[test]
    fn host_session_validation_fails_closed_for_identity_and_timestamp_mismatch() {
        let (_root, store) = store("mounted-mutation-invalid-session-state");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let origin = session.origin(None, "validate-session").expect("origin");
        let mut wrong_adapter_origin = origin.clone();
        wrong_adapter_origin.adapter = MountedSifsMutationAdapter::ExternalProvider;
        assert_eq!(
            store
                .session_liveness(&wrong_adapter_origin)
                .expect("mismatched adapter"),
            LeaseLiveness::Unproven
        );

        let mut invalid = session.marker().clone();
        invalid.updated_at_unix_nanos = invalid.started_at_unix_nanos.saturating_sub(1);
        write_bounded_sobj(&store.session_state_path(&invalid.session_id), &invalid)
            .expect("inject invalid timestamp state");
        assert_eq!(
            store.session_liveness(&origin).expect("invalid liveness"),
            LeaseLiveness::Unproven
        );
        assert!(
            !store
                .all_host_mount_sessions_proven_dead()
                .expect("invalid global liveness")
        );
        drop(session);
        assert!(
            store
                .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/reopened")
                .is_err(),
            "malformed authoritative session state must remain fail closed"
        );
    }

    #[test]
    fn session_publication_recovers_pre_rename_cut_as_private_staging() {
        let (root, store) = store("mounted-mutation-session-pre-rename-cut");
        let store_lock =
            lock_file_exclusive(&store.root.join(STORE_LOCK_FILE)).expect("store lock");
        let (_session_id, preparing_directory, published_directory, staged_lease) =
            stage_host_mount_session_for_publication(
                &store,
                MountedSifsMutationAdapter::ExternalProvider,
                "sync-root",
            );
        let preparing_lease_path = preparing_directory.join(LEASE_FILE);

        let error = publish_host_mount_session_directory_with_cuts(
            &preparing_directory,
            &published_directory,
            staged_lease,
            |lease_path| {
                assert_eq!(lease_path, preparing_lease_path);
                assert!(!process_lock_is_held(lease_path)?);
                Err(outbox_error("injected crash before session rename"))
            },
            |_| panic!("pre-rename cut must not reach final lease reacquire"),
        )
        .expect_err("injected pre-rename cut");
        assert!(
            error
                .to_string()
                .contains("injected crash before session rename")
        );
        assert!(preparing_directory.is_dir());
        assert!(!published_directory.exists());
        assert_eq!(
            lock_liveness(&preparing_lease_path).expect("dead staging lease"),
            LeaseLiveness::Dead
        );
        drop(store_lock);

        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("reopen after pre-rename cut");
        let status = reopened.status().expect("scan pre-rename cut");
        assert_eq!(status.total_marker_count, 0);
        assert_eq!(status.pending_marker_count, 0);
        assert!(!status.requires_full_recovery());
        assert!(
            reopened
                .recovery_candidate_marker_ids()
                .expect("recovery candidates")
                .is_empty(),
            "private staging cannot fabricate recovery work"
        );
        assert!(!preparing_directory.exists());
        assert!(!published_directory.exists());

        reopened
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("replacement session after dead private staging");
    }

    #[test]
    fn windows_compatible_session_publication_recovers_post_rename_cut() {
        let (root, store) = store("mounted-mutation-windows-session-publication");
        let store_lock =
            lock_file_exclusive(&store.root.join(STORE_LOCK_FILE)).expect("store lock");
        let (_session_id, preparing_directory, published_directory, staged_lease) =
            stage_host_mount_session_for_publication(
                &store,
                MountedSifsMutationAdapter::ExternalProvider,
                "sync-root",
            );
        let preparing_lease_path = preparing_directory.join(LEASE_FILE);

        let error = publish_host_mount_session_directory_with_cuts(
            &preparing_directory,
            &published_directory,
            staged_lease,
            |lease_path| {
                assert_eq!(lease_path, preparing_lease_path);
                assert!(
                    !process_lock_is_held(lease_path)?,
                    "the staging lease and its Windows child handle must be released before rename"
                );
                Ok(())
            },
            |lease_path| {
                assert_eq!(lease_path, published_directory.join(LEASE_FILE));
                assert!(published_directory.is_dir());
                assert_eq!(lock_liveness(lease_path)?, LeaseLiveness::Dead);
                Err(outbox_error(
                    "injected crash after session rename and before lease reacquire",
                ))
            },
        )
        .expect_err("injected post-rename cut");
        assert!(
            error
                .to_string()
                .contains("injected crash after session rename")
        );
        assert!(!preparing_directory.exists());
        assert!(published_directory.is_dir());
        assert_eq!(
            lock_liveness(&published_directory.join(LEASE_FILE)).expect("dead published lease"),
            LeaseLiveness::Dead
        );
        drop(store_lock);

        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("reopen after post-rename cut");
        let status = reopened.status().expect("scan post-rename cut");
        assert_eq!(status.total_marker_count, 0);
        assert_eq!(status.pending_marker_count, 0);
        assert!(!status.requires_full_recovery());
        assert!(
            reopened
                .recovery_candidate_marker_ids()
                .expect("recovery candidates")
                .is_empty(),
            "an unpublished adapter session cannot fabricate recovery work"
        );
        assert!(!published_directory.exists());

        let replacement = reopened
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("replacement session after dead publication cut");
        let replacement_directory = reopened.session_directory(&replacement.marker().session_id);
        assert_eq!(
            lock_liveness(&replacement_directory.join(LEASE_FILE))
                .expect("replacement session liveness"),
            LeaseLiveness::Live
        );
        assert!(
            process_lock_is_held(&replacement_directory.join(LEASE_FILE))
                .expect("replacement process lease")
        );
        assert!(
            !reopened
                .root
                .join(PREPARING_SESSIONS_DIRECTORY)
                .join(replacement.marker().session_id.as_str())
                .exists(),
            "the complete session directory must be authoritative before its lease is exposed"
        );
    }

    #[test]
    fn session_publication_does_not_recreate_a_missing_published_lease() {
        let (root, store) = store("mounted-mutation-session-missing-published-lease");
        let store_lock =
            lock_file_exclusive(&store.root.join(STORE_LOCK_FILE)).expect("store lock");
        let (_session_id, preparing_directory, published_directory, staged_lease) =
            stage_host_mount_session_for_publication(
                &store,
                MountedSifsMutationAdapter::ExternalProvider,
                "sync-root",
            );

        let error = publish_host_mount_session_directory_with_cuts(
            &preparing_directory,
            &published_directory,
            staged_lease,
            |lease_path| {
                assert!(!process_lock_is_held(lease_path)?);
                Ok(())
            },
            |lease_path| {
                remove_file_durable(lease_path)?;
                Ok(())
            },
        )
        .expect_err("a vanished published lease must fail closed");
        assert!(
            error
                .to_string()
                .contains("published session lease is unavailable")
        );
        assert!(published_directory.is_dir());
        assert!(!published_directory.join(LEASE_FILE).exists());
        drop(store_lock);

        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("reopen missing-lease session");
        assert!(
            reopened
                .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
                .is_err(),
            "missing authoritative liveness proof must block admission instead of being recreated"
        );
        assert!(!published_directory.join(LEASE_FILE).exists());
    }

    #[test]
    fn session_publication_staging_is_pruned_only_after_lease_death() {
        let (root, store) = store("mounted-mutation-session-publication-cut");
        let session_id = Id::generate();
        let preparing_directory = store
            .root
            .join(PREPARING_SESSIONS_DIRECTORY)
            .join(session_id.as_str());
        create_private_directory(&preparing_directory).expect("preparing session directory");
        let lease = HeldLeaseLock::acquire_new(&preparing_directory.join(LEASE_FILE))
            .expect("preparing lease");
        let now = now_unix_nanos();
        store
            .write_session_marker_in_directory(
                &preparing_directory,
                &MountedSifsHostMountSessionMarker {
                    schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
                    session_id: session_id.clone(),
                    session_nonce: Id::generate(),
                    adapter: MountedSifsMutationAdapter::Fuse,
                    mount_identity: "/mnt/staging".to_string(),
                    started_at_unix_nanos: now,
                    updated_at_unix_nanos: now,
                    phase: MountedSifsHostMountSessionPhase::Active,
                },
            )
            .expect("complete staged state");

        assert!(
            MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path())).is_err(),
            "a live staging publisher cannot be removed or bypassed"
        );
        assert!(!store.session_directory(&session_id).exists());
        drop(lease);

        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("prune dead staging session");
        assert!(!preparing_directory.exists());
        assert!(!reopened.session_directory(&session_id).exists());
        reopened
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/reopened")
            .expect("new session after staging cleanup");
    }

    #[test]
    fn session_publication_prunes_crash_before_lease_creation() {
        let (root, store) = store("mounted-mutation-session-prelease-cut");
        let session_id = Id::generate();
        let preparing_directory = store
            .root
            .join(PREPARING_SESSIONS_DIRECTORY)
            .join(session_id.as_str());
        create_private_directory(&preparing_directory).expect("preparing session directory");
        fs::write(preparing_directory.join(STATE_FILE), b"partial-state")
            .expect("partial staged state");

        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("prune pre-lease staging cut");
        assert!(!preparing_directory.exists());
        reopened
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("staging cut cannot poison future sessions");
    }

    #[test]
    fn deleted_state_after_arm_is_corrupt_full_recovery_not_unpublished() {
        let (root, store) = store("mounted-mutation-deleted-armed-state");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mutation = store
            .prepare(&session, Some("armed-delete".into()), "rename")
            .expect("prepare");
        let marker_id = mutation.marker.marker_id.clone();
        arm(&store, &mutation);
        remove_file_durable(&store.operation_state_path(&marker_id)).expect("delete state");
        drop(mutation);
        drop(session);

        let reopened =
            MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path())).expect("reopen");
        let status = reopened.status().expect("status");
        assert_eq!(status.malformed_marker_count, 1);
        assert!(status.requires_full_recovery());
        assert!(
            reopened
                .recovery_candidate_marker_ids()
                .expect("candidates")
                .contains(&marker_id)
        );
        reopened
            .try_claim_corrupt_recovery(&marker_id)
            .expect("claim")
            .expect("corrupt claim")
            .terminalize(full_recovery_receipt())
            .expect("terminalize corrupt recovery");
        assert_all_work_is_retained(
            reopened
                .daemon_committed_work_latch()
                .expect("corrupt recovery latch")
                .committed_work,
        );
        assert_eq!(reopened.status().expect("retired").total_marker_count, 0);
    }

    #[test]
    fn corrupt_committed_work_latch_self_heals_to_all_work() {
        let (root, store) = store("mounted-mutation-corrupt-latch");
        fs::write(store.daemon_committed_work_latch_path(), b"not-sobj").expect("corrupt latch");
        let healed = store
            .daemon_committed_work_latch()
            .expect("self-heal corrupt latch");
        assert_eq!(healed.generation, 1);
        assert!(healed.updated_at_unix_nanos > 0);
        assert_all_work_is_retained(healed.committed_work);

        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("reopen healed latch");
        assert_eq!(
            reopened
                .daemon_committed_work_latch()
                .expect("persisted healed latch"),
            healed
        );
    }

    #[test]
    fn creation_token_proves_prepared_state_was_never_published() {
        let (_root, store) = store("mounted-mutation-prestate-token");
        let marker_id = Id::generate();
        let marker_nonce = Id::generate();
        let directory = store.operation_directory(&marker_id);
        create_private_directory(&directory).expect("operation directory");
        write_bounded_sobj(
            &directory.join(CREATION_TOKEN_FILE),
            &MountedSifsMutationCreationToken {
                schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
                marker_id: marker_id.clone(),
                marker_nonce,
                created_at_unix_nanos: now_unix_nanos(),
            },
        )
        .expect("creation token");
        drop(HeldLeaseLock::acquire_new(&directory.join(LEASE_FILE)).expect("lease"));

        let status = store.status().expect("cleanup pre-state crash");
        assert_eq!(status.total_marker_count, 0);
        assert!(!directory.exists());
    }

    #[test]
    fn terminal_churn_does_not_consume_pending_marker_capacity_after_reopen() {
        let (root, store) = store("mounted-mutation-terminal-churn");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        for index in 0..(MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS + 8) {
            let mutation = store
                .prepare(&session, Some(format!("abort-{index}")), "create")
                .expect("prepare churn marker");
            store
                .abort_unstarted_live(&mutation.marker.marker_id, &mutation.marker.marker_nonce)
                .expect("abort before Arm");
            mutation
                .release_after_daemon_ack()
                .expect("release aborted marker");
        }
        drop(session);
        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("reopen after churn");
        let status = reopened.status().expect("status");
        assert_eq!(status.total_marker_count, 0);
        assert!(!status.marker_limit_exceeded);
        reopened
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/reopened")
            .expect("terminal history cannot mask new work");
    }

    #[test]
    fn successful_accept_churn_is_latched_and_does_not_consume_capacity() {
        let (root, store) = store("mounted-mutation-success-churn");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let operation_count = MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS + 8;
        for index in 0..operation_count {
            let mut mutation = store
                .prepare(&session, Some(format!("create-{index}")), "create")
                .expect("prepare successful mutation");
            let marker_id = mutation.marker.marker_id.clone();
            let marker_nonce = mutation.marker.marker_nonce.clone();
            let arm_request_id = arm(&store, &mutation);
            let captured = MountedSifsMutationCapturedWork::new(
                DfsCommittedWork {
                    publish_metadata: true,
                    ..DfsCommittedWork::default()
                },
                Vec::new(),
            )
            .expect("captured committed work");
            mutation
                .record_captured_success(&arm_request_id, captured.clone())
                .expect("capture successful mutation");
            store
                .accept_captured_live(&marker_id, &marker_nonce, &arm_request_id, &captured)
                .expect("durably accept successful mutation");
            mutation
                .release_after_daemon_ack()
                .expect("release successful mutation");
        }
        assert_eq!(
            store
                .status()
                .expect("prune accepted churn")
                .total_marker_count,
            0
        );
        assert_eq!(
            store
                .daemon_committed_work_latch()
                .expect("coalesced latch")
                .generation,
            1,
            "successful churn must coalesce one retained work bit into one ownership generation"
        );
        drop(session);

        let reopened = MountedSifsMutationOutboxStore::new(&SinomoDataDir::new(root.path()))
            .expect("reopen after successful churn");
        assert_eq!(
            reopened
                .status()
                .expect("reopened status")
                .total_marker_count,
            0
        );
        assert_eq!(
            reopened
                .daemon_committed_work_latch()
                .expect("reopened latch")
                .generation,
            1,
            "the coalesced ownership generation must survive reopen"
        );
        reopened
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/reopened")
            .expect("accepted terminal history cannot exhaust the outbox");
    }

    #[test]
    fn operation_and_session_scan_budget_never_fabricates_full_death_proof() {
        let (_root, store) = store("mounted-mutation-bounded-operation-session-scan");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let mutation = store
            .prepare(&session, Some("bounded-full-recovery".into()), "unlink")
            .expect("prepare");
        arm(&store, &mutation);
        drop(mutation);
        drop(session);

        // One operation entry plus one session entry consumes the exact test
        // budget. Since the session iterator cannot then be proven complete,
        // the result must be overflow, never AllDead.
        let mut budget =
            MountedSifsMutationDirectoryScanBudget::with_limits(2, Duration::from_secs(60));
        let scan = store
            .scan_unlocked_with_budget(&mut budget)
            .expect("bounded scan");
        assert!(budget.visited_entries <= 2);
        assert!(scan.status.directory_scan_limit_exceeded);
        assert!(scan.status.requires_full_recovery());
        assert_eq!(scan.status.full_recovery_marker_count, 0);
        assert_eq!(scan.status.exact_recovery_marker_count, 0);
        assert_eq!(scan.pending_marker_ids.len(), 1);
        assert!(scan.recovery_candidate_marker_ids.is_empty());

        let mut proof_budget =
            MountedSifsMutationDirectoryScanBudget::with_limits(1, Duration::from_secs(60));
        let mut proof_phase_remaining = MOUNTED_SIFS_MUTATION_OUTBOX_SESSION_SCAN_ENTRIES;
        assert_eq!(
            store
                .all_host_mount_sessions_proven_dead_with_budget(
                    &mut proof_budget,
                    &mut proof_phase_remaining,
                )
                .expect("bounded death proof"),
            HostMountSessionsDeathProof::ScanLimitExceeded
        );
        assert!(proof_budget.visited_entries <= 1);
    }

    #[test]
    fn preparing_session_parent_and_children_share_the_scan_budget() {
        let (_root, store) = store("mounted-mutation-bounded-preparing-session-scan");
        let session_id = Id::generate();
        let directory = store
            .root
            .join(PREPARING_SESSIONS_DIRECTORY)
            .join(session_id.as_str());
        create_private_directory(&directory).expect("preparing session directory");
        for name in [LEASE_FILE, STATE_FILE, STATE_LOCK_FILE] {
            fs::write(directory.join(name), []).expect("preparing child");
        }

        let mut budget =
            MountedSifsMutationDirectoryScanBudget::with_limits(2, Duration::from_secs(60));
        let scan = store
            .scan_unlocked_with_budget(&mut budget)
            .expect("bounded preparing scan");
        assert_eq!(budget.visited_entries, 2);
        assert!(scan.status.directory_scan_limit_exceeded);
        assert!(
            directory.is_dir(),
            "partial validation cannot prune staging state"
        );
        assert!(scan.recovery_candidate_marker_ids.is_empty());
    }

    #[test]
    fn retired_nested_cleanup_is_bounded_and_makes_progress_across_scans() {
        let (_root, store) = store("mounted-mutation-bounded-retired-scan");
        let marker_id = Id::generate();
        let directory = store
            .root
            .join(RETIRED_OPERATIONS_DIRECTORY)
            .join(marker_id.as_str());
        create_private_directory(&directory).expect("retired operation directory");
        fs::write(directory.join("first"), b"one").expect("first retired child");
        fs::write(directory.join("second"), b"two").expect("second retired child");

        let mut previous_children = 2;
        let mut completed = false;
        for _ in 0..4 {
            let mut budget =
                MountedSifsMutationDirectoryScanBudget::with_limits(3, Duration::from_secs(60));
            let scan = store
                .scan_unlocked_with_budget(&mut budget)
                .expect("bounded retired scan");
            assert!(budget.visited_entries <= 3);
            let remaining_children = if directory.is_dir() {
                fs::read_dir(&directory)
                    .expect("remaining retired children")
                    .count()
            } else {
                0
            };
            assert!(remaining_children <= previous_children);
            if scan.status.directory_scan_limit_exceeded {
                assert!(scan.recovery_candidate_marker_ids.is_empty());
            } else {
                completed = true;
                break;
            }
            previous_children = remaining_children;
        }
        assert!(
            completed,
            "repeated bounded scans must finish retired cleanup"
        );
        assert!(!directory.exists());
    }

    #[test]
    fn unreferenced_session_pruning_uses_the_same_scan_budget() {
        let (_root, store) = store("mounted-mutation-bounded-session-prune");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("session");
        let session_id = session.marker().session_id.clone();
        drop(session);

        let mut budget =
            MountedSifsMutationDirectoryScanBudget::with_limits(2, Duration::from_secs(60));
        let scan = store
            .scan_unlocked_with_budget(&mut budget)
            .expect("bounded session prune");
        assert_eq!(budget.visited_entries, 2);
        assert!(scan.status.directory_scan_limit_exceeded);
        assert!(!store.session_directory(&session_id).exists());
        assert!(
            store
                .root
                .join(RETIRED_SESSIONS_DIRECTORY)
                .join(session_id.as_str())
                .is_dir(),
            "the completed entry may retire safely before the scan stops"
        );
        assert!(scan.recovery_candidate_marker_ids.is_empty());
    }

    #[test]
    fn expired_monotonic_scan_deadline_fails_closed_without_visiting_entries() {
        let (_root, store) = store("mounted-mutation-expired-scan-deadline");
        let mut budget = MountedSifsMutationDirectoryScanBudget::with_limits(16, Duration::ZERO);
        let scan = store
            .scan_unlocked_with_budget(&mut budget)
            .expect("expired scan");
        assert_eq!(budget.visited_entries, 0);
        assert!(scan.status.directory_scan_limit_exceeded);
        assert!(scan.status.requires_full_recovery());
        assert!(scan.pending_marker_ids.is_empty());
        assert!(scan.recovery_candidate_marker_ids.is_empty());
    }

    #[test]
    fn non_directory_operation_entry_is_malformed_and_blocks_mount_admission() {
        let (_root, store) = store("mounted-mutation-nondirectory-operation");
        let marker_id = Id::generate();
        fs::write(
            store
                .root
                .join(OPERATIONS_DIRECTORY)
                .join(marker_id.as_str()),
            b"not-an-operation-directory",
        )
        .expect("inject non-directory operation");

        let scan = store.scan_unlocked().expect("fail-closed scan");
        assert_eq!(scan.status.total_marker_count, 1);
        assert_eq!(scan.status.pending_marker_count, 1);
        assert_eq!(scan.status.malformed_marker_count, 1);
        assert!(scan.status.requires_full_recovery());
        assert!(scan.pending_marker_ids.is_empty());
        assert!(scan.recovery_candidate_marker_ids.is_empty());
        assert!(
            store
                .try_claim_corrupt_recovery(&marker_id)
                .expect("non-directory claim probe")
                .is_none()
        );
        assert!(
            store
                .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/must-not-open")
                .is_err(),
            "malformed authoritative operation entries must keep Probe/admission non-ready"
        );
    }

    #[cfg(unix)]
    #[test]
    fn operation_symlink_is_never_traversed_or_exposed_as_a_recovery_candidate() {
        use std::os::unix::fs::symlink;

        let (root, store) = store("mounted-mutation-operation-symlink");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session before corruption");
        let external = root.path().join("external-operation-target");
        fs::create_dir(&external).expect("external target");
        fs::write(external.join(LEASE_FILE), b"external-lease").expect("external lease");
        fs::write(external.join(STATE_FILE), b"external-state").expect("external state");
        fs::write(external.join("sentinel"), b"must-remain-byte-identical")
            .expect("external sentinel");
        let marker_id = Id::generate();
        symlink(
            &external,
            store
                .root
                .join(OPERATIONS_DIRECTORY)
                .join(marker_id.as_str()),
        )
        .expect("operation symlink");

        let scan = store.scan_unlocked().expect("fail-closed symlink scan");
        assert_eq!(scan.status.pending_marker_count, 1);
        assert_eq!(scan.status.malformed_marker_count, 1);
        assert!(scan.status.requires_full_recovery());
        assert!(scan.pending_marker_ids.is_empty());
        assert!(scan.recovery_candidate_marker_ids.is_empty());
        assert!(
            store
                .prepare(&session, Some("must-not-admit".into()), "write")
                .is_err(),
            "a malformed authoritative entry must fence existing mount mutation admission"
        );
        drop(session);
        assert!(
            store
                .try_claim_corrupt_recovery(&marker_id)
                .expect("symlink corrupt claim probe")
                .is_none(),
            "direct claim APIs must reject a symlink before following lease/state paths"
        );
        assert!(
            store
                .try_claim_recovery(&marker_id)
                .expect("symlink exact claim probe")
                .is_none()
        );
        assert!(
            store
                .open_host_mount_session(
                    MountedSifsMutationAdapter::ExternalProvider,
                    "must-not-open"
                )
                .is_err(),
            "Probe/new-session admission must remain fail closed"
        );
        assert_eq!(
            fs::read(external.join("sentinel")).expect("external sentinel after scan"),
            b"must-remain-byte-identical"
        );
        assert_eq!(
            fs::read(external.join(STATE_FILE)).expect("external state after scan"),
            b"external-state"
        );
        assert!(!external.join(CLEANUP_AUTHORIZATION_FILE).exists());
    }

    #[test]
    fn incomplete_operation_scan_cannot_admit_marker_beyond_capacity() {
        let (_root, store) = store("mounted-mutation-incomplete-capacity-admission");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let origin = session.origin(None, "injected-capacity").expect("origin");
        for _ in 0..MOUNTED_SIFS_MUTATION_OUTBOX_OPERATION_SCAN_ENTRIES {
            let marker_id = Id::generate();
            let directory = store.operation_directory(&marker_id);
            create_private_directory(&directory).expect("terminal operation directory");
            drop(
                HeldLeaseLock::acquire_new(&directory.join(LEASE_FILE))
                    .expect("dead terminal lease"),
            );
            let now = now_unix_nanos().max(1);
            store
                .write_operation_marker(&MountedSifsMutationOutboxMarker {
                    schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
                    marker_id,
                    marker_nonce: Id::generate(),
                    origin: origin.clone(),
                    prepared_at_unix_nanos: now,
                    updated_at_unix_nanos: now,
                    phase: MountedSifsMutationOutboxPhase::Terminal {
                        recovery_receipt: full_recovery_receipt(),
                    },
                })
                .expect("terminal marker");
        }
        for _ in 0..MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
            let marker_id = Id::generate();
            let directory = store.operation_directory(&marker_id);
            create_private_directory(&directory).expect("pending operation directory");
            let now = now_unix_nanos().max(1);
            store
                .write_operation_marker(&MountedSifsMutationOutboxMarker {
                    schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
                    marker_id,
                    marker_nonce: Id::generate(),
                    origin: origin.clone(),
                    prepared_at_unix_nanos: now,
                    updated_at_unix_nanos: now,
                    phase: MountedSifsMutationOutboxPhase::Prepared,
                })
                .expect("pending marker");
        }

        let operations = store.root.join(OPERATIONS_DIRECTORY);
        let before = fs::read_dir(&operations)
            .expect("operations before admission")
            .count();
        assert_eq!(
            before,
            MOUNTED_SIFS_MUTATION_OUTBOX_OPERATION_SCAN_ENTRIES
                + MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS
        );
        assert!(
            store
                .prepare(&session, Some("must-not-be-marker-65".into()), "write")
                .is_err(),
            "an incomplete operation classification must reject capacity admission"
        );
        let after = fs::read_dir(&operations)
            .expect("operations after rejected admission")
            .count();
        assert!(
            after <= before,
            "rejected admission may prune terminal history but cannot grow active operations"
        );
    }

    #[test]
    fn partial_operation_scan_preserves_independently_safe_exact_candidate() {
        let (_root, store) = store("mounted-mutation-partial-exact-candidate");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        for index in 0..2 {
            let mut mutation = store
                .prepare(&session, Some(format!("captured-{index}")), "create")
                .expect("prepare");
            let arm_request_id = arm(&store, &mutation);
            mutation
                .record_captured_success(
                    &arm_request_id,
                    MountedSifsMutationCapturedWork::default(),
                )
                .expect("capture exact work");
        }

        let mut budget =
            MountedSifsMutationDirectoryScanBudget::with_limits(2, Duration::from_secs(60));
        let scan = store
            .scan_unlocked_with_budget(&mut budget)
            .expect("partial exact scan");
        assert_eq!(budget.visited_entries, 2);
        assert!(scan.status.directory_scan_limit_exceeded);
        assert_eq!(scan.status.exact_recovery_marker_count, 1);
        assert_eq!(scan.recovery_candidate_marker_ids.len(), 1);
        assert_eq!(scan.pending_marker_ids.len(), 1);
    }

    #[test]
    fn optional_retired_cleanup_cannot_suppress_proven_full_candidate() {
        let (_root, store) = store("mounted-mutation-retired-cleanup-after-full-proof");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::ExternalProvider, "sync-root")
            .expect("session");
        let mut exact_mutation = store
            .prepare(&session, Some("exact-before-cleanup".into()), "create")
            .expect("prepare exact");
        let exact_arm_request_id = arm(&store, &exact_mutation);
        exact_mutation
            .record_captured_success(
                &exact_arm_request_id,
                MountedSifsMutationCapturedWork::default(),
            )
            .expect("capture exact work");
        drop(exact_mutation);
        let full_mutation = store
            .prepare(&session, Some("full-before-cleanup".into()), "rename")
            .expect("prepare full");
        arm(&store, &full_mutation);
        drop(full_mutation);
        drop(session);

        let retired = store
            .root
            .join(RETIRED_OPERATIONS_DIRECTORY)
            .join(Id::generate().as_str());
        create_private_directory(&retired).expect("retired directory");
        for index in 0..MOUNTED_SIFS_MUTATION_OUTBOX_CLEANUP_SCAN_ENTRIES {
            fs::write(retired.join(format!("child-{index}")), b"retired").expect("retired child");
        }

        let mut budget = MountedSifsMutationDirectoryScanBudget::production();
        let scan = store
            .scan_unlocked_with_budget(&mut budget)
            .expect("full proof before cleanup overflow");
        assert!(scan.status.directory_scan_limit_exceeded);
        assert_eq!(scan.status.exact_recovery_marker_count, 1);
        assert_eq!(scan.status.full_recovery_marker_count, 1);
        assert_eq!(scan.recovery_candidate_marker_ids.len(), 2);
        assert!(budget.visited_entries <= MOUNTED_SIFS_MUTATION_OUTBOX_MAX_DIRECTORY_SCAN_ENTRIES);
    }

    #[test]
    fn oversized_corrupt_operation_tree_drains_in_repeated_bounded_passes() {
        let (_root, store) = store("mounted-mutation-oversized-corrupt-drain");
        let operation_count = MOUNTED_SIFS_MUTATION_OUTBOX_OPERATION_SCAN_ENTRIES + 8;
        for _ in 0..operation_count {
            let marker_id = Id::generate();
            let directory = store.operation_directory(&marker_id);
            create_private_directory(&directory).expect("corrupt operation directory");
            drop(
                HeldLeaseLock::acquire_new(&directory.join(LEASE_FILE))
                    .expect("dead operation lease file"),
            );
            fs::write(directory.join(STATE_FILE), b"not-a-sobj-marker")
                .expect("corrupt marker state");
        }

        let operations = store.root.join(OPERATIONS_DIRECTORY);
        let retired = store.root.join(RETIRED_OPERATIONS_DIRECTORY);
        let entry_count = |root: &Path| fs::read_dir(root).expect("directory count").count();
        let mut previous_active = operation_count;
        let mut previous_retired = 0;
        let mut completed = false;
        for _ in 0..16 {
            let mut budget = MountedSifsMutationDirectoryScanBudget::production();
            let scan = store
                .scan_unlocked_with_budget(&mut budget)
                .expect("bounded corrupt scan");
            assert!(
                budget.visited_entries <= MOUNTED_SIFS_MUTATION_OUTBOX_MAX_DIRECTORY_SCAN_ENTRIES
            );
            if previous_active > 0 {
                assert!(
                    !scan.recovery_candidate_marker_ids.is_empty(),
                    "a complete empty-session proof must expose a bounded corrupt prefix"
                );
                assert_eq!(
                    scan.status.full_recovery_marker_count,
                    scan.recovery_candidate_marker_ids.len()
                );
            }
            for marker_id in scan.recovery_candidate_marker_ids {
                store
                    .try_claim_corrupt_recovery(&marker_id)
                    .expect("claim corrupt prefix")
                    .expect("recoverable corrupt marker")
                    .terminalize(full_recovery_receipt())
                    .expect("retire corrupt prefix");
            }

            let active = entry_count(&operations);
            let retired_count = entry_count(&retired);
            if previous_active > 0 {
                assert!(
                    active < previous_active,
                    "each recovery pass must shrink active ambiguity"
                );
            } else if retired_count > 0 {
                assert!(
                    retired_count < previous_retired,
                    "cleanup-only passes must shrink retired history"
                );
            }
            if active == 0 && retired_count == 0 {
                completed = true;
                break;
            }
            previous_active = active;
            previous_retired = retired_count;
        }
        assert!(
            completed,
            "bounded repeated passes must fully drain the wide tree"
        );
    }

    #[test]
    fn normal_maximum_marker_set_remains_complete_and_non_overflowing() {
        let (_root, store) = store("mounted-mutation-normal-max-marker-scan");
        let session = store
            .open_host_mount_session(MountedSifsMutationAdapter::Fuse, "/mnt/sinomo")
            .expect("session");
        let origin = session.origin(None, "bounded-normal").expect("origin");
        for _ in 0..MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS {
            let marker_id = Id::generate();
            let directory = store.operation_directory(&marker_id);
            create_private_directory(&directory).expect("operation directory");
            let now = now_unix_nanos().max(1);
            store
                .write_operation_marker(&MountedSifsMutationOutboxMarker {
                    schema_version: MOUNTED_SIFS_MUTATION_OUTBOX_SCHEMA_VERSION,
                    marker_id,
                    marker_nonce: Id::generate(),
                    origin: origin.clone(),
                    prepared_at_unix_nanos: now,
                    updated_at_unix_nanos: now,
                    phase: MountedSifsMutationOutboxPhase::Prepared,
                })
                .expect("marker state");
        }

        let mut budget = MountedSifsMutationDirectoryScanBudget::production();
        let scan = store
            .scan_unlocked_with_budget(&mut budget)
            .expect("normal bounded scan");
        assert!(!scan.status.directory_scan_limit_exceeded);
        assert!(!scan.status.marker_limit_exceeded);
        assert_eq!(
            scan.status.pending_marker_count,
            MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS
        );
        assert_eq!(
            scan.pending_marker_ids.len(),
            MOUNTED_SIFS_MUTATION_OUTBOX_MAX_PENDING_MARKERS
        );
        assert!(budget.visited_entries <= MOUNTED_SIFS_MUTATION_OUTBOX_MAX_DIRECTORY_SCAN_ENTRIES);
    }
}
