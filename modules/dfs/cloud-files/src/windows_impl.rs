#[allow(unused_imports)]
use sinomo_runtime::cluster::ClusterKvPathsExt;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{OsStr, c_void};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::fs::MetadataExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use crate::protocol::{
    CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION, CLOUD_FILES_CURSOR_ONLY_RECOVERY_TOKEN_DOMAIN,
    CLOUD_FILES_CURSOR_RECOVERY_MARKER_MAX_BYTES, CloudFilesCursorOnlyRecoveryApplied,
    CloudFilesCursorOnlyRecoveryApplyRequest, CloudFilesCursorOnlyRecoveryIdentity,
    CloudFilesCursorOnlyRecoveryInspectRequest, CloudFilesCursorOnlyRecoveryInspection,
    CloudFilesCursorRecoveryContext, CloudFilesFetchDataError, CloudFilesLocalChangeSyncMode,
    CloudFilesLocalChangeSyncStatus, CloudFilesModuleError, CloudFilesModuleEvent,
    CloudFilesMountLifecycleEvent, CloudFilesMountRequest, CloudFilesMountResponse,
    CloudFilesPlaceholderIdentity, CloudFilesPreserveStateUnregisterOutcome,
    CloudFilesPreserveStateUnregisterResponse, CloudFilesRegistrationReceipt,
    CloudFilesStaleIdentityReason, CloudFilesStalePlaceholderIdentity, CloudFilesUnregisterRequest,
    CloudFilesUnregisterResponse, SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION,
    SINOMO_DFS_CLOUD_FILES_MODULE_ID,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sinomo_module_api::{decode_sobj, encode_sobj};
use sinomo_runtime::dfs::DaemonDfsPeerTransport;
use sinomo_runtime::dfs::{
    self, DfsControlPlaneKvClient, DfsMessageTransport, DfsObjectKind, DfsObjectRpcSendOptions,
    DfsPeerTransport, DfsPosixError, DfsRoutedCall, DfsRoutedMessage, DfsTransportIdentity,
    LocalContentStore, MountedSifsControlPlaneKvClient, NetworkedDfsService,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, NetworkError};
use sinomo_runtime::node::NodeId;
use windows::Win32::Foundation::{
    CloseHandle, ERROR_CLOUD_FILE_NOT_UNDER_SYNC_ROOT, ERROR_INVALID_FUNCTION, HANDLE, NTSTATUS,
    STATUS_CANCELLED, STATUS_CLOUD_FILE_UNSUCCESSFUL, STATUS_FILE_IS_A_DIRECTORY,
    STATUS_NOT_A_DIRECTORY, STATUS_OBJECT_NAME_NOT_FOUND, STATUS_SUCCESS, STATUS_UNSUCCESSFUL,
};
use windows::Win32::Storage::CloudFilters::{
    CF_CALLBACK_INFO, CF_CALLBACK_PARAMETERS, CF_CALLBACK_REGISTRATION,
    CF_CALLBACK_TYPE_CANCEL_FETCH_DATA, CF_CALLBACK_TYPE_CANCEL_FETCH_PLACEHOLDERS,
    CF_CALLBACK_TYPE_FETCH_DATA, CF_CALLBACK_TYPE_FETCH_PLACEHOLDERS, CF_CALLBACK_TYPE_NONE,
    CF_CONNECT_FLAG_NONE, CF_CONNECTION_KEY, CF_CREATE_FLAG_NONE, CF_FS_METADATA,
    CF_HARDLINK_POLICY_NONE, CF_HYDRATION_POLICY,
    CF_HYDRATION_POLICY_MODIFIER_AUTO_DEHYDRATION_ALLOWED, CF_HYDRATION_POLICY_PROGRESSIVE,
    CF_IN_SYNC_STATE_IN_SYNC, CF_INSYNC_POLICY_TRACK_ALL, CF_OPERATION_INFO,
    CF_OPERATION_PARAMETERS, CF_OPERATION_PARAMETERS_0, CF_OPERATION_PARAMETERS_0_0,
    CF_OPERATION_PARAMETERS_0_4, CF_OPERATION_TRANSFER_DATA_FLAG_NONE,
    CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_DISABLE_ON_DEMAND_POPULATION,
    CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE, CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAGS,
    CF_OPERATION_TYPE_TRANSFER_DATA, CF_OPERATION_TYPE_TRANSFER_PLACEHOLDERS,
    CF_PLACEHOLDER_CREATE_FLAG_MARK_IN_SYNC, CF_PLACEHOLDER_CREATE_INFO,
    CF_PLACEHOLDER_INFO_STANDARD, CF_PLACEHOLDER_MANAGEMENT_POLICY_DEFAULT,
    CF_PLACEHOLDER_STANDARD_INFO, CF_POPULATION_POLICY, CF_POPULATION_POLICY_MODIFIER_NONE,
    CF_POPULATION_POLICY_PARTIAL, CF_REGISTER_FLAG_NONE, CF_REGISTER_FLAG_UPDATE, CF_SYNC_POLICIES,
    CF_SYNC_REGISTRATION, CF_SYNC_ROOT_INFO_STANDARD, CF_SYNC_ROOT_STANDARD_INFO,
    CfConnectSyncRoot, CfCreatePlaceholders, CfDisconnectSyncRoot, CfExecute, CfGetPlaceholderInfo,
    CfGetSyncRootInfoByPath, CfRegisterSyncRoot, CfUnregisterSyncRoot,
};
use windows::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL,
    FILE_ATTRIBUTE_READONLY, FILE_ATTRIBUTE_REPARSE_POINT, FILE_BASIC_INFO,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ,
    FILE_SHARE_WRITE, GetFileInformationByHandle, MOVEFILE_REPLACE_EXISTING,
    MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_EXISTING,
};
use windows::core::{GUID, HRESULT, PCWSTR};

use crate::local_change_queue::inspect_cursor_only_local_change_queue;
use crate::namespace_reconciler::{
    CloudFilesCursorRecoveryReconciliationPlan, CloudFilesMaterializationAuditIntentStore,
    inspect_cursor_only_reconciliation_state, resume_authorized_cursor_recovery,
};
use crate::registration_journal::{
    REGISTRATION_IDENTITY_MAX_BYTES, RegistrationJournal, RegistrationOwnershipDisposition,
    RegistrationPhase, RegistrationRecoveryAction,
};
use crate::windows_sync::{
    CLOUD_FILES_DIRECTORY_PAGE_ENTRIES, CloudFilesMountedMutationHandoff,
    CloudFilesObjectProjection, CloudFilesWritableSyncState, cloud_files_object_projection,
    configure_cloud_files_mounted_read_policy, fresh_writable_scopes_are_absent,
};
use crate::{
    DecodedCloudFilesPlaceholderIdentity, decode_placeholder_identity, encode_placeholder_identity,
    host_emit_event, host_emit_event_ack, host_is_cancelled, host_log,
    placeholder_identity_mismatch,
};

#[path = "windows_adapter.rs"]
mod windows_adapter;

const WINDOWS_TICKS_PER_SECOND: u64 = 10_000_000;
const UNIX_EPOCH_AS_WINDOWS_FILETIME: u64 = 116_444_736 * WINDOWS_TICKS_PER_SECOND;
const CLOUD_FILES_PROVIDER_ID: GUID = GUID::from_u128(0xa318f9ce_140b_41bc_9e44_d0f154459584);
const CLOUD_FILES_PROVIDER_ID_TEXT: &str = "a318f9ce140b41bc9e44d0f154459584";
const CLOUD_FILES_REGISTRATION_RECEIPT_FILE: &str = "registration.sobj";
const CLOUD_FILES_REGISTRATION_JOURNAL_FILE: &str = "registration-journal.sobj";
const CLOUD_FILES_REGISTRATION_LOCK_FILE: &str = "registration.lock";
const CLOUD_FILES_CURSOR_RECOVERY_MARKER_FILE: &str = "cursor-recovery-control.sobj";
const CLOUD_FILES_CURSOR_RECOVERY_MODULE_TEMP_LABEL: &str = "cursor-recovery-marker-next";
const CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES: u64 = 1024 * 1024;
const CLOUD_FILES_REGISTRATION_RECEIPT_MAX_BYTES: u64 = 64 * 1024;
/// One Cloud Files DFS call remains bounded so cancellation can make progress,
/// but the bound must cover the daemon loopback hop plus a reused routed
/// cluster stream and the authority's durable commit. Two seconds was shorter
/// than a healthy routed namespace commit under load and converted successful
/// stable operations into `PossiblyApplied` recovery cycles.
use crate::endpoint_transport::{CLOUD_FILES_DFS_CALL_TIMEOUT, DfsCloudFilesEndpointTransport};
const CLOUD_FILES_DFS_LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const CLOUD_FILES_DFS_LOCK_RETRY: Duration = Duration::from_millis(5);
const CLOUD_FILES_TRANSFER_ALIGNMENT_BYTES: u64 = 4 * 1024;
const CLOUD_FILES_TRANSFER_CHUNK_BYTES: usize = 1024 * 1024;
const CLOUD_FILES_EOF: i64 = -1;

type SifsService = NetworkedDfsService<DfsCloudFilesEndpointTransport>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HydrationCallbackRange {
    offset: i64,
    length: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HydrationChunk {
    offset: u64,
    length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HydrationPlan {
    callback: HydrationCallbackRange,
    next_offset: u64,
    remaining: u64,
}

impl HydrationPlan {
    fn new(offset: i64, length: i64, file_size: i64) -> Result<Self, &'static str> {
        let callback = HydrationCallbackRange { offset, length };
        if offset < 0 {
            return Err("FETCH_DATA offset is negative");
        }
        let offset = offset as u64;
        if !offset.is_multiple_of(CLOUD_FILES_TRANSFER_ALIGNMENT_BYTES) {
            return Err("FETCH_DATA offset is not 4 KiB aligned");
        }
        if file_size < 0 {
            return Err("FETCH_DATA file size is negative");
        }
        let file_size = file_size as u64;
        if offset >= file_size {
            return Err("FETCH_DATA range starts at or beyond end of file");
        }
        let requested_end = if length == CLOUD_FILES_EOF {
            file_size
        } else {
            if length <= 0 {
                return Err("FETCH_DATA length is not positive or CF_EOF");
            }
            offset
                .checked_add(length as u64)
                .ok_or("FETCH_DATA range overflows")?
        };
        if requested_end < file_size
            && !(length as u64).is_multiple_of(CLOUD_FILES_TRANSFER_ALIGNMENT_BYTES)
        {
            return Err("FETCH_DATA length is not 4 KiB aligned before end of file");
        }
        let transfer_end = requested_end.min(file_size);
        let remaining = transfer_end
            .checked_sub(offset)
            .filter(|remaining| *remaining > 0)
            .ok_or("FETCH_DATA range is empty")?;
        Ok(Self {
            callback,
            next_offset: offset,
            remaining,
        })
    }

    fn next_chunk(&self) -> Option<HydrationChunk> {
        if self.remaining == 0 {
            return None;
        }
        Some(HydrationChunk {
            offset: self.next_offset,
            length: self.remaining.min(CLOUD_FILES_TRANSFER_CHUNK_BYTES as u64) as usize,
        })
    }

    fn advance(&mut self, transferred: usize) -> Result<(), &'static str> {
        let transferred = transferred as u64;
        if transferred == 0 || transferred > self.remaining {
            return Err("TRANSFER_DATA returned an invalid chunk length");
        }
        self.next_offset = self
            .next_offset
            .checked_add(transferred)
            .ok_or("TRANSFER_DATA offset overflows")?;
        self.remaining -= transferred;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalPlaceholderCollisionState {
    Absent,
    CleanPlaceholder,
    DirtyPlaceholder,
    OtherLocalEntry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlaceholderCreateDisposition {
    CreateAbsent,
    PreserveLocal,
}

fn placeholder_create_disposition(
    state: LocalPlaceholderCollisionState,
) -> PlaceholderCreateDisposition {
    match state {
        LocalPlaceholderCollisionState::Absent => PlaceholderCreateDisposition::CreateAbsent,
        LocalPlaceholderCollisionState::CleanPlaceholder
        | LocalPlaceholderCollisionState::DirtyPlaceholder
        | LocalPlaceholderCollisionState::OtherLocalEntry => {
            PlaceholderCreateDisposition::PreserveLocal
        }
    }
}

fn placeholder_transfer_fully_succeeded(
    execute_succeeded: bool,
    expected_count: u32,
    entries_processed: u32,
    entry_results: &[i32],
) -> bool {
    execute_succeeded
        && entries_processed == expected_count
        && entry_results.len() == expected_count as usize
        && entry_results.iter().all(|result| *result >= 0)
}

fn writable_worker_ready(status: &CloudFilesLocalChangeSyncStatus) -> bool {
    status.mode == CloudFilesLocalChangeSyncMode::Active
}

fn after_durable_materialization_intent<T, E>(
    persist_intent: impl FnOnce() -> Result<(), E>,
    effect: impl FnOnce() -> T,
) -> Result<T, E> {
    persist_intent()?;
    Ok(effect())
}

#[derive(Clone, Debug)]
struct MountLifecycleState {
    sync_root: PathBuf,
    root_path: String,
    placeholders_created: u64,
    registered: bool,
    connected: bool,
    local_change_sync: CloudFilesLocalChangeSyncStatus,
}

impl MountLifecycleState {
    fn new(request: &CloudFilesMountRequest) -> Self {
        Self {
            sync_root: request.sync_root.clone(),
            root_path: request.root_path.clone(),
            placeholders_created: 0,
            registered: false,
            connected: false,
            local_change_sync: request.local_change_sync_status(),
        }
    }

    fn for_unregister(request: &CloudFilesUnregisterRequest) -> Self {
        Self {
            sync_root: request.sync_root.clone(),
            root_path: request.root_path.clone(),
            placeholders_created: 0,
            registered: true,
            connected: false,
            local_change_sync: CloudFilesLocalChangeSyncStatus::disabled(),
        }
    }

    fn emit_registered(&self) {
        self.emit(CloudFilesMountLifecycleEvent::registered(
            self.sync_root.clone(),
            self.root_path.clone(),
            self.placeholders_created,
        ));
    }

    fn emit_connected(&self) {
        self.emit(CloudFilesMountLifecycleEvent::connected(
            self.sync_root.clone(),
            self.root_path.clone(),
            self.placeholders_created,
        ));
    }

    fn emit_disconnected(&self) {
        self.emit(CloudFilesMountLifecycleEvent::disconnected(
            self.sync_root.clone(),
            self.root_path.clone(),
            self.placeholders_created,
        ));
    }

    fn emit_unregistered(&self, detail: impl Into<String>) {
        self.emit(CloudFilesMountLifecycleEvent::unregistered(
            self.sync_root.clone(),
            self.root_path.clone(),
            detail,
        ));
    }

    fn emit_failed(&self, error: &CloudFilesModuleError) {
        self.emit(CloudFilesMountLifecycleEvent::failed(
            self.sync_root.clone(),
            self.root_path.clone(),
            self.placeholders_created,
            self.registered,
            self.connected,
            error.clone(),
        ));
    }

    fn emit(&self, event: CloudFilesMountLifecycleEvent) {
        host_emit_event(&CloudFilesModuleEvent::MountLifecycle(
            event.with_local_change_sync(self.local_change_sync.clone()),
        ));
    }
}

struct CloudFilesConnection {
    key: Option<CF_CONNECTION_KEY>,
    sync_root: PathBuf,
}

impl CloudFilesConnection {
    fn new(key: CF_CONNECTION_KEY, sync_root: PathBuf) -> Self {
        Self {
            key: Some(key),
            sync_root,
        }
    }

    fn disconnect(&mut self) -> Result<(), CloudFilesModuleError> {
        let Some(key) = self.key else {
            return Ok(());
        };
        unsafe { CfDisconnectSyncRoot(key) }.map_err(|source| CloudFilesModuleError {
            code: "disconnect-sync-root".to_string(),
            detail: format!(
                "disconnect Cloud Files sync root {}: {source}",
                self.sync_root.display()
            ),
        })?;
        self.key = None;
        Ok(())
    }
}

impl Drop for CloudFilesConnection {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        if let Err(source) = unsafe { CfDisconnectSyncRoot(key) } {
            host_log(&format!(
                "best-effort Cloud Files disconnect for {} failed: {source}",
                self.sync_root.display()
            ));
        }
    }
}

#[derive(Debug)]
enum RecoveredRegistration {
    Fresh,
    Owned {
        journal: Box<RegistrationJournal>,
        registration_present: bool,
    },
}

struct RecoveredUnregister {
    was_registered: bool,
    ownership: RegistrationOwnershipDisposition,
    cursor_recovery: Option<CloudFilesCursorRecoveryContext>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnregisterPolicy {
    Release,
    PreserveState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorRecoveryPreserveContextRelation {
    Current,
    PredecessorRetry,
}

struct RegistrationActivation {
    data_dir: SinomoDataDir,
    journal: RegistrationJournal,
    state_owned: bool,
    registration_preexisting: bool,
    armed: bool,
    terminal_completion_handoff: bool,
}

struct RegistrationLease {
    _file: fs::File,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum CursorRecoveryModulePhase {
    Authorized,
    Validated,
    StateOwned,
    ReconcileArmed,
    Ready,
    NamespaceFinalized,
    CompletionAuthorized,
    PreserveAuthorized,
    PreservedStateOwned,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum CursorRecoveryNamespaceResume {
    /// The authoritative root scan has not yet durably reached Terminal.
    #[default]
    ReconcileProgress,
    /// Terminal reconciliation is durable, but its rewrite still awaits the
    /// synchronous Ready/host finalization handshake.
    TerminalPendingFinalize,
    /// Namespace finalization was durably authorized. The Terminal rewrite may
    /// be present or absent, and no later recovery cycle may rescan remotely.
    Finalized,
}

fn cursor_recovery_namespace_resume_is_monotonic(
    authorized: CursorRecoveryNamespaceResume,
    current: CursorRecoveryNamespaceResume,
) -> bool {
    matches!(
        (authorized, current),
        (
            CursorRecoveryNamespaceResume::ReconcileProgress,
            CursorRecoveryNamespaceResume::ReconcileProgress
                | CursorRecoveryNamespaceResume::TerminalPendingFinalize
                | CursorRecoveryNamespaceResume::Finalized
        ) | (
            CursorRecoveryNamespaceResume::TerminalPendingFinalize,
            CursorRecoveryNamespaceResume::TerminalPendingFinalize
                | CursorRecoveryNamespaceResume::Finalized
        ) | (
            CursorRecoveryNamespaceResume::Finalized,
            CursorRecoveryNamespaceResume::Finalized
        )
    )
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CursorRecoveryModuleMarker {
    schema_version: u16,
    identity: CloudFilesCursorOnlyRecoveryIdentity,
    recovery_id: String,
    recovery_token: String,
    raw_state_fingerprint: String,
    registration_lock_bytes: Vec<u8>,
    module_topology: Vec<String>,
    queue_raw_files: BTreeMap<String, Vec<u8>>,
    reconciliation_raw_files: BTreeMap<String, Vec<u8>>,
    outbox_raw_files: BTreeMap<String, Vec<u8>>,
    sync_root_file_id: u64,
    registration_nonce: String,
    plan: CloudFilesCursorRecoveryReconciliationPlan,
    #[serde(default)]
    recovery_cycle: u64,
    /// Stable cycle commitment. PreserveAuthorized/PreservedStateOwned retain
    /// the predecessor cycle's commitment for lost-response replay; the
    /// PSO->ReconcileArmed transition replaces it with the hash of the exact
    /// PSO image and later same-cycle phases retain it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cycle_authorization_fingerprint: Option<String>,
    /// Exact preceding-cycle commitment carried through the successor cycle.
    /// It is part of the PSO hash and authorizes only a lost-response retry of
    /// the already-durable preserve intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    predecessor_cycle_fingerprint: Option<String>,
    /// Immutable namespace resume class captured at PreserveAuthorized. The
    /// live class may only advance; successor hashing always resets to this
    /// authorized predecessor.
    #[serde(default)]
    cycle_authorized_namespace_resume: CursorRecoveryNamespaceResume,
    #[serde(default)]
    namespace_resume: CursorRecoveryNamespaceResume,
    phase: CursorRecoveryModulePhase,
}

struct CursorRecoveryRawInspection {
    identity: CloudFilesCursorOnlyRecoveryIdentity,
    data_dir: SinomoDataDir,
    sync_root_file_id: u64,
    raw_state_fingerprint: String,
    recovery_token: String,
    registration_lock_bytes: Vec<u8>,
    module_topology: Vec<String>,
    queue_raw_files: BTreeMap<String, Vec<u8>>,
    reconciliation_raw_files: BTreeMap<String, Vec<u8>>,
    outbox_raw_files: BTreeMap<String, Vec<u8>>,
    reconciliation: crate::namespace_reconciler::CloudFilesCursorOnlyReconciliationInspectionGuard,
    root_fence: CursorRecoveryRootFence,
    _registration_lease: RegistrationLease,
    _queue: crate::local_change_queue::CloudFilesCursorOnlyQueueInspectionGuard,
    _outbox: dfs::MountedSifsMutationOutboxCursorRecoveryGuard,
}

struct CursorRecoveryOwnerGuards {
    _queue: crate::local_change_queue::CloudFilesCursorOnlyQueueInspectionGuard,
    _outbox: dfs::MountedSifsMutationOutboxCursorRecoveryGuard,
}

fn acquire_cursor_recovery_terminal_owner_guards(
    data_dir: &SinomoDataDir,
    marker: &CursorRecoveryModuleMarker,
) -> Result<CursorRecoveryOwnerGuards, CloudFilesModuleError> {
    // Keep the same outbox -> queue -> reconciliation order used by the raw
    // recovery snapshot. Each inspector retains its owner lock, so no work can
    // appear between strict Terminal proof and the module successor fsync.
    let outbox = dfs::inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(data_dir).map_err(
        |error| CloudFilesModuleError {
            code: "cursor-recovery-outbox".to_string(),
            detail: error.to_string(),
        },
    )?;
    if !outbox.inspection().outbox_present {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-outbox-absent".to_string(),
            detail: "terminal successor lost its canonical empty outbox owner".to_string(),
        });
    }
    if !outbox
        .matches_original_or_committed_work_successor(&marker.outbox_raw_files)
        .map_err(|error| CloudFilesModuleError {
            code: "cursor-recovery-outbox".to_string(),
            detail: error.to_string(),
        })?
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-outbox-drift".to_string(),
            detail: "terminal successor outbox is neither the token-bound snapshot nor its monotonic committed-work latch successor".to_string(),
        });
    }
    let queue = inspect_cursor_only_local_change_queue(
        data_dir,
        &marker.identity.sync_root.to_string_lossy(),
        &marker.identity.root_path,
    )
    .map_err(|error| CloudFilesModuleError {
        code: error.code.to_string(),
        detail: error.detail,
    })?;
    if !queue.inspection().scope_present {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-queue-absent".to_string(),
            detail: "terminal successor lost its canonical empty local-change queue".to_string(),
        });
    }
    if !queue
        .matches_original_or_reconciled_successor(
            &marker.queue_raw_files,
            &marker.identity.sync_root.to_string_lossy(),
            &marker.identity.root_path,
        )
        .map_err(|error| CloudFilesModuleError {
            code: error.code.to_string(),
            detail: error.detail,
        })?
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-queue-drift".to_string(),
            detail: "terminal successor queue is neither the exact empty token-bound snapshot nor an exact scan-reconciled terminal cut".to_string(),
        });
    }
    Ok(CursorRecoveryOwnerGuards {
        _queue: queue,
        _outbox: outbox,
    })
}

struct CursorRecoveryRootFence {
    _handle: OwnedWin32Handle,
    file_id: u64,
}

struct RegistrationArtifactSnapshot<T> {
    current: Option<T>,
    temporary: Option<T>,
}

impl RegistrationLease {
    fn acquire(data_dir: &SinomoDataDir) -> Result<Self, CloudFilesModuleError> {
        let path = data_dir
            .module_runtime_dir()
            .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
            .join(CLOUD_FILES_REGISTRATION_LOCK_FILE);
        let Some(parent) = path.parent() else {
            return Err(CloudFilesModuleError {
                code: "registration-lock-path".to_string(),
                detail: format!("registration lock path {} has no parent", path.display()),
            });
        };
        fs::create_dir_all(parent).map_err(|source| CloudFilesModuleError {
            code: "registration-lock-dir".to_string(),
            detail: format!(
                "create Cloud Files registration lock directory {}: {source}",
                parent.display()
            ),
        })?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| CloudFilesModuleError {
                code: "registration-lock-open".to_string(),
                detail: format!(
                    "open Cloud Files registration lock {}: {source}",
                    path.display()
                ),
            })?;
        file.try_lock_exclusive()
            .map_err(|source| CloudFilesModuleError {
                code: "registration-lease-busy".to_string(),
                detail: format!(
                    "another Cloud Files mount or unregister owns {}: {source}",
                    path.display()
                ),
            })?;
        Ok(Self { _file: file })
    }

    /// Read-only inspection may serialize against a pre-existing registration
    /// owner but must never create the module directory or lock file.
    fn acquire_existing_for_inspection(
        data_dir: &SinomoDataDir,
    ) -> Result<(Self, Vec<u8>), CloudFilesModuleError> {
        let path = data_dir
            .module_runtime_dir()
            .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
            .join(CLOUD_FILES_REGISTRATION_LOCK_FILE);
        let metadata = fs::symlink_metadata(&path).map_err(|source| CloudFilesModuleError {
            code: "cursor-recovery-registration-lock".to_string(),
            detail: format!(
                "cursor recovery requires a pre-existing registration lock {}: {source}",
                path.display()
            ),
        })?;
        if !metadata.file_type().is_file()
            || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
            || metadata.len() != 0
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-registration-lock".to_string(),
                detail: format!(
                    "cursor recovery registration lock {} is not a canonical zero-byte plain file",
                    path.display()
                ),
            });
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .map_err(|source| CloudFilesModuleError {
                code: "cursor-recovery-registration-lock".to_string(),
                detail: format!(
                    "open pre-existing registration lock {}: {source}",
                    path.display()
                ),
            })?;
        file.try_lock_exclusive()
            .map_err(|source| CloudFilesModuleError {
                code: "registration-lease-busy".to_string(),
                detail: format!(
                    "another Cloud Files operation owns {}: {source}",
                    path.display()
                ),
            })?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.read_to_end(&mut bytes))
            .map_err(|source| CloudFilesModuleError {
                code: "cursor-recovery-registration-lock".to_string(),
                detail: format!(
                    "read pre-existing registration lock {}: {source}",
                    path.display()
                ),
            })?;
        if !bytes.is_empty() {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-registration-lock".to_string(),
                detail: "registration lock changed during cursor recovery inspection".to_string(),
            });
        }
        Ok((Self { _file: file }, bytes))
    }
}

fn acquire_registration_lease_for_request(
    data_dir: &SinomoDataDir,
    cursor_recovery: Option<&CloudFilesCursorRecoveryContext>,
) -> Result<(RegistrationLease, Option<Vec<u8>>), CloudFilesModuleError> {
    if cursor_recovery.is_some() {
        let (lease, bytes) = RegistrationLease::acquire_existing_for_inspection(data_dir)?;
        return Ok((lease, Some(bytes)));
    }
    for path in [
        cursor_recovery_module_marker_path(data_dir),
        cursor_recovery_module_marker_temp_path(data_dir),
    ] {
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-context-required".to_string(),
                    detail: format!(
                        "ordinary operation found cursor-recovery authority at {}",
                        path.display()
                    ),
                });
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-marker-inspect".to_string(),
                    detail: format!(
                        "inspect cursor-recovery authority {}: {source}",
                        path.display()
                    ),
                });
            }
        }
    }
    let lease = RegistrationLease::acquire(data_dir)?;
    for path in [
        cursor_recovery_module_marker_path(data_dir),
        cursor_recovery_module_marker_temp_path(data_dir),
    ] {
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-context-required".to_string(),
                    detail: format!(
                        "cursor-recovery authority appeared before ordinary lease admission at {}",
                        path.display()
                    ),
                });
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-marker-inspect".to_string(),
                    detail: format!(
                        "reinspect cursor-recovery authority {}: {source}",
                        path.display()
                    ),
                });
            }
        }
    }
    Ok((lease, None))
}

impl RegistrationActivation {
    fn new(data_dir: SinomoDataDir, journal: RegistrationJournal, state_owned: bool) -> Self {
        Self {
            data_dir,
            journal,
            state_owned,
            registration_preexisting: false,
            armed: true,
            terminal_completion_handoff: false,
        }
    }

    fn retain_terminal_completion_handoff(&mut self) -> Result<(), CloudFilesModuleError> {
        if self.journal.phase != RegistrationPhase::Ready || !self.state_owned {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-completion-journal-phase".to_string(),
                detail: "CompletionAuthorized mount requires an already-owned Ready journal"
                    .to_string(),
            });
        }
        self.terminal_completion_handoff = true;
        // CompletionAuthorized is forward-only. A failed restart must retain
        // its exact Ready/ordinary normalization cut instead of invoking the
        // generic activation rollback.
        self.armed = false;
        Ok(())
    }

    fn journal(&self) -> &RegistrationJournal {
        &self.journal
    }

    fn mark_registered(
        &mut self,
        request: &CloudFilesMountRequest,
    ) -> Result<(), CloudFilesModuleError> {
        if self.terminal_completion_handoff {
            return Ok(());
        }
        if !self.state_owned {
            self.journal.phase = RegistrationPhase::Registered;
        }
        self.journal.provider_version = request.provider_version.clone();
        // From the first attempt to persist Registered onward, a failed or
        // ambiguous write must retain ownership. The on-disk outcome may be
        // Registered even when the atomic-write call reports failure.
        self.state_owned = true;
        persist_registration_journal(&self.data_dir, &self.journal)
    }

    fn mark_state_owned(
        &mut self,
        request: &CloudFilesMountRequest,
    ) -> Result<(), CloudFilesModuleError> {
        if self.terminal_completion_handoff {
            return Ok(());
        }
        self.journal.phase = RegistrationPhase::StateOwned;
        self.journal.provider_version = request.provider_version.clone();
        self.state_owned = true;
        persist_registration_journal(&self.data_dir, &self.journal)?;
        Ok(())
    }

    fn mark_ready(
        &mut self,
        request: &CloudFilesMountRequest,
    ) -> Result<(), CloudFilesModuleError> {
        if self.terminal_completion_handoff {
            return Ok(());
        }
        // The stable receipt means that the whole provider is ready, not merely
        // that CfRegisterSyncRoot returned. Keep the journal at StateOwned until
        // the receipt is durable so a crash cannot surrender writable state.
        persist_registration_receipt(request, &self.journal, &self.data_dir)?;
        self.journal.phase = RegistrationPhase::Ready;
        self.journal.provider_version = request.provider_version.clone();
        persist_registration_journal(&self.data_dir, &self.journal)
    }

    fn mark_cursor_recovery_ready(
        &mut self,
        request: &CloudFilesMountRequest,
        marker_path: &Path,
        marker: &mut CursorRecoveryModuleMarker,
    ) -> Result<(), CloudFilesModuleError> {
        if marker.phase != CursorRecoveryModulePhase::ReconcileArmed
            || !matches!(
                marker.namespace_resume,
                CursorRecoveryNamespaceResume::TerminalPendingFinalize
                    | CursorRecoveryNamespaceResume::Finalized
            )
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-terminal-successor-missing".to_string(),
                detail: "Ready artifacts require ReconcileArmed with a prior durable terminal/finalized namespace successor".to_string(),
            });
        }
        self.mark_ready(request)?;
        marker.phase = CursorRecoveryModulePhase::Ready;
        persist_cursor_recovery_module_marker(marker_path, marker)
    }

    fn mark_cursor_recovery_terminal_pending_finalize(
        &mut self,
        marker_path: &Path,
        marker: &mut CursorRecoveryModuleMarker,
    ) -> Result<(), CloudFilesModuleError> {
        if marker.phase != CursorRecoveryModulePhase::ReconcileArmed
            || marker.namespace_resume != CursorRecoveryNamespaceResume::ReconcileProgress
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-terminal-successor-invalid".to_string(),
                detail: "only ReconcileArmed/ReconcileProgress may enter TerminalPendingFinalize"
                    .to_string(),
            });
        }
        marker.namespace_resume = CursorRecoveryNamespaceResume::TerminalPendingFinalize;
        persist_cursor_recovery_module_marker(marker_path, marker)
    }

    fn mark_cursor_recovery_preserve_authorized(
        &mut self,
        marker_path: &Path,
        marker: &mut CursorRecoveryModuleMarker,
    ) -> Result<(), CloudFilesModuleError> {
        if matches!(
            marker.phase,
            CursorRecoveryModulePhase::Authorized
                | CursorRecoveryModulePhase::Validated
                | CursorRecoveryModulePhase::PreserveAuthorized
        ) {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-preserve-predecessor".to_string(),
                detail: "cursor recovery has no exact state-owned preserve predecessor".to_string(),
            });
        }
        marker.predecessor_cycle_fingerprint = cursor_recovery_cycle_authorization(marker)?;
        marker.cycle_authorization_fingerprint = None;
        marker.recovery_cycle =
            marker
                .recovery_cycle
                .checked_add(1)
                .ok_or_else(|| CloudFilesModuleError {
                    code: "cursor-recovery-cycle-overflow".to_string(),
                    detail: "cursor recovery preserve cycle overflowed".to_string(),
                })?;
        marker.cycle_authorized_namespace_resume = marker.namespace_resume;
        marker.phase = CursorRecoveryModulePhase::PreserveAuthorized;
        persist_cursor_recovery_module_marker(marker_path, marker)
    }

    fn mark_cursor_recovery_preserved_state_owned(
        &mut self,
        marker_path: &Path,
        marker: &mut CursorRecoveryModuleMarker,
    ) -> Result<CloudFilesCursorRecoveryContext, CloudFilesModuleError> {
        if marker.phase != CursorRecoveryModulePhase::PreserveAuthorized {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-preserve-successor".to_string(),
                detail: "PreservedStateOwned requires a durable PreserveAuthorized predecessor"
                    .to_string(),
            });
        }
        marker.phase = CursorRecoveryModulePhase::PreservedStateOwned;
        persist_cursor_recovery_module_marker(marker_path, marker)?;
        Ok(CloudFilesCursorRecoveryContext {
            recovery_id: marker.recovery_id.clone(),
            recovery_token: marker.recovery_token.clone(),
            recovery_cycle: marker.recovery_cycle,
            native_successor_fingerprint: Some(cursor_recovery_native_successor_fingerprint(
                marker,
            )?),
        })
    }

    fn mark_cursor_recovery_namespace_finalized(
        &mut self,
        marker_path: &Path,
        marker: &mut CursorRecoveryModuleMarker,
    ) -> Result<(), CloudFilesModuleError> {
        marker.namespace_resume = CursorRecoveryNamespaceResume::Finalized;
        marker.phase = CursorRecoveryModulePhase::NamespaceFinalized;
        persist_cursor_recovery_module_marker(marker_path, marker)
    }

    fn mark_cursor_recovery_completion_authorized(
        &mut self,
        marker_path: &Path,
        marker: &mut CursorRecoveryModuleMarker,
    ) -> Result<(), CloudFilesModuleError> {
        if marker.phase != CursorRecoveryModulePhase::NamespaceFinalized
            || marker.namespace_resume != CursorRecoveryNamespaceResume::Finalized
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-completion-predecessor".to_string(),
                detail: "CompletionAuthorized requires the exact NamespaceFinalized/Finalized predecessor"
                    .to_string(),
            });
        }
        marker.phase = CursorRecoveryModulePhase::CompletionAuthorized;
        persist_cursor_recovery_module_marker(marker_path, marker)
    }

    fn rollback(&mut self) -> Result<(), CloudFilesModuleError> {
        rollback_activation_registration(
            &self.data_dir,
            &self.journal,
            self.state_owned,
            self.registration_preexisting,
        )?;
        self.armed = false;
        Ok(())
    }

    fn fail(
        &mut self,
        mut error: CloudFilesModuleError,
        lifecycle: &mut MountLifecycleState,
    ) -> CloudFilesModuleError {
        if !self.armed {
            error.detail.push_str(
                "; durable cursor-recovery Ready ownership was retained for exact replay",
            );
            return error;
        }
        match self.rollback() {
            Ok(()) => {
                if self.registration_preexisting {
                    lifecycle.registered = true;
                    error.detail.push_str(
                        "; pre-existing nonce-owned Cloud Files registration and durable state ownership were retained for retry",
                    );
                } else if self.state_owned {
                    lifecycle.registered = false;
                    error.detail.push_str(
                        "; nonce-owned Cloud Files registration rolled back and verified absent while durable state ownership was retained",
                    );
                } else {
                    lifecycle.registered = false;
                    error.detail.push_str(
                        "; nonce-owned Cloud Files registration rolled back and verified absent",
                    );
                }
            }
            Err(rollback_error) => {
                error.detail.push_str(&format!(
                    "; registration rollback pending in durable journal: {rollback_error}"
                ));
            }
        }
        error
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RegistrationActivation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Err(error) = self.rollback() {
            host_log(&format!(
                "best-effort Cloud Files activation rollback deferred to durable recovery: {error}"
            ));
        }
    }
}

pub(crate) fn inspect_cursor_only_recovery(
    mut request: CloudFilesCursorOnlyRecoveryInspectRequest,
) -> Result<CloudFilesCursorOnlyRecoveryInspection, CloudFilesModuleError> {
    request.identity.root_path = normalize_sifs_root(&request.identity.root_path)?;
    let inspection = inspect_cursor_only_recovery_raw(request.identity)?;
    let cursor_count = u64::try_from(inspection.reconciliation.inspection().cursors.len())
        .map_err(|_| CloudFilesModuleError {
            code: "cursor-recovery-cursor-count".to_string(),
            detail: "cursor count overflowed u64".to_string(),
        })?;
    let startup_conflict_count = inspection
        .reconciliation
        .inspection()
        .startup_conflict_count;
    let materialized_directory_count = inspection
        .reconciliation
        .inspection()
        .materialized_directory_count;
    Ok(CloudFilesCursorOnlyRecoveryInspection {
        schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
        sync_root: inspection.identity.sync_root.clone(),
        root_path: inspection.identity.root_path.clone(),
        sync_root_file_id: inspection.sync_root_file_id,
        cursor_count,
        raw_state_fingerprint: inspection.raw_state_fingerprint,
        recovery_token: inspection.recovery_token,
        detail: format!(
            "exact recovery state is eligible with {} retained startup conflict diagnostic(s) and {} materialized-directory marker(s); applying this token clears the stale cursors and derived startup conflicts, then requires retained writeback, full authoritative reconciliation, and a fresh local startup audit before readiness",
            startup_conflict_count, materialized_directory_count,
        ),
    })
}

pub(crate) fn apply_cursor_only_recovery(
    mut request: CloudFilesCursorOnlyRecoveryApplyRequest,
) -> Result<CloudFilesCursorOnlyRecoveryApplied, CloudFilesModuleError> {
    if !request.acknowledge_authoritative_full_reconciliation {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-acknowledgement-required".to_string(),
            detail: "the native effect boundary requires explicit acknowledgement of authoritative full reconciliation".to_string(),
        });
    }
    if request.expected_token.trim().is_empty() || request.recovery_id.trim().is_empty() {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-authorization-invalid".to_string(),
            detail: "expected token and stable recovery ID must be nonempty".to_string(),
        });
    }
    request.identity.root_path = normalize_sifs_root(&request.identity.root_path)?;
    let data_dir = resolve_cursor_recovery_data_dir(&request.identity)?;
    let (_registration_lease, _) = RegistrationLease::acquire_existing_for_inspection(&data_dir)?;
    let marker_path = cursor_recovery_module_marker_path(&data_dir);
    let existing = recover_cursor_recovery_registration_snapshot(&data_dir, true, |marker| {
        validate_cursor_recovery_apply_authority(marker, &request)
    })?;
    if let Some(marker) = existing {
        inspect_cursor_recovery_module_topology(&data_dir, Some(marker.phase))?;
        if matches!(
            marker.phase,
            CursorRecoveryModulePhase::ReconcileArmed
                | CursorRecoveryModulePhase::Ready
                | CursorRecoveryModulePhase::NamespaceFinalized
        ) {
            validate_owned_cursor_recovery_root(&request.identity, marker.sync_root_file_id)?;
            validate_cursor_recovery_registration_artifacts(&data_dir, &marker)?;
            return Ok(cursor_recovery_applied_response(marker));
        }
        let root_fence = CursorRecoveryRootFence::acquire(&request.identity.sync_root)?;
        validate_cursor_recovery_root(&request.identity, marker.sync_root_file_id, &root_fence)?;
        if query_registered_sync_root(&request.identity.sync_root)?.is_some() {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-registration-present".to_string(),
                detail: "cursor recovery cannot resume while a CFAPI registration is present"
                    .to_string(),
            });
        }
        let outbox = dfs::inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir)
            .map_err(|error| CloudFilesModuleError {
                code: "cursor-recovery-outbox".to_string(),
                detail: error.to_string(),
            })?;
        let queue = inspect_cursor_only_local_change_queue(
            &data_dir,
            &request.identity.sync_root.to_string_lossy(),
            &request.identity.root_path,
        )
        .map_err(|error| CloudFilesModuleError {
            code: error.code.to_string(),
            detail: error.detail,
        })?;
        let reconciliation = resume_authorized_cursor_recovery(
            &data_dir,
            &request.identity.sync_root.to_string_lossy(),
            &request.identity.root_path,
            &marker.plan,
        )
        .map_err(reconciliation_recovery_error)?;
        if !queue.inspection().scope_present || !outbox.inspection().outbox_present {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-owner-evidence-lost".to_string(),
                detail: "authorized cursor recovery lost queue or outbox ownership evidence"
                    .to_string(),
            });
        }
        if queue.inspection().raw_files != marker.queue_raw_files
            || outbox.inspection().raw_files != marker.outbox_raw_files
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-owner-evidence-drift".to_string(),
                detail: "authorized cursor recovery queue or outbox bytes changed from the token-bound owner snapshot"
                    .to_string(),
            });
        }
        let guards = CursorRecoveryOwnerGuards {
            _queue: queue,
            _outbox: outbox,
        };
        return finish_cursor_only_recovery(
            &data_dir,
            &marker_path,
            marker,
            &reconciliation,
            &root_fence,
            &guards,
        );
    } else {
        drop(_registration_lease);
        let raw = inspect_cursor_only_recovery_raw(request.identity.clone())?;
        if raw.recovery_token != request.expected_token {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-token-mismatch".to_string(),
                detail: "the exact cursor-only snapshot changed after inspection".to_string(),
            });
        }
        let nonce = GUID::new().map_err(|source| CloudFilesModuleError {
            code: "registration-nonce-generate".to_string(),
            detail: format!("generate cursor recovery registration nonce: {source}"),
        })?;
        let nonce = format!("{:032x}", nonce.to_u128());
        let plan = raw
            .reconciliation
            .authoritative_reconciliation_plan(
                &request.recovery_id,
                &request.expected_token,
                &nonce,
            )
            .map_err(reconciliation_recovery_error)?;
        let encoded_marker_bound = CursorRecoveryModuleMarker {
            schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            identity: request.identity.clone(),
            recovery_id: request.recovery_id.clone(),
            recovery_token: request.expected_token.clone(),
            raw_state_fingerprint: raw.raw_state_fingerprint.clone(),
            registration_lock_bytes: raw.registration_lock_bytes.clone(),
            module_topology: raw.module_topology.clone(),
            queue_raw_files: raw.queue_raw_files.clone(),
            reconciliation_raw_files: raw.reconciliation_raw_files.clone(),
            outbox_raw_files: raw.outbox_raw_files.clone(),
            sync_root_file_id: raw.sync_root_file_id,
            registration_nonce: nonce,
            plan,
            recovery_cycle: 0,
            cycle_authorization_fingerprint: None,
            predecessor_cycle_fingerprint: None,
            cycle_authorized_namespace_resume: CursorRecoveryNamespaceResume::ReconcileProgress,
            namespace_resume: CursorRecoveryNamespaceResume::ReconcileProgress,
            phase: CursorRecoveryModulePhase::Authorized,
        };
        let marker_bytes =
            encode_sobj(&encoded_marker_bound).map_err(|source| CloudFilesModuleError {
                code: "cursor-recovery-marker-encode".to_string(),
                detail: format!("encode cursor recovery marker: {source}"),
            })?;
        if marker_bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_MARKER_MAX_BYTES {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-marker-too-large".to_string(),
                detail: "cursor recovery marker exceeds its durable owner bound".to_string(),
            });
        }
        validate_cursor_recovery_root(&request.identity, raw.sync_root_file_id, &raw.root_fence)?;
        persist_cursor_recovery_module_marker(&marker_path, &encoded_marker_bound)?;
        return finish_cursor_only_recovery(
            &raw.data_dir,
            &marker_path,
            encoded_marker_bound,
            &raw.reconciliation,
            &raw.root_fence,
            &CursorRecoveryOwnerGuards {
                _queue: raw._queue,
                _outbox: raw._outbox,
            },
        );
    }
}

fn cursor_recovery_applied_response(
    marker: CursorRecoveryModuleMarker,
) -> CloudFilesCursorOnlyRecoveryApplied {
    CloudFilesCursorOnlyRecoveryApplied {
        schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
        sync_root: marker.identity.sync_root,
        root_path: marker.identity.root_path,
        sync_root_file_id: marker.sync_root_file_id,
        cursor_count_cleared: marker.plan.cursor_count_cleared,
        recovery_id: marker.recovery_id,
        recovery_token: marker.recovery_token,
        detail: "cursor recovery is already durably owned and ReconcileArmed or Ready; the same authorization is idempotently retained".to_string(),
    }
}

fn validate_cursor_recovery_registration_artifacts(
    data_dir: &SinomoDataDir,
    marker: &CursorRecoveryModuleMarker,
) -> Result<(), CloudFilesModuleError> {
    let request = cursor_recovery_mount_request(marker);
    let expected_journal = RegistrationJournal::cursor_recovery_state_owned(
        &request,
        CLOUD_FILES_PROVIDER_ID_TEXT,
        marker.registration_nonce.clone(),
        marker.sync_root_file_id,
        marker.recovery_id.clone(),
        marker.recovery_token.clone(),
        marker.recovery_cycle,
    )?;
    let journal_path = registration_journal_path(data_dir);
    let journal_snapshot = inspect_registration_artifact_snapshot::<RegistrationJournal>(
        &journal_path,
        &registration_journal_temp_path(data_dir),
        CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES,
        "registration-journal",
        "Cloud Files registration journal",
    )?;
    if let Some(journal) = journal_snapshot.current.as_ref() {
        journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    }
    if let Some(journal) = journal_snapshot.temporary.as_ref() {
        journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    }
    let mut ready_journal = expected_journal.clone();
    ready_journal.phase = RegistrationPhase::Ready;
    let mut ordinary_ready_journal = ready_journal.clone();
    ordinary_ready_journal.clear_cursor_recovery();
    let journal_matches = match marker.phase {
        CursorRecoveryModulePhase::ReconcileArmed => {
            journal_snapshot.current.as_ref() == Some(&expected_journal)
                || journal_snapshot.current.as_ref() == Some(&ready_journal)
                || (journal_snapshot.current.as_ref() == Some(&expected_journal)
                    && journal_snapshot.temporary.as_ref() == Some(&ready_journal))
        }
        CursorRecoveryModulePhase::Ready | CursorRecoveryModulePhase::NamespaceFinalized => {
            journal_snapshot.current.as_ref() == Some(&ready_journal)
                || journal_snapshot.current.as_ref() == Some(&expected_journal)
                || (journal_snapshot.current.is_none()
                    && journal_snapshot.temporary.as_ref() == Some(&ready_journal))
        }
        CursorRecoveryModulePhase::CompletionAuthorized => {
            (journal_snapshot.current.as_ref() == Some(&ready_journal)
                || journal_snapshot.current.as_ref() == Some(&ordinary_ready_journal))
                && journal_snapshot.temporary.is_none()
        }
        _ => false,
    };
    if !journal_matches {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-state-owned-mismatch".to_string(),
            detail: "cursor recovery registration journal does not match its exact durable phase"
                .to_string(),
        });
    }
    let receipt_path = registration_receipt_path(data_dir);
    let receipt_snapshot = inspect_registration_artifact_snapshot::<CloudFilesRegistrationReceipt>(
        &receipt_path,
        &registration_receipt_temp_path(data_dir),
        CLOUD_FILES_REGISTRATION_RECEIPT_MAX_BYTES,
        "registration-receipt",
        "Cloud Files registration receipt",
    )?;
    let receipt = receipt_snapshot.current;
    let expected = CloudFilesRegistrationReceipt {
        interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
        registration_identity_interface_version: None,
        sync_root: request.sync_root,
        root_path: request.root_path,
        provider_name: request.provider_name,
    };
    if marker.phase != CursorRecoveryModulePhase::CompletionAuthorized
        && (journal_snapshot
            .temporary
            .as_ref()
            .is_some_and(|value| value != &ready_journal)
            || (journal_snapshot.current.is_some()
                && journal_snapshot.temporary.is_some()
                && journal_snapshot.current.as_ref() != journal_snapshot.temporary.as_ref()))
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-journal-temp-mismatch".to_string(),
            detail: "cursor recovery journal temporary file is not the exact next Ready image"
                .to_string(),
        });
    }
    if receipt_snapshot
        .temporary
        .as_ref()
        .is_some_and(|value| value != &expected)
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-receipt-temp-mismatch".to_string(),
            detail: "cursor recovery receipt temporary file changed from its exact Ready image"
                .to_string(),
        });
    }
    if journal_snapshot.current.is_none() && journal_snapshot.temporary.is_some() {
        promote_exact_registration_artifact(
            &journal_path,
            &registration_journal_temp_path(data_dir),
            CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES,
            &ready_journal,
            "registration-journal",
            "Cloud Files registration journal",
        )?;
    }
    if receipt.is_none() && receipt_snapshot.temporary.is_some() {
        promote_exact_registration_artifact(
            &receipt_path,
            &registration_receipt_temp_path(data_dir),
            CLOUD_FILES_REGISTRATION_RECEIPT_MAX_BYTES,
            &expected,
            "registration-receipt",
            "Cloud Files registration receipt",
        )?;
    }
    let receipt_matches =
        receipt.as_ref() == Some(&expected) || receipt_snapshot.temporary.is_some();
    match marker.phase {
        CursorRecoveryModulePhase::ReconcileArmed
            if receipt.is_none() || receipt.as_ref() == Some(&expected) =>
        {
            Ok(())
        }
        CursorRecoveryModulePhase::Ready
        | CursorRecoveryModulePhase::NamespaceFinalized
        | CursorRecoveryModulePhase::CompletionAuthorized => {
            if receipt_matches {
                Ok(())
            } else {
                Err(CloudFilesModuleError {
                    code: "cursor-recovery-ready-receipt-mismatch".to_string(),
                    detail: "Ready cursor recovery receipt does not match its exact authorization"
                        .to_string(),
                })
            }
        }
        _ => Err(CloudFilesModuleError {
            code: "cursor-recovery-registration-phase".to_string(),
            detail: "registration artifact validation was requested for a non-owned phase"
                .to_string(),
        }),
    }
}

fn rearm_preserved_cursor_recovery(
    data_dir: &SinomoDataDir,
    marker_path: &Path,
    marker: &mut CursorRecoveryModuleMarker,
) -> Result<(), CloudFilesModuleError> {
    if marker.phase != CursorRecoveryModulePhase::PreservedStateOwned {
        return Ok(());
    }
    let (state_owned, _, receipt) = cursor_recovery_expected_registration_artifacts(marker)?;
    let journal_snapshot = inspect_registration_artifact_snapshot::<RegistrationJournal>(
        &registration_journal_path(data_dir),
        &registration_journal_temp_path(data_dir),
        CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES,
        "registration-journal",
        "Cloud Files registration journal",
    )?;
    let receipt_snapshot = inspect_registration_artifact_snapshot::<CloudFilesRegistrationReceipt>(
        &registration_receipt_path(data_dir),
        &registration_receipt_temp_path(data_dir),
        CLOUD_FILES_REGISTRATION_RECEIPT_MAX_BYTES,
        "registration-receipt",
        "Cloud Files registration receipt",
    )?;
    if journal_snapshot.current.as_ref() != Some(&state_owned)
        || journal_snapshot.temporary.is_some()
        || receipt_snapshot
            .current
            .as_ref()
            .is_some_and(|value| value != &receipt)
        || receipt_snapshot.temporary.is_some()
        || query_registered_sync_root(&marker.identity.sync_root)?.is_some()
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-rearm-artifacts".to_string(),
            detail: "PreservedStateOwned rearm requires exact owned-absent successor artifacts"
                .to_string(),
        });
    }
    let successor = cursor_recovery_rearmed_successor(marker)?;
    persist_cursor_recovery_module_marker(marker_path, &successor)?;
    *marker = successor;
    Ok(())
}

fn finish_cursor_only_recovery(
    data_dir: &SinomoDataDir,
    marker_path: &Path,
    mut marker: CursorRecoveryModuleMarker,
    reconciliation: &crate::namespace_reconciler::CloudFilesCursorOnlyReconciliationInspectionGuard,
    root_fence: &CursorRecoveryRootFence,
    _owner_guards: &CursorRecoveryOwnerGuards,
) -> Result<CloudFilesCursorOnlyRecoveryApplied, CloudFilesModuleError> {
    if marker.phase == CursorRecoveryModulePhase::Authorized {
        validate_cursor_recovery_root(&marker.identity, marker.sync_root_file_id, root_fence)?;
        marker.phase = CursorRecoveryModulePhase::Validated;
        persist_cursor_recovery_module_marker(marker_path, &marker)?;
    }
    if marker.phase == CursorRecoveryModulePhase::Validated {
        validate_cursor_recovery_root(&marker.identity, marker.sync_root_file_id, root_fence)?;
        let mount_request = cursor_recovery_mount_request(&marker);
        let expected_journal = RegistrationJournal::cursor_recovery_state_owned(
            &mount_request,
            CLOUD_FILES_PROVIDER_ID_TEXT,
            marker.registration_nonce.clone(),
            marker.sync_root_file_id,
            marker.recovery_id.clone(),
            marker.recovery_token.clone(),
            marker.recovery_cycle,
        )?;
        match load_registration_journal(data_dir)? {
            None => persist_registration_journal(data_dir, &expected_journal)?,
            Some(journal) if journal == expected_journal => {}
            Some(_) => {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-state-owned-mismatch".to_string(),
                    detail: "Validated recovery found a changed StateOwned journal".to_string(),
                });
            }
        }
        marker.phase = CursorRecoveryModulePhase::StateOwned;
        persist_cursor_recovery_module_marker(marker_path, &marker)?;
    } else {
        let journal =
            load_registration_journal(data_dir)?.ok_or_else(|| CloudFilesModuleError {
                code: "cursor-recovery-state-owned-missing".to_string(),
                detail: "authorized cursor recovery lost its StateOwned registration journal"
                    .to_string(),
            })?;
        let mount_request = cursor_recovery_mount_request(&marker);
        if journal.phase != RegistrationPhase::StateOwned
            || journal.registration_nonce != marker.registration_nonce
            || !journal.matches_mount_request(&mount_request)
            || journal.sync_root_file_id != marker.sync_root_file_id
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-state-owned-mismatch".to_string(),
                detail: "StateOwned journal does not exactly match the authorized cursor recovery"
                    .to_string(),
            });
        }
    }
    validate_cursor_recovery_root(&marker.identity, marker.sync_root_file_id, root_fence)?;
    let cursor_count = reconciliation
        .apply_authoritative_reconciliation_plan(&marker.plan)
        .map_err(reconciliation_recovery_error)?;
    marker.phase = CursorRecoveryModulePhase::ReconcileArmed;
    persist_cursor_recovery_module_marker(marker_path, &marker)?;
    Ok(CloudFilesCursorOnlyRecoveryApplied {
        schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
        sync_root: marker.identity.sync_root,
        root_path: marker.identity.root_path,
        sync_root_file_id: marker.sync_root_file_id,
        cursor_count_cleared: cursor_count,
        recovery_id: marker.recovery_id,
        recovery_token: marker.recovery_token,
        detail: "cursor state is durably ReconcileArmed; the supervisor must retain StateOwned ownership and keep readiness gated through terminal authoritative and local startup reconciliation".to_string(),
    })
}

fn inspect_cursor_only_recovery_raw(
    identity: CloudFilesCursorOnlyRecoveryIdentity,
) -> Result<CursorRecoveryRawInspection, CloudFilesModuleError> {
    validate_cursor_recovery_identity(&identity)?;
    let data_dir = resolve_cursor_recovery_data_dir(&identity)?;
    let (registration_lease, registration_lock_bytes) =
        RegistrationLease::acquire_existing_for_inspection(&data_dir)?;
    let module_topology = inspect_cursor_recovery_module_topology(&data_dir, None)?;
    if load_registration_journal(&data_dir)?.is_some()
        || load_registration_receipt(&registration_receipt_path(&data_dir))?.is_some()
        || load_cursor_recovery_module_marker(&cursor_recovery_module_marker_path(&data_dir))?
            .is_some()
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-registration-evidence".to_string(),
            detail: "cursor-only inspection requires absent journal, receipt, and recovery marker"
                .to_string(),
        });
    }
    ensure_new_sync_root_is_empty(&identity.sync_root)?;
    let root_fence = CursorRecoveryRootFence::acquire(&identity.sync_root)?;
    if query_registered_sync_root(&identity.sync_root)?.is_some() {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-registration-present".to_string(),
            detail: "cursor-only inspection requires an unregistered CFAPI root".to_string(),
        });
    }
    let sync_root_file_id = root_fence.file_id;
    let outbox = dfs::inspect_mounted_sifs_mutation_outbox_for_cursor_recovery(&data_dir).map_err(
        |error| CloudFilesModuleError {
            code: "cursor-recovery-outbox".to_string(),
            detail: error.to_string(),
        },
    )?;
    if !outbox.inspection().outbox_present {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-outbox-absent".to_string(),
            detail:
                "nonempty namespace cursors require the canonical mounted-mutation outbox evidence"
                    .to_string(),
        });
    }
    let queue = inspect_cursor_only_local_change_queue(
        &data_dir,
        &identity.sync_root.to_string_lossy(),
        &identity.root_path,
    )
    .map_err(|error| CloudFilesModuleError {
        code: error.code.to_string(),
        detail: error.detail,
    })?;
    if !queue.inspection().scope_present {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-queue-absent".to_string(),
            detail: "nonempty namespace cursors require the canonical local-change queue scope"
                .to_string(),
        });
    }
    let reconciliation = inspect_cursor_only_reconciliation_state(
        &data_dir,
        &identity.sync_root.to_string_lossy(),
        &identity.root_path,
    )
    .map_err(reconciliation_recovery_error)?;
    validate_cursor_recovery_root(&identity, sync_root_file_id, &root_fence)?;

    let raw_state_fingerprint = cursor_recovery_raw_fingerprint(
        &identity,
        sync_root_file_id,
        &registration_lock_bytes,
        &module_topology,
        &queue.inspection().raw_files,
        &reconciliation.inspection().raw_files,
        &outbox.inspection().raw_files,
    )?;
    let recovery_token = cursor_recovery_token(&raw_state_fingerprint);
    Ok(CursorRecoveryRawInspection {
        identity,
        data_dir,
        sync_root_file_id,
        raw_state_fingerprint,
        recovery_token,
        registration_lock_bytes,
        module_topology,
        queue_raw_files: queue.inspection().raw_files.clone(),
        reconciliation_raw_files: reconciliation.inspection().raw_files.clone(),
        outbox_raw_files: outbox.inspection().raw_files.clone(),
        reconciliation,
        root_fence,
        _registration_lease: registration_lease,
        _queue: queue,
        _outbox: outbox,
    })
}

fn inspect_cursor_recovery_module_topology(
    data_dir: &SinomoDataDir,
    phase: Option<CursorRecoveryModulePhase>,
) -> Result<Vec<String>, CloudFilesModuleError> {
    let runtime = data_dir.module_runtime_dir();
    let module_dir = runtime.join(SINOMO_DFS_CLOUD_FILES_MODULE_ID);
    for (label, path) in [
        ("module runtime", runtime.as_path()),
        ("module", module_dir.as_path()),
    ] {
        let metadata = fs::symlink_metadata(path).map_err(|source| CloudFilesModuleError {
            code: "cursor-recovery-module-topology".to_string(),
            detail: format!("inspect {label} path {}: {source}", path.display()),
        })?;
        if !metadata.file_type().is_dir()
            || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-module-topology".to_string(),
                detail: format!("{label} path {} is not a plain directory", path.display()),
            });
        }
    }
    let mut observed = Vec::new();
    for entry in fs::read_dir(&module_dir).map_err(|source| CloudFilesModuleError {
        code: "cursor-recovery-module-topology".to_string(),
        detail: format!(
            "list Cloud Files module directory {}: {source}",
            module_dir.display()
        ),
    })? {
        let entry = entry.map_err(|source| CloudFilesModuleError {
            code: "cursor-recovery-module-topology".to_string(),
            detail: format!("read Cloud Files module directory entry: {source}"),
        })?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| CloudFilesModuleError {
                code: "cursor-recovery-module-topology".to_string(),
                detail: "Cloud Files module directory contains a non-UTF-8 entry".to_string(),
            })?;
        let metadata =
            fs::symlink_metadata(entry.path()).map_err(|source| CloudFilesModuleError {
                code: "cursor-recovery-module-topology".to_string(),
                detail: format!("inspect Cloud Files module entry `{name}`: {source}"),
            })?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-module-topology".to_string(),
                detail: format!("Cloud Files module entry `{name}` is reparse evidence"),
            });
        }
        let valid = match name.as_str() {
            CLOUD_FILES_REGISTRATION_LOCK_FILE => {
                metadata.file_type().is_file() && metadata.len() == 0
            }
            "local-changes" | "namespace-reconciliation" => metadata.file_type().is_dir(),
            CLOUD_FILES_CURSOR_RECOVERY_MARKER_FILE if phase.is_some() => {
                metadata.file_type().is_file()
            }
            name if name
                == cursor_recovery_module_marker_temp_path(data_dir)
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default()
                && phase.is_some() =>
            {
                metadata.file_type().is_file()
            }
            name if name
                == registration_journal_temp_path(data_dir)
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default()
                && phase.is_some_and(|value| {
                    matches!(
                        value,
                        CursorRecoveryModulePhase::Validated
                            | CursorRecoveryModulePhase::StateOwned
                            | CursorRecoveryModulePhase::ReconcileArmed
                            | CursorRecoveryModulePhase::CompletionAuthorized
                    )
                }) =>
            {
                metadata.file_type().is_file()
            }
            name if name
                == registration_receipt_temp_path(data_dir)
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or_default()
                && phase == Some(CursorRecoveryModulePhase::ReconcileArmed) =>
            {
                metadata.file_type().is_file()
            }
            CLOUD_FILES_REGISTRATION_JOURNAL_FILE
                if phase.is_some_and(|value| {
                    matches!(
                        value,
                        CursorRecoveryModulePhase::Validated
                            | CursorRecoveryModulePhase::StateOwned
                            | CursorRecoveryModulePhase::ReconcileArmed
                            | CursorRecoveryModulePhase::Ready
                            | CursorRecoveryModulePhase::NamespaceFinalized
                            | CursorRecoveryModulePhase::CompletionAuthorized
                    )
                }) =>
            {
                metadata.file_type().is_file()
            }
            CLOUD_FILES_REGISTRATION_RECEIPT_FILE
                if phase.is_some_and(|value| {
                    matches!(
                        value,
                        CursorRecoveryModulePhase::ReconcileArmed
                            | CursorRecoveryModulePhase::Ready
                            | CursorRecoveryModulePhase::NamespaceFinalized
                            | CursorRecoveryModulePhase::CompletionAuthorized
                    )
                }) =>
            {
                metadata.file_type().is_file()
            }
            _ => false,
        };
        if !valid {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-module-topology".to_string(),
                detail: format!("unexpected Cloud Files module recovery entry `{name}`"),
            });
        }
        observed.push(name);
    }
    observed.sort();
    let mut expected = vec![
        "local-changes".to_string(),
        "namespace-reconciliation".to_string(),
        CLOUD_FILES_REGISTRATION_LOCK_FILE.to_string(),
    ];
    if let Some(value) = phase {
        expected.push(CLOUD_FILES_CURSOR_RECOVERY_MARKER_FILE.to_string());
        if matches!(
            value,
            CursorRecoveryModulePhase::StateOwned
                | CursorRecoveryModulePhase::ReconcileArmed
                | CursorRecoveryModulePhase::Ready
                | CursorRecoveryModulePhase::NamespaceFinalized
                | CursorRecoveryModulePhase::CompletionAuthorized
        ) {
            expected.push(CLOUD_FILES_REGISTRATION_JOURNAL_FILE.to_string());
        }
        if matches!(
            value,
            CursorRecoveryModulePhase::Ready
                | CursorRecoveryModulePhase::NamespaceFinalized
                | CursorRecoveryModulePhase::CompletionAuthorized
        ) {
            expected.push(CLOUD_FILES_REGISTRATION_RECEIPT_FILE.to_string());
        }
        expected.sort();
    }
    let authorized_with_exact_marker_temp = phase.is_some()
        && observed == {
            let mut value = expected.clone();
            value.push(
                cursor_recovery_module_marker_temp_path(data_dir)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_string(),
            );
            value.sort();
            value
        };
    let with_exact_journal_temp = phase.is_some_and(|value| {
        matches!(
            value,
            CursorRecoveryModulePhase::Validated
                | CursorRecoveryModulePhase::StateOwned
                | CursorRecoveryModulePhase::ReconcileArmed
                | CursorRecoveryModulePhase::CompletionAuthorized
        )
    }) && observed == {
        let mut value = expected.clone();
        value.push(
            registration_journal_temp_path(data_dir)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string(),
        );
        value.sort();
        value
    };
    let with_exact_receipt_temp = phase == Some(CursorRecoveryModulePhase::ReconcileArmed)
        && observed == {
            let mut value = expected.clone();
            value.push(
                registration_receipt_temp_path(data_dir)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_string(),
            );
            value.sort();
            value
        };
    let validated_with_journal = phase == Some(CursorRecoveryModulePhase::Validated)
        && observed == {
            let mut value = expected.clone();
            value.push(CLOUD_FILES_REGISTRATION_JOURNAL_FILE.to_string());
            value.sort();
            value
        };
    let reconcile_armed_with_receipt = phase == Some(CursorRecoveryModulePhase::ReconcileArmed)
        && observed == {
            let mut value = expected.clone();
            value.push(CLOUD_FILES_REGISTRATION_RECEIPT_FILE.to_string());
            value.sort();
            value
        };
    if observed != expected
        && !authorized_with_exact_marker_temp
        && !with_exact_journal_temp
        && !with_exact_receipt_temp
        && !validated_with_journal
        && !reconcile_armed_with_receipt
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-module-topology".to_string(),
            detail: format!(
                "Cloud Files module topology {:?} is not canonical",
                observed
            ),
        });
    }
    Ok(observed)
}

fn validate_cursor_recovery_identity(
    identity: &CloudFilesCursorOnlyRecoveryIdentity,
) -> Result<(), CloudFilesModuleError> {
    if identity.schema_version != CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION
        || identity.provider_name != crate::protocol::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME
        || identity.provider_version != env!("CARGO_PKG_VERSION")
        || identity.disabled_config_fingerprint.trim().is_empty()
        || identity.provider_provenance_fingerprint.trim().is_empty()
        || identity.enabled
        || identity.read_only
        || identity.allow_other
        || !identity.sync_local_changes
        || !identity.connect
        || identity.prepopulate_depth != 0
        || identity.max_run_seconds.is_some()
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-identity-invalid".to_string(),
            detail: "cursor recovery identity is incomplete or not the exact disabled writable configuration".to_string(),
        });
    }
    if identity.root_path != normalize_sifs_root(&identity.root_path)? {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-root-not-canonical".to_string(),
            detail: "cursor recovery root path is not canonical".to_string(),
        });
    }
    Ok(())
}

fn resolve_cursor_recovery_data_dir(
    identity: &CloudFilesCursorOnlyRecoveryIdentity,
) -> Result<SinomoDataDir, CloudFilesModuleError> {
    let data_dir = SinomoDataDir::new(identity.data_dir_root.clone());
    if data_dir.root() != identity.data_dir_root {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-data-dir".to_string(),
            detail: "cursor recovery data directory identity changed".to_string(),
        });
    }
    Ok(data_dir)
}

fn cursor_recovery_mount_request(marker: &CursorRecoveryModuleMarker) -> CloudFilesMountRequest {
    let identity = &marker.identity;
    CloudFilesMountRequest {
        sync_root: identity.sync_root.clone(),
        root_path: identity.root_path.clone(),
        data_dir_root: Some(identity.data_dir_root.clone()),
        read_only: identity.read_only,
        sync_local_changes: identity.sync_local_changes,
        provider_name: identity.provider_name.clone(),
        provider_version: identity.provider_version.clone(),
        prepopulate_depth: identity.prepopulate_depth,
        connect: true,
        max_run_seconds: identity.max_run_seconds,
        cancellation_event_handle: None,
        cursor_recovery: Some(CloudFilesCursorRecoveryContext {
            recovery_id: marker.recovery_id.clone(),
            recovery_token: marker.recovery_token.clone(),
            recovery_cycle: marker.recovery_cycle,
            native_successor_fingerprint: None,
        }),
        provider_replacement: None,
    }
}

fn cursor_recovery_native_successor_fingerprint(
    marker: &CursorRecoveryModuleMarker,
) -> Result<String, CloudFilesModuleError> {
    let mut committed = marker.clone();
    committed.phase = CursorRecoveryModulePhase::PreservedStateOwned;
    committed.namespace_resume = marker.cycle_authorized_namespace_resume;
    committed.cycle_authorization_fingerprint = None;
    let bytes = sinomo_runtime::module_api::encode_sobj(&committed).map_err(|source| {
        CloudFilesModuleError {
            code: "cursor-recovery-successor-encode".to_string(),
            detail: format!("encode PreservedStateOwned successor: {source}"),
        }
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.cursor-recovery.native-successor.v1\0");
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(&bytes);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn cursor_recovery_module_marker_fingerprint(
    marker: &CursorRecoveryModuleMarker,
) -> Result<String, CloudFilesModuleError> {
    let bytes = encode_sobj(marker).map_err(|source| CloudFilesModuleError {
        code: "cursor-recovery-marker-encode".to_string(),
        detail: format!("encode current cursor recovery marker: {source}"),
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.cursor-recovery.module-marker.v1\0");
    hash_cursor_recovery_field(&mut hasher, &bytes);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn cursor_recovery_completed_registration_artifacts_fingerprint(
    marker: &CursorRecoveryModuleMarker,
) -> Result<String, CloudFilesModuleError> {
    let (_, mut ready, receipt) = cursor_recovery_expected_registration_artifacts(marker)?;
    ready.phase = RegistrationPhase::Ready;
    ready.clear_cursor_recovery();
    let journal_bytes = encode_sobj(&ready).map_err(|source| CloudFilesModuleError {
        code: "cursor-recovery-completion-journal-encode".to_string(),
        detail: format!("encode expected ordinary Ready journal: {source}"),
    })?;
    let receipt_bytes = encode_sobj(&receipt).map_err(|source| CloudFilesModuleError {
        code: "cursor-recovery-completion-receipt-encode".to_string(),
        detail: format!("encode expected Ready receipt: {source}"),
    })?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"sinomo.dfs.cloud-files.cursor-recovery.completed-registration.v1\0");
    for (label, bytes) in [
        (b"receipt".as_slice(), receipt_bytes.as_slice()),
        (b"journal".as_slice(), journal_bytes.as_slice()),
    ] {
        hash_cursor_recovery_field(&mut hasher, label);
        hash_cursor_recovery_field(&mut hasher, bytes);
    }
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn cursor_recovery_active_observed_event(
    marker: &CursorRecoveryModuleMarker,
) -> Result<CloudFilesModuleEvent, CloudFilesModuleError> {
    Ok(CloudFilesModuleEvent::CursorRecoveryActiveObserved(
        crate::protocol::CloudFilesCursorRecoveryActiveObservedRequest {
            schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            recovery_id: marker.recovery_id.clone(),
            recovery_token: marker.recovery_token.clone(),
            sync_root: marker.identity.sync_root.clone(),
            root_path: marker.identity.root_path.clone(),
            sync_root_file_id: marker.sync_root_file_id,
            recovery_cycle: marker.recovery_cycle,
            module_marker_fingerprint: cursor_recovery_module_marker_fingerprint(marker)?,
            completed_registration_artifacts_fingerprint:
                cursor_recovery_completed_registration_artifacts_fingerprint(marker)?,
        },
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorRecoveryTerminalHandoffStep {
    AcknowledgeNamespaceFinalized,
    PersistCompletionAuthorized,
    AcknowledgeCompletionAuthority,
    NormalizeRegistration,
    AcknowledgeNormalization,
    ClearCompletionAuthorized,
    AcknowledgeAuthorityRelease,
    CompleteWorker,
    ReleasePopulation,
}

/// Runs the forward-only terminal handoff while the writable worker remains
/// parked. Keeping this ordering in one production-used seam makes it
/// impossible for restart handling and the live path to disagree about which
/// durable cut precedes the next host acknowledgement.
fn run_cursor_recovery_terminal_handoff_with<FStep, FCancelled>(
    marker: &mut CursorRecoveryModuleMarker,
    mut step: FStep,
    mut cancelled: FCancelled,
) -> Result<bool, CloudFilesModuleError>
where
    FStep: FnMut(
        CursorRecoveryTerminalHandoffStep,
        &mut CursorRecoveryModuleMarker,
    ) -> Result<(), CloudFilesModuleError>,
    FCancelled: FnMut() -> bool,
{
    if marker.phase == CursorRecoveryModulePhase::NamespaceFinalized {
        step(
            CursorRecoveryTerminalHandoffStep::AcknowledgeNamespaceFinalized,
            marker,
        )?;
        step(
            CursorRecoveryTerminalHandoffStep::PersistCompletionAuthorized,
            marker,
        )?;
    }
    if marker.phase != CursorRecoveryModulePhase::CompletionAuthorized {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-completion-phase".to_string(),
            detail: "ActiveParked recovery lacks exact CompletionAuthorized authority".to_string(),
        });
    }
    for terminal_step in [
        CursorRecoveryTerminalHandoffStep::AcknowledgeCompletionAuthority,
        CursorRecoveryTerminalHandoffStep::NormalizeRegistration,
        CursorRecoveryTerminalHandoffStep::AcknowledgeNormalization,
        CursorRecoveryTerminalHandoffStep::ClearCompletionAuthorized,
        CursorRecoveryTerminalHandoffStep::AcknowledgeAuthorityRelease,
    ] {
        step(terminal_step, marker)?;
    }
    // The final acknowledged host decision may have observed disabled consent
    // and latched cancellation. Complete/Release are the first operations
    // which unpark ordinary writeback, so they must remain strictly after this
    // cancellation observation.
    if cancelled() {
        return Ok(false);
    }
    step(CursorRecoveryTerminalHandoffStep::CompleteWorker, marker)?;
    if cancelled() {
        return Ok(false);
    }
    step(CursorRecoveryTerminalHandoffStep::ReleasePopulation, marker)?;
    Ok(true)
}

fn cursor_recovery_cycle_authorization(
    marker: &CursorRecoveryModuleMarker,
) -> Result<Option<String>, CloudFilesModuleError> {
    if marker.recovery_cycle == 0 {
        return Ok(None);
    }
    if marker.phase == CursorRecoveryModulePhase::PreservedStateOwned {
        return cursor_recovery_native_successor_fingerprint(marker).map(Some);
    }
    let observed = marker
        .cycle_authorization_fingerprint
        .clone()
        .ok_or_else(|| CloudFilesModuleError {
            code: "cursor-recovery-cycle-authorization-missing".to_string(),
            detail: "rearmed cursor recovery lost its PreservedStateOwned commitment".to_string(),
        })?;
    let expected = cursor_recovery_native_successor_fingerprint(marker)?;
    if observed != expected {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-cycle-authorization-mismatch".to_string(),
            detail: "rearmed cursor recovery changed from its exact PreservedStateOwned commitment"
                .to_string(),
        });
    }
    Ok(Some(observed))
}

fn validate_cursor_recovery_root(
    identity: &CloudFilesCursorOnlyRecoveryIdentity,
    expected_file_id: u64,
    root_fence: &CursorRecoveryRootFence,
) -> Result<(), CloudFilesModuleError> {
    ensure_new_sync_root_is_empty(&identity.sync_root)?;
    if root_fence.file_id != expected_file_id
        || sync_root_file_index(&identity.sync_root)? != expected_file_id
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-root-drift".to_string(),
            detail: "cursor recovery sync root physical identity changed".to_string(),
        });
    }
    if query_registered_sync_root(&identity.sync_root)?.is_some() {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-registration-present".to_string(),
            detail: "cursor recovery requires absent CFAPI registration".to_string(),
        });
    }
    Ok(())
}

fn validate_owned_cursor_recovery_root(
    identity: &CloudFilesCursorOnlyRecoveryIdentity,
    expected_file_id: u64,
) -> Result<(), CloudFilesModuleError> {
    ensure_owned_sync_root_is_plain_directory(&identity.sync_root)?;
    if sync_root_file_index(&identity.sync_root)? != expected_file_id {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-root-drift".to_string(),
            detail: "owned cursor recovery sync root physical identity changed".to_string(),
        });
    }
    Ok(())
}

fn cursor_recovery_module_marker_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .module_runtime_dir()
        .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(CLOUD_FILES_CURSOR_RECOVERY_MARKER_FILE)
}

fn cursor_recovery_artifact_temp_path(path: &Path, label: &str) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("cursor-recovery");
    path.with_file_name(format!(".{name}.{label}"))
}

fn registration_journal_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    cursor_recovery_artifact_temp_path(
        &registration_journal_path(data_dir),
        "registration-journal-next",
    )
}

fn registration_receipt_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    cursor_recovery_artifact_temp_path(
        &registration_receipt_path(data_dir),
        "registration-receipt-next",
    )
}

fn cursor_recovery_module_marker_temp_path(data_dir: &SinomoDataDir) -> PathBuf {
    cursor_recovery_artifact_temp_path(
        &cursor_recovery_module_marker_path(data_dir),
        CLOUD_FILES_CURSOR_RECOVERY_MODULE_TEMP_LABEL,
    )
}

fn load_cursor_recovery_module_marker(
    path: &Path,
) -> Result<Option<CursorRecoveryModuleMarker>, CloudFilesModuleError> {
    let Some(bytes) = read_bounded_plain_registration_file(
        path,
        CLOUD_FILES_CURSOR_RECOVERY_MARKER_MAX_BYTES,
        "Cloud Files cursor recovery marker",
    )?
    else {
        return Ok(None);
    };
    let marker: CursorRecoveryModuleMarker = decode_canonical_registration_artifact(
        path,
        &bytes,
        "cursor-recovery-marker",
        "Cloud Files cursor recovery marker",
    )?;
    validate_cursor_recovery_marker(&marker)?;
    Ok(Some(marker))
}

fn inspect_cursor_recovery_module_marker_snapshot(
    data_dir: &SinomoDataDir,
) -> Result<RegistrationArtifactSnapshot<CursorRecoveryModuleMarker>, CloudFilesModuleError> {
    let path = cursor_recovery_module_marker_path(data_dir);
    let temporary = cursor_recovery_module_marker_temp_path(data_dir);
    let snapshot = inspect_registration_artifact_snapshot::<CursorRecoveryModuleMarker>(
        &path,
        &temporary,
        CLOUD_FILES_CURSOR_RECOVERY_MARKER_MAX_BYTES,
        "cursor-recovery-marker",
        "Cloud Files cursor recovery marker",
    )?;
    if let Some(marker) = snapshot.current.as_ref() {
        validate_cursor_recovery_marker(marker)?;
    }
    if let Some(marker) = snapshot.temporary.as_ref() {
        validate_cursor_recovery_marker(marker)?;
    }
    Ok(snapshot)
}

fn cursor_recovery_marker_with_phase(
    marker: &CursorRecoveryModuleMarker,
    phase: CursorRecoveryModulePhase,
) -> CursorRecoveryModuleMarker {
    let mut expected = marker.clone();
    match phase {
        CursorRecoveryModulePhase::NamespaceFinalized
        | CursorRecoveryModulePhase::CompletionAuthorized => {
            expected.namespace_resume = CursorRecoveryNamespaceResume::Finalized;
        }
        _ => {}
    }
    expected.phase = phase;
    expected
}

fn cursor_recovery_phase_successor(
    phase: CursorRecoveryModulePhase,
) -> Option<CursorRecoveryModulePhase> {
    match phase {
        CursorRecoveryModulePhase::Authorized => Some(CursorRecoveryModulePhase::Validated),
        CursorRecoveryModulePhase::Validated => Some(CursorRecoveryModulePhase::StateOwned),
        CursorRecoveryModulePhase::StateOwned => Some(CursorRecoveryModulePhase::ReconcileArmed),
        CursorRecoveryModulePhase::ReconcileArmed => Some(CursorRecoveryModulePhase::Ready),
        CursorRecoveryModulePhase::Ready => Some(CursorRecoveryModulePhase::NamespaceFinalized),
        CursorRecoveryModulePhase::NamespaceFinalized => {
            Some(CursorRecoveryModulePhase::CompletionAuthorized)
        }
        CursorRecoveryModulePhase::CompletionAuthorized
        | CursorRecoveryModulePhase::PreserveAuthorized
        | CursorRecoveryModulePhase::PreservedStateOwned => None,
    }
}

fn cursor_recovery_preserve_authorized_successor(
    marker: &CursorRecoveryModuleMarker,
) -> Result<CursorRecoveryModuleMarker, CloudFilesModuleError> {
    if matches!(
        marker.phase,
        CursorRecoveryModulePhase::Authorized
            | CursorRecoveryModulePhase::Validated
            | CursorRecoveryModulePhase::PreserveAuthorized
    ) {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-preserve-predecessor".to_string(),
            detail: "cursor recovery has no exact state-owned preserve predecessor".to_string(),
        });
    }
    let mut expected = marker.clone();
    expected.predecessor_cycle_fingerprint = cursor_recovery_cycle_authorization(marker)?;
    expected.cycle_authorization_fingerprint = None;
    expected.recovery_cycle =
        expected
            .recovery_cycle
            .checked_add(1)
            .ok_or_else(|| CloudFilesModuleError {
                code: "cursor-recovery-cycle-overflow".to_string(),
                detail: "cursor recovery preserve cycle overflowed".to_string(),
            })?;
    expected.cycle_authorized_namespace_resume = marker.namespace_resume;
    expected.phase = CursorRecoveryModulePhase::PreserveAuthorized;
    Ok(expected)
}

fn cursor_recovery_preserved_state_owned_successor(
    marker: &CursorRecoveryModuleMarker,
) -> Result<CursorRecoveryModuleMarker, CloudFilesModuleError> {
    if marker.phase != CursorRecoveryModulePhase::PreserveAuthorized {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-preserve-successor".to_string(),
            detail: "PreservedStateOwned requires PreserveAuthorized".to_string(),
        });
    }
    let mut expected = marker.clone();
    expected.phase = CursorRecoveryModulePhase::PreservedStateOwned;
    Ok(expected)
}

fn cursor_recovery_rearmed_successor(
    marker: &CursorRecoveryModuleMarker,
) -> Result<CursorRecoveryModuleMarker, CloudFilesModuleError> {
    if marker.phase != CursorRecoveryModulePhase::PreservedStateOwned {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-rearm-predecessor".to_string(),
            detail: "rearm requires PreservedStateOwned".to_string(),
        });
    }
    let mut expected = marker.clone();
    expected.namespace_resume = marker.cycle_authorized_namespace_resume;
    expected.cycle_authorization_fingerprint =
        Some(cursor_recovery_native_successor_fingerprint(marker)?);
    expected.phase = CursorRecoveryModulePhase::ReconcileArmed;
    Ok(expected)
}

fn validate_cursor_recovery_apply_authority(
    marker: &CursorRecoveryModuleMarker,
    request: &CloudFilesCursorOnlyRecoveryApplyRequest,
) -> Result<(), CloudFilesModuleError> {
    if marker.schema_version != request.identity.schema_version
        || marker.identity != request.identity
        || marker.recovery_id != request.recovery_id
        || marker.recovery_token != request.expected_token
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-marker-mismatch".to_string(),
            detail: "a durable cursor recovery artifact belongs to another token, identity, or recovery ID"
                .to_string(),
        });
    }
    Ok(())
}

fn validate_cursor_recovery_mount_authority(
    marker: &CursorRecoveryModuleMarker,
    request: &CloudFilesMountRequest,
    data_dir: &SinomoDataDir,
) -> Result<(), CloudFilesModuleError> {
    if !matches!(
        marker.phase,
        CursorRecoveryModulePhase::ReconcileArmed
            | CursorRecoveryModulePhase::Ready
            | CursorRecoveryModulePhase::NamespaceFinalized
            | CursorRecoveryModulePhase::CompletionAuthorized
            | CursorRecoveryModulePhase::PreservedStateOwned
    ) || marker.identity.sync_root != request.sync_root
        || marker.identity.root_path != request.root_path
        || marker.identity.data_dir_root != data_dir.root()
        || marker.identity.provider_name != request.provider_name
        || marker.identity.provider_version != request.provider_version
        || marker.identity.read_only != request.read_only
        || marker.identity.sync_local_changes != request.sync_local_changes
        || marker.identity.prepopulate_depth != request.prepopulate_depth
        || marker.identity.max_run_seconds != request.max_run_seconds
        || request.cursor_recovery.as_ref()
            != Some(&CloudFilesCursorRecoveryContext {
                recovery_id: marker.recovery_id.clone(),
                recovery_token: marker.recovery_token.clone(),
                recovery_cycle: marker.recovery_cycle,
                native_successor_fingerprint: cursor_recovery_cycle_authorization(marker)?,
            })
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-mount-blocked".to_string(),
            detail:
                "ordinary mount is blocked by a nonterminal or mismatched cursor recovery artifact"
                    .to_string(),
        });
    }
    Ok(())
}

fn validate_cursor_recovery_preserve_authority(
    marker: &CursorRecoveryModuleMarker,
    request: &CloudFilesUnregisterRequest,
    data_dir: &SinomoDataDir,
    registration_lock_bytes: Option<&[u8]>,
) -> Result<CursorRecoveryPreserveContextRelation, CloudFilesModuleError> {
    if marker.identity.sync_root != request.sync_root
        || marker.identity.root_path != request.root_path
        || marker.identity.data_dir_root != data_dir.root()
        || marker.identity.provider_name != request.provider_name
        || registration_lock_bytes != Some(marker.registration_lock_bytes.as_slice())
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-unregister-mismatch".to_string(),
            detail: "state-preserving unregister identity or existing registration lock does not match the durable cursor recovery"
                .to_string(),
        });
    }
    let context = request
        .cursor_recovery
        .as_ref()
        .ok_or_else(|| CloudFilesModuleError {
            code: "cursor-recovery-unregister-context-missing".to_string(),
            detail: "state-preserving unregister omitted its exact cursor-recovery context"
                .to_string(),
        })?;
    if context.recovery_id != marker.recovery_id || context.recovery_token != marker.recovery_token
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-unregister-context-mismatch".to_string(),
            detail: "state-preserving unregister token does not match cursor recovery".to_string(),
        });
    }
    let predecessor_retry = matches!(
        marker.phase,
        CursorRecoveryModulePhase::PreserveAuthorized
            | CursorRecoveryModulePhase::PreservedStateOwned
    ) && context
        .recovery_cycle
        .checked_add(1)
        .is_some_and(|cycle| cycle == marker.recovery_cycle)
        && context.native_successor_fingerprint == marker.predecessor_cycle_fingerprint;
    if predecessor_retry {
        return Ok(CursorRecoveryPreserveContextRelation::PredecessorRetry);
    }
    let current_authorization = cursor_recovery_cycle_authorization(marker)?;
    if context.recovery_cycle == marker.recovery_cycle
        && context.native_successor_fingerprint == current_authorization
        && marker.phase == CursorRecoveryModulePhase::PreservedStateOwned
    {
        // Rapid disable after the host rearmed PSO but before native PSO->RA.
        return Ok(CursorRecoveryPreserveContextRelation::Current);
    }
    if context.recovery_cycle == marker.recovery_cycle
        && context.native_successor_fingerprint == current_authorization
        && !matches!(marker.phase, CursorRecoveryModulePhase::PreserveAuthorized)
    {
        return Ok(CursorRecoveryPreserveContextRelation::Current);
    }
    Err(CloudFilesModuleError {
        code: "cursor-recovery-unregister-context-mismatch".to_string(),
        detail: "state-preserving unregister context is neither the exact current cycle nor a durable preserve retry"
            .to_string(),
    })
}

fn promote_cursor_recovery_artifact_temp(
    temporary: &Path,
    path: &Path,
    code: &str,
    label: &str,
) -> Result<(), CloudFilesModuleError> {
    let temporary_wide = wide_path(temporary);
    let path_wide = wide_path(path);
    unsafe {
        MoveFileExW(
            PCWSTR(temporary_wide.as_ptr()),
            PCWSTR(path_wide.as_ptr()),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    }
    .map_err(|source| CloudFilesModuleError {
        code: format!("{code}-commit"),
        detail: format!("promote exact {label} {}: {source}", path.display()),
    })
}

fn cursor_recovery_expected_registration_artifacts(
    marker: &CursorRecoveryModuleMarker,
) -> Result<
    (
        RegistrationJournal,
        RegistrationJournal,
        CloudFilesRegistrationReceipt,
    ),
    CloudFilesModuleError,
> {
    let request = cursor_recovery_mount_request(marker);
    let state_owned = RegistrationJournal::cursor_recovery_state_owned(
        &request,
        CLOUD_FILES_PROVIDER_ID_TEXT,
        marker.registration_nonce.clone(),
        marker.sync_root_file_id,
        marker.recovery_id.clone(),
        marker.recovery_token.clone(),
        marker.recovery_cycle,
    )?;
    let mut ready = state_owned.clone();
    ready.phase = RegistrationPhase::Ready;
    let receipt = CloudFilesRegistrationReceipt {
        interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
        registration_identity_interface_version: None,
        sync_root: request.sync_root,
        root_path: request.root_path,
        provider_name: request.provider_name,
    };
    Ok((state_owned, ready, receipt))
}

fn cursor_recovery_predecessor_registration_artifacts(
    marker: &CursorRecoveryModuleMarker,
) -> Result<
    (
        RegistrationJournal,
        RegistrationJournal,
        CloudFilesRegistrationReceipt,
    ),
    CloudFilesModuleError,
> {
    let mut predecessor = marker.clone();
    predecessor.recovery_cycle =
        predecessor
            .recovery_cycle
            .checked_sub(1)
            .ok_or_else(|| CloudFilesModuleError {
                code: "cursor-recovery-preserve-cycle".to_string(),
                detail: "preserve successor has no predecessor cycle".to_string(),
            })?;
    cursor_recovery_expected_registration_artifacts(&predecessor)
}

fn cursor_recovery_receipt_cut_matches(
    journal: &RegistrationJournal,
    receipt: Option<&CloudFilesRegistrationReceipt>,
    expected_receipt: &CloudFilesRegistrationReceipt,
) -> bool {
    match journal.phase {
        RegistrationPhase::Ready => receipt == Some(expected_receipt),
        RegistrationPhase::StateOwned => receipt.is_none() || receipt == Some(expected_receipt),
        RegistrationPhase::Prepared | RegistrationPhase::Registered => false,
    }
}

/// Classify and advance only the finite, causally ordered registration
/// artifact cuts produced by cursor recovery. This runs under the pre-existing
/// registration lease before phase topology admission. It never deletes or
/// normalizes an unknown artifact.
fn recover_cursor_recovery_registration_snapshot<F>(
    data_dir: &SinomoDataDir,
    allow_initial_authorized_temp: bool,
    validate_authority: F,
) -> Result<Option<CursorRecoveryModuleMarker>, CloudFilesModuleError>
where
    F: Fn(&CursorRecoveryModuleMarker) -> Result<(), CloudFilesModuleError>,
{
    let marker_path = cursor_recovery_module_marker_path(data_dir);
    let marker_temp_path = cursor_recovery_module_marker_temp_path(data_dir);
    let journal_path = registration_journal_path(data_dir);
    let journal_temp_path = registration_journal_temp_path(data_dir);
    let receipt_path = registration_receipt_path(data_dir);
    let receipt_temp_path = registration_receipt_temp_path(data_dir);

    for _ in 0..8 {
        let marker_snapshot = inspect_cursor_recovery_module_marker_snapshot(data_dir)?;
        let journal_snapshot = inspect_registration_artifact_snapshot::<RegistrationJournal>(
            &journal_path,
            &journal_temp_path,
            CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES,
            "registration-journal",
            "Cloud Files registration journal",
        )?;
        let receipt_snapshot =
            inspect_registration_artifact_snapshot::<CloudFilesRegistrationReceipt>(
                &receipt_path,
                &receipt_temp_path,
                CLOUD_FILES_REGISTRATION_RECEIPT_MAX_BYTES,
                "registration-receipt",
                "Cloud Files registration receipt",
            )?;
        for journal in [
            journal_snapshot.current.as_ref(),
            journal_snapshot.temporary.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
        }

        let Some(current) = marker_snapshot.current.as_ref() else {
            let Some(temporary) = marker_snapshot.temporary.as_ref() else {
                if journal_snapshot
                    .current
                    .as_ref()
                    .is_some_and(|journal| journal.cursor_recovery.is_some())
                    || journal_snapshot.temporary.is_some()
                    || receipt_snapshot.temporary.is_some()
                {
                    return Err(CloudFilesModuleError {
                        code: "cursor-recovery-artifact-orphan".to_string(),
                        detail: "cursor-provenance journal or temporary evidence exists without an exact cursor recovery marker"
                            .to_string(),
                    });
                }
                // Ordinary nonce-journal/receipt recovery is owned by the
                // registration lifecycle below. It is not cursor evidence and
                // must remain admissible when the optional provenance is None.
                return Ok(None);
            };
            validate_authority(temporary)?;
            if !allow_initial_authorized_temp
                || temporary.phase != CursorRecoveryModulePhase::Authorized
                || journal_snapshot.current.is_some()
                || journal_snapshot.temporary.is_some()
                || receipt_snapshot.current.is_some()
                || receipt_snapshot.temporary.is_some()
            {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-artifact-cut-invalid".to_string(),
                    detail: "final-absent cursor recovery evidence is not the exact request-bound Authorized marker cut"
                        .to_string(),
                });
            }
            promote_cursor_recovery_artifact_temp(
                &marker_temp_path,
                &marker_path,
                "cursor-recovery-marker",
                "Cloud Files cursor recovery marker",
            )?;
            continue;
        };
        validate_authority(current)?;
        let (state_owned, ready, receipt) =
            cursor_recovery_expected_registration_artifacts(current)?;
        let mut ordinary_ready = ready.clone();
        ordinary_ready.clear_cursor_recovery();

        if let Some(temporary) = marker_snapshot.temporary.as_ref() {
            validate_authority(temporary)?;
            if matches!(
                temporary.phase,
                CursorRecoveryModulePhase::PreserveAuthorized
            ) && temporary == &cursor_recovery_preserve_authorized_successor(current)?
            {
                let (_, predecessor_ready, predecessor_receipt) =
                    cursor_recovery_expected_registration_artifacts(current)?;
                let predecessor_state_owned = {
                    let mut journal = predecessor_ready.clone();
                    journal.phase = RegistrationPhase::StateOwned;
                    journal
                };
                let predecessor = journal_snapshot.current.as_ref().is_some_and(|journal| {
                    (journal == &predecessor_ready || journal == &predecessor_state_owned)
                        && cursor_recovery_receipt_cut_matches(
                            journal,
                            receipt_snapshot.current.as_ref(),
                            &predecessor_receipt,
                        )
                });
                if !predecessor
                    || journal_snapshot.temporary.is_some()
                    || receipt_snapshot.temporary.is_some()
                {
                    return Err(CloudFilesModuleError {
                        code: "cursor-recovery-preserve-authorized-cut".to_string(),
                        detail: "PreserveAuthorized temp lacks its exact predecessor artifacts"
                            .to_string(),
                    });
                }
                promote_cursor_recovery_artifact_temp(
                    &marker_temp_path,
                    &marker_path,
                    "cursor-recovery-marker",
                    "Cloud Files PreserveAuthorized marker",
                )?;
                continue;
            }
            if current.phase == CursorRecoveryModulePhase::PreserveAuthorized
                && temporary == &cursor_recovery_preserved_state_owned_successor(current)?
            {
                let (successor_state_owned, _, successor_receipt) =
                    cursor_recovery_expected_registration_artifacts(current)?;
                if journal_snapshot.current.as_ref() != Some(&successor_state_owned)
                    || journal_snapshot.temporary.is_some()
                    || receipt_snapshot
                        .current
                        .as_ref()
                        .is_some_and(|receipt| receipt != &successor_receipt)
                    || receipt_snapshot.temporary.is_some()
                {
                    return Err(CloudFilesModuleError {
                        code: "cursor-recovery-preserved-state-owned-cut".to_string(),
                        detail: "PreservedStateOwned temp lacks its exact successor journal"
                            .to_string(),
                    });
                }
                promote_cursor_recovery_artifact_temp(
                    &marker_temp_path,
                    &marker_path,
                    "cursor-recovery-marker",
                    "Cloud Files PreservedStateOwned marker",
                )?;
                continue;
            }
            if current.phase == CursorRecoveryModulePhase::PreservedStateOwned
                && temporary == &cursor_recovery_rearmed_successor(current)?
            {
                let (state_owned, _, receipt) =
                    cursor_recovery_expected_registration_artifacts(current)?;
                if journal_snapshot.current.as_ref() != Some(&state_owned)
                    || journal_snapshot.temporary.is_some()
                    || receipt_snapshot
                        .current
                        .as_ref()
                        .is_some_and(|value| value != &receipt)
                    || receipt_snapshot.temporary.is_some()
                {
                    return Err(CloudFilesModuleError {
                        code: "cursor-recovery-rearm-cut".to_string(),
                        detail: "rearm temp lacks exact PreservedStateOwned artifacts".to_string(),
                    });
                }
                promote_cursor_recovery_artifact_temp(
                    &marker_temp_path,
                    &marker_path,
                    "cursor-recovery-marker",
                    "Cloud Files rearmed marker",
                )?;
                continue;
            }
            if current.phase == CursorRecoveryModulePhase::ReconcileArmed
                && current.namespace_resume == CursorRecoveryNamespaceResume::ReconcileProgress
            {
                let mut terminal_pending = current.clone();
                terminal_pending.namespace_resume =
                    CursorRecoveryNamespaceResume::TerminalPendingFinalize;
                if temporary == &terminal_pending {
                    if journal_snapshot.current.as_ref() != Some(&state_owned)
                        || journal_snapshot.temporary.is_some()
                        || receipt_snapshot.current.is_some()
                        || receipt_snapshot.temporary.is_some()
                    {
                        return Err(CloudFilesModuleError {
                            code: "cursor-recovery-terminal-successor-artifacts".to_string(),
                            detail: "TerminalPendingFinalize marker temp is not preceded by the exact StateOwned journal with receipt absent"
                                .to_string(),
                        });
                    }
                    let owner_guards =
                        acquire_cursor_recovery_terminal_owner_guards(data_dir, current)?;
                    let terminal = crate::namespace_reconciler::acquire_authorized_cursor_recovery_terminal_successor(
                        data_dir,
                        current.identity.sync_root.display().to_string(),
                        current.identity.root_path.clone(),
                        &current.plan,
                    )
                    .map_err(reconciliation_recovery_error)?
                    .ok_or_else(|| CloudFilesModuleError {
                        code: "cursor-recovery-terminal-successor-invalid".to_string(),
                        detail: "TerminalPendingFinalize marker temp lacks exact terminal owner state"
                            .to_string(),
                    })?;
                    promote_cursor_recovery_artifact_temp(
                        &marker_temp_path,
                        &marker_path,
                        "cursor-recovery-marker",
                        "Cloud Files cursor recovery marker",
                    )?;
                    drop(terminal);
                    drop(owner_guards);
                    continue;
                }
            }
            let Some(next) = cursor_recovery_phase_successor(current.phase) else {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-marker-temp-unexpected".to_string(),
                    detail: "NamespaceFinalized recovery cannot have a successor marker"
                        .to_string(),
                });
            };
            if temporary != &cursor_recovery_marker_with_phase(current, next) {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-marker-temp-mismatch".to_string(),
                    detail: "cursor recovery marker temporary file is not the exact single-phase successor"
                        .to_string(),
                });
            }
            let artifacts_match = match next {
                CursorRecoveryModulePhase::Validated => {
                    journal_snapshot.current.is_none()
                        && journal_snapshot.temporary.is_none()
                        && receipt_snapshot.current.is_none()
                        && receipt_snapshot.temporary.is_none()
                }
                CursorRecoveryModulePhase::StateOwned => {
                    journal_snapshot.current.as_ref() == Some(&state_owned)
                        && journal_snapshot.temporary.is_none()
                        && receipt_snapshot.current.is_none()
                        && receipt_snapshot.temporary.is_none()
                }
                CursorRecoveryModulePhase::ReconcileArmed => {
                    journal_snapshot.current.as_ref() == Some(&state_owned)
                        && journal_snapshot.temporary.is_none()
                        && receipt_snapshot.current.is_none()
                        && receipt_snapshot.temporary.is_none()
                }
                CursorRecoveryModulePhase::Ready
                | CursorRecoveryModulePhase::NamespaceFinalized => {
                    journal_snapshot.current.as_ref() == Some(&ready)
                        && journal_snapshot.temporary.is_none()
                        && receipt_snapshot.current.as_ref() == Some(&receipt)
                        && receipt_snapshot.temporary.is_none()
                }
                CursorRecoveryModulePhase::CompletionAuthorized => {
                    journal_snapshot.current.as_ref() == Some(&ready)
                        && journal_snapshot.temporary.is_none()
                        && receipt_snapshot.current.as_ref() == Some(&receipt)
                        && receipt_snapshot.temporary.is_none()
                }
                CursorRecoveryModulePhase::PreserveAuthorized
                | CursorRecoveryModulePhase::PreservedStateOwned => false,
                CursorRecoveryModulePhase::Authorized => false,
            };
            if !artifacts_match {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-artifact-cut-invalid".to_string(),
                    detail: "cursor recovery marker successor is not preceded by its exact causal journal and receipt artifacts"
                        .to_string(),
                });
            }
            if next == CursorRecoveryModulePhase::ReconcileArmed {
                // The exact inner rewrite must be revalidated under its owner
                // lock before this marker successor is promoted. The Apply
                // state machine performs that check and then replays the same
                // deterministic marker write.
                return Ok(Some(current.clone()));
            }
            promote_cursor_recovery_artifact_temp(
                &marker_temp_path,
                &marker_path,
                "cursor-recovery-marker",
                "Cloud Files cursor recovery marker",
            )?;
            continue;
        }

        if current.phase == CursorRecoveryModulePhase::ReconcileArmed
            && current.namespace_resume == CursorRecoveryNamespaceResume::ReconcileProgress
        {
            if journal_snapshot.current.as_ref() != Some(&state_owned)
                || journal_snapshot.temporary.is_some()
                || receipt_snapshot.current.is_some()
                || receipt_snapshot.temporary.is_some()
            {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-terminal-successor-artifacts".to_string(),
                    detail: "TerminalPendingFinalize synthesis requires the exact StateOwned journal with receipt absent"
                        .to_string(),
                });
            }
            let owner_guards = acquire_cursor_recovery_terminal_owner_guards(data_dir, current)?;
            if let Some(terminal) =
                crate::namespace_reconciler::acquire_authorized_cursor_recovery_terminal_successor(
                    data_dir,
                    current.identity.sync_root.display().to_string(),
                    current.identity.root_path.clone(),
                    &current.plan,
                )
                .map_err(reconciliation_recovery_error)?
            {
                let mut terminal_pending = current.clone();
                terminal_pending.namespace_resume =
                    CursorRecoveryNamespaceResume::TerminalPendingFinalize;
                persist_cursor_recovery_module_marker(&marker_path, &terminal_pending)?;
                drop(terminal);
                drop(owner_guards);
                continue;
            }
        }

        match current.phase {
            CursorRecoveryModulePhase::Authorized => {
                if journal_snapshot.current.is_none()
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot.current.is_none()
                    && receipt_snapshot.temporary.is_none()
                {
                    return Ok(Some(current.clone()));
                }
            }
            CursorRecoveryModulePhase::Validated => {
                if journal_snapshot.current.is_none()
                    && journal_snapshot.temporary.as_ref() == Some(&state_owned)
                    && receipt_snapshot.current.is_none()
                    && receipt_snapshot.temporary.is_none()
                {
                    promote_cursor_recovery_artifact_temp(
                        &journal_temp_path,
                        &journal_path,
                        "registration-journal",
                        "Cloud Files registration journal",
                    )?;
                    continue;
                }
                if journal_snapshot.temporary.is_none()
                    && receipt_snapshot.current.is_none()
                    && receipt_snapshot.temporary.is_none()
                    && (journal_snapshot.current.is_none()
                        || journal_snapshot.current.as_ref() == Some(&state_owned))
                {
                    return Ok(Some(current.clone()));
                }
            }
            CursorRecoveryModulePhase::StateOwned => {
                if journal_snapshot.current.as_ref() == Some(&state_owned)
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot.current.is_none()
                    && receipt_snapshot.temporary.is_none()
                {
                    return Ok(Some(current.clone()));
                }
            }
            CursorRecoveryModulePhase::ReconcileArmed => {
                if current.namespace_resume == CursorRecoveryNamespaceResume::ReconcileProgress {
                    if journal_snapshot.current.as_ref() == Some(&state_owned)
                        && journal_snapshot.temporary.is_none()
                        && receipt_snapshot.current.is_none()
                        && receipt_snapshot.temporary.is_none()
                    {
                        return Ok(Some(current.clone()));
                    }
                    return Err(CloudFilesModuleError {
                        code: "cursor-recovery-progress-artifact-cut-invalid".to_string(),
                        detail: "ReconcileProgress permits only the exact StateOwned journal with no receipt or artifact temporary evidence"
                            .to_string(),
                    });
                }
                if journal_snapshot.current.as_ref() == Some(&state_owned)
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot.current.is_none()
                    && receipt_snapshot.temporary.as_ref() == Some(&receipt)
                {
                    promote_cursor_recovery_artifact_temp(
                        &receipt_temp_path,
                        &receipt_path,
                        "registration-receipt",
                        "Cloud Files registration receipt",
                    )?;
                    continue;
                }
                if journal_snapshot.current.as_ref() == Some(&state_owned)
                    && journal_snapshot.temporary.as_ref() == Some(&ready)
                    && receipt_snapshot.current.as_ref() == Some(&receipt)
                    && receipt_snapshot.temporary.is_none()
                {
                    promote_cursor_recovery_artifact_temp(
                        &journal_temp_path,
                        &journal_path,
                        "registration-journal",
                        "Cloud Files registration journal",
                    )?;
                    continue;
                }
                let exact_state_owned_cut = journal_snapshot.current.as_ref() == Some(&state_owned)
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot.temporary.is_none()
                    && (receipt_snapshot.current.is_none()
                        || receipt_snapshot.current.as_ref() == Some(&receipt));
                let exact_ready_cut = journal_snapshot.current.as_ref() == Some(&ready)
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot.current.as_ref() == Some(&receipt)
                    && receipt_snapshot.temporary.is_none();
                if exact_state_owned_cut || exact_ready_cut {
                    return Ok(Some(current.clone()));
                }
            }
            CursorRecoveryModulePhase::Ready | CursorRecoveryModulePhase::NamespaceFinalized => {
                if journal_snapshot.current.as_ref() == Some(&ready)
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot.current.as_ref() == Some(&receipt)
                    && receipt_snapshot.temporary.is_none()
                {
                    return Ok(Some(current.clone()));
                }
            }
            CursorRecoveryModulePhase::PreserveAuthorized => {
                let (predecessor_state_owned, predecessor_ready, predecessor_receipt) =
                    cursor_recovery_predecessor_registration_artifacts(current)?;
                if journal_snapshot.current.as_ref().is_some_and(|journal| {
                    (journal == &predecessor_ready || journal == &predecessor_state_owned)
                        && cursor_recovery_receipt_cut_matches(
                            journal,
                            receipt_snapshot.current.as_ref(),
                            &predecessor_receipt,
                        )
                }) && journal_snapshot.temporary.is_none()
                    && receipt_snapshot.temporary.is_none()
                {
                    return Ok(Some(current.clone()));
                }
                if journal_snapshot.current.as_ref().is_some_and(|journal| {
                    (journal == &predecessor_ready || journal == &predecessor_state_owned)
                        && cursor_recovery_receipt_cut_matches(
                            journal,
                            receipt_snapshot.current.as_ref(),
                            &predecessor_receipt,
                        )
                }) && journal_snapshot.temporary.as_ref() == Some(&state_owned)
                    && receipt_snapshot.temporary.is_none()
                {
                    promote_cursor_recovery_artifact_temp(
                        &journal_temp_path,
                        &journal_path,
                        "registration-journal",
                        "Cloud Files preserved StateOwned journal",
                    )?;
                    continue;
                }
                if journal_snapshot.current.as_ref() == Some(&state_owned)
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot
                        .current
                        .as_ref()
                        .is_none_or(|value| value == &receipt)
                    && receipt_snapshot.temporary.is_none()
                {
                    return Ok(Some(current.clone()));
                }
            }
            CursorRecoveryModulePhase::PreservedStateOwned => {
                if journal_snapshot.current.as_ref() == Some(&state_owned)
                    && journal_snapshot.temporary.is_none()
                    && receipt_snapshot
                        .current
                        .as_ref()
                        .is_none_or(|value| value == &receipt)
                    && receipt_snapshot.temporary.is_none()
                {
                    return Ok(Some(current.clone()));
                }
            }
            CursorRecoveryModulePhase::CompletionAuthorized => {
                if receipt_snapshot.current.as_ref() != Some(&receipt)
                    || receipt_snapshot.temporary.is_some()
                {
                    return Err(CloudFilesModuleError {
                        code: "cursor-recovery-completion-receipt-cut".to_string(),
                        detail: "CompletionAuthorized requires the exact Ready receipt with no temporary evidence"
                            .to_string(),
                    });
                }
                let exact_recovery_ready = journal_snapshot.current.as_ref() == Some(&ready)
                    && journal_snapshot.temporary.is_none();
                let exact_normalized_ready = journal_snapshot.current.as_ref()
                    == Some(&ordinary_ready)
                    && journal_snapshot.temporary.is_none();
                let exact_normalization_temp = journal_snapshot.current.as_ref() == Some(&ready)
                    && journal_snapshot.temporary.as_ref() == Some(&ordinary_ready);
                let exact_redundant_normalized_temp = journal_snapshot.current.as_ref()
                    == Some(&ordinary_ready)
                    && journal_snapshot.temporary.as_ref() == Some(&ordinary_ready);
                if exact_normalization_temp {
                    promote_cursor_recovery_artifact_temp(
                        &journal_temp_path,
                        &journal_path,
                        "registration-journal",
                        "normalized Cloud Files registration journal",
                    )?;
                    continue;
                }
                if exact_redundant_normalized_temp {
                    promote_exact_registration_artifact(
                        &journal_path,
                        &journal_temp_path,
                        CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES,
                        &ordinary_ready,
                        "registration-journal",
                        "normalized Cloud Files registration journal",
                    )?;
                    continue;
                }
                if exact_recovery_ready || exact_normalized_ready {
                    return Ok(Some(current.clone()));
                }
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-completion-journal-cut".to_string(),
                    detail: "CompletionAuthorized journal is neither its exact recovery Ready predecessor nor exact ordinary Ready successor"
                        .to_string(),
                });
            }
        }
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-artifact-cut-invalid".to_string(),
            detail: format!(
                "cursor recovery {:?} marker/journal/receipt snapshot is not an exact causal cut",
                current.phase
            ),
        });
    }
    Err(CloudFilesModuleError {
        code: "cursor-recovery-artifact-replay-loop".to_string(),
        detail: "cursor recovery artifact replay exceeded its finite causal transition bound"
            .to_string(),
    })
}

fn validate_cursor_recovery_marker(
    marker: &CursorRecoveryModuleMarker,
) -> Result<(), CloudFilesModuleError> {
    validate_cursor_recovery_identity(&marker.identity)?;
    let valid_fingerprint = |value: &str| {
        value
            .strip_prefix("blake3:")
            .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
    };
    let valid_token = |value: &str| {
        value
            .strip_prefix("v1:blake3:")
            .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
    };
    let valid_cycle_authorization = marker
        .cycle_authorization_fingerprint
        .as_deref()
        .is_some_and(|value| {
            value.strip_prefix("blake3:").is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        });
    let valid_predecessor_authorization = marker
        .predecessor_cycle_fingerprint
        .as_deref()
        .is_some_and(|value| {
            value.strip_prefix("blake3:").is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        });
    let preserve_phase = matches!(
        marker.phase,
        CursorRecoveryModulePhase::PreserveAuthorized
            | CursorRecoveryModulePhase::PreservedStateOwned
    );
    if marker.schema_version != CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION
        || marker.recovery_id.trim().is_empty()
        || !valid_token(&marker.recovery_token)
        || !valid_fingerprint(&marker.raw_state_fingerprint)
        || marker.sync_root_file_id == 0
        || marker.registration_nonce.trim().is_empty()
        || marker.registration_lock_bytes != Vec::<u8>::new()
        || (marker.recovery_cycle == 0
            && (marker.cycle_authorization_fingerprint.is_some()
                || marker.predecessor_cycle_fingerprint.is_some()))
        || (marker.recovery_cycle > 0
            && ((preserve_phase && marker.cycle_authorization_fingerprint.is_some())
                || (!preserve_phase && !valid_cycle_authorization)
                || (marker.recovery_cycle == 1 && marker.predecessor_cycle_fingerprint.is_some())
                || (marker.recovery_cycle > 1 && !valid_predecessor_authorization)))
        || marker.module_topology
            != vec![
                "local-changes".to_string(),
                "namespace-reconciliation".to_string(),
                CLOUD_FILES_REGISTRATION_LOCK_FILE.to_string(),
            ]
        || marker.plan.schema_version != marker.schema_version
        || marker.plan.recovery_id != marker.recovery_id
        || marker.plan.recovery_token != marker.recovery_token
        || marker.plan.nonce != marker.registration_nonce
        || marker.plan.root != marker.identity.root_path
        || marker.plan.cursor_count_cleared == 0
        || marker.plan.original_state.is_empty()
        || marker.plan.expected_state.is_empty()
        || marker.plan.original_audit.is_empty()
        || marker.plan.expected_audit.is_empty()
        || matches!(
            marker.phase,
            CursorRecoveryModulePhase::PreserveAuthorized
                | CursorRecoveryModulePhase::PreservedStateOwned
        ) && (marker.recovery_cycle == 0
            || marker.namespace_resume != marker.cycle_authorized_namespace_resume)
        || !cursor_recovery_namespace_resume_is_monotonic(
            marker.cycle_authorized_namespace_resume,
            marker.namespace_resume,
        )
        || !match marker.phase {
            CursorRecoveryModulePhase::Authorized
            | CursorRecoveryModulePhase::Validated
            | CursorRecoveryModulePhase::StateOwned => {
                marker.namespace_resume == CursorRecoveryNamespaceResume::ReconcileProgress
                    && marker.recovery_cycle == 0
            }
            CursorRecoveryModulePhase::ReconcileArmed => {
                marker.recovery_cycle > 0
                    || marker.namespace_resume != CursorRecoveryNamespaceResume::Finalized
            }
            CursorRecoveryModulePhase::Ready => matches!(
                marker.namespace_resume,
                CursorRecoveryNamespaceResume::TerminalPendingFinalize
                    | CursorRecoveryNamespaceResume::Finalized
            ),
            CursorRecoveryModulePhase::NamespaceFinalized
            | CursorRecoveryModulePhase::CompletionAuthorized => {
                marker.namespace_resume == CursorRecoveryNamespaceResume::Finalized
            }
            CursorRecoveryModulePhase::PreserveAuthorized
            | CursorRecoveryModulePhase::PreservedStateOwned => true,
        }
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-marker-invariant".to_string(),
            detail: "cursor recovery marker outer identity does not match its embedded exact plan"
                .to_string(),
        });
    }
    if marker.recovery_cycle > 0
        && !matches!(
            marker.phase,
            CursorRecoveryModulePhase::PreserveAuthorized
                | CursorRecoveryModulePhase::PreservedStateOwned
        )
    {
        let expected = cursor_recovery_native_successor_fingerprint(marker)?;
        if marker.cycle_authorization_fingerprint.as_deref() != Some(&expected) {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-cycle-authorization-mismatch".to_string(),
                detail:
                    "cursor recovery cycle authorization does not match its exact native successor"
                        .to_string(),
            });
        }
    }
    if marker.phase == CursorRecoveryModulePhase::PreservedStateOwned {
        let expected = cursor_recovery_native_successor_fingerprint(marker)?;
        if expected.trim().is_empty() {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-successor-fingerprint".to_string(),
                detail: "PreservedStateOwned successor fingerprint is empty".to_string(),
            });
        }
    }
    if marker.reconciliation_raw_files.get("state.sobj") != Some(&marker.plan.original_state)
        || marker
            .reconciliation_raw_files
            .get("materialization-audit-intents.sobj")
            != Some(&marker.plan.original_audit)
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-marker-original-mismatch".to_string(),
            detail: "cursor recovery plan original bytes do not match the captured owner snapshot"
                .to_string(),
        });
    }
    crate::namespace_reconciler::validate_authorized_cursor_recovery_plan(
        &marker.plan,
        &marker.identity.sync_root.display().to_string(),
        &marker.identity.root_path,
    )
    .map_err(|source| CloudFilesModuleError {
        code: "cursor-recovery-marker-plan".to_string(),
        detail: format!("cursor recovery marker embeds an invalid deterministic plan: {source}"),
    })?;
    let raw_state_fingerprint = cursor_recovery_raw_fingerprint(
        &marker.identity,
        marker.sync_root_file_id,
        &marker.registration_lock_bytes,
        &marker.module_topology,
        &marker.queue_raw_files,
        &marker.reconciliation_raw_files,
        &marker.outbox_raw_files,
    )?;
    let recovery_token = cursor_recovery_token(&raw_state_fingerprint);
    if marker.raw_state_fingerprint != raw_state_fingerprint
        || marker.recovery_token != recovery_token
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-marker-token-mismatch".to_string(),
            detail:
                "cursor recovery marker token is not derived from its complete captured preimage"
                    .to_string(),
        });
    }
    Ok(())
}

fn cursor_recovery_raw_fingerprint(
    identity: &CloudFilesCursorOnlyRecoveryIdentity,
    sync_root_file_id: u64,
    registration_lock_bytes: &[u8],
    module_topology: &[String],
    queue_raw_files: &BTreeMap<String, Vec<u8>>,
    reconciliation_raw_files: &BTreeMap<String, Vec<u8>>,
    outbox_raw_files: &BTreeMap<String, Vec<u8>>,
) -> Result<String, CloudFilesModuleError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CLOUD_FILES_CURSOR_ONLY_RECOVERY_TOKEN_DOMAIN.as_bytes());
    hasher.update(&[0]);
    hash_cursor_recovery_field(&mut hasher, b"identity");
    hash_cursor_recovery_field(
        &mut hasher,
        &encode_sobj(identity).map_err(|source| CloudFilesModuleError {
            code: "cursor-recovery-identity-encode".to_string(),
            detail: source.to_string(),
        })?,
    );
    hash_cursor_recovery_field(&mut hasher, b"sync-root-file-id");
    hash_cursor_recovery_field(&mut hasher, &sync_root_file_id.to_le_bytes());
    hash_cursor_recovery_field(&mut hasher, b"registration-lock");
    hash_cursor_recovery_field(&mut hasher, registration_lock_bytes);
    hash_cursor_recovery_field(&mut hasher, b"module-topology");
    hash_cursor_recovery_field(&mut hasher, &(module_topology.len() as u64).to_le_bytes());
    for name in module_topology {
        hash_cursor_recovery_field(&mut hasher, name.as_bytes());
    }
    hash_cursor_recovery_field(&mut hasher, b"local-change-queue");
    hash_cursor_recovery_map(&mut hasher, queue_raw_files);
    hash_cursor_recovery_field(&mut hasher, b"namespace-reconciliation");
    hash_cursor_recovery_map(&mut hasher, reconciliation_raw_files);
    hash_cursor_recovery_field(&mut hasher, b"mounted-mutation-outbox");
    hash_cursor_recovery_map(&mut hasher, outbox_raw_files);
    Ok(format!("blake3:{}", hasher.finalize().to_hex()))
}

fn cursor_recovery_token(raw_state_fingerprint: &str) -> String {
    let mut token_hasher = blake3::Hasher::new();
    token_hasher.update(CLOUD_FILES_CURSOR_ONLY_RECOVERY_TOKEN_DOMAIN.as_bytes());
    token_hasher.update(b"\0operator-token\0");
    token_hasher.update(raw_state_fingerprint.as_bytes());
    format!("v1:blake3:{}", token_hasher.finalize().to_hex())
}

fn persist_cursor_recovery_module_marker(
    path: &Path,
    marker: &CursorRecoveryModuleMarker,
) -> Result<(), CloudFilesModuleError> {
    let bytes = encode_sobj(marker).map_err(|source| CloudFilesModuleError {
        code: "cursor-recovery-marker-encode".to_string(),
        detail: source.to_string(),
    })?;
    if bytes.len() as u64 > CLOUD_FILES_CURSOR_RECOVERY_MARKER_MAX_BYTES {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-marker-too-large".to_string(),
            detail: "cursor recovery marker exceeds its bound".to_string(),
        });
    }
    persist_cursor_recovery_registration_artifact(
        path,
        marker,
        "cursor-recovery-marker",
        "Cloud Files cursor recovery marker",
    )
}

fn clear_completion_authorized_cursor_recovery_module_marker(
    data_dir: &SinomoDataDir,
    expected: &CursorRecoveryModuleMarker,
) -> Result<(), CloudFilesModuleError> {
    if expected.phase != CursorRecoveryModulePhase::CompletionAuthorized
        || expected.namespace_resume != CursorRecoveryNamespaceResume::Finalized
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-completion-not-authorized".to_string(),
            detail: "only exact CompletionAuthorized/Finalized authority may be cleared"
                .to_string(),
        });
    }
    let path = cursor_recovery_module_marker_path(data_dir);
    let marker_snapshot = inspect_cursor_recovery_module_marker_snapshot(data_dir)?;
    if marker_snapshot.current.as_ref() != Some(expected) || marker_snapshot.temporary.is_some() {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-marker-mismatch".to_string(),
            detail: "refusing to clear changed or temporary completion authority".to_string(),
        });
    }
    let (_, mut recovery_ready, expected_receipt) =
        cursor_recovery_expected_registration_artifacts(expected)?;
    recovery_ready.phase = RegistrationPhase::Ready;
    let mut ordinary_ready = recovery_ready;
    ordinary_ready.clear_cursor_recovery();
    let journal_snapshot = inspect_registration_artifact_snapshot::<RegistrationJournal>(
        &registration_journal_path(data_dir),
        &registration_journal_temp_path(data_dir),
        CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES,
        "registration-journal",
        "normalized Cloud Files registration journal",
    )?;
    let receipt_snapshot = inspect_registration_artifact_snapshot::<CloudFilesRegistrationReceipt>(
        &registration_receipt_path(data_dir),
        &registration_receipt_temp_path(data_dir),
        CLOUD_FILES_REGISTRATION_RECEIPT_MAX_BYTES,
        "registration-receipt",
        "Cloud Files registration receipt",
    )?;
    if journal_snapshot.current.as_ref() != Some(&ordinary_ready)
        || journal_snapshot.temporary.is_some()
        || receipt_snapshot.current.as_ref() != Some(&expected_receipt)
        || receipt_snapshot.temporary.is_some()
    {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-completion-artifacts".to_string(),
            detail: "CompletionAuthorized may clear only after exact ordinary Ready journal normalization"
                .to_string(),
        });
    }
    sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(&path)
        .map(|_| ())
        .map_err(|source| CloudFilesModuleError {
            code: "cursor-recovery-marker-finalize".to_string(),
            detail: format!("clear CompletionAuthorized cursor recovery marker: {source}"),
        })
}

fn hash_cursor_recovery_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn hash_cursor_recovery_map(hasher: &mut blake3::Hasher, files: &BTreeMap<String, Vec<u8>>) {
    hasher.update(&(files.len() as u64).to_le_bytes());
    for (name, bytes) in files {
        hash_cursor_recovery_field(hasher, name.as_bytes());
        hash_cursor_recovery_field(hasher, bytes);
    }
}

fn reconciliation_recovery_error(
    error: crate::namespace_reconciler::CloudFilesNamespaceReconcileError,
) -> CloudFilesModuleError {
    CloudFilesModuleError {
        code: error.code.to_string(),
        detail: error.detail,
    }
}

fn with_cleanup_result(
    mut error: CloudFilesModuleError,
    action: &str,
    cleanup: Result<(), CloudFilesModuleError>,
) -> CloudFilesModuleError {
    if let Err(cleanup_error) = cleanup {
        error
            .detail
            .push_str(&format!("; {action} failed: {cleanup_error}"));
    }
    error
}

fn activate_registration_before_writable_effects_with<
    A,
    R,
    FRegister,
    FRegistered,
    FRecheck,
    FOwned,
    FEffects,
>(
    activation: &mut A,
    register: FRegister,
    mark_registered: FRegistered,
    recheck_fresh_boundary: FRecheck,
    mark_state_owned: FOwned,
    effects: FEffects,
) -> Result<R, CloudFilesModuleError>
where
    FRegister: FnOnce(&A) -> Result<(), CloudFilesModuleError>,
    FRegistered: FnOnce(&mut A) -> Result<(), CloudFilesModuleError>,
    FRecheck: FnOnce(&A) -> Result<(), CloudFilesModuleError>,
    FOwned: FnOnce(&mut A) -> Result<(), CloudFilesModuleError>,
    FEffects: FnOnce() -> Result<R, CloudFilesModuleError>,
{
    register(activation)?;
    mark_registered(activation)?;
    recheck_fresh_boundary(activation)?;
    mark_state_owned(activation)?;
    effects()
}

fn verify_fresh_state_owned_boundary_with<FEmpty, FScopes, FFileId>(
    fresh: bool,
    adopt_replaced_provider_state: bool,
    sync_root: &Path,
    expected_file_id: u64,
    ensure_empty: FEmpty,
    scopes_are_absent: FScopes,
    current_file_id: FFileId,
) -> Result<(), CloudFilesModuleError>
where
    FEmpty: FnOnce() -> Result<(), CloudFilesModuleError>,
    FScopes: FnOnce() -> Result<bool, CloudFilesModuleError>,
    FFileId: FnOnce() -> Result<u64, CloudFilesModuleError>,
{
    if !fresh {
        return Ok(());
    }
    if !adopt_replaced_provider_state {
        ensure_empty()?;
        if !scopes_are_absent()? {
            return Err(CloudFilesModuleError {
                code: "unowned-sync-root-state-not-pristine".to_string(),
                detail: "Fresh Cloud Files activation found a queue or reconciliation scope created before durable StateOwned; refusing to open or reinterpret it"
                    .to_string(),
            });
        }
    }
    if current_file_id()? != expected_file_id {
        return Err(CloudFilesModuleError {
            code: "sync-root-file-id-changed".to_string(),
            detail: format!(
                "Cloud Files sync root {} changed between registration and durable state ownership",
                sync_root.display()
            ),
        });
    }
    Ok(())
}

fn validate_provider_replacement_context(
    request: &CloudFilesMountRequest,
) -> Result<bool, CloudFilesModuleError> {
    let Some(replacement) = request.provider_replacement.as_ref() else {
        return Ok(false);
    };
    if request.cursor_recovery.is_some() {
        return Err(CloudFilesModuleError {
            code: "provider-replacement-cursor-recovery".to_string(),
            detail: "provider replacement and cursor recovery cannot share one mount authorization"
                .to_string(),
        });
    }
    let canonical = |value: &str| {
        value.strip_prefix("blake3:").is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    };
    if !canonical(&replacement.predecessor_content_hash)
        || !canonical(&replacement.successor_content_hash)
        || replacement.predecessor_content_hash == replacement.successor_content_hash
    {
        return Err(CloudFilesModuleError {
            code: "provider-replacement-context-invalid".to_string(),
            detail: "provider replacement requires distinct canonical predecessor and successor content hashes"
                .to_string(),
        });
    }
    Ok(true)
}

pub fn mount_sifs_cloud_files(
    request: CloudFilesMountRequest,
) -> Result<CloudFilesMountResponse, CloudFilesModuleError> {
    let mut lifecycle = MountLifecycleState::new(&request);
    let result = catch_unwind(AssertUnwindSafe(|| {
        mount_sifs_cloud_files_inner(request, &mut lifecycle)
    }))
    .unwrap_or_else(|_| {
        Err(CloudFilesModuleError {
            code: "mount-panic".to_string(),
            detail: "Cloud Files mount panicked while callbacks were active".to_string(),
        })
    });
    if let Err(error) = &result {
        lifecycle.emit_failed(error);
    }
    result
}

pub fn unregister_sifs_cloud_files(
    request: CloudFilesUnregisterRequest,
) -> Result<CloudFilesUnregisterResponse, CloudFilesModuleError> {
    let mut lifecycle = MountLifecycleState::for_unregister(&request);
    let result = catch_unwind(AssertUnwindSafe(|| {
        unregister_sifs_cloud_files_inner(request, &mut lifecycle)
    }))
    .unwrap_or_else(|_| {
        Err(CloudFilesModuleError {
            code: "unregister-panic".to_string(),
            detail: "Cloud Files sync-root unregistration panicked".to_string(),
        })
    });
    if let Err(error) = &result {
        lifecycle.emit_failed(error);
    }
    result
}

fn unregister_sifs_cloud_files_inner(
    mut request: CloudFilesUnregisterRequest,
    lifecycle: &mut MountLifecycleState,
) -> Result<CloudFilesUnregisterResponse, CloudFilesModuleError> {
    request.root_path = normalize_sifs_root(&request.root_path)?;
    lifecycle.root_path.clone_from(&request.root_path);
    let data_dir = resolve_data_dir(request.data_dir_root.clone())?;
    let (_registration_lease, registration_lock_bytes) =
        acquire_registration_lease_for_request(&data_dir, request.cursor_recovery.as_ref())?;
    let recovered = recover_registration_for_unregister(
        &request,
        &data_dir,
        UnregisterPolicy::Release,
        registration_lock_bytes.as_deref(),
    )?;
    lifecycle.registered = false;
    let detail = if recovered.was_registered {
        "Cloud Files sync root unregistered after nonce-bound ownership verification".to_string()
    } else {
        "Cloud Files sync root was already unregistered".to_string()
    };
    lifecycle.emit_unregistered(detail.clone());
    Ok(CloudFilesUnregisterResponse {
        sync_root: request.sync_root,
        root_path: request.root_path,
        was_registered: recovered.was_registered,
        detail,
    })
}

pub fn unregister_sifs_cloud_files_preserving_state(
    mut request: CloudFilesUnregisterRequest,
) -> Result<CloudFilesPreserveStateUnregisterResponse, CloudFilesModuleError> {
    let mut lifecycle = MountLifecycleState::for_unregister(&request);
    let result = catch_unwind(AssertUnwindSafe(|| {
        request.root_path = normalize_sifs_root(&request.root_path)?;
        lifecycle.root_path.clone_from(&request.root_path);
        let data_dir = resolve_data_dir(request.data_dir_root.clone())?;
        let (_registration_lease, registration_lock_bytes) =
            acquire_registration_lease_for_request(
                &data_dir,
                request.cursor_recovery.as_ref(),
            )?;
        let recovered = recover_registration_for_unregister(
            &request,
            &data_dir,
            UnregisterPolicy::PreserveState,
            registration_lock_bytes.as_deref(),
        )?;
        lifecycle.registered = false;
        let outcome = match recovered.ownership {
            RegistrationOwnershipDisposition::Retain => {
                CloudFilesPreserveStateUnregisterOutcome::Retained
            }
            RegistrationOwnershipDisposition::Release => {
                CloudFilesPreserveStateUnregisterOutcome::Released
            }
        };
        let detail = match (outcome, recovered.was_registered) {
            (CloudFilesPreserveStateUnregisterOutcome::Retained, true) => {
                "Cloud Files sync root unregistered while exact durable state ownership was retained"
            }
            (CloudFilesPreserveStateUnregisterOutcome::Retained, false) => {
                "Cloud Files sync root was already absent and exact durable state ownership remains retained"
            }
            (CloudFilesPreserveStateUnregisterOutcome::Released, true) => {
                "Cloud Files sync root unregistered before writable state ownership was established"
            }
            (CloudFilesPreserveStateUnregisterOutcome::Released, false) => {
                "Cloud Files sync root was absent before writable state ownership was established"
            }
        }
        .to_string();
        lifecycle.emit_unregistered(detail.clone());
        Ok(CloudFilesPreserveStateUnregisterResponse {
            sync_root: request.sync_root,
            root_path: request.root_path,
            was_registered: recovered.was_registered,
            outcome,
            cursor_recovery: recovered.cursor_recovery,
            detail,
        })
    }))
    .unwrap_or_else(|_| {
        Err(CloudFilesModuleError {
            code: "unregister-panic".to_string(),
            detail: "Cloud Files state-preserving unregistration panicked".to_string(),
        })
    });
    if let Err(error) = &result {
        lifecycle.emit_failed(error);
    }
    result
}

fn mount_sifs_cloud_files_inner(
    mut request: CloudFilesMountRequest,
    lifecycle: &mut MountLifecycleState,
) -> Result<CloudFilesMountResponse, CloudFilesModuleError> {
    request.validate_supported_mode()?;
    let cancellation_event_handle =
        request
            .cancellation_event_handle
            .ok_or_else(|| CloudFilesModuleError {
                code: "mount-cancellation-event".to_string(),
                detail: "Cloud Files mount requires a host-owned cancellation event".to_string(),
            })?;
    request.root_path = normalize_sifs_root(&request.root_path)?;
    lifecycle.root_path.clone_from(&request.root_path);
    let data_dir = resolve_data_dir(request.data_dir_root.clone())?;
    let (_registration_lease, registration_lock_bytes) =
        acquire_registration_lease_for_request(&data_dir, request.cursor_recovery.as_ref())?;
    let cursor_recovery_root_fence = request
        .cursor_recovery
        .as_ref()
        .map(|_| CursorRecoveryRootFence::acquire(&request.sync_root))
        .transpose()?;
    let cursor_recovery_marker = if let Some(marker) =
        recover_cursor_recovery_registration_snapshot(&data_dir, false, |marker| {
            if registration_lock_bytes.as_deref() != Some(marker.registration_lock_bytes.as_slice())
            {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-registration-lock-drift".to_string(),
                    detail: "cursor recovery registration lock bytes changed before mount"
                        .to_string(),
                });
            }
            if cursor_recovery_root_fence
                .as_ref()
                .map(|fence| fence.file_id)
                != Some(marker.sync_root_file_id)
            {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-root-drift".to_string(),
                    detail: "owned cursor recovery root changed before mount snapshot replay"
                        .to_string(),
                });
            }
            validate_cursor_recovery_mount_authority(marker, &request, &data_dir)
        })? {
        Some(marker)
    } else {
        None
    };
    let cursor_recovery_marker = if let Some(mut marker) = cursor_recovery_marker {
        if marker.phase == CursorRecoveryModulePhase::PreservedStateOwned {
            rearm_preserved_cursor_recovery(
                &data_dir,
                &cursor_recovery_module_marker_path(&data_dir),
                &mut marker,
            )?;
        }
        inspect_cursor_recovery_module_topology(&data_dir, Some(marker.phase))?;
        validate_cursor_recovery_registration_artifacts(&data_dir, &marker)?;
        Some(marker)
    } else {
        None
    };
    if request.cursor_recovery.is_some() != cursor_recovery_marker.is_some() {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-context-marker-mismatch".to_string(),
            detail: "mount cursor-recovery context and durable native authority must be present together"
                .to_string(),
        });
    }
    let provider_replacement = validate_provider_replacement_context(&request)?;
    let _cursor_recovery_root_fence = cursor_recovery_root_fence;
    if !request.connect {
        return Err(CloudFilesModuleError {
            code: "writable-sync-requires-connection".to_string(),
            detail: "writable Cloud Files requires connected hydration and synchronization workers"
                .to_string(),
        });
    }
    let recovered_registration = recover_registration_for_mount(&request, &data_dir)?;
    let existing_owned_root =
        matches!(&recovered_registration, RecoveredRegistration::Owned { .. });
    let existing_registration = matches!(
        &recovered_registration,
        RecoveredRegistration::Owned {
            registration_present: true,
            ..
        }
    );
    if !existing_owned_root {
        fs::create_dir_all(&request.sync_root).map_err(|source| CloudFilesModuleError {
            code: "sync-root-create".to_string(),
            detail: format!(
                "create Cloud Files sync root {}: {source}",
                request.sync_root.display()
            ),
        })?;
        verify_fresh_registration_absent(&request)?;
        if provider_replacement {
            ensure_owned_sync_root_is_plain_directory(&request.sync_root)?;
        } else {
            ensure_new_sync_root_is_empty(&request.sync_root)?;
            let scopes_absent = fresh_writable_scopes_are_absent(
                &data_dir,
                &request.sync_root.to_string_lossy(),
                &request.root_path,
            )
            .map_err(|error| CloudFilesModuleError {
                code: error.code.to_string(),
                detail: error.detail,
            })?;
            if !scopes_absent {
                return Err(CloudFilesModuleError {
                    code: "unowned-sync-root-state-not-pristine".to_string(),
                    detail: "new or unowned Cloud Files activation found a pre-existing exact queue or reconciliation scope; refusing to open, repair, or reinterpret retained state without verified registration ownership"
                        .to_string(),
                });
            }
        }
    }
    let terminal_ready_handoff = cursor_recovery_marker.as_ref().is_some_and(|marker| {
        matches!(
            marker.phase,
            CursorRecoveryModulePhase::Ready
                | CursorRecoveryModulePhase::NamespaceFinalized
                | CursorRecoveryModulePhase::CompletionAuthorized
        )
    });
    let (journal, state_owned) = prepare_registration_activation(
        &request,
        &data_dir,
        recovered_registration,
        terminal_ready_handoff,
    )?;
    let mut activation = RegistrationActivation::new(data_dir.clone(), journal, state_owned);
    activation.registration_preexisting = existing_registration;
    if terminal_ready_handoff {
        activation.retain_terminal_completion_handoff()?;
    }
    let opened_effects = activate_registration_before_writable_effects_with(
        &mut activation,
        |activation| register_sync_root(&request, activation.journal(), existing_registration),
        |activation| activation.mark_registered(&request),
        |activation| {
            verify_fresh_state_owned_boundary_with(
                !existing_owned_root,
                provider_replacement,
                &request.sync_root,
                activation.journal().sync_root_file_id,
                || ensure_new_sync_root_is_empty(&request.sync_root),
                || {
                    fresh_writable_scopes_are_absent(
                        &data_dir,
                        &request.sync_root.to_string_lossy(),
                        &request.root_path,
                    )
                    .map_err(|error| CloudFilesModuleError {
                        code: error.code.to_string(),
                        detail: error.detail,
                    })
                },
                || sync_root_file_index(&request.sync_root),
            )
        },
        |activation| activation.mark_state_owned(&request),
        || {
            lifecycle.registered = true;
            lifecycle.emit_registered();
            // Every constructor below can create, repair, migrate, or commit
            // durable writable state and is therefore inside the StateOwned
            // effect closure.
            let mounted_mutation_handoff = Arc::new(
                CloudFilesMountedMutationHandoff::open(
                    &data_dir,
                    format!(
                        "cloud-files:{}:{}",
                        request.sync_root.to_string_lossy(),
                        request.root_path
                    ),
                )
                .map_err(|detail| CloudFilesModuleError {
                    code: "mounted-mutation-handoff-unavailable".to_string(),
                    detail,
                })?,
            );
            let writable_state = if let Some(marker) = cursor_recovery_marker.as_ref() {
                match marker.namespace_resume {
                    CursorRecoveryNamespaceResume::ReconcileProgress => {
                        CloudFilesWritableSyncState::open_authorized_cursor_recovery(
                            &data_dir,
                            request.sync_root.to_string_lossy(),
                            request.root_path.clone(),
                            &marker.plan,
                        )
                    }
                    CursorRecoveryNamespaceResume::TerminalPendingFinalize => {
                        CloudFilesWritableSyncState::open_authorized_cursor_recovery_terminal_pending_finalize(
                            &data_dir,
                            request.sync_root.to_string_lossy(),
                            request.root_path.clone(),
                            &marker.plan,
                        )
                    }
                    CursorRecoveryNamespaceResume::Finalized => {
                        CloudFilesWritableSyncState::open_authorized_cursor_recovery_namespace_finalized(
                            &data_dir,
                            request.sync_root.to_string_lossy(),
                            request.root_path.clone(),
                            &marker.plan,
                        )
                    }
                }
            } else {
                CloudFilesWritableSyncState::open(
                    &data_dir,
                    request.sync_root.to_string_lossy(),
                    request.root_path.clone(),
                )
            }
            .map_err(|error| CloudFilesModuleError {
                code: error.code.to_string(),
                detail: error.detail,
            })?;
            lifecycle.local_change_sync = writable_state.status();
            let materialization_audit_intents = writable_state.materialization_audit_intents();
            let opened = mounted_mutation_handoff
                .run_captured_mount_open(|| {
                    let opened = open_sifs_service(request.data_dir_root.clone())?;
                    let repair_records = opened.service.durable_repair_queue_records();
                    Ok::<_, CloudFilesModuleError>((opened, repair_records))
                })
                .map_err(|detail| CloudFilesModuleError {
                    code: "mounted-mutation-mount-open".to_string(),
                    detail,
                })?;
            Ok((
                mounted_mutation_handoff,
                writable_state,
                materialization_audit_intents,
                opened,
            ))
        },
    );
    let (mounted_mutation_handoff, writable_state, materialization_audit_intents, opened) =
        match opened_effects {
            Ok(opened) => opened,
            Err(error) => return Err(activation.fail(error, lifecycle)),
        };
    let shared_service = Arc::new(Mutex::new(opened.service));
    let materialized_directories = Arc::new(Mutex::new(BTreeSet::new()));
    let writable_worker_wake = Arc::new(windows_adapter::WindowsWorkerWake::default());
    let _namespace_event_subscription = match windows_adapter::subscribe_namespace_changes(
        data_dir.clone(),
        &request.root_path,
        Arc::clone(&writable_worker_wake),
    ) {
        Ok(subscription) => subscription,
        Err(error) => return Err(activation.fail(error, lifecycle)),
    };
    // Existing placeholders need a connected provider before CFAPI can open
    // and validate their identities. Keep nested population parked until the
    // worker has re-established its durable Ready proof; cursor recovery keeps
    // the same gate until its stronger terminal handoff releases it.
    let provider_population_released = (cursor_recovery_marker.is_some() || existing_owned_root)
        .then(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let provider = Arc::new(CloudFilesProvider::new(
        Arc::clone(&shared_service),
        Arc::clone(&mounted_mutation_handoff),
        request.sync_root.clone(),
        request.root_path.clone(),
        request.read_only,
        provider_population_released.clone(),
        cursor_recovery_marker.is_some(),
        Arc::clone(&materialized_directories),
        materialization_audit_intents,
        Arc::clone(&writable_worker_wake),
    ));

    let write_options = dfs::DfsWriteOptions::replicated_eventual(opened.replica_candidates);
    let (mut writable_worker, writable_worker_cancelled, explorer_status) =
        windows_adapter::build_writable_worker(
            writable_state,
            Arc::clone(&shared_service),
            write_options,
            Arc::clone(&mounted_mutation_handoff),
            Arc::clone(&writable_worker_wake),
            windows_adapter::WindowsWritableWorkerOptions {
                sync_root: request.sync_root.clone(),
                root_path: request.root_path.clone(),
                existing_owned_root,
                expected_cursor_recovery_sync_root_file_id: cursor_recovery_marker
                    .as_ref()
                    .map(|marker| marker.sync_root_file_id),
                prepopulate_depth: request.prepopulate_depth,
                materialized_directories,
            },
        );
    let connect_provider = || {
        let callback_table = callback_table();
        let sync_root_wide = wide_path(&request.sync_root);
        unsafe {
            CfConnectSyncRoot(
                PCWSTR(sync_root_wide.as_ptr()),
                callback_table.as_ptr(),
                Some(Arc::as_ptr(&provider).cast::<c_void>()),
                CF_CONNECT_FLAG_NONE,
            )
        }
        .map(|key| CloudFilesConnection::new(key, request.sync_root.clone()))
        .map_err(|source| CloudFilesModuleError {
            code: "connect-sync-root".to_string(),
            detail: format!(
                "connect Cloud Files sync root {}: {source}",
                request.sync_root.display()
            ),
        })
    };
    let mut connection = if existing_owned_root {
        match connect_provider() {
            Ok(connection) => Some(connection),
            Err(error) => return Err(activation.fail(error, lifecycle)),
        }
    } else {
        None
    };
    if let Err(error) = writable_worker.startup(&writable_worker_cancelled) {
        lifecycle.local_change_sync = writable_worker.state().status();
        let mut error = CloudFilesModuleError {
            code: error.code.to_string(),
            detail: error.detail,
        };
        if let Some(connection) = connection.as_mut() {
            error = with_cleanup_result(error, "disconnect", connection.disconnect());
        }
        return Err(activation.fail(error, lifecycle));
    }
    crate::windows_shell::observe_explorer_state(&explorer_status, writable_worker.state());
    lifecycle.local_change_sync = writable_worker.state().status();
    lifecycle.placeholders_created = writable_worker.state().reconciler().manifest().len() as u64;
    let placeholders_created = lifecycle.placeholders_created;

    let mut connection = match connection {
        Some(connection) => connection,
        None => match connect_provider() {
            Ok(connection) => connection,
            Err(error) => return Err(activation.fail(error, lifecycle)),
        },
    };
    lifecycle.connected = true;
    lifecycle.emit_connected();
    let emitter = windows_adapter::WorkerLifecycleEmitter::from_lifecycle(lifecycle);
    let initial_worker_ready = writable_worker_ready(&lifecycle.local_change_sync);
    emitter.emit(
        lifecycle.local_change_sync.clone(),
        true,
        initial_worker_ready,
        1,
        writable_worker.ordinary_reconciliation_phase(),
    );
    let mut writable_worker = match windows_adapter::WindowsWritableWorkerHandle::spawn(
        writable_worker,
        writable_worker_cancelled,
        writable_worker_wake,
        emitter,
        1,
        explorer_status,
    ) {
        Ok(worker) => worker,
        Err(error) => {
            let error = with_cleanup_result(error, "disconnect", connection.disconnect());
            lifecycle.connected = false;
            return Err(activation.fail(error, lifecycle));
        }
    };
    if !writable_worker.is_alive() {
        let error = writable_worker
            .take_failure()
            .unwrap_or_else(|| CloudFilesModuleError {
                code: "writable-worker-startup-stopped".to_string(),
                detail: "writable Cloud Files worker stopped before activation became ready"
                    .to_string(),
            });
        let error = with_cleanup_result(error, "worker join", writable_worker.stop_and_join());
        let error = with_cleanup_result(error, "disconnect", connection.disconnect());
        lifecycle.connected = false;
        return Err(activation.fail(error, lifecycle));
    }
    let first_progress_generation = if let Some(mut marker) = cursor_recovery_marker.clone() {
        let terminal_generation =
            match writable_worker.wait_for_cursor_recovery_terminal(Duration::from_secs(10)) {
                Ok(generation) => generation,
                Err(error) => {
                    let error =
                        with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                    let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                    lifecycle.connected = false;
                    return Err(activation.fail(error, lifecycle));
                }
            };
        let marker_path = cursor_recovery_module_marker_path(&data_dir);
        // From the first Ready artifact write onward, every failure is a
        // replayable recovery cut. Never roll the owned registration back to
        // an earlier phase after a receipt or Ready journal may be durable.
        activation.disarm();
        if marker.phase == CursorRecoveryModulePhase::ReconcileArmed
            && marker.namespace_resume == CursorRecoveryNamespaceResume::ReconcileProgress
        {
            if let Err(error) =
                activation.mark_cursor_recovery_terminal_pending_finalize(&marker_path, &mut marker)
            {
                let error =
                    with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                lifecycle.connected = false;
                return Err(activation.fail(error, lifecycle));
            }
        }
        let restoring_preserved_ready = matches!(
            marker.phase,
            CursorRecoveryModulePhase::Ready | CursorRecoveryModulePhase::NamespaceFinalized
        ) && activation.journal().phase
            == RegistrationPhase::StateOwned;
        if marker.phase == CursorRecoveryModulePhase::ReconcileArmed {
            if let Err(error) =
                activation.mark_cursor_recovery_ready(&request, &marker_path, &mut marker)
            {
                let error =
                    with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                lifecycle.connected = false;
                return Err(activation.fail(error, lifecycle));
            }
        } else if restoring_preserved_ready {
            if let Err(error) = activation.mark_ready(&request) {
                let error =
                    with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                lifecycle.connected = false;
                return Err(activation.fail(error, lifecycle));
            }
        }
        if marker.phase == CursorRecoveryModulePhase::Ready || restoring_preserved_ready {
            let ready_observed = CloudFilesModuleEvent::CursorRecoveryReadyObserved(
                crate::protocol::CloudFilesCursorRecoveryReadyObservedRequest {
                    schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
                    recovery_id: marker.recovery_id.clone(),
                    recovery_token: marker.recovery_token.clone(),
                    sync_root: marker.identity.sync_root.clone(),
                    root_path: marker.identity.root_path.clone(),
                    sync_root_file_id: marker.sync_root_file_id,
                    recovery_cycle: marker.recovery_cycle,
                },
            );
            if let Err(detail) = host_emit_event_ack(&ready_observed) {
                let error = CloudFilesModuleError {
                    code: "cursor-recovery-host-ready-ack".to_string(),
                    detail,
                };
                let error =
                    with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                lifecycle.connected = false;
                return Err(activation.fail(error, lifecycle));
            }
        }
        // Persist the successor before removing the inner Terminal rewrite.
        // A restart can therefore distinguish "host acked, inner present"
        // from "host acked, inner already finalized" without guessing.
        if marker.phase == CursorRecoveryModulePhase::Ready {
            if let Err(error) =
                activation.mark_cursor_recovery_namespace_finalized(&marker_path, &mut marker)
            {
                let error =
                    with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                lifecycle.connected = false;
                return Err(activation.fail(error, lifecycle));
            }
        }
        if let Err(error) =
            writable_worker.finalize_cursor_recovery_after_host_ready(Duration::from_secs(10))
        {
            let error = with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
            let error = with_cleanup_result(error, "disconnect", connection.disconnect());
            lifecycle.connected = false;
            return Err(activation.fail(error, lifecycle));
        }
        if let Err(error) = writable_worker.release_cursor_recovery() {
            let error = with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
            let error = with_cleanup_result(error, "disconnect", connection.disconnect());
            lifecycle.connected = false;
            return Err(activation.fail(error, lifecycle));
        }
        match writable_worker.wait_for_first_progress(Duration::from_secs(5)) {
            Ok(generation) => {
                let handoff = run_cursor_recovery_terminal_handoff_with(
                    &mut marker,
                    |step, marker| match step {
                        CursorRecoveryTerminalHandoffStep::AcknowledgeNamespaceFinalized
                        | CursorRecoveryTerminalHandoffStep::AcknowledgeCompletionAuthority
                        | CursorRecoveryTerminalHandoffStep::AcknowledgeNormalization
                        | CursorRecoveryTerminalHandoffStep::AcknowledgeAuthorityRelease => {
                            let code = match step {
                                CursorRecoveryTerminalHandoffStep::AcknowledgeNamespaceFinalized => {
                                    "cursor-recovery-host-active-ack"
                                }
                                CursorRecoveryTerminalHandoffStep::AcknowledgeCompletionAuthority => {
                                    "cursor-recovery-host-completion-authority-ack"
                                }
                                CursorRecoveryTerminalHandoffStep::AcknowledgeNormalization => {
                                    "cursor-recovery-host-normalization-ack"
                                }
                                CursorRecoveryTerminalHandoffStep::AcknowledgeAuthorityRelease => {
                                    "cursor-recovery-host-authority-release-ack"
                                }
                                _ => unreachable!("matched acknowledged terminal step"),
                            };
                            let event = cursor_recovery_active_observed_event(marker)?;
                            host_emit_event_ack(&event).map_err(|detail| CloudFilesModuleError {
                                code: code.to_string(),
                                detail,
                            })
                        }
                        CursorRecoveryTerminalHandoffStep::PersistCompletionAuthorized => {
                            activation
                                .mark_cursor_recovery_completion_authorized(&marker_path, marker)
                        }
                        CursorRecoveryTerminalHandoffStep::NormalizeRegistration => {
                            normalize_completed_cursor_recovery_journal(&data_dir, marker)
                        }
                        CursorRecoveryTerminalHandoffStep::ClearCompletionAuthorized => {
                            clear_completion_authorized_cursor_recovery_module_marker(
                                &data_dir, marker,
                            )
                        }
                        CursorRecoveryTerminalHandoffStep::CompleteWorker => {
                            writable_worker.complete_cursor_recovery_after_host_active()
                        }
                        CursorRecoveryTerminalHandoffStep::ReleasePopulation => {
                            if let Some(released) = provider_population_released.as_ref() {
                                released.store(true, std::sync::atomic::Ordering::SeqCst);
                            }
                            Ok(())
                        }
                    },
                    host_is_cancelled,
                );
                if let Err(error) = handoff {
                    let error =
                        with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                    let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                    lifecycle.connected = false;
                    return Err(activation.fail(error, lifecycle));
                }
                host_log(&format!(
                    "Cloud Files cursor recovery reached terminal generation {terminal_generation} and became Active at generation {generation}"
                ));
                generation
            }
            Err(error) => {
                let error =
                    with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                lifecycle.connected = false;
                return Err(activation.fail(error, lifecycle));
            }
        }
    } else {
        let generation = match writable_worker.wait_for_first_progress(Duration::from_secs(5)) {
            Ok(generation) => generation,
            Err(error) => {
                let error =
                    with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
                let error = with_cleanup_result(error, "disconnect", connection.disconnect());
                lifecycle.connected = false;
                return Err(activation.fail(error, lifecycle));
            }
        };
        if let Err(error) = activation.mark_ready(&request) {
            let error = with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
            let error = with_cleanup_result(error, "disconnect", connection.disconnect());
            lifecycle.connected = false;
            return Err(activation.fail(error, lifecycle));
        }
        if let Some(released) = provider_population_released.as_ref() {
            released.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        generation
    };
    activation.disarm();
    match crate::windows_shell::configure_sync_root_status_schema(&request.sync_root) {
        Ok(()) => writable_worker.enable_explorer_status(),
        Err(error) => host_log(&format!(
            "Cloud Files Explorer custom status is unavailable: {error}"
        )),
    }
    host_log(&format!(
        "Cloud Files registration became ready after worker progress generation {first_progress_generation}"
    ));

    host_log(&format!(
        "Cloud Files provider connected {} -> {}",
        request.sync_root.display(),
        request.root_path
    ));
    let started = Instant::now();
    let timeout = request.max_run_seconds.map(|max_run_seconds| {
        Duration::from_secs(max_run_seconds).saturating_sub(started.elapsed())
    });
    match writable_worker.wait_for_stop_or_cancellation(cancellation_event_handle, timeout)? {
        windows_adapter::WindowsWritableWorkerWait::Cancelled
        | windows_adapter::WindowsWritableWorkerWait::TimedOut => {}
        windows_adapter::WindowsWritableWorkerWait::Stopped => {
            let failure = writable_worker
                .take_failure()
                .unwrap_or_else(|| CloudFilesModuleError {
                    code: "writable-worker-stopped".to_string(),
                    detail: "writable Cloud Files worker stopped without a terminal status"
                        .to_string(),
                });
            writable_worker.stop_and_join()?;
            connection.disconnect()?;
            lifecycle.connected = false;
            return Err(failure);
        }
    }
    writable_worker.stop_and_join()?;
    connection.disconnect()?;
    lifecycle.connected = false;
    mounted_mutation_handoff
        .close_clean()
        .map_err(|detail| CloudFilesModuleError {
            code: "mounted-mutation-session-close".to_string(),
            detail,
        })?;
    lifecycle.emit_disconnected();
    Ok(CloudFilesMountResponse {
        sync_root: request.sync_root,
        root_path: request.root_path,
        placeholders_created,
        connected: true,
        detail: "Cloud Files provider disconnected after cancellation".to_string(),
    })
}

fn callback_table() -> [CF_CALLBACK_REGISTRATION; 5] {
    [
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_FETCH_DATA,
            Callback: Some(fetch_data_callback),
        },
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_CANCEL_FETCH_DATA,
            Callback: Some(cancel_fetch_callback),
        },
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_FETCH_PLACEHOLDERS,
            Callback: Some(fetch_placeholders_callback),
        },
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_CANCEL_FETCH_PLACEHOLDERS,
            Callback: Some(cancel_fetch_callback),
        },
        CF_CALLBACK_REGISTRATION {
            Type: CF_CALLBACK_TYPE_NONE,
            Callback: None,
        },
    ]
}

unsafe extern "system" fn fetch_data_callback(
    callback_info: *const CF_CALLBACK_INFO,
    callback_parameters: *const CF_CALLBACK_PARAMETERS,
) {
    let (Some(info), Some(parameters), Some(provider)) = (unsafe {
        (
            callback_info.as_ref(),
            callback_parameters.as_ref(),
            callback_info
                .as_ref()
                .and_then(|info| (info.CallbackContext as *const CloudFilesProvider).as_ref()),
        )
    }) else {
        return;
    };
    let request = provider.begin_request(info.RequestKey);
    let fetch = unsafe { parameters.Anonymous.FetchData };
    let callback_range = HydrationCallbackRange {
        offset: fetch.RequiredFileOffset,
        length: fetch.RequiredLength,
    };
    let mut plan =
        match HydrationPlan::new(callback_range.offset, callback_range.length, info.FileSize) {
            Ok(plan) => plan,
            Err(error) => {
                host_log(&format!("Cloud Files hydration range rejected: {error}"));
                let _ = unsafe {
                    transfer_data(
                        info,
                        None,
                        callback_range.offset,
                        callback_range.length,
                        STATUS_CLOUD_FILE_UNSUCCESSFUL,
                    )
                };
                return;
            }
        };
    let callback_range = plan.callback;
    if request.is_cancelled() {
        let _ = unsafe {
            transfer_data(
                info,
                None,
                callback_range.offset,
                callback_range.length,
                STATUS_CANCELLED,
            )
        };
        return;
    }
    let identity = match unsafe { versioned_identity_for_fetch_data(info) } {
        Ok(identity) => identity,
        Err(error) => {
            host_log(&format!("Cloud Files exact hydration rejected: {error}"));
            let _ = unsafe {
                transfer_data(
                    info,
                    None,
                    callback_range.offset,
                    callback_range.length,
                    ntstatus_for_fetch_error(&error),
                )
            };
            return;
        }
    };

    while let Some(chunk) = plan.next_chunk() {
        if request.is_cancelled() {
            let _ = unsafe {
                transfer_data(
                    info,
                    None,
                    callback_range.offset,
                    callback_range.length,
                    STATUS_CANCELLED,
                )
            };
            return;
        }

        let bytes = match provider.read_range(
            &identity,
            chunk.offset,
            chunk.length,
            info.RequestKey,
        ) {
            Ok(bytes) if bytes.len() == chunk.length => bytes,
            Ok(bytes) => {
                host_log(&format!(
                    "Cloud Files exact hydration returned {} bytes for {} requested bytes at offset {}",
                    bytes.len(),
                    chunk.length,
                    chunk.offset
                ));
                let _ = unsafe {
                    transfer_data(
                        info,
                        None,
                        callback_range.offset,
                        callback_range.length,
                        STATUS_CLOUD_FILE_UNSUCCESSFUL,
                    )
                };
                return;
            }
            Err(error) => {
                host_log(&format!("Cloud Files exact hydration failed: {error}"));
                let _ = unsafe {
                    transfer_data(
                        info,
                        None,
                        callback_range.offset,
                        callback_range.length,
                        ntstatus_for_fetch_error(&error),
                    )
                };
                return;
            }
        };
        if request.is_cancelled() {
            let _ = unsafe {
                transfer_data(
                    info,
                    None,
                    callback_range.offset,
                    callback_range.length,
                    STATUS_CANCELLED,
                )
            };
            return;
        }
        let transfer_result = unsafe {
            transfer_data(
                info,
                Some(&bytes),
                chunk.offset as i64,
                bytes.len() as i64,
                STATUS_SUCCESS,
            )
        };
        if let Err(error) = transfer_result {
            host_log(&format!(
                "Cloud Files TRANSFER_DATA failed at offset {} for {} bytes: {error}",
                chunk.offset,
                bytes.len()
            ));
            let _ = unsafe {
                transfer_data(
                    info,
                    None,
                    callback_range.offset,
                    callback_range.length,
                    STATUS_CLOUD_FILE_UNSUCCESSFUL,
                )
            };
            return;
        }
        if let Err(error) = plan.advance(bytes.len()) {
            host_log(&format!("Cloud Files hydration progress failed: {error}"));
            let _ = unsafe {
                transfer_data(
                    info,
                    None,
                    callback_range.offset,
                    callback_range.length,
                    STATUS_CLOUD_FILE_UNSUCCESSFUL,
                )
            };
            return;
        }
    }
}

unsafe extern "system" fn cancel_fetch_callback(
    callback_info: *const CF_CALLBACK_INFO,
    _callback_parameters: *const CF_CALLBACK_PARAMETERS,
) {
    let (Some(info), Some(provider)) = (unsafe {
        (
            callback_info.as_ref(),
            callback_info
                .as_ref()
                .and_then(|info| (info.CallbackContext as *const CloudFilesProvider).as_ref()),
        )
    }) else {
        return;
    };
    provider.cancel_request(info.RequestKey);
}

unsafe extern "system" fn fetch_placeholders_callback(
    callback_info: *const CF_CALLBACK_INFO,
    _callback_parameters: *const CF_CALLBACK_PARAMETERS,
) {
    let (Some(info), Some(provider)) = (unsafe {
        (
            callback_info.as_ref(),
            callback_info
                .as_ref()
                .and_then(|info| (info.CallbackContext as *const CloudFilesProvider).as_ref()),
        )
    }) else {
        return;
    };
    let request = provider.begin_request(info.RequestKey);
    let path = match unsafe { sifs_path_from_callback(info, &provider.root_path) } {
        Ok(path) => path,
        Err(status) => {
            let _ = unsafe {
                transfer_placeholders(
                    info,
                    Vec::new(),
                    0,
                    CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
                    status,
                )
            };
            return;
        }
    };
    if provider.blocks_population(&path) {
        host_log(&format!(
            "Cloud Files rejected placeholder population for `{path}` before the provider reached Ready"
        ));
        let _ = unsafe {
            transfer_placeholders(
                info,
                Vec::new(),
                0,
                CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
                STATUS_CLOUD_FILE_UNSUCCESSFUL,
            )
        };
        return;
    }
    let page_state = match after_durable_materialization_intent(
        || {
            windows_adapter::record_materialization_audit_intent(
                &provider.materialization_audit_intents,
                &path,
            )?;
            provider.writable_worker_wake.signal_materialization();
            Ok::<(), String>(())
        },
        || provider.begin_placeholder_page(&path),
    ) {
        Ok(Ok(state)) => state,
        Ok(Err(status)) => {
            let _ = unsafe {
                transfer_placeholders(
                    info,
                    Vec::new(),
                    0,
                    CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
                    status,
                )
            };
            return;
        }
        Err(detail) => {
            host_log(&format!(
                "Cloud Files materialization audit intent was not durable for `{path}`: {detail}"
            ));
            let _ = unsafe {
                transfer_placeholders(
                    info,
                    Vec::new(),
                    0,
                    CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
                    STATUS_CLOUD_FILE_UNSUCCESSFUL,
                )
            };
            return;
        }
    };
    let (placeholders, next_cursor) =
        match provider.placeholder_payload_page(&path, page_state.cursor.as_ref(), info.RequestKey)
        {
            Ok(page) => page,
            Err(status) => {
                provider.finish_placeholder_page(&path, None, page_state.entries_processed, false);
                let _ = unsafe {
                    transfer_placeholders(
                        info,
                        Vec::new(),
                        0,
                        CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
                        status,
                    )
                };
                return;
            }
        };
    if request.is_cancelled() {
        provider.finish_placeholder_page(&path, None, page_state.entries_processed, false);
        let _ = unsafe {
            transfer_placeholders(
                info,
                Vec::new(),
                0,
                CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
                STATUS_CANCELLED,
            )
        };
        return;
    }
    let placeholder_count = placeholders.len() as u32;
    let prospective_entries_processed = page_state
        .entries_processed
        .saturating_add(placeholder_count);
    let (placeholder_total_count, transfer_flags) =
        placeholder_page_transfer_plan(prospective_entries_processed, next_cursor.is_some());
    let page_outcome = unsafe {
        transfer_placeholders(
            info,
            placeholders,
            placeholder_total_count,
            transfer_flags,
            STATUS_SUCCESS,
        )
    };
    if !page_outcome.fully_succeeded() {
        host_log(&format!(
            "Cloud Files placeholder page was not committed for `{path}`: {}",
            page_outcome.detail()
        ));
        provider.finish_placeholder_page(&path, None, page_state.entries_processed, false);
        return;
    }
    let next_entries_processed = page_state
        .entries_processed
        .saturating_add(page_outcome.entries_processed);
    provider.finish_placeholder_page(&path, next_cursor, next_entries_processed, true);
}

fn placeholder_page_transfer_plan(
    prospective_entries_processed: u32,
    has_next_cursor: bool,
) -> (i64, CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAGS) {
    if has_next_cursor {
        (
            i64::from(prospective_entries_processed).saturating_add(1),
            CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
        )
    } else {
        (
            i64::from(prospective_entries_processed),
            CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_DISABLE_ON_DEMAND_POPULATION,
        )
    }
}

unsafe fn transfer_data(
    info: &CF_CALLBACK_INFO,
    bytes: Option<&[u8]>,
    offset: i64,
    length: i64,
    status: NTSTATUS,
) -> windows::core::Result<()> {
    debug_assert!(
        status != STATUS_SUCCESS
            || bytes.is_some_and(|bytes| i64::try_from(bytes.len()) == Ok(length))
    );
    let mut params = CF_OPERATION_PARAMETERS {
        ParamSize: std::mem::size_of::<CF_OPERATION_PARAMETERS>() as u32,
        Anonymous: CF_OPERATION_PARAMETERS_0 {
            TransferData: CF_OPERATION_PARAMETERS_0_0 {
                Flags: CF_OPERATION_TRANSFER_DATA_FLAG_NONE,
                CompletionStatus: status,
                Buffer: bytes
                    .map(|bytes| bytes.as_ptr().cast::<c_void>())
                    .unwrap_or_else(ptr::null),
                Offset: offset,
                Length: length,
            },
        },
    };
    let op = operation_info(info, CF_OPERATION_TYPE_TRANSFER_DATA);
    unsafe { CfExecute(&op, &mut params) }
}

struct PlaceholderTransferOutcome {
    execute_result: windows::core::Result<()>,
    expected_count: u32,
    entries_processed: u32,
    entry_results: Vec<(String, i32)>,
}

impl PlaceholderTransferOutcome {
    fn fully_succeeded(&self) -> bool {
        let entry_results = self
            .entry_results
            .iter()
            .map(|(_, result)| *result)
            .collect::<Vec<_>>();
        placeholder_transfer_fully_succeeded(
            self.execute_result.is_ok(),
            self.expected_count,
            self.entries_processed,
            &entry_results,
        )
    }

    fn detail(&self) -> String {
        let execute = match &self.execute_result {
            Ok(()) => "CfExecute=success".to_string(),
            Err(error) => format!("CfExecute={error}"),
        };
        let results = self
            .entry_results
            .iter()
            .map(|(name, result)| format!("{name}=0x{:08X}", *result as u32))
            .collect::<Vec<_>>();
        format!(
            "{execute}; expected_count={}; entries_processed={}; placeholder_results=[{}]",
            self.expected_count,
            self.entries_processed,
            results.join(", ")
        )
    }
}

unsafe fn transfer_placeholders(
    info: &CF_CALLBACK_INFO,
    mut payloads: Vec<PlaceholderPayload>,
    placeholder_total_count: i64,
    flags: CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAGS,
    status: NTSTATUS,
) -> PlaceholderTransferOutcome {
    let mut infos = payloads
        .iter_mut()
        .map(PlaceholderPayload::create_info)
        .collect::<Vec<_>>();
    let expected_count = if status == STATUS_SUCCESS {
        infos.len() as u32
    } else {
        0
    };
    let mut params = CF_OPERATION_PARAMETERS {
        ParamSize: std::mem::size_of::<CF_OPERATION_PARAMETERS>() as u32,
        Anonymous: CF_OPERATION_PARAMETERS_0 {
            TransferPlaceholders: CF_OPERATION_PARAMETERS_0_4 {
                Flags: flags,
                CompletionStatus: status,
                PlaceholderTotalCount: placeholder_total_count,
                PlaceholderArray: if status == STATUS_SUCCESS && !infos.is_empty() {
                    infos.as_mut_ptr()
                } else {
                    ptr::null_mut()
                },
                PlaceholderCount: if status == STATUS_SUCCESS {
                    infos.len() as u32
                } else {
                    0
                },
                EntriesProcessed: 0,
            },
        },
    };
    let op = operation_info(info, CF_OPERATION_TYPE_TRANSFER_PLACEHOLDERS);
    let execute_result = unsafe { CfExecute(&op, &mut params) };
    let entries_processed = unsafe { params.Anonymous.TransferPlaceholders.EntriesProcessed };
    let entry_results = payloads
        .iter()
        .zip(infos.iter())
        .map(|(payload, info)| (payload.display_name.clone(), info.Result.0))
        .collect();
    PlaceholderTransferOutcome {
        execute_result,
        expected_count,
        entries_processed,
        entry_results,
    }
}

fn operation_info(
    info: &CF_CALLBACK_INFO,
    operation_type: windows::Win32::Storage::CloudFilters::CF_OPERATION_TYPE,
) -> CF_OPERATION_INFO {
    CF_OPERATION_INFO {
        StructSize: std::mem::size_of::<CF_OPERATION_INFO>() as u32,
        Type: operation_type,
        ConnectionKey: info.ConnectionKey,
        TransferKey: info.TransferKey,
        CorrelationVector: info.CorrelationVector as *const _,
        SyncStatus: ptr::null(),
        RequestKey: info.RequestKey,
    }
}

unsafe fn sifs_path_from_callback(
    info: &CF_CALLBACK_INFO,
    sync_root_sifs_path: &str,
) -> Result<String, NTSTATUS> {
    if !info.FileIdentity.is_null() && info.FileIdentityLength > 0 {
        return decoded_identity_path(unsafe {
            std::slice::from_raw_parts(
                info.FileIdentity.cast::<u8>(),
                info.FileIdentityLength as usize,
            )
        });
    }
    normalize_sifs_root(sync_root_sifs_path).map_err(|_| STATUS_OBJECT_NAME_NOT_FOUND)
}

unsafe fn versioned_identity_for_fetch_data(
    info: &CF_CALLBACK_INFO,
) -> Result<CloudFilesPlaceholderIdentity, CloudFilesFetchDataError> {
    if info.FileIdentity.is_null() || info.FileIdentityLength == 0 {
        return Err(stale_identity_error(
            CloudFilesStaleIdentityReason::LegacyPathOnly,
            None,
            None,
            "Cloud Files FETCH_DATA did not carry a versioned file identity",
        ));
    }
    let bytes = unsafe {
        std::slice::from_raw_parts(
            info.FileIdentity.cast::<u8>(),
            info.FileIdentityLength as usize,
        )
    };
    match decode_placeholder_identity(bytes) {
        Ok(DecodedCloudFilesPlaceholderIdentity::Versioned(identity)) => Ok(identity),
        Ok(DecodedCloudFilesPlaceholderIdentity::LegacyPath(path)) => Err(stale_identity_error(
            CloudFilesStaleIdentityReason::LegacyPathOnly,
            None,
            None,
            format!(
                "legacy path-only placeholder `{path}` cannot prove an immutable SIFS object version"
            ),
        )),
        Err(error) => Err(stale_identity_error(
            CloudFilesStaleIdentityReason::InvalidIdentity,
            None,
            None,
            error.to_string(),
        )),
    }
}

fn decoded_identity_path(bytes: &[u8]) -> Result<String, NTSTATUS> {
    let path = match decode_placeholder_identity(bytes) {
        Ok(DecodedCloudFilesPlaceholderIdentity::Versioned(identity)) => identity.path,
        Ok(DecodedCloudFilesPlaceholderIdentity::LegacyPath(path)) => path,
        Err(_) => return Err(STATUS_OBJECT_NAME_NOT_FOUND),
    };
    normalize_sifs_root(&path).map_err(|_| STATUS_OBJECT_NAME_NOT_FOUND)
}

struct CloudFilesProvider {
    dfs: Arc<Mutex<SifsService>>,
    mounted_mutation_handoff: Arc<CloudFilesMountedMutationHandoff>,
    mount_identity: String,
    sync_root: PathBuf,
    root_path: String,
    read_only: bool,
    population_released: Option<Arc<std::sync::atomic::AtomicBool>>,
    block_root_population_until_released: bool,
    cancelled_requests: Mutex<BTreeSet<i64>>,
    placeholder_pages: Mutex<BTreeMap<String, CloudFilesPlaceholderPageState>>,
    placeholder_page_leases: Mutex<BTreeSet<String>>,
    materialized_directories: Arc<Mutex<BTreeSet<String>>>,
    materialization_audit_intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
    writable_worker_wake: Arc<windows_adapter::WindowsWorkerWake>,
}

#[derive(Clone, Debug, Default)]
struct CloudFilesPlaceholderPageState {
    cursor: Option<dfs::DfsDirectoryPageCursor>,
    entries_processed: u32,
}

struct CloudFilesCallbackRequest<'provider> {
    provider: &'provider CloudFilesProvider,
    request_key: i64,
}

impl CloudFilesCallbackRequest<'_> {
    fn is_cancelled(&self) -> bool {
        self.provider.request_is_cancelled(self.request_key)
    }
}

impl Drop for CloudFilesCallbackRequest<'_> {
    fn drop(&mut self) {
        self.provider.finish_request(self.request_key);
    }
}

impl CloudFilesProvider {
    fn new(
        dfs: Arc<Mutex<SifsService>>,
        mounted_mutation_handoff: Arc<CloudFilesMountedMutationHandoff>,
        sync_root: PathBuf,
        root_path: String,
        read_only: bool,
        population_released: Option<Arc<std::sync::atomic::AtomicBool>>,
        block_root_population_until_released: bool,
        materialized_directories: Arc<Mutex<BTreeSet<String>>>,
        materialization_audit_intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
        writable_worker_wake: Arc<windows_adapter::WindowsWorkerWake>,
    ) -> Self {
        let mount_identity = sync_root.to_string_lossy().into_owned();
        Self {
            dfs,
            mounted_mutation_handoff,
            mount_identity,
            sync_root,
            root_path,
            read_only,
            population_released,
            block_root_population_until_released,
            cancelled_requests: Mutex::new(BTreeSet::new()),
            placeholder_pages: Mutex::new(BTreeMap::new()),
            placeholder_page_leases: Mutex::new(BTreeSet::new()),
            materialized_directories,
            materialization_audit_intents,
            writable_worker_wake,
        }
    }

    fn begin_request(&self, request_key: i64) -> CloudFilesCallbackRequest<'_> {
        CloudFilesCallbackRequest {
            provider: self,
            request_key,
        }
    }

    fn cancel_request(&self, request_key: i64) {
        if let Ok(mut cancelled) = self.cancelled_requests.lock() {
            cancelled.insert(request_key);
        }
    }

    fn request_is_cancelled(&self, request_key: i64) -> bool {
        self.cancelled_requests
            .lock()
            .is_ok_and(|cancelled| cancelled.contains(&request_key))
    }

    fn finish_request(&self, request_key: i64) {
        if let Ok(mut cancelled) = self.cancelled_requests.lock() {
            cancelled.remove(&request_key);
        }
    }

    fn begin_placeholder_page(
        &self,
        path: &str,
    ) -> Result<CloudFilesPlaceholderPageState, NTSTATUS> {
        let mut leases = self
            .placeholder_page_leases
            .try_lock()
            .map_err(|_| STATUS_CLOUD_FILE_UNSUCCESSFUL)?;
        if !leases.insert(path.to_string()) {
            return Err(STATUS_CLOUD_FILE_UNSUCCESSFUL);
        }
        drop(leases);
        self.placeholder_pages
            .lock()
            .map_err(|_| STATUS_UNSUCCESSFUL)
            .map(|pages| pages.get(path).cloned().unwrap_or_default())
    }

    fn blocks_population(&self, path: &str) -> bool {
        population_is_parked(
            self.population_released.as_deref(),
            self.block_root_population_until_released,
            path,
            &self.root_path,
        )
    }

    fn finish_placeholder_page(
        &self,
        path: &str,
        next_cursor: Option<dfs::DfsDirectoryPageCursor>,
        entries_processed: u32,
        committed: bool,
    ) {
        if committed && let Ok(mut pages) = self.placeholder_pages.lock() {
            if next_cursor.is_some() {
                pages.insert(
                    path.to_string(),
                    CloudFilesPlaceholderPageState {
                        cursor: next_cursor,
                        entries_processed,
                    },
                );
            } else {
                pages.remove(path);
            }
        }
        if committed && let Ok(mut materialized) = self.materialized_directories.lock() {
            materialized.insert(path.to_string());
        }
        if let Ok(mut leases) = self.placeholder_page_leases.lock() {
            leases.remove(path);
        }
    }

    fn lock_dfs(&self) -> Result<MutexGuard<'_, SifsService>, NTSTATUS> {
        let Some(deadline) = Instant::now().checked_add(CLOUD_FILES_DFS_LOCK_TIMEOUT) else {
            return Err(STATUS_UNSUCCESSFUL);
        };
        loop {
            match self.dfs.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(TryLockError::Poisoned(_)) => return Err(STATUS_UNSUCCESSFUL),
                Err(TryLockError::WouldBlock) if Instant::now() >= deadline => {
                    return Err(STATUS_CLOUD_FILE_UNSUCCESSFUL);
                }
                Err(TryLockError::WouldBlock) => thread::sleep(CLOUD_FILES_DFS_LOCK_RETRY),
            }
        }
    }

    fn placeholder_payload_page(
        &self,
        sifs_path: &str,
        after: Option<&dfs::DfsDirectoryPageCursor>,
        request_key: i64,
    ) -> Result<(Vec<PlaceholderPayload>, Option<dfs::DfsDirectoryPageCursor>), NTSTATUS> {
        if self.request_is_cancelled(request_key) {
            return Err(STATUS_CANCELLED);
        }
        let page = self
            .mounted_mutation_handoff
            .run_captured_read_ingest(
                &self.dfs,
                "cloud-files.provider-placeholder-directory-page",
                |dfs| {
                    dfs.capture_mounted_mutation(|dfs| {
                        dfs.posix_list_dir_distributed_page(
                            sifs_path,
                            after,
                            CLOUD_FILES_DIRECTORY_PAGE_ENTRIES,
                        )
                    })
                },
            )
            .map_err(|error| {
                host_log(&format!(
                    "Cloud Files placeholder directory refresh for `{sifs_path}` lost its durable handoff: {error}"
                ));
                STATUS_CLOUD_FILE_UNSUCCESSFUL
            })?
            .map_err(ntstatus_for_posix_error)?;
        let mut payloads = Vec::new();
        for child in page.entries {
            if self.request_is_cancelled(request_key) {
                return Err(STATUS_CANCELLED);
            }
            let stat = {
                let mut dfs = self.lock_dfs()?;
                dfs.posix_stat_distributed(&child.path)
                    .map_err(ntstatus_for_posix_error)?
            };
            if !is_visible_placeholder_kind(&stat.kind) {
                continue;
            }
            let local_path = self.local_path_for_sifs(&stat.path).map_err(|error| {
                host_log(&format!(
                    "Cloud Files refused unsafe placeholder path `{}`: {error}",
                    stat.path
                ));
                STATUS_CLOUD_FILE_UNSUCCESSFUL
            })?;
            let local_state = match local_placeholder_collision_state(&local_path) {
                Ok(state) => state,
                Err(error) => {
                    host_log(&format!(
                        "Cloud Files preserved unclassified local entry {} while populating `{}`: {error}",
                        local_path.display(),
                        stat.path
                    ));
                    LocalPlaceholderCollisionState::OtherLocalEntry
                }
            };
            let disposition = placeholder_create_disposition(local_state);
            if disposition == PlaceholderCreateDisposition::PreserveLocal {
                host_log(&format!(
                    "Cloud Files preserved {:?} at {} instead of superseding it from `{}`",
                    local_state,
                    local_path.display(),
                    stat.path
                ));
                continue;
            }
            let identity = self.placeholder_identity(&stat);
            payloads.push(
                PlaceholderPayload::new(child.name, identity, stat, self.read_only, disposition)
                    .map_err(|_| STATUS_CLOUD_FILE_UNSUCCESSFUL)?,
            );
        }
        Ok((payloads, page.next_cursor))
    }

    fn local_path_for_sifs(&self, path: &str) -> Result<PathBuf, String> {
        let root = normalize_sifs_root(&self.root_path).map_err(|error| error.to_string())?;
        let path = normalize_sifs_root(path).map_err(|error| error.to_string())?;
        let suffix = if root == "/" {
            path.trim_start_matches('/')
        } else if path == root {
            ""
        } else {
            path.strip_prefix(&(root.clone() + "/"))
                .ok_or_else(|| format!("SIFS path `{path}` is outside mounted root `{root}`"))?
        };
        let mut local = self.sync_root.clone();
        for component in suffix.split('/').filter(|component| !component.is_empty()) {
            if component == "." || component == ".." || component.contains('\\') {
                return Err(format!("invalid SIFS path component `{component}`"));
            }
            local.push(component);
        }
        Ok(local)
    }

    fn placeholder_identity(&self, stat: &dfs::DfsStat) -> CloudFilesPlaceholderIdentity {
        CloudFilesPlaceholderIdentity::new(
            self.mount_identity.clone(),
            self.root_path.clone(),
            stat.path.clone(),
            stat.object_id.to_string(),
            stat.version.to_string(),
            stat.content_hash.clone(),
        )
    }

    fn read_range(
        &self,
        expected: &CloudFilesPlaceholderIdentity,
        offset: u64,
        len: usize,
        request_key: i64,
    ) -> Result<Vec<u8>, CloudFilesFetchDataError> {
        if self.request_is_cancelled(request_key) {
            return Err(CloudFilesFetchDataError::Backend {
                code: "fetch-cancelled".to_string(),
                detail: "Cloud Files hydration was cancelled before authoritative stat".to_string(),
            });
        }
        if expected.mount != self.mount_identity {
            return Err(stale_identity_error(
                CloudFilesStaleIdentityReason::MountChanged,
                Some(expected.clone()),
                None,
                format!(
                    "placeholder mount `{}` does not match connected mount `{}`",
                    expected.mount, self.mount_identity
                ),
            ));
        }
        if expected.root != self.root_path {
            return Err(stale_identity_error(
                CloudFilesStaleIdentityReason::RootChanged,
                Some(expected.clone()),
                None,
                format!(
                    "placeholder root `{}` does not match connected root `{}`",
                    expected.root, self.root_path
                ),
            ));
        }

        let stat = match self
            .lock_dfs()
            .map_err(|status| CloudFilesFetchDataError::Backend {
                code: "dfs-lock-timeout".to_string(),
                detail: format!(
                    "Cloud Files DFS service was unavailable before the bounded lock deadline ({status:?})"
                ),
            })?
            .posix_stat_distributed(&expected.path)
        {
            Ok(stat) => stat,
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
                return Err(stale_identity_error(
                    CloudFilesStaleIdentityReason::PathMissing,
                    Some(expected.clone()),
                    None,
                    format!(
                        "placeholder path `{}` no longer resolves authoritatively",
                        expected.path
                    ),
                ));
            }
            Err(error) => {
                return Err(CloudFilesFetchDataError::Backend {
                    code: "authoritative-stat".to_string(),
                    detail: error.to_string(),
                });
            }
        };
        let observed = self.placeholder_identity(&stat);
        if let Some(reason) = placeholder_identity_mismatch(expected, &observed) {
            return Err(stale_identity_error(
                reason,
                Some(expected.clone()),
                Some(observed),
                format!(
                    "authoritative SIFS identity for `{}` no longer matches its Cloud Files placeholder",
                    expected.path
                ),
            ));
        }
        if self.request_is_cancelled(request_key) {
            return Err(CloudFilesFetchDataError::Backend {
                code: "fetch-cancelled".to_string(),
                detail: "Cloud Files hydration was cancelled before exact range read".to_string(),
            });
        }
        let mut dfs = self.lock_dfs().map_err(|status| CloudFilesFetchDataError::Backend {
            code: "dfs-lock-timeout".to_string(),
            detail: format!(
                "Cloud Files DFS service was unavailable before the bounded lock deadline ({status:?})"
            ),
        })?;
        match dfs.posix_read_file_range_for_opened_stat_outcome(&expected.path, &stat, offset, len)
        {
            Ok(dfs::DfsPosixDistributedFileReadOutcome::Found { bytes }) => Ok(bytes),
            Ok(dfs::DfsPosixDistributedFileReadOutcome::NotFound { .. }) => {
                Err(stale_identity_error(
                    CloudFilesStaleIdentityReason::PathMissing,
                    Some(expected.clone()),
                    Some(observed),
                    "exact immutable content disappeared after authoritative stat",
                ))
            }
            Ok(dfs::DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail }) => {
                Err(CloudFilesFetchDataError::ContentUnavailable { path, detail })
            }
            Err(error) => Err(CloudFilesFetchDataError::Backend {
                code: "exact-range-read".to_string(),
                detail: error.to_string(),
            }),
        }
    }
}

fn population_is_parked(
    released: Option<&std::sync::atomic::AtomicBool>,
    block_root_population: bool,
    path: &str,
    root: &str,
) -> bool {
    released.is_some_and(|released| !released.load(std::sync::atomic::Ordering::SeqCst))
        && (block_root_population || path != root)
}

fn is_visible_placeholder_kind(kind: &DfsObjectKind) -> bool {
    !matches!(
        cloud_files_object_projection(kind),
        CloudFilesObjectProjection::Unsupported(_)
    )
}

struct PlaceholderPayload {
    display_name: String,
    relative_name: Vec<u16>,
    identity: Vec<u8>,
    metadata: CF_FS_METADATA,
    flags: windows::Win32::Storage::CloudFilters::CF_PLACEHOLDER_CREATE_FLAGS,
}

impl PlaceholderPayload {
    fn new(
        relative_name: String,
        identity: CloudFilesPlaceholderIdentity,
        stat: dfs::DfsStat,
        read_only: bool,
        disposition: PlaceholderCreateDisposition,
    ) -> Result<Self, CloudFilesModuleError> {
        let projection = cloud_files_object_projection(&stat.kind);
        let is_directory = match projection {
            CloudFilesObjectProjection::File => false,
            CloudFilesObjectProjection::Directory => true,
            CloudFilesObjectProjection::Unsupported(kind) => {
                return Err(CloudFilesModuleError {
                    code: "unsupported-placeholder-kind".to_string(),
                    detail: format!(
                        "SIFS {kind} `{}` cannot be represented as a Windows Cloud Files placeholder",
                        stat.path
                    ),
                });
            }
        };
        let mut attributes = if is_directory {
            FILE_ATTRIBUTE_DIRECTORY.0
        } else {
            FILE_ATTRIBUTE_NORMAL.0
        };
        if read_only {
            attributes |= FILE_ATTRIBUTE_READONLY.0;
        }
        let size = if is_directory {
            0
        } else {
            stat.size_bytes.unwrap_or(0) as i64
        };
        let flags = match disposition {
            PlaceholderCreateDisposition::CreateAbsent => CF_PLACEHOLDER_CREATE_FLAG_MARK_IN_SYNC,
            PlaceholderCreateDisposition::PreserveLocal => {
                return Err(CloudFilesModuleError {
                    code: "preserved-local-placeholder-collision".to_string(),
                    detail: format!(
                        "refusing to construct a placeholder for preserved local path `{}`",
                        stat.path
                    ),
                });
            }
        };
        Ok(Self {
            display_name: relative_name.clone(),
            relative_name: wide_str(&relative_name),
            identity: encode_placeholder_identity(&identity)?,
            metadata: CF_FS_METADATA {
                BasicInfo: FILE_BASIC_INFO {
                    CreationTime: unix_nanos_to_filetime(stat.created_at_unix_nanos),
                    LastAccessTime: unix_nanos_to_filetime(stat.accessed_at_unix_nanos),
                    LastWriteTime: unix_nanos_to_filetime(stat.modified_at_unix_nanos),
                    ChangeTime: unix_nanos_to_filetime(stat.changed_at_unix_nanos),
                    FileAttributes: attributes,
                },
                FileSize: size,
            },
            flags,
        })
    }

    fn create_info(&mut self) -> CF_PLACEHOLDER_CREATE_INFO {
        CF_PLACEHOLDER_CREATE_INFO {
            RelativeFileName: PCWSTR(self.relative_name.as_ptr()),
            FsMetadata: self.metadata,
            FileIdentity: self.identity.as_ptr().cast::<c_void>(),
            FileIdentityLength: self.identity.len() as u32,
            Flags: self.flags,
            Result: windows::core::HRESULT(0),
            CreateUsn: 0,
        }
    }
}

fn create_placeholders(
    base: &Path,
    mut payloads: Vec<PlaceholderPayload>,
) -> Result<u64, CloudFilesModuleError> {
    if payloads.is_empty() {
        return Ok(0);
    }
    let base_wide = wide_path(base);
    let mut infos = payloads
        .iter_mut()
        .map(PlaceholderPayload::create_info)
        .collect::<Vec<_>>();
    let expected_count = infos.len() as u32;
    let mut entries_processed = 0;
    let execute_result = unsafe {
        CfCreatePlaceholders(
            PCWSTR(base_wide.as_ptr()),
            &mut infos,
            CF_CREATE_FLAG_NONE,
            Some(&mut entries_processed),
        )
    };
    let entry_results = infos.iter().map(|info| info.Result.0).collect::<Vec<_>>();
    if !placeholder_transfer_fully_succeeded(
        execute_result.is_ok(),
        expected_count,
        entries_processed,
        &entry_results,
    ) {
        return Err(CloudFilesModuleError {
            code: "create-placeholders".to_string(),
            detail: format!(
                "create Cloud Files placeholders under {}: {}; expected_count={expected_count}; entries_processed={entries_processed}; {}",
                base.display(),
                execute_result
                    .as_ref()
                    .err()
                    .map_or_else(|| "per-entry failure".to_string(), ToString::to_string),
                placeholder_results_detail(&payloads, &infos)
            ),
        });
    }
    Ok(entries_processed as u64)
}

fn placeholder_results_detail(
    payloads: &[PlaceholderPayload],
    infos: &[CF_PLACEHOLDER_CREATE_INFO],
) -> String {
    let results = payloads
        .iter()
        .zip(infos.iter())
        .map(|(payload, info)| format!("{}=0x{:08X}", payload.display_name, info.Result.0 as u32))
        .collect::<Vec<_>>();
    if results.is_empty() {
        "placeholder_results=[]".to_string()
    } else {
        format!("placeholder_results=[{}]", results.join(", "))
    }
}

#[derive(Debug)]
struct RegisteredSyncRootInfo {
    provider_name: String,
    provider_version: String,
    sync_root_file_id: u64,
    root_identity: Vec<u8>,
}

struct OwnedWin32Handle(HANDLE);

impl Drop for OwnedWin32Handle {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.0);
        }
    }
}

fn local_placeholder_collision_state(
    path: &Path,
) -> Result<LocalPlaceholderCollisionState, String> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LocalPlaceholderCollisionState::Absent);
        }
        Err(error) => {
            return Err(format!(
                "inspect local placeholder collision {}: {error}",
                path.display()
            ));
        }
    };
    if metadata.file_type().is_symlink() {
        return Ok(LocalPlaceholderCollisionState::OtherLocalEntry);
    }
    match query_placeholder_in_sync(path, metadata.is_dir())? {
        Some(true) => Ok(LocalPlaceholderCollisionState::CleanPlaceholder),
        Some(false) => Ok(LocalPlaceholderCollisionState::DirtyPlaceholder),
        None => Ok(LocalPlaceholderCollisionState::OtherLocalEntry),
    }
}

fn query_placeholder_in_sync(path: &Path, is_directory: bool) -> Result<Option<bool>, String> {
    let path_wide = wide_path(path);
    let flags = if is_directory {
        FILE_FLAG_BACKUP_SEMANTICS
    } else {
        Default::default()
    };
    let handle = unsafe {
        CreateFileW(
            PCWSTR(path_wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            flags,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| format!("open local collision {}: {error}", path.display()))?;
    let mut buffer =
        vec![0u64; (std::mem::size_of::<CF_PLACEHOLDER_STANDARD_INFO>() + 4096).div_ceil(8)];
    let result = unsafe {
        CfGetPlaceholderInfo(
            handle.0,
            CF_PLACEHOLDER_INFO_STANDARD,
            buffer.as_mut_ptr().cast::<c_void>(),
            (buffer.len() * std::mem::size_of::<u64>()) as u32,
            None,
        )
    };
    if result.is_err() {
        return Ok(None);
    }
    let info = unsafe { &*buffer.as_ptr().cast::<CF_PLACEHOLDER_STANDARD_INFO>() };
    Ok(Some(
        info.InSyncState == CF_IN_SYNC_STATE_IN_SYNC && info.ModifiedDataSize == 0,
    ))
}

fn sync_root_file_index(path: &Path) -> Result<u64, CloudFilesModuleError> {
    let path_wide = wide_path(path);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(path_wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|source| CloudFilesModuleError {
        code: "query-sync-root-open".to_string(),
        detail: format!(
            "open sync root for ownership verification {}: {source}",
            path.display()
        ),
    })?;
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle.0, &mut info) }.map_err(|source| {
        CloudFilesModuleError {
            code: "query-sync-root-file-id".to_string(),
            detail: format!(
                "read stable file ID for sync root {}: {source}",
                path.display()
            ),
        }
    })?;
    Ok(((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64)
}

impl CursorRecoveryRootFence {
    fn acquire(path: &Path) -> Result<Self, CloudFilesModuleError> {
        ensure_sync_root_is_plain_directory(path, "cursor recovery")?;
        let path_wide = wide_path(path);
        let handle = unsafe {
            CreateFileW(
                PCWSTR(path_wide.as_ptr()),
                FILE_READ_ATTRIBUTES.0,
                // Deliberately omit FILE_SHARE_DELETE so replacement cannot
                // race the durable authorization and StateOwned boundary.
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                None,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                None,
            )
        }
        .map(OwnedWin32Handle)
        .map_err(|source| CloudFilesModuleError {
            code: "cursor-recovery-root-fence".to_string(),
            detail: format!(
                "open cursor recovery root fence {}: {source}",
                path.display()
            ),
        })?;
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        unsafe { GetFileInformationByHandle(handle.0, &mut info) }.map_err(|source| {
            CloudFilesModuleError {
                code: "cursor-recovery-root-fence".to_string(),
                detail: format!(
                    "read cursor recovery root fence identity {}: {source}",
                    path.display()
                ),
            }
        })?;
        Ok(Self {
            _handle: handle,
            file_id: ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64,
        })
    }
}

fn resolve_data_dir(
    data_dir_root: Option<PathBuf>,
) -> Result<SinomoDataDir, CloudFilesModuleError> {
    match data_dir_root {
        Some(path) => Ok(SinomoDataDir::new(path)),
        None => SinomoDataDir::from_env_or_home().map_err(error("local-data")),
    }
}

fn registration_receipt_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .module_runtime_dir()
        .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(CLOUD_FILES_REGISTRATION_RECEIPT_FILE)
}

fn registration_journal_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .module_runtime_dir()
        .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
        .join(CLOUD_FILES_REGISTRATION_JOURNAL_FILE)
}

fn read_bounded_plain_registration_file(
    path: &Path,
    maximum_bytes: u64,
    label: &str,
) -> Result<Option<Vec<u8>>, CloudFilesModuleError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(CloudFilesModuleError {
                code: "registration-artifact-inspect".to_string(),
                detail: format!("inspect {label} {}: {source}", path.display()),
            });
        }
    };
    if !metadata.file_type().is_file()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        || metadata.len() > maximum_bytes
    {
        return Err(CloudFilesModuleError {
            code: "registration-artifact-invalid".to_string(),
            detail: format!(
                "{label} {} is reparse, non-file, or exceeds {maximum_bytes} bytes",
                path.display()
            ),
        });
    }
    let mut file = OpenOptions::new()
        .create(false)
        .read(true)
        .open(path)
        .map_err(|source| CloudFilesModuleError {
            code: "registration-artifact-open".to_string(),
            detail: format!("open {label} {}: {source}", path.display()),
        })?;
    let opened_length = file
        .metadata()
        .map_err(|source| CloudFilesModuleError {
            code: "registration-artifact-inspect".to_string(),
            detail: format!("inspect open {label} {}: {source}", path.display()),
        })?
        .len();
    if opened_length != metadata.len() || opened_length > maximum_bytes {
        return Err(CloudFilesModuleError {
            code: "registration-artifact-changed".to_string(),
            detail: format!("{label} {} changed before it was read", path.display()),
        });
    }
    let mut bytes = Vec::with_capacity(opened_length as usize);
    file.read_to_end(&mut bytes)
        .map_err(|source| CloudFilesModuleError {
            code: "registration-artifact-read".to_string(),
            detail: format!("read {label} {}: {source}", path.display()),
        })?;
    if bytes.len() as u64 != opened_length
        || file
            .metadata()
            .map_err(|source| CloudFilesModuleError {
                code: "registration-artifact-inspect".to_string(),
                detail: format!("reinspect open {label} {}: {source}", path.display()),
            })?
            .len()
            != opened_length
    {
        return Err(CloudFilesModuleError {
            code: "registration-artifact-changed".to_string(),
            detail: format!("{label} {} changed while it was read", path.display()),
        });
    }
    Ok(Some(bytes))
}

fn decode_canonical_registration_artifact<T: DeserializeOwned + Serialize>(
    path: &Path,
    bytes: &[u8],
    code: &str,
    label: &str,
) -> Result<T, CloudFilesModuleError> {
    let value = decode_sobj::<T>(bytes).map_err(|source| CloudFilesModuleError {
        code: format!("{code}-decode"),
        detail: format!("decode {label} {}: {source}", path.display()),
    })?;
    if encode_sobj(&value).map_err(|source| CloudFilesModuleError {
        code: format!("{code}-encode"),
        detail: format!("re-encode {label}: {source}"),
    })? != bytes
    {
        return Err(CloudFilesModuleError {
            code: format!("{code}-noncanonical"),
            detail: format!("{label} {} is not canonically encoded", path.display()),
        });
    }
    Ok(value)
}

fn inspect_registration_artifact_snapshot<T: DeserializeOwned + Serialize>(
    path: &Path,
    temporary: &Path,
    maximum_bytes: u64,
    code: &str,
    label: &str,
) -> Result<RegistrationArtifactSnapshot<T>, CloudFilesModuleError> {
    let current = read_bounded_plain_registration_file(path, maximum_bytes, label)?
        .map(|bytes| decode_canonical_registration_artifact(path, &bytes, code, label))
        .transpose()?;
    let temporary_value = read_bounded_plain_registration_file(
        temporary,
        maximum_bytes,
        &format!("{label} temporary file"),
    )?
    .map(|bytes| decode_canonical_registration_artifact(temporary, &bytes, code, label))
    .transpose()?;
    Ok(RegistrationArtifactSnapshot {
        current,
        temporary: temporary_value,
    })
}

fn promote_exact_registration_artifact<T: DeserializeOwned + Serialize + Eq>(
    path: &Path,
    temporary: &Path,
    maximum_bytes: u64,
    expected: &T,
    code: &str,
    label: &str,
) -> Result<(), CloudFilesModuleError> {
    let snapshot =
        inspect_registration_artifact_snapshot::<T>(path, temporary, maximum_bytes, code, label)?;
    match (snapshot.current, snapshot.temporary) {
        (Some(current), None) if current == *expected => Ok(()),
        (None, Some(staged)) if staged == *expected => {
            let temporary_wide = wide_path(temporary);
            let path_wide = wide_path(path);
            unsafe {
                MoveFileExW(
                    PCWSTR(temporary_wide.as_ptr()),
                    PCWSTR(path_wide.as_ptr()),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            }
            .map_err(|source| CloudFilesModuleError {
                code: format!("{code}-commit"),
                detail: format!("promote exact {label} {}: {source}", path.display()),
            })
        }
        (Some(current), Some(staged)) if current == *expected && staged == *expected => {
            sinomo_runtime::storage::durable_file::remove_file_if_exists_and_sync_parent(temporary)
                .map_err(|source| CloudFilesModuleError {
                    code: format!("{code}-temp-cleanup"),
                    detail: format!("remove redundant exact {label} temporary file: {source}"),
                })?;
            Ok(())
        }
        _ => Err(CloudFilesModuleError {
            code: format!("{code}-replay-mismatch"),
            detail: format!("{label} current/temporary cut is not the exact expected image"),
        }),
    }
}

fn load_registration_journal(
    data_dir: &SinomoDataDir,
) -> Result<Option<RegistrationJournal>, CloudFilesModuleError> {
    let path = registration_journal_path(data_dir);
    let Some(bytes) = read_bounded_plain_registration_file(
        &path,
        CLOUD_FILES_REGISTRATION_JOURNAL_MAX_BYTES,
        "Cloud Files registration journal",
    )?
    else {
        return Ok(None);
    };
    let journal: RegistrationJournal = decode_canonical_registration_artifact(
        &path,
        &bytes,
        "registration-journal",
        "Cloud Files registration journal",
    )?;
    journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    Ok(Some(journal))
}

fn persist_registration_journal(
    data_dir: &SinomoDataDir,
    journal: &RegistrationJournal,
) -> Result<(), CloudFilesModuleError> {
    journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    let path = registration_journal_path(data_dir);
    if journal.cursor_recovery.is_some() {
        persist_cursor_recovery_registration_artifact(
            &path,
            journal,
            "registration-journal",
            "Cloud Files registration journal",
        )
    } else {
        persist_ordinary_registration_artifact(
            &path,
            journal,
            "registration-journal",
            "Cloud Files registration journal",
        )
    }
}

fn normalize_completed_cursor_recovery_journal(
    data_dir: &SinomoDataDir,
    marker: &CursorRecoveryModuleMarker,
) -> Result<(), CloudFilesModuleError> {
    if marker.phase != CursorRecoveryModulePhase::CompletionAuthorized {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-completion-not-authorized".to_string(),
            detail: "only CompletionAuthorized may normalize cursor provenance".to_string(),
        });
    }
    let current = load_registration_journal(data_dir)?.ok_or_else(|| CloudFilesModuleError {
        code: "cursor-recovery-completion-journal-missing".to_string(),
        detail: "CompletionAuthorized recovery lost its Ready journal".to_string(),
    })?;
    let mut expected = cursor_recovery_expected_registration_artifacts(marker)?.0;
    expected.phase = RegistrationPhase::Ready;
    let mut ordinary = expected.clone();
    ordinary.clear_cursor_recovery();
    if current == ordinary {
        return Ok(());
    }
    if current != expected {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-completion-journal-mismatch".to_string(),
            detail: "CompletionAuthorized journal is not the exact cycle-bound Ready predecessor"
                .to_string(),
        });
    }
    persist_cursor_recovery_registration_artifact(
        &registration_journal_path(data_dir),
        &ordinary,
        "registration-journal",
        "Cloud Files registration journal",
    )
}

fn load_registration_receipt(
    path: &Path,
) -> Result<Option<CloudFilesRegistrationReceipt>, CloudFilesModuleError> {
    let Some(bytes) = read_bounded_plain_registration_file(
        path,
        CLOUD_FILES_REGISTRATION_RECEIPT_MAX_BYTES,
        "Cloud Files registration receipt",
    )?
    else {
        return Ok(None);
    };
    decode_canonical_registration_artifact(
        path,
        &bytes,
        "registration-receipt",
        "Cloud Files registration receipt",
    )
    .map(Some)
}

fn persist_registration_receipt(
    request: &CloudFilesMountRequest,
    journal: &RegistrationJournal,
    data_dir: &SinomoDataDir,
) -> Result<(), CloudFilesModuleError> {
    let path = registration_receipt_path(data_dir);
    let receipt = CloudFilesRegistrationReceipt {
        interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
        registration_identity_interface_version: (journal.interface_version
            != SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION)
            .then(|| journal.interface_version.clone()),
        sync_root: request.sync_root.clone(),
        root_path: request.root_path.clone(),
        provider_name: request.provider_name.clone(),
    };
    if journal.cursor_recovery.is_some() {
        persist_cursor_recovery_registration_artifact(
            &path,
            &receipt,
            "registration-receipt",
            "Cloud Files registration receipt",
        )
    } else {
        persist_ordinary_registration_artifact(
            &path,
            &receipt,
            "registration-receipt",
            "Cloud Files registration receipt",
        )
    }
}

fn persist_cursor_recovery_registration_artifact<T: serde::Serialize>(
    path: &Path,
    value: &T,
    code_prefix: &str,
    label: &str,
) -> Result<(), CloudFilesModuleError> {
    let Some(parent) = path.parent() else {
        return Err(CloudFilesModuleError {
            code: format!("{code_prefix}-path"),
            detail: format!("{label} path {} has no parent", path.display()),
        });
    };
    fs::create_dir_all(parent).map_err(|source| CloudFilesModuleError {
        code: format!("{code_prefix}-dir"),
        detail: format!("create {label} directory {}: {source}", parent.display()),
    })?;
    let bytes = encode_sobj(value).map_err(|source| CloudFilesModuleError {
        code: format!("{code_prefix}-encode"),
        detail: format!("encode {label}: {source}"),
    })?;
    let temporary = cursor_recovery_artifact_temp_path(path, &format!("{code_prefix}-next"));
    if read_bounded_plain_registration_file(
        path,
        CLOUD_FILES_CURSOR_RECOVERY_MARKER_MAX_BYTES,
        label,
    )?
    .as_deref()
        == Some(bytes.as_slice())
        && matches!(
            fs::symlink_metadata(&temporary),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound
        )
    {
        return Ok(());
    }
    match fs::symlink_metadata(&temporary) {
        Ok(metadata)
            if metadata.file_type().is_file()
                && metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 == 0
                && metadata.len() == bytes.len() as u64 =>
        {
            let observed = fs::read(&temporary).map_err(|source| CloudFilesModuleError {
                code: format!("{code_prefix}-temp-read"),
                detail: format!(
                    "read {label} temporary file {}: {source}",
                    temporary.display()
                ),
            })?;
            if observed != bytes {
                return Err(CloudFilesModuleError {
                    code: format!("{code_prefix}-temp-mismatch"),
                    detail: format!(
                        "refusing to replay changed {label} temporary file {}",
                        temporary.display()
                    ),
                });
            }
            let temporary_wide = wide_path(&temporary);
            let path_wide = wide_path(path);
            unsafe {
                MoveFileExW(
                    PCWSTR(temporary_wide.as_ptr()),
                    PCWSTR(path_wide.as_ptr()),
                    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                )
            }
            .map_err(|source| CloudFilesModuleError {
                code: format!("{code_prefix}-commit"),
                detail: format!("commit exact replayed {label} {}: {source}", path.display()),
            })?;
            return Ok(());
        }
        Ok(_) => {
            return Err(CloudFilesModuleError {
                code: format!("{code_prefix}-temp-invalid"),
                detail: format!(
                    "{label} temporary file {} is reparse, non-file, changed, or torn",
                    temporary.display()
                ),
            });
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(CloudFilesModuleError {
                code: format!("{code_prefix}-temp-inspect"),
                detail: format!(
                    "inspect {label} temporary file {}: {source}",
                    temporary.display()
                ),
            });
        }
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|source| CloudFilesModuleError {
            code: format!("{code_prefix}-create"),
            detail: format!("create {label} {}: {source}", temporary.display()),
        })?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| CloudFilesModuleError {
            code: format!("{code_prefix}-write"),
            detail: format!("write {label} {}: {source}", temporary.display()),
        })?;
    drop(file);
    let temporary_wide = wide_path(&temporary);
    let path_wide = wide_path(path);
    unsafe {
        MoveFileExW(
            PCWSTR(temporary_wide.as_ptr()),
            PCWSTR(path_wide.as_ptr()),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    }
    .map_err(|source| CloudFilesModuleError {
        code: format!("{code_prefix}-commit"),
        detail: format!("commit {label} {}: {source}", path.display()),
    })
}

/// Keep the established ordinary registration writer and its process-scoped
/// temporary name separate from the recovery classifier. Cursor recovery uses
/// the deterministic writer above only while a durable marker/provenance makes
/// every current/temp cut exactly classifiable.
fn persist_ordinary_registration_artifact<T: serde::Serialize>(
    path: &Path,
    value: &T,
    code_prefix: &str,
    label: &str,
) -> Result<(), CloudFilesModuleError> {
    let Some(parent) = path.parent() else {
        return Err(CloudFilesModuleError {
            code: format!("{code_prefix}-path"),
            detail: format!("{label} path {} has no parent", path.display()),
        });
    };
    fs::create_dir_all(parent).map_err(|source| CloudFilesModuleError {
        code: format!("{code_prefix}-dir"),
        detail: format!("create {label} directory {}: {source}", parent.display()),
    })?;
    let bytes = encode_sobj(value).map_err(|source| CloudFilesModuleError {
        code: format!("{code_prefix}-encode"),
        detail: format!("encode {label}: {source}"),
    })?;
    let temporary = path.with_extension(format!("sobj.tmp.{}", std::process::id()));
    let mut file = fs::File::create(&temporary).map_err(|source| CloudFilesModuleError {
        code: format!("{code_prefix}-create"),
        detail: format!("create {label} {}: {source}", temporary.display()),
    })?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| CloudFilesModuleError {
            code: format!("{code_prefix}-write"),
            detail: format!("write {label} {}: {source}", temporary.display()),
        })?;
    drop(file);
    let temporary_wide = wide_path(&temporary);
    let path_wide = wide_path(path);
    unsafe {
        MoveFileExW(
            PCWSTR(temporary_wide.as_ptr()),
            PCWSTR(path_wide.as_ptr()),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    }
    .map_err(|source| CloudFilesModuleError {
        code: format!("{code_prefix}-commit"),
        detail: format!("commit {label} {}: {source}", path.display()),
    })
}

fn remove_registration_receipt(path: &Path) -> Result<(), CloudFilesModuleError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CloudFilesModuleError {
            code: "registration-receipt-remove".to_string(),
            detail: format!(
                "remove Cloud Files registration receipt {}: {source}",
                path.display()
            ),
        }),
    }
}

fn remove_registration_journal(data_dir: &SinomoDataDir) -> Result<(), CloudFilesModuleError> {
    let path = registration_journal_path(data_dir);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(CloudFilesModuleError {
            code: "registration-journal-remove".to_string(),
            detail: format!(
                "remove Cloud Files registration journal {}: {source}",
                path.display()
            ),
        }),
    }
}

fn query_registered_sync_root(
    sync_root: &Path,
) -> Result<Option<RegisteredSyncRootInfo>, CloudFilesModuleError> {
    query_registered_sync_root_with(sync_root, false)
}

/// A volume without any Cloud Files roots may not have a CldFlt instance yet.
/// On that exact fresh-admission cut CfGetSyncRootInfoByPath returns
/// ERROR_INVALID_FUNCTION instead of ERROR_CLOUD_FILE_NOT_UNDER_SYNC_ROOT.
/// Owned and recovery paths deliberately use the strict query above so loss of
/// an expected filter instance can never be mistaken for registration absence.
fn query_registered_sync_root_for_fresh_admission(
    sync_root: &Path,
) -> Result<Option<RegisteredSyncRootInfo>, CloudFilesModuleError> {
    query_registered_sync_root_with(sync_root, true)
}

fn sync_root_query_reports_absence(code: HRESULT, allow_uninitialized_volume: bool) -> bool {
    code == HRESULT::from_win32(ERROR_CLOUD_FILE_NOT_UNDER_SYNC_ROOT.0)
        || (allow_uninitialized_volume && code == HRESULT::from_win32(ERROR_INVALID_FUNCTION.0))
}

fn query_registered_sync_root_with(
    sync_root: &Path,
    allow_uninitialized_volume: bool,
) -> Result<Option<RegisteredSyncRootInfo>, CloudFilesModuleError> {
    let buffer_bytes =
        std::mem::size_of::<CF_SYNC_ROOT_STANDARD_INFO>() + REGISTRATION_IDENTITY_MAX_BYTES;
    let word_bytes = std::mem::size_of::<usize>();
    let mut buffer = vec![0usize; buffer_bytes.div_ceil(word_bytes)];
    let mut returned_length = 0u32;
    let sync_root_wide = wide_path(sync_root);
    if let Err(source) = unsafe {
        CfGetSyncRootInfoByPath(
            PCWSTR(sync_root_wide.as_ptr()),
            CF_SYNC_ROOT_INFO_STANDARD,
            buffer.as_mut_ptr().cast::<c_void>(),
            (buffer.len() * word_bytes) as u32,
            Some(&mut returned_length),
        )
    } {
        if sync_root_query_reports_absence(source.code(), allow_uninitialized_volume) {
            return Ok(None);
        }
        return Err(CloudFilesModuleError {
            code: "query-sync-root".to_string(),
            detail: format!(
                "query Cloud Files sync root {} before unregister: {source}",
                sync_root.display()
            ),
        });
    }
    let available = returned_length as usize;
    let identity_offset = std::mem::offset_of!(CF_SYNC_ROOT_STANDARD_INFO, SyncRootIdentity);
    if available < identity_offset || available > buffer.len() * word_bytes {
        return Err(CloudFilesModuleError {
            code: "query-sync-root-invalid".to_string(),
            detail: format!(
                "Cloud Files sync-root info for {} returned invalid length {available}",
                sync_root.display()
            ),
        });
    }
    let info = unsafe { &*buffer.as_ptr().cast::<CF_SYNC_ROOT_STANDARD_INFO>() };
    let identity_len = info.SyncRootIdentityLength as usize;
    if identity_offset.saturating_add(identity_len) > available {
        return Err(CloudFilesModuleError {
            code: "query-sync-root-invalid".to_string(),
            detail: format!(
                "Cloud Files sync-root identity for {} exceeds returned info",
                sync_root.display()
            ),
        });
    }
    let file_index = sync_root_file_index(sync_root)?;
    if file_index != info.SyncRootFileId as u64 {
        return Err(CloudFilesModuleError {
            code: "sync-root-path-not-root".to_string(),
            detail: format!(
                "{} is inside a Cloud Files tree but is not its registered root",
                sync_root.display()
            ),
        });
    }
    let identity = unsafe {
        std::slice::from_raw_parts(
            buffer.as_ptr().cast::<u8>().add(identity_offset),
            identity_len,
        )
    }
    .to_vec();
    let provider_name_len = info
        .ProviderName
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(info.ProviderName.len());
    let provider_version_len = info
        .ProviderVersion
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(info.ProviderVersion.len());
    Ok(Some(RegisteredSyncRootInfo {
        provider_name: String::from_utf16_lossy(&info.ProviderName[..provider_name_len]),
        provider_version: String::from_utf16_lossy(&info.ProviderVersion[..provider_version_len]),
        sync_root_file_id: info.SyncRootFileId as u64,
        root_identity: identity,
    }))
}

fn verify_registration_ownership(
    request: &CloudFilesUnregisterRequest,
    receipt: Option<&CloudFilesRegistrationReceipt>,
    observed: &RegisteredSyncRootInfo,
) -> Result<(), CloudFilesModuleError> {
    let Some(receipt) = receipt else {
        return Err(CloudFilesModuleError {
            code: "sync-root-ownership-unverifiable".to_string(),
            detail: format!(
                "refusing to unregister {} without a durable nonce journal or legacy receipt",
                request.sync_root.display()
            ),
        });
    };
    if observed.provider_name != request.provider_name
        || observed.root_identity != request.root_path.as_bytes()
    {
        return Err(CloudFilesModuleError {
            code: "sync-root-ownership-mismatch".to_string(),
            detail: format!(
                "refusing to unregister {}: registered provider/root identity does not match `{}` and `{}`",
                request.sync_root.display(),
                request.provider_name,
                request.root_path
            ),
        });
    }
    verify_registration_receipt(request, Some(receipt))
}

fn verify_registration_receipt(
    request: &CloudFilesUnregisterRequest,
    receipt: Option<&CloudFilesRegistrationReceipt>,
) -> Result<(), CloudFilesModuleError> {
    let Some(receipt) = receipt else {
        return Err(CloudFilesModuleError {
            code: "registration-receipt-missing".to_string(),
            detail: format!(
                "refusing to unregister {} without a durable registration receipt",
                request.sync_root.display()
            ),
        });
    };
    if !matches!(
        receipt.interface_version.as_str(),
        crate::protocol::SINOMO_DFS_CLOUD_FILES_INTERFACE_V1
            | crate::protocol::SINOMO_DFS_CLOUD_FILES_INTERFACE_V2
            | SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
    ) || receipt.sync_root != request.sync_root
        || receipt.root_path != request.root_path
        || receipt.provider_name != request.provider_name
    {
        return Err(CloudFilesModuleError {
            code: "registration-receipt-mismatch".to_string(),
            detail: format!(
                "refusing to unregister {}: durable registration receipt does not match the request",
                request.sync_root.display()
            ),
        });
    }
    Ok(())
}

fn verify_journal_registration_ownership(
    journal: &RegistrationJournal,
    observed: &RegisteredSyncRootInfo,
) -> Result<(), CloudFilesModuleError> {
    journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    if observed.provider_name != journal.provider_name
        || observed.sync_root_file_id != journal.sync_root_file_id
        || observed.root_identity != journal.root_identity
    {
        return Err(CloudFilesModuleError {
            code: "sync-root-ownership-mismatch".to_string(),
            detail: format!(
                "refusing to alter {}: observed provider, registered-root file ID, or nonce-bound identity does not match the durable journal",
                journal.sync_root.display()
            ),
        });
    }
    Ok(())
}

fn recover_registration_for_mount(
    request: &CloudFilesMountRequest,
    data_dir: &SinomoDataDir,
) -> Result<RecoveredRegistration, CloudFilesModuleError> {
    if let Some(journal) = load_registration_journal(data_dir)? {
        validate_owned_sync_root_physical_identity_with(
            &journal.sync_root,
            journal.sync_root_file_id,
            || ensure_owned_sync_root_is_plain_directory(&journal.sync_root),
            || sync_root_file_index(&journal.sync_root),
        )?;
        let observed = query_registered_sync_root(&journal.sync_root)?;
        if let Some(observed) = observed.as_ref() {
            verify_journal_registration_ownership(&journal, observed)?;
        }
        let receipt_result = load_registration_receipt(&registration_receipt_path(data_dir));
        let receipt = receipt_result.as_ref().map_err(Clone::clone)?;
        let ownership_evidence_verified =
            journal.mount_ownership_evidence_verified(receipt.as_ref())?;
        match journal.recovery_action(observed.is_some(), ownership_evidence_verified) {
            RegistrationRecoveryAction::ReuseOwnedRegistration => {
                if !journal.matches_mount_request(request) {
                    return Err(CloudFilesModuleError {
                        code: "registration-journal-mismatch".to_string(),
                        detail: format!(
                            "owned Cloud Files registration for {} does not match requested mount {}; explicitly unregister the retained root first",
                            journal.sync_root.display(),
                            request.sync_root.display()
                        ),
                    });
                }
                return Ok(RecoveredRegistration::Owned {
                    journal: Box::new(journal),
                    registration_present: true,
                });
            }
            RegistrationRecoveryAction::ReuseOwnedState => {
                if !journal.matches_mount_request(request) {
                    return Err(CloudFilesModuleError {
                        code: "registration-journal-mismatch".to_string(),
                        detail: format!(
                            "retained Cloud Files state ownership for {} does not match requested mount {}; explicitly unregister the retained root first",
                            journal.sync_root.display(),
                            request.sync_root.display()
                        ),
                    });
                }
                return Ok(RecoveredRegistration::Owned {
                    journal: Box::new(journal),
                    registration_present: false,
                });
            }
            RegistrationRecoveryAction::RollBackRegistration => {
                rollback_owned_registration(data_dir, &journal)?;
            }
            RegistrationRecoveryAction::ClearArtifacts => {
                clear_registration_artifacts(data_dir)?;
            }
        }
        if let Err(mut error) = receipt_result {
            error.detail.push_str(
                "; the nonce-owned registration was rolled back before retrying the corrupt receipt",
            );
            return Err(error);
        }
    }

    recover_legacy_receipt_before_mount(request, data_dir)?;
    match fs::symlink_metadata(&request.sync_root) {
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            // No journal, no receipt, and no physical root: the Fresh path may
            // create the directory only after this read-only recovery pass.
            return Ok(RecoveredRegistration::Fresh);
        }
        Ok(_) => {}
        Err(source) => {
            return Err(CloudFilesModuleError {
                code: "sync-root-inspect".to_string(),
                detail: format!(
                    "inspect prospective Cloud Files sync root {}: {source}",
                    request.sync_root.display()
                ),
            });
        }
    }
    if let Some(observed) = query_registered_sync_root_for_fresh_admission(&request.sync_root)? {
        return Err(CloudFilesModuleError {
            code: "sync-root-ownership-unverifiable".to_string(),
            detail: format!(
                "refusing to replace existing Cloud Files root {} without a matching nonce journal; observed provider `{}`",
                request.sync_root.display(),
                observed.provider_name
            ),
        });
    }
    Ok(RecoveredRegistration::Fresh)
}

fn verify_fresh_registration_absent(
    request: &CloudFilesMountRequest,
) -> Result<(), CloudFilesModuleError> {
    if let Some(observed) = query_registered_sync_root_for_fresh_admission(&request.sync_root)? {
        return Err(CloudFilesModuleError {
            code: "sync-root-ownership-unverifiable".to_string(),
            detail: format!(
                "refusing Fresh activation for existing Cloud Files root {} without a matching nonce journal; observed provider `{}`",
                request.sync_root.display(),
                observed.provider_name
            ),
        });
    }
    Ok(())
}

fn recover_legacy_receipt_before_mount(
    request: &CloudFilesMountRequest,
    data_dir: &SinomoDataDir,
) -> Result<(), CloudFilesModuleError> {
    let receipt_path = registration_receipt_path(data_dir);
    let Some(receipt) = load_registration_receipt(&receipt_path)? else {
        return Ok(());
    };
    let observed = query_registered_sync_root(&receipt.sync_root)?;
    let Some(observed) = observed else {
        return Err(CloudFilesModuleError {
            code: "legacy-registration-artifacts-require-cleanup".to_string(),
            detail: format!(
                "durable Cloud Files receipt {} remains without a nonce journal; refusing Fresh activation until explicit cleanup resolves it",
                receipt_path.display()
            ),
        });
    };
    let legacy_request = CloudFilesUnregisterRequest {
        sync_root: receipt.sync_root.clone(),
        root_path: receipt.root_path.clone(),
        data_dir_root: request.data_dir_root.clone(),
        provider_name: receipt.provider_name.clone(),
        cursor_recovery: None,
    };
    verify_registration_ownership(&legacy_request, Some(&receipt), &observed)?;
    Err(CloudFilesModuleError {
        code: "legacy-registration-requires-unregister".to_string(),
        detail: format!(
            "existing Cloud Files registration for {} has only a legacy receipt; explicitly unregister it before creating a nonce-bound registration",
            receipt.sync_root.display()
        ),
    })
}

fn recover_registration_for_unregister(
    request: &CloudFilesUnregisterRequest,
    data_dir: &SinomoDataDir,
    policy: UnregisterPolicy,
    registration_lock_bytes: Option<&[u8]>,
) -> Result<RecoveredUnregister, CloudFilesModuleError> {
    let cursor_root_fence = request
        .cursor_recovery
        .as_ref()
        .map(|_| CursorRecoveryRootFence::acquire(&request.sync_root))
        .transpose()?;
    let mut recovery_marker =
        recover_cursor_recovery_registration_snapshot(data_dir, false, |marker| {
            if policy == UnregisterPolicy::Release {
                return Err(CloudFilesModuleError {
                    code: "cursor-recovery-release-forbidden".to_string(),
                    detail: "release unregister may not mutate cursor recovery artifacts"
                        .to_string(),
                });
            } else {
                validate_cursor_recovery_preserve_authority(
                    marker,
                    request,
                    data_dir,
                    registration_lock_bytes,
                )?;
                if cursor_root_fence.as_ref().map(|fence| fence.file_id)
                    != Some(marker.sync_root_file_id)
                {
                    return Err(CloudFilesModuleError {
                        code: "cursor-recovery-root-drift".to_string(),
                        detail: "cursor-recovery preserve root file ID changed".to_string(),
                    });
                }
            }
            Ok(())
        })?;
    let preserve_relation = recovery_marker
        .as_ref()
        .map(|marker| {
            validate_cursor_recovery_preserve_authority(
                marker,
                request,
                data_dir,
                registration_lock_bytes,
            )
        })
        .transpose()?;
    let _cursor_root_fence = cursor_root_fence;
    if let Some(marker) = recovery_marker.as_ref() {
        if policy == UnregisterPolicy::Release {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-release-forbidden".to_string(),
                detail: format!(
                    "cursor recovery {} is {:?}; release unregister may not clear its StateOwned ownership",
                    marker.recovery_id, marker.phase
                ),
            });
        }
        if !marker.identity.sync_root.eq(&request.sync_root)
            || marker.identity.root_path != request.root_path
            || marker.identity.provider_name != request.provider_name
        {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-unregister-mismatch".to_string(),
                detail: "state-preserving unregister does not match the durable cursor recovery"
                    .to_string(),
            });
        }
        if matches!(
            marker.phase,
            CursorRecoveryModulePhase::Authorized | CursorRecoveryModulePhase::Validated
        ) {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-not-state-owned".to_string(),
                detail: "state-preserving unregister cannot cancel or release an authorized cursor recovery before its StateOwned journal is durable"
                    .to_string(),
            });
        }
    } else if request.cursor_recovery.is_some() {
        return Err(CloudFilesModuleError {
            code: "cursor-recovery-unregister-orphan-context".to_string(),
            detail: "ordinary unregister cannot manufacture cursor-recovery cycle authority"
                .to_string(),
        });
    }

    let marker_path = cursor_recovery_module_marker_path(data_dir);
    if policy == UnregisterPolicy::PreserveState
        && preserve_relation == Some(CursorRecoveryPreserveContextRelation::Current)
        && recovery_marker
            .as_ref()
            .is_some_and(|marker| marker.phase != CursorRecoveryModulePhase::PreserveAuthorized)
    {
        let mut marker = recovery_marker.take().expect("checked cursor marker");
        let mut activation = RegistrationActivation::new(
            data_dir.clone(),
            load_registration_journal(data_dir)?.ok_or_else(|| CloudFilesModuleError {
                code: "registration-journal-missing".to_string(),
                detail: "cursor recovery preserve lost its nonce-bound journal".to_string(),
            })?,
            true,
        );
        activation.disarm();
        activation.mark_cursor_recovery_preserve_authorized(&marker_path, &mut marker)?;
        recovery_marker = Some(marker);
    }
    if let Some(journal) = load_registration_journal(data_dir)? {
        if !journal.matches_unregister_request(request) {
            return Err(CloudFilesModuleError {
                code: "registration-journal-mismatch".to_string(),
                detail: format!(
                    "refusing to unregister {}: durable nonce journal belongs to {}",
                    request.sync_root.display(),
                    journal.sync_root.display()
                ),
            });
        }
        let ownership = match policy {
            UnregisterPolicy::Release => RegistrationOwnershipDisposition::Release,
            UnregisterPolicy::PreserveState if recovery_marker.is_some() => {
                RegistrationOwnershipDisposition::Retain
            }
            UnregisterPolicy::PreserveState => {
                registration_ownership_disposition(data_dir, &journal)?
            }
        };
        let was_registered = unregister_journal_owned_registration(&journal)?;
        verify_registration_absent(&journal.sync_root)?;
        match ownership {
            RegistrationOwnershipDisposition::Release => clear_registration_artifacts(data_dir)?,
            RegistrationOwnershipDisposition::Retain => {
                let mut retained = journal;
                retained.phase = RegistrationPhase::StateOwned;
                let mut response_context = None;
                if let Some(marker) = recovery_marker.as_mut() {
                    retained.bind_cursor_recovery(
                        marker.recovery_id.clone(),
                        marker.recovery_token.clone(),
                        marker.recovery_cycle,
                    );
                    persist_registration_journal(data_dir, &retained)?;
                    if marker.phase == CursorRecoveryModulePhase::PreserveAuthorized {
                        let mut activation =
                            RegistrationActivation::new(data_dir.clone(), retained.clone(), true);
                        activation.disarm();
                        response_context = Some(
                            activation
                                .mark_cursor_recovery_preserved_state_owned(&marker_path, marker)?,
                        );
                    } else {
                        response_context = Some(CloudFilesCursorRecoveryContext {
                            recovery_id: marker.recovery_id.clone(),
                            recovery_token: marker.recovery_token.clone(),
                            recovery_cycle: marker.recovery_cycle,
                            native_successor_fingerprint: Some(
                                cursor_recovery_native_successor_fingerprint(marker)?,
                            ),
                        });
                    }
                } else {
                    persist_registration_journal(data_dir, &retained)?;
                }
                return Ok(RecoveredUnregister {
                    was_registered,
                    ownership,
                    cursor_recovery: response_context,
                });
            }
        }
        return Ok(RecoveredUnregister {
            was_registered,
            ownership,
            cursor_recovery: None,
        });
    }

    if policy == UnregisterPolicy::PreserveState {
        let receipt_path = registration_receipt_path(data_dir);
        let receipt = load_registration_receipt(&receipt_path)?;
        let observed = query_registered_sync_root_if_path_exists(
            &request.sync_root,
            query_registered_sync_root,
        )?;
        if receipt.is_none() && observed.is_none() {
            // This is the only journal-free replay-safe success. A prior v3
            // Prepared attempt may have proved there were no artifacts at all;
            // returning Released again must not create or delete anything.
            return Ok(RecoveredUnregister {
                was_registered: false,
                ownership: RegistrationOwnershipDisposition::Release,
                cursor_recovery: None,
            });
        }
        return Err(CloudFilesModuleError {
            code: "registration-journal-missing".to_string(),
            detail: format!(
                "refusing state-preserving unregister for {} without a nonce-bound registration journal while a receipt or CFAPI registration still exists",
                request.sync_root.display(),
            ),
        });
    }

    let receipt_path = registration_receipt_path(data_dir);
    let receipt = load_registration_receipt(&receipt_path)?;
    if let Some(receipt) = receipt.as_ref() {
        verify_registration_receipt(request, Some(receipt))?;
    }
    let Some(observed) = query_registered_sync_root(&request.sync_root)? else {
        remove_registration_receipt(&receipt_path)?;
        return Ok(RecoveredUnregister {
            was_registered: false,
            ownership: RegistrationOwnershipDisposition::Release,
            cursor_recovery: None,
        });
    };
    verify_registration_ownership(request, receipt.as_ref(), &observed)?;
    unregister_sync_root_after_verification(&request.sync_root)?;
    verify_registration_absent(&request.sync_root)?;
    remove_registration_receipt(&receipt_path)?;
    Ok(RecoveredUnregister {
        was_registered: true,
        ownership: RegistrationOwnershipDisposition::Release,
        cursor_recovery: None,
    })
}

fn query_registered_sync_root_if_path_exists<F>(
    sync_root: &Path,
    query: F,
) -> Result<Option<RegisteredSyncRootInfo>, CloudFilesModuleError>
where
    F: FnOnce(&Path) -> Result<Option<RegisteredSyncRootInfo>, CloudFilesModuleError>,
{
    match fs::symlink_metadata(sync_root) {
        Ok(_) => query(sync_root),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(CloudFilesModuleError {
            code: "sync-root-inspect".to_string(),
            detail: format!(
                "inspect journal-free Cloud Files sync root {} before unregister replay: {source}",
                sync_root.display()
            ),
        }),
    }
}

fn prepare_registration_activation(
    request: &CloudFilesMountRequest,
    data_dir: &SinomoDataDir,
    recovered: RecoveredRegistration,
    retain_ready_for_completion: bool,
) -> Result<(RegistrationJournal, bool), CloudFilesModuleError> {
    let (mut journal, state_owned) = match recovered {
        RecoveredRegistration::Fresh => {
            let nonce = GUID::new().map_err(|source| CloudFilesModuleError {
                code: "registration-nonce-generate".to_string(),
                detail: format!("generate unguessable Cloud Files registration nonce: {source}"),
            })?;
            (
                RegistrationJournal::prepared(
                    request,
                    CLOUD_FILES_PROVIDER_ID_TEXT,
                    format!("{:032x}", nonce.to_u128()),
                    sync_root_file_index(&request.sync_root)?,
                )?,
                false,
            )
        }
        RecoveredRegistration::Owned { journal, .. } => (*journal, true),
    };
    if state_owned && retain_ready_for_completion {
        if journal.phase != RegistrationPhase::Ready {
            return Err(CloudFilesModuleError {
                code: "cursor-recovery-completion-journal-phase".to_string(),
                detail: "CompletionAuthorized restart requires the exact Ready ownership phase"
                    .to_string(),
            });
        }
    } else if state_owned {
        // A reactivation must prove health again, but never surrender the
        // durable boundary that already owns queue/reconciliation state.
        journal.phase = RegistrationPhase::StateOwned;
    } else {
        journal.phase = RegistrationPhase::Prepared;
    }
    journal.provider_version = request.provider_version.clone();
    if !retain_ready_for_completion {
        persist_registration_journal(data_dir, &journal)?;
    }
    Ok((journal, state_owned))
}

fn register_sync_root(
    request: &CloudFilesMountRequest,
    journal: &RegistrationJournal,
    was_registered: bool,
) -> Result<(), CloudFilesModuleError> {
    if !journal.matches_mount_request(request) {
        return Err(CloudFilesModuleError {
            code: "registration-journal-mismatch".to_string(),
            detail: "Cloud Files registration journal does not match the mount request".to_string(),
        });
    }
    journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    let file_id = sync_root_file_index(&request.sync_root)?;
    if file_id != journal.sync_root_file_id {
        return Err(CloudFilesModuleError {
            code: "sync-root-file-id-changed".to_string(),
            detail: format!(
                "Cloud Files sync root {} was replaced after registration intent persistence",
                request.sync_root.display()
            ),
        });
    }
    let sync_root_wide = wide_path(&request.sync_root);
    let provider_name = wide_str(&request.provider_name);
    let provider_version = wide_str(&request.provider_version);
    let sync_root_identity = &journal.root_identity;
    let registration = CF_SYNC_REGISTRATION {
        StructSize: std::mem::size_of::<CF_SYNC_REGISTRATION>() as u32,
        ProviderName: PCWSTR(provider_name.as_ptr()),
        ProviderVersion: PCWSTR(provider_version.as_ptr()),
        SyncRootIdentity: sync_root_identity.as_ptr().cast::<c_void>(),
        SyncRootIdentityLength: sync_root_identity.len() as u32,
        FileIdentity: ptr::null(),
        FileIdentityLength: 0,
        ProviderId: CLOUD_FILES_PROVIDER_ID,
    };
    let policies = CF_SYNC_POLICIES {
        StructSize: std::mem::size_of::<CF_SYNC_POLICIES>() as u32,
        Hydration: CF_HYDRATION_POLICY {
            Primary: CF_HYDRATION_POLICY_PROGRESSIVE,
            Modifier: CF_HYDRATION_POLICY_MODIFIER_AUTO_DEHYDRATION_ALLOWED,
        },
        Population: CF_POPULATION_POLICY {
            Primary: CF_POPULATION_POLICY_PARTIAL,
            Modifier: CF_POPULATION_POLICY_MODIFIER_NONE,
        },
        InSync: CF_INSYNC_POLICY_TRACK_ALL,
        HardLink: CF_HARDLINK_POLICY_NONE,
        PlaceholderManagement: CF_PLACEHOLDER_MANAGEMENT_POLICY_DEFAULT,
    };
    unsafe {
        CfRegisterSyncRoot(
            PCWSTR(sync_root_wide.as_ptr()),
            &registration,
            &policies,
            if was_registered {
                CF_REGISTER_FLAG_UPDATE
            } else {
                CF_REGISTER_FLAG_NONE
            },
        )
    }
    .map_err(|source| CloudFilesModuleError {
        code: "register-sync-root".to_string(),
        detail: format!(
            "register Cloud Files sync root {}: {source}",
            request.sync_root.display()
        ),
    })?;
    let observed =
        query_registered_sync_root(&request.sync_root)?.ok_or_else(|| CloudFilesModuleError {
            code: "registered-sync-root-not-observed".to_string(),
            detail: format!(
                "Cloud Files registration for {} succeeded but could not be observed",
                request.sync_root.display()
            ),
        })?;
    verify_journal_registration_ownership(journal, &observed)?;
    if observed.provider_version != request.provider_version {
        return Err(CloudFilesModuleError {
            code: "registered-provider-version-mismatch".to_string(),
            detail: format!(
                "Cloud Files registration for {} reported provider version `{}` instead of `{}`",
                request.sync_root.display(),
                observed.provider_version,
                request.provider_version
            ),
        });
    }
    Ok(())
}

fn unregister_journal_owned_registration(
    journal: &RegistrationJournal,
) -> Result<bool, CloudFilesModuleError> {
    let Some(observed) = query_registered_sync_root(&journal.sync_root)? else {
        return Ok(false);
    };
    verify_journal_registration_ownership(journal, &observed)?;
    unregister_sync_root_after_verification(&journal.sync_root)?;
    Ok(true)
}

fn registration_ownership_disposition(
    data_dir: &SinomoDataDir,
    journal: &RegistrationJournal,
) -> Result<RegistrationOwnershipDisposition, CloudFilesModuleError> {
    let receipt = load_registration_receipt(&registration_receipt_path(data_dir))?;
    let current_sync_root_file_id = sync_root_file_index(&journal.sync_root)?;
    journal.state_preservation_disposition(receipt.as_ref(), Some(current_sync_root_file_id))
}

fn unregister_sync_root_after_verification(sync_root: &Path) -> Result<(), CloudFilesModuleError> {
    let sync_root_wide = wide_path(sync_root);
    if let Err(source) = unsafe { CfUnregisterSyncRoot(PCWSTR(sync_root_wide.as_ptr())) } {
        if query_registered_sync_root(sync_root)?.is_none() {
            return Ok(());
        }
        return Err(CloudFilesModuleError {
            code: "unregister-sync-root".to_string(),
            detail: format!(
                "unregister Cloud Files sync root {} after ownership verification: {source}",
                sync_root.display()
            ),
        });
    }
    Ok(())
}

fn verify_registration_absent(sync_root: &Path) -> Result<(), CloudFilesModuleError> {
    if let Some(observed) = query_registered_sync_root(sync_root)? {
        return Err(CloudFilesModuleError {
            code: "unregister-sync-root-not-absent".to_string(),
            detail: format!(
                "Cloud Files sync root {} remains registered as provider `{}` after unregister",
                sync_root.display(),
                observed.provider_name
            ),
        });
    }
    Ok(())
}

fn rollback_owned_registration(
    data_dir: &SinomoDataDir,
    journal: &RegistrationJournal,
) -> Result<(), CloudFilesModuleError> {
    unregister_journal_owned_registration(journal)?;
    verify_registration_absent(&journal.sync_root)?;
    clear_registration_artifacts(data_dir)
}

fn rollback_activation_registration(
    data_dir: &SinomoDataDir,
    journal: &RegistrationJournal,
    state_owned: bool,
    registration_preexisting: bool,
) -> Result<(), CloudFilesModuleError> {
    if registration_preexisting {
        let observed = query_registered_sync_root(&journal.sync_root)?.ok_or_else(|| {
            CloudFilesModuleError {
                code: "preexisting-registration-lost".to_string(),
                detail: format!(
                    "pre-existing Cloud Files registration for {} disappeared during failed activation",
                    journal.sync_root.display()
                ),
            }
        })?;
        verify_journal_registration_ownership(journal, &observed)?;
        let mut retained = journal.clone();
        retained.phase = RegistrationPhase::StateOwned;
        return persist_registration_journal(data_dir, &retained);
    }
    if !state_owned {
        return rollback_owned_registration(data_dir, journal);
    }
    unregister_journal_owned_registration(journal)?;
    verify_registration_absent(&journal.sync_root)?;
    let mut retained = journal.clone();
    retained.phase = RegistrationPhase::StateOwned;
    persist_registration_journal(data_dir, &retained)
}

fn clear_registration_artifacts(data_dir: &SinomoDataDir) -> Result<(), CloudFilesModuleError> {
    let receipt_path = registration_receipt_path(data_dir);
    remove_registration_receipt(&receipt_path)?;
    remove_registration_journal(data_dir)?;
    for (label, path) in [
        ("receipt", receipt_path),
        ("journal", registration_journal_path(data_dir)),
    ] {
        match fs::metadata(&path) {
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {
                return Err(CloudFilesModuleError {
                    code: "registration-artifact-not-cleared".to_string(),
                    detail: format!(
                        "Cloud Files registration {label} {} remains after cleanup",
                        path.display()
                    ),
                });
            }
            Err(source) => {
                return Err(CloudFilesModuleError {
                    code: "registration-artifact-verify".to_string(),
                    detail: format!(
                        "verify Cloud Files registration {label} removal {}: {source}",
                        path.display()
                    ),
                });
            }
        }
    }
    Ok(())
}

fn ensure_new_sync_root_is_empty(sync_root: &Path) -> Result<(), CloudFilesModuleError> {
    ensure_sync_root_is_plain_directory(sync_root, "new or unowned")?;
    let mut entries = fs::read_dir(sync_root).map_err(|source| CloudFilesModuleError {
        code: "sync-root-read".to_string(),
        detail: format!(
            "inspect unowned Cloud Files sync root {}: {source}",
            sync_root.display()
        ),
    })?;
    if let Some(entry) = entries.next() {
        let entry = entry.map_err(|source| CloudFilesModuleError {
            code: "sync-root-read".to_string(),
            detail: format!(
                "inspect unowned Cloud Files sync root {}: {source}",
                sync_root.display()
            ),
        })?;
        return Err(CloudFilesModuleError {
            code: "unowned-sync-root-not-empty".to_string(),
            detail: format!(
                "new or unowned Cloud Files sync root {} must be empty; found `{}`",
                sync_root.display(),
                entry.file_name().to_string_lossy()
            ),
        });
    }
    Ok(())
}

fn ensure_owned_sync_root_is_plain_directory(
    sync_root: &Path,
) -> Result<(), CloudFilesModuleError> {
    ensure_sync_root_is_plain_directory(sync_root, "owned")
}

fn validate_owned_sync_root_physical_identity_with<FPlain, FFileId>(
    sync_root: &Path,
    expected_file_id: u64,
    ensure_plain: FPlain,
    current_file_id: FFileId,
) -> Result<(), CloudFilesModuleError>
where
    FPlain: FnOnce() -> Result<(), CloudFilesModuleError>,
    FFileId: FnOnce() -> Result<u64, CloudFilesModuleError>,
{
    ensure_plain()?;
    if current_file_id()? != expected_file_id {
        return Err(CloudFilesModuleError {
            code: "sync-root-file-id-changed".to_string(),
            detail: format!(
                "refusing recovery for {} because its stable file ID changed",
                sync_root.display()
            ),
        });
    }
    Ok(())
}

fn ensure_sync_root_is_plain_directory(
    sync_root: &Path,
    ownership: &str,
) -> Result<(), CloudFilesModuleError> {
    let metadata = fs::symlink_metadata(sync_root).map_err(|source| CloudFilesModuleError {
        code: "sync-root-inspect".to_string(),
        detail: format!(
            "inspect {ownership} Cloud Files sync root {}: {source}",
            sync_root.display()
        ),
    })?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(CloudFilesModuleError {
            code: "unowned-sync-root-not-plain-directory".to_string(),
            detail: format!(
                "{ownership} Cloud Files sync root {} must be a non-reparse directory",
                sync_root.display(),
            ),
        });
    }
    Ok(())
}

#[derive(Debug)]
struct OpenedSifsService {
    service: SifsService,
    replica_candidates: Vec<dfs::DfsNodeId>,
}

fn open_sifs_service(
    data_dir_root: Option<PathBuf>,
) -> Result<OpenedSifsService, CloudFilesModuleError> {
    // This cdylib links its own copy of sinomo-network, so install the cluster
    // directory in this image before any runtime-owned route hydration.
    sinomo_runtime::cluster::install_kv_cluster_directory();
    let data_dir = resolve_data_dir(data_dir_root)?;
    data_dir.ensure_root_layout().map_err(error("local-data"))?;
    let cluster_paths = network::ClusterPaths::new(&data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        Some(
            sinomo_runtime::cluster::read_cluster_state_without_kv(&cluster_paths)
                .map_err(error("network"))?,
        )
    } else {
        None
    };
    let local_node_id = cluster_state
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(dfs::fallback_dfs_node_id);
    let known_remote_nodes = cluster_state
        .as_ref()
        .map(|state| {
            state
                .members
                .iter()
                .filter(|member| member.node_id != local_node_id && member.state.is_authorized())
                .map(|member| member.node_id.clone())
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let content =
        LocalContentStore::new(data_dir.dfs_dir().join("content")).map_err(error("dfs"))?;
    let daemon_peer = cluster_state
        .as_ref()
        .map(|_| DaemonDfsPeerTransport::connect(data_dir.clone()).map(Arc::new))
        .transpose()
        .map_err(error("network"))?;
    let control_plane = daemon_peer.as_ref().map(|peer_transport| {
        let peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
        MountedSifsControlPlaneKvClient::new_authoritative(cluster_paths.clone(), peer_transport)
    });
    let replica_candidates = mounted_content_replica_candidates(
        cluster_state.is_some(),
        &local_node_id,
        control_plane.as_ref(),
    )?;
    let mut service = NetworkedDfsService::new(
        DfsCloudFilesEndpointTransport::new(
            local_node_id,
            known_remote_nodes,
            daemon_peer.map(|peer| peer as Arc<dyn DfsPeerTransport>),
        ),
        content,
    );
    if let Some(control_plane) = control_plane.as_ref() {
        service = service.with_control_plane(control_plane.clone());
    }
    let metadata = open_metadata_backend(&data_dir, &cluster_paths, control_plane.as_ref())?;
    let mut service = service
        .with_boxed_metadata_backend_lazy(metadata)
        .map_err(error("dfs"))?;
    service.bootstrap_distributed_sifs().map_err(error("dfs"))?;
    configure_cloud_files_mounted_read_policy(&mut service);
    Ok(OpenedSifsService {
        service,
        replica_candidates,
    })
}

fn mounted_content_replica_candidates(
    clustered: bool,
    local_node_id: &NodeId,
    control_plane: Option<&MountedSifsControlPlaneKvClient>,
) -> Result<Vec<NodeId>, CloudFilesModuleError> {
    if !clustered {
        return Ok(vec![local_node_id.clone()]);
    }
    let control_plane = control_plane.ok_or_else(|| CloudFilesModuleError {
        code: "network".to_string(),
        detail: "clustered Cloud Files requires an authoritative DFS control-plane client"
            .to_string(),
    })?;
    let entries = control_plane
        .range_prefix(dfs::content_services_prefix())
        .map_err(error("network"))?;
    Ok(mounted_content_replica_candidates_from_entries(entries))
}

fn mounted_content_replica_candidates_from_entries(
    entries: impl IntoIterator<Item = sinomo_runtime::kv::KvEntry>,
) -> Vec<NodeId> {
    let mut candidates = Vec::new();
    for advertisement in entries
        .into_iter()
        .filter_map(|entry| entry.decode::<dfs::DfsNodeAdvertisement>().ok())
    {
        if advertisement.accepts_content && !candidates.contains(&advertisement.node_id) {
            candidates.push(advertisement.node_id);
        }
    }
    candidates
}

fn open_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &network::ClusterPaths,
    control_plane: Option<&MountedSifsControlPlaneKvClient>,
) -> Result<Box<dyn dfs::DfsMetadataBackend>, CloudFilesModuleError> {
    if cluster_paths.state.exists() {
        let control_plane = control_plane.ok_or_else(|| CloudFilesModuleError {
            code: "network".to_string(),
            detail: "clustered Cloud Files requires an authoritative DFS control-plane client"
                .to_string(),
        })?;
        let file_system_id = dfs::sifs_file_system_id();
        let mut shards = control_plane
            .range_prefix(&format!(
                "{}/",
                dfs::metadata_shards_prefix(&file_system_id)
            ))
            .map_err(error("network"))?
            .into_iter()
            .map(|entry| entry.decode::<dfs::DfsMetadataShardConfig>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(error("dfs"))?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        if !shards.is_empty() {
            let backend = dfs::open_local_sharded_metadata_backend(
                dfs::default_dfs_metadata_shards_root(data_dir),
                shards,
            )
            .map_err(error("dfs"))?;
            return Ok(Box::new(backend));
        }
    }
    dfs::open_root_local_metadata_backend(dfs::default_dfs_metadata_root(data_dir))
        .map(|backend| Box::new(backend) as Box<dyn dfs::DfsMetadataBackend>)
        .map_err(error("dfs"))
}

fn normalize_sifs_root(path: &str) -> Result<String, CloudFilesModuleError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(CloudFilesModuleError {
            code: "invalid-path".to_string(),
            detail: "SIFS root path cannot be empty".to_string(),
        });
    }
    if !trimmed.starts_with('/') {
        return Err(CloudFilesModuleError {
            code: "invalid-path".to_string(),
            detail: format!("SIFS root path `{path}` must be absolute"),
        });
    }
    let mut parts = Vec::new();
    for component in trimmed.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn ntstatus_for_posix_error(error: DfsPosixError) -> NTSTATUS {
    match error {
        DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
            STATUS_OBJECT_NAME_NOT_FOUND
        }
        DfsPosixError::NotDirectory { .. } => STATUS_NOT_A_DIRECTORY,
        DfsPosixError::IsDirectory { .. } => STATUS_FILE_IS_A_DIRECTORY,
        _ => STATUS_UNSUCCESSFUL,
    }
}

fn stale_identity_error(
    reason: CloudFilesStaleIdentityReason,
    expected: Option<CloudFilesPlaceholderIdentity>,
    observed: Option<CloudFilesPlaceholderIdentity>,
    detail: impl Into<String>,
) -> CloudFilesFetchDataError {
    CloudFilesFetchDataError::StaleIdentity(Box::new(CloudFilesStalePlaceholderIdentity {
        reason,
        expected,
        observed,
        detail: detail.into(),
    }))
}

fn ntstatus_for_fetch_error(error: &CloudFilesFetchDataError) -> NTSTATUS {
    if matches!(
        error,
        CloudFilesFetchDataError::Backend { code, .. } if code == "fetch-cancelled"
    ) {
        return STATUS_CANCELLED;
    }
    // A stale identity is a provider consistency failure, not proof that the
    // namespace path is absent. Returning a Cloud Files failure keeps Windows
    // from treating replacement bytes (or a false deletion) as authoritative.
    STATUS_CLOUD_FILE_UNSUCCESSFUL
}

fn unix_nanos_to_filetime(nanos: u64) -> i64 {
    (UNIX_EPOCH_AS_WINDOWS_FILETIME + nanos / 100).min(i64::MAX as u64) as i64
}

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn wide_str(value: &str) -> Vec<u16> {
    OsStr::new(value).encode_wide().chain(Some(0)).collect()
}

fn error<E>(code: &'static str) -> impl FnOnce(E) -> CloudFilesModuleError
where
    E: std::fmt::Display,
{
    move |source| CloudFilesModuleError {
        code: code.to_string(),
        detail: source.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::dfs::DfsMessageResponse;
    use sinomo_runtime::network::{ObjectRpcCall, ObjectRpcResponse, ObjectRpcResult};

    #[test]
    fn terminal_placeholder_page_completes_in_one_transfer() {
        let (paged_total, paged_flags) = placeholder_page_transfer_plan(64, true);
        assert_eq!(paged_total, 65);
        assert_eq!(paged_flags, CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE);

        let (terminal_total, terminal_flags) = placeholder_page_transfer_plan(64, false);
        assert_eq!(terminal_total, 64);
        assert_eq!(
            terminal_flags,
            CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_DISABLE_ON_DEMAND_POPULATION
        );
    }

    struct CursorRecoveryTestDir(PathBuf);

    impl CursorRecoveryTestDir {
        fn new(label: &str) -> Self {
            let suffix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("sinomo-{label}-{}-{suffix}", std::process::id()));
            std::fs::create_dir_all(&path).expect("test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for CursorRecoveryTestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn cursor_recovery_marker_fixture(
        data_dir: &SinomoDataDir,
        sync_root: &Path,
        phase: CursorRecoveryModulePhase,
    ) -> CursorRecoveryModuleMarker {
        let recovery_id = "cursor-recovery-test-id";
        let nonce = "0123456789abcdef0123456789abcdef";
        let identity = CloudFilesCursorOnlyRecoveryIdentity {
            schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            enabled: false,
            sync_root: sync_root.to_path_buf(),
            root_path: "/".to_string(),
            data_dir_root: data_dir.root(),
            provider_name: crate::protocol::SINOMO_DFS_CLOUD_FILES_PROVIDER_NAME.to_string(),
            provider_version: env!("CARGO_PKG_VERSION").to_string(),
            read_only: false,
            allow_other: false,
            sync_local_changes: true,
            prepopulate_depth: 0,
            connect: true,
            max_run_seconds: None,
            disabled_config_fingerprint: format!("blake3:{}", "1".repeat(64)),
            provider_provenance_fingerprint: format!("blake3:{}", "2".repeat(64)),
        };
        let provisional_token = format!("v1:blake3:{}", "0".repeat(64));
        let provisional_plan = crate::namespace_reconciler::cursor_recovery_plan_fixture(
            &sync_root.display().to_string(),
            "/",
            recovery_id,
            &provisional_token,
            nonce,
        );
        let mut reconciliation_raw_files = BTreeMap::new();
        reconciliation_raw_files.insert(
            "materialization-audit-intents.sobj".to_string(),
            provisional_plan.original_audit.clone(),
        );
        reconciliation_raw_files.insert(
            "state.sobj".to_string(),
            provisional_plan.original_state.clone(),
        );
        let module_topology = vec![
            "local-changes".to_string(),
            "namespace-reconciliation".to_string(),
            CLOUD_FILES_REGISTRATION_LOCK_FILE.to_string(),
        ];
        let queue_raw_files = BTreeMap::from([("operations.journal".to_string(), Vec::new())]);
        let outbox_raw_files = BTreeMap::from([("handoff.lock".to_string(), Vec::new())]);
        let raw_state_fingerprint = cursor_recovery_raw_fingerprint(
            &identity,
            41,
            &[],
            &module_topology,
            &queue_raw_files,
            &reconciliation_raw_files,
            &outbox_raw_files,
        )
        .expect("fixture raw fingerprint");
        let recovery_token = cursor_recovery_token(&raw_state_fingerprint);
        let plan = crate::namespace_reconciler::cursor_recovery_plan_fixture(
            &sync_root.display().to_string(),
            "/",
            recovery_id,
            &recovery_token,
            nonce,
        );
        CursorRecoveryModuleMarker {
            schema_version: CLOUD_FILES_CURSOR_ONLY_RECOVERY_SCHEMA_VERSION,
            identity,
            recovery_id: recovery_id.to_string(),
            recovery_token,
            raw_state_fingerprint,
            registration_lock_bytes: Vec::new(),
            module_topology,
            queue_raw_files,
            reconciliation_raw_files,
            outbox_raw_files,
            sync_root_file_id: 41,
            registration_nonce: nonce.to_string(),
            plan,
            recovery_cycle: 0,
            cycle_authorization_fingerprint: None,
            predecessor_cycle_fingerprint: None,
            cycle_authorized_namespace_resume: CursorRecoveryNamespaceResume::ReconcileProgress,
            namespace_resume: match phase {
                CursorRecoveryModulePhase::Ready => {
                    CursorRecoveryNamespaceResume::TerminalPendingFinalize
                }
                CursorRecoveryModulePhase::NamespaceFinalized
                | CursorRecoveryModulePhase::CompletionAuthorized => {
                    CursorRecoveryNamespaceResume::Finalized
                }
                _ => CursorRecoveryNamespaceResume::ReconcileProgress,
            },
            phase,
        }
    }

    fn write_canonical_registration_artifact<T: serde::Serialize>(path: &Path, value: &T) {
        std::fs::create_dir_all(path.parent().expect("artifact parent"))
            .expect("artifact directory");
        std::fs::write(path, encode_sobj(value).expect("canonical artifact bytes"))
            .expect("write artifact");
    }

    #[derive(Debug)]
    struct RecordingCloudFilesDaemonPeer {
        calls: Mutex<Vec<(NodeId, Vec<u8>, DfsObjectRpcSendOptions)>>,
        response: Mutex<Option<ObjectRpcResult>>,
    }

    impl DfsPeerTransport for RecordingCloudFilesDaemonPeer {
        fn call_object_rpc(
            &self,
            target_node_id: &NodeId,
            call: ObjectRpcCall,
            options: DfsObjectRpcSendOptions,
        ) -> std::result::Result<ObjectRpcResult, dfs::DfsPeerTransportError> {
            let body = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|source| dfs::DfsPeerTransportError::new(source.to_string()))?
                .block_on(call.body.collect_to_vec(1024))
                .map_err(|source| dfs::DfsPeerTransportError::new(source.to_string()))?;
            self.calls
                .lock()
                .expect("calls")
                .push((target_node_id.clone(), body, options));
            self.response
                .lock()
                .expect("response")
                .take()
                .ok_or_else(|| dfs::DfsPeerTransportError::new("missing test response"))
        }
    }

    fn read_request_message(source: &NodeId, target: &NodeId, frame_id: u64) -> DfsRoutedMessage {
        DfsRoutedMessage {
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            frame_id,
            message_id: frame_id,
            message: dfs::DfsNetworkMessage::ReadRequest(dfs::DfsReadRequest {
                request_id: format!("cloud-files-read-{frame_id}"),
                object_id: dfs::DfsObjectId::from_legacy_named(format!(
                    "cloud_files_read_object_{frame_id}"
                ))
                .expect("object id"),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        }
    }

    fn test_kv_entry(
        key: &str,
        advertisement: Option<&dfs::DfsNodeAdvertisement>,
    ) -> sinomo_runtime::kv::KvEntry {
        sinomo_runtime::kv::KvEntry {
            key: key.to_string(),
            value: advertisement
                .map(sinomo_runtime::kv::encode_kv_value)
                .transpose()
                .expect("encode advertisement")
                .unwrap_or_else(|| b"not-an-advertisement".to_vec()),
            create_revision: sinomo_runtime::kv::KvRevision(1),
            modify_revision: sinomo_runtime::kv::KvRevision(1),
            version: sinomo_runtime::kv::KvVersion(1),
            lease_id: None,
        }
    }

    #[test]
    fn hydration_plan_uses_bounded_aligned_chunks_and_exact_callback_range() {
        let file_size = (CLOUD_FILES_TRANSFER_CHUNK_BYTES * 2 + 123) as i64;
        let mut plan = HydrationPlan::new(0, CLOUD_FILES_EOF, file_size).expect("plan");
        assert_eq!(
            plan.callback,
            HydrationCallbackRange {
                offset: 0,
                length: CLOUD_FILES_EOF,
            }
        );

        let first = plan.next_chunk().expect("first chunk");
        assert_eq!(first.offset, 0);
        assert_eq!(first.length, CLOUD_FILES_TRANSFER_CHUNK_BYTES);
        assert_eq!(
            first.length as u64 % CLOUD_FILES_TRANSFER_ALIGNMENT_BYTES,
            0
        );
        plan.advance(first.length).expect("advance first");

        let second = plan.next_chunk().expect("second chunk");
        assert_eq!(second.offset, CLOUD_FILES_TRANSFER_CHUNK_BYTES as u64);
        assert_eq!(second.length, CLOUD_FILES_TRANSFER_CHUNK_BYTES);
        assert_eq!(
            second.length as u64 % CLOUD_FILES_TRANSFER_ALIGNMENT_BYTES,
            0
        );
        plan.advance(second.length).expect("advance second");

        let eof = plan.next_chunk().expect("EOF chunk");
        assert_eq!(eof.offset, (CLOUD_FILES_TRANSFER_CHUNK_BYTES * 2) as u64);
        assert_eq!(eof.length, 123);
        plan.advance(eof.length).expect("advance EOF");
        assert_eq!(plan.next_chunk(), None);
    }

    #[test]
    fn hydration_plan_rejects_unaligned_non_eof_ranges() {
        assert_eq!(
            HydrationPlan::new(0, 123, 8192),
            Err("FETCH_DATA length is not 4 KiB aligned before end of file")
        );
        assert!(HydrationPlan::new(4096, 123, 4219).is_ok());
    }

    #[test]
    fn placeholder_page_commit_requires_execute_count_and_every_hresult() {
        assert!(placeholder_transfer_fully_succeeded(
            true,
            2,
            2,
            &[HRESULT(0).0, HRESULT(1).0]
        ));
        assert!(!placeholder_transfer_fully_succeeded(
            false,
            2,
            2,
            &[HRESULT(0).0, HRESULT(0).0]
        ));
        assert!(!placeholder_transfer_fully_succeeded(
            true,
            2,
            1,
            &[HRESULT(0).0, HRESULT(0).0]
        ));
        assert!(!placeholder_transfer_fully_succeeded(
            true,
            2,
            2,
            &[HRESULT(0).0, HRESULT(-1).0]
        ));
    }

    #[test]
    fn placeholder_collision_disposition_creates_only_proven_absent_entries() {
        assert_eq!(
            placeholder_create_disposition(LocalPlaceholderCollisionState::Absent),
            PlaceholderCreateDisposition::CreateAbsent
        );
        assert_eq!(
            placeholder_create_disposition(LocalPlaceholderCollisionState::CleanPlaceholder),
            PlaceholderCreateDisposition::PreserveLocal
        );
        assert_eq!(
            placeholder_create_disposition(LocalPlaceholderCollisionState::DirtyPlaceholder),
            PlaceholderCreateDisposition::PreserveLocal
        );
        assert_eq!(
            placeholder_create_disposition(LocalPlaceholderCollisionState::OtherLocalEntry),
            PlaceholderCreateDisposition::PreserveLocal
        );
    }

    #[test]
    fn worker_readiness_requires_active_writeback_status() {
        assert!(!writable_worker_ready(
            &CloudFilesLocalChangeSyncStatus::writable_starting(0, 0, 0, 0)
        ));
        assert!(writable_worker_ready(
            &CloudFilesLocalChangeSyncStatus::writable_active(0, 0, 0, 0)
        ));
        assert!(!writable_worker_ready(
            &CloudFilesLocalChangeSyncStatus::disabled()
        ));
    }

    #[test]
    fn cursor_recovery_registration_snapshot_promotes_exact_causal_crash_cuts() {
        let temp = CursorRecoveryTestDir::new("cloud-files-cursor-recovery-artifact-cuts");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let sync_root = temp.path().join("root");
        let authorized = cursor_recovery_marker_fixture(
            &data_dir,
            &sync_root,
            CursorRecoveryModulePhase::Authorized,
        );
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_temp_path(&data_dir),
            &authorized,
        );
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, true, |_| Ok(()))
            .expect("promote request-bound Authorized marker")
            .expect("authorized marker");
        assert_eq!(recovered.phase, CursorRecoveryModulePhase::Authorized);
        assert!(cursor_recovery_module_marker_path(&data_dir).is_file());
        assert!(!cursor_recovery_module_marker_temp_path(&data_dir).exists());

        let validated =
            cursor_recovery_marker_with_phase(&authorized, CursorRecoveryModulePhase::Validated);
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_path(&data_dir),
            &validated,
        );
        let (state_owned, ready, receipt) =
            cursor_recovery_expected_registration_artifacts(&validated)
                .expect("expected registration artifacts");
        write_canonical_registration_artifact(
            &registration_journal_temp_path(&data_dir),
            &state_owned,
        );
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote StateOwned journal cut")
            .expect("validated marker");
        assert_eq!(recovered.phase, CursorRecoveryModulePhase::Validated);
        assert_eq!(
            load_registration_journal(&data_dir)
                .expect("journal load")
                .expect("journal"),
            state_owned
        );
        assert!(!registration_journal_temp_path(&data_dir).exists());

        let armed_progress = cursor_recovery_marker_with_phase(
            &authorized,
            CursorRecoveryModulePhase::ReconcileArmed,
        );
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_path(&data_dir),
            &armed_progress,
        );
        write_canonical_registration_artifact(&registration_receipt_temp_path(&data_dir), &receipt);
        let marker_before = std::fs::read(cursor_recovery_module_marker_path(&data_dir))
            .expect("progress marker before rejected receipt cut");
        let journal_before = std::fs::read(registration_journal_path(&data_dir))
            .expect("StateOwned journal before rejected receipt cut");
        let receipt_temp_before = std::fs::read(registration_receipt_temp_path(&data_dir))
            .expect("receipt temp before rejected progress cut");
        let error = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect_err("ReconcileProgress must reject Ready receipt evidence");
        assert_eq!(error.code, "cursor-recovery-terminal-successor-artifacts");
        assert_eq!(
            std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                .expect("progress marker after rejected receipt cut"),
            marker_before
        );
        assert_eq!(
            std::fs::read(registration_journal_path(&data_dir))
                .expect("StateOwned journal after rejected receipt cut"),
            journal_before
        );
        assert_eq!(
            std::fs::read(registration_receipt_temp_path(&data_dir))
                .expect("receipt temp after rejected progress cut"),
            receipt_temp_before
        );

        std::fs::remove_file(registration_receipt_temp_path(&data_dir))
            .expect("remove rejected progress receipt temp");
        let mut armed_terminal = armed_progress.clone();
        armed_terminal.namespace_resume = CursorRecoveryNamespaceResume::TerminalPendingFinalize;
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_path(&data_dir),
            &armed_terminal,
        );
        write_canonical_registration_artifact(&registration_receipt_temp_path(&data_dir), &receipt);
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote Ready receipt cut")
            .expect("armed marker");
        assert_eq!(recovered.phase, CursorRecoveryModulePhase::ReconcileArmed);
        assert_eq!(
            recovered.namespace_resume,
            CursorRecoveryNamespaceResume::TerminalPendingFinalize
        );
        assert!(registration_receipt_path(&data_dir).is_file());
        write_canonical_registration_artifact(&registration_journal_temp_path(&data_dir), &ready);
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote Ready journal cut")
            .expect("armed marker");
        assert_eq!(recovered.phase, CursorRecoveryModulePhase::ReconcileArmed);
        assert_eq!(
            load_registration_journal(&data_dir)
                .expect("ready journal load")
                .expect("ready journal"),
            ready
        );

        let ready_marker =
            cursor_recovery_marker_with_phase(&armed_terminal, CursorRecoveryModulePhase::Ready);
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_path(&data_dir),
            &ready_marker,
        );
        let namespace_finalized = cursor_recovery_marker_with_phase(
            &ready_marker,
            CursorRecoveryModulePhase::NamespaceFinalized,
        );
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_temp_path(&data_dir),
            &namespace_finalized,
        );
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote NamespaceFinalized marker cut")
            .expect("namespace-finalized marker");
        assert_eq!(
            recovered.phase,
            CursorRecoveryModulePhase::NamespaceFinalized
        );
    }

    #[test]
    fn cursor_recovery_registration_snapshot_replays_preserve_cycle_successors() {
        let temp = CursorRecoveryTestDir::new("cloud-files-cursor-recovery-preserve-cuts");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let sync_root = temp.path().join("root");
        let ready_marker =
            cursor_recovery_marker_fixture(&data_dir, &sync_root, CursorRecoveryModulePhase::Ready);
        let (state_owned, ready, receipt) =
            cursor_recovery_expected_registration_artifacts(&ready_marker)
                .expect("expected registration artifacts");
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_path(&data_dir),
            &ready_marker,
        );
        write_canonical_registration_artifact(&registration_journal_path(&data_dir), &ready);
        write_canonical_registration_artifact(&registration_receipt_path(&data_dir), &receipt);

        let preserve_authorized = cursor_recovery_preserve_authorized_successor(&ready_marker)
            .expect("PreserveAuthorized successor");
        let request_for = |marker: &CursorRecoveryModuleMarker,
                           recovery_cycle: u64,
                           native_successor_fingerprint: Option<String>| {
            CloudFilesUnregisterRequest {
                sync_root: marker.identity.sync_root.clone(),
                root_path: marker.identity.root_path.clone(),
                data_dir_root: Some(marker.identity.data_dir_root.clone()),
                provider_name: marker.identity.provider_name.clone(),
                cursor_recovery: Some(CloudFilesCursorRecoveryContext {
                    recovery_id: marker.recovery_id.clone(),
                    recovery_token: marker.recovery_token.clone(),
                    recovery_cycle,
                    native_successor_fingerprint,
                }),
            }
        };
        let predecessor_request = request_for(&ready_marker, 0, None);
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_temp_path(&data_dir),
            &preserve_authorized,
        );
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote PreserveAuthorized successor")
            .expect("PreserveAuthorized marker");
        assert_eq!(
            recovered.phase,
            CursorRecoveryModulePhase::PreserveAuthorized
        );
        assert_eq!(recovered.recovery_cycle, 1);
        assert_eq!(preserve_authorized.predecessor_cycle_fingerprint, None);
        assert_eq!(
            validate_cursor_recovery_preserve_authority(
                &preserve_authorized,
                &predecessor_request,
                &data_dir,
                Some(preserve_authorized.registration_lock_bytes.as_slice()),
            )
            .expect("PreserveAuthorized predecessor retry"),
            CursorRecoveryPreserveContextRelation::PredecessorRetry
        );

        let (preserved_state_owned, _, _) =
            cursor_recovery_expected_registration_artifacts(&preserve_authorized)
                .expect("preserved registration artifacts");
        assert_ne!(preserved_state_owned, state_owned);
        write_canonical_registration_artifact(
            &registration_journal_temp_path(&data_dir),
            &preserved_state_owned,
        );
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote next-cycle StateOwned journal")
            .expect("PreserveAuthorized marker after journal replay");
        assert_eq!(
            recovered.phase,
            CursorRecoveryModulePhase::PreserveAuthorized
        );
        assert_eq!(
            load_registration_journal(&data_dir)
                .expect("preserved journal load")
                .expect("preserved journal"),
            preserved_state_owned
        );

        let preserved = cursor_recovery_preserved_state_owned_successor(&preserve_authorized)
            .expect("PreservedStateOwned successor");
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_temp_path(&data_dir),
            &preserved,
        );
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote PreservedStateOwned successor")
            .expect("PreservedStateOwned marker");
        assert_eq!(
            recovered.phase,
            CursorRecoveryModulePhase::PreservedStateOwned
        );
        let successor_fingerprint = cursor_recovery_native_successor_fingerprint(&recovered)
            .expect("native successor fingerprint");
        assert_eq!(
            validate_cursor_recovery_preserve_authority(
                &preserved,
                &predecessor_request,
                &data_dir,
                Some(preserved.registration_lock_bytes.as_slice()),
            )
            .expect("PreservedStateOwned predecessor retry"),
            CursorRecoveryPreserveContextRelation::PredecessorRetry
        );
        let current_request = request_for(&preserved, 1, Some(successor_fingerprint.clone()));
        assert_eq!(
            validate_cursor_recovery_preserve_authority(
                &preserved,
                &current_request,
                &data_dir,
                Some(preserved.registration_lock_bytes.as_slice()),
            )
            .expect("rapid current-cycle preserve"),
            CursorRecoveryPreserveContextRelation::Current
        );

        let rearmed = cursor_recovery_rearmed_successor(&preserved).expect("rearmed successor");
        assert_eq!(
            rearmed.cycle_authorization_fingerprint.as_deref(),
            Some(successor_fingerprint.as_str())
        );
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_temp_path(&data_dir),
            &rearmed,
        );
        let recovered = recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect("promote rearmed successor")
            .expect("rearmed marker");
        assert_eq!(recovered, rearmed);
        assert_eq!(recovered.recovery_cycle, 1);
        assert!(!cursor_recovery_module_marker_temp_path(&data_dir).exists());
        assert_eq!(
            validate_cursor_recovery_preserve_authority(
                &rearmed,
                &current_request,
                &data_dir,
                Some(rearmed.registration_lock_bytes.as_slice()),
            )
            .expect("rearmed current cycle"),
            CursorRecoveryPreserveContextRelation::Current
        );

        let wrong_request = request_for(&rearmed, 1, Some(format!("blake3:{}", "9".repeat(64))));
        assert!(
            validate_cursor_recovery_preserve_authority(
                &rearmed,
                &wrong_request,
                &data_dir,
                Some(rearmed.registration_lock_bytes.as_slice()),
            )
            .is_err()
        );

        let preserve_authorized_two = cursor_recovery_preserve_authorized_successor(&rearmed)
            .expect("second PreserveAuthorized successor");
        assert_eq!(preserve_authorized_two.recovery_cycle, 2);
        assert_eq!(
            preserve_authorized_two
                .predecessor_cycle_fingerprint
                .as_deref(),
            Some(successor_fingerprint.as_str())
        );
        assert_eq!(
            validate_cursor_recovery_preserve_authority(
                &preserve_authorized_two,
                &current_request,
                &data_dir,
                Some(preserve_authorized_two.registration_lock_bytes.as_slice()),
            )
            .expect("second-cycle predecessor retry"),
            CursorRecoveryPreserveContextRelation::PredecessorRetry
        );
        let preserved_two =
            cursor_recovery_preserved_state_owned_successor(&preserve_authorized_two)
                .expect("second PreservedStateOwned successor");
        let successor_fingerprint_two =
            cursor_recovery_native_successor_fingerprint(&preserved_two)
                .expect("second native successor fingerprint");
        assert_ne!(successor_fingerprint_two, successor_fingerprint);
        let mut changed_predecessor = preserved_two.clone();
        changed_predecessor.predecessor_cycle_fingerprint =
            Some(format!("blake3:{}", "8".repeat(64)));
        assert_ne!(
            cursor_recovery_native_successor_fingerprint(&changed_predecessor)
                .expect("changed predecessor fingerprint"),
            successor_fingerprint_two
        );
        let mut rearmed_with_changed_predecessor =
            cursor_recovery_rearmed_successor(&preserved_two)
                .expect("second-cycle rearmed successor");
        rearmed_with_changed_predecessor.predecessor_cycle_fingerprint =
            changed_predecessor.predecessor_cycle_fingerprint;
        assert_eq!(
            validate_cursor_recovery_marker(&rearmed_with_changed_predecessor)
                .expect_err("changed predecessor must invalidate installed cycle proof")
                .code,
            "cursor-recovery-cycle-authorization-mismatch"
        );

        let finalized = cursor_recovery_marker_fixture(
            &data_dir,
            &sync_root,
            CursorRecoveryModulePhase::NamespaceFinalized,
        );
        let preserved_finalized = cursor_recovery_preserved_state_owned_successor(
            &cursor_recovery_preserve_authorized_successor(&finalized)
                .expect("finalized PreserveAuthorized successor"),
        )
        .expect("finalized PreservedStateOwned successor");
        let rearmed_finalized = cursor_recovery_rearmed_successor(&preserved_finalized)
            .expect("finalized rearmed successor");
        assert_eq!(
            rearmed_finalized.namespace_resume,
            CursorRecoveryNamespaceResume::Finalized
        );

        let final_before = std::fs::read(cursor_recovery_module_marker_path(&data_dir))
            .expect("rearmed marker before changed temp");
        let mut changed_temp =
            cursor_recovery_marker_with_phase(&rearmed, CursorRecoveryModulePhase::Ready);
        changed_temp.recovery_id.push_str("-changed");
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_temp_path(&data_dir),
            &changed_temp,
        );
        let changed_temp_before = std::fs::read(cursor_recovery_module_marker_temp_path(&data_dir))
            .expect("changed marker temp before rejection");
        assert!(
            recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(())).is_err()
        );
        assert_eq!(
            std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                .expect("rearmed marker after changed temp rejection"),
            final_before
        );
        assert_eq!(
            std::fs::read(cursor_recovery_module_marker_temp_path(&data_dir))
                .expect("changed marker temp after rejection"),
            changed_temp_before
        );
    }

    fn completion_authorized_fixture(
        data_dir: &SinomoDataDir,
        sync_root: &Path,
    ) -> (
        CursorRecoveryModuleMarker,
        RegistrationJournal,
        RegistrationJournal,
        CloudFilesRegistrationReceipt,
    ) {
        let marker = cursor_recovery_marker_fixture(
            data_dir,
            sync_root,
            CursorRecoveryModulePhase::CompletionAuthorized,
        );
        let (_, mut recovery_ready, receipt) =
            cursor_recovery_expected_registration_artifacts(&marker)
                .expect("completion registration artifacts");
        recovery_ready.phase = RegistrationPhase::Ready;
        let mut ordinary_ready = recovery_ready.clone();
        ordinary_ready.clear_cursor_recovery();
        (marker, recovery_ready, ordinary_ready, receipt)
    }

    #[test]
    fn cursor_recovery_completion_authority_replays_exact_normalization_and_clear_cuts() {
        for cut in [
            "namespace-finalized-ca-temp",
            "normalization-temp",
            "normalized-redundant-temp",
        ] {
            let temp = CursorRecoveryTestDir::new(cut);
            let data_dir = SinomoDataDir::new(temp.path().join("data"));
            let sync_root = temp.path().join("root");
            let (completion, recovery_ready, ordinary_ready, receipt) =
                completion_authorized_fixture(&data_dir, &sync_root);
            let namespace_finalized = cursor_recovery_marker_with_phase(
                &completion,
                CursorRecoveryModulePhase::NamespaceFinalized,
            );

            match cut {
                "namespace-finalized-ca-temp" => {
                    write_canonical_registration_artifact(
                        &cursor_recovery_module_marker_path(&data_dir),
                        &namespace_finalized,
                    );
                    write_canonical_registration_artifact(
                        &cursor_recovery_module_marker_temp_path(&data_dir),
                        &completion,
                    );
                    write_canonical_registration_artifact(
                        &registration_journal_path(&data_dir),
                        &recovery_ready,
                    );
                }
                "normalization-temp" => {
                    write_canonical_registration_artifact(
                        &cursor_recovery_module_marker_path(&data_dir),
                        &completion,
                    );
                    write_canonical_registration_artifact(
                        &registration_journal_path(&data_dir),
                        &recovery_ready,
                    );
                    write_canonical_registration_artifact(
                        &registration_journal_temp_path(&data_dir),
                        &ordinary_ready,
                    );
                }
                "normalized-redundant-temp" => {
                    write_canonical_registration_artifact(
                        &cursor_recovery_module_marker_path(&data_dir),
                        &completion,
                    );
                    write_canonical_registration_artifact(
                        &registration_journal_path(&data_dir),
                        &ordinary_ready,
                    );
                    write_canonical_registration_artifact(
                        &registration_journal_temp_path(&data_dir),
                        &ordinary_ready,
                    );
                }
                _ => unreachable!(),
            }
            write_canonical_registration_artifact(&registration_receipt_path(&data_dir), &receipt);

            let recovered =
                recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
                    .expect("recover exact completion cut")
                    .expect("completion marker");
            assert_eq!(recovered, completion);
            assert!(!cursor_recovery_module_marker_temp_path(&data_dir).exists());
            if cut != "namespace-finalized-ca-temp" {
                assert_eq!(
                    load_registration_journal(&data_dir)
                        .expect("load normalized journal")
                        .expect("normalized journal"),
                    ordinary_ready
                );
                assert!(!registration_journal_temp_path(&data_dir).exists());
            }
        }

        for changed in ["marker-temp", "receipt"] {
            let temp = CursorRecoveryTestDir::new(if changed == "marker-temp" {
                "cloud-files-completion-changed-marker-temp"
            } else {
                "cloud-files-completion-changed-receipt"
            });
            let data_dir = SinomoDataDir::new(temp.path().join("data"));
            let sync_root = temp.path().join("root");
            let (completion, recovery_ready, _, receipt) =
                completion_authorized_fixture(&data_dir, &sync_root);
            write_canonical_registration_artifact(
                &cursor_recovery_module_marker_path(&data_dir),
                &completion,
            );
            write_canonical_registration_artifact(
                &registration_journal_path(&data_dir),
                &recovery_ready,
            );
            write_canonical_registration_artifact(&registration_receipt_path(&data_dir), &receipt);
            if changed == "marker-temp" {
                let mut changed_marker = completion.clone();
                changed_marker.recovery_id.push_str("-changed");
                write_canonical_registration_artifact(
                    &cursor_recovery_module_marker_temp_path(&data_dir),
                    &changed_marker,
                );
            } else {
                let mut changed_receipt = receipt.clone();
                changed_receipt.provider_name.push_str(" changed");
                write_canonical_registration_artifact(
                    &registration_receipt_path(&data_dir),
                    &changed_receipt,
                );
            }
            let marker_before = std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                .expect("completion marker before changed evidence");
            let journal_before = std::fs::read(registration_journal_path(&data_dir))
                .expect("completion journal before changed evidence");
            let receipt_before = std::fs::read(registration_receipt_path(&data_dir))
                .expect("completion receipt before changed evidence");
            let marker_temp_before =
                std::fs::read(cursor_recovery_module_marker_temp_path(&data_dir)).ok();
            recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
                .expect_err("changed completion evidence must fail closed");
            assert_eq!(
                std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                    .expect("completion marker after changed evidence"),
                marker_before
            );
            assert_eq!(
                std::fs::read(registration_journal_path(&data_dir))
                    .expect("completion journal after changed evidence"),
                journal_before
            );
            assert_eq!(
                std::fs::read(registration_receipt_path(&data_dir))
                    .expect("completion receipt after changed evidence"),
                receipt_before
            );
            assert_eq!(
                std::fs::read(cursor_recovery_module_marker_temp_path(&data_dir)).ok(),
                marker_temp_before
            );
        }

        let temp = CursorRecoveryTestDir::new("cloud-files-completion-clear-matrix");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let sync_root = temp.path().join("root");
        let (completion, recovery_ready, ordinary_ready, receipt) =
            completion_authorized_fixture(&data_dir, &sync_root);
        write_canonical_registration_artifact(
            &cursor_recovery_module_marker_path(&data_dir),
            &completion,
        );
        write_canonical_registration_artifact(
            &registration_journal_path(&data_dir),
            &recovery_ready,
        );
        write_canonical_registration_artifact(&registration_receipt_path(&data_dir), &receipt);

        let marker_before_normalize = std::fs::read(cursor_recovery_module_marker_path(&data_dir))
            .expect("completion marker before premature clear");
        let journal_before_normalize = std::fs::read(registration_journal_path(&data_dir))
            .expect("recovery journal before premature clear");
        let receipt_before_normalize = std::fs::read(registration_receipt_path(&data_dir))
            .expect("receipt before premature clear");
        clear_completion_authorized_cursor_recovery_module_marker(&data_dir, &completion)
            .expect_err("CompletionAuthorized cannot clear before journal normalization");
        assert_eq!(
            std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                .expect("completion marker after premature clear"),
            marker_before_normalize
        );
        assert_eq!(
            std::fs::read(registration_journal_path(&data_dir))
                .expect("recovery journal after premature clear"),
            journal_before_normalize
        );
        assert_eq!(
            std::fs::read(registration_receipt_path(&data_dir))
                .expect("receipt after premature clear"),
            receipt_before_normalize
        );

        normalize_completed_cursor_recovery_journal(&data_dir, &completion)
            .expect("normalize completion journal");
        assert_eq!(
            load_registration_journal(&data_dir)
                .expect("load ordinary journal")
                .expect("ordinary journal"),
            ordinary_ready
        );

        let mut changed_ready = ordinary_ready.clone();
        changed_ready.provider_version.push_str("-changed");
        write_canonical_registration_artifact(
            &registration_journal_temp_path(&data_dir),
            &changed_ready,
        );
        let marker_before = std::fs::read(cursor_recovery_module_marker_path(&data_dir))
            .expect("completion marker before changed journal temp");
        let journal_before = std::fs::read(registration_journal_path(&data_dir))
            .expect("ordinary journal before changed temp");
        let receipt_before = std::fs::read(registration_receipt_path(&data_dir))
            .expect("receipt before changed journal temp");
        let changed_temp_before = std::fs::read(registration_journal_temp_path(&data_dir))
            .expect("changed journal temp before rejection");
        recover_cursor_recovery_registration_snapshot(&data_dir, false, |_| Ok(()))
            .expect_err("changed normalized journal temp must fail closed");
        assert_eq!(
            std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                .expect("completion marker after changed journal temp"),
            marker_before
        );
        assert_eq!(
            std::fs::read(registration_journal_path(&data_dir))
                .expect("ordinary journal after changed temp"),
            journal_before
        );
        assert_eq!(
            std::fs::read(registration_receipt_path(&data_dir))
                .expect("receipt after changed journal temp"),
            receipt_before
        );
        assert_eq!(
            std::fs::read(registration_journal_temp_path(&data_dir))
                .expect("changed journal temp after rejection"),
            changed_temp_before
        );
        std::fs::remove_file(registration_journal_temp_path(&data_dir))
            .expect("remove changed journal temp");

        for (path, value) in [
            (
                cursor_recovery_module_marker_temp_path(&data_dir),
                encode_sobj(&completion).expect("marker temp bytes"),
            ),
            (
                registration_journal_temp_path(&data_dir),
                encode_sobj(&ordinary_ready).expect("journal temp bytes"),
            ),
            (
                registration_receipt_temp_path(&data_dir),
                encode_sobj(&receipt).expect("receipt temp bytes"),
            ),
        ] {
            std::fs::write(&path, &value).expect("write rejected completion temp");
            let marker_before = std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                .expect("completion marker before rejection");
            let temp_before = std::fs::read(&path).expect("temporary bytes before rejection");
            clear_completion_authorized_cursor_recovery_module_marker(&data_dir, &completion)
                .expect_err("completion clear must reject temporary evidence");
            assert_eq!(
                std::fs::read(cursor_recovery_module_marker_path(&data_dir))
                    .expect("completion marker after rejection"),
                marker_before
            );
            assert_eq!(
                std::fs::read(&path).expect("temporary bytes after rejection"),
                temp_before
            );
            std::fs::remove_file(path).expect("remove rejected temporary evidence");
        }
        clear_completion_authorized_cursor_recovery_module_marker(&data_dir, &completion)
            .expect("clear exact CompletionAuthorized marker");
        assert!(!cursor_recovery_module_marker_path(&data_dir).exists());
    }

    #[test]
    fn cursor_recovery_terminal_handoff_orders_four_acks_and_cancels_before_unpark() {
        let temp = CursorRecoveryTestDir::new("cloud-files-terminal-handoff-order");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let sync_root = temp.path().join("root");
        let mut marker = cursor_recovery_marker_fixture(
            &data_dir,
            &sync_root,
            CursorRecoveryModulePhase::NamespaceFinalized,
        );
        let order = std::cell::RefCell::new(Vec::new());
        let cancelled = std::cell::Cell::new(false);
        let completed = run_cursor_recovery_terminal_handoff_with(
            &mut marker,
            |step, marker| {
                order.borrow_mut().push(step);
                match step {
                    CursorRecoveryTerminalHandoffStep::PersistCompletionAuthorized => {
                        marker.phase = CursorRecoveryModulePhase::CompletionAuthorized;
                    }
                    CursorRecoveryTerminalHandoffStep::AcknowledgeAuthorityRelease => {
                        cancelled.set(true);
                    }
                    _ => {}
                }
                Ok(())
            },
            || cancelled.get(),
        )
        .expect("terminal handoff with final cancellation");
        assert!(!completed);
        assert_eq!(
            order.into_inner(),
            vec![
                CursorRecoveryTerminalHandoffStep::AcknowledgeNamespaceFinalized,
                CursorRecoveryTerminalHandoffStep::PersistCompletionAuthorized,
                CursorRecoveryTerminalHandoffStep::AcknowledgeCompletionAuthority,
                CursorRecoveryTerminalHandoffStep::NormalizeRegistration,
                CursorRecoveryTerminalHandoffStep::AcknowledgeNormalization,
                CursorRecoveryTerminalHandoffStep::ClearCompletionAuthorized,
                CursorRecoveryTerminalHandoffStep::AcknowledgeAuthorityRelease,
            ]
        );

        let mut marker = cursor_recovery_marker_fixture(
            &data_dir,
            &sync_root,
            CursorRecoveryModulePhase::CompletionAuthorized,
        );
        let success_order = std::cell::RefCell::new(Vec::new());
        assert!(
            run_cursor_recovery_terminal_handoff_with(
                &mut marker,
                |step, _| {
                    success_order.borrow_mut().push(step);
                    Ok(())
                },
                || false,
            )
            .expect("uncancelled terminal handoff")
        );
        assert_eq!(
            success_order.into_inner(),
            vec![
                CursorRecoveryTerminalHandoffStep::AcknowledgeCompletionAuthority,
                CursorRecoveryTerminalHandoffStep::NormalizeRegistration,
                CursorRecoveryTerminalHandoffStep::AcknowledgeNormalization,
                CursorRecoveryTerminalHandoffStep::ClearCompletionAuthorized,
                CursorRecoveryTerminalHandoffStep::AcknowledgeAuthorityRelease,
                CursorRecoveryTerminalHandoffStep::CompleteWorker,
                CursorRecoveryTerminalHandoffStep::ReleasePopulation,
            ]
        );

        let mut marker = cursor_recovery_marker_fixture(
            &data_dir,
            &sync_root,
            CursorRecoveryModulePhase::CompletionAuthorized,
        );
        let after_complete_order = std::cell::RefCell::new(Vec::new());
        let cancel_after_complete = std::cell::Cell::new(false);
        assert!(
            !run_cursor_recovery_terminal_handoff_with(
                &mut marker,
                |step, _| {
                    after_complete_order.borrow_mut().push(step);
                    if step == CursorRecoveryTerminalHandoffStep::CompleteWorker {
                        cancel_after_complete.set(true);
                    }
                    Ok(())
                },
                || cancel_after_complete.get(),
            )
            .expect("cancellation after CompleteWorker")
        );
        assert_eq!(
            after_complete_order.borrow().last().copied(),
            Some(CursorRecoveryTerminalHandoffStep::CompleteWorker)
        );
        assert!(
            !after_complete_order
                .borrow()
                .contains(&CursorRecoveryTerminalHandoffStep::ReleasePopulation)
        );

        for fail_at in [
            CursorRecoveryTerminalHandoffStep::AcknowledgeNamespaceFinalized,
            CursorRecoveryTerminalHandoffStep::PersistCompletionAuthorized,
            CursorRecoveryTerminalHandoffStep::AcknowledgeCompletionAuthority,
            CursorRecoveryTerminalHandoffStep::NormalizeRegistration,
            CursorRecoveryTerminalHandoffStep::AcknowledgeNormalization,
            CursorRecoveryTerminalHandoffStep::ClearCompletionAuthorized,
            CursorRecoveryTerminalHandoffStep::AcknowledgeAuthorityRelease,
        ] {
            let mut marker = cursor_recovery_marker_fixture(
                &data_dir,
                &sync_root,
                CursorRecoveryModulePhase::NamespaceFinalized,
            );
            let observed = std::cell::RefCell::new(Vec::new());
            let error = run_cursor_recovery_terminal_handoff_with(
                &mut marker,
                |step, marker| {
                    observed.borrow_mut().push(step);
                    if step == fail_at {
                        return Err(CloudFilesModuleError {
                            code: "injected-terminal-cut".to_string(),
                            detail: format!("fail at {step:?}"),
                        });
                    }
                    if step == CursorRecoveryTerminalHandoffStep::PersistCompletionAuthorized {
                        marker.phase = CursorRecoveryModulePhase::CompletionAuthorized;
                    }
                    Ok(())
                },
                || false,
            )
            .expect_err("injected durable boundary must stop the handoff");
            assert_eq!(error.code, "injected-terminal-cut");
            assert_eq!(observed.borrow().last().copied(), Some(fail_at));
            assert!(
                !observed
                    .borrow()
                    .contains(&CursorRecoveryTerminalHandoffStep::CompleteWorker)
            );
        }
    }

    #[test]
    fn registration_artifact_same_image_is_a_strict_noop() {
        let temp = CursorRecoveryTestDir::new("cloud-files-registration-artifact-same-image");
        let data_dir = SinomoDataDir::new(temp.path().join("data"));
        let sync_root = temp.path().join("root");
        let marker = cursor_recovery_marker_fixture(
            &data_dir,
            &sync_root,
            CursorRecoveryModulePhase::Authorized,
        );
        let path = cursor_recovery_module_marker_path(&data_dir);
        write_canonical_registration_artifact(&path, &marker);
        let before = std::fs::read(&path).expect("marker before no-op");
        persist_cursor_recovery_registration_artifact(
            &path,
            &marker,
            "cursor-recovery-marker",
            "Cloud Files cursor recovery marker",
        )
        .expect("same-image persistence");
        assert_eq!(std::fs::read(&path).expect("marker after no-op"), before);
        assert!(!cursor_recovery_module_marker_temp_path(&data_dir).exists());
    }

    #[test]
    fn materialization_intent_precedes_effect_and_failure_skips_effect() {
        let order = std::cell::RefCell::new(Vec::new());
        let succeeded = after_durable_materialization_intent(
            || {
                order.borrow_mut().push("durable-intent");
                Ok::<(), &'static str>(())
            },
            || {
                order.borrow_mut().push("placeholder-effect");
                7
            },
        );
        assert_eq!(succeeded, Ok(7));
        assert_eq!(
            order.into_inner(),
            vec!["durable-intent", "placeholder-effect"]
        );

        let effect_called = std::cell::Cell::new(false);
        let failed = after_durable_materialization_intent(
            || Err::<(), _>("persist-failed"),
            || {
                effect_called.set(true);
                9
            },
        );
        assert_eq!(failed, Err("persist-failed"));
        assert!(!effect_called.get());
    }

    #[test]
    fn cursor_recovery_parks_root_population_until_terminal_release() {
        let released = std::sync::atomic::AtomicBool::new(false);
        assert!(population_is_parked(
            Some(&released),
            true,
            "/services",
            "/services"
        ));
        assert!(population_is_parked(
            Some(&released),
            true,
            "/services/nested",
            "/services"
        ));

        // Ordinary restart of an existing owned root still admits the root
        // page required to reconnect, but parks its descendants until Ready.
        assert!(!population_is_parked(
            Some(&released),
            false,
            "/services",
            "/services"
        ));
        assert!(population_is_parked(
            Some(&released),
            false,
            "/services/nested",
            "/services"
        ));

        released.store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(!population_is_parked(
            Some(&released),
            true,
            "/services/nested",
            "/services"
        ));
    }

    #[test]
    fn cloud_files_transport_uses_shared_default_health_timeout() {
        let local = NodeId::from_legacy_named("cloud_files_timeout_local").expect("local");
        let mut transport = DfsCloudFilesEndpointTransport::new(local, Vec::new(), None);
        let expected_health_timeout =
            network::DEFAULT_NETWORK_TIMEOUT.min(CLOUD_FILES_DFS_CALL_TIMEOUT);

        let ordinary = transport.send_timeouts().expect("ordinary timeouts");
        assert_eq!(
            ordinary,
            network::DaemonRpcTimeouts::connect_and_idle(expected_health_timeout)
        );
        assert_eq!(
            transport.send_options().expect("ordinary options"),
            DfsObjectRpcSendOptions::new(expected_health_timeout)
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        assert_eq!(transport.set_operation_deadline(Some(deadline)), None);
        assert_eq!(transport.operation_deadline(), Some(deadline));
        let bounded = transport
            .send_timeouts()
            .expect("deadline-bounded timeouts");
        assert!(
            bounded
                .connect_timeout
                .is_some_and(|timeout| timeout <= Duration::from_secs(2))
        );
        assert!(
            bounded
                .idle_timeout
                .is_some_and(|timeout| timeout <= Duration::from_secs(2))
        );
        let bounded_options = transport.send_options().expect("bounded options");
        assert!(
            bounded_options.connect_timeout <= Duration::from_secs(2)
                && bounded_options
                    .overall_timeout
                    .is_some_and(|timeout| timeout <= Duration::from_secs(2))
        );
        assert_eq!(transport.set_operation_deadline(None), Some(deadline));
        assert_eq!(transport.operation_deadline(), None);
    }

    #[test]
    fn cloud_files_transport_delegates_remote_attachment_to_daemon_peer() {
        let local = NodeId::from_legacy_named("cloud_files_daemon_peer_local").expect("local");
        let remote = NodeId::from_legacy_named("cloud_files_daemon_peer_remote").expect("remote");
        let expected_health_timeout =
            network::DEFAULT_NETWORK_TIMEOUT.min(CLOUD_FILES_DFS_CALL_TIMEOUT);
        let returned = DfsRoutedMessage {
            source_node_id: remote.clone(),
            target_node_id: local.clone(),
            frame_id: 109,
            message_id: 109,
            message: dfs::DfsNetworkMessage::ReadResponse(dfs::DfsReadResponse {
                request_id: "cloud-files-daemon-peer-return".to_string(),
                object_id: dfs::DfsObjectId::from_legacy_named(
                    "cloud_files_daemon_peer_return_object",
                )
                .expect("object id"),
                result: dfs::DfsReadResult::NotFound,
            }),
        };
        let response = ObjectRpcResponse::encode_body(
            dfs::DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE,
            &DfsMessageResponse {
                request_id: "dfs_message_107".to_string(),
                outbound_messages: vec![returned.clone()],
            },
        )
        .expect("encode response");
        let peer = Arc::new(RecordingCloudFilesDaemonPeer {
            calls: Mutex::new(Vec::new()),
            response: Mutex::new(Some(ObjectRpcResult::value(response))),
        });
        let mut transport = DfsCloudFilesEndpointTransport::new(
            local.clone(),
            vec![remote.clone()],
            Some(peer.clone()),
        );
        let bytes = b"cloud-files-daemon-peer-attachment".to_vec();
        let message = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: remote.clone(),
            frame_id: 107,
            message_id: 107,
            message: dfs::DfsNetworkMessage::StageBlob(dfs::DfsStageBlobCommand {
                request_id: "cloud-files-daemon-peer-stage".to_string(),
                intent_id: "cloud-files-daemon-peer-intent".to_string(),
                file_system_id: dfs::sifs_file_system_id(),
                object_id: dfs::DfsObjectId::from_legacy_named(
                    "cloud_files_daemon_peer_stage_object",
                )
                .expect("object id"),
                expected_base_version: None,
                version: dfs::DfsVersion::from_legacy_named(
                    "cloud_files_daemon_peer_stage_version",
                )
                .expect("version"),
                content_hash: "cloud-files-daemon-peer-content".to_string(),
                bytes: Vec::new(),
                object_metadata: None,
                authority_shard: None,
            }),
        };

        transport
            .send_routed_call(
                &remote,
                DfsRoutedCall::with_attachment(
                    message,
                    dfs::DfsOutboundAttachment::from_bytes(
                        bytes.clone(),
                        "cloud-files-daemon-peer-content",
                    ),
                ),
            )
            .expect("delegate through daemon peer");

        assert_eq!(
            *peer.calls.lock().expect("calls"),
            vec![(
                remote,
                bytes,
                DfsObjectRpcSendOptions::new(expected_health_timeout),
            )]
        );
        assert_eq!(
            transport
                .drain_incoming_messages()
                .expect("returned local message"),
            vec![returned]
        );
    }

    #[test]
    fn cloud_files_transport_fails_closed_without_daemon_peer() {
        let local = NodeId::from_legacy_named("cloud_files_no_peer_local").expect("local");
        let remote = NodeId::from_legacy_named("cloud_files_no_peer_remote").expect("remote");
        let mut transport =
            DfsCloudFilesEndpointTransport::new(local.clone(), vec![remote.clone()], None);

        let error = transport
            .send_routed_call(
                &remote,
                DfsRoutedCall::value(read_request_message(&local, &remote, 111)),
            )
            .expect_err("missing daemon peer must fail closed");
        assert!(error.to_string().contains("local daemon DFS service path"));
    }

    #[test]
    fn cloud_files_replica_candidates_preserve_order_filter_and_deduplicate() {
        let first = NodeId::from_legacy_named("cloud_files_candidate_first").expect("first");
        let second = NodeId::from_legacy_named("cloud_files_candidate_second").expect("second");
        let rejected =
            NodeId::from_legacy_named("cloud_files_candidate_rejected").expect("rejected");
        let first_advertisement = dfs::DfsNodeAdvertisement::content(first.clone());
        let second_advertisement = dfs::DfsNodeAdvertisement::content(second.clone());
        let mut rejected_advertisement = dfs::DfsNodeAdvertisement::content(rejected);
        rejected_advertisement.accepts_content = false;

        let candidates = mounted_content_replica_candidates_from_entries([
            test_kv_entry("first", Some(&first_advertisement)),
            test_kv_entry("invalid", None),
            test_kv_entry("rejected", Some(&rejected_advertisement)),
            test_kv_entry("first-duplicate", Some(&first_advertisement)),
            test_kv_entry("second", Some(&second_advertisement)),
        ]);

        assert_eq!(candidates, vec![first, second]);
    }

    #[test]
    fn cloud_files_transport_queues_local_value_calls_without_network_routing() {
        let local = NodeId::from_legacy_named("cloud_files_loopback_value").expect("local");
        let mut transport = DfsCloudFilesEndpointTransport::new(local.clone(), Vec::new(), None);
        let message = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: local.clone(),
            frame_id: 41,
            message_id: 41,
            message: dfs::DfsNetworkMessage::ReadRequest(dfs::DfsReadRequest {
                request_id: "cloud-files-loopback-read".to_string(),
                object_id: dfs::DfsObjectId::from_legacy_named("cloud_files_loopback_object")
                    .expect("object id"),
                version: None,
                content_hash: None,
                content_size_bytes: None,
                range_offset: None,
                range_max_bytes: None,
            }),
        };

        transport
            .send_routed_call(&local, DfsRoutedCall::value(message.clone()))
            .expect("queue local value call");

        assert_eq!(
            transport
                .drain_incoming_messages()
                .expect("drain loopback value"),
            vec![message]
        );
    }

    #[test]
    fn cloud_files_transport_materializes_local_attachments_before_queueing() {
        let local = NodeId::from_legacy_named("cloud_files_loopback_attachment").expect("local");
        let mut transport = DfsCloudFilesEndpointTransport::new(local.clone(), Vec::new(), None);
        let bytes = b"cloud-files-loopback-attachment".to_vec();
        let message = DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: local.clone(),
            frame_id: 43,
            message_id: 43,
            message: dfs::DfsNetworkMessage::StageBlob(dfs::DfsStageBlobCommand {
                request_id: "cloud-files-loopback-stage".to_string(),
                intent_id: "cloud-files-loopback-intent".to_string(),
                file_system_id: dfs::sifs_file_system_id(),
                object_id: dfs::DfsObjectId::from_legacy_named(
                    "cloud_files_loopback_attachment_object",
                )
                .expect("object id"),
                expected_base_version: None,
                version: dfs::DfsVersion::from_legacy_named(
                    "cloud_files_loopback_attachment_version",
                )
                .expect("version"),
                content_hash: "cloud-files-loopback-content".to_string(),
                bytes: Vec::new(),
                object_metadata: None,
                authority_shard: None,
            }),
        };
        let call = DfsRoutedCall::with_attachment(
            message,
            dfs::DfsOutboundAttachment::from_bytes(bytes.clone(), "cloud-files-loopback-content"),
        );

        transport
            .send_routed_call(&local, call)
            .expect("queue local attachment call");

        let queued = transport
            .drain_incoming_messages()
            .expect("drain loopback attachment");
        assert_eq!(queued.len(), 1);
        let dfs::DfsNetworkMessage::StageBlob(command) = &queued[0].message else {
            panic!("expected staged blob loopback message");
        };
        assert_eq!(command.bytes, bytes);
        assert_eq!(command.content_hash, "cloud-files-loopback-content");
    }

    #[test]
    fn only_file_directory_and_mount_kinds_are_cloud_files_placeholders() {
        assert!(is_visible_placeholder_kind(&DfsObjectKind::File));
        assert!(is_visible_placeholder_kind(&DfsObjectKind::Directory));
        assert!(is_visible_placeholder_kind(&DfsObjectKind::Mount));
        assert!(!is_visible_placeholder_kind(&DfsObjectKind::Symlink));
        assert!(!is_visible_placeholder_kind(&DfsObjectKind::Hardlink));
        assert!(!is_visible_placeholder_kind(&DfsObjectKind::Artifact));
        assert!(!is_visible_placeholder_kind(&DfsObjectKind::Tombstone));
    }

    #[test]
    fn new_or_unowned_sync_root_must_be_empty_before_registration() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let data_root = std::env::temp_dir().join(format!(
            "sinomo-cloud-files-unowned-root-{}-{suffix}",
            std::process::id()
        ));
        let sync_root = data_root.join("sync-root");
        fs::create_dir_all(&sync_root).expect("create sync root");
        fs::write(sync_root.join("preexisting.txt"), b"must not upload")
            .expect("write preexisting file");

        let error = ensure_new_sync_root_is_empty(&sync_root)
            .expect_err("preexisting local data must never become queued creates");

        assert_eq!(error.code, "unowned-sync-root-not-empty");
        assert!(error.detail.contains("preexisting.txt"));
        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn exact_provider_replacement_adopts_nonempty_root_and_retained_writable_scopes() {
        let mut request = CloudFilesMountRequest::new(PathBuf::from(r"C:\SinomoSIFS"));
        request.provider_replacement =
            Some(crate::protocol::CloudFilesProviderReplacementContext {
                predecessor_content_hash: format!("blake3:{}", "1".repeat(64)),
                successor_content_hash: format!("blake3:{}", "2".repeat(64)),
            });
        assert!(validate_provider_replacement_context(&request).expect("replacement context"));

        verify_fresh_state_owned_boundary_with(
            true,
            true,
            &request.sync_root,
            42,
            || panic!("replacement adoption must not require an empty root"),
            || panic!("replacement adoption must retain its durable writable scopes"),
            || Ok(42),
        )
        .expect("replacement adoption still verifies the root file identity");

        request
            .provider_replacement
            .as_mut()
            .expect("replacement")
            .successor_content_hash = format!("blake3:{}", "1".repeat(64));
        let error = validate_provider_replacement_context(&request)
            .expect_err("same provider image is not a replacement");
        assert_eq!(error.code, "provider-replacement-context-invalid");
    }

    #[test]
    fn journal_free_preserve_replay_releases_a_missing_root_without_cfapi_query() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let sync_root = std::env::temp_dir().join(format!(
            "sinomo-cloud-files-missing-preserve-replay-{}-{suffix}",
            std::process::id()
        ));
        assert!(!sync_root.exists());
        let queried = std::cell::Cell::new(false);

        let observed = query_registered_sync_root_if_path_exists(&sync_root, |_| {
            queried.set(true);
            Err(CloudFilesModuleError {
                code: "unexpected-cfapi-query".to_string(),
                detail: "missing physical root must settle before CfGetSyncRootInfoByPath"
                    .to_string(),
            })
        })
        .expect("missing journal-free root is registration-absent");

        assert!(observed.is_none());
        assert!(!queried.get());
        assert!(!sync_root.exists());
    }

    #[test]
    fn only_fresh_admission_accepts_an_uninitialized_cloud_files_volume() {
        let not_under_sync_root = HRESULT::from_win32(ERROR_CLOUD_FILE_NOT_UNDER_SYNC_ROOT.0);
        let uninitialized_volume = HRESULT::from_win32(ERROR_INVALID_FUNCTION.0);

        assert!(sync_root_query_reports_absence(not_under_sync_root, false));
        assert!(sync_root_query_reports_absence(not_under_sync_root, true));
        assert!(!sync_root_query_reports_absence(
            uninitialized_volume,
            false
        ));
        assert!(sync_root_query_reports_absence(uninitialized_volume, true));
        assert!(!sync_root_query_reports_absence(
            HRESULT::from_win32(5),
            true
        ));
    }

    #[test]
    fn fresh_activation_opens_no_writable_effect_before_state_owned() {
        #[derive(Default)]
        struct Activation {
            registered: bool,
            state_owned: bool,
        }

        for failure in ["entry", "scope", "file-id"] {
            let effects = std::cell::Cell::new(0usize);
            let mut activation = Activation::default();
            let error = activate_registration_before_writable_effects_with(
                &mut activation,
                |_| Ok(()),
                |activation| {
                    activation.registered = true;
                    Ok(())
                },
                |_| {
                    verify_fresh_state_owned_boundary_with(
                        true,
                        false,
                        Path::new(r"C:\SinomoSIFS"),
                        42,
                        || {
                            if failure == "entry" {
                                Err(CloudFilesModuleError {
                                    code: "unowned-sync-root-not-empty".to_string(),
                                    detail: "injected entry".to_string(),
                                })
                            } else {
                                Ok(())
                            }
                        },
                        || Ok(failure != "scope"),
                        || Ok(if failure == "file-id" { 43 } else { 42 }),
                    )
                },
                |activation| {
                    activation.state_owned = true;
                    Ok(())
                },
                || {
                    effects.set(effects.get() + 1);
                    Ok(())
                },
            )
            .expect_err("every injected Fresh race must fail closed");

            assert!(activation.registered, "{failure}");
            assert!(!activation.state_owned, "{failure}");
            assert_eq!(effects.get(), 0, "{failure}");
            assert!(matches!(
                error.code.as_str(),
                "unowned-sync-root-not-empty"
                    | "unowned-sync-root-state-not-pristine"
                    | "sync-root-file-id-changed"
            ));
        }

        let effects = std::cell::Cell::new(0usize);
        let mut activation = Activation::default();
        activate_registration_before_writable_effects_with(
            &mut activation,
            |_| Ok(()),
            |activation| {
                activation.registered = true;
                Ok(())
            },
            |_| Ok(()),
            |activation| {
                activation.state_owned = true;
                Ok(())
            },
            || {
                effects.set(effects.get() + 1);
                Ok(())
            },
        )
        .expect("effects begin only after StateOwned");
        assert!(activation.state_owned);
        assert_eq!(effects.get(), 1);
    }

    #[test]
    fn owned_recovery_rejects_missing_reparse_or_replaced_root_identity() {
        let root = Path::new(r"C:\SinomoSIFS");
        for (expected_code, plain_result, observed_file_id) in [
            (
                "sync-root-inspect",
                Err(CloudFilesModuleError {
                    code: "sync-root-inspect".to_string(),
                    detail: "missing owned root".to_string(),
                }),
                42,
            ),
            (
                "unowned-sync-root-not-plain-directory",
                Err(CloudFilesModuleError {
                    code: "unowned-sync-root-not-plain-directory".to_string(),
                    detail: "reparse point".to_string(),
                }),
                42,
            ),
            ("sync-root-file-id-changed", Ok(()), 43),
        ] {
            let error = validate_owned_sync_root_physical_identity_with(
                root,
                42,
                || plain_result,
                || Ok(observed_file_id),
            )
            .expect_err("changed owned physical identity must block recovery");
            assert_eq!(error.code, expected_code);
        }
    }

    #[test]
    fn nominal_read_only_mount_fails_mode_validation_before_side_effects() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let sync_root = std::env::temp_dir().join(format!(
            "sinomo-cloud-files-read-only-unenforced-{}-{suffix}",
            std::process::id()
        ));
        assert!(!sync_root.exists());

        let request = CloudFilesMountRequest::new(sync_root.clone());
        let mut lifecycle = MountLifecycleState::new(&request);
        let error = mount_sifs_cloud_files_inner(request, &mut lifecycle)
            .expect_err("unenforceable read-only mount must fail closed");

        assert_eq!(
            error.code,
            crate::protocol::CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE
        );
        assert_eq!(
            lifecycle.local_change_sync.mode,
            crate::protocol::CloudFilesLocalChangeSyncMode::Unsupported
        );
        assert!(!lifecycle.local_change_sync.local_mutations_blocked);
        assert!(!sync_root.exists());
    }
}
