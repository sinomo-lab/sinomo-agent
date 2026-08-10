#[allow(unused_imports)]
use sinomo_runtime::cluster::ClusterKvPathsExt;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{OsStr, c_void};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::windows::ffi::OsStrExt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use fs2::FileExt;
use sinomo_dfs_cloud_files_api::{
    CloudFilesFetchDataError, CloudFilesLocalChangeSyncMode, CloudFilesLocalChangeSyncStatus,
    CloudFilesModuleError, CloudFilesModuleEvent, CloudFilesMountLifecycleEvent,
    CloudFilesMountRequest, CloudFilesMountResponse, CloudFilesPlaceholderIdentity,
    CloudFilesRegistrationReceipt, CloudFilesStaleIdentityReason,
    CloudFilesStalePlaceholderIdentity, CloudFilesUnregisterRequest, CloudFilesUnregisterResponse,
    SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION, SINOMO_DFS_CLOUD_FILES_MODULE_ID,
};
use sinomo_module_api::{decode_sobj, encode_sobj};
use sinomo_runtime::api::{DaemonApiClient, DaemonPaths};
use sinomo_runtime::daemon;
use sinomo_runtime::dfs::{
    self, DfsControlPlaneKvClient, DfsMessageResponse, DfsMessageTransport, DfsObjectKind,
    DfsObjectRpcSendOptions, DfsPeerTransport, DfsPeerTransportError, DfsPosixError, DfsRoutedCall,
    DfsRoutedMessage, DfsTransportIdentity, LocalContentStore, MountedSifsControlPlaneKvClient,
    NetworkedDfsService,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, NetworkError, ObjectRpcCall, ObjectRpcResult};
use sinomo_runtime::node::NodeId;
use windows::Win32::Foundation::{
    CloseHandle, ERROR_CLOUD_FILE_NOT_UNDER_SYNC_ROOT, HANDLE, NTSTATUS, STATUS_CANCELLED,
    STATUS_CLOUD_FILE_UNSUCCESSFUL, STATUS_FILE_IS_A_DIRECTORY, STATUS_NOT_A_DIRECTORY,
    STATUS_OBJECT_NAME_NOT_FOUND, STATUS_SUCCESS, STATUS_UNSUCCESSFUL,
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
    FILE_ATTRIBUTE_READONLY, FILE_BASIC_INFO, FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, GetFileInformationByHandle,
    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_EXISTING,
};
use windows::core::{GUID, HRESULT, PCWSTR};

use crate::namespace_reconciler::CloudFilesMaterializationAuditIntentStore;
use crate::registration_journal::{
    REGISTRATION_IDENTITY_MAX_BYTES, RegistrationJournal, RegistrationPhase,
    RegistrationRecoveryAction,
};
use crate::windows_sync::{
    CLOUD_FILES_DIRECTORY_PAGE_ENTRIES, CloudFilesObjectProjection, CloudFilesWritableSyncState,
    cloud_files_object_projection,
};
use crate::{
    DecodedCloudFilesPlaceholderIdentity, decode_placeholder_identity, encode_placeholder_identity,
    host_emit_event, host_is_cancelled, host_log, placeholder_identity_mismatch,
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
/// One Cloud Files DFS call must yield to cancellation promptly. Higher-level
/// DFS operations may request longer deadlines, but this provider clamps each
/// routed call and observes cancellation between bounded work items.
const CLOUD_FILES_DFS_CALL_TIMEOUT: Duration = Duration::from_secs(2);
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
    Ready(Box<RegistrationJournal>),
}

struct RegistrationActivation {
    data_dir: SinomoDataDir,
    journal: RegistrationJournal,
    armed: bool,
}

struct RegistrationLease {
    _file: fs::File,
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
}

impl RegistrationActivation {
    fn new(data_dir: SinomoDataDir, journal: RegistrationJournal) -> Self {
        Self {
            data_dir,
            journal,
            armed: true,
        }
    }

    fn journal(&self) -> &RegistrationJournal {
        &self.journal
    }

    fn mark_registered(
        &mut self,
        request: &CloudFilesMountRequest,
    ) -> Result<(), CloudFilesModuleError> {
        self.journal.phase = RegistrationPhase::Registered;
        self.journal.provider_version = request.provider_version.clone();
        persist_registration_journal(&self.data_dir, &self.journal)
    }

    fn mark_ready(
        &mut self,
        request: &CloudFilesMountRequest,
    ) -> Result<(), CloudFilesModuleError> {
        // The stable receipt means that the whole provider is ready, not merely
        // that CfRegisterSyncRoot returned. Keep the journal at Registered until
        // the receipt is durable so a crash in between is recovered as rollback.
        persist_registration_receipt(request, &self.data_dir)?;
        self.journal.phase = RegistrationPhase::Ready;
        self.journal.provider_version = request.provider_version.clone();
        persist_registration_journal(&self.data_dir, &self.journal)
    }

    fn rollback(&mut self) -> Result<(), CloudFilesModuleError> {
        rollback_owned_registration(&self.data_dir, &self.journal)?;
        self.armed = false;
        Ok(())
    }

    fn fail(
        &mut self,
        mut error: CloudFilesModuleError,
        lifecycle: &mut MountLifecycleState,
    ) -> CloudFilesModuleError {
        match self.rollback() {
            Ok(()) => {
                lifecycle.registered = false;
                error.detail.push_str(
                    "; nonce-owned Cloud Files registration rolled back and verified absent",
                );
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
    let _registration_lease = RegistrationLease::acquire(&data_dir)?;
    let was_registered = recover_registration_for_unregister(&request, &data_dir)?;
    lifecycle.registered = false;
    let detail = if was_registered {
        "Cloud Files sync root unregistered after nonce-bound ownership verification".to_string()
    } else {
        "Cloud Files sync root was already unregistered".to_string()
    };
    lifecycle.emit_unregistered(detail.clone());
    Ok(CloudFilesUnregisterResponse {
        sync_root: request.sync_root,
        root_path: request.root_path,
        was_registered,
        detail,
    })
}

fn mount_sifs_cloud_files_inner(
    mut request: CloudFilesMountRequest,
    lifecycle: &mut MountLifecycleState,
) -> Result<CloudFilesMountResponse, CloudFilesModuleError> {
    request.validate_supported_mode()?;
    request.root_path = normalize_sifs_root(&request.root_path)?;
    lifecycle.root_path.clone_from(&request.root_path);
    let data_dir = resolve_data_dir(request.data_dir_root.clone())?;
    let _registration_lease = RegistrationLease::acquire(&data_dir)?;
    if !request.connect {
        return Err(CloudFilesModuleError {
            code: "writable-sync-requires-connection".to_string(),
            detail: "writable Cloud Files requires connected hydration and synchronization workers"
                .to_string(),
        });
    }
    let writable_state = CloudFilesWritableSyncState::open(
        &data_dir,
        request.sync_root.to_string_lossy(),
        request.root_path.clone(),
    )
    .map_err(|error| CloudFilesModuleError {
        code: error.code.to_string(),
        detail: error.detail,
    })?;
    lifecycle.local_change_sync = writable_state.status();
    let materialization_audit_intents = writable_state.materialization_audit_intents();
    fs::create_dir_all(&request.sync_root).map_err(|source| CloudFilesModuleError {
        code: "sync-root-create".to_string(),
        detail: format!(
            "create Cloud Files sync root {}: {source}",
            request.sync_root.display()
        ),
    })?;

    let recovered_registration = recover_registration_for_mount(&request, &data_dir)?;
    let existing_owned_root = matches!(&recovered_registration, RecoveredRegistration::Ready(_));

    let opened = open_sifs_service(request.data_dir_root.clone())?;
    let shared_service = Arc::new(Mutex::new(opened.service));
    let materialized_directories = Arc::new(Mutex::new(BTreeSet::new()));
    let provider = Arc::new(CloudFilesProvider::new(
        Arc::clone(&shared_service),
        request.sync_root.clone(),
        request.root_path.clone(),
        request.read_only,
        Arc::clone(&materialized_directories),
        materialization_audit_intents,
    ));
    if !existing_owned_root {
        ensure_new_sync_root_is_empty(&request.sync_root)?;
        ensure_new_sync_root_state_is_pristine(&writable_state)?;
    }
    let journal = prepare_registration_activation(&request, &data_dir, recovered_registration)?;
    let mut activation = RegistrationActivation::new(data_dir.clone(), journal);
    if let Err(error) = register_sync_root(&request, activation.journal(), existing_owned_root) {
        return Err(activation.fail(error, lifecycle));
    }
    if let Err(error) = activation.mark_registered(&request) {
        return Err(activation.fail(error, lifecycle));
    }
    lifecycle.registered = true;
    lifecycle.emit_registered();

    let write_options = dfs::DfsWriteOptions::replicated_eventual(opened.replica_candidates);
    let (mut writable_worker, writable_worker_cancelled) = windows_adapter::build_writable_worker(
        writable_state,
        Arc::clone(&shared_service),
        write_options,
        windows_adapter::WindowsWritableWorkerOptions {
            sync_root: request.sync_root.clone(),
            root_path: request.root_path.clone(),
            existing_owned_root,
            prepopulate_depth: request.prepopulate_depth,
            materialized_directories,
        },
    );
    if let Err(error) = writable_worker.startup(&writable_worker_cancelled) {
        lifecycle.local_change_sync = writable_worker.state().status();
        let error = CloudFilesModuleError {
            code: error.code.to_string(),
            detail: error.detail,
        };
        return Err(activation.fail(error, lifecycle));
    }
    lifecycle.local_change_sync = writable_worker.state().status();
    lifecycle.placeholders_created = writable_worker.state().reconciler().manifest().len() as u64;
    let placeholders_created = lifecycle.placeholders_created;

    let callback_table = callback_table();
    let sync_root_wide = wide_path(&request.sync_root);
    let connection_key = match unsafe {
        CfConnectSyncRoot(
            PCWSTR(sync_root_wide.as_ptr()),
            callback_table.as_ptr(),
            Some(Arc::as_ptr(&provider).cast::<c_void>()),
            CF_CONNECT_FLAG_NONE,
        )
    }
    .map_err(|source| CloudFilesModuleError {
        code: "connect-sync-root".to_string(),
        detail: format!(
            "connect Cloud Files sync root {}: {source}",
            request.sync_root.display()
        ),
    }) {
        Ok(connection_key) => connection_key,
        Err(error) => return Err(activation.fail(error, lifecycle)),
    };
    let mut connection = CloudFilesConnection::new(connection_key, request.sync_root.clone());
    lifecycle.connected = true;
    lifecycle.emit_connected();
    let emitter = windows_adapter::WorkerLifecycleEmitter::from_lifecycle(lifecycle);
    let initial_worker_ready = writable_worker_ready(&lifecycle.local_change_sync);
    emitter.emit(
        lifecycle.local_change_sync.clone(),
        true,
        initial_worker_ready,
        1,
    );
    let mut writable_worker = match windows_adapter::WindowsWritableWorkerHandle::spawn(
        writable_worker,
        writable_worker_cancelled,
        emitter,
        1,
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
    let first_progress_generation = match writable_worker
        .wait_for_first_progress(Duration::from_secs(5))
    {
        Ok(generation) => generation,
        Err(error) => {
            let error = with_cleanup_result(error, "worker stop", writable_worker.stop_and_join());
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
    activation.disarm();
    host_log(&format!(
        "Cloud Files registration became ready after worker progress generation {first_progress_generation}"
    ));

    host_log(&format!(
        "Cloud Files provider connected {} -> {}",
        request.sync_root.display(),
        request.root_path
    ));
    let started = Instant::now();
    loop {
        if host_is_cancelled() {
            break;
        }
        if !writable_worker.is_alive() {
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
        if let Some(max_run_seconds) = request.max_run_seconds
            && started.elapsed() >= Duration::from_secs(max_run_seconds)
        {
            break;
        }
        thread::sleep(Duration::from_millis(250));
    }
    writable_worker.stop_and_join()?;
    connection.disconnect()?;
    lifecycle.connected = false;
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
    let page_state = match after_durable_materialization_intent(
        || {
            windows_adapter::record_materialization_audit_intent(
                &provider.materialization_audit_intents,
                &path,
            )
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
    let placeholder_total_count = if next_cursor.is_some() {
        i64::from(prospective_entries_processed).saturating_add(1)
    } else {
        i64::from(prospective_entries_processed)
    };
    let page_outcome = unsafe {
        transfer_placeholders(
            info,
            placeholders,
            placeholder_total_count,
            CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_NONE,
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
    let committed = if next_cursor.is_none() {
        let terminal_outcome = unsafe {
            transfer_placeholders(
                info,
                Vec::new(),
                i64::from(next_entries_processed),
                CF_OPERATION_TRANSFER_PLACEHOLDERS_FLAG_DISABLE_ON_DEMAND_POPULATION,
                STATUS_SUCCESS,
            )
        };
        let terminal_succeeded = terminal_outcome.fully_succeeded();
        if !terminal_succeeded {
            host_log(&format!(
                "Cloud Files terminal placeholder population was not committed for `{path}`: {}",
                terminal_outcome.detail()
            ));
        }
        terminal_succeeded
    } else {
        true
    };
    provider.finish_placeholder_page(&path, next_cursor, next_entries_processed, committed);
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
    mount_identity: String,
    sync_root: PathBuf,
    root_path: String,
    read_only: bool,
    cancelled_requests: Mutex<BTreeSet<i64>>,
    placeholder_pages: Mutex<BTreeMap<String, CloudFilesPlaceholderPageState>>,
    placeholder_page_leases: Mutex<BTreeSet<String>>,
    materialized_directories: Arc<Mutex<BTreeSet<String>>>,
    materialization_audit_intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
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
        sync_root: PathBuf,
        root_path: String,
        read_only: bool,
        materialized_directories: Arc<Mutex<BTreeSet<String>>>,
        materialization_audit_intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
    ) -> Self {
        let mount_identity = sync_root.to_string_lossy().into_owned();
        Self {
            dfs,
            mount_identity,
            sync_root,
            root_path,
            read_only,
            cancelled_requests: Mutex::new(BTreeSet::new()),
            placeholder_pages: Mutex::new(BTreeMap::new()),
            placeholder_page_leases: Mutex::new(BTreeSet::new()),
            materialized_directories,
            materialization_audit_intents,
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
        let page = {
            let mut dfs = self.lock_dfs()?;
            dfs.posix_list_dir_distributed_page(
                sifs_path,
                after,
                CLOUD_FILES_DIRECTORY_PAGE_ENTRIES,
            )
            .map_err(ntstatus_for_posix_error)?
        };
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

fn load_registration_journal(
    data_dir: &SinomoDataDir,
) -> Result<Option<RegistrationJournal>, CloudFilesModuleError> {
    let path = registration_journal_path(data_dir);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(CloudFilesModuleError {
                code: "registration-journal-read".to_string(),
                detail: format!(
                    "read Cloud Files registration journal {}: {source}",
                    path.display()
                ),
            });
        }
    };
    let journal =
        decode_sobj::<RegistrationJournal>(&bytes).map_err(|source| CloudFilesModuleError {
            code: "registration-journal-decode".to_string(),
            detail: format!(
                "decode Cloud Files registration journal {}: {source}",
                path.display()
            ),
        })?;
    journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    Ok(Some(journal))
}

fn persist_registration_journal(
    data_dir: &SinomoDataDir,
    journal: &RegistrationJournal,
) -> Result<(), CloudFilesModuleError> {
    journal.validate(CLOUD_FILES_PROVIDER_ID_TEXT)?;
    let path = registration_journal_path(data_dir);
    persist_registration_artifact(
        &path,
        journal,
        "registration-journal",
        "Cloud Files registration journal",
    )
}

fn load_registration_receipt(
    path: &Path,
) -> Result<Option<CloudFilesRegistrationReceipt>, CloudFilesModuleError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(CloudFilesModuleError {
                code: "registration-receipt-read".to_string(),
                detail: format!(
                    "read Cloud Files registration receipt {}: {source}",
                    path.display()
                ),
            });
        }
    };
    decode_sobj(&bytes)
        .map(Some)
        .map_err(|source| CloudFilesModuleError {
            code: "registration-receipt-decode".to_string(),
            detail: format!(
                "decode Cloud Files registration receipt {}: {source}",
                path.display()
            ),
        })
}

fn persist_registration_receipt(
    request: &CloudFilesMountRequest,
    data_dir: &SinomoDataDir,
) -> Result<(), CloudFilesModuleError> {
    let path = registration_receipt_path(data_dir);
    let receipt = CloudFilesRegistrationReceipt {
        interface_version: SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION.to_string(),
        sync_root: request.sync_root.clone(),
        root_path: request.root_path.clone(),
        provider_name: request.provider_name.clone(),
    };
    persist_registration_artifact(
        &path,
        &receipt,
        "registration-receipt",
        "Cloud Files registration receipt",
    )
}

fn persist_registration_artifact<T: serde::Serialize>(
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
        if source.code() == HRESULT::from_win32(ERROR_CLOUD_FILE_NOT_UNDER_SYNC_ROOT.0) {
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
        "sinomo.dfs.cloud-files.v1" | SINOMO_DFS_CLOUD_FILES_INTERFACE_VERSION
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
        let observed = query_registered_sync_root(&journal.sync_root)?;
        if let Some(observed) = observed.as_ref() {
            verify_journal_registration_ownership(&journal, observed)?;
        }
        let receipt_result = load_registration_receipt(&registration_receipt_path(data_dir));
        if let Ok(Some(receipt)) = receipt_result.as_ref()
            && !journal.matches_receipt(receipt)
        {
            return Err(CloudFilesModuleError {
                code: "registration-artifact-mismatch".to_string(),
                detail: format!(
                    "refusing recovery for {}: durable receipt and nonce journal identify different registrations",
                    journal.sync_root.display()
                ),
            });
        }
        let receipt_verified = receipt_result
            .as_ref()
            .ok()
            .and_then(Option::as_ref)
            .is_some_and(|receipt| journal.matches_receipt(receipt));
        match journal.recovery_action(observed.is_some(), receipt_verified) {
            RegistrationRecoveryAction::ReuseReadyRegistration => {
                if !journal.matches_mount_request(request) {
                    return Err(CloudFilesModuleError {
                        code: "registration-journal-mismatch".to_string(),
                        detail: format!(
                            "ready Cloud Files registration for {} does not match requested mount {}; explicitly unregister the retained root first",
                            journal.sync_root.display(),
                            request.sync_root.display()
                        ),
                    });
                }
                return Ok(RecoveredRegistration::Ready(Box::new(journal)));
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
    if let Some(observed) = query_registered_sync_root(&request.sync_root)? {
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
        remove_registration_receipt(&receipt_path)?;
        return Ok(());
    };
    let legacy_request = CloudFilesUnregisterRequest {
        sync_root: receipt.sync_root.clone(),
        root_path: receipt.root_path.clone(),
        data_dir_root: request.data_dir_root.clone(),
        provider_name: receipt.provider_name.clone(),
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
) -> Result<bool, CloudFilesModuleError> {
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
        let was_registered = unregister_journal_owned_registration(&journal)?;
        verify_registration_absent(&journal.sync_root)?;
        clear_registration_artifacts(data_dir)?;
        return Ok(was_registered);
    }

    let receipt_path = registration_receipt_path(data_dir);
    let receipt = load_registration_receipt(&receipt_path)?;
    if let Some(receipt) = receipt.as_ref() {
        verify_registration_receipt(request, Some(receipt))?;
    }
    let Some(observed) = query_registered_sync_root(&request.sync_root)? else {
        remove_registration_receipt(&receipt_path)?;
        return Ok(false);
    };
    verify_registration_ownership(request, receipt.as_ref(), &observed)?;
    unregister_sync_root_after_verification(&request.sync_root)?;
    verify_registration_absent(&request.sync_root)?;
    remove_registration_receipt(&receipt_path)?;
    Ok(true)
}

fn prepare_registration_activation(
    request: &CloudFilesMountRequest,
    data_dir: &SinomoDataDir,
    recovered: RecoveredRegistration,
) -> Result<RegistrationJournal, CloudFilesModuleError> {
    let mut journal = match recovered {
        RecoveredRegistration::Fresh => {
            let nonce = GUID::new().map_err(|source| CloudFilesModuleError {
                code: "registration-nonce-generate".to_string(),
                detail: format!("generate unguessable Cloud Files registration nonce: {source}"),
            })?;
            RegistrationJournal::prepared(
                request,
                CLOUD_FILES_PROVIDER_ID_TEXT,
                format!("{:032x}", nonce.to_u128()),
                sync_root_file_index(&request.sync_root)?,
            )?
        }
        RecoveredRegistration::Ready(journal) => *journal,
    };
    journal.phase = RegistrationPhase::Prepared;
    journal.provider_version = request.provider_version.clone();
    persist_registration_journal(data_dir, &journal)?;
    Ok(journal)
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

fn ensure_new_sync_root_state_is_pristine(
    state: &CloudFilesWritableSyncState,
) -> Result<(), CloudFilesModuleError> {
    let has_queue_history = state.queue().operations().next().is_some();
    let reconciler = state.reconciler();
    if has_queue_history
        || !reconciler.manifest().is_empty()
        || !reconciler.cursors().is_empty()
        || reconciler.conflict_count() != 0
    {
        return Err(CloudFilesModuleError {
            code: "unowned-sync-root-state-not-pristine".to_string(),
            detail: "new or unowned Cloud Files activation found durable queue/manifest/cursor/conflict history; refusing to reinterpret retained state without a verified registration receipt"
                .to_string(),
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
    // This cdylib links its own copy of sinomo-network, so the host's cluster
    // directory install never reaches this image; without an in-module install
    // every directory read (endpoint selection, route hydration) comes back
    // empty.
    sinomo_runtime::cluster::install_kv_cluster_directory();
    let data_dir = resolve_data_dir(data_dir_root)?;
    data_dir.ensure_root_layout().map_err(error("local-data"))?;
    let replica_candidates =
        dfs::discover_mounted_sifs_content_replica_candidates(&data_dir).map_err(error("dfs"))?;
    let cluster_paths = network::ClusterPaths::new(&data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        sinomo_runtime::cluster::read_cluster_state_without_kv(&cluster_paths).ok()
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
    let control_plane = cluster_state.as_ref().map(|_| {
        let peer_transport: Arc<dyn DfsPeerTransport> = Arc::new(DfsCloudFilesKvPeerTransport {
            data_dir: data_dir.clone(),
            local_node_id: local_node_id.clone(),
        });
        MountedSifsControlPlaneKvClient::new_authoritative(cluster_paths.clone(), peer_transport)
    });
    let mut service = NetworkedDfsService::new(
        DfsCloudFilesEndpointTransport::new(
            local_node_id,
            cluster_paths.clone(),
            known_remote_nodes,
        ),
        content,
    );
    if let Some(control_plane) = control_plane.as_ref() {
        service = service.with_control_plane(control_plane.clone());
    }
    let metadata = open_metadata_backend(&data_dir, &cluster_paths, control_plane.as_ref())?;
    let mut service = service
        .with_boxed_metadata_backend(metadata)
        .map_err(error("dfs"))?;
    service.bootstrap_distributed_sifs().map_err(error("dfs"))?;
    Ok(OpenedSifsService {
        service,
        replica_candidates,
    })
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

#[derive(Clone, Debug)]
struct DfsCloudFilesKvPeerTransport {
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
}

impl DfsPeerTransport for DfsCloudFilesKvPeerTransport {
    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> std::result::Result<ObjectRpcResult, DfsPeerTransportError> {
        if target_node_id != &self.local_node_id {
            return Err(DfsPeerTransportError::new(format!(
                "Cloud Files KV control-plane call targeted non-local node {target_node_id}"
            )));
        }
        if !call.body.is_empty() {
            return Err(DfsPeerTransportError::new(
                "Cloud Files KV control-plane call unexpectedly carried a streamed body",
            ));
        }
        let timeout = options
            .overall_timeout
            .unwrap_or(network::DEFAULT_NETWORK_TIMEOUT)
            .min(CLOUD_FILES_DFS_CALL_TIMEOUT);
        let client = DaemonApiClient::connect_for_paths_with_timeout(
            &DaemonPaths::new(&self.data_dir),
            options.connect_timeout,
        )
        .map_err(|source| DfsPeerTransportError::new(source.to_string()))?
        .with_timeout(timeout);
        client
            .call_request(&call.request)
            .map(ObjectRpcResult::value)
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
    }
}

#[derive(Clone)]
struct DfsCloudFilesEndpointTransport {
    local_node_id: NodeId,
    cluster_paths: network::ClusterPaths,
    known_remote_nodes: BTreeSet<NodeId>,
    incoming_messages: VecDeque<DfsRoutedMessage>,
    operation_deadline: Option<Instant>,
}

impl std::fmt::Debug for DfsCloudFilesEndpointTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsCloudFilesEndpointTransport")
            .field("local_node_id", &self.local_node_id)
            .field("cluster_paths", &self.cluster_paths)
            .field("known_remote_nodes", &self.known_remote_nodes)
            .field("incoming_messages", &self.incoming_messages)
            .field("operation_deadline", &self.operation_deadline)
            .finish()
    }
}

impl DfsCloudFilesEndpointTransport {
    fn new(
        local_node_id: NodeId,
        cluster_paths: network::ClusterPaths,
        known_remote_nodes: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self {
            known_remote_nodes: known_remote_nodes
                .into_iter()
                .filter(|node_id| node_id != &local_node_id)
                .collect(),
            local_node_id,
            cluster_paths,
            incoming_messages: VecDeque::new(),
            operation_deadline: None,
        }
    }

    fn send_timeouts(&self) -> std::result::Result<network::DaemonRpcTimeouts, NetworkError> {
        let mut connect_timeout =
            network::DEFAULT_NETWORK_TIMEOUT.min(CLOUD_FILES_DFS_CALL_TIMEOUT);
        let mut idle_timeout = network::DEFAULT_NETWORK_TIMEOUT.min(CLOUD_FILES_DFS_CALL_TIMEOUT);
        if let Some(deadline) = self.operation_deadline {
            let Some(remaining) = deadline
                .checked_duration_since(Instant::now())
                .filter(|duration| !duration.is_zero())
            else {
                return Err(NetworkError::Io {
                    operation: "send Cloud Files DFS message".to_string(),
                    detail: "DFS operation deadline expired before endpoint send".to_string(),
                });
            };
            connect_timeout = connect_timeout.min(remaining);
            idle_timeout = idle_timeout.min(remaining);
        }
        Ok(network::DaemonRpcTimeouts {
            connect_timeout: Some(connect_timeout),
            idle_timeout: Some(idle_timeout),
        })
    }

    fn deliver_returned_message(
        &mut self,
        message: DfsRoutedMessage,
    ) -> std::result::Result<(), NetworkError> {
        let target_node_id = message.target_node_id.clone();
        if target_node_id == self.local_node_id {
            self.incoming_messages.push_back(message);
            return Ok(());
        }
        self.send_routed_call_to_target(&target_node_id, DfsRoutedCall::value(message), false)
    }

    fn send_routed_call_to_target(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
        require_local_source: bool,
    ) -> std::result::Result<(), NetworkError> {
        if target_node_id == &self.local_node_id {
            self.incoming_messages
                .push_back(dfs::materialize_dfs_routed_call_sync(call)?);
            return Ok(());
        }
        let message = &call.message;
        if require_local_source && message.source_node_id != self.local_node_id {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: message.source_node_id.clone(),
            });
        }
        if !require_local_source
            && message.source_node_id != self.local_node_id
            && !self.known_remote_nodes.contains(&message.source_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: message.source_node_id.clone(),
            });
        }
        if &message.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: message.target_node_id.clone(),
            });
        }
        if !self.known_remote_nodes.contains(target_node_id) {
            return Err(NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            });
        }
        let (_, call) = dfs::dfs_routed_call_rpc_call(call)?;
        let response = send_dfs_rpc_call(
            &self.cluster_paths,
            target_node_id,
            call,
            self.send_timeouts()?,
        )?;
        for returned_message in response.outbound_messages {
            self.deliver_returned_message(returned_message)?;
        }
        Ok(())
    }
}

impl DfsTransportIdentity for DfsCloudFilesEndpointTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }
}

impl DfsMessageTransport for DfsCloudFilesEndpointTransport {
    fn send_routed_call(
        &mut self,
        target_node_id: &NodeId,
        call: DfsRoutedCall,
    ) -> std::result::Result<(), NetworkError> {
        self.send_routed_call_to_target(target_node_id, call, true)
    }

    fn drain_incoming_messages(
        &mut self,
    ) -> std::result::Result<Vec<DfsRoutedMessage>, NetworkError> {
        Ok(self.incoming_messages.drain(..).collect())
    }

    fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
        let deadline =
            deadline.map(|requested| requested.min(Instant::now() + CLOUD_FILES_DFS_CALL_TIMEOUT));
        std::mem::replace(&mut self.operation_deadline, deadline)
    }

    fn operation_deadline(&self) -> Option<Instant> {
        self.operation_deadline
    }
}

fn send_dfs_rpc_call(
    cluster_paths: &network::ClusterPaths,
    target_node_id: &NodeId,
    call: network::ObjectRpcCall,
    timeouts: network::DaemonRpcTimeouts,
) -> std::result::Result<DfsMessageResponse, NetworkError> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            let runtime = handle.clone();
            let runtime_for_future = runtime.clone();
            tokio::task::block_in_place(|| {
                runtime.block_on(send_dfs_rpc_call_async(
                    cluster_paths,
                    target_node_id,
                    call,
                    timeouts,
                    &runtime_for_future,
                ))
            })
        } else {
            Err(NetworkError::Io {
                operation: "send Cloud Files DFS RPC".to_string(),
                detail: "Cloud Files DFS sync transport cannot run inside a current-thread Tokio runtime"
                    .to_string(),
            })
        };
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| NetworkError::Io {
            operation: "create Cloud Files DFS client runtime".to_string(),
            detail: source.to_string(),
        })?;
    let output = runtime.block_on(send_dfs_rpc_call_async(
        cluster_paths,
        target_node_id,
        call,
        timeouts,
        runtime.handle(),
    ));
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

async fn send_dfs_rpc_call_async(
    cluster_paths: &network::ClusterPaths,
    target_node_id: &NodeId,
    call: network::ObjectRpcCall,
    timeouts: network::DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> std::result::Result<DfsMessageResponse, NetworkError> {
    let result = daemon::send_object_rpc_call_to_target_async_with_cluster_paths_and_options(
        cluster_paths,
        target_node_id,
        call,
        network::ClusterObjectRpcOptions::new(timeouts),
        runtime,
        true,
    )
    .await
    .map_err(|source| NetworkError::Io {
        operation: format!("send Cloud Files DFS call to {target_node_id}"),
        detail: source.to_string(),
    })?;
    dfs::dfs_message_response_from_rpc_result(result)
        .await
        .map_err(|source| NetworkError::Io {
            operation: format!("decode Cloud Files DFS response from {target_node_id}"),
            detail: source.to_string(),
        })
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
    fn cloud_files_transport_uses_shared_default_health_timeout() {
        let local = NodeId::from_legacy_named("cloud_files_timeout_local").expect("local");
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join("sinomo-cloud-files-timeout"));
        let mut transport = DfsCloudFilesEndpointTransport::new(
            local,
            network::ClusterPaths::new(&data_dir),
            Vec::new(),
        );

        let ordinary = transport.send_timeouts().expect("ordinary timeouts");
        assert_eq!(
            ordinary,
            network::DaemonRpcTimeouts::connect_and_idle(network::DEFAULT_NETWORK_TIMEOUT)
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
        assert_eq!(transport.set_operation_deadline(None), Some(deadline));
        assert_eq!(transport.operation_deadline(), None);
    }

    #[test]
    fn cloud_files_transport_queues_local_value_calls_without_network_routing() {
        let local = NodeId::from_legacy_named("cloud_files_loopback_value").expect("local");
        let data_dir = SinomoDataDir::new(std::env::temp_dir().join("sinomo-cloud-files-loopback"));
        let mut transport = DfsCloudFilesEndpointTransport::new(
            local.clone(),
            network::ClusterPaths::new(&data_dir),
            Vec::new(),
        );
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
        let data_dir =
            SinomoDataDir::new(std::env::temp_dir().join("sinomo-cloud-files-loopback-attachment"));
        let mut transport = DfsCloudFilesEndpointTransport::new(
            local.clone(),
            network::ClusterPaths::new(&data_dir),
            Vec::new(),
        );
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
            sinomo_dfs_cloud_files_api::CLOUD_FILES_WRITABLE_SYNC_UNSUPPORTED_CODE
        );
        assert_eq!(
            lifecycle.local_change_sync.mode,
            sinomo_dfs_cloud_files_api::CloudFilesLocalChangeSyncMode::Unsupported
        );
        assert!(!lifecycle.local_change_sync.local_mutations_blocked);
        assert!(!sync_root.exists());
    }
}
