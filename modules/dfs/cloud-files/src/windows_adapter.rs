//! Concrete Windows capture, DFS feed, and Cloud Files effect adapters.
//!
//! This module is a child of `windows_impl`, so the mount and hydration
//! provider share the exact same routed DFS service and placeholder encoder.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::{OsString, c_void};
use std::fs;
use std::io::Read;
use std::mem::{offset_of, size_of};
use std::os::windows::ffi::OsStringExt;
use std::os::windows::fs::MetadataExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::protocol::{
    CloudFilesLocalChangeSyncMode, CloudFilesLocalChangeSyncStatus, CloudFilesModuleError,
    CloudFilesPlaceholderIdentity,
};
use sinomo_runtime::dfs::{self, DfsMessageTransport, DfsObjectKind, NetworkedDfsService};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{DaemonRpcTimeouts, ObjectRpcRequest};
use windows::Win32::Foundation::{
    ERROR_FILE_NOT_FOUND, ERROR_LOCK_VIOLATION, ERROR_NO_MORE_FILES, ERROR_PATH_NOT_FOUND,
    ERROR_SHARING_VIOLATION, GENERIC_READ, GENERIC_WRITE, HANDLE, WAIT_FAILED, WAIT_OBJECT_0,
    WAIT_TIMEOUT,
};
use windows::Win32::Storage::CloudFilters::{
    CF_CONVERT_FLAG_MARK_IN_SYNC, CF_CONVERT_FLAG_NONE, CF_IN_SYNC_STATE_IN_SYNC,
    CF_OPEN_FILE_FLAG_NONE, CF_PLACEHOLDER_INFO_STANDARD, CF_PLACEHOLDER_STANDARD_INFO,
    CF_UPDATE_FLAG_DEHYDRATE, CF_UPDATE_FLAG_MARK_IN_SYNC, CF_UPDATE_FLAG_NONE, CfCloseHandle,
    CfConvertToPlaceholder, CfGetPlaceholderInfo, CfOpenFileWithOplock, CfUpdatePlaceholder,
};
use windows::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateFileW, DELETE, FILE_ACTION_ADDED,
    FILE_ACTION_MODIFIED, FILE_ACTION_REMOVED, FILE_ACTION_RENAMED_NEW_NAME,
    FILE_ACTION_RENAMED_OLD_NAME, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_HIDDEN,
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_BEGIN, FILE_DISPOSITION_INFO, FILE_FLAG_BACKUP_SEMANTICS,
    FILE_FLAG_DELETE_ON_CLOSE, FILE_FLAG_OPEN_REPARSE_POINT, FILE_FLAG_OVERLAPPED,
    FILE_FLAG_WRITE_THROUGH, FILE_LIST_DIRECTORY, FILE_NOTIFY_CHANGE_CREATION,
    FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME, FILE_NOTIFY_CHANGE_LAST_WRITE,
    FILE_NOTIFY_CHANGE_SIZE, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_MODE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_DATA, FIND_FIRST_EX_ON_DISK_ENTRIES_ONLY,
    FileDispositionInfo, FindClose, FindExInfoStandard, FindExSearchNameMatch, FindFirstFileExW,
    FindNextFileW, FlushFileBuffers, GetFileInformationByHandle, OPEN_EXISTING,
    ReadDirectoryChangesW, ReadFile, SetFileInformationByHandle, SetFilePointerEx,
    WIN32_FIND_DATAW, WriteFile,
};
use windows::Win32::System::IO::{
    CancelIoEx, CancelSynchronousIo, GetOverlappedResult, OVERLAPPED,
};
use windows::Win32::System::Threading::{
    CreateEventW, GetCurrentThread, INFINITE, SetThreadPriority, THREAD_MODE_BACKGROUND_BEGIN,
    WaitForMultipleObjects,
};
use windows::core::{HRESULT, PCWSTR};

use super::{
    CloudFilesModuleEvent, CloudFilesMountLifecycleEvent, MountLifecycleState, OwnedWin32Handle,
    PlaceholderCreateDisposition, PlaceholderPayload, create_placeholders,
    decode_placeholder_identity, encode_placeholder_identity, host_emit_event, normalize_sifs_root,
    wide_path,
};
use crate::DecodedCloudFilesPlaceholderIdentity;
use crate::local_change_queue::{
    CloudFilesContentStager, CloudFilesDestinationPrecondition, CloudFilesExpectedObjectState,
    CloudFilesLocalOperationKind, CloudFilesLocalOperationPreconditions,
    CloudFilesLocalOperationRequest, CloudFilesRemoteMutationEvidence, CloudFilesStagedContent,
};
use crate::namespace_reconciler::{
    CloudFilesFullScanRequest, CloudFilesLocalPathState, CloudFilesMaterializationAuditIntentStore,
    CloudFilesNamespaceAction, CloudFilesNamespaceConflict, CloudFilesNamespaceCursor,
    CloudFilesNamespaceEffectAdapter, CloudFilesNamespaceFeedBatch, CloudFilesNamespaceSuppression,
    CloudFilesReconciledManifestEntry,
};
use crate::sync_executor::{
    CloudFilesDfsMutationCommand, CloudFilesLocalCompletionEffect, CloudFilesLocalCompletionOutcome,
};
use crate::windows_shell::{
    CloudFilesExplorerStatusProjector, SharedExplorerStatusProjector, enable_explorer_status,
    observe_explorer_state, observe_explorer_upload,
};
use crate::windows_sync::{
    CLOUD_FILES_DIRECTORY_PAGE_ENTRIES, CloudFilesCapturedChangeBatch, CloudFilesCapturedContent,
    CloudFilesCapturedLocalChange, CloudFilesConcreteDfsAdapter, CloudFilesCursorRecoveryControl,
    CloudFilesDfsServiceMutationBackend, CloudFilesDirectoryChange,
    CloudFilesDirectoryChangeAction, CloudFilesDirectoryChangePlan,
    CloudFilesFullScanEffectOutcome, CloudFilesFullScanSnapshot, CloudFilesLocalCaptureRetries,
    CloudFilesLocalChangeAccelerator, CloudFilesMountedMutationHandoff, CloudFilesNamespaceSource,
    CloudFilesObjectProjection, CloudFilesOfflineCreatePlan, CloudFilesRenamePairingState,
    CloudFilesSparsePlaceholderObservation, CloudFilesStartupConflictEvidence,
    CloudFilesStartupScanBatch, CloudFilesWindowsPlaceholderEffect, CloudFilesWritableSyncState,
    CloudFilesWritableSyncWorker, cloud_files_object_projection,
    cloud_files_root_kind_is_supported, cloud_namespace_batch_from_dfs,
    cloud_namespace_cursor_from_dfs, completion_follow_up_request, dfs_namespace_cursor_from_cloud,
    local_capture_retry_delay, next_sparse_known_path, plan_directory_changes,
    plan_owned_offline_create, run_retrying_namespace_read_pass,
    sparse_child_directory_is_enumerated, sparse_root_is_enumerated,
    staged_content_matches_manifest_entry, suppression_for_completed_mutation,
};

const DIRECTORY_CHANGE_BUFFER_BYTES: usize = 64 * 1024;
const DIRECTORY_CHANGE_CHANNEL_CAPACITY: usize = 8;
const DIRECTORY_CHANGES_PER_PASS: usize = 64;
const DEFERRED_CAPTURE_RETRIES_MAX: usize = 4_096;
const LOCAL_STARTUP_SCAN_WORK_PER_PASS: usize = 32;
const NAMESPACE_FULL_SCAN_OBSERVATIONS_PER_PASS: usize = 32;
const SERVICE_LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const SERVICE_LOCK_RETRY: Duration = Duration::from_millis(5);
const CURSOR_RECOVERY_SENTINEL_TIMEOUT: Duration = Duration::from_secs(5);
const WORKER_LIFECYCLE_MIN_EMIT_INTERVAL: Duration = Duration::from_secs(1);
const CURSOR_RECOVERY_SENTINEL_PAYLOAD_DOMAIN: &[u8] =
    b"sinomo.cloud-files.cursor-recovery.sentinel.v1";

fn adapter_error(code: &str, detail: impl Into<String>) -> CloudFilesModuleError {
    CloudFilesModuleError {
        code: code.to_string(),
        detail: detail.into(),
    }
}

const WORKER_WAKE_LOCAL_CHANGE: u8 = 1 << 0;
const WORKER_WAKE_NAMESPACE_CHANGE: u8 = 1 << 1;
const WORKER_WAKE_AUTHORITATIVE_RESCAN: u8 = 1 << 2;
const WORKER_WAKE_MATERIALIZATION: u8 = 1 << 3;
const WORKER_WAKE_CONTROL: u8 = 1 << 4;

#[derive(Default)]
struct WindowsWorkerWakeState {
    generation: u64,
    reasons: u8,
    failure: Option<String>,
}

/// Process-local reactive gate for the writable provider. Producers only set
/// an in-memory bit and notify the condition variable; no idle timer, status
/// file, or namespace poll is required to keep the mount alive.
#[derive(Default)]
pub(super) struct WindowsWorkerWake {
    state: Mutex<WindowsWorkerWakeState>,
    changed: Condvar,
}

impl WindowsWorkerWake {
    fn signal(&self, reason: u8) {
        if let Ok(mut state) = self.state.lock() {
            state.generation = state.generation.saturating_add(1);
            state.reasons |= reason;
            self.changed.notify_all();
        }
    }

    pub(super) fn signal_namespace_change(&self) {
        self.signal(WORKER_WAKE_NAMESPACE_CHANGE);
    }

    pub(super) fn signal_authoritative_rescan(&self) {
        self.signal(WORKER_WAKE_AUTHORITATIVE_RESCAN);
    }

    fn signal_failure(&self, detail: String) {
        if let Ok(mut state) = self.state.lock() {
            state.failure = Some(detail);
            state.generation = state.generation.saturating_add(1);
            state.reasons |= WORKER_WAKE_CONTROL;
            self.changed.notify_all();
        }
    }

    fn take_failure(&self) -> Option<String> {
        self.state.lock().ok()?.failure.take()
    }

    pub(super) fn signal_materialization(&self) {
        self.signal(WORKER_WAKE_MATERIALIZATION);
    }

    fn signal_control(&self) {
        self.signal(WORKER_WAKE_CONTROL);
    }

    fn wait(&self, cancelled: &AtomicBool, deadline: Option<Duration>) -> u8 {
        let Ok(mut state) = self.state.lock() else {
            return WORKER_WAKE_CONTROL;
        };
        if state.reasons != 0 {
            return std::mem::take(&mut state.reasons);
        }
        if cancelled.load(Ordering::SeqCst) {
            return WORKER_WAKE_CONTROL;
        }
        state = match deadline {
            Some(delay) => self
                .changed
                .wait_timeout(state, delay)
                .map(|(state, _)| state)
                .unwrap_or_else(|error| error.into_inner().0),
            None => self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner()),
        };
        std::mem::take(&mut state.reasons)
    }
}

pub(super) struct WindowsNamespaceEventSubscription {
    stop: tokio::sync::watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
}

impl Drop for WindowsNamespaceEventSubscription {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

/// Subscribe before the startup snapshot so every later remote mutation either
/// participates in that snapshot or leaves a process-local wake bit. The
/// stream blocks inside the daemon transport and performs no periodic probe or
/// reconnect I/O; a broken stream fails the provider so authority can recover
/// explicitly instead of silently polling.
pub(super) fn subscribe_namespace_changes(
    data_dir: SinomoDataDir,
    root_path: &str,
    wake: Arc<WindowsWorkerWake>,
) -> Result<WindowsNamespaceEventSubscription, CloudFilesModuleError> {
    let address = match sinomo_runtime::api::probe_daemon_status(
        &sinomo_runtime::api::DaemonPaths::new(&data_dir),
        Duration::from_secs(3),
    ) {
        sinomo_runtime::api::DaemonProbeStatus::Running(status) => status.address,
        status => {
            return Err(adapter_error(
                "namespace-event-daemon-unavailable",
                format!("local daemon is not available for namespace events: {status:?}"),
            ));
        }
    };
    let request = ObjectRpcRequest::encode_body(
        sinomo_runtime::api::DAEMON_EVENT_SUBSCRIBE_ROUTE,
        sinomo_runtime::api::DAEMON_RPC_CATEGORY,
        &sinomo_runtime::api::DaemonEventSubscribeRequest::for_path_prefix(root_path),
    )
    .map_err(|error| adapter_error("namespace-event-request", error.to_string()))?;
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    let (stop, mut stop_receiver) = tokio::sync::watch::channel(false);
    let join = thread::Builder::new()
        .name("sinomo-cloud-files-namespace-events".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_sender.send(Err(error.to_string()));
                    return;
                }
            };
            let mut ready_sender = Some(ready_sender);
            let result = runtime.block_on(async {
                tokio::select! {
                    result = sinomo_runtime::api::send_daemon_rpc_stream_from_data_dir_async(
                        data_dir,
                        &address,
                        &request,
                        namespace_event_stream_timeouts(),
                        |response| {
                            response.error_detail()?;
                            match response.decode_body::<sinomo_runtime::api::DaemonEventStreamItem>()? {
                                sinomo_runtime::api::DaemonEventStreamItem::Ready { .. } => {
                                    if let Some(sender) = ready_sender.take() {
                                        let _ = sender.send(Ok(()));
                                    }
                                }
                                sinomo_runtime::api::DaemonEventStreamItem::Event { .. } => {
                                    wake.signal_namespace_change();
                                }
                                sinomo_runtime::api::DaemonEventStreamItem::Lagged { .. } => {
                                    wake.signal_authoritative_rescan();
                                }
                            }
                            Ok(true)
                        },
                    ) => Some(result),
                    _ = stop_receiver.changed() => None,
                }
            });
            let Some(result) = result else {
                return;
            };
            let detail = match result {
                Ok(()) => "local daemon closed the namespace event stream".to_string(),
                Err(error) => format!("local daemon namespace event stream failed: {error}"),
            };
            if let Some(sender) = ready_sender.take() {
                let _ = sender.send(Err(detail));
            } else {
                wake.signal_failure(detail);
            }
        })
        .map_err(|error| adapter_error("namespace-event-spawn", error.to_string()))?;
    match ready_receiver.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => Ok(WindowsNamespaceEventSubscription {
            stop,
            join: Some(join),
        }),
        Ok(Err(detail)) => {
            let _ = stop.send(true);
            let _ = join.join();
            Err(adapter_error("namespace-event-subscribe", detail))
        }
        Err(error) => {
            let _ = stop.send(true);
            let _ = join.join();
            Err(adapter_error(
                "namespace-event-ready-timeout",
                format!("namespace event subscription did not become ready: {error}"),
            ))
        }
    }
}

fn namespace_event_stream_timeouts() -> DaemonRpcTimeouts {
    // Silence is the successful steady state for this subscription. Bound
    // connection establishment, then let the already-established transport
    // block until an event, explicit stop, or actual stream failure arrives.
    // An idle transfer timeout would turn every quiet mount into a periodic
    // provider restart and full reconciliation.
    DaemonRpcTimeouts::connect_only(Duration::from_secs(3))
}

fn enter_writable_worker_background_mode() -> Result<(), String> {
    // SAFETY: GetCurrentThread returns the pseudo-handle for this worker, and
    // THREAD_MODE_BACKGROUND_BEGIN is defined specifically for the calling
    // thread. The mode lasts only until this dedicated worker exits.
    unsafe { SetThreadPriority(GetCurrentThread(), THREAD_MODE_BACKGROUND_BEGIN) }
        .map_err(|error| format!("set Cloud Files reconciliation thread background mode: {error}"))
}

fn lock_service<T>(
    service: &Arc<Mutex<NetworkedDfsService<T>>>,
) -> Result<MutexGuard<'_, NetworkedDfsService<T>>, String> {
    let deadline = Instant::now()
        .checked_add(SERVICE_LOCK_TIMEOUT)
        .ok_or_else(|| "mounted SIFS service lock deadline overflowed".to_string())?;
    loop {
        match service.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => {
                return Err("mounted SIFS service lock is poisoned".to_string());
            }
            Err(TryLockError::WouldBlock) if Instant::now() >= deadline => {
                return Err(format!(
                    "mounted SIFS service remained busy for {} ms",
                    SERVICE_LOCK_TIMEOUT.as_millis()
                ));
            }
            Err(TryLockError::WouldBlock) => thread::sleep(SERVICE_LOCK_RETRY),
        }
    }
}

pub(super) struct WindowsDfsNamespaceSource<T> {
    service: Arc<Mutex<NetworkedDfsService<T>>>,
    mounted_mutation_handoff: Arc<CloudFilesMountedMutationHandoff>,
    prepopulate_depth: u32,
    materialized_directories: Arc<Mutex<BTreeSet<String>>>,
    materialization_audit_intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
    scan: Option<WindowsSparseScanPlan>,
}

impl<T> WindowsDfsNamespaceSource<T> {
    pub(super) fn new(
        service: Arc<Mutex<NetworkedDfsService<T>>>,
        mounted_mutation_handoff: Arc<CloudFilesMountedMutationHandoff>,
        prepopulate_depth: u32,
        materialized_directories: Arc<Mutex<BTreeSet<String>>>,
        materialization_audit_intents: Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
    ) -> Self {
        Self {
            service,
            mounted_mutation_handoff,
            prepopulate_depth,
            materialized_directories,
            materialization_audit_intents,
            scan: None,
        }
    }
}

pub(super) fn record_materialization_audit_intent(
    intents: &Arc<Mutex<CloudFilesMaterializationAuditIntentStore>>,
    directory: &str,
) -> Result<(), String> {
    intents
        .lock()
        .map_err(|_| "Cloud Files materialization audit-intent store is poisoned".to_string())?
        .record_directory(directory)
        .map_err(|error| error.to_string())
}

#[derive(Clone, Debug)]
struct WindowsDirectoryScanTask {
    path: String,
    depth: u32,
    after: Option<dfs::DfsDirectoryPageCursor>,
}

#[derive(Clone, Debug)]
struct WindowsActiveDirectoryPage {
    task: WindowsDirectoryScanTask,
    entries: VecDeque<dfs::DfsDirEntry>,
    next_cursor: Option<dfs::DfsDirectoryPageCursor>,
}

#[derive(Clone, Debug)]
struct WindowsSparseScanPlan {
    boundary_cursors: BTreeMap<String, CloudFilesNamespaceCursor>,
    known_before: Option<String>,
    known_complete: bool,
    directories: VecDeque<WindowsDirectoryScanTask>,
    queued_directories: BTreeSet<String>,
    materialized_directories: BTreeSet<String>,
    active_page: Option<WindowsActiveDirectoryPage>,
}

impl<T: DfsMessageTransport> CloudFilesNamespaceSource for WindowsDfsNamespaceSource<T> {
    fn poll_batches(
        &mut self,
        root: &str,
        cursors: &[CloudFilesNamespaceCursor],
    ) -> Result<Vec<CloudFilesNamespaceFeedBatch>, String> {
        let dfs_cursors = cursors
            .iter()
            .map(dfs_namespace_cursor_from_cloud)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let inputs = dfs_cursors
            .iter()
            .map(|cursor| (cursor.shard_id.clone(), cursor.clone()))
            .collect::<BTreeMap<_, _>>();
        let batches = lock_service(&self.service)?
            .poll_namespace_changes_for_subtree(dfs::sifs_file_system_id(), root, &dfs_cursors)
            .map_err(|error| error.to_string())?;
        batches
            .into_iter()
            .map(|batch| {
                let input = inputs
                    .get(&batch.next_cursor.shard_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        let mut cursor = batch.next_cursor.clone();
                        cursor.after_sequence = 0;
                        cursor
                    });
                Ok(cloud_namespace_batch_from_dfs(&input, batch))
            })
            .collect()
    }

    fn full_scan(
        &mut self,
        root: &str,
        previous_cursors: &[CloudFilesNamespaceCursor],
        materialized_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        durable_materialized_directories: &BTreeSet<String>,
    ) -> Result<CloudFilesFullScanSnapshot, String> {
        if self.scan.is_none() {
            let root_stat = lock_service(&self.service)?
                .posix_lstat_distributed(root)
                .map_err(|error| {
                    format!("authoritative stat of Cloud Files root `{root}`: {error}")
                })?;
            if !cloud_files_root_kind_is_supported(&root_stat.kind) {
                return Err(format!(
                    "Cloud Files root `{root}` is {:?}, not a directory or mount",
                    root_stat.kind
                ));
            }
            let cursors = previous_cursors
                .iter()
                .map(dfs_namespace_cursor_from_cloud)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?;
            let batches = lock_service(&self.service)?
                .poll_namespace_changes_for_subtree(dfs::sifs_file_system_id(), root, &cursors)
                .map_err(|error| error.to_string())?;
            if batches.is_empty() {
                return Err("DFS returned no namespace shards for full reconciliation".to_string());
            }
            let mut boundary_cursors = BTreeMap::new();
            for batch in batches {
                let mut cursor = batch.next_cursor;
                // The authoritative sparse scan begins after this observed
                // head. Replaying from the head after its terminal delta is
                // conservative and cannot skip a concurrent mutation.
                cursor.after_sequence = batch.journal_head_sequence;
                let cloud = cloud_namespace_cursor_from_dfs(&cursor);
                boundary_cursors
                    .insert(cloud.shard_key().map_err(|error| error.to_string())?, cloud);
            }
            let mut materialized_directories = self
                .materialized_directories
                .lock()
                .map_err(|_| "Cloud Files materialized-directory ledger is poisoned".to_string())?
                .clone();
            materialized_directories.extend(
                self.materialization_audit_intents
                    .lock()
                    .map_err(|_| {
                        "Cloud Files materialization audit-intent store is poisoned".to_string()
                    })?
                    .pending_directories()
                    .iter()
                    .cloned(),
            );
            let root_was_materialized_in_process = materialized_directories.contains(root);
            materialized_directories.extend(durable_materialized_directories.iter().cloned());
            if sparse_root_is_enumerated(
                self.prepopulate_depth,
                root_was_materialized_in_process || durable_materialized_directories.contains(root),
            ) {
                materialized_directories.insert(root.to_string());
            } else {
                materialized_directories.remove(root);
            }
            let mut directories = VecDeque::new();
            let mut queued_directories = BTreeSet::new();
            for directory in &materialized_directories {
                if queued_directories.insert(directory.clone()) {
                    directories.push_back(WindowsDirectoryScanTask {
                        path: directory.clone(),
                        depth: relative_depth(root, directory),
                        after: None,
                    });
                }
            }
            self.scan = Some(WindowsSparseScanPlan {
                boundary_cursors,
                known_before: None,
                known_complete: materialized_manifest.is_empty(),
                directories,
                queued_directories,
                materialized_directories,
                active_page: None,
            });
        }

        let mut plan = self.scan.take().expect("initialized sparse scan plan");
        let mut manifest = BTreeMap::new();
        let mut removed_paths = BTreeSet::new();
        let mut audited_materialized_directories = BTreeSet::new();

        // One durable worker pass should cover one bounded page of remote
        // observations, rather than fsyncing state after every individual
        // stat. The in-memory plan remains resumable between these quanta.
        for _ in 0..NAMESPACE_FULL_SCAN_OBSERVATIONS_PER_PASS {
            let known_path = if plan.known_complete {
                None
            } else {
                let page = next_sparse_known_path(
                    materialized_manifest,
                    &plan.materialized_directories,
                    plan.known_before.as_deref(),
                    CLOUD_FILES_DIRECTORY_PAGE_ENTRIES,
                );
                plan.known_before = page.before;
                plan.known_complete = page.complete;
                page.path
            };
            if let Some(path) = known_path {
                match lock_service(&self.service)?.posix_lstat_distributed(&path) {
                    Ok(stat) if stat.kind != DfsObjectKind::Tombstone => {
                        let entry = manifest_entry_from_stat(stat);
                        removed_paths.remove(&entry.path);
                        manifest.insert(entry.path.clone(), entry);
                    }
                    Ok(_) | Err(dfs::DfsPosixError::NotFound { .. }) => {
                        manifest.remove(&path);
                        removed_paths.insert(path);
                    }
                    Err(error) => return Err(error.to_string()),
                }
            } else if plan.known_complete {
                if plan.active_page.is_none()
                    && let Some(task) = plan.directories.pop_front()
                {
                    let page = self
                        .mounted_mutation_handoff
                        .run_captured_read_ingest(
                            &self.service,
                            "cloud-files.namespace-full-scan-directory-page",
                            |service| {
                                service.capture_mounted_mutation(|service| {
                                    service.posix_list_dir_distributed_page(
                                        &task.path,
                                        task.after.as_ref(),
                                        CLOUD_FILES_DIRECTORY_PAGE_ENTRIES,
                                    )
                                })
                            },
                        )
                        .map_err(|error| {
                            format!(
                                "durably hand off Cloud Files namespace full-scan directory refresh for `{}`: {error}",
                                task.path
                            )
                        })?;
                    match page {
                        Ok(page) => {
                            // A durable materialization ledger can outlive the
                            // physical directory placeholder (for example, when a
                            // provider restart follows a rolled-back registration).
                            // Re-establish the enumerated directory itself before
                            // later snapshots try to create any of its children.
                            // The root is the registered sync-root directory and is
                            // therefore not created as a child placeholder.
                            if task.path != root {
                                let stat = lock_service(&self.service)?
                                    .posix_lstat_distributed(&task.path)
                                    .map_err(|error| {
                                        format!(
                                            "authoritative stat of materialized Cloud Files directory `{}`: {error}",
                                            task.path
                                        )
                                    })?;
                                let entry = materialized_directory_manifest_entry(stat)?;
                                removed_paths.remove(&entry.path);
                                manifest.insert(entry.path.clone(), entry);
                            }
                            audited_materialized_directories.insert(task.path.clone());
                            plan.active_page = Some(WindowsActiveDirectoryPage {
                                task,
                                entries: page.entries.into(),
                                next_cursor: page.next_cursor,
                            });
                        }
                        Err(dfs::DfsPosixError::NotFound { .. })
                        | Err(dfs::DfsPosixError::MissingParent { .. }) => {
                            audited_materialized_directories.insert(task.path.clone());
                            manifest.remove(&task.path);
                            removed_paths.insert(task.path.clone());
                            remove_path_subtree(&mut plan.materialized_directories, &task.path);
                            if let Ok(mut materialized) = self.materialized_directories.lock() {
                                remove_path_subtree(&mut materialized, &task.path);
                            }
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                }
                if let Some(active) = plan.active_page.as_mut()
                    && let Some(child) = active.entries.pop_front()
                {
                    // A crash can persist an already-open directory page before
                    // the parent placeholder effect completes. Carry the parent
                    // in every resumed page snapshot so recovery still orders the
                    // directory before the next child.
                    if active.task.path != root {
                        let stat = lock_service(&self.service)?
                            .posix_lstat_distributed(&active.task.path)
                            .map_err(|error| {
                                format!(
                                    "authoritative stat of resumed materialized Cloud Files directory `{}`: {error}",
                                    active.task.path
                                )
                            })?;
                        let entry = materialized_directory_manifest_entry(stat)?;
                        removed_paths.remove(&entry.path);
                        manifest.insert(entry.path.clone(), entry);
                    }
                    match lock_service(&self.service)?.posix_lstat_distributed(&child.path) {
                        Ok(stat) if stat.kind != DfsObjectKind::Tombstone => {
                            if cloud_files_object_projection(&stat.kind)
                                == CloudFilesObjectProjection::Directory
                            {
                                let child_depth = active.task.depth.saturating_add(1);
                                let should_scan = sparse_child_directory_is_enumerated(
                                    child_depth,
                                    self.prepopulate_depth,
                                    plan.materialized_directories.contains(&stat.path),
                                );
                                if should_scan && plan.queued_directories.insert(stat.path.clone())
                                {
                                    plan.directories.push_back(WindowsDirectoryScanTask {
                                        path: stat.path.clone(),
                                        depth: child_depth,
                                        after: None,
                                    });
                                }
                            }
                            let entry = manifest_entry_from_stat(stat);
                            removed_paths.remove(&entry.path);
                            manifest.insert(entry.path.clone(), entry);
                        }
                        Ok(_) | Err(dfs::DfsPosixError::NotFound { .. }) => {
                            manifest.remove(&child.path);
                            removed_paths.insert(child.path);
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                }
                let page_finished = plan
                    .active_page
                    .as_ref()
                    .is_some_and(|active| active.entries.is_empty());
                if page_finished {
                    let active = plan.active_page.take().expect("finished active page");
                    if let Some(after) = active.next_cursor {
                        plan.directories.push_front(WindowsDirectoryScanTask {
                            after: Some(after),
                            ..active.task
                        });
                    }
                }
            }

            if plan.known_complete && plan.directories.is_empty() && plan.active_page.is_none() {
                break;
            }
        }

        let scan_complete =
            plan.known_complete && plan.directories.is_empty() && plan.active_page.is_none();
        let cursors = if scan_complete {
            plan.boundary_cursors
        } else {
            self.scan = Some(plan);
            BTreeMap::new()
        };
        Ok(CloudFilesFullScanSnapshot {
            manifest,
            removed_paths: removed_paths.into_iter().collect(),
            materialized_directories: audited_materialized_directories.into_iter().collect(),
            scan_complete,
            cursors,
        })
    }

    fn resolve_placeholder_identity(
        &mut self,
        identity: &CloudFilesPlaceholderIdentity,
    ) -> Result<Option<CloudFilesReconciledManifestEntry>, String> {
        match lock_service(&self.service)?.posix_lstat_distributed(&identity.path) {
            Ok(stat) if stat.kind != DfsObjectKind::Tombstone => {
                Ok(Some(manifest_entry_from_stat(stat)))
            }
            Ok(_)
            | Err(dfs::DfsPosixError::NotFound { .. })
            | Err(dfs::DfsPosixError::MissingParent { .. }) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    fn has_pending_materialized_directories(
        &self,
        durable_materialized_directories: &BTreeSet<String>,
    ) -> Result<bool, String> {
        let in_process_pending = self
            .materialized_directories
            .lock()
            .map_err(|_| "Cloud Files materialized-directory ledger is poisoned".to_string())?
            .iter()
            .any(|directory| !durable_materialized_directories.contains(directory));
        if in_process_pending {
            return Ok(true);
        }
        let audit_intents = self.materialization_audit_intents.lock().map_err(|_| {
            "Cloud Files materialization audit-intent store is poisoned".to_string()
        })?;
        Ok(!audit_intents.pending_directories().is_empty())
    }

    fn acknowledge_materialized_directories(
        &mut self,
        directories: &[String],
    ) -> Result<(), String> {
        self.materialization_audit_intents
            .lock()
            .map_err(|_| "Cloud Files materialization audit-intent store is poisoned".to_string())?
            .acknowledge_directories(directories.iter().map(String::as_str))
            .map_err(|error| error.to_string())
    }

    fn acknowledge_materialized_directories_after_cursor_recovery_root_audit(
        &mut self,
    ) -> Result<(), String> {
        let mut intents = self.materialization_audit_intents.lock().map_err(|_| {
            "Cloud Files materialization audit-intent store is poisoned".to_string()
        })?;
        let audited = intents
            .pending_directories()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        intents
            .acknowledge_directories(audited.iter().map(String::as_str))
            .map_err(|error| error.to_string())
    }
}

fn remove_path_subtree(paths: &mut BTreeSet<String>, prefix: &str) {
    paths.retain(|path| {
        path != prefix
            && !path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    });
}

fn relative_depth(root: &str, path: &str) -> u32 {
    let suffix = if root == "/" {
        path.trim_start_matches('/')
    } else {
        path.strip_prefix(root)
            .unwrap_or(path)
            .trim_start_matches('/')
    };
    suffix
        .split('/')
        .filter(|component| !component.is_empty())
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
}

fn materialized_directory_manifest_entry(
    stat: dfs::DfsStat,
) -> Result<CloudFilesReconciledManifestEntry, String> {
    if !matches!(stat.kind, DfsObjectKind::Directory | DfsObjectKind::Mount) {
        return Err(format!(
            "materialized Cloud Files directory `{}` resolved as {:?}",
            stat.path, stat.kind
        ));
    }
    Ok(manifest_entry_from_stat(stat))
}

fn manifest_entry_from_stat(stat: dfs::DfsStat) -> CloudFilesReconciledManifestEntry {
    CloudFilesReconciledManifestEntry {
        path: stat.path,
        object_id: stat.object_id.to_string(),
        version: stat.version.to_string(),
        content_hash: stat.content_hash,
        content_size_bytes: stat.size_bytes,
        kind: dfs_kind_name(&stat.kind).to_string(),
    }
}

fn dfs_kind_name(kind: &DfsObjectKind) -> &'static str {
    match kind {
        DfsObjectKind::File => "file",
        DfsObjectKind::Directory => "directory",
        DfsObjectKind::Symlink => "symlink",
        DfsObjectKind::Hardlink => "hardlink",
        DfsObjectKind::Artifact => "artifact",
        DfsObjectKind::Mount => "mount",
        DfsObjectKind::Tombstone => "tombstone",
    }
}

#[derive(Clone, Debug, Default)]
struct RawDirectoryChangeBatch {
    changes: Vec<CloudFilesDirectoryChange>,
    overflowed: bool,
}

#[derive(Debug)]
struct CursorRecoverySentinelCommand {
    name: String,
    payload: Vec<u8>,
    stale_specs: Vec<CursorRecoverySentinelSpec>,
    reply: mpsc::SyncSender<Result<CursorRecoverySentinelAck, String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CursorRecoverySentinelSpec {
    name: String,
    payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CursorRecoverySentinelAck {
    non_sentinel_epoch: u64,
    sync_root_file_id: u64,
}

struct CursorRecoveryAuditGuard {
    children: Vec<CursorRecoveryHeldChild>,
    sync_root_file_id: u64,
    post_audit_fence_epoch: Option<u64>,
}

struct CursorRecoveryHeldChild {
    handle: OwnedWin32Handle,
    sifs_path: String,
    is_directory: bool,
    file_id: u64,
}

// Win32 kernel handles are process-wide values. The guard is created and
// destroyed by the writable worker, but that worker itself is moved into its
// dedicated thread before recovery begins.
unsafe impl Send for CursorRecoveryAuditGuard {}

struct ActiveCursorRecoverySentinel {
    command: CursorRecoverySentinelCommand,
    state: CursorRecoverySentinelState,
    handle: Option<OwnedWin32Handle>,
    stale_removal: Option<PathBuf>,
    needs_create: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PhysicalDirectoryEntryName {
    name: OsString,
    alternate_name: Option<OsString>,
}

struct OwnedFindHandle(HANDLE);

impl Drop for OwnedFindHandle {
    fn drop(&mut self) {
        unsafe {
            let _ = FindClose(self.0);
        }
    }
}

struct OwnedCloudFilesHandle(HANDLE);

impl Drop for OwnedCloudFilesHandle {
    fn drop(&mut self) {
        unsafe {
            CfCloseHandle(self.0);
        }
    }
}

#[derive(Clone, Debug)]
struct PlaceholderStandardState {
    in_sync: bool,
    modified_data_size: i64,
    file_id: u64,
    sync_root_file_id: u64,
    identity: Option<CloudFilesPlaceholderIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorRecoverySentinelPhase {
    AwaitAdded,
    AwaitRemoved,
    Complete,
}

#[derive(Debug)]
struct CursorRecoverySentinelState {
    name: PathBuf,
    phase: CursorRecoverySentinelPhase,
}

impl CursorRecoverySentinelState {
    fn new(name: &str) -> Self {
        Self {
            name: PathBuf::from(name),
            phase: CursorRecoverySentinelPhase::AwaitAdded,
        }
    }

    fn observe(
        &mut self,
        change: &CloudFilesDirectoryChange,
    ) -> Result<SentinelObservation, String> {
        let (action, path) = match change {
            CloudFilesDirectoryChange::Added(path) => ("added", path),
            CloudFilesDirectoryChange::Removed(path) => ("removed", path),
            CloudFilesDirectoryChange::Modified(path) => ("modified", path),
            CloudFilesDirectoryChange::Renamed { from, to } => {
                if from == &self.name || to == &self.name {
                    return Err("cursor recovery sentinel was renamed".to_string());
                }
                return Ok(SentinelObservation::NonSentinel);
            }
        };
        if path != &self.name {
            return Ok(SentinelObservation::NonSentinel);
        }
        match (self.phase, action) {
            (CursorRecoverySentinelPhase::AwaitAdded, "added") => {
                self.phase = CursorRecoverySentinelPhase::AwaitRemoved;
                Ok(SentinelObservation::Added)
            }
            (CursorRecoverySentinelPhase::AwaitRemoved, "removed") => {
                self.phase = CursorRecoverySentinelPhase::Complete;
                Ok(SentinelObservation::Removed)
            }
            (CursorRecoverySentinelPhase::AwaitRemoved, "modified") => {
                Ok(SentinelObservation::SentinelModified)
            }
            _ => Err(format!(
                "cursor recovery sentinel received out-of-order `{action}` notification"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SentinelObservation {
    Added,
    Removed,
    SentinelModified,
    NonSentinel,
}

fn hash_cursor_recovery_sentinel_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn cursor_recovery_sentinel_digest(
    recovery_id: &str,
    recovery_token: &str,
    nonce: &str,
    sync_root_file_id: u64,
    round: u8,
) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hash_cursor_recovery_sentinel_field(&mut hasher, CURSOR_RECOVERY_SENTINEL_PAYLOAD_DOMAIN);
    hash_cursor_recovery_sentinel_field(&mut hasher, recovery_id.as_bytes());
    hash_cursor_recovery_sentinel_field(&mut hasher, recovery_token.as_bytes());
    hash_cursor_recovery_sentinel_field(&mut hasher, nonce.as_bytes());
    hash_cursor_recovery_sentinel_field(&mut hasher, &sync_root_file_id.to_le_bytes());
    hash_cursor_recovery_sentinel_field(&mut hasher, &[round]);
    hasher.finalize()
}

fn cursor_recovery_sentinel_name(
    recovery_id: &str,
    recovery_token: &str,
    nonce: &str,
    sync_root_file_id: u64,
    round: u8,
) -> String {
    let hex = cursor_recovery_sentinel_digest(
        recovery_id,
        recovery_token,
        nonce,
        sync_root_file_id,
        round,
    )
    .to_hex()
    .to_string()
    .to_ascii_uppercase();
    format!("{}.{}", &hex[..8], &hex[8..11])
}

fn cursor_recovery_sentinel_payload(
    recovery_id: &str,
    recovery_token: &str,
    nonce: &str,
    sync_root_file_id: u64,
    round: u8,
) -> Vec<u8> {
    cursor_recovery_sentinel_digest(recovery_id, recovery_token, nonce, sync_root_file_id, round)
        .as_bytes()
        .to_vec()
}

fn cursor_recovery_sentinel_spec(
    recovery_id: &str,
    recovery_token: &str,
    nonce: &str,
    sync_root_file_id: u64,
    round: u8,
) -> CursorRecoverySentinelSpec {
    CursorRecoverySentinelSpec {
        name: cursor_recovery_sentinel_name(
            recovery_id,
            recovery_token,
            nonce,
            sync_root_file_id,
            round,
        ),
        payload: cursor_recovery_sentinel_payload(
            recovery_id,
            recovery_token,
            nonce,
            sync_root_file_id,
            round,
        ),
    }
}

fn validate_cursor_recovery_final_snapshot(
    post_audit_fence_epoch: Option<u64>,
    current_non_sentinel_epoch: u64,
    guard_root_file_id: u64,
    expected_root_file_id: u64,
    watcher_root_file_id: u64,
) -> Result<(), String> {
    let _linearization_epoch = post_audit_fence_epoch.ok_or_else(|| {
        "cursor recovery root audit has no completed post-audit fence".to_string()
    })?;
    // The sentinel Removed record is the proof cut. Later non-sentinel changes
    // are valid buffered work, so continuity deliberately does not require
    // the live epoch to equal the cut epoch.
    let _post_cut_epoch = current_non_sentinel_epoch;
    if guard_root_file_id != expected_root_file_id {
        return Err("cursor recovery root audit guard identity changed".to_string());
    }
    if watcher_root_file_id != guard_root_file_id {
        return Err("cursor recovery watcher root identity changed".to_string());
    }
    Ok(())
}

struct OwnedCrossThreadEvent(OwnedWin32Handle);

// Win32 event handles are kernel objects whose SetEvent/Wait operations are
// explicitly thread-safe. `windows` leaves raw HANDLE Send/Sync unconstrained,
// so this narrow owner documents and contains the cross-thread use.
unsafe impl Send for OwnedCrossThreadEvent {}
unsafe impl Sync for OwnedCrossThreadEvent {}

struct DirectoryWatcherThread {
    stop: Arc<AtomicBool>,
    stop_event: Arc<OwnedCrossThreadEvent>,
    raw_handle: Arc<AtomicIsize>,
    sync_root_file_id: u64,
    join: Option<JoinHandle<()>>,
    sentinel: mpsc::SyncSender<CursorRecoverySentinelCommand>,
}

impl DirectoryWatcherThread {
    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        unsafe {
            let _ = windows::Win32::System::Threading::SetEvent(self.stop_event.0.0);
        }
        let raw = self.raw_handle.load(Ordering::SeqCst);
        if raw != 0 {
            let handle = HANDLE(raw as *mut c_void);
            unsafe {
                let _ = CancelIoEx(handle, None);
            }
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for DirectoryWatcherThread {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

pub(super) struct WindowsDirectoryChangeAccelerator {
    sync_root: PathBuf,
    root_path: String,
    expected_cursor_recovery_sync_root_file_id: Option<u64>,
    startup_scan_plan: Option<WindowsLocalStartupScanPlan>,
    startup_conflicts: Vec<CloudFilesStartupConflictEvidence>,
    suppressed_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
    receiver: Option<mpsc::Receiver<RawDirectoryChangeBatch>>,
    notification_overflow: Arc<AtomicBool>,
    notification_epoch: Arc<AtomicU64>,
    watcher_failure: Arc<Mutex<Option<String>>>,
    watcher: Option<DirectoryWatcherThread>,
    cursor_recovery_fence_round: u8,
    cursor_recovery_audit_guard: Option<CursorRecoveryAuditGuard>,
    capture_retry_epoch: Instant,
    capture_retries: CloudFilesLocalCaptureRetries,
    content_stager: CloudFilesContentStager,
    worker_wake: Arc<WindowsWorkerWake>,
}

impl WindowsDirectoryChangeAccelerator {
    pub(super) fn new(
        sync_root: PathBuf,
        root_path: String,
        _existing_owned_root: bool,
        expected_cursor_recovery_sync_root_file_id: Option<u64>,
        suppressed_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
        content_stager: CloudFilesContentStager,
        worker_wake: Arc<WindowsWorkerWake>,
    ) -> Self {
        Self {
            sync_root,
            root_path,
            expected_cursor_recovery_sync_root_file_id,
            startup_scan_plan: None,
            startup_conflicts: Vec::new(),
            suppressed_paths,
            receiver: None,
            notification_overflow: Arc::new(AtomicBool::new(false)),
            notification_epoch: Arc::new(AtomicU64::new(0)),
            watcher_failure: Arc::new(Mutex::new(None)),
            watcher: None,
            cursor_recovery_fence_round: 0,
            cursor_recovery_audit_guard: None,
            capture_retry_epoch: Instant::now(),
            capture_retries: CloudFilesLocalCaptureRetries::default(),
            content_stager,
            worker_wake,
        }
    }
}

impl CloudFilesLocalChangeAccelerator for WindowsDirectoryChangeAccelerator {
    fn arm_cursor_recovery_observer(&mut self) -> Result<u64, String> {
        let epoch = self.notification_epoch.load(Ordering::SeqCst);
        self.activate()?;
        Ok(epoch)
    }

    fn cursor_recovery_observation_epoch(&self) -> Result<u64, String> {
        if self.watcher.is_none() || self.receiver.is_none() {
            return Err("ReadDirectoryChangesW recovery observer is not active".to_string());
        }
        if let Some(error) = self
            .watcher_failure
            .lock()
            .map_err(|_| "ReadDirectoryChangesW failure state is poisoned".to_string())?
            .as_ref()
        {
            return Err(error.clone());
        }
        if self.notification_overflow.load(Ordering::SeqCst) {
            return Err("ReadDirectoryChangesW recovery observer overflowed".to_string());
        }
        if self
            .watcher
            .as_ref()
            .is_none_or(|watcher| watcher.raw_handle.load(Ordering::SeqCst) == 0)
        {
            return Err("ReadDirectoryChangesW recovery observer stopped".to_string());
        }
        Ok(self.notification_epoch.load(Ordering::SeqCst))
    }

    fn cursor_recovery_namespace_fence(
        &mut self,
        recovery_id: &str,
        recovery_token: &str,
        nonce: &str,
    ) -> Result<u64, String> {
        let expected_sync_root_file_id = self
            .expected_cursor_recovery_sync_root_file_id
            .ok_or_else(|| {
                "cursor recovery namespace fence has no pinned sync-root file ID".to_string()
            })?;
        let round = self
            .cursor_recovery_fence_round
            .checked_add(1)
            .ok_or_else(|| "cursor recovery namespace fence round overflowed".to_string())?;
        if round > 2 {
            return Err("cursor recovery namespace fence ran more than twice".to_string());
        }
        let spec = cursor_recovery_sentinel_spec(
            recovery_id,
            recovery_token,
            nonce,
            expected_sync_root_file_id,
            round,
        );
        let stale_specs = if round == 1 {
            vec![
                cursor_recovery_sentinel_spec(
                    recovery_id,
                    recovery_token,
                    nonce,
                    expected_sync_root_file_id,
                    1,
                ),
                cursor_recovery_sentinel_spec(
                    recovery_id,
                    recovery_token,
                    nonce,
                    expected_sync_root_file_id,
                    2,
                ),
            ]
        } else {
            Vec::new()
        };
        let watcher = self
            .watcher
            .as_ref()
            .ok_or_else(|| "ReadDirectoryChangesW recovery observer is not active".to_string())?;
        let (reply, response) = mpsc::sync_channel(1);
        watcher
            .sentinel
            .send(CursorRecoverySentinelCommand {
                name: spec.name,
                payload: spec.payload,
                stale_specs,
                reply,
            })
            .map_err(|_| "ReadDirectoryChangesW recovery observer stopped".to_string())?;
        let ack = response
            .recv_timeout(CURSOR_RECOVERY_SENTINEL_TIMEOUT)
            .map_err(|error| format!("cursor recovery namespace fence timed out: {error}"))??;
        if ack.sync_root_file_id != expected_sync_root_file_id {
            return Err(format!(
                "cursor recovery watcher root file ID {} does not match pinned root file ID {}",
                ack.sync_root_file_id, expected_sync_root_file_id
            ));
        }
        self.cursor_recovery_observation_epoch()?;
        if round == 2 {
            let guard = self.cursor_recovery_audit_guard.as_mut().ok_or_else(|| {
                "cursor recovery post-audit fence has no retained root-audit handles".to_string()
            })?;
            guard.post_audit_fence_epoch = Some(ack.non_sentinel_epoch);
        }
        self.cursor_recovery_fence_round = round;
        Ok(ack.non_sentinel_epoch)
    }

    fn audit_cursor_recovery_root(
        &mut self,
        mount: &str,
        root: &str,
        remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        cancelled: &AtomicBool,
    ) -> Result<(), String> {
        if self.cursor_recovery_audit_guard.is_some() {
            return Err("cursor recovery root audit already retains child handles".to_string());
        }
        let expected_sync_root_file_id = self
            .expected_cursor_recovery_sync_root_file_id
            .ok_or_else(|| "cursor recovery root audit has no pinned root file ID".to_string())?;
        let children = audit_cursor_recovery_root(
            &self.sync_root,
            mount,
            root,
            expected_sync_root_file_id,
            remote_manifest,
            cancelled,
        )?;
        self.cursor_recovery_audit_guard = Some(CursorRecoveryAuditGuard {
            children,
            sync_root_file_id: expected_sync_root_file_id,
            post_audit_fence_epoch: None,
        });
        Ok(())
    }

    fn verify_cursor_recovery_root_audit(&self) -> Result<(), String> {
        let guard = self.cursor_recovery_audit_guard.as_ref().ok_or_else(|| {
            "cursor recovery root audit has no retained child handles".to_string()
        })?;
        let current_epoch = self.cursor_recovery_observation_epoch()?;
        let expected_root_file_id = self
            .expected_cursor_recovery_sync_root_file_id
            .ok_or_else(|| "cursor recovery root file ID pin disappeared".to_string())?;
        let watcher_root_file_id = self
            .watcher
            .as_ref()
            .ok_or_else(|| "cursor recovery watcher disappeared".to_string())?
            .sync_root_file_id;
        validate_cursor_recovery_final_snapshot(
            guard.post_audit_fence_epoch,
            current_epoch,
            guard.sync_root_file_id,
            expected_root_file_id,
            watcher_root_file_id,
        )?;
        for child in &guard.children {
            verify_held_cursor_recovery_child(child)?;
        }
        Ok(())
    }

    fn release_cursor_recovery_root_audit(&mut self) {
        if let Some(mut watcher) = self.watcher.take() {
            watcher.stop_and_join();
        }
        self.receiver = None;
        self.cursor_recovery_audit_guard = None;
        self.expected_cursor_recovery_sync_root_file_id = None;
    }

    fn startup_scan(
        &mut self,
        previous_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesStartupScanBatch, String> {
        let plan = self
            .startup_scan_plan
            .get_or_insert_with(|| WindowsLocalStartupScanPlan::new(&self.sync_root));
        let (scan, complete) = plan.next_batch(
            &self.sync_root,
            &self.root_path,
            previous_manifest,
            remote_manifest,
            &self.content_stager,
            cancelled,
        )?;
        self.startup_conflicts.extend(scan.conflicts);
        let now = self.capture_retry_epoch.elapsed();
        for path in scan.deferred_upserts {
            self.capture_retries.schedule(path, now);
        }
        if complete {
            self.startup_scan_plan = None;
        }
        Ok(CloudFilesStartupScanBatch {
            changes: scan.captures,
            sparse_placeholders: scan.sparse_placeholders,
            complete,
        })
    }

    fn drain_startup_conflicts(&mut self) -> Vec<CloudFilesStartupConflictEvidence> {
        std::mem::take(&mut self.startup_conflicts)
    }

    fn finish_sparse_placeholder_resolution(&mut self, path: &str, accepted: bool) {
        if let Some(plan) = self.startup_scan_plan.as_mut() {
            if accepted {
                plan.phase = LocalStartupScanPhase::Classify;
                plan.phase_paths.push_front(path.to_string());
            } else {
                plan.blocked_paths.insert(path.to_string());
            }
            return;
        }
        if accepted
            && let Ok(local) = sifs_to_local(&self.sync_root, &self.root_path, path)
            && let Ok(relative) = local.strip_prefix(&self.sync_root)
        {
            self.capture_retries
                .schedule(relative.to_path_buf(), self.capture_retry_epoch.elapsed());
        }
    }

    fn activate(&mut self) -> Result<(), String> {
        if self.watcher.is_some() {
            if self.expected_cursor_recovery_sync_root_file_id.is_some() {
                self.cursor_recovery_observation_epoch()?;
            }
            return Ok(());
        }
        let (sender, receiver) = mpsc::sync_channel(DIRECTORY_CHANGE_CHANNEL_CAPACITY);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (sentinel_sender, sentinel_receiver) = mpsc::sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let stop_event = Arc::new(OwnedCrossThreadEvent(OwnedWin32Handle(
            unsafe { CreateEventW(None, false, false, None) }
                .map_err(|error| format!("create ReadDirectoryChangesW stop event: {error}"))?,
        )));
        let raw_handle = Arc::new(AtomicIsize::new(0));
        let sync_root = self.sync_root.clone();
        let thread_stop = Arc::clone(&stop);
        let thread_handle = Arc::clone(&raw_handle);
        let notification_overflow = Arc::clone(&self.notification_overflow);
        let notification_epoch = Arc::clone(&self.notification_epoch);
        let watcher_failure = Arc::clone(&self.watcher_failure);
        let cursor_recovery = self.expected_cursor_recovery_sync_root_file_id.is_some();
        let thread_stop_event = Arc::clone(&stop_event);
        let worker_wake = Arc::clone(&self.worker_wake);
        let join = thread::Builder::new()
            .name("sinomo-cloud-files-directory-changes".to_string())
            .spawn(move || {
                watch_directory_changes(
                    sync_root,
                    thread_stop,
                    thread_stop_event,
                    thread_handle,
                    sender,
                    ready_sender,
                    notification_overflow,
                    notification_epoch,
                    watcher_failure,
                    sentinel_receiver,
                    cursor_recovery,
                    worker_wake,
                )
            })
            .map_err(|error| format!("spawn ReadDirectoryChangesW worker: {error}"))?;
        let observed_sync_root_file_id = match ready_receiver.recv_timeout(Duration::from_secs(5)) {
            Ok(Ok(file_id)) => file_id,
            Ok(Err(error)) => {
                let mut watcher = DirectoryWatcherThread {
                    stop,
                    stop_event,
                    raw_handle,
                    sync_root_file_id: 0,
                    join: Some(join),
                    sentinel: sentinel_sender.clone(),
                };
                watcher.stop_and_join();
                return Err(error);
            }
            Err(error) => {
                let mut watcher = DirectoryWatcherThread {
                    stop,
                    stop_event,
                    raw_handle,
                    sync_root_file_id: 0,
                    join: Some(join),
                    sentinel: sentinel_sender.clone(),
                };
                watcher.stop_and_join();
                return Err(format!(
                    "ReadDirectoryChangesW worker did not become ready: {error}"
                ));
            }
        };
        if let Some(expected) = self.expected_cursor_recovery_sync_root_file_id
            && observed_sync_root_file_id != expected
        {
            let mut watcher = DirectoryWatcherThread {
                stop,
                stop_event,
                raw_handle,
                sync_root_file_id: observed_sync_root_file_id,
                join: Some(join),
                sentinel: sentinel_sender,
            };
            watcher.stop_and_join();
            return Err(format!(
                "ReadDirectoryChangesW root file ID {observed_sync_root_file_id} does not match pinned cursor recovery root file ID {expected}"
            ));
        }
        self.receiver = Some(receiver);
        self.watcher = Some(DirectoryWatcherThread {
            stop,
            stop_event,
            raw_handle,
            sync_root_file_id: observed_sync_root_file_id,
            join: Some(join),
            sentinel: sentinel_sender,
        });
        Ok(())
    }

    fn drain_changes(
        &mut self,
        manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        cancelled: &AtomicBool,
    ) -> Result<CloudFilesCapturedChangeBatch, String> {
        if cancelled.load(Ordering::SeqCst) {
            return Err("sync-cancelled".to_string());
        }
        let Some(receiver) = self.receiver.as_ref() else {
            return Err("ReadDirectoryChangesW worker is not active".to_string());
        };
        if let Some(error) = self
            .watcher_failure
            .lock()
            .map_err(|_| "ReadDirectoryChangesW failure state is poisoned".to_string())?
            .take()
        {
            return Err(error);
        }
        if self.notification_overflow.swap(false, Ordering::SeqCst) {
            // The producer channel is deliberately bounded. Once it overflows,
            // discard its finite stale backlog and let the authoritative sparse
            // scan reconstruct state across later cooperative passes.
            for _ in 0..DIRECTORY_CHANGE_CHANNEL_CAPACITY {
                match receiver.try_recv() {
                    Ok(_) => {}
                    Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => break,
                }
            }
            return Ok(CloudFilesCapturedChangeBatch {
                changes: Vec::new(),
                sparse_placeholders: Vec::new(),
                overflowed: true,
            });
        }
        // `WindowsWorkerWake` deliberately coalesces repeated local-change
        // signals into one bit. Consume the complete bounded receiver backlog
        // for that edge: reading only one batch can strand a later batch
        // forever when several ReadDirectoryChangesW completions arrive before
        // the worker takes the bit and no subsequent filesystem event occurs.
        let mut raw = drain_directory_change_batches(receiver)?;
        if raw.overflowed {
            return Ok(CloudFilesCapturedChangeBatch {
                changes: Vec::new(),
                sparse_placeholders: Vec::new(),
                overflowed: true,
            });
        }
        let incoming_paths = raw
            .changes
            .iter()
            .flat_map(raw_change_paths)
            .cloned()
            .collect::<BTreeSet<_>>();
        for path in incoming_paths {
            self.capture_retries.clear(&path);
        }
        let now = self.capture_retry_epoch.elapsed();
        let due_budget = DIRECTORY_CHANGES_PER_PASS.saturating_sub(raw.changes.len());
        let due = self.capture_retries.due_bounded(now, due_budget);
        for path in &due {
            raw.changes
                .push(CloudFilesDirectoryChange::Modified(path.clone()));
        }
        let mut suppressed = self
            .suppressed_paths
            .lock()
            .map_err(|_| "Cloud Files local suppression ledger is poisoned".to_string())?;
        raw.changes.retain(|change| {
            let mut matched = false;
            for path in raw_change_paths(change) {
                if suppressed.remove(path) {
                    matched |= suppression_matches_current(&self.sync_root, path);
                }
            }
            !matched
        });
        drop(suppressed);
        let changes = match plan_directory_changes(raw.changes) {
            CloudFilesDirectoryChangePlan::Ordered(changes) => changes,
            CloudFilesDirectoryChangePlan::FullScanRequired => {
                return Ok(CloudFilesCapturedChangeBatch {
                    changes: Vec::new(),
                    sparse_placeholders: Vec::new(),
                    overflowed: true,
                });
            }
        };
        let captured = capture_raw_changes(
            &self.sync_root,
            &self.root_path,
            manifest,
            changes,
            &self.content_stager,
            cancelled,
        )?;
        let deferred = captured
            .deferred_upserts
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        for path in due {
            if !deferred.contains(&path) {
                self.capture_retries.clear(&path);
            }
        }
        for path in captured.deferred_upserts {
            self.capture_retries.schedule(path, now);
        }
        if self.capture_retries.len() > DEFERRED_CAPTURE_RETRIES_MAX {
            return Ok(CloudFilesCapturedChangeBatch {
                changes: Vec::new(),
                sparse_placeholders: Vec::new(),
                overflowed: true,
            });
        }
        Ok(CloudFilesCapturedChangeBatch {
            changes: captured.changes,
            sparse_placeholders: captured.sparse_placeholders,
            overflowed: false,
        })
    }

    fn next_capture_retry_in(&self) -> Option<Duration> {
        self.capture_retries
            .next_due_in(self.capture_retry_epoch.elapsed())
    }
}

fn drain_directory_change_batches(
    receiver: &mpsc::Receiver<RawDirectoryChangeBatch>,
) -> Result<RawDirectoryChangeBatch, String> {
    let mut combined = RawDirectoryChangeBatch::default();
    loop {
        match receiver.try_recv() {
            Ok(mut batch) => {
                if batch.overflowed
                    || combined.changes.len().saturating_add(batch.changes.len())
                        > DIRECTORY_CHANGES_PER_PASS
                {
                    // The authoritative sparse scan will reconstruct the
                    // current namespace. Discard the finite stale backlog so
                    // it cannot survive the same coalesced wake and replay
                    // observations from before that scan.
                    for _ in 0..DIRECTORY_CHANGE_CHANNEL_CAPACITY {
                        match receiver.try_recv() {
                            Ok(_) => {}
                            Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => {
                                break;
                            }
                        }
                    }
                    return Ok(RawDirectoryChangeBatch {
                        changes: Vec::new(),
                        overflowed: true,
                    });
                }
                combined.changes.append(&mut batch.changes);
            }
            Err(mpsc::TryRecvError::Empty) => return Ok(combined),
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err("ReadDirectoryChangesW worker stopped unexpectedly".to_string());
            }
        }
    }
}

fn raw_change_paths(change: &CloudFilesDirectoryChange) -> Vec<&PathBuf> {
    match change {
        CloudFilesDirectoryChange::Added(path)
        | CloudFilesDirectoryChange::Modified(path)
        | CloudFilesDirectoryChange::Removed(path) => vec![path],
        CloudFilesDirectoryChange::Renamed { from, to } => vec![from, to],
    }
}

fn suppression_matches_current(sync_root: &Path, relative: &Path) -> bool {
    let local = sync_root.join(relative);
    match fs::symlink_metadata(&local) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    }
    query_placeholder_state(&local)
        .ok()
        .flatten()
        .is_some_and(|(in_sync, _)| in_sync)
}

fn watch_directory_changes(
    sync_root: PathBuf,
    stop: Arc<AtomicBool>,
    stop_event: Arc<OwnedCrossThreadEvent>,
    raw_handle: Arc<AtomicIsize>,
    sender: mpsc::SyncSender<RawDirectoryChangeBatch>,
    ready: mpsc::SyncSender<Result<u64, String>>,
    notification_overflow: Arc<AtomicBool>,
    notification_epoch: Arc<AtomicU64>,
    watcher_failure: Arc<Mutex<Option<String>>>,
    sentinel: mpsc::Receiver<CursorRecoverySentinelCommand>,
    cursor_recovery: bool,
    worker_wake: Arc<WindowsWorkerWake>,
) {
    let root_wide = wide_path(&sync_root);
    let handle = match unsafe {
        CreateFileW(
            PCWSTR(root_wide.as_ptr()),
            FILE_LIST_DIRECTORY.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
            None,
        )
    } {
        Ok(handle) => handle,
        Err(error) => {
            let detail = format!(
                "open {} for ReadDirectoryChangesW: {error}",
                sync_root.display()
            );
            let _ = ready.send(Err(detail.clone()));
            if let Ok(mut failure) = watcher_failure.lock() {
                *failure = Some(detail);
            }
            worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
            return;
        }
    };
    raw_handle.store(handle.0 as isize, Ordering::SeqCst);
    let owned = OwnedWin32Handle(handle);
    let sync_root_file_id = match file_id_for_handle(owned.0, &sync_root) {
        Ok(file_id) => file_id,
        Err(detail) => {
            let _ = ready.send(Err(detail.clone()));
            if let Ok(mut failure) = watcher_failure.lock() {
                *failure = Some(detail);
            }
            worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
            raw_handle.store(0, Ordering::SeqCst);
            return;
        }
    };
    let event = match unsafe { CreateEventW(None, false, false, None) } {
        Ok(event) => OwnedWin32Handle(event),
        Err(error) => {
            let detail = format!(
                "create overlapped ReadDirectoryChangesW event for {}: {error}",
                sync_root.display()
            );
            let _ = ready.send(Err(detail.clone()));
            if let Ok(mut failure) = watcher_failure.lock() {
                *failure = Some(detail);
            }
            worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
            raw_handle.store(0, Ordering::SeqCst);
            return;
        }
    };
    let mut buffer = vec![0u8; DIRECTORY_CHANGE_BUFFER_BYTES];
    let mut rename_pairing = CloudFilesRenamePairingState::default();
    let mut active_sentinel: Option<ActiveCursorRecoverySentinel> = None;
    let mut first_request = true;
    while !stop.load(Ordering::SeqCst) {
        let mut overlapped = OVERLAPPED {
            hEvent: event.0,
            ..OVERLAPPED::default()
        };
        let issued = unsafe {
            ReadDirectoryChangesW(
                owned.0,
                buffer.as_mut_ptr().cast::<c_void>(),
                buffer.len() as u32,
                true,
                if cursor_recovery {
                    FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME
                } else {
                    FILE_NOTIFY_CHANGE_FILE_NAME
                        | FILE_NOTIFY_CHANGE_DIR_NAME
                        | FILE_NOTIFY_CHANGE_SIZE
                        | FILE_NOTIFY_CHANGE_LAST_WRITE
                        | FILE_NOTIFY_CHANGE_CREATION
                },
                None,
                Some(&mut overlapped),
                None,
            )
        };
        if let Err(error) = issued {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            notification_epoch.fetch_add(1, Ordering::SeqCst);
            let detail = format!("start overlapped ReadDirectoryChangesW: {error}");
            if first_request {
                let _ = ready.send(Err(detail.clone()));
            }
            if let Ok(mut failure) = watcher_failure.lock() {
                *failure = Some(detail);
            }
            worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
            break;
        }
        if first_request {
            // The first request is now owned by the kernel. A successful arm
            // acknowledgement therefore has no open-to-observe gap.
            let _ = ready.send(Ok(sync_root_file_id));
            first_request = false;
        }
        let mut bytes_returned = 0u32;
        loop {
            if active_sentinel.is_none() {
                match sentinel.try_recv() {
                    Ok(command) if !cursor_recovery => {
                        let _ = command.reply.send(Err(
                            "cursor recovery sentinel requested on an ordinary watcher".to_string(),
                        ));
                    }
                    Ok(command) => match start_cursor_recovery_sentinel_command(
                        &sync_root,
                        sync_root_file_id,
                        command,
                    ) {
                        Ok(active) => active_sentinel = Some(active),
                        Err(error) => {
                            let _ = error.0.reply.send(Err(error.1));
                        }
                    },
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) => {}
                }
            } else if let Ok(command) = sentinel.try_recv() {
                let _ = command.reply.send(Err(
                    "cursor recovery sentinel fence is already in progress".to_string(),
                ));
            }
            if active_sentinel
                .as_ref()
                .is_some_and(|active| active.needs_create)
            {
                let create = {
                    let active = active_sentinel
                        .as_ref()
                        .expect("active cursor recovery sentinel checked above");
                    create_cursor_recovery_sentinel(
                        &sync_root,
                        &active.command.name,
                        &active.command.payload,
                    )
                };
                match create {
                    Ok(handle) => {
                        let active = active_sentinel
                            .as_mut()
                            .expect("active cursor recovery sentinel checked above");
                        active.handle = Some(handle);
                        active.needs_create = false;
                    }
                    Err(detail) => {
                        fail_active_cursor_recovery_sentinel(&mut active_sentinel, detail);
                    }
                }
            }
            let timeout = if cursor_recovery { 50 } else { INFINITE };
            let completion =
                unsafe { WaitForMultipleObjects(&[event.0, stop_event.0.0], false, timeout) };
            if completion == WAIT_OBJECT_0 {
                break;
            }
            if completion.0 == WAIT_OBJECT_0.0 + 1 {
                stop.store(true, Ordering::SeqCst);
                unsafe {
                    let _ = CancelIoEx(owned.0, Some(&overlapped));
                }
                break;
            }
            if completion == WAIT_FAILED {
                let detail = format!(
                    "wait for overlapped ReadDirectoryChangesW on {} failed",
                    sync_root.display()
                );
                fail_active_cursor_recovery_sentinel(&mut active_sentinel, detail.clone());
                if let Ok(mut failure) = watcher_failure.lock() {
                    *failure = Some(detail);
                }
                worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
                stop.store(true, Ordering::SeqCst);
                unsafe {
                    let _ = CancelIoEx(owned.0, Some(&overlapped));
                }
                break;
            }
            if completion != WAIT_TIMEOUT {
                let detail = format!(
                    "unexpected ReadDirectoryChangesW wait result {}",
                    completion.0
                );
                fail_active_cursor_recovery_sentinel(&mut active_sentinel, detail.clone());
                if let Ok(mut failure) = watcher_failure.lock() {
                    *failure = Some(detail);
                }
                worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
                stop.store(true, Ordering::SeqCst);
                unsafe {
                    let _ = CancelIoEx(owned.0, Some(&overlapped));
                }
                break;
            }
            if stop.load(Ordering::SeqCst) {
                unsafe {
                    let _ = CancelIoEx(owned.0, Some(&overlapped));
                }
                break;
            }
        }
        if stop.load(Ordering::SeqCst) {
            let _ = unsafe { GetOverlappedResult(owned.0, &overlapped, &mut bytes_returned, true) };
            break;
        }
        if let Err(error) =
            unsafe { GetOverlappedResult(owned.0, &overlapped, &mut bytes_returned, true) }
        {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            notification_epoch.fetch_add(1, Ordering::SeqCst);
            let detail = format!("complete overlapped ReadDirectoryChangesW: {error}");
            fail_active_cursor_recovery_sentinel(&mut active_sentinel, detail.clone());
            if let Ok(mut failure) = watcher_failure.lock() {
                *failure = Some(detail);
            }
            worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
            break;
        }
        if bytes_returned == 0 {
            notification_epoch.fetch_add(1, Ordering::SeqCst);
            rename_pairing.reset_for_overflow();
            notification_overflow.store(true, Ordering::SeqCst);
            worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
            fail_active_cursor_recovery_sentinel(
                &mut active_sentinel,
                "ReadDirectoryChangesW overflowed during cursor recovery fence".to_string(),
            );
            continue;
        }
        match parse_directory_changes(&buffer[..bytes_returned as usize], &mut rename_pairing) {
            Ok(batch) => {
                match process_cursor_recovery_sentinel_batch(
                    batch,
                    &mut active_sentinel,
                    &sync_root,
                    sync_root_file_id,
                ) {
                    Ok((batch, ack)) => {
                        if !batch.changes.is_empty() || batch.overflowed {
                            notification_epoch.fetch_add(1, Ordering::SeqCst);
                            match sender.try_send(batch) {
                                Ok(()) => worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE),
                                Err(mpsc::TrySendError::Full(_)) => {
                                    rename_pairing.reset_for_overflow();
                                    notification_overflow.store(true, Ordering::SeqCst);
                                    worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
                                }
                                Err(mpsc::TrySendError::Disconnected(_)) => break,
                            }
                        }
                        if let Some(reply) = ack {
                            let _ = reply.send(Ok(CursorRecoverySentinelAck {
                                non_sentinel_epoch: notification_epoch.load(Ordering::SeqCst),
                                sync_root_file_id,
                            }));
                        }
                    }
                    Err(error) => {
                        notification_epoch.fetch_add(1, Ordering::SeqCst);
                        if let Ok(mut failure) = watcher_failure.lock() {
                            *failure = Some(error);
                        }
                        worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
                        break;
                    }
                }
            }
            Err(error) => {
                notification_epoch.fetch_add(1, Ordering::SeqCst);
                fail_active_cursor_recovery_sentinel(&mut active_sentinel, error.clone());
                if let Ok(mut failure) = watcher_failure.lock() {
                    *failure = Some(error);
                }
                worker_wake.signal(WORKER_WAKE_LOCAL_CHANGE);
                break;
            }
        }
    }
    fail_active_cursor_recovery_sentinel(
        &mut active_sentinel,
        "ReadDirectoryChangesW recovery observer stopped during sentinel fence".to_string(),
    );
    while let Ok(command) = sentinel.try_recv() {
        let _ = command.reply.send(Err(
            "ReadDirectoryChangesW recovery observer stopped".to_string()
        ));
    }
    raw_handle.store(0, Ordering::SeqCst);
}

fn file_id_for_handle(handle: HANDLE, path: &Path) -> Result<u64, String> {
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle, &mut info) }
        .map_err(|error| format!("read stable file ID for {}: {error}", path.display()))?;
    Ok(((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64)
}

fn recover_stale_cursor_recovery_sentinel(
    sync_root: &Path,
    expected_sync_root_file_id: u64,
    specs: &[CursorRecoverySentinelSpec],
) -> Result<Option<PathBuf>, String> {
    if specs.is_empty() {
        return Ok(None);
    }
    let entries = physical_directory_entries(sync_root)?;
    let Some(spec_index) = select_stale_cursor_recovery_sentinel(&entries, specs)? else {
        return Ok(None);
    };
    let spec = &specs[spec_index];
    let root_wide = wide_path(sync_root);
    let root_guard = unsafe {
        CreateFileW(
            PCWSTR(root_wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| {
        format!(
            "open pinned cursor recovery root for stale-sentinel cleanup {}: {error}",
            sync_root.display()
        )
    })?;
    let root_file_id = file_id_for_handle(root_guard.0, sync_root)?;
    if root_file_id != expected_sync_root_file_id {
        return Err(format!(
            "stale sentinel root file ID {root_file_id} does not match pinned root file ID {expected_sync_root_file_id}"
        ));
    }

    let path = sync_root.join(&spec.name);
    let path_wide = wide_path(&path);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(path_wide.as_ptr()),
            GENERIC_READ.0 | GENERIC_WRITE.0 | DELETE.0,
            FILE_SHARE_MODE(0),
            None,
            OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| {
        format!(
            "open stale cursor recovery sentinel {}: {error}",
            path.display()
        )
    })?;
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle.0, &mut info) }.map_err(|error| {
        format!(
            "inspect stale cursor recovery sentinel {}: {error}",
            path.display()
        )
    })?;
    let logical_size = ((info.nFileSizeHigh as u64) << 32) | info.nFileSizeLow as u64;
    validate_stale_cursor_recovery_sentinel_metadata(
        info.dwFileAttributes,
        logical_size,
        spec.payload.len(),
    )
    .map_err(|detail| format!("{}: {detail}", path.display()))?;
    let mut observed = vec![0u8; logical_size as usize];
    let mut read = 0u32;
    unsafe { ReadFile(handle.0, Some(&mut observed), Some(&mut read), None) }.map_err(|error| {
        format!(
            "read stale cursor recovery sentinel {}: {error}",
            path.display()
        )
    })?;
    if read as usize != observed.len() {
        return Err(format!(
            "stale cursor recovery sentinel {} returned a short payload",
            path.display()
        ));
    }
    validate_stale_cursor_recovery_sentinel(
        info.dwFileAttributes,
        logical_size,
        &observed,
        &spec.payload,
    )
    .map_err(|detail| format!("{}: {detail}", path.display()))?;

    unsafe { FlushFileBuffers(handle.0) }.map_err(|error| {
        format!(
            "flush stale cursor recovery sentinel {}: {error}",
            path.display()
        )
    })?;
    let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
    unsafe {
        SetFileInformationByHandle(
            handle.0,
            FileDispositionInfo,
            (&disposition as *const FILE_DISPOSITION_INFO).cast::<c_void>(),
            size_of::<FILE_DISPOSITION_INFO>() as u32,
        )
    }
    .map_err(|error| {
        format!(
            "mark stale cursor recovery sentinel {} for same-object deletion: {error}",
            path.display()
        )
    })?;
    unsafe { FlushFileBuffers(handle.0) }.map_err(|error| {
        format!(
            "flush stale cursor recovery sentinel deletion {}: {error}",
            path.display()
        )
    })?;
    drop(handle);
    // Directory handles do not support FlushFileBuffers. Replay safety does
    // not require a durable directory flush: after a crash the marker-bound
    // file is either absent or is revalidated and removed again. Retaining the
    // no-share-delete root handle above pins its identity through this cleanup.
    if select_stale_cursor_recovery_sentinel(&physical_directory_entries(sync_root)?, specs)?
        .is_some()
    {
        return Err("cursor recovery sentinel reappeared after exact cleanup".to_string());
    }
    Ok(Some(PathBuf::from(&spec.name)))
}

fn start_cursor_recovery_sentinel_command(
    sync_root: &Path,
    sync_root_file_id: u64,
    command: CursorRecoverySentinelCommand,
) -> Result<ActiveCursorRecoverySentinel, (CursorRecoverySentinelCommand, String)> {
    let stale_removal = match recover_stale_cursor_recovery_sentinel(
        sync_root,
        sync_root_file_id,
        &command.stale_specs,
    ) {
        Ok(stale) => stale,
        Err(detail) => return Err((command, detail)),
    };
    let handle = if stale_removal.is_none() {
        match create_cursor_recovery_sentinel(sync_root, &command.name, &command.payload) {
            Ok(handle) => Some(handle),
            Err(detail) => return Err((command, detail)),
        }
    } else {
        None
    };
    Ok(ActiveCursorRecoverySentinel {
        state: CursorRecoverySentinelState::new(&command.name),
        command,
        handle,
        stale_removal,
        needs_create: false,
    })
}

fn create_cursor_recovery_sentinel(
    sync_root: &Path,
    name: &str,
    payload: &[u8],
) -> Result<OwnedWin32Handle, String> {
    if Path::new(name).components().count() != 1 || payload.is_empty() {
        return Err("cursor recovery sentinel name or payload is invalid".to_string());
    }
    let path = sync_root.join(name);
    let wide = wide_path(&path);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            GENERIC_READ.0 | GENERIC_WRITE.0 | DELETE.0,
            FILE_SHARE_MODE(0),
            None,
            CREATE_NEW,
            FILE_ATTRIBUTE_HIDDEN | FILE_FLAG_DELETE_ON_CLOSE | FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| {
        format!(
            "create cursor recovery sentinel {}: {error}",
            path.display()
        )
    })?;
    let mut written = 0u32;
    unsafe { WriteFile(handle.0, Some(payload), Some(&mut written), None) }
        .map_err(|error| format!("write cursor recovery sentinel {}: {error}", path.display()))?;
    if written as usize != payload.len() {
        return Err(format!(
            "cursor recovery sentinel {} wrote {written} of {} bytes",
            path.display(),
            payload.len()
        ));
    }
    unsafe { FlushFileBuffers(handle.0) }
        .map_err(|error| format!("flush cursor recovery sentinel {}: {error}", path.display()))?;
    unsafe { SetFilePointerEx(handle.0, 0, None, FILE_BEGIN) }.map_err(|error| {
        format!(
            "rewind cursor recovery sentinel {}: {error}",
            path.display()
        )
    })?;
    let mut observed = vec![0u8; payload.len()];
    let mut read = 0u32;
    unsafe { ReadFile(handle.0, Some(&mut observed), Some(&mut read), None) }
        .map_err(|error| format!("read cursor recovery sentinel {}: {error}", path.display()))?;
    if read as usize != payload.len() || observed != payload {
        return Err(format!(
            "cursor recovery sentinel {} payload verification failed",
            path.display()
        ));
    }
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle.0, &mut info) }.map_err(|error| {
        format!(
            "read cursor recovery sentinel metadata {}: {error}",
            path.display()
        )
    })?;
    if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0
        || info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
        || ((info.nFileSizeHigh as u64) << 32) | info.nFileSizeLow as u64 != payload.len() as u64
    {
        return Err(format!(
            "cursor recovery sentinel {} is not the exact payload-bound regular file",
            path.display()
        ));
    }
    Ok(handle)
}

fn fail_active_cursor_recovery_sentinel(
    active: &mut Option<ActiveCursorRecoverySentinel>,
    detail: String,
) {
    if let Some(active) = active.take() {
        let _ = active.command.reply.send(Err(detail));
    }
}

fn process_cursor_recovery_sentinel_batch(
    batch: RawDirectoryChangeBatch,
    active: &mut Option<ActiveCursorRecoverySentinel>,
    sync_root: &Path,
    sync_root_file_id: u64,
) -> Result<
    (
        RawDirectoryChangeBatch,
        Option<mpsc::SyncSender<Result<CursorRecoverySentinelAck, String>>>,
    ),
    String,
> {
    let Some(sentinel) = active.as_mut() else {
        return Ok((batch, None));
    };
    if let Some(stale_name) = sentinel.stale_removal.as_ref() {
        let outcome = classify_stale_cursor_recovery_removal_batch(batch, stale_name)?;
        if outcome.removed {
            sentinel.stale_removal = None;
            sentinel.needs_create = true;
        }
        return Ok((outcome.preserved, None));
    }
    let outcome = classify_cursor_recovery_sentinel_batch(batch, &mut sentinel.state)?;
    if outcome.added {
        // Closing the DELETE_ON_CLOSE handle creates the ordered removal only
        // after the exact Added record was consumed. ReadDirectoryChangesW's
        // handle-associated kernel buffer records changes between calls.
        sentinel.handle.take();
    }
    let preserved = outcome.preserved;
    let complete = outcome.complete;
    if !complete {
        return Ok((preserved, None));
    }
    let completed = active
        .take()
        .ok_or_else(|| "cursor recovery sentinel completion lost its command".to_string())?;
    let sentinel_path = sync_root.join(&completed.command.name);
    match fs::symlink_metadata(&sentinel_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(_) => {
            return Err(format!(
                "cursor recovery sentinel {} remained after its removal record",
                sentinel_path.display()
            ));
        }
        Err(error) => {
            return Err(format!(
                "verify removed cursor recovery sentinel {}: {error}",
                sentinel_path.display()
            ));
        }
    }
    if sync_root_file_id == 0 {
        return Err("cursor recovery watcher root has an invalid zero file ID".to_string());
    }
    Ok((preserved, Some(completed.command.reply)))
}

struct StaleCursorRecoveryRemovalBatchOutcome {
    preserved: RawDirectoryChangeBatch,
    removed: bool,
}

fn classify_stale_cursor_recovery_removal_batch(
    batch: RawDirectoryChangeBatch,
    stale_name: &Path,
) -> Result<StaleCursorRecoveryRemovalBatchOutcome, String> {
    let mut outcome = StaleCursorRecoveryRemovalBatchOutcome {
        preserved: RawDirectoryChangeBatch {
            changes: Vec::new(),
            overflowed: batch.overflowed,
        },
        removed: false,
    };
    for change in batch.changes {
        let touches_stale = raw_change_paths(&change)
            .into_iter()
            .any(|path| path == stale_name);
        if !touches_stale {
            outcome.preserved.changes.push(change);
            continue;
        }
        if matches!(&change, CloudFilesDirectoryChange::Removed(path) if path == stale_name)
            && !outcome.removed
        {
            outcome.removed = true;
            continue;
        }
        return Err(
            "stale cursor recovery sentinel emitted an unexpected namespace record".to_string(),
        );
    }
    Ok(outcome)
}

struct CursorRecoverySentinelBatchOutcome {
    preserved: RawDirectoryChangeBatch,
    added: bool,
    complete: bool,
}

fn classify_cursor_recovery_sentinel_batch(
    batch: RawDirectoryChangeBatch,
    state: &mut CursorRecoverySentinelState,
) -> Result<CursorRecoverySentinelBatchOutcome, String> {
    let mut outcome = CursorRecoverySentinelBatchOutcome {
        preserved: RawDirectoryChangeBatch {
            changes: Vec::new(),
            overflowed: batch.overflowed,
        },
        added: false,
        complete: false,
    };
    for change in batch.changes {
        match state.observe(&change)? {
            SentinelObservation::Added => outcome.added = true,
            SentinelObservation::Removed => outcome.complete = true,
            SentinelObservation::SentinelModified => {}
            SentinelObservation::NonSentinel => outcome.preserved.changes.push(change),
        }
    }
    Ok(outcome)
}

fn parse_directory_changes(
    buffer: &[u8],
    rename_pairing: &mut CloudFilesRenamePairingState,
) -> Result<RawDirectoryChangeBatch, String> {
    let mut changes = Vec::new();
    let mut offset = 0usize;
    const HEADER_BYTES: usize = 12;
    loop {
        if buffer.len().saturating_sub(offset) < HEADER_BYTES {
            return Err("ReadDirectoryChangesW returned a truncated record header".to_string());
        }
        let next_entry_offset = u32::from_le_bytes(
            buffer[offset..offset + 4]
                .try_into()
                .map_err(|_| "invalid directory-change offset".to_string())?,
        );
        let action = u32::from_le_bytes(
            buffer[offset + 4..offset + 8]
                .try_into()
                .map_err(|_| "invalid directory-change action".to_string())?,
        );
        let name_bytes = u32::from_le_bytes(
            buffer[offset + 8..offset + 12]
                .try_into()
                .map_err(|_| "invalid directory-change name length".to_string())?,
        ) as usize;
        let name_offset = offset + HEADER_BYTES;
        if !name_bytes.is_multiple_of(2) || name_offset.saturating_add(name_bytes) > buffer.len() {
            return Err("ReadDirectoryChangesW returned a truncated UTF-16 filename".to_string());
        }
        let name = buffer[name_offset..name_offset + name_bytes]
            .chunks_exact(2)
            .map(|unit| u16::from_le_bytes([unit[0], unit[1]]))
            .collect::<Vec<_>>();
        let path = PathBuf::from(String::from_utf16(&name).map_err(|_| {
            "ReadDirectoryChangesW returned an invalid UTF-16 filename".to_string()
        })?);
        let action = match action {
            value if value == FILE_ACTION_ADDED.0 => CloudFilesDirectoryChangeAction::Added,
            value if value == FILE_ACTION_MODIFIED.0 => CloudFilesDirectoryChangeAction::Modified,
            value if value == FILE_ACTION_REMOVED.0 => CloudFilesDirectoryChangeAction::Removed,
            value if value == FILE_ACTION_RENAMED_OLD_NAME.0 => {
                CloudFilesDirectoryChangeAction::RenamedOldName
            }
            value if value == FILE_ACTION_RENAMED_NEW_NAME.0 => {
                CloudFilesDirectoryChangeAction::RenamedNewName
            }
            _ => {
                return Err(format!(
                    "ReadDirectoryChangesW returned unknown action {action}"
                ));
            }
        };
        rename_pairing.push(action, path, &mut changes);
        if next_entry_offset == 0 {
            break;
        }
        let next = offset.saturating_add(next_entry_offset as usize);
        if next <= offset || next >= buffer.len() {
            return Err("ReadDirectoryChangesW returned an invalid next-record offset".to_string());
        }
        offset = next;
    }
    Ok(RawDirectoryChangeBatch {
        changes,
        overflowed: false,
    })
}

#[derive(Clone, Debug)]
struct LocalPlaceholderState {
    relative_path: PathBuf,
    sifs_path: String,
    is_directory: bool,
    in_sync: bool,
    identity: Option<CloudFilesPlaceholderIdentity>,
}

#[derive(Default)]
struct LocalStartupScan {
    captures: Vec<CloudFilesCapturedLocalChange>,
    conflicts: Vec<CloudFilesStartupConflictEvidence>,
    deferred_upserts: Vec<PathBuf>,
    sparse_placeholders: Vec<CloudFilesSparsePlaceholderObservation>,
}

enum LocalStartupScanPhase {
    Enumerate,
    Classify,
    Missing,
    Complete,
}

struct WindowsLocalStartupScanPlan {
    pending_directories: VecDeque<PathBuf>,
    active_directory: Option<fs::ReadDir>,
    local: BTreeMap<String, LocalPlaceholderState>,
    blocked_paths: BTreeSet<String>,
    phase_paths: VecDeque<String>,
    renamed_from: BTreeSet<String>,
    phase: LocalStartupScanPhase,
}

impl WindowsLocalStartupScanPlan {
    fn new(sync_root: &Path) -> Self {
        let pending_directories = if sync_root.exists() {
            VecDeque::from([sync_root.to_path_buf()])
        } else {
            VecDeque::new()
        };
        Self {
            pending_directories,
            active_directory: None,
            local: BTreeMap::new(),
            blocked_paths: BTreeSet::new(),
            phase_paths: VecDeque::new(),
            renamed_from: BTreeSet::new(),
            phase: LocalStartupScanPhase::Enumerate,
        }
    }

    fn next_batch(
        &mut self,
        sync_root: &Path,
        root_path: &str,
        previous_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
        content_stager: &CloudFilesContentStager,
        cancelled: &AtomicBool,
    ) -> Result<(LocalStartupScan, bool), String> {
        let mut output = LocalStartupScan::default();
        let mut work = 0usize;
        while work < LOCAL_STARTUP_SCAN_WORK_PER_PASS {
            if cancelled.load(Ordering::SeqCst) {
                return Err("sync-cancelled".to_string());
            }
            match self.phase {
                LocalStartupScanPhase::Enumerate => {
                    if self.active_directory.is_none() {
                        let Some(directory) = self.pending_directories.pop_front() else {
                            self.phase_paths = self.local.keys().cloned().collect();
                            self.phase = LocalStartupScanPhase::Classify;
                            continue;
                        };
                        self.active_directory =
                            Some(fs::read_dir(&directory).map_err(|error| {
                                format!("scan local directory {}: {error}", directory.display())
                            })?);
                        work += 1;
                        continue;
                    }
                    let next = self
                        .active_directory
                        .as_mut()
                        .expect("active local directory")
                        .next();
                    let Some(entry) = next else {
                        self.active_directory = None;
                        continue;
                    };
                    work += 1;
                    let entry =
                        entry.map_err(|error| format!("read local directory entry: {error}"))?;
                    // `std::fs::ReadDir` can retain the trailing separator from
                    // the Win32 enumeration root when it reconstructs an entry
                    // path. Cloud Files rejects such paths (for example,
                    // `D:\\sinomo\\\\users`) even though ordinary NTFS APIs
                    // commonly accept them. Rebuild the physical path from its
                    // components before it crosses any Cloud Files boundary.
                    let path = normalize_enumerated_local_path(&entry.path())?;
                    let relative = path
                        .strip_prefix(sync_root)
                        .map_err(|_| format!("{} escaped the sync root", path.display()))?
                        .to_path_buf();
                    let sifs_path = local_relative_to_sifs(root_path, &relative)?;
                    // Windows includes the entry metadata in the directory
                    // enumeration. Reopening a newly projected Cloud Files
                    // placeholder with `symlink_metadata` asks the filter for
                    // reparse-point metadata and can fail with
                    // ERROR_CLOUD_FILE_INVALID_REQUEST. Use the metadata from
                    // the same enumeration cut instead, which also avoids a
                    // path-based TOCTOU between discovery and classification.
                    let metadata = entry.metadata().map_err(|error| {
                        format!("read enumerated metadata for {}: {error}", path.display())
                    })?;
                    let is_directory = metadata.is_dir();
                    let placeholder = query_placeholder_state(&path)?;
                    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
                        && placeholder.is_none()
                    {
                        self.blocked_paths.insert(sifs_path.clone());
                        output.conflicts.push(startup_conflict(
                            &sifs_path,
                            format!(
                                "local reparse point {} has no safe SIFS projection; synchronization is blocked for this path",
                                path.display()
                            ),
                        ));
                        self.local.insert(
                            sifs_path.clone(),
                            LocalPlaceholderState {
                                relative_path: relative,
                                sifs_path,
                                is_directory: false,
                                in_sync: false,
                                identity: None,
                            },
                        );
                        continue;
                    }
                    if is_directory {
                        self.pending_directories.push_back(path.clone());
                    }
                    self.local.insert(
                        sifs_path.clone(),
                        LocalPlaceholderState {
                            relative_path: relative,
                            sifs_path,
                            is_directory,
                            in_sync: placeholder.as_ref().is_some_and(|value| value.0),
                            identity: placeholder.and_then(|value| value.1),
                        },
                    );
                }
                LocalStartupScanPhase::Classify => {
                    let Some(path) = self.phase_paths.pop_front() else {
                        self.phase_paths = previous_manifest.keys().cloned().collect();
                        self.phase = LocalStartupScanPhase::Missing;
                        continue;
                    };
                    work += 1;
                    if self.blocked_paths.contains(&path) {
                        continue;
                    }
                    let entry = self
                        .local
                        .get(&path)
                        .expect("classification path came from local map")
                        .clone();
                    let context = LocalStartupClassificationContext {
                        sync_root,
                        root_path,
                        previous_manifest,
                        remote_manifest,
                        content_stager,
                        cancelled,
                    };
                    let classified =
                        classify_local_divergence(&context, &BTreeMap::from([(path, entry)]))?;
                    for capture in &classified.captures {
                        if capture.kind == CloudFilesLocalOperationKind::Rename {
                            self.renamed_from.insert(capture.source_path.clone());
                        }
                    }
                    output.captures.extend(classified.captures);
                    output.conflicts.extend(classified.conflicts);
                    output.deferred_upserts.extend(classified.deferred_upserts);
                    output
                        .sparse_placeholders
                        .extend(classified.sparse_placeholders);
                    if !output.sparse_placeholders.is_empty() {
                        // Identity recovery is a durable worker-side boundary.
                        // Do not classify later paths against the stale sparse
                        // manifest before that boundary commits.
                        break;
                    }
                }
                LocalStartupScanPhase::Missing => {
                    let Some(path) = self.phase_paths.pop_front() else {
                        self.phase = LocalStartupScanPhase::Complete;
                        continue;
                    };
                    work += 1;
                    if self.renamed_from.contains(&path) || self.local.contains_key(&path) {
                        continue;
                    }
                    let previous = previous_manifest
                        .get(&path)
                        .expect("missing-audit path came from prior manifest");
                    let Some(remote) = remote_manifest.get(&path) else {
                        continue;
                    };
                    if !manifest_generations_match(previous, remote) {
                        output.conflicts.push(startup_conflict(
                            &path,
                            "local absence races a remote generation change; delete is blocked",
                        ));
                    } else {
                        output
                            .captures
                            .push(delete_capture("startup-delete", &path, previous));
                    }
                }
                LocalStartupScanPhase::Complete => break,
            }
        }
        Ok((
            output,
            matches!(self.phase, LocalStartupScanPhase::Complete),
        ))
    }
}

struct LocalStartupClassificationContext<'a> {
    sync_root: &'a Path,
    root_path: &'a str,
    previous_manifest: &'a BTreeMap<String, CloudFilesReconciledManifestEntry>,
    remote_manifest: &'a BTreeMap<String, CloudFilesReconciledManifestEntry>,
    content_stager: &'a CloudFilesContentStager,
    cancelled: &'a AtomicBool,
}

fn classify_local_divergence(
    context: &LocalStartupClassificationContext<'_>,
    local: &BTreeMap<String, LocalPlaceholderState>,
) -> Result<LocalStartupScan, String> {
    let LocalStartupClassificationContext {
        sync_root,
        root_path,
        previous_manifest,
        remote_manifest,
        content_stager,
        cancelled,
    } = *context;
    let mut output = LocalStartupScan::default();
    let mut handled_local = BTreeSet::new();
    let mount_identity = sync_root.to_string_lossy();
    for entry in local.values() {
        let Some(identity) = entry.identity.as_ref() else {
            continue;
        };
        if identity.path == entry.sifs_path {
            continue;
        }
        if identity.mount != mount_identity || identity.root != root_path {
            output.conflicts.push(startup_conflict(
                &entry.sifs_path,
                format!(
                    "placeholder identity belongs to mount `{}` root `{}`, not this owned mount/root",
                    identity.mount, identity.root
                ),
            ));
            handled_local.insert(entry.sifs_path.clone());
            continue;
        }
        let Some(previous) = previous_manifest.get(&identity.path).or_else(|| {
            remote_manifest
                .get(&identity.path)
                .filter(|remote| identity_matches_manifest(identity, remote))
        }) else {
            output
                .sparse_placeholders
                .push(CloudFilesSparsePlaceholderObservation {
                    local_path: entry.sifs_path.clone(),
                    identity: identity.clone(),
                });
            handled_local.insert(entry.sifs_path.clone());
            continue;
        };
        if !identity_matches_manifest(identity, previous) {
            output.conflicts.push(startup_conflict(
                &entry.sifs_path,
                format!(
                    "renamed placeholder identity does not match durable source `{}`",
                    identity.path
                ),
            ));
            handled_local.insert(entry.sifs_path.clone());
            continue;
        }
        if !remote_manifest
            .get(&identity.path)
            .is_some_and(|remote| manifest_generations_match(previous, remote))
        {
            output.conflicts.push(startup_conflict(
                &entry.sifs_path,
                format!(
                    "local rename of `{}` races a remote source generation change",
                    identity.path
                ),
            ));
            handled_local.insert(entry.sifs_path.clone());
            continue;
        }
        let destination = match (
            previous_manifest.get(&entry.sifs_path),
            remote_manifest.get(&entry.sifs_path),
        ) {
            (None, None) => CloudFilesDestinationPrecondition::Absent,
            (Some(previous_destination), Some(remote_destination))
                if manifest_generations_match(previous_destination, remote_destination) =>
            {
                exact_destination_precondition(remote_destination)
            }
            _ => {
                output.conflicts.push(startup_conflict(
                    &entry.sifs_path,
                    "local rename destination races an untracked or remotely changed generation",
                ));
                handled_local.insert(entry.sifs_path.clone());
                continue;
            }
        };
        output.captures.push(CloudFilesCapturedLocalChange {
            capture_id: stable_capture_label(&[
                "startup-rename",
                &identity.path,
                &entry.sifs_path,
                &identity.object_id,
                &identity.version,
            ]),
            kind: CloudFilesLocalOperationKind::Rename,
            source_path: identity.path.clone(),
            destination_path: Some(entry.sifs_path.clone()),
            preconditions: CloudFilesLocalOperationPreconditions {
                source: Some(expected_state(previous)),
                destination,
            },
            captured_content: None,
        });
        handled_local.insert(entry.sifs_path.clone());
    }

    for entry in local.values() {
        if handled_local.contains(&entry.sifs_path) {
            continue;
        }
        if let Some(identity) = entry.identity.as_ref()
            && (identity.mount != mount_identity || identity.root != root_path)
        {
            output.conflicts.push(startup_conflict(
                &entry.sifs_path,
                format!(
                    "placeholder identity belongs to mount `{}` root `{}`, not this owned mount/root",
                    identity.mount, identity.root
                ),
            ));
            continue;
        }
        if let Some(identity) = entry.identity.as_ref()
            && previous_manifest.get(&entry.sifs_path).is_none()
            && !remote_manifest
                .get(&entry.sifs_path)
                .is_some_and(|remote| identity_matches_manifest(identity, remote))
        {
            output
                .sparse_placeholders
                .push(CloudFilesSparsePlaceholderObservation {
                    local_path: entry.sifs_path.clone(),
                    identity: identity.clone(),
                });
            continue;
        }
        if entry.in_sync
            && entry.identity.as_ref().is_some_and(|identity| {
                remote_manifest.get(&entry.sifs_path).is_some_and(|remote| {
                    identity.path == remote.path
                        && identity.object_id == remote.object_id
                        && identity.version == remote.version
                        && identity.content_hash == remote.content_hash
                })
            })
        {
            continue;
        }
        let recovered_remote = entry.identity.as_ref().and_then(|identity| {
            remote_manifest
                .get(&entry.sifs_path)
                .filter(|remote| identity_matches_manifest(identity, remote))
        });
        let Some(previous) = previous_manifest.get(&entry.sifs_path).or(recovered_remote) else {
            match plan_owned_offline_create(root_path, &entry.sifs_path, remote_manifest) {
                CloudFilesOfflineCreatePlan::QueueNow => {
                    match capture_local_upsert_attempt(
                        sync_root,
                        root_path,
                        &entry.relative_path,
                        &entry.sifs_path,
                        remote_manifest,
                        content_stager,
                        cancelled,
                    )? {
                        LocalUpsertCapture::Captured(capture) => output.captures.push(*capture),
                        LocalUpsertCapture::NoChange => {}
                        LocalUpsertCapture::Deferred => {
                            output.deferred_upserts.push(entry.relative_path.clone())
                        }
                        LocalUpsertCapture::NeedsMaterialization(identity) => {
                            output.sparse_placeholders.push(
                                CloudFilesSparsePlaceholderObservation {
                                    local_path: entry.sifs_path.clone(),
                                    identity,
                                },
                            );
                        }
                    }
                }
                CloudFilesOfflineCreatePlan::DeferUntilParentIsRemote => {
                    output.deferred_upserts.push(entry.relative_path.clone());
                }
                CloudFilesOfflineCreatePlan::ConflictWithRemote => {
                    output.conflicts.push(startup_conflict(
                        &entry.sifs_path,
                        "offline local create races an authoritative remote path; upload is blocked",
                    ));
                }
            }
            continue;
        };
        if entry.in_sync
            && entry
                .identity
                .as_ref()
                .is_some_and(|identity| identity_matches_manifest(identity, previous))
        {
            // The local placeholder is unchanged. A newer/removed remote
            // generation is applied by the full reconciliation below.
            continue;
        }
        let Some(remote) = remote_manifest.get(&entry.sifs_path) else {
            output.conflicts.push(startup_conflict(
                &entry.sifs_path,
                "local dirty state races a remote delete",
            ));
            continue;
        };
        if !manifest_generations_match(previous, remote) {
            output.conflicts.push(startup_conflict(
                &entry.sifs_path,
                "local dirty state races a remote generation change",
            ));
            continue;
        }
        let expected_directory = matches!(previous.kind.as_str(), "directory" | "mount");
        if entry.is_directory != expected_directory {
            output.conflicts.push(startup_conflict(
                &entry.sifs_path,
                "local object kind differs from the durable remote manifest",
            ));
            continue;
        }
        if entry.is_directory && remote_manifest.contains_key(&entry.sifs_path) {
            // Directory child notifications carry the semantic mutations;
            // there is no content write to perform for an existing directory.
            continue;
        }
        if let Some(capture) = capture_local_upsert(
            sync_root,
            root_path,
            &entry.relative_path,
            &entry.sifs_path,
            remote_manifest,
            content_stager,
            cancelled,
        )? {
            output.captures.push(capture);
        }
    }

    Ok(output)
}

fn startup_conflict(path: &str, detail: impl Into<String>) -> CloudFilesStartupConflictEvidence {
    CloudFilesStartupConflictEvidence {
        path: path.to_string(),
        detail: detail.into(),
    }
}

fn identity_matches_manifest(
    identity: &CloudFilesPlaceholderIdentity,
    manifest: &CloudFilesReconciledManifestEntry,
) -> bool {
    identity.path == manifest.path
        && identity.object_id == manifest.object_id
        && identity.version == manifest.version
        && identity.content_hash == manifest.content_hash
}

fn find_data_name(data: &WIN32_FIND_DATAW) -> Result<OsString, String> {
    let length = data
        .cFileName
        .iter()
        .position(|unit| *unit == 0)
        .ok_or_else(|| "FindFirstFileExW returned a filename without a terminator".to_string())?;
    Ok(OsString::from_wide(&data.cFileName[..length]))
}

fn find_data_alternate_name(data: &WIN32_FIND_DATAW) -> Result<Option<OsString>, String> {
    let length = data
        .cAlternateFileName
        .iter()
        .position(|unit| *unit == 0)
        .ok_or_else(|| {
            "FindFirstFileExW returned an alternate filename without a terminator".to_string()
        })?;
    Ok((length != 0).then(|| OsString::from_wide(&data.cAlternateFileName[..length])))
}

/// Enumerate only entries represented on disk. With a virtualization filter,
/// FIND_FIRST_EX_ON_DISK_ENTRIES_ONLY excludes projected-but-unmaterialized
/// children and therefore cannot request provider population. Unsupported
/// filtering is deliberately an error, never a fallback to ordinary listing.
fn physical_directory_entries(path: &Path) -> Result<Vec<PhysicalDirectoryEntryName>, String> {
    let pattern = path.join("*");
    let pattern_wide = wide_path(&pattern);
    let mut data = WIN32_FIND_DATAW::default();
    let find = match unsafe {
        FindFirstFileExW(
            PCWSTR(pattern_wide.as_ptr()),
            // Standard info is required so short-name aliases of the exact
            // marker-derived sentinel are visible and can fail closed.
            FindExInfoStandard,
            (&mut data as *mut WIN32_FIND_DATAW).cast::<c_void>(),
            FindExSearchNameMatch,
            None,
            FIND_FIRST_EX_ON_DISK_ENTRIES_ONLY,
        )
    } {
        Ok(handle) => OwnedFindHandle(handle),
        Err(error)
            if error.code() == HRESULT::from_win32(ERROR_FILE_NOT_FOUND.0)
                || error.code() == HRESULT::from_win32(ERROR_NO_MORE_FILES.0) =>
        {
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(format!(
                "enumerate physical children of {} without population: {error}",
                path.display()
            ));
        }
    };
    let mut children = Vec::new();
    loop {
        let name = find_data_name(&data)?;
        if name != "." && name != ".." {
            children.push(PhysicalDirectoryEntryName {
                name,
                alternate_name: find_data_alternate_name(&data)?,
            });
        }
        data = WIN32_FIND_DATAW::default();
        match unsafe { FindNextFileW(find.0, &mut data) } {
            Ok(()) => {}
            Err(error) if error.code() == HRESULT::from_win32(ERROR_NO_MORE_FILES.0) => break,
            Err(error) => {
                return Err(format!(
                    "continue enumerating physical children of {}: {error}",
                    path.display()
                ));
            }
        }
    }
    Ok(children)
}

fn physical_directory_children(path: &Path) -> Result<Vec<OsString>, String> {
    physical_directory_entries(path).map(|entries| {
        entries
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>()
    })
}

fn select_stale_cursor_recovery_sentinel(
    entries: &[PhysicalDirectoryEntryName],
    specs: &[CursorRecoverySentinelSpec],
) -> Result<Option<usize>, String> {
    for (index, spec) in specs.iter().enumerate() {
        if specs[..index].iter().any(|prior| prior.name == spec.name) {
            return Err("cursor recovery sentinel rounds derived the same filename".to_string());
        }
    }
    let mut exact = Vec::new();
    for entry in entries {
        let primary = entry.name.to_str();
        for (index, spec) in specs.iter().enumerate() {
            if primary.is_some_and(|name| name.eq_ignore_ascii_case(&spec.name)) {
                if primary != Some(spec.name.as_str()) {
                    return Err(format!(
                        "cursor recovery root contains case-alias `{}` for sentinel `{}`",
                        entry.name.to_string_lossy(),
                        spec.name
                    ));
                }
                exact.push(index);
            }
            if entry.alternate_name.as_ref().is_some_and(|alternate| {
                alternate
                    .to_str()
                    .is_some_and(|name| name.eq_ignore_ascii_case(&spec.name))
            }) {
                return Err(format!(
                    "cursor recovery root contains long-name alias `{}` for sentinel `{}`",
                    entry.name.to_string_lossy(),
                    spec.name
                ));
            }
        }
    }
    match exact.as_slice() {
        [] => Ok(None),
        [index] => Ok(Some(*index)),
        _ => Err("cursor recovery root contains multiple stale sentinels".to_string()),
    }
}

fn validate_stale_cursor_recovery_sentinel(
    attributes: u32,
    logical_size: u64,
    observed_payload: &[u8],
    expected_payload: &[u8],
) -> Result<(), String> {
    validate_stale_cursor_recovery_sentinel_metadata(
        attributes,
        logical_size,
        expected_payload.len(),
    )?;
    if observed_payload != expected_payload {
        return Err(
            "stale cursor recovery sentinel payload does not match its recovery marker".to_string(),
        );
    }
    Ok(())
}

fn validate_stale_cursor_recovery_sentinel_metadata(
    attributes: u32,
    logical_size: u64,
    expected_payload_bytes: usize,
) -> Result<(), String> {
    const MAX_SENTINEL_BYTES: usize = 64;
    if attributes & FILE_ATTRIBUTE_HIDDEN.0 == 0
        || attributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0
        || attributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
    {
        return Err("stale cursor recovery sentinel is not a plain hidden file".to_string());
    }
    if expected_payload_bytes == 0
        || expected_payload_bytes > MAX_SENTINEL_BYTES
        || logical_size != expected_payload_bytes as u64
    {
        return Err(
            "stale cursor recovery sentinel payload does not match its recovery marker".to_string(),
        );
    }
    Ok(())
}

fn verify_held_cursor_recovery_child(child: &CursorRecoveryHeldChild) -> Result<(), String> {
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(child.handle.0, &mut info) }.map_err(|error| {
        format!(
            "cursor recovery retained child handle `{}` is invalid: {error}",
            child.sifs_path
        )
    })?;
    let is_directory = info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0;
    let file_id = ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64;
    if is_directory != child.is_directory || file_id != child.file_id {
        return Err(format!(
            "cursor recovery retained child `{}` changed kind or file identity",
            child.sifs_path
        ));
    }
    Ok(())
}

fn hash_cursor_recovery_file_handle(
    handle: HANDLE,
    path: &Path,
    cancelled: &AtomicBool,
) -> Result<CloudFilesStagedContent, String> {
    unsafe { SetFilePointerEx(handle, 0, None, FILE_BEGIN) }
        .map_err(|error| format!("rewind recovery-root file {}: {error}", path.display()))?;
    let mut before = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle, &mut before) }.map_err(|error| {
        format!(
            "read initial recovery-root file identity {}: {error}",
            path.display()
        )
    })?;
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancelled.load(Ordering::SeqCst) {
            return Err("sync-cancelled".to_string());
        }
        let mut read = 0_u32;
        unsafe { ReadFile(handle, Some(&mut buffer), Some(&mut read), None) }.map_err(|error| {
            format!(
                "hash recovery-root file {} through its retained handle: {error}",
                path.display()
            )
        })?;
        if read == 0 {
            break;
        }
        let read = read as usize;
        length = length.checked_add(read as u64).ok_or_else(|| {
            format!(
                "recovery-root file length overflowed u64 while hashing {}",
                path.display()
            )
        })?;
        hasher.update(&buffer[..read]);
    }
    let mut after = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle, &mut after) }.map_err(|error| {
        format!(
            "read final recovery-root file identity {}: {error}",
            path.display()
        )
    })?;
    if stable_file_information(&before) != stable_file_information(&after)
        || length != file_size(&after)
    {
        return Err(format!(
            "recovery-root file {} changed while its authoritative bytes were verified",
            path.display()
        ));
    }
    Ok(CloudFilesStagedContent {
        content_hash: format!("blake3:{}", hasher.finalize().to_hex()),
        length,
    })
}

fn cursor_recovery_authoritative_entry<'a>(
    root: &str,
    relative: &Path,
    remote_manifest: &'a BTreeMap<String, CloudFilesReconciledManifestEntry>,
) -> Result<(String, &'a CloudFilesReconciledManifestEntry), String> {
    let sifs_path = local_relative_to_sifs(root, relative)?;
    let authoritative = remote_manifest.get(&sifs_path).ok_or_else(|| {
        format!(
            "recovery root contains local child `{sifs_path}` without an authoritative projectable identity"
        )
    })?;
    if !matches!(authoritative.kind.as_str(), "file" | "directory" | "mount") {
        return Err(format!(
            "recovery-root child `{sifs_path}` has unsupported authoritative kind `{}`",
            authoritative.kind
        ));
    }
    Ok((sifs_path, authoritative))
}

#[allow(clippy::too_many_arguments)]
fn audit_cursor_recovery_entry(
    sync_root: &Path,
    relative: &Path,
    mount: &str,
    root: &str,
    expected_sync_root_file_id: u64,
    remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    cancelled: &AtomicBool,
    handles: &mut Vec<CursorRecoveryHeldChild>,
) -> Result<String, String> {
    if cancelled.load(Ordering::SeqCst) {
        return Err("sync-cancelled".to_string());
    }
    let (sifs_path, authoritative) =
        cursor_recovery_authoritative_entry(root, relative, remote_manifest)?;
    let authoritative_is_directory = matches!(authoritative.kind.as_str(), "directory" | "mount");
    let child_path = sync_root.join(relative);
    let child_wide = wide_path(&child_path);
    let desired_access = if authoritative_is_directory {
        FILE_READ_ATTRIBUTES.0 | FILE_LIST_DIRECTORY.0 | FILE_WRITE_DATA.0
    } else {
        GENERIC_READ.0 | FILE_WRITE_DATA.0
    };
    let flags = FILE_FLAG_OPEN_REPARSE_POINT
        | if authoritative_is_directory {
            FILE_FLAG_BACKUP_SEMANTICS
        } else {
            Default::default()
        };
    let handle = unsafe {
        CreateFileW(
            PCWSTR(child_wide.as_ptr()),
            desired_access,
            // Existing writers make this open fail, and new write/delete
            // handles cannot open until the audit guard is released.
            FILE_SHARE_READ,
            None,
            OPEN_EXISTING,
            flags,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| {
        format!(
            "open recovery-root child fence {}: {error}",
            child_path.display()
        )
    })?;
    let mut handle_info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle.0, &mut handle_info) }.map_err(|error| {
        format!(
            "read recovery-root child metadata {}: {error}",
            child_path.display()
        )
    })?;
    let is_directory = handle_info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0;
    if is_directory != authoritative_is_directory {
        return Err(format!(
            "recovery-root child `{sifs_path}` has the wrong local object kind"
        ));
    }

    let mut placeholder = query_placeholder_state_handle(handle.0, &child_path)?;
    if placeholder.is_none() {
        if handle_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
            return Err(format!(
                "recovery-root child `{sifs_path}` is an unowned reparse point"
            ));
        }
        if is_directory {
            for name in physical_directory_children(&child_path)? {
                let name = name.to_str().ok_or_else(|| {
                    format!("recovery directory `{sifs_path}` contains a non-Unicode child")
                })?;
                audit_cursor_recovery_entry(
                    sync_root,
                    &relative.join(name),
                    mount,
                    root,
                    expected_sync_root_file_id,
                    remote_manifest,
                    cancelled,
                    handles,
                )?;
            }
        } else {
            let observed = hash_cursor_recovery_file_handle(handle.0, &child_path, cancelled)?;
            if !staged_content_matches_manifest_entry(&observed, authoritative) {
                return Err(format!(
                    "ordinary recovery-root file `{sifs_path}` does not match its authoritative content"
                ));
            }
        }
        let identity = CloudFilesPlaceholderIdentity::new(
            mount,
            root,
            &authoritative.path,
            &authoritative.object_id,
            &authoritative.version,
            authoritative.content_hash.clone(),
        );
        let identity_bytes =
            encode_placeholder_identity(&identity).map_err(|error| error.to_string())?;
        unsafe {
            CfConvertToPlaceholder(
                handle.0,
                Some(identity_bytes.as_ptr().cast::<c_void>()),
                identity_bytes.len() as u32,
                CF_CONVERT_FLAG_MARK_IN_SYNC,
                None,
                None,
            )
        }
        .map_err(|error| {
            format!(
                "adopt authoritative recovery-root child `{sifs_path}` as a placeholder: {error}"
            )
        })?;
        placeholder = query_placeholder_state_handle(handle.0, &child_path)?;
    } else if is_directory {
        for name in physical_directory_children(&child_path)? {
            let name = name.to_str().ok_or_else(|| {
                format!("recovery directory `{sifs_path}` contains a non-Unicode child")
            })?;
            audit_cursor_recovery_entry(
                sync_root,
                &relative.join(name),
                mount,
                root,
                expected_sync_root_file_id,
                remote_manifest,
                cancelled,
                handles,
            )?;
        }
    }

    let placeholder = placeholder.ok_or_else(|| {
        format!("recovery-root child `{sifs_path}` is not a Cloud Files placeholder")
    })?;
    if !placeholder.in_sync {
        return Err(format!(
            "recovery-root child `{sifs_path}` is not marked in sync"
        ));
    }
    if placeholder.modified_data_size != 0 {
        return Err(format!(
            "recovery-root child `{sifs_path}` has {} modified bytes",
            placeholder.modified_data_size
        ));
    }
    let handle_file_id =
        ((handle_info.nFileIndexHigh as u64) << 32) | handle_info.nFileIndexLow as u64;
    if placeholder.file_id != handle_file_id {
        return Err(format!(
            "recovery-root child `{sifs_path}` CFAPI file ID does not match its held handle"
        ));
    }
    if placeholder.sync_root_file_id != expected_sync_root_file_id {
        return Err(format!(
            "recovery-root child `{sifs_path}` belongs to sync-root file ID {}, expected {}",
            placeholder.sync_root_file_id, expected_sync_root_file_id
        ));
    }
    let identity = placeholder.identity.ok_or_else(|| {
        format!("recovery-root child `{sifs_path}` has no current versioned placeholder identity")
    })?;
    if identity.mount != mount
        || identity.root != root
        || !identity_matches_manifest(&identity, authoritative)
    {
        return Err(format!(
            "recovery-root child `{sifs_path}` does not match its authoritative placeholder identity"
        ));
    }
    if !is_directory {
        let logical_size =
            ((handle_info.nFileSizeHigh as u64) << 32) | handle_info.nFileSizeLow as u64;
        let expected_size = authoritative.content_size_bytes.ok_or_else(|| {
            format!("authoritative recovery-root file `{sifs_path}` has no exact logical size")
        })?;
        if logical_size != expected_size {
            return Err(format!(
                "recovery-root file `{sifs_path}` logical size {logical_size} does not match authoritative size {expected_size}"
            ));
        }
    }
    handles.push(CursorRecoveryHeldChild {
        handle,
        sifs_path: sifs_path.clone(),
        is_directory,
        file_id: handle_file_id,
    });
    Ok(sifs_path)
}

fn audit_cursor_recovery_root(
    sync_root: &Path,
    mount: &str,
    root: &str,
    expected_sync_root_file_id: u64,
    remote_manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    cancelled: &AtomicBool,
) -> Result<Vec<CursorRecoveryHeldChild>, String> {
    // Recovery registration may have removed every old placeholder. Missing
    // authoritative entries are therefore valid sparse projection state and
    // will be populated lazily. The safety boundary is the inverse: every
    // physical entry must be held and must match one exact manifest identity.
    let mut handles = Vec::with_capacity(remote_manifest.len());
    for name in physical_directory_children(sync_root)? {
        if cancelled.load(Ordering::SeqCst) {
            return Err("sync-cancelled".to_string());
        }
        let name = name.to_str().ok_or_else(|| {
            format!(
                "recovery root {} contains a non-Unicode child",
                sync_root.display()
            )
        })?;
        audit_cursor_recovery_entry(
            sync_root,
            Path::new(name),
            mount,
            root,
            expected_sync_root_file_id,
            remote_manifest,
            cancelled,
            &mut handles,
        )?;
    }
    Ok(handles)
}

fn manifest_generations_match(
    previous: &CloudFilesReconciledManifestEntry,
    current: &CloudFilesReconciledManifestEntry,
) -> bool {
    previous.object_id == current.object_id
        && previous.version == current.version
        && previous.content_hash == current.content_hash
        && previous.kind == current.kind
}

fn capture_raw_changes(
    sync_root: &Path,
    root_path: &str,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    raw: Vec<CloudFilesDirectoryChange>,
    content_stager: &CloudFilesContentStager,
    cancelled: &AtomicBool,
) -> Result<CapturedRawChanges, String> {
    let mut captures = Vec::new();
    let mut deferred_upserts = Vec::new();
    let mut sparse_placeholders = Vec::new();
    for change in raw {
        if cancelled.load(Ordering::SeqCst) {
            return Err("sync-cancelled".to_string());
        }
        match change {
            CloudFilesDirectoryChange::Renamed { from, to } => {
                let from_path = local_relative_to_sifs(root_path, &from)?;
                let to_path = local_relative_to_sifs(root_path, &to)?;
                let Some(source) = manifest.get(&from_path) else {
                    match capture_local_upsert_attempt(
                        sync_root,
                        root_path,
                        &to,
                        &to_path,
                        manifest,
                        content_stager,
                        cancelled,
                    )? {
                        LocalUpsertCapture::Captured(capture) => captures.push(*capture),
                        LocalUpsertCapture::NoChange => {}
                        LocalUpsertCapture::Deferred => deferred_upserts.push(to),
                        LocalUpsertCapture::NeedsMaterialization(identity) => {
                            sparse_placeholders.push(CloudFilesSparsePlaceholderObservation {
                                local_path: to_path,
                                identity,
                            });
                        }
                    }
                    continue;
                };
                let destination = manifest
                    .get(&to_path)
                    .map(exact_destination_precondition)
                    .unwrap_or(CloudFilesDestinationPrecondition::Absent);
                captures.push(CloudFilesCapturedLocalChange {
                    capture_id: stable_capture_label(&[
                        "notify-rename",
                        &from_path,
                        &to_path,
                        &source.object_id,
                        &source.version,
                    ]),
                    kind: CloudFilesLocalOperationKind::Rename,
                    source_path: from_path,
                    destination_path: Some(to_path),
                    preconditions: CloudFilesLocalOperationPreconditions {
                        source: Some(expected_state(source)),
                        destination,
                    },
                    captured_content: None,
                });
            }
            CloudFilesDirectoryChange::Removed(relative) => {
                let path = local_relative_to_sifs(root_path, &relative)?;
                if let Some(previous) = manifest.get(&path) {
                    captures.push(delete_capture("notify-delete", &path, previous));
                }
            }
            CloudFilesDirectoryChange::Added(relative)
            | CloudFilesDirectoryChange::Modified(relative) => {
                let path = local_relative_to_sifs(root_path, &relative)?;
                match capture_local_upsert_attempt(
                    sync_root,
                    root_path,
                    &relative,
                    &path,
                    manifest,
                    content_stager,
                    cancelled,
                )? {
                    LocalUpsertCapture::Captured(capture) => captures.push(*capture),
                    LocalUpsertCapture::NoChange => {}
                    LocalUpsertCapture::Deferred => deferred_upserts.push(relative),
                    LocalUpsertCapture::NeedsMaterialization(identity) => {
                        sparse_placeholders.push(CloudFilesSparsePlaceholderObservation {
                            local_path: path,
                            identity,
                        });
                    }
                }
            }
        }
    }
    Ok(CapturedRawChanges {
        changes: captures,
        deferred_upserts,
        sparse_placeholders,
    })
}

struct CapturedRawChanges {
    changes: Vec<CloudFilesCapturedLocalChange>,
    deferred_upserts: Vec<PathBuf>,
    sparse_placeholders: Vec<CloudFilesSparsePlaceholderObservation>,
}

enum LocalUpsertCapture {
    NoChange,
    Captured(Box<CloudFilesCapturedLocalChange>),
    Deferred,
    NeedsMaterialization(CloudFilesPlaceholderIdentity),
}

fn capture_local_upsert(
    sync_root: &Path,
    root_path: &str,
    relative: &Path,
    sifs_path: &str,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    content_stager: &CloudFilesContentStager,
    cancelled: &AtomicBool,
) -> Result<Option<CloudFilesCapturedLocalChange>, String> {
    match capture_local_upsert_with_reader(
        sync_root,
        root_path,
        relative,
        sifs_path,
        manifest,
        |path| read_stable_local_file(path, content_stager, cancelled),
    )? {
        LocalUpsertCapture::NoChange => Ok(None),
        LocalUpsertCapture::Captured(capture) => Ok(Some(*capture)),
        LocalUpsertCapture::Deferred => unreachable!("bounded reader cannot defer"),
        LocalUpsertCapture::NeedsMaterialization(_) => Err(format!(
            "sparse placeholder `{sifs_path}` requires authoritative identity recovery before capture"
        )),
    }
}

fn capture_local_upsert_attempt(
    sync_root: &Path,
    root_path: &str,
    relative: &Path,
    sifs_path: &str,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    content_stager: &CloudFilesContentStager,
    cancelled: &AtomicBool,
) -> Result<LocalUpsertCapture, String> {
    capture_local_upsert_with_reader(
        sync_root,
        root_path,
        relative,
        sifs_path,
        manifest,
        |path| read_stable_local_file_once(path, content_stager, cancelled),
    )
}

fn capture_local_upsert_with_reader(
    sync_root: &Path,
    root_path: &str,
    relative: &Path,
    sifs_path: &str,
    manifest: &BTreeMap<String, CloudFilesReconciledManifestEntry>,
    read_file: impl FnOnce(
        &Path,
    ) -> Result<(CloudFilesCapturedContent, String), StableLocalFileReadError>,
) -> Result<LocalUpsertCapture, String> {
    let local_path = sync_root.join(relative);
    let metadata = match fs::symlink_metadata(&local_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LocalUpsertCapture::NoChange);
        }
        Err(error) => return Err(format!("stat local path {}: {error}", local_path.display())),
    };
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(format!(
            "local reparse point {} cannot be synchronized safely",
            local_path.display()
        ));
    }
    let previous = manifest.get(sifs_path);
    let placeholder = query_placeholder_state(&local_path)?;
    if previous.is_none()
        && let Some((_in_sync, Some(identity))) = placeholder.as_ref()
    {
        return Ok(LocalUpsertCapture::NeedsMaterialization(identity.clone()));
    }
    if previous.is_none() {
        match plan_owned_offline_create(root_path, sifs_path, manifest) {
            CloudFilesOfflineCreatePlan::QueueNow => {}
            CloudFilesOfflineCreatePlan::DeferUntilParentIsRemote => {
                return Ok(LocalUpsertCapture::Deferred);
            }
            CloudFilesOfflineCreatePlan::ConflictWithRemote => {
                unreachable!("a path present in the manifest has an exact replace precondition")
            }
        }
    }
    if let Some((true, identity)) = placeholder
        && identity.as_ref().is_some_and(|identity| {
            previous.is_some_and(|remote| {
                identity.path == remote.path
                    && identity.object_id == remote.object_id
                    && identity.version == remote.version
                    && identity.content_hash == remote.content_hash
            })
        })
    {
        return Ok(LocalUpsertCapture::NoChange);
    }
    if metadata.is_dir() {
        if previous.is_some() {
            return Ok(LocalUpsertCapture::NoChange);
        }
        let signature = local_object_signature(&local_file_information(&local_path)?);
        return Ok(LocalUpsertCapture::Captured(Box::new(
            CloudFilesCapturedLocalChange {
                capture_id: stable_capture_label(&["mkdir", sifs_path, &signature]),
                kind: CloudFilesLocalOperationKind::CreateDirectory,
                source_path: sifs_path.to_string(),
                destination_path: None,
                preconditions: CloudFilesLocalOperationPreconditions {
                    source: None,
                    destination: CloudFilesDestinationPrecondition::Absent,
                },
                captured_content: None,
            },
        )));
    }
    if !metadata.is_file() {
        return Err(format!(
            "unsupported local filesystem object {}",
            local_path.display()
        ));
    }
    let (content, signature) = match read_file(&local_path) {
        Ok(capture) => capture,
        Err(StableLocalFileReadError::Retryable(_)) => return Ok(LocalUpsertCapture::Deferred),
        Err(StableLocalFileReadError::Fatal(detail)) => return Err(detail),
    };
    if let (Some(previous), CloudFilesCapturedContent::Staged(staged)) = (previous, &content)
        && staged_content_matches_manifest_entry(staged, previous)
    {
        return Ok(LocalUpsertCapture::NoChange);
    }
    let (kind, source, destination) = match previous {
        Some(previous) => (
            CloudFilesLocalOperationKind::ReplaceFile,
            Some(expected_state(previous)),
            CloudFilesDestinationPrecondition::Unspecified,
        ),
        None => (
            CloudFilesLocalOperationKind::CreateFile,
            None,
            CloudFilesDestinationPrecondition::Absent,
        ),
    };
    Ok(LocalUpsertCapture::Captured(Box::new(
        CloudFilesCapturedLocalChange {
            capture_id: stable_capture_label(&["file", sifs_path, &signature]),
            kind,
            source_path: sifs_path.to_string(),
            destination_path: None,
            preconditions: CloudFilesLocalOperationPreconditions {
                source,
                destination,
            },
            captured_content: Some(content),
        },
    )))
}

fn read_stable_local_file(
    path: &Path,
    content_stager: &CloudFilesContentStager,
    cancelled: &AtomicBool,
) -> Result<(CloudFilesCapturedContent, String), StableLocalFileReadError> {
    const ATTEMPTS: u32 = 5;
    for attempt in 1..=ATTEMPTS {
        match read_stable_local_file_once(path, content_stager, cancelled) {
            Ok(capture) => return Ok(capture),
            Err(StableLocalFileReadError::Fatal(detail)) => {
                return Err(StableLocalFileReadError::Fatal(detail));
            }
            Err(StableLocalFileReadError::Retryable(detail)) if attempt == ATTEMPTS => {
                return Err(StableLocalFileReadError::Retryable(detail));
            }
            Err(StableLocalFileReadError::Retryable(_)) => {
                thread::sleep(local_capture_retry_delay(attempt));
            }
        }
    }
    unreachable!("bounded local capture attempts always return")
}

enum StableLocalFileReadError {
    Retryable(String),
    Fatal(String),
}

fn read_stable_local_file_once(
    path: &Path,
    content_stager: &CloudFilesContentStager,
    cancelled: &AtomicBool,
) -> Result<(CloudFilesCapturedContent, String), StableLocalFileReadError> {
    if cancelled.load(Ordering::SeqCst) {
        return Err(StableLocalFileReadError::Fatal(
            "sync-cancelled".to_string(),
        ));
    }
    let wide = wide_path(path);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            Default::default(),
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| {
        let detail = format!(
            "open {} without write sharing for immutable capture: {error}",
            path.display()
        );
        if is_retryable_file_capture_error(&error) {
            StableLocalFileReadError::Retryable(detail)
        } else {
            StableLocalFileReadError::Fatal(detail)
        }
    })?;
    let mut before = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle.0, &mut before) }.map_err(|error| {
        StableLocalFileReadError::Fatal(format!(
            "read initial stable identity for {}: {error}",
            path.display()
        ))
    })?;
    let expected_size = file_size(&before);
    let raw_handle = handle.0.0;
    std::mem::forget(handle);
    let mut file = unsafe { fs::File::from_raw_handle(raw_handle) };
    let mut reader = CancellationReader {
        inner: &mut file,
        cancelled,
    };
    let staged = content_stager.stage_reader(&mut reader).map_err(|error| {
        if cancelled.load(Ordering::SeqCst) {
            return StableLocalFileReadError::Fatal("sync-cancelled".to_string());
        }
        StableLocalFileReadError::Fatal(format!(
            "stream immutable local bytes {} into the durable queue: {error}",
            path.display()
        ))
    })?;
    let mut after = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(HANDLE(file.as_raw_handle()), &mut after) }.map_err(
        |error| {
            StableLocalFileReadError::Fatal(format!(
                "read final stable identity for {}: {error}",
                path.display()
            ))
        },
    )?;
    if stable_file_information(&before) != stable_file_information(&after)
        || staged.length != expected_size
        || staged.length != file_size(&after)
    {
        return Err(StableLocalFileReadError::Retryable(format!(
            "local file {} changed while its immutable bytes were captured",
            path.display()
        )));
    }
    Ok((
        CloudFilesCapturedContent::Staged(staged.clone()),
        format!(
            "{}:{}:{}:{}:{}",
            after.dwVolumeSerialNumber,
            after.nFileIndexHigh,
            after.nFileIndexLow,
            filetime_u64(after.ftLastWriteTime),
            staged.content_hash
        ),
    ))
}

struct CancellationReader<'a, R> {
    inner: &'a mut R,
    cancelled: &'a AtomicBool,
}

impl<R: Read> Read for CancellationReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "Cloud Files capture cancelled",
            ));
        }
        self.inner.read(buffer)
    }
}

fn hash_stable_local_file(
    path: &Path,
    cancelled: &AtomicBool,
) -> Result<CloudFilesStagedContent, String> {
    const ATTEMPTS: u32 = 5;
    for attempt in 1..=ATTEMPTS {
        match hash_stable_local_file_once(path, cancelled) {
            Ok(content) => return Ok(content),
            Err(StableLocalFileReadError::Fatal(detail)) => return Err(detail),
            Err(StableLocalFileReadError::Retryable(detail)) if attempt == ATTEMPTS => {
                return Err(detail);
            }
            Err(StableLocalFileReadError::Retryable(_)) => {
                thread::sleep(local_capture_retry_delay(attempt));
            }
        }
    }
    unreachable!("bounded local hash attempts always return")
}

fn hash_stable_local_file_once(
    path: &Path,
    cancelled: &AtomicBool,
) -> Result<CloudFilesStagedContent, StableLocalFileReadError> {
    if cancelled.load(Ordering::SeqCst) {
        return Err(StableLocalFileReadError::Fatal(
            "sync-cancelled".to_string(),
        ));
    }
    let wide = wide_path(path);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            Default::default(),
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| {
        let detail = format!("open {} for stable content hash: {error}", path.display());
        if is_retryable_file_capture_error(&error) {
            StableLocalFileReadError::Retryable(detail)
        } else {
            StableLocalFileReadError::Fatal(detail)
        }
    })?;
    let mut before = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle.0, &mut before) }.map_err(|error| {
        StableLocalFileReadError::Fatal(format!(
            "read initial stable identity for {}: {error}",
            path.display()
        ))
    })?;
    let raw_handle = handle.0.0;
    std::mem::forget(handle);
    let mut file = unsafe { fs::File::from_raw_handle(raw_handle) };
    let mut hasher = blake3::Hasher::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        if cancelled.load(Ordering::SeqCst) {
            return Err(StableLocalFileReadError::Fatal(
                "sync-cancelled".to_string(),
            ));
        }
        let read = file.read(&mut buffer).map_err(|error| {
            StableLocalFileReadError::Fatal(format!(
                "hash immutable local bytes {}: {error}",
                path.display()
            ))
        })?;
        if read == 0 {
            break;
        }
        length = length.checked_add(read as u64).ok_or_else(|| {
            StableLocalFileReadError::Fatal(format!(
                "local file length overflowed u64 while hashing {}",
                path.display()
            ))
        })?;
        hasher.update(&buffer[..read]);
    }
    let mut after = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(HANDLE(file.as_raw_handle()), &mut after) }.map_err(
        |error| {
            StableLocalFileReadError::Fatal(format!(
                "read final stable identity for {}: {error}",
                path.display()
            ))
        },
    )?;
    if stable_file_information(&before) != stable_file_information(&after)
        || length != file_size(&after)
    {
        return Err(StableLocalFileReadError::Retryable(format!(
            "local file {} changed while its immutable hash was captured",
            path.display()
        )));
    }
    Ok(CloudFilesStagedContent {
        content_hash: format!("blake3:{}", hasher.finalize().to_hex()),
        length,
    })
}

fn is_retryable_file_capture_error(error: &windows::core::Error) -> bool {
    let code = error.code();
    code == HRESULT::from_win32(ERROR_SHARING_VIOLATION.0)
        || code == HRESULT::from_win32(ERROR_LOCK_VIOLATION.0)
}

fn local_file_information(path: &Path) -> Result<BY_HANDLE_FILE_INFORMATION, String> {
    let wide = wide_path(path);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| format!("open {} for stable identity: {error}", path.display()))?;
    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    unsafe { GetFileInformationByHandle(handle.0, &mut info) }
        .map_err(|error| format!("read stable identity for {}: {error}", path.display()))?;
    Ok(info)
}

fn file_size(info: &BY_HANDLE_FILE_INFORMATION) -> u64 {
    ((info.nFileSizeHigh as u64) << 32) | info.nFileSizeLow as u64
}

fn stable_file_information(info: &BY_HANDLE_FILE_INFORMATION) -> (u32, u32, u32, u64, u64) {
    (
        info.dwVolumeSerialNumber,
        info.nFileIndexHigh,
        info.nFileIndexLow,
        file_size(info),
        filetime_u64(info.ftLastWriteTime),
    )
}

fn local_object_signature(info: &BY_HANDLE_FILE_INFORMATION) -> String {
    let (volume, index_high, index_low, size, last_write) = stable_file_information(info);
    format!("{volume}:{index_high}:{index_low}:{size}:{last_write}")
}

fn filetime_u64(value: windows::Win32::Foundation::FILETIME) -> u64 {
    ((value.dwHighDateTime as u64) << 32) | value.dwLowDateTime as u64
}

fn delete_capture(
    source: &str,
    path: &str,
    previous: &CloudFilesReconciledManifestEntry,
) -> CloudFilesCapturedLocalChange {
    let is_directory = matches!(previous.kind.as_str(), "directory" | "mount");
    CloudFilesCapturedLocalChange {
        capture_id: stable_capture_label(&[source, path, &previous.object_id, &previous.version]),
        kind: if is_directory {
            CloudFilesLocalOperationKind::DeleteDirectory
        } else {
            CloudFilesLocalOperationKind::DeleteFile
        },
        source_path: path.to_string(),
        destination_path: None,
        preconditions: CloudFilesLocalOperationPreconditions {
            source: Some(expected_state(previous)),
            destination: CloudFilesDestinationPrecondition::Unspecified,
        },
        captured_content: None,
    }
}

fn expected_state(entry: &CloudFilesReconciledManifestEntry) -> CloudFilesExpectedObjectState {
    CloudFilesExpectedObjectState {
        object_id: entry.object_id.clone(),
        version: entry.version.clone(),
        content_hash: entry.content_hash.clone(),
    }
}

fn exact_destination_precondition(
    entry: &CloudFilesReconciledManifestEntry,
) -> CloudFilesDestinationPrecondition {
    CloudFilesDestinationPrecondition::Exact(expected_state(entry))
}

fn stable_capture_label(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("windows:{}", hasher.finalize().to_hex())
}

fn local_relative_to_sifs(root_path: &str, relative: &Path) -> Result<String, String> {
    let mut result = normalize_sifs_root(root_path).map_err(|error| error.to_string())?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(format!(
                "local change path {} is not a canonical relative path",
                relative.display()
            ));
        };
        let name = name.to_string_lossy();
        if name.is_empty() || name.contains('/') || name.contains('\\') {
            return Err(format!("invalid local Cloud Files component `{name}`"));
        }
        if result != "/" {
            result.push('/');
        }
        result.push_str(&name);
    }
    Ok(result)
}

fn normalize_enumerated_local_path(path: &Path) -> Result<PathBuf, String> {
    if path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(format!(
            "enumerated local path {} contains a parent component",
            path.display()
        ));
    }
    Ok(path.components().collect())
}

fn sifs_to_local(sync_root: &Path, root_path: &str, path: &str) -> Result<PathBuf, String> {
    let root = normalize_sifs_root(root_path).map_err(|error| error.to_string())?;
    let path = normalize_sifs_root(path).map_err(|error| error.to_string())?;
    let suffix = if root == "/" {
        path.trim_start_matches('/')
    } else if path == root {
        ""
    } else {
        path.strip_prefix(&(root.clone() + "/"))
            .ok_or_else(|| format!("SIFS path `{path}` is outside mounted root `{root}`"))?
    };
    let mut local = sync_root.to_path_buf();
    for component in suffix.split('/').filter(|component| !component.is_empty()) {
        if component == "." || component == ".." || component.contains('\\') {
            return Err(format!("invalid SIFS path component `{component}`"));
        }
        local.push(component);
    }
    Ok(local)
}

fn query_placeholder_state(
    path: &Path,
) -> Result<Option<(bool, Option<CloudFilesPlaceholderIdentity>)>, String> {
    let wide = wide_path(path);
    // A projected directory can exist in the Cloud Files namespace without a
    // normal NTFS name-open being available yet. CreateFileW then fails with
    // ERROR_CLOUD_FILE_INVALID_REQUEST even when OPEN_REPARSE_POINT is used.
    // The provider API's protected opener is defined for both normal and
    // placeholder files/directories and does not turn this metadata probe into
    // a foreground hydration or reparse-point open.
    let handle =
        match unsafe { CfOpenFileWithOplock(PCWSTR(wide.as_ptr()), CF_OPEN_FILE_FLAG_NONE) } {
            Ok(handle) => OwnedCloudFilesHandle(handle),
            Err(error)
                if error.code() == HRESULT::from_win32(ERROR_FILE_NOT_FOUND.0)
                    || error.code() == HRESULT::from_win32(ERROR_PATH_NOT_FOUND.0) =>
            {
                return Ok(None);
            }
            Err(error) => {
                return Err(format!(
                    "open placeholder {} through Cloud Files: {error}",
                    path.display()
                ));
            }
        };
    let state = query_placeholder_state_handle(handle.0, path)?;
    Ok(state.map(|state| (state.in_sync, state.identity)))
}

fn blocking_local_placeholder_ancestor(
    sync_root: &Path,
    local: &Path,
) -> Result<Option<String>, String> {
    let mut current = local.parent();
    while let Some(ancestor) = current {
        if ancestor == sync_root {
            return Ok(None);
        }
        match query_placeholder_state(ancestor)? {
            Some((true, _)) => {}
            Some((false, _)) => {
                return Ok(Some(format!(
                    "Cloud Files ancestor {} is not in sync",
                    ancestor.display()
                )));
            }
            None => match fs::symlink_metadata(ancestor) {
                Ok(_) => {
                    return Ok(Some(format!(
                        "local ancestor {} is not a Cloud Files placeholder",
                        ancestor.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "stat local Cloud Files ancestor {}: {error}",
                        ancestor.display()
                    ));
                }
            },
        }
        current = ancestor.parent();
    }
    Err(format!(
        "local placeholder {} escaped Cloud Files root {} while inspecting ancestors",
        local.display(),
        sync_root.display()
    ))
}

fn query_placeholder_state_handle(
    handle: HANDLE,
    path: &Path,
) -> Result<Option<PlaceholderStandardState>, String> {
    let mut buffer = vec![0u64; (size_of::<CF_PLACEHOLDER_STANDARD_INFO>() + 4096).div_ceil(8)];
    let result = unsafe {
        CfGetPlaceholderInfo(
            handle,
            CF_PLACEHOLDER_INFO_STANDARD,
            buffer.as_mut_ptr().cast::<c_void>(),
            (buffer.len() * size_of::<u64>()) as u32,
            None,
        )
    };
    if result.is_err() {
        return Ok(None);
    }
    let info = unsafe { &*buffer.as_ptr().cast::<CF_PLACEHOLDER_STANDARD_INFO>() };
    let identity_length = info.FileIdentityLength as usize;
    if identity_length > 4096 {
        return Err(format!(
            "placeholder {} identity is {} bytes",
            path.display(),
            identity_length
        ));
    }
    let identity_offset = offset_of!(CF_PLACEHOLDER_STANDARD_INFO, FileIdentity);
    if identity_offset.saturating_add(identity_length) > buffer.len() * size_of::<u64>() {
        return Err(format!(
            "placeholder {} identity is truncated",
            path.display()
        ));
    }
    let identity = unsafe {
        std::slice::from_raw_parts(
            buffer.as_ptr().cast::<u8>().add(identity_offset),
            identity_length,
        )
    };
    let decoded = match decode_placeholder_identity(identity) {
        Ok(DecodedCloudFilesPlaceholderIdentity::Versioned(identity)) => Some(identity),
        Ok(DecodedCloudFilesPlaceholderIdentity::LegacyPath(_)) | Err(_) => None,
    };
    Ok(Some(PlaceholderStandardState {
        in_sync: info.InSyncState == CF_IN_SYNC_STATE_IN_SYNC,
        modified_data_size: info.ModifiedDataSize,
        file_id: info.FileId as u64,
        sync_root_file_id: info.SyncRootFileId as u64,
        identity: decoded,
    }))
}

pub(super) struct WindowsPlaceholderEffects<T> {
    service: Arc<Mutex<NetworkedDfsService<T>>>,
    sync_root: PathBuf,
    mount_identity: String,
    root_path: String,
    suppressed_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
    namespace_suppressions: BTreeMap<String, CloudFilesNamespaceSuppression>,
    cancelled: Arc<AtomicBool>,
    content_stager: CloudFilesContentStager,
}

impl<T> WindowsPlaceholderEffects<T> {
    pub(super) fn new(
        service: Arc<Mutex<NetworkedDfsService<T>>>,
        sync_root: PathBuf,
        root_path: String,
        suppressed_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
        cancelled: Arc<AtomicBool>,
        content_stager: CloudFilesContentStager,
    ) -> Self {
        Self {
            service,
            mount_identity: sync_root.to_string_lossy().into_owned(),
            sync_root,
            root_path,
            suppressed_paths,
            namespace_suppressions: BTreeMap::new(),
            cancelled,
            content_stager,
        }
    }

    fn local_path(&self, path: &str) -> Result<PathBuf, String> {
        sifs_to_local(&self.sync_root, &self.root_path, path)
    }

    fn suppress_local_path(&self, local: &Path) -> Result<(), String> {
        let relative = local
            .strip_prefix(&self.sync_root)
            .map_err(|_| format!("{} escaped the Cloud Files root", local.display()))?
            .to_path_buf();
        self.suppressed_paths
            .lock()
            .map_err(|_| "Cloud Files local suppression ledger is poisoned".to_string())?
            .insert(relative);
        Ok(())
    }
}

impl<T: DfsMessageTransport> WindowsPlaceholderEffects<T> {
    fn check_cancelled(&self) -> Result<(), String> {
        if self.cancelled.load(Ordering::SeqCst) {
            Err("sync-cancelled".to_string())
        } else {
            Ok(())
        }
    }

    fn authoritative_stat(&self, path: &str) -> Result<dfs::DfsStat, String> {
        self.check_cancelled()?;
        lock_service(&self.service)?
            .posix_lstat_distributed(path)
            .map_err(|error| error.to_string())
    }

    fn identity(&self, stat: &dfs::DfsStat) -> CloudFilesPlaceholderIdentity {
        CloudFilesPlaceholderIdentity::new(
            self.mount_identity.clone(),
            self.root_path.clone(),
            stat.path.clone(),
            stat.object_id.to_string(),
            stat.version.to_string(),
            stat.content_hash.clone(),
        )
    }

    fn ensure_remote_placeholder(&self, path: &str, dehydrate_clean: bool) -> Result<(), String> {
        self.check_cancelled()?;
        let stat = self.authoritative_stat(path)?;
        self.check_cancelled()?;
        if stat.kind == DfsObjectKind::Tombstone {
            return self.remove_local_path(path);
        }
        let projection = cloud_files_object_projection(&stat.kind);
        if let CloudFilesObjectProjection::Unsupported(kind) = projection {
            super::host_log(&format!(
                "Cloud Files omitted unsupported SIFS {kind} `{path}`; any clean older placeholder is removed"
            ));
            return self.remove_local_path(path);
        }
        let local = self.local_path(path)?;
        let is_directory = projection == CloudFilesObjectProjection::Directory;
        if !local.exists() {
            let parent = local
                .parent()
                .ok_or_else(|| format!("local placeholder {} has no parent", local.display()))?;
            let name = local
                .file_name()
                .ok_or_else(|| format!("local placeholder {} has no name", local.display()))?
                .to_string_lossy()
                .into_owned();
            self.suppress_local_path(&local)?;
            create_placeholders(
                parent,
                vec![
                    PlaceholderPayload::new(
                        name,
                        self.identity(&stat),
                        stat,
                        false,
                        PlaceholderCreateDisposition::CreateAbsent,
                    )
                    .map_err(|error| error.to_string())?,
                ],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())?;
            return Ok(());
        }
        let Some((in_sync, observed)) = query_placeholder_state(&local)? else {
            return Err(format!(
                "local path {} is not a Cloud Files placeholder and cannot be overwritten by remote state",
                local.display()
            ));
        };
        let expected = self.identity(&stat);
        if !in_sync && observed.as_ref() != Some(&expected) {
            return Err(format!(
                "local placeholder {} is dirty while remote identity changed",
                local.display()
            ));
        }
        if in_sync && observed.as_ref() == Some(&expected) {
            return Ok(());
        }
        self.suppress_local_path(&local)?;
        update_placeholder(
            &local,
            stat,
            expected,
            dehydrate_clean && !is_directory,
            true,
        )
    }

    fn remove_local_path(&self, path: &str) -> Result<(), String> {
        match self.remove_local_path_for_full_scan(path)? {
            CloudFilesLocalRemovalOutcome::RemovedOrAbsent => Ok(()),
            CloudFilesLocalRemovalOutcome::Preserved(conflict) => Err(conflict.detail),
        }
    }

    fn remove_local_path_for_full_scan(
        &self,
        path: &str,
    ) -> Result<CloudFilesLocalRemovalOutcome, String> {
        self.check_cancelled()?;
        let local = self.local_path(path)?;
        if !local.exists() {
            return Ok(CloudFilesLocalRemovalOutcome::RemovedOrAbsent);
        }
        let metadata = fs::symlink_metadata(&local)
            .map_err(|error| format!("stat local path {}: {error}", local.display()))?;
        let Some((in_sync, _identity)) = query_placeholder_state(&local)? else {
            return Ok(self.preserve_remote_delete_conflict(
                path,
                &local,
                "local-non-placeholder-blocks-remote-delete",
                "the local entry is a normal file or directory, not a Cloud Files placeholder",
            ));
        };
        if !in_sync {
            return Ok(self.preserve_remote_delete_conflict(
                path,
                &local,
                "local-dirty-placeholder-blocks-remote-delete",
                "the local Cloud Files placeholder contains changes which are not marked in sync",
            ));
        }
        self.suppress_local_path(&local)?;
        if metadata.is_dir() {
            fs::remove_dir(&local)
        } else {
            fs::remove_file(&local)
        }
        .map_err(|error| {
            format!(
                "remove remote-deleted placeholder {}: {error}",
                local.display()
            )
        })?;
        Ok(CloudFilesLocalRemovalOutcome::RemovedOrAbsent)
    }

    fn preserve_remote_delete_conflict(
        &self,
        path: &str,
        local: &Path,
        reason_code: &str,
        reason: &str,
    ) -> CloudFilesLocalRemovalOutcome {
        let detail = format!(
            "{reason_code}: authoritative SIFS state deletes `{path}`, but {} was preserved because {reason}. Move or preserve the local entry outside this mount, or resolve the authoritative state, then retry reconciliation; unrelated paths remain available",
            local.display()
        );
        super::host_log(&format!(
            "Cloud Files preserved path-level conflict at `{path}`: {detail}"
        ));
        CloudFilesLocalRemovalOutcome::Preserved(CloudFilesStartupConflictEvidence {
            path: path.to_string(),
            detail,
        })
    }

    fn rename_local_path(&self, from: &str, to: &str) -> Result<(), String> {
        self.check_cancelled()?;
        let source = self.local_path(from)?;
        let destination = self.local_path(to)?;
        if !source.exists() {
            if destination.exists() {
                return self.ensure_remote_placeholder(to, true);
            }
            return Err(format!(
                "remote rename source {} and destination {} are both absent locally",
                source.display(),
                destination.display()
            ));
        }
        if destination.exists() {
            return Err(format!(
                "remote rename destination {} already exists locally",
                destination.display()
            ));
        }
        let Some((in_sync, _)) = query_placeholder_state(&source)? else {
            return Err(format!(
                "remote rename source {} is not a placeholder",
                source.display()
            ));
        };
        if !in_sync {
            return Err(format!(
                "remote rename source {} has dirty local data",
                source.display()
            ));
        }
        let parent = destination.parent().ok_or_else(|| {
            format!(
                "remote rename destination {} has no parent",
                destination.display()
            )
        })?;
        if !parent.exists() {
            return Err(format!(
                "remote rename destination parent {} is absent",
                parent.display()
            ));
        }
        self.suppress_local_path(&source)?;
        self.suppress_local_path(&destination)?;
        self.check_cancelled()?;
        let source_wide = wide_path(&source);
        let destination_wide = wide_path(&destination);
        unsafe {
            super::MoveFileExW(
                PCWSTR(source_wide.as_ptr()),
                PCWSTR(destination_wide.as_ptr()),
                super::MOVEFILE_WRITE_THROUGH,
            )
        }
        .map_err(|error| {
            format!(
                "apply remote placeholder rename {} -> {}: {error}",
                source.display(),
                destination.display()
            )
        })?;
        self.ensure_remote_placeholder(to, true)
    }

    fn convert_local_completion(
        &self,
        parent_operation_id: &str,
        path: &str,
        staged_content: Option<&crate::sync_executor::CloudFilesDfsMutationContent>,
    ) -> Result<Option<CloudFilesLocalOperationRequest>, String> {
        self.check_cancelled()?;
        let stat = self.authoritative_stat(path)?;
        let local = self.local_path(path)?;
        if !local.exists() {
            return Err(format!(
                "locally completed DFS mutation has no Windows path {}",
                local.display()
            ));
        }
        let is_directory = matches!(stat.kind, DfsObjectKind::Directory | DfsObjectKind::Mount);
        let local_matches_remote = match staged_content {
            Some(staged) => {
                let observed = hash_stable_local_file(&local, &self.cancelled)?;
                observed == staged.descriptor
            }
            None if is_directory => true,
            None => {
                let observed = hash_stable_local_file(&local, &self.cancelled)?;
                stat.size_bytes.is_some_and(|size| observed.length == size)
                    && stat
                        .content_hash
                        .as_deref()
                        .is_some_and(|expected| observed.content_hash == expected)
            }
        };
        let follow_up = if !local_matches_remote && !is_directory {
            let (captured, signature) =
                read_stable_local_file(&local, &self.content_stager, &self.cancelled).map_err(
                    |error| match error {
                        StableLocalFileReadError::Retryable(detail)
                        | StableLocalFileReadError::Fatal(detail) => detail,
                    },
                )?;
            let CloudFilesCapturedContent::Staged(staged_content) = captured else {
                return Err(
                    "production completion capture did not stage immutable bytes".to_string(),
                );
            };
            let entry = manifest_entry_from_stat(stat.clone());
            Some(completion_follow_up_request(
                &self.mount_identity,
                &self.root_path,
                parent_operation_id,
                &entry,
                staged_content,
                &signature,
            ))
        } else {
            None
        };
        let identity = self.identity(&stat);
        self.suppress_local_path(&local)?;
        self.check_cancelled()?;
        if query_placeholder_state(&local)?.is_none() {
            let wide = wide_path(&local);
            let handle = unsafe {
                CreateFileW(
                    PCWSTR(wide.as_ptr()),
                    (FILE_READ_ATTRIBUTES | FILE_WRITE_DATA).0,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                    None,
                    OPEN_EXISTING,
                    if is_directory {
                        FILE_FLAG_BACKUP_SEMANTICS
                    } else {
                        Default::default()
                    },
                    None,
                )
            }
            .map(OwnedWin32Handle)
            .map_err(|error| format!("open local completion {}: {error}", local.display()))?;
            let identity_bytes =
                encode_placeholder_identity(&identity).map_err(|error| error.to_string())?;
            unsafe {
                CfConvertToPlaceholder(
                    handle.0,
                    Some(identity_bytes.as_ptr().cast::<c_void>()),
                    identity_bytes.len() as u32,
                    if local_matches_remote {
                        CF_CONVERT_FLAG_MARK_IN_SYNC
                    } else {
                        CF_CONVERT_FLAG_NONE
                    },
                    None,
                    None,
                )
            }
            .map_err(|error| {
                format!(
                    "convert local completion {} to placeholder: {error}",
                    local.display()
                )
            })?;
        }
        update_placeholder(&local, stat, identity, false, local_matches_remote)?;
        Ok(follow_up)
    }
}

impl<T: DfsMessageTransport> CloudFilesNamespaceEffectAdapter for WindowsPlaceholderEffects<T> {
    fn local_path_state(&mut self, path: &str) -> Result<CloudFilesLocalPathState, String> {
        let local = self.local_path(path)?;
        if let Some((in_sync, _)) = query_placeholder_state(&local)? {
            return if in_sync {
                Ok(CloudFilesLocalPathState::Clean)
            } else {
                Ok(CloudFilesLocalPathState::Dirty {
                    detail: "Cloud Files placeholder is not in sync".to_string(),
                })
            };
        }
        match fs::symlink_metadata(&local) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(
                    match blocking_local_placeholder_ancestor(&self.sync_root, &local)? {
                        Some(detail) => CloudFilesLocalPathState::Dirty { detail },
                        None => CloudFilesLocalPathState::Clean,
                    },
                );
            }
            Err(error) => return Err(format!("stat local path {}: {error}", local.display())),
        }
        Ok(CloudFilesLocalPathState::Dirty {
            detail: "local path is not a Cloud Files placeholder".to_string(),
        })
    }

    fn apply(&mut self, action: &CloudFilesNamespaceAction) -> Result<(), String> {
        match action {
            CloudFilesNamespaceAction::Upsert { path, .. } => {
                self.ensure_remote_placeholder(path, true)
            }
            CloudFilesNamespaceAction::Remove { path, .. } => self.remove_local_path(path),
            CloudFilesNamespaceAction::Rename {
                from_path,
                to_path,
                kind,
                ..
            } if matches!(
                kind.as_str(),
                "symlink" | "hardlink" | "artifact" | "tombstone"
            ) =>
            {
                super::host_log(&format!(
                    "Cloud Files omitted unsupported SIFS {kind} rename destination `{to_path}`"
                ));
                self.remove_local_path(from_path)
            }
            CloudFilesNamespaceAction::Rename {
                from_path, to_path, ..
            } => self.rename_local_path(from_path, to_path),
        }
    }

    fn record_conflict(&mut self, conflict: &CloudFilesNamespaceConflict) -> Result<(), String> {
        super::host_log(&format!(
            "Cloud Files remote/local conflict {}: {}",
            conflict.event_id, conflict.detail
        ));
        Ok(())
    }

    fn request_full_scan(&mut self, request: &CloudFilesFullScanRequest) -> Result<(), String> {
        super::host_log(&format!(
            "Cloud Files namespace feed requested full scan for {} at sequence {}",
            request.shard_key, request.cursor.after_sequence
        ));
        Ok(())
    }
}

impl<T: DfsMessageTransport> CloudFilesLocalCompletionEffect for WindowsPlaceholderEffects<T> {
    fn complete(
        &mut self,
        command: &CloudFilesDfsMutationCommand,
        evidence: &CloudFilesRemoteMutationEvidence,
    ) -> Result<CloudFilesLocalCompletionOutcome, String> {
        let suppression = suppression_for_completed_mutation(command, evidence)
            .map_err(|error| error.to_string())?;
        let follow_up = match command.kind {
            CloudFilesLocalOperationKind::DeleteFile
            | CloudFilesLocalOperationKind::DeleteDirectory => None,
            CloudFilesLocalOperationKind::Rename => {
                let destination = command
                    .destination_path
                    .as_deref()
                    .ok_or_else(|| "completed rename has no destination".to_string())?;
                self.convert_local_completion(&command.operation_id, destination, None)?
            }
            CloudFilesLocalOperationKind::CreateFile
            | CloudFilesLocalOperationKind::ReplaceFile
            | CloudFilesLocalOperationKind::CreateDirectory => self.convert_local_completion(
                &command.operation_id,
                &command.source_path,
                command.staged_content.as_ref(),
            )?,
        };
        self.namespace_suppressions
            .insert(suppression.operation_id.clone(), suppression);
        Ok(match follow_up {
            Some(request) => CloudFilesLocalCompletionOutcome::EnqueueFollowUp(Box::new(request)),
            None => CloudFilesLocalCompletionOutcome::Completed,
        })
    }
}

impl<T: DfsMessageTransport> CloudFilesWindowsPlaceholderEffect for WindowsPlaceholderEffects<T> {
    fn apply_full_scan(
        &mut self,
        snapshot: &CloudFilesFullScanSnapshot,
    ) -> Result<CloudFilesFullScanEffectOutcome, String> {
        let mut outcome = CloudFilesFullScanEffectOutcome::default();
        let mut entries = snapshot.manifest.values().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.path.matches('/').count());
        for entry in entries {
            if matches!(
                entry.kind.as_str(),
                "symlink" | "hardlink" | "artifact" | "tombstone"
            ) {
                // The authoritative manifest tracks the unprojectable entry so
                // parent/name conflicts remain visible, but Windows receives
                // no misleading regular-file placeholder.
                match self.local_path_state(&entry.path)? {
                    CloudFilesLocalPathState::Clean => {
                        record_local_removal_outcome(
                            &mut outcome,
                            &entry.path,
                            self.remove_local_path_for_full_scan(&entry.path)?,
                        );
                    }
                    CloudFilesLocalPathState::Dirty { detail } => {
                        outcome.conflicts.push(startup_conflict(
                            &entry.path,
                            format!(
                                "local-dirty-entry-blocks-authoritative-projection: authoritative SIFS kind `{}` cannot replace the preserved local entry: {detail}",
                                entry.kind
                            ),
                        ));
                    }
                    CloudFilesLocalPathState::Pending { operation_id } => {
                        outcome.conflicts.push(startup_conflict(
                            &entry.path,
                            format!(
                                "local-operation-blocks-authoritative-projection: operation `{operation_id}` must settle before authoritative SIFS kind `{}` can be projected",
                                entry.kind
                            ),
                        ));
                    }
                }
                continue;
            }
            if matches!(
                self.local_path_state(&entry.path)?,
                CloudFilesLocalPathState::Dirty { .. }
            ) {
                // Startup scanning has already queued this local generation;
                // never replace it with the remote snapshot.
                continue;
            }
            self.ensure_remote_placeholder(&entry.path, true)?;
        }
        for path in &snapshot.removed_paths {
            record_local_removal_outcome(
                &mut outcome,
                path,
                self.remove_local_path_for_full_scan(path)?,
            );
        }
        Ok(outcome)
    }

    fn drain_namespace_suppressions(&mut self) -> Vec<CloudFilesNamespaceSuppression> {
        std::mem::take(&mut self.namespace_suppressions)
            .into_values()
            .collect()
    }
}

enum CloudFilesLocalRemovalOutcome {
    RemovedOrAbsent,
    Preserved(CloudFilesStartupConflictEvidence),
}

fn record_local_removal_outcome(
    outcome: &mut CloudFilesFullScanEffectOutcome,
    path: &str,
    removal: CloudFilesLocalRemovalOutcome,
) {
    match removal {
        CloudFilesLocalRemovalOutcome::RemovedOrAbsent => {
            outcome.resolved_paths.push(path.to_string());
        }
        CloudFilesLocalRemovalOutcome::Preserved(conflict) => {
            outcome.conflicts.push(conflict);
        }
    }
}

fn update_placeholder(
    local: &Path,
    stat: dfs::DfsStat,
    identity: CloudFilesPlaceholderIdentity,
    dehydrate: bool,
    mark_in_sync: bool,
) -> Result<(), String> {
    let is_directory = matches!(stat.kind, DfsObjectKind::Directory | DfsObjectKind::Mount);
    let wide = wide_path(local);
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            (FILE_READ_ATTRIBUTES | FILE_WRITE_DATA).0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            if is_directory {
                FILE_FLAG_BACKUP_SEMANTICS
            } else {
                Default::default()
            },
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| format!("open placeholder {} for update: {error}", local.display()))?;
    let name = local
        .file_name()
        .ok_or_else(|| format!("placeholder {} has no filename", local.display()))?
        .to_string_lossy()
        .into_owned();
    let payload = PlaceholderPayload::new(
        name,
        identity,
        stat,
        false,
        PlaceholderCreateDisposition::CreateAbsent,
    )
    .map_err(|error| error.to_string())?;
    let mut flags = CF_UPDATE_FLAG_NONE;
    if mark_in_sync {
        flags |= CF_UPDATE_FLAG_MARK_IN_SYNC;
    }
    if dehydrate && mark_in_sync {
        flags |= CF_UPDATE_FLAG_DEHYDRATE;
    }
    unsafe {
        CfUpdatePlaceholder(
            handle.0,
            Some(&payload.metadata),
            Some(payload.identity.as_ptr().cast::<c_void>()),
            payload.identity.len() as u32,
            None,
            flags,
            None,
            None,
        )
    }
    .map_err(|error| format!("update placeholder {}: {error}", local.display()))
}

pub(super) type ConcreteWindowsWritableWorker<T> = CloudFilesWritableSyncWorker<
    WindowsDirectoryChangeAccelerator,
    CloudFilesConcreteDfsAdapter<CloudFilesDfsServiceMutationBackend<T>>,
    WindowsDfsNamespaceSource<T>,
    WindowsPlaceholderEffects<T>,
>;

pub(super) struct WindowsWritableWorkerOptions {
    pub sync_root: PathBuf,
    pub root_path: String,
    pub existing_owned_root: bool,
    pub expected_cursor_recovery_sync_root_file_id: Option<u64>,
    pub prepopulate_depth: u32,
    pub materialized_directories: Arc<Mutex<BTreeSet<String>>>,
}

pub(super) fn build_writable_worker<T: DfsMessageTransport>(
    state: CloudFilesWritableSyncState,
    service: Arc<Mutex<NetworkedDfsService<T>>>,
    write_options: dfs::DfsWriteOptions,
    mounted_mutation_handoff: Arc<CloudFilesMountedMutationHandoff>,
    worker_wake: Arc<WindowsWorkerWake>,
    options: WindowsWritableWorkerOptions,
) -> (
    ConcreteWindowsWritableWorker<T>,
    Arc<AtomicBool>,
    SharedExplorerStatusProjector,
) {
    let materialization_audit_intents = state.materialization_audit_intents();
    let WindowsWritableWorkerOptions {
        sync_root,
        root_path,
        existing_owned_root,
        expected_cursor_recovery_sync_root_file_id,
        prepopulate_depth,
        materialized_directories,
    } = options;
    let suppressed_paths = Arc::new(Mutex::new(BTreeSet::new()));
    let content_stager = state.queue().content_stager();
    let cancelled = Arc::new(AtomicBool::new(false));
    let explorer_status = Arc::new(Mutex::new(CloudFilesExplorerStatusProjector::new(
        sync_root.clone(),
        root_path.clone(),
    )));
    let operation_explorer_status = Arc::clone(&explorer_status);
    let operation_started = Arc::new(move |command: &CloudFilesDfsMutationCommand| {
        observe_explorer_upload(&operation_explorer_status, command);
    });
    let worker = CloudFilesWritableSyncWorker::new(
        state,
        WindowsDirectoryChangeAccelerator::new(
            sync_root.clone(),
            root_path.clone(),
            existing_owned_root,
            expected_cursor_recovery_sync_root_file_id,
            Arc::clone(&suppressed_paths),
            content_stager.clone(),
            worker_wake,
        ),
        CloudFilesConcreteDfsAdapter::new(
            CloudFilesDfsServiceMutationBackend::new_with_cancellation(
                Arc::clone(&service),
                write_options,
                Arc::clone(&cancelled),
                Arc::clone(&mounted_mutation_handoff),
                operation_started,
            ),
        ),
        WindowsDfsNamespaceSource::new(
            Arc::clone(&service),
            mounted_mutation_handoff,
            prepopulate_depth,
            materialized_directories,
            materialization_audit_intents,
        ),
        WindowsPlaceholderEffects::new(
            service,
            sync_root,
            root_path,
            suppressed_paths,
            Arc::clone(&cancelled),
            content_stager,
        ),
    );
    (worker, cancelled, explorer_status)
}

#[derive(Clone)]
pub(super) struct WorkerLifecycleEmitter {
    sync_root: PathBuf,
    root_path: String,
    placeholders_created: u64,
}

impl WorkerLifecycleEmitter {
    pub(super) fn from_lifecycle(lifecycle: &MountLifecycleState) -> Self {
        Self {
            sync_root: lifecycle.sync_root.clone(),
            root_path: lifecycle.root_path.clone(),
            placeholders_created: lifecycle.placeholders_created,
        }
    }

    pub(super) fn emit(
        &self,
        status: CloudFilesLocalChangeSyncStatus,
        worker_alive: bool,
        worker_ready: bool,
        health_generation: u64,
    ) {
        let event = CloudFilesMountLifecycleEvent::connected(
            self.sync_root.clone(),
            self.root_path.clone(),
            self.placeholders_created,
        )
        .with_local_change_sync(status)
        .with_worker_health(worker_alive, worker_ready, health_generation);
        host_emit_event(&CloudFilesModuleEvent::MountLifecycle(event));
    }

    fn emit_failed(
        &self,
        status: CloudFilesLocalChangeSyncStatus,
        health_generation: u64,
        error: &CloudFilesModuleError,
    ) {
        let event = CloudFilesMountLifecycleEvent::failed(
            self.sync_root.clone(),
            self.root_path.clone(),
            self.placeholders_created,
            true,
            true,
            error.clone(),
        )
        .with_local_change_sync(status)
        .with_worker_health(false, false, health_generation);
        host_emit_event(&CloudFilesModuleEvent::MountLifecycle(event));
    }
}

fn worker_lifecycle_emission_due(
    previous_status: Option<&CloudFilesLocalChangeSyncStatus>,
    status: &CloudFilesLocalChangeSyncStatus,
    elapsed_since_previous: Option<Duration>,
) -> bool {
    previous_status != Some(status)
        || elapsed_since_previous
            .is_none_or(|elapsed| elapsed >= WORKER_LIFECYCLE_MIN_EMIT_INTERVAL)
}

pub(super) struct WindowsWritableWorkerHandle {
    cancelled: Arc<AtomicBool>,
    wake: Arc<WindowsWorkerWake>,
    alive: Arc<AtomicBool>,
    failure: mpsc::Receiver<CloudFilesModuleError>,
    progress: mpsc::Receiver<u64>,
    cursor_recovery_progress: mpsc::Receiver<CursorRecoveryWorkerProgress>,
    cursor_recovery: Option<CloudFilesCursorRecoveryControl>,
    cursor_recovery_terminal_observed: AtomicBool,
    cursor_recovery_finalize:
        mpsc::SyncSender<mpsc::SyncSender<Result<bool, CloudFilesModuleError>>>,
    worker_join: Option<JoinHandle<()>>,
    stopped_event: Arc<OwnedCrossThreadEvent>,
    explorer_status: SharedExplorerStatusProjector,
}

pub(super) enum WindowsWritableWorkerWait {
    Cancelled,
    Stopped,
    TimedOut,
}

enum CursorRecoveryWorkerProgress {
    Advanced(u64),
    Terminal(u64),
}

impl WindowsWritableWorkerHandle {
    pub(super) fn spawn<T>(
        mut worker: ConcreteWindowsWritableWorker<T>,
        cancelled: Arc<AtomicBool>,
        wake: Arc<WindowsWorkerWake>,
        emitter: WorkerLifecycleEmitter,
        initial_generation: u64,
        explorer_status: SharedExplorerStatusProjector,
    ) -> Result<Self, CloudFilesModuleError>
    where
        T: DfsMessageTransport + Send + 'static,
    {
        let cursor_recovery = worker.cursor_recovery_control();
        let stopped_event = Arc::new(OwnedCrossThreadEvent(OwnedWin32Handle(
            unsafe { CreateEventW(None, true, false, None) }
                .map_err(|error| adapter_error("writable-worker-event", error.to_string()))?,
        )));
        let thread_stopped_event = Arc::clone(&stopped_event);
        let thread_cursor_recovery = cursor_recovery.clone();
        let alive = Arc::new(AtomicBool::new(true));
        let thread_cancelled = Arc::clone(&cancelled);
        let thread_wake = Arc::clone(&wake);
        let thread_alive = Arc::clone(&alive);
        let thread_explorer_status = Arc::clone(&explorer_status);
        let failure_emitter = emitter.clone();
        let (failure_sender, failure) = mpsc::channel();
        let (progress_sender, progress) = mpsc::sync_channel(1);
        let (cursor_recovery_progress_sender, cursor_recovery_progress) = mpsc::channel();
        let (cursor_recovery_finalize_sender, cursor_recovery_finalize_receiver) =
            mpsc::sync_channel::<mpsc::SyncSender<Result<bool, CloudFilesModuleError>>>(1);
        let worker_join = thread::Builder::new()
            .name("sinomo-cloud-files-writable-sync".to_string())
            .spawn(move || {
                if let Err(error) = enter_writable_worker_background_mode() {
                    super::host_log(&error);
                }
                let progress_emitter = emitter;
                let last_generation = AtomicU64::new(initial_generation);
                let mut last_emitted_status = None;
                let mut last_lifecycle_emit = None;
                let mut cursor_recovery_terminal_emitted = false;
                let run_result = catch_unwind(AssertUnwindSafe(|| {
                    let mut generation = initial_generation;
                    let mut reasons = WORKER_WAKE_NAMESPACE_CHANGE;
                    while !thread_cancelled.load(Ordering::SeqCst) {
                        if let Some(detail) = thread_wake.take_failure() {
                            return Err(crate::windows_sync::CloudFilesWindowsSyncError {
                                code: "namespace-event-stream",
                                detail,
                            });
                        }
                        if let Ok(reply) = cursor_recovery_finalize_receiver.try_recv() {
                            let finalized = worker
                                .finalize_cursor_recovery_after_host_ready()
                                .map_err(|error| adapter_error(error.code, error.detail));
                            let _ = reply.send(finalized);
                        }
                        let status = run_retrying_namespace_read_pass(
                            &thread_cancelled,
                            || {
                                worker
                                    .run_once_for_wake(
                                        &thread_cancelled,
                                        reasons & WORKER_WAKE_NAMESPACE_CHANGE != 0,
                                    )
                                    .map(|_| worker.state().status())
                            },
                            |cancelled| {
                                for _ in 0..5 {
                                    if cancelled.load(Ordering::SeqCst) {
                                        break;
                                    }
                                    thread::sleep(Duration::from_millis(50));
                                }
                            },
                        )?;
                        if thread_cancelled.load(Ordering::SeqCst) {
                            break;
                        }
                        observe_explorer_state(&thread_explorer_status, worker.state());
                        generation = generation.saturating_add(1);
                        let ready = status.mode == CloudFilesLocalChangeSyncMode::Active;
                        last_generation.store(generation, Ordering::SeqCst);
                        let now = Instant::now();
                        if worker_lifecycle_emission_due(
                            last_emitted_status.as_ref(),
                            &status,
                            last_lifecycle_emit
                                .map(|emitted: Instant| now.saturating_duration_since(emitted)),
                        ) {
                            last_emitted_status = Some(status.clone());
                            last_lifecycle_emit = Some(now);
                            progress_emitter.emit(status, true, ready, generation);
                        }
                        if ready {
                            let _ = progress_sender.try_send(generation);
                        }
                        if let Some(control) = thread_cursor_recovery.as_ref() {
                            if control.terminal() && !cursor_recovery_terminal_emitted {
                                let _ = cursor_recovery_progress_sender
                                    .send(CursorRecoveryWorkerProgress::Terminal(generation));
                                cursor_recovery_terminal_emitted = true;
                            } else if !control.terminal() && control.should_run_immediately() {
                                let _ = cursor_recovery_progress_sender
                                    .send(CursorRecoveryWorkerProgress::Advanced(generation));
                            }
                        }
                        reasons =
                            thread_wake.wait(&thread_cancelled, worker.next_scheduled_work_in());
                        if let Some(detail) = thread_wake.take_failure() {
                            return Err(crate::windows_sync::CloudFilesWindowsSyncError {
                                code: "namespace-event-stream",
                                detail,
                            });
                        }
                        if reasons & WORKER_WAKE_AUTHORITATIVE_RESCAN != 0 {
                            worker.request_authoritative_rescan();
                        }
                    }
                    Ok::<(), crate::windows_sync::CloudFilesWindowsSyncError>(())
                }));
                match run_result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if error.code == "sync-cancelled" => {}
                    Ok(Err(error)) => {
                        let module_error = adapter_error(error.code, error.detail);
                        let generation = last_generation
                            .fetch_add(1, Ordering::SeqCst)
                            .saturating_add(1);
                        let status = worker.state().status();
                        observe_explorer_state(&thread_explorer_status, worker.state());
                        failure_emitter.emit_failed(status, generation, &module_error);
                        let _ = failure_sender.send(module_error);
                    }
                    Err(_) => {
                        let module_error = adapter_error(
                            "writable-worker-panic",
                            "writable Cloud Files worker panicked",
                        );
                        let generation = last_generation
                            .fetch_add(1, Ordering::SeqCst)
                            .saturating_add(1);
                        let status = worker.state().status();
                        observe_explorer_state(&thread_explorer_status, worker.state());
                        failure_emitter.emit_failed(status, generation, &module_error);
                        let _ = failure_sender.send(module_error);
                    }
                }
                thread_alive.store(false, Ordering::SeqCst);
                unsafe {
                    let _ = windows::Win32::System::Threading::SetEvent(thread_stopped_event.0.0);
                }
            })
            .map_err(|error| adapter_error("writable-worker-spawn", error.to_string()))?;
        Ok(Self {
            cancelled,
            wake,
            alive,
            failure,
            progress,
            cursor_recovery_progress,
            cursor_recovery,
            cursor_recovery_terminal_observed: AtomicBool::new(false),
            cursor_recovery_finalize: cursor_recovery_finalize_sender,
            worker_join: Some(worker_join),
            stopped_event,
            explorer_status,
        })
    }

    pub(super) fn enable_explorer_status(&self) {
        enable_explorer_status(&self.explorer_status);
    }

    pub(super) fn wait_for_stop_or_cancellation(
        &self,
        cancellation_event_handle: u64,
        timeout: Option<Duration>,
    ) -> Result<WindowsWritableWorkerWait, CloudFilesModuleError> {
        if cancellation_event_handle == 0 {
            return Err(adapter_error(
                "mount-cancellation-event",
                "host supplied a null Cloud Files cancellation event handle",
            ));
        }
        let timeout = timeout
            .map(|timeout| timeout.as_millis().min(u32::MAX as u128) as u32)
            .unwrap_or(INFINITE);
        let cancellation = HANDLE(cancellation_event_handle as usize as *mut c_void);
        let result = unsafe {
            WaitForMultipleObjects(&[cancellation, self.stopped_event.0.0], false, timeout)
        };
        if result == WAIT_OBJECT_0 {
            Ok(WindowsWritableWorkerWait::Cancelled)
        } else if result.0 == WAIT_OBJECT_0.0 + 1 {
            Ok(WindowsWritableWorkerWait::Stopped)
        } else if result == WAIT_TIMEOUT {
            Ok(WindowsWritableWorkerWait::TimedOut)
        } else {
            Err(adapter_error(
                "mount-reactive-wait",
                format!(
                    "wait for Cloud Files cancellation/worker event returned {}",
                    result.0
                ),
            ))
        }
    }

    pub(super) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub(super) fn take_failure(&self) -> Option<CloudFilesModuleError> {
        self.failure.try_recv().ok()
    }

    /// Wait for one complete post-startup worker pass. Registration may become
    /// durable Ready only after this acknowledgement; thread spawn alone is
    /// not a liveness proof.
    pub(super) fn wait_for_first_progress(
        &self,
        timeout: Duration,
    ) -> Result<u64, CloudFilesModuleError> {
        match self.progress.recv_timeout(timeout) {
            Ok(generation) => Ok(generation),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(self.take_failure().unwrap_or_else(|| {
                    adapter_error(
                        "writable-worker-stopped",
                        "writable worker stopped before completing its first pass",
                    )
                }))
            }
            Err(mpsc::RecvTimeoutError::Timeout) => Err(adapter_error(
                "writable-worker-progress-timeout",
                format!(
                    "writable worker made no complete progress within {} ms",
                    timeout.as_millis()
                ),
            )),
        }
    }

    /// Wait until each authoritative depth-0 scan quantum is durably applied,
    /// the terminal nonempty cursor boundary is installed, and the exact
    /// immediate-root placeholder audit passes. `stall_timeout` is refreshed
    /// after every successful scan quantum rather than bounding total root size.
    pub(super) fn wait_for_cursor_recovery_terminal(
        &self,
        stall_timeout: Duration,
    ) -> Result<u64, CloudFilesModuleError> {
        loop {
            match self.cursor_recovery_progress.recv_timeout(stall_timeout) {
                Ok(CursorRecoveryWorkerProgress::Advanced(_generation)) => continue,
                Ok(CursorRecoveryWorkerProgress::Terminal(generation)) => {
                    self.cursor_recovery_terminal_observed
                        .store(true, Ordering::SeqCst);
                    return Ok(generation);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(self.take_failure().unwrap_or_else(|| {
                        adapter_error(
                            "cursor-recovery-worker-stopped",
                            "writable worker stopped before cursor recovery became terminal",
                        )
                    }));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(adapter_error(
                        "cursor-recovery-progress-timeout",
                        format!(
                            "cursor recovery made no complete scan progress within {} ms",
                            stall_timeout.as_millis()
                        ),
                    ));
                }
            }
        }
    }

    /// Release a proven terminal worker only after the owner has durably
    /// completed the recovery marker/registration transition.
    pub(super) fn release_cursor_recovery(&self) -> Result<(), CloudFilesModuleError> {
        let control = self.cursor_recovery.as_ref().ok_or_else(|| {
            adapter_error(
                "cursor-recovery-not-armed",
                "writable worker was not opened for authorized cursor recovery",
            )
        })?;
        if !self
            .cursor_recovery_terminal_observed
            .load(Ordering::SeqCst)
        {
            return Err(adapter_error(
                "cursor-recovery-terminal-not-observed",
                "cursor recovery cannot be released before its terminal waiter succeeds",
            ));
        }
        control
            .release()
            .map_err(|detail| adapter_error("cursor-recovery-release-rejected", detail))?;
        self.wake.signal_control();
        Ok(())
    }

    pub(super) fn complete_cursor_recovery_after_host_active(
        &self,
    ) -> Result<(), CloudFilesModuleError> {
        self.cursor_recovery
            .as_ref()
            .ok_or_else(|| {
                adapter_error(
                    "cursor-recovery-not-armed",
                    "writable worker was not opened for authorized cursor recovery",
                )
            })?
            .complete_after_host_active()
            .map_err(|detail| {
                adapter_error("cursor-recovery-active-completion-rejected", detail)
            })?;
        self.wake.signal_control();
        Ok(())
    }

    pub(super) fn finalize_cursor_recovery_after_host_ready(
        &self,
        timeout: Duration,
    ) -> Result<bool, CloudFilesModuleError> {
        if !self
            .cursor_recovery_terminal_observed
            .load(Ordering::SeqCst)
        {
            return Err(adapter_error(
                "cursor-recovery-terminal-not-observed",
                "cursor recovery cannot finalize before its terminal waiter succeeds",
            ));
        }
        let (reply, response) = mpsc::sync_channel(1);
        self.cursor_recovery_finalize.send(reply).map_err(|_| {
            adapter_error(
                "cursor-recovery-worker-stopped",
                "cursor recovery worker stopped before marker finalization",
            )
        })?;
        self.wake.signal_control();
        response.recv_timeout(timeout).map_err(|error| {
            adapter_error(
                "cursor-recovery-finalize-timeout",
                format!("cursor recovery marker finalization did not complete: {error}"),
            )
        })?
    }

    pub(super) fn stop_and_join(&mut self) -> Result<(), CloudFilesModuleError> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.wake.signal_control();
        if let Some(join) = self.worker_join.as_ref() {
            // Interrupt a worker blocked in a synchronous local filesystem or
            // CFAPI call. Routed DFS calls carry their own hard deadline and
            // staged readers observe the same cancellation flag.
            let thread_handle = HANDLE(join.as_raw_handle());
            unsafe {
                let _ = CancelSynchronousIo(thread_handle);
            }
        }
        if let Some(join) = self.worker_join.take() {
            join.join().map_err(|_| {
                adapter_error("writable-worker-join", "writable worker thread panicked")
            })?;
        }
        self.alive.store(false, Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for WindowsWritableWorkerHandle {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct WindowsFilesystemTestDir(PathBuf);

    impl WindowsFilesystemTestDir {
        fn new(label: &str) -> Self {
            let suffix = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("sinomo-{label}-{}-{suffix}", std::process::id()));
            fs::create_dir_all(&path).expect("create Windows filesystem test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for WindowsFilesystemTestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn raw_batch(changes: Vec<CloudFilesDirectoryChange>) -> RawDirectoryChangeBatch {
        RawDirectoryChangeBatch {
            changes,
            overflowed: false,
        }
    }

    #[test]
    fn one_coalesced_wake_drains_every_queued_directory_change_batch() {
        let (sender, receiver) = mpsc::sync_channel(DIRECTORY_CHANGE_CHANNEL_CAPACITY);
        sender
            .send(raw_batch(vec![CloudFilesDirectoryChange::Added(
                PathBuf::from("first.txt"),
            )]))
            .expect("queue first directory-change batch");
        sender
            .send(raw_batch(vec![CloudFilesDirectoryChange::Modified(
                PathBuf::from("second.txt"),
            )]))
            .expect("queue second directory-change batch");
        sender
            .send(raw_batch(vec![CloudFilesDirectoryChange::Added(
                PathBuf::from("third.txt"),
            )]))
            .expect("queue third directory-change batch");

        let drained = drain_directory_change_batches(&receiver)
            .expect("drain every batch represented by one coalesced wake");
        assert!(!drained.overflowed);
        assert_eq!(
            drained.changes,
            vec![
                CloudFilesDirectoryChange::Added(PathBuf::from("first.txt")),
                CloudFilesDirectoryChange::Modified(PathBuf::from("second.txt")),
                CloudFilesDirectoryChange::Added(PathBuf::from("third.txt")),
            ]
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn coalesced_directory_change_backlog_over_budget_requires_full_scan() {
        let (sender, receiver) = mpsc::sync_channel(DIRECTORY_CHANGE_CHANNEL_CAPACITY);
        sender
            .send(raw_batch(
                (0..DIRECTORY_CHANGES_PER_PASS)
                    .map(|index| {
                        CloudFilesDirectoryChange::Added(PathBuf::from(format!(
                            "first-{index}.txt"
                        )))
                    })
                    .collect(),
            ))
            .expect("queue bounded directory-change batch");
        sender
            .send(raw_batch(vec![CloudFilesDirectoryChange::Added(
                PathBuf::from("over-budget.txt"),
            )]))
            .expect("queue over-budget directory-change batch");

        let drained = drain_directory_change_batches(&receiver)
            .expect("classify over-budget coalesced backlog");
        assert!(drained.overflowed);
        assert!(drained.changes.is_empty());
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
    }

    fn test_stat(path: &str, kind: DfsObjectKind) -> dfs::DfsStat {
        dfs::DfsStat {
            file_system_id: dfs::sifs_file_system_id(),
            object_id: sinomo_runtime::core::Id::derived(
                "test.cloud-files.materialized-directory.object",
                path.as_bytes(),
            ),
            path: path.to_string(),
            kind,
            version: sinomo_runtime::core::Id::derived(
                "test.cloud-files.materialized-directory.version",
                path.as_bytes(),
            ),
            content_hash: None,
            size_bytes: None,
            link_count: 1,
            mode: 0o755,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 3,
            changed_at_unix_nanos: 4,
            policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: Vec::new(),
        }
    }

    #[test]
    fn materialized_directory_scan_reintroduces_its_parent_manifest_entry() {
        let path = "/services/sinomo-agent/channels";
        let entry =
            materialized_directory_manifest_entry(test_stat(path, DfsObjectKind::Directory))
                .expect("materialized directory is projectable before its children");
        assert_eq!(entry.path, path);
        assert_eq!(entry.kind, "directory");

        let error = materialized_directory_manifest_entry(test_stat(path, DfsObjectKind::File))
            .expect_err("a stale materialization path cannot silently become a file");
        assert!(error.contains("resolved as File"), "{error}");
    }

    #[test]
    fn namespace_event_subscription_treats_idle_as_success() {
        let timeouts = namespace_event_stream_timeouts();
        assert_eq!(timeouts.connect_timeout, Some(Duration::from_secs(3)));
        assert_eq!(timeouts.idle_timeout, None);
    }

    #[test]
    fn unchanged_worker_lifecycle_is_rate_limited_but_transitions_are_immediate() {
        let active = CloudFilesLocalChangeSyncStatus::writable_active(0, 0, 0, 0);
        let changed = CloudFilesLocalChangeSyncStatus::writable_active(1, 0, 0, 0);

        assert!(worker_lifecycle_emission_due(None, &active, None));
        assert!(!worker_lifecycle_emission_due(
            Some(&active),
            &active,
            Some(WORKER_LIFECYCLE_MIN_EMIT_INTERVAL - Duration::from_millis(1)),
        ));
        assert!(worker_lifecycle_emission_due(
            Some(&active),
            &active,
            Some(WORKER_LIFECYCLE_MIN_EMIT_INTERVAL),
        ));
        assert!(worker_lifecycle_emission_due(
            Some(&active),
            &changed,
            Some(Duration::ZERO),
        ));
    }

    #[test]
    fn absent_child_under_ordinary_local_directory_is_a_subtree_collision() {
        let root = WindowsFilesystemTestDir::new("ordinary-ancestor-collision");
        let ordinary = root.path().join("ordinary");
        fs::create_dir(&ordinary).expect("create ordinary local ancestor");
        let child = ordinary.join("remote-child");

        let detail = blocking_local_placeholder_ancestor(root.path(), &child)
            .expect("inspect local ancestors")
            .expect("ordinary ancestor blocks placeholder creation");
        assert!(detail.contains(&ordinary.display().to_string()), "{detail}");
        assert!(detail.contains("not a Cloud Files placeholder"), "{detail}");
    }

    #[test]
    fn idle_worker_gate_blocks_until_an_explicit_notification() {
        let wake = Arc::new(WindowsWorkerWake::default());
        let cancelled = Arc::new(AtomicBool::new(false));
        let (completed, observed) = mpsc::sync_channel(1);
        let thread_wake = Arc::clone(&wake);
        let thread_cancelled = Arc::clone(&cancelled);
        let join = thread::spawn(move || {
            completed
                .send(thread_wake.wait(&thread_cancelled, None))
                .expect("report wake reason");
        });
        assert!(matches!(
            observed.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        wake.signal_namespace_change();
        assert_eq!(
            observed
                .recv_timeout(Duration::from_secs(1))
                .expect("explicit namespace notification wakes worker"),
            WORKER_WAKE_NAMESPACE_CHANGE
        );
        join.join().expect("join reactive worker gate");
    }

    fn directory_change_record(action: u32, name: &[u16]) -> Vec<u8> {
        let mut record = Vec::with_capacity(12 + name.len() * 2);
        record.extend_from_slice(&0u32.to_le_bytes());
        record.extend_from_slice(&action.to_le_bytes());
        record.extend_from_slice(&((name.len() * 2) as u32).to_le_bytes());
        for unit in name {
            record.extend_from_slice(&unit.to_le_bytes());
        }
        record
    }

    #[test]
    fn enumerated_local_path_collapses_redundant_windows_separators() {
        assert_eq!(
            normalize_enumerated_local_path(Path::new(r"D:\sinomo\\users"))
                .expect("normalize enumerated path"),
            PathBuf::from(r"D:\sinomo\users")
        );
    }

    #[test]
    fn cursor_recovery_sentinel_is_stable_exact_uppercase_8dot3() {
        let first = cursor_recovery_sentinel_name("recovery", "token", "nonce", 71, 1);
        let replay = cursor_recovery_sentinel_name("recovery", "token", "nonce", 71, 1);
        let second = cursor_recovery_sentinel_name("recovery", "token", "nonce", 71, 2);
        assert_eq!(first, replay);
        assert_ne!(first, second);
        assert_eq!(first.len(), 12);
        assert_eq!(first.as_bytes()[8], b'.');
        assert!(first.bytes().enumerate().all(|(index, byte)| index == 8
            || byte.is_ascii_digit()
            || (b'A'..=b'F').contains(&byte)));
        assert_eq!(
            cursor_recovery_sentinel_payload("recovery", "token", "nonce", 71, 1).len(),
            32
        );
    }

    #[test]
    fn cursor_recovery_sentinel_batch_preserves_every_unrelated_record() {
        let name = "1234ABCD.EF0";
        let sentinel = PathBuf::from(name);
        let unrelated = PathBuf::from("ordinary.txt");
        let mut state = CursorRecoverySentinelState::new(name);
        let added = classify_cursor_recovery_sentinel_batch(
            raw_batch(vec![
                CloudFilesDirectoryChange::Added(sentinel.clone()),
                CloudFilesDirectoryChange::Modified(sentinel.clone()),
                CloudFilesDirectoryChange::Modified(unrelated.clone()),
            ]),
            &mut state,
        )
        .expect("ordered sentinel add batch");
        assert!(added.added);
        assert!(!added.complete);
        assert_eq!(
            added.preserved.changes,
            vec![CloudFilesDirectoryChange::Modified(unrelated.clone())]
        );

        let removed = classify_cursor_recovery_sentinel_batch(
            raw_batch(vec![
                CloudFilesDirectoryChange::Removed(sentinel),
                CloudFilesDirectoryChange::Added(PathBuf::from("after-cut")),
            ]),
            &mut state,
        )
        .expect("ordered sentinel removal batch");
        assert!(!removed.added);
        assert!(removed.complete);
        assert_eq!(
            removed.preserved.changes,
            vec![CloudFilesDirectoryChange::Added(PathBuf::from("after-cut"))]
        );
    }

    #[test]
    fn cursor_recovery_sentinel_consumes_added_modified_removed_in_one_batch() {
        let name = "1234ABCD.EF0";
        let sentinel = PathBuf::from(name);
        let before = CloudFilesDirectoryChange::Added(PathBuf::from("before"));
        let after = CloudFilesDirectoryChange::Removed(PathBuf::from("after"));
        let mut state = CursorRecoverySentinelState::new(name);
        let outcome = classify_cursor_recovery_sentinel_batch(
            raw_batch(vec![
                before.clone(),
                CloudFilesDirectoryChange::Added(sentinel.clone()),
                CloudFilesDirectoryChange::Modified(sentinel.clone()),
                CloudFilesDirectoryChange::Removed(sentinel),
                after.clone(),
            ]),
            &mut state,
        )
        .expect("complete ordered sentinel batch");
        assert!(outcome.added);
        assert!(outcome.complete);
        assert_eq!(outcome.preserved.changes, vec![before, after]);
    }

    #[test]
    fn cursor_recovery_sentinel_rejects_aliases_and_out_of_order_records() {
        let name = "1234ABCD.EF0";
        let mut state = CursorRecoverySentinelState::new(name);
        assert_eq!(
            state
                .observe(&CloudFilesDirectoryChange::Added(PathBuf::from(
                    "1234abcd.ef0"
                )))
                .expect("case alias is ordinary namespace work"),
            SentinelObservation::NonSentinel
        );
        assert!(
            state
                .observe(&CloudFilesDirectoryChange::Modified(PathBuf::from(name)))
                .is_err()
        );

        let mut duplicate = CursorRecoverySentinelState::new(name);
        duplicate
            .observe(&CloudFilesDirectoryChange::Added(PathBuf::from(name)))
            .expect("first add");
        assert!(
            duplicate
                .observe(&CloudFilesDirectoryChange::Added(PathBuf::from(name)))
                .is_err()
        );

        let mut renamed = CursorRecoverySentinelState::new(name);
        assert!(
            renamed
                .observe(&CloudFilesDirectoryChange::Renamed {
                    from: PathBuf::from(name),
                    to: PathBuf::from("OTHER.TMP"),
                })
                .is_err()
        );
    }

    #[test]
    fn directory_change_parser_rejects_unknown_actions_and_invalid_utf16() {
        let mut pairing = CloudFilesRenamePairingState::default();
        assert!(
            parse_directory_changes(
                &directory_change_record(0xFFFF_FFFE, &[b'x' as u16]),
                &mut pairing,
            )
            .is_err()
        );

        let mut pairing = CloudFilesRenamePairingState::default();
        assert!(
            parse_directory_changes(
                &directory_change_record(FILE_ACTION_ADDED.0, &[0xD800]),
                &mut pairing,
            )
            .is_err()
        );
    }

    #[test]
    fn final_snapshot_accepts_post_cut_epoch_advance_but_not_identity_drift() {
        assert!(validate_cursor_recovery_final_snapshot(Some(4), 99, 71, 71, 71).is_ok());
        assert!(validate_cursor_recovery_final_snapshot(None, 99, 71, 71, 71).is_err());
        assert!(validate_cursor_recovery_final_snapshot(Some(4), 4, 71, 72, 71).is_err());
        assert!(validate_cursor_recovery_final_snapshot(Some(4), 4, 71, 71, 72).is_err());
    }

    #[test]
    fn stale_sentinel_selection_accepts_one_exact_name_and_rejects_aliases_or_multiple() {
        let specs = vec![
            CursorRecoverySentinelSpec {
                name: "1234ABCD.EF0".to_string(),
                payload: vec![1; 32],
            },
            CursorRecoverySentinelSpec {
                name: "5678ABCD.EF0".to_string(),
                payload: vec![2; 32],
            },
        ];
        let exact = vec![PhysicalDirectoryEntryName {
            name: OsString::from("1234ABCD.EF0"),
            alternate_name: None,
        }];
        assert_eq!(
            select_stale_cursor_recovery_sentinel(&exact, &specs).expect("one exact sentinel"),
            Some(0)
        );

        let case_alias = vec![PhysicalDirectoryEntryName {
            name: OsString::from("1234abcd.ef0"),
            alternate_name: None,
        }];
        assert!(select_stale_cursor_recovery_sentinel(&case_alias, &specs).is_err());

        let long_name_alias = vec![PhysicalDirectoryEntryName {
            name: OsString::from("forged-long-name"),
            alternate_name: Some(OsString::from("1234ABCD.EF0")),
        }];
        assert!(select_stale_cursor_recovery_sentinel(&long_name_alias, &specs).is_err());

        let multiple = vec![
            exact[0].clone(),
            PhysicalDirectoryEntryName {
                name: OsString::from("5678ABCD.EF0"),
                alternate_name: None,
            },
        ];
        assert!(select_stale_cursor_recovery_sentinel(&multiple, &specs).is_err());
    }

    #[test]
    fn stale_sentinel_validation_rejects_forged_or_reparse_entries() {
        let payload = vec![7; 32];
        assert!(
            validate_stale_cursor_recovery_sentinel(
                FILE_ATTRIBUTE_HIDDEN.0,
                payload.len() as u64,
                &payload,
                &payload,
            )
            .is_ok()
        );
        let mut forged = payload.clone();
        forged[0] ^= 1;
        assert!(
            validate_stale_cursor_recovery_sentinel(
                FILE_ATTRIBUTE_HIDDEN.0,
                forged.len() as u64,
                &forged,
                &payload,
            )
            .is_err()
        );
        assert!(
            validate_stale_cursor_recovery_sentinel(
                FILE_ATTRIBUTE_HIDDEN.0 | FILE_ATTRIBUTE_REPARSE_POINT.0,
                payload.len() as u64,
                &payload,
                &payload,
            )
            .is_err()
        );
        assert!(
            validate_stale_cursor_recovery_sentinel(0, payload.len() as u64, &payload, &payload,)
                .is_err()
        );
    }

    #[test]
    fn stale_sentinel_same_handle_cleanup_succeeds_on_windows_filesystem() {
        let root = WindowsFilesystemTestDir::new("cursor-recovery-stale-sentinel");
        let root_wide = wide_path(root.path());
        let root_handle = unsafe {
            CreateFileW(
                PCWSTR(root_wide.as_ptr()),
                FILE_READ_ATTRIBUTES.0,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                None,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                None,
            )
        }
        .map(OwnedWin32Handle)
        .expect("open test root");
        let root_file_id =
            file_id_for_handle(root_handle.0, root.path()).expect("test root file ID");
        drop(root_handle);

        let spec = cursor_recovery_sentinel_spec("recovery", "token", "nonce", root_file_id, 1);
        let stale_path = root.path().join(&spec.name);
        let stale_wide = wide_path(&stale_path);
        let stale_handle = unsafe {
            CreateFileW(
                PCWSTR(stale_wide.as_ptr()),
                GENERIC_READ.0 | GENERIC_WRITE.0,
                FILE_SHARE_MODE(0),
                None,
                CREATE_NEW,
                FILE_ATTRIBUTE_HIDDEN | FILE_FLAG_WRITE_THROUGH,
                None,
            )
        }
        .map(OwnedWin32Handle)
        .expect("create stale sentinel fixture");
        let mut written = 0u32;
        unsafe {
            WriteFile(
                stale_handle.0,
                Some(&spec.payload),
                Some(&mut written),
                None,
            )
        }
        .expect("write stale sentinel fixture");
        assert_eq!(written as usize, spec.payload.len());
        unsafe { FlushFileBuffers(stale_handle.0) }.expect("flush stale sentinel fixture");
        drop(stale_handle);

        assert_eq!(
            recover_stale_cursor_recovery_sentinel(root.path(), root_file_id, &[spec.clone()])
                .expect("clean exact stale sentinel"),
            Some(PathBuf::from(&spec.name))
        );
        assert!(!stale_path.exists());
        assert!(
            select_stale_cursor_recovery_sentinel(
                &physical_directory_entries(root.path()).expect("enumerate test root"),
                &[spec],
            )
            .expect("classify cleaned test root")
            .is_none()
        );
    }

    #[test]
    fn stale_sentinel_removal_batch_is_exact_and_preserves_unrelated_work() {
        let stale = PathBuf::from("1234ABCD.EF0");
        let unrelated = PathBuf::from("ordinary.txt");
        let outcome = classify_stale_cursor_recovery_removal_batch(
            raw_batch(vec![
                CloudFilesDirectoryChange::Modified(unrelated.clone()),
                CloudFilesDirectoryChange::Removed(stale.clone()),
                CloudFilesDirectoryChange::Added(PathBuf::from("after")),
            ]),
            &stale,
        )
        .expect("exact stale removal");
        assert!(outcome.removed);
        assert_eq!(
            outcome.preserved.changes,
            vec![
                CloudFilesDirectoryChange::Modified(unrelated),
                CloudFilesDirectoryChange::Added(PathBuf::from("after")),
            ]
        );
        assert!(
            classify_stale_cursor_recovery_removal_batch(
                raw_batch(vec![CloudFilesDirectoryChange::Added(stale.clone())]),
                &stale,
            )
            .is_err()
        );
        assert!(
            classify_stale_cursor_recovery_removal_batch(
                raw_batch(vec![
                    CloudFilesDirectoryChange::Removed(stale.clone()),
                    CloudFilesDirectoryChange::Removed(stale),
                ]),
                Path::new("1234ABCD.EF0"),
            )
            .is_err()
        );
    }

    #[test]
    fn recovery_tree_audit_accepts_only_authoritative_projectable_descendants() {
        let file = CloudFilesReconciledManifestEntry {
            path: "/root/materialized/nested.txt".to_string(),
            object_id: "object-a".to_string(),
            version: "version-a".to_string(),
            content_hash: Some("blake3:abc".to_string()),
            content_size_bytes: Some(3),
            kind: "file".to_string(),
        };
        let unsupported = CloudFilesReconciledManifestEntry {
            path: "/root/link".to_string(),
            object_id: "object-b".to_string(),
            version: "version-b".to_string(),
            content_hash: None,
            content_size_bytes: None,
            kind: "symlink".to_string(),
        };
        let manifest = BTreeMap::from([
            (file.path.clone(), file.clone()),
            (unsupported.path.clone(), unsupported),
        ]);
        assert_eq!(
            cursor_recovery_authoritative_entry(
                "/root",
                Path::new("materialized/nested.txt"),
                &manifest,
            )
            .expect("accept nested authoritative file"),
            (file.path.clone(), &file),
        );
        assert!(
            cursor_recovery_authoritative_entry("/root", Path::new("extra.txt"), &manifest)
                .is_err()
        );
        assert!(
            cursor_recovery_authoritative_entry("/root", Path::new("link"), &manifest).is_err()
        );
    }
}
