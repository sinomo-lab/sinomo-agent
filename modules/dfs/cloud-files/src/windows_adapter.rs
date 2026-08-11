//! Concrete Windows capture, DFS feed, and Cloud Files effect adapters.
//!
//! This module is a child of `windows_impl`, so the mount and hydration
//! provider share the exact same routed DFS service and placeholder encoder.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::c_void;
use std::fs;
use std::io::Read;
use std::mem::{offset_of, size_of};
use std::os::windows::fs::MetadataExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use sinomo_dfs_cloud_files_api::{
    CloudFilesLocalChangeSyncMode, CloudFilesLocalChangeSyncStatus, CloudFilesModuleError,
    CloudFilesPlaceholderIdentity,
};
use sinomo_runtime::dfs::{self, DfsMessageTransport, DfsObjectKind, NetworkedDfsService};
use windows::Win32::Foundation::{
    ERROR_LOCK_VIOLATION, ERROR_SHARING_VIOLATION, GENERIC_READ, HANDLE,
};
use windows::Win32::Storage::CloudFilters::{
    CF_CONVERT_FLAG_MARK_IN_SYNC, CF_CONVERT_FLAG_NONE, CF_IN_SYNC_STATE_IN_SYNC,
    CF_PLACEHOLDER_INFO_STANDARD, CF_PLACEHOLDER_STANDARD_INFO, CF_UPDATE_FLAG_DEHYDRATE,
    CF_UPDATE_FLAG_MARK_IN_SYNC, CF_UPDATE_FLAG_NONE, CfConvertToPlaceholder, CfGetPlaceholderInfo,
    CfUpdatePlaceholder,
};
use windows::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_ACTION_ADDED, FILE_ACTION_MODIFIED,
    FILE_ACTION_REMOVED, FILE_ACTION_RENAMED_NEW_NAME, FILE_ACTION_RENAMED_OLD_NAME,
    FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_LIST_DIRECTORY,
    FILE_NOTIFY_CHANGE_CREATION, FILE_NOTIFY_CHANGE_DIR_NAME, FILE_NOTIFY_CHANGE_FILE_NAME,
    FILE_NOTIFY_CHANGE_LAST_WRITE, FILE_NOTIFY_CHANGE_SIZE, FILE_READ_ATTRIBUTES,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_WRITE_DATA,
    GetFileInformationByHandle, OPEN_EXISTING, ReadDirectoryChangesW,
};
use windows::Win32::System::IO::{CancelIoEx, CancelSynchronousIo};
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
use crate::windows_sync::{
    CLOUD_FILES_DIRECTORY_PAGE_ENTRIES, CloudFilesCapturedChangeBatch, CloudFilesCapturedContent,
    CloudFilesCapturedLocalChange, CloudFilesConcreteDfsAdapter,
    CloudFilesDfsServiceMutationBackend, CloudFilesDirectoryChange,
    CloudFilesDirectoryChangeAction, CloudFilesDirectoryChangePlan, CloudFilesFullScanSnapshot,
    CloudFilesLocalCaptureRetries, CloudFilesLocalChangeAccelerator,
    CloudFilesMountedMutationHandoff, CloudFilesNamespaceSource, CloudFilesObjectProjection,
    CloudFilesOfflineCreatePlan, CloudFilesRenamePairingState,
    CloudFilesSparsePlaceholderObservation, CloudFilesStartupConflictEvidence,
    CloudFilesStartupScanBatch, CloudFilesWindowsPlaceholderEffect, CloudFilesWritableSyncState,
    CloudFilesWritableSyncWorker, cloud_files_object_projection,
    cloud_files_root_kind_is_supported, cloud_namespace_batch_from_dfs,
    cloud_namespace_cursor_from_dfs, completion_follow_up_request, dfs_namespace_cursor_from_cloud,
    local_capture_retry_delay, next_sparse_known_path, plan_directory_changes,
    plan_owned_offline_create, run_progress_gated_worker_loop,
    sparse_child_directory_is_enumerated, sparse_root_is_enumerated,
    suppression_for_completed_mutation,
};

const DIRECTORY_CHANGE_BUFFER_BYTES: usize = 64 * 1024;
const DIRECTORY_CHANGE_CHANNEL_CAPACITY: usize = 8;
const DIRECTORY_CHANGES_PER_PASS: usize = 64;
const DEFERRED_CAPTURE_RETRIES_MAX: usize = 4_096;
const LOCAL_STARTUP_SCAN_WORK_PER_PASS: usize = 32;
const WORKER_HEARTBEAT_MILLIS: u64 = 1_000;
const SERVICE_LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const SERVICE_LOCK_RETRY: Duration = Duration::from_millis(5);

fn adapter_error(code: &str, detail: impl Into<String>) -> CloudFilesModuleError {
    CloudFilesModuleError {
        code: code.to_string(),
        detail: detail.into(),
    }
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
        let mut removed_paths = Vec::new();
        let mut audited_materialized_directories = Vec::new();

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
                    manifest.insert(entry.path.clone(), entry);
                }
                Ok(_) | Err(dfs::DfsPosixError::NotFound { .. }) => removed_paths.push(path),
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
                        audited_materialized_directories.push(task.path.clone());
                        plan.active_page = Some(WindowsActiveDirectoryPage {
                            task,
                            entries: page.entries.into(),
                            next_cursor: page.next_cursor,
                        });
                    }
                    Err(dfs::DfsPosixError::NotFound { .. })
                    | Err(dfs::DfsPosixError::MissingParent { .. }) => {
                        audited_materialized_directories.push(task.path.clone());
                        removed_paths.push(task.path.clone());
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
                            if should_scan && plan.queued_directories.insert(stat.path.clone()) {
                                plan.directories.push_back(WindowsDirectoryScanTask {
                                    path: stat.path.clone(),
                                    depth: child_depth,
                                    after: None,
                                });
                            }
                        }
                        let entry = manifest_entry_from_stat(stat);
                        manifest.insert(entry.path.clone(), entry);
                    }
                    Ok(_) | Err(dfs::DfsPosixError::NotFound { .. }) => {
                        removed_paths.push(child.path)
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
            removed_paths,
            materialized_directories: audited_materialized_directories,
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

struct DirectoryWatcherThread {
    stop: Arc<AtomicBool>,
    raw_handle: Arc<AtomicIsize>,
    join: Option<JoinHandle<()>>,
}

impl DirectoryWatcherThread {
    fn stop_and_join(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
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
    startup_scan_plan: Option<WindowsLocalStartupScanPlan>,
    startup_conflicts: Vec<CloudFilesStartupConflictEvidence>,
    suppressed_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
    receiver: Option<mpsc::Receiver<RawDirectoryChangeBatch>>,
    notification_overflow: Arc<AtomicBool>,
    watcher_failure: Arc<Mutex<Option<String>>>,
    watcher: Option<DirectoryWatcherThread>,
    capture_retry_epoch: Instant,
    capture_retries: CloudFilesLocalCaptureRetries,
    content_stager: CloudFilesContentStager,
}

impl WindowsDirectoryChangeAccelerator {
    pub(super) fn new(
        sync_root: PathBuf,
        root_path: String,
        _existing_owned_root: bool,
        suppressed_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
        content_stager: CloudFilesContentStager,
    ) -> Self {
        Self {
            sync_root,
            root_path,
            startup_scan_plan: None,
            startup_conflicts: Vec::new(),
            suppressed_paths,
            receiver: None,
            notification_overflow: Arc::new(AtomicBool::new(false)),
            watcher_failure: Arc::new(Mutex::new(None)),
            watcher: None,
            capture_retry_epoch: Instant::now(),
            capture_retries: CloudFilesLocalCaptureRetries::default(),
            content_stager,
        }
    }
}

impl CloudFilesLocalChangeAccelerator for WindowsDirectoryChangeAccelerator {
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
            return Ok(());
        }
        let (sender, receiver) = mpsc::sync_channel(DIRECTORY_CHANGE_CHANNEL_CAPACITY);
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let raw_handle = Arc::new(AtomicIsize::new(0));
        let sync_root = self.sync_root.clone();
        let thread_stop = Arc::clone(&stop);
        let thread_handle = Arc::clone(&raw_handle);
        let notification_overflow = Arc::clone(&self.notification_overflow);
        let watcher_failure = Arc::clone(&self.watcher_failure);
        let join = thread::Builder::new()
            .name("sinomo-cloud-files-directory-changes".to_string())
            .spawn(move || {
                watch_directory_changes(
                    sync_root,
                    thread_stop,
                    thread_handle,
                    sender,
                    ready_sender,
                    notification_overflow,
                    watcher_failure,
                )
            })
            .map_err(|error| format!("spawn ReadDirectoryChangesW worker: {error}"))?;
        match ready_receiver.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let mut watcher = DirectoryWatcherThread {
                    stop,
                    raw_handle,
                    join: Some(join),
                };
                watcher.stop_and_join();
                return Err(error);
            }
            Err(error) => {
                let mut watcher = DirectoryWatcherThread {
                    stop,
                    raw_handle,
                    join: Some(join),
                };
                watcher.stop_and_join();
                return Err(format!(
                    "ReadDirectoryChangesW worker did not become ready: {error}"
                ));
            }
        }
        self.receiver = Some(receiver);
        self.watcher = Some(DirectoryWatcherThread {
            stop,
            raw_handle,
            join: Some(join),
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
        let mut raw = RawDirectoryChangeBatch::default();
        match receiver.try_recv() {
            Ok(batch) => raw = batch,
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => {
                return Err("ReadDirectoryChangesW worker stopped unexpectedly".to_string());
            }
        }
        if raw.overflowed || raw.changes.len() > DIRECTORY_CHANGES_PER_PASS {
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
    let metadata = match fs::symlink_metadata(&local) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        Err(_) => return false,
    };
    query_placeholder_state(&local, metadata.is_dir())
        .ok()
        .flatten()
        .is_some_and(|(in_sync, _)| in_sync)
}

fn watch_directory_changes(
    sync_root: PathBuf,
    stop: Arc<AtomicBool>,
    raw_handle: Arc<AtomicIsize>,
    sender: mpsc::SyncSender<RawDirectoryChangeBatch>,
    ready: mpsc::SyncSender<Result<(), String>>,
    notification_overflow: Arc<AtomicBool>,
    watcher_failure: Arc<Mutex<Option<String>>>,
) {
    let root_wide = wide_path(&sync_root);
    let handle = match unsafe {
        CreateFileW(
            PCWSTR(root_wide.as_ptr()),
            FILE_LIST_DIRECTORY.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
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
            return;
        }
    };
    raw_handle.store(handle.0 as isize, Ordering::SeqCst);
    let _ = ready.send(Ok(()));
    let owned = OwnedWin32Handle(handle);
    let mut buffer = vec![0u8; DIRECTORY_CHANGE_BUFFER_BYTES];
    let mut rename_pairing = CloudFilesRenamePairingState::default();
    while !stop.load(Ordering::SeqCst) {
        let mut bytes_returned = 0u32;
        let result = unsafe {
            ReadDirectoryChangesW(
                owned.0,
                buffer.as_mut_ptr().cast::<c_void>(),
                buffer.len() as u32,
                true,
                FILE_NOTIFY_CHANGE_FILE_NAME
                    | FILE_NOTIFY_CHANGE_DIR_NAME
                    | FILE_NOTIFY_CHANGE_SIZE
                    | FILE_NOTIFY_CHANGE_LAST_WRITE
                    | FILE_NOTIFY_CHANGE_CREATION,
                Some(&mut bytes_returned),
                None,
                None,
            )
        };
        if let Err(error) = result {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            if let Ok(mut failure) = watcher_failure.lock() {
                *failure = Some(format!("ReadDirectoryChangesW failed: {error}"));
            }
            break;
        }
        if bytes_returned == 0 {
            rename_pairing.reset_for_overflow();
            notification_overflow.store(true, Ordering::SeqCst);
            continue;
        }
        match parse_directory_changes(&buffer[..bytes_returned as usize], &mut rename_pairing) {
            Ok(batch) => match sender.try_send(batch) {
                Ok(()) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    rename_pairing.reset_for_overflow();
                    notification_overflow.store(true, Ordering::SeqCst);
                }
                Err(mpsc::TrySendError::Disconnected(_)) => break,
            },
            Err(error) => {
                if let Ok(mut failure) = watcher_failure.lock() {
                    *failure = Some(error);
                }
                break;
            }
        }
    }
    raw_handle.store(0, Ordering::SeqCst);
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
        let path = PathBuf::from(String::from_utf16_lossy(&name));
        let action = match action {
            value if value == FILE_ACTION_ADDED.0 => Some(CloudFilesDirectoryChangeAction::Added),
            value if value == FILE_ACTION_MODIFIED.0 => {
                Some(CloudFilesDirectoryChangeAction::Modified)
            }
            value if value == FILE_ACTION_REMOVED.0 => {
                Some(CloudFilesDirectoryChangeAction::Removed)
            }
            value if value == FILE_ACTION_RENAMED_OLD_NAME.0 => {
                Some(CloudFilesDirectoryChangeAction::RenamedOldName)
            }
            value if value == FILE_ACTION_RENAMED_NEW_NAME.0 => {
                Some(CloudFilesDirectoryChangeAction::RenamedNewName)
            }
            _ => None,
        };
        if let Some(action) = action {
            rename_pairing.push(action, path, &mut changes);
        }
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
                    let path = entry.path();
                    let relative = path
                        .strip_prefix(sync_root)
                        .map_err(|_| format!("{} escaped the sync root", path.display()))?
                        .to_path_buf();
                    let sifs_path = local_relative_to_sifs(root_path, &relative)?;
                    let metadata = fs::symlink_metadata(&path)
                        .map_err(|error| format!("stat local path {}: {error}", path.display()))?;
                    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
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
                    let is_directory = metadata.is_dir();
                    if is_directory {
                        self.pending_directories.push_back(path.clone());
                    }
                    let placeholder = query_placeholder_state(&path, is_directory)?;
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
    let placeholder = query_placeholder_state(&local_path, metadata.is_dir())?;
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
    is_directory: bool,
) -> Result<Option<(bool, Option<CloudFilesPlaceholderIdentity>)>, String> {
    let wide = wide_path(path);
    let flags = if is_directory {
        FILE_FLAG_BACKUP_SEMANTICS
    } else {
        Default::default()
    };
    let handle = unsafe {
        CreateFileW(
            PCWSTR(wide.as_ptr()),
            FILE_READ_ATTRIBUTES.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            flags,
            None,
        )
    }
    .map(OwnedWin32Handle)
    .map_err(|error| format!("open placeholder {}: {error}", path.display()))?;
    let mut buffer = vec![0u64; (size_of::<CF_PLACEHOLDER_STANDARD_INFO>() + 4096).div_ceil(8)];
    let result = unsafe {
        CfGetPlaceholderInfo(
            handle.0,
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
    Ok(Some((
        info.InSyncState == CF_IN_SYNC_STATE_IN_SYNC,
        decoded,
    )))
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
        let Some((in_sync, observed)) = query_placeholder_state(&local, is_directory)? else {
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
        self.check_cancelled()?;
        let local = self.local_path(path)?;
        if !local.exists() {
            return Ok(());
        }
        let metadata = fs::symlink_metadata(&local)
            .map_err(|error| format!("stat local path {}: {error}", local.display()))?;
        let Some((in_sync, _identity)) = query_placeholder_state(&local, metadata.is_dir())? else {
            return Err(format!(
                "refusing to remove non-placeholder local path {} for a remote delete",
                local.display()
            ));
        };
        if !in_sync {
            return Err(format!(
                "refusing to remove dirty local placeholder {} for a remote delete",
                local.display()
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
        let metadata = fs::symlink_metadata(&source)
            .map_err(|error| format!("stat rename source {}: {error}", source.display()))?;
        let Some((in_sync, _)) = query_placeholder_state(&source, metadata.is_dir())? else {
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
        if query_placeholder_state(&local, is_directory)?.is_none() {
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
        let metadata = match fs::symlink_metadata(&local) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(CloudFilesLocalPathState::Clean);
            }
            Err(error) => return Err(format!("stat local path {}: {error}", local.display())),
        };
        match query_placeholder_state(&local, metadata.is_dir())? {
            Some((true, _)) => Ok(CloudFilesLocalPathState::Clean),
            Some((false, _)) => Ok(CloudFilesLocalPathState::Dirty {
                detail: "Cloud Files placeholder is not in sync".to_string(),
            }),
            None => Ok(CloudFilesLocalPathState::Dirty {
                detail: "local path is not a Cloud Files placeholder".to_string(),
            }),
        }
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
    fn apply_full_scan(&mut self, snapshot: &CloudFilesFullScanSnapshot) -> Result<(), String> {
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
                if matches!(
                    self.local_path_state(&entry.path)?,
                    CloudFilesLocalPathState::Clean
                ) {
                    self.remove_local_path(&entry.path)?;
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
            self.remove_local_path(path)?;
        }
        Ok(())
    }

    fn drain_namespace_suppressions(&mut self) -> Vec<CloudFilesNamespaceSuppression> {
        std::mem::take(&mut self.namespace_suppressions)
            .into_values()
            .collect()
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
    pub prepopulate_depth: u32,
    pub materialized_directories: Arc<Mutex<BTreeSet<String>>>,
}

pub(super) fn build_writable_worker<T: DfsMessageTransport>(
    state: CloudFilesWritableSyncState,
    service: Arc<Mutex<NetworkedDfsService<T>>>,
    write_options: dfs::DfsWriteOptions,
    mounted_mutation_handoff: Arc<CloudFilesMountedMutationHandoff>,
    options: WindowsWritableWorkerOptions,
) -> (ConcreteWindowsWritableWorker<T>, Arc<AtomicBool>) {
    let materialization_audit_intents = state.materialization_audit_intents();
    let WindowsWritableWorkerOptions {
        sync_root,
        root_path,
        existing_owned_root,
        prepopulate_depth,
        materialized_directories,
    } = options;
    let suppressed_paths = Arc::new(Mutex::new(BTreeSet::new()));
    let content_stager = state.queue().content_stager();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker = CloudFilesWritableSyncWorker::new(
        state,
        WindowsDirectoryChangeAccelerator::new(
            sync_root.clone(),
            root_path.clone(),
            existing_owned_root,
            Arc::clone(&suppressed_paths),
            content_stager.clone(),
        ),
        CloudFilesConcreteDfsAdapter::new(
            CloudFilesDfsServiceMutationBackend::new_with_cancellation(
                Arc::clone(&service),
                write_options,
                Arc::clone(&cancelled),
                Arc::clone(&mounted_mutation_handoff),
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
    (worker, cancelled)
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

pub(super) struct WindowsWritableWorkerHandle {
    cancelled: Arc<AtomicBool>,
    alive: Arc<AtomicBool>,
    failure: mpsc::Receiver<CloudFilesModuleError>,
    progress: mpsc::Receiver<u64>,
    worker_join: Option<JoinHandle<()>>,
}

impl WindowsWritableWorkerHandle {
    pub(super) fn spawn<T>(
        mut worker: ConcreteWindowsWritableWorker<T>,
        cancelled: Arc<AtomicBool>,
        emitter: WorkerLifecycleEmitter,
        initial_generation: u64,
    ) -> Result<Self, CloudFilesModuleError>
    where
        T: DfsMessageTransport + Send + 'static,
    {
        let alive = Arc::new(AtomicBool::new(true));
        let thread_cancelled = Arc::clone(&cancelled);
        let thread_alive = Arc::clone(&alive);
        let failure_emitter = emitter.clone();
        let (failure_sender, failure) = mpsc::channel();
        let (progress_sender, progress) = mpsc::sync_channel(1);
        let worker_join = thread::Builder::new()
            .name("sinomo-cloud-files-writable-sync".to_string())
            .spawn(move || {
                let progress_emitter = emitter;
                let last_generation = AtomicU64::new(initial_generation);
                let run_result = catch_unwind(AssertUnwindSafe(|| {
                    run_progress_gated_worker_loop(
                        &thread_cancelled,
                        initial_generation,
                        || {
                            worker
                                .run_once(&thread_cancelled)
                                .map(|_| worker.state().status())
                        },
                        |status, generation| {
                            let ready = status.mode == CloudFilesLocalChangeSyncMode::Active;
                            last_generation.store(generation, Ordering::SeqCst);
                            progress_emitter.emit(status, true, ready, generation);
                            if ready {
                                let _ = progress_sender.try_send(generation);
                            }
                        },
                        |cancelled| {
                            for _ in 0..(WORKER_HEARTBEAT_MILLIS / 50) {
                                if cancelled.load(Ordering::SeqCst) {
                                    break;
                                }
                                thread::sleep(std::time::Duration::from_millis(50));
                            }
                        },
                    )
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
                        failure_emitter.emit_failed(status, generation, &module_error);
                        let _ = failure_sender.send(module_error);
                    }
                }
                thread_alive.store(false, Ordering::SeqCst);
            })
            .map_err(|error| adapter_error("writable-worker-spawn", error.to_string()))?;
        Ok(Self {
            cancelled,
            alive,
            failure,
            progress,
            worker_join: Some(worker_join),
        })
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

    pub(super) fn stop_and_join(&mut self) -> Result<(), CloudFilesModuleError> {
        self.cancelled.store(true, Ordering::SeqCst);
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
