use crate::{
    DfsFuseError, DfsFuseMountHandle, DfsFuseMountOptions, Result, join_sifs_child,
    normalize_sifs_root, parent_sifs_path, prepare_background_mount_point,
};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    TimeOrNow,
};
use sinomo_runtime::dfs::{
    self, DfsError, DfsObjectKind, DfsPosixCreateAttrs, DfsPosixError, DfsWriteOptions,
};
use sinomo_runtime::local::SinomoDataDir;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError, Weak};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ROOT_INO: u64 = 1;
const DEFAULT_BLOCK_SIZE: u32 = 4096;
const LOCK_RENEW_INTERVAL: Duration = Duration::from_secs(5);
const BLOCKING_LOCK_POLL_INTERVAL: Duration = Duration::from_millis(500);
const FUSE_LOCK_RPC_TIMEOUT: Duration = Duration::from_secs(10);
const FUSE_LOCK_QUEUE_POLL_INTERVAL: Duration = Duration::from_millis(2);
const LOCKED_IO_MIN_LEASE: Duration = Duration::from_secs(60);
const MAX_ACTIVE_LOCK_KEYS_PER_MOUNT: usize = 2;
const FUSE_CREATE_RACE_RETRY_ROUNDS: usize = 4;
const MIN_FUSE_READ_WORKERS: usize = 4;
const MAX_FUSE_READ_WORKERS: usize = 16;
const FUSE_READ_QUEUE_DEPTH_PER_WORKER: usize = 4;
// Bound acknowledged-but-not-yet-published data per descriptor. Ordinary
// Linux FUSE writes arrive as much smaller kernel chunks, so this keeps a
// sequential transfer to one canonical DFS publication without allowing an
// idle descriptor to retain unbounded mount memory.
const MAX_DIRTY_WRITE_BYTES_PER_HANDLE: usize = 64 * 1024 * 1024;
static NEXT_MOUNT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
type SifsService = dfs::MountedSifsService;
type FuseReadJob = Box<dyn FnOnce() + Send + 'static>;

struct FuseReadExecutor {
    sender: Option<SyncSender<FuseReadJob>>,
    workers: Vec<thread::JoinHandle<()>>,
    cancellation: dfs::DfsCancellationFlag,
}

impl std::fmt::Debug for FuseReadExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FuseReadExecutor")
            .field("worker_count", &self.workers.len())
            .finish_non_exhaustive()
    }
}

impl FuseReadExecutor {
    fn new(cancellation: dfs::DfsCancellationFlag) -> io::Result<Self> {
        let available = thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(MIN_FUSE_READ_WORKERS);
        let worker_count = available
            .saturating_mul(2)
            .clamp(MIN_FUSE_READ_WORKERS, MAX_FUSE_READ_WORKERS);
        Self::with_worker_count(worker_count, cancellation)
    }

    fn with_worker_count(
        worker_count: usize,
        cancellation: dfs::DfsCancellationFlag,
    ) -> io::Result<Self> {
        let worker_count = worker_count.max(1);
        let queue_depth = worker_count.saturating_mul(FUSE_READ_QUEUE_DEPTH_PER_WORKER);
        let (sender, receiver) = mpsc::sync_channel::<FuseReadJob>(queue_depth);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers: Vec<thread::JoinHandle<()>> = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let worker = match thread::Builder::new()
                .name(format!("sinomo-fuse-read-{index}"))
                .spawn(move || {
                    loop {
                        let received = receiver
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .recv();
                        let Ok(job) = received else {
                            break;
                        };
                        // A single failed request must not silently reduce the
                        // bounded pool's capacity for the rest of the mount.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                    }
                }) {
                Ok(worker) => worker,
                Err(error) => {
                    drop(sender);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(error);
                }
            };
            workers.push(worker);
        }
        Ok(Self {
            sender: Some(sender),
            workers,
            cancellation,
        })
    }

    fn submit(&self, job: FuseReadJob) -> std::result::Result<(), FuseReadJob> {
        let Some(sender) = self.sender.as_ref() else {
            return Err(job);
        };
        sender.send(job).map_err(|error| error.0)
    }
}

impl Drop for FuseReadExecutor {
    fn drop(&mut self) {
        // Cancel before closing and joining. Jobs which were accepted but have
        // not started must not each spend a remote timeout during teardown.
        self.cancellation.cancel();
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

pub fn mount_sifs(mut options: DfsFuseMountOptions) -> Result<()> {
    options.root_path = normalize_sifs_root(&options.root_path)?;
    let mount_helper = fuse_mount_helper();
    let mount_point = options.mount_point.clone();
    let fs = SifsFuseFileSystem::open(options.clone())?;
    let mount_options = fuse_mount_options(&options, mount_helper.is_some());
    fuser::mount2(fs, &mount_point, &mount_options)
        .map_err(|source| map_mount_error(&mount_point, mount_helper.is_some(), source))
}

pub fn mount_sifs_background(mut options: DfsFuseMountOptions) -> Result<DfsFuseMountHandle> {
    options.root_path = normalize_sifs_root(&options.root_path)?;
    let mount_helper = fuse_mount_helper();
    recover_disconnected_mount_point(&options.mount_point, mount_helper.as_deref())?;
    let mount_point = prepare_background_mount_point(&options.mount_point)?;
    options.mount_point = mount_point.clone();
    let fs = SifsFuseFileSystem::open(options.clone())?;
    let mount_options = background_fuse_mount_options(&options, mount_helper.is_some());
    let session = fuser::spawn_mount2(fs, &mount_point, &mount_options)
        .map_err(|source| map_mount_error(&mount_point, mount_helper.is_some(), source))?;
    Ok(DfsFuseMountHandle::new(mount_point, session))
}

fn recover_disconnected_mount_point(path: &Path, mount_helper: Option<&Path>) -> Result<()> {
    let Err(source) = std::fs::metadata(path) else {
        return Ok(());
    };
    if !is_disconnected_mount_error(&source) {
        return Ok(());
    }
    let Some(mount_helper) = mount_helper else {
        return Err(DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: format!(
                "stale disconnected FUSE mount requires `fusermount3` or `fusermount` cleanup: {source}"
            ),
        });
    };
    let status = Command::new(mount_helper)
        .arg("-u")
        .arg("-z")
        .arg(path)
        .status()
        .map_err(|cleanup_error| DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: format!(
                "failed to start stale FUSE mount cleanup with {}: {cleanup_error}",
                mount_helper.display()
            ),
        })?;
    if status.success() {
        Ok(())
    } else {
        Err(DfsFuseError::Mount {
            mount_point: path.display().to_string(),
            detail: format!(
                "stale FUSE mount cleanup with {} exited with {status}",
                mount_helper.display()
            ),
        })
    }
}

fn is_disconnected_mount_error(source: &io::Error) -> bool {
    source.raw_os_error() == Some(libc::ENOTCONN)
}

fn fuse_mount_options(
    options: &DfsFuseMountOptions,
    mount_helper_available: bool,
) -> Vec<MountOption> {
    let mut mount_options = vec![
        MountOption::FSName(options.mount_name.clone()),
        MountOption::DefaultPermissions,
    ];
    // fuser implements AutoUnmount through fusermount by implicitly enabling
    // `allow_other`. Only request it when a helper is available and the caller
    // explicitly opted in, keeping default foreground mounts owner-only.
    if mount_helper_available && options.allow_other {
        mount_options.push(MountOption::AutoUnmount);
    }
    if options.read_only {
        mount_options.push(MountOption::RO);
    }
    if options.allow_other {
        mount_options.push(MountOption::AllowOther);
    }
    mount_options
}

fn background_fuse_mount_options(
    options: &DfsFuseMountOptions,
    mount_helper_available: bool,
) -> Vec<MountOption> {
    // BackgroundSession owns the mount and unmounts it when dropped. Do not
    // force AutoUnmount for an owner-only mount: fuser implements that option
    // by implicitly adding allow_other, which fails on standard installations
    // without `user_allow_other` in /etc/fuse.conf and silently widens the
    // kernel mount request beyond the caller's intent.
    fuse_mount_options(options, mount_helper_available)
}

fn fuse_mount_helper() -> Option<PathBuf> {
    fuse_mount_helper_on_path(std::env::var_os("PATH").as_deref()).or_else(|| {
        ["/bin/fusermount3", "/bin/fusermount"]
            .into_iter()
            .map(PathBuf::from)
            .find(|candidate| is_executable_file(candidate))
    })
}

fn fuse_mount_helper_on_path(path: Option<&OsStr>) -> Option<PathBuf> {
    let path = path?;
    std::env::split_paths(path).find_map(|directory| {
        ["fusermount3", "fusermount"]
            .into_iter()
            .map(|name| directory.join(name))
            .find(|candidate| is_executable_file(candidate))
    })
}

fn is_executable_file(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn map_mount_error(
    mount_point: &Path,
    mount_helper_available: bool,
    source: io::Error,
) -> DfsFuseError {
    let detail = if !mount_point.exists() {
        format!("mount point does not exist: {source}")
    } else if !Path::new("/dev/fuse").exists() {
        format!("/dev/fuse is unavailable; load or enable the FUSE kernel device: {source}")
    } else if !mount_helper_available
        && matches!(
            source.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied
        )
    {
        format!(
            "direct /dev/fuse mounting was not permitted and no executable `fusermount3` or \
             `fusermount` helper was found; grant the service mount capability or install a FUSE \
             helper: {source}"
        )
    } else {
        source.to_string()
    };
    DfsFuseError::Mount {
        mount_point: mount_point.display().to_string(),
        detail,
    }
}

#[derive(Debug)]
struct SifsFuseFileSystem {
    dfs: Arc<Mutex<SifsService>>,
    content_reads: dfs::MountedSifsContentReadContext,
    read_cancellation: dfs::DfsCancellationFlag,
    read_executor: FuseReadExecutor,
    state: Arc<Mutex<FuseState>>,
    read_only: bool,
    attr_ttl: Duration,
    entry_ttl: Duration,
    write_replica_candidates: Vec<dfs::DfsNodeId>,
    lock_mount_session_id: String,
    lock_request_sequence: Arc<AtomicU64>,
    lock_renewer_stop: Arc<AtomicBool>,
}

impl SifsFuseFileSystem {
    fn open(options: DfsFuseMountOptions) -> Result<Self> {
        let data_dir = match options.data_dir {
            Some(path) => SinomoDataDir::new(path),
            None => SinomoDataDir::from_env_or_home()
                .map_err(|source| DfsFuseError::LocalData(source.to_string()))?,
        };
        let dfs::OpenedMountedSifsService {
            mut service,
            replica_candidates,
        } = if sinomo_runtime::network::ClusterPaths::new(&data_dir)
            .state
            .exists()
        {
            let peer_transport = Arc::new(
                sinomo_runtime::api::DaemonDfsPeerTransport::connect(data_dir.clone())
                    .map_err(|source| DfsFuseError::Dfs(source.to_string()))?,
            );
            let sync_peer_transport: Arc<dyn dfs::DfsPeerTransport> = peer_transport.clone();
            let async_peer_transport: Arc<dyn dfs::AsyncDfsPeerTransport> = peer_transport;
            dfs::open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
                data_dir,
                sync_peer_transport,
                async_peer_transport,
            )
            .map_err(|source| DfsFuseError::Dfs(source.to_string()))?
        } else {
            // A standalone one-node data directory has no daemon-owned cluster
            // transport to share, so retain the local-only mounted service.
            dfs::open_mounted_sifs_service(Some(data_dir.root()))
                .map_err(|source| DfsFuseError::Dfs(source.to_string()))?
        };
        // Resolve the local-only fallback before the service is placed behind
        // the mount-wide mutex. FUSE mutation handlers already hold that
        // mutex while calling `write_options`; trying to recover the local
        // node id there would recursively lock a non-reentrant mutex whenever
        // the cluster currently advertises no content-service candidates.
        let replica_candidates =
            fuse_write_replica_candidates(replica_candidates, service.node_id().clone());
        let read_cancellation = dfs::DfsCancellationFlag::new();
        service.add_cancellation(read_cancellation.clone());
        let content_reads = service.mounted_content_read_context();
        let read_executor = FuseReadExecutor::new(read_cancellation.clone())
            .map_err(|source| DfsFuseError::Dfs(format!("start FUSE read workers: {source}")))?;
        let dfs = Arc::new(Mutex::new(service));
        let state = Arc::new(Mutex::new(FuseState::new(options.root_path.clone())));
        let lock_mount_session_id = format!(
            "fuse-{}-{}-{}",
            std::process::id(),
            system_time_to_nanos(SystemTime::now()),
            NEXT_MOUNT_SESSION_ID.fetch_add(1, Ordering::Relaxed)
        );
        let lock_request_sequence = Arc::new(AtomicU64::new(1));
        let lock_renewer_stop = Arc::new(AtomicBool::new(false));
        spawn_lock_renewer(
            Arc::downgrade(&dfs),
            Arc::downgrade(&state),
            lock_request_sequence.clone(),
            lock_renewer_stop.clone(),
        );
        Ok(Self {
            dfs,
            content_reads,
            read_cancellation,
            read_executor,
            state,
            read_only: options.read_only,
            attr_ttl: options.attr_ttl,
            entry_ttl: options.entry_ttl,
            write_replica_candidates: replica_candidates,
            lock_mount_session_id,
            lock_request_sequence,
            lock_renewer_stop,
        })
    }

    fn lock_state(&self) -> std::result::Result<MutexGuard<'_, FuseState>, libc::c_int> {
        self.state.lock().map_err(|_| libc::EIO)
    }

    fn ensure_handle_lock_not_lost(&self, fh: u64) -> std::result::Result<(), libc::c_int> {
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        if state.has_lost_lock_for_handle(fh) {
            Err(libc::EIO)
        } else {
            Ok(())
        }
    }

    fn ensure_io_lock_not_lost(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        // Drop the state guard before the path fallback, which also consults
        // FuseState. Holding the guard across that fallback would deadlock.
        let object_id = {
            let state = self.lock_state()?;
            state
                .handles
                .get(&fh)
                .map(|handle| handle.object_id.clone())
        };
        let object_id = match object_id {
            Some(object_id) => Some(object_id),
            None => {
                let path = self.path_for_ino(ino)?;
                Some(self.stat_path(&path, true)?.object_id)
            }
        };
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        if state.has_lost_lock_for_handle(fh) {
            return Err(libc::EIO);
        }
        let (Some(lock_owner), Some(object_id)) = (lock_owner, object_id) else {
            return Ok(());
        };
        let key = (object_id, lock_owner);
        if let Some(lost) = state.lost_locks.get_mut(&key) {
            lost.associated_fhs.insert(fh);
            return Err(libc::EIO);
        }
        if let Some(held) = state.held_locks.get_mut(&key) {
            held.associated_fhs.insert(fh);
        }
        if let Some(pending) = state.pending_locks.get_mut(&key) {
            pending.associated_fhs.insert(fh);
        }
        Ok(())
    }

    fn ensure_handle_lock_budget(
        &self,
        fh: u64,
        lock_owner: Option<u64>,
        minimum_remaining: Duration,
    ) -> std::result::Result<(), libc::c_int> {
        self.ensure_handle_lock_not_lost(fh)?;
        let now = Instant::now();
        let renewals = {
            let mut state = self.lock_state()?;
            state.expire_elapsed_locks(now);
            let object_id = state
                .handles
                .get(&fh)
                .map(|handle| handle.object_id.clone())
                .ok_or(libc::EBADF)?;
            state
                .held_locks
                .iter_mut()
                .filter_map(|(key, held)| {
                    let owner_matches =
                        lock_owner.is_some_and(|owner| key.0 == object_id && key.1 == owner);
                    if held.associated_fhs.contains(&fh) || owner_matches {
                        held.associated_fhs.insert(fh);
                        Some((key.clone(), held.clone()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        for (key, held) in renewals {
            let command = dfs::DfsAdvisoryLockCommand::Renew {
                request_id: self.next_lock_request_id("io-budget-renew"),
                token: held.grant.token.clone(),
            };
            let response =
                match run_advisory_lock_command(&self.dfs, &held.path, &held.object_id, command) {
                    Ok(response) => response,
                    Err(_) => {
                        mark_fuse_lock_lost(&self.state, &key, &held.grant);
                        return Err(libc::EIO);
                    }
                };
            let FuseLockCommandResult {
                outcome,
                local_lease_deadline,
            } = response;
            let dfs::DfsAdvisoryLockOutcome::Renewed { grant: renewed } = outcome else {
                mark_fuse_lock_lost(&self.state, &key, &held.grant);
                return Err(libc::EIO);
            };
            let Some(local_deadline) = local_lease_deadline else {
                mark_fuse_lock_lost(&self.state, &key, &held.grant);
                return Err(libc::EIO);
            };
            if local_deadline.saturating_duration_since(Instant::now()) < minimum_remaining {
                mark_fuse_lock_lost(&self.state, &key, &held.grant);
                return Err(libc::EIO);
            }
            let mut state = self.lock_state()?;
            if let Some(current) = state.held_locks.get_mut(&key)
                && current.grant == held.grant
            {
                current.grant = renewed;
                current.local_lease_deadline = local_deadline;
            }
        }
        self.ensure_handle_lock_not_lost(fh)
    }

    fn next_lock_request_id(&self, operation: &str) -> dfs::DfsAdvisoryLockRequestId {
        next_lock_request_id(&self.lock_request_sequence, operation)
    }

    fn lock_session_id(&self, lock_owner: u64) -> dfs::DfsAdvisoryLockSessionId {
        dfs::DfsAdvisoryLockSessionId::new(format!("{}:{lock_owner}", self.lock_mount_session_id))
    }

    fn lock_dfs(&self) -> std::result::Result<MutexGuard<'_, SifsService>, libc::c_int> {
        self.dfs.lock().map_err(|_| libc::EIO)
    }

    fn write_options(&self) -> DfsWriteOptions {
        fuse_write_options(self.write_replica_candidates.clone())
    }

    fn ensure_writable(&self) -> std::result::Result<(), libc::c_int> {
        if self.read_only {
            Err(libc::EROFS)
        } else {
            Ok(())
        }
    }

    fn path_for_ino(&self, ino: u64) -> std::result::Result<String, libc::c_int> {
        self.lock_state()?
            .path_for(ino)
            .ok_or(libc::ENOENT)
            .map(ToOwned::to_owned)
    }

    fn child_path(&self, parent: u64, name: &OsStr) -> std::result::Result<String, libc::c_int> {
        let parent_path = self.path_for_ino(parent)?;
        join_sifs_child(&parent_path, name).map_err(|_| libc::EINVAL)
    }

    fn intern_entry(
        &self,
        path: String,
        object_id: &dfs::DfsObjectId,
    ) -> std::result::Result<u64, libc::c_int> {
        Ok(self.lock_state()?.intern_entry(path, object_id.clone()))
    }

    fn stat_path(
        &self,
        path: &str,
        follow: bool,
    ) -> std::result::Result<dfs::DfsStat, libc::c_int> {
        let mut dfs = self.lock_dfs()?;
        let result = if follow {
            dfs.posix_stat_distributed(path)
        } else {
            dfs.posix_lstat_distributed(path)
        };
        result.map_err(errno_for_posix_error)
    }

    fn reply_entry_for_path(&self, reply: ReplyEntry, path: String, stat: dfs::DfsStat) {
        match self.intern_entry(path, &stat.object_id) {
            Ok(ino) => reply.entry(&self.entry_ttl, &file_attr(ino, &stat), 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn commit_handle(&self, fh: u64) -> std::result::Result<(), libc::c_int> {
        self.ensure_handle_lock_not_lost(fh)?;
        let has_dirty = {
            let state = self.lock_state()?;
            let handle = state.handles.get(&fh).ok_or(libc::EBADF)?;
            !handle.dirty.is_empty()
        };
        if !has_dirty {
            return Ok(());
        }
        self.ensure_handle_lock_budget(fh, None, LOCKED_IO_MIN_LEASE)?;
        let Some(publication) = self.lock_state()?.take_dirty_publication(fh)? else {
            return Ok(());
        };
        match self.publish_dirty_publication(&publication) {
            Ok(stat) => self.refresh_opened_object_stats(&publication.object_id, &stat),
            Err(errno) => {
                // A timeout may be ambiguous, but replaying the same ranges
                // through the stable object id is content-idempotent. Keep the
                // acknowledged bytes available for a later flush/fsync retry.
                self.lock_state()?
                    .restore_dirty_publication(fh, publication)?;
                Err(errno)
            }
        }
    }

    fn publish_dirty_publication(
        &self,
        publication: &FuseDirtyPublication,
    ) -> std::result::Result<dfs::DfsStat, libc::c_int> {
        debug_assert!(!publication.dirty.is_empty());
        let options = self.write_options();
        let mut dfs = self.lock_dfs()?;
        let mut latest = None;
        for (&offset, bytes) in &publication.dirty.ranges {
            latest = Some(
                dfs.posix_write_file_at_for_object(
                    &publication.object_id,
                    offset,
                    bytes,
                    options.clone(),
                )
                .map_err(errno_for_posix_error)?,
            );
        }
        latest.ok_or(libc::EIO)
    }

    fn commit_dirty_handles_for_path(
        &self,
        path: &str,
        include_descendants: bool,
    ) -> std::result::Result<(), libc::c_int> {
        let handles = self
            .lock_state()?
            .dirty_handles_for_path(path, include_descendants);
        for fh in handles {
            self.commit_handle(fh)?;
        }
        Ok(())
    }

    fn commit_dirty_handles_for_object(
        &self,
        object_id: &dfs::DfsObjectId,
        except_fh: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        let handles = self
            .lock_state()?
            .dirty_handles_for_object(object_id, except_fh);
        for fh in handles {
            self.commit_handle(fh)?;
        }
        Ok(())
    }

    fn commit_dirty_handles_for_handle_object(
        &self,
        fh: u64,
    ) -> std::result::Result<dfs::DfsObjectId, libc::c_int> {
        let object_id = self
            .lock_state()?
            .handles
            .get(&fh)
            .ok_or(libc::EBADF)?
            .object_id
            .clone();
        self.commit_dirty_handles_for_object(&object_id, None)?;
        Ok(object_id)
    }

    fn sync_handle(&self, fh: u64, datasync: bool) -> std::result::Result<(), libc::c_int> {
        self.commit_dirty_handles_for_handle_object(fh)?;
        let kind = if datasync {
            dfs::DfsPosixSyncKind::FileData
        } else {
            dfs::DfsPosixSyncKind::File
        };
        let target = self.current_file_sync_target_for_handle(fh, kind)?;
        self.ensure_handle_lock_budget(fh, None, LOCKED_IO_MIN_LEASE)?;
        self.lock_dfs()?
            .posix_sync_file_target(&target)
            .map(drop)
            .map_err(errno_for_posix_error)
    }

    fn current_file_sync_target_for_handle(
        &self,
        fh: u64,
        kind: dfs::DfsPosixSyncKind,
    ) -> std::result::Result<dfs::DfsPosixFileSyncTarget, libc::c_int> {
        let object_id = self
            .lock_state()?
            .handles
            .get(&fh)
            .ok_or(libc::EBADF)?
            .object_id
            .clone();
        self.lock_dfs()?
            .posix_capture_file_sync_target_for_object(&object_id, kind)
            .map_err(errno_for_posix_error)
    }

    fn refresh_opened_object_stats(
        &self,
        object_id: &dfs::DfsObjectId,
        stat: &dfs::DfsStat,
    ) -> std::result::Result<(), libc::c_int> {
        self.lock_state()?
            .refresh_object_handle_stats(object_id, stat)
    }

    fn handle_lock_identity(
        &self,
        ino: u64,
        fh: u64,
    ) -> std::result::Result<(String, dfs::DfsObjectId), libc::c_int> {
        if let Some(handle) = self.lock_state()?.handles.get(&fh) {
            return Ok((handle.path.clone(), handle.object_id.clone()));
        }
        let path = self.path_for_ino(ino)?;
        let stat = self.stat_path(&path, true)?;
        Ok((path, stat.object_id))
    }

    fn query_advisory_lock(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        mode: dfs::DfsAdvisoryLockMode,
    ) -> std::result::Result<Option<dfs::DfsAdvisoryLockConflict>, libc::c_int> {
        let (path, object_id) = self.handle_lock_identity(ino, fh)?;
        let key = (object_id.clone(), lock_owner);
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        if state.lost_locks.contains_key(&key) {
            return Err(libc::EIO);
        }
        drop(state);
        let command = dfs::DfsAdvisoryLockCommand::Query {
            request_id: self.next_lock_request_id("query"),
            session_id: self.lock_session_id(lock_owner),
            mode,
        };
        match run_advisory_lock_command(&self.dfs, &path, &object_id, command)?.outcome {
            dfs::DfsAdvisoryLockOutcome::QueryResult { conflict } => Ok(conflict),
            outcome => Err(errno_for_unexpected_lock_outcome(&outcome)),
        }
    }

    fn set_advisory_lock(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        mode: dfs::DfsAdvisoryLockMode,
        process_id: u32,
        blocking: bool,
        reply: ReplyEmpty,
    ) {
        let (path, object_id) = match self.handle_lock_identity(ino, fh) {
            Ok(identity) => identity,
            Err(errno) => return reply.error(errno),
        };
        let key = (object_id.clone(), lock_owner);
        let (existing, prior_lost_fhs, can_start_lock_key) = match self.lock_state() {
            Ok(mut state) => {
                state.expire_elapsed_locks(Instant::now());
                (
                    state.held_locks.get(&key).cloned(),
                    state
                        .lost_locks
                        .get(&key)
                        .map(|lost| lost.associated_fhs.clone())
                        .unwrap_or_default(),
                    state.can_start_lock_key(&key),
                )
            }
            Err(errno) => return reply.error(errno),
        };
        if let Some(existing) = existing {
            let command = if existing.grant.mode == mode {
                dfs::DfsAdvisoryLockCommand::Renew {
                    request_id: self.next_lock_request_id("setlk-renew"),
                    token: existing.grant.token.clone(),
                }
            } else {
                dfs::DfsAdvisoryLockCommand::Convert {
                    request_id: self.next_lock_request_id("setlk-convert"),
                    token: existing.grant.token.clone(),
                    mode,
                }
            };
            match run_advisory_lock_command(&self.dfs, &path, &object_id, command) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Renewed { grant },
                    local_lease_deadline,
                })
                | Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Converted { grant },
                    local_lease_deadline,
                }) => {
                    let mut associated_fhs = existing.associated_fhs.clone();
                    associated_fhs.extend(prior_lost_fhs.iter().copied());
                    associated_fhs.insert(fh);
                    let held = fuse_held_lock(
                        path,
                        object_id,
                        existing.session_id,
                        grant,
                        local_lease_deadline,
                        associated_fhs.clone(),
                    );
                    let mut state = match self.lock_state() {
                        Ok(state) => state,
                        Err(errno) => return reply.error(errno),
                    };
                    if let Some(held) = held {
                        state.held_locks.insert(key.clone(), held);
                        state.lost_locks.remove(&key);
                        reply.ok();
                    } else {
                        state.held_locks.remove(&key);
                        state.record_lost_lock(key, associated_fhs);
                        reply.error(libc::EIO);
                    }
                }
                // Two shared holders attempting blocking upgrades would both
                // retain their old grant and wait forever. Surface POSIX's
                // deadlock result while keeping the existing lock intact.
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::WouldBlock { .. },
                    ..
                }) if blocking => reply.error(libc::EDEADLK),
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::WouldBlock { .. },
                    ..
                })
                | Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::AuthorityWarming { .. },
                    ..
                }) => reply.error(libc::EAGAIN),
                Ok(response) => {
                    let outcome = response.outcome;
                    if lock_outcome_loses_grant(&outcome)
                        && let Ok(mut state) = self.lock_state()
                    {
                        let mut associated_fhs = state
                            .held_locks
                            .remove(&key)
                            .map(|held| held.associated_fhs)
                            .unwrap_or_default();
                        associated_fhs.extend(prior_lost_fhs.iter().copied());
                        associated_fhs.insert(fh);
                        state.record_lost_lock(key, associated_fhs);
                    }
                    reply.error(errno_for_unexpected_lock_outcome(&outcome));
                }
                Err(errno) => {
                    // After both exact retries fail, a renew or conversion may
                    // have committed remotely. Stop using the grant locally;
                    // its finite server lease bounds any uncertain residue.
                    if let Ok(mut state) = self.lock_state() {
                        let mut associated_fhs = state
                            .held_locks
                            .remove(&key)
                            .map(|held| held.associated_fhs)
                            .unwrap_or_else(|| existing.associated_fhs.clone());
                        associated_fhs.extend(prior_lost_fhs.iter().copied());
                        associated_fhs.insert(fh);
                        state.record_lost_lock(key, associated_fhs);
                    }
                    reply.error(errno);
                }
            }
            return;
        }

        if !can_start_lock_key {
            return reply.error(libc::ENOLCK);
        }

        let acquire_request_id = self.next_lock_request_id("acquire");
        let session_id = self.lock_session_id(lock_owner);
        let command = dfs::DfsAdvisoryLockCommand::Acquire {
            request_id: acquire_request_id.clone(),
            session_id: session_id.clone(),
            mode,
            blocking,
            process_id: Some(process_id),
        };
        if blocking {
            let mut associated_fhs = prior_lost_fhs;
            associated_fhs.insert(fh);
            let pending = FusePendingLock {
                path: path.clone(),
                object_id: object_id.clone(),
                session_id: session_id.clone(),
                acquire_request_id,
                associated_fhs,
            };
            match self.lock_state() {
                Ok(mut state) => {
                    if state.pending_locks.contains_key(&key) {
                        return reply.error(libc::EDEADLK);
                    }
                    state.pending_locks.insert(key.clone(), pending);
                }
                Err(errno) => return reply.error(errno),
            }
            spawn_blocking_lock_waiter(
                Arc::downgrade(&self.dfs),
                Arc::downgrade(&self.state),
                self.lock_renewer_stop.clone(),
                key,
                command,
                reply,
            );
            return;
        }

        match run_advisory_lock_command(&self.dfs, &path, &object_id, command) {
            Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::Acquired { grant },
                local_lease_deadline,
            }) => {
                let mut associated_fhs = prior_lost_fhs;
                associated_fhs.insert(fh);
                let held = fuse_held_lock(
                    path.clone(),
                    object_id.clone(),
                    session_id.clone(),
                    grant,
                    local_lease_deadline,
                    associated_fhs.clone(),
                );
                let mut state = match self.lock_state() {
                    Ok(state) => state,
                    Err(errno) => return reply.error(errno),
                };
                if let Some(held) = held {
                    state.held_locks.insert(key.clone(), held);
                    state.lost_locks.remove(&key);
                    reply.ok();
                } else {
                    state.record_lost_lock(key, associated_fhs);
                    reply.error(libc::EIO);
                }
            }
            Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::WouldBlock { .. },
                ..
            })
            | Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::AuthorityWarming { .. },
                ..
            }) => reply.error(libc::EAGAIN),
            Ok(response) => reply.error(errno_for_unexpected_lock_outcome(&response.outcome)),
            Err(errno) => {
                let mut associated_fhs = prior_lost_fhs;
                associated_fhs.insert(fh);
                if let Ok(mut state) = self.lock_state() {
                    state.record_lost_lock(key, associated_fhs);
                }
                reply.error(errno);
            }
        }
    }

    fn unlock_advisory_lock(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
    ) -> std::result::Result<(), libc::c_int> {
        let (path, object_id) = self.handle_lock_identity(ino, fh)?;
        let key = (object_id.clone(), lock_owner);
        let (held, pending, lost) = {
            let mut state = self.lock_state()?;
            state.expire_elapsed_locks(Instant::now());
            (
                state.held_locks.get(&key).cloned(),
                state.pending_locks.get(&key).cloned(),
                state.lost_locks.get(&key).cloned(),
            )
        };
        let mut observed_loss = lost.is_some();
        let mut lost_associated_fhs = lost.map(|lost| lost.associated_fhs).unwrap_or_default();
        if let Some(held) = held {
            lost_associated_fhs.extend(held.associated_fhs.iter().copied());
            let command = dfs::DfsAdvisoryLockCommand::Unlock {
                request_id: self.next_lock_request_id("unlock"),
                token: held.grant.token,
            };
            match run_advisory_lock_command(&self.dfs, &path, &object_id, command) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Unlocked,
                    ..
                }) => {}
                Ok(response) if lock_outcome_loses_grant(&response.outcome) => observed_loss = true,
                Ok(response) => {
                    return Err(errno_for_unexpected_lock_outcome(&response.outcome));
                }
                Err(_) => observed_loss = true,
            }
        } else if let Some(pending) = pending {
            lost_associated_fhs.extend(pending.associated_fhs.iter().copied());
            // Publish local cancellation before waiting for the serialized
            // service lane. The blocking waiter rechecks this marker while it
            // owns that lane and therefore cannot send a later first Acquire.
            self.lock_state()?.pending_locks.remove(&key);
            let command = dfs::DfsAdvisoryLockCommand::Cancel {
                request_id: self.next_lock_request_id("cancel"),
                session_id: pending.session_id,
                acquire_request_id: pending.acquire_request_id,
            };
            match run_advisory_lock_command(&self.dfs, &path, &object_id, command) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Cancelled { .. },
                    ..
                }) => {}
                Ok(response) => {
                    return Err(errno_for_unexpected_lock_outcome(&response.outcome));
                }
                Err(_) => observed_loss = true,
            }
        } else if observed_loss {
            return Err(libc::EIO);
        }
        let mut state = self.lock_state()?;
        state.held_locks.remove(&key);
        state.pending_locks.remove(&key);
        if observed_loss {
            state.record_lost_lock(key, lost_associated_fhs);
            Err(libc::EIO)
        } else {
            state.lost_locks.remove(&key);
            Ok(())
        }
    }

    fn close_advisory_locks_for_handle(
        &self,
        fh: u64,
        lock_owner: u64,
    ) -> std::result::Result<(), libc::c_int> {
        let Some((path, object_id)) = self
            .lock_state()?
            .handles
            .get(&fh)
            .map(|handle| (handle.path.clone(), handle.object_id.clone()))
        else {
            return Ok(());
        };
        let key = (object_id.clone(), lock_owner);
        let (tracked, was_lost, pending) = {
            let mut state = self.lock_state()?;
            state.expire_elapsed_locks(Instant::now());
            (
                state.held_locks.contains_key(&key)
                    || state.pending_locks.contains_key(&key)
                    || state.lost_locks.contains_key(&key),
                state.lost_locks.contains_key(&key),
                state.pending_locks.get(&key).cloned(),
            )
        };
        if !tracked {
            return Ok(());
        }
        // Stop waiter dispatch and background renewal before the close RPC.
        // Any waiter already holding the service lane linearizes first; Close
        // then releases its result. A waiter behind us observes no pending
        // marker and cannot send a new Acquire after Close.
        {
            let mut state = self.lock_state()?;
            state.held_locks.remove(&key);
            state.pending_locks.remove(&key);
            state.lost_locks.remove(&key);
        }
        let pending_cleanup_result = if let Some(pending) = pending {
            let cancel = dfs::DfsAdvisoryLockCommand::Cancel {
                request_id: self.next_lock_request_id("close-cancel"),
                session_id: pending.session_id,
                acquire_request_id: pending.acquire_request_id,
            };
            match run_advisory_lock_command(&self.dfs, &path, &object_id, cancel) {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Cancelled { .. },
                    ..
                }) => Ok(()),
                Ok(response) => Err(errno_for_unexpected_lock_outcome(&response.outcome)),
                Err(errno) => Err(errno),
            }
        } else {
            Ok(())
        };
        let command = dfs::DfsAdvisoryLockCommand::Close {
            request_id: self.next_lock_request_id("close"),
            session_id: self.lock_session_id(lock_owner),
        };
        let result = run_advisory_lock_command(&self.dfs, &path, &object_id, command);
        // Closing an fd must stop renewal even when the cluster is unavailable;
        // the server-side lease then guarantees bounded cleanup.
        let close_result = match result {
            Ok(FuseLockCommandResult {
                outcome: dfs::DfsAdvisoryLockOutcome::Closed { .. },
                ..
            }) => Ok(()),
            Ok(response) if lock_outcome_loses_grant(&response.outcome) => Ok(()),
            Ok(response) => Err(errno_for_unexpected_lock_outcome(&response.outcome)),
            Err(errno) => Err(errno),
        };
        let cleanup_result = pending_cleanup_result.and(close_result);
        if was_lost {
            Err(libc::EIO)
        } else {
            cleanup_result
        }
    }

    fn close_advisory_locks_for_release(
        &self,
        fh: u64,
        lock_owner: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        let mut state = self.lock_state()?;
        state.expire_elapsed_locks(Instant::now());
        let mut lock_owners = state.lock_owners_for_handle(fh);
        drop(state);
        lock_owners.extend(lock_owner);
        let mut first_error = None;
        for lock_owner in lock_owners {
            if let Err(errno) = self.close_advisory_locks_for_handle(fh, lock_owner)
                && first_error.is_none()
            {
                first_error = Some(errno);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn plan_read_handle_or_path(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        lock_owner: Option<u64>,
    ) -> std::result::Result<FuseReadPlan, libc::c_int> {
        self.ensure_io_lock_not_lost(ino, fh, lock_owner)?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let opened_read = {
            let state = self.lock_state()?;
            state.opened_read(fh, start, size as usize)
        };
        self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
        // SIFS content versions currently also order metadata-only atime
        // updates. Advancing that version after a read leaves the new version
        // without matching immutable content/receipts, which breaks later
        // range reads and fsync validation. Keep FUSE reads noatime until DFS
        // has a metadata revision independent from the content version.
        Ok(match opened_read {
            Some(read) => FuseReadPlan::Opened(read),
            None => FuseReadPlan::Path {
                path: self.path_for_ino(ino)?,
                offset: start,
                max_bytes: size as usize,
            },
        })
    }

    fn write_handle(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        lock_owner: Option<u64>,
    ) -> std::result::Result<u32, libc::c_int> {
        self.ensure_writable()?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let written = u32::try_from(data.len()).map_err(|_| libc::EFBIG)?;
        let (object_id, sync_mode) = {
            let state = self.lock_state()?;
            let handle = state.handles.get(&fh).ok_or(libc::EBADF)?;
            if !handle.writable {
                return Err(libc::EBADF);
            }
            (handle.object_id.clone(), handle.sync_mode)
        };
        self.ensure_io_lock_not_lost(ino, fh, lock_owner)?;
        self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
        if data.is_empty() {
            return Ok(0);
        }

        if sync_mode != HandleSyncMode::None {
            // An O_SYNC/O_DSYNC publication must follow every earlier
            // acknowledged write to this object, including another open
            // descriptor's coalesced buffer.
            self.commit_dirty_handles_for_object(&object_id, None)?;
            self.publish_immediate_handle_write(
                fh, &object_id, start, data, sync_mode, lock_owner,
            )?;
            return Ok(written);
        }

        // Preserve the serialized FUSE write order across descriptors. At
        // most one descriptor per object remains dirty, while repeated chunks
        // on that descriptor retain the fast coalescing path.
        self.commit_dirty_handles_for_object(&object_id, Some(fh))?;

        loop {
            let outcome = self.lock_state()?.buffer_handle_write(fh, start, data)?;
            match outcome {
                DirtyWriteInsert::Buffered => return Ok(written),
                DirtyWriteInsert::NeedsPublication => self.commit_handle(fh)?,
                DirtyWriteInsert::PublishDirectly => {
                    self.publish_immediate_handle_write(
                        fh,
                        &object_id,
                        start,
                        data,
                        HandleSyncMode::None,
                        lock_owner,
                    )?;
                    return Ok(written);
                }
            }
        }
    }

    fn publish_immediate_handle_write(
        &self,
        fh: u64,
        object_id: &dfs::DfsObjectId,
        offset: u64,
        data: &[u8],
        sync_mode: HandleSyncMode,
        lock_owner: Option<u64>,
    ) -> std::result::Result<(), libc::c_int> {
        // The state snapshot above carries only stable identity. Do not hold
        // FuseState while content upload, metadata publication, or a sync
        // barrier waits on disk/network.
        let stat = self
            .lock_dfs()?
            .posix_write_file_at_for_object(object_id, offset, data, self.write_options())
            .map_err(errno_for_posix_error)?;
        self.refresh_opened_object_stats(object_id, &stat)?;
        if let Some(kind) = sync_mode.kind() {
            self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
            let target = self.current_file_sync_target_for_handle(fh, kind)?;
            self.lock_dfs()?
                .posix_sync_file_target(&target)
                .map(drop)
                .map_err(errno_for_posix_error)?;
        }
        Ok(())
    }

    fn truncate_handle_or_path(
        &self,
        ino: u64,
        fh: Option<u64>,
        size: u64,
    ) -> std::result::Result<(), libc::c_int> {
        self.ensure_writable()?;
        let handle = if let Some(fh) = fh {
            let state = self.lock_state()?;
            Some(
                state
                    .handles
                    .get(&fh)
                    .map(|handle| {
                        (
                            handle.path.clone(),
                            handle.object_id.clone(),
                            handle.sync_mode,
                        )
                    })
                    .ok_or(libc::EBADF)?,
            )
        } else {
            None
        };
        let (path, object_id, sync_mode) = match handle {
            Some((path, object_id, sync_mode)) => (path, Some(object_id), sync_mode),
            None => (self.path_for_ino(ino)?, None, HandleSyncMode::None),
        };
        if let Some(object_id) = object_id.as_ref() {
            // Preserve object-wide write/truncate program order: buffered
            // writes from every descriptor name pre-truncate offsets.
            self.commit_dirty_handles_for_object(object_id, None)?;
        } else {
            self.commit_dirty_handles_for_path(&path, false)?;
        }
        if let Some(fh) = fh {
            self.ensure_handle_lock_budget(fh, None, LOCKED_IO_MIN_LEASE)?;
        }
        let stat = if let Some(object_id) = object_id.as_ref() {
            self.lock_dfs()?
                .posix_truncate_file_for_object(object_id, size, self.write_options())
        } else {
            self.lock_dfs()?
                .posix_truncate_file(&path, size, self.write_options())
        }
        .map_err(errno_for_posix_error)?;
        self.refresh_opened_object_stats(&stat.object_id, &stat)?;
        if let Some(kind) = sync_mode.kind() {
            if let Some(fh) = fh {
                self.ensure_handle_lock_budget(fh, None, LOCKED_IO_MIN_LEASE)?;
            }
            let target = fh
                .map(|fh| self.current_file_sync_target_for_handle(fh, kind))
                .transpose()?;
            self.lock_dfs()?
                .posix_sync_file_target(target.as_ref().ok_or(libc::EIO)?)
                .map(drop)
                .map_err(errno_for_posix_error)?;
        }
        Ok(())
    }
}

impl Filesystem for SifsFuseFileSystem {
    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        config
            .add_capabilities(fuser::consts::FUSE_POSIX_LOCKS)
            .map_err(|_| libc::EIO)
    }

    fn destroy(&mut self) {
        self.lock_renewer_stop.store(true, Ordering::Release);
        self.read_cancellation.cancel();
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        match self.stat_path(&path, false) {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error(errno),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        if let Some(fh) = fh {
            match self.lock_state() {
                Ok(state) => {
                    if let Some(stat) = state.visible_handle_stat(fh) {
                        return reply.attr(&self.attr_ttl, &file_attr(ino, &stat));
                    }
                }
                Err(errno) => return reply.error(errno),
            }
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        match self.stat_path(&path, false) {
            Ok(stat) => reply.attr(&self.attr_ttl, &file_attr(ino, &stat)),
            Err(errno) => reply.error(errno),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let changes_metadata =
            mode.is_some() || uid.is_some() || gid.is_some() || atime.is_some() || mtime.is_some();
        if size.is_none() && changes_metadata {
            let commit_result = match fh {
                Some(fh) => self.commit_dirty_handles_for_handle_object(fh).map(drop),
                None => self
                    .path_for_ino(ino)
                    .and_then(|path| self.commit_dirty_handles_for_path(&path, false)),
            };
            if let Err(errno) = commit_result {
                return reply.error(errno);
            }
        }
        if let Some(size) = size
            && let Err(errno) = self.truncate_handle_or_path(ino, fh, size)
        {
            return reply.error(errno);
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let mut result = Ok(());
        if let Some(mode) = mode {
            result = result.and_then(|_| {
                self.ensure_writable()?;
                self.lock_dfs()?
                    .posix_set_mode(&path, mode)
                    .map(|_| ())
                    .map_err(errno_for_posix_error)
            });
        }
        if uid.is_some() || gid.is_some() {
            result = result.and_then(|_| {
                self.ensure_writable()?;
                let current = self.stat_path(&path, false)?;
                let uid = uid.unwrap_or(current.uid);
                let gid = gid.unwrap_or(current.gid);
                let mut dfs = self.lock_dfs()?;
                let updated = if current.kind == DfsObjectKind::Symlink {
                    dfs.posix_set_symlink_owner(&path, uid, gid)
                } else {
                    dfs.posix_set_owner(&path, uid, gid)
                };
                updated.map(|_| ()).map_err(errno_for_posix_error)
            });
        }
        if atime.is_some() || mtime.is_some() {
            result = result.and_then(|_| {
                self.ensure_writable()?;
                self.lock_dfs()?
                    .posix_set_times(
                        &path,
                        atime.map(time_or_now_to_nanos),
                        mtime.map(time_or_now_to_nanos),
                    )
                    .map(|_| ())
                    .map_err(errno_for_posix_error)
            });
        }
        if let Err(errno) = result {
            return reply.error(errno);
        }
        match self.stat_path(&path, false) {
            Ok(stat) => reply.attr(&self.attr_ttl, &file_attr(ino, &stat)),
            Err(errno) => reply.error(errno),
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let result = self
            .lock_dfs()
            .and_then(|dfs| dfs.posix_readlink(&path).map_err(errno_for_posix_error));
        match result {
            Ok(target) => reply.data(target.as_bytes()),
            Err(errno) => reply.error(errno),
        }
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let file_type = mode & libc::S_IFMT;
        if file_type != 0 && file_type != libc::S_IFREG {
            return reply.error(libc::ENOSYS);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let result = self.lock_dfs().and_then(|mut dfs| {
            let attrs = DfsPosixCreateAttrs::new(created_mode(mode, umask), req.uid(), req.gid());
            dfs.posix_create_file_with_attrs(&path, self.write_options(), attrs)
                .map_err(errno_for_posix_error)
        });
        match result {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error(errno),
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let result = self.lock_dfs().and_then(|mut dfs| {
            let attrs = DfsPosixCreateAttrs::new(created_mode(mode, umask), req.uid(), req.gid());
            dfs.posix_mkdir_with_create_attrs(&path, attrs)
                .map_err(errno_for_posix_error)
        });
        match result {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error(errno),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        if let Err(errno) = self.commit_dirty_handles_for_path(&path, false) {
            return reply.error(errno);
        }
        let result = self
            .lock_dfs()
            .and_then(|mut dfs| dfs.posix_unlink(&path).map_err(errno_for_posix_error));
        match result {
            Ok(_) => {
                if let Ok(mut state) = self.lock_state() {
                    state.remove_path(&path);
                }
                reply.ok();
            }
            Err(errno) => reply.error(errno),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let result = self
            .lock_dfs()
            .and_then(|mut dfs| dfs.posix_rmdir(&path).map_err(errno_for_posix_error));
        match result {
            Ok(_) => {
                if let Ok(mut state) = self.lock_state() {
                    state.remove_path(&path);
                }
                reply.ok();
            }
            Err(errno) => reply.error(errno),
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let Some(target) = link.to_str() else {
            return reply.error(libc::EINVAL);
        };
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let result = self.lock_dfs().and_then(|mut dfs| {
            let attrs = DfsPosixCreateAttrs::new(0o777, req.uid(), req.gid());
            dfs.posix_symlink_with_create_attrs(target, &path, self.write_options(), attrs)
                .map_err(errno_for_posix_error)
        });
        match result {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error(errno),
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: ReplyEmpty,
    ) {
        if flags != 0 {
            return reply.error(libc::ENOSYS);
        }
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let from = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let to = match self.child_path(newparent, newname) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        // Publish both sides before namespace replacement. Source descriptors
        // then carry the latest stable object version through the rename, and
        // an open destination cannot lose acknowledged data when replaced.
        if let Err(errno) = self
            .commit_dirty_handles_for_path(&from, true)
            .and_then(|_| self.commit_dirty_handles_for_path(&to, true))
        {
            return reply.error(errno);
        }
        let result = self
            .lock_dfs()
            .and_then(|mut dfs| dfs.posix_rename(&from, &to).map_err(errno_for_posix_error));
        match result {
            Ok(_) => {
                if let Ok(mut state) = self.lock_state() {
                    state.rename_path(&from, to);
                }
                reply.ok();
            }
            Err(errno) => reply.error(errno),
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let existing = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let link_path = match self.child_path(newparent, newname) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let result = self.lock_dfs().and_then(|mut dfs| {
            dfs.posix_hardlink(&existing, &link_path)
                .map_err(errno_for_posix_error)
        });
        match result {
            Ok(stat) => match self.lock_state() {
                Ok(mut state) => match state.intern_alias_path(ino, link_path, &stat.object_id) {
                    Ok(link_ino) => reply.entry(&self.entry_ttl, &file_attr(link_ino, &stat), 0),
                    Err(errno) => reply.error(errno),
                },
                Err(errno) => reply.error(errno),
            },
            Err(errno) => reply.error(errno),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let writable = flags_has_write_access(flags);
        if writable && let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        match self.stat_path(&path, true) {
            Ok(stat) if stat.kind == DfsObjectKind::Directory => reply.error(libc::EISDIR),
            Ok(stat) => match self.lock_state() {
                Ok(mut state) => {
                    let fh = state.allocate_handle(path, stat, flags);
                    reply.opened(fh, 0);
                }
                Err(errno) => reply.error(errno),
            },
            Err(errno) => reply.error(errno),
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        if let Err(errno) = self.ensure_writable() {
            return reply.error(errno);
        }
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let result = self.lock_dfs().and_then(|mut dfs| {
            let attrs = DfsPosixCreateAttrs::new(created_mode(mode, umask), req.uid(), req.gid());
            create_file_for_fuse(&mut dfs, &path, self.write_options(), attrs, flags)
        });
        match result {
            Ok(stat) => match self.lock_state() {
                Ok(mut state) => {
                    let ino = state.intern_entry(path.clone(), stat.object_id.clone());
                    let fh = state.allocate_handle(path, stat.clone(), flags);
                    reply.created(&self.entry_ttl, &file_attr(ino, &stat), 0, fh, 0);
                }
                Err(errno) => reply.error(errno),
            },
            Err(errno) => reply.error(errno),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let plan = match self.plan_read_handle_or_path(ino, fh, offset, size, lock_owner) {
            Ok(plan) => plan,
            Err(errno) => return reply.error(errno),
        };
        match plan {
            FuseReadPlan::Opened(read) => {
                let content_reads = self.content_reads.clone();
                let job: FuseReadJob = Box::new(move || {
                    let base_request_len = read.base_request_len();
                    let base = if base_request_len == 0 {
                        Ok(Vec::new())
                    } else {
                        content_reads.read_opened_file_range(
                            &read.identity.path,
                            &read.identity.opened_stat,
                            read.offset,
                            base_request_len,
                        )
                    };
                    match base {
                        Ok(base) => match read.finish(base) {
                            Ok(bytes) => reply.data(&bytes),
                            Err(errno) => reply.error(errno),
                        },
                        Err(error) => reply.error(errno_for_posix_error(error)),
                    }
                });
                if let Err(job) = self.read_executor.submit(job) {
                    // The queue can disconnect only during shutdown or after
                    // every worker panicked. Preserve the request rather than
                    // dropping its ReplyData without a response.
                    job();
                }
            }
            FuseReadPlan::Path {
                path,
                offset,
                max_bytes,
            } => {
                let result = self.lock_dfs().and_then(|mut dfs| {
                    dfs.posix_read_file_range_distributed(&path, offset, max_bytes)
                        .map_err(errno_for_posix_error)
                });
                match result {
                    Ok(bytes) => reply.data(&bytes),
                    Err(errno) => reply.error(errno),
                }
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.write_handle(ino, fh, offset, data, lock_owner) {
            Ok(written) => reply.written(written),
            Err(errno) => reply.error(errno),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let commit_result = self.commit_handle(fh);
        let close_result = self.close_advisory_locks_for_handle(fh, lock_owner);
        let result = commit_result.and(close_result);
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let commit_result = self.commit_handle(fh);
        let commit_succeeded = commit_result.is_ok();
        let close_result = self.close_advisory_locks_for_release(fh, lock_owner);
        let result = commit_result.and(close_result);
        if commit_succeeded {
            if let Ok(mut state) = self.lock_state() {
                state.handles.remove(&fh);
            }
        }
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        match self.sync_handle(fh, datasync) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.path_for_ino(ino).and_then(|path| {
            self.stat_path(&path, true).and_then(|stat| {
                if stat.kind == DfsObjectKind::Directory {
                    Ok(())
                } else {
                    Err(libc::ENOTDIR)
                }
            })
        }) {
            Ok(()) => reply.opened(0, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            return reply.error(libc::EINVAL);
        }
        let path = match self.path_for_ino(ino) {
            Ok(path) => path,
            Err(errno) => return reply.error(errno),
        };
        let children = match self.lock_dfs().and_then(|mut dfs| {
            dfs.posix_list_dir_distributed(&path)
                .map_err(errno_for_posix_error)
        }) {
            Ok(children) => children,
            Err(errno) => return reply.error(errno),
        };
        let start = offset as usize;
        let parent_ino = match self.lock_state() {
            Ok(mut state) => state.intern_path(parent_sifs_path(&path)),
            Err(errno) => return reply.error(errno),
        };
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];
        for child in children {
            let child_ino = match self.intern_entry(child.path.clone(), &child.object_id) {
                Ok(child_ino) => child_ino,
                Err(errno) => return reply.error(errno),
            };
            entries.push((child_ino, file_type_for_kind(&child.kind), child.name));
        }
        for (index, (entry_ino, kind, name)) in entries.into_iter().enumerate().skip(start) {
            if reply.add(entry_ino, (index + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let result = self.path_for_ino(ino).and_then(|path| {
            self.lock_dfs()?
                .posix_sync_directory(&path)
                .map(drop)
                .map_err(errno_for_posix_error)
        });
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn getlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        if !is_whole_file_lock_range(start, end) {
            return reply.error(libc::EOPNOTSUPP);
        }
        let mode = match advisory_lock_mode(typ) {
            Ok(mode) => mode,
            Err(errno) => return reply.error(errno),
        };
        match self.query_advisory_lock(ino, fh, lock_owner, mode) {
            Ok(Some(conflict)) => reply.locked(
                0,
                end,
                advisory_lock_type(conflict.mode),
                conflict.process_id.unwrap_or(0),
            ),
            Ok(None) => reply.locked(0, end, libc::F_UNLCK, 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        if !is_whole_file_lock_range(start, end) {
            return reply.error(libc::EOPNOTSUPP);
        }
        if typ == libc::F_UNLCK {
            return match self.unlock_advisory_lock(ino, fh, lock_owner) {
                Ok(()) => reply.ok(),
                Err(errno) => reply.error(errno),
            };
        }
        let mode = match advisory_lock_mode(typ) {
            Ok(mode) => mode,
            Err(errno) => return reply.error(errno),
        };
        self.set_advisory_lock(ino, fh, lock_owner, mode, pid, sleep, reply);
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            1024 * 1024 * 1024,
            DEFAULT_BLOCK_SIZE,
            255,
            DEFAULT_BLOCK_SIZE,
        );
    }

    fn access(&mut self, _req: &Request<'_>, _ino: u64, mask: i32, reply: ReplyEmpty) {
        if mask & libc::W_OK != 0
            && let Err(errno) = self.ensure_writable()
        {
            return reply.error(errno);
        }
        reply.ok();
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FuseOpenedReadIdentity {
    path: String,
    opened_stat: dfs::DfsStat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FuseOpenedRead {
    identity: FuseOpenedReadIdentity,
    offset: u64,
    max_bytes: usize,
    logical_size: u64,
    dirty_overlays: Vec<DirtyWriteRange>,
}

impl FuseOpenedRead {
    fn requested_len(&self) -> usize {
        if self.offset >= self.logical_size {
            return 0;
        }
        usize::try_from(self.logical_size - self.offset)
            .unwrap_or(usize::MAX)
            .min(self.max_bytes)
    }

    fn base_request_len(&self) -> usize {
        let committed_size = self.identity.opened_stat.size_bytes.unwrap_or(0);
        if self.offset >= committed_size {
            return 0;
        }
        usize::try_from(committed_size - self.offset)
            .unwrap_or(usize::MAX)
            .min(self.requested_len())
    }

    fn finish(self, base: Vec<u8>) -> std::result::Result<Vec<u8>, libc::c_int> {
        let requested_len = self.requested_len();
        let base_request_len = self.base_request_len();
        if base.len() < base_request_len {
            // The opened stat names an exact immutable version and size. A
            // short read inside that committed extent is corruption or a
            // stale peer response, not a sparse hole.
            return Err(libc::EIO);
        }
        let mut bytes = vec![0; requested_len];
        bytes[..base_request_len].copy_from_slice(&base[..base_request_len]);
        let request_end = self.offset.saturating_add(requested_len as u64);
        for overlay in self.dirty_overlays {
            let overlay_end = overlay.offset.saturating_add(overlay.bytes.len() as u64);
            let copy_start = overlay.offset.max(self.offset);
            let copy_end = overlay_end.min(request_end);
            if copy_start >= copy_end {
                continue;
            }
            let source_start = usize::try_from(copy_start - overlay.offset)
                .expect("dirty read source offset fits in the resident buffer");
            let target_start = usize::try_from(copy_start - self.offset)
                .expect("dirty read target offset is bounded by max_bytes");
            let len = usize::try_from(copy_end - copy_start)
                .expect("dirty read length is bounded by max_bytes");
            bytes[target_start..target_start + len]
                .copy_from_slice(&overlay.bytes[source_start..source_start + len]);
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FuseReadPlan {
    Opened(FuseOpenedRead),
    Path {
        path: String,
        offset: u64,
        max_bytes: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirtyWriteRange {
    offset: u64,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DirtyWriteBuffer {
    ranges: BTreeMap<u64, Vec<u8>>,
    buffered_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirtyWriteInsert {
    Buffered,
    NeedsPublication,
    PublishDirectly,
}

impl DirtyWriteBuffer {
    fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.ranges.len()
    }

    #[cfg(test)]
    fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn try_write(
        &mut self,
        offset: u64,
        bytes: &[u8],
        limit: usize,
    ) -> std::result::Result<DirtyWriteInsert, libc::c_int> {
        if bytes.is_empty() {
            return Ok(DirtyWriteInsert::Buffered);
        }
        let end = offset
            .checked_add(u64::try_from(bytes.len()).map_err(|_| libc::EFBIG)?)
            .ok_or(libc::EFBIG)?;
        if bytes.len() > limit {
            return Ok(if self.is_empty() {
                DirtyWriteInsert::PublishDirectly
            } else {
                DirtyWriteInsert::NeedsPublication
            });
        }

        let prior = self
            .ranges
            .range(..=offset)
            .next_back()
            .map(|(&range_start, range_bytes)| (range_start, range_bytes.len()));
        if let Some((prior_start, prior_len)) = prior {
            let prior_end = prior_start.saturating_add(prior_len as u64);
            if end <= prior_end {
                // A rewrite fully contained in one resident range can update
                // in place. This is also important for page-cache retry
                // patterns that repeatedly dirty the same kernel chunk.
                let target = usize::try_from(offset - prior_start)
                    .expect("contained dirty write offset fits in resident range");
                self.ranges
                    .get_mut(&prior_start)
                    .expect("contained dirty range remains present")[target..target + bytes.len()]
                    .copy_from_slice(bytes);
                return Ok(DirtyWriteInsert::Buffered);
            }
            let next_start = self.ranges.range(offset..).next().map(|(&start, _)| start);
            if prior_end == offset && next_start.is_none_or(|next| next > end) {
                let projected_bytes = self
                    .buffered_bytes
                    .checked_add(bytes.len())
                    .ok_or(libc::EFBIG)?;
                if projected_bytes > limit {
                    return Ok(DirtyWriteInsert::NeedsPublication);
                }
                // This is the dominant kernel pattern. Vec's spare capacity
                // makes sequential coalescing amortized O(total bytes) rather
                // than reallocating and copying the whole file per chunk.
                self.ranges
                    .get_mut(&prior_start)
                    .expect("append dirty range remains present")
                    .extend_from_slice(bytes);
                self.buffered_bytes = projected_bytes;
                return Ok(DirtyWriteInsert::Buffered);
            }
        }

        let mut merged_start = offset;
        let mut merged_end = end;
        if let Some((&prior_start, prior_bytes)) = self.ranges.range(..=offset).next_back() {
            let prior_end = prior_start.saturating_add(prior_bytes.len() as u64);
            if prior_end >= offset {
                merged_start = prior_start;
                merged_end = merged_end.max(prior_end);
            }
        }

        let mut merged_keys = Vec::new();
        let mut replaced_bytes = 0usize;
        for (&range_start, range_bytes) in self.ranges.range(merged_start..) {
            if range_start > merged_end {
                break;
            }
            let range_end = range_start.saturating_add(range_bytes.len() as u64);
            if range_end < merged_start {
                continue;
            }
            merged_end = merged_end.max(range_end);
            merged_keys.push(range_start);
            replaced_bytes = replaced_bytes.saturating_add(range_bytes.len());
        }
        let merged_len = usize::try_from(merged_end - merged_start).map_err(|_| libc::EFBIG)?;
        let projected_bytes = self
            .buffered_bytes
            .checked_sub(replaced_bytes)
            .and_then(|retained| retained.checked_add(merged_len))
            .ok_or(libc::EFBIG)?;
        if projected_bytes > limit {
            return Ok(DirtyWriteInsert::NeedsPublication);
        }

        let mut merged = vec![0; merged_len];
        for range_start in &merged_keys {
            let range_bytes = self
                .ranges
                .get(range_start)
                .expect("planned dirty range remains present");
            let target = usize::try_from(*range_start - merged_start)
                .expect("dirty range offset fits in merged buffer");
            merged[target..target + range_bytes.len()].copy_from_slice(range_bytes);
        }
        let incoming_target = usize::try_from(offset - merged_start)
            .expect("incoming dirty range offset fits in merged buffer");
        merged[incoming_target..incoming_target + bytes.len()].copy_from_slice(bytes);
        for range_start in merged_keys {
            self.ranges.remove(&range_start);
        }
        self.ranges.insert(merged_start, merged);
        self.buffered_bytes = projected_bytes;
        Ok(DirtyWriteInsert::Buffered)
    }

    fn overlays_for_read(&self, offset: u64, max_bytes: usize) -> Vec<DirtyWriteRange> {
        let request_end = offset.saturating_add(max_bytes as u64);
        self.ranges
            .iter()
            .filter_map(|(&range_start, range_bytes)| {
                let range_end = range_start.saturating_add(range_bytes.len() as u64);
                let copy_start = range_start.max(offset);
                let copy_end = range_end.min(request_end);
                if copy_start >= copy_end {
                    return None;
                }
                let source_start = usize::try_from(copy_start - range_start).ok()?;
                let len = usize::try_from(copy_end - copy_start).ok()?;
                Some(DirtyWriteRange {
                    offset: copy_start,
                    bytes: range_bytes[source_start..source_start + len].to_vec(),
                })
            })
            .collect()
    }

    fn restore_older(&mut self, older: Self) {
        debug_assert!(self.is_empty(), "FUSE dispatch serializes handle writes");
        if self.is_empty() {
            *self = older;
            return;
        }

        // Be conservative if a future dispatcher permits concurrent writes:
        // the resident buffer is newer and therefore overlays the restored
        // publication on overlap.
        let newer = std::mem::take(self);
        *self = older;
        for (offset, bytes) in newer.ranges {
            let inserted = self
                .try_write(offset, &bytes, usize::MAX)
                .expect("resident dirty range was already size-checked");
            debug_assert_eq!(inserted, DirtyWriteInsert::Buffered);
        }
    }
}

#[derive(Debug)]
struct FuseDirtyPublication {
    object_id: dfs::DfsObjectId,
    dirty: DirtyWriteBuffer,
}

#[derive(Debug)]
struct FuseState {
    next_ino: u64,
    next_fh: u64,
    paths_by_ino: BTreeMap<u64, BTreeSet<String>>,
    preferred_path_by_ino: BTreeMap<u64, String>,
    ino_by_path: BTreeMap<String, u64>,
    ino_by_object_id: BTreeMap<dfs::DfsObjectId, u64>,
    object_id_by_ino: BTreeMap<u64, dfs::DfsObjectId>,
    handles: BTreeMap<u64, OpenFileHandle>,
    held_locks: BTreeMap<(dfs::DfsObjectId, u64), FuseHeldLock>,
    pending_locks: BTreeMap<(dfs::DfsObjectId, u64), FusePendingLock>,
    lost_locks: BTreeMap<(dfs::DfsObjectId, u64), FuseLostLock>,
}

impl FuseState {
    fn new(root_path: String) -> Self {
        let mut paths_by_ino = BTreeMap::new();
        let mut preferred_path_by_ino = BTreeMap::new();
        let mut ino_by_path = BTreeMap::new();
        paths_by_ino.insert(ROOT_INO, BTreeSet::from([root_path.clone()]));
        preferred_path_by_ino.insert(ROOT_INO, root_path.clone());
        ino_by_path.insert(root_path, ROOT_INO);
        Self {
            next_ino: ROOT_INO + 1,
            next_fh: 1,
            paths_by_ino,
            preferred_path_by_ino,
            ino_by_path,
            ino_by_object_id: BTreeMap::new(),
            object_id_by_ino: BTreeMap::new(),
            handles: BTreeMap::new(),
            held_locks: BTreeMap::new(),
            pending_locks: BTreeMap::new(),
            lost_locks: BTreeMap::new(),
        }
    }

    fn path_for(&self, ino: u64) -> Option<&str> {
        let paths = self.paths_by_ino.get(&ino)?;
        self.preferred_path_by_ino
            .get(&ino)
            .filter(|path| paths.contains(path.as_str()))
            .or_else(|| paths.first())
            .map(String::as_str)
    }

    #[cfg(test)]
    fn opened_read_identity(&self, fh: u64) -> Option<FuseOpenedReadIdentity> {
        self.handles.get(&fh).map(|handle| FuseOpenedReadIdentity {
            path: handle.path.clone(),
            opened_stat: handle.opened_stat.clone(),
        })
    }

    fn opened_read(&self, fh: u64, offset: u64, max_bytes: usize) -> Option<FuseOpenedRead> {
        let handle = self.handles.get(&fh)?;
        let object_id = &handle.object_id;
        let logical_size = self
            .handles
            .values()
            .filter(|candidate| &candidate.object_id == object_id)
            .map(|candidate| candidate.logical_size)
            .max()
            .unwrap_or(handle.logical_size);
        let dirty_overlays = self
            .handles
            .values()
            .filter(|candidate| &candidate.object_id == object_id)
            .flat_map(|candidate| candidate.dirty.overlays_for_read(offset, max_bytes))
            .collect();
        Some(FuseOpenedRead {
            identity: FuseOpenedReadIdentity {
                path: handle.path.clone(),
                opened_stat: handle.opened_stat.clone(),
            },
            offset,
            max_bytes,
            logical_size,
            dirty_overlays,
        })
    }

    fn visible_handle_stat(&self, fh: u64) -> Option<dfs::DfsStat> {
        let handle = self.handles.get(&fh)?;
        let mut stat = handle.visible_stat();
        stat.size_bytes = self
            .handles
            .values()
            .filter(|candidate| candidate.object_id == handle.object_id)
            .map(|candidate| candidate.logical_size)
            .max()
            .or(stat.size_bytes);
        Some(stat)
    }

    fn intern_path(&mut self, path: String) -> u64 {
        if let Some(&ino) = self.ino_by_path.get(&path) {
            self.preferred_path_by_ino.insert(ino, path);
            return ino;
        }
        let ino = self.allocate_inode();
        self.attach_path(ino, path);
        ino
    }

    fn intern_entry(&mut self, path: String, object_id: dfs::DfsObjectId) -> u64 {
        if let Some(&ino) = self.ino_by_object_id.get(&object_id) {
            if self.ino_by_path.get(&path).copied() != Some(ino) {
                self.detach_path(&path, false);
                self.attach_path(ino, path);
            } else {
                self.preferred_path_by_ino.insert(ino, path);
            }
            return ino;
        }

        if let Some(&ino) = self.ino_by_path.get(&path) {
            match self.object_id_by_ino.get(&ino) {
                Some(existing) if existing == &object_id => {
                    self.ino_by_object_id.insert(object_id, ino);
                    self.preferred_path_by_ino.insert(ino, path);
                    return ino;
                }
                None => {
                    self.object_id_by_ino.insert(ino, object_id.clone());
                    self.ino_by_object_id.insert(object_id, ino);
                    self.preferred_path_by_ino.insert(ino, path);
                    return ino;
                }
                Some(_) => {
                    self.detach_path(&path, false);
                }
            }
        }

        let ino = self.allocate_inode();
        self.attach_path(ino, path);
        self.object_id_by_ino.insert(ino, object_id.clone());
        self.ino_by_object_id.insert(object_id, ino);
        ino
    }

    fn intern_alias_path(
        &mut self,
        target_ino: u64,
        path: String,
        object_id: &dfs::DfsObjectId,
    ) -> std::result::Result<u64, libc::c_int> {
        if !self.paths_by_ino.contains_key(&target_ino) {
            return Err(libc::ENOENT);
        }
        if self
            .object_id_by_ino
            .get(&target_ino)
            .is_some_and(|existing| existing != object_id)
        {
            return Err(libc::ESTALE);
        }
        if let Some(&existing_ino) = self.ino_by_object_id.get(object_id)
            && existing_ino != target_ino
        {
            if existing_ino == ROOT_INO {
                return Err(libc::ESTALE);
            }
            self.merge_inode_paths(existing_ino, target_ino);
        }
        self.object_id_by_ino.insert(target_ino, object_id.clone());
        self.ino_by_object_id.insert(object_id.clone(), target_ino);
        if self.ino_by_path.get(&path).copied() != Some(target_ino) {
            self.detach_path(&path, false);
            self.attach_path(target_ino, path);
        } else {
            self.preferred_path_by_ino.insert(target_ino, path);
        }
        Ok(target_ino)
    }

    fn remove_path(&mut self, path: &str) {
        if self.ino_by_path.get(path).copied() == Some(ROOT_INO) {
            return;
        }
        self.detach_path(path, false);
    }

    fn allocate_inode(&mut self) -> u64 {
        let ino = self.next_ino;
        self.next_ino = self.next_ino.saturating_add(1);
        ino
    }

    fn attach_path(&mut self, ino: u64, path: String) {
        self.paths_by_ino
            .entry(ino)
            .or_default()
            .insert(path.clone());
        self.preferred_path_by_ino.insert(ino, path.clone());
        self.ino_by_path.insert(path, ino);
    }

    fn detach_path(&mut self, path: &str, preserve_inode: bool) -> Option<u64> {
        let ino = self.ino_by_path.remove(path)?;
        let remove_inode = self.paths_by_ino.get_mut(&ino).is_some_and(|paths| {
            paths.remove(path);
            paths.is_empty()
        });
        if remove_inode {
            self.paths_by_ino.remove(&ino);
            self.preferred_path_by_ino.remove(&ino);
            if !preserve_inode && ino != ROOT_INO {
                self.remove_inode_identity(ino);
            }
        } else if self
            .preferred_path_by_ino
            .get(&ino)
            .is_some_and(|preferred| preferred == path)
            && let Some(replacement) = self
                .paths_by_ino
                .get(&ino)
                .and_then(|paths| paths.first())
                .cloned()
        {
            self.preferred_path_by_ino.insert(ino, replacement);
        }
        Some(ino)
    }

    fn remove_inode_identity(&mut self, ino: u64) {
        self.preferred_path_by_ino.remove(&ino);
        if let Some(object_id) = self.object_id_by_ino.remove(&ino)
            && self.ino_by_object_id.get(&object_id).copied() == Some(ino)
        {
            self.ino_by_object_id.remove(&object_id);
        }
    }

    fn merge_inode_paths(&mut self, from_ino: u64, to_ino: u64) {
        if from_ino == to_ino {
            return;
        }
        let paths = self.paths_by_ino.remove(&from_ino).unwrap_or_default();
        for path in paths {
            self.ino_by_path.insert(path.clone(), to_ino);
            self.paths_by_ino.entry(to_ino).or_default().insert(path);
        }
        self.remove_inode_identity(from_ino);
    }

    fn has_lost_lock_for_handle(&self, fh: u64) -> bool {
        self.lost_locks
            .values()
            .any(|lost| lost.associated_fhs.contains(&fh))
    }

    fn expire_elapsed_locks(&mut self, now: Instant) {
        let expired = self
            .held_locks
            .iter()
            .filter_map(|(key, held)| (held.local_lease_deadline <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in expired {
            if let Some(held) = self.held_locks.remove(&key) {
                self.record_lost_lock(key, held.associated_fhs);
            }
        }
    }

    fn lock_owners_for_handle(&self, fh: u64) -> BTreeSet<u64> {
        self.held_locks
            .iter()
            .filter_map(|((_, lock_owner), held)| {
                held.associated_fhs.contains(&fh).then_some(*lock_owner)
            })
            .chain(
                self.pending_locks
                    .iter()
                    .filter_map(|((_, lock_owner), pending)| {
                        pending.associated_fhs.contains(&fh).then_some(*lock_owner)
                    }),
            )
            .chain(
                self.lost_locks
                    .iter()
                    .filter_map(|((_, lock_owner), lost)| {
                        lost.associated_fhs.contains(&fh).then_some(*lock_owner)
                    }),
            )
            .collect()
    }

    fn can_start_lock_key(&self, key: &(dfs::DfsObjectId, u64)) -> bool {
        if self.held_locks.contains_key(key) || self.pending_locks.contains_key(key) {
            return true;
        }
        self.held_locks
            .keys()
            .chain(self.pending_locks.keys())
            .collect::<BTreeSet<_>>()
            .len()
            < MAX_ACTIVE_LOCK_KEYS_PER_MOUNT
    }

    fn record_lost_lock(
        &mut self,
        key: (dfs::DfsObjectId, u64),
        associated_fhs: impl IntoIterator<Item = u64>,
    ) {
        self.lost_locks
            .entry(key)
            .or_default()
            .associated_fhs
            .extend(associated_fhs);
    }

    fn rename_path(&mut self, from: &str, to: String) {
        if self
            .paths_by_ino
            .get(&ROOT_INO)
            .is_some_and(|paths| paths.contains(from))
        {
            return;
        }
        if from == to
            || self
                .ino_by_path
                .get(from)
                .zip(self.ino_by_path.get(&to))
                .is_some_and(|(from_ino, to_ino)| from_ino == to_ino)
        {
            return;
        }
        let rewritten_inodes = self
            .ino_by_path
            .iter()
            .filter_map(|(path, ino)| {
                renamed_path(path, from, &to).map(|renamed| (*ino, path.clone(), renamed))
            })
            .collect::<Vec<_>>();
        let rewritten_old_paths = rewritten_inodes
            .iter()
            .map(|(_, old_path, _)| old_path.as_str())
            .collect::<BTreeSet<_>>();
        for (_, _, renamed) in &rewritten_inodes {
            if !rewritten_old_paths.contains(renamed.as_str()) {
                self.remove_path(renamed);
            }
        }
        for (_, old_path, _) in &rewritten_inodes {
            self.detach_path(old_path, true);
        }
        for (ino, _, renamed) in rewritten_inodes {
            self.attach_path(ino, renamed);
        }
        for handle in self.handles.values_mut() {
            if let Some(renamed) = renamed_path(&handle.path, from, &to) {
                handle.path = renamed;
            }
        }
        for held in self.held_locks.values_mut() {
            if let Some(renamed) = renamed_path(&held.path, from, &to) {
                held.path = renamed;
            }
        }
        for pending in self.pending_locks.values_mut() {
            if let Some(renamed) = renamed_path(&pending.path, from, &to) {
                pending.path = renamed;
            }
        }
    }

    fn allocate_handle(&mut self, path: String, stat: dfs::DfsStat, flags: i32) -> u64 {
        let fh = self.next_fh;
        self.next_fh = self.next_fh.saturating_add(1);
        let logical_size = stat.size_bytes.unwrap_or(0);
        self.handles.insert(
            fh,
            OpenFileHandle {
                path,
                object_id: stat.object_id.clone(),
                opened_stat: stat,
                logical_size,
                dirty: DirtyWriteBuffer::default(),
                writable: flags_has_write_access(flags),
                sync_mode: HandleSyncMode::from_open_flags(flags),
            },
        );
        fh
    }

    #[cfg(test)]
    fn refresh_handle_stat(
        &mut self,
        fh: u64,
        stat: &dfs::DfsStat,
    ) -> std::result::Result<(), libc::c_int> {
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if handle.object_id != stat.object_id {
            return Err(libc::ESTALE);
        }
        handle.refresh_stat(stat);
        Ok(())
    }

    fn refresh_object_handle_stats(
        &mut self,
        object_id: &dfs::DfsObjectId,
        stat: &dfs::DfsStat,
    ) -> std::result::Result<(), libc::c_int> {
        if &stat.object_id != object_id {
            return Err(libc::ESTALE);
        }
        for handle in self
            .handles
            .values_mut()
            .filter(|handle| &handle.object_id == object_id)
        {
            handle.refresh_stat(stat);
        }
        Ok(())
    }

    fn buffer_handle_write(
        &mut self,
        fh: u64,
        offset: u64,
        bytes: &[u8],
    ) -> std::result::Result<DirtyWriteInsert, libc::c_int> {
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if !handle.writable {
            return Err(libc::EBADF);
        }
        if handle.sync_mode != HandleSyncMode::None {
            return Ok(DirtyWriteInsert::PublishDirectly);
        }
        let end = offset
            .checked_add(u64::try_from(bytes.len()).map_err(|_| libc::EFBIG)?)
            .ok_or(libc::EFBIG)?;
        let outcome = handle
            .dirty
            .try_write(offset, bytes, MAX_DIRTY_WRITE_BYTES_PER_HANDLE)?;
        if outcome == DirtyWriteInsert::Buffered && !bytes.is_empty() {
            handle.logical_size = handle.logical_size.max(end);
        }
        Ok(outcome)
    }

    fn take_dirty_publication(
        &mut self,
        fh: u64,
    ) -> std::result::Result<Option<FuseDirtyPublication>, libc::c_int> {
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if handle.dirty.is_empty() {
            return Ok(None);
        }
        Ok(Some(FuseDirtyPublication {
            object_id: handle.object_id.clone(),
            dirty: std::mem::take(&mut handle.dirty),
        }))
    }

    fn restore_dirty_publication(
        &mut self,
        fh: u64,
        publication: FuseDirtyPublication,
    ) -> std::result::Result<(), libc::c_int> {
        let handle = self.handles.get_mut(&fh).ok_or(libc::EBADF)?;
        if handle.object_id != publication.object_id {
            return Err(libc::ESTALE);
        }
        handle.dirty.restore_older(publication.dirty);
        Ok(())
    }

    fn dirty_handles_for_path(&self, path: &str, include_descendants: bool) -> Vec<u64> {
        let matching_objects = self
            .handles
            .values()
            .filter(|handle| {
                handle.path == path
                    || (include_descendants && renamed_path(&handle.path, path, path).is_some())
            })
            .map(|handle| handle.object_id.clone())
            .collect::<BTreeSet<_>>();
        self.handles
            .iter()
            .filter_map(|(&fh, handle)| {
                let matches = handle.path == path
                    || (include_descendants && renamed_path(&handle.path, path, path).is_some())
                    || matching_objects.contains(&handle.object_id);
                (matches && !handle.dirty.is_empty()).then_some(fh)
            })
            .collect()
    }

    fn dirty_handles_for_object(
        &self,
        object_id: &dfs::DfsObjectId,
        except_fh: Option<u64>,
    ) -> Vec<u64> {
        self.handles
            .iter()
            .filter_map(|(&fh, handle)| {
                (Some(fh) != except_fh
                    && &handle.object_id == object_id
                    && !handle.dirty.is_empty())
                .then_some(fh)
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
struct OpenFileHandle {
    path: String,
    object_id: dfs::DfsObjectId,
    // Exact immutable content identity last published by this descriptor.
    // Dirty data is deliberately separate so opened-version reads never ask a
    // peer for bytes under a size/version combination that was not committed.
    opened_stat: dfs::DfsStat,
    logical_size: u64,
    dirty: DirtyWriteBuffer,
    writable: bool,
    sync_mode: HandleSyncMode,
}

impl OpenFileHandle {
    fn visible_stat(&self) -> dfs::DfsStat {
        let mut stat = self.opened_stat.clone();
        stat.size_bytes = Some(self.logical_size);
        stat
    }

    fn refresh_stat(&mut self, stat: &dfs::DfsStat) {
        self.opened_stat = stat.clone();
        let committed_size = stat.size_bytes.unwrap_or(0);
        self.logical_size = if self.dirty.is_empty() {
            committed_size
        } else {
            self.logical_size.max(committed_size)
        };
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandleSyncMode {
    None,
    Data,
    Full,
}

impl HandleSyncMode {
    fn from_open_flags(flags: i32) -> Self {
        // Linux defines O_SYNC as O_DSYNC plus an additional bit, so test the
        // stronger mode first.
        if flags & libc::O_SYNC == libc::O_SYNC {
            Self::Full
        } else if flags & libc::O_DSYNC == libc::O_DSYNC {
            Self::Data
        } else {
            Self::None
        }
    }

    fn kind(self) -> Option<dfs::DfsPosixSyncKind> {
        match self {
            Self::None => None,
            Self::Data => Some(dfs::DfsPosixSyncKind::FileData),
            Self::Full => Some(dfs::DfsPosixSyncKind::File),
        }
    }
}

#[derive(Clone, Debug)]
struct FuseHeldLock {
    path: String,
    object_id: dfs::DfsObjectId,
    session_id: dfs::DfsAdvisoryLockSessionId,
    grant: dfs::DfsAdvisoryLockGrant,
    local_lease_deadline: Instant,
    associated_fhs: BTreeSet<u64>,
}

#[derive(Clone, Debug)]
struct FusePendingLock {
    path: String,
    object_id: dfs::DfsObjectId,
    session_id: dfs::DfsAdvisoryLockSessionId,
    acquire_request_id: dfs::DfsAdvisoryLockRequestId,
    associated_fhs: BTreeSet<u64>,
}

#[derive(Clone, Debug, Default)]
struct FuseLostLock {
    associated_fhs: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FuseLockInstallOutcome {
    Installed,
    Expired,
    Cancelled,
}

impl Drop for SifsFuseFileSystem {
    fn drop(&mut self) {
        self.lock_renewer_stop.store(true, Ordering::Release);
        // Field teardown drops the executor after this hook; cancellation must
        // be visible before it starts joining in-flight workers.
        self.read_cancellation.cancel();
    }
}

fn next_lock_request_id(sequence: &AtomicU64, operation: &str) -> dfs::DfsAdvisoryLockRequestId {
    dfs::DfsAdvisoryLockRequestId::new(format!(
        "fuse-{operation}-{}-{}",
        system_time_to_nanos(SystemTime::now()),
        sequence.fetch_add(1, Ordering::Relaxed)
    ))
}

fn fuse_held_lock(
    path: String,
    object_id: dfs::DfsObjectId,
    session_id: dfs::DfsAdvisoryLockSessionId,
    grant: dfs::DfsAdvisoryLockGrant,
    local_lease_deadline: Option<Instant>,
    associated_fhs: BTreeSet<u64>,
) -> Option<FuseHeldLock> {
    let local_lease_deadline =
        local_lease_deadline.filter(|deadline| *deadline > Instant::now())?;
    Some(FuseHeldLock {
        path,
        object_id,
        session_id,
        grant,
        local_lease_deadline,
        associated_fhs,
    })
}

#[derive(Debug)]
struct FuseLockCommandResult {
    outcome: dfs::DfsAdvisoryLockOutcome,
    local_lease_deadline: Option<Instant>,
}

fn lock_outcome_lease_remaining(outcome: &dfs::DfsAdvisoryLockOutcome) -> Option<Duration> {
    let grant = match outcome {
        dfs::DfsAdvisoryLockOutcome::Acquired { grant }
        | dfs::DfsAdvisoryLockOutcome::Renewed { grant }
        | dfs::DfsAdvisoryLockOutcome::Converted { grant } => grant,
        _ => return None,
    };
    Some(Duration::from_nanos(grant.lease_remaining_nanos))
}

fn conservative_local_lease_deadline(
    outcome: &dfs::DfsAdvisoryLockOutcome,
    request_started_at: Instant,
) -> Option<Instant> {
    lock_outcome_lease_remaining(outcome)
        .filter(|remaining| !remaining.is_zero())
        .and_then(|remaining| request_started_at.checked_add(remaining))
}

fn renamed_path(path: &str, from: &str, to: &str) -> Option<String> {
    if path == from {
        return Some(to.to_string());
    }
    let suffix = path.strip_prefix(from)?;
    suffix.starts_with('/').then(|| format!("{to}{suffix}"))
}

fn run_advisory_lock_command(
    service: &Arc<Mutex<SifsService>>,
    path: &str,
    object_id: &dfs::DfsObjectId,
    command: dfs::DfsAdvisoryLockCommand,
) -> std::result::Result<FuseLockCommandResult, libc::c_int> {
    // Anchor before the first attempt. The authority reports a relative
    // duration after it has durably settled the transition; projecting that
    // duration from this earlier instant is conservative across request,
    // response, and exact-retry delay and can never extend the server lease.
    let started_at = Instant::now();
    let deadline = started_at
        .checked_add(FUSE_LOCK_RPC_TIMEOUT)
        .ok_or(libc::EIO)?;
    let mut service = lock_service_until(service, deadline)?;
    run_advisory_lock_command_with_service(
        &mut service,
        path,
        object_id,
        command,
        started_at,
        deadline,
    )
}

fn lock_service_until<'a>(
    service: &'a Arc<Mutex<SifsService>>,
    deadline: Instant,
) -> std::result::Result<MutexGuard<'a, SifsService>, libc::c_int> {
    loop {
        match service.try_lock() {
            Ok(service) => return Ok(service),
            Err(TryLockError::Poisoned(_)) => return Err(libc::EIO),
            Err(TryLockError::WouldBlock) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(libc::ETIMEDOUT);
                }
                thread::sleep(FUSE_LOCK_QUEUE_POLL_INTERVAL.min(remaining));
            }
        }
    }
}

fn run_advisory_lock_command_with_service(
    service: &mut SifsService,
    path: &str,
    object_id: &dfs::DfsObjectId,
    command: dfs::DfsAdvisoryLockCommand,
    started_at: Instant,
    deadline: Instant,
) -> std::result::Result<FuseLockCommandResult, libc::c_int> {
    let retry_command = command.clone();
    let first_budget = deadline.saturating_duration_since(Instant::now());
    if first_budget.is_zero() {
        return Err(libc::ETIMEDOUT);
    }
    let first =
        service.posix_advisory_lock_for_object_with_timeout(path, object_id, command, first_budget);
    let outcome = match first {
        Ok(outcome) => Ok(outcome),
        Err(first_error) => {
            if std::env::var_os("SINOMO_FUSE_LOCK_DEBUG").is_some() {
                eprintln!("sinomo-dfs-fuse: advisory-lock first attempt failed: {first_error:?}");
            }
            let retry_budget = deadline.saturating_duration_since(Instant::now());
            if retry_budget.is_zero() {
                return Err(errno_for_posix_error(first_error));
            }
            service
                .posix_advisory_lock_for_object_with_timeout(
                    path,
                    object_id,
                    retry_command,
                    retry_budget,
                )
                .map_err(|error| {
                    if std::env::var_os("SINOMO_FUSE_LOCK_DEBUG").is_some() {
                        eprintln!("sinomo-dfs-fuse: advisory-lock exact retry failed: {error:?}");
                    }
                    errno_for_posix_error(error)
                })
        }
    }?;
    let local_lease_deadline = conservative_local_lease_deadline(&outcome, started_at);
    Ok(FuseLockCommandResult {
        outcome,
        local_lease_deadline,
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_blocking_lock_waiter(
    service: Weak<Mutex<SifsService>>,
    state: Weak<Mutex<FuseState>>,
    stop: Arc<AtomicBool>,
    key: (dfs::DfsObjectId, u64),
    command: dfs::DfsAdvisoryLockCommand,
    reply: ReplyEmpty,
) {
    thread::spawn(move || {
        let mut reply = Some(reply);
        let acquire_request_id = command.request_id().clone();
        loop {
            if stop.load(Ordering::Acquire) {
                remove_pending_lock(&state, &key, &acquire_request_id);
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error(libc::EINTR);
                return;
            }
            let Some(state_arc) = state.upgrade() else {
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error(libc::EINTR);
                return;
            };
            let current_pending = state_arc
                .lock()
                .ok()
                .and_then(|state| state.pending_locks.get(&key).cloned())
                .filter(|pending| pending.acquire_request_id == acquire_request_id);
            let Some(current_pending) = current_pending else {
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error(libc::EINTR);
                return;
            };
            let Some(service_arc) = service.upgrade() else {
                remove_pending_lock(&state, &key, &acquire_request_id);
                reply.take().expect("blocking lock reply").error(libc::EIO);
                return;
            };
            let started_at = Instant::now();
            let Some(deadline) = started_at.checked_add(FUSE_LOCK_RPC_TIMEOUT) else {
                remove_pending_lock(&state, &key, &acquire_request_id);
                reply.take().expect("blocking lock reply").error(libc::EIO);
                return;
            };
            let mut service_guard = match lock_service_until(&service_arc, deadline) {
                Ok(service) => service,
                Err(libc::ETIMEDOUT) => {
                    thread::sleep(BLOCKING_LOCK_POLL_INTERVAL);
                    continue;
                }
                Err(errno) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply.take().expect("blocking lock reply").error(errno);
                    return;
                }
            };
            // Cancel/close removes the pending marker before waiting for this
            // serialized RPC lane. Recheck after acquiring it so an Acquire
            // cannot be sent after cleanup has already linearized locally.
            let still_pending = state_arc.lock().ok().is_some_and(|state| {
                state
                    .pending_locks
                    .get(&key)
                    .is_some_and(|pending| pending.acquire_request_id == acquire_request_id)
            });
            if !still_pending {
                reply
                    .take()
                    .expect("blocking lock reply")
                    .error(libc::EINTR);
                return;
            }
            let outcome = run_advisory_lock_command_with_service(
                &mut service_guard,
                &current_pending.path,
                &current_pending.object_id,
                command.clone(),
                started_at,
                deadline,
            );
            drop(service_guard);
            match outcome {
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Acquired { grant },
                    local_lease_deadline,
                }) => {
                    let install_outcome = match state_arc.lock() {
                        Ok(mut state) => {
                            let current_pending = state
                                .pending_locks
                                .get(&key)
                                .filter(|pending| pending.acquire_request_id == acquire_request_id)
                                .cloned();
                            let Some(current_pending) = current_pending else {
                                reply
                                    .take()
                                    .expect("blocking lock reply")
                                    .error(libc::EINTR);
                                return;
                            };
                            state.pending_locks.remove(&key);
                            let associated_fhs = current_pending.associated_fhs.clone();
                            if let Some(held) = fuse_held_lock(
                                current_pending.path,
                                current_pending.object_id,
                                current_pending.session_id,
                                grant,
                                local_lease_deadline,
                                associated_fhs.clone(),
                            ) {
                                state.held_locks.insert(key.clone(), held);
                                state.lost_locks.remove(&key);
                                FuseLockInstallOutcome::Installed
                            } else {
                                state.record_lost_lock(key.clone(), associated_fhs);
                                FuseLockInstallOutcome::Expired
                            }
                        }
                        Err(_) => FuseLockInstallOutcome::Cancelled,
                    };
                    match install_outcome {
                        FuseLockInstallOutcome::Installed => {
                            reply.take().expect("blocking lock reply").ok()
                        }
                        FuseLockInstallOutcome::Expired => {
                            reply.take().expect("blocking lock reply").error(libc::EIO)
                        }
                        FuseLockInstallOutcome::Cancelled => reply
                            .take()
                            .expect("blocking lock reply")
                            .error(libc::EINTR),
                    }
                    return;
                }
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Waiting { .. },
                    ..
                }) => {
                    thread::sleep(BLOCKING_LOCK_POLL_INTERVAL);
                }
                Ok(FuseLockCommandResult {
                    outcome:
                        dfs::DfsAdvisoryLockOutcome::AuthorityWarming {
                            retry_after_unix_nanos,
                        },
                    ..
                }) => {
                    let remaining = retry_after_unix_nanos
                        .saturating_sub(system_time_to_nanos(SystemTime::now()));
                    thread::sleep(Duration::from_nanos(remaining).min(BLOCKING_LOCK_POLL_INTERVAL));
                }
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::Cancelled { .. },
                    ..
                }) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error(libc::EINTR);
                    return;
                }
                Ok(FuseLockCommandResult {
                    outcome: dfs::DfsAdvisoryLockOutcome::WaitExpired { .. },
                    ..
                }) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error(libc::ETIMEDOUT);
                    return;
                }
                Ok(response) => {
                    remove_pending_lock(&state, &key, &acquire_request_id);
                    reply
                        .take()
                        .expect("blocking lock reply")
                        .error(errno_for_unexpected_lock_outcome(&response.outcome));
                    return;
                }
                Err(errno) => {
                    if let Ok(mut state) = state_arc.lock()
                        && let Some(pending) = state.pending_locks.remove(&key)
                    {
                        state.record_lost_lock(key.clone(), pending.associated_fhs);
                    }
                    reply.take().expect("blocking lock reply").error(errno);
                    return;
                }
            }
        }
    });
}

fn remove_pending_lock(
    state: &Weak<Mutex<FuseState>>,
    key: &(dfs::DfsObjectId, u64),
    acquire_request_id: &dfs::DfsAdvisoryLockRequestId,
) {
    if let Some(state) = state.upgrade()
        && let Ok(mut state) = state.lock()
        && state
            .pending_locks
            .get(key)
            .is_some_and(|pending| &pending.acquire_request_id == acquire_request_id)
    {
        state.pending_locks.remove(key);
    }
}

fn spawn_lock_renewer(
    service: Weak<Mutex<SifsService>>,
    state: Weak<Mutex<FuseState>>,
    request_sequence: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            thread::sleep(LOCK_RENEW_INTERVAL);
            if stop.load(Ordering::Acquire) {
                return;
            }
            let (Some(service), Some(state)) = (service.upgrade(), state.upgrade()) else {
                return;
            };
            let held = match state.lock() {
                Ok(state) => state
                    .held_locks
                    .iter()
                    .map(|(key, held)| (key.clone(), held.clone()))
                    .collect::<Vec<_>>(),
                Err(_) => return,
            };
            for (key, held) in held {
                if stop.load(Ordering::Acquire) {
                    return;
                }
                let command = dfs::DfsAdvisoryLockCommand::Renew {
                    request_id: next_lock_request_id(&request_sequence, "renew"),
                    token: held.grant.token.clone(),
                };
                match run_advisory_lock_command(&service, &held.path, &held.object_id, command) {
                    Ok(FuseLockCommandResult {
                        outcome: dfs::DfsAdvisoryLockOutcome::Renewed { grant },
                        local_lease_deadline,
                    }) => {
                        if let Some(local_lease_deadline) = local_lease_deadline
                            && local_lease_deadline > Instant::now()
                        {
                            if let Ok(mut state) = state.lock()
                                && let Some(current) = state.held_locks.get_mut(&key)
                                && current.grant == held.grant
                            {
                                current.grant = grant;
                                current.local_lease_deadline = local_lease_deadline;
                            }
                        } else {
                            mark_fuse_lock_lost(&state, &key, &held.grant);
                        }
                    }
                    Ok(response) if lock_outcome_loses_grant(&response.outcome) => {
                        mark_fuse_lock_lost(&state, &key, &held.grant);
                    }
                    Ok(_) | Err(_) => {
                        if held.local_lease_deadline <= Instant::now() {
                            mark_fuse_lock_lost(&state, &key, &held.grant);
                        }
                    }
                }
            }
        }
    });
}

fn mark_fuse_lock_lost(
    state: &Arc<Mutex<FuseState>>,
    key: &(dfs::DfsObjectId, u64),
    expected_grant: &dfs::DfsAdvisoryLockGrant,
) {
    if let Ok(mut state) = state.lock() {
        let is_expected = state
            .held_locks
            .get(key)
            .is_some_and(|current| current.grant == *expected_grant);
        if is_expected {
            let held = state
                .held_locks
                .remove(key)
                .expect("expected advisory lock disappeared while state was held");
            state.record_lost_lock(key.clone(), held.associated_fhs);
        }
    }
}

fn is_whole_file_lock_range(start: u64, end: u64) -> bool {
    start == 0 && (end == u64::MAX || end == i64::MAX as u64)
}

fn advisory_lock_mode(
    lock_type: i32,
) -> std::result::Result<dfs::DfsAdvisoryLockMode, libc::c_int> {
    match lock_type {
        libc::F_RDLCK => Ok(dfs::DfsAdvisoryLockMode::Shared),
        libc::F_WRLCK => Ok(dfs::DfsAdvisoryLockMode::Exclusive),
        _ => Err(libc::EINVAL),
    }
}

fn advisory_lock_type(mode: dfs::DfsAdvisoryLockMode) -> i32 {
    match mode {
        dfs::DfsAdvisoryLockMode::Shared => libc::F_RDLCK,
        dfs::DfsAdvisoryLockMode::Exclusive => libc::F_WRLCK,
    }
}

fn lock_outcome_loses_grant(outcome: &dfs::DfsAdvisoryLockOutcome) -> bool {
    matches!(
        outcome,
        dfs::DfsAdvisoryLockOutcome::Fenced { .. }
            | dfs::DfsAdvisoryLockOutcome::LockLost
            | dfs::DfsAdvisoryLockOutcome::StaleToken
            | dfs::DfsAdvisoryLockOutcome::StaleAuthority { .. }
    )
}

fn errno_for_unexpected_lock_outcome(outcome: &dfs::DfsAdvisoryLockOutcome) -> libc::c_int {
    match outcome {
        dfs::DfsAdvisoryLockOutcome::WouldBlock { .. }
        | dfs::DfsAdvisoryLockOutcome::Waiting { .. }
        | dfs::DfsAdvisoryLockOutcome::AuthorityWarming { .. } => libc::EAGAIN,
        dfs::DfsAdvisoryLockOutcome::SessionBusy => libc::EDEADLK,
        dfs::DfsAdvisoryLockOutcome::RequestMismatch => libc::EINVAL,
        dfs::DfsAdvisoryLockOutcome::WaitExpired { .. } => libc::ETIMEDOUT,
        dfs::DfsAdvisoryLockOutcome::Cancelled { .. } => libc::EINTR,
        dfs::DfsAdvisoryLockOutcome::Fenced { .. }
        | dfs::DfsAdvisoryLockOutcome::LockLost
        | dfs::DfsAdvisoryLockOutcome::StaleToken
        | dfs::DfsAdvisoryLockOutcome::StaleAuthority { .. } => libc::EIO,
        _ => libc::EIO,
    }
}

fn file_attr(ino: u64, stat: &dfs::DfsStat) -> FileAttr {
    let size = stat.size_bytes.unwrap_or(0);
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: nanos_to_system_time(stat.accessed_at_unix_nanos),
        mtime: nanos_to_system_time(stat.modified_at_unix_nanos),
        ctime: nanos_to_system_time(stat.changed_at_unix_nanos),
        crtime: nanos_to_system_time(stat.created_at_unix_nanos),
        kind: file_type_for_kind(&stat.kind),
        perm: (stat.mode & 0o7777) as u16,
        nlink: stat.link_count.min(u32::MAX as u64) as u32,
        uid: stat.uid,
        gid: stat.gid,
        rdev: 0,
        blksize: DEFAULT_BLOCK_SIZE,
        flags: 0,
    }
}

fn file_type_for_kind(kind: &DfsObjectKind) -> FileType {
    match kind {
        DfsObjectKind::Directory | DfsObjectKind::Mount => FileType::Directory,
        DfsObjectKind::Symlink => FileType::Symlink,
        DfsObjectKind::File
        | DfsObjectKind::Artifact
        | DfsObjectKind::Hardlink
        | DfsObjectKind::Tombstone => FileType::RegularFile,
    }
}

fn errno_for_posix_error(error: DfsPosixError) -> libc::c_int {
    match error {
        DfsPosixError::NotFound { .. } => libc::ENOENT,
        DfsPosixError::AlreadyExists { .. } => libc::EEXIST,
        DfsPosixError::NotDirectory { .. } => libc::ENOTDIR,
        DfsPosixError::IsDirectory { .. } => libc::EISDIR,
        DfsPosixError::NotSymlink { .. } => libc::EINVAL,
        DfsPosixError::MissingParent { .. } => libc::ENOENT,
        DfsPosixError::InvalidPath { .. } => libc::EINVAL,
        DfsPosixError::PendingWrite { .. } => libc::EAGAIN,
        DfsPosixError::Conflict { .. } => libc::ESTALE,
        DfsPosixError::DirectoryNotEmpty { .. } => libc::ENOTEMPTY,
        // In-flight cross-shard moves reject mutations transiently; tell the
        // application to retry rather than reporting an I/O failure.
        DfsPosixError::Dfs(DfsError::Busy { .. }) => libc::EAGAIN,
        DfsPosixError::Dfs(_) | DfsPosixError::Metadata(_) => libc::EIO,
    }
}

fn created_mode(mode: u32, umask: u32) -> u32 {
    (mode & 0o7777) & !(umask & 0o777)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingFileCreateAction {
    Reject,
    ReturnExisting,
    Truncate,
}

fn existing_file_create_action(flags: i32) -> ExistingFileCreateAction {
    if flags & libc::O_EXCL != 0 {
        ExistingFileCreateAction::Reject
    } else if flags & libc::O_TRUNC != 0 {
        ExistingFileCreateAction::Truncate
    } else {
        ExistingFileCreateAction::ReturnExisting
    }
}

fn create_file_for_fuse(
    dfs: &mut SifsService,
    path: &str,
    options: DfsWriteOptions,
    attrs: DfsPosixCreateAttrs,
    flags: i32,
) -> std::result::Result<dfs::DfsStat, libc::c_int> {
    if existing_file_create_action(flags) == ExistingFileCreateAction::Reject {
        return dfs
            .posix_create_file_with_attrs(path, options, attrs)
            .map_err(errno_for_posix_error);
    }

    let mut create_needed = true;
    for _ in 0..=FUSE_CREATE_RACE_RETRY_ROUNDS {
        if create_needed {
            match dfs.posix_create_file_following_symlink_with_attrs(path, options.clone(), attrs) {
                Ok(created) => return Ok(created),
                Err(DfsPosixError::AlreadyExists { .. }) => create_needed = false,
                Err(error) => return Err(errno_for_posix_error(error)),
            }
        }
        match dfs.posix_stat_distributed(path) {
            Ok(existing) => {
                if existing.kind == DfsObjectKind::Directory {
                    return Err(libc::EISDIR);
                }
                if existing.kind != DfsObjectKind::File {
                    return Err(libc::EINVAL);
                }
                if existing_file_create_action(flags) == ExistingFileCreateAction::Truncate {
                    // The create attrs are ignored while this inode still
                    // exists, but become the initial attrs if an unlink wins
                    // just before the truncating write recreates the path.
                    return dfs
                        .posix_write_file_with_create_attrs(path, &[], options.clone(), attrs)
                        .map_err(errno_for_posix_error);
                }
                match dfs.posix_stat_distributed(path) {
                    Ok(confirmed) if confirmed.object_id == existing.object_id => {
                        return Ok(confirmed);
                    }
                    Ok(_) => continue,
                    Err(DfsPosixError::NotFound { .. }) => {
                        create_needed = true;
                        continue;
                    }
                    Err(error) => return Err(errno_for_posix_error(error)),
                }
            }
            Err(DfsPosixError::NotFound { .. }) => {
                create_needed = true;
                continue;
            }
            Err(error) => return Err(errno_for_posix_error(error)),
        }
    }
    Err(libc::EAGAIN)
}

fn fuse_write_replica_candidates(
    replica_candidates: Vec<dfs::DfsNodeId>,
    local_node_id: dfs::DfsNodeId,
) -> Vec<dfs::DfsNodeId> {
    if replica_candidates.is_empty() {
        vec![local_node_id]
    } else {
        replica_candidates
    }
}

fn fuse_write_options(replica_candidates: Vec<dfs::DfsNodeId>) -> DfsWriteOptions {
    let mut options = DfsWriteOptions::replicated_eventual(replica_candidates);
    // Replicated-eventual becomes visible once its W durable receipt threshold
    // is met. Finishing the desired N-W replicas belongs to repair; waiting for
    // it on every kernel write serializes each FUSE chunk behind another WAN
    // round trip. fsync still captures the returned version and waits for all
    // desired receipts through posix_sync_file_target.
    options.foreground_replica_repair = false;
    options
}

fn flags_has_write_access(flags: i32) -> bool {
    matches!(flags & libc::O_ACCMODE, libc::O_WRONLY | libc::O_RDWR)
}

fn nanos_to_system_time(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

fn system_time_to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn time_or_now_to_nanos(time: TimeOrNow) -> u64 {
    match time {
        TimeOrNow::SpecificTime(time) => system_time_to_nanos(time),
        TimeOrNow::Now => system_time_to_nanos(SystemTime::now()),
    }
}

#[cfg(test)]
mod errno_tests {
    use super::*;

    #[test]
    fn fuse_mount_helper_lookup_requires_an_executable_helper() {
        let root = sinomo_testdir::TestDir::new("sinomo-fuse-helper-path");
        let helper = root.join("fusermount3");
        std::fs::write(&helper, b"helper").expect("write helper fixture");

        assert!(fuse_mount_helper_on_path(Some(root.as_os_str())).is_none());
        let mut permissions = std::fs::metadata(&helper)
            .expect("helper metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&helper, permissions).expect("make helper executable");
        assert_eq!(
            fuse_mount_helper_on_path(Some(root.as_os_str())),
            Some(helper)
        );
        assert!(fuse_mount_helper_on_path(None).is_none());
    }

    #[test]
    fn stale_mount_recovery_only_classifies_enotconn() {
        assert!(is_disconnected_mount_error(&io::Error::from_raw_os_error(
            libc::ENOTCONN
        )));
        assert!(!is_disconnected_mount_error(&io::Error::from_raw_os_error(
            libc::EACCES
        )));
        assert!(!is_disconnected_mount_error(&io::Error::new(
            io::ErrorKind::NotFound,
            "missing"
        )));
    }

    #[test]
    fn owner_only_foreground_mount_omits_auto_unmount_and_allow_other() {
        let options = DfsFuseMountOptions::new("/mnt/sinomo");

        let direct_options = fuse_mount_options(&options, false);
        assert!(!direct_options.contains(&MountOption::AutoUnmount));
        assert!(!direct_options.contains(&MountOption::AllowOther));

        let helper_options = fuse_mount_options(&options, true);
        assert!(!helper_options.contains(&MountOption::AutoUnmount));
        assert!(!helper_options.contains(&MountOption::AllowOther));
    }

    #[test]
    fn allow_other_mount_retains_helper_auto_unmount() {
        let mut options = DfsFuseMountOptions::new("/mnt/sinomo");
        options.allow_other = true;

        let direct_options = fuse_mount_options(&options, false);
        assert!(!direct_options.contains(&MountOption::AutoUnmount));
        assert!(direct_options.contains(&MountOption::AllowOther));

        let helper_options = fuse_mount_options(&options, true);
        assert!(helper_options.contains(&MountOption::AutoUnmount));
        assert!(helper_options.contains(&MountOption::AllowOther));
    }

    #[test]
    fn owner_only_background_mount_does_not_implicitly_request_allow_other() {
        let options = DfsFuseMountOptions::new("/mnt/sinomo");

        let background_options = background_fuse_mount_options(&options, true);
        assert!(!background_options.contains(&MountOption::AutoUnmount));
        assert!(!background_options.contains(&MountOption::AllowOther));
    }

    #[test]
    fn created_entry_mode_applies_the_request_umask() {
        assert_eq!(created_mode(libc::S_IFREG | 0o666, 0o022), 0o644);
        assert_eq!(created_mode(libc::S_IFDIR | 0o777, 0o027), 0o750);
        assert_eq!(created_mode(0o700, 0), 0o700);
        assert_eq!(created_mode(0o6777, 0o7000), 0o6777);
    }

    #[test]
    fn create_flags_preserve_existing_files_unless_exclusive_or_truncating() {
        assert_eq!(
            existing_file_create_action(libc::O_CREAT),
            ExistingFileCreateAction::ReturnExisting
        );
        assert_eq!(
            existing_file_create_action(libc::O_CREAT | libc::O_TRUNC),
            ExistingFileCreateAction::Truncate
        );
        assert_eq!(
            existing_file_create_action(libc::O_CREAT | libc::O_EXCL),
            ExistingFileCreateAction::Reject
        );
        assert_eq!(
            existing_file_create_action(libc::O_CREAT | libc::O_EXCL | libc::O_TRUNC),
            ExistingFileCreateAction::Reject
        );
    }

    fn test_object_id(name: &str) -> dfs::DfsObjectId {
        dfs::DfsObjectId::from_legacy_named(name).expect("test object id")
    }

    fn test_stat(object_name: &str, version_name: &str, path: &str) -> dfs::DfsStat {
        dfs::DfsStat {
            file_system_id: dfs::DfsFileSystemId::from_legacy_named("sifs")
                .expect("test filesystem id"),
            object_id: test_object_id(object_name),
            path: path.to_string(),
            kind: DfsObjectKind::File,
            version: dfs::DfsVersion::from_legacy_named(version_name).expect("test version"),
            content_hash: Some(format!("hash-{version_name}")),
            size_bytes: Some(12),
            link_count: 1,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            accessed_at_unix_nanos: 1,
            modified_at_unix_nanos: 2,
            created_at_unix_nanos: 3,
            changed_at_unix_nanos: 4,
            policy: sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual,
            current_locations: vec![
                dfs::DfsNodeId::from_legacy_named("fuse-test-node").expect("test node id"),
            ],
        }
    }

    fn test_grant(
        object_id: dfs::DfsObjectId,
        grant_id: &str,
        mode: dfs::DfsAdvisoryLockMode,
    ) -> dfs::DfsAdvisoryLockGrant {
        dfs::DfsAdvisoryLockGrant {
            token: dfs::DfsAdvisoryLockToken {
                file_system_id: dfs::DfsFileSystemId::from_legacy_named("sifs")
                    .expect("test filesystem id"),
                object_id,
                session_id: dfs::DfsAdvisoryLockSessionId::new("test-session"),
                authority_epoch: 7,
                grant_id: dfs::DfsAdvisoryLockRequestId::new(grant_id),
            },
            mode,
            process_id: Some(42),
            lease_deadline_unix_nanos: 999,
            lease_remaining_nanos: Duration::from_secs(60).as_nanos() as u64,
        }
    }

    fn live_local_lease_deadline() -> Option<Instant> {
        Instant::now().checked_add(Duration::from_secs(60))
    }

    #[test]
    fn busy_cross_shard_moves_map_to_eagain() {
        assert_eq!(
            errno_for_posix_error(DfsPosixError::Dfs(DfsError::Busy {
                path: "/users/moving.txt".to_string(),
            })),
            libc::EAGAIN
        );
        // Other internal errors still surface as I/O failures.
        assert_eq!(
            errno_for_posix_error(DfsPosixError::Dfs(DfsError::Distributed {
                message: "boom".to_string(),
            })),
            libc::EIO
        );
    }

    #[test]
    fn distributed_lock_surface_only_accepts_whole_file_ranges() {
        assert!(is_whole_file_lock_range(0, u64::MAX));
        assert!(is_whole_file_lock_range(0, i64::MAX as u64));
        assert!(!is_whole_file_lock_range(1, u64::MAX));
        assert!(!is_whole_file_lock_range(0, 4095));
    }

    #[test]
    fn fuse_lock_types_map_to_shared_and_exclusive_modes() {
        assert_eq!(
            advisory_lock_mode(libc::F_RDLCK),
            Ok(dfs::DfsAdvisoryLockMode::Shared)
        );
        assert_eq!(
            advisory_lock_mode(libc::F_WRLCK),
            Ok(dfs::DfsAdvisoryLockMode::Exclusive)
        );
        assert_eq!(advisory_lock_mode(libc::F_UNLCK), Err(libc::EINVAL));
        assert_eq!(
            advisory_lock_type(dfs::DfsAdvisoryLockMode::Shared),
            libc::F_RDLCK
        );
        assert_eq!(
            advisory_lock_type(dfs::DfsAdvisoryLockMode::Exclusive),
            libc::F_WRLCK
        );
    }

    #[test]
    fn open_sync_flags_select_the_matching_distributed_barrier() {
        assert_eq!(
            HandleSyncMode::from_open_flags(libc::O_RDWR),
            HandleSyncMode::None
        );
        assert_eq!(
            HandleSyncMode::from_open_flags(libc::O_RDWR | libc::O_DSYNC).kind(),
            Some(dfs::DfsPosixSyncKind::FileData)
        );
        assert_eq!(
            HandleSyncMode::from_open_flags(libc::O_RDWR | libc::O_SYNC).kind(),
            Some(dfs::DfsPosixSyncKind::File)
        );
    }

    #[test]
    fn ordinary_fuse_writes_commit_at_w_and_leave_n_minus_w_to_repair() {
        let local = dfs::DfsNodeId::from_legacy_named("fuse-write-local").expect("local node");
        let replica =
            dfs::DfsNodeId::from_legacy_named("fuse-write-replica").expect("replica node");

        let options = fuse_write_options(vec![local, replica]);

        assert!(options.wait_for_commit, "a returned write must be visible");
        assert_eq!(options.policy.min_commit_replicas, 1);
        assert_eq!(options.policy.desired_replicas, 2);
        assert!(
            !options.foreground_replica_repair,
            "replicated-eventual repair must not extend every FUSE write"
        );
    }

    #[test]
    fn sequential_eight_mib_fuse_chunks_coalesce_to_one_publication() {
        let path = "/root/sequential.bin";
        let mut opened = test_stat("fusesequentialobject", "fusesequentialversion", path);
        opened.size_bytes = Some(0);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened, libc::O_RDWR);
        let chunk = vec![0x5a; 64 * 1024];

        for index in 0..128u64 {
            assert_eq!(
                state
                    .buffer_handle_write(fh, index * chunk.len() as u64, &chunk)
                    .expect("buffer sequential kernel chunk"),
                DirtyWriteInsert::Buffered
            );
        }

        let handle = state.handles.get(&fh).expect("open handle");
        assert_eq!(handle.dirty.len(), 1);
        assert_eq!(handle.dirty.buffered_bytes(), 8 * 1024 * 1024);
        assert_eq!(handle.logical_size, 8 * 1024 * 1024);
        let publication = state
            .take_dirty_publication(fh)
            .expect("take publication")
            .expect("dirty publication");
        assert_eq!(publication.dirty.len(), 1);
        assert_eq!(
            publication
                .dirty
                .ranges
                .get(&0)
                .expect("coalesced sequential range")
                .len(),
            8 * 1024 * 1024
        );
    }

    #[test]
    fn overlapping_dirty_writes_are_merged_with_latest_bytes_winning() {
        let mut dirty = DirtyWriteBuffer::default();
        assert_eq!(
            dirty.try_write(4, b"abcd", 64).expect("first write"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(
            dirty.try_write(6, b"WXYZ", 64).expect("overlap write"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty.buffered_bytes(), 6);
        assert_eq!(dirty.ranges.get(&4).expect("merged range"), b"abWXYZ");
    }

    #[test]
    fn dirty_buffer_requests_publication_before_exceeding_its_bound() {
        let mut dirty = DirtyWriteBuffer::default();
        assert_eq!(
            dirty.try_write(0, b"123456", 8).expect("first write"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(
            dirty
                .try_write(16, b"789", 8)
                .expect("bounded second write"),
            DirtyWriteInsert::NeedsPublication
        );
        assert_eq!(dirty.buffered_bytes(), 6);
        assert_eq!(dirty.ranges.len(), 1);

        let mut empty = DirtyWriteBuffer::default();
        assert_eq!(
            empty
                .try_write(0, b"larger-than-limit", 4)
                .expect("large direct write"),
            DirtyWriteInsert::PublishDirectly
        );
        assert!(empty.is_empty());
    }

    #[test]
    fn opened_reads_overlay_dirty_bytes_and_zero_fill_new_sparse_extents() {
        let path = "/root/read-your-writes.bin";
        let mut opened = test_stat("fuserywobject", "fuserywversion", path);
        opened.size_bytes = Some(6);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened, libc::O_RDWR);
        assert_eq!(
            state
                .buffer_handle_write(fh, 2, b"XY")
                .expect("overwrite committed extent"),
            DirtyWriteInsert::Buffered
        );
        assert_eq!(
            state
                .buffer_handle_write(fh, 8, b"Z")
                .expect("extend with sparse gap"),
            DirtyWriteInsert::Buffered
        );

        let read = state.opened_read(fh, 0, 16).expect("opened read plan");
        assert_eq!(read.base_request_len(), 6);
        assert_eq!(
            read.finish(b"abcdef".to_vec()).expect("overlay base read"),
            b"abXYef\0\0Z"
        );
        let sparse = state.opened_read(fh, 6, 3).expect("sparse read plan");
        assert_eq!(sparse.base_request_len(), 0);
        assert_eq!(
            sparse.finish(Vec::new()).expect("local sparse read"),
            b"\0\0Z"
        );
        assert_eq!(
            state
                .visible_handle_stat(fh)
                .expect("visible handle stat")
                .size_bytes,
            Some(9)
        );
    }

    #[test]
    fn dirty_visibility_and_publication_order_are_object_wide_across_handles() {
        let path = "/root/shared-open.bin";
        let mut opened = test_stat("fusesharedobject", "fusesharedversion", path);
        opened.size_bytes = Some(4);
        let mut state = FuseState::new("/root".to_string());
        let writer = state.allocate_handle(path.to_string(), opened.clone(), libc::O_RDWR);
        let reader = state.allocate_handle(path.to_string(), opened, libc::O_RDONLY);
        state
            .buffer_handle_write(writer, 4, b"++")
            .expect("buffer through writer");

        assert_eq!(
            state.dirty_handles_for_object(
                &state.handles.get(&writer).expect("writer").object_id,
                Some(reader),
            ),
            vec![writer]
        );
        let read = state
            .opened_read(reader, 0, 6)
            .expect("reader sees mount-local dirty object state");
        assert_eq!(read.base_request_len(), 4);
        assert_eq!(
            read.finish(b"base".to_vec()).expect("object-wide overlay"),
            b"base++"
        );
        assert_eq!(
            state
                .visible_handle_stat(reader)
                .expect("reader-visible stat")
                .size_bytes,
            Some(6)
        );
    }

    #[test]
    fn sync_handles_never_accept_buffered_dirty_state() {
        let path = "/root/sync.bin";
        let opened = test_stat("fusesyncobject", "fusesyncversion", path);
        let mut state = FuseState::new("/root".to_string());
        for flags in [libc::O_RDWR | libc::O_DSYNC, libc::O_RDWR | libc::O_SYNC] {
            let fh = state.allocate_handle(path.to_string(), opened.clone(), flags);
            assert_eq!(
                state
                    .buffer_handle_write(fh, 0, b"immediate")
                    .expect("classify sync write"),
                DirtyWriteInsert::PublishDirectly
            );
            assert!(
                state
                    .handles
                    .get(&fh)
                    .expect("sync handle")
                    .dirty
                    .is_empty()
            );
        }
    }

    #[test]
    fn failed_publication_can_restore_all_acknowledged_dirty_bytes() {
        let path = "/root/retry.bin";
        let mut opened = test_stat("fuseretryobject", "fuseretryversion", path);
        opened.size_bytes = Some(0);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened, libc::O_RDWR);
        state
            .buffer_handle_write(fh, 0, b"retry-me")
            .expect("buffer write");
        let publication = state
            .take_dirty_publication(fh)
            .expect("take publication")
            .expect("dirty publication");
        assert!(state.handles.get(&fh).expect("handle").dirty.is_empty());

        state
            .restore_dirty_publication(fh, publication)
            .expect("restore failed publication");
        let restored = &state.handles.get(&fh).expect("handle").dirty;
        assert_eq!(restored.buffered_bytes(), 8);
        assert_eq!(
            restored.ranges.get(&0).expect("restored range"),
            b"retry-me"
        );
    }

    #[test]
    fn empty_content_service_candidates_fall_back_before_the_dfs_mutex_is_taken() {
        let local =
            dfs::DfsNodeId::from_legacy_named("fuse-write-fallback-local").expect("local node");

        assert_eq!(
            fuse_write_replica_candidates(Vec::new(), local.clone()),
            vec![local]
        );
    }

    #[test]
    fn lock_warmup_is_retriable_but_lost_grants_are_io_failures() {
        let warming = dfs::DfsAdvisoryLockOutcome::AuthorityWarming {
            retry_after_unix_nanos: 99,
        };
        assert_eq!(errno_for_unexpected_lock_outcome(&warming), libc::EAGAIN);
        assert!(!lock_outcome_loses_grant(&warming));

        let lost = dfs::DfsAdvisoryLockOutcome::LockLost;
        assert_eq!(errno_for_unexpected_lock_outcome(&lost), libc::EIO);
        assert!(lock_outcome_loses_grant(&lost));
    }

    #[test]
    fn stale_renewal_cannot_remove_a_newer_full_grant() {
        let object_id = test_object_id("fuselockobject");
        let key = (object_id.clone(), 9);
        let original_grant = test_grant(
            object_id.clone(),
            "grant-original",
            dfs::DfsAdvisoryLockMode::Shared,
        );
        let held = fuse_held_lock(
            "/locked.txt".to_string(),
            object_id,
            dfs::DfsAdvisoryLockSessionId::new("test-session"),
            original_grant.clone(),
            live_local_lease_deadline(),
            BTreeSet::from([3]),
        )
        .expect("live grant");
        let state = Arc::new(Mutex::new(FuseState::new("/".to_string())));
        state
            .lock()
            .expect("state")
            .held_locks
            .insert(key.clone(), held);

        {
            let mut state = state.lock().expect("state");
            let current = state.held_locks.get_mut(&key).expect("held lock");
            current.grant.mode = dfs::DfsAdvisoryLockMode::Exclusive;
        }
        mark_fuse_lock_lost(&state, &key, &original_grant);
        {
            let state = state.lock().expect("state");
            assert_eq!(
                state.held_locks.get(&key).expect("newer grant").grant.mode,
                dfs::DfsAdvisoryLockMode::Exclusive
            );
            assert!(!state.lost_locks.contains_key(&key));
        }

        let current_grant = state
            .lock()
            .expect("state")
            .held_locks
            .get(&key)
            .expect("newer grant")
            .grant
            .clone();
        mark_fuse_lock_lost(&state, &key, &current_grant);
        let state = state.lock().expect("state");
        assert!(!state.held_locks.contains_key(&key));
        assert_eq!(
            state
                .lost_locks
                .get(&key)
                .expect("lost marker")
                .associated_fhs,
            BTreeSet::from([3])
        );
    }

    #[test]
    fn handle_associations_find_every_release_fallback_owner() {
        let object_id = test_object_id("fusereleaseobject");
        let mut state = FuseState::new("/".to_string());
        let held = fuse_held_lock(
            "/locked.txt".to_string(),
            object_id.clone(),
            dfs::DfsAdvisoryLockSessionId::new("held-session"),
            test_grant(
                object_id.clone(),
                "grant-held",
                dfs::DfsAdvisoryLockMode::Exclusive,
            ),
            live_local_lease_deadline(),
            BTreeSet::from([11]),
        )
        .expect("live grant");
        state.held_locks.insert((object_id.clone(), 101), held);
        state.pending_locks.insert(
            (object_id.clone(), 202),
            FusePendingLock {
                path: "/locked.txt".to_string(),
                object_id: object_id.clone(),
                session_id: dfs::DfsAdvisoryLockSessionId::new("pending-session"),
                acquire_request_id: dfs::DfsAdvisoryLockRequestId::new("pending-request"),
                associated_fhs: BTreeSet::from([11]),
            },
        );
        state.record_lost_lock((object_id, 303), [12]);

        assert_eq!(state.lock_owners_for_handle(11), BTreeSet::from([101, 202]));
        assert_eq!(state.lock_owners_for_handle(12), BTreeSet::from([303]));
    }

    #[test]
    fn missing_or_elapsed_local_lease_becomes_a_handle_scoped_loss() {
        let object_id = test_object_id("fuseexpiredobject");
        let key = (object_id.clone(), 404);
        let mut zero = test_grant(
            object_id.clone(),
            "grant-zero",
            dfs::DfsAdvisoryLockMode::Exclusive,
        );
        zero.lease_remaining_nanos = 0;
        assert!(
            fuse_held_lock(
                "/expired.txt".to_string(),
                object_id.clone(),
                dfs::DfsAdvisoryLockSessionId::new("expired-session"),
                zero,
                None,
                BTreeSet::from([14]),
            )
            .is_none()
        );

        let mut state = FuseState::new("/".to_string());
        let mut held = fuse_held_lock(
            "/expired.txt".to_string(),
            object_id.clone(),
            dfs::DfsAdvisoryLockSessionId::new("expired-session"),
            test_grant(
                object_id,
                "grant-elapsed",
                dfs::DfsAdvisoryLockMode::Exclusive,
            ),
            Some(Instant::now() + Duration::from_secs(1)),
            BTreeSet::from([14]),
        )
        .expect("initially live grant");
        held.local_lease_deadline = Instant::now();
        state.held_locks.insert(key.clone(), held);
        state.expire_elapsed_locks(Instant::now());

        assert!(!state.held_locks.contains_key(&key));
        assert!(state.has_lost_lock_for_handle(14));
    }

    #[test]
    fn local_lease_projection_is_anchored_before_the_rpc() {
        let object_id = test_object_id("fuseleaseprojection");
        let grant = test_grant(
            object_id,
            "grant-projection",
            dfs::DfsAdvisoryLockMode::Exclusive,
        );
        let remaining = Duration::from_nanos(grant.lease_remaining_nanos);
        let started_at = Instant::now();
        let outcome = dfs::DfsAdvisoryLockOutcome::Acquired { grant };

        assert_eq!(
            conservative_local_lease_deadline(&outcome, started_at),
            started_at.checked_add(remaining)
        );
    }

    #[test]
    fn mounted_lock_key_cap_rejects_the_next_distinct_key() {
        let mut state = FuseState::new("/".to_string());
        let mut first_key = None;
        for index in 0..MAX_ACTIVE_LOCK_KEYS_PER_MOUNT {
            let object_id = test_object_id(&format!("fusecapobject{index}"));
            let key = (object_id.clone(), index as u64 + 1);
            first_key.get_or_insert_with(|| key.clone());
            state.pending_locks.insert(
                key,
                FusePendingLock {
                    path: format!("/pending-{index}.txt"),
                    object_id,
                    session_id: dfs::DfsAdvisoryLockSessionId::new(format!(
                        "pending-session-{index}"
                    )),
                    acquire_request_id: dfs::DfsAdvisoryLockRequestId::new(format!(
                        "pending-request-{index}"
                    )),
                    associated_fhs: BTreeSet::from([index as u64 + 10]),
                },
            );
        }

        assert!(state.can_start_lock_key(&first_key.expect("first key")));
        assert!(!state.can_start_lock_key(&(test_object_id("fusecapfifth"), 99)));
    }

    #[test]
    fn directory_rename_rewrites_descendant_handles_and_lock_paths() {
        let object_id = test_object_id("fuserenameobject");
        let opened_stat = test_stat("fuserenameobject", "fuserenameversion", "/root/a/child.txt");
        let mut state = FuseState::new("/root".to_string());
        let directory_ino = state.intern_path("/root/a".to_string());
        let file_ino = state.intern_path("/root/a/child.txt".to_string());
        let fh = state.allocate_handle(
            "/root/a/child.txt".to_string(),
            opened_stat.clone(),
            libc::O_RDWR,
        );
        state.held_locks.insert(
            (object_id.clone(), 1),
            fuse_held_lock(
                "/root/a/child.txt".to_string(),
                object_id.clone(),
                dfs::DfsAdvisoryLockSessionId::new("held-session"),
                test_grant(
                    object_id.clone(),
                    "grant-rename",
                    dfs::DfsAdvisoryLockMode::Shared,
                ),
                live_local_lease_deadline(),
                BTreeSet::from([fh]),
            )
            .expect("live grant"),
        );
        state.pending_locks.insert(
            (object_id.clone(), 2),
            FusePendingLock {
                path: "/root/a/deeper/child.txt".to_string(),
                object_id,
                session_id: dfs::DfsAdvisoryLockSessionId::new("pending-session"),
                acquire_request_id: dfs::DfsAdvisoryLockRequestId::new("pending-rename"),
                associated_fhs: BTreeSet::from([fh]),
            },
        );

        state.rename_path("/root/a", "/root/b".to_string());

        assert_eq!(state.path_for(directory_ino), Some("/root/b"));
        assert_eq!(state.path_for(file_ino), Some("/root/b/child.txt"));
        assert_eq!(
            state.handles.get(&fh).expect("handle").path,
            "/root/b/child.txt"
        );
        assert_eq!(
            state.handles.get(&fh).expect("handle").opened_stat,
            opened_stat,
            "rename changes the live pathname but not the exact opened content identity"
        );
        assert_eq!(
            state.held_locks.values().next().expect("held").path,
            "/root/b/child.txt"
        );
        assert_eq!(
            state.pending_locks.values().next().expect("pending").path,
            "/root/b/deeper/child.txt"
        );
        assert_eq!(renamed_path("/root/ab", "/root/a", "/root/b"), None);
    }

    #[test]
    fn hardlink_aliases_share_target_inode_and_survive_individual_unlinks() {
        let object_id = test_object_id("fusehardlinkidentity");
        let original = "/root/original.txt";
        let alias_a = "/root/alias-a.txt";
        let alias_b = "/root/alias-b.txt";
        let mut state = FuseState::new("/root".to_string());

        let target_ino = state.intern_entry(original.to_string(), object_id.clone());
        assert_eq!(
            state.intern_entry(alias_a.to_string(), object_id.clone()),
            target_ino,
            "lookup/readdir of a hardlink must reuse the target inode"
        );
        assert_eq!(
            state
                .intern_alias_path(target_ino, alias_b.to_string(), &object_id)
                .expect("link alias"),
            target_ino,
            "link(2) must reply with the supplied target inode"
        );

        state.remove_path(original);
        assert_eq!(state.ino_by_path.get(alias_a), Some(&target_ino));
        assert_eq!(state.ino_by_path.get(alias_b), Some(&target_ino));
        assert!(state.path_for(target_ino).is_some());
        assert_eq!(state.ino_by_object_id.get(&object_id), Some(&target_ino));

        state.remove_path(alias_a);
        assert_eq!(state.path_for(target_ino), Some(alias_b));
        state.remove_path(alias_b);
        assert_eq!(state.path_for(target_ino), None);
        assert!(!state.ino_by_object_id.contains_key(&object_id));
    }

    #[test]
    fn freshly_observed_object_path_becomes_the_inode_routing_path() {
        let object_id = test_object_id("fuseremoterenameidentity");
        let mut state = FuseState::new("/root".to_string());
        let ino = state.intern_entry("/root/a-stale.txt".to_string(), object_id.clone());

        assert_eq!(
            state.intern_entry("/root/z-current.txt".to_string(), object_id),
            ino
        );
        assert_eq!(
            state.path_for(ino),
            Some("/root/z-current.txt"),
            "a fresh lookup after a remote rename must not route through a lexically earlier stale path"
        );

        state.remove_path("/root/z-current.txt");
        assert_eq!(
            state.path_for(ino),
            Some("/root/a-stale.txt"),
            "removing the preferred alias must retain a remaining hardlink path"
        );
    }

    #[test]
    fn hardlink_rename_moves_only_the_named_alias_and_preserves_other_identities() {
        let source_object = test_object_id("fusehardlinkrenamesource");
        let destination_object = test_object_id("fusehardlinkrenamedestination");
        let mut state = FuseState::new("/root".to_string());
        let source_ino = state.intern_entry("/root/source".to_string(), source_object);
        assert_eq!(
            state.intern_entry(
                "/root/source-alias".to_string(),
                test_object_id("fusehardlinkrenamesource"),
            ),
            source_ino
        );
        let destination_ino =
            state.intern_entry("/root/destination".to_string(), destination_object.clone());
        assert_eq!(
            state.intern_entry("/root/destination-alias".to_string(), destination_object),
            destination_ino
        );

        state.rename_path("/root/source", "/root/destination".to_string());
        assert!(!state.ino_by_path.contains_key("/root/source"));
        assert_eq!(
            state.ino_by_path.get("/root/destination"),
            Some(&source_ino)
        );
        assert_eq!(
            state.ino_by_path.get("/root/source-alias"),
            Some(&source_ino),
            "renaming one hardlink must not move its siblings"
        );
        assert_eq!(
            state.ino_by_path.get("/root/destination-alias"),
            Some(&destination_ino),
            "replacing a path must not detach other links to the replaced object"
        );

        state.rename_path("/root/source-alias", "/root/destination".to_string());
        assert_eq!(
            state.ino_by_path.get("/root/source-alias"),
            Some(&source_ino)
        );
        assert_eq!(
            state.ino_by_path.get("/root/destination"),
            Some(&source_ino)
        );
    }

    #[test]
    fn handle_keeps_opened_identity_after_unlink_and_refreshes_only_same_object() {
        let path = "/root/opened.txt";
        let opened = test_stat("fuseopenedobject", "fuseopenedversion1", path);
        let mut state = FuseState::new("/root".to_string());
        state.intern_path(path.to_string());
        let fh = state.allocate_handle(path.to_string(), opened.clone(), libc::O_RDWR);

        state.remove_path(path);
        assert_eq!(
            state.handles.get(&fh).expect("open handle").opened_stat,
            opened,
            "unlink removes the pathname cache, not an open descriptor's immutable content identity"
        );

        let mut committed = test_stat("fuseopenedobject", "fuseopenedversion2", path);
        committed.size_bytes = Some(24);
        state
            .refresh_handle_stat(fh, &committed)
            .expect("refresh same object after descriptor write");
        assert_eq!(
            state
                .handles
                .get(&fh)
                .expect("refreshed handle")
                .opened_stat,
            committed
        );

        let replacement = test_stat("fusereplacementobject", "fuseopenedversion3", path);
        assert_eq!(
            state.refresh_handle_stat(fh, &replacement),
            Err(libc::ESTALE)
        );
        assert_eq!(
            state
                .handles
                .get(&fh)
                .expect("unchanged handle")
                .opened_stat,
            committed,
            "a pathname replacement must not retarget an existing descriptor"
        );
    }

    #[test]
    fn bounded_read_executor_runs_blocking_requests_concurrently() {
        let executor = FuseReadExecutor::with_worker_count(2, dfs::DfsCancellationFlag::new())
            .expect("read executor");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        for _ in 0..2 {
            let entered_tx = entered_tx.clone();
            let completed_tx = completed_tx.clone();
            let release = Arc::clone(&release);
            assert!(
                executor
                    .submit(Box::new(move || {
                        entered_tx.send(()).expect("entered signal");
                        let (released, wake) = &*release;
                        let mut released = released.lock().expect("release lock");
                        while !*released {
                            released = wake.wait(released).expect("release wait");
                        }
                        completed_tx.send(()).expect("completion signal");
                    }))
                    .is_ok(),
                "submit read job"
            );
        }

        let first_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let second_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        assert!(first_entered && second_entered, "read jobs did not overlap");
        completed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first completion");
        completed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second completion");
        drop(executor);
    }

    #[test]
    fn read_executor_shutdown_cancels_queued_timeout_waves_before_join() {
        let cancellation = dfs::DfsCancellationFlag::new();
        let executor =
            FuseReadExecutor::with_worker_count(2, cancellation.clone()).expect("read executor");
        let started = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let skipped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        for _ in 0..10 {
            let cancellation = cancellation.clone();
            let started = Arc::clone(&started);
            let skipped = Arc::clone(&skipped);
            let entered_tx = entered_tx.clone();
            let release = Arc::clone(&release);
            assert!(
                executor
                    .submit(Box::new(move || {
                        // Mirrors MountedSifsContentReadContext's pre-send
                        // check: queued requests reply Cancelled without
                        // starting another network-timeout wave.
                        if cancellation.is_cancelled() {
                            skipped.fetch_add(1, Ordering::SeqCst);
                            return;
                        }
                        started.fetch_add(1, Ordering::SeqCst);
                        entered_tx.send(()).expect("entered remote send");
                        let (released, wake) = &*release;
                        let mut released = released.lock().expect("release lock");
                        while !*released {
                            released = wake.wait(released).expect("release wait");
                        }
                    }))
                    .is_ok(),
                "submit queued reply"
            );
        }

        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first in-flight read");
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second in-flight read");
        let shutdown = thread::spawn(move || drop(executor));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !cancellation.is_cancelled() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(
            cancellation.is_cancelled(),
            "executor drop did not cancel before joining"
        );
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        shutdown.join().expect("executor shutdown");

        assert_eq!(started.load(Ordering::SeqCst), 2);
        assert_eq!(skipped.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn queued_opened_read_identity_survives_handle_close() {
        let path = "/root/close-race.txt";
        let opened = test_stat("fusecloseraceobject", "fusecloseraceversion", path);
        let mut state = FuseState::new("/root".to_string());
        let fh = state.allocate_handle(path.to_string(), opened.clone(), libc::O_RDONLY);
        let identity = state
            .opened_read_identity(fh)
            .expect("snapshot opened identity before queueing");

        let executor = FuseReadExecutor::with_worker_count(1, dfs::DfsCancellationFlag::new())
            .expect("read executor");
        let (blocker_entered_tx, blocker_entered_rx) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let release_for_blocker = Arc::clone(&release);
        assert!(
            executor
                .submit(Box::new(move || {
                    blocker_entered_tx.send(()).expect("blocker entered");
                    let (released, wake) = &*release_for_blocker;
                    let mut released = released.lock().expect("release lock");
                    while !*released {
                        released = wake.wait(released).expect("release wait");
                    }
                }))
                .is_ok(),
            "submit blocker"
        );
        blocker_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("blocker running");
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        assert!(
            executor
                .submit(Box::new(move || {
                    observed_tx.send(identity).expect("observed identity");
                }))
                .is_ok(),
            "submit opened read"
        );

        // release(2) removes the handle synchronously, while a previously
        // accepted read owns its exact identity and remains valid.
        state.handles.remove(&fh);
        assert!(state.opened_read_identity(fh).is_none());
        {
            let (released, wake) = &*release;
            *released.lock().expect("release lock") = true;
            wake.notify_all();
        }
        let observed = observed_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("queued read completed after close");
        assert_eq!(observed.path, path);
        assert_eq!(observed.opened_stat, opened);
        drop(executor);
    }
}
