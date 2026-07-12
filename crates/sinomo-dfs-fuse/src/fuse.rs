use crate::{
    DfsFuseError, DfsFuseMountOptions, Result, join_sifs_child, normalize_sifs_root,
    parent_sifs_path,
};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
    TimeOrNow,
};
use sinomo_runtime::dfs::{self, DfsError, DfsObjectKind, DfsPosixError, DfsWriteOptions};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
static NEXT_MOUNT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
type SifsService = dfs::MountedSifsService;

pub fn mount_sifs(mut options: DfsFuseMountOptions) -> Result<()> {
    options.root_path = normalize_sifs_root(&options.root_path)?;
    let mount_helper = fuse_mount_helper();
    let mount_point = options.mount_point.clone();
    let fs = SifsFuseFileSystem::open(options.clone())?;
    let mount_options = fuse_mount_options(&options, mount_helper.is_some());
    fuser::mount2(fs, &mount_point, &mount_options)
        .map_err(|source| map_mount_error(&mount_point, mount_helper.is_some(), source))
}

fn fuse_mount_options(
    options: &DfsFuseMountOptions,
    mount_helper_available: bool,
) -> Vec<MountOption> {
    let mut mount_options = vec![
        MountOption::FSName(options.mount_name.clone()),
        MountOption::DefaultPermissions,
    ];
    // fuser routes AutoUnmount mounts directly through fusermount. Omit it when
    // no executable helper exists so privileged/systemd services can use the
    // kernel /dev/fuse mount path instead.
    if mount_helper_available {
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
        let dfs::OpenedMountedSifsService {
            service,
            replica_candidates,
        } = dfs::open_mounted_sifs_service(options.data_dir)
            .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
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
        let candidates = if self.write_replica_candidates.is_empty() {
            self.dfs
                .lock()
                .ok()
                .map(|dfs| vec![dfs.node_id().clone()])
                .unwrap_or_default()
        } else {
            self.write_replica_candidates.clone()
        };
        DfsWriteOptions::replicated_eventual(candidates)
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

    fn intern_path(&self, path: String) -> std::result::Result<u64, libc::c_int> {
        Ok(self.lock_state()?.intern_path(path))
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
        match self.intern_path(path) {
            Ok(ino) => reply.entry(&self.entry_ttl, &file_attr(ino, &stat), 0),
            Err(errno) => reply.error(errno),
        }
    }

    fn commit_handle(&self, fh: u64) -> std::result::Result<(), libc::c_int> {
        self.ensure_handle_lock_not_lost(fh)?;
        if !self.lock_state()?.handles.contains_key(&fh) {
            return Err(libc::EBADF);
        }
        Ok(())
    }

    fn sync_handle(&self, fh: u64, datasync: bool) -> std::result::Result<(), libc::c_int> {
        self.commit_handle(fh)?;
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

    fn verify_handle_object_identity(
        &self,
        fh: u64,
        stat: &dfs::DfsStat,
    ) -> std::result::Result<(), libc::c_int> {
        let state = self.lock_state()?;
        let handle = state.handles.get(&fh).ok_or(libc::EBADF)?;
        if handle.object_id != stat.object_id {
            return Err(libc::ESTALE);
        }
        Ok(())
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

    fn read_handle_or_path(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        lock_owner: Option<u64>,
    ) -> std::result::Result<Vec<u8>, libc::c_int> {
        self.ensure_io_lock_not_lost(ino, fh, lock_owner)?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let handle_identity = {
            let state = self.lock_state()?;
            state
                .handles
                .get(&fh)
                .map(|handle| (handle.path.clone(), handle.object_id.clone()))
        };
        self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
        let bytes = {
            let mut dfs = self.lock_dfs()?;
            match handle_identity {
                Some((_, object_id)) => dfs.posix_read_file_for_object(&object_id),
                None => dfs.posix_read_file_distributed(&self.path_for_ino(ino)?),
            }
            .map_err(errno_for_posix_error)?
        };
        Ok(slice_bytes(&bytes, start, size as usize))
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
        self.ensure_io_lock_not_lost(ino, fh, lock_owner)?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let handle = {
            let state = self.lock_state()?;
            state.handles.get(&fh).map(|handle| {
                if handle.writable {
                    Ok((
                        handle.path.clone(),
                        handle.object_id.clone(),
                        handle.sync_mode,
                    ))
                } else {
                    Err(libc::EBADF)
                }
            })
        };
        let (path, object_id, sync_mode) = handle
            .transpose()?
            .map(|(path, object_id, sync_mode)| (path, Some(object_id), sync_mode))
            .unwrap_or((self.path_for_ino(ino)?, None, HandleSyncMode::None));
        self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
        let stat = if let Some(object_id) = object_id.as_ref() {
            self.lock_dfs()?.posix_write_file_at_for_object(
                object_id,
                start as u64,
                data,
                self.write_options(),
            )
        } else {
            self.lock_dfs()?
                .posix_write_file_at(&path, start as u64, data, self.write_options())
        }
        .map_err(errno_for_posix_error)?;
        self.verify_handle_object_identity(fh, &stat)?;
        if let Some(kind) = sync_mode.kind() {
            self.ensure_handle_lock_budget(fh, lock_owner, LOCKED_IO_MIN_LEASE)?;
            let target = self.current_file_sync_target_for_handle(fh, kind)?;
            self.lock_dfs()?
                .posix_sync_file_target(&target)
                .map(drop)
                .map_err(errno_for_posix_error)?;
        }
        Ok(data.len() as u32)
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
            state.handles.get(&fh).map(|handle| {
                (
                    handle.path.clone(),
                    handle.object_id.clone(),
                    handle.sync_mode,
                )
            })
        } else {
            None
        };
        let (path, object_id, sync_mode) = match handle {
            Some((path, object_id, sync_mode)) => (path, Some(object_id), sync_mode),
            None => (self.path_for_ino(ino)?, None, HandleSyncMode::None),
        };
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
        if let Some(fh) = fh {
            self.verify_handle_object_identity(fh, &stat)?;
        }
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

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
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
                self.lock_dfs()?
                    .posix_set_owner(
                        &path,
                        uid.unwrap_or(current.uid),
                        gid.unwrap_or(current.gid),
                    )
                    .map(|_| ())
                    .map_err(errno_for_posix_error)
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
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
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
            let mut stat = dfs
                .posix_write_file(&path, &[], self.write_options())
                .map_err(errno_for_posix_error)?;
            if mode & 0o7777 != 0o644 {
                stat = dfs
                    .posix_set_mode(&path, mode)
                    .map_err(errno_for_posix_error)?;
            }
            Ok(stat)
        });
        match result {
            Ok(stat) => self.reply_entry_for_path(reply, path, stat),
            Err(errno) => reply.error(errno),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
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
            let mut stat = dfs.posix_mkdir(&path).map_err(errno_for_posix_error)?;
            if mode & 0o7777 != 0o755 {
                stat = dfs
                    .posix_set_mode(&path, mode)
                    .map_err(errno_for_posix_error)?;
            }
            Ok(stat)
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
        _req: &Request<'_>,
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
            dfs.posix_symlink(target, &path, self.write_options())
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
            Ok(stat) => self.reply_entry_for_path(reply, link_path, stat),
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
                    let fh = state.allocate_handle(path, stat.object_id, flags);
                    reply.opened(fh, 0);
                }
                Err(errno) => reply.error(errno),
            },
            Err(errno) => reply.error(errno),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
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
            let mut stat = dfs
                .posix_write_file(&path, &[], self.write_options())
                .map_err(errno_for_posix_error)?;
            if mode & 0o7777 != 0o644 {
                stat = dfs
                    .posix_set_mode(&path, mode)
                    .map_err(errno_for_posix_error)?;
            }
            Ok(stat)
        });
        match result {
            Ok(stat) => match self.lock_state() {
                Ok(mut state) => {
                    let ino = state.intern_path(path.clone());
                    let fh = state.allocate_handle(path, stat.object_id.clone(), flags);
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
        match self.read_handle_or_path(ino, fh, offset, size, lock_owner) {
            Ok(bytes) => reply.data(&bytes),
            Err(errno) => reply.error(errno),
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
        let close_result = self.close_advisory_locks_for_release(fh, lock_owner);
        let result = commit_result.and(close_result);
        if let Ok(mut state) = self.lock_state() {
            state.handles.remove(&fh);
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
            let child_ino = match self.intern_path(child.path) {
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

#[derive(Debug)]
struct FuseState {
    next_ino: u64,
    next_fh: u64,
    path_by_ino: BTreeMap<u64, String>,
    ino_by_path: BTreeMap<String, u64>,
    handles: BTreeMap<u64, OpenFileHandle>,
    held_locks: BTreeMap<(dfs::DfsObjectId, u64), FuseHeldLock>,
    pending_locks: BTreeMap<(dfs::DfsObjectId, u64), FusePendingLock>,
    lost_locks: BTreeMap<(dfs::DfsObjectId, u64), FuseLostLock>,
}

impl FuseState {
    fn new(root_path: String) -> Self {
        let mut path_by_ino = BTreeMap::new();
        let mut ino_by_path = BTreeMap::new();
        path_by_ino.insert(ROOT_INO, root_path.clone());
        ino_by_path.insert(root_path, ROOT_INO);
        Self {
            next_ino: ROOT_INO + 1,
            next_fh: 1,
            path_by_ino,
            ino_by_path,
            handles: BTreeMap::new(),
            held_locks: BTreeMap::new(),
            pending_locks: BTreeMap::new(),
            lost_locks: BTreeMap::new(),
        }
    }

    fn path_for(&self, ino: u64) -> Option<&str> {
        self.path_by_ino.get(&ino).map(String::as_str)
    }

    fn intern_path(&mut self, path: String) -> u64 {
        if let Some(ino) = self.ino_by_path.get(&path) {
            return *ino;
        }
        let ino = self.next_ino;
        self.next_ino = self.next_ino.saturating_add(1);
        self.path_by_ino.insert(ino, path.clone());
        self.ino_by_path.insert(path, ino);
        ino
    }

    fn remove_path(&mut self, path: &str) {
        if let Some(ino) = self.ino_by_path.remove(path)
            && ino != ROOT_INO
        {
            self.path_by_ino.remove(&ino);
        }
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
            .path_by_ino
            .get(&ROOT_INO)
            .is_some_and(|root| root == from)
        {
            return;
        }
        let rewritten_inodes = self
            .path_by_ino
            .iter()
            .filter_map(|(ino, path)| {
                renamed_path(path, from, &to).map(|renamed| (*ino, path.clone(), renamed))
            })
            .collect::<Vec<_>>();
        for (_, old_path, _) in &rewritten_inodes {
            self.ino_by_path.remove(old_path);
        }
        for (ino, _, renamed) in rewritten_inodes {
            self.path_by_ino.insert(ino, renamed.clone());
            self.ino_by_path.insert(renamed, ino);
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

    fn allocate_handle(&mut self, path: String, object_id: dfs::DfsObjectId, flags: i32) -> u64 {
        let fh = self.next_fh;
        self.next_fh = self.next_fh.saturating_add(1);
        self.handles.insert(
            fh,
            OpenFileHandle {
                path,
                object_id,
                writable: flags_has_write_access(flags),
                sync_mode: HandleSyncMode::from_open_flags(flags),
            },
        );
        fh
    }
}

#[derive(Clone, Debug)]
struct OpenFileHandle {
    path: String,
    object_id: dfs::DfsObjectId,
    writable: bool,
    sync_mode: HandleSyncMode,
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
                .map_err(errno_for_posix_error)
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

fn slice_bytes(bytes: &[u8], start: usize, len: usize) -> Vec<u8> {
    if start >= bytes.len() {
        return Vec::new();
    }
    let end = start.saturating_add(len).min(bytes.len());
    bytes[start..end].to_vec()
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
    fn fuse_mount_without_helper_keeps_direct_mount_available() {
        let options = DfsFuseMountOptions::new("/mnt/sinomo");

        let direct_options = fuse_mount_options(&options, false);
        assert!(!direct_options.contains(&MountOption::AutoUnmount));

        let helper_options = fuse_mount_options(&options, true);
        assert!(helper_options.contains(&MountOption::AutoUnmount));
    }

    fn test_object_id(name: &str) -> dfs::DfsObjectId {
        dfs::DfsObjectId::from_legacy_named(name).expect("test object id")
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
        let mut state = FuseState::new("/root".to_string());
        let directory_ino = state.intern_path("/root/a".to_string());
        let file_ino = state.intern_path("/root/a/child.txt".to_string());
        let fh = state.allocate_handle(
            "/root/a/child.txt".to_string(),
            object_id.clone(),
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
            state.held_locks.values().next().expect("held").path,
            "/root/b/child.txt"
        );
        assert_eq!(
            state.pending_locks.values().next().expect("pending").path,
            "/root/b/deeper/child.txt"
        );
        assert_eq!(renamed_path("/root/ab", "/root/a", "/root/b"), None);
    }
}
