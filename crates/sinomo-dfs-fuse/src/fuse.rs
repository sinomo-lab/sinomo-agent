use crate::{
    DfsFuseError, DfsFuseMountOptions, Result, join_sifs_child, normalize_sifs_root,
    parent_sifs_path,
};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use sinomo_runtime::core::{ClusterBootstrapState, ClusterEndpointRecord};
use sinomo_runtime::daemon;
use sinomo_runtime::dfs::{
    self, DfsControlPlaneKvClient, DfsObjectKind, DfsPosixError, DfsProtocolFrameCommand,
    DfsProtocolFrameResponse, DfsWriteOptions, LocalContentStore, NetworkedDfsService,
};
use sinomo_runtime::kv::{
    ClusterKvRaftCommandStorage, ClusterKvRaftCommitResult, ClusterKvRaftUncommittedAppend,
    ClusterKvWatchEventsRequest, ClusterKvWatchEventsResponse, KvCommand, KvEntry, KvError,
    KvEvent, KvOp, KvRevision, KvSnapshot, KvTxn, KvTxnResponse,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, InboundFrame, NetworkError, NetworkFrame, NetworkTransport};
use sinomo_runtime::node::NodeId;
use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ROOT_INO: u64 = 1;
const DEFAULT_BLOCK_SIZE: u32 = 4096;
const DEFAULT_FUSE_TIMEOUT: Duration = Duration::from_secs(3);
const FUSE_CLUSTER_RPC_TIMEOUT: Duration = Duration::from_millis(750);
const FUSE_CLUSTER_TCP_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);
const FUSE_CLUSTER_IROH_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
const FUSE_CLUSTER_TCP_FOREGROUND_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);
const FUSE_CLUSTER_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(4);
const FUSE_CLUSTER_IROH_FOREGROUND_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(1500);
const FUSE_CLUSTER_RAFT_APPEND_TIMEOUT: Duration = Duration::from_secs(5);
const FUSE_CLUSTER_RAFT_APPEND_MAX_ATTEMPTS: usize = 32;

type SifsService = NetworkedDfsService<DfsFuseEndpointTransport>;

pub fn mount_sifs(mut options: DfsFuseMountOptions) -> Result<()> {
    options.root_path = normalize_sifs_root(&options.root_path)?;
    let mount_point = options.mount_point.clone();
    let fs = SifsFuseFileSystem::open(options.clone())?;
    let mut mount_options = vec![
        MountOption::FSName(options.mount_name.clone()),
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    if options.read_only {
        mount_options.push(MountOption::RO);
    }
    if options.allow_other {
        mount_options.push(MountOption::AllowOther);
    }
    let _ = options.foreground;
    fuser::mount2(fs, &mount_point, &mount_options).map_err(|source| DfsFuseError::Mount {
        mount_point: mount_point.display().to_string(),
        detail: source.to_string(),
    })
}

#[derive(Debug)]
struct SifsFuseFileSystem {
    dfs: Arc<Mutex<SifsService>>,
    state: Mutex<FuseState>,
    read_only: bool,
    attr_ttl: Duration,
    entry_ttl: Duration,
    write_replica_candidates: Vec<NodeId>,
}

impl SifsFuseFileSystem {
    fn open(options: DfsFuseMountOptions) -> Result<Self> {
        let OpenedSifsService {
            service,
            replica_candidates,
        } = open_sifs_service(options.data_dir)?;
        Ok(Self {
            dfs: Arc::new(Mutex::new(service)),
            state: Mutex::new(FuseState::new(options.root_path.clone())),
            read_only: options.read_only,
            attr_ttl: options.attr_ttl,
            entry_ttl: options.entry_ttl,
            write_replica_candidates: replica_candidates,
        })
    }

    fn lock_state(&self) -> std::result::Result<MutexGuard<'_, FuseState>, libc::c_int> {
        self.state.lock().map_err(|_| libc::EIO)
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

    fn load_handle_buffer(&self, fh: u64) -> std::result::Result<(), libc::c_int> {
        let path = {
            let state = self.lock_state()?;
            let Some(handle) = state.handles.get(&fh) else {
                return Err(libc::EBADF);
            };
            if handle.buffer.is_some() {
                return Ok(());
            }
            handle.path.clone()
        };
        let bytes = {
            let mut dfs = self.lock_dfs()?;
            dfs.posix_read_file_distributed(&path)
                .map_err(errno_for_posix_error)?
        };
        let mut state = self.lock_state()?;
        if let Some(handle) = state.handles.get_mut(&fh)
            && handle.buffer.is_none()
        {
            handle.buffer = Some(bytes);
        }
        Ok(())
    }

    fn commit_handle(&self, fh: u64) -> std::result::Result<(), libc::c_int> {
        let pending = {
            let state = self.lock_state()?;
            let Some(handle) = state.handles.get(&fh) else {
                return Err(libc::EBADF);
            };
            if !handle.dirty {
                return Ok(());
            }
            let Some(bytes) = handle.buffer.clone() else {
                return Ok(());
            };
            (handle.path.clone(), bytes)
        };
        {
            let mut dfs = self.lock_dfs()?;
            dfs.posix_write_file(&pending.0, &pending.1, self.write_options())
                .map_err(errno_for_posix_error)?;
        }
        let mut state = self.lock_state()?;
        if let Some(handle) = state.handles.get_mut(&fh) {
            handle.dirty = false;
        }
        Ok(())
    }

    fn read_handle_or_path(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> std::result::Result<Vec<u8>, libc::c_int> {
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        if let Some(buffer) = self
            .lock_state()?
            .handles
            .get(&fh)
            .and_then(|handle| handle.buffer.clone())
        {
            return Ok(slice_bytes(&buffer, start, size as usize));
        }
        let path = self.path_for_ino(ino)?;
        let bytes = {
            let mut dfs = self.lock_dfs()?;
            dfs.posix_read_file_distributed(&path)
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
    ) -> std::result::Result<u32, libc::c_int> {
        self.ensure_writable()?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        let start = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let handle_path = {
            let state = self.lock_state()?;
            state.handles.get(&fh).map(|handle| {
                if handle.writable {
                    Ok(handle.path.clone())
                } else {
                    Err(libc::EBADF)
                }
            })
        };
        let Some(handle_path) = handle_path.transpose()? else {
            let path = self.path_for_ino(ino)?;
            let mut dfs = self.lock_dfs()?;
            dfs.posix_write_file_at(&path, start as u64, data, self.write_options())
                .map_err(errno_for_posix_error)?;
            return Ok(data.len() as u32);
        };
        let _ = handle_path;
        self.load_handle_buffer(fh)?;
        let mut state = self.lock_state()?;
        let Some(handle) = state.handles.get_mut(&fh) else {
            return Err(libc::EBADF);
        };
        let Some(buffer) = handle.buffer.as_mut() else {
            return Err(libc::EIO);
        };
        let end = start.checked_add(data.len()).ok_or(libc::EINVAL)?;
        if buffer.len() < end {
            buffer.resize(end, 0);
        }
        buffer[start..end].copy_from_slice(data);
        handle.dirty = true;
        Ok(data.len() as u32)
    }

    fn truncate_handle_or_path(
        &self,
        ino: u64,
        fh: Option<u64>,
        size: u64,
    ) -> std::result::Result<(), libc::c_int> {
        self.ensure_writable()?;
        if let Some(fh) = fh
            && self.lock_state()?.handles.contains_key(&fh)
        {
            self.load_handle_buffer(fh)?;
            {
                let mut state = self.lock_state()?;
                let Some(handle) = state.handles.get_mut(&fh) else {
                    return Err(libc::EBADF);
                };
                let len = usize::try_from(size).map_err(|_| libc::EINVAL)?;
                let Some(buffer) = handle.buffer.as_mut() else {
                    return Err(libc::EIO);
                };
                buffer.resize(len, 0);
                handle.dirty = true;
            }
            return self.commit_handle(fh);
        }
        let path = self.path_for_ino(ino)?;
        let mut dfs = self.lock_dfs()?;
        dfs.posix_truncate_file(&path, size, self.write_options())
            .map_err(errno_for_posix_error)?;
        Ok(())
    }
}

impl Filesystem for SifsFuseFileSystem {
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
            Ok(_) => match self.lock_state() {
                Ok(mut state) => {
                    let fh = state.allocate_handle(path, writable, None);
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
                    let fh = state.allocate_handle(
                        path,
                        flags_has_write_access(flags),
                        Some(Vec::new()),
                    );
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
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.read_handle_or_path(ino, fh, offset, size) {
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
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.write_handle(ino, fh, offset, data) {
            Ok(written) => reply.written(written),
            Err(errno) => reply.error(errno),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.commit_handle(fh) {
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
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let result = self.commit_handle(fh);
        if let Ok(mut state) = self.lock_state() {
            state.handles.remove(&fh);
        }
        match result {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.commit_handle(fh) {
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

    fn rename_path(&mut self, from: &str, to: String) {
        let Some(ino) = self.ino_by_path.remove(from) else {
            return;
        };
        if ino == ROOT_INO {
            return;
        }
        self.path_by_ino.insert(ino, to.clone());
        self.ino_by_path.insert(to, ino);
    }

    fn allocate_handle(&mut self, path: String, writable: bool, buffer: Option<Vec<u8>>) -> u64 {
        let fh = self.next_fh;
        self.next_fh = self.next_fh.saturating_add(1);
        self.handles.insert(
            fh,
            OpenFileHandle {
                path,
                writable,
                dirty: false,
                buffer,
            },
        );
        fh
    }
}

#[derive(Clone, Debug)]
struct OpenFileHandle {
    path: String,
    writable: bool,
    dirty: bool,
    buffer: Option<Vec<u8>>,
}

#[derive(Debug)]
struct OpenedSifsService {
    service: SifsService,
    replica_candidates: Vec<NodeId>,
}

fn open_sifs_service(data_dir: Option<PathBuf>) -> Result<OpenedSifsService> {
    let data_dir = match data_dir {
        Some(path) => SinomoDataDir::new(path),
        None => SinomoDataDir::from_env_or_home()
            .map_err(|source| DfsFuseError::LocalData(source.to_string()))?,
    };
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsFuseError::LocalData(source.to_string()))?;
    let cluster_paths = network::ClusterPaths::new(&data_dir);
    let cluster_state = if cluster_paths.state.exists() {
        network::read_cluster_state_without_kv(&cluster_paths).ok()
    } else {
        None
    };
    let local_node_id = cluster_state
        .as_ref()
        .map(|state| state.local_node_id.clone())
        .unwrap_or_else(dfs::fallback_dfs_node_id);
    let endpoints = cluster_state
        .as_ref()
        .map(|state| network::cluster_dfs_protocol_targets(&cluster_paths, state))
        .unwrap_or_default();
    let content = LocalContentStore::new(data_dir.dfs_dir().join("content"))
        .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
    let mut service = NetworkedDfsService::new(
        DfsFuseEndpointTransport::new(local_node_id.clone(), endpoints),
        content,
    );
    if cluster_paths.kv_snapshot.exists() {
        service = service.with_control_plane(DfsFuseControlPlaneKvClient {
            cluster_paths: cluster_paths.clone(),
        });
    }
    let metadata = open_metadata_backend(&data_dir, &cluster_paths)?;
    let mut service = service
        .with_boxed_metadata_backend(metadata)
        .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
    service
        .bootstrap_distributed_sifs()
        .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
    Ok(OpenedSifsService {
        service,
        replica_candidates: replica_candidates(
            &cluster_paths,
            cluster_state.as_ref(),
            local_node_id,
        ),
    })
}

fn open_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &network::ClusterPaths,
) -> Result<Box<dyn dfs::DfsMetadataBackend>> {
    if cluster_paths.state.exists() {
        let file_system_id = dfs::sifs_file_system_id();
        let mut shards = network::read_cluster_kv_prefix_entries(
            cluster_paths,
            &format!("{}/", dfs::metadata_shards_prefix(&file_system_id)),
        )
        .map_err(|source| DfsFuseError::Network(source.to_string()))?
        .into_iter()
        .map(|entry| entry.decode::<dfs::DfsMetadataShardConfig>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
        shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
        if !shards.is_empty() {
            let backend = dfs::open_local_sharded_metadata_backend(
                dfs::default_dfs_metadata_shards_root(data_dir),
                shards,
            )
            .map_err(|source| DfsFuseError::Dfs(source.to_string()))?;
            return Ok(Box::new(backend));
        }
    }
    dfs::open_root_local_metadata_backend(dfs::default_dfs_metadata_root(data_dir))
        .map(|backend| Box::new(backend) as Box<dyn dfs::DfsMetadataBackend>)
        .map_err(|source| DfsFuseError::Dfs(source.to_string()))
}

fn replica_candidates(
    cluster_paths: &network::ClusterPaths,
    cluster_state: Option<&sinomo_runtime::core::ClusterBootstrapState>,
    local_node_id: NodeId,
) -> Vec<NodeId> {
    let mut candidates = vec![local_node_id.clone()];
    if cluster_state.is_some()
        && let Ok(entries) =
            network::read_cluster_kv_prefix_entries(cluster_paths, dfs::content_services_prefix())
    {
        for advertisement in entries
            .into_iter()
            .filter_map(|entry| entry.decode::<dfs::DfsNodeAdvertisement>().ok())
        {
            if advertisement.accepts_content && !candidates.contains(&advertisement.node_id) {
                candidates.push(advertisement.node_id);
            }
        }
    }
    candidates
}

#[derive(Clone, Debug)]
struct DfsFuseControlPlaneKvClient {
    cluster_paths: network::ClusterPaths,
}

impl DfsFuseControlPlaneKvClient {
    fn network_error(&self, source: NetworkError) -> KvError {
        KvError::Io {
            path: self.cluster_paths.kv_snapshot.display().to_string(),
            detail: source.to_string(),
        }
    }
}

impl DfsControlPlaneKvClient for DfsFuseControlPlaneKvClient {
    fn get(&self, key: &str) -> std::result::Result<Option<KvEntry>, KvError> {
        network::read_cluster_kv_entry(&self.cluster_paths, key)
            .map_err(|source| self.network_error(source))
    }

    fn range_prefix(&self, prefix: &str) -> std::result::Result<Vec<KvEntry>, KvError> {
        network::read_cluster_kv_prefix_entries(&self.cluster_paths, prefix)
            .map_err(|source| self.network_error(source))
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> std::result::Result<KvRevision, KvError> {
        let response = self.txn(KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::Put {
                key,
                value,
                lease_id: None,
            }],
            failure: Vec::new(),
        })?;
        Ok(response.revision)
    }

    fn txn(&mut self, txn: KvTxn) -> std::result::Result<KvTxnResponse, KvError> {
        let mut state = network::read_cluster_state(&self.cluster_paths)
            .map_err(|source| self.network_error(source))?;
        let result = network::commit_cluster_kv_txn_with_quorum_response_with_host(
            &self.cluster_paths,
            &mut state,
            txn,
            dfs_fuse_cluster_kv_txn_quorum_options(true),
            &mut DfsFuseClusterKvTxnQuorumHost,
        )
        .map_err(|source| self.network_error(source))?;
        Ok(result.txn_response.unwrap_or_else(|| KvTxnResponse {
            succeeded: true,
            revision: result.snapshot.revision,
            responses: Vec::new(),
        }))
    }
}

fn dfs_fuse_network_error(operation: &str, error: impl std::fmt::Display) -> NetworkError {
    NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
    }
}

fn dfs_fuse_send_daemon_rpc_to_endpoint(
    endpoint: &ClusterEndpointRecord,
    request: &network::ObjectRpcRequest,
    timeout: Duration,
    operation: &str,
) -> std::result::Result<network::ObjectRpcResponse, NetworkError> {
    daemon::send_daemon_rpc_to_endpoint(endpoint, request, timeout)
        .map_err(|error| dfs_fuse_network_error(operation, error))
}

fn with_dfs_fuse_cluster_raft_operation<T>(
    operation: &str,
    f: impl FnOnce() -> std::result::Result<T, NetworkError>,
) -> std::result::Result<T, NetworkError> {
    let _guard = network::lock_cluster_raft_operation()
        .map_err(|error| dfs_fuse_network_error(operation, error))?;
    f()
}

fn dfs_fuse_cluster_raft_append_replication_options(
    timeout: Duration,
) -> network::ClusterRaftAppendReplicationOptions {
    network::ClusterRaftAppendReplicationOptions {
        max_attempts: FUSE_CLUSTER_RAFT_APPEND_MAX_ATTEMPTS,
        timeout,
        tcp_command_timeout: FUSE_CLUSTER_TCP_COMMAND_TIMEOUT,
        iroh_command_timeout: FUSE_CLUSTER_IROH_COMMAND_TIMEOUT,
        tcp_foreground_attempt_timeout: FUSE_CLUSTER_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
        public_foreground_attempt_timeout: FUSE_CLUSTER_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
        iroh_foreground_attempt_timeout: FUSE_CLUSTER_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
    }
}

fn dfs_fuse_cluster_raft_committed_fanout_options() -> network::ClusterRaftCommittedFanoutOptions {
    network::ClusterRaftCommittedFanoutOptions {
        raft_append_timeout: FUSE_CLUSTER_RAFT_APPEND_TIMEOUT,
        watch_event_timeout: FUSE_CLUSTER_RPC_TIMEOUT,
    }
}

fn dfs_fuse_cluster_raft_leader_election_options() -> network::ClusterRaftLeaderElectionOptions {
    network::ClusterRaftLeaderElectionOptions {
        max_attempts: network::CLUSTER_RAFT_LEADER_ELECTION_ATTEMPTS,
        timeout: FUSE_CLUSTER_RAFT_APPEND_TIMEOUT,
        tcp_command_timeout: FUSE_CLUSTER_TCP_COMMAND_TIMEOUT,
        iroh_command_timeout: FUSE_CLUSTER_IROH_COMMAND_TIMEOUT,
        tcp_foreground_attempt_timeout: FUSE_CLUSTER_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
        public_foreground_attempt_timeout: FUSE_CLUSTER_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
        iroh_foreground_attempt_timeout: FUSE_CLUSTER_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
    }
}

fn dfs_fuse_cluster_voter_reachability_options() -> network::ClusterVoterReachabilityOptions {
    network::ClusterVoterReachabilityOptions {
        timeout: FUSE_CLUSTER_RAFT_APPEND_TIMEOUT,
        tcp_command_timeout: FUSE_CLUSTER_TCP_COMMAND_TIMEOUT,
        iroh_command_timeout: FUSE_CLUSTER_IROH_COMMAND_TIMEOUT,
        tcp_foreground_attempt_timeout: FUSE_CLUSTER_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
        public_foreground_attempt_timeout: FUSE_CLUSTER_PUBLIC_FOREGROUND_ATTEMPT_TIMEOUT,
        iroh_foreground_attempt_timeout: FUSE_CLUSTER_IROH_FOREGROUND_ATTEMPT_TIMEOUT,
    }
}

fn dfs_fuse_cluster_kv_txn_quorum_options(
    allow_forwarding: bool,
) -> network::ClusterKvTxnQuorumOptions {
    network::ClusterKvTxnQuorumOptions {
        allow_forwarding,
        raft_append_timeout: FUSE_CLUSTER_RAFT_APPEND_TIMEOUT,
        forward_options: dfs_fuse_cluster_kv_txn_forward_options(),
        leader_election_options: dfs_fuse_cluster_raft_leader_election_options(),
        voter_reachability_options: dfs_fuse_cluster_voter_reachability_options(),
    }
}

fn dfs_fuse_cluster_kv_txn_forward_options() -> network::ClusterKvTxnForwardOptions {
    network::ClusterKvTxnForwardOptions {
        timeout: FUSE_CLUSTER_RAFT_APPEND_TIMEOUT,
        tcp_command_timeout: FUSE_CLUSTER_TCP_COMMAND_TIMEOUT,
        iroh_command_timeout: FUSE_CLUSTER_IROH_COMMAND_TIMEOUT,
    }
}

fn dfs_fuse_replicate_cluster_raft_to_endpoint_with_timeout_result(
    paths: &network::ClusterPaths,
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    timeout: Duration,
    watch_events: &[KvEvent],
) -> std::result::Result<network::ClusterRaftAppendReplicationOutcome, NetworkError> {
    network::replicate_cluster_raft_to_endpoint_with_host(
        paths,
        state,
        endpoint,
        watch_events,
        dfs_fuse_cluster_raft_append_replication_options(timeout),
        &mut DfsFuseClusterRaftAppendReplicationHost,
    )
}

struct DfsFuseClusterKvTxnQuorumHost;

impl network::ClusterKvTxnQuorumHost for DfsFuseClusterKvTxnQuorumHost {
    fn refresh_current_cluster_kv_store(
        &mut self,
        paths: &network::ClusterPaths,
        _state: &ClusterBootstrapState,
    ) -> std::result::Result<(), NetworkError> {
        network::read_cluster_kv_snapshot_store(paths).map(|_| ())
    }

    fn local_is_raft_leader(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> std::result::Result<bool, NetworkError> {
        with_dfs_fuse_cluster_raft_operation("cluster raft leader check", || {
            network::cluster_raft_local_is_leader(paths, state)
        })
    }

    fn forward_cluster_kv_txn_to_voter(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
        txn: &KvTxn,
        options: network::ClusterKvTxnForwardOptions,
    ) -> std::result::Result<Option<network::ClusterKvTxnForwardResult>, NetworkError> {
        network::forward_cluster_kv_txn_to_voter_with_host(
            paths,
            state,
            txn,
            options,
            &mut DfsFuseClusterKvTxnForwardHost,
        )
    }

    fn elect_cluster_raft_leader(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
        options: network::ClusterRaftLeaderElectionOptions,
    ) -> std::result::Result<bool, NetworkError> {
        network::elect_cluster_raft_leader_with_host(
            paths,
            state,
            options,
            &mut DfsFuseClusterRaftLeaderElectionHost,
        )
    }

    fn reachable_voter_count(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
        options: network::ClusterVoterReachabilityOptions,
    ) -> usize {
        network::cluster_reachable_voter_count_with_host(
            paths,
            state,
            options,
            &mut DfsFuseClusterVoterReachabilityHost,
        )
    }

    fn commit_cluster_raft_command_with_quorum(
        &mut self,
        paths: &network::ClusterPaths,
        state: &mut ClusterBootstrapState,
        command: KvCommand,
        options: network::ClusterRaftQuorumCommitOptions,
    ) -> std::result::Result<network::ClusterRaftQuorumCommitResult, NetworkError> {
        let mut host = DfsFuseClusterRaftQuorumCommitHost {
            paths,
            local_node_id: state.local_node_id.clone(),
        };
        network::commit_cluster_raft_command_with_quorum_driver(
            paths, state, command, options, &mut host,
        )
    }
}

struct DfsFuseClusterRaftAppendReplicationHost;

impl network::ClusterRaftAppendReplicationHost for DfsFuseClusterRaftAppendReplicationHost {
    fn last_local_raft_log_index(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> std::result::Result<u64, NetworkError> {
        with_dfs_fuse_cluster_raft_operation("cluster raft last-log read", || {
            Ok(network::open_cluster_raft_kv_node(paths, &state.local_node_id)?.last_log_index())
        })
    }

    fn build_raft_append_request(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
        next_index: u64,
    ) -> std::result::Result<network::ClusterRaftAppendRequest, NetworkError> {
        with_dfs_fuse_cluster_raft_operation("cluster raft append request build", || {
            network::build_cluster_raft_append_request_from_next_index(paths, state, next_index)
        })
    }

    fn send_raft_append(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &network::ClusterRaftAppendRequest,
        timeout: Duration,
    ) -> std::result::Result<network::ClusterRaftAppendResponse, NetworkError> {
        let rpc_request = network::cluster_raft_append_rpc_request(request)?;
        let response = dfs_fuse_send_daemon_rpc_to_endpoint(
            endpoint,
            &rpc_request,
            timeout,
            "cluster raft append",
        )?;
        network::decode_cluster_raft_append_rpc_response(&response)
    }
}

struct DfsFuseClusterRaftCommittedFanoutHost;

impl network::ClusterRaftCommittedFanoutHost for DfsFuseClusterRaftCommittedFanoutHost {
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        watch_events: &[KvEvent],
        timeout: Duration,
    ) -> std::result::Result<network::ClusterRaftAppendReplicationOutcome, NetworkError> {
        dfs_fuse_replicate_cluster_raft_to_endpoint_with_timeout_result(
            paths,
            state,
            endpoint,
            timeout,
            watch_events,
        )
    }

    fn send_cluster_kv_watch_events(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &ClusterKvWatchEventsRequest,
        timeout: Duration,
    ) -> std::result::Result<ClusterKvWatchEventsResponse, NetworkError> {
        if let Some(response) = sinomo_runtime::kv::cluster_kv_watch_events_noop_response(request) {
            return Ok(response);
        }
        let rpc_request = network::cluster_kv_watch_events_rpc_request(request)?;
        let response = dfs_fuse_send_daemon_rpc_to_endpoint(
            endpoint,
            &rpc_request,
            timeout,
            "cluster kv watch-event fanout",
        )?;
        network::decode_cluster_kv_watch_events_rpc_response(&response)
    }
}

struct DfsFuseClusterRaftQuorumCommitHost<'a> {
    paths: &'a network::ClusterPaths,
    local_node_id: NodeId,
}

impl ClusterKvRaftCommandStorage for DfsFuseClusterRaftQuorumCommitHost<'_> {
    type Error = NetworkError;

    fn append_cluster_raft_command_uncommitted_or_no_effect(
        &mut self,
        command: KvCommand,
    ) -> std::result::Result<ClusterKvRaftUncommittedAppend, NetworkError> {
        with_dfs_fuse_cluster_raft_operation("cluster raft uncommitted append", || {
            network::append_cluster_raft_command_uncommitted_or_no_effect(
                self.paths,
                &self.local_node_id,
                command,
            )
        })
    }

    fn discard_cluster_raft_uncommitted_tail_and_clear_leader(
        &mut self,
    ) -> std::result::Result<(), NetworkError> {
        with_dfs_fuse_cluster_raft_operation("cluster raft discard uncommitted tail", || {
            let _ = network::discard_cluster_raft_uncommitted_tail_and_clear_leader(
                self.paths,
                &self.local_node_id,
            );
            Ok(())
        })
    }

    fn commit_cluster_raft_index_with_events(
        &mut self,
        index: u64,
    ) -> std::result::Result<ClusterKvRaftCommitResult, NetworkError> {
        with_dfs_fuse_cluster_raft_operation("cluster raft index commit", || {
            network::commit_cluster_raft_index_with_events(self.paths, &self.local_node_id, index)
        })
    }
}

impl network::ClusterRaftQuorumTransportHost for DfsFuseClusterRaftQuorumCommitHost<'_> {
    fn replicate_cluster_raft_to_endpoint(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> std::result::Result<network::ClusterRaftAppendResponse, NetworkError> {
        dfs_fuse_replicate_cluster_raft_to_endpoint_with_timeout_result(
            paths,
            state,
            endpoint,
            timeout,
            &[],
        )
        .map(|outcome| outcome.response)
    }

    fn fan_out_committed_cluster_raft_command(
        &mut self,
        paths: network::ClusterPaths,
        state: ClusterBootstrapState,
        events: Vec<KvEvent>,
        _targets: Vec<ClusterEndpointRecord>,
    ) {
        let _ = network::fan_out_committed_cluster_raft_command_with_host(
            &paths,
            &state,
            &events,
            dfs_fuse_cluster_raft_committed_fanout_options(),
            &mut DfsFuseClusterRaftCommittedFanoutHost,
        );
    }
}

struct DfsFuseClusterVoterReachabilityHost;

impl network::ClusterVoterReachabilityHost for DfsFuseClusterVoterReachabilityHost {
    fn probe_voter(&mut self, endpoint: &ClusterEndpointRecord, timeout: Duration) -> bool {
        daemon::fetch_daemon_status_from_endpoint(endpoint, timeout).is_ok()
    }
}

struct DfsFuseClusterRaftLeaderElectionHost;

impl network::ClusterRaftVoteTransportHost for DfsFuseClusterRaftLeaderElectionHost {
    fn send_raft_vote(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        request: &network::ClusterRaftVoteRequest,
        timeout: Duration,
    ) -> std::result::Result<network::ClusterRaftVoteResponse, NetworkError> {
        let rpc_request = network::cluster_raft_vote_rpc_request(request)?;
        let response = dfs_fuse_send_daemon_rpc_to_endpoint(
            endpoint,
            &rpc_request,
            timeout,
            "cluster raft vote",
        )?;
        network::decode_cluster_raft_vote_rpc_response(&response)
    }
}

struct DfsFuseClusterKvTxnForwardHost;

impl network::ClusterKvTxnForwardHost for DfsFuseClusterKvTxnForwardHost {
    fn known_raft_leader_id(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> std::result::Result<Option<NodeId>, NetworkError> {
        with_dfs_fuse_cluster_raft_operation("cluster raft leader lookup", || {
            network::cluster_raft_known_leader_id(paths, state)
        })
    }

    fn send_cluster_kv_txn(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        txn: &KvTxn,
        timeout: Duration,
    ) -> std::result::Result<KvTxnResponse, NetworkError> {
        let rpc_request = network::cluster_kv_txn_rpc_request(txn)?;
        let response = dfs_fuse_send_daemon_rpc_to_endpoint(
            endpoint,
            &rpc_request,
            timeout,
            "cluster kv txn",
        )?;
        network::decode_cluster_kv_txn_rpc_response(&response)
    }

    fn fetch_cluster_kv_snapshot(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        timeout: Duration,
    ) -> std::result::Result<KvSnapshot, NetworkError> {
        let rpc_request = network::cluster_kv_snapshot_rpc_request()?;
        let response = dfs_fuse_send_daemon_rpc_to_endpoint(
            endpoint,
            &rpc_request,
            timeout,
            "cluster kv snapshot",
        )?;
        network::decode_cluster_kv_snapshot_rpc_response(&response)
    }

    fn relay_peers(
        &mut self,
        paths: &network::ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Vec<ClusterEndpointRecord> {
        network::cluster_member_publish_targets(paths, state)
    }

    fn relay_rpc_to_node(
        &mut self,
        state: &ClusterBootstrapState,
        target_node_id: &NodeId,
        peer: &ClusterEndpointRecord,
        request: &network::ObjectRpcRequest,
        timeout: Duration,
    ) -> std::result::Result<network::ObjectRpcResponse, NetworkError> {
        let signed = daemon::sign_daemon_rpc_request_for_local_node(request)
            .map_err(|error| dfs_fuse_network_error("cluster relay sign inner", error))?;
        let inner = network::DaemonControlRequest::authenticated_rpc(
            daemon::daemon_rpc_command(&request.route),
            request.clone(),
            signed,
        );
        let outer =
            network::RelayEnvelope::new(state.cluster_id.clone(), target_node_id.clone(), inner)
                .into_request()?;
        daemon::send_daemon_rpc_to_endpoint(peer, &outer, timeout)
            .map_err(|error| dfs_fuse_network_error("cluster relay", error))
    }
}

#[derive(Clone)]
struct DfsFuseEndpointTransport {
    local_node_id: NodeId,
    endpoints_by_node_id: BTreeMap<NodeId, Vec<ClusterEndpointRecord>>,
    incoming_frames: VecDeque<InboundFrame>,
    timeout: Duration,
}

impl std::fmt::Debug for DfsFuseEndpointTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsFuseEndpointTransport")
            .field("local_node_id", &self.local_node_id)
            .field("endpoints_by_node_id", &self.endpoints_by_node_id)
            .field("incoming_frames", &self.incoming_frames)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl DfsFuseEndpointTransport {
    fn new(
        local_node_id: NodeId,
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
    ) -> Self {
        let mut endpoints_by_node_id = BTreeMap::<NodeId, Vec<ClusterEndpointRecord>>::new();
        for endpoint in endpoints {
            if endpoint.node_id != local_node_id {
                endpoints_by_node_id
                    .entry(endpoint.node_id.clone())
                    .or_default()
                    .push(endpoint);
            }
        }
        Self {
            local_node_id,
            endpoints_by_node_id,
            incoming_frames: VecDeque::new(),
            timeout: DEFAULT_FUSE_TIMEOUT,
        }
    }

    fn deliver_returned_frame(
        &mut self,
        frame: NetworkFrame,
    ) -> std::result::Result<(), NetworkError> {
        let target_node_id = frame.target_node_id.clone();
        if target_node_id == self.local_node_id {
            self.incoming_frames.push_back(InboundFrame {
                received_by: self.local_node_id.clone(),
                frame,
            });
            return Ok(());
        }
        self.send_frame_to_endpoint(&target_node_id, frame, false)
    }

    fn send_frame_to_endpoint(
        &mut self,
        target_node_id: &NodeId,
        frame: NetworkFrame,
        require_local_source: bool,
    ) -> std::result::Result<(), NetworkError> {
        if require_local_source && &frame.source_node_id != self.local_node_id() {
            return Err(NetworkError::FrameSourceMismatch {
                local_node_id: self.local_node_id.clone(),
                source_node_id: frame.source_node_id,
            });
        }
        if !require_local_source
            && frame.source_node_id != self.local_node_id
            && !self
                .endpoints_by_node_id
                .contains_key(&frame.source_node_id)
        {
            return Err(NetworkError::UnknownNode {
                node_id: frame.source_node_id,
            });
        }
        if &frame.target_node_id != target_node_id {
            return Err(NetworkError::FrameTargetMismatch {
                requested_target_node_id: target_node_id.clone(),
                frame_target_node_id: frame.target_node_id,
            });
        }
        let endpoints = self
            .endpoints_by_node_id
            .get(target_node_id)
            .cloned()
            .ok_or_else(|| NetworkError::UnknownNode {
                node_id: target_node_id.clone(),
            })?;
        let request = DfsProtocolFrameCommand {
            request_id: format!("fuse_dfs_protocol_frame_{}", frame.frame_id.0),
            frame,
        };
        let mut failures = Vec::new();
        for endpoint in &endpoints {
            match send_dfs_protocol_frame(endpoint, &request, self.timeout) {
                Ok(response) => {
                    for returned_frame in response.outbound_frames {
                        self.deliver_returned_frame(returned_frame)?;
                    }
                    return Ok(());
                }
                Err(error) => failures.push(format!("{} failed: {error}", endpoint.address)),
            }
        }
        Err(NetworkError::Io {
            operation: format!("send DFS protocol frame to {target_node_id}"),
            detail: failures.join("; "),
        })
    }
}

impl NetworkTransport for DfsFuseEndpointTransport {
    fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    fn send(
        &mut self,
        target_node_id: &NodeId,
        frame: NetworkFrame,
    ) -> std::result::Result<(), NetworkError> {
        self.send_frame_to_endpoint(target_node_id, frame, true)
    }

    fn drain_incoming(&mut self) -> std::result::Result<Vec<InboundFrame>, NetworkError> {
        Ok(self.incoming_frames.drain(..).collect())
    }
}

fn send_dfs_protocol_frame(
    endpoint: &ClusterEndpointRecord,
    request: &DfsProtocolFrameCommand,
    timeout: Duration,
) -> std::result::Result<DfsProtocolFrameResponse, DfsFuseError> {
    let rpc_request = dfs::dfs_protocol_frame_rpc_request(request)
        .map_err(|source| DfsFuseError::Daemon(source.to_string()))?;
    let response = daemon::send_daemon_rpc_to_endpoint(endpoint, &rpc_request, timeout)
        .map_err(|source| DfsFuseError::Daemon(source.to_string()))?;
    dfs::decode_dfs_protocol_frame_rpc_response(&response)
        .map_err(|source| DfsFuseError::Daemon(source.to_string()))
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
