use crate::handoff::NfsMountedMutationHandoff;
use crate::{DfsNfsError, Result, normalize_root_path};
use nfs3_server::nfs3_types::nfs3::{
    Nfs3Option, createverf3, fattr3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3,
    set_atime, set_mtime, specdata3, stable_how,
};
use nfs3_server::vfs::{
    DirEntry, DirEntryPlus, FileHandle, NextResult, NfsFileSystem, NfsReadFileSystem,
    ReadDirIterator, ReadDirPlusIterator, VFSCapabilities,
};
use sinomo_core::local::SinomoDataDir;
use sinomo_dfs::{
    self as dfs, DfsDirEntry, DfsObjectId, DfsObjectKind, DfsPosixCreateAttrs, DfsPosixError,
    DfsStat, DfsWriteOptions, MountedSifsService,
};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_NFS_IO_BYTES: usize = 1024 * 1024;
const DEFAULT_DIRECTORY_MODE: u32 = 0o755;
const DEFAULT_FILE_MODE: u32 = 0o644;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SifsNfsHandle {
    object_id: DfsObjectId,
    encoded: Arc<[u8]>,
}

impl SifsNfsHandle {
    fn new(object_id: DfsObjectId) -> Self {
        let encoded: Arc<[u8]> = Arc::from(object_id.to_string().into_bytes());
        debug_assert!(
            encoded.len() <= 56,
            "DFS object ID exceeds NFS handle payload"
        );
        Self { object_id, encoded }
    }

    fn file_id(&self) -> u64 {
        object_file_id(&self.object_id)
    }
}

impl FileHandle for SifsNfsHandle {
    fn len(&self) -> usize {
        self.encoded.len()
    }

    fn as_bytes(&self) -> &[u8] {
        &self.encoded
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() || bytes.len() > 56 {
            return None;
        }
        let object_id = std::str::from_utf8(bytes).ok()?.parse().ok()?;
        let handle = Self::new(object_id);
        (handle.as_bytes() == bytes).then_some(handle)
    }
}

#[derive(Debug)]
struct SifsNfsState {
    root_path: String,
    write_options: DfsWriteOptions,
    read_only: bool,
}

impl SifsNfsState {
    fn stat_handle(
        &self,
        service: &mut MountedSifsService,
        handle: &SifsNfsHandle,
    ) -> std::result::Result<DfsStat, nfsstat3> {
        let stat = service
            .posix_stat_object(&handle.object_id)
            .map_err(map_posix_error)?
            .ok_or(nfsstat3::NFS3ERR_STALE)?;
        if !path_is_beneath(&self.root_path, &stat.path) {
            return Err(nfsstat3::NFS3ERR_STALE);
        }
        Ok(stat)
    }

    fn handle_for_stat(&self, stat: &DfsStat) -> SifsNfsHandle {
        SifsNfsHandle::new(stat.object_id.clone())
    }

    fn child_path(
        &self,
        service: &mut MountedSifsService,
        parent: &SifsNfsHandle,
        name: &[u8],
    ) -> std::result::Result<String, nfsstat3> {
        let parent = self.stat_handle(service, parent)?;
        if !matches!(parent.kind, DfsObjectKind::Directory | DfsObjectKind::Mount) {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }
        let name = valid_name(name)?;
        Ok(join_path(&parent.path, name))
    }

    fn write_options(&self) -> DfsWriteOptions {
        self.write_options.clone()
    }

    fn ensure_writable(&self) -> std::result::Result<(), nfsstat3> {
        if self.read_only {
            Err(nfsstat3::NFS3ERR_ROFS)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
pub struct SifsNfsFileSystem {
    service: Arc<Mutex<MountedSifsService>>,
    state: Arc<Mutex<SifsNfsState>>,
    handoff: Option<Arc<NfsMountedMutationHandoff>>,
    root: SifsNfsHandle,
}

impl SifsNfsFileSystem {
    pub fn open(data_dir: Option<PathBuf>, root_path: String, read_only: bool) -> Result<Self> {
        let root_path = normalize_root_path(&root_path)?;
        let data_dir = match data_dir {
            Some(path) => SinomoDataDir::new(path),
            None => SinomoDataDir::from_env_or_home()
                .map_err(|source| DfsNfsError::Dfs(source.to_string()))?,
        };
        #[cfg(test)]
        if let Some(opened) = sinomo_vfs_sifs::test_support::open_registered(&data_dir, None) {
            // Adapter tests borrow the real component owner in this process;
            // the external daemon handoff remains a separate host boundary.
            let mut opened = opened.map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
            let root_stat = opened
                .service
                .posix_lstat_distributed(&root_path)
                .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
            return Self::from_opened(opened, root_stat, root_path, read_only, None);
        }
        let clustered = sinomo_core::network::ClusterPaths::new(&data_dir)
            .state
            .exists();
        let handoff = if clustered {
            Some(Arc::new(NfsMountedMutationHandoff::open(
                &data_dir,
                format!("nfs:{root_path}"),
            )?))
        } else {
            None
        };
        let open_service = || {
            let mut opened = if clustered {
                let peer = Arc::new(
                    sinomo_dfs::DaemonDfsPeerTransport::connect(data_dir.clone())
                        .map_err(|source| DfsNfsError::Dfs(source.to_string()))?,
                );
                let sync_peer: Arc<dyn dfs::DfsPeerTransport> = peer.clone();
                let async_peer: Arc<dyn dfs::AsyncDfsPeerTransport> = peer;
                dfs::open_mounted_sifs_service_with_peer_transports_and_default_control_plane(
                    data_dir.clone(),
                    sync_peer,
                    async_peer,
                )
                .map_err(|source| DfsNfsError::Dfs(source.to_string()))?
            } else {
                dfs::open_mounted_sifs_service(Some(data_dir.root()))
                    .map_err(|source| DfsNfsError::Dfs(source.to_string()))?
            };
            let root_stat = opened
                .service
                .posix_lstat_distributed(&root_path)
                .map_err(|source| DfsNfsError::Dfs(source.to_string()))?;
            Ok((opened, root_stat))
        };
        let opened = if let Some(handoff) = &handoff {
            handoff.run_mount_open(open_service)?
        } else {
            open_service()?
        };
        let (opened, root_stat) = opened;
        Self::from_opened(opened, root_stat, root_path, read_only, handoff)
    }

    fn from_opened(
        opened: dfs::OpenedMountedSifsService,
        root_stat: DfsStat,
        root_path: String,
        read_only: bool,
        handoff: Option<Arc<NfsMountedMutationHandoff>>,
    ) -> Result<Self> {
        let dfs::OpenedMountedSifsService {
            service,
            replica_candidates,
        } = opened;
        if !matches!(
            root_stat.kind,
            DfsObjectKind::Directory | DfsObjectKind::Mount
        ) {
            return Err(DfsNfsError::InvalidConfiguration(format!(
                "NFS export root is not a directory: {root_path}"
            )));
        }
        let local_node = service.node_id().clone();
        let replica_candidates = if replica_candidates.is_empty() {
            vec![local_node]
        } else {
            replica_candidates
        };
        let mut write_options = DfsWriteOptions::replicated_eventual(replica_candidates);
        write_options.foreground_replica_repair = false;
        Ok(Self {
            service: Arc::new(Mutex::new(service)),
            state: Arc::new(Mutex::new(SifsNfsState {
                root_path,
                write_options,
                read_only,
            })),
            handoff,
            root: SifsNfsHandle::new(root_stat.object_id),
        })
    }

    async fn with_state<R, F>(&self, operation: F) -> std::result::Result<R, nfsstat3>
    where
        R: Send + 'static,
        F: FnOnce(&mut SifsNfsState) -> std::result::Result<R, nfsstat3> + Send + 'static,
    {
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            let mut state = state.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
            operation(&mut state)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
    }

    async fn with_service_state<R, F>(&self, operation: F) -> std::result::Result<R, nfsstat3>
    where
        R: Send + 'static,
        F: FnOnce(&mut MountedSifsService, &mut SifsNfsState) -> std::result::Result<R, nfsstat3>
            + Send
            + 'static,
    {
        if self
            .handoff
            .as_ref()
            .is_some_and(|handoff| handoff.is_gated())
        {
            return Err(nfsstat3::NFS3ERR_IO);
        }
        let service = Arc::clone(&self.service);
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            let mut service = service.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
            let mut state = state.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
            operation(&mut service, &mut state)
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
    }

    async fn mutate<R, F>(
        &self,
        operation: &'static str,
        stable_operation_id: Option<String>,
        mutation: F,
    ) -> std::result::Result<R, nfsstat3>
    where
        R: Send + 'static,
        F: FnOnce(&mut MountedSifsService) -> std::result::Result<R, DfsPosixError>
            + Send
            + 'static,
    {
        let service = Arc::clone(&self.service);
        let handoff = self.handoff.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(handoff) = handoff {
                handoff
                    .run_operation(operation, stable_operation_id, &service, mutation)
                    .map_err(map_posix_error)
            } else {
                let mut service = service.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
                mutation(&mut service).map_err(map_posix_error)
            }
        })
        .await
        .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
    }
}

impl NfsReadFileSystem for SifsNfsFileSystem {
    type Handle = SifsNfsHandle;

    fn root_dir(&self) -> Self::Handle {
        self.root.clone()
    }

    async fn lookup(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
    ) -> std::result::Result<Self::Handle, nfsstat3> {
        let dirid = dirid.clone();
        let name = filename.as_ref().to_vec();
        self.with_service_state(move |service, state| {
            match name.as_slice() {
                b"." => return Ok(dirid.clone()),
                b".." => {
                    let current = state.stat_handle(service, &dirid)?;
                    if current.path == state.root_path {
                        return Ok(state.handle_for_stat(&current));
                    }
                    let parent = parent_path(&current.path).ok_or(nfsstat3::NFS3ERR_STALE)?;
                    if !path_is_beneath(&state.root_path, &parent) {
                        return Err(nfsstat3::NFS3ERR_STALE);
                    }
                    let stat = service
                        .posix_lstat_distributed(&parent)
                        .map_err(map_posix_error)?;
                    return Ok(state.handle_for_stat(&stat));
                }
                _ => {}
            }
            let path = state.child_path(service, &dirid, &name)?;
            let stat = service
                .posix_lstat_distributed(&path)
                .map_err(map_posix_error)?;
            Ok(state.handle_for_stat(&stat))
        })
        .await
    }

    async fn getattr(&self, id: &Self::Handle) -> std::result::Result<fattr3, nfsstat3> {
        let id = id.clone();
        self.with_service_state(move |service, state| {
            let stat = state.stat_handle(service, &id)?;
            Ok(stat_to_fattr(&stat))
        })
        .await
    }

    async fn read(
        &self,
        id: &Self::Handle,
        offset: u64,
        count: u32,
    ) -> std::result::Result<(Vec<u8>, bool), nfsstat3> {
        let id = id.clone();
        let count = usize::try_from(count)
            .unwrap_or(usize::MAX)
            .min(MAX_NFS_IO_BYTES);
        self.with_service_state(move |service, state| {
            let stat = state.stat_handle(service, &id)?;
            if !matches!(
                stat.kind,
                DfsObjectKind::File | DfsObjectKind::Hardlink | DfsObjectKind::Artifact
            ) {
                return Err(nfsstat3::NFS3ERR_INVAL);
            }
            let size = stat.size_bytes.unwrap_or(0);
            if offset >= size {
                return Ok((Vec::new(), true));
            }
            let bytes = service
                .posix_read_file_range_for_object(&stat.object_id, offset, count)
                .map_err(map_posix_error)?;
            let eof = offset.saturating_add(bytes.len() as u64) >= size;
            Ok((bytes, eof))
        })
        .await
    }

    async fn readdir(
        &self,
        dirid: &Self::Handle,
        cookie: u64,
    ) -> std::result::Result<impl ReadDirIterator, nfsstat3> {
        let iterator = directory_iterator(
            Arc::clone(&self.service),
            Arc::clone(&self.state),
            self.handoff.clone(),
            dirid.clone(),
            cookie,
        )
        .await?;
        Ok(iterator)
    }

    async fn readdirplus(
        &self,
        dirid: &Self::Handle,
        cookie: u64,
    ) -> std::result::Result<impl ReadDirPlusIterator<Self::Handle>, nfsstat3> {
        let iterator = directory_iterator(
            Arc::clone(&self.service),
            Arc::clone(&self.state),
            self.handoff.clone(),
            dirid.clone(),
            cookie,
        )
        .await?;
        Ok(iterator)
    }

    async fn readlink(&self, id: &Self::Handle) -> std::result::Result<nfspath3<'_>, nfsstat3> {
        let id = id.clone();
        self.with_service_state(move |service, state| {
            let stat = state.stat_handle(service, &id)?;
            let target = service
                .posix_readlink_distributed(&stat.path)
                .map_err(map_posix_error)?;
            Ok(nfspath3::from(target.into_bytes()))
        })
        .await
    }
}

impl NfsFileSystem for SifsNfsFileSystem {
    fn capabilities(&self) -> VFSCapabilities {
        self.state
            .lock()
            .map(|state| {
                if state.read_only {
                    VFSCapabilities::ReadOnly
                } else {
                    VFSCapabilities::ReadWrite
                }
            })
            .unwrap_or(VFSCapabilities::ReadOnly)
    }

    async fn setattr(
        &self,
        id: &Self::Handle,
        setattr: sattr3,
    ) -> std::result::Result<fattr3, nfsstat3> {
        let id = id.clone();
        let (mut stat, write_options) = self
            .with_service_state(move |service, state| {
                state.ensure_writable()?;
                let stat = state.stat_handle(service, &id)?;
                if setattr.mode.is_some() && stat.kind == DfsObjectKind::Symlink {
                    return Err(nfsstat3::NFS3ERR_NOTSUPP);
                }
                Ok((stat, state.write_options()))
            })
            .await?;
        let stat_kind = stat.kind.clone();
        let stat = self
            .mutate("nfs.setattr", None, move |service| {
                if let Nfs3Option::Some(size) = setattr.size {
                    stat = service.posix_truncate_file_for_object(
                        &stat.object_id,
                        size,
                        write_options.clone(),
                    )?;
                }
                if let Nfs3Option::Some(mode) = setattr.mode {
                    stat = service.posix_set_mode(&stat.path, mode)?;
                }
                if setattr.uid.is_some() || setattr.gid.is_some() {
                    let uid = match setattr.uid {
                        Nfs3Option::Some(uid) => uid,
                        Nfs3Option::None => stat.uid,
                    };
                    let gid = match setattr.gid {
                        Nfs3Option::Some(gid) => gid,
                        Nfs3Option::None => stat.gid,
                    };
                    stat = if stat_kind == DfsObjectKind::Symlink {
                        service.posix_set_symlink_owner(&stat.path, uid, gid)
                    } else {
                        service.posix_set_owner(&stat.path, uid, gid)
                    }?;
                }
                let atime = requested_time(setattr.atime);
                let mtime = requested_mtime(setattr.mtime);
                if atime.is_some() || mtime.is_some() {
                    stat = if stat_kind == DfsObjectKind::Symlink {
                        service.posix_set_symlink_times(&stat.path, atime, mtime)
                    } else {
                        service.posix_set_times(&stat.path, atime, mtime)
                    }?;
                }
                Ok(stat)
            })
            .await?;
        Ok(stat_to_fattr(&stat))
    }

    async fn write(
        &self,
        id: &Self::Handle,
        offset: u64,
        data: &[u8],
        _stable: stable_how,
    ) -> std::result::Result<(fattr3, stable_how), nfsstat3> {
        if data.len() > MAX_NFS_IO_BYTES {
            return Err(nfsstat3::NFS3ERR_FBIG);
        }
        let id = id.clone();
        let (object_id, write_options) = self
            .with_service_state(move |service, state| {
                state.ensure_writable()?;
                let stat = state.stat_handle(service, &id)?;
                Ok((stat.object_id, state.write_options()))
            })
            .await?;
        let data = data.to_vec();
        let stat = self
            .mutate("nfs.write", None, move |service| {
                service.posix_write_file_at_for_object(&object_id, offset, &data, write_options)
            })
            .await?;
        Ok((stat_to_fattr(&stat), stable_how::FILE_SYNC))
    }

    async fn create(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
        attr: sattr3,
    ) -> std::result::Result<(Self::Handle, fattr3), nfsstat3> {
        let dirid = dirid.clone();
        let name = filename.as_ref().to_vec();
        let (path, write_options) = self
            .with_service_state(move |service, state| {
                state.ensure_writable()?;
                Ok((
                    state.child_path(service, &dirid, &name)?,
                    state.write_options(),
                ))
            })
            .await?;
        let attrs = create_attrs(&attr, false);
        let stat = self
            .mutate("nfs.create", None, move |service| {
                service.posix_create_file_with_attrs(&path, write_options, attrs)
            })
            .await?;
        let handle = self
            .with_state({
                let stat = stat.clone();
                move |state| Ok(state.handle_for_stat(&stat))
            })
            .await?;
        Ok((handle, stat_to_fattr(&stat)))
    }

    async fn create_exclusive(
        &self,
        _dirid: &Self::Handle,
        _filename: &filename3<'_>,
        _createverf: createverf3,
    ) -> std::result::Result<Self::Handle, nfsstat3> {
        // NFS EXCLUSIVE requires the verifier-to-object mapping to survive a
        // server crash. SIFS has no durable field for that protocol verifier,
        // so advertising support would violate RFC 1813 replay semantics.
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    async fn mkdir(
        &self,
        dirid: &Self::Handle,
        dirname: &filename3<'_>,
    ) -> std::result::Result<(Self::Handle, fattr3), nfsstat3> {
        let dirid = dirid.clone();
        let name = dirname.as_ref().to_vec();
        let path = self
            .with_service_state(move |service, state| {
                state.ensure_writable()?;
                state.child_path(service, &dirid, &name)
            })
            .await?;
        let stat = self
            .mutate("nfs.mkdir", None, move |service| {
                service.posix_mkdir_with_create_attrs(
                    &path,
                    DfsPosixCreateAttrs::new(DEFAULT_DIRECTORY_MODE, 0, 0),
                )
            })
            .await?;
        let handle = self
            .with_state({
                let stat = stat.clone();
                move |state| Ok(state.handle_for_stat(&stat))
            })
            .await?;
        Ok((handle, stat_to_fattr(&stat)))
    }

    async fn remove(
        &self,
        dirid: &Self::Handle,
        filename: &filename3<'_>,
    ) -> std::result::Result<(), nfsstat3> {
        let dirid = dirid.clone();
        let name = filename.as_ref().to_vec();
        let (path, directory) = self
            .with_service_state(move |service, state| {
                state.ensure_writable()?;
                let path = state.child_path(service, &dirid, &name)?;
                let stat = service
                    .posix_lstat_distributed(&path)
                    .map_err(map_posix_error)?;
                Ok((
                    path,
                    matches!(stat.kind, DfsObjectKind::Directory | DfsObjectKind::Mount),
                ))
            })
            .await?;
        self.mutate("nfs.remove", None, move |service| {
            if directory {
                service.posix_rmdir(&path)
            } else {
                service.posix_unlink(&path)
            }
            .map(drop)
        })
        .await
    }

    async fn rename<'a>(
        &self,
        from_dirid: &Self::Handle,
        from_filename: &filename3<'a>,
        to_dirid: &Self::Handle,
        to_filename: &filename3<'a>,
    ) -> std::result::Result<(), nfsstat3> {
        let from_dirid = from_dirid.clone();
        let to_dirid = to_dirid.clone();
        let from_name = from_filename.as_ref().to_vec();
        let to_name = to_filename.as_ref().to_vec();
        let (from, to) = self
            .with_service_state(move |service, state| {
                state.ensure_writable()?;
                Ok((
                    state.child_path(service, &from_dirid, &from_name)?,
                    state.child_path(service, &to_dirid, &to_name)?,
                ))
            })
            .await?;
        self.mutate("nfs.rename", None, move |service| {
            service.posix_rename(&from, &to).map(drop)
        })
        .await
    }

    async fn symlink<'a>(
        &self,
        dirid: &Self::Handle,
        linkname: &filename3<'a>,
        symlink: &nfspath3<'a>,
        attr: &sattr3,
    ) -> std::result::Result<(Self::Handle, fattr3), nfsstat3> {
        let dirid = dirid.clone();
        let name = linkname.as_ref().to_vec();
        let target = std::str::from_utf8(symlink.as_ref())
            .map_err(|_| nfsstat3::NFS3ERR_INVAL)?
            .to_string();
        let attrs = create_attrs(attr, true);
        let (path, write_options) = self
            .with_service_state(move |service, state| {
                state.ensure_writable()?;
                Ok((
                    state.child_path(service, &dirid, &name)?,
                    state.write_options(),
                ))
            })
            .await?;
        let stat = self
            .mutate("nfs.symlink", None, move |service| {
                service.posix_symlink_with_create_attrs(&target, &path, write_options, attrs)
            })
            .await?;
        let handle = self
            .with_state({
                let stat = stat.clone();
                move |state| Ok(state.handle_for_stat(&stat))
            })
            .await?;
        Ok((handle, stat_to_fattr(&stat)))
    }

    async fn commit(
        &self,
        id: &Self::Handle,
        _offset: u64,
        _count: u32,
    ) -> std::result::Result<(), nfsstat3> {
        let id = id.clone();
        self.with_service_state(move |service, state| state.stat_handle(service, &id).map(drop))
            .await
    }
}

#[derive(Clone, Debug)]
struct NfsDirectoryRecord {
    handle: SifsNfsHandle,
    name: filename3<'static>,
}

#[derive(Debug)]
struct SifsNfsDirIterator {
    entries: Vec<NfsDirectoryRecord>,
    index: usize,
}

impl SifsNfsDirIterator {
    fn next_record(&mut self) -> NextResult<NfsDirectoryRecord> {
        let Some(record) = self.entries.get(self.index).cloned() else {
            return NextResult::Eof;
        };
        self.index += 1;
        NextResult::Ok(record)
    }
}

impl ReadDirIterator for SifsNfsDirIterator {
    async fn next(&mut self) -> NextResult<DirEntry> {
        match self.next_record() {
            NextResult::Ok(record) => NextResult::Ok(DirEntry {
                fileid: record.handle.file_id(),
                name: record.name,
                cookie: record.handle.file_id(),
            }),
            NextResult::Eof => NextResult::Eof,
            NextResult::Err(error) => NextResult::Err(error),
        }
    }
}

impl ReadDirPlusIterator<SifsNfsHandle> for SifsNfsDirIterator {
    async fn next(&mut self) -> NextResult<DirEntryPlus<SifsNfsHandle>> {
        match self.next_record() {
            NextResult::Ok(record) => NextResult::Ok(DirEntryPlus {
                fileid: record.handle.file_id(),
                name: record.name,
                cookie: record.handle.file_id(),
                name_attributes: None,
                name_handle: Some(record.handle),
            }),
            NextResult::Eof => NextResult::Eof,
            NextResult::Err(error) => NextResult::Err(error),
        }
    }
}

async fn directory_iterator(
    service: Arc<Mutex<MountedSifsService>>,
    state: Arc<Mutex<SifsNfsState>>,
    handoff: Option<Arc<NfsMountedMutationHandoff>>,
    dirid: SifsNfsHandle,
    cookie: u64,
) -> std::result::Result<SifsNfsDirIterator, nfsstat3> {
    tokio::task::spawn_blocking(move || {
        let directory = {
            let mut service = service.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
            let state = state.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
            state.stat_handle(&mut service, &dirid)?
        };
        if !matches!(
            directory.kind,
            DfsObjectKind::Directory | DfsObjectKind::Mount
        ) {
            return Err(nfsstat3::NFS3ERR_NOTDIR);
        }
        let directory_path = directory.path;
        let mut entries = if let Some(handoff) = handoff {
            handoff
                .run_operation("nfs.readdir-refresh", None, &service, move |service| {
                    service.posix_list_dir_distributed(&directory_path)
                })
                .map_err(map_posix_error)?
        } else {
            let mut service = service.lock().map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?;
            service
                .posix_list_dir_distributed(&directory_path)
                .map_err(map_posix_error)?
        };
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        let mut records = entries
            .into_iter()
            .map(directory_record)
            .collect::<Vec<_>>();
        let index = if cookie == 0 {
            0
        } else {
            records
                .iter()
                .position(|record| record.handle.file_id() == cookie)
                .map(|index| index + 1)
                .ok_or(nfsstat3::NFS3ERR_BAD_COOKIE)?
        };
        records.shrink_to_fit();
        Ok(SifsNfsDirIterator {
            entries: records,
            index,
        })
    })
    .await
    .map_err(|_| nfsstat3::NFS3ERR_SERVERFAULT)?
}

fn directory_record(entry: DfsDirEntry) -> NfsDirectoryRecord {
    NfsDirectoryRecord {
        handle: SifsNfsHandle::new(entry.object_id),
        name: filename3::from(entry.name.into_bytes()),
    }
}

fn valid_name(bytes: &[u8]) -> std::result::Result<&str, nfsstat3> {
    if bytes.is_empty()
        || bytes.len() > 255
        || bytes.iter().any(|byte| matches!(byte, 0 | b'/' | b'\\'))
    {
        return Err(nfsstat3::NFS3ERR_INVAL);
    }
    std::str::from_utf8(bytes).map_err(|_| nfsstat3::NFS3ERR_INVAL)
}

fn join_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{}/{name}", parent.trim_end_matches('/'))
    }
}

fn parent_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let trimmed = path.trim_end_matches('/');
    let parent = trimmed.rsplit_once('/').map(|(parent, _)| parent)?;
    Some(if parent.is_empty() { "/" } else { parent }.to_string())
}

fn path_is_beneath(root: &str, path: &str) -> bool {
    root == "/"
        || path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn stat_to_fattr(stat: &DfsStat) -> fattr3 {
    let type_ = match stat.kind {
        DfsObjectKind::Directory | DfsObjectKind::Mount => ftype3::NF3DIR,
        DfsObjectKind::Symlink => ftype3::NF3LNK,
        DfsObjectKind::File | DfsObjectKind::Hardlink | DfsObjectKind::Artifact => ftype3::NF3REG,
        DfsObjectKind::Tombstone => ftype3::NF3REG,
    };
    let size = stat.size_bytes.unwrap_or(0);
    fattr3 {
        type_,
        mode: stat.mode & 0o7777,
        nlink: u32::try_from(stat.link_count).unwrap_or(u32::MAX),
        uid: stat.uid,
        gid: stat.gid,
        size,
        used: size,
        rdev: specdata3::default(),
        fsid: filesystem_id(stat),
        fileid: object_file_id(&stat.object_id),
        atime: nanos_to_nfs_time(stat.accessed_at_unix_nanos),
        mtime: nanos_to_nfs_time(stat.modified_at_unix_nanos),
        ctime: nanos_to_nfs_time(stat.changed_at_unix_nanos),
    }
}

fn object_file_id(object_id: &DfsObjectId) -> u64 {
    let digest = blake3::hash(object_id.to_string().as_bytes());
    u64::from_le_bytes(
        digest.as_bytes()[..8]
            .try_into()
            .expect("eight-byte digest prefix"),
    )
}

fn filesystem_id(stat: &DfsStat) -> u64 {
    let digest = blake3::hash(stat.file_system_id.to_string().as_bytes());
    u64::from_le_bytes(
        digest.as_bytes()[..8]
            .try_into()
            .expect("eight-byte digest prefix"),
    )
}

fn nanos_to_nfs_time(nanos: u64) -> nfstime3 {
    nfstime3 {
        seconds: u32::try_from(nanos / 1_000_000_000).unwrap_or(u32::MAX),
        nseconds: (nanos % 1_000_000_000) as u32,
    }
}

fn nfs_time_to_nanos(time: nfstime3) -> u64 {
    u64::from(time.seconds)
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(time.nseconds.min(999_999_999)))
}

fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn requested_time(time: set_atime) -> Option<u64> {
    match time {
        set_atime::DONT_CHANGE => None,
        set_atime::SET_TO_SERVER_TIME => Some(now_unix_nanos()),
        set_atime::SET_TO_CLIENT_TIME(time) => Some(nfs_time_to_nanos(time)),
    }
}

fn requested_mtime(time: set_mtime) -> Option<u64> {
    match time {
        set_mtime::DONT_CHANGE => None,
        set_mtime::SET_TO_SERVER_TIME => Some(now_unix_nanos()),
        set_mtime::SET_TO_CLIENT_TIME(time) => Some(nfs_time_to_nanos(time)),
    }
}

fn create_attrs(attrs: &sattr3, symlink: bool) -> DfsPosixCreateAttrs {
    let mode = match attrs.mode {
        Nfs3Option::Some(mode) => mode,
        Nfs3Option::None if symlink => 0o777,
        Nfs3Option::None => DEFAULT_FILE_MODE,
    };
    let uid = match attrs.uid {
        Nfs3Option::Some(uid) => uid,
        Nfs3Option::None => 0,
    };
    let gid = match attrs.gid {
        Nfs3Option::Some(gid) => gid,
        Nfs3Option::None => 0,
    };
    DfsPosixCreateAttrs::new(mode, uid, gid)
}

fn map_posix_error(error: DfsPosixError) -> nfsstat3 {
    match error {
        DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
            nfsstat3::NFS3ERR_NOENT
        }
        DfsPosixError::AlreadyExists { .. } => nfsstat3::NFS3ERR_EXIST,
        DfsPosixError::NotDirectory { .. } => nfsstat3::NFS3ERR_NOTDIR,
        DfsPosixError::IsDirectory { .. } => nfsstat3::NFS3ERR_ISDIR,
        DfsPosixError::NotSymlink { .. } | DfsPosixError::InvalidPath { .. } => {
            nfsstat3::NFS3ERR_INVAL
        }
        DfsPosixError::DirectoryNotEmpty { .. } => nfsstat3::NFS3ERR_NOTEMPTY,
        DfsPosixError::PendingWrite { .. }
        | DfsPosixError::SymlinkContentPending { .. }
        | DfsPosixError::PartialBulkCommit { .. } => nfsstat3::NFS3ERR_JUKEBOX,
        DfsPosixError::Conflict { .. } => nfsstat3::NFS3ERR_NOT_SYNC,
        DfsPosixError::Dfs(_) | DfsPosixError::Metadata(_) => nfsstat3::NFS3ERR_IO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nfs3_server::nfs3_types::nfs3::Nfs3Option;
    use nfs3_server::vfs::{NfsFileSystem, NfsReadFileSystem, ReadDirIterator};

    fn filename(value: &str) -> filename3<'static> {
        filename3::from(value.as_bytes().to_vec())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn nfs_adapter_roundtrips_without_a_projection_tree() {
        let owner =
            sinomo_vfs_sifs::test_support::NativeMountedOwner::start("sinomo-dfs-nfs-roundtrip")
                .expect("start native SIFS owner");
        let filesystem =
            SifsNfsFileSystem::open(Some(owner.data_dir().root()), "/".to_string(), false)
                .expect("open NFS adapter");
        let root_handle = filesystem.root_dir();
        let attrs = sattr3 {
            mode: Nfs3Option::Some(0o640),
            ..sattr3::default()
        };
        let (file, created) = filesystem
            .create(&root_handle, &filename("direct.txt"), attrs)
            .await
            .expect("create file");
        assert_eq!(created.mode, 0o640);
        filesystem
            .write(&file, 0, b"zero-copy", stable_how::FILE_SYNC)
            .await
            .expect("write file");
        let (bytes, eof) = filesystem.read(&file, 0, 64).await.expect("read file");
        assert_eq!(bytes, b"zero-copy");
        assert!(eof);

        let mut listing = filesystem
            .readdir(&root_handle, 0)
            .await
            .expect("list root");
        let mut saw_file = false;
        loop {
            match listing.next().await {
                NextResult::Ok(entry) => {
                    saw_file |= entry.name.as_ref() == b"direct.txt";
                }
                NextResult::Eof => break,
                NextResult::Err(error) => panic!("directory iteration failed: {error}"),
            }
        }
        assert!(saw_file);

        filesystem
            .rename(
                &root_handle,
                &filename("direct.txt"),
                &root_handle,
                &filename("renamed.txt"),
            )
            .await
            .expect("rename file");
        let renamed = filesystem
            .lookup(&root_handle, &filename("renamed.txt"))
            .await
            .expect("lookup renamed");
        assert_eq!(renamed, file, "NFS handle must survive rename");
        filesystem
            .remove(&root_handle, &filename("renamed.txt"))
            .await
            .expect("remove file");
        assert!(matches!(
            filesystem.getattr(&file).await,
            Err(nfsstat3::NFS3ERR_STALE)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_handles_are_self_contained_and_cannot_escape_export_root() {
        let owner = sinomo_vfs_sifs::test_support::NativeMountedOwner::start(
            "sinomo-dfs-nfs-handle-boundary",
        )
        .expect("start native SIFS owner");
        let mut bootstrap = sinomo_vfs_sifs::test_support::open_registered(owner.data_dir(), None)
            .expect("registered native SIFS owner")
            .expect("open bootstrap service")
            .service;
        bootstrap
            .posix_mkdir_with_create_attrs(
                "/export",
                DfsPosixCreateAttrs::new(DEFAULT_DIRECTORY_MODE, 0, 0),
            )
            .expect("create export root");
        bootstrap
            .posix_mkdir_with_create_attrs(
                "/outside",
                DfsPosixCreateAttrs::new(DEFAULT_DIRECTORY_MODE, 0, 0),
            )
            .expect("create outside directory");
        drop(bootstrap);

        let filesystem =
            SifsNfsFileSystem::open(Some(owner.data_dir().root()), "/export".to_string(), false)
                .expect("open bounded export");
        let root_handle = filesystem.root_dir();
        let (file, _) = filesystem
            .create(&root_handle, &filename("inside.txt"), sattr3::default())
            .await
            .expect("create exported file");
        let decoded = SifsNfsHandle::from_bytes(file.as_bytes()).expect("decode object handle");
        assert_eq!(decoded, file);
        assert!(file.len() <= 56);

        filesystem
            .service
            .lock()
            .expect("lock mounted service")
            .posix_rename("/export/inside.txt", "/outside/moved.txt")
            .expect("move file outside export through an administrative path");
        assert!(matches!(
            filesystem.getattr(&file).await,
            Err(nfsstat3::NFS3ERR_STALE)
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_only_and_exclusive_create_capabilities_fail_closed() {
        let owner =
            sinomo_vfs_sifs::test_support::NativeMountedOwner::start("sinomo-dfs-nfs-capabilities")
                .expect("start native SIFS owner");
        let writable =
            SifsNfsFileSystem::open(Some(owner.data_dir().root()), "/".to_string(), false)
                .expect("open writable adapter");
        assert!(matches!(
            writable
                .create_exclusive(
                    &writable.root_dir(),
                    &filename("exclusive.txt"),
                    createverf3([7; 8]),
                )
                .await,
            Err(nfsstat3::NFS3ERR_NOTSUPP)
        ));
        drop(writable);

        let read_only =
            SifsNfsFileSystem::open(Some(owner.data_dir().root()), "/".to_string(), true)
                .expect("open read-only adapter");
        assert_eq!(read_only.capabilities(), VFSCapabilities::ReadOnly);
        assert!(matches!(
            read_only
                .create(
                    &read_only.root_dir(),
                    &filename("denied.txt"),
                    sattr3::default(),
                )
                .await,
            Err(nfsstat3::NFS3ERR_ROFS)
        ));
    }
}
