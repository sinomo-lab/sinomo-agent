use crate::{SandboxFileSystem, SandboxFsError, SandboxMetadata, SandboxPath};
use std::{
    fmt,
    future::Future,
    io::{self, SeekFrom},
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn wasix_sandbox_filesystem_preopen<F>(
    guest_path: SandboxPath,
    fs: Arc<Mutex<F>>,
    root: SandboxPath,
    mode: crate::SandboxMountMode,
) -> crate::SandboxWasiVirtualPreopen
where
    F: SandboxFileSystem + Send + 'static,
{
    crate::SandboxWasiVirtualPreopen {
        guest_path,
        mode: mode.clone(),
        fs: Arc::new(WasixSandboxFileSystem::new(fs, root, mode)),
    }
}

pub(crate) fn wasix_sandbox_workspace_filesystem_preopens<F>(
    fs: Arc<Mutex<F>>,
    mode: crate::SandboxMountMode,
) -> Vec<crate::SandboxWasiVirtualPreopen>
where
    F: SandboxFileSystem + Send + 'static,
{
    let root = SandboxPath::new("/").expect("static sandbox path");
    vec![wasix_sandbox_filesystem_preopen(
        root.clone(),
        fs,
        root,
        mode,
    )]
}

pub(super) struct WasixSandboxFileSystem<F: SandboxFileSystem + Send + 'static> {
    inner: Weak<Mutex<F>>,
    root: SandboxPath,
    mode: crate::SandboxMountMode,
}

impl<F: SandboxFileSystem + Send + 'static> fmt::Debug for WasixSandboxFileSystem<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasixSandboxFileSystem")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl<F: SandboxFileSystem + Send + 'static> WasixSandboxFileSystem<F> {
    pub(super) fn new(
        inner: Arc<Mutex<F>>,
        root: SandboxPath,
        mode: crate::SandboxMountMode,
    ) -> Self {
        Self {
            inner: Arc::downgrade(&inner),
            root,
            mode,
        }
    }

    fn ensure_writable(&self) -> virtual_fs::Result<()> {
        if self.mode == crate::SandboxMountMode::ReadWrite {
            Ok(())
        } else {
            Err(virtual_fs::FsError::PermissionDenied)
        }
    }

    fn inner(&self) -> virtual_fs::Result<Arc<Mutex<F>>> {
        self.inner.upgrade().ok_or(virtual_fs::FsError::IOError)
    }

    fn sandbox_path(&self, path: &Path) -> virtual_fs::Result<SandboxPath> {
        let mut components = Vec::new();
        for component in path.components() {
            match component {
                Component::Prefix(_) => return Err(virtual_fs::FsError::InvalidInput),
                Component::RootDir | Component::CurDir => {}
                Component::ParentDir => {
                    if components.pop().is_none() {
                        return Err(virtual_fs::FsError::PermissionDenied);
                    }
                }
                Component::Normal(value) => {
                    let value = value.to_str().ok_or(virtual_fs::FsError::InvalidInput)?;
                    components.push(value.to_string());
                }
            }
        }
        let mut current = self.root.clone();
        for component in components {
            current = current.join(component).map_err(fs_error_from_path)?;
        }
        Ok(current)
    }

    fn sandbox_path_with_trailing_directory(
        &self,
        path: &Path,
    ) -> virtual_fs::Result<(SandboxPath, bool)> {
        Ok((self.sandbox_path(path)?, path_requires_directory(path)?))
    }

    fn ensure_followed_path_in_preopen_mut(
        &self,
        fs: &mut F,
        path: &SandboxPath,
    ) -> virtual_fs::Result<()> {
        let followed = fs
            .resolve_followed_path_mut(path)
            .map_err(fs_error_from_sandbox)?;
        if sandbox_path_is_within_root(&followed, &self.root) {
            Ok(())
        } else {
            Err(virtual_fs::FsError::PermissionDenied)
        }
    }

    fn ensure_mutation_path_in_preopen_mut(
        &self,
        fs: &mut F,
        path: &SandboxPath,
    ) -> virtual_fs::Result<()> {
        let resolved = fs
            .resolve_mutation_path_mut(path)
            .map_err(fs_error_from_sandbox)?;
        if sandbox_path_is_within_root(&resolved, &self.root) {
            Ok(())
        } else {
            Err(virtual_fs::FsError::PermissionDenied)
        }
    }
}

impl<F: SandboxFileSystem + Send + 'static> virtual_fs::FileOpener for WasixSandboxFileSystem<F> {
    fn open(
        &self,
        path: &Path,
        conf: &virtual_fs::OpenOptionsConfig,
    ) -> virtual_fs::Result<Box<dyn virtual_fs::VirtualFile + Send + Sync + 'static>> {
        let (path, requires_directory) = self.sandbox_path_with_trailing_directory(path)?;
        if conf.write() || conf.append() || conf.truncate() || conf.create() || conf.create_new() {
            self.ensure_writable()?;
        }
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        let truncate = conf.truncate() && !conf.create_new();
        if truncate && !conf.write() {
            return Err(virtual_fs::FsError::PermissionDenied);
        }
        if conf.create_new() {
            self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
            match fs.stat_mut(&path) {
                Ok(_) => return Err(virtual_fs::FsError::AlreadyExists),
                Err(SandboxFsError::NotFound { .. }) => {}
                Err(error) => return Err(fs_error_from_sandbox(error)),
            }
        }
        let stat = fs.stat_follow_mut(&path);
        if requires_directory {
            return match stat {
                Ok(_) => {
                    self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
                    Err(virtual_fs::FsError::BaseNotDirectory)
                }
                Err(SandboxFsError::NotFound { .. }) => Err(virtual_fs::FsError::EntryNotFound),
                Err(error) => Err(fs_error_from_sandbox(error)),
            };
        }
        if let Ok(metadata) = &stat
            && metadata.is_dir
        {
            return Err(virtual_fs::FsError::BaseNotDirectory);
        }
        if stat.is_ok() {
            self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
        }
        if let Ok(metadata) = &stat {
            if conf.read() && !sandbox_mode_allows_read(metadata.mode) {
                return Err(virtual_fs::FsError::PermissionDenied);
            }
            if (conf.write() || conf.append() || truncate)
                && !sandbox_mode_allows_write(metadata.mode)
            {
                return Err(virtual_fs::FsError::PermissionDenied);
            }
        }
        let mut metadata = None;
        let mut created = false;
        let mut data = match stat {
            Ok(found_metadata) => {
                let data = fs
                    .read_unchecked_mut(&path, None)
                    .map_err(fs_error_from_sandbox)?;
                metadata = Some(found_metadata);
                data
            }
            Err(SandboxFsError::NotFound { .. }) if conf.create() || conf.create_new() => {
                created = true;
                Vec::new()
            }
            Err(error) => return Err(fs_error_from_sandbox(error)),
        };
        if truncate {
            data.clear();
        }
        if created || truncate {
            self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
            if created
                && fs
                    .stat_mut(&path)
                    .map(|metadata| metadata.is_symlink)
                    .unwrap_or(false)
            {
                self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
            }
            if truncate && !created {
                fs.set_len_unchecked(&path, 0)
                    .map_err(fs_error_from_sandbox)?;
            } else {
                fs.write_file(&path, &data).map_err(fs_error_from_sandbox)?;
            }
            metadata = Some(fs.stat_follow_mut(&path).map_err(fs_error_from_sandbox)?);
        }
        let cursor = if conf.append() { data.len() as u64 } else { 0 };
        let metadata = metadata.unwrap_or(SandboxMetadata {
            path: path.clone(),
            is_dir: false,
            is_symlink: false,
            len: data.len() as u64,
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            inode: 0,
            object_identity: None,
            accessed_at_unix_nanos: 1_000_000_000,
            modified_at_unix_nanos: 1_000_000_000,
            created_at_unix_nanos: 1_000_000_000,
            changed_at_unix_nanos: 1_000_000_000,
        });
        drop(fs);
        Ok(Box::new(WasixSandboxFile {
            fs: inner,
            path,
            data,
            cursor,
            accessed_at_unix_nanos: metadata.accessed_at_unix_nanos,
            modified_at_unix_nanos: metadata.modified_at_unix_nanos,
            created_at_unix_nanos: metadata.changed_at_unix_nanos,
            inode: metadata.inode,
            link_count: metadata.link_count,
            mode: metadata.mode,
            uid: metadata.uid,
            gid: metadata.gid,
            object_identity: metadata.object_identity,
            read: conf.read(),
            write: conf.write() || conf.append(),
            writable_mount: self.mode == crate::SandboxMountMode::ReadWrite,
            append: conf.append(),
            dirty: false,
            unlinked: false,
        }))
    }
}

impl<F: SandboxFileSystem + Send + 'static> virtual_fs::FileSystem for WasixSandboxFileSystem<F> {
    fn readlink(&self, path: &Path) -> virtual_fs::Result<PathBuf> {
        let (path, requires_directory) = self.sandbox_path_with_trailing_directory(path)?;
        if requires_directory {
            let inner = self.inner()?;
            let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
            fs.stat_follow_mut(&path).map_err(fs_error_from_sandbox)?;
            self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
            return Err(virtual_fs::FsError::NotAFile);
        }
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
        fs.readlink_mut(&path).map_err(fs_error_from_sandbox)
    }

    fn readlink_update_access(&self, path: &Path) -> virtual_fs::Result<PathBuf> {
        let (path, requires_directory) = self.sandbox_path_with_trailing_directory(path)?;
        if requires_directory {
            let inner = self.inner()?;
            let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
            fs.stat_follow_mut(&path).map_err(fs_error_from_sandbox)?;
            self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
            return Err(virtual_fs::FsError::NotAFile);
        }
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
        let result = if self.mode == crate::SandboxMountMode::ReadWrite {
            fs.readlink_update_access(&path)
        } else {
            fs.readlink_mut(&path)
        };
        result.map_err(fs_error_from_sandbox)
    }

    fn read_dir(&self, path: &Path) -> virtual_fs::Result<virtual_fs::ReadDir> {
        let path = self.sandbox_path(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
        let entries = if self.mode == crate::SandboxMountMode::ReadWrite {
            fs.list_update_access(&path)
        } else {
            fs.list_unchecked_mut(&path)
        }
        .map_err(fs_error_from_sandbox)?
        .into_iter()
        .map(|entry| virtual_fs::DirEntry {
            path: PathBuf::from(entry.path.as_str()),
            metadata: Ok(metadata_from_sandbox(&entry.metadata)),
        })
        .collect();
        Ok(virtual_fs::ReadDir::new(entries))
    }

    fn create_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let (path, requires_directory) = self.sandbox_path_with_trailing_directory(path)?;
        let fs = self.inner()?;
        let mut fs = fs.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
        if requires_directory {
            match fs.stat_follow_mut(&path) {
                Ok(metadata) => {
                    self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
                    if metadata.is_dir {
                        return Err(virtual_fs::FsError::AlreadyExists);
                    }
                    return Err(virtual_fs::FsError::BaseNotDirectory);
                }
                Err(SandboxFsError::NotFound { .. }) => {}
                Err(error) => return Err(fs_error_from_sandbox(error)),
            }
        }
        fs.create_dir(&path).map_err(fs_error_from_sandbox)
    }

    fn remove_dir(&self, path: &Path) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let path = self.sandbox_path(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
        fs.remove_dir(&path).map_err(fs_error_from_sandbox)
    }

    fn rename<'a>(
        &'a self,
        from: &'a Path,
        to: &'a Path,
    ) -> Pin<Box<dyn Future<Output = virtual_fs::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.ensure_writable()?;
            let (from, from_requires_directory) =
                self.sandbox_path_with_trailing_directory(from)?;
            let (to, to_requires_directory) = self.sandbox_path_with_trailing_directory(to)?;
            let fs = self.inner()?;
            let mut fs = fs.lock().map_err(|_| virtual_fs::FsError::Lock)?;
            self.ensure_mutation_path_in_preopen_mut(&mut *fs, &from)?;
            self.ensure_mutation_path_in_preopen_mut(&mut *fs, &to)?;
            if from_requires_directory {
                let metadata = fs.stat_follow_mut(&from).map_err(fs_error_from_sandbox)?;
                self.ensure_followed_path_in_preopen_mut(&mut *fs, &from)?;
                if !metadata.is_dir {
                    return Err(virtual_fs::FsError::BaseNotDirectory);
                }
            }
            if to_requires_directory {
                let source_metadata = fs.stat_mut(&from).map_err(fs_error_from_sandbox)?;
                if !source_metadata.is_dir {
                    return Err(virtual_fs::FsError::BaseNotDirectory);
                }
                match fs.stat_follow_mut(&to) {
                    Ok(metadata) => {
                        self.ensure_followed_path_in_preopen_mut(&mut *fs, &to)?;
                        if !metadata.is_dir {
                            return Err(virtual_fs::FsError::BaseNotDirectory);
                        }
                    }
                    Err(SandboxFsError::NotFound { .. }) => {
                        return Err(virtual_fs::FsError::EntryNotFound);
                    }
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            }
            fs.rename(&from, &to).map_err(fs_error_from_sandbox)
        })
    }

    fn metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        let (path, requires_directory) = self.sandbox_path_with_trailing_directory(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        let metadata = fs.stat_follow_mut(&path).map_err(fs_error_from_sandbox)?;
        self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
        if requires_directory && !metadata.is_dir {
            return Err(virtual_fs::FsError::BaseNotDirectory);
        }
        Ok(metadata_from_sandbox(&metadata))
    }

    fn symlink_metadata(&self, path: &Path) -> virtual_fs::Result<virtual_fs::Metadata> {
        let (path, requires_directory) = self.sandbox_path_with_trailing_directory(path)?;
        let fs = self.inner()?;
        let mut fs = fs.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        let metadata = if requires_directory {
            let metadata = fs.stat_follow_mut(&path).map_err(fs_error_from_sandbox)?;
            self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
            metadata
        } else {
            self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
            fs.stat_mut(&path).map_err(fs_error_from_sandbox)?
        };
        Ok(metadata_from_sandbox(&metadata))
    }

    fn remove_file(&self, path: &Path) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let (path, requires_directory) = self.sandbox_path_with_trailing_directory(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
        if requires_directory {
            fs.stat_follow_mut(&path).map_err(fs_error_from_sandbox)?;
            self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
            return Err(virtual_fs::FsError::NotAFile);
        }
        fs.unlink(&path).map_err(fs_error_from_sandbox)
    }

    fn symlink(&self, target: &Path, link_path: &Path) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let (link_path, requires_directory) =
            self.sandbox_path_with_trailing_directory(link_path)?;
        if requires_directory {
            return Err(virtual_fs::FsError::BaseNotDirectory);
        }
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &link_path)?;
        fs.symlink(target, &link_path)
            .map_err(fs_error_from_sandbox)
    }

    fn hard_link(&self, old_path: &Path, new_path: &Path) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let (old_path, old_requires_directory) =
            self.sandbox_path_with_trailing_directory(old_path)?;
        let (new_path, new_requires_directory) =
            self.sandbox_path_with_trailing_directory(new_path)?;
        let fs = self.inner()?;
        let mut fs = fs.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &old_path)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &new_path)?;
        if old_requires_directory {
            let metadata = fs
                .stat_follow_mut(&old_path)
                .map_err(fs_error_from_sandbox)?;
            self.ensure_followed_path_in_preopen_mut(&mut *fs, &old_path)?;
            if !metadata.is_dir {
                return Err(virtual_fs::FsError::BaseNotDirectory);
            }
        }
        if new_requires_directory {
            return match fs.stat_follow_mut(&new_path) {
                Ok(metadata) => {
                    self.ensure_followed_path_in_preopen_mut(&mut *fs, &new_path)?;
                    if metadata.is_dir {
                        Err(virtual_fs::FsError::AlreadyExists)
                    } else {
                        Err(virtual_fs::FsError::BaseNotDirectory)
                    }
                }
                Err(SandboxFsError::NotFound { .. }) => Err(virtual_fs::FsError::EntryNotFound),
                Err(error) => Err(fs_error_from_sandbox(error)),
            };
        }
        fs.hard_link(&old_path, &new_path)
            .map_err(fs_error_from_sandbox)
    }

    fn set_mode(&self, path: &Path, mode: u32) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let path = self.sandbox_path(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
        fs.set_mode(&path, mode).map_err(fs_error_from_sandbox)
    }

    fn set_owner(&self, path: &Path, uid: u32, gid: u32) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let path = self.sandbox_path(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
        fs.set_owner(&path, uid, gid).map_err(fs_error_from_sandbox)
    }

    fn set_times(
        &self,
        path: &Path,
        atime: Option<u64>,
        mtime: Option<u64>,
    ) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let path = self.sandbox_path(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_followed_path_in_preopen_mut(&mut *fs, &path)?;
        fs.set_times(&path, atime, mtime)
            .map_err(fs_error_from_sandbox)
    }

    fn set_symlink_times(
        &self,
        path: &Path,
        atime: Option<u64>,
        mtime: Option<u64>,
    ) -> virtual_fs::Result<()> {
        self.ensure_writable()?;
        let path = self.sandbox_path(path)?;
        let inner = self.inner()?;
        let mut fs = inner.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        self.ensure_mutation_path_in_preopen_mut(&mut *fs, &path)?;
        fs.set_symlink_times(&path, atime, mtime)
            .map_err(fs_error_from_sandbox)
    }

    fn new_open_options(&self) -> virtual_fs::OpenOptions<'_> {
        virtual_fs::OpenOptions::new(self)
    }

    fn mount(
        &self,
        _name: String,
        _path: &Path,
        _fs: Box<dyn virtual_fs::FileSystem + Send + Sync>,
    ) -> virtual_fs::Result<()> {
        Err(virtual_fs::FsError::Unsupported)
    }
}

struct WasixSandboxFile<F: SandboxFileSystem + Send + 'static> {
    fs: Arc<Mutex<F>>,
    path: SandboxPath,
    data: Vec<u8>,
    cursor: u64,
    accessed_at_unix_nanos: u64,
    modified_at_unix_nanos: u64,
    created_at_unix_nanos: u64,
    inode: u64,
    link_count: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    object_identity: Option<String>,
    read: bool,
    write: bool,
    writable_mount: bool,
    append: bool,
    dirty: bool,
    unlinked: bool,
}

impl<F: SandboxFileSystem + Send + 'static> fmt::Debug for WasixSandboxFile<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasixSandboxFile")
            .field("path", &self.path)
            .field("read", &self.read)
            .field("write", &self.write)
            .field("append", &self.append)
            .finish_non_exhaustive()
    }
}

impl<F: SandboxFileSystem + Send + 'static> WasixSandboxFile<F> {
    fn refresh_handle_metadata(&mut self, metadata: &SandboxMetadata) {
        self.link_count = metadata.link_count;
        self.accessed_at_unix_nanos = metadata.accessed_at_unix_nanos;
        self.modified_at_unix_nanos = metadata.modified_at_unix_nanos;
        self.created_at_unix_nanos = metadata.changed_at_unix_nanos;
        self.mode = metadata.mode;
        self.uid = metadata.uid;
        self.gid = metadata.gid;
    }

    fn mark_detached(&mut self) {
        self.unlinked = true;
        self.link_count = 0;
    }

    fn apply_detached_mode(&mut self, mode: u32) {
        self.mode = mode;
        self.created_at_unix_nanos = current_unix_nanos();
    }

    fn apply_detached_owner(&mut self, uid: u32, gid: u32) {
        self.uid = uid;
        self.gid = gid;
        self.created_at_unix_nanos = current_unix_nanos();
    }

    fn apply_detached_times(&mut self, atime: Option<u64>, mtime: Option<u64>) {
        if let Some(atime) = atime {
            self.accessed_at_unix_nanos = atime;
        }
        if let Some(mtime) = mtime {
            self.modified_at_unix_nanos = mtime;
        }
        self.created_at_unix_nanos = current_unix_nanos();
    }

    fn refresh_metadata_by_identity(&mut self) -> virtual_fs::Result<bool> {
        let fs_handle = Arc::clone(&self.fs);
        let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        let resolved = match fs.stat_follow_unchecked_mut(&self.path) {
            Ok(metadata) if metadata.inode == self.inode => Some((self.path.clone(), metadata)),
            Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                    Ok(Some(path)) => {
                        let metadata = fs
                            .stat_follow_unchecked_mut(&path)
                            .map_err(fs_error_from_sandbox)?;
                        Some((path, metadata))
                    }
                    Ok(None) => None,
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            }
            Err(error) => return Err(fs_error_from_sandbox(error)),
        };
        drop(fs);
        if let Some((path, metadata)) = resolved {
            self.path = path;
            self.refresh_handle_metadata(&metadata);
            self.unlinked = false;
            Ok(true)
        } else {
            self.unlinked = true;
            self.link_count = 0;
            Ok(false)
        }
    }

    fn refresh_from_fs(&mut self, update_access: bool) -> virtual_fs::Result<()> {
        if self.unlinked {
            return Ok(());
        }
        let fs_handle = Arc::clone(&self.fs);
        let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        let mut metadata = match fs.stat_follow_unchecked_mut(&self.path) {
            Ok(metadata) if metadata.inode == self.inode => metadata,
            Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                    Ok(Some(path)) => {
                        self.path = path;
                        fs.stat_follow_unchecked_mut(&self.path)
                            .map_err(fs_error_from_sandbox)?
                    }
                    Ok(None) => {
                        self.unlinked = true;
                        self.link_count = 0;
                        return Ok(());
                    }
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            }
            Err(error) => return Err(fs_error_from_sandbox(error)),
        };
        let result = if update_access {
            fs.read_unchecked_update_access(&self.path, None)
        } else {
            fs.read_unchecked_mut(&self.path, None)
        };
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(SandboxFsError::NotFound { .. }) => {
                self.unlinked = true;
                self.link_count = 0;
                return Ok(());
            }
            Err(error) => return Err(fs_error_from_sandbox(error)),
        };
        if update_access {
            metadata = fs
                .stat_follow_unchecked_mut(&self.path)
                .map_err(fs_error_from_sandbox)?;
        }
        self.refresh_handle_metadata(&metadata);
        self.data = bytes;
        Ok(())
    }

    fn persist(&mut self) -> virtual_fs::Result<()> {
        if !self.dirty || self.unlinked {
            return Ok(());
        }
        self.fs
            .lock()
            .map_err(|_| virtual_fs::FsError::Lock)?
            .write_file_unchecked(&self.path, &self.data)
            .map_err(fs_error_from_sandbox)?;
        self.dirty = false;
        Ok(())
    }

    fn set_len_atomically(&mut self, new_size: u64) -> virtual_fs::Result<()> {
        let new_size = usize::try_from(new_size).map_err(|_| virtual_fs::FsError::StorageFull)?;
        if self.unlinked {
            self.data.resize(new_size, 0);
            return Ok(());
        }
        let fs_handle = Arc::clone(&self.fs);
        let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        match fs.stat_follow_unchecked_mut(&self.path) {
            Ok(metadata) if metadata.inode == self.inode => {}
            Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                    Ok(Some(path)) => {
                        self.path = path;
                        fs.stat_follow_unchecked_mut(&self.path)
                            .map_err(fs_error_from_sandbox)?;
                    }
                    Ok(None) => {
                        self.unlinked = true;
                        self.link_count = 0;
                        self.data.resize(new_size, 0);
                        return Ok(());
                    }
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            }
            Err(error) => return Err(fs_error_from_sandbox(error)),
        }
        self.data.resize(new_size, 0);
        let metadata = fs
            .set_len_unchecked(&self.path, new_size as u64)
            .map_err(fs_error_from_sandbox)?;
        self.refresh_handle_metadata(&metadata);
        self.dirty = false;
        Ok(())
    }

    fn write_atomically(&mut self, buf: &[u8]) -> virtual_fs::Result<usize> {
        if self.unlinked {
            self.write_detached(buf)?;
            return Ok(buf.len());
        }
        let fs_handle = Arc::clone(&self.fs);
        let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        let metadata = match fs.stat_follow_unchecked_mut(&self.path) {
            Ok(metadata) if metadata.inode == self.inode => metadata,
            Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                    Ok(Some(path)) => {
                        self.path = path;
                        fs.stat_follow_unchecked_mut(&self.path)
                            .map_err(fs_error_from_sandbox)?
                    }
                    Ok(None) => {
                        self.unlinked = true;
                        self.link_count = 0;
                        drop(fs);
                        self.write_detached(buf)?;
                        return Ok(buf.len());
                    }
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            }
            Err(error) => return Err(fs_error_from_sandbox(error)),
        };
        self.link_count = metadata.link_count;
        if self.append {
            self.cursor = metadata.len;
        }
        let offset = self.cursor;
        self.write_buffer(buf)?;
        let metadata = fs
            .write_at_unchecked(&self.path, offset, buf)
            .map_err(fs_error_from_sandbox)?;
        self.refresh_handle_metadata(&metadata);
        self.dirty = false;
        Ok(buf.len())
    }

    fn write_detached(&mut self, buf: &[u8]) -> virtual_fs::Result<()> {
        if self.append {
            self.cursor = self.data.len() as u64;
        }
        self.write_buffer(buf)
    }

    fn write_buffer(&mut self, buf: &[u8]) -> virtual_fs::Result<()> {
        let position =
            usize::try_from(self.cursor).map_err(|_| virtual_fs::FsError::StorageFull)?;
        let end = position
            .checked_add(buf.len())
            .ok_or(virtual_fs::FsError::StorageFull)?;
        if end > self.data.len() {
            self.data.resize(end, 0);
        }
        self.data[position..end].copy_from_slice(buf);
        self.cursor = end as u64;
        self.dirty = self.unlinked;
        Ok(())
    }
}

impl<F: SandboxFileSystem + Send + 'static> Drop for WasixSandboxFile<F> {
    fn drop(&mut self) {
        let _ = self.persist();
    }
}

impl<F: SandboxFileSystem + Send + 'static> virtual_fs::VirtualFile for WasixSandboxFile<F> {
    fn last_accessed(&self) -> u64 {
        self.accessed_at_unix_nanos
    }

    fn last_modified(&self) -> u64 {
        self.modified_at_unix_nanos
    }

    fn created_time(&self) -> u64 {
        self.created_at_unix_nanos
    }

    fn inode(&self) -> Option<u64> {
        (self.inode != 0).then_some(self.inode)
    }

    fn link_count(&self) -> Option<u64> {
        Some(self.link_count)
    }

    fn refresh_metadata(&mut self) -> virtual_fs::Result<()> {
        self.refresh_from_fs(false)
    }

    fn note_link_added(&mut self) -> virtual_fs::Result<()> {
        if !self.refresh_metadata_by_identity()? {
            self.link_count = self.link_count.saturating_add(1);
            self.unlinked = false;
        }
        Ok(())
    }

    fn note_link_removed(&mut self) -> virtual_fs::Result<()> {
        if !self.refresh_metadata_by_identity()? {
            self.link_count = self.link_count.saturating_sub(1);
            if self.link_count == 0 {
                self.mark_detached();
            }
        }
        Ok(())
    }

    fn set_mode(&mut self, mode: u32) -> virtual_fs::Result<()> {
        if !self.writable_mount {
            return Err(virtual_fs::FsError::PermissionDenied);
        }
        if self.unlinked {
            self.apply_detached_mode(mode);
        } else {
            let fs_handle = Arc::clone(&self.fs);
            let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
            let resolved_path = match fs.stat_follow_unchecked_mut(&self.path) {
                Ok(metadata) if metadata.inode == self.inode => Some((self.path.clone(), metadata)),
                Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                    match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                        Ok(Some(path)) => {
                            let metadata = fs
                                .stat_follow_unchecked_mut(&path)
                                .map_err(fs_error_from_sandbox)?;
                            Some((path, metadata))
                        }
                        Ok(None) => None,
                        Err(error) => return Err(fs_error_from_sandbox(error)),
                    }
                }
                Err(error) => return Err(fs_error_from_sandbox(error)),
            };
            if let Some((path, _metadata)) = resolved_path {
                match fs.set_mode(&path, mode) {
                    Ok(()) => {
                        let metadata = fs
                            .stat_follow_unchecked_mut(&path)
                            .map_err(fs_error_from_sandbox)?;
                        self.path = path;
                        self.refresh_handle_metadata(&metadata);
                    }
                    Err(SandboxFsError::NotFound { .. }) => {
                        self.mark_detached();
                        self.apply_detached_mode(mode);
                    }
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            } else {
                self.mark_detached();
                self.apply_detached_mode(mode);
            }
        }
        Ok(())
    }

    fn set_owner(&mut self, uid: u32, gid: u32) -> virtual_fs::Result<()> {
        if !self.writable_mount {
            return Err(virtual_fs::FsError::PermissionDenied);
        }
        if self.unlinked {
            self.apply_detached_owner(uid, gid);
        } else {
            let fs_handle = Arc::clone(&self.fs);
            let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
            let resolved_path = match fs.stat_follow_unchecked_mut(&self.path) {
                Ok(metadata) if metadata.inode == self.inode => Some((self.path.clone(), metadata)),
                Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                    match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                        Ok(Some(path)) => {
                            let metadata = fs
                                .stat_follow_unchecked_mut(&path)
                                .map_err(fs_error_from_sandbox)?;
                            Some((path, metadata))
                        }
                        Ok(None) => None,
                        Err(error) => return Err(fs_error_from_sandbox(error)),
                    }
                }
                Err(error) => return Err(fs_error_from_sandbox(error)),
            };
            if let Some((path, _metadata)) = resolved_path {
                match fs.set_owner(&path, uid, gid) {
                    Ok(()) => {
                        let metadata = fs
                            .stat_follow_unchecked_mut(&path)
                            .map_err(fs_error_from_sandbox)?;
                        self.path = path;
                        self.refresh_handle_metadata(&metadata);
                    }
                    Err(SandboxFsError::NotFound { .. }) => {
                        self.mark_detached();
                        self.apply_detached_owner(uid, gid);
                    }
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            } else {
                self.mark_detached();
                self.apply_detached_owner(uid, gid);
            }
        }
        Ok(())
    }

    fn set_times(&mut self, atime: Option<u64>, mtime: Option<u64>) -> virtual_fs::Result<()> {
        if !self.writable_mount {
            return Err(virtual_fs::FsError::PermissionDenied);
        }
        if self.unlinked {
            self.apply_detached_times(atime, mtime);
        } else {
            let fs_handle = Arc::clone(&self.fs);
            let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
            let resolved_path = match fs.stat_follow_unchecked_mut(&self.path) {
                Ok(metadata) if metadata.inode == self.inode => Some((self.path.clone(), metadata)),
                Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                    match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                        Ok(Some(path)) => {
                            let metadata = fs
                                .stat_follow_unchecked_mut(&path)
                                .map_err(fs_error_from_sandbox)?;
                            Some((path, metadata))
                        }
                        Ok(None) => None,
                        Err(error) => return Err(fs_error_from_sandbox(error)),
                    }
                }
                Err(error) => return Err(fs_error_from_sandbox(error)),
            };
            if let Some((path, _metadata)) = resolved_path {
                match fs.set_times(&path, atime, mtime) {
                    Ok(()) => {
                        let metadata = fs
                            .stat_follow_unchecked_mut(&path)
                            .map_err(fs_error_from_sandbox)?;
                        self.path = path;
                        self.refresh_handle_metadata(&metadata);
                    }
                    Err(SandboxFsError::NotFound { .. }) => {
                        self.mark_detached();
                        self.apply_detached_times(atime, mtime);
                    }
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            } else {
                self.mark_detached();
                self.apply_detached_times(atime, mtime);
            }
        }
        Ok(())
    }

    fn size(&self) -> u64 {
        self.data.len() as u64
    }

    fn set_len(&mut self, new_size: u64) -> virtual_fs::Result<()> {
        if !self.write || !self.writable_mount {
            return Err(virtual_fs::FsError::PermissionDenied);
        }
        self.set_len_atomically(new_size)
    }

    fn unlink(&mut self) -> virtual_fs::Result<()> {
        if !self.writable_mount {
            return Err(virtual_fs::FsError::PermissionDenied);
        }
        if self.unlinked {
            return Ok(());
        }
        let fs_handle = Arc::clone(&self.fs);
        let mut fs = fs_handle.lock().map_err(|_| virtual_fs::FsError::Lock)?;
        let resolved_path = match fs.stat_follow_unchecked_mut(&self.path) {
            Ok(metadata) if metadata.inode == self.inode => Some(self.path.clone()),
            Ok(_) | Err(SandboxFsError::NotFound { .. }) => {
                match fs.path_for_open_identity(self.object_identity.as_deref(), self.inode) {
                    Ok(path) => path,
                    Err(error) => return Err(fs_error_from_sandbox(error)),
                }
            }
            Err(error) => return Err(fs_error_from_sandbox(error)),
        };
        if let Some(path) = resolved_path {
            match fs.unlink(&path) {
                Ok(()) | Err(SandboxFsError::NotFound { .. }) => {}
                Err(error) => return Err(fs_error_from_sandbox(error)),
            }
        }
        self.mark_detached();
        Ok(())
    }

    fn poll_read_ready(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        if !self.read {
            return Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()));
        }
        if let Err(error) = self.refresh_from_fs(false) {
            return Poll::Ready(Err(io::Error::from(error)));
        }
        let position = usize::try_from(self.cursor).unwrap_or(usize::MAX);
        Poll::Ready(Ok(self.data.len().saturating_sub(position)))
    }

    fn poll_write_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        if self.write && self.writable_mount {
            Poll::Ready(Ok(usize::MAX))
        } else {
            Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()))
        }
    }
}

impl<F: SandboxFileSystem + Send + 'static> virtual_fs::AsyncRead for WasixSandboxFile<F> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut virtual_fs::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.read {
            return Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()));
        }
        let update_access = self.writable_mount;
        if let Err(error) = self.refresh_from_fs(update_access) {
            return Poll::Ready(Err(io::Error::from(error)));
        }
        let position = usize::try_from(self.cursor).unwrap_or(usize::MAX);
        if position >= self.data.len() {
            return Poll::Ready(Ok(()));
        }
        let len = self.data[position..].len().min(buf.remaining());
        buf.put_slice(&self.data[position..position + len]);
        self.cursor = self.cursor.saturating_add(len as u64);
        Poll::Ready(Ok(()))
    }
}

impl<F: SandboxFileSystem + Send + 'static> virtual_fs::AsyncWrite for WasixSandboxFile<F> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.write {
            return Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()));
        }
        if !self.writable_mount {
            return Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()));
        }
        match self.write_atomically(buf) {
            Ok(written) => Poll::Ready(Ok(written)),
            Err(error) => Poll::Ready(Err(io::Error::from(error))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.persist().map_err(io::Error::from))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl<F: SandboxFileSystem + Send + 'static> virtual_fs::AsyncSeek for WasixSandboxFile<F> {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        if matches!(position, SeekFrom::End(_))
            && let Err(error) = self.refresh_from_fs(false)
        {
            return Err(io::Error::from(error));
        }
        let next = match position {
            SeekFrom::Start(position) => position,
            SeekFrom::End(offset) => seek_with_offset(self.data.len() as u64, offset)?,
            SeekFrom::Current(offset) => seek_with_offset(self.cursor, offset)?,
        };
        self.cursor = next;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.cursor))
    }
}

fn seek_with_offset(base: u64, offset: i64) -> io::Result<u64> {
    if offset >= 0 {
        base.checked_add(offset as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek overflow"))
    } else {
        base.checked_sub(offset.unsigned_abs())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek before start"))
    }
}

fn current_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn path_requires_directory(path: &Path) -> virtual_fs::Result<bool> {
    let path = path.to_str().ok_or(virtual_fs::FsError::InvalidInput)?;
    Ok(path.len() > 1 && path.ends_with('/'))
}

fn sandbox_path_is_within_root(path: &SandboxPath, root: &SandboxPath) -> bool {
    if root.as_str() == "/" || path == root {
        return true;
    }
    path.as_str()
        .strip_prefix(root.as_str().trim_end_matches('/'))
        .is_some_and(|tail| tail.starts_with('/'))
}

fn metadata_from_sandbox(metadata: &SandboxMetadata) -> virtual_fs::Metadata {
    virtual_fs::Metadata {
        ft: if metadata.is_symlink {
            virtual_fs::FileType {
                symlink: true,
                ..Default::default()
            }
        } else if metadata.is_dir {
            virtual_fs::FileType::new_dir()
        } else {
            virtual_fs::FileType::new_file()
        },
        dev: 1,
        ino: if metadata.inode == 0 {
            sandbox_inode(&metadata.path)
        } else {
            metadata.inode
        },
        nlink: metadata.link_count.max(1),
        mode: metadata.mode,
        uid: metadata.uid,
        gid: metadata.gid,
        accessed: metadata.accessed_at_unix_nanos,
        created: metadata.changed_at_unix_nanos,
        modified: metadata.modified_at_unix_nanos,
        len: metadata.len,
    }
}

fn sandbox_mode_allows_read(mode: u32) -> bool {
    mode & 0o444 != 0
}

fn sandbox_mode_allows_write(mode: u32) -> bool {
    mode & 0o222 != 0
}

fn sandbox_inode(path: &SandboxPath) -> u64 {
    let hash = blake3::hash(path.as_str().as_bytes());
    u64::from_le_bytes(hash.as_bytes()[0..8].try_into().expect("hash slice"))
}

fn fs_error_from_path(_error: crate::SandboxPathError) -> virtual_fs::FsError {
    virtual_fs::FsError::InvalidInput
}

fn fs_error_from_sandbox(error: SandboxFsError) -> virtual_fs::FsError {
    match error {
        SandboxFsError::Path(_) => virtual_fs::FsError::InvalidInput,
        SandboxFsError::NotFound { .. } => virtual_fs::FsError::EntryNotFound,
        SandboxFsError::AlreadyExists { .. } => virtual_fs::FsError::AlreadyExists,
        SandboxFsError::ReadOnly { .. } => virtual_fs::FsError::PermissionDenied,
        SandboxFsError::NotDirectory { .. } => virtual_fs::FsError::BaseNotDirectory,
        SandboxFsError::IsDirectory { .. } => virtual_fs::FsError::NotAFile,
        SandboxFsError::NotSymlink { .. } => virtual_fs::FsError::NotAFile,
        SandboxFsError::DirectoryNotEmpty { .. } => virtual_fs::FsError::DirectoryNotEmpty,
        SandboxFsError::Unsupported { .. } => virtual_fs::FsError::Unsupported,
        SandboxFsError::QuotaExceeded { .. } => virtual_fs::FsError::StorageFull,
        SandboxFsError::Unavailable { .. } => virtual_fs::FsError::IOError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LocalSandboxFileSystem, LocalSandboxInstanceLayout, SandboxId, SandboxInstanceManifest,
        SandboxMount, SandboxToolchainId, SifsSandboxFileSystem, SifsSandboxInstanceStore,
    };
    use sinomo_dfs::{DfsWriteOptions, LocalContentStore, NetworkedDfsService};
    use sinomo_network::InProcessNetwork;
    use sinomo_node::NodeId;
    use time::OffsetDateTime;

    fn test_layout() -> (sinomo_testdir::TestDir, LocalSandboxInstanceLayout) {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs");
        let layout = LocalSandboxInstanceLayout::new(&root, &SandboxId::generate());
        for dir in layout.required_dirs() {
            std::fs::create_dir_all(dir).unwrap();
        }
        (root, layout)
    }

    fn sifs_wasix_fs(
        name: &str,
    ) -> (
        sinomo_testdir::TestDir,
        Arc<Mutex<SifsSandboxFileSystem<sinomo_network::InProcessTransport>>>,
        WasixSandboxFileSystem<SifsSandboxFileSystem<sinomo_network::InProcessTransport>>,
    ) {
        sifs_wasix_fs_with_mode(name, crate::SandboxMountMode::ReadWrite)
    }

    fn sifs_wasix_fs_with_mode(
        name: &str,
        mode: crate::SandboxMountMode,
    ) -> (
        sinomo_testdir::TestDir,
        Arc<Mutex<SifsSandboxFileSystem<sinomo_network::InProcessTransport>>>,
        WasixSandboxFileSystem<SifsSandboxFileSystem<sinomo_network::InProcessTransport>>,
    ) {
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named(format!("node_{name}")).unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut store = SifsSandboxInstanceStore::new(dfs);
        let manifest = store
            .create(
                sandbox_id,
                SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
                OffsetDateTime::now_utc(),
            )
            .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs =
            WasixSandboxFileSystem::new(Arc::clone(&sifs_fs), SandboxPath::new("/").unwrap(), mode);
        (root, sifs_fs, wasix_fs)
    }

    #[test]
    fn wasix_sandbox_filesystem_root_preopen_lists_nested_sifs_mount_ancestors() {
        let name = "wasix_nested_sifs_mount";
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named(format!("node_{name}")).unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let shared_dir = "/services/shared/wasix-nested-readonly";
        dfs.posix_mkdir_all(shared_dir).unwrap();
        dfs.posix_write_file(
            &format!("{shared_dir}/source.txt"),
            b"mounted",
            DfsWriteOptions::replicated_eventual(vec![dfs.node_id().clone()]),
        )
        .unwrap();
        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut manifest = SandboxInstanceManifest::new(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/opt/pkg").unwrap(),
            source_sifs_path: shared_dir.to_string(),
            mode: crate::SandboxMountMode::ReadOnly,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let root_entries = virtual_fs::FileSystem::read_dir(&wasix_fs, "/".as_ref())
            .expect("root read_dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("root entries");
        assert!(
            root_entries
                .iter()
                .any(|entry| entry.path == PathBuf::from("/opt")),
            "root preopen should expose the synthetic nested mount ancestor: {root_entries:?}"
        );
        let opt_metadata =
            virtual_fs::FileSystem::metadata(&wasix_fs, "/opt".as_ref()).expect("opt metadata");
        assert!(opt_metadata.ft.is_dir());
        assert_eq!(opt_metadata.mode & 0o222, 0);
        let opt_entries = virtual_fs::FileSystem::read_dir(&wasix_fs, "/opt".as_ref())
            .expect("opt read_dir")
            .collect::<Result<Vec<_>, _>>()
            .expect("opt entries");
        assert!(
            opt_entries
                .iter()
                .any(|entry| entry.path == PathBuf::from("/opt/pkg")),
            "root preopen should expose the nested SIFS mount point: {opt_entries:?}"
        );
        assert_eq!(
            virtual_fs::FileSystem::create_dir(&wasix_fs, "/opt/new".as_ref())
                .expect_err("synthetic mount ancestor must be read-only"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::create_dir(&wasix_fs, "/opt".as_ref())
                .expect_err("synthetic mount ancestor itself must be read-only"),
            virtual_fs::FsError::PermissionDenied
        );
        assert!(matches!(
            virtual_fs::FileSystem::new_open_options(&wasix_fs)
                .create(true)
                .write(true)
                .open("/opt/new/file.txt"),
            Err(virtual_fs::FsError::PermissionDenied | virtual_fs::FsError::EntryNotFound)
        ));
        assert!(
            virtual_fs::FileSystem::metadata(&wasix_fs, "/opt/new".as_ref()).is_err(),
            "open create must not create parents through synthetic mount ancestor"
        );

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/opt/pkg/source.txt")
            .expect("open nested mounted file");
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .unwrap();
        assert_eq!(bytes, b"mounted");
    }

    #[test]
    fn wasix_sandbox_filesystem_metadata_fetches_remote_intermediate_symlink() {
        let name = "wasix_remote_intermediate_symlink_metadata";
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let owner = NodeId::from_legacy_named(format!("node_{name}_owner")).unwrap();
        let reader = NodeId::from_legacy_named(format!("node_{name}_reader")).unwrap();
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).unwrap(),
        );
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader.clone()),
            LocalContentStore::new(root.join("reader-content")).unwrap(),
        );
        owner_dfs.bootstrap_distributed_sifs().unwrap();
        reader_dfs.bootstrap_distributed_sifs().unwrap();
        owner_dfs.posix_mkdir_all("/services/shared").unwrap();
        let file_stat = owner_dfs
            .posix_write_file(
                "/services/shared/file.txt",
                b"remote",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        let link_stat = owner_dfs
            .posix_symlink(
                "shared",
                "/services/shared-link",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        for object in [
            owner_dfs.posix_lstat("/services/shared").unwrap().object_id,
            file_stat.object_id.clone(),
            link_stat.object_id.clone(),
        ] {
            let metadata = owner_dfs.metadata().object(&object).unwrap().clone();
            reader_dfs.add_object_metadata(metadata).unwrap();
        }

        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut manifest = SandboxInstanceManifest::new(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/shared").unwrap(),
            source_sifs_path: "/services".to_string(),
            mode: crate::SandboxMountMode::ReadWrite,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let owner_dfs = Arc::new(Mutex::new(owner_dfs));
        let owner_worker = {
            let owner_dfs = Arc::clone(&owner_dfs);
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    let events = owner_dfs.lock().unwrap().process_incoming().unwrap();
                    if events.iter().any(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed {
                                object_id,
                                result: sinomo_dfs::DfsReadResult::Found(_),
                                ..
                            } if object_id == &link_stat.object_id
                        )
                    }) {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "owner did not serve remote symlink target"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        };

        let metadata =
            virtual_fs::FileSystem::metadata(&wasix_fs, "/shared/shared-link/file.txt".as_ref())
                .expect("metadata follows remote intermediate symlink");
        owner_worker.join().unwrap();
        assert_eq!(metadata.len(), b"remote".len() as u64);
        assert!(metadata.is_file());
        assert_eq!(
            virtual_fs::FileSystem::readlink(&wasix_fs, "/shared/shared-link".as_ref())
                .expect("cached remote symlink readlink"),
            PathBuf::from("shared")
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_hard_link_fetches_remote_intermediate_symlink() {
        let name = "wasix_remote_intermediate_symlink_hardlink";
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let owner = NodeId::from_legacy_named(format!("node_{name}_owner")).unwrap();
        let reader = NodeId::from_legacy_named(format!("node_{name}_reader")).unwrap();
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).unwrap(),
        );
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader.clone()),
            LocalContentStore::new(root.join("reader-content")).unwrap(),
        );
        owner_dfs.bootstrap_distributed_sifs().unwrap();
        reader_dfs.bootstrap_distributed_sifs().unwrap();
        owner_dfs.posix_mkdir_all("/services/shared").unwrap();
        let file_stat = owner_dfs
            .posix_write_file(
                "/services/shared/file.txt",
                b"remote",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        let link_stat = owner_dfs
            .posix_symlink(
                "shared",
                "/services/shared-link",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        for object in [
            owner_dfs.posix_lstat("/services/shared").unwrap().object_id,
            file_stat.object_id.clone(),
            link_stat.object_id.clone(),
        ] {
            let metadata = owner_dfs.metadata().object(&object).unwrap().clone();
            reader_dfs.add_object_metadata(metadata).unwrap();
        }

        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut manifest = SandboxInstanceManifest::new(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/shared").unwrap(),
            source_sifs_path: "/services".to_string(),
            mode: crate::SandboxMountMode::ReadWrite,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let owner_dfs = Arc::new(Mutex::new(owner_dfs));
        let owner_worker = {
            let owner_dfs = Arc::clone(&owner_dfs);
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    let events = owner_dfs.lock().unwrap().process_incoming().unwrap();
                    if events.iter().any(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed {
                                object_id,
                                result: sinomo_dfs::DfsReadResult::Found(_),
                                ..
                            } if object_id == &link_stat.object_id
                        )
                    }) {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "owner did not serve remote symlink target"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        };

        virtual_fs::FileSystem::hard_link(
            &wasix_fs,
            "/shared/shared-link/file.txt".as_ref(),
            "/shared/alias.txt".as_ref(),
        )
        .expect("hard link follows remote intermediate symlink");
        owner_worker.join().unwrap();

        let alias_metadata =
            virtual_fs::FileSystem::metadata(&wasix_fs, "/shared/alias.txt".as_ref())
                .expect("alias metadata");
        let original_metadata =
            virtual_fs::FileSystem::metadata(&wasix_fs, "/shared/shared/file.txt".as_ref())
                .expect("original metadata");
        assert_eq!(alias_metadata.ino, original_metadata.ino);
        assert_eq!(alias_metadata.nlink, 2);
    }

    #[test]
    fn sifs_sandbox_filesystem_write_at_fetches_remote_intermediate_symlink() {
        let name = "sifs_remote_intermediate_symlink_write_at";
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let owner = NodeId::from_legacy_named(format!("node_{name}_owner")).unwrap();
        let reader = NodeId::from_legacy_named(format!("node_{name}_reader")).unwrap();
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).unwrap(),
        );
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader.clone()),
            LocalContentStore::new(root.join("reader-content")).unwrap(),
        );
        owner_dfs.bootstrap_distributed_sifs().unwrap();
        reader_dfs.bootstrap_distributed_sifs().unwrap();
        owner_dfs.posix_mkdir_all("/services/shared").unwrap();
        let file_stat = owner_dfs
            .posix_write_file(
                "/services/shared/file.txt",
                b"remote",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        let link_stat = owner_dfs
            .posix_symlink(
                "shared",
                "/services/shared-link",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        for object in [
            owner_dfs.posix_lstat("/services/shared").unwrap().object_id,
            file_stat.object_id.clone(),
            link_stat.object_id.clone(),
        ] {
            let metadata = owner_dfs.metadata().object(&object).unwrap().clone();
            reader_dfs.add_object_metadata(metadata).unwrap();
        }

        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut manifest = SandboxInstanceManifest::new(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/shared").unwrap(),
            source_sifs_path: "/services".to_string(),
            mode: crate::SandboxMountMode::ReadWrite,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();

        let owner_dfs = Arc::new(Mutex::new(owner_dfs));
        let file_object = file_stat.object_id.clone();
        let link_object = link_stat.object_id.clone();
        let owner_worker = {
            let owner_dfs = Arc::clone(&owner_dfs);
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let mut served_file = false;
                let mut served_link = false;
                loop {
                    let events = owner_dfs.lock().unwrap().process_incoming().unwrap();
                    for event in events {
                        match event {
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed {
                                object_id,
                                result: sinomo_dfs::DfsReadResult::Found(_),
                                ..
                            } if object_id == link_object => served_link = true,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed {
                                object_id,
                                result: sinomo_dfs::DfsReadResult::Found(_),
                                ..
                            } if object_id == file_object => served_file = true,
                            _ => {}
                        }
                    }
                    if served_file && served_link {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "owner did not serve remote write path dependencies"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        };

        let path = SandboxPath::new("/shared/shared-link/file.txt").unwrap();
        sifs_fs
            .write_at_unchecked(&path, b"remote".len() as u64, b" update")
            .expect("write_at follows remote intermediate symlink");
        owner_worker.join().unwrap();
        assert_eq!(
            sifs_fs.read_unchecked_mut(&path, None).unwrap(),
            b"remote update"
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_read_only_read_dir_fetches_remote_intermediate_symlink() {
        let name = "wasix_ro_remote_intermediate_symlink_read_dir";
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let owner = NodeId::from_legacy_named(format!("node_{name}_owner")).unwrap();
        let reader = NodeId::from_legacy_named(format!("node_{name}_reader")).unwrap();
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).unwrap(),
        );
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader.clone()),
            LocalContentStore::new(root.join("reader-content")).unwrap(),
        );
        owner_dfs.bootstrap_distributed_sifs().unwrap();
        reader_dfs.bootstrap_distributed_sifs().unwrap();
        owner_dfs.posix_mkdir_all("/services/shared").unwrap();
        let file_stat = owner_dfs
            .posix_write_file(
                "/services/shared/file.txt",
                b"remote",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        let link_stat = owner_dfs
            .posix_symlink(
                "shared",
                "/services/shared-link",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        for object in [
            owner_dfs.posix_lstat("/services/shared").unwrap().object_id,
            file_stat.object_id.clone(),
            link_stat.object_id.clone(),
        ] {
            let metadata = owner_dfs.metadata().object(&object).unwrap().clone();
            reader_dfs.add_object_metadata(metadata).unwrap();
        }

        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut manifest = SandboxInstanceManifest::new(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/shared").unwrap(),
            source_sifs_path: "/services".to_string(),
            mode: crate::SandboxMountMode::ReadOnly,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadOnly,
        );

        let owner_dfs = Arc::new(Mutex::new(owner_dfs));
        let owner_worker = {
            let owner_dfs = Arc::clone(&owner_dfs);
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    let events = owner_dfs.lock().unwrap().process_incoming().unwrap();
                    if events.iter().any(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed {
                                object_id,
                                result: sinomo_dfs::DfsReadResult::Found(_),
                                ..
                            } if object_id == &link_stat.object_id
                        )
                    }) {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "owner did not serve remote symlink target"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        };

        let entries = virtual_fs::FileSystem::read_dir(&wasix_fs, "/shared/shared-link".as_ref())
            .expect("read_dir follows remote intermediate symlink")
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        owner_worker.join().unwrap();
        assert_eq!(entries, vec!["file.txt"]);
    }

    #[test]
    fn wasix_sandbox_filesystem_read_write_read_dir_fetches_remote_intermediate_symlink() {
        let name = "wasix_rw_remote_intermediate_symlink_read_dir";
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let owner = NodeId::from_legacy_named(format!("node_{name}_owner")).unwrap();
        let reader = NodeId::from_legacy_named(format!("node_{name}_reader")).unwrap();
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).unwrap(),
        );
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader.clone()),
            LocalContentStore::new(root.join("reader-content")).unwrap(),
        );
        owner_dfs.bootstrap_distributed_sifs().unwrap();
        reader_dfs.bootstrap_distributed_sifs().unwrap();
        owner_dfs.posix_mkdir_all("/services/shared").unwrap();
        let file_stat = owner_dfs
            .posix_write_file(
                "/services/shared/file.txt",
                b"remote",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        let link_stat = owner_dfs
            .posix_symlink(
                "shared",
                "/services/shared-link",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        for object in [
            owner_dfs.posix_lstat("/services/shared").unwrap().object_id,
            file_stat.object_id.clone(),
            link_stat.object_id.clone(),
        ] {
            let metadata = owner_dfs.metadata().object(&object).unwrap().clone();
            reader_dfs.add_object_metadata(metadata).unwrap();
        }

        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut manifest = SandboxInstanceManifest::new(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/shared").unwrap(),
            source_sifs_path: "/services".to_string(),
            mode: crate::SandboxMountMode::ReadWrite,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let owner_dfs = Arc::new(Mutex::new(owner_dfs));
        let owner_worker = {
            let owner_dfs = Arc::clone(&owner_dfs);
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    let events = owner_dfs.lock().unwrap().process_incoming().unwrap();
                    if events.iter().any(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed {
                                object_id,
                                result: sinomo_dfs::DfsReadResult::Found(_),
                                ..
                            } if object_id == &link_stat.object_id
                        )
                    }) {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "owner did not serve remote symlink target"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        };

        let entries = virtual_fs::FileSystem::read_dir(&wasix_fs, "/shared/shared-link".as_ref())
            .expect("read_dir follows remote intermediate symlink")
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        owner_worker.join().unwrap();
        assert_eq!(entries, vec!["file.txt"]);
    }

    #[test]
    fn wasix_sandbox_filesystem_read_only_readlink_fetches_remote_symlink_target() {
        let name = "wasix_ro_remote_symlink_readlink";
        let root = sinomo_testdir::TestDir::new(name);
        let network = InProcessNetwork::new();
        let owner = NodeId::from_legacy_named(format!("node_{name}_owner")).unwrap();
        let reader = NodeId::from_legacy_named(format!("node_{name}_reader")).unwrap();
        let mut owner_dfs = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(root.join("owner-content")).unwrap(),
        );
        let mut reader_dfs = NetworkedDfsService::new(
            network.add_node(reader.clone()),
            LocalContentStore::new(root.join("reader-content")).unwrap(),
        );
        owner_dfs.bootstrap_distributed_sifs().unwrap();
        reader_dfs.bootstrap_distributed_sifs().unwrap();
        let link_stat = owner_dfs
            .posix_symlink(
                "target.txt",
                "/services/link.txt",
                DfsWriteOptions::replicated_eventual(vec![owner.clone()]),
            )
            .unwrap();
        let metadata = owner_dfs
            .metadata()
            .object(&link_stat.object_id)
            .unwrap()
            .clone();
        reader_dfs.add_object_metadata(metadata).unwrap();

        let sandbox_id = SandboxId::from_legacy_named(format!("sandbox_{name}")).unwrap();
        let mut manifest = SandboxInstanceManifest::new(
            sandbox_id,
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/shared").unwrap(),
            source_sifs_path: "/services".to_string(),
            mode: crate::SandboxMountMode::ReadOnly,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(reader_dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadOnly,
        );

        let owner_dfs = Arc::new(Mutex::new(owner_dfs));
        let owner_worker = {
            let owner_dfs = Arc::clone(&owner_dfs);
            std::thread::spawn(move || {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                loop {
                    let events = owner_dfs.lock().unwrap().process_incoming().unwrap();
                    if events.iter().any(|event| {
                        matches!(
                            event,
                            sinomo_dfs::NetworkedDfsServiceEvent::RemoteReadServed {
                                object_id,
                                result: sinomo_dfs::DfsReadResult::Found(_),
                                ..
                            } if object_id == &link_stat.object_id
                        )
                    }) {
                        return;
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "owner did not serve remote symlink target"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
            })
        };

        assert_eq!(
            virtual_fs::FileSystem::readlink_update_access(&wasix_fs, "/shared/link.txt".as_ref())
                .expect("readlink fetches remote symlink body"),
            PathBuf::from("target.txt")
        );
        owner_worker.join().unwrap();
    }

    #[test]
    fn wasix_sandbox_filesystem_reads_and_writes_through_sandbox_fs() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/input.txt").unwrap();
        local_fs.write(&path, b"hello").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/input.txt")
            .unwrap();
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .unwrap();
        assert_eq!(bytes, b"hello");

        virtual_mio::block_on(async {
            use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
            file.rewind().await?;
            file.write_all(b"j").await?;
            file.flush().await
        })
        .unwrap();

        assert_eq!(
            local_fs.lock().unwrap().read(&path, None).unwrap(),
            b"jello"
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_rejects_read_only_truncate() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/input.txt").unwrap();
        local_fs.write(&path, b"keep").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let error = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .truncate(true)
            .open("/work/input.txt")
            .expect_err("read-only truncate must fail");
        assert_eq!(error, virtual_fs::FsError::PermissionDenied);
        assert_eq!(local_fs.lock().unwrap().read(&path, None).unwrap(), b"keep");
    }

    #[test]
    fn wasix_sandbox_filesystem_enforces_read_only_mount_in_vfs() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/input.txt").unwrap();
        local_fs.write(&path, b"keep").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadOnly,
        );

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/input.txt")
            .expect("read from read-only mount");
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .unwrap();
        assert_eq!(bytes, b"keep");

        assert_eq!(
            virtual_fs::FileSystem::new_open_options(&wasix_fs)
                .write(true)
                .open("/work/input.txt")
                .expect_err("write open on read-only mount must fail"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::create_dir(&wasix_fs, "/work/new-dir".as_ref())
                .expect_err("mkdir on read-only mount must fail"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::set_mode(&wasix_fs, "/work/input.txt".as_ref(), 0o600)
                .expect_err("chmod on read-only mount must fail"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(local_fs.lock().unwrap().read(&path, None).unwrap(), b"keep");
    }

    #[test]
    fn wasix_sandbox_filesystem_refreshes_open_files_before_io() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/shared.txt").unwrap();
        local_fs.write(&path, b"0000").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let mut first = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/shared.txt")
            .unwrap();
        let mut second = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/shared.txt")
            .unwrap();

        virtual_mio::block_on(async {
            use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
            first.write_all(b"A").await?;
            second.seek(SeekFrom::Start(1)).await?;
            second.write_all(b"B").await
        })
        .unwrap();
        assert_eq!(local_fs.lock().unwrap().read(&path, None).unwrap(), b"AB00");

        let mut reader = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/shared.txt")
            .unwrap();
        virtual_mio::block_on(async {
            use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
            first.seek(SeekFrom::Start(2)).await?;
            first.write_all(b"C").await?;
            Ok::<(), std::io::Error>(())
        })
        .unwrap();
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(
            &mut reader,
            &mut bytes,
        ))
        .unwrap();
        assert_eq!(bytes, b"ABC0");
    }

    #[test]
    fn wasix_sandbox_filesystem_seek_end_refreshes_backing_sifs_size() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_seek_end_refreshes_sifs_size");
        let path = SandboxPath::new("/work/seek-end-refresh.txt").unwrap();
        {
            let mut guard = sifs_fs.lock().unwrap();
            guard.write(&path, b"abc").unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/seek-end-refresh.txt")
            .unwrap();
        {
            let mut guard = sifs_fs.lock().unwrap();
            guard.write_at_unchecked(&path, 8, b"Z").unwrap();
        }

        virtual_mio::block_on(async {
            use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
            let offset = file.seek(SeekFrom::End(0)).await?;
            assert_eq!(offset, 9);
            file.write_all(b"A").await
        })
        .unwrap();

        assert_eq!(
            sifs_fs.lock().unwrap().read(&path, None).unwrap(),
            b"abc\0\0\0\0\0ZA"
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_append_handles_preserve_all_writes() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/append.txt").unwrap();
        local_fs.write(&path, b"").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let mut first = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .append(true)
            .open("/work/append.txt")
            .unwrap();
        let mut second = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .append(true)
            .open("/work/append.txt")
            .unwrap();

        virtual_mio::block_on(async {
            use virtual_fs::AsyncWriteExt;
            first.write_all(b"A").await?;
            second.write_all(b"B").await?;
            first.write_all(b"C").await
        })
        .unwrap();

        assert_eq!(local_fs.lock().unwrap().read(&path, None).unwrap(), b"ABC");
    }

    #[test]
    fn wasix_sandbox_filesystem_open_write_handle_survives_later_chmod() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-sifs");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_sandbox_fs_chmod").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let sandbox_id = SandboxId::from_legacy_named("sandbox_wasix_sandbox_fs_chmod").unwrap();
        let mut store = SifsSandboxInstanceStore::new(dfs);
        let manifest = store
            .create(
                sandbox_id,
                SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
                OffsetDateTime::now_utc(),
            )
            .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let path = SandboxPath::new("/work/open-chmod.txt").unwrap();
        sifs_fs.write(&path, b"old").unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .append(true)
            .open("/work/open-chmod.txt")
            .unwrap();
        sifs_fs.lock().unwrap().set_mode(&path, 0).unwrap();
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(&mut file, b"X"))
            .expect("write through descriptor opened before chmod");

        let guard = sifs_fs.lock().unwrap();
        assert_eq!(guard.read_unchecked(&path, None).unwrap(), b"oldX");
        assert_eq!(guard.stat_unchecked(&path).unwrap().mode, 0);
        drop(guard);
    }

    #[test]
    fn wasix_sandbox_filesystem_open_read_handle_survives_later_chmod() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-sifs-read");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_sandbox_fs_read_chmod").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let sandbox_id =
            SandboxId::from_legacy_named("sandbox_wasix_sandbox_fs_read_chmod").unwrap();
        let mut store = SifsSandboxInstanceStore::new(dfs);
        let manifest = store
            .create(
                sandbox_id,
                SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
                OffsetDateTime::now_utc(),
            )
            .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let path = SandboxPath::new("/work/open-read-chmod.txt").unwrap();
        sifs_fs.write(&path, b"readable").unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/open-read-chmod.txt")
            .unwrap();
        sifs_fs.lock().unwrap().set_mode(&path, 0).unwrap();

        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .expect("read through descriptor opened before chmod");
        assert_eq!(bytes, b"readable");
        assert_eq!(
            sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap().mode,
            0
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_descriptor_read_updates_sifs_atime() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_read_updates_atime");
        let path = SandboxPath::new("/work/read-atime.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"readable").unwrap();
            fs.set_times(&path, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/read-atime.txt")
            .unwrap();
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .expect("read through descriptor");

        assert_eq!(bytes, b"readable");
        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap();
        assert!(metadata.accessed_at_unix_nanos > 12_345);
        assert_eq!(metadata.modified_at_unix_nanos, 67_890);
    }

    #[test]
    fn wasix_sandbox_filesystem_read_only_descriptor_read_does_not_update_sifs_atime() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs_with_mode(
            "wasix_ro_read_keeps_atime",
            crate::SandboxMountMode::ReadOnly,
        );
        let path = SandboxPath::new("/work/read-atime.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"readable").unwrap();
            fs.set_times(&path, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/read-atime.txt")
            .unwrap();
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .expect("read through read-only descriptor");

        assert_eq!(bytes, b"readable");
        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap();
        assert_eq!(metadata.accessed_at_unix_nanos, 12_345);
        assert_eq!(metadata.modified_at_unix_nanos, 67_890);
    }

    #[test]
    fn wasix_sandbox_filesystem_descriptor_created_time_reports_wasi_ctime() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_created_is_ctime");
        let path = SandboxPath::new("/work/created.txt").unwrap();
        let changed_at = {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"metadata").unwrap();
            let initial = fs.stat_unchecked(&path).unwrap();
            fs.set_mode(&path, 0o600).unwrap();
            let changed = fs.stat_unchecked(&path).unwrap();
            assert_eq!(changed.created_at_unix_nanos, initial.created_at_unix_nanos);
            assert!(changed.changed_at_unix_nanos >= initial.changed_at_unix_nanos);
            changed.changed_at_unix_nanos
        };

        let file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/created.txt")
            .unwrap();

        assert_eq!(file.created_time(), changed_at);
    }

    #[test]
    fn wasix_sandbox_filesystem_descriptor_write_refreshes_handle_times() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_write_refreshes_handle_times");
        let path = SandboxPath::new("/work/write-metadata.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"old").unwrap();
            fs.set_times(&path, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .write(true)
            .open("/work/write-metadata.txt")
            .unwrap();
        virtual_mio::block_on(async {
            use virtual_fs::AsyncWriteExt;
            file.write_all(b"new").await?;
            file.flush().await
        })
        .expect("write through descriptor");

        assert!(
            file.last_modified() > 67_890,
            "open file handle mtime should reflect the SIFS write"
        );
        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap();
        assert_eq!(file.last_modified(), metadata.modified_at_unix_nanos);
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
    }

    #[test]
    fn wasix_sandbox_filesystem_set_len_refreshes_handle_times() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_set_len_refreshes_handle_times");
        let path = SandboxPath::new("/work/truncate-metadata.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"old-content").unwrap();
            fs.set_times(&path, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .write(true)
            .open("/work/truncate-metadata.txt")
            .unwrap();
        file.as_mut()
            .set_len(3)
            .expect("set len through descriptor");

        assert_eq!(file.size(), 3);
        assert!(
            file.last_modified() > 67_890,
            "open file handle mtime should reflect the SIFS truncate"
        );
        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap();
        assert_eq!(file.last_modified(), metadata.modified_at_unix_nanos);
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
    }

    #[test]
    fn wasix_sandbox_filesystem_set_len_preserves_descriptor_offset() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_set_len_preserves_offset");
        let path = SandboxPath::new("/work/ftruncate-offset.txt").unwrap();
        sifs_fs.lock().unwrap().write(&path, b"abcdef").unwrap();

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .write(true)
            .open("/work/ftruncate-offset.txt")
            .unwrap();
        virtual_mio::block_on(async {
            use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
            file.seek(SeekFrom::Start(8)).await?;
            file.as_mut().set_len(0)?;
            file.write_all(b"Z").await
        })
        .expect("ftruncate must not move descriptor offset");

        assert_eq!(
            sifs_fs.lock().unwrap().read(&path, None).unwrap(),
            b"\0\0\0\0\0\0\0\0Z"
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_fd_set_mode_refreshes_handle_metadata() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_fd_set_mode_refreshes_metadata");
        let path = SandboxPath::new("/work/mode-metadata.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"mode").unwrap();
            fs.set_times(&path, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/mode-metadata.txt")
            .unwrap();
        file.as_mut().set_mode(0o600).expect("fd chmod");

        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap();
        assert_eq!(metadata.mode, 0o600);
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
    }

    #[test]
    fn wasix_sandbox_filesystem_fd_set_owner_refreshes_handle_metadata() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_fd_set_owner_refreshes_metadata");
        let path = SandboxPath::new("/work/owner-metadata.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"owner").unwrap();
            fs.set_times(&path, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/owner-metadata.txt")
            .unwrap();
        file.as_mut().set_owner(1001, 1002).expect("fd chown");

        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap();
        assert_eq!((metadata.uid, metadata.gid), (1001, 1002));
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
    }

    #[test]
    fn wasix_sandbox_filesystem_fd_set_times_refreshes_handle_metadata() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_fd_set_times_refreshes_metadata");
        let path = SandboxPath::new("/work/times-metadata.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"times").unwrap();
            fs.set_times(&path, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/times-metadata.txt")
            .unwrap();
        file.as_mut()
            .set_times(Some(22_222), Some(88_888))
            .expect("fd futimens");

        assert_eq!(file.last_accessed(), 22_222);
        assert_eq!(file.last_modified(), 88_888);
        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&path).unwrap();
        assert_eq!(file.last_accessed(), metadata.accessed_at_unix_nanos);
        assert_eq!(file.last_modified(), metadata.modified_at_unix_nanos);
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
    }

    #[test]
    fn wasix_sandbox_filesystem_fd_metadata_refreshes_after_alias_write() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_fd_metadata_refresh_alias_write");
        let original = SandboxPath::new("/work/original.txt").unwrap();
        let alias = SandboxPath::new("/work/alias.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&original, b"old").unwrap();
            fs.hard_link(&original, &alias).unwrap();
            fs.set_times(&original, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/original.txt")
            .unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&alias, b"new content").unwrap();
        }

        file.as_mut()
            .refresh_metadata()
            .expect("refresh fd metadata");

        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&original).unwrap();
        assert_eq!(file.size(), b"new content".len() as u64);
        assert_eq!(file.last_modified(), metadata.modified_at_unix_nanos);
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
        assert_eq!(file.link_count(), Some(2));
    }

    #[test]
    fn wasix_sandbox_filesystem_note_link_added_refreshes_handle_metadata() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_note_link_added_refreshes_metadata");
        let original = SandboxPath::new("/work/original.txt").unwrap();
        let alias = SandboxPath::new("/work/alias.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&original, b"content").unwrap();
            fs.set_times(&original, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/original.txt")
            .unwrap();
        virtual_fs::FileSystem::hard_link(
            &wasix_fs,
            "/work/original.txt".as_ref(),
            "/work/alias.txt".as_ref(),
        )
        .expect("hardlink");
        file.as_mut().note_link_added().expect("link added");

        assert_eq!(file.link_count(), Some(2));
        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&alias).unwrap();
        assert_eq!(metadata.link_count, 2);
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
    }

    #[test]
    fn wasix_sandbox_filesystem_note_link_removed_refreshes_handle_metadata() {
        let (_root, sifs_fs, wasix_fs) =
            sifs_wasix_fs("wasix_note_link_removed_refreshes_metadata");
        let original = SandboxPath::new("/work/original.txt").unwrap();
        let alias = SandboxPath::new("/work/alias.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&original, b"content").unwrap();
            fs.hard_link(&original, &alias).unwrap();
            fs.set_times(&original, Some(12_345), Some(67_890)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/original.txt")
            .unwrap();
        virtual_fs::FileSystem::remove_file(&wasix_fs, "/work/original.txt".as_ref())
            .expect("unlink original");
        file.as_mut().note_link_removed().expect("link removed");

        assert_eq!(file.link_count(), Some(1));
        let metadata = sifs_fs.lock().unwrap().stat_unchecked(&alias).unwrap();
        assert_eq!(metadata.link_count, 1);
        assert_eq!(file.created_time(), metadata.changed_at_unix_nanos);
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .expect("open descriptor should keep reading through surviving alias");
        assert_eq!(bytes, b"content");
    }

    #[test]
    fn wasix_sandbox_filesystem_open_file_survives_unlink() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/remove-open.txt").unwrap();
        local_fs.write(&path, b"before").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/remove-open.txt")
            .unwrap();
        virtual_fs::FileSystem::remove_file(&wasix_fs, "/work/remove-open.txt".as_ref())
            .expect("unlink path");

        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .expect("read open unlinked file");
        assert_eq!(bytes, b"before");
        assert_eq!(file.link_count(), Some(0));
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(
            &mut file,
            b" detached",
        ))
        .expect("write detached file");
        drop(file);
        assert!(matches!(
            local_fs.lock().unwrap().stat(&path),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn wasix_sandbox_filesystem_fd_times_update_unlinked_open_file_metadata() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_sandbox_fs_unlinked_fd_times");
        let path = SandboxPath::new("/work/remove-open-times.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&path, b"before").unwrap();
            fs.set_times(&path, Some(11_111), Some(22_222)).unwrap();
        }

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/remove-open-times.txt")
            .unwrap();
        virtual_fs::FileSystem::remove_file(&wasix_fs, "/work/remove-open-times.txt".as_ref())
            .expect("unlink path");

        file.set_times(Some(33_333), Some(44_444))
            .expect("futimens on unlinked open descriptor");

        assert_eq!(file.link_count(), Some(0));
        assert_eq!(file.last_accessed(), 33_333);
        assert_eq!(file.last_modified(), 44_444);
        assert!(file.created_time() >= 44_444);
        assert!(matches!(
            sifs_fs.lock().unwrap().stat(&path),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn wasix_sandbox_filesystem_open_file_survives_unlink_and_recreate() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_sandbox_fs_recreate_after_unlink");
        let path = SandboxPath::new("/work/recreated.txt").unwrap();
        sifs_fs.lock().unwrap().write(&path, b"old").unwrap();
        let old_inode = sifs_fs.lock().unwrap().stat(&path).unwrap().inode;

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/recreated.txt")
            .unwrap();
        {
            let mut guard = sifs_fs.lock().unwrap();
            guard.unlink(&path).unwrap();
            guard.write(&path, b"new").unwrap();
        }
        let new_inode = sifs_fs.lock().unwrap().stat(&path).unwrap().inode;
        assert_ne!(old_inode, new_inode);

        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .expect("read old descriptor after path recreation");
        assert_eq!(bytes, b"old");
        assert_eq!(file.link_count(), Some(0));
        assert_eq!(sifs_fs.lock().unwrap().read(&path, None).unwrap(), b"new");
    }

    #[test]
    fn wasix_sandbox_filesystem_reopen_after_recreate_uses_new_sifs_inode() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_sandbox_fs_reopen_recreate");
        let path = SandboxPath::new("/work/recreated-open.txt").unwrap();
        sifs_fs.lock().unwrap().write(&path, b"old").unwrap();
        let old_inode = sifs_fs.lock().unwrap().stat(&path).unwrap().inode;

        let mut old_file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/recreated-open.txt")
            .unwrap();
        {
            let mut guard = sifs_fs.lock().unwrap();
            guard.unlink(&path).unwrap();
            guard.write(&path, b"new").unwrap();
        }
        let new_inode = sifs_fs.lock().unwrap().stat(&path).unwrap().inode;
        assert_ne!(old_inode, new_inode);

        let mut reopened = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/recreated-open.txt")
            .unwrap();
        let mut reopened_bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(
            &mut reopened,
            &mut reopened_bytes,
        ))
        .expect("read reopened file after path recreation");
        assert_eq!(reopened_bytes, b"new");
        assert_eq!(reopened.inode(), Some(new_inode));

        let mut old_bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(
            &mut old_file,
            &mut old_bytes,
        ))
        .expect("read old descriptor after path recreation");
        assert_eq!(old_bytes, b"old");
        assert_eq!(old_file.link_count(), Some(0));
    }

    #[test]
    fn wasix_sandbox_filesystem_open_write_after_recreate_does_not_replace_new_path() {
        let (_root, sifs_fs, wasix_fs) =
            sifs_wasix_fs("wasix_sandbox_fs_write_recreate_after_unlink");
        let path = SandboxPath::new("/work/recreated-write.txt").unwrap();
        sifs_fs.lock().unwrap().write(&path, b"old").unwrap();

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .append(true)
            .open("/work/recreated-write.txt")
            .unwrap();
        {
            let mut guard = sifs_fs.lock().unwrap();
            guard.unlink(&path).unwrap();
            guard.write(&path, b"new").unwrap();
        }

        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(
            &mut file,
            b"-detached",
        ))
        .expect("write old descriptor after path recreation");
        assert_eq!(sifs_fs.lock().unwrap().read(&path, None).unwrap(), b"new");
    }

    #[test]
    fn wasix_sandbox_filesystem_fd_unlink_after_recreate_does_not_remove_new_path() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_sandbox_fs_fd_unlink_recreate");
        let path = SandboxPath::new("/work/recreated-unlink.txt").unwrap();
        sifs_fs.lock().unwrap().write(&path, b"old").unwrap();

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/recreated-unlink.txt")
            .unwrap();
        {
            let mut guard = sifs_fs.lock().unwrap();
            guard.unlink(&path).unwrap();
            guard.write(&path, b"new").unwrap();
        }

        file.unlink()
            .expect("fd unlink should detach the opened inode only");
        assert_eq!(file.link_count(), Some(0));
        assert_eq!(sifs_fs.lock().unwrap().read(&path, None).unwrap(), b"new");
    }

    #[test]
    fn wasix_sandbox_filesystem_open_write_follows_rename() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_sandbox_fs_write_after_rename");
        let old_path = SandboxPath::new("/work/old.txt").unwrap();
        let new_path = SandboxPath::new("/work/new.txt").unwrap();
        sifs_fs.lock().unwrap().write(&old_path, b"old").unwrap();
        let old_inode = sifs_fs.lock().unwrap().stat(&old_path).unwrap().inode;

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .append(true)
            .open("/work/old.txt")
            .unwrap();
        virtual_mio::block_on(virtual_fs::FileSystem::rename(
            &wasix_fs,
            "/work/old.txt".as_ref(),
            "/work/new.txt".as_ref(),
        ))
        .expect("rename opened path");
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(&mut file, b"-renamed"))
            .expect("write descriptor after rename");

        let guard = sifs_fs.lock().unwrap();
        assert!(matches!(
            guard.stat(&old_path),
            Err(SandboxFsError::NotFound { .. }) | Err(SandboxFsError::Unavailable { .. })
        ));
        assert_eq!(guard.read(&new_path, None).unwrap(), b"old-renamed");
        assert_eq!(guard.stat(&new_path).unwrap().inode, old_inode);
        drop(guard);
    }

    #[test]
    fn wasix_sandbox_filesystem_open_write_identity_prefers_workspace_over_read_only_alias() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-identity-workspace");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_identity_workspace").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let sandbox_id = SandboxId::from_legacy_named("sandbox_wasix_identity_workspace").unwrap();
        let mut store = SifsSandboxInstanceStore::new(dfs);
        let mut manifest = store
            .create(
                sandbox_id,
                SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
                OffsetDateTime::now_utc(),
            )
            .unwrap();
        manifest.mounts.push(SandboxMount {
            mount_path: SandboxPath::new("/readonly-work").unwrap(),
            source_sifs_path: format!("{}/work", manifest.workspace_root.trim_end_matches('/')),
            mode: crate::SandboxMountMode::ReadOnly,
        });
        let mut sifs_fs = SifsSandboxFileSystem::new(store.into_inner(), &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );
        let old_path = SandboxPath::new("/work/old.txt").unwrap();
        let new_path = SandboxPath::new("/work/new.txt").unwrap();
        sifs_fs.lock().unwrap().write(&old_path, b"old").unwrap();

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .append(true)
            .open("/work/old.txt")
            .unwrap();
        virtual_mio::block_on(virtual_fs::FileSystem::rename(
            &wasix_fs,
            "/work/old.txt".as_ref(),
            "/work/new.txt".as_ref(),
        ))
        .expect("rename opened path");
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(&mut file, b"-renamed"))
            .expect("write descriptor after rename must keep workspace identity");

        let guard = sifs_fs.lock().unwrap();
        assert_eq!(guard.read(&new_path, None).unwrap(), b"old-renamed");
        assert_eq!(
            guard
                .read(&SandboxPath::new("/readonly-work/new.txt").unwrap(), None)
                .unwrap(),
            b"old-renamed"
        );
        drop(guard);
    }

    #[test]
    fn wasix_sandbox_filesystem_open_replaced_destination_stays_detached() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_sandbox_fs_rename_replace_open_dst");
        let source = SandboxPath::new("/work/source.txt").unwrap();
        let target = SandboxPath::new("/work/target.txt").unwrap();
        {
            let mut guard = sifs_fs.lock().unwrap();
            guard.write(&source, b"source").unwrap();
            guard.write(&target, b"target").unwrap();
        }
        let target_inode = sifs_fs.lock().unwrap().stat(&target).unwrap().inode;

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/target.txt")
            .unwrap();
        virtual_mio::block_on(virtual_fs::FileSystem::rename(
            &wasix_fs,
            "/work/source.txt".as_ref(),
            "/work/target.txt".as_ref(),
        ))
        .expect("replace target path");

        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .expect("read replaced destination fd");
        assert_eq!(bytes, b"target");
        assert_eq!(file.link_count(), Some(0));
        file.set_times(Some(12_345), Some(67_890))
            .expect("set times on detached destination fd");
        virtual_mio::block_on(async {
            use virtual_fs::{AsyncSeekExt, AsyncWriteExt};
            file.seek(SeekFrom::End(0)).await?;
            file.write_all(b"-detached").await
        })
        .expect("write replaced destination fd");

        let guard = sifs_fs.lock().unwrap();
        assert_eq!(guard.read(&target, None).unwrap(), b"source");
        assert_ne!(guard.stat(&target).unwrap().inode, target_inode);
        assert_ne!(
            guard.stat(&target).unwrap().modified_at_unix_nanos,
            67_890,
            "fd set_times after replacement must not update the replacement path"
        );
        drop(guard);
    }

    #[test]
    fn wasix_sandbox_filesystem_set_mode_uses_sandbox_fs_metadata() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/mode.txt").unwrap();
        local_fs.write(&path, b"mode").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        virtual_fs::FileSystem::set_mode(&wasix_fs, "/work/mode.txt".as_ref(), 0o700)
            .expect("set mode");
        let metadata = virtual_fs::FileSystem::metadata(&wasix_fs, "/work/mode.txt".as_ref())
            .expect("metadata");

        assert_eq!(metadata.mode(), 0o700);
        assert_eq!(local_fs.lock().unwrap().stat(&path).unwrap().mode, 0o700);
    }

    #[test]
    fn wasix_sandbox_filesystem_set_owner_uses_sandbox_fs_metadata() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/owner.txt").unwrap();
        local_fs.write(&path, b"owner").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        virtual_fs::FileSystem::set_owner(&wasix_fs, "/work/owner.txt".as_ref(), 1001, 1002)
            .expect("set owner");
        let metadata = virtual_fs::FileSystem::metadata(&wasix_fs, "/work/owner.txt".as_ref())
            .expect("metadata");

        assert_eq!(metadata.uid(), 1001);
        assert_eq!(metadata.gid(), 1002);
        let sandbox_metadata = local_fs.lock().unwrap().stat(&path).unwrap();
        assert_eq!(sandbox_metadata.uid, 1001);
        assert_eq!(sandbox_metadata.gid, 1002);
    }

    #[test]
    fn wasix_sandbox_filesystem_set_times_uses_sandbox_fs_metadata() {
        let (_root, layout) = test_layout();
        let mut local_fs = LocalSandboxFileSystem::new(layout.clone());
        let path = SandboxPath::new("/work/times.txt").unwrap();
        local_fs.write(&path, b"times").unwrap();
        let local_fs = Arc::new(Mutex::new(local_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        virtual_fs::FileSystem::set_times(
            &wasix_fs,
            "/work/times.txt".as_ref(),
            Some(12_345),
            Some(67_890),
        )
        .expect("set times");
        let metadata = virtual_fs::FileSystem::metadata(&wasix_fs, "/work/times.txt".as_ref())
            .expect("metadata");

        assert_eq!(metadata.accessed(), 12_345);
        assert_eq!(metadata.modified(), 67_890);
        let sandbox_metadata = local_fs.lock().unwrap().stat(&path).unwrap();
        assert_eq!(sandbox_metadata.accessed_at_unix_nanos, 12_345);
        assert_eq!(sandbox_metadata.modified_at_unix_nanos, 67_890);
    }

    #[test]
    fn wasix_sandbox_filesystem_create_dir_and_list_use_sandbox_fs() {
        let (_root, layout) = test_layout();
        let local_fs = Arc::new(Mutex::new(LocalSandboxFileSystem::new(layout.clone())));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        virtual_fs::FileSystem::create_dir(&wasix_fs, "/work/generated".as_ref()).unwrap();
        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .create(true)
            .write(true)
            .open("/work/generated/result.txt")
            .unwrap();
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(&mut file, b"ok")).unwrap();

        let entries = virtual_fs::FileSystem::read_dir(&wasix_fs, "/work/generated".as_ref())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec!["result.txt"]);
        let work_metadata =
            virtual_fs::FileSystem::metadata(&wasix_fs, "/work".as_ref()).expect("work metadata");
        assert_eq!(work_metadata.nlink(), 3);
        assert_eq!(
            local_fs
                .lock()
                .unwrap()
                .read(
                    &SandboxPath::new("/work/generated/result.txt").unwrap(),
                    None
                )
                .unwrap(),
            b"ok"
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_read_dir_updates_sifs_atime() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("sifs-wasix-dir-atime");
        let dir = SandboxPath::new("/work/generated").unwrap();
        let file = SandboxPath::new("/work/generated/result.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.create_dir(&dir).expect("create dir");
            fs.write(&file, b"ok").expect("write file");
            fs.set_times(&dir, Some(12_345), Some(67_890))
                .expect("set times");
        }

        let entries = virtual_fs::FileSystem::read_dir(&wasix_fs, "/work/generated".as_ref())
            .expect("read dir")
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec!["result.txt"]);

        let metadata = sifs_fs.lock().unwrap().stat(&dir).expect("stat dir");
        assert!(metadata.accessed_at_unix_nanos > 12_345);
        assert_eq!(metadata.modified_at_unix_nanos, 67_890);
    }

    #[test]
    fn wasix_sandbox_filesystem_read_only_read_dir_does_not_update_sifs_atime() {
        let (_root, sifs_fs, wasix_fs) =
            sifs_wasix_fs_with_mode("sifs-wasix-ro-dir-atime", crate::SandboxMountMode::ReadOnly);
        let dir = SandboxPath::new("/work/generated").unwrap();
        let file = SandboxPath::new("/work/generated/result.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.create_dir(&dir).expect("create dir");
            fs.write(&file, b"ok").expect("write file");
            fs.set_times(&dir, Some(12_345), Some(67_890))
                .expect("set times");
        }

        let entries = virtual_fs::FileSystem::read_dir(&wasix_fs, "/work/generated".as_ref())
            .expect("read dir")
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(entries, vec!["result.txt"]);

        let metadata = sifs_fs.lock().unwrap().stat(&dir).expect("stat dir");
        assert_eq!(metadata.accessed_at_unix_nanos, 12_345);
        assert_eq!(metadata.modified_at_unix_nanos, 67_890);
    }

    #[test]
    fn wasix_sandbox_filesystem_read_only_readlink_does_not_update_sifs_atime() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs_with_mode(
            "sifs-wasix-ro-readlink-atime",
            crate::SandboxMountMode::ReadOnly,
        );
        let target = SandboxPath::new("/work/target.txt").unwrap();
        let link = SandboxPath::new("/work/link.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&target, b"target").expect("write target");
            fs.symlink(Path::new("target.txt"), &link)
                .expect("create link");
            fs.set_symlink_times(&link, Some(12_345), Some(67_890))
                .expect("set link times");
        }

        assert_eq!(
            virtual_fs::FileSystem::readlink_update_access(&wasix_fs, "/work/link.txt".as_ref())
                .expect("readlink"),
            PathBuf::from("target.txt")
        );

        let metadata = sifs_fs.lock().unwrap().stat(&link).expect("stat link");
        assert_eq!(metadata.accessed_at_unix_nanos, 12_345);
        assert_eq!(metadata.modified_at_unix_nanos, 67_890);
    }

    #[test]
    fn wasix_sandbox_filesystem_create_dir_is_not_recursive() {
        let (_root, layout) = test_layout();
        let local_fs = Arc::new(Mutex::new(LocalSandboxFileSystem::new(layout.clone())));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let error = virtual_fs::FileSystem::create_dir(&wasix_fs, "/work/missing/leaf".as_ref())
            .expect_err("mkdir must not create missing parents");
        assert_eq!(error, virtual_fs::FsError::EntryNotFound);
        assert!(matches!(
            local_fs
                .lock()
                .unwrap()
                .stat(&SandboxPath::new("/work/missing").unwrap()),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn wasix_sandbox_filesystem_preopen_root_rejects_parent_escape() {
        let (_root, layout) = test_layout();
        let mut fs = LocalSandboxFileSystem::new(layout.clone());
        let work_path = SandboxPath::new("/work/file.txt").unwrap();
        let tmp_path = SandboxPath::new("/tmp/file.txt").unwrap();
        fs.write(&work_path, b"work").unwrap();
        fs.write(&tmp_path, b"tmp").unwrap();
        let local_fs = Arc::new(Mutex::new(fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/work").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let error = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("../tmp/file.txt")
            .expect_err("preopen must not escape through parent directory");
        assert_eq!(error, virtual_fs::FsError::PermissionDenied);

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("nested/../file.txt")
            .expect("parent inside preopen should be allowed");
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .unwrap();
        assert_eq!(bytes, b"work");
    }

    #[test]
    fn wasix_sandbox_filesystem_preopen_root_rejects_absolute_symlink_escape() {
        let (_root, sifs_fs, _root_wasix_fs) = sifs_wasix_fs("wasix_preopen_symlink_escape");
        let work_link = SandboxPath::new("/work/link.txt").unwrap();
        let work_dangling_link = SandboxPath::new("/work/dangling.txt").unwrap();
        let work_dir_link = SandboxPath::new("/work/dir-link").unwrap();
        let tmp_target = SandboxPath::new("/tmp/secret.txt").unwrap();
        let tmp_created_target = SandboxPath::new("/tmp/created-through-dangling.txt").unwrap();
        let tmp_dir = SandboxPath::new("/tmp/hidden-dir").unwrap();
        let tmp_dir_file = SandboxPath::new("/tmp/hidden-dir/file.txt").unwrap();
        let tmp_child_link = SandboxPath::new("/tmp/hidden-dir/child-link").unwrap();
        let work_source = SandboxPath::new("/work/source.txt").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.write(&work_source, b"source").unwrap();
            fs.write(&tmp_target, b"secret").unwrap();
            fs.create_dir(&tmp_dir).unwrap();
            fs.write(&tmp_dir_file, b"hidden").unwrap();
            fs.symlink(Path::new("/tmp/secret.txt"), &work_link)
                .unwrap();
            fs.symlink(
                Path::new("/tmp/created-through-dangling.txt"),
                &work_dangling_link,
            )
            .unwrap();
            fs.symlink(Path::new("/tmp/hidden-dir"), &work_dir_link)
                .unwrap();
            fs.symlink(Path::new("/tmp/secret.txt"), &tmp_child_link)
                .unwrap();
        }
        let work_preopen = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/work").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        assert_eq!(
            virtual_fs::FileSystem::new_open_options(&work_preopen)
                .read(true)
                .open("link.txt")
                .expect_err("absolute symlink must not escape the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::new_open_options(&work_preopen)
                .read(true)
                .open("dir-link/")
                .expect_err("trailing-slash open must not validate outside preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::new_open_options(&work_preopen)
                .write(true)
                .create_new(true)
                .open("dir-link/child-link")
                .expect_err("exclusive create must not stat through a parent symlink escape"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::new_open_options(&work_preopen)
                .write(true)
                .create(true)
                .open("dangling.txt")
                .expect_err(
                    "open create must not create a dangling symlink target outside preopen",
                ),
            virtual_fs::FsError::PermissionDenied
        );
        assert!(matches!(
            sifs_fs.lock().unwrap().stat(&tmp_created_target),
            Err(SandboxFsError::NotFound { .. })
        ));
        assert_eq!(
            virtual_fs::FileSystem::metadata(&work_preopen, "link.txt".as_ref())
                .expect_err("metadata follow must not escape the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::read_dir(&work_preopen, "dir-link".as_ref())
                .expect_err("readdir must not follow a symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::symlink_metadata(&work_preopen, "dir-link/child-link".as_ref())
                .expect_err("lstat must not follow a parent symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::readlink(&work_preopen, "dir-link/child-link".as_ref())
                .expect_err("readlink must not follow a parent symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::readlink_update_access(
                &work_preopen,
                "dir-link/child-link".as_ref(),
            )
            .expect_err("readlink with atime must not follow a parent symlink out of preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::symlink_metadata(&work_preopen, "dir-link/".as_ref())
                .expect_err("trailing-slash symlink metadata must not validate outside preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::readlink(&work_preopen, "dir-link/".as_ref())
                .expect_err("trailing-slash readlink must not validate outside preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::readlink_update_access(&work_preopen, "dir-link/".as_ref())
                .expect_err("trailing-slash readlink with atime must not validate outside preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::set_times(
                &work_preopen,
                "link.txt".as_ref(),
                Some(33_333),
                Some(44_444),
            )
            .expect_err("utimens must not follow a symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::set_symlink_times(
                &work_preopen,
                "dir-link/child-link".as_ref(),
                Some(55_555),
                Some(66_666),
            )
            .expect_err("lutimens must not follow a parent symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::create_dir(&work_preopen, "dir-link/new-dir".as_ref())
                .expect_err("mkdir must not create through a symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::create_dir(&work_preopen, "dir-link/".as_ref())
                .expect_err("trailing-slash mkdir must not validate outside preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::remove_file(&work_preopen, "dir-link/".as_ref())
                .expect_err("trailing-slash unlink must not validate outside preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::symlink(
                &work_preopen,
                "source.txt".as_ref(),
                "dir-link/new-link".as_ref(),
            )
            .expect_err("symlink must not create through a symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_mio::block_on(virtual_fs::FileSystem::rename(
                &work_preopen,
                "source.txt".as_ref(),
                "dir-link/moved.txt".as_ref(),
            ))
            .expect_err("rename must not write through a symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            virtual_fs::FileSystem::hard_link(
                &work_preopen,
                "source.txt".as_ref(),
                "dir-link/alias.txt".as_ref(),
            )
            .expect_err("hard link must not write through a symlink out of the /work preopen"),
            virtual_fs::FsError::PermissionDenied
        );
        assert_eq!(
            sifs_fs
                .lock()
                .unwrap()
                .read(&tmp_target, None)
                .expect("escaped target remains untouched"),
            b"secret"
        );
        assert!(matches!(
            sifs_fs
                .lock()
                .unwrap()
                .stat(&SandboxPath::new("/tmp/hidden-dir/new-dir").unwrap()),
            Err(SandboxFsError::NotFound { .. })
        ));
        assert_eq!(
            sifs_fs
                .lock()
                .unwrap()
                .read(&work_source, None)
                .expect("source remains after failed rename"),
            b"source"
        );
        assert_eq!(
            virtual_fs::FileSystem::readlink(&work_preopen, "link.txt".as_ref())
                .expect("readlink itself does not follow the symlink"),
            PathBuf::from("/tmp/secret.txt")
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_rename_is_not_recursive() {
        let (_root, layout) = test_layout();
        let mut fs = LocalSandboxFileSystem::new(layout.clone());
        let source = SandboxPath::new("/work/source.txt").unwrap();
        fs.write(&source, b"data").unwrap();
        let local_fs = Arc::new(Mutex::new(fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let error = virtual_mio::block_on(virtual_fs::FileSystem::rename(
            &wasix_fs,
            "/work/missing/source.txt".as_ref(),
            "/work/missing/dest.txt".as_ref(),
        ))
        .expect_err("rename source should be missing");
        assert_eq!(error, virtual_fs::FsError::EntryNotFound);
        let error = virtual_mio::block_on(virtual_fs::FileSystem::rename(
            &wasix_fs,
            "/work/source.txt".as_ref(),
            "/work/missing/dest.txt".as_ref(),
        ))
        .expect_err("rename must not create missing parents");
        assert_eq!(error, virtual_fs::FsError::EntryNotFound);
        assert_eq!(
            local_fs.lock().unwrap().read(&source, None).unwrap(),
            b"data"
        );
        assert!(matches!(
            local_fs
                .lock()
                .unwrap()
                .stat(&SandboxPath::new("/work/missing").unwrap()),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn wasix_sandbox_filesystem_open_create_is_not_recursive() {
        let (_root, layout) = test_layout();
        let local_fs = Arc::new(Mutex::new(LocalSandboxFileSystem::new(layout.clone())));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let error = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .create(true)
            .write(true)
            .open("/work/missing/file.txt")
            .expect_err("open create must not create missing parents");
        assert_eq!(error, virtual_fs::FsError::EntryNotFound);
        assert!(matches!(
            local_fs
                .lock()
                .unwrap()
                .stat(&SandboxPath::new("/work/missing").unwrap()),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn wasix_sandbox_filesystem_reports_file_parent_as_not_directory() {
        let (_root, layout) = test_layout();
        let mut fs = LocalSandboxFileSystem::new(layout.clone());
        let parent = SandboxPath::new("/work/file-parent").unwrap();
        let source = SandboxPath::new("/work/source.txt").unwrap();
        fs.write(&parent, b"parent").unwrap();
        fs.write(&source, b"source").unwrap();
        let local_fs = Arc::new(Mutex::new(fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&local_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let error = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .create(true)
            .write(true)
            .open("/work/file-parent/child.txt")
            .expect_err("file parent must not be treated as a directory");
        assert_eq!(error, virtual_fs::FsError::BaseNotDirectory);

        let error = virtual_mio::block_on(virtual_fs::FileSystem::rename(
            &wasix_fs,
            "/work/source.txt".as_ref(),
            "/work/file-parent/child.txt".as_ref(),
        ))
        .expect_err("rename through file parent must fail as not-directory");
        assert_eq!(error, virtual_fs::FsError::BaseNotDirectory);
    }

    #[test]
    fn wasix_sandbox_filesystem_follows_sifs_symlink_for_open_and_metadata() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-sifs-link");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_sifs_link").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let manifest = SandboxInstanceManifest::new(
            SandboxId::from_legacy_named("sandbox_wasix_sifs_link").unwrap(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let target = SandboxPath::new("/work/target.txt").unwrap();
        let link = SandboxPath::new("/work/link.txt").unwrap();
        sifs_fs.write(&target, b"target").unwrap();
        sifs_fs.symlink(Path::new("target.txt"), &link).unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let metadata = virtual_fs::FileSystem::metadata(&wasix_fs, "/work/link.txt".as_ref())
            .expect("metadata follows symlink");
        assert!(metadata.file_type().is_file());
        assert!(!metadata.file_type().is_symlink());
        let link_metadata =
            virtual_fs::FileSystem::symlink_metadata(&wasix_fs, "/work/link.txt".as_ref())
                .expect("symlink metadata");
        assert!(link_metadata.file_type().is_symlink());
        assert_eq!(
            virtual_fs::FileSystem::readlink(&wasix_fs, "/work/link.txt".as_ref())
                .expect("readlink"),
            PathBuf::from("target.txt")
        );

        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .open("/work/link.txt")
            .expect("open follows symlink");
        let mut bytes = Vec::new();
        virtual_mio::block_on(virtual_fs::AsyncReadExt::read_to_end(&mut file, &mut bytes))
            .unwrap();
        assert_eq!(bytes, b"target");
    }

    #[test]
    fn wasix_sandbox_filesystem_preserves_trailing_slash_directory_requirement() {
        let (_root, sifs_fs, wasix_fs) = sifs_wasix_fs("wasix_trailing_slash");
        let real_dir = SandboxPath::new("/work/real-dir").unwrap();
        let file_path = SandboxPath::new("/work/file.txt").unwrap();
        let link_dir = SandboxPath::new("/work/link-dir").unwrap();
        let link_file = SandboxPath::new("/work/link-file").unwrap();
        {
            let mut fs = sifs_fs.lock().unwrap();
            fs.create_dir(&real_dir).unwrap();
            fs.write(&file_path, b"file").unwrap();
            fs.symlink(Path::new("real-dir"), &link_dir).unwrap();
            fs.symlink(Path::new("file.txt"), &link_file).unwrap();
        }

        assert_eq!(
            virtual_fs::FileSystem::new_open_options(&wasix_fs)
                .read(true)
                .open("/work/file.txt/")
                .expect_err("open file with trailing slash must fail"),
            virtual_fs::FsError::BaseNotDirectory
        );
        assert_eq!(
            virtual_fs::FileSystem::new_open_options(&wasix_fs)
                .read(true)
                .open("/work/link-file/")
                .expect_err("open file symlink with trailing slash must fail"),
            virtual_fs::FsError::BaseNotDirectory
        );
        assert_eq!(
            virtual_fs::FileSystem::metadata(&wasix_fs, "/work/file.txt/".as_ref())
                .expect_err("metadata on file with trailing slash must fail"),
            virtual_fs::FsError::BaseNotDirectory
        );
        assert_eq!(
            virtual_fs::FileSystem::readlink(&wasix_fs, "/work/file.txt".as_ref())
                .expect_err("readlink on a regular SIFS file must not degrade to generic I/O"),
            virtual_fs::FsError::NotAFile
        );
        assert_eq!(
            virtual_fs::FileSystem::create_dir(&wasix_fs, "/work/file.txt/".as_ref())
                .expect_err("mkdir on file with trailing slash must fail"),
            virtual_fs::FsError::BaseNotDirectory
        );
        assert_eq!(
            virtual_mio::block_on(virtual_fs::FileSystem::rename(
                &wasix_fs,
                "/work/file.txt/".as_ref(),
                "/work/renamed.txt".as_ref(),
            ))
            .expect_err("rename source file with trailing slash must fail"),
            virtual_fs::FsError::BaseNotDirectory
        );
        assert_eq!(
            virtual_mio::block_on(virtual_fs::FileSystem::rename(
                &wasix_fs,
                "/work/real-dir".as_ref(),
                "/work/missing-dir/".as_ref(),
            ))
            .expect_err("rename destination with trailing slash must already be a directory"),
            virtual_fs::FsError::EntryNotFound
        );
        assert!(
            sifs_fs
                .lock()
                .unwrap()
                .stat(&real_dir)
                .expect("source directory remains after failed trailing-slash rename")
                .is_dir
        );
        assert!(matches!(
            sifs_fs
                .lock()
                .unwrap()
                .stat(&SandboxPath::new("/work/missing-dir").unwrap()),
            Err(SandboxFsError::NotFound { .. })
        ));
        assert_eq!(
            virtual_fs::FileSystem::hard_link(
                &wasix_fs,
                "/work/file.txt/".as_ref(),
                "/work/file-alias.txt".as_ref(),
            )
            .expect_err("hard link source file with trailing slash must fail"),
            virtual_fs::FsError::BaseNotDirectory
        );
        assert_eq!(
            virtual_fs::FileSystem::hard_link(
                &wasix_fs,
                "/work/file.txt".as_ref(),
                "/work/file-alias.txt/".as_ref(),
            )
            .expect_err("hard link destination with trailing slash must fail"),
            virtual_fs::FsError::EntryNotFound
        );
        assert_eq!(
            virtual_fs::FileSystem::symlink(
                &wasix_fs,
                "file.txt".as_ref(),
                "/work/new-link/".as_ref(),
            )
            .expect_err("symlink destination with trailing slash must fail"),
            virtual_fs::FsError::BaseNotDirectory
        );
        assert!(matches!(
            sifs_fs
                .lock()
                .unwrap()
                .stat(&SandboxPath::new("/work/new-link").unwrap()),
            Err(SandboxFsError::NotFound { .. })
        ));
        assert!(
            virtual_fs::FileSystem::symlink_metadata(&wasix_fs, "/work/link-dir/".as_ref())
                .expect("trailing slash symlink metadata follows directory target")
                .file_type()
                .is_dir()
        );
        assert_eq!(
            virtual_fs::FileSystem::readlink(&wasix_fs, "/work/link-dir/".as_ref())
                .expect_err("readlink with trailing slash must not read the symlink object"),
            virtual_fs::FsError::NotAFile
        );
        assert_eq!(
            virtual_fs::FileSystem::remove_file(&wasix_fs, "/work/link-dir/".as_ref())
                .expect_err("unlink with trailing slash must not remove the symlink object"),
            virtual_fs::FsError::NotAFile
        );
        assert!(
            sifs_fs
                .lock()
                .unwrap()
                .stat(&link_dir)
                .expect("directory symlink remains")
                .is_symlink
        );
        assert!(
            sifs_fs
                .lock()
                .unwrap()
                .stat_follow(&link_dir)
                .expect("directory target remains")
                .is_dir
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_create_new_rejects_broken_sifs_symlink() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-sifs-create-new-link");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_create_new_link").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let manifest = SandboxInstanceManifest::new(
            SandboxId::from_legacy_named("sandbox_wasix_create_new_link").unwrap(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let link = SandboxPath::new("/work/link.txt").unwrap();
        sifs_fs.symlink(Path::new("missing.txt"), &link).unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let error = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .create_new(true)
            .write(true)
            .open("/work/link.txt")
            .expect_err("exclusive create must reject existing symlink");
        assert_eq!(error, virtual_fs::FsError::AlreadyExists);
        assert_eq!(
            virtual_fs::FileSystem::readlink(&wasix_fs, "/work/link.txt".as_ref())
                .expect("symlink remains"),
            PathBuf::from("missing.txt")
        );
        assert!(matches!(
            sifs_fs
                .lock()
                .unwrap()
                .stat(&SandboxPath::new("/work/missing.txt").unwrap()),
            Err(SandboxFsError::NotFound { .. })
        ));
    }

    #[test]
    fn wasix_sandbox_filesystem_open_create_existing_file_does_not_rewrite_sifs() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-sifs-create-existing");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_create_existing").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let manifest = SandboxInstanceManifest::new(
            SandboxId::from_legacy_named("sandbox_wasix_create_existing").unwrap(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        let path = SandboxPath::new("/work/existing.txt").unwrap();
        sifs_fs.write(&path, b"content").unwrap();
        sifs_fs
            .set_times(&path, Some(12_345), Some(67_890))
            .unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        let file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .create(true)
            .write(true)
            .open("/work/existing.txt")
            .expect("open existing file with create");
        drop(file);

        let metadata = sifs_fs.lock().unwrap().stat(&path).unwrap();
        assert_eq!(metadata.modified_at_unix_nanos, 67_890);
        assert_eq!(
            sifs_fs.lock().unwrap().read(&path, None).unwrap(),
            b"content"
        );
    }

    #[test]
    fn wasix_sandbox_filesystem_separates_remove_file_and_remove_dir() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-remove-kind");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_remove_kind").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let manifest = SandboxInstanceManifest::new(
            SandboxId::from_legacy_named("sandbox_wasix_remove_kind").unwrap(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        sifs_fs
            .create_dir_all(&SandboxPath::new("/work/dir").unwrap())
            .unwrap();
        sifs_fs
            .write(&SandboxPath::new("/work/file.txt").unwrap(), b"file")
            .unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        assert!(matches!(
            virtual_fs::FileSystem::remove_file(&wasix_fs, "/work/dir".as_ref()),
            Err(virtual_fs::FsError::NotAFile)
        ));
        assert!(matches!(
            virtual_fs::FileSystem::remove_dir(&wasix_fs, "/work/file.txt".as_ref()),
            Err(virtual_fs::FsError::BaseNotDirectory)
        ));
        virtual_fs::FileSystem::remove_file(&wasix_fs, "/work/file.txt".as_ref())
            .expect("remove file");
        virtual_fs::FileSystem::remove_dir(&wasix_fs, "/work/dir".as_ref()).expect("remove dir");
    }

    #[test]
    fn wasix_sandbox_filesystem_hard_link_uses_sandbox_fs() {
        let root = sinomo_testdir::TestDir::new("sinomo-wasix-sandbox-fs-hardlink");
        let network = InProcessNetwork::new();
        let node_id = NodeId::from_legacy_named("node_wasix_hardlink").unwrap();
        let mut dfs = NetworkedDfsService::new(
            network.add_node(node_id),
            LocalContentStore::new(root.join("content")).unwrap(),
        );
        dfs.bootstrap_distributed_sifs().unwrap();
        let manifest = SandboxInstanceManifest::new(
            SandboxId::from_legacy_named("sandbox_wasix_hardlink").unwrap(),
            SandboxToolchainId::from_legacy_named("toolchain_busybox").unwrap(),
            OffsetDateTime::now_utc(),
        )
        .unwrap();
        let mut sifs_fs = SifsSandboxFileSystem::new(dfs, &manifest);
        sifs_fs.ensure_workspace().unwrap();
        sifs_fs
            .write(&SandboxPath::new("/work/file.txt").unwrap(), b"original")
            .unwrap();
        let sifs_fs = Arc::new(Mutex::new(sifs_fs));
        let wasix_fs = WasixSandboxFileSystem::new(
            Arc::clone(&sifs_fs),
            SandboxPath::new("/").unwrap(),
            crate::SandboxMountMode::ReadWrite,
        );

        virtual_fs::FileSystem::hard_link(
            &wasix_fs,
            "/work/file.txt".as_ref(),
            "/work/alias.txt".as_ref(),
        )
        .expect("hard link");
        let original_metadata =
            virtual_fs::FileSystem::metadata(&wasix_fs, "/work/file.txt".as_ref())
                .expect("original metadata");
        let link_metadata = virtual_fs::FileSystem::metadata(&wasix_fs, "/work/alias.txt".as_ref())
            .expect("link metadata");
        assert_eq!(original_metadata.nlink, 2);
        assert_eq!(link_metadata.nlink, 2);
        assert_eq!(original_metadata.ino, link_metadata.ino);
        let mut time_file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .read(true)
            .write(true)
            .open("/work/alias.txt")
            .expect("open link for times");
        time_file
            .set_times(Some(12_345), Some(67_890))
            .expect("set times");
        drop(time_file);
        assert_eq!(
            virtual_fs::FileSystem::metadata(&wasix_fs, "/work/file.txt".as_ref())
                .expect("original metadata after set times")
                .accessed,
            12_345
        );
        assert_eq!(
            virtual_fs::FileSystem::metadata(&wasix_fs, "/work/alias.txt".as_ref())
                .expect("link metadata after set times")
                .modified,
            67_890
        );
        let mut file = virtual_fs::FileSystem::new_open_options(&wasix_fs)
            .write(true)
            .truncate(true)
            .open("/work/alias.txt")
            .expect("open link for write");
        assert_eq!(
            sifs_fs
                .lock()
                .unwrap()
                .read(&SandboxPath::new("/work/file.txt").unwrap(), None)
                .expect("read original after open truncate"),
            b"",
            "O_TRUNC through a hardlink must truncate the shared SIFS object immediately"
        );
        assert_eq!(
            virtual_fs::FileSystem::metadata(&wasix_fs, "/work/file.txt".as_ref())
                .expect("original metadata after open truncate")
                .ino,
            original_metadata.ino,
            "O_TRUNC must preserve hardlink object identity"
        );
        virtual_mio::block_on(virtual_fs::AsyncWriteExt::write_all(&mut file, b"updated"))
            .expect("write link");
        drop(file);

        let bytes = sifs_fs
            .lock()
            .unwrap()
            .read(&SandboxPath::new("/work/file.txt").unwrap(), None)
            .expect("read original");
        assert_eq!(bytes, b"updated");
    }
}
