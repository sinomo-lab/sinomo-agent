//! Root-scoped virtual filesystem facade.
//!
//! The VFS layer gives high-level crates a storage-oriented API without
//! binding them to the current backing store. Handles are always scoped to a
//! root path, so callers can operate with local names while the backend remains
//! local, mounted SIFS, daemon-backed SIFS, or an in-memory test store.

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_core::network::ObjectRpcRequest;
use sinomo_core::object::{LengthMode, serde_ext};
use sinomo_dfs::{
    DfsDirEntry, DfsObjectKind, DfsPosixError, DfsStat, DfsWriteOptions, OpenedMountedSifsService,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, VfsError>;

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("invalid VFS path {path}: {detail}")]
    InvalidPath { path: String, detail: String },
    #[error("VFS path {path} is outside scoped root {root}")]
    OutsideScope { root: String, path: String },
    #[error("VFS path not found: {path}")]
    NotFound { path: String },
    #[error("VFS path already exists: {path}")]
    AlreadyExists { path: String },
    #[error("VFS path is not a directory: {path}")]
    NotDirectory { path: String },
    #[error("VFS path is a directory: {path}")]
    IsDirectory { path: String },
    #[error("VFS directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
    #[error("VFS write conflicted at {path}: {detail}")]
    Conflict { path: String, detail: String },
    #[error("VFS operation is unsupported by {backend}: {detail}")]
    Unsupported { backend: String, detail: String },
    #[error("VFS I/O failed at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("VFS JSON codec failed at {path}: {detail}")]
    Json { path: String, detail: String },
    #[error("VFS SOBJ codec failed at {path}: {detail}")]
    Sobj { path: String, detail: String },
    #[error("VFS backend {backend} failed at {path}: {detail}")]
    Backend {
        backend: String,
        path: String,
        detail: String,
    },
}

impl VfsError {
    pub fn path(&self) -> Option<&str> {
        match self {
            Self::InvalidPath { path, .. }
            | Self::OutsideScope { path, .. }
            | Self::NotFound { path }
            | Self::AlreadyExists { path }
            | Self::NotDirectory { path }
            | Self::IsDirectory { path }
            | Self::DirectoryNotEmpty { path }
            | Self::Conflict { path, .. }
            | Self::Io { path, .. }
            | Self::Json { path, .. }
            | Self::Sobj { path, .. }
            | Self::Backend { path, .. } => Some(path),
            Self::Unsupported { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsObjectKind {
    File,
    Directory,
    Symlink,
    Hardlink,
    Artifact,
    Mount,
    Tombstone,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsMetadata {
    pub path: String,
    pub kind: VfsObjectKind,
    pub size_bytes: Option<u64>,
    pub content_hash: Option<String>,
    pub version: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsDirEntry {
    pub name: String,
    pub path: String,
    pub kind: VfsObjectKind,
    pub size_bytes: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsFileRead {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsWriteDurability {
    Local,
    ReplicatedEventual,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsConsistencyTarget {
    BestEffort,
    ReadYourWrites,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsCreateMode {
    CreateOrReplace,
    CreateNew,
    ReplaceExisting,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VfsConflictBehavior {
    Fail,
    Overwrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsWriteOptions {
    pub durability: VfsWriteDurability,
    pub consistency: VfsConsistencyTarget,
    pub create_mode: VfsCreateMode,
    pub conflict_behavior: VfsConflictBehavior,
}

impl Default for VfsWriteOptions {
    fn default() -> Self {
        Self {
            durability: VfsWriteDurability::ReplicatedEventual,
            consistency: VfsConsistencyTarget::ReadYourWrites,
            create_mode: VfsCreateMode::CreateOrReplace,
            conflict_behavior: VfsConflictBehavior::Fail,
        }
    }
}

impl VfsWriteOptions {
    pub fn local_replace() -> Self {
        Self {
            durability: VfsWriteDurability::Local,
            ..Self::default()
        }
    }

    pub fn service_artifact() -> Self {
        Self {
            durability: VfsWriteDurability::ReplicatedEventual,
            consistency: VfsConsistencyTarget::BestEffort,
            create_mode: VfsCreateMode::CreateOrReplace,
            conflict_behavior: VfsConflictBehavior::Overwrite,
        }
    }

    pub fn create_new(mut self) -> Self {
        self.create_mode = VfsCreateMode::CreateNew;
        self
    }

    pub fn replace_existing(mut self) -> Self {
        self.create_mode = VfsCreateMode::ReplaceExisting;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VfsRemoveOptions {
    pub directory: bool,
    pub missing_ok: bool,
}

impl Default for VfsRemoveOptions {
    fn default() -> Self {
        Self {
            directory: false,
            missing_ok: false,
        }
    }
}

impl VfsRemoveOptions {
    pub fn file() -> Self {
        Self::default()
    }

    pub fn directory() -> Self {
        Self {
            directory: true,
            missing_ok: false,
        }
    }

    pub fn missing_ok(mut self) -> Self {
        self.missing_ok = true;
        self
    }
}

pub trait VfsBackend: Send + Sync + std::fmt::Debug {
    fn backend_name(&self) -> &'static str;
    fn stat(&self, path: &str) -> Result<VfsMetadata>;
    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>>;
    fn read(&self, path: &str) -> Result<Vec<u8>>;
    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead>;
    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()>;
    fn create_dir_all(&self, path: &str) -> Result<()>;
    fn rename(&self, from: &str, to: &str) -> Result<()>;
    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct VfsHandle {
    backend: Arc<dyn VfsBackend>,
    root: String,
}

impl VfsHandle {
    pub fn new(backend: Arc<dyn VfsBackend>, root: impl AsRef<str>) -> Result<Self> {
        let root = normalize_absolute_path(root.as_ref())?;
        Ok(Self { backend, root })
    }

    pub fn local(root: impl Into<PathBuf>) -> Result<Self> {
        Self::new(Arc::new(LocalVfsBackend::new(root.into())), "/")
    }

    pub fn mounted_sifs(data_dir: Option<PathBuf>) -> Result<Self> {
        Ok(Self {
            backend: Arc::new(MountedSifsVfsBackend::open(data_dir)?),
            root: "/".to_string(),
        })
    }

    pub fn daemon_sifs(address: impl Into<String>) -> Result<Self> {
        Ok(Self {
            backend: Arc::new(DaemonSifsVfsBackend::new(address.into())),
            root: "/".to_string(),
        })
    }

    pub fn memory() -> Self {
        Self {
            backend: Arc::new(MemoryVfsBackend::new()),
            root: "/".to_string(),
        }
    }

    pub fn scoped(&self, root: impl AsRef<str>) -> Result<Self> {
        let root = self.resolve(root.as_ref())?;
        Ok(Self {
            backend: Arc::clone(&self.backend),
            root,
        })
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn backend_name(&self) -> &'static str {
        self.backend.backend_name()
    }

    pub fn stat(&self, path: impl AsRef<str>) -> Result<VfsMetadata> {
        let path = self.resolve(path.as_ref())?;
        self.backend.stat(&path)
    }

    pub fn list(&self, path: impl AsRef<str>) -> Result<Vec<VfsDirEntry>> {
        let path = self.resolve(path.as_ref())?;
        self.backend.list(&path)
    }

    pub fn list_if_exists(&self, path: impl AsRef<str>) -> Result<Vec<VfsDirEntry>> {
        let path = self.resolve(path.as_ref())?;
        match self.backend.list(&path) {
            Ok(entries) => Ok(entries),
            Err(VfsError::NotFound { .. }) => Ok(Vec::new()),
            Err(source) => Err(source),
        }
    }

    pub fn read(&self, path: impl AsRef<str>) -> Result<Vec<u8>> {
        let path = self.resolve(path.as_ref())?;
        self.backend.read(&path)
    }

    pub fn read_optional(&self, path: impl AsRef<str>) -> Result<Option<Vec<u8>>> {
        let path = self.resolve(path.as_ref())?;
        match self.backend.read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(VfsError::NotFound { .. }) => Ok(None),
            Err(source) => Err(source),
        }
    }

    pub fn read_prefix(&self, path: impl AsRef<str>, max_bytes: u64) -> Result<VfsFileRead> {
        let path = self.resolve(path.as_ref())?;
        self.backend.read_prefix(&path, max_bytes)
    }

    pub fn write(&self, path: impl AsRef<str>, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.write_with_options(path, bytes, VfsWriteOptions::default())
    }

    pub fn write_with_options(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: VfsWriteOptions,
    ) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend.write(&path, bytes.as_ref(), &options)
    }

    pub fn create_dir_all(&self, path: impl AsRef<str>) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend.create_dir_all(&path)
    }

    pub fn rename(&self, from: impl AsRef<str>, to: impl AsRef<str>) -> Result<()> {
        let from = self.resolve(from.as_ref())?;
        let to = self.resolve(to.as_ref())?;
        self.backend.rename(&from, &to)
    }

    pub fn remove(&self, path: impl AsRef<str>) -> Result<()> {
        self.remove_with_options(path, VfsRemoveOptions::default())
    }

    pub fn remove_with_options(
        &self,
        path: impl AsRef<str>,
        options: VfsRemoveOptions,
    ) -> Result<()> {
        let path = self.resolve(path.as_ref())?;
        self.backend.remove(&path, &options)
    }

    fn resolve(&self, input: &str) -> Result<String> {
        let path = if input.is_empty() || input == "." {
            self.root.clone()
        } else if input.starts_with('/') {
            normalize_absolute_path(input)?
        } else {
            normalize_absolute_path(&join_vfs_path(&self.root, input))?
        };
        if path == self.root || path.starts_with(&format!("{}/", self.root.trim_end_matches('/'))) {
            Ok(path)
        } else {
            Err(VfsError::OutsideScope {
                root: self.root.clone(),
                path,
            })
        }
    }
}

#[derive(Debug)]
pub struct LocalVfsBackend {
    root: PathBuf,
}

impl LocalVfsBackend {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn host_path(&self, path: &str) -> PathBuf {
        let relative = path.trim_start_matches('/');
        if relative.is_empty() {
            self.root.clone()
        } else {
            self.root.join(relative)
        }
    }

    fn map_io(&self, path: &str, source: io::Error) -> VfsError {
        match source.kind() {
            io::ErrorKind::NotFound => VfsError::NotFound {
                path: path.to_string(),
            },
            io::ErrorKind::AlreadyExists => VfsError::AlreadyExists {
                path: path.to_string(),
            },
            _ => VfsError::Io {
                path: path.to_string(),
                source,
            },
        }
    }
}

impl VfsBackend for LocalVfsBackend {
    fn backend_name(&self) -> &'static str {
        "local"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let metadata = fs::symlink_metadata(self.host_path(path))
            .map_err(|source| self.map_io(path, source))?;
        Ok(VfsMetadata {
            path: path.to_string(),
            kind: local_file_kind(&metadata),
            size_bytes: metadata.is_file().then_some(metadata.len()),
            content_hash: None,
            version: None,
        })
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        let mut entries = Vec::new();
        for entry in
            fs::read_dir(self.host_path(path)).map_err(|source| self.map_io(path, source))?
        {
            let entry = entry.map_err(|source| self.map_io(path, source))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let child_path = join_vfs_path(path, &name);
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|source| self.map_io(&child_path, source))?;
            entries.push(VfsDirEntry {
                name,
                path: child_path,
                kind: local_file_kind(&metadata),
                size_bytes: metadata.is_file().then_some(metadata.len()),
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        fs::read(self.host_path(path)).map_err(|source| self.map_io(path, source))
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let host_path = self.host_path(path);
        let metadata = fs::metadata(&host_path).map_err(|source| self.map_io(path, source))?;
        let mut file = fs::File::open(host_path).map_err(|source| self.map_io(path, source))?;
        let mut bytes = Vec::new();
        file.by_ref()
            .take(max_bytes)
            .read_to_end(&mut bytes)
            .map_err(|source| self.map_io(path, source))?;
        Ok(VfsFileRead {
            bytes,
            truncated: metadata.len() > max_bytes,
        })
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        enforce_local_create_mode(path, &self.host_path(path), options)?;
        fs::write(self.host_path(path), bytes).map_err(|source| self.map_io(path, source))
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        fs::create_dir_all(self.host_path(path)).map_err(|source| self.map_io(path, source))
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        fs::rename(self.host_path(from), self.host_path(to))
            .map_err(|source| self.map_io(from, source))
    }

    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        let result = if options.directory {
            fs::remove_dir(self.host_path(path))
        } else {
            fs::remove_file(self.host_path(path))
        };
        match result {
            Ok(()) => Ok(()),
            Err(source) if options.missing_ok && source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(self.map_io(path, source)),
        }
    }
}

#[derive(Debug)]
pub struct MountedSifsVfsBackend {
    opened: Mutex<OpenedMountedSifsService>,
}

impl MountedSifsVfsBackend {
    pub fn open(data_dir: Option<PathBuf>) -> Result<Self> {
        let opened = sinomo_dfs::open_mounted_sifs_service(data_dir).map_err(|source| {
            VfsError::Backend {
                backend: "mounted-sifs".to_string(),
                path: "/".to_string(),
                detail: source.to_string(),
            }
        })?;
        Ok(Self {
            opened: Mutex::new(opened),
        })
    }

    fn lock(&self, path: &str) -> Result<std::sync::MutexGuard<'_, OpenedMountedSifsService>> {
        self.opened.lock().map_err(|_| VfsError::Backend {
            backend: self.backend_name().to_string(),
            path: path.to_string(),
            detail: "mounted SIFS backend lock is poisoned".to_string(),
        })
    }

    fn stat_exists(opened: &mut OpenedMountedSifsService, path: &str) -> Result<bool> {
        match opened.service.posix_stat_distributed(path) {
            Ok(_) => Ok(true),
            Err(DfsPosixError::NotFound { .. }) => Ok(false),
            Err(source) => Err(dfs_error_to_vfs("mounted-sifs", path, source)),
        }
    }
}

impl VfsBackend for MountedSifsVfsBackend {
    fn backend_name(&self) -> &'static str {
        "mounted-sifs"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
        let stat = opened
            .service
            .posix_stat_distributed(path)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(metadata_from_dfs_stat(stat))
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        let mut opened = self.lock(path)?;
        let entries = opened
            .service
            .posix_list_dir_distributed(path)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(entries.into_iter().map(dir_entry_from_dfs).collect())
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let mut opened = self.lock(path)?;
        opened
            .service
            .posix_read_file_distributed(path)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let mut opened = self.lock(path)?;
        let stat = opened
            .service
            .posix_stat_distributed(path)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        let length = usize::try_from(max_bytes).map_err(|_| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "read prefix length does not fit this platform".to_string(),
        })?;
        let bytes = opened
            .service
            .posix_read_file_range(path, 0, length)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(VfsFileRead {
            bytes,
            truncated: stat.size_bytes.unwrap_or(0) > max_bytes,
        })
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        let mut opened = self.lock(path)?;
        enforce_sifs_create_mode(&mut opened, path, options, self.backend_name())?;
        let mut candidates = if options.durability == VfsWriteDurability::Local {
            vec![opened.service.node_id().clone()]
        } else {
            opened.replica_candidates.clone()
        };
        if !candidates.contains(opened.service.node_id()) {
            candidates.insert(0, opened.service.node_id().clone());
        }
        let mut dfs_options = DfsWriteOptions::replicated_eventual(candidates);
        dfs_options.foreground_replica_repair =
            options.consistency == VfsConsistencyTarget::ReadYourWrites;
        opened
            .service
            .posix_write_file(path, bytes, dfs_options)
            .map(drop)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        let mut opened = self.lock(path)?;
        opened
            .service
            .posix_mkdir_all(path)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let mut opened = self.lock(from)?;
        opened
            .service
            .posix_rename(from, to)
            .map(drop)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), from, source))
    }

    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        let mut opened = self.lock(path)?;
        let result = if options.directory {
            opened.service.posix_rmdir(path)
        } else {
            opened.service.posix_unlink(path)
        };
        match result {
            Ok(_) => Ok(()),
            Err(DfsPosixError::NotFound { .. }) if options.missing_ok => Ok(()),
            Err(source) => Err(dfs_error_to_vfs(self.backend_name(), path, source)),
        }
    }
}

#[derive(Debug)]
pub struct DaemonSifsVfsBackend {
    address: String,
    timeout: Duration,
}

impl DaemonSifsVfsBackend {
    pub fn new(address: String) -> Self {
        Self {
            address,
            timeout: Duration::from_secs(5),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn rpc<Request, Response>(
        &self,
        path: &str,
        route: &str,
        request_body: &Request,
    ) -> Result<Response>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let request =
            ObjectRpcRequest::encode_body(route, sinomo_daemon::DAEMON_RPC_CATEGORY, request_body)
                .map_err(|source| {
                    daemon_detail_to_vfs(self.backend_name(), path, source.to_string())
                })?;
        let response = sinomo_daemon::send_daemon_rpc(&self.address, &request, self.timeout)
            .map_err(|source| {
                daemon_detail_to_vfs(self.backend_name(), path, source.to_string())
            })?;
        response
            .decode_body()
            .map_err(|source| daemon_detail_to_vfs(self.backend_name(), path, source.to_string()))
    }

    fn stat_exists(&self, path: &str) -> Result<bool> {
        match self.stat(path) {
            Ok(_) => Ok(true),
            Err(VfsError::NotFound { .. }) => Ok(false),
            Err(source) => Err(source),
        }
    }
}

impl VfsBackend for DaemonSifsVfsBackend {
    fn backend_name(&self) -> &'static str {
        "daemon-sifs"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let response: sinomo_daemon::DaemonDfsBrowserStatResponse = self.rpc(
            path,
            sinomo_daemon::DAEMON_DFS_BROWSER_STAT_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserStatRequest {
                path: path.to_string(),
            },
        )?;
        Ok(VfsMetadata {
            path: response.path,
            kind: vfs_kind_from_dfs(&response.kind),
            size_bytes: response.size_bytes,
            content_hash: response.content_hash,
            version: Some(response.version),
        })
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        let response: sinomo_daemon::DaemonDfsBrowserListResponse = self.rpc(
            path,
            sinomo_daemon::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow: true,
                missing_is_empty: false,
                sort_key: sinomo_daemon::DaemonDfsBrowserSortKey::Name,
                sort_direction: sinomo_daemon::DaemonDfsBrowserSortDirection::Ascending,
                offset: 0,
                limit: None,
            },
        )?;
        Ok(response
            .entries
            .into_iter()
            .map(|entry| VfsDirEntry {
                name: entry.name,
                path: entry.path,
                kind: vfs_kind_from_dfs(&entry.kind),
                size_bytes: entry.size,
            })
            .collect())
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let response: sinomo_daemon::DaemonDfsBrowserReadFileResponse = self.rpc(
            path,
            sinomo_daemon::DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserReadFileRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.bytes)
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let response: sinomo_daemon::DaemonDfsBrowserReadFilePrefixResponse = self.rpc(
            path,
            sinomo_daemon::DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserReadFilePrefixRequest {
                path: path.to_string(),
                max_bytes,
            },
        )?;
        Ok(VfsFileRead {
            bytes: response.bytes,
            truncated: response.truncated,
        })
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        enforce_daemon_create_mode(self, path, options)?;
        let _: sinomo_daemon::DaemonDfsBrowserWriteFileResponse = self.rpc(
            path,
            sinomo_daemon::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserWriteFileRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
            },
        )?;
        Ok(())
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        let _: sinomo_daemon::DaemonDfsBrowserCreateDirAllResponse = self.rpc(
            path,
            sinomo_daemon::DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserCreateDirAllRequest {
                path: path.to_string(),
            },
        )?;
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let _: sinomo_daemon::DaemonDfsBrowserRenameResponse = self.rpc(
            from,
            sinomo_daemon::DAEMON_DFS_BROWSER_RENAME_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserRenameRequest {
                from: from.to_string(),
                to: to.to_string(),
            },
        )?;
        Ok(())
    }

    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        if options.missing_ok && !self.stat_exists(path)? {
            return Ok(());
        }
        let _: sinomo_daemon::DaemonDfsBrowserDeleteResponse = self.rpc(
            path,
            sinomo_daemon::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            &sinomo_daemon::DaemonDfsBrowserDeleteRequest {
                path: path.to_string(),
                directory: options.directory,
            },
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MemoryVfsBackend {
    inner: Arc<Mutex<BTreeMap<String, MemoryNode>>>,
}

#[derive(Clone, Debug)]
enum MemoryNode {
    File(Vec<u8>),
    Directory,
}

impl MemoryVfsBackend {
    pub fn new() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert("/".to_string(), MemoryNode::Directory);
        Self {
            inner: Arc::new(Mutex::new(nodes)),
        }
    }

    fn lock(&self, path: &str) -> Result<std::sync::MutexGuard<'_, BTreeMap<String, MemoryNode>>> {
        self.inner.lock().map_err(|_| VfsError::Backend {
            backend: self.backend_name().to_string(),
            path: path.to_string(),
            detail: "memory VFS backend lock is poisoned".to_string(),
        })
    }
}

impl Default for MemoryVfsBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl VfsBackend for MemoryVfsBackend {
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let nodes = self.lock(path)?;
        let node = nodes.get(path).ok_or_else(|| VfsError::NotFound {
            path: path.to_string(),
        })?;
        Ok(memory_metadata(path, node))
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        let nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::Directory) => {}
            Some(MemoryNode::File(_)) => {
                return Err(VfsError::NotDirectory {
                    path: path.to_string(),
                });
            }
            None => {
                return Err(VfsError::NotFound {
                    path: path.to_string(),
                });
            }
        }
        let mut entries = Vec::new();
        for (candidate, node) in nodes.iter() {
            if candidate == path {
                continue;
            }
            if parent_vfs_path(candidate).as_deref() == Some(path) {
                entries.push(VfsDirEntry {
                    name: file_name(candidate).unwrap_or_default().to_string(),
                    path: candidate.clone(),
                    kind: memory_kind(node),
                    size_bytes: match node {
                        MemoryNode::File(bytes) => Some(bytes.len() as u64),
                        MemoryNode::Directory => None,
                    },
                });
            }
        }
        Ok(entries)
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::File(bytes)) => Ok(bytes.clone()),
            Some(MemoryNode::Directory) => Err(VfsError::IsDirectory {
                path: path.to_string(),
            }),
            None => Err(VfsError::NotFound {
                path: path.to_string(),
            }),
        }
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::File(bytes)) => {
                let len = bytes.len().min(max_bytes as usize);
                Ok(VfsFileRead {
                    bytes: bytes[..len].to_vec(),
                    truncated: bytes.len() as u64 > max_bytes,
                })
            }
            Some(MemoryNode::Directory) => Err(VfsError::IsDirectory {
                path: path.to_string(),
            }),
            None => Err(VfsError::NotFound {
                path: path.to_string(),
            }),
        }
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        let mut nodes = self.lock(path)?;
        enforce_memory_create_mode(&nodes, path, options)?;
        let parent = parent_vfs_path(path).ok_or_else(|| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "cannot write root as a file".to_string(),
        })?;
        match nodes.get(&parent) {
            Some(MemoryNode::Directory) => {}
            Some(MemoryNode::File(_)) => {
                return Err(VfsError::NotDirectory { path: parent });
            }
            None => {
                return Err(VfsError::NotFound { path: parent });
            }
        }
        nodes.insert(path.to_string(), MemoryNode::File(bytes.to_vec()));
        Ok(())
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        let mut nodes = self.lock(path)?;
        let mut current = "/".to_string();
        for segment in path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
        {
            current = join_vfs_path(&current, segment);
            match nodes.get(&current) {
                Some(MemoryNode::Directory) => {}
                Some(MemoryNode::File(_)) => {
                    return Err(VfsError::NotDirectory {
                        path: current.clone(),
                    });
                }
                None => {
                    nodes.insert(current.clone(), MemoryNode::Directory);
                }
            }
        }
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let mut nodes = self.lock(from)?;
        if !nodes.contains_key(from) {
            return Err(VfsError::NotFound {
                path: from.to_string(),
            });
        }
        let parent = parent_vfs_path(to).ok_or_else(|| VfsError::InvalidPath {
            path: to.to_string(),
            detail: "cannot rename onto root".to_string(),
        })?;
        match nodes.get(&parent) {
            Some(MemoryNode::Directory) => {}
            Some(MemoryNode::File(_)) => {
                return Err(VfsError::NotDirectory { path: parent });
            }
            None => return Err(VfsError::NotFound { path: parent }),
        }
        let moving = nodes
            .iter()
            .filter(|(path, _)| *path == from || path.starts_with(&format!("{from}/")))
            .map(|(path, node)| (path.clone(), node.clone()))
            .collect::<Vec<_>>();
        if moving.is_empty() {
            return Err(VfsError::NotFound {
                path: from.to_string(),
            });
        }
        for (path, _) in &moving {
            nodes.remove(path);
        }
        for (path, node) in moving {
            let suffix = path.strip_prefix(from).unwrap_or_default();
            nodes.insert(format!("{to}{suffix}"), node);
        }
        Ok(())
    }

    fn remove(&self, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        let mut nodes = self.lock(path)?;
        match nodes.get(path) {
            Some(MemoryNode::Directory) if !options.directory => {
                return Err(VfsError::IsDirectory {
                    path: path.to_string(),
                });
            }
            Some(MemoryNode::File(_)) if options.directory => {
                return Err(VfsError::NotDirectory {
                    path: path.to_string(),
                });
            }
            Some(MemoryNode::Directory) => {
                let child_prefix = format!("{}/", path.trim_end_matches('/'));
                if nodes
                    .keys()
                    .any(|candidate| candidate.starts_with(&child_prefix))
                {
                    return Err(VfsError::DirectoryNotEmpty {
                        path: path.to_string(),
                    });
                }
            }
            Some(MemoryNode::File(_)) => {}
            None if options.missing_ok => return Ok(()),
            None => {
                return Err(VfsError::NotFound {
                    path: path.to_string(),
                });
            }
        }
        nodes.remove(path);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct JsonFileStore<T> {
    handle: VfsHandle,
    path: String,
    pretty: bool,
    _marker: PhantomData<T>,
}

impl<T> JsonFileStore<T> {
    pub fn new(handle: VfsHandle, path: impl Into<String>) -> Self {
        Self {
            handle,
            path: path.into(),
            pretty: true,
            _marker: PhantomData,
        }
    }

    pub fn compact(mut self) -> Self {
        self.pretty = false;
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<T> JsonFileStore<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn load_optional(&self) -> Result<Option<T>> {
        let Some(bytes) = self.handle.read_optional(&self.path)? else {
            return Ok(None);
        };
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|source| VfsError::Json {
                path: self.path.clone(),
                detail: source.to_string(),
            })
    }

    pub fn load(&self) -> Result<T> {
        let bytes = self.handle.read(&self.path)?;
        serde_json::from_slice(&bytes).map_err(|source| VfsError::Json {
            path: self.path.clone(),
            detail: source.to_string(),
        })
    }

    pub fn save(&self, value: &T) -> Result<()> {
        self.save_with_options(value, VfsWriteOptions::service_artifact())
    }

    pub fn save_with_options(&self, value: &T, options: VfsWriteOptions) -> Result<()> {
        let bytes = if self.pretty {
            serde_json::to_vec_pretty(value)
        } else {
            serde_json::to_vec(value)
        }
        .map_err(|source| VfsError::Json {
            path: self.path.clone(),
            detail: source.to_string(),
        })?;
        self.handle.write_with_options(&self.path, bytes, options)
    }
}

#[derive(Clone, Debug)]
pub struct SobjFileStore<T> {
    handle: VfsHandle,
    path: String,
    version: u16,
    length_mode: LengthMode,
    _marker: PhantomData<T>,
}

impl<T> SobjFileStore<T> {
    pub fn new(handle: VfsHandle, path: impl Into<String>, version: u16) -> Self {
        Self {
            handle,
            path: path.into(),
            version,
            length_mode: LengthMode::U32,
            _marker: PhantomData,
        }
    }

    pub fn with_length_mode(mut self, length_mode: LengthMode) -> Self {
        self.length_mode = length_mode;
        self
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<T> SobjFileStore<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn load_optional(&self) -> Result<Option<T>> {
        let Some(bytes) = self.handle.read_optional(&self.path)? else {
            return Ok(None);
        };
        serde_ext::from_bytes(&bytes)
            .map(Some)
            .map_err(|source| VfsError::Sobj {
                path: self.path.clone(),
                detail: source.to_string(),
            })
    }

    pub fn load(&self) -> Result<T> {
        let bytes = self.handle.read(&self.path)?;
        serde_ext::from_bytes(&bytes).map_err(|source| VfsError::Sobj {
            path: self.path.clone(),
            detail: source.to_string(),
        })
    }

    pub fn save(&self, value: &T) -> Result<()> {
        self.save_with_options(value, VfsWriteOptions::service_artifact())
    }

    pub fn save_with_options(&self, value: &T, options: VfsWriteOptions) -> Result<()> {
        let bytes =
            serde_ext::to_bytes(value, self.version, self.length_mode).map_err(|source| {
                VfsError::Sobj {
                    path: self.path.clone(),
                    detail: source.to_string(),
                }
            })?;
        self.handle.write_with_options(&self.path, bytes, options)
    }
}

#[derive(Clone, Debug)]
pub struct DirectoryObjectStore<T> {
    handle: VfsHandle,
    extension: String,
    codec: DirectoryObjectCodec,
    pretty: bool,
    _marker: PhantomData<T>,
}

#[derive(Clone, Debug)]
enum DirectoryObjectCodec {
    Json,
    Sobj {
        version: u16,
        length_mode: LengthMode,
    },
}

impl<T> DirectoryObjectStore<T> {
    pub fn json(handle: VfsHandle, extension: impl Into<String>) -> Self {
        Self {
            handle,
            extension: normalize_extension(extension.into()),
            codec: DirectoryObjectCodec::Json,
            pretty: true,
            _marker: PhantomData,
        }
    }

    pub fn sobj(handle: VfsHandle, extension: impl Into<String>, version: u16) -> Self {
        Self {
            handle,
            extension: normalize_extension(extension.into()),
            codec: DirectoryObjectCodec::Sobj {
                version,
                length_mode: LengthMode::U32,
            },
            pretty: false,
            _marker: PhantomData,
        }
    }

    pub fn compact_json(mut self) -> Self {
        self.pretty = false;
        self
    }

    pub fn root(&self) -> &str {
        self.handle.root()
    }

    pub fn path_for_id(&self, id: &str) -> Result<String> {
        validate_object_id(id)?;
        Ok(format!("{}.{}", id, self.extension))
    }
}

impl<T> DirectoryObjectStore<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn load_all(&self) -> Result<Vec<T>> {
        let mut entries = self
            .handle
            .list_if_exists("")?
            .into_iter()
            .filter(|entry| {
                entry.kind == VfsObjectKind::File
                    && entry.name.ends_with(&format!(".{}", self.extension))
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        let mut values = Vec::with_capacity(entries.len());
        for entry in entries {
            let bytes = self.handle.read(&entry.path)?;
            values.push(self.decode(&entry.path, &bytes)?);
        }
        Ok(values)
    }

    pub fn load(&self, id: &str) -> Result<T> {
        let path = self.path_for_id(id)?;
        let bytes = self.handle.read(&path)?;
        self.decode(&path, &bytes)
    }

    pub fn load_optional(&self, id: &str) -> Result<Option<T>> {
        let path = self.path_for_id(id)?;
        let Some(bytes) = self.handle.read_optional(&path)? else {
            return Ok(None);
        };
        self.decode(&path, &bytes).map(Some)
    }

    pub fn save(&self, id: &str, value: &T) -> Result<String> {
        self.save_with_options(id, value, VfsWriteOptions::service_artifact())
    }

    pub fn save_with_options(
        &self,
        id: &str,
        value: &T,
        options: VfsWriteOptions,
    ) -> Result<String> {
        let path = self.path_for_id(id)?;
        self.handle.create_dir_all("")?;
        let bytes = self.encode(&path, value)?;
        self.handle.write_with_options(&path, bytes, options)?;
        Ok(join_vfs_path(self.handle.root(), &path))
    }

    pub fn delete(&self, id: &str) -> Result<bool> {
        let path = self.path_for_id(id)?;
        match self
            .handle
            .remove_with_options(&path, VfsRemoveOptions::file())
        {
            Ok(()) => Ok(true),
            Err(VfsError::NotFound { .. }) => Ok(false),
            Err(source) => Err(source),
        }
    }

    fn encode(&self, path: &str, value: &T) -> Result<Vec<u8>> {
        match self.codec {
            DirectoryObjectCodec::Json => if self.pretty {
                serde_json::to_vec_pretty(value)
            } else {
                serde_json::to_vec(value)
            }
            .map_err(|source| VfsError::Json {
                path: path.to_string(),
                detail: source.to_string(),
            }),
            DirectoryObjectCodec::Sobj {
                version,
                length_mode,
            } => {
                serde_ext::to_bytes(value, version, length_mode).map_err(|source| VfsError::Sobj {
                    path: path.to_string(),
                    detail: source.to_string(),
                })
            }
        }
    }

    fn decode(&self, path: &str, bytes: &[u8]) -> Result<T> {
        match self.codec {
            DirectoryObjectCodec::Json => {
                serde_json::from_slice(bytes).map_err(|source| VfsError::Json {
                    path: path.to_string(),
                    detail: source.to_string(),
                })
            }
            DirectoryObjectCodec::Sobj { .. } => {
                serde_ext::from_bytes(bytes).map_err(|source| VfsError::Sobj {
                    path: path.to_string(),
                    detail: source.to_string(),
                })
            }
        }
    }
}

fn normalize_absolute_path(path: &str) -> Result<String> {
    if !path.starts_with('/') {
        return Err(VfsError::InvalidPath {
            path: path.to_string(),
            detail: "path must be absolute".to_string(),
        });
    }
    let mut segments = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                return Err(VfsError::InvalidPath {
                    path: path.to_string(),
                    detail: "parent traversal is not allowed".to_string(),
                });
            }
            segment => segments.push(segment),
        }
    }
    if segments.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", segments.join("/")))
    }
}

fn join_vfs_path(root: &str, child: &str) -> String {
    if root == "/" {
        format!("/{}", child.trim_start_matches('/'))
    } else if child.is_empty() {
        root.to_string()
    } else {
        format!(
            "{}/{}",
            root.trim_end_matches('/'),
            child.trim_start_matches('/')
        )
    }
}

fn parent_vfs_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let trimmed = path.trim_end_matches('/');
    let index = trimmed.rfind('/')?;
    if index == 0 {
        Some("/".to_string())
    } else {
        Some(trimmed[..index].to_string())
    }
}

fn file_name(path: &str) -> Option<&str> {
    path.trim_end_matches('/').rsplit('/').next()
}

fn validate_object_id(id: &str) -> Result<()> {
    if id.is_empty() || id.contains('/') || id == "." || id == ".." {
        return Err(VfsError::InvalidPath {
            path: id.to_string(),
            detail: "object id must be a single path segment".to_string(),
        });
    }
    Ok(())
}

fn normalize_extension(extension: String) -> String {
    extension.trim_start_matches('.').to_string()
}

fn local_file_kind(metadata: &fs::Metadata) -> VfsObjectKind {
    if metadata.is_file() {
        VfsObjectKind::File
    } else if metadata.is_dir() {
        VfsObjectKind::Directory
    } else if metadata.file_type().is_symlink() {
        VfsObjectKind::Symlink
    } else {
        VfsObjectKind::Other
    }
}

fn enforce_local_create_mode(
    path: &str,
    host_path: &Path,
    options: &VfsWriteOptions,
) -> Result<()> {
    let exists = host_path.exists();
    enforce_create_mode(path, exists, options)
}

fn enforce_memory_create_mode(
    nodes: &BTreeMap<String, MemoryNode>,
    path: &str,
    options: &VfsWriteOptions,
) -> Result<()> {
    enforce_create_mode(path, nodes.contains_key(path), options)
}

fn enforce_sifs_create_mode(
    opened: &mut OpenedMountedSifsService,
    path: &str,
    options: &VfsWriteOptions,
    backend: &str,
) -> Result<()> {
    let exists = MountedSifsVfsBackend::stat_exists(opened, path)?;
    enforce_create_mode(path, exists, options).map_err(|source| match source {
        VfsError::Conflict { path, detail } => VfsError::Backend {
            backend: backend.to_string(),
            path,
            detail,
        },
        other => other,
    })
}

fn enforce_daemon_create_mode(
    backend: &DaemonSifsVfsBackend,
    path: &str,
    options: &VfsWriteOptions,
) -> Result<()> {
    let exists = backend.stat_exists(path)?;
    enforce_create_mode(path, exists, options)
}

fn enforce_create_mode(path: &str, exists: bool, options: &VfsWriteOptions) -> Result<()> {
    match options.create_mode {
        VfsCreateMode::CreateNew if exists => Err(VfsError::AlreadyExists {
            path: path.to_string(),
        }),
        VfsCreateMode::ReplaceExisting if !exists => Err(VfsError::NotFound {
            path: path.to_string(),
        }),
        _ => Ok(()),
    }
}

fn metadata_from_dfs_stat(stat: DfsStat) -> VfsMetadata {
    VfsMetadata {
        path: stat.path,
        kind: vfs_kind_from_dfs(&stat.kind),
        size_bytes: stat.size_bytes,
        content_hash: stat.content_hash,
        version: Some(stat.version.as_str().to_string()),
    }
}

fn dir_entry_from_dfs(entry: DfsDirEntry) -> VfsDirEntry {
    VfsDirEntry {
        name: entry.name,
        path: entry.path,
        kind: vfs_kind_from_dfs(&entry.kind),
        size_bytes: None,
    }
}

fn vfs_kind_from_dfs(kind: &DfsObjectKind) -> VfsObjectKind {
    match kind {
        DfsObjectKind::File => VfsObjectKind::File,
        DfsObjectKind::Directory => VfsObjectKind::Directory,
        DfsObjectKind::Symlink => VfsObjectKind::Symlink,
        DfsObjectKind::Hardlink => VfsObjectKind::Hardlink,
        DfsObjectKind::Artifact => VfsObjectKind::Artifact,
        DfsObjectKind::Mount => VfsObjectKind::Mount,
        DfsObjectKind::Tombstone => VfsObjectKind::Tombstone,
    }
}

fn dfs_error_to_vfs(backend: &str, path: &str, source: DfsPosixError) -> VfsError {
    match source {
        DfsPosixError::NotFound { path } => VfsError::NotFound { path },
        DfsPosixError::AlreadyExists { path } => VfsError::AlreadyExists { path },
        DfsPosixError::NotDirectory { path } => VfsError::NotDirectory { path },
        DfsPosixError::IsDirectory { path } => VfsError::IsDirectory { path },
        DfsPosixError::DirectoryNotEmpty { path } => VfsError::DirectoryNotEmpty { path },
        DfsPosixError::InvalidPath { path } => VfsError::InvalidPath {
            path,
            detail: "invalid SIFS path".to_string(),
        },
        DfsPosixError::Conflict { path, reason } => VfsError::Conflict {
            path,
            detail: reason,
        },
        other => VfsError::Backend {
            backend: backend.to_string(),
            path: path.to_string(),
            detail: other.to_string(),
        },
    }
}

fn daemon_detail_to_vfs(backend: &str, path: &str, detail: String) -> VfsError {
    let lower = detail.to_ascii_lowercase();
    if lower.contains("not found") {
        VfsError::NotFound {
            path: path.to_string(),
        }
    } else if lower.contains("already exists") {
        VfsError::AlreadyExists {
            path: path.to_string(),
        }
    } else if lower.contains("not a directory") {
        VfsError::NotDirectory {
            path: path.to_string(),
        }
    } else if lower.contains("is a directory") {
        VfsError::IsDirectory {
            path: path.to_string(),
        }
    } else if lower.contains("directory is not empty") {
        VfsError::DirectoryNotEmpty {
            path: path.to_string(),
        }
    } else if lower.contains("conflict") || lower.contains("conflicted") {
        VfsError::Conflict {
            path: path.to_string(),
            detail,
        }
    } else {
        VfsError::Backend {
            backend: backend.to_string(),
            path: path.to_string(),
            detail,
        }
    }
}

fn memory_kind(node: &MemoryNode) -> VfsObjectKind {
    match node {
        MemoryNode::File(_) => VfsObjectKind::File,
        MemoryNode::Directory => VfsObjectKind::Directory,
    }
}

fn memory_metadata(path: &str, node: &MemoryNode) -> VfsMetadata {
    VfsMetadata {
        path: path.to_string(),
        kind: memory_kind(node),
        size_bytes: match node {
            MemoryNode::File(bytes) => Some(bytes.len() as u64),
            MemoryNode::Directory => None,
        },
        content_hash: None,
        version: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn scoped_handle_rejects_parent_escape() {
        let handle = VfsHandle::memory()
            .scoped("/services/sinomo/agents")
            .unwrap();
        assert!(matches!(
            handle.write("../outside.txt", b"bad"),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            handle.write("/services/sinomo/other/file.txt", b"bad"),
            Err(VfsError::OutsideScope { .. })
        ));
    }

    #[test]
    fn memory_backend_reads_writes_lists_and_renames() {
        let handle = VfsHandle::memory()
            .scoped("/services/sinomo/agents")
            .unwrap();
        handle.create_dir_all("").unwrap();
        handle.write("alpha.txt", b"abcdef").unwrap();
        assert_eq!(handle.read("alpha.txt").unwrap(), b"abcdef");
        assert_eq!(
            handle.read_prefix("alpha.txt", 3).unwrap(),
            VfsFileRead {
                bytes: b"abc".to_vec(),
                truncated: true
            }
        );
        let entries = handle.list("").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "alpha.txt");
        handle.rename("alpha.txt", "beta.txt").unwrap();
        assert!(matches!(
            handle.read("alpha.txt"),
            Err(VfsError::NotFound { .. })
        ));
        assert_eq!(handle.read("beta.txt").unwrap(), b"abcdef");
    }

    #[test]
    fn write_create_modes_are_enforced() {
        let handle = VfsHandle::memory().scoped("/configs").unwrap();
        handle.create_dir_all("").unwrap();
        handle
            .write_with_options(
                "agent.json",
                b"{}",
                VfsWriteOptions::service_artifact().create_new(),
            )
            .unwrap();
        assert!(matches!(
            handle.write_with_options(
                "agent.json",
                b"{}",
                VfsWriteOptions::service_artifact().create_new()
            ),
            Err(VfsError::AlreadyExists { .. })
        ));
        handle
            .write_with_options(
                "agent.json",
                b"{\"ok\":true}",
                VfsWriteOptions::service_artifact().replace_existing(),
            )
            .unwrap();
    }

    #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
    struct TestObject {
        id: String,
        value: u32,
    }

    #[test]
    fn directory_object_store_round_trips_json() {
        let handle = VfsHandle::memory()
            .scoped("/services/sinomo/agents")
            .unwrap();
        let store = DirectoryObjectStore::<TestObject>::json(handle, "json");
        store
            .save(
                "agent-one",
                &TestObject {
                    id: "agent-one".to_string(),
                    value: 7,
                },
            )
            .unwrap();
        assert_eq!(
            store.load_all().unwrap(),
            vec![TestObject {
                id: "agent-one".to_string(),
                value: 7
            }]
        );
        assert!(store.delete("agent-one").unwrap());
        assert!(!store.delete("agent-one").unwrap());
        assert!(store.load_all().unwrap().is_empty());
    }

    #[test]
    fn local_backend_maps_absolute_vfs_paths_under_host_root() {
        let test_dir = sinomo_testdir::TestDir::new("sinomo-vfs-local");
        let handle = VfsHandle::local(test_dir.path()).unwrap();
        let scoped = handle.scoped("/services/sinomo/agents").unwrap();
        scoped.create_dir_all("").unwrap();
        scoped.write("agent.json", b"{}").unwrap();
        let host_path = test_dir
            .path()
            .join("services")
            .join("sinomo")
            .join("agents")
            .join("agent.json");
        assert_eq!(fs::read(host_path).unwrap(), b"{}");
    }
}
