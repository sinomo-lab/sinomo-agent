//! SIFS-backed backends for the rooted `sinomo-vfs` facade.
//!
//! This crate adapts SIFS/DFS storage into the generic VFS traits. Keeping it
//! separate lets crates that only need local or in-memory VFS handles depend on
//! `sinomo-vfs` without pulling in DFS or daemon implementation crates.

use serde::Serialize;
use serde::de::DeserializeOwned;
use sinomo_core::network::ObjectRpcRequest;
pub use sinomo_dfs::DfsCancellationFlag;
use sinomo_dfs::{
    DfsDirEntry, DfsObjectKind, DfsPosixError, DfsStat, DfsWriteOptions, OpenedMountedSifsService,
};
use sinomo_vfs::{
    Result, VfsAppendOptions, VfsBackend, VfsConsistencyTarget, VfsCreateMode, VfsDirEntry,
    VfsError, VfsFileRead, VfsHandle, VfsMetadata, VfsObjectKind, VfsRemoveOptions,
    VfsWriteDurability, VfsWriteOptions,
};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn mounted_sifs(data_dir: Option<PathBuf>) -> Result<VfsHandle> {
    VfsHandle::new(Arc::new(MountedSifsVfsBackend::open(data_dir)?), "/")
}

/// Mounted SIFS serving reads from this node's local mirror: stat/list/read
/// never route to peers or wait on distributed freshness, so they stay fast —
/// and never burn CPU in retry loops — while the cluster/DFS link is degraded
/// (a normal state on mobile devices). Writes still replicate normally.
///
/// Use for node-local catalog/config artifacts (agent definitions, skills)
/// where a stale-but-instant answer beats blocking on cluster consensus; the
/// local mirror converges through background replication.
pub fn mounted_sifs_local_reads(data_dir: Option<PathBuf>) -> Result<VfsHandle> {
    VfsHandle::new(
        Arc::new(MountedSifsVfsBackend::open_with_options(
            data_dir,
            MountedSifsOpenOptions {
                read_mode: MountedSifsReadMode::LocalMirror,
                cancellation: None,
            },
        )?),
        "/",
    )
}

/// Mounted SIFS with a cooperative cancellation flag: budgeted callers flip the
/// flag to make in-flight and subsequent distributed operations return a
/// `Cancelled` error promptly instead of running out their network timeouts.
pub fn mounted_sifs_with_cancellation(
    data_dir: Option<PathBuf>,
    cancellation: DfsCancellationFlag,
) -> Result<VfsHandle> {
    VfsHandle::new(
        Arc::new(MountedSifsVfsBackend::open_with_options(
            data_dir,
            MountedSifsOpenOptions {
                read_mode: MountedSifsReadMode::Distributed,
                cancellation: Some(cancellation),
            },
        )?),
        "/",
    )
}

pub fn daemon_sifs(address: impl Into<String>) -> Result<VfsHandle> {
    VfsHandle::new(Arc::new(DaemonSifsVfsBackend::new(address.into())), "/")
}

/// How mounted-SIFS reads resolve.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MountedSifsReadMode {
    /// Route through distributed freshness/consensus machinery (full SIFS
    /// semantics; can wait on peers).
    #[default]
    Distributed,
    /// Serve stat/list/read from the local metadata/content mirror only.
    LocalMirror,
}

#[derive(Clone, Debug, Default)]
pub struct MountedSifsOpenOptions {
    pub read_mode: MountedSifsReadMode,
    pub cancellation: Option<DfsCancellationFlag>,
}

#[derive(Debug)]
pub struct MountedSifsVfsBackend {
    opened: Mutex<OpenedMountedSifsService>,
    read_mode: MountedSifsReadMode,
}

impl MountedSifsVfsBackend {
    pub fn open(data_dir: Option<PathBuf>) -> Result<Self> {
        Self::open_with_options(data_dir, MountedSifsOpenOptions::default())
    }

    pub fn open_with_options(
        data_dir: Option<PathBuf>,
        options: MountedSifsOpenOptions,
    ) -> Result<Self> {
        let opened = sinomo_dfs::open_mounted_sifs_service_with_cancellation(
            data_dir,
            options.cancellation,
        )
        .map_err(|source| VfsError::Backend {
            backend: "mounted-sifs".to_string(),
            path: "/".to_string(),
            detail: source.to_string(),
        })?;
        Ok(Self {
            opened: Mutex::new(opened),
            read_mode: options.read_mode,
        })
    }

    fn local_reads(&self) -> bool {
        self.read_mode == MountedSifsReadMode::LocalMirror
    }

    fn lock(&self, path: &str) -> Result<std::sync::MutexGuard<'_, OpenedMountedSifsService>> {
        self.opened.lock().map_err(|_| VfsError::Backend {
            backend: self.backend_name().to_string(),
            path: path.to_string(),
            detail: "mounted SIFS backend lock is poisoned".to_string(),
        })
    }

    fn stat_exists(
        opened: &mut OpenedMountedSifsService,
        path: &str,
        local_reads: bool,
    ) -> Result<bool> {
        let stat = if local_reads {
            opened.service.posix_stat(path)
        } else {
            opened.service.posix_stat_distributed(path)
        };
        match stat {
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
        let stat = if self.local_reads() {
            opened.service.posix_stat(path)
        } else {
            opened.service.posix_stat_distributed(path)
        }
        .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(metadata_from_dfs_stat(stat))
    }

    fn list(&self, path: &str) -> Result<Vec<VfsDirEntry>> {
        let mut opened = self.lock(path)?;
        let entries = if self.local_reads() {
            opened.service.posix_list_dir(path)
        } else {
            opened.service.posix_list_dir_distributed(path)
        }
        .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(entries.into_iter().map(dir_entry_from_dfs).collect())
    }

    fn read(&self, path: &str) -> Result<Vec<u8>> {
        let mut opened = self.lock(path)?;
        if self.local_reads() {
            opened.service.posix_read_file(path)
        } else {
            opened.service.posix_read_file_distributed(path)
        }
        .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let mut opened = self.lock(path)?;
        let stat = if self.local_reads() {
            opened.service.posix_stat(path)
        } else {
            opened.service.posix_stat_distributed(path)
        }
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
        enforce_sifs_create_mode(
            &mut opened,
            path,
            options,
            self.backend_name(),
            self.local_reads(),
        )?;
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

    fn append(&self, path: &str, bytes: &[u8], options: &VfsAppendOptions) -> Result<()> {
        let mut opened = self.lock(path)?;
        let mut contents = match opened.service.posix_read_file_distributed(path) {
            Ok(bytes) => bytes,
            Err(DfsPosixError::NotFound { .. }) if options.create => Vec::new(),
            Err(source) => return Err(dfs_error_to_vfs(self.backend_name(), path, source)),
        };
        contents.extend_from_slice(bytes);
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
            .posix_write_file(path, &contents, dfs_options)
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
            ObjectRpcRequest::encode_body(route, sinomo_api::DAEMON_RPC_CATEGORY, request_body)
                .map_err(|source| {
                    daemon_detail_to_vfs(self.backend_name(), path, source.to_string())
                })?;
        let response =
            send_daemon_rpc(&self.address, &request, self.timeout).map_err(|source| {
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
        let response: sinomo_api::DaemonDfsBrowserStatResponse = self.rpc(
            path,
            sinomo_api::DAEMON_DFS_BROWSER_STAT_ROUTE,
            &sinomo_api::DaemonDfsBrowserStatRequest {
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
        let response: sinomo_api::DaemonDfsBrowserListResponse = self.rpc(
            path,
            sinomo_api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            &sinomo_api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow: true,
                missing_is_empty: false,
                sort_key: sinomo_api::DaemonDfsBrowserSortKey::Name,
                sort_direction: sinomo_api::DaemonDfsBrowserSortDirection::Ascending,
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
        let response: sinomo_api::DaemonDfsBrowserReadFileResponse = self.rpc(
            path,
            sinomo_api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            &sinomo_api::DaemonDfsBrowserReadFileRequest {
                path: path.to_string(),
            },
        )?;
        Ok(response.bytes)
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let response: sinomo_api::DaemonDfsBrowserReadFilePrefixResponse = self.rpc(
            path,
            sinomo_api::DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            &sinomo_api::DaemonDfsBrowserReadFilePrefixRequest {
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
        let _: sinomo_api::DaemonDfsBrowserWriteFileResponse = self.rpc(
            path,
            sinomo_api::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            &sinomo_api::DaemonDfsBrowserWriteFileRequest {
                path: path.to_string(),
                bytes: bytes.to_vec(),
            },
        )?;
        Ok(())
    }

    fn append(&self, path: &str, bytes: &[u8], options: &VfsAppendOptions) -> Result<()> {
        let mut contents = match self.read(path) {
            Ok(bytes) => bytes,
            Err(VfsError::NotFound { .. }) if options.create => Vec::new(),
            Err(source) => return Err(source),
        };
        contents.extend_from_slice(bytes);
        self.write(path, &contents, &write_options_from_append(options))
    }

    fn create_dir_all(&self, path: &str) -> Result<()> {
        let _: sinomo_api::DaemonDfsBrowserCreateDirAllResponse = self.rpc(
            path,
            sinomo_api::DAEMON_DFS_BROWSER_CREATE_DIR_ALL_ROUTE,
            &sinomo_api::DaemonDfsBrowserCreateDirAllRequest {
                path: path.to_string(),
            },
        )?;
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        let _: sinomo_api::DaemonDfsBrowserRenameResponse = self.rpc(
            from,
            sinomo_api::DAEMON_DFS_BROWSER_RENAME_ROUTE,
            &sinomo_api::DaemonDfsBrowserRenameRequest {
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
        let _: sinomo_api::DaemonDfsBrowserDeleteResponse = self.rpc(
            path,
            sinomo_api::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            &sinomo_api::DaemonDfsBrowserDeleteRequest {
                path: path.to_string(),
                directory: options.directory,
            },
        )?;
        Ok(())
    }
}

fn send_daemon_rpc(
    address: &str,
    request: &ObjectRpcRequest,
    timeout: Duration,
) -> std::result::Result<sinomo_core::network::ObjectRpcResponse, sinomo_core::network::NetworkError>
{
    let signed = sinomo_core::network::sign_daemon_rpc_request_for_local_node(request)?;
    let control_request = request.clone().with_auth(signed);
    block_on_daemon_client(async move {
        sinomo_core::network::send_daemon_control_rpc_request_async(
            address,
            &control_request,
            sinomo_core::network::DaemonRpcTimeouts::connect_and_idle(timeout),
            sinomo_core::network::DAEMON_CONTROL_RPC_MAX_FRAME_BYTES,
        )
        .await
    })
}

fn block_on_daemon_client<F, T>(
    future: F,
) -> std::result::Result<T, sinomo_core::network::NetworkError>
where
    F: std::future::Future<Output = std::result::Result<T, sinomo_core::network::NetworkError>>
        + Send,
    T: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return if matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        ) {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            Err(sinomo_core::network::NetworkError::Io {
                operation: "daemon VFS client runtime".to_string(),
                detail: "daemon VFS sync client cannot run inside a current-thread Tokio runtime"
                    .to_string(),
            })
        };
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| sinomo_core::network::NetworkError::Io {
            operation: "daemon VFS client runtime".to_string(),
            detail: source.to_string(),
        })?;
    let output = runtime.block_on(future);
    runtime.shutdown_timeout(Duration::from_millis(250));
    output
}

fn write_options_from_append(options: &VfsAppendOptions) -> VfsWriteOptions {
    VfsWriteOptions {
        durability: options.durability.clone(),
        consistency: options.consistency.clone(),
        create_mode: if options.create {
            VfsCreateMode::CreateOrReplace
        } else {
            VfsCreateMode::ReplaceExisting
        },
        conflict_behavior: options.conflict_behavior.clone(),
    }
}

fn enforce_sifs_create_mode(
    opened: &mut OpenedMountedSifsService,
    path: &str,
    options: &VfsWriteOptions,
    backend: &str,
    local_reads: bool,
) -> Result<()> {
    let exists = MountedSifsVfsBackend::stat_exists(opened, path, local_reads)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_data_dir(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "sinomo-vfs-sifs-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&root);
        root
    }

    #[test]
    fn local_mirror_reads_serve_same_node_writes() {
        let root = temp_data_dir("local-mirror");
        let distributed = mounted_sifs(Some(root.clone())).expect("open distributed handle");
        distributed
            .create_dir_all("catalog")
            .expect("create catalog dir");
        distributed
            .write_with_options(
                "catalog/entry.json",
                b"{\"id\":1}".to_vec(),
                VfsWriteOptions::service_artifact(),
            )
            .expect("write through distributed handle");
        drop(distributed);

        let local = mounted_sifs_local_reads(Some(root.clone())).expect("open local-reads handle");
        assert_eq!(
            local.read("catalog/entry.json").expect("local read"),
            b"{\"id\":1}".to_vec(),
            "local mirror should serve data written on this node"
        );
        let names = local
            .list_if_exists("catalog")
            .expect("local list")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"entry.json".to_string()), "{names:?}");
        assert!(local.stat("catalog/entry.json").is_ok());

        // Writes through a local-reads handle keep working (they replicate
        // normally); only the read side is pinned to the local mirror.
        local
            .write_with_options(
                "catalog/second.json",
                b"{}".to_vec(),
                VfsWriteOptions::service_artifact(),
            )
            .expect("write through local-reads handle");
        assert_eq!(
            local.read("catalog/second.json").expect("read own write"),
            b"{}".to_vec()
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cancellation_flag_fails_distributed_reads_fast() {
        let root = temp_data_dir("cancel");
        let cancellation = DfsCancellationFlag::new();
        let handle = mounted_sifs_with_cancellation(Some(root.clone()), cancellation.clone())
            .expect("open cancellable handle");

        cancellation.cancel();

        let error = handle
            .read("catalog/missing.json")
            .expect_err("cancelled distributed read must fail fast");
        assert!(
            error.to_string().to_ascii_lowercase().contains("cancelled"),
            "unexpected error: {error}"
        );

        let _ = std::fs::remove_dir_all(root);
    }
}
