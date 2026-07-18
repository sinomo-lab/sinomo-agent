//! SIFS-backed backends for the rooted `sinomo-vfs` facade.
//!
//! This crate adapts SIFS/DFS storage into the generic VFS traits. Keeping it
//! separate lets crates that only need local or in-memory VFS handles depend on
//! `sinomo-vfs` without pulling in DFS or daemon implementation crates.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_core::network::ObjectRpcRequest;
pub use sinomo_dfs::DfsCancellationFlag;
use sinomo_dfs::{
    DfsDirEntry, DfsDirectoryPageCursor, DfsObjectId, DfsObjectKind, DfsPosixError, DfsStat,
    DfsVersion, DfsWriteOptions, OpenedMountedSifsService,
};
use sinomo_vfs::{
    Result, VfsAppendOptions, VfsBackend, VfsBackendDirectoryPage, VfsConsistencyTarget,
    VfsCreateMode, VfsDirEntry, VfsError, VfsFileRead, VfsHandle, VfsMetadata, VfsObjectKind,
    VfsOpenedFileRead, VfsRemoveOptions, VfsWriteDurability, VfsWriteOptions,
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

#[derive(Debug, Deserialize, Serialize)]
struct MountedSifsNoLinkDirectoryCursor {
    directory_object_id: String,
    directory_version: String,
    cursor: DfsDirectoryPageCursor,
}

impl MountedSifsVfsBackend {
    pub fn open(data_dir: Option<PathBuf>) -> Result<Self> {
        Self::open_with_options(data_dir, MountedSifsOpenOptions::default())
    }

    pub fn open_with_options(
        data_dir: Option<PathBuf>,
        options: MountedSifsOpenOptions,
    ) -> Result<Self> {
        let opened =
            sinomo_dfs::open_mounted_sifs_service_with_cancellation(data_dir, options.cancellation)
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

    fn lstat(&self, path: &str) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
        let stat = if self.local_reads() {
            opened.service.posix_namespace_lstat(path)
        } else {
            opened.service.posix_namespace_lstat_distributed(path)
        }
        .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(metadata_from_dfs_stat(stat))
    }

    fn stat_beneath_no_links(&self, root: &str, path: &str) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
        let stat = opened
            .service
            .posix_open_beneath_no_links_distributed(root, path)
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

    fn list_page(
        &self,
        path: &str,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> Result<VfsBackendDirectoryPage> {
        let after = after
            .map(|cursor| decode_directory_cursor(path, cursor))
            .transpose()?;
        let mut opened = self.lock(path)?;
        let page = if self.local_reads() {
            opened
                .service
                .posix_list_dir_page(path, after.as_ref(), max_entries)
        } else {
            opened
                .service
                .posix_list_dir_distributed_page(path, after.as_ref(), max_entries)
        }
        .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(VfsBackendDirectoryPage {
            directory_metadata: None,
            entries: page.entries.into_iter().map(dir_entry_from_dfs).collect(),
            next_cursor: page
                .next_cursor
                .as_ref()
                .map(|cursor| encode_directory_cursor(path, cursor))
                .transpose()?,
        })
    }

    fn list_page_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> Result<VfsBackendDirectoryPage> {
        let after = after
            .map(|cursor| decode_no_link_directory_cursor(path, cursor))
            .transpose()?;
        let mut opened = self.lock(path)?;
        let (directory, page) = opened
            .service
            .posix_list_dir_beneath_no_links_distributed_page(
                root,
                path,
                after.as_ref().map(|cursor| &cursor.cursor),
                max_entries,
            )
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        if let Some(after) = after.as_ref()
            && (after.directory_object_id != directory.object_id.as_str()
                || after.directory_version != directory.version.as_str())
        {
            return Err(VfsError::InvalidCursor {
                detail: format!("mounted-SIFS no-link directory changed while paging {path}"),
            });
        }
        Ok(VfsBackendDirectoryPage {
            directory_metadata: Some(metadata_from_dfs_stat(directory.clone())),
            entries: page.entries.into_iter().map(dir_entry_from_dfs).collect(),
            next_cursor: page
                .next_cursor
                .as_ref()
                .map(|cursor| {
                    encode_no_link_directory_cursor(
                        path,
                        &directory.object_id,
                        &directory.version,
                        cursor,
                    )
                })
                .transpose()?,
        })
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
        let bytes = if self.local_reads() {
            opened.service.posix_read_file_range(path, 0, length)
        } else {
            opened
                .service
                .posix_read_file_range_distributed(path, 0, length)
        }
        .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(VfsFileRead {
            bytes,
            truncated: stat.size_bytes.unwrap_or(0) > max_bytes,
        })
    }

    fn read_prefix_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        let length = usize::try_from(max_bytes).map_err(|_| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "read prefix length does not fit this platform".to_string(),
        })?;
        let mut opened = self.lock(path)?;
        let (stat, read) = opened
            .service
            .posix_read_file_prefix_beneath_no_links_distributed(root, path, length)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(VfsOpenedFileRead {
            metadata: metadata_from_dfs_stat(stat),
            bytes: read.bytes,
            truncated: read.truncated,
        })
    }

    fn read_range_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        offset: u64,
        max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        let length = usize::try_from(max_bytes).map_err(|_| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "read range length does not fit this platform".to_string(),
        })?;
        let mut opened = self.lock(path)?;
        let (stat, read) = opened
            .service
            .posix_read_file_range_beneath_no_links_distributed(root, path, offset, length)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        Ok(VfsOpenedFileRead {
            metadata: metadata_from_dfs_stat(stat),
            bytes: read.bytes,
            truncated: read.truncated,
        })
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        let mut opened = self.lock(path)?;
        if options.create_mode != VfsCreateMode::CreateNew {
            enforce_sifs_create_mode(
                &mut opened,
                path,
                options,
                self.backend_name(),
                self.local_reads(),
            )?;
        }
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
        let write = if options.create_mode == VfsCreateMode::CreateNew {
            opened.service.posix_create_file(path, bytes, dfs_options)
        } else {
            opened.service.posix_write_file(path, bytes, dfs_options)
        };
        write
            .map(drop)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn create_new_versioned(
        &self,
        path: &str,
        bytes: &[u8],
        options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
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
            .posix_create_file(path, bytes, dfs_options)
            .map(metadata_from_dfs_stat)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn create_new_versioned_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        bytes: &[u8],
        options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
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
            .posix_create_file_beneath_no_links(root, path, bytes, dfs_options)
            .map(metadata_from_dfs_stat)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn replace_if_version(
        &self,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        bytes: &[u8],
        options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
        let expected_object_id =
            DfsObjectId::new(expected_object_id).map_err(|source| VfsError::InvalidPath {
                path: path.to_string(),
                detail: format!("invalid expected object identity: {source}"),
            })?;
        let expected_version =
            DfsVersion::new(expected_version).map_err(|source| VfsError::InvalidPath {
                path: path.to_string(),
                detail: format!("invalid expected object version: {source}"),
            })?;
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
            .posix_replace_file_if_version(
                path,
                &expected_object_id,
                &expected_version,
                bytes,
                dfs_options,
            )
            .map(metadata_from_dfs_stat)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn replace_if_version_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        expected_object_id: &str,
        expected_version: &str,
        bytes: &[u8],
        options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
        let expected_object_id =
            DfsObjectId::new(expected_object_id).map_err(|source| VfsError::InvalidPath {
                path: path.to_string(),
                detail: format!("invalid expected object identity: {source}"),
            })?;
        let expected_version =
            DfsVersion::new(expected_version).map_err(|source| VfsError::InvalidPath {
                path: path.to_string(),
                detail: format!("invalid expected object version: {source}"),
            })?;
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
            .posix_replace_file_if_version_beneath_no_links(
                root,
                path,
                &expected_object_id,
                &expected_version,
                bytes,
                dfs_options,
            )
            .map(metadata_from_dfs_stat)
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

    fn create_dir_all_beneath_no_links(&self, root: &str, path: &str) -> Result<()> {
        let mut opened = self.lock(path)?;
        opened
            .service
            .posix_mkdir_all_beneath_no_links(root, path)
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
            timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
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
        let response = sinomo_api::send_daemon_rpc(&self.address, &request, self.timeout).map_err(
            |source| daemon_detail_to_vfs(self.backend_name(), path, source.to_string()),
        )?;
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
                max_staleness_millis: 0,
            },
        )?;
        Ok(VfsMetadata {
            path: response.path,
            kind: vfs_kind_from_dfs(&response.kind),
            object_id: Some(response.object_id),
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
                include_metadata: false,
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
        object_id: Some(stat.object_id.to_string()),
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
        size_bytes: entry.size_bytes,
    }
}

fn encode_directory_cursor(path: &str, cursor: &DfsDirectoryPageCursor) -> Result<Vec<u8>> {
    serde_json::to_vec(cursor).map_err(|source| VfsError::Backend {
        backend: "mounted-sifs".to_string(),
        path: path.to_string(),
        detail: format!("cannot encode DFS directory cursor: {source}"),
    })
}

fn decode_directory_cursor(path: &str, cursor: &[u8]) -> Result<DfsDirectoryPageCursor> {
    serde_json::from_slice(cursor).map_err(|source| VfsError::InvalidCursor {
        detail: format!("invalid mounted-SIFS cursor for {path}: {source}"),
    })
}

fn encode_no_link_directory_cursor(
    path: &str,
    directory_object_id: &DfsObjectId,
    directory_version: &DfsVersion,
    cursor: &DfsDirectoryPageCursor,
) -> Result<Vec<u8>> {
    serde_json::to_vec(&MountedSifsNoLinkDirectoryCursor {
        directory_object_id: directory_object_id.to_string(),
        directory_version: directory_version.as_str().to_string(),
        cursor: cursor.clone(),
    })
    .map_err(|source| VfsError::Backend {
        backend: "mounted-sifs".to_string(),
        path: path.to_string(),
        detail: format!("cannot encode no-link DFS directory cursor: {source}"),
    })
}

fn decode_no_link_directory_cursor(
    path: &str,
    cursor: &[u8],
) -> Result<MountedSifsNoLinkDirectoryCursor> {
    serde_json::from_slice(cursor).map_err(|source| VfsError::InvalidCursor {
        detail: format!("invalid mounted-SIFS no-link cursor for {path}: {source}"),
    })
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
    fn mounted_sifs_directory_pages_use_bounded_dfs_continuations() {
        let root = temp_data_dir("directory-pages");
        let setup = mounted_sifs(Some(root.clone())).expect("open setup handle");
        setup.create_dir_all("catalog").expect("create catalog");
        for name in ["echo.md", "alpha.md", "delta.md", "bravo.md", "charlie.md"] {
            setup
                .write_with_options(
                    format!("catalog/{name}"),
                    name.as_bytes(),
                    VfsWriteOptions::service_artifact(),
                )
                .expect("write catalog entry");
        }
        drop(setup);

        let handle = mounted_sifs_local_reads(Some(root.clone())).expect("open local handle");
        let mut cursor = None;
        let mut names = Vec::new();
        let mut page_sizes = Vec::new();
        loop {
            let page = handle
                .list_page("catalog", cursor.as_ref(), 2)
                .expect("list directory page");
            page_sizes.push(page.entries.len());
            names.extend(page.entries.into_iter().map(|entry| entry.name));
            cursor = page.next_cursor.map(|cursor| {
                let encoded = cursor.to_opaque_string().expect("encode cursor");
                sinomo_vfs::VfsDirectoryCursor::from_opaque_string(&encoded).expect("decode cursor")
            });
            if cursor.is_none() {
                break;
            }
        }

        assert_eq!(page_sizes, vec![2, 2, 1]);
        names.sort();
        assert_eq!(
            names,
            vec!["alpha.md", "bravo.md", "charlie.md", "delta.md", "echo.md"]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn create_new_is_exclusive_across_mounted_sifs_handles() {
        let root = temp_data_dir("exclusive-create");
        let setup = mounted_sifs(Some(root.clone())).expect("open setup handle");
        setup
            .create_dir_all("notes")
            .expect("create notes directory");
        drop(setup);

        let left = mounted_sifs(Some(root.clone())).expect("open left handle");
        let right = mounted_sifs(Some(root.clone())).expect("open right handle");
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let (left_result, right_result) = std::thread::scope(|scope| {
            let left_barrier = Arc::clone(&barrier);
            let left = scope.spawn(move || {
                left_barrier.wait();
                left.write_with_options(
                    "notes/shared.md",
                    b"left",
                    VfsWriteOptions::default().create_new(),
                )
            });
            let right_barrier = Arc::clone(&barrier);
            let right = scope.spawn(move || {
                right_barrier.wait();
                right.write_with_options(
                    "notes/shared.md",
                    b"right",
                    VfsWriteOptions::default().create_new(),
                )
            });
            (
                left.join().expect("left writer"),
                right.join().expect("right writer"),
            )
        });

        let successes = usize::from(left_result.is_ok()) + usize::from(right_result.is_ok());
        assert_eq!(successes, 1, "left={left_result:?}, right={right_result:?}");
        let loser = if left_result.is_err() {
            left_result.expect_err("left loses")
        } else {
            right_result.expect_err("right loses")
        };
        assert!(
            matches!(&loser, VfsError::AlreadyExists { .. }),
            "unexpected losing create: {loser}"
        );

        let reader = mounted_sifs_local_reads(Some(root.clone())).expect("open reader");
        let contents = reader.read("notes/shared.md").expect("read winner");
        assert!(contents == b"left" || contents == b"right", "{contents:?}");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn conditional_replace_rejects_a_stale_vfs_version() {
        let root = temp_data_dir("conditional-replace");
        let handle = mounted_sifs(Some(root.clone())).expect("open handle");
        handle
            .create_dir_all("notes")
            .expect("create notes directory");
        let opened = handle
            .create_new_versioned(
                "notes/versioned.md",
                b"first",
                VfsWriteOptions::default().create_new(),
            )
            .expect("create note");
        let opened_object_id = opened.object_id.expect("opened object id");
        let opened_version = opened.version.expect("opened version");

        let saved = handle
            .replace_if_version(
                "notes/versioned.md",
                &opened_object_id,
                &opened_version,
                b"second",
                VfsWriteOptions::default().replace_existing(),
            )
            .expect("matching replace");
        assert_ne!(saved.version.as_deref(), Some(opened_version.as_str()));
        let stale = handle
            .replace_if_version(
                "notes/versioned.md",
                &opened_object_id,
                &opened_version,
                b"stale",
                VfsWriteOptions::default().replace_existing(),
            )
            .expect_err("stale replace");
        assert!(matches!(&stale, VfsError::Conflict { .. }), "{stale}");
        assert_eq!(
            handle.read("notes/versioned.md").expect("read saved note"),
            b"second"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn conditional_replace_rejects_an_old_identity_after_recreate() {
        let root = temp_data_dir("conditional-replace-recreate");
        let handle = mounted_sifs(Some(root.clone())).expect("open handle");
        handle
            .create_dir_all("notes")
            .expect("create notes directory");
        let opened = handle
            .create_new_versioned(
                "notes/recreated.md",
                b"original object",
                VfsWriteOptions::default().create_new(),
            )
            .expect("create original note");
        let opened_object_id = opened.object_id.expect("original object id");
        let opened_version = opened.version.expect("original version");
        handle
            .remove("notes/recreated.md")
            .expect("remove original");
        let recreated = handle
            .create_new_versioned(
                "notes/recreated.md",
                b"replacement object",
                VfsWriteOptions::default().create_new(),
            )
            .expect("recreate note");
        assert_ne!(
            recreated.object_id.as_deref(),
            Some(opened_object_id.as_str())
        );

        let stale = handle
            .replace_if_version(
                "notes/recreated.md",
                &opened_object_id,
                &opened_version,
                b"must not replace the recreated object",
                VfsWriteOptions::default().replace_existing(),
            )
            .expect_err("old object identity must conflict");
        assert!(matches!(&stale, VfsError::Conflict { .. }), "{stale}");
        assert_eq!(
            handle
                .read("notes/recreated.md")
                .expect("read recreated note"),
            b"replacement object"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn no_link_beneath_operations_reject_link_components() {
        let root = temp_data_dir("beneath-no-links");
        let backend = Arc::new(
            MountedSifsVfsBackend::open(Some(root.clone())).expect("open mounted backend"),
        );
        {
            let mut opened = backend.opened.lock().expect("lock setup service");
            let options =
                DfsWriteOptions::replicated_eventual(vec![opened.service.node_id().clone()]);
            opened
                .service
                .posix_mkdir_all("/scope/notes")
                .expect("create scoped notes");
            opened
                .service
                .posix_mkdir_all("/outside")
                .expect("create outside dir");
            opened
                .service
                .posix_create_file("/scope/notes/existing.md", b"abcdef", options.clone())
                .expect("create scoped file");
            opened
                .service
                .posix_create_file("/outside/secret.md", b"outside", options.clone())
                .expect("create outside file");
            opened
                .service
                .posix_symlink("/outside", "/scope/link", options)
                .expect("create escaping link");
        }

        let scoped = VfsHandle::new(backend.clone(), "/scope").expect("scope handle");
        scoped
            .create_dir_all_beneath_no_links("notes/nested/deep")
            .expect("create guarded directory chain");
        let notes_page = scoped
            .list_page_beneath_no_links("notes", None, 16)
            .expect("guarded directory listing");
        assert!(
            notes_page
                .entries
                .iter()
                .any(|entry| entry.name == "existing.md")
        );
        let read = scoped
            .read_prefix_beneath_no_links("notes/existing.md", 3)
            .expect("bounded no-link read");
        assert_eq!(read.bytes, b"abc");
        assert!(read.truncated);
        assert_eq!(read.metadata.path, "/scope/notes/existing.md");
        assert!(read.metadata.object_id.is_some());
        assert!(read.metadata.version.is_some());
        let exact_metadata = scoped
            .stat_beneath_no_links("notes/existing.md")
            .expect("no-link metadata receipt");
        assert_eq!(exact_metadata, read.metadata);
        let middle = scoped
            .read_range_beneath_no_links("notes/existing.md", 2, 2)
            .expect("bounded middle range");
        assert_eq!(middle.bytes, b"cd");
        assert!(middle.truncated);
        assert_eq!(middle.metadata, exact_metadata);
        let tail = scoped
            .read_range_beneath_no_links("notes/existing.md", 4, 64)
            .expect("bounded tail range");
        assert_eq!(tail.bytes, b"ef");
        assert!(!tail.truncated);
        assert_eq!(tail.metadata, exact_metadata);
        let eof = scoped
            .read_range_beneath_no_links("notes/existing.md", 6, 64)
            .expect("range at EOF");
        assert!(eof.bytes.is_empty());
        assert!(!eof.truncated);
        assert!(matches!(
            scoped.read_range_beneath_no_links("notes/existing.md", 7, 64),
            Err(VfsError::InvalidPath { .. })
        ));
        let created = scoped
            .create_new_versioned_beneath_no_links(
                "notes/new.md",
                b"new",
                VfsWriteOptions::default().create_new(),
            )
            .expect("exclusive no-link create");
        let replaced = scoped
            .replace_if_version_beneath_no_links(
                "notes/new.md",
                created.object_id.as_deref().expect("created object id"),
                created.version.as_deref().expect("created version"),
                b"updated",
                VfsWriteOptions::default().replace_existing(),
            )
            .expect("no-link CAS replace");
        assert_ne!(replaced.version, created.version);

        assert!(matches!(
            scoped.read_prefix_beneath_no_links("link/secret.md", 64),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            scoped.read_range_beneath_no_links("link/secret.md", 0, 64),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            scoped.stat_beneath_no_links("link/secret.md"),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            scoped.list_page_beneath_no_links("link", None, 16),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            scoped.create_new_versioned_beneath_no_links(
                "link/escaped.md",
                b"must stay scoped",
                VfsWriteOptions::default().create_new(),
            ),
            Err(VfsError::InvalidPath { .. })
        ));
        assert!(matches!(
            scoped.create_dir_all_beneath_no_links("link/escaped-dir"),
            Err(VfsError::InvalidPath { .. })
        ));

        let missing_root =
            VfsHandle::new(backend.clone(), "/new-agent/home/memory").expect("missing root handle");
        missing_root
            .create_dir_all_beneath_no_links("")
            .expect("create missing scoped root without links");
        missing_root
            .create_dir_all_beneath_no_links("projects/alpha")
            .expect("create beneath newly created root");

        let outside = {
            let mut opened = backend.opened.lock().expect("lock verification service");
            let outside = opened
                .service
                .posix_open_beneath_no_links_distributed("/outside", "/outside/secret.md")
                .expect("open outside file");
            assert!(matches!(
                opened.service.posix_stat_distributed("/outside/escaped.md"),
                Err(DfsPosixError::NotFound { .. })
            ));
            assert!(matches!(
                opened
                    .service
                    .posix_stat_distributed("/outside/escaped-dir"),
                Err(DfsPosixError::NotFound { .. })
            ));
            outside
        };
        assert!(matches!(
            scoped.replace_if_version_beneath_no_links(
                "link/secret.md",
                outside.object_id.as_str(),
                outside.version.as_str(),
                b"must not replace outside",
                VfsWriteOptions::default().replace_existing(),
            ),
            Err(VfsError::InvalidPath { .. })
        ));
        let mut opened = backend.opened.lock().expect("lock final verification");
        assert_eq!(
            opened
                .service
                .posix_read_file_distributed("/outside/secret.md")
                .expect("outside file unchanged"),
            b"outside"
        );
        drop(opened);
        drop(scoped);
        drop(backend);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn opened_no_link_mutations_cannot_follow_a_later_link_swap() {
        let root = temp_data_dir("beneath-link-swap");
        let backend = MountedSifsVfsBackend::open(Some(root.clone())).expect("open backend");
        let mut opened = backend.opened.lock().expect("lock service");
        let options = DfsWriteOptions::replicated_eventual(vec![opened.service.node_id().clone()]);
        opened
            .service
            .posix_mkdir_all("/scope/race")
            .expect("create race parent");
        opened
            .service
            .posix_mkdir_all("/outside")
            .expect("create outside");
        opened
            .service
            .posix_create_file("/scope/victim.md", b"inside", options.clone())
            .expect("create victim");
        opened
            .service
            .posix_create_file("/outside/secret.md", b"outside", options.clone())
            .expect("create outside secret");

        let opened_parent = opened
            .service
            .posix_open_beneath_no_links_distributed("/scope", "/scope/race")
            .expect("open parent before swap");
        opened
            .service
            .posix_rmdir("/scope/race")
            .expect("remove opened parent path");
        opened
            .service
            .posix_symlink("/outside", "/scope/race", options.clone())
            .expect("swap parent for link");
        assert!(matches!(
            opened
                .service
                .posix_create_file_at_opened_directory_no_links(
                    &opened_parent,
                    "escaped.md",
                    b"must not escape",
                    options.clone(),
                ),
            Err(DfsPosixError::InvalidPath { .. })
                | Err(DfsPosixError::Conflict { .. })
                | Err(DfsPosixError::NotFound { .. })
        ));
        assert!(matches!(
            opened
                .service
                .posix_mkdir_at_opened_directory_no_links(&opened_parent, "escaped-dir"),
            Err(DfsPosixError::InvalidPath { .. })
                | Err(DfsPosixError::Conflict { .. })
                | Err(DfsPosixError::NotFound { .. })
        ));
        assert!(matches!(
            opened.service.posix_stat_distributed("/outside/escaped.md"),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert!(matches!(
            opened
                .service
                .posix_stat_distributed("/outside/escaped-dir"),
            Err(DfsPosixError::NotFound { .. })
        ));

        let opened_file = opened
            .service
            .posix_open_beneath_no_links_distributed("/scope", "/scope/victim.md")
            .expect("open victim before swap");
        opened
            .service
            .posix_unlink("/scope/victim.md")
            .expect("unlink victim");
        opened
            .service
            .posix_symlink("/outside/secret.md", "/scope/victim.md", options.clone())
            .expect("swap victim for link");
        assert!(matches!(
            opened
                .service
                .posix_replace_opened_file_if_version_no_links(
                    &opened_file,
                    b"must not escape",
                    options,
                ),
            Err(DfsPosixError::InvalidPath { .. })
                | Err(DfsPosixError::Conflict { .. })
                | Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            opened
                .service
                .posix_read_file_distributed("/outside/secret.md")
                .expect("outside secret unchanged"),
            b"outside"
        );
        drop(opened);
        drop(backend);
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
