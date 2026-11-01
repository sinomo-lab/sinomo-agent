//! SIFS-backed backends for the rooted `sinomo-vfs` facade.
//!
//! This crate adapts SIFS/DFS storage into the generic VFS traits. Keeping it
//! separate lets crates that only need local or in-memory VFS handles depend on
//! `sinomo-vfs` without pulling in DFS or daemon implementation crates.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ClusterPaths, ObjectRpcRequest};
use sinomo_core::node::NodeId;
pub use sinomo_dfs::DfsCancellationFlag;
use sinomo_dfs::{
    DfsDirEntry, DfsDirectoryPageCursor, DfsNamespaceDestinationPrecondition,
    DfsNamespaceObjectPrecondition, DfsObjectId, DfsObjectKind, DfsPosixError, DfsStat, DfsVersion,
    DfsWriteOptions, OpenedMountedSifsService,
};
use sinomo_vfs::{
    Result, VfsAppendOptions, VfsBackend, VfsBackendDirectoryPage, VfsConsistencyTarget,
    VfsCreateMode, VfsDirEntry, VfsError, VfsFileRead, VfsHandle, VfsMetadata, VfsObjectKind,
    VfsOpenedFileRead, VfsRemoveOptions, VfsWriteDurability, VfsWriteOptions,
};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(feature = "test-support")]
pub mod test_support;
#[cfg(all(test, feature = "test-support"))]
mod provider_discovery_tests;

pub fn mounted_sifs(data_dir: Option<PathBuf>) -> Result<VfsHandle> {
    VfsHandle::new(Arc::new(MountedSifsVfsBackend::open(data_dir)?), "/")
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
                cancellation: Some(cancellation),
            },
        )?),
        "/",
    )
}

pub fn daemon_sifs(address: impl Into<String>) -> Result<VfsHandle> {
    VfsHandle::new(Arc::new(DaemonSifsVfsBackend::new(address.into())), "/")
}

/// Resolve a live capability-advertised filesystem provider for each operation.
/// Discovery happens before effect dispatch and never replays an uncertain write.
pub fn discovered_daemon_sifs(
    data_dir: SinomoDataDir,
    address: impl Into<String>,
) -> Result<VfsHandle> {
    let mut backend = DaemonSifsVfsBackend::new(address.into());
    backend.discovery_data_dir = Some(data_dir);
    VfsHandle::new(Arc::new(backend), "/")
}

/// Discover providers while retaining an exact authenticated cluster namespace.
/// The owning no-link route checks the same binding at the operation boundary.
pub fn discovered_daemon_sifs_in_cluster(
    data_dir: SinomoDataDir,
    address: impl Into<String>,
    cluster_id: sinomo_core::Id,
) -> Result<VfsHandle> {
    let mut backend = DaemonSifsVfsBackend::new(address.into());
    backend.discovery_data_dir = Some(data_dir);
    backend.expected_cluster_id = Some(cluster_id);
    VfsHandle::new(Arc::new(backend), "/")
}

pub fn daemon_sifs_with_timeout(
    address: impl Into<String>,
    timeout: Duration,
) -> Result<VfsHandle> {
    VfsHandle::new(
        Arc::new(DaemonSifsVfsBackend::new(address.into()).with_timeout(timeout)),
        "/",
    )
}

/// SIFS access through a connected daemon to an explicitly selected provider
/// node. Node selection stays with the caller; this backend only delivers to
/// that target through the daemon-owned cluster transport.
pub fn daemon_sifs_on_node(
    data_dir: SinomoDataDir,
    address: impl Into<String>,
    node_id: NodeId,
) -> Result<VfsHandle> {
    VfsHandle::new(
        Arc::new(DaemonSifsVfsBackend::on_node(
            data_dir,
            address.into(),
            node_id,
        )),
        "/",
    )
}

#[derive(Clone, Debug, Default)]
pub struct MountedSifsOpenOptions {
    pub cancellation: Option<DfsCancellationFlag>,
}

#[derive(Debug)]
pub struct MountedSifsVfsBackend {
    opened: Mutex<OpenedMountedSifsService>,
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
        let data_dir = match data_dir {
            Some(path) => SinomoDataDir::new(path),
            None => SinomoDataDir::from_env_or_home().map_err(|source| VfsError::Backend {
                backend: "mounted-sifs".to_string(),
                path: "/".to_string(),
                detail: source.to_string(),
            })?,
        };
        let cluster_paths = ClusterPaths::new(&data_dir);
        #[cfg(feature = "test-support")]
        if let Some(opened) = test_support::open_registered(&data_dir, options.cancellation.clone()) {
            return opened.map(|opened| Self { opened: Mutex::new(opened) });
        }
        let opened = if cluster_paths.state.exists() {
            let peer_transport = Arc::new(
                sinomo_dfs::DaemonDfsPeerTransport::connect(data_dir.clone()).map_err(|source| {
                    VfsError::Backend {
                        backend: "mounted-sifs".to_string(),
                        path: "/".to_string(),
                        detail: source.to_string(),
                    }
                })?,
            );
            let sync_peer_transport: Arc<dyn sinomo_dfs::DfsPeerTransport> =
                peer_transport.clone();
            let async_peer_transport: Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> = peer_transport;
            sinomo_dfs::open_mounted_sifs_service_with_peer_transports_and_default_control_plane_and_cancellation(
                data_dir,
                sync_peer_transport,
                async_peer_transport,
                options.cancellation,
            )
        } else {
            sinomo_dfs::open_mounted_sifs_service_with_cancellation(
                Some(data_dir.root()),
                options.cancellation,
            )
        }
        .map_err(|source| VfsError::Backend {
                    backend: "mounted-sifs".to_string(),
                    path: "/".to_string(),
                    detail: source.to_string(),
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
        match mounted_distributed_stat(opened, path) {
            Ok(_) => Ok(true),
            Err(VfsError::NotFound { .. }) => Ok(false),
            Err(source) => Err(source),
        }
    }
}

fn mounted_distributed_stat(opened: &mut OpenedMountedSifsService, path: &str) -> Result<DfsStat> {
    match opened.service.posix_stat_distributed(path) {
        Ok(stat) => Ok(stat),
        Err(DfsPosixError::NotFound { path: missing_path }) => {
            match opened.service.posix_namespace_lstat_distributed(path) {
                Err(DfsPosixError::NotFound { .. }) => {
                    Err(VfsError::NotFound { path: missing_path })
                }
                // A dangling symlink has a confirmed absent dereferenced
                // target. Missing link-body content is a distinct
                // SymlinkContentPending error and never reaches this branch.
                Ok(candidate) if candidate.kind == DfsObjectKind::Symlink => {
                    Err(VfsError::NotFound { path: missing_path })
                }
                Ok(candidate) => Err(VfsError::Backend {
                    backend: "mounted-sifs".to_string(),
                    path: path.to_string(),
                    detail: format!(
                        "SIFS stat could not resolve the path, but authoritative namespace metadata still contains {:?} object {}",
                        candidate.kind, candidate.object_id
                    ),
                }),
                Err(source) => Err(dfs_error_to_vfs("mounted-sifs", path, source)),
            }
        }
        Err(source) => Err(dfs_error_to_vfs("mounted-sifs", path, source)),
    }
}

impl VfsBackend for MountedSifsVfsBackend {
    fn backend_name(&self) -> &'static str {
        "mounted-sifs"
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
        let stat = mounted_distributed_stat(&mut opened, path)?;
        Ok(metadata_from_dfs_stat(stat))
    }

    fn lstat(&self, path: &str) -> Result<VfsMetadata> {
        let mut opened = self.lock(path)?;
        let stat = opened
            .service
            .posix_namespace_lstat_distributed(path)
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
        let entries = opened
            .service
            .posix_list_dir_distributed(path)
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
        let page = opened
            .service
            .posix_list_dir_distributed_page(path, after.as_ref(), max_entries)
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
        let outcome = opened
            .service
            .posix_read_file_distributed_outcome(path)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        dfs_distributed_read_outcome_to_vfs(self.backend_name(), path, outcome)
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let mut opened = self.lock(path)?;
        let length = usize::try_from(max_bytes).map_err(|_| VfsError::InvalidPath {
            path: path.to_string(),
            detail: "read prefix length does not fit this platform".to_string(),
        })?;
        let outcome = opened
            .service
            .posix_read_file_prefix_distributed_outcome(path, length)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        dfs_distributed_prefix_read_outcome_to_vfs(self.backend_name(), path, outcome)
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
        let outcome = opened
            .service
            .posix_read_file_prefix_beneath_no_links_distributed_outcome(root, path, length)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        dfs_distributed_opened_range_read_outcome_to_vfs(self.backend_name(), path, outcome)
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
        let outcome = opened
            .service
            .posix_read_file_range_beneath_no_links_distributed_outcome(root, path, offset, length)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        dfs_distributed_opened_range_read_outcome_to_vfs(self.backend_name(), path, outcome)
    }

    fn write(&self, path: &str, bytes: &[u8], options: &VfsWriteOptions) -> Result<()> {
        let mut opened = self.lock(path)?;
        if options.create_mode != VfsCreateMode::CreateNew {
            enforce_sifs_create_mode(&mut opened, path, options, self.backend_name())?;
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
        let outcome = opened
            .service
            .posix_read_file_distributed_outcome(path)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))?;
        let mut contents =
            match dfs_distributed_read_outcome_to_vfs(self.backend_name(), path, outcome) {
                Ok(bytes) => bytes,
                Err(VfsError::NotFound { .. }) if options.create => Vec::new(),
                Err(source) => return Err(source),
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

    // SIFS confinement is strict: every confined operation walks from `root`
    // without following any symbolic or hardlink record, and mutations commit
    // only against the exact generation that walk opened.

    fn stat_confined(&self, root: &str, path: &str) -> Result<VfsMetadata> {
        self.stat_beneath_no_links(root, path)
    }

    fn list_confined(&self, root: &str, path: &str) -> Result<Vec<VfsDirEntry>> {
        const PAGE_ENTRIES: usize = 1024;
        let mut entries = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let page =
                self.list_page_beneath_no_links(root, path, after.as_deref(), PAGE_ENTRIES)?;
            entries.extend(page.entries);
            match page.next_cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(entries)
    }

    fn read_range_confined(
        &self,
        root: &str,
        path: &str,
        offset: u64,
        max_bytes: u64,
    ) -> Result<VfsFileRead> {
        let opened = self.read_range_beneath_no_links(root, path, offset, max_bytes)?;
        Ok(VfsFileRead {
            bytes: opened.bytes,
            truncated: opened.truncated,
        })
    }

    fn write_confined(
        &self,
        root: &str,
        path: &str,
        bytes: &[u8],
        options: &VfsWriteOptions,
    ) -> Result<()> {
        if options.create_mode == VfsCreateMode::CreateNew {
            return self
                .create_new_versioned_beneath_no_links(root, path, bytes, options)
                .map(drop);
        }
        match self.stat_beneath_no_links(root, path) {
            Ok(existing) => {
                if existing.kind == VfsObjectKind::Directory {
                    return Err(VfsError::IsDirectory {
                        path: path.to_string(),
                    });
                }
                let (Some(object_id), Some(version)) =
                    (existing.object_id.as_deref(), existing.version.as_deref())
                else {
                    return Err(VfsError::Backend {
                        backend: self.backend_name().to_string(),
                        path: path.to_string(),
                        detail: "SIFS metadata did not include an object generation".to_string(),
                    });
                };
                self.replace_if_version_beneath_no_links(
                    root, path, object_id, version, bytes, options,
                )
                .map(drop)
            }
            Err(VfsError::NotFound { .. })
                if options.create_mode != VfsCreateMode::ReplaceExisting =>
            {
                self.create_new_versioned_beneath_no_links(root, path, bytes, options)
                    .map(drop)
            }
            Err(source) => Err(source),
        }
    }

    fn create_dir_all_confined(&self, root: &str, path: &str) -> Result<()> {
        self.create_dir_all_beneath_no_links(root, path)
    }

    fn remove_confined(&self, root: &str, path: &str, options: &VfsRemoveOptions) -> Result<()> {
        let mut opened = self.lock(path)?;
        let target = match opened
            .service
            .posix_open_beneath_no_links_distributed(root, path)
        {
            Ok(stat) => stat,
            Err(DfsPosixError::NotFound { .. }) if options.missing_ok => return Ok(()),
            Err(source) => return Err(dfs_error_to_vfs(self.backend_name(), path, source)),
        };
        let is_directory = matches!(target.kind, DfsObjectKind::Directory);
        if is_directory != options.directory {
            return Err(if is_directory {
                VfsError::IsDirectory {
                    path: path.to_string(),
                }
            } else {
                VfsError::NotDirectory {
                    path: path.to_string(),
                }
            });
        }
        let operation_id = format!(
            "vfs-confined-remove:{}:{}",
            target.object_id,
            target.version.as_str()
        );
        let expected = DfsNamespaceObjectPrecondition {
            object_id: target.object_id,
            version: target.version,
        };
        let result = if is_directory {
            opened
                .service
                .posix_rmdir_if_generation(path, operation_id, expected)
        } else {
            opened
                .service
                .posix_unlink_if_generation(path, operation_id, expected)
        };
        result
            .map(drop)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), path, source))
    }

    fn rename_confined(&self, root: &str, from: &str, to: &str) -> Result<()> {
        let to_parent = to
            .rsplit_once('/')
            .map(|(parent, _)| if parent.is_empty() { "/" } else { parent })
            .ok_or_else(|| VfsError::InvalidPath {
                path: to.to_string(),
                detail: "rename destination has no parent".to_string(),
            })?;
        let mut opened = self.lock(from)?;
        let source = opened
            .service
            .posix_open_beneath_no_links_distributed(root, from)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), from, source))?;
        let parent = opened
            .service
            .posix_open_beneath_no_links_distributed(root, to_parent)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), to, source))?;
        if !matches!(parent.kind, DfsObjectKind::Directory) {
            return Err(VfsError::NotDirectory {
                path: to_parent.to_string(),
            });
        }
        let destination = match opened
            .service
            .posix_open_beneath_no_links_distributed(root, to)
        {
            Ok(existing) if matches!(existing.kind, DfsObjectKind::Directory) => {
                return Err(VfsError::IsDirectory {
                    path: to.to_string(),
                });
            }
            Ok(existing) => {
                DfsNamespaceDestinationPrecondition::Exact(DfsNamespaceObjectPrecondition {
                    object_id: existing.object_id,
                    version: existing.version,
                })
            }
            Err(DfsPosixError::NotFound { .. }) => DfsNamespaceDestinationPrecondition::Absent,
            Err(source) => return Err(dfs_error_to_vfs(self.backend_name(), to, source)),
        };
        let operation_id = format!(
            "vfs-confined-rename:{}:{}:{to}",
            source.object_id,
            source.version.as_str()
        );
        opened
            .service
            .posix_rename_if_generation(
                from,
                to,
                operation_id,
                DfsNamespaceObjectPrecondition {
                    object_id: source.object_id,
                    version: source.version,
                },
                destination,
            )
            .map(drop)
            .map_err(|source| dfs_error_to_vfs(self.backend_name(), from, source))
    }
}

#[derive(Debug)]
pub struct DaemonSifsVfsBackend {
    address: String,
    timeout: Duration,
    target: Option<(SinomoDataDir, NodeId)>,
    discovery_data_dir: Option<SinomoDataDir>,
    expected_cluster_id: Option<sinomo_core::Id>,
}

fn remaining_provider_budget(deadline: Instant) -> std::result::Result<Duration, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err("filesystem provider discovery exhausted the operation deadline".into())
    } else {
        Ok(remaining)
    }
}

impl DaemonSifsVfsBackend {
    pub fn new(address: String) -> Self {
        Self {
            address,
            timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            target: None,
            discovery_data_dir: None,
            expected_cluster_id: None,
        }
    }

    pub fn on_node(data_dir: SinomoDataDir, address: String, node_id: NodeId) -> Self {
        Self {
            address,
            timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            target: Some((data_dir, node_id)),
            discovery_data_dir: None,
            expected_cluster_id: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn send_rpc(
        &self,
        request: &ObjectRpcRequest,
    ) -> std::result::Result<sinomo_core::network::ObjectRpcResponse, String> {
        self.send_rpc_with_timeout(request, self.timeout)
    }

    fn send_rpc_with_timeout(
        &self,
        request: &ObjectRpcRequest,
        timeout: Duration,
    ) -> std::result::Result<sinomo_core::network::ObjectRpcResponse, String> {
        if let Some(data_dir) = &self.discovery_data_dir {
            // Select using read-only catalog/status requests. Once selected,
            // this exact operation has one transport owner and one destination.
            // A failed send is returned unchanged, without another provider send.
            let deadline = Instant::now() + timeout;
            let target = self.discover_provider(data_dir, deadline)?;
            let timeout = remaining_provider_budget(deadline)?;
            return match target {
                Some(target) => Self::on_node(data_dir.clone(), self.address.clone(), target)
                    .send_rpc_with_timeout(request, timeout),
                None => sinomo_api::send_daemon_rpc(&self.address, request, timeout)
                    .map_err(|error| error.to_string()),
            };
        }
        if let Some((data_dir, target_node_id)) = &self.target {
            return sinomo_api::DaemonApiClient::at_address(self.address.clone())
                .with_timeout(timeout)
                .object_rpc_to_target_from_data_dir_with_affinity(
                    data_dir,
                    target_node_id,
                    request.clone(),
                    timeout,
                    sinomo_api::DaemonObjectRpcDestinationProcessAffinity::MainDaemon,
                )
                .map_err(|source| source.to_string());
        }
        sinomo_api::send_daemon_rpc(&self.address, request, timeout)
            .map_err(|source| source.to_string())
    }

    fn discover_provider(
        &self,
        data_dir: &SinomoDataDir,
        deadline: Instant,
    ) -> std::result::Result<Option<NodeId>, String> {
        let client = sinomo_api::DaemonApiClient::at_address(self.address.clone())
            .with_timeout(remaining_provider_budget(deadline)?);
        let status = client.status().map_err(|error| error.to_string())?;
        let Some(cluster) = status.cluster else {
            if self.expected_cluster_id.is_some() {
                return Err(
                    "retained filesystem source requires its original cluster namespace".into(),
                );
            }
            return Ok(None);
        };
        if self
            .expected_cluster_id
            .as_ref()
            .is_some_and(|expected| expected != &cluster.cluster_id)
        {
            return Err("filesystem source cluster namespace changed".into());
        }
        let request = ObjectRpcRequest::encode_body(
            sinomo_core::kv::CLUSTER_KV_LOCAL_RANGE_PREFIX_ROUTE,
            sinomo_core::kv::CLUSTER_KV_RPC_CATEGORY,
            &sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX.to_string(),
        )
        .map_err(|error| error.to_string())?;
        let response = client
            .with_timeout(remaining_provider_budget(deadline)?)
            .call_request(&request)
            .map_err(|error| error.to_string())?;
        let entries: sinomo_core::kv::KvLocalRead<Vec<sinomo_core::kv::KvEntry>> =
            response.decode_body().map_err(|error| error.to_string())?;
        let instances = sinomo_core::service::service_instances_from_entries(&entries.value)
            .map_err(|error| error.to_string())?;
        let mut providers: Vec<_> = sinomo_core::service::service_instances_providing_capability(
            instances,
            sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY,
        )
        .into_iter()
        .map(|instance| instance.node_id)
        .collect();
        providers.sort();
        providers.dedup();
        providers.sort_by_key(|node| node != &cluster.node_id);
        let mut failures = Vec::new();
        let probe = ObjectRpcRequest::encode_body(
            sinomo_api::DAEMON_DFS_BROWSER_STATUS_ROUTE,
            sinomo_api::DAEMON_RPC_CATEGORY,
            &sinomo_api::DaemonDfsBrowserStatusRequest::default(),
        )
        .map_err(|error| error.to_string())?;
        let provider_count = providers.len();
        for (index, provider) in providers.into_iter().enumerate() {
            // Preserve time for every later candidate and for the selected
            // operation. Reuse the control-plane foreground route-health cap;
            // this remains inside the caller's one absolute deadline.
            let remaining = remaining_provider_budget(deadline)?;
            let slots = u32::try_from(provider_count - index + 1).unwrap_or(u32::MAX);
            let probe_budget = (remaining / slots).min(
                sinomo_core::cluster::DEFAULT_CLUSTER_CONTROL_PLANE_TCP_FOREGROUND_ATTEMPT_TIMEOUT,
            );
            if probe_budget.is_zero() {
                return Err(
                    "filesystem provider discovery exhausted the operation deadline".into(),
                );
            }
            match Self::on_node(data_dir.clone(), self.address.clone(), provider.clone())
                .send_rpc_with_timeout(&probe, probe_budget)
                .and_then(|response| {
                    response
                        .decode_body::<sinomo_api::DaemonDfsBrowserStatusResponse>()
                        .map_err(|error| error.to_string())
                }) {
                Ok(status) if status.node_id == provider.as_str() => return Ok(Some(provider)),
                Ok(_) => failures.push(format!("{provider}: provider identity mismatch")),
                Err(error) => failures.push(format!("{provider}: {error}")),
            }
        }
        Err(format!(
            "no reachable cluster service instance provides {} ({})",
            sinomo_dfs::SIFS_PATH_ACCESS_CAPABILITY,
            failures.join("; ")
        ))
    }

    fn no_links(
        &self,
        root: &str,
        path: &str,
        operation: sinomo_api::DaemonDfsBrowserNoLinksOperation,
    ) -> Result<sinomo_api::DaemonDfsBrowserNoLinksResponse> {
        let mutation = operation.is_mutation();
        let request = ObjectRpcRequest::encode_body(
            sinomo_api::DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
            sinomo_api::DAEMON_RPC_CATEGORY,
            &sinomo_api::DaemonDfsBrowserNoLinksRequest {
                expected_cluster_id: self.expected_cluster_id.clone(),
                root: root.into(),
                path: path.into(),
                operation,
            },
        )
        .map_err(|error| {
            daemon_non_absence_detail_to_vfs(
                self.backend_name(),
                path,
                sinomo_api::DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
                error.to_string(),
            )
        })?;
        let result = if mutation {
            self.send_rpc(&request)
                .and_then(|response| response.decode_body().map_err(|error| error.to_string()))
        } else {
            self.read_response(&request)
        };
        let response: sinomo_api::DaemonDfsBrowserNoLinksResponse = result.map_err(|error| {
            daemon_non_absence_detail_to_vfs(
                self.backend_name(),
                path,
                sinomo_api::DAEMON_DFS_BROWSER_NO_LINKS_ROUTE,
                error,
            )
        })?;
        if let sinomo_api::DaemonDfsBrowserNoLinksResponse::Failed { failure } = response {
            use sinomo_api::DaemonDfsBrowserNoLinksFailure as F;
            return Err(match failure {
                F::NotFound { path } => VfsError::NotFound { path },
                F::AlreadyExists { path } => VfsError::AlreadyExists { path },
                F::NotDirectory { path } => VfsError::NotDirectory { path },
                F::IsDirectory { path } => VfsError::IsDirectory { path },
                F::InvalidPath { path, detail } => VfsError::InvalidPath { path, detail },
                F::Conflict { path, detail } => VfsError::Conflict { path, detail },
                F::Unsupported { detail, .. } => VfsError::Unsupported {
                    backend: self.backend_name().into(), detail,
                },
                F::Unavailable { path, detail } => VfsError::Backend {
                    backend: self.backend_name().into(),
                    path,
                    detail,
                },
            });
        }
        Ok(response)
    }

    fn no_links_metadata(
        &self,
        path: &str,
        metadata: sinomo_api::DaemonDfsBrowserStatResponse,
    ) -> Result<VfsMetadata> {
        if metadata.path != path {
            return Err(self.invalid_no_links_response(path));
        }
        Ok(VfsMetadata {
            path: metadata.path,
            kind: vfs_kind_from_dfs(&metadata.kind),
            object_id: Some(metadata.object_id),
            size_bytes: metadata.size_bytes,
            content_hash: metadata.content_hash,
            version: Some(metadata.version),
        })
    }

    fn invalid_no_links_response(&self, path: &str) -> VfsError {
        VfsError::Backend {
            backend: self.backend_name().into(),
            path: path.into(),
            detail: "provider returned an invalid exact scoped-operation response".into(),
        }
    }

    fn read_response<Response: DeserializeOwned>(
        &self,
        request: &ObjectRpcRequest,
    ) -> std::result::Result<Response, String> {
        sinomo_api::dfs::retry_dfs_browser_read(self.timeout, |remaining| {
            self.send_rpc_with_timeout(request, remaining)?
                .decode_body()
                .map_err(|source| source.to_string())
        })
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
                    daemon_non_absence_detail_to_vfs(
                        self.backend_name(),
                        path,
                        route,
                        source.to_string(),
                    )
                })?;
        let response = self.send_rpc(&request).map_err(|source| {
            daemon_non_absence_detail_to_vfs(self.backend_name(), path, route, source)
        })?;
        response.decode_body().map_err(|source| {
            daemon_non_absence_detail_to_vfs(self.backend_name(), path, route, source.to_string())
        })
    }

    fn read_file_rpc(&self, path: &str) -> Result<sinomo_api::DaemonDfsBrowserReadFileResponse> {
        let request = ObjectRpcRequest::encode_body(
            sinomo_api::DAEMON_DFS_BROWSER_READ_FILE_ROUTE,
            sinomo_api::DAEMON_RPC_CATEGORY,
            &sinomo_api::DaemonDfsBrowserReadFileRequest {
                path: path.to_string(),
            },
        )
        .map_err(|source| {
            daemon_read_file_detail_to_vfs(self.backend_name(), path, source.to_string())
        })?;
        self.read_response(&request)
            .map_err(|source| daemon_read_file_detail_to_vfs(self.backend_name(), path, source))
    }

    fn stat_rpc(&self, path: &str) -> Result<sinomo_api::DaemonDfsBrowserStatOutcomeResponse> {
        let request = ObjectRpcRequest::encode_body(
            sinomo_api::DAEMON_DFS_BROWSER_STAT_ROUTE,
            sinomo_api::DAEMON_RPC_CATEGORY,
            &sinomo_api::DaemonDfsBrowserStatRequest {
                path: path.to_string(),
                max_staleness_millis: 0,
            },
        )
        .map_err(|source| {
            daemon_stat_detail_to_vfs(self.backend_name(), path, source.to_string())
        })?;
        self.read_response(&request)
            .map_err(|source| daemon_stat_detail_to_vfs(self.backend_name(), path, source))
    }

    fn read_file_prefix_rpc(
        &self,
        path: &str,
        max_bytes: u64,
    ) -> Result<sinomo_api::DaemonDfsBrowserReadFilePrefixResponse> {
        let request = ObjectRpcRequest::encode_body(
            sinomo_api::DAEMON_DFS_BROWSER_READ_FILE_PREFIX_ROUTE,
            sinomo_api::DAEMON_RPC_CATEGORY,
            &sinomo_api::DaemonDfsBrowserReadFilePrefixRequest {
                path: path.to_string(),
                max_bytes,
            },
        )
        .map_err(|source| {
            daemon_read_file_prefix_detail_to_vfs(self.backend_name(), path, source.to_string())
        })?;
        self.read_response(&request).map_err(|source| {
            daemon_read_file_prefix_detail_to_vfs(self.backend_name(), path, source)
        })
    }

    fn list_directory_rpc(&self, path: &str) -> Result<sinomo_api::DaemonDfsBrowserListResponse> {
        let request = ObjectRpcRequest::encode_body(
            sinomo_api::DAEMON_DFS_BROWSER_LIST_ROUTE,
            sinomo_api::DAEMON_RPC_CATEGORY,
            &sinomo_api::DaemonDfsBrowserListRequest {
                path: path.to_string(),
                shallow: true,
                // The daemon establishes namespace absence before returning
                // an empty listing. This preserves rolling compatibility with
                // older daemons without interpreting an opaque RPC error as
                // authoritative NotFound.
                missing_is_empty: true,
                sort_key: sinomo_api::DaemonDfsBrowserSortKey::Name,
                sort_direction: sinomo_api::DaemonDfsBrowserSortDirection::Ascending,
                offset: 0,
                limit: None,
                include_metadata: false,
            },
        )
        .map_err(|source| {
            daemon_list_directory_detail_to_vfs(self.backend_name(), path, source.to_string())
        })?;
        self.read_response(&request).map_err(|source| {
            daemon_list_directory_detail_to_vfs(self.backend_name(), path, source)
        })
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

    fn lstat(&self, path: &str) -> Result<VfsMetadata> {
        let request = ObjectRpcRequest::encode_body(
            sinomo_api::DAEMON_DFS_BROWSER_NAMESPACE_LSTAT_ROUTE,
            sinomo_api::DAEMON_RPC_CATEGORY,
            &sinomo_api::DaemonDfsBrowserStatRequest {
                path: path.into(),
                max_staleness_millis: 0,
            },
        )
        .map_err(|error| self.invalid_no_links_response(&error.to_string()))?;
        let response = self.read_response(&request).map_err(|error| {
            daemon_non_absence_detail_to_vfs(self.backend_name(), path, "namespace-lstat", error)
        })?;
        let stat = daemon_stat_response_to_vfs(self.backend_name(), path, response)?;
        self.no_links_metadata(path, stat)
    }

    fn stat_beneath_no_links(&self, root: &str, path: &str) -> Result<VfsMetadata> {
        match self.no_links(
            root,
            path,
            sinomo_api::DaemonDfsBrowserNoLinksOperation::Stat,
        )? {
            sinomo_api::DaemonDfsBrowserNoLinksResponse::Metadata { metadata } => {
                self.no_links_metadata(path, metadata)
            }
            _ => Err(self.invalid_no_links_response(path)),
        }
    }

    fn list_page_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        after: Option<&[u8]>,
        max_entries: usize,
    ) -> Result<VfsBackendDirectoryPage> {
        match self.no_links(
            root,
            path,
            sinomo_api::DaemonDfsBrowserNoLinksOperation::ListPage {
                cursor: after.map(<[u8]>::to_vec),
                max_entries,
            },
        )? {
            sinomo_api::DaemonDfsBrowserNoLinksResponse::DirectoryPage {
                directory,
                entries,
                next_cursor,
            } => Ok(VfsBackendDirectoryPage {
                directory_metadata: Some(self.no_links_metadata(path, directory)?),
                entries: entries
                    .into_iter()
                    .map(|entry| VfsDirEntry {
                        name: entry.name,
                        path: entry.path,
                        kind: vfs_kind_from_dfs(&entry.kind),
                        size_bytes: entry.size,
                    })
                    .collect(),
                next_cursor,
            }),
            _ => Err(self.invalid_no_links_response(path)),
        }
    }

    fn read_prefix_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        self.read_range_beneath_no_links(root, path, 0, max_bytes)
    }

    fn read_range_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        offset: u64,
        max_bytes: u64,
    ) -> Result<VfsOpenedFileRead> {
        match self.no_links(
            root,
            path,
            sinomo_api::DaemonDfsBrowserNoLinksOperation::ReadRange { offset, max_bytes },
        )? {
            sinomo_api::DaemonDfsBrowserNoLinksResponse::Read {
                metadata,
                bytes,
                truncated,
            } => {
                if bytes.len() as u64 > max_bytes {
                    return Err(self.invalid_no_links_response(path));
                }
                Ok(VfsOpenedFileRead {
                    metadata: self.no_links_metadata(path, metadata)?,
                    bytes,
                    truncated,
                })
            }
            _ => Err(self.invalid_no_links_response(path)),
        }
    }

    fn create_dir_all_beneath_no_links(&self, root: &str, path: &str) -> Result<()> {
        match self.no_links(
            root,
            path,
            sinomo_api::DaemonDfsBrowserNoLinksOperation::CreateDirAll,
        )? {
            sinomo_api::DaemonDfsBrowserNoLinksResponse::DirectoryCreated => Ok(()),
            _ => Err(self.invalid_no_links_response(path)),
        }
    }

    fn create_new_versioned_beneath_no_links(
        &self,
        root: &str,
        path: &str,
        bytes: &[u8],
        options: &VfsWriteOptions,
    ) -> Result<VfsMetadata> {
        if options.durability != VfsWriteDurability::ReplicatedEventual {
            return Err(VfsError::Unsupported {
                backend: self.backend_name().into(),
                detail: "daemon scoped mutations require distributed policy durability".into(),
            });
        }
        match self.no_links(
            root,
            path,
            sinomo_api::DaemonDfsBrowserNoLinksOperation::CreateNew {
                bytes: bytes.to_vec(),
            },
        )? {
            sinomo_api::DaemonDfsBrowserNoLinksResponse::Metadata { metadata } => {
                self.no_links_metadata(path, metadata)
            }
            _ => Err(self.invalid_no_links_response(path)),
        }
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
        if options.durability != VfsWriteDurability::ReplicatedEventual {
            return Err(VfsError::Unsupported {
                backend: self.backend_name().into(),
                detail: "daemon scoped mutations require distributed policy durability".into(),
            });
        }
        match self.no_links(
            root,
            path,
            sinomo_api::DaemonDfsBrowserNoLinksOperation::ReplaceIfVersion {
                expected_object_id: expected_object_id.into(),
                expected_version: expected_version.into(),
                bytes: bytes.to_vec(),
            },
        )? {
            sinomo_api::DaemonDfsBrowserNoLinksResponse::Metadata { metadata } => {
                self.no_links_metadata(path, metadata)
            }
            _ => Err(self.invalid_no_links_response(path)),
        }
    }

    fn stat(&self, path: &str) -> Result<VfsMetadata> {
        let response = self.stat_rpc(path)?;
        let response = daemon_stat_response_to_vfs(self.backend_name(), path, response)?;
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
        let response = self.list_directory_rpc(path)?;
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
        let response = self.read_file_rpc(path)?;
        daemon_read_file_response_to_vfs(self.backend_name(), path, response)
    }

    fn read_prefix(&self, path: &str, max_bytes: u64) -> Result<VfsFileRead> {
        let response = self.read_file_prefix_rpc(path, max_bytes)?;
        daemon_read_file_prefix_response_to_vfs(self.backend_name(), path, response)
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
        DfsPosixError::Dfs(sinomo_dfs::DfsError::UnsupportedWritePrecondition { reason, .. }) => VfsError::Unsupported {
            backend: backend.to_string(), detail: reason,
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

fn daemon_read_file_response_to_vfs(
    backend: &str,
    requested_path: &str,
    response: sinomo_api::DaemonDfsBrowserReadFileResponse,
) -> Result<Vec<u8>> {
    match response.into_result() {
        Ok(bytes) => Ok(bytes),
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::NotFound { path }) => {
            Err(VfsError::NotFound { path })
        }
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ContentUnavailable { path, detail }) => {
            Err(VfsError::Backend {
                backend: backend.to_string(),
                path,
                detail: format!("SIFS content is temporarily unavailable: {detail}"),
            })
        }
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ReadFailed { path, detail }) => Err(
            daemon_non_absence_detail_to_vfs(backend, &path, "read-file", detail),
        ),
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::InvalidResponse { detail }) => {
            Err(VfsError::Backend {
                backend: backend.to_string(),
                path: requested_path.to_string(),
                detail,
            })
        }
    }
}

fn daemon_stat_response_to_vfs(
    backend: &str,
    requested_path: &str,
    response: sinomo_api::DaemonDfsBrowserStatOutcomeResponse,
) -> Result<sinomo_api::DaemonDfsBrowserStatResponse> {
    match response.into_result() {
        Ok(stat) => Ok(stat),
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::NotFound { path }) => {
            Err(VfsError::NotFound { path })
        }
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ContentUnavailable { path, detail }) => {
            Err(VfsError::Backend {
                backend: backend.to_string(),
                path,
                detail: format!(
                    "SIFS stat content dependency is temporarily unavailable: {detail}"
                ),
            })
        }
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ReadFailed { path, detail }) => Err(
            daemon_non_absence_detail_to_vfs(backend, &path, "stat", detail),
        ),
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::InvalidResponse { detail }) => {
            Err(VfsError::Backend {
                backend: backend.to_string(),
                path: requested_path.to_string(),
                detail,
            })
        }
    }
}

fn dfs_distributed_read_outcome_to_vfs(
    backend: &str,
    requested_path: &str,
    outcome: sinomo_dfs::DfsPosixDistributedFileReadOutcome,
) -> Result<Vec<u8>> {
    match outcome {
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::Found { bytes } => Ok(bytes),
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::NotFound { path } => {
            Err(VfsError::NotFound { path })
        }
        sinomo_dfs::DfsPosixDistributedFileReadOutcome::ContentUnavailable { path, detail } => {
            Err(VfsError::Backend {
                backend: backend.to_string(),
                path,
                detail: format!(
                    "SIFS content is temporarily unavailable while reading {requested_path}: {detail}"
                ),
            })
        }
    }
}

fn dfs_distributed_prefix_read_outcome_to_vfs(
    backend: &str,
    requested_path: &str,
    outcome: sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome,
) -> Result<VfsFileRead> {
    match outcome {
        sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::Found { read } => Ok(VfsFileRead {
            bytes: read.bytes,
            truncated: read.truncated,
        }),
        sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::NotFound { path } => {
            Err(VfsError::NotFound { path })
        }
        sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
            path,
            detail,
        } => Err(VfsError::Backend {
            backend: backend.to_string(),
            path,
            detail: format!(
                "SIFS content prefix is temporarily unavailable while reading {requested_path}: {detail}"
            ),
        }),
    }
}

fn dfs_distributed_opened_range_read_outcome_to_vfs(
    backend: &str,
    requested_path: &str,
    outcome: sinomo_dfs::DfsPosixDistributedOpenedFileRangeReadOutcome,
) -> Result<VfsOpenedFileRead> {
    match outcome {
        sinomo_dfs::DfsPosixDistributedOpenedFileRangeReadOutcome::Found { stat, read } => {
            Ok(VfsOpenedFileRead {
                metadata: metadata_from_dfs_stat(stat),
                bytes: read.bytes,
                truncated: read.truncated,
            })
        }
        sinomo_dfs::DfsPosixDistributedOpenedFileRangeReadOutcome::NotFound { path } => {
            Err(VfsError::NotFound { path })
        }
        sinomo_dfs::DfsPosixDistributedOpenedFileRangeReadOutcome::ContentUnavailable {
            path,
            detail,
        } => Err(VfsError::Backend {
            backend: backend.to_string(),
            path,
            detail: format!(
                "SIFS opened content range is temporarily unavailable while reading {requested_path}: {detail}"
            ),
        }),
    }
}

fn daemon_read_file_prefix_response_to_vfs(
    backend: &str,
    requested_path: &str,
    response: sinomo_api::DaemonDfsBrowserReadFilePrefixResponse,
) -> Result<VfsFileRead> {
    match response.into_result() {
        Ok((bytes, truncated)) => Ok(VfsFileRead { bytes, truncated }),
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::NotFound { path }) => {
            Err(VfsError::NotFound { path })
        }
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ContentUnavailable { path, detail }) => {
            Err(VfsError::Backend {
                backend: backend.to_string(),
                path,
                detail: format!("SIFS content prefix is temporarily unavailable: {detail}"),
            })
        }
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::ReadFailed { path, detail }) => Err(
            daemon_non_absence_detail_to_vfs(backend, &path, "read-file-prefix", detail),
        ),
        Err(sinomo_api::DaemonDfsBrowserReadFileFailure::InvalidResponse { detail }) => {
            Err(VfsError::Backend {
                backend: backend.to_string(),
                path: requested_path.to_string(),
                detail,
            })
        }
    }
}

fn daemon_read_file_detail_to_vfs(backend: &str, path: &str, detail: String) -> VfsError {
    daemon_non_absence_detail_to_vfs(backend, path, "read-file", detail)
}

fn daemon_stat_detail_to_vfs(backend: &str, path: &str, detail: String) -> VfsError {
    daemon_non_absence_detail_to_vfs(backend, path, "stat", detail)
}

fn daemon_read_file_prefix_detail_to_vfs(backend: &str, path: &str, detail: String) -> VfsError {
    daemon_non_absence_detail_to_vfs(backend, path, "read-file-prefix", detail)
}

fn daemon_list_directory_detail_to_vfs(backend: &str, path: &str, detail: String) -> VfsError {
    daemon_non_absence_detail_to_vfs(backend, path, "list-directory", detail)
}

fn daemon_non_absence_detail_to_vfs(
    backend: &str,
    path: &str,
    operation: &str,
    detail: String,
) -> VfsError {
    match daemon_detail_to_vfs(backend, path, detail.clone()) {
        VfsError::NotFound { .. } => VfsError::Backend {
            backend: backend.to_string(),
            path: path.to_string(),
            detail: format!("daemon SIFS {operation} request failed: {detail}"),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_read_file_preserves_absence_and_fails_closed_on_content_unavailability() {
        let missing = daemon_read_file_response_to_vfs(
            "daemon-sifs",
            "/services/skills/catalog.sobj",
            sinomo_api::DaemonDfsBrowserReadFileResponse::not_found(
                "/services/skills/catalog.sobj",
            ),
        )
        .expect_err("confirmed namespace absence remains NotFound");
        assert!(matches!(missing, VfsError::NotFound { .. }));

        let unavailable = daemon_read_file_response_to_vfs(
            "daemon-sifs",
            "/services/skills/catalog.sobj",
            sinomo_api::DaemonDfsBrowserReadFileResponse::content_unavailable(
                "/services/skills/catalog.sobj",
                "authoritative metadata exists but no content replica is readable",
            ),
        )
        .expect_err("content unavailability must fail closed");
        assert!(
            matches!(unavailable, VfsError::Backend { ref backend, .. } if backend == "daemon-sifs"),
            "unexpected unavailable mapping: {unavailable}"
        );

        let legacy_route_error = daemon_read_file_detail_to_vfs(
            "daemon-sifs",
            "/services/skills/catalog.sobj",
            "SIFS path not found: /services/skills/catalog.sobj".to_string(),
        );
        assert!(
            matches!(legacy_route_error, VfsError::Backend { .. }),
            "only a typed read response may establish namespace absence"
        );

        let conflict = daemon_read_file_detail_to_vfs(
            "daemon-sifs",
            "/services/skills/catalog.sobj",
            "SIFS path is conflicted".to_string(),
        );
        assert!(matches!(conflict, VfsError::Conflict { .. }));

        let directory = daemon_read_file_detail_to_vfs(
            "daemon-sifs",
            "/services/skills",
            "SIFS path is a directory".to_string(),
        );
        assert!(matches!(directory, VfsError::IsDirectory { .. }));

        let stat_missing = daemon_stat_response_to_vfs(
            "daemon-sifs",
            "/services/skills/missing.sobj",
            sinomo_api::DaemonDfsBrowserStatOutcomeResponse::not_found(
                "/services/skills/missing.sobj",
            ),
        )
        .expect_err("typed stat absence remains NotFound");
        assert!(matches!(stat_missing, VfsError::NotFound { .. }));

        let legacy_stat_route_error = daemon_stat_detail_to_vfs(
            "daemon-sifs",
            "/services/skills/catalog.sobj",
            "SIFS path not found: /services/skills/catalog.sobj".to_string(),
        );
        assert!(
            matches!(legacy_stat_route_error, VfsError::Backend { .. }),
            "an old daemon's untyped stat error must fail closed"
        );

        let stat_conflict = daemon_stat_detail_to_vfs(
            "daemon-sifs",
            "/services/skills/catalog.sobj",
            "SIFS path is conflicted".to_string(),
        );
        assert!(matches!(stat_conflict, VfsError::Conflict { .. }));

        for operation in [
            sinomo_api::DAEMON_DFS_BROWSER_WRITE_FILE_ROUTE,
            sinomo_api::DAEMON_DFS_BROWSER_DELETE_ROUTE,
            sinomo_api::DAEMON_DFS_BROWSER_RENAME_ROUTE,
        ] {
            let opaque_missing = daemon_non_absence_detail_to_vfs(
                "daemon-sifs",
                "/services/skills/catalog.sobj",
                operation,
                "daemon object RPC route not found while delivery is unknown".to_string(),
            );
            assert!(
                matches!(opaque_missing, VfsError::Backend { .. }),
                "mutation route {operation} must not report opaque transport failure as absence"
            );
        }

        let prefix_missing = daemon_read_file_prefix_response_to_vfs(
            "daemon-sifs",
            "/files/missing.bin",
            sinomo_api::DaemonDfsBrowserReadFilePrefixResponse::not_found("/files/missing.bin"),
        )
        .expect_err("typed prefix absence remains NotFound");
        assert!(matches!(prefix_missing, VfsError::NotFound { .. }));

        let prefix_unavailable = daemon_read_file_prefix_response_to_vfs(
            "daemon-sifs",
            "/files/preview.bin",
            sinomo_api::DaemonDfsBrowserReadFilePrefixResponse::content_unavailable(
                "/files/preview.bin",
                "metadata exists but the exact version has no readable replica",
            ),
        )
        .expect_err("prefix content unavailability must fail closed");
        assert!(matches!(prefix_unavailable, VfsError::Backend { .. }));

        let legacy_prefix_route_error = daemon_read_file_prefix_detail_to_vfs(
            "daemon-sifs",
            "/files/preview.bin",
            "SIFS path not found: /files/preview.bin".to_string(),
        );
        assert!(
            matches!(legacy_prefix_route_error, VfsError::Backend { .. }),
            "an old daemon's untyped prefix error must fail closed"
        );

        let mounted_unavailable = dfs_distributed_read_outcome_to_vfs(
            "mounted-sifs",
            "/services/skills/catalog.sobj",
            sinomo_dfs::DfsPosixDistributedFileReadOutcome::ContentUnavailable {
                path: "/services/skills/catalog.sobj".to_string(),
                detail: "authoritative metadata exists but no exact content version is readable"
                    .to_string(),
            },
        )
        .expect_err("mounted content unavailability must fail closed");
        assert!(matches!(mounted_unavailable, VfsError::Backend { .. }));

        let mounted_prefix_unavailable = dfs_distributed_prefix_read_outcome_to_vfs(
            "mounted-sifs",
            "/files/preview.bin",
            sinomo_dfs::DfsPosixDistributedFilePrefixReadOutcome::ContentUnavailable {
                path: "/files/preview.bin".to_string(),
                detail: "authoritative metadata exists but the prefix replica is unavailable"
                    .to_string(),
            },
        )
        .expect_err("mounted prefix unavailability must fail closed");
        assert!(matches!(
            mounted_prefix_unavailable,
            VfsError::Backend { .. }
        ));
    }

    #[test]
    fn daemon_catalog_listing_does_not_infer_absence_from_opaque_error_text() {
        let route_error = daemon_list_directory_detail_to_vfs(
            "daemon-sifs",
            "/services/agents/definitions",
            "daemon service route not found while the cluster is unavailable".to_string(),
        );
        assert!(
            matches!(route_error, VfsError::Backend { .. }),
            "only the daemon's successful missing-is-empty response may establish an absent catalog directory"
        );

        let not_directory = daemon_list_directory_detail_to_vfs(
            "daemon-sifs",
            "/services/agents/definitions.json",
            "SIFS path is not a directory".to_string(),
        );
        assert!(matches!(not_directory, VfsError::NotDirectory { .. }));
    }

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
    fn mounted_sifs_reopened_handle_serves_same_node_writes() {
        let root = temp_data_dir("reopened-handle");
        let writer = mounted_sifs(Some(root.clone())).expect("open writer");
        writer
            .create_dir_all("catalog")
            .expect("create catalog dir");
        writer
            .write_with_options(
                "catalog/entry.json",
                b"{\"id\":1}",
                VfsWriteOptions::service_artifact(),
            )
            .expect("write through writer handle");
        drop(writer);

        let reader = mounted_sifs(Some(root.clone())).expect("open reader");
        assert_eq!(
            reader.read("catalog/entry.json").expect("read"),
            b"{\"id\":1}".to_vec(),
            "a reopened mounted-SIFS handle should serve current data"
        );
        let names = reader
            .list_if_exists("catalog")
            .expect("list")
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"entry.json".to_string()), "{names:?}");
        assert!(reader.stat("catalog/entry.json").is_ok());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn mounted_sifs_does_not_treat_missing_content_as_namespace_absence() {
        let root = temp_data_dir("missing-content-is-not-absence");
        let backend = MountedSifsVfsBackend::open(Some(root.clone())).expect("open mounted SIFS");
        let path = "/catalog/present.sobj";
        backend
            .create_dir_all("/catalog")
            .expect("create catalog directory");
        backend
            .write(
                path,
                &vec![0x5a; 2 * 1024 * 1024 + 4096],
                &VfsWriteOptions::service_artifact(),
            )
            .expect("write catalog object");
        let metadata = backend.stat(path).expect("stat written catalog object");
        let content_hash = metadata.content_hash.expect("catalog content hash");
        sinomo_dfs::LocalContentStore::new(root.join("dfs").join("content"))
            .expect("open content store")
            .debug_remove_loose_blob(&content_hash)
            .expect("remove only the local content blob");

        let read_error = backend
            .read(path)
            .expect_err("metadata-present content miss must fail closed");
        assert!(
            matches!(read_error, VfsError::Backend { .. }),
            "full read must retain content unavailability: {read_error}"
        );

        let prefix_error = backend
            .read_prefix(path, 8)
            .expect_err("metadata-present prefix miss must fail closed");
        assert!(
            matches!(prefix_error, VfsError::Backend { .. }),
            "prefix read must retain content unavailability: {prefix_error}"
        );

        let guarded_prefix_error = backend
            .read_prefix_beneath_no_links("/catalog", path, 8)
            .expect_err("guarded prefix must retain metadata-present content unavailability");
        assert!(
            matches!(guarded_prefix_error, VfsError::Backend { .. }),
            "guarded prefix must not turn content unavailability into absence: {guarded_prefix_error}"
        );

        let guarded_range_error = backend
            .read_range_beneath_no_links("/catalog", path, 4, 8)
            .expect_err("guarded range must retain metadata-present content unavailability");
        assert!(
            matches!(guarded_range_error, VfsError::Backend { .. }),
            "guarded range must not turn content unavailability into absence: {guarded_range_error}"
        );

        let append_error = backend
            .append(path, b"replacement", &VfsAppendOptions::default())
            .expect_err("append must not recreate a metadata-present unavailable file");
        assert!(
            matches!(append_error, VfsError::Backend { .. }),
            "append must retain content unavailability: {append_error}"
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

        let handle = mounted_sifs(Some(root.clone())).expect("open handle");
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

        let reader = mounted_sifs(Some(root.clone())).expect("open reader");
        let contents = reader.read("notes/shared.md").expect("read winner");
        assert!(contents == b"left" || contents == b"right", "{contents:?}");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn confined_operations_stay_beneath_a_scoped_workspace() {
        let root = temp_data_dir("confined-workspace");
        let setup = mounted_sifs(Some(root.clone())).expect("open setup handle");
        setup
            .create_dir_all("workspaces/project")
            .expect("create workspace");
        setup
            .create_dir_all("workspaces/other")
            .expect("create sibling");
        setup
            .write("workspaces/other/secret.txt", b"secret")
            .expect("write sibling secret");
        let workspace = setup.scoped("/workspaces/project").expect("scope");

        workspace
            .create_dir_all_confined("src")
            .expect("create src");
        workspace
            .write_confined("src/a.txt", b"first", VfsWriteOptions::default())
            .expect("create file");
        let error = workspace
            .write_confined(
                "src/a.txt",
                b"again",
                VfsWriteOptions::default().create_new(),
            )
            .expect_err("create_new must not replace");
        assert!(matches!(error, VfsError::AlreadyExists { .. }), "{error}");
        workspace
            .write_confined("src/a.txt", b"second value", VfsWriteOptions::default())
            .expect("replace file");
        let read = workspace
            .read_range_confined("src/a.txt", 7, 3)
            .expect("read range");
        assert_eq!(read.bytes, b"val");
        assert!(read.truncated);
        assert_eq!(
            workspace
                .list_confined("src")
                .expect("list")
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>(),
            vec!["a.txt"]
        );

        workspace
            .rename_confined("src/a.txt", "b.txt")
            .expect("rename");
        assert_eq!(
            workspace
                .read_range_confined("b.txt", 0, 64)
                .expect("renamed")
                .bytes,
            b"second value"
        );
        workspace
            .remove_confined("b.txt", VfsRemoveOptions::file())
            .expect("remove");
        assert!(matches!(
            workspace.stat_confined("b.txt"),
            Err(VfsError::NotFound { .. })
        ));
        workspace
            .remove_confined("b.txt", VfsRemoveOptions::file().missing_ok())
            .expect("missing ok");

        assert!(
            workspace
                .read_range_confined("/workspaces/other/secret.txt", 0, 64)
                .is_err()
        );
        assert_eq!(
            setup
                .read("workspaces/other/secret.txt")
                .expect("sibling intact"),
            b"secret"
        );
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
