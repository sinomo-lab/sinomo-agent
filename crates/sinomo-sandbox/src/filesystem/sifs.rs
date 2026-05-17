use crate::internal::*;

use super::support::{normalize_sandbox_sifs_root, sandbox_fs_error_from_dfs, sandbox_parent_path};
use super::virtual_devices::*;

#[derive(Debug)]
pub struct SifsSandboxFileSystem<T: NetworkTransport> {
    pub(crate) dfs: NetworkedDfsService<T>,
    root: String,
    mounts: Vec<SandboxMount>,
    temp_epoch: u64,
    replica_candidates: Vec<NodeId>,
}

impl<T: NetworkTransport> SifsSandboxFileSystem<T> {
    pub fn new(dfs: NetworkedDfsService<T>, manifest: &SandboxInstanceManifest) -> Self {
        let replica_candidates = vec![dfs.node_id().clone()];
        Self {
            dfs,
            root: normalize_sandbox_sifs_root(&manifest.workspace_root),
            mounts: manifest.mounts.clone(),
            temp_epoch: manifest.temp_epoch,
            replica_candidates,
        }
    }

    pub fn into_inner(self) -> NetworkedDfsService<T> {
        self.dfs
    }

    pub fn with_replica_candidates(mut self, replica_candidates: Vec<NodeId>) -> Self {
        let mut replica_candidates = replica_candidates;
        if !replica_candidates.contains(self.dfs.node_id()) {
            replica_candidates.insert(0, self.dfs.node_id().clone());
        }
        replica_candidates.sort();
        replica_candidates.dedup();
        self.replica_candidates = replica_candidates;
        self
    }

    pub fn ensure_workspace(&mut self) -> Result<(), SandboxFsError> {
        self.dfs.posix_mkdir_all(&self.root).map_err(|error| {
            sandbox_fs_error_from_dfs(&SandboxPath::new("/").expect("root"), error)
        })?;
        for path in [
            "/root",
            "/home",
            "/tmp",
            "/work",
            "/checkpoints",
            "/logs",
            "/artifacts",
        ] {
            let sandbox_path = SandboxPath::new(path).expect("static path");
            self.dfs
                .posix_mkdir_all(&self.sifs_path(&sandbox_path))
                .map_err(|error| sandbox_fs_error_from_dfs(&sandbox_path, error))?;
        }
        Ok(())
    }

    fn workspace_sifs_path(&self, path: &SandboxPath) -> String {
        if path.as_str() == "/" {
            self.root.clone()
        } else if path.as_str() == "/tmp" {
            format!(
                "{}/tmp/{}",
                self.root.trim_end_matches('/'),
                self.temp_epoch
            )
        } else if let Some(rest) = path.as_str().strip_prefix("/tmp/") {
            format!(
                "{}/tmp/{}/{}",
                self.root.trim_end_matches('/'),
                self.temp_epoch,
                rest
            )
        } else {
            format!(
                "{}/{}",
                self.root.trim_end_matches('/'),
                path.as_str().trim_start_matches('/')
            )
        }
    }

    fn sifs_path(&self, path: &SandboxPath) -> String {
        self.mount_resolution(path)
            .map(|resolution| resolution.sifs_path)
            .unwrap_or_else(|| self.workspace_sifs_path(path))
    }

    fn mount_resolution(&self, path: &SandboxPath) -> Option<SifsMountResolution> {
        let mut best: Option<SifsMountResolution> = None;
        for mount in &self.mounts {
            let Some(tail) = sandbox_mount_path_tail(path, &mount.mount_path) else {
                continue;
            };
            let source = normalize_sandbox_sifs_root(&mount.source_sifs_path);
            let sifs_path = if tail.is_empty() {
                source
            } else {
                format!("{}/{}", source.trim_end_matches('/'), tail)
            };
            let candidate = SifsMountResolution {
                mount_len: mount.mount_path.as_str().len(),
                sifs_path,
                mode: mount.mode.clone(),
            };
            if best
                .as_ref()
                .is_none_or(|current| candidate.mount_len > current.mount_len)
            {
                best = Some(candidate);
            }
        }
        best
    }

    fn ensure_writable_path(&self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if matches!(
            self.mount_resolution(path)
                .map(|resolution| resolution.mode),
            Some(SandboxMountMode::ReadOnly)
        ) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(())
    }

    fn metadata_from_stat(&self, path: &SandboxPath, stat: sinomo_dfs::DfsStat) -> SandboxMetadata {
        SandboxMetadata {
            path: path.clone(),
            is_dir: stat.kind == DfsObjectKind::Directory,
            len: stat.size_bytes.unwrap_or(0),
        }
    }

    fn write_options(&self) -> DfsWriteOptions {
        DfsWriteOptions::replicated_eventual(self.replica_candidates.clone())
    }

    fn ensure_parent_dir(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        let Some(parent) = sandbox_parent_path(path)? else {
            return Ok(());
        };
        self.ensure_writable_path(&parent)?;
        self.dfs
            .posix_mkdir_all(&self.sifs_path(&parent))
            .map_err(|error| sandbox_fs_error_from_dfs(&parent, error))
    }
}

struct SifsMountResolution {
    mount_len: usize,
    sifs_path: String,
    mode: SandboxMountMode,
}

impl<T: NetworkTransport> SandboxFileSystem for SifsSandboxFileSystem<T> {
    fn stat(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        if let Some(metadata) = sandbox_virtual_device_metadata(path) {
            return Ok(metadata);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.dfs
            .posix_stat(&self.sifs_path(path))
            .map(|stat| self.metadata_from_stat(path, stat))
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn read(
        &self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return read_sandbox_virtual_device(path, range);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let bytes = self
            .dfs
            .posix_read_file(&self.sifs_path(path))
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        let Some(range) = range else {
            return Ok(bytes);
        };
        let start = usize::try_from(range.start).map_err(|_| SandboxFsError::Unavailable {
            reason: format!("range start too large for {}", path),
        })?;
        let end = range
            .end
            .map(usize::try_from)
            .transpose()
            .map_err(|_| SandboxFsError::Unavailable {
                reason: format!("range end too large for {}", path),
            })?
            .unwrap_or(bytes.len())
            .min(bytes.len());
        if start >= bytes.len() || start >= end {
            return Ok(Vec::new());
        }
        Ok(bytes[start..end].to_vec())
    }

    fn create_dir_all(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if matches!(
            sandbox_virtual_device_path(path),
            Some(
                SandboxVirtualDevicePath::DevDirectory
                    | SandboxVirtualDevicePath::FdDirectory
                    | SandboxVirtualDevicePath::PtsDirectory
                    | SandboxVirtualDevicePath::ShmDirectory
            )
        ) {
            return Ok(());
        }
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        self.dfs
            .posix_mkdir_all(&self.sifs_path(path))
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn write(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError> {
        if path.as_str() == "/" {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_virtual_device_path(path).is_some() {
            let _ = bytes;
            return write_sandbox_virtual_device(path);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        self.ensure_parent_dir(path)?;
        self.dfs
            .posix_write_file(&self.sifs_path(path), bytes, self.write_options())
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn list(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        if path.as_str() == "/" {
            let mut entries = self
                .dfs
                .posix_list_dir(&self.sifs_path(path))
                .map_err(|error| sandbox_fs_error_from_dfs(path, error))?
                .into_iter()
                .filter(|entry| entry.name != "dev")
                .map(|entry| {
                    let child_path = path.join(&entry.name)?;
                    let metadata = self.stat(&child_path)?;
                    Ok(SandboxDirEntry {
                        path: child_path,
                        metadata,
                    })
                })
                .collect::<Result<Vec<_>, SandboxFsError>>()?;
            let dev_path = SandboxPath::new("/dev").expect("static virtual device path");
            entries.push(SandboxDirEntry {
                metadata: sandbox_virtual_device_metadata(&dev_path)
                    .expect("virtual device metadata"),
                path: dev_path,
            });
            entries.sort_by(|left, right| left.path.cmp(&right.path));
            return Ok(entries);
        }
        if matches!(
            sandbox_virtual_device_path(path),
            Some(
                SandboxVirtualDevicePath::DevDirectory
                    | SandboxVirtualDevicePath::FdDirectory
                    | SandboxVirtualDevicePath::ShmDirectory
            )
        ) {
            return Ok(sandbox_virtual_device_dir_entries(path));
        }
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::Unavailable {
                reason: format!("list {}: not a directory", path),
            });
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let entries = self
            .dfs
            .posix_list_dir(&self.sifs_path(path))
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        entries
            .into_iter()
            .map(|entry| {
                let child_path = path.join(&entry.name)?;
                let metadata = self.stat(&child_path)?;
                Ok(SandboxDirEntry {
                    path: child_path,
                    metadata,
                })
            })
            .collect()
    }

    fn rename(&mut self, from: &SandboxPath, to: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(from).is_some() || sandbox_virtual_device_path(to).is_some()
        {
            return Err(SandboxFsError::ReadOnly { path: from.clone() });
        }
        if sandbox_path_is_dev_tree(from) || sandbox_path_is_dev_tree(to) {
            return Err(SandboxFsError::ReadOnly { path: from.clone() });
        }
        self.ensure_writable_path(from)?;
        self.ensure_writable_path(to)?;
        self.ensure_parent_dir(to)?;
        self.dfs
            .posix_rename(&self.sifs_path(from), &self.sifs_path(to))
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(from, error))
    }

    fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        match self.dfs.posix_stat(&self.sifs_path(path)) {
            Ok(stat) if stat.kind == DfsObjectKind::Directory => self
                .dfs
                .posix_rmdir(&self.sifs_path(path))
                .map(|_| ())
                .map_err(|error| sandbox_fs_error_from_dfs(path, error)),
            Ok(_) => self
                .dfs
                .posix_unlink(&self.sifs_path(path))
                .map(|_| ())
                .map_err(|error| sandbox_fs_error_from_dfs(path, error)),
            Err(error) => Err(sandbox_fs_error_from_dfs(path, error)),
        }
    }

    fn checkpoint(&mut self) -> Result<SandboxCheckpoint, SandboxFsError> {
        Err(SandboxFsError::Unsupported {
            operation: "SIFS sandbox filesystem checkpoint".to_string(),
        })
    }
}

fn sandbox_mount_path_tail(path: &SandboxPath, mount_path: &SandboxPath) -> Option<String> {
    let path_value = path.as_str();
    let mount_value = mount_path.as_str();
    if path_value == mount_value {
        return Some(String::new());
    }
    if mount_value == "/" {
        return Some(path_value.trim_start_matches('/').to_string());
    }
    path_value
        .strip_prefix(&format!("{}/", mount_value.trim_end_matches('/')))
        .map(ToString::to_string)
}
