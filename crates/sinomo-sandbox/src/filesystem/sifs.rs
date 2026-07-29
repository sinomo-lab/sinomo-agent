use crate::internal::*;

use super::support::{normalize_sandbox_sifs_root, sandbox_fs_error_from_dfs, sandbox_parent_path};
use super::virtual_devices::*;

#[derive(Debug)]
pub struct SifsSandboxFileSystem<T: DfsMessageTransport> {
    pub dfs: NetworkedDfsService<T>,
    root: String,
    mounts: Vec<SandboxMount>,
    temp_epoch: u64,
    replica_candidates: Vec<NodeId>,
    symlink_target_cache: BTreeMap<String, PathBuf>,
}

impl<T: DfsMessageTransport> SifsSandboxFileSystem<T> {
    pub fn new(dfs: NetworkedDfsService<T>, manifest: &SandboxInstanceManifest) -> Self {
        let replica_candidates = vec![dfs.node_id().clone()];
        Self {
            dfs,
            root: normalize_sandbox_sifs_root(&manifest.workspace_root),
            mounts: manifest.mounts.clone(),
            temp_epoch: manifest.temp_epoch,
            replica_candidates,
            symlink_target_cache: BTreeMap::new(),
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
            "/var",
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

    fn sandbox_path_for_sifs_path(&self, sifs_path: &str) -> Option<SandboxPath> {
        let normalized = normalize_sandbox_sifs_root(sifs_path);
        if normalized == self.root {
            return SandboxPath::new("/").ok();
        }
        let root = self.root.trim_end_matches('/');
        if let Some(tail) = normalized
            .strip_prefix(root)
            .and_then(|tail| tail.strip_prefix('/'))
        {
            return self.sandbox_path_for_workspace_tail(tail);
        }
        if let Some(tail) = sandbox_generation_tail_for_instance_alias(&normalized, &self.root) {
            return self.sandbox_path_for_workspace_tail(tail);
        }
        for mount in &self.mounts {
            let source = normalize_sandbox_sifs_root(&mount.source_sifs_path);
            if normalized == source {
                return Some(mount.mount_path.clone());
            }
            if let Some(tail) = normalized
                .strip_prefix(source.trim_end_matches('/'))
                .and_then(|tail| tail.strip_prefix('/'))
            {
                return mount.mount_path.join(tail).ok();
            }
            if let Some(tail) = sandbox_generation_tail_for_instance_alias(&normalized, &source) {
                return if tail.is_empty() {
                    Some(mount.mount_path.clone())
                } else {
                    mount.mount_path.join(tail).ok()
                };
            }
        }
        None
    }

    fn sandbox_path_for_workspace_tail(&self, tail: &str) -> Option<SandboxPath> {
        if tail.is_empty() {
            return SandboxPath::new("/").ok();
        }
        let tmp_prefix = format!("tmp/{}/", self.temp_epoch);
        if let Some(tmp_tail) = tail.strip_prefix(&tmp_prefix) {
            return SandboxPath::new(format!("/tmp/{tmp_tail}")).ok();
        }
        if tail == format!("tmp/{}", self.temp_epoch) {
            return SandboxPath::new("/tmp").ok();
        }
        SandboxPath::new(format!("/{tail}")).ok()
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

    fn mount_domain(&self, path: &SandboxPath) -> Option<usize> {
        let mut best: Option<(usize, usize)> = None;
        for (index, mount) in self.mounts.iter().enumerate() {
            if sandbox_mount_path_tail(path, &mount.mount_path).is_none() {
                continue;
            }
            let mount_len = mount.mount_path.as_str().len();
            if best
                .as_ref()
                .is_none_or(|(_, current_len)| mount_len > *current_len)
            {
                best = Some((index, mount_len));
            }
        }
        best.map(|(index, _)| index)
    }

    fn ensure_same_mount_domain(
        &self,
        operation: &str,
        from: &SandboxPath,
        to: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        let from_domain = self.mount_domain(from);
        let to_domain = self.mount_domain(to);
        if from_domain != to_domain {
            return Err(SandboxFsError::Unsupported {
                operation: format!("cross-mount {operation} {from} -> {to}"),
            });
        }
        Ok(())
    }

    fn path_is_mount_ancestor(&self, path: &SandboxPath) -> bool {
        path.as_str() != "/"
            && self
                .mounts
                .iter()
                .any(|mount| sandbox_mount_path_tail(&mount.mount_path, path).is_some())
    }

    fn synthetic_mount_directory_metadata(&self, path: &SandboxPath) -> Option<SandboxMetadata> {
        if self.mount_resolution(path).is_some() || !self.path_is_mount_ancestor(path) {
            return None;
        }
        if self.dfs.posix_lstat(&self.sifs_path(path)).is_ok() {
            return None;
        }
        Some(SandboxMetadata {
            path: path.clone(),
            is_dir: true,
            is_symlink: false,
            len: 0,
            link_count: 2,
            mode: 0o555,
            uid: 0,
            gid: 0,
            inode: sifs_inode_for_object(&format!("mount-ancestor:{path}"), 0),
            object_identity: None,
            content_hash: None,
            accessed_at_unix_nanos: 1_000_000_000,
            modified_at_unix_nanos: 1_000_000_000,
            created_at_unix_nanos: 1_000_000_000,
            changed_at_unix_nanos: 1_000_000_000,
        })
    }

    fn direct_mount_child_for_list(
        &self,
        path: &SandboxPath,
        mount_path: &SandboxPath,
    ) -> Result<Option<SandboxPath>, SandboxFsError> {
        if path == mount_path {
            return Ok(None);
        }
        let Some(tail) = sandbox_mount_path_tail(mount_path, path) else {
            return Ok(None);
        };
        let Some(child) = tail.split('/').find(|component| !component.is_empty()) else {
            return Ok(None);
        };
        path.join(child).map(Some).map_err(Into::into)
    }

    fn ensure_writable_path(&self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if self.synthetic_mount_directory_metadata(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if matches!(
            self.mount_resolution(path)
                .map(|resolution| resolution.mode),
            Some(SandboxMountMode::ReadOnly)
        ) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(())
    }

    fn ensure_namespace_path_is_not_mount_anchor(
        &self,
        path: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        if self.path_is_mount_ancestor(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(())
    }

    fn metadata_from_stat(&self, path: &SandboxPath, stat: sinomo_dfs::DfsStat) -> SandboxMetadata {
        SandboxMetadata {
            path: path.clone(),
            is_dir: stat.kind == DfsObjectKind::Directory,
            is_symlink: stat.kind == DfsObjectKind::Symlink,
            len: stat.size_bytes.unwrap_or(0),
            link_count: stat.link_count,
            mode: stat.mode,
            uid: stat.uid,
            gid: stat.gid,
            inode: sifs_inode_for_object(stat.object_id.as_str(), stat.created_at_unix_nanos),
            object_identity: Some(stat.object_id.as_str().to_string()),
            content_hash: stat.content_hash,
            accessed_at_unix_nanos: stat.accessed_at_unix_nanos,
            modified_at_unix_nanos: stat.modified_at_unix_nanos,
            created_at_unix_nanos: stat.created_at_unix_nanos,
            changed_at_unix_nanos: stat.changed_at_unix_nanos,
        }
    }

    fn write_options(&self) -> DfsWriteOptions {
        DfsWriteOptions::replicated_eventual(self.replica_candidates.clone())
    }

    fn process_incoming_dfs_messages(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        self.dfs
            .process_incoming()
            .map(|_| ())
            .map_err(|error| SandboxFsError::Unavailable {
                reason: format!("process SIFS network messages for {path}: {error}"),
            })
    }

    fn read_sifs_path(
        &mut self,
        sifs_path: &str,
        update_access: bool,
    ) -> Result<Vec<u8>, DfsPosixError> {
        if update_access {
            self.dfs.posix_read_file_update_access(sifs_path)
        } else {
            self.dfs.posix_read_file_distributed(sifs_path)
        }
    }

    fn read_sifs_file_mut(
        &mut self,
        path: &SandboxPath,
        resolved: &SandboxPath,
        range: Option<ByteRange>,
        update_access: bool,
    ) -> Result<Vec<u8>, SandboxFsError> {
        self.process_incoming_dfs_messages(path)?;
        let sifs_path = self.sifs_path(resolved);
        if !update_access
            && let Some(range) = range.as_ref()
            && let Some(end) = range.end
        {
            let max_bytes = usize::try_from(end.saturating_sub(range.start)).unwrap_or(usize::MAX);
            if max_bytes == 0 {
                return Ok(Vec::new());
            }
            return self
                .dfs
                .posix_read_file_range_distributed(&sifs_path, range.start, max_bytes)
                .map_err(|error| sandbox_fs_error_from_dfs(path, error));
        }
        let bytes = self
            .read_sifs_path(&sifs_path, update_access)
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        read_range(path, bytes, range)
    }

    fn readlink_sifs_path_mut(
        &mut self,
        path: &SandboxPath,
        update_access: bool,
    ) -> Result<PathBuf, SandboxFsError> {
        self.process_incoming_dfs_messages(path)?;
        let sifs_path = self.sifs_path(path);
        if !update_access && let Some(target) = self.symlink_target_cache.get(&sifs_path) {
            return Ok(target.clone());
        }
        let result = if update_access {
            self.dfs.posix_readlink_update_access(&sifs_path)
        } else {
            self.dfs.posix_readlink_distributed(&sifs_path)
        };
        let target = result
            .map(PathBuf::from)
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        self.symlink_target_cache.insert(sifs_path, target.clone());
        Ok(target)
    }

    fn ensure_parent_dir(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        let Some(parent) = sandbox_parent_path(path)? else {
            return Ok(());
        };
        self.ensure_create_dir_all_allowed_mut(&parent)?;
        self.dfs
            .posix_mkdir_all(&self.sifs_path(&parent))
            .map_err(|error| sandbox_fs_error_from_dfs(&parent, error))
    }

    fn stat_raw(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        if let Some(metadata) = sandbox_virtual_device_metadata(path) {
            return Ok(metadata);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        match self.dfs.posix_lstat(&self.sifs_path(path)) {
            Ok(stat) => Ok(self.metadata_from_stat(path, stat)),
            Err(error) => {
                if let Some(metadata) = self.synthetic_mount_directory_metadata(path) {
                    Ok(metadata)
                } else {
                    Err(sandbox_fs_error_from_dfs(path, error))
                }
            }
        }
    }

    fn stat_raw_mut(&mut self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        if let Some(metadata) = sandbox_virtual_device_metadata(path) {
            return Ok(metadata);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.process_incoming_dfs_messages(path)?;
        match self.dfs.posix_lstat_distributed(&self.sifs_path(path)) {
            Ok(stat) => Ok(self.metadata_from_stat(path, stat)),
            Err(error) => {
                if let Some(metadata) = self.synthetic_mount_directory_metadata(path) {
                    Ok(metadata)
                } else {
                    Err(sandbox_fs_error_from_dfs(path, error))
                }
            }
        }
    }

    fn resolve_path(
        &self,
        path: &SandboxPath,
        follow_final: bool,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path_with_permissions(path, follow_final, true)
    }

    fn resolve_path_unchecked(
        &self,
        path: &SandboxPath,
        follow_final: bool,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path_with_permissions(path, follow_final, false)
    }

    fn resolve_path_mut(
        &mut self,
        path: &SandboxPath,
        follow_final: bool,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path_with_permissions_mut(path, follow_final, true)
    }

    fn resolve_path_unchecked_mut(
        &mut self,
        path: &SandboxPath,
        follow_final: bool,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path_with_permissions_mut(path, follow_final, false)
    }

    fn resolve_path_with_permissions(
        &self,
        path: &SandboxPath,
        follow_final: bool,
        check_search: bool,
    ) -> Result<SandboxPath, SandboxFsError> {
        let mut current = path.clone();
        for _ in 0..40 {
            let mut resolved = SandboxPath::new("/")?;
            let components = current
                .as_str()
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let candidate = resolved.join(component)?;
                let final_component = index + 1 == components.len();
                if final_component && !follow_final {
                    resolved = candidate;
                    continue;
                }
                match self.stat_raw(&candidate) {
                    Ok(metadata) if metadata.is_symlink => {
                        let target = self.readlink(&candidate)?;
                        current = self.resolve_symlink_target(&candidate, &target)?;
                        for rest in &components[index + 1..] {
                            current = current.join(rest)?;
                        }
                        restarted = true;
                        break;
                    }
                    Ok(metadata) => {
                        if check_search && !final_component {
                            if !metadata.is_dir {
                                return Err(SandboxFsError::NotDirectory { path: candidate });
                            }
                            if !sandbox_mode_allows_directory_search(metadata.mode) {
                                return Err(SandboxFsError::ReadOnly { path: candidate });
                            }
                        }
                        resolved = candidate;
                    }
                    Err(SandboxFsError::NotFound { .. }) => {
                        let mut missing_path = candidate;
                        for rest in &components[index + 1..] {
                            missing_path = missing_path.join(rest)?;
                        }
                        return Ok(missing_path);
                    }
                    Err(error) => return Err(error),
                }
            }
            if !restarted {
                return Ok(resolved);
            }
        }
        Err(SandboxFsError::Unsupported {
            operation: format!("too many symbolic links resolving {path}"),
        })
    }

    fn resolve_path_with_permissions_mut(
        &mut self,
        path: &SandboxPath,
        follow_final: bool,
        check_search: bool,
    ) -> Result<SandboxPath, SandboxFsError> {
        let mut current = path.clone();
        for _ in 0..40 {
            let mut resolved = SandboxPath::new("/")?;
            let components = current
                .as_str()
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let candidate = resolved.join(component)?;
                let final_component = index + 1 == components.len();
                if final_component && !follow_final {
                    resolved = candidate;
                    continue;
                }
                match self.stat_raw_mut(&candidate) {
                    Ok(metadata) if metadata.is_symlink => {
                        let target = self.readlink_sifs_path_mut(&candidate, false)?;
                        current = self.resolve_symlink_target(&candidate, &target)?;
                        for rest in &components[index + 1..] {
                            current = current.join(rest)?;
                        }
                        restarted = true;
                        break;
                    }
                    Ok(metadata) => {
                        if check_search && !final_component {
                            if !metadata.is_dir {
                                return Err(SandboxFsError::NotDirectory { path: candidate });
                            }
                            if !sandbox_mode_allows_directory_search(metadata.mode) {
                                return Err(SandboxFsError::ReadOnly { path: candidate });
                            }
                        }
                        resolved = candidate;
                    }
                    Err(SandboxFsError::NotFound { .. }) => {
                        let mut missing_path = candidate;
                        for rest in &components[index + 1..] {
                            missing_path = missing_path.join(rest)?;
                        }
                        return Ok(missing_path);
                    }
                    Err(error) => return Err(error),
                }
            }
            if !restarted {
                return Ok(resolved);
            }
        }
        Err(SandboxFsError::Unsupported {
            operation: format!("too many symbolic links resolving {path}"),
        })
    }

    fn resolve_symlink_target(
        &self,
        link_path: &SandboxPath,
        target: &Path,
    ) -> Result<SandboxPath, SandboxFsError> {
        let target = target
            .to_str()
            .ok_or_else(|| SandboxFsError::Path(SandboxPathError::NulByte))?;
        if target.starts_with('/') {
            return SandboxPath::new(target).map_err(Into::into);
        }
        let parent = sandbox_parent_path(link_path)?.unwrap_or_else(|| {
            SandboxPath::new("/").expect("static sandbox root path should be valid")
        });
        parent.join(target).map_err(Into::into)
    }

    fn resolve_path_for_mutation_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path_mut(path, false)
    }

    fn ensure_parent_allows_mutation_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        let Some(parent) = sandbox_parent_path(path)? else {
            return Ok(());
        };
        let resolved_parent = self.resolve_path_mut(&parent, true)?;
        self.ensure_writable_path(&parent)?;
        self.ensure_writable_path(&resolved_parent)?;
        let metadata = self.stat_raw_mut(&resolved_parent)?;
        if !metadata.is_dir {
            return Err(SandboxFsError::NotDirectory { path: parent });
        }
        if !sandbox_mode_allows_directory_mutation(metadata.mode) {
            return Err(SandboxFsError::ReadOnly { path: parent });
        }
        Ok(())
    }

    fn ensure_file_allows_read(&self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        let metadata = self.stat_raw(path)?;
        if metadata.is_dir {
            return Err(SandboxFsError::IsDirectory { path: path.clone() });
        }
        if !sandbox_mode_allows_file_read(metadata.mode) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(())
    }

    fn ensure_file_allows_read_mut(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        let metadata = self.stat_raw_mut(path)?;
        if metadata.is_dir {
            return Err(SandboxFsError::IsDirectory { path: path.clone() });
        }
        if !sandbox_mode_allows_file_read(metadata.mode) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(())
    }

    fn ensure_directory_allows_list(&self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        let metadata = self.stat_raw(path)?;
        if !metadata.is_dir {
            return Err(SandboxFsError::NotDirectory { path: path.clone() });
        }
        if !sandbox_mode_allows_directory_read(metadata.mode)
            || !sandbox_mode_allows_directory_search(metadata.mode)
        {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(())
    }

    fn ensure_directory_allows_list_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        let metadata = self.stat_raw_mut(path)?;
        if !metadata.is_dir {
            return Err(SandboxFsError::NotDirectory { path: path.clone() });
        }
        if !sandbox_mode_allows_directory_read(metadata.mode)
            || !sandbox_mode_allows_directory_search(metadata.mode)
        {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        Ok(())
    }

    fn ensure_write_target_allows_mutation_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        match self.stat_raw_mut(path) {
            Ok(metadata) if metadata.is_dir => {
                Err(SandboxFsError::IsDirectory { path: path.clone() })
            }
            Ok(metadata) if !sandbox_mode_allows_file_write(metadata.mode) => {
                Err(SandboxFsError::ReadOnly { path: path.clone() })
            }
            Ok(_) => Ok(()),
            Err(SandboxFsError::NotFound { .. }) => self.ensure_parent_allows_mutation_mut(path),
            Err(error) => Err(error),
        }
    }

    fn ensure_create_dir_all_allowed_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        if path.as_str() == "/" {
            return Ok(());
        }
        let mut current = SandboxPath::new("/")?;
        for component in path
            .as_str()
            .trim_start_matches('/')
            .split('/')
            .filter(|component| !component.is_empty())
        {
            current = current.join(component)?;
            match self.stat_raw_mut(&current) {
                Ok(metadata) if metadata.is_dir => {
                    if !sandbox_mode_allows_directory_search(metadata.mode) {
                        return Err(SandboxFsError::ReadOnly {
                            path: current.clone(),
                        });
                    }
                }
                Ok(_) => return Err(SandboxFsError::NotDirectory { path: current }),
                Err(SandboxFsError::NotFound { .. }) => {
                    self.ensure_parent_allows_mutation_mut(&current)?;
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn list_with_access_checks_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        if path.as_str() == "/" {
            let mut entries = self
                .dfs
                .posix_list_dir_distributed(&self.sifs_path(path))
                .map_err(|error| sandbox_fs_error_from_dfs(path, error))?
                .into_iter()
                .filter(|entry| entry.name != "dev")
                .map(|entry| {
                    let child_path = path.join(&entry.name)?;
                    let metadata = self.stat_unchecked_mut(&child_path)?;
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
            self.merge_mount_entries(&mut entries, path, false)?;
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
        let resolved = self.resolve_path_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return sandbox_virtual_device_dir_entries(&resolved)
                .into_iter()
                .map(|mut entry| {
                    if let Some(tail) = sandbox_mount_path_tail(&entry.path, &resolved) {
                        entry.path = if tail.is_empty() {
                            path.clone()
                        } else {
                            path.join(tail)?
                        };
                    }
                    Ok(entry)
                })
                .collect();
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.ensure_directory_allows_list_mut(&resolved)?;
        let entries = match self
            .dfs
            .posix_list_dir_distributed(&self.sifs_path(&resolved))
        {
            Ok(entries) => entries,
            Err(_error) if self.synthetic_mount_directory_metadata(&resolved).is_some() => {
                Vec::new()
            }
            Err(error) => return Err(sandbox_fs_error_from_dfs(path, error)),
        };
        let mut entries = entries
            .into_iter()
            .map(|entry| {
                let child_path = path.join(&entry.name)?;
                let metadata = self.stat_unchecked_mut(&child_path)?;
                Ok(SandboxDirEntry {
                    path: child_path,
                    metadata,
                })
            })
            .collect::<Result<Vec<_>, SandboxFsError>>()?;
        self.merge_mount_entries(&mut entries, path, false)?;
        Ok(entries)
    }

    fn mount_entries_for_list(
        &self,
        path: &SandboxPath,
        unchecked: bool,
    ) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        let mut entries = Vec::new();
        for mount in &self.mounts {
            let Some(child_path) = self.direct_mount_child_for_list(path, &mount.mount_path)?
            else {
                continue;
            };
            let metadata = if child_path == mount.mount_path {
                if unchecked {
                    self.stat_unchecked(&child_path)?
                } else {
                    self.stat(&child_path)?
                }
            } else {
                self.synthetic_mount_directory_metadata(&child_path)
                    .unwrap_or_else(|| SandboxMetadata {
                        path: child_path.clone(),
                        is_dir: true,
                        is_symlink: false,
                        len: 0,
                        link_count: 2,
                        mode: 0o555,
                        uid: 0,
                        gid: 0,
                        inode: sifs_inode_for_object(&format!("mount-ancestor:{child_path}"), 0),
                        object_identity: None,
                        content_hash: None,
                        accessed_at_unix_nanos: 1_000_000_000,
                        modified_at_unix_nanos: 1_000_000_000,
                        created_at_unix_nanos: 1_000_000_000,
                        changed_at_unix_nanos: 1_000_000_000,
                    })
            };
            entries.push(SandboxDirEntry {
                path: child_path,
                metadata,
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        entries.dedup_by(|left, right| left.path == right.path);
        Ok(entries)
    }

    fn merge_mount_entries(
        &self,
        entries: &mut Vec<SandboxDirEntry>,
        path: &SandboxPath,
        unchecked: bool,
    ) -> Result<(), SandboxFsError> {
        for mount_entry in self.mount_entries_for_list(path, unchecked)? {
            if let Some(existing) = entries
                .iter_mut()
                .find(|entry| entry.path == mount_entry.path)
            {
                if self.mount_resolution(&mount_entry.path).is_some() {
                    *existing = mount_entry;
                }
            } else {
                entries.push(mount_entry);
            }
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(())
    }
}

fn sandbox_generation_tail_for_instance_alias<'a>(
    generation_path: &'a str,
    instance_alias: &str,
) -> Option<&'a str> {
    let instances_prefix = format!("{SIFS_SANDBOX_ROOT}/instances/");
    let alias = instance_alias.strip_prefix(&instances_prefix)?;
    let (sandbox_id, alias_tail) = alias
        .split_once('/')
        .map_or((alias, ""), |(sandbox_id, tail)| (sandbox_id, tail));
    if sandbox_id.is_empty() {
        return None;
    }

    let generation_prefix = format!("{SIFS_SANDBOX_ROOT}/generations/{sandbox_id}/");
    let after_incarnation = generation_path
        .strip_prefix(&generation_prefix)?
        .split_once('/')?
        .1;
    let generation_tail = after_incarnation
        .split_once('/')
        .map_or("", |(_, tail)| tail);
    if alias_tail.is_empty() {
        return Some(generation_tail);
    }
    if generation_tail == alias_tail {
        return Some("");
    }
    generation_tail
        .strip_prefix(alias_tail)
        .and_then(|tail| tail.strip_prefix('/'))
}

struct SifsMountResolution {
    mount_len: usize,
    sifs_path: String,
    mode: SandboxMountMode,
}

impl<T: DfsMessageTransport> SandboxFileSystem for SifsSandboxFileSystem<T> {
    fn stat(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path(path, false)?;
        let mut metadata = self.stat_raw(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn stat_mut(&mut self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path_mut(path, false)?;
        let mut metadata = self.stat_raw_mut(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn stat_unchecked(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path_unchecked(path, false)?;
        let mut metadata = self.stat_raw(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn stat_unchecked_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path_unchecked_mut(path, false)?;
        let mut metadata = self.stat_raw_mut(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn stat_follow(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path(path, true)?;
        let mut metadata = self.stat(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn stat_follow_mut(&mut self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path_mut(path, true)?;
        let mut metadata = self.stat_raw_mut(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn stat_follow_unchecked(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path_unchecked(path, true)?;
        let mut metadata = self.stat_raw(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn stat_follow_unchecked_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        let resolved = self.resolve_path_unchecked_mut(path, true)?;
        let mut metadata = self.stat_raw_mut(&resolved)?;
        metadata.path = path.clone();
        Ok(metadata)
    }

    fn path_for_open_identity(
        &self,
        identity: Option<&str>,
        inode: u64,
    ) -> Result<Option<SandboxPath>, SandboxFsError> {
        let Some(identity) = identity else {
            return Ok(None);
        };
        let object_id = identity.parse().map_err(|_| SandboxFsError::Unavailable {
            reason: format!("invalid SIFS object identity: {identity}"),
        })?;
        let Some(stat) = self.dfs.posix_stat_object(&object_id).map_err(|error| {
            sandbox_fs_error_from_dfs(&SandboxPath::new("/").expect("root path"), error)
        })?
        else {
            return Ok(None);
        };
        if stat.kind == DfsObjectKind::Tombstone || stat.link_count == 0 {
            return Ok(None);
        }
        let Some(candidate_path) = self.sandbox_path_for_sifs_path(&stat.path) else {
            return Ok(None);
        };
        if self.metadata_from_stat(&candidate_path, stat).inode == inode {
            return Ok(Some(candidate_path));
        }
        Ok(None)
    }

    fn resolve_followed_path(&self, path: &SandboxPath) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path(path, true)
    }

    fn resolve_followed_path_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path_mut(path, true)
    }

    fn resolve_mutation_path(&self, path: &SandboxPath) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path(path, false)
    }

    fn resolve_mutation_path_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_path_for_mutation_mut(path)
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
        let resolved = self.resolve_path(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return read_sandbox_virtual_device(&resolved, range);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.ensure_file_allows_read(&resolved)?;
        let bytes = self
            .dfs
            .posix_read_file(&self.sifs_path(&resolved))
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

    fn read_unchecked(
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
        let resolved = self.resolve_path_unchecked(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return read_sandbox_virtual_device(&resolved, range);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let bytes = self
            .dfs
            .posix_read_file(&self.sifs_path(&resolved))
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        read_range(path, bytes, range)
    }

    fn read_unchecked_mut(
        &mut self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return read_sandbox_virtual_device(path, range);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let resolved = self.resolve_path_unchecked_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return read_sandbox_virtual_device(&resolved, range);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.read_sifs_file_mut(path, &resolved, range, false)
    }

    fn read_file(
        &mut self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return read_sandbox_virtual_device(path, range);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let resolved = self.resolve_path_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return read_sandbox_virtual_device(&resolved, range);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.ensure_file_allows_read_mut(&resolved)?;
        self.read_sifs_file_mut(path, &resolved, range, true)
    }

    fn read_unchecked_update_access(
        &mut self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return read_sandbox_virtual_device(path, range);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let resolved = self.resolve_path_unchecked_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return read_sandbox_virtual_device(&resolved, range);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.read_sifs_file_mut(path, &resolved, range, true)
    }

    fn create_dir(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if path.as_str() == "/" {
            return Err(SandboxFsError::AlreadyExists { path: path.clone() });
        }
        if matches!(
            sandbox_virtual_device_path(path),
            Some(
                SandboxVirtualDevicePath::DevDirectory
                    | SandboxVirtualDevicePath::FdDirectory
                    | SandboxVirtualDevicePath::PtsDirectory
                    | SandboxVirtualDevicePath::ShmDirectory
            )
        ) {
            return Err(SandboxFsError::AlreadyExists { path: path.clone() });
        }
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let resolved = self.resolve_path_for_mutation_mut(path)?;
        self.ensure_writable_path(path)?;
        self.ensure_writable_path(&resolved)?;
        self.ensure_parent_allows_mutation_mut(&resolved)?;
        self.dfs
            .posix_mkdir(&self.sifs_path(&resolved))
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
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
        let resolved = self.resolve_path_mut(path, true)?;
        match self.stat_raw_mut(&resolved) {
            Ok(metadata) if metadata.is_dir => return Ok(()),
            Ok(_) => return Err(SandboxFsError::NotDirectory { path: path.clone() }),
            Err(SandboxFsError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        self.ensure_writable_path(path)?;
        self.ensure_writable_path(&resolved)?;
        self.ensure_create_dir_all_allowed_mut(&resolved)?;
        self.dfs
            .posix_mkdir_all(&self.sifs_path(&resolved))
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
        let resolved = self.resolve_path_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            let _ = bytes;
            return write_sandbox_virtual_device(&resolved);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(&resolved)?;
        self.ensure_parent_dir(&resolved)?;
        self.ensure_write_target_allows_mutation_mut(&resolved)?;
        self.dfs
            .posix_write_file(&self.sifs_path(&resolved), bytes, self.write_options())
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn write_file(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError> {
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
        let resolved = self.resolve_path_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            let _ = bytes;
            return write_sandbox_virtual_device(&resolved);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(&resolved)?;
        self.ensure_write_target_allows_mutation_mut(&resolved)?;
        self.dfs
            .posix_write_file(&self.sifs_path(&resolved), bytes, self.write_options())
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn write_file_unchecked(
        &mut self,
        path: &SandboxPath,
        bytes: &[u8],
    ) -> Result<(), SandboxFsError> {
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
        let resolved = self.resolve_path_unchecked_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            let _ = bytes;
            return write_sandbox_virtual_device(&resolved);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(&resolved)?;
        self.dfs
            .posix_write_file(&self.sifs_path(&resolved), bytes, self.write_options())
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn write_at_unchecked(
        &mut self,
        path: &SandboxPath,
        offset: u64,
        bytes: &[u8],
    ) -> Result<SandboxMetadata, SandboxFsError> {
        if path.as_str() == "/" {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_virtual_device_path(path).is_some() {
            let _ = (offset, bytes);
            write_sandbox_virtual_device(path)?;
            return self.stat_unchecked(path);
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        let resolved = self.resolve_path_unchecked_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            let _ = (offset, bytes);
            write_sandbox_virtual_device(&resolved)?;
            return self.stat_unchecked(&resolved);
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(&resolved)?;
        let stat = self
            .dfs
            .posix_write_file_at(
                &self.sifs_path(&resolved),
                offset,
                bytes,
                self.write_options(),
            )
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        Ok(self.metadata_from_stat(path, stat))
    }

    fn set_len_unchecked(
        &mut self,
        path: &SandboxPath,
        len: u64,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        if path.as_str() == "/" {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::Unsupported {
                operation: format!("truncate virtual device {path}"),
            });
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        let resolved = self.resolve_path_unchecked_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return Err(SandboxFsError::Unsupported {
                operation: format!("truncate virtual device {resolved}"),
            });
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(&resolved)?;
        let stat = self
            .dfs
            .posix_truncate_file(&self.sifs_path(&resolved), len, self.write_options())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        Ok(self.metadata_from_stat(path, stat))
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
            self.merge_mount_entries(&mut entries, path, false)?;
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
        let resolved = self.resolve_path(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return sandbox_virtual_device_dir_entries(&resolved)
                .into_iter()
                .map(|mut entry| {
                    if let Some(tail) = sandbox_mount_path_tail(&entry.path, &resolved) {
                        entry.path = if tail.is_empty() {
                            path.clone()
                        } else {
                            path.join(tail)?
                        };
                    }
                    Ok(entry)
                })
                .collect();
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        self.ensure_directory_allows_list(&resolved)?;
        let entries = match self.dfs.posix_list_dir(&self.sifs_path(&resolved)) {
            Ok(entries) => entries,
            Err(_error) if self.synthetic_mount_directory_metadata(&resolved).is_some() => {
                Vec::new()
            }
            Err(error) => return Err(sandbox_fs_error_from_dfs(path, error)),
        };
        let mut entries = entries
            .into_iter()
            .map(|entry| {
                let child_path = path.join(&entry.name)?;
                let metadata = self.stat(&child_path)?;
                Ok(SandboxDirEntry {
                    path: child_path,
                    metadata,
                })
            })
            .collect::<Result<Vec<_>, SandboxFsError>>()?;
        self.merge_mount_entries(&mut entries, path, false)?;
        Ok(entries)
    }

    fn list_update_access(
        &mut self,
        path: &SandboxPath,
    ) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        let entries = self.list_with_access_checks_mut(path)?;
        if path.as_str() == "/" {
            return Ok(entries);
        }
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Ok(entries);
        }
        let resolved = self.resolve_path_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() || sandbox_path_is_dev_tree(&resolved) {
            return Ok(entries);
        }
        if self.synthetic_mount_directory_metadata(&resolved).is_some() {
            return Ok(entries);
        }
        self.dfs
            .posix_list_dir_update_access(&self.sifs_path(&resolved))
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))?;
        Ok(entries)
    }

    fn list_unchecked(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        if path.as_str() == "/" {
            let mut entries = self
                .dfs
                .posix_list_dir(&self.sifs_path(path))
                .map_err(|error| sandbox_fs_error_from_dfs(path, error))?
                .into_iter()
                .filter(|entry| entry.name != "dev")
                .map(|entry| {
                    let child_path = path.join(&entry.name)?;
                    let metadata = self.stat_unchecked(&child_path)?;
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
            self.merge_mount_entries(&mut entries, path, true)?;
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
        let resolved = self.resolve_path_unchecked(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return sandbox_virtual_device_dir_entries(&resolved)
                .into_iter()
                .map(|mut entry| {
                    if let Some(tail) = sandbox_mount_path_tail(&entry.path, &resolved) {
                        entry.path = if tail.is_empty() {
                            path.clone()
                        } else {
                            path.join(tail)?
                        };
                    }
                    Ok(entry)
                })
                .collect();
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let entries = match self.dfs.posix_list_dir(&self.sifs_path(&resolved)) {
            Ok(entries) => entries,
            Err(_error) if self.synthetic_mount_directory_metadata(&resolved).is_some() => {
                Vec::new()
            }
            Err(error) => return Err(sandbox_fs_error_from_dfs(path, error)),
        };
        let mut entries = entries
            .into_iter()
            .map(|entry| {
                let child_path = path.join(&entry.name)?;
                let metadata = self.stat_unchecked(&child_path)?;
                Ok(SandboxDirEntry {
                    path: child_path,
                    metadata,
                })
            })
            .collect::<Result<Vec<_>, SandboxFsError>>()?;
        self.merge_mount_entries(&mut entries, path, true)?;
        Ok(entries)
    }

    fn list_unchecked_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        if path.as_str() == "/" {
            let mut entries = self
                .dfs
                .posix_list_dir_distributed(&self.sifs_path(path))
                .map_err(|error| sandbox_fs_error_from_dfs(path, error))?
                .into_iter()
                .filter(|entry| entry.name != "dev")
                .map(|entry| {
                    let child_path = path.join(&entry.name)?;
                    let metadata = self.stat_unchecked_mut(&child_path)?;
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
            self.merge_mount_entries(&mut entries, path, true)?;
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
        let resolved = self.resolve_path_unchecked_mut(path, true)?;
        if sandbox_virtual_device_path(&resolved).is_some() {
            return sandbox_virtual_device_dir_entries(&resolved)
                .into_iter()
                .map(|mut entry| {
                    if let Some(tail) = sandbox_mount_path_tail(&entry.path, &resolved) {
                        entry.path = if tail.is_empty() {
                            path.clone()
                        } else {
                            path.join(tail)?
                        };
                    }
                    Ok(entry)
                })
                .collect();
        }
        if sandbox_path_is_dev_tree(&resolved) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let entries = match self
            .dfs
            .posix_list_dir_distributed(&self.sifs_path(&resolved))
        {
            Ok(entries) => entries,
            Err(_error) if self.synthetic_mount_directory_metadata(&resolved).is_some() => {
                Vec::new()
            }
            Err(error) => return Err(sandbox_fs_error_from_dfs(path, error)),
        };
        let mut entries = entries
            .into_iter()
            .map(|entry| {
                let child_path = path.join(&entry.name)?;
                let metadata = self.stat_unchecked_mut(&child_path)?;
                Ok(SandboxDirEntry {
                    path: child_path,
                    metadata,
                })
            })
            .collect::<Result<Vec<_>, SandboxFsError>>()?;
        self.merge_mount_entries(&mut entries, path, true)?;
        Ok(entries)
    }

    fn rename(&mut self, from: &SandboxPath, to: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(from).is_some() || sandbox_virtual_device_path(to).is_some()
        {
            return Err(SandboxFsError::ReadOnly { path: from.clone() });
        }
        if sandbox_path_is_dev_tree(from) || sandbox_path_is_dev_tree(to) {
            return Err(SandboxFsError::ReadOnly { path: from.clone() });
        }
        let resolved_from = self.resolve_path_for_mutation_mut(from)?;
        let resolved_to = self.resolve_path_for_mutation_mut(to)?;
        self.ensure_namespace_path_is_not_mount_anchor(from)?;
        self.ensure_namespace_path_is_not_mount_anchor(to)?;
        self.ensure_namespace_path_is_not_mount_anchor(&resolved_from)?;
        self.ensure_namespace_path_is_not_mount_anchor(&resolved_to)?;
        self.ensure_writable_path(from)?;
        self.ensure_writable_path(to)?;
        self.ensure_writable_path(&resolved_from)?;
        self.ensure_writable_path(&resolved_to)?;
        self.ensure_parent_allows_mutation_mut(&resolved_from)?;
        self.ensure_parent_allows_mutation_mut(&resolved_to)?;
        self.ensure_same_mount_domain("rename", &resolved_from, &resolved_to)?;
        self.dfs
            .posix_rename(
                &self.sifs_path(&resolved_from),
                &self.sifs_path(&resolved_to),
            )
            .map(|_| {
                self.symlink_target_cache.clear();
            })
            .map_err(|error| sandbox_fs_error_from_dfs(from, error))
    }

    fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let resolved = self.resolve_path_for_mutation_mut(path)?;
        self.ensure_namespace_path_is_not_mount_anchor(path)?;
        self.ensure_namespace_path_is_not_mount_anchor(&resolved)?;
        self.ensure_writable_path(path)?;
        self.ensure_writable_path(&resolved)?;
        self.ensure_parent_allows_mutation_mut(&resolved)?;
        match self.dfs.posix_lstat(&self.sifs_path(&resolved)) {
            Ok(stat) if stat.kind == DfsObjectKind::Directory => {
                Err(SandboxFsError::IsDirectory { path: path.clone() })
            }
            Ok(_) => self
                .dfs
                .posix_unlink(&self.sifs_path(&resolved))
                .map(|_| {
                    self.symlink_target_cache.clear();
                })
                .map_err(|error| sandbox_fs_error_from_dfs(path, error)),
            Err(error) => Err(sandbox_fs_error_from_dfs(path, error)),
        }
    }

    fn remove_dir(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let resolved = self.resolve_path_for_mutation_mut(path)?;
        self.ensure_namespace_path_is_not_mount_anchor(path)?;
        self.ensure_namespace_path_is_not_mount_anchor(&resolved)?;
        self.ensure_writable_path(path)?;
        self.ensure_writable_path(&resolved)?;
        self.ensure_parent_allows_mutation_mut(&resolved)?;
        self.dfs
            .posix_rmdir(&self.sifs_path(&resolved))
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn readlink(&self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let resolved = self.resolve_path(path, false)?;
        self.dfs
            .posix_readlink(&self.sifs_path(&resolved))
            .map(PathBuf::from)
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn readlink_mut(&mut self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let resolved = self.resolve_path_mut(path, false)?;
        self.readlink_sifs_path_mut(&resolved, false)
    }

    fn readlink_update_access(&mut self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let resolved = self.resolve_path_mut(path, false)?;
        self.readlink_sifs_path_mut(&resolved, true)
    }

    fn symlink(&mut self, target: &Path, link_path: &SandboxPath) -> Result<(), SandboxFsError> {
        if link_path.as_str() == "/" {
            return Err(SandboxFsError::ReadOnly {
                path: link_path.clone(),
            });
        }
        if sandbox_virtual_device_path(link_path).is_some() || sandbox_path_is_dev_tree(link_path) {
            return Err(SandboxFsError::ReadOnly {
                path: link_path.clone(),
            });
        }
        let resolved_link_path = self.resolve_path_for_mutation_mut(link_path)?;
        self.ensure_writable_path(link_path)?;
        self.ensure_writable_path(&resolved_link_path)?;
        self.ensure_parent_allows_mutation_mut(&resolved_link_path)?;
        self.dfs
            .posix_symlink(
                &target.to_string_lossy(),
                &self.sifs_path(&resolved_link_path),
                self.write_options(),
            )
            .map(|_| {
                self.symlink_target_cache.clear();
            })
            .map_err(|error| sandbox_fs_error_from_dfs(link_path, error))
    }

    fn hard_link(
        &mut self,
        old_path: &SandboxPath,
        new_path: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(old_path).is_some()
            || sandbox_virtual_device_path(new_path).is_some()
            || sandbox_path_is_dev_tree(old_path)
            || sandbox_path_is_dev_tree(new_path)
        {
            return Err(SandboxFsError::ReadOnly {
                path: new_path.clone(),
            });
        }
        let resolved_old_path = self.resolve_path_for_mutation_mut(old_path)?;
        let resolved_new_path = self.resolve_path_for_mutation_mut(new_path)?;
        self.ensure_writable_path(old_path)?;
        self.ensure_writable_path(new_path)?;
        self.ensure_writable_path(&resolved_old_path)?;
        self.ensure_writable_path(&resolved_new_path)?;
        self.ensure_parent_allows_mutation_mut(&resolved_new_path)?;
        self.ensure_same_mount_domain("hard link", &resolved_old_path, &resolved_new_path)?;
        self.dfs
            .posix_hardlink(
                &self.sifs_path(&resolved_old_path),
                &self.sifs_path(&resolved_new_path),
            )
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(new_path, error))
    }

    fn set_times(
        &mut self,
        path: &SandboxPath,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        let resolved = self.resolve_path_mut(path, true)?;
        self.ensure_writable_path(&resolved)?;
        self.dfs
            .posix_set_times(
                &self.sifs_path(&resolved),
                accessed_at_unix_nanos,
                modified_at_unix_nanos,
            )
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn set_symlink_times(
        &mut self,
        path: &SandboxPath,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        let resolved = self.resolve_path_mut(path, false)?;
        self.ensure_writable_path(&resolved)?;
        self.dfs
            .posix_set_symlink_times(
                &self.sifs_path(&resolved),
                accessed_at_unix_nanos,
                modified_at_unix_nanos,
            )
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn set_mode(&mut self, path: &SandboxPath, mode: u32) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        let resolved = self.resolve_path_mut(path, true)?;
        self.ensure_writable_path(&resolved)?;
        self.dfs
            .posix_set_mode(&self.sifs_path(&resolved), mode)
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
    }

    fn set_owner(&mut self, path: &SandboxPath, uid: u32, gid: u32) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.ensure_writable_path(path)?;
        let resolved = self.resolve_path_mut(path, true)?;
        self.ensure_writable_path(&resolved)?;
        self.dfs
            .posix_set_owner(&self.sifs_path(&resolved), uid, gid)
            .map(|_| ())
            .map_err(|error| sandbox_fs_error_from_dfs(path, error))
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

fn read_range(
    path: &SandboxPath,
    bytes: Vec<u8>,
    range: Option<ByteRange>,
) -> Result<Vec<u8>, SandboxFsError> {
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

fn sifs_inode_for_object(object_id: &str, created_at_unix_nanos: u64) -> u64 {
    let hash = blake3::hash(format!("{object_id}:{created_at_unix_nanos}").as_bytes());
    u64::from_le_bytes(hash.as_bytes()[0..8].try_into().expect("hash slice"))
}

fn sandbox_mode_allows_file_read(mode: u32) -> bool {
    mode & 0o444 != 0
}

fn sandbox_mode_allows_file_write(mode: u32) -> bool {
    mode & 0o222 != 0
}

fn sandbox_mode_allows_directory_read(mode: u32) -> bool {
    mode & 0o444 != 0
}

fn sandbox_mode_allows_directory_search(mode: u32) -> bool {
    mode & 0o111 != 0
}

fn sandbox_mode_allows_directory_mutation(mode: u32) -> bool {
    sandbox_mode_allows_file_write(mode) && sandbox_mode_allows_directory_search(mode)
}
