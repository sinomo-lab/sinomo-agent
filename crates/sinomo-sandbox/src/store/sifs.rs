use crate::internal::*;

use super::{decode_instance_manifest, encode_instance_manifest};

#[derive(Debug)]
pub struct SifsSandboxInstanceStore<T: DfsMessageTransport> {
    pub dfs: NetworkedDfsService<T>,
    replica_candidates: Vec<NodeId>,
}

impl<T: DfsMessageTransport> SifsSandboxInstanceStore<T> {
    pub fn new(dfs: NetworkedDfsService<T>) -> Self {
        let replica_candidates = vec![dfs.node_id().clone()];
        Self {
            dfs,
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

    pub fn create(
        &mut self,
        sandbox_id: SandboxId,
        toolchain_id: SandboxToolchainId,
        now: OffsetDateTime,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let manifest_path = sandbox_manifest_path(&sandbox_id);
        match self.dfs.posix_stat_distributed(&manifest_path) {
            Ok(_) => return Err(SandboxStoreError::AlreadyExists { sandbox_id }),
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {}
            Err(source) => {
                return Err(sandbox_store_error_from_dfs(manifest_path, source));
            }
        }

        let manifest = SandboxInstanceManifest::new(sandbox_id, toolchain_id, now)?;
        self.ensure_instance_dirs(&manifest)?;
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn load(
        &mut self,
        sandbox_id: &SandboxId,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let path = sandbox_manifest_path(sandbox_id);
        let bytes = self
            .dfs
            .posix_read_file_distributed(&path)
            .map_err(|source| match source {
                DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
                    SandboxStoreError::NotFound {
                        sandbox_id: sandbox_id.clone(),
                    }
                }
                other => sandbox_store_error_from_dfs(path.clone(), other),
            })?;
        decode_instance_manifest(&bytes, &path)
    }

    pub fn save_manifest(
        &mut self,
        manifest: &SandboxInstanceManifest,
    ) -> Result<(), SandboxStoreError> {
        let path = sandbox_manifest_path(&manifest.sandbox_id);
        self.dfs
            .posix_stat_distributed(&manifest.workspace_root)
            .map_err(|source| match source {
                DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
                    SandboxStoreError::NotFound {
                        sandbox_id: manifest.sandbox_id.clone(),
                    }
                }
                other => sandbox_store_error_from_dfs(manifest.workspace_root.clone(), other),
            })?;
        self.ensure_instance_dirs(manifest)?;
        self.write_manifest(manifest).map_err(|error| match error {
            SandboxStoreError::Io { .. } | SandboxStoreError::Object { .. } => error,
            other => SandboxStoreError::Io {
                path,
                detail: other.to_string(),
            },
        })
    }

    pub fn create_checkpoint(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<SandboxCheckpoint, SandboxStoreError> {
        let manifest = self.load(sandbox_id)?;
        let checkpoint_id = SandboxCheckpointId::generate();
        let checkpoint_root = format!(
            "{}/checkpoints/{}",
            manifest.workspace_root.trim_end_matches('/'),
            checkpoint_id
        );
        self.dfs
            .posix_mkdir_all(&checkpoint_root)
            .map_err(|source| sandbox_store_error_from_dfs(checkpoint_root.clone(), source))?;
        let mut hardlink_targets = BTreeMap::new();
        for name in sifs_checkpoint_root_names() {
            self.copy_sifs_tree_preserving_hardlinks(
                &format!("{}/{}", manifest.workspace_root.trim_end_matches('/'), name),
                &format!("{checkpoint_root}/{name}"),
                &mut hardlink_targets,
            )?;
        }
        let manifest_ref = format!("{checkpoint_root}/manifest.sobj");
        let bytes = encode_instance_manifest(&manifest, &manifest_ref)?;
        self.dfs
            .posix_write_file(&manifest_ref, &bytes, self.write_options())
            .map_err(|source| sandbox_store_error_from_dfs(manifest_ref.clone(), source))?;
        Ok(SandboxCheckpoint {
            checkpoint_id,
            sandbox_id: sandbox_id.clone(),
            created_at: now,
            manifest_ref,
        })
    }

    pub fn resume_checkpoint(
        &mut self,
        sandbox_id: &SandboxId,
        checkpoint_id: &SandboxCheckpointId,
        now: OffsetDateTime,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let current = self.load(sandbox_id)?;
        let checkpoint_root = format!(
            "{}/checkpoints/{}",
            current.workspace_root.trim_end_matches('/'),
            checkpoint_id
        );
        let checkpoint_manifest_path = format!("{checkpoint_root}/manifest.sobj");
        let bytes = self
            .dfs
            .posix_read_file_distributed(&checkpoint_manifest_path)
            .map_err(|source| match source {
                DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
                    SandboxStoreError::CheckpointNotFound {
                        sandbox_id: sandbox_id.clone(),
                        checkpoint_id: checkpoint_id.clone(),
                    }
                }
                other => sandbox_store_error_from_dfs(checkpoint_manifest_path.clone(), other),
            })?;
        let mut manifest = decode_instance_manifest(&bytes, &checkpoint_manifest_path)?;
        if self.checkpoint_tree_matches_current(
            &checkpoint_root,
            current.workspace_root.trim_end_matches('/'),
        )? {
            manifest.updated_at = now;
            self.write_manifest(&manifest)?;
            return Ok(manifest);
        }
        let mut hardlink_targets = BTreeMap::new();
        for name in sifs_checkpoint_root_names() {
            let destination = format!("{}/{}", current.workspace_root.trim_end_matches('/'), name);
            let source = format!("{checkpoint_root}/{name}");
            match self.dfs.posix_stat_distributed(&source) {
                Ok(stat) if stat.kind == DfsObjectKind::Directory => {}
                Ok(_) => {
                    return Err(SandboxStoreError::Io {
                        path: source,
                        detail: "checkpoint is missing a required directory".to_string(),
                    });
                }
                Err(error) => {
                    return Err(sandbox_store_error_from_dfs(source, error));
                }
            }
            self.clear_sifs_directory(&destination)?;
            self.copy_sifs_directory_contents(&source, &destination, &mut hardlink_targets)?;
        }
        manifest.updated_at = now;
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn reboot_session(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, bool), SandboxStoreError> {
        let mut manifest = self.load(sandbox_id)?;
        let cleared_tmp = match manifest.temp_policy {
            SandboxTempPolicy::ClearOnSessionStart => {
                let old_tmp_path = format!(
                    "{}/tmp/{}",
                    manifest.workspace_root.trim_end_matches('/'),
                    manifest.temp_epoch
                );
                self.remove_sifs_tree_if_exists(&old_tmp_path)?;
                manifest.temp_epoch = manifest.temp_epoch.saturating_add(1);
                let tmp_path = format!(
                    "{}/tmp/{}",
                    manifest.workspace_root.trim_end_matches('/'),
                    manifest.temp_epoch
                );
                self.dfs
                    .posix_mkdir_all(&tmp_path)
                    .map_err(|error| sandbox_store_error_from_dfs(tmp_path.clone(), error))?;
                true
            }
            SandboxTempPolicy::RetainAcrossSessions => false,
        };
        manifest.updated_at = now;
        self.write_manifest(&manifest)?;
        Ok((manifest, cleared_tmp))
    }

    pub fn delete_checkpoint(
        &mut self,
        sandbox_id: &SandboxId,
        checkpoint_id: &SandboxCheckpointId,
    ) -> Result<bool, SandboxStoreError> {
        let manifest = self.load(sandbox_id)?;
        let checkpoint_root = format!(
            "{}/checkpoints/{}",
            manifest.workspace_root.trim_end_matches('/'),
            checkpoint_id
        );
        self.remove_sifs_tree_if_exists(&checkpoint_root)
    }

    pub fn destroy(&mut self, sandbox_id: &SandboxId) -> Result<(), SandboxStoreError> {
        let root = sandbox_instance_root(sandbox_id);
        match self.remove_sifs_tree_if_exists(&root)? {
            true => Ok(()),
            false => Err(SandboxStoreError::NotFound {
                sandbox_id: sandbox_id.clone(),
            }),
        }
    }

    fn ensure_instance_dirs(
        &mut self,
        manifest: &SandboxInstanceManifest,
    ) -> Result<(), SandboxStoreError> {
        self.dfs
            .posix_mkdir_all(&manifest.workspace_root)
            .map_err(|source| {
                sandbox_store_error_from_dfs(manifest.workspace_root.clone(), source)
            })?;
        for path in [
            "root",
            "home",
            "tmp",
            "work",
            "var",
            "checkpoints",
            "logs",
            "artifacts",
        ] {
            let sifs_path = format!("{}/{}", manifest.workspace_root.trim_end_matches('/'), path);
            self.dfs
                .posix_mkdir_all(&sifs_path)
                .map_err(|source| sandbox_store_error_from_dfs(sifs_path, source))?;
        }
        let tmp_epoch_path = format!(
            "{}/tmp/{}",
            manifest.workspace_root.trim_end_matches('/'),
            manifest.temp_epoch
        );
        self.dfs
            .posix_mkdir_all(&tmp_epoch_path)
            .map_err(|source| sandbox_store_error_from_dfs(tmp_epoch_path, source))?;
        Ok(())
    }

    fn write_manifest(
        &mut self,
        manifest: &SandboxInstanceManifest,
    ) -> Result<(), SandboxStoreError> {
        let path = sandbox_manifest_path(&manifest.sandbox_id);
        let bytes = encode_instance_manifest(manifest, &path)?;
        self.dfs
            .posix_write_file(&path, &bytes, self.write_options())
            .map(|_| ())
            .map_err(|source| sandbox_store_error_from_dfs(path, source))
    }

    fn write_options(&self) -> DfsWriteOptions {
        DfsWriteOptions::replicated_eventual(self.replica_candidates.clone())
    }

    fn copy_sifs_tree_preserving_hardlinks(
        &mut self,
        source: &str,
        destination: &str,
        hardlink_targets: &mut BTreeMap<String, String>,
    ) -> Result<(), SandboxStoreError> {
        let stat = self
            .dfs
            .posix_lstat_distributed(source)
            .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
        match stat.kind {
            DfsObjectKind::Directory => {
                self.dfs.posix_mkdir_all(destination).map_err(|error| {
                    sandbox_store_error_from_dfs(destination.to_string(), error)
                })?;
                let entries = self
                    .dfs
                    .posix_list_dir_distributed(source)
                    .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
                for entry in entries {
                    self.copy_sifs_tree_preserving_hardlinks(
                        &entry.path,
                        &format!("{}/{}", destination.trim_end_matches('/'), entry.name),
                        hardlink_targets,
                    )?;
                }
                Ok(())
            }
            DfsObjectKind::File | DfsObjectKind::Artifact => {
                if stat.link_count > 1 {
                    let identity = stat.object_id.to_string();
                    if let Some(existing_destination) = hardlink_targets.get(&identity) {
                        return self
                            .dfs
                            .posix_hardlink(existing_destination, destination)
                            .map(|_| ())
                            .map_err(|error| {
                                sandbox_store_error_from_dfs(destination.to_string(), error)
                            });
                    }
                    hardlink_targets.insert(identity, destination.to_string());
                }
                let bytes = self
                    .dfs
                    .posix_read_file_distributed(source)
                    .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
                self.dfs
                    .posix_write_file(destination, &bytes, self.write_options())
                    .map(|_| ())
                    .map_err(|error| sandbox_store_error_from_dfs(destination.to_string(), error))
            }
            DfsObjectKind::Symlink => {
                if stat.link_count > 1 {
                    let identity = stat.object_id.to_string();
                    if let Some(existing_destination) = hardlink_targets.get(&identity) {
                        return self
                            .dfs
                            .posix_hardlink(existing_destination, destination)
                            .map(|_| ())
                            .map_err(|error| {
                                sandbox_store_error_from_dfs(destination.to_string(), error)
                            });
                    }
                    hardlink_targets.insert(identity, destination.to_string());
                }
                let target = self
                    .dfs
                    .posix_readlink_distributed(source)
                    .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
                self.dfs
                    .posix_symlink(&target, destination, self.write_options())
                    .map(|_| ())
                    .map_err(|error| sandbox_store_error_from_dfs(destination.to_string(), error))
            }
            other => Err(SandboxStoreError::Io {
                path: source.to_string(),
                detail: format!("refusing to copy unsupported DFS object kind {other:?}"),
            }),
        }
    }

    fn copy_sifs_directory_contents(
        &mut self,
        source: &str,
        destination: &str,
        hardlink_targets: &mut BTreeMap<String, String>,
    ) -> Result<(), SandboxStoreError> {
        self.dfs
            .posix_mkdir_all(destination)
            .map_err(|error| sandbox_store_error_from_dfs(destination.to_string(), error))?;
        let entries = self
            .dfs
            .posix_list_dir_distributed(source)
            .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
        for entry in entries {
            let destination_child = format!("{}/{}", destination.trim_end_matches('/'), entry.name);
            self.remove_sifs_tree_if_exists(&destination_child)?;
            self.copy_sifs_tree_preserving_hardlinks(
                &entry.path,
                &destination_child,
                hardlink_targets,
            )?;
        }
        Ok(())
    }

    fn checkpoint_tree_matches_current(
        &mut self,
        checkpoint_root: &str,
        workspace_root: &str,
    ) -> Result<bool, SandboxStoreError> {
        for name in sifs_checkpoint_root_names() {
            let source = format!("{checkpoint_root}/{name}");
            let destination = format!("{}/{}", workspace_root.trim_end_matches('/'), name);
            if !self.sifs_tree_content_matches(&source, &destination)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn sifs_tree_content_matches(
        &mut self,
        source: &str,
        destination: &str,
    ) -> Result<bool, SandboxStoreError> {
        let source_stat = self
            .dfs
            .posix_lstat_distributed(source)
            .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
        let destination_stat = match self.dfs.posix_lstat_distributed(destination) {
            Ok(stat) => stat,
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
                return Ok(false);
            }
            Err(error) => return Err(sandbox_store_error_from_dfs(destination.to_string(), error)),
        };
        match (&source_stat.kind, &destination_stat.kind) {
            (DfsObjectKind::Directory, DfsObjectKind::Directory) => {
                let source_entries = self
                    .dfs
                    .posix_list_dir_distributed(source)
                    .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?
                    .into_iter()
                    .map(|entry| (entry.name, entry.path))
                    .collect::<BTreeMap<_, _>>();
                let destination_entries = self
                    .dfs
                    .posix_list_dir_distributed(destination)
                    .map_err(|error| sandbox_store_error_from_dfs(destination.to_string(), error))?
                    .into_iter()
                    .map(|entry| (entry.name, entry.path))
                    .collect::<BTreeMap<_, _>>();
                if source_entries.keys().ne(destination_entries.keys()) {
                    return Ok(false);
                }
                for (name, source_child) in source_entries {
                    let destination_child = destination_entries
                        .get(&name)
                        .expect("destination key already checked");
                    if !self.sifs_tree_content_matches(&source_child, destination_child)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            (
                DfsObjectKind::File | DfsObjectKind::Artifact,
                DfsObjectKind::File | DfsObjectKind::Artifact,
            ) => Ok(source_stat.content_hash == destination_stat.content_hash
                && source_stat.size_bytes == destination_stat.size_bytes),
            (DfsObjectKind::Symlink, DfsObjectKind::Symlink) => {
                let source_target = self
                    .dfs
                    .posix_readlink_distributed(source)
                    .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
                let destination_target =
                    self.dfs
                        .posix_readlink_distributed(destination)
                        .map_err(|error| {
                            sandbox_store_error_from_dfs(destination.to_string(), error)
                        })?;
                Ok(source_target == destination_target)
            }
            _ => Ok(false),
        }
    }

    fn clear_sifs_directory(&mut self, path: &str) -> Result<(), SandboxStoreError> {
        self.dfs
            .posix_mkdir_all(path)
            .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))?;
        let entries = self
            .dfs
            .posix_list_dir_distributed(path)
            .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))?;
        for entry in entries {
            self.remove_sifs_tree_if_exists(&entry.path)?;
        }
        Ok(())
    }

    fn remove_sifs_tree_if_exists(&mut self, path: &str) -> Result<bool, SandboxStoreError> {
        let stat = match self.dfs.posix_lstat_distributed(path) {
            Ok(stat) => stat,
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
                return Ok(false);
            }
            Err(error) => return Err(sandbox_store_error_from_dfs(path.to_string(), error)),
        };
        match stat.kind {
            DfsObjectKind::Directory => {
                let entries = self
                    .dfs
                    .posix_list_dir_distributed(path)
                    .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))?;
                for entry in entries {
                    self.remove_sifs_tree_if_exists(&entry.path)?;
                }
                self.dfs
                    .posix_rmdir(path)
                    .map(|_| true)
                    .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))
            }
            DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink => self
                .dfs
                .posix_unlink(path)
                .map(|_| true)
                .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error)),
            other => Err(SandboxStoreError::Io {
                path: path.to_string(),
                detail: format!("refusing to remove unsupported DFS object kind {other:?}"),
            }),
        }
    }
}

fn sifs_checkpoint_root_names() -> [&'static str; 7] {
    ["root", "home", "tmp", "work", "var", "logs", "artifacts"]
}
