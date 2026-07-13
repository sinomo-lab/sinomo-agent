use crate::internal::*;

use super::{decode_instance_manifest, encode_instance_manifest};

const SIFS_SANDBOX_NAMESPACE_JOB_BATCH_STEPS: usize = 128;

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
        let manifest = SandboxInstanceManifest::new(sandbox_id, toolchain_id, now)?;
        let pending = self.create_manifest(&manifest)?;
        let transaction_id = pending
            .pending_reconciliation
            .as_ref()
            .expect("create_manifest always returns its lifecycle fence")
            .transaction_id
            .clone();
        self.clear_reconciliation_transaction(&pending.sandbox_id, &transaction_id, now)
    }

    /// Start or resume creation of one fully validated candidate manifest. The
    /// exact candidate state is fenced before the stable generation is
    /// published; retries with a conflicting request fail closed.
    pub fn create_manifest(
        &mut self,
        candidate: &SandboxInstanceManifest,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let sandbox_id = candidate.sandbox_id.clone();
        let now = candidate.updated_at;
        if candidate.workspace_root != sandbox_instance_root(&sandbox_id)
            || candidate.pending_reconciliation.is_some()
        {
            return Err(SandboxStoreError::Object {
                path: sandbox_manifest_path(&sandbox_id),
                detail: "create candidate must use the stable workspace root without a pending transaction"
                    .to_string(),
            });
        }
        let manifest_path = sandbox_manifest_path(&sandbox_id);
        let (mut manifest, target_generation) = match self
            .dfs
            .posix_read_file_distributed(&manifest_path)
        {
            Ok(bytes) => {
                let manifest = decode_instance_manifest(&bytes, &manifest_path)?;
                let pending = manifest.pending_reconciliation.as_ref().ok_or_else(|| {
                    SandboxStoreError::AlreadyExists {
                        sandbox_id: sandbox_id.clone(),
                    }
                })?;
                if !sandbox_create_candidate_matches(&manifest, candidate)
                    || !matches!(
                        pending.operation,
                        SandboxAuthoritativeMutationOperation::Create
                    )
                {
                    return Err(SandboxStoreError::AlreadyExists { sandbox_id });
                }
                let target = pending.target_workspace_root.clone().ok_or_else(|| {
                    SandboxStoreError::Object {
                        path: manifest_path.clone(),
                        detail: "pending create is missing its target generation".to_string(),
                    }
                })?;
                (manifest, target)
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
                let stable_root = sandbox_instance_root(&sandbox_id);
                match self.dfs.posix_lstat_distributed(&stable_root) {
                    Ok(stat) if stat.kind == DfsObjectKind::Directory => {
                        return Err(SandboxStoreError::LegacyLayout {
                            sandbox_id,
                            path: stable_root,
                        });
                    }
                    Ok(_) => return Err(SandboxStoreError::AlreadyExists { sandbox_id }),
                    Err(DfsPosixError::NotFound { .. })
                    | Err(DfsPosixError::MissingParent { .. }) => {}
                    Err(error) => {
                        return Err(sandbox_store_error_from_dfs(stable_root, error));
                    }
                }
                let mut manifest = candidate.clone();
                let transaction_id = Id::generate();
                let incarnation_id = manifest.effective_incarnation_id();
                let target = sandbox_transaction_workspace_root(
                    &sandbox_id,
                    &incarnation_id,
                    &transaction_id,
                );
                manifest.pending_reconciliation = Some(SandboxAuthoritativeReconciliationFence {
                    transaction_id,
                    source_incarnation_id: incarnation_id,
                    source_generation: manifest.generation,
                    previous_workspace_root: None,
                    target_workspace_root: Some(target.clone()),
                    operation: SandboxAuthoritativeMutationOperation::Create,
                });
                self.dfs
                    .posix_mkdir_all(&sandbox_control_root(&sandbox_id))
                    .map_err(|error| sandbox_store_error_from_dfs(manifest_path.clone(), error))?;
                self.write_manifest(&manifest)?;
                (manifest, target)
            }
            Err(error) => {
                return Err(sandbox_store_error_from_dfs(manifest_path, error));
            }
        };

        let mut staging = manifest.clone();
        staging.workspace_root = target_generation.clone();
        self.ensure_instance_dirs(&staging)?;
        self.publish_stable_generation_link(
            &sandbox_id,
            manifest
                .pending_reconciliation
                .as_ref()
                .expect("create fence")
                .transaction_id
                .clone(),
            &target_generation,
        )?;
        manifest.updated_at = now;
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn load(
        &mut self,
        sandbox_id: &SandboxId,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let manifest = self.load_allow_pending_reconciliation(sandbox_id)?;
        if let Some(pending) = manifest.pending_reconciliation.as_ref() {
            return Err(SandboxStoreError::ReconciliationPending {
                sandbox_id: sandbox_id.clone(),
                transaction_id: pending.transaction_id.clone(),
            });
        }
        Ok(manifest)
    }

    /// One-time, explicitly invoked migration for pre-symlink instances. The
    /// external control fence is published before the legacy directory rename.
    /// DFS directory rename is a resumable bounded job rather than atomic, so
    /// sandbox APIs fail closed for the full migration; raw operators may see
    /// the documented migration window and should recreate internal sandboxes
    /// instead when uninterrupted FUSE visibility is required.
    pub fn migrate_legacy_instance(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        self.migrate_legacy_instance_with_checkpoints(sandbox_id, now, || Ok(()))
    }

    pub fn migrate_legacy_instance_with_checkpoints<F>(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
        mut checkpoint: F,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        let stable_root = sandbox_instance_root(sandbox_id);
        let control_path = sandbox_manifest_path(sandbox_id);
        let mut manifest = match self.dfs.posix_read_file_distributed(&control_path) {
            Ok(bytes) => {
                let manifest = decode_instance_manifest(&bytes, &control_path)?;
                if !manifest
                    .pending_reconciliation
                    .as_ref()
                    .is_some_and(|pending| {
                        matches!(
                            pending.operation,
                            SandboxAuthoritativeMutationOperation::LegacyMigration
                        )
                    })
                {
                    return Err(SandboxStoreError::AlreadyExists {
                        sandbox_id: sandbox_id.clone(),
                    });
                }
                manifest
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
                let legacy_manifest_path = format!("{stable_root}/manifest.sobj");
                let bytes = self
                    .dfs
                    .posix_read_file_distributed(&legacy_manifest_path)
                    .map_err(|error| {
                        sandbox_store_error_from_dfs(legacy_manifest_path.clone(), error)
                    })?;
                let mut manifest = decode_instance_manifest(&bytes, &legacy_manifest_path)?;
                if manifest.sandbox_id != *sandbox_id {
                    return Err(SandboxStoreError::Object {
                        path: legacy_manifest_path,
                        detail: format!(
                            "legacy manifest belongs to sandbox {} instead of {sandbox_id}",
                            manifest.sandbox_id
                        ),
                    });
                }
                manifest.workspace_root = stable_root.clone();
                let transaction_id = Id::generate();
                let incarnation_id = manifest.effective_incarnation_id();
                let target = sandbox_transaction_workspace_root(
                    sandbox_id,
                    &incarnation_id,
                    &transaction_id,
                );
                let source_generation = manifest.generation;
                manifest.advance_generation()?;
                manifest.pending_reconciliation = Some(SandboxAuthoritativeReconciliationFence {
                    transaction_id,
                    source_incarnation_id: incarnation_id,
                    source_generation,
                    previous_workspace_root: Some(stable_root.clone()),
                    target_workspace_root: Some(target),
                    operation: SandboxAuthoritativeMutationOperation::LegacyMigration,
                });
                manifest.updated_at = now;
                self.dfs
                    .posix_mkdir_all(&sandbox_control_root(sandbox_id))
                    .map_err(|error| sandbox_store_error_from_dfs(control_path.clone(), error))?;
                self.write_manifest(&manifest)?;
                checkpoint()?;
                manifest
            }
            Err(error) => return Err(sandbox_store_error_from_dfs(control_path, error)),
        };
        let pending = manifest
            .pending_reconciliation
            .clone()
            .expect("legacy migration fence validated");
        let target =
            pending
                .target_workspace_root
                .clone()
                .ok_or_else(|| SandboxStoreError::Object {
                    path: sandbox_manifest_path(sandbox_id),
                    detail: "legacy migration fence is missing target generation".to_string(),
                })?;
        match self.dfs.posix_lstat_distributed(&stable_root) {
            Ok(stat) if stat.kind == DfsObjectKind::Directory => {
                self.copy_sifs_tree_preserving_hardlinks_with_checkpoint(
                    &stable_root,
                    &target,
                    &mut BTreeMap::new(),
                    &mut checkpoint,
                )?;
                let copied_legacy_manifest = format!("{target}/manifest.sobj");
                let _ = self.remove_sifs_tree_if_exists(&copied_legacy_manifest);
                let legacy_checkpoints = format!("{stable_root}/checkpoints");
                if self
                    .dfs
                    .posix_lstat_distributed(&legacy_checkpoints)
                    .is_ok_and(|stat| stat.kind == DfsObjectKind::Directory)
                {
                    let control_checkpoints = sandbox_checkpoint_root(sandbox_id);
                    self.dfs
                        .posix_mkdir_all(&control_checkpoints)
                        .map_err(|error| {
                            sandbox_store_error_from_dfs(control_checkpoints.clone(), error)
                        })?;
                    self.copy_sifs_directory_contents_with_checkpoint(
                        &legacy_checkpoints,
                        &control_checkpoints,
                        &mut BTreeMap::new(),
                        &mut checkpoint,
                    )?;
                }
                let copied_checkpoints = format!("{target}/checkpoints");
                let _ = self.remove_sifs_tree_if_exists(&copied_checkpoints);
                let backup = sandbox_legacy_backup_root(sandbox_id, &pending.transaction_id);
                self.dfs
                    .posix_mkdir_all(&format!("{SIFS_SANDBOX_ROOT}/legacy-backups/{sandbox_id}"))
                    .map_err(|error| sandbox_store_error_from_dfs(backup.clone(), error))?;
                let mut job = self
                    .dfs
                    .posix_rename_directory_bounded_routed(
                        &stable_root,
                        &backup,
                        SIFS_SANDBOX_NAMESPACE_JOB_BATCH_STEPS,
                    )
                    .map_err(|error| sandbox_store_error_from_dfs(stable_root.clone(), error))?;
                while job.status == DfsNamespaceJobStatus::Running {
                    checkpoint()?;
                    job = self
                        .dfs
                        .posix_resume_namespace_job_bounded_routed(
                            &job.job_id,
                            &stable_root,
                            SIFS_SANDBOX_NAMESPACE_JOB_BATCH_STEPS,
                        )
                        .map_err(|error| {
                            sandbox_store_error_from_dfs(stable_root.clone(), error)
                        })?;
                }
                if job.status != DfsNamespaceJobStatus::Completed {
                    return Err(SandboxStoreError::Io {
                        path: stable_root.clone(),
                        detail: format!(
                            "legacy migration namespace job {} ended {:?}: {}",
                            job.job_id,
                            job.status,
                            job.failure_reason.as_deref().unwrap_or("no failure reason")
                        ),
                    });
                }
                checkpoint()?;
            }
            Ok(stat) if stat.kind == DfsObjectKind::Symlink => {}
            Ok(stat) => {
                return Err(SandboxStoreError::Io {
                    path: stable_root,
                    detail: format!(
                        "legacy migration source has unsupported kind {:?}",
                        stat.kind
                    ),
                });
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {}
            Err(error) => return Err(sandbox_store_error_from_dfs(stable_root, error)),
        }
        checkpoint()?;
        self.publish_stable_generation_link(sandbox_id, pending.transaction_id, &target)?;
        manifest.workspace_root = sandbox_instance_root(sandbox_id);
        manifest.updated_at = now;
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn recover_create(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let mut manifest = self.load_allow_pending_reconciliation(sandbox_id)?;
        let pending =
            manifest
                .pending_reconciliation
                .clone()
                .ok_or_else(|| SandboxStoreError::Io {
                    path: sandbox_manifest_path(sandbox_id),
                    detail: "create recovery found no pending transaction".to_string(),
                })?;
        if !matches!(
            pending.operation,
            SandboxAuthoritativeMutationOperation::Create
        ) {
            return Err(SandboxStoreError::ReconciliationPending {
                sandbox_id: sandbox_id.clone(),
                transaction_id: pending.transaction_id,
            });
        }
        let target = pending
            .target_workspace_root
            .ok_or_else(|| SandboxStoreError::Object {
                path: sandbox_manifest_path(sandbox_id),
                detail: "create recovery fence is missing target generation".to_string(),
            })?;
        let published = self
            .dfs
            .posix_lstat_distributed(&sandbox_instance_root(sandbox_id))
            .is_ok_and(|stat| stat.kind == DfsObjectKind::Symlink)
            && self.stable_generation_target(sandbox_id)? == target;
        if !published {
            let mut staging = manifest.clone();
            staging.workspace_root = target.clone();
            self.ensure_instance_dirs(&staging)?;
            self.publish_stable_generation_link(sandbox_id, pending.transaction_id, &target)?;
        }
        manifest.updated_at = now;
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn load_allow_pending_reconciliation(
        &mut self,
        sandbox_id: &SandboxId,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let path = sandbox_manifest_path(sandbox_id);
        let bytes = match self.dfs.posix_read_file_distributed(&path) {
            Ok(bytes) => bytes,
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
                let stable = sandbox_instance_root(sandbox_id);
                if self
                    .dfs
                    .posix_lstat_distributed(&stable)
                    .is_ok_and(|stat| stat.kind == DfsObjectKind::Directory)
                {
                    return Err(SandboxStoreError::LegacyLayout {
                        sandbox_id: sandbox_id.clone(),
                        path: stable,
                    });
                }
                return Err(SandboxStoreError::NotFound {
                    sandbox_id: sandbox_id.clone(),
                });
            }
            Err(other) => return Err(sandbox_store_error_from_dfs(path.clone(), other)),
        };
        let manifest = decode_instance_manifest(&bytes, &path)?;
        let stable = sandbox_instance_root(sandbox_id);
        if manifest.workspace_root != stable {
            return Err(SandboxStoreError::Object {
                path,
                detail: format!(
                    "authoritative manifest workspace root {} is not stable root {stable}",
                    manifest.workspace_root
                ),
            });
        }
        match self.dfs.posix_lstat_distributed(&stable) {
            Ok(stat) if stat.kind == DfsObjectKind::Symlink => {}
            Ok(stat)
                if stat.kind == DfsObjectKind::Directory
                    && manifest
                        .pending_reconciliation
                        .as_ref()
                        .is_some_and(|pending| {
                            matches!(
                                pending.operation,
                                SandboxAuthoritativeMutationOperation::LegacyMigration
                            )
                        }) => {}
            Ok(stat) if stat.kind == DfsObjectKind::Directory => {
                return Err(SandboxStoreError::LegacyLayout {
                    sandbox_id: sandbox_id.clone(),
                    path: stable,
                });
            }
            Ok(stat) => {
                return Err(SandboxStoreError::Io {
                    path: stable,
                    detail: format!("stable sandbox root has unsupported kind {:?}", stat.kind),
                });
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. })
                if manifest
                    .pending_reconciliation
                    .as_ref()
                    .is_some_and(|pending| {
                        matches!(
                            pending.operation,
                            SandboxAuthoritativeMutationOperation::Create
                                | SandboxAuthoritativeMutationOperation::Destroy
                                | SandboxAuthoritativeMutationOperation::LegacyMigration
                        )
                    }) => {}
            Err(error) => return Err(sandbox_store_error_from_dfs(stable, error)),
        }
        Ok(manifest)
    }

    pub fn list_pending_authoritative_manifests(
        &mut self,
    ) -> Result<Vec<SandboxInstanceManifest>, SandboxStoreError> {
        let control_root = format!("{SIFS_SANDBOX_ROOT}/control");
        let entries = match self.dfs.posix_list_dir_distributed(&control_root) {
            Ok(entries) => entries,
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(sandbox_store_error_from_dfs(control_root, error)),
        };
        let mut manifests = Vec::new();
        for entry in entries {
            let Ok(sandbox_id) = SandboxId::new(entry.name) else {
                continue;
            };
            let manifest = self.load_allow_pending_reconciliation(&sandbox_id)?;
            if manifest.pending_reconciliation.is_some() {
                manifests.push(manifest);
            }
        }
        manifests.sort_by(|left, right| left.sandbox_id.cmp(&right.sandbox_id));
        Ok(manifests)
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

    /// Clear exactly one published lifecycle transaction. Daemon-managed
    /// operations keep this separate from pointer publication so their durable
    /// lifecycle events can be established between the two writes.
    pub fn clear_reconciliation_transaction(
        &mut self,
        sandbox_id: &SandboxId,
        transaction_id: &Id,
        now: OffsetDateTime,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let mut authoritative = self.load_allow_pending_reconciliation(sandbox_id)?;
        let pending = authoritative
            .pending_reconciliation
            .as_ref()
            .ok_or_else(|| SandboxStoreError::Io {
                path: sandbox_manifest_path(sandbox_id),
                detail: format!(
                    "authoritative reconciliation transaction {transaction_id} is already clear"
                ),
            })?;
        if pending.transaction_id != *transaction_id {
            return Err(SandboxStoreError::ReconciliationPending {
                sandbox_id: sandbox_id.clone(),
                transaction_id: pending.transaction_id.clone(),
            });
        }
        if let Some(target) = pending.target_workspace_root.as_deref()
            && self.stable_generation_target(sandbox_id)? != target
        {
            return Err(SandboxStoreError::Io {
                path: sandbox_manifest_path(sandbox_id),
                detail: format!(
                    "refusing to clear transaction {transaction_id} before publishing target workspace {target}"
                ),
            });
        }
        authoritative.pending_reconciliation = None;
        authoritative.updated_at = now;
        self.write_manifest(&authoritative)?;
        Ok(authoritative)
    }

    pub fn create_checkpoint(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<SandboxCheckpoint, SandboxStoreError> {
        self.create_checkpoint_with_checkpoints(sandbox_id, now, || Ok(()))
    }

    pub fn create_checkpoint_with_checkpoints<F>(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
        mut checkpoint: F,
    ) -> Result<SandboxCheckpoint, SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        let manifest = self.load(sandbox_id)?;
        let checkpoint_id = SandboxCheckpointId::generate();
        let checkpoint_root = format!("{}/{}", sandbox_checkpoint_root(sandbox_id), checkpoint_id);
        self.dfs
            .posix_mkdir_all(&checkpoint_root)
            .map_err(|source| sandbox_store_error_from_dfs(checkpoint_root.clone(), source))?;
        let mut hardlink_targets = BTreeMap::new();
        for name in sifs_checkpoint_root_names() {
            checkpoint()?;
            self.copy_sifs_tree_preserving_hardlinks_with_checkpoint(
                &format!("{}/{}", manifest.workspace_root.trim_end_matches('/'), name),
                &format!("{checkpoint_root}/{name}"),
                &mut hardlink_targets,
                &mut checkpoint,
            )?;
        }
        checkpoint()?;
        let manifest_ref = format!("{checkpoint_root}/manifest.sobj");
        let bytes = encode_instance_manifest(&manifest, &manifest_ref)?;
        self.dfs
            .posix_write_file(&manifest_ref, &bytes, self.write_options())
            .map_err(|source| sandbox_store_error_from_dfs(manifest_ref.clone(), source))?;
        checkpoint()?;
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
        self.resume_checkpoint_with_checkpoints(sandbox_id, checkpoint_id, now, || Ok(()))
    }

    pub fn resume_checkpoint_with_checkpoints<F>(
        &mut self,
        sandbox_id: &SandboxId,
        checkpoint_id: &SandboxCheckpointId,
        now: OffsetDateTime,
        mut checkpoint: F,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        let current = self.load_allow_pending_reconciliation(sandbox_id)?;
        let checkpoint_root = format!("{}/{}", sandbox_checkpoint_root(sandbox_id), checkpoint_id);
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
        if manifest.sandbox_id != *sandbox_id {
            return Err(SandboxStoreError::Object {
                path: checkpoint_manifest_path,
                detail: format!(
                    "checkpoint belongs to sandbox {} instead of {sandbox_id}",
                    manifest.sandbox_id
                ),
            });
        }
        let checkpoint_incarnation_matches = match (
            manifest.incarnation_id.as_ref(),
            current.incarnation_id.as_ref(),
        ) {
            (Some(checkpoint), Some(authoritative)) => checkpoint == authoritative,
            (None, None) => {
                manifest.effective_incarnation_id() == current.effective_incarnation_id()
            }
            _ => false,
        };
        if !checkpoint_incarnation_matches {
            return Err(SandboxStoreError::Object {
                path: checkpoint_manifest_path,
                detail: format!(
                    "checkpoint incarnation {} does not match current incarnation {}",
                    manifest.effective_incarnation_id(),
                    current.effective_incarnation_id()
                ),
            });
        }
        for name in sifs_checkpoint_root_names() {
            checkpoint()?;
            let source = format!("{checkpoint_root}/{name}");
            match self.dfs.posix_stat_distributed(&source) {
                Ok(stat) if stat.kind == DfsObjectKind::Directory => {}
                Ok(_) => {
                    return Err(SandboxStoreError::Io {
                        path: source,
                        detail: "checkpoint is missing a required directory".to_string(),
                    });
                }
                Err(error) => return Err(sandbox_store_error_from_dfs(source, error)),
            }
        }

        let operation = SandboxAuthoritativeMutationOperation::ResumeCheckpoint {
            checkpoint_id: checkpoint_id.clone(),
        };
        let (transaction_id, target_workspace_root, authoritative_generation, pending_fence) =
            match current.pending_reconciliation.as_ref() {
                None => {
                    let transaction_id = Id::generate();
                    let incarnation_id = current.effective_incarnation_id();
                    let target_workspace_root = sandbox_transaction_workspace_root(
                        sandbox_id,
                        &incarnation_id,
                        &transaction_id,
                    );
                    let mut fenced = current.clone();
                    fenced.advance_generation()?;
                    let pending = SandboxAuthoritativeReconciliationFence {
                        transaction_id: transaction_id.clone(),
                        source_incarnation_id: incarnation_id,
                        source_generation: current.generation,
                        previous_workspace_root: Some(self.stable_generation_target(sandbox_id)?),
                        target_workspace_root: Some(target_workspace_root.clone()),
                        operation: operation.clone(),
                    };
                    fenced.pending_reconciliation = Some(pending.clone());
                    fenced.updated_at = now;
                    checkpoint()?;
                    self.write_manifest(&fenced)?;
                    checkpoint()?;
                    (
                        transaction_id,
                        target_workspace_root,
                        fenced.generation,
                        pending,
                    )
                }
                Some(pending) => {
                    let target_workspace_root =
                        pending.target_workspace_root.clone().ok_or_else(|| {
                            SandboxStoreError::Io {
                                path: sandbox_manifest_path(sandbox_id),
                                detail: "resume transaction is missing its target workspace"
                                    .to_string(),
                            }
                        })?;
                    self.validate_workspace_transaction(
                        sandbox_id,
                        &current,
                        pending,
                        &pending.transaction_id,
                        &pending.source_incarnation_id,
                        pending.source_generation,
                        &target_workspace_root,
                        &operation,
                    )?;
                    (
                        pending.transaction_id.clone(),
                        target_workspace_root,
                        current.generation,
                        pending.clone(),
                    )
                }
            };

        if self.stable_generation_target(sandbox_id)? == target_workspace_root {
            // The atomic pointer write completed and only durable audit/fence
            // cleanup remains. Never reopen the published tree for staging.
            return Ok(current);
        }

        manifest.workspace_root = target_workspace_root.clone();
        manifest.incarnation_id = current.incarnation_id.clone();
        manifest.generation = authoritative_generation;
        manifest.pending_reconciliation = Some(pending_fence);
        self.ensure_instance_dirs(&manifest)?;
        let mut hardlink_targets = BTreeMap::new();
        for name in sifs_checkpoint_root_names() {
            checkpoint()?;
            let destination = format!("{}/{name}", target_workspace_root.trim_end_matches('/'));
            let source = format!("{checkpoint_root}/{name}");
            self.clear_sifs_directory_with_checkpoint(&destination, &mut checkpoint)?;
            self.copy_sifs_directory_contents_with_checkpoint(
                &source,
                &destination,
                &mut hardlink_targets,
                &mut checkpoint,
            )?;
        }
        checkpoint()?;
        manifest.workspace_root = sandbox_instance_root(sandbox_id);
        manifest.updated_at = now;
        self.write_manifest(&manifest)?;
        self.publish_stable_generation_link(
            sandbox_id,
            transaction_id.clone(),
            &target_workspace_root,
        )?;
        debug_assert_eq!(
            manifest
                .pending_reconciliation
                .as_ref()
                .map(|pending| &pending.transaction_id),
            Some(&transaction_id)
        );
        Ok(manifest)
    }

    pub fn reboot_session(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, bool), SandboxStoreError> {
        self.reboot_session_with_checkpoints(sandbox_id, now, || Ok(()))
    }

    pub fn reboot_session_with_checkpoints<F>(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
        mut checkpoint: F,
    ) -> Result<(SandboxInstanceManifest, bool), SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        let mut manifest = self.load_allow_pending_reconciliation(sandbox_id)?;
        let pending_target_epoch = match manifest.pending_reconciliation.as_ref() {
            Some(SandboxAuthoritativeReconciliationFence {
                operation: SandboxAuthoritativeMutationOperation::Reboot { target_temp_epoch },
                ..
            }) => Some(*target_temp_epoch),
            Some(pending) => {
                return Err(SandboxStoreError::ReconciliationPending {
                    sandbox_id: sandbox_id.clone(),
                    transaction_id: pending.transaction_id.clone(),
                });
            }
            None => None,
        };
        let cleared_tmp = manifest.temp_policy == SandboxTempPolicy::ClearOnSessionStart;
        let target_temp_epoch = pending_target_epoch.unwrap_or_else(|| {
            if cleared_tmp {
                manifest.temp_epoch.saturating_add(1)
            } else {
                manifest.temp_epoch
            }
        });
        if pending_target_epoch.is_none() {
            let source_generation = manifest.generation;
            manifest.advance_generation()?;
            manifest.pending_reconciliation = Some(SandboxAuthoritativeReconciliationFence {
                transaction_id: Id::generate(),
                source_incarnation_id: manifest.effective_incarnation_id(),
                source_generation,
                previous_workspace_root: None,
                target_workspace_root: None,
                operation: SandboxAuthoritativeMutationOperation::Reboot { target_temp_epoch },
            });
            manifest.updated_at = now;
            checkpoint()?;
            self.write_manifest(&manifest)?;
            checkpoint()?;
        } else if manifest.temp_epoch == target_temp_epoch {
            // The temp-epoch pointer was already published; only durable event
            // finalization and exact-fence cleanup remain.
            return Ok((manifest, cleared_tmp));
        }
        if cleared_tmp {
            let tmp_path = format!(
                "{}/tmp/{target_temp_epoch}",
                manifest.workspace_root.trim_end_matches('/')
            );
            self.dfs
                .posix_mkdir_all(&tmp_path)
                .map_err(|error| sandbox_store_error_from_dfs(tmp_path.clone(), error))?;
            checkpoint()?;
            manifest.temp_epoch = target_temp_epoch;
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
        self.load(sandbox_id)?;
        let checkpoint_root = format!("{}/{}", sandbox_checkpoint_root(sandbox_id), checkpoint_id);
        self.remove_sifs_tree_if_exists(&checkpoint_root)
    }

    pub fn destroy(&mut self, sandbox_id: &SandboxId) -> Result<(), SandboxStoreError> {
        let manifest = self.prepare_destroy(sandbox_id, OffsetDateTime::now_utc())?;
        let transaction_id = manifest
            .pending_reconciliation
            .as_ref()
            .expect("destroy fence")
            .transaction_id
            .clone();
        self.apply_destroy(sandbox_id, &transaction_id)?;
        self.finalize_destroy(sandbox_id, &transaction_id)
    }

    pub fn prepare_destroy(
        &mut self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<SandboxInstanceManifest, SandboxStoreError> {
        let mut manifest = self.load_allow_pending_reconciliation(sandbox_id)?;
        match manifest.pending_reconciliation.as_ref() {
            Some(pending)
                if matches!(
                    pending.operation,
                    SandboxAuthoritativeMutationOperation::Destroy
                ) =>
            {
                return Ok(manifest);
            }
            Some(pending) => {
                return Err(SandboxStoreError::ReconciliationPending {
                    sandbox_id: sandbox_id.clone(),
                    transaction_id: pending.transaction_id.clone(),
                });
            }
            None => {}
        }
        let source_generation = manifest.generation;
        let previous_generation = self.stable_generation_target(sandbox_id)?;
        manifest.advance_generation()?;
        manifest.pending_reconciliation = Some(SandboxAuthoritativeReconciliationFence {
            transaction_id: Id::generate(),
            source_incarnation_id: manifest.effective_incarnation_id(),
            source_generation,
            previous_workspace_root: Some(previous_generation),
            target_workspace_root: None,
            operation: SandboxAuthoritativeMutationOperation::Destroy,
        });
        manifest.updated_at = now;
        self.write_manifest(&manifest)?;
        Ok(manifest)
    }

    pub fn apply_destroy(
        &mut self,
        sandbox_id: &SandboxId,
        transaction_id: &Id,
    ) -> Result<(), SandboxStoreError> {
        self.apply_destroy_with_checkpoints(sandbox_id, transaction_id, || Ok(()))
    }

    pub fn apply_destroy_with_checkpoints<F>(
        &mut self,
        sandbox_id: &SandboxId,
        transaction_id: &Id,
        mut checkpoint: F,
    ) -> Result<(), SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        let manifest = self.load_allow_pending_reconciliation(sandbox_id)?;
        let pending =
            manifest
                .pending_reconciliation
                .as_ref()
                .ok_or_else(|| SandboxStoreError::Io {
                    path: sandbox_manifest_path(sandbox_id),
                    detail: format!("destroy transaction {transaction_id} is not pending"),
                })?;
        if pending.transaction_id != *transaction_id
            || !matches!(
                pending.operation,
                SandboxAuthoritativeMutationOperation::Destroy
            )
        {
            return Err(SandboxStoreError::ReconciliationPending {
                sandbox_id: sandbox_id.clone(),
                transaction_id: pending.transaction_id.clone(),
            });
        }
        let stable_root = sandbox_instance_root(sandbox_id);
        match self.dfs.posix_lstat_distributed(&stable_root) {
            Ok(stat) if stat.kind == DfsObjectKind::Symlink => {
                self.dfs
                    .posix_unlink(&stable_root)
                    .map_err(|error| sandbox_store_error_from_dfs(stable_root.clone(), error))?;
                checkpoint()?;
            }
            Ok(stat) if stat.kind == DfsObjectKind::Directory => {
                return Err(SandboxStoreError::LegacyLayout {
                    sandbox_id: sandbox_id.clone(),
                    path: stable_root,
                });
            }
            Ok(stat) => {
                return Err(SandboxStoreError::Io {
                    path: stable_root,
                    detail: format!("destroy stable root has unsupported kind {:?}", stat.kind),
                });
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {}
            Err(error) => return Err(sandbox_store_error_from_dfs(stable_root, error)),
        }
        self.remove_sifs_tree_if_exists_with_checkpoint(
            &sandbox_incarnation_generation_root(sandbox_id, &pending.source_incarnation_id),
            &mut checkpoint,
        )?;
        checkpoint()?;
        Ok(())
    }

    pub fn finalize_destroy(
        &mut self,
        sandbox_id: &SandboxId,
        transaction_id: &Id,
    ) -> Result<(), SandboxStoreError> {
        self.finalize_destroy_with_checkpoints(sandbox_id, transaction_id, || Ok(()))
    }

    pub fn finalize_destroy_with_checkpoints<F>(
        &mut self,
        sandbox_id: &SandboxId,
        transaction_id: &Id,
        mut checkpoint: F,
    ) -> Result<(), SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        self.validate_pending_destroy_transaction(sandbox_id, transaction_id)?;
        let control_root = sandbox_control_root(sandbox_id);
        let control_manifest = sandbox_manifest_path(sandbox_id);
        let entries = self
            .dfs
            .posix_list_dir_distributed(&control_root)
            .map_err(|error| sandbox_store_error_from_dfs(control_root.clone(), error))?;
        for entry in entries {
            if entry.path == control_manifest {
                continue;
            }
            self.remove_sifs_tree_for_destroy_with_checkpoint(
                &entry.path,
                sandbox_id,
                transaction_id,
                &mut checkpoint,
            )?;
        }
        checkpoint()?;
        self.validate_pending_destroy_transaction(sandbox_id, transaction_id)?;
        self.dfs
            .posix_unlink(&control_manifest)
            .map_err(|error| sandbox_store_error_from_dfs(control_manifest, error))?;
        // Once the tombstone manifest is removed, the only permitted mutation
        // is rmdir. A concurrent same-id create makes that rmdir fail with
        // DirectoryNotEmpty; the old destroy never walks the reusable path.
        checkpoint()?;
        self.dfs
            .posix_rmdir(&control_root)
            .map(|_| ())
            .map_err(|error| sandbox_store_error_from_dfs(control_root, error))
    }

    fn validate_pending_destroy_transaction(
        &mut self,
        sandbox_id: &SandboxId,
        transaction_id: &Id,
    ) -> Result<(), SandboxStoreError> {
        let manifest = self.load_allow_pending_reconciliation(sandbox_id)?;
        let pending =
            manifest
                .pending_reconciliation
                .as_ref()
                .ok_or_else(|| SandboxStoreError::Io {
                    path: sandbox_manifest_path(sandbox_id),
                    detail: format!("destroy transaction {transaction_id} is not pending"),
                })?;
        if pending.transaction_id == *transaction_id
            && matches!(
                pending.operation,
                SandboxAuthoritativeMutationOperation::Destroy
            )
        {
            return Ok(());
        }
        Err(SandboxStoreError::ReconciliationPending {
            sandbox_id: sandbox_id.clone(),
            transaction_id: pending.transaction_id.clone(),
        })
    }

    fn remove_sifs_tree_for_destroy_with_checkpoint<F>(
        &mut self,
        path: &str,
        sandbox_id: &SandboxId,
        transaction_id: &Id,
        checkpoint: &mut F,
    ) -> Result<bool, SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        self.validate_pending_destroy_transaction(sandbox_id, transaction_id)?;
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
                    self.remove_sifs_tree_for_destroy_with_checkpoint(
                        &entry.path,
                        sandbox_id,
                        transaction_id,
                        checkpoint,
                    )?;
                }
                checkpoint()?;
                self.validate_pending_destroy_transaction(sandbox_id, transaction_id)?;
                self.dfs
                    .posix_rmdir(path)
                    .map(|_| true)
                    .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))
            }
            DfsObjectKind::File | DfsObjectKind::Artifact | DfsObjectKind::Symlink => {
                checkpoint()?;
                self.validate_pending_destroy_transaction(sandbox_id, transaction_id)?;
                self.dfs
                    .posix_unlink(path)
                    .map(|_| true)
                    .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))
            }
            other => Err(SandboxStoreError::Io {
                path: path.to_string(),
                detail: format!("refusing to remove unsupported DFS object kind {other:?}"),
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
        for path in ["root", "home", "tmp", "work", "var", "logs", "artifacts"] {
            let sifs_path = format!("{}/{}", manifest.workspace_root.trim_end_matches('/'), path);
            self.dfs
                .posix_mkdir_all(&sifs_path)
                .map_err(|source| sandbox_store_error_from_dfs(sifs_path, source))?;
        }
        let checkpoint_root = sandbox_checkpoint_root(&manifest.sandbox_id);
        self.dfs
            .posix_mkdir_all(&checkpoint_root)
            .map_err(|source| sandbox_store_error_from_dfs(checkpoint_root, source))?;
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

    fn stable_generation_target(
        &mut self,
        sandbox_id: &SandboxId,
    ) -> Result<String, SandboxStoreError> {
        let stable_root = sandbox_instance_root(sandbox_id);
        let stat = self
            .dfs
            .posix_lstat_distributed(&stable_root)
            .map_err(|error| sandbox_store_error_from_dfs(stable_root.clone(), error))?;
        if stat.kind == DfsObjectKind::Directory {
            return Err(SandboxStoreError::LegacyLayout {
                sandbox_id: sandbox_id.clone(),
                path: stable_root,
            });
        }
        if stat.kind != DfsObjectKind::Symlink {
            return Err(SandboxStoreError::Io {
                path: stable_root,
                detail: format!("stable sandbox root has unsupported kind {:?}", stat.kind),
            });
        }
        self.dfs
            .posix_readlink_distributed(&stable_root)
            .map(|target| target.to_string())
            .map_err(|error| sandbox_store_error_from_dfs(stable_root, error))
    }

    fn publish_stable_generation_link(
        &mut self,
        sandbox_id: &SandboxId,
        transaction_id: Id,
        target_generation: &str,
    ) -> Result<(), SandboxStoreError> {
        let stable_root = sandbox_instance_root(sandbox_id);
        match self.dfs.posix_lstat_distributed(&stable_root) {
            Ok(stat) if stat.kind == DfsObjectKind::Symlink => {
                if self.stable_generation_target(sandbox_id)? == target_generation {
                    return Ok(());
                }
            }
            Ok(stat) if stat.kind == DfsObjectKind::Directory => {
                return Err(SandboxStoreError::LegacyLayout {
                    sandbox_id: sandbox_id.clone(),
                    path: stable_root,
                });
            }
            Ok(stat) => {
                return Err(SandboxStoreError::Io {
                    path: stable_root,
                    detail: format!("stable sandbox root has unsupported kind {:?}", stat.kind),
                });
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {}
            Err(error) => return Err(sandbox_store_error_from_dfs(stable_root, error)),
        }
        let instances_root = format!("{SIFS_SANDBOX_ROOT}/instances");
        self.dfs
            .posix_mkdir_all(&instances_root)
            .map_err(|error| sandbox_store_error_from_dfs(instances_root, error))?;
        let swap_link = sandbox_transaction_swap_link(sandbox_id, &transaction_id);
        match self.dfs.posix_lstat_distributed(&swap_link) {
            Ok(stat) if stat.kind == DfsObjectKind::Symlink => {
                self.dfs
                    .posix_unlink(&swap_link)
                    .map_err(|error| sandbox_store_error_from_dfs(swap_link.clone(), error))?;
            }
            Ok(stat) => {
                return Err(SandboxStoreError::Io {
                    path: swap_link,
                    detail: format!("transaction swap path has unsupported kind {:?}", stat.kind),
                });
            }
            Err(DfsPosixError::NotFound { .. }) | Err(DfsPosixError::MissingParent { .. }) => {}
            Err(error) => return Err(sandbox_store_error_from_dfs(swap_link, error)),
        }
        self.dfs
            .posix_symlink(target_generation, &swap_link, self.write_options())
            .map_err(|error| sandbox_store_error_from_dfs(swap_link.clone(), error))?;
        self.dfs
            .posix_rename_single_namespace_atomic(&swap_link, &stable_root)
            .map(|_| ())
            .map_err(|error| sandbox_store_error_from_dfs(stable_root, error))
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_workspace_transaction(
        &mut self,
        sandbox_id: &SandboxId,
        authoritative: &SandboxInstanceManifest,
        pending: &SandboxAuthoritativeReconciliationFence,
        transaction_id: &Id,
        source_incarnation_id: &Id,
        source_generation: u64,
        target_workspace_root: &str,
        operation: &SandboxAuthoritativeMutationOperation,
    ) -> Result<(), SandboxStoreError> {
        if pending.transaction_id != *transaction_id
            || pending.source_incarnation_id != *source_incarnation_id
            || pending.source_generation != source_generation
            || pending.operation != *operation
            || pending.target_workspace_root.as_deref() != Some(target_workspace_root)
            || authoritative.generation != source_generation.saturating_add(1)
            || authoritative.effective_incarnation_id() != pending.source_incarnation_id
        {
            return Err(SandboxStoreError::ReconciliationPending {
                sandbox_id: sandbox_id.clone(),
                transaction_id: pending.transaction_id.clone(),
            });
        }
        let previous_workspace_root =
            pending
                .previous_workspace_root
                .as_deref()
                .ok_or_else(|| SandboxStoreError::Io {
                    path: sandbox_manifest_path(sandbox_id),
                    detail: format!(
                        "workspace transaction {transaction_id} is missing its previous workspace"
                    ),
                })?;
        let current_generation = self.stable_generation_target(sandbox_id)?;
        if current_generation != previous_workspace_root
            && current_generation != target_workspace_root
        {
            return Err(SandboxStoreError::Io {
                path: sandbox_manifest_path(sandbox_id),
                detail: format!(
                    "workspace transaction {transaction_id} has invalid stable generation {current_generation}"
                ),
            });
        }
        Ok(())
    }

    fn copy_sifs_tree_preserving_hardlinks_with_checkpoint<F>(
        &mut self,
        source: &str,
        destination: &str,
        hardlink_targets: &mut BTreeMap<String, String>,
        checkpoint: &mut F,
    ) -> Result<(), SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        let stat = self
            .dfs
            .posix_lstat_distributed(source)
            .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
        checkpoint()?;
        match stat.kind {
            DfsObjectKind::Directory => {
                self.dfs.posix_mkdir_all(destination).map_err(|error| {
                    sandbox_store_error_from_dfs(destination.to_string(), error)
                })?;
                checkpoint()?;
                let entries = self
                    .dfs
                    .posix_list_dir_distributed(source)
                    .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
                for entry in entries {
                    self.copy_sifs_tree_preserving_hardlinks_with_checkpoint(
                        &entry.path,
                        &format!("{}/{}", destination.trim_end_matches('/'), entry.name),
                        hardlink_targets,
                        checkpoint,
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
                checkpoint()?;
                self.dfs
                    .posix_write_file(destination, &bytes, self.write_options())
                    .map(|_| ())
                    .map_err(|error| {
                        sandbox_store_error_from_dfs(destination.to_string(), error)
                    })?;
                checkpoint()?;
                Ok(())
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
                checkpoint()?;
                self.dfs
                    .posix_symlink(&target, destination, self.write_options())
                    .map(|_| ())
                    .map_err(|error| {
                        sandbox_store_error_from_dfs(destination.to_string(), error)
                    })?;
                checkpoint()?;
                Ok(())
            }
            other => Err(SandboxStoreError::Io {
                path: source.to_string(),
                detail: format!("refusing to copy unsupported DFS object kind {other:?}"),
            }),
        }
    }

    fn copy_sifs_directory_contents_with_checkpoint<F>(
        &mut self,
        source: &str,
        destination: &str,
        hardlink_targets: &mut BTreeMap<String, String>,
        checkpoint: &mut F,
    ) -> Result<(), SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        self.dfs
            .posix_mkdir_all(destination)
            .map_err(|error| sandbox_store_error_from_dfs(destination.to_string(), error))?;
        let entries = self
            .dfs
            .posix_list_dir_distributed(source)
            .map_err(|error| sandbox_store_error_from_dfs(source.to_string(), error))?;
        for entry in entries {
            checkpoint()?;
            let destination_child = format!("{}/{}", destination.trim_end_matches('/'), entry.name);
            self.remove_sifs_tree_if_exists_with_checkpoint(&destination_child, checkpoint)?;
            self.copy_sifs_tree_preserving_hardlinks_with_checkpoint(
                &entry.path,
                &destination_child,
                hardlink_targets,
                checkpoint,
            )?;
        }
        checkpoint()?;
        Ok(())
    }

    fn clear_sifs_directory_with_checkpoint<F>(
        &mut self,
        path: &str,
        checkpoint: &mut F,
    ) -> Result<(), SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
        self.dfs
            .posix_mkdir_all(path)
            .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))?;
        let entries = self
            .dfs
            .posix_list_dir_distributed(path)
            .map_err(|error| sandbox_store_error_from_dfs(path.to_string(), error))?;
        for entry in entries {
            self.remove_sifs_tree_if_exists_with_checkpoint(&entry.path, checkpoint)?;
        }
        checkpoint()?;
        Ok(())
    }

    fn remove_sifs_tree_if_exists(&mut self, path: &str) -> Result<bool, SandboxStoreError> {
        self.remove_sifs_tree_if_exists_with_checkpoint(path, &mut || Ok(()))
    }

    fn remove_sifs_tree_if_exists_with_checkpoint<F>(
        &mut self,
        path: &str,
        checkpoint: &mut F,
    ) -> Result<bool, SandboxStoreError>
    where
        F: FnMut() -> Result<(), SandboxStoreError>,
    {
        checkpoint()?;
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
                    self.remove_sifs_tree_if_exists_with_checkpoint(&entry.path, checkpoint)?;
                }
                checkpoint()?;
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

fn sandbox_create_candidate_matches(
    pending: &SandboxInstanceManifest,
    candidate: &SandboxInstanceManifest,
) -> bool {
    pending.sandbox_id == candidate.sandbox_id
        && pending.workspace_root == candidate.workspace_root
        && pending.toolchain_id == candidate.toolchain_id
        && pending.cwd == candidate.cwd
        && pending.env == candidate.env
        && pending.mounts == candidate.mounts
        && pending.last_placement == candidate.last_placement
        && pending.lifecycle == candidate.lifecycle
        && pending.runtime_binding == candidate.runtime_binding
        && pending.active_processes == candidate.active_processes
        && pending.temp_policy == candidate.temp_policy
        && pending.temp_epoch == candidate.temp_epoch
        && pending.limits == candidate.limits
}

fn sifs_checkpoint_root_names() -> [&'static str; 7] {
    ["root", "home", "tmp", "work", "var", "logs", "artifacts"]
}
