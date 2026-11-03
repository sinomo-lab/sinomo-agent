use crate::internal::*;
use sinomo_core::storage::durable_file::{
    DurableAtomicFileOptions, sync_directory, write_file_create_new_atomic,
};
use std::io::{Read, Write};

use super::{
    SandboxLocalExecutionLease, decode_instance_manifest, encode_instance_manifest,
    try_acquire_sandbox_local_execution_lease, write_manifest,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalSandboxInstanceLayout {
    pub runtime_root: PathBuf,
    pub root: PathBuf,
    pub manifest: PathBuf,
    pub root_dir: PathBuf,
    pub home: PathBuf,
    pub tmp: PathBuf,
    pub work: PathBuf,
    pub var: PathBuf,
    pub checkpoints: PathBuf,
    pub logs: PathBuf,
    pub artifacts: PathBuf,
    pub mounts: PathBuf,
    pub metadata: PathBuf,
}

impl LocalSandboxInstanceLayout {
    pub fn new(runtime_root: impl Into<PathBuf>, sandbox_id: &SandboxId) -> Self {
        let runtime_root = runtime_root.into();
        let root = runtime_root.join("instances").join(sandbox_id.as_str());
        Self {
            runtime_root,
            manifest: root.join("manifest.sobj"),
            root_dir: root.join("root"),
            home: root.join("home"),
            tmp: root.join("tmp"),
            work: root.join("work"),
            var: root.join("var"),
            checkpoints: root.join("checkpoints"),
            logs: root.join("logs"),
            artifacts: root.join("artifacts"),
            mounts: root.join("mounts"),
            metadata: root.join("metadata"),
            root,
        }
    }

    pub fn required_dirs(&self) -> [&PathBuf; 10] {
        [
            &self.root_dir,
            &self.home,
            &self.tmp,
            &self.work,
            &self.var,
            &self.checkpoints,
            &self.logs,
            &self.artifacts,
            &self.mounts,
            &self.metadata,
        ]
    }

    pub fn toolchain_runtime_cache_root(&self) -> PathBuf {
        self.runtime_root.join("toolchain-cache")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalSandboxInstanceStore {
    runtime_root: PathBuf,
}

impl LocalSandboxInstanceStore {
    pub fn new(runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            runtime_root: runtime_root.into(),
        }
    }

    pub fn layout(&self, sandbox_id: &SandboxId) -> LocalSandboxInstanceLayout {
        LocalSandboxInstanceLayout::new(&self.runtime_root, sandbox_id)
    }

    pub fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    pub fn instances_root(&self) -> PathBuf {
        self.runtime_root.join("instances")
    }

    pub fn create(
        &self,
        sandbox_id: SandboxId,
        toolchain_id: SandboxToolchainId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout), SandboxStoreError> {
        let lease = try_acquire_sandbox_local_execution_lease(self, &sandbox_id)?;
        self.create_under_local_execution_lease(&lease, sandbox_id, toolchain_id, now)
    }

    pub fn create_under_local_execution_lease(
        &self,
        lease: &SandboxLocalExecutionLease,
        sandbox_id: SandboxId,
        toolchain_id: SandboxToolchainId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout), SandboxStoreError> {
        lease.ensure_scope(&sandbox_id)?;
        let layout = self.layout(&sandbox_id);
        if layout.manifest.exists() {
            return Err(SandboxStoreError::AlreadyExists { sandbox_id });
        }
        for dir in layout.required_dirs() {
            fs::create_dir_all(dir).map_err(|source| SandboxStoreError::Io {
                path: dir.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        let manifest = SandboxInstanceManifest::new(sandbox_id.clone(), toolchain_id, now)?;
        let manifest_path = layout.manifest.display().to_string();
        let bytes = encode_instance_manifest(&manifest, &manifest_path)?;
        match write_file_create_new_atomic(
            &layout.manifest,
            &bytes,
            DurableAtomicFileOptions::new("manifest.sobj", "sandbox-create").sync_all(),
        ) {
            Ok(()) => {}
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(SandboxStoreError::AlreadyExists { sandbox_id });
            }
            Err(source) => {
                return Err(SandboxStoreError::Io {
                    path: manifest_path,
                    detail: source.to_string(),
                });
            }
        }
        Ok((manifest, layout))
    }

    pub fn load(
        &self,
        sandbox_id: &SandboxId,
    ) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout), SandboxStoreError> {
        let layout = self.layout(sandbox_id);
        if !layout.manifest.exists() {
            return Err(SandboxStoreError::NotFound {
                sandbox_id: sandbox_id.clone(),
            });
        }
        let bytes = fs::read(&layout.manifest).map_err(|source| SandboxStoreError::Io {
            path: layout.manifest.display().to_string(),
            detail: source.to_string(),
        })?;
        let manifest = decode_instance_manifest(&bytes, &layout.manifest.display().to_string())?;
        Ok((manifest, layout))
    }

    pub fn save_manifest(
        &self,
        manifest: &SandboxInstanceManifest,
    ) -> Result<LocalSandboxInstanceLayout, SandboxStoreError> {
        let layout = self.layout(&manifest.sandbox_id);
        if !layout.root.exists() {
            return Err(SandboxStoreError::NotFound {
                sandbox_id: manifest.sandbox_id.clone(),
            });
        }
        write_manifest(&layout.manifest, manifest)?;
        Ok(layout)
    }

    pub fn create_checkpoint(
        &self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<SandboxCheckpoint, SandboxStoreError> {
        let (manifest, layout) = self.load(sandbox_id)?;
        let checkpoint_id = SandboxCheckpointId::generate();
        let checkpoint_root = layout.checkpoints.join(checkpoint_id.as_str());
        fs::create_dir_all(&checkpoint_root).map_err(|source| SandboxStoreError::Io {
            path: checkpoint_root.display().to_string(),
            detail: source.to_string(),
        })?;
        for (name, source) in local_checkpoint_roots(&layout) {
            copy_dir_tree_no_symlinks(source, &checkpoint_root.join(name))?;
        }
        let manifest_ref = checkpoint_root.join("manifest.sobj");
        write_manifest(&manifest_ref, &manifest)?;
        Ok(SandboxCheckpoint {
            checkpoint_id,
            sandbox_id: sandbox_id.clone(),
            created_at: now,
            manifest_ref: format!(
                "{}/checkpoints/{}/manifest.sobj",
                sandbox_instance_root(sandbox_id),
                checkpoint_root
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
            ),
        })
    }

    /// Create an explicit work-tracking checkpoint with cancellation checkpoints
    /// between every directory entry and bounded file chunk. A cancelled partial
    /// checkpoint is deliberately left unreferenced: synchronously deleting a
    /// large partial tree would defeat prompt cancellation, and no lifecycle
    /// record points at it until this method returns successfully.
    pub fn create_checkpoint_with_checkpoints<F>(
        &self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
        checkpoint: F,
    ) -> Result<SandboxCheckpoint, SandboxCommandExecutionError>
    where
        F: FnMut() -> Result<(), SandboxCommandExecutionError>,
    {
        let checkpoint_id = SandboxCheckpointId::generate();
        self.create_checkpoint_with_id_and_checkpoints(sandbox_id, checkpoint_id, now, checkpoint)
    }

    pub fn create_checkpoint_with_id_and_checkpoints<F>(
        &self,
        sandbox_id: &SandboxId,
        checkpoint_id: SandboxCheckpointId,
        now: OffsetDateTime,
        mut checkpoint: F,
    ) -> Result<SandboxCheckpoint, SandboxCommandExecutionError>
    where
        F: FnMut() -> Result<(), SandboxCommandExecutionError>,
    {
        checkpoint()?;
        let (manifest, layout) = self.load(sandbox_id)?;
        let checkpoint_root = layout.checkpoints.join(checkpoint_id.as_str());
        let incomplete_root = incomplete_checkpoint_root(&layout, &checkpoint_id);
        if checkpoint_root.exists() || incomplete_root.exists() {
            return Err(SandboxStoreError::Io {
                path: checkpoint_root.display().to_string(),
                detail: "sandbox checkpoint transaction path already exists".to_string(),
            }
            .into());
        }
        fs::create_dir_all(&incomplete_root).map_err(|source| SandboxStoreError::Io {
            path: incomplete_root.display().to_string(),
            detail: source.to_string(),
        })?;
        for (name, source) in local_checkpoint_roots(&layout) {
            checkpoint()?;
            copy_dir_tree_no_symlinks_with_checkpoints(
                source,
                &incomplete_root.join(name),
                &mut checkpoint,
            )?;
        }
        checkpoint()?;
        write_manifest(&incomplete_root.join("manifest.sobj"), &manifest)?;
        checkpoint()?;
        sync_directory(&incomplete_root).map_err(|source| SandboxStoreError::Io {
            path: incomplete_root.display().to_string(),
            detail: source.to_string(),
        })?;
        fs::rename(&incomplete_root, &checkpoint_root).map_err(|source| SandboxStoreError::Io {
            path: checkpoint_root.display().to_string(),
            detail: format!("publish sandbox checkpoint: {source}"),
        })?;
        sync_directory(&layout.checkpoints).map_err(|source| SandboxStoreError::Io {
            path: layout.checkpoints.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(SandboxCheckpoint {
            checkpoint_id,
            sandbox_id: sandbox_id.clone(),
            created_at: now,
            manifest_ref: format!(
                "{}/checkpoints/{}/manifest.sobj",
                sandbox_instance_root(sandbox_id),
                checkpoint_root
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
            ),
        })
    }

    pub fn discard_incomplete_checkpoint(
        &self,
        sandbox_id: &SandboxId,
        checkpoint_id: &SandboxCheckpointId,
    ) -> Result<bool, SandboxStoreError> {
        let layout = self.layout(sandbox_id);
        let path = incomplete_checkpoint_root(&layout, checkpoint_id);
        if !path.exists() {
            return Ok(false);
        }
        fs::remove_dir_all(&path).map_err(|source| SandboxStoreError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        sync_directory(&layout.checkpoints).map_err(|source| SandboxStoreError::Io {
            path: layout.checkpoints.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(true)
    }

    pub fn resume_checkpoint(
        &self,
        sandbox_id: &SandboxId,
        checkpoint_id: &SandboxCheckpointId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout), SandboxStoreError> {
        let (current_manifest, layout) = self.load(sandbox_id)?;
        let checkpoint_root = layout.checkpoints.join(checkpoint_id.as_str());
        if !checkpoint_root.is_dir() {
            return Err(SandboxStoreError::CheckpointNotFound {
                sandbox_id: sandbox_id.clone(),
                checkpoint_id: checkpoint_id.clone(),
            });
        }
        for (name, destination) in local_checkpoint_roots(&layout) {
            let source = checkpoint_root.join(name);
            if !source.is_dir() {
                return Err(SandboxStoreError::Io {
                    path: source.display().to_string(),
                    detail: "checkpoint is missing a required directory".to_string(),
                });
            }
            if destination.exists() {
                fs::remove_dir_all(destination).map_err(|source| SandboxStoreError::Io {
                    path: destination.display().to_string(),
                    detail: source.to_string(),
                })?;
            }
            copy_dir_tree_no_symlinks(&source, destination)?;
        }

        let checkpoint_manifest = checkpoint_root.join("manifest.sobj");
        let bytes = fs::read(&checkpoint_manifest).map_err(|source| SandboxStoreError::Io {
            path: checkpoint_manifest.display().to_string(),
            detail: source.to_string(),
        })?;
        let mut manifest =
            decode_instance_manifest(&bytes, &checkpoint_manifest.display().to_string())?;
        manifest.generation = current_manifest.generation;
        manifest.advance_generation()?;
        manifest.updated_at = now;
        write_manifest(&layout.manifest, &manifest)?;
        Ok((manifest, layout))
    }

    pub fn reboot_session(
        &self,
        sandbox_id: &SandboxId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout, bool), SandboxStoreError>
    {
        let (mut manifest, layout) = self.load(sandbox_id)?;
        let cleared_tmp = match manifest.temp_policy {
            SandboxTempPolicy::ClearOnSessionStart => {
                clear_local_directory_contents(&layout.tmp)?;
                true
            }
            SandboxTempPolicy::RetainAcrossSessions => false,
        };
        manifest.advance_generation()?;
        manifest.updated_at = now;
        write_manifest(&layout.manifest, &manifest)?;
        Ok((manifest, layout, cleared_tmp))
    }

    pub fn delete_checkpoint(
        &self,
        sandbox_id: &SandboxId,
        checkpoint_id: &SandboxCheckpointId,
    ) -> Result<bool, SandboxStoreError> {
        let layout = self.layout(sandbox_id);
        let checkpoint_root = layout.checkpoints.join(checkpoint_id.as_str());
        if !checkpoint_root.exists() {
            return Ok(false);
        }
        fs::remove_dir_all(&checkpoint_root).map_err(|source| SandboxStoreError::Io {
            path: checkpoint_root.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(true)
    }

    pub fn destroy(&self, sandbox_id: &SandboxId) -> Result<(), SandboxStoreError> {
        let lease = try_acquire_sandbox_local_execution_lease(self, sandbox_id)?;
        self.destroy_under_local_execution_lease(&lease, sandbox_id)
    }

    pub fn destroy_under_local_execution_lease(
        &self,
        lease: &SandboxLocalExecutionLease,
        sandbox_id: &SandboxId,
    ) -> Result<(), SandboxStoreError> {
        lease.ensure_scope(sandbox_id)?;
        let layout = self.layout(sandbox_id);
        if !layout.root.exists() {
            return Err(SandboxStoreError::NotFound {
                sandbox_id: sandbox_id.clone(),
            });
        }
        fs::remove_dir_all(&layout.root).map_err(|source| SandboxStoreError::Io {
            path: layout.root.display().to_string(),
            detail: source.to_string(),
        })
    }
}

fn local_checkpoint_roots(layout: &LocalSandboxInstanceLayout) -> [(&'static str, &PathBuf); 7] {
    [
        ("root", &layout.root_dir),
        ("home", &layout.home),
        ("tmp", &layout.tmp),
        ("work", &layout.work),
        ("var", &layout.var),
        ("logs", &layout.logs),
        ("artifacts", &layout.artifacts),
    ]
}

fn incomplete_checkpoint_root(
    layout: &LocalSandboxInstanceLayout,
    checkpoint_id: &SandboxCheckpointId,
) -> PathBuf {
    layout
        .checkpoints
        .join(format!(".incomplete-{}", checkpoint_id.as_str()))
}

fn clear_local_directory_contents(path: &Path) -> Result<(), SandboxStoreError> {
    fs::create_dir_all(path).map_err(|error| SandboxStoreError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    })?;
    let entries = fs::read_dir(path).map_err(|error| SandboxStoreError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| SandboxStoreError::Io {
            path: path.display().to_string(),
            detail: error.to_string(),
        })?;
        let child = entry.path();
        let metadata = fs::symlink_metadata(&child).map_err(|error| SandboxStoreError::Io {
            path: child.display().to_string(),
            detail: error.to_string(),
        })?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            fs::remove_dir_all(&child).map_err(|error| SandboxStoreError::Io {
                path: child.display().to_string(),
                detail: error.to_string(),
            })?;
        } else {
            fs::remove_file(&child).map_err(|error| SandboxStoreError::Io {
                path: child.display().to_string(),
                detail: error.to_string(),
            })?;
        }
    }
    Ok(())
}

fn copy_dir_tree_no_symlinks(source: &Path, destination: &Path) -> Result<(), SandboxStoreError> {
    let metadata = fs::symlink_metadata(source).map_err(|error| SandboxStoreError::Io {
        path: source.display().to_string(),
        detail: error.to_string(),
    })?;
    if metadata.file_type().is_symlink() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| SandboxStoreError::Io {
                path: parent.display().to_string(),
                detail: error.to_string(),
            })?;
        }
        #[cfg(unix)]
        {
            let target = fs::read_link(source).map_err(|error| SandboxStoreError::Io {
                path: source.display().to_string(),
                detail: error.to_string(),
            })?;
            std::os::unix::fs::symlink(&target, destination).map_err(|error| {
                SandboxStoreError::Io {
                    path: destination.display().to_string(),
                    detail: error.to_string(),
                }
            })?;
        }
        #[cfg(not(unix))]
        return Err(SandboxStoreError::Io {
            path: destination.display().to_string(),
            detail: "copying symlinks in sandbox checkpoints is unsupported on this platform"
                .to_string(),
        });
    } else if metadata.is_dir() {
        fs::create_dir_all(destination).map_err(|error| SandboxStoreError::Io {
            path: destination.display().to_string(),
            detail: error.to_string(),
        })?;
        for entry in fs::read_dir(source).map_err(|error| SandboxStoreError::Io {
            path: source.display().to_string(),
            detail: error.to_string(),
        })? {
            let entry = entry.map_err(|error| SandboxStoreError::Io {
                path: source.display().to_string(),
                detail: error.to_string(),
            })?;
            copy_dir_tree_no_symlinks(&entry.path(), &destination.join(entry.file_name()))?;
        }
    } else if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| SandboxStoreError::Io {
                path: parent.display().to_string(),
                detail: error.to_string(),
            })?;
        }
        fs::copy(source, destination).map_err(|error| SandboxStoreError::Io {
            path: destination.display().to_string(),
            detail: error.to_string(),
        })?;
    } else {
        return Err(SandboxStoreError::Io {
            path: source.display().to_string(),
            detail: "refusing to copy special file in sandbox checkpoint".to_string(),
        });
    }
    Ok(())
}

fn copy_dir_tree_no_symlinks_with_checkpoints<F>(
    source: &Path,
    destination: &Path,
    checkpoint: &mut F,
) -> Result<(), SandboxCommandExecutionError>
where
    F: FnMut() -> Result<(), SandboxCommandExecutionError>,
{
    const CHECKPOINT_COPY_CHUNK_BYTES: usize = 256 * 1024;

    checkpoint()?;
    let metadata = fs::symlink_metadata(source).map_err(|error| SandboxStoreError::Io {
        path: source.display().to_string(),
        detail: error.to_string(),
    })?;
    if metadata.file_type().is_symlink() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| SandboxStoreError::Io {
                path: parent.display().to_string(),
                detail: error.to_string(),
            })?;
        }
        #[cfg(unix)]
        {
            let target = fs::read_link(source).map_err(|error| SandboxStoreError::Io {
                path: source.display().to_string(),
                detail: error.to_string(),
            })?;
            std::os::unix::fs::symlink(&target, destination).map_err(|error| {
                SandboxStoreError::Io {
                    path: destination.display().to_string(),
                    detail: error.to_string(),
                }
            })?;
        }
        #[cfg(not(unix))]
        return Err(SandboxStoreError::Io {
            path: destination.display().to_string(),
            detail: "copying symlinks in sandbox checkpoints is unsupported on this platform"
                .to_string(),
        }
        .into());
    } else if metadata.is_dir() {
        fs::create_dir_all(destination).map_err(|error| SandboxStoreError::Io {
            path: destination.display().to_string(),
            detail: error.to_string(),
        })?;
        for entry in fs::read_dir(source).map_err(|error| SandboxStoreError::Io {
            path: source.display().to_string(),
            detail: error.to_string(),
        })? {
            checkpoint()?;
            let entry = entry.map_err(|error| SandboxStoreError::Io {
                path: source.display().to_string(),
                detail: error.to_string(),
            })?;
            copy_dir_tree_no_symlinks_with_checkpoints(
                &entry.path(),
                &destination.join(entry.file_name()),
                checkpoint,
            )?;
        }
        sync_directory(destination).map_err(|error| SandboxStoreError::Io {
            path: destination.display().to_string(),
            detail: error.to_string(),
        })?;
    } else if metadata.is_file() {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| SandboxStoreError::Io {
                path: parent.display().to_string(),
                detail: error.to_string(),
            })?;
        }
        let mut source_file = fs::File::open(source).map_err(|error| SandboxStoreError::Io {
            path: source.display().to_string(),
            detail: error.to_string(),
        })?;
        let mut destination_file =
            fs::File::create(destination).map_err(|error| SandboxStoreError::Io {
                path: destination.display().to_string(),
                detail: error.to_string(),
            })?;
        let mut buffer = vec![0_u8; CHECKPOINT_COPY_CHUNK_BYTES];
        loop {
            checkpoint()?;
            let read = source_file
                .read(&mut buffer)
                .map_err(|error| SandboxStoreError::Io {
                    path: source.display().to_string(),
                    detail: error.to_string(),
                })?;
            if read == 0 {
                break;
            }
            destination_file
                .write_all(&buffer[..read])
                .map_err(|error| SandboxStoreError::Io {
                    path: destination.display().to_string(),
                    detail: error.to_string(),
                })?;
        }
        destination_file
            .sync_all()
            .map_err(|error| SandboxStoreError::Io {
                path: destination.display().to_string(),
                detail: error.to_string(),
            })?;
    } else {
        return Err(SandboxStoreError::Io {
            path: source.display().to_string(),
            detail: "refusing to copy special file in sandbox checkpoint".to_string(),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_create_publishes_exactly_one_manifest_incarnation() {
        const CREATORS: usize = 12;
        let root = sinomo_testdir::TestDir::new("sandbox-local-concurrent-create");
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let sandbox_id = SandboxId::from_legacy_named("concurrent_create").expect("sandbox id");
        let toolchain_id =
            SandboxToolchainId::from_legacy_named("concurrent_toolchain").expect("toolchain id");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(CREATORS));
        let joins = (0..CREATORS)
            .map(|_| {
                let store = store.clone();
                let sandbox_id = sandbox_id.clone();
                let toolchain_id = toolchain_id.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    store.create(sandbox_id, toolchain_id, OffsetDateTime::UNIX_EPOCH)
                })
            })
            .collect::<Vec<_>>();
        let results = joins
            .into_iter()
            .map(|join| join.join().expect("creator thread"))
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| {
                    matches!(
                        result,
                        Err(SandboxStoreError::AlreadyExists { .. })
                            | Err(SandboxStoreError::LocalExecutionBusy { .. })
                    )
                })
                .count(),
            CREATORS - 1
        );

        let published = store.load(&sandbox_id).expect("load published manifest").0;
        let winner = results
            .into_iter()
            .find_map(Result::ok)
            .expect("winning create")
            .0;
        assert_eq!(published, winner);
    }

    #[test]
    fn cancellable_checkpoint_stops_between_large_file_chunks() {
        let root = sinomo_testdir::TestDir::new("sandbox-local-checkpoint-cancel");
        let store = LocalSandboxInstanceStore::new(root.join("runtime"));
        let sandbox_id = SandboxId::from_legacy_named("checkpoint_cancel").expect("sandbox id");
        let (_, layout) = store
            .create(
                sandbox_id.clone(),
                SandboxToolchainId::from_legacy_named("checkpoint_toolchain")
                    .expect("toolchain id"),
                OffsetDateTime::UNIX_EPOCH,
            )
            .expect("create sandbox");
        let total_bytes = 4 * 1024 * 1024;
        fs::write(layout.work.join("large.bin"), vec![0x5a; total_bytes])
            .expect("write large source");
        let mut checkpoints = 0_usize;
        let checkpoint_id = SandboxCheckpointId::generate();

        let error = store
            .create_checkpoint_with_id_and_checkpoints(
                &sandbox_id,
                checkpoint_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                || {
                    checkpoints += 1;
                    if checkpoints >= 14 {
                        Err(SandboxWasiError::Cancelled.into())
                    } else {
                        Ok(())
                    }
                },
            )
            .expect_err("checkpoint should observe cancellation");

        assert!(matches!(
            error,
            SandboxCommandExecutionError::Wasi(SandboxWasiError::Cancelled)
        ));
        let partial_len = fs::read_dir(&layout.checkpoints)
            .expect("checkpoint directory")
            .next()
            .expect("partial checkpoint")
            .expect("checkpoint entry")
            .path()
            .join("work")
            .join("large.bin")
            .metadata()
            .expect("partial large file")
            .len();
        assert!(partial_len > 0);
        assert!(partial_len < total_bytes as u64);
        assert!(
            store
                .discard_incomplete_checkpoint(&sandbox_id, &checkpoint_id)
                .expect("discard incomplete checkpoint")
        );
        assert!(
            fs::read_dir(&layout.checkpoints)
                .expect("checkpoint directory after cleanup")
                .next()
                .is_none()
        );
    }
}
