use crate::internal::*;

use super::{decode_instance_manifest, write_manifest};

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

    pub fn create(
        &self,
        sandbox_id: SandboxId,
        toolchain_id: SandboxToolchainId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout), SandboxStoreError> {
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
        let manifest = SandboxInstanceManifest::new(sandbox_id, toolchain_id, now)?;
        write_manifest(&layout.manifest, &manifest)?;
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

    pub fn resume_checkpoint(
        &self,
        sandbox_id: &SandboxId,
        checkpoint_id: &SandboxCheckpointId,
        now: OffsetDateTime,
    ) -> Result<(SandboxInstanceManifest, LocalSandboxInstanceLayout), SandboxStoreError> {
        let (_current_manifest, layout) = self.load(sandbox_id)?;
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
