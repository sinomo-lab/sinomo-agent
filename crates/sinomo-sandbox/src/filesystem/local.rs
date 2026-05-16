use crate::internal::*;

use super::support::{ensure_host_path_is_within, reject_symlink_chain};
use super::virtual_devices::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalSandboxFileSystem {
    layout: LocalSandboxInstanceLayout,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LocalSandboxFileSnapshot {
    entries: BTreeMap<SandboxPath, LocalSandboxFileFingerprint>,
}

impl LocalSandboxFileSnapshot {
    pub fn workspace_usage(&self) -> SandboxWorkspaceUsage {
        let mut usage = SandboxWorkspaceUsage::default();
        for fingerprint in self.entries.values() {
            if fingerprint.is_dir {
                continue;
            }
            usage.file_count = usage.file_count.saturating_add(1);
            usage.total_bytes = usage.total_bytes.saturating_add(fingerprint.len);
        }
        usage
    }

    pub fn written_bytes_since(&self, before: &Self) -> u64 {
        let mut written = 0u64;
        for (path, fingerprint) in &self.entries {
            if fingerprint.is_dir {
                continue;
            }
            if before.entries.get(path) != Some(fingerprint) {
                written = written.saturating_add(fingerprint.len);
            }
        }
        written
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LocalSandboxFileFingerprint {
    is_dir: bool,
    len: u64,
    content_hash: Option<String>,
}

impl LocalSandboxFileSystem {
    pub fn new(layout: LocalSandboxInstanceLayout) -> Self {
        Self { layout }
    }

    pub fn snapshot(&self) -> Result<LocalSandboxFileSnapshot, SandboxFsError> {
        let mut entries = BTreeMap::new();
        for root in [
            SandboxPath::new("/root").expect("static path"),
            SandboxPath::new("/home").expect("static path"),
            SandboxPath::new("/tmp").expect("static path"),
            SandboxPath::new("/work").expect("static path"),
            SandboxPath::new("/logs").expect("static path"),
            SandboxPath::new("/artifacts").expect("static path"),
        ] {
            self.collect_snapshot_entry(&root, &mut entries)?;
        }
        Ok(LocalSandboxFileSnapshot { entries })
    }

    pub fn changed_paths_since(
        &self,
        before: &LocalSandboxFileSnapshot,
    ) -> Result<Vec<SandboxPath>, SandboxFsError> {
        let after = self.snapshot()?;
        let mut changed = Vec::new();
        for (path, fingerprint) in &after.entries {
            if before.entries.get(path) != Some(fingerprint) {
                changed.push(path.clone());
            }
        }
        for path in before.entries.keys() {
            if !after.entries.contains_key(path) {
                changed.push(path.clone());
            }
        }
        changed.sort();
        changed.dedup();
        Ok(changed)
    }

    pub fn validate_resource_limits(
        &self,
        limits: &SandboxResourceLimits,
    ) -> Result<SandboxWorkspaceUsage, SandboxFsError> {
        let usage = self.snapshot()?.workspace_usage();
        validate_workspace_usage(&usage, limits)?;
        Ok(usage)
    }

    fn collect_snapshot_entry(
        &self,
        path: &SandboxPath,
        entries: &mut BTreeMap<SandboxPath, LocalSandboxFileFingerprint>,
    ) -> Result<(), SandboxFsError> {
        let metadata = self.stat(path)?;
        let content_hash = if metadata.is_dir {
            None
        } else {
            Some(sandbox_module_hash(&self.read(path, None)?))
        };
        entries.insert(
            path.clone(),
            LocalSandboxFileFingerprint {
                is_dir: metadata.is_dir,
                len: metadata.len,
                content_hash,
            },
        );
        if metadata.is_dir {
            for child in self.list(path)? {
                self.collect_snapshot_entry(&child.path, entries)?;
            }
        }
        Ok(())
    }

    fn mount_for(&self, path: &SandboxPath) -> Result<(&Path, String), SandboxFsError> {
        let normalized = path.as_str();
        let Some((head, tail)) = normalized
            .trim_start_matches('/')
            .split_once('/')
            .map_or_else(
                || {
                    normalized
                        .trim_start_matches('/')
                        .is_empty()
                        .then_some(("", ""))
                        .or_else(|| Some((normalized.trim_start_matches('/'), "")))
                },
                Some,
            )
        else {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        };
        match head {
            "root" => Ok((&self.layout.root_dir, tail.to_string())),
            "home" => Ok((&self.layout.home, tail.to_string())),
            "tmp" => Ok((&self.layout.tmp, tail.to_string())),
            "work" => Ok((&self.layout.work, tail.to_string())),
            "checkpoints" => Ok((&self.layout.checkpoints, tail.to_string())),
            "logs" => Ok((&self.layout.logs, tail.to_string())),
            "artifacts" => Ok((&self.layout.artifacts, tail.to_string())),
            _ => Err(SandboxFsError::NotFound { path: path.clone() }),
        }
    }

    pub(crate) fn host_path(&self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        if path.as_str() == "/" {
            return Ok(self.layout.root.clone());
        }
        let (base, tail) = self.mount_for(path)?;
        let host_path = if tail.is_empty() {
            base.to_path_buf()
        } else {
            base.join(tail)
        };
        ensure_host_path_is_within(base, &host_path, path)?;
        Ok(host_path)
    }

    fn metadata_for(
        &self,
        path: &SandboxPath,
        host_path: &Path,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        let metadata =
            fs::metadata(host_path).map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
        Ok(SandboxMetadata {
            path: path.clone(),
            is_dir: metadata.is_dir(),
            len: metadata.len(),
        })
    }

    fn root_entries(&self) -> Vec<SandboxDirEntry> {
        let mut entries: Vec<_> = [
            ("root", &self.layout.root_dir),
            ("home", &self.layout.home),
            ("tmp", &self.layout.tmp),
            ("work", &self.layout.work),
            ("checkpoints", &self.layout.checkpoints),
            ("logs", &self.layout.logs),
            ("artifacts", &self.layout.artifacts),
        ]
        .into_iter()
        .filter_map(|(name, host_path)| {
            let path = SandboxPath::new(format!("/{name}")).ok()?;
            let metadata = self.metadata_for(&path, host_path).ok()?;
            Some(SandboxDirEntry { path, metadata })
        })
        .collect();
        let dev_path = SandboxPath::new("/dev").expect("static virtual device path");
        entries.push(SandboxDirEntry {
            metadata: sandbox_virtual_device_metadata(&dev_path).expect("virtual device metadata"),
            path: dev_path,
        });
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        entries
    }
}

impl SandboxFileSystem for LocalSandboxFileSystem {
    fn stat(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        if path.as_str() == "/" {
            return Ok(SandboxMetadata {
                path: path.clone(),
                is_dir: true,
                len: 0,
            });
        }
        if let Some(metadata) = sandbox_virtual_device_metadata(path) {
            return Ok(metadata);
        }
        let host_path = self.host_path(path)?;
        self.metadata_for(path, &host_path)
    }

    fn read(
        &self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return read_sandbox_virtual_device(path, range);
        }
        let host_path = self.host_path(path)?;
        reject_symlink_chain(&host_path, path)?;
        let bytes = fs::read(&host_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                SandboxFsError::NotFound { path: path.clone() }
            } else {
                SandboxFsError::Unavailable {
                    reason: format!("read {}: {source}", path),
                }
            }
        })?;
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
        if path.as_str() == "/" {
            return Ok(());
        }
        if matches!(
            sandbox_virtual_device_path(path),
            Some(SandboxVirtualDevicePath::DevDirectory)
        ) {
            return Ok(());
        }
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let host_path = self.host_path(path)?;
        if let Some(parent) = host_path.parent() {
            reject_symlink_chain(parent, path)?;
        }
        if host_path.exists() {
            reject_symlink_chain(&host_path, path)?;
        }
        fs::create_dir_all(&host_path).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("create dir {}: {source}", path),
        })
    }

    fn write(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError> {
        if path.as_str() == "/" {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        if sandbox_virtual_device_path(path).is_some() {
            let _ = bytes;
            return write_sandbox_virtual_device(path);
        }
        let host_path = self.host_path(path)?;
        let parent = host_path
            .parent()
            .ok_or_else(|| SandboxFsError::Unavailable {
                reason: format!("{} has no parent", path),
            })?;
        reject_symlink_chain(parent, path)?;
        fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("create parent for {}: {source}", path),
        })?;
        if host_path.exists() {
            reject_symlink_chain(&host_path, path)?;
        }
        fs::write(&host_path, bytes).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("write {}: {source}", path),
        })
    }

    fn list(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        if path.as_str() == "/" {
            return Ok(self.root_entries());
        }
        if matches!(
            sandbox_virtual_device_path(path),
            Some(SandboxVirtualDevicePath::DevDirectory)
        ) {
            return Ok(sandbox_virtual_device_dir_entries());
        }
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::Unavailable {
                reason: format!("list {}: not a directory", path),
            });
        }
        let host_path = self.host_path(path)?;
        reject_symlink_chain(&host_path, path)?;
        let entries = fs::read_dir(&host_path).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("list {}: {source}", path),
        })?;
        let mut output = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| SandboxFsError::Unavailable {
                reason: format!("list {}: {source}", path),
            })?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let child_path = path.join(name.as_ref())?;
            let metadata = self.metadata_for(&child_path, &entry.path())?;
            output.push(SandboxDirEntry {
                path: child_path,
                metadata,
            });
        }
        output.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(output)
    }

    fn rename(&mut self, from: &SandboxPath, to: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(from).is_some() || sandbox_virtual_device_path(to).is_some()
        {
            return Err(SandboxFsError::ReadOnly { path: from.clone() });
        }
        let from_host = self.host_path(from)?;
        let to_host = self.host_path(to)?;
        reject_symlink_chain(&from_host, from)?;
        if let Some(parent) = to_host.parent() {
            reject_symlink_chain(parent, to)?;
            fs::create_dir_all(parent).map_err(|source| SandboxFsError::Unavailable {
                reason: format!("create parent for {}: {source}", to),
            })?;
        }
        fs::rename(&from_host, &to_host).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("rename {from} to {to}: {source}"),
        })
    }

    fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let host_path = self.host_path(path)?;
        reject_symlink_chain(&host_path, path)?;
        let metadata = fs::metadata(&host_path)
            .map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
        if metadata.is_dir() {
            fs::remove_dir_all(&host_path).map_err(|source| SandboxFsError::Unavailable {
                reason: format!("remove dir {}: {source}", path),
            })
        } else {
            fs::remove_file(&host_path).map_err(|source| SandboxFsError::Unavailable {
                reason: format!("remove file {}: {source}", path),
            })
        }
    }

    fn checkpoint(&mut self) -> Result<SandboxCheckpoint, SandboxFsError> {
        Err(SandboxFsError::Unsupported {
            operation: "local sandbox filesystem checkpoint".to_string(),
        })
    }
}
