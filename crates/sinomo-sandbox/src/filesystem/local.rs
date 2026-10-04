use crate::internal::*;
use std::io::Read;

use super::support::{ensure_host_path_is_within, reject_symlink_chain};
use super::virtual_devices::*;

#[derive(Clone, Debug)]
pub struct LocalSandboxFileSystem {
    layout: LocalSandboxInstanceLayout,
    owners: Arc<Mutex<BTreeMap<SandboxPath, (u32, u32)>>>,
    times: Arc<Mutex<BTreeMap<SandboxPath, (u64, u64)>>>,
    /// Guest permission bits on hosts whose filesystem cannot store POSIX
    /// modes (Windows). Unix hosts store them on the file itself.
    modes: Arc<Mutex<BTreeMap<SandboxPath, u32>>>,
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

    pub fn changed_paths_since(&self, before: &Self) -> Vec<SandboxPath> {
        let mut changed = Vec::new();
        for (path, fingerprint) in &self.entries {
            if before.entries.get(path) != Some(fingerprint) {
                changed.push(path.clone());
            }
        }
        for path in before.entries.keys() {
            if !self.entries.contains_key(path) {
                changed.push(path.clone());
            }
        }
        changed.sort();
        changed.dedup();
        changed
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
        Self {
            layout,
            owners: Arc::new(Mutex::new(BTreeMap::new())),
            times: Arc::new(Mutex::new(BTreeMap::new())),
            modes: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn snapshot(&self) -> Result<LocalSandboxFileSnapshot, SandboxFsError> {
        let mut entries = BTreeMap::new();
        for root in [
            SandboxPath::new("/root").expect("static path"),
            SandboxPath::new("/home").expect("static path"),
            SandboxPath::new("/tmp").expect("static path"),
            SandboxPath::new("/work").expect("static path"),
            SandboxPath::new("/var").expect("static path"),
            SandboxPath::new("/logs").expect("static path"),
            SandboxPath::new("/artifacts").expect("static path"),
        ] {
            self.collect_snapshot_entry(&root, &mut entries)?;
        }
        Ok(LocalSandboxFileSnapshot { entries })
    }

    /// Capture a content fingerprint with cancellation checkpoints between every
    /// directory entry and bounded file-read chunk. This keeps large staged trees
    /// from turning cancellation into an unbounded pre-execution wait.
    pub fn snapshot_with_checkpoints<F>(
        &self,
        mut checkpoint: F,
    ) -> Result<LocalSandboxFileSnapshot, SandboxCommandExecutionError>
    where
        F: FnMut() -> Result<(), SandboxCommandExecutionError>,
    {
        let mut entries = BTreeMap::new();
        for root in [
            SandboxPath::new("/root").expect("static path"),
            SandboxPath::new("/home").expect("static path"),
            SandboxPath::new("/tmp").expect("static path"),
            SandboxPath::new("/work").expect("static path"),
            SandboxPath::new("/var").expect("static path"),
            SandboxPath::new("/logs").expect("static path"),
            SandboxPath::new("/artifacts").expect("static path"),
        ] {
            checkpoint()?;
            self.collect_snapshot_entry_with_checkpoints(&root, &mut entries, &mut checkpoint)?;
        }
        Ok(LocalSandboxFileSnapshot { entries })
    }

    pub fn changed_paths_since(
        &self,
        before: &LocalSandboxFileSnapshot,
    ) -> Result<Vec<SandboxPath>, SandboxFsError> {
        let after = self.snapshot()?;
        Ok(after.changed_paths_since(before))
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
        let host_path = self.host_path(path)?;
        let metadata = self.stat(path)?;
        let content_hash = if metadata.is_dir {
            None
        } else if fs::symlink_metadata(&host_path)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
            let target =
                fs::read_link(&host_path).map_err(|source| SandboxFsError::Unavailable {
                    reason: format!("read symlink {}: {source}", path),
                })?;
            Some(sandbox_module_hash(target.to_string_lossy().as_bytes()))
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

    fn collect_snapshot_entry_with_checkpoints<F>(
        &self,
        path: &SandboxPath,
        entries: &mut BTreeMap<SandboxPath, LocalSandboxFileFingerprint>,
        checkpoint: &mut F,
    ) -> Result<(), SandboxCommandExecutionError>
    where
        F: FnMut() -> Result<(), SandboxCommandExecutionError>,
    {
        const SNAPSHOT_READ_CHUNK_BYTES: usize = 256 * 1024;

        checkpoint()?;
        let host_path = self.host_path(path)?;
        let metadata = self.stat(path)?;
        let content_hash = if metadata.is_dir {
            None
        } else if fs::symlink_metadata(&host_path)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
        {
            let target =
                fs::read_link(&host_path).map_err(|source| SandboxFsError::Unavailable {
                    reason: format!("read symlink {}: {source}", path),
                })?;
            Some(sandbox_module_hash(target.to_string_lossy().as_bytes()))
        } else {
            let mut file =
                fs::File::open(&host_path).map_err(|source| SandboxFsError::Unavailable {
                    reason: format!("open {} for snapshot: {source}", path),
                })?;
            let mut hasher = SandboxModuleHasher::new();
            let mut buffer = vec![0_u8; SNAPSHOT_READ_CHUNK_BYTES];
            loop {
                checkpoint()?;
                let read =
                    file.read(&mut buffer)
                        .map_err(|source| SandboxFsError::Unavailable {
                            reason: format!("read {} for snapshot: {source}", path),
                        })?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
            Some(hasher.finalize())
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
                checkpoint()?;
                self.collect_snapshot_entry_with_checkpoints(&child.path, entries, checkpoint)?;
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
            "var" => Ok((&self.layout.var, tail.to_string())),
            "checkpoints" => Ok((&self.layout.checkpoints, tail.to_string())),
            "logs" => Ok((&self.layout.logs, tail.to_string())),
            "artifacts" => Ok((&self.layout.artifacts, tail.to_string())),
            _ => Err(SandboxFsError::NotFound { path: path.clone() }),
        }
    }

    pub fn host_path(&self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
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

    fn host_path_follow_final(&self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        let host_path = self.host_path(path)?;
        let parent = host_path
            .parent()
            .ok_or_else(|| SandboxFsError::NotFound { path: path.clone() })?;
        reject_symlink_chain(parent, path)?;
        let target = fs::canonicalize(&host_path)
            .map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
        let (base, _) = self.mount_for(path)?;
        let base =
            fs::canonicalize(base).map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
        ensure_host_path_is_within(&base, &target, path)?;
        Ok(target)
    }

    fn metadata_for(
        &self,
        path: &SandboxPath,
        host_path: &Path,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        let metadata = fs::symlink_metadata(host_path)
            .map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
        Ok(SandboxMetadata {
            path: path.clone(),
            is_dir: metadata.is_dir(),
            is_symlink: metadata.file_type().is_symlink(),
            len: metadata.len(),
            link_count: metadata_link_count(&metadata, host_path),
            mode: self
                .modes
                .lock()
                .ok()
                .and_then(|modes| modes.get(path).copied())
                .unwrap_or_else(|| metadata_mode(&metadata)),
            uid: self
                .owners
                .lock()
                .ok()
                .and_then(|owners| owners.get(path).copied())
                .map(|(uid, _)| uid)
                .unwrap_or_else(|| metadata_uid(&metadata)),
            gid: self
                .owners
                .lock()
                .ok()
                .and_then(|owners| owners.get(path).copied())
                .map(|(_, gid)| gid)
                .unwrap_or_else(|| metadata_gid(&metadata)),
            inode: metadata_inode(&metadata, host_path, path),
            object_identity: None,
            content_hash: None,
            accessed_at_unix_nanos: self
                .times
                .lock()
                .ok()
                .and_then(|times| times.get(path).copied())
                .map(|(accessed, _)| accessed)
                .unwrap_or_else(|| metadata_time_nanos(metadata.accessed().ok())),
            modified_at_unix_nanos: self
                .times
                .lock()
                .ok()
                .and_then(|times| times.get(path).copied())
                .map(|(_, modified)| modified)
                .unwrap_or_else(|| metadata_time_nanos(metadata.modified().ok())),
            created_at_unix_nanos: metadata_time_nanos(metadata.created().ok()),
            changed_at_unix_nanos: metadata_time_nanos(metadata.modified().ok()),
        })
    }

    fn root_entries(&self) -> Vec<SandboxDirEntry> {
        let mut entries: Vec<_> = [
            ("root", &self.layout.root_dir),
            ("home", &self.layout.home),
            ("tmp", &self.layout.tmp),
            ("work", &self.layout.work),
            ("var", &self.layout.var),
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
                is_symlink: false,
                len: 0,
                link_count: 2,
                mode: 0o755,
                uid: 0,
                gid: 0,
                inode: sandbox_inode_for_path(path),
                object_identity: None,
                content_hash: None,
                accessed_at_unix_nanos: 1_000_000_000,
                modified_at_unix_nanos: 1_000_000_000,
                created_at_unix_nanos: 1_000_000_000,
                changed_at_unix_nanos: 1_000_000_000,
            });
        }
        if let Some(metadata) = sandbox_virtual_device_metadata(path) {
            return Ok(metadata);
        }
        let host_path = self.host_path(path)?;
        self.metadata_for(path, &host_path)
    }

    fn stat_follow(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        if path.as_str() == "/" || sandbox_virtual_device_path(path).is_some() {
            return self.stat(path);
        }
        let host_path = self.host_path_follow_final(path)?;
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
        let host_path = self.host_path_follow_final(path)?;
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
        let host_path = self.host_path(path)?;
        let parent = host_path
            .parent()
            .ok_or_else(|| SandboxFsError::NotFound { path: path.clone() })?;
        reject_symlink_chain(parent, path)?;
        if host_path.exists() {
            reject_symlink_chain(&host_path, path)?;
            return Err(SandboxFsError::AlreadyExists { path: path.clone() });
        }
        fs::create_dir(&host_path).map_err(|source| match source.kind() {
            std::io::ErrorKind::NotFound => missing_or_not_directory(path, &host_path),
            std::io::ErrorKind::NotADirectory => {
                SandboxFsError::NotDirectory { path: path.clone() }
            }
            std::io::ErrorKind::AlreadyExists => {
                SandboxFsError::AlreadyExists { path: path.clone() }
            }
            _ => SandboxFsError::Unavailable {
                reason: format!("create dir {}: {source}", path),
            },
        })
    }

    fn create_dir_all(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if path.as_str() == "/" {
            return Ok(());
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

    fn write_file(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError> {
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
            .ok_or_else(|| SandboxFsError::NotFound { path: path.clone() })?;
        reject_symlink_chain(parent, path)?;
        if !parent.is_dir() {
            return Err(missing_or_not_directory(path, &host_path));
        }
        if host_path.exists() {
            reject_symlink_chain(&host_path, path)?;
        }
        fs::write(&host_path, bytes).map_err(|source| match source.kind() {
            std::io::ErrorKind::NotFound => missing_or_not_directory(path, &host_path),
            std::io::ErrorKind::NotADirectory => {
                SandboxFsError::NotDirectory { path: path.clone() }
            }
            _ => SandboxFsError::Unavailable {
                reason: format!("write {}: {source}", path),
            },
        })
    }

    fn list(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        if path.as_str() == "/" {
            return Ok(self.root_entries());
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
        }
        fs::rename(&from_host, &to_host).map_err(|source| match source.kind() {
            std::io::ErrorKind::NotFound if !from_host.exists() => {
                SandboxFsError::NotFound { path: from.clone() }
            }
            std::io::ErrorKind::NotFound => missing_or_not_directory(to, &to_host),
            std::io::ErrorKind::NotADirectory => SandboxFsError::NotDirectory { path: to.clone() },
            std::io::ErrorKind::AlreadyExists => SandboxFsError::AlreadyExists { path: to.clone() },
            _ => SandboxFsError::Unavailable {
                reason: format!("rename {from} to {to}: {source}"),
            },
        })
    }

    fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let host_path = self.host_path(path)?;
        let parent = host_path
            .parent()
            .ok_or_else(|| SandboxFsError::NotFound { path: path.clone() })?;
        reject_symlink_chain(parent, path)?;
        let metadata = fs::symlink_metadata(&host_path)
            .map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
        if metadata.is_dir() {
            Err(SandboxFsError::IsDirectory { path: path.clone() })
        } else {
            fs::remove_file(&host_path).map_err(|source| SandboxFsError::Unavailable {
                reason: format!("remove file {}: {source}", path),
            })
        }
    }

    fn remove_dir(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let host_path = self.host_path(path)?;
        reject_symlink_chain(&host_path, path)?;
        let metadata = fs::symlink_metadata(&host_path)
            .map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
        if !metadata.is_dir() {
            return Err(SandboxFsError::NotDirectory { path: path.clone() });
        }
        fs::remove_dir(&host_path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::DirectoryNotEmpty {
                SandboxFsError::DirectoryNotEmpty { path: path.clone() }
            } else {
                SandboxFsError::Unavailable {
                    reason: format!("remove dir {}: {source}", path),
                }
            }
        })
    }

    fn readlink(&self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::NotFound { path: path.clone() });
        }
        let host_path = self.host_path(path)?;
        let parent = host_path
            .parent()
            .ok_or_else(|| SandboxFsError::NotFound { path: path.clone() })?;
        reject_symlink_chain(parent, path)?;
        fs::read_link(&host_path).map_err(|source| match source.kind() {
            std::io::ErrorKind::NotFound => SandboxFsError::NotFound { path: path.clone() },
            _ => SandboxFsError::Unavailable {
                reason: format!("readlink {}: {source}", path),
            },
        })
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
        let host_path = self.host_path(link_path)?;
        let parent = host_path.parent().ok_or_else(|| SandboxFsError::NotFound {
            path: link_path.clone(),
        })?;
        reject_symlink_chain(parent, link_path)?;
        if !parent.exists() {
            return Err(SandboxFsError::NotFound {
                path: link_path.clone(),
            });
        }
        if host_path.exists() || fs::symlink_metadata(&host_path).is_ok() {
            return Err(SandboxFsError::AlreadyExists {
                path: link_path.clone(),
            });
        }
        create_host_symlink(target, &host_path).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("symlink {} -> {}: {source}", target.display(), link_path),
        })
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
        let old_host = self.host_path(old_path)?;
        let new_host = self.host_path(new_path)?;
        let old_parent = old_host.parent().ok_or_else(|| SandboxFsError::NotFound {
            path: old_path.clone(),
        })?;
        reject_symlink_chain(old_parent, old_path)?;
        fs::symlink_metadata(&old_host).map_err(|_| SandboxFsError::NotFound {
            path: old_path.clone(),
        })?;
        let parent = new_host.parent().ok_or_else(|| SandboxFsError::NotFound {
            path: new_path.clone(),
        })?;
        reject_symlink_chain(parent, new_path)?;
        if !parent.exists() {
            return Err(SandboxFsError::NotFound {
                path: new_path.clone(),
            });
        }
        if new_host.exists() || fs::symlink_metadata(&new_host).is_ok() {
            return Err(SandboxFsError::AlreadyExists {
                path: new_path.clone(),
            });
        }
        fs::hard_link(&old_host, &new_host).map_err(|source| match source.kind() {
            std::io::ErrorKind::NotFound => SandboxFsError::NotFound {
                path: old_path.clone(),
            },
            std::io::ErrorKind::AlreadyExists => SandboxFsError::AlreadyExists {
                path: new_path.clone(),
            },
            _ => SandboxFsError::Unavailable {
                reason: format!("hard link {old_path} -> {new_path}: {source}"),
            },
        })
    }

    fn set_mode(&mut self, path: &SandboxPath, mode: u32) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        let host_path = self.host_path_follow_final(path)?;
        if !cfg!(unix) {
            fs::symlink_metadata(&host_path)
                .map_err(|_| SandboxFsError::NotFound { path: path.clone() })?;
            self.modes
                .lock()
                .map_err(|_| SandboxFsError::Unavailable {
                    reason: "local sandbox mode metadata lock poisoned".to_string(),
                })?
                .insert(path.clone(), mode & 0o7777);
            return Ok(());
        }
        set_host_mode(&host_path, mode).map_err(|source| SandboxFsError::Unavailable {
            reason: format!("set mode {}: {source}", path),
        })
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
        let host_path = self.host_path_follow_final(path)?;
        let metadata = self.metadata_for(path, &host_path)?;
        self.times
            .lock()
            .map_err(|_| SandboxFsError::Unavailable {
                reason: "local sandbox time metadata lock poisoned".to_string(),
            })?
            .insert(
                path.clone(),
                (
                    accessed_at_unix_nanos.unwrap_or(metadata.accessed_at_unix_nanos),
                    modified_at_unix_nanos.unwrap_or(metadata.modified_at_unix_nanos),
                ),
            );
        Ok(())
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
        let host_path = self.host_path(path)?;
        let parent = host_path
            .parent()
            .ok_or_else(|| SandboxFsError::NotFound { path: path.clone() })?;
        reject_symlink_chain(parent, path)?;
        let metadata = self.metadata_for(path, &host_path)?;
        self.times
            .lock()
            .map_err(|_| SandboxFsError::Unavailable {
                reason: "local sandbox time metadata lock poisoned".to_string(),
            })?
            .insert(
                path.clone(),
                (
                    accessed_at_unix_nanos.unwrap_or(metadata.accessed_at_unix_nanos),
                    modified_at_unix_nanos.unwrap_or(metadata.modified_at_unix_nanos),
                ),
            );
        Ok(())
    }

    fn set_owner(&mut self, path: &SandboxPath, uid: u32, gid: u32) -> Result<(), SandboxFsError> {
        if sandbox_virtual_device_path(path).is_some() || sandbox_path_is_dev_tree(path) {
            return Err(SandboxFsError::ReadOnly { path: path.clone() });
        }
        self.host_path_follow_final(path)?;
        self.owners
            .lock()
            .map_err(|_| SandboxFsError::Unavailable {
                reason: "local sandbox owner metadata lock poisoned".to_string(),
            })?
            .insert(path.clone(), (uid, gid));
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<SandboxCheckpoint, SandboxFsError> {
        Err(SandboxFsError::Unsupported {
            operation: "local sandbox filesystem checkpoint".to_string(),
        })
    }
}

fn create_host_symlink(target: &Path, link_path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target, link_path)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::symlink_file(target, link_path)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (target, link_path);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "host symlinks are not supported on this platform",
        ))
    }
}

/// A missing path, or (as POSIX reports it) a path whose existing ancestor is
/// a file. Windows reports both as "path not found".
fn missing_or_not_directory(path: &SandboxPath, host_path: &Path) -> SandboxFsError {
    let blocked = host_path
        .ancestors()
        .skip(1)
        .find(|ancestor| fs::symlink_metadata(ancestor).is_ok())
        .is_some_and(|ancestor| !ancestor.is_dir());
    if blocked {
        SandboxFsError::NotDirectory { path: path.clone() }
    } else {
        SandboxFsError::NotFound { path: path.clone() }
    }
}

fn metadata_link_count(metadata: &fs::Metadata, host_path: &Path) -> u64 {
    #[cfg(unix)]
    {
        let _ = host_path;
        use std::os::unix::fs::MetadataExt;
        metadata.nlink()
    }
    #[cfg(not(unix))]
    {
        if metadata.is_dir() {
            // POSIX: the entry in its parent, its own `.`, and each child
            // directory's `..`.
            let subdirectories = fs::read_dir(host_path)
                .map(|entries| {
                    entries
                        .filter_map(Result::ok)
                        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
                        .count() as u64
                })
                .unwrap_or_default();
            2 + subdirectories
        } else {
            host_file_identity(host_path)
                .map(|identity| identity.links)
                .unwrap_or(1)
        }
    }
}

/// The host's identity for a file: its hard-link count and a per-volume
/// file index shared by every link to it.
#[cfg(windows)]
struct HostFileIdentity {
    links: u64,
    index: u64,
}

#[cfg(windows)]
fn host_file_identity(host_path: &Path) -> Option<HostFileIdentity> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        GetFileInformationByHandle,
    };
    // Metadata access only, without following a final link, and directories
    // too (backup semantics).
    let file = fs::OpenOptions::new()
        .access_mode(0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(host_path)
        .ok()?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the handle is open for the duration of the call and `info` is a
    // valid, writable out-parameter of the documented type.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) } == 0 {
        return None;
    }
    Some(HostFileIdentity {
        links: u64::from(info.nNumberOfLinks),
        index: (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
    })
}

#[cfg(all(not(unix), not(windows)))]
struct HostFileIdentity {
    links: u64,
    index: u64,
}

#[cfg(all(not(unix), not(windows)))]
fn host_file_identity(_: &Path) -> Option<HostFileIdentity> {
    None
}

fn metadata_mode(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o7777
    }
    #[cfg(not(unix))]
    {
        if metadata.is_dir() { 0o755 } else { 0o644 }
    }
}

fn metadata_uid(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.uid()
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn metadata_gid(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.gid()
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

fn set_host_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::symlink_metadata(path)?;
        let mut permissions = metadata.permissions();
        permissions.set_mode((permissions.mode() & !0o7777) | (mode & 0o7777));
        fs::set_permissions(path, permissions)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
        Ok(())
    }
}

fn metadata_inode(metadata: &fs::Metadata, host_path: &Path, path: &SandboxPath) -> u64 {
    #[cfg(unix)]
    {
        let _ = (host_path, path);
        use std::os::unix::fs::MetadataExt;
        metadata.ino()
    }
    #[cfg(not(unix))]
    {
        // Hard links share the host file index, as they share an inode on
        // Unix. Fall back to a path-derived identity where none is available.
        let _ = metadata;
        host_file_identity(host_path)
            .map(|identity| identity.index)
            .filter(|index| *index != 0)
            .unwrap_or_else(|| sandbox_inode_for_path(path))
    }
}

fn sandbox_inode_for_path(path: &SandboxPath) -> u64 {
    let hash = blake3::hash(path.as_str().as_bytes());
    u64::from_le_bytes(hash.as_bytes()[0..8].try_into().expect("hash slice"))
}

fn metadata_time_nanos(time: Option<std::time::SystemTime>) -> u64 {
    time.and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
        .unwrap_or(1_000_000_000)
}
