use crate::internal::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxOutputStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxOutputChunk {
    pub sandbox_id: SandboxId,
    pub command_id: SandboxCommandId,
    pub stream: SandboxOutputStream,
    pub offset: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCheckpoint {
    pub checkpoint_id: SandboxCheckpointId,
    pub sandbox_id: SandboxId,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub manifest_ref: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxMetadata {
    pub path: SandboxPath,
    pub is_dir: bool,
    #[serde(default)]
    pub is_symlink: bool,
    pub len: u64,
    #[serde(default = "default_sandbox_link_count")]
    pub link_count: u64,
    #[serde(default = "default_sandbox_mode")]
    pub mode: u32,
    #[serde(default)]
    pub uid: u32,
    #[serde(default)]
    pub gid: u32,
    #[serde(default)]
    pub inode: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_identity: Option<String>,
    #[serde(default = "default_sandbox_timestamp")]
    pub accessed_at_unix_nanos: u64,
    #[serde(default = "default_sandbox_timestamp")]
    pub modified_at_unix_nanos: u64,
    #[serde(default = "default_sandbox_timestamp")]
    pub created_at_unix_nanos: u64,
    #[serde(default = "default_sandbox_timestamp")]
    pub changed_at_unix_nanos: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxDirEntry {
    pub path: SandboxPath,
    pub metadata: SandboxMetadata,
}

fn default_sandbox_link_count() -> u64 {
    1
}

fn default_sandbox_mode() -> u32 {
    0o644
}

fn default_sandbox_timestamp() -> u64 {
    1_000_000_000
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ByteRange {
    pub start: u64,
    pub end: Option<u64>,
}

pub trait SandboxFileSystem {
    fn stat(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError>;
    fn stat_mut(&mut self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        self.stat(path)
    }
    fn stat_unchecked(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        self.stat(path)
    }
    fn stat_unchecked_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        self.stat_unchecked(path)
    }
    fn stat_follow(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        self.stat(path)
    }
    fn stat_follow_mut(&mut self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        self.stat_follow(path)
    }
    fn stat_follow_unchecked(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError> {
        self.stat_follow(path)
    }
    fn stat_follow_unchecked_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        self.stat_follow_unchecked(path)
    }
    fn path_for_open_identity(
        &self,
        _identity: Option<&str>,
        _inode: u64,
    ) -> Result<Option<SandboxPath>, SandboxFsError> {
        Ok(None)
    }
    fn resolve_followed_path(&self, path: &SandboxPath) -> Result<SandboxPath, SandboxFsError> {
        self.stat_follow(path)?;
        Ok(path.clone())
    }
    fn resolve_followed_path_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_followed_path(path)
    }
    fn resolve_mutation_path(&self, path: &SandboxPath) -> Result<SandboxPath, SandboxFsError> {
        Ok(path.clone())
    }
    fn resolve_mutation_path_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<SandboxPath, SandboxFsError> {
        self.resolve_mutation_path(path)
    }
    fn read(&self, path: &SandboxPath, range: Option<ByteRange>)
    -> Result<Vec<u8>, SandboxFsError>;
    fn read_unchecked(
        &self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        self.read(path, range)
    }
    fn read_unchecked_mut(
        &mut self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        self.read_unchecked(path, range)
    }
    fn read_unchecked_update_access(
        &mut self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        self.read_unchecked(path, range)
    }
    fn read_file(
        &mut self,
        path: &SandboxPath,
        range: Option<ByteRange>,
    ) -> Result<Vec<u8>, SandboxFsError> {
        self.read(path, range)
    }
    fn create_dir(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        self.create_dir_all(path)
    }
    fn create_dir_all(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError>;
    fn write(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError>;
    fn write_file(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError> {
        self.write(path, bytes)
    }
    fn write_file_unchecked(
        &mut self,
        path: &SandboxPath,
        bytes: &[u8],
    ) -> Result<(), SandboxFsError> {
        self.write_file(path, bytes)
    }
    fn write_at_unchecked(
        &mut self,
        path: &SandboxPath,
        offset: u64,
        bytes: &[u8],
    ) -> Result<SandboxMetadata, SandboxFsError> {
        let mut current = self.read_unchecked(path, None)?;
        let start = usize::try_from(offset).map_err(|_| SandboxFsError::Unavailable {
            reason: format!("write offset too large for {path}"),
        })?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| SandboxFsError::Unavailable {
                reason: format!("write range too large for {path}"),
            })?;
        if current.len() < end {
            current.resize(end, 0);
        }
        current[start..end].copy_from_slice(bytes);
        self.write_file_unchecked(path, &current)?;
        self.stat_follow_unchecked(path)
    }
    fn set_len_unchecked(
        &mut self,
        path: &SandboxPath,
        len: u64,
    ) -> Result<SandboxMetadata, SandboxFsError> {
        let mut current = self.read_unchecked(path, None)?;
        let len = usize::try_from(len).map_err(|_| SandboxFsError::Unavailable {
            reason: format!("file length too large for {path}"),
        })?;
        current.resize(len, 0);
        self.write_file_unchecked(path, &current)?;
        self.stat_follow_unchecked(path)
    }
    fn list(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError>;
    fn list_update_access(
        &mut self,
        path: &SandboxPath,
    ) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        self.list(path)
    }
    fn list_unchecked(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        self.list(path)
    }
    fn list_unchecked_mut(
        &mut self,
        path: &SandboxPath,
    ) -> Result<Vec<SandboxDirEntry>, SandboxFsError> {
        self.list_unchecked(path)
    }
    fn rename(&mut self, from: &SandboxPath, to: &SandboxPath) -> Result<(), SandboxFsError>;
    fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError>;
    fn remove_dir(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError> {
        self.unlink(path)
    }
    fn readlink(&self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        Err(SandboxFsError::Unsupported {
            operation: format!("readlink {path}"),
        })
    }
    fn readlink_mut(&mut self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        self.readlink(path)
    }
    fn readlink_update_access(&mut self, path: &SandboxPath) -> Result<PathBuf, SandboxFsError> {
        self.readlink_mut(path)
    }
    fn symlink(&mut self, target: &Path, link_path: &SandboxPath) -> Result<(), SandboxFsError> {
        Err(SandboxFsError::Unsupported {
            operation: format!("symlink {} -> {link_path}", target.display()),
        })
    }
    fn hard_link(
        &mut self,
        old_path: &SandboxPath,
        new_path: &SandboxPath,
    ) -> Result<(), SandboxFsError> {
        Err(SandboxFsError::Unsupported {
            operation: format!("hard link {old_path} -> {new_path}"),
        })
    }
    fn set_times(
        &mut self,
        path: &SandboxPath,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<(), SandboxFsError> {
        let _ = (path, accessed_at_unix_nanos, modified_at_unix_nanos);
        Ok(())
    }
    fn set_symlink_times(
        &mut self,
        path: &SandboxPath,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<(), SandboxFsError> {
        self.set_times(path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }
    fn set_mode(&mut self, path: &SandboxPath, mode: u32) -> Result<(), SandboxFsError> {
        let _ = (path, mode);
        Err(SandboxFsError::Unsupported {
            operation: "set mode".to_string(),
        })
    }
    fn set_owner(&mut self, path: &SandboxPath, uid: u32, gid: u32) -> Result<(), SandboxFsError> {
        let _ = (path, uid, gid);
        Err(SandboxFsError::Unsupported {
            operation: "set owner".to_string(),
        })
    }
    fn checkpoint(&mut self) -> Result<SandboxCheckpoint, SandboxFsError>;
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SandboxFsError {
    #[error("sandbox filesystem path error: {0}")]
    Path(#[from] SandboxPathError),
    #[error("sandbox filesystem path not found: {path}")]
    NotFound { path: SandboxPath },
    #[error("sandbox filesystem path already exists: {path}")]
    AlreadyExists { path: SandboxPath },
    #[error("sandbox filesystem path is read-only: {path}")]
    ReadOnly { path: SandboxPath },
    #[error("sandbox filesystem path is not a directory: {path}")]
    NotDirectory { path: SandboxPath },
    #[error("sandbox filesystem path is a directory: {path}")]
    IsDirectory { path: SandboxPath },
    #[error("sandbox filesystem path is not a symbolic link: {path}")]
    NotSymlink { path: SandboxPath },
    #[error("sandbox filesystem directory is not empty: {path}")]
    DirectoryNotEmpty { path: SandboxPath },
    #[error("sandbox filesystem operation is unsupported: {operation}")]
    Unsupported { operation: String },
    #[error("sandbox filesystem quota exceeded for {kind}: actual {actual}, limit {limit}")]
    QuotaExceeded {
        kind: String,
        actual: u64,
        limit: u64,
    },
    #[error("sandbox filesystem unavailable: {reason}")]
    Unavailable { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxPolicyDenial {
    pub code: String,
    pub detail: String,
}

impl SandboxPolicyDenial {
    pub fn new(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
        }
    }
}

impl fmt::Display for SandboxPolicyDenial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.detail)
    }
}
