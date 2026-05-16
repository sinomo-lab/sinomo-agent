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
    pub len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxDirEntry {
    pub path: SandboxPath,
    pub metadata: SandboxMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ByteRange {
    pub start: u64,
    pub end: Option<u64>,
}

pub trait SandboxFileSystem {
    fn stat(&self, path: &SandboxPath) -> Result<SandboxMetadata, SandboxFsError>;
    fn read(&self, path: &SandboxPath, range: Option<ByteRange>)
    -> Result<Vec<u8>, SandboxFsError>;
    fn create_dir_all(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError>;
    fn write(&mut self, path: &SandboxPath, bytes: &[u8]) -> Result<(), SandboxFsError>;
    fn list(&self, path: &SandboxPath) -> Result<Vec<SandboxDirEntry>, SandboxFsError>;
    fn rename(&mut self, from: &SandboxPath, to: &SandboxPath) -> Result<(), SandboxFsError>;
    fn unlink(&mut self, path: &SandboxPath) -> Result<(), SandboxFsError>;
    fn checkpoint(&mut self) -> Result<SandboxCheckpoint, SandboxFsError>;
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SandboxFsError {
    #[error("sandbox filesystem path error: {0}")]
    Path(#[from] SandboxPathError),
    #[error("sandbox filesystem path not found: {path}")]
    NotFound { path: SandboxPath },
    #[error("sandbox filesystem path is read-only: {path}")]
    ReadOnly { path: SandboxPath },
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
