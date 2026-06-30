use crate::internal::*;

mod local;
mod records;
mod sifs;
mod sync;

pub use local::*;
pub use records::*;
pub use sifs::*;
pub use sync::*;

fn write_manifest(
    path: &std::path::Path,
    manifest: &SandboxInstanceManifest,
) -> Result<(), SandboxStoreError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| SandboxStoreError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes = encode_instance_manifest(manifest, &path.display().to_string())?;
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("manifest.sobj");
    let temp_path = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));
    fs::write(&temp_path, bytes).map_err(|source| SandboxStoreError::Io {
        path: temp_path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::rename(&temp_path, path).map_err(|source| SandboxStoreError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}

fn encode_instance_manifest(
    manifest: &SandboxInstanceManifest,
    path: &str,
) -> Result<Vec<u8>, SandboxStoreError> {
    let stored = StoredSandboxInstanceManifest::try_from(manifest)?;
    sinomo_object_serde::to_bytes(&stored, SANDBOX_MANIFEST_OBJECT_VERSION, LengthMode::U32)
        .map_err(|source| SandboxStoreError::Object {
            path: path.to_string(),
            detail: source.to_string(),
        })
}

fn decode_instance_manifest(
    bytes: &[u8],
    path: &str,
) -> Result<SandboxInstanceManifest, SandboxStoreError> {
    let stored: StoredSandboxInstanceManifest =
        sinomo_object_serde::from_bytes(bytes).map_err(|source| SandboxStoreError::Object {
            path: path.to_string(),
            detail: source.to_string(),
        })?;
    SandboxInstanceManifest::try_from(stored)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxInstanceManifest {
    sandbox_id: String,
    workspace_root: String,
    toolchain_id: String,
    cwd: String,
    env: Vec<StoredEnvPair>,
    mounts: Vec<StoredSandboxMount>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    command_history: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_placement: Option<SandboxExecutionPlacement>,
    #[serde(default)]
    lifecycle: SandboxInstanceLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    runtime_binding: Option<SandboxRuntimeBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    active_processes: Vec<SandboxTrackedProcess>,
    temp_policy: SandboxTempPolicy,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    temp_epoch: u64,
    limits: SandboxResourceLimits,
    created_at: String,
    updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredEnvPair {
    key: String,
    value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxMount {
    source_sifs_path: String,
    mount_path: String,
    mode: SandboxMountMode,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxCommandRecord {
    sandbox_id: String,
    command_id: String,
    status: SandboxCommandRecordStatus,
    workspace_root: String,
    toolchain_id: String,
    argv: Vec<String>,
    cwd: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    env: Vec<StoredEnvPair>,
    started_at: String,
    completed_at: Option<String>,
    response: Option<StoredSandboxCommandResponse>,
    error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxCommandResponse {
    command_id: String,
    exit_status: i32,
    stdout_ref: String,
    stderr_ref: String,
    changed_paths: Vec<String>,
    resource_usage: SandboxResourceUsage,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxHttpExchangeRecord {
    request_id: String,
    sandbox_id: String,
    command_id: String,
    method: SandboxHttpMethod,
    url: String,
    host: String,
    status: Option<u16>,
    request_bytes: u64,
    response_bytes: u64,
    redacted_headers: Vec<String>,
}

impl TryFrom<&SandboxCommandRecord> for StoredSandboxCommandRecord {
    type Error = SandboxStoreError;

    fn try_from(record: &SandboxCommandRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            sandbox_id: record.sandbox_id.to_string(),
            command_id: record.command_id.to_string(),
            status: record.status.clone(),
            workspace_root: record.workspace_root.clone(),
            toolchain_id: record.toolchain_id.to_string(),
            argv: record.argv.clone(),
            cwd: record.cwd.to_string(),
            env: record
                .env
                .iter()
                .map(|(key, value)| StoredEnvPair {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            started_at: format_time(record.started_at)?,
            completed_at: record.completed_at.map(format_time).transpose()?,
            response: record
                .response
                .as_ref()
                .map(StoredSandboxCommandResponse::from),
            error: record.error.clone(),
        })
    }
}

impl TryFrom<StoredSandboxCommandRecord> for SandboxCommandRecord {
    type Error = SandboxStoreError;

    fn try_from(stored: StoredSandboxCommandRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            sandbox_id: SandboxId::new(stored.sandbox_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "command.record.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            command_id: SandboxCommandId::new(stored.command_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "command.record.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            status: stored.status,
            workspace_root: stored.workspace_root,
            toolchain_id: SandboxToolchainId::new(stored.toolchain_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "command.record.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            argv: stored.argv,
            cwd: SandboxPath::new(stored.cwd)?,
            env: stored
                .env
                .into_iter()
                .map(|pair| (pair.key, pair.value))
                .collect(),
            started_at: parse_time(&stored.started_at)?,
            completed_at: stored.completed_at.as_deref().map(parse_time).transpose()?,
            response: stored
                .response
                .map(SandboxCommandResponse::try_from)
                .transpose()?,
            error: stored.error,
        })
    }
}

impl From<&SandboxCommandResponse> for StoredSandboxCommandResponse {
    fn from(response: &SandboxCommandResponse) -> Self {
        Self {
            command_id: response.command_id.to_string(),
            exit_status: response.exit_status,
            stdout_ref: response.stdout_ref.clone(),
            stderr_ref: response.stderr_ref.clone(),
            changed_paths: response
                .changed_paths
                .iter()
                .map(ToString::to_string)
                .collect(),
            resource_usage: response.resource_usage.clone(),
        }
    }
}

impl TryFrom<StoredSandboxCommandResponse> for SandboxCommandResponse {
    type Error = SandboxStoreError;

    fn try_from(stored: StoredSandboxCommandResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            command_id: SandboxCommandId::new(stored.command_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "command.record.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            exit_status: stored.exit_status,
            stdout_ref: stored.stdout_ref,
            stderr_ref: stored.stderr_ref,
            changed_paths: stored
                .changed_paths
                .into_iter()
                .map(SandboxPath::new)
                .collect::<Result<Vec<_>, SandboxPathError>>()?,
            resource_usage: stored.resource_usage,
        })
    }
}

impl TryFrom<&SandboxHttpExchangeRecord> for StoredSandboxHttpExchangeRecord {
    type Error = SandboxStoreError;

    fn try_from(record: &SandboxHttpExchangeRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            request_id: record.request_id.to_string(),
            sandbox_id: record.sandbox_id.to_string(),
            command_id: record.command_id.to_string(),
            method: record.method.clone(),
            url: record.url.clone(),
            host: record.host.clone(),
            status: record.status,
            request_bytes: record.request_bytes,
            response_bytes: record.response_bytes,
            redacted_headers: record.redacted_headers.clone(),
        })
    }
}

impl TryFrom<StoredSandboxHttpExchangeRecord> for SandboxHttpExchangeRecord {
    type Error = SandboxStoreError;

    fn try_from(stored: StoredSandboxHttpExchangeRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            request_id: SandboxNetworkRequestId::new(stored.request_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "http.exchange.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            sandbox_id: SandboxId::new(stored.sandbox_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "http.exchange.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            command_id: SandboxCommandId::new(stored.command_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "http.exchange.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            method: stored.method,
            url: stored.url,
            host: stored.host,
            status: stored.status,
            request_bytes: stored.request_bytes,
            response_bytes: stored.response_bytes,
            redacted_headers: stored.redacted_headers,
        })
    }
}

impl TryFrom<&SandboxInstanceManifest> for StoredSandboxInstanceManifest {
    type Error = SandboxStoreError;

    fn try_from(manifest: &SandboxInstanceManifest) -> Result<Self, Self::Error> {
        Ok(Self {
            sandbox_id: manifest.sandbox_id.to_string(),
            workspace_root: manifest.workspace_root.clone(),
            toolchain_id: manifest.toolchain_id.to_string(),
            cwd: manifest.cwd.to_string(),
            env: manifest
                .env
                .iter()
                .map(|(key, value)| StoredEnvPair {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            mounts: manifest
                .mounts
                .iter()
                .map(|mount| StoredSandboxMount {
                    source_sifs_path: mount.source_sifs_path.clone(),
                    mount_path: mount.mount_path.to_string(),
                    mode: mount.mode.clone(),
                })
                .collect(),
            command_history: manifest
                .command_history
                .iter()
                .map(ToString::to_string)
                .collect(),
            last_placement: manifest.last_placement.clone(),
            lifecycle: manifest.lifecycle.clone(),
            runtime_binding: manifest.runtime_binding.clone(),
            active_processes: manifest.active_processes.clone(),
            temp_policy: manifest.temp_policy.clone(),
            temp_epoch: manifest.temp_epoch,
            limits: manifest.limits.clone(),
            created_at: format_time(manifest.created_at)?,
            updated_at: format_time(manifest.updated_at)?,
        })
    }
}

impl TryFrom<StoredSandboxInstanceManifest> for SandboxInstanceManifest {
    type Error = SandboxStoreError;

    fn try_from(stored: StoredSandboxInstanceManifest) -> Result<Self, Self::Error> {
        Ok(Self {
            sandbox_id: SandboxId::new(stored.sandbox_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "manifest.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            workspace_root: stored.workspace_root,
            toolchain_id: SandboxToolchainId::new(stored.toolchain_id).map_err(|source| {
                SandboxStoreError::Object {
                    path: "manifest.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
            cwd: SandboxPath::new(stored.cwd)?,
            env: stored
                .env
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect(),
            mounts: stored
                .mounts
                .into_iter()
                .map(|mount| {
                    Ok(SandboxMount {
                        source_sifs_path: mount.source_sifs_path,
                        mount_path: SandboxPath::new(mount.mount_path)?,
                        mode: mount.mode,
                    })
                })
                .collect::<Result<Vec<_>, SandboxStoreError>>()?,
            command_history: stored
                .command_history
                .into_iter()
                .map(|command_id| {
                    SandboxCommandId::new(command_id).map_err(|source| SandboxStoreError::Object {
                        path: "manifest.sobj".to_string(),
                        detail: source.to_string(),
                    })
                })
                .collect::<Result<Vec<_>, SandboxStoreError>>()?,
            last_placement: stored.last_placement,
            lifecycle: stored.lifecycle,
            runtime_binding: stored.runtime_binding,
            active_processes: stored.active_processes,
            temp_policy: stored.temp_policy,
            temp_epoch: stored.temp_epoch,
            limits: stored.limits,
            created_at: parse_time(&stored.created_at)?,
            updated_at: parse_time(&stored.updated_at)?,
        })
    }
}

pub(crate) fn format_time(time: OffsetDateTime) -> Result<String, SandboxStoreError> {
    time.format(&Rfc3339)
        .map_err(|source| SandboxStoreError::Object {
            path: "manifest.sobj".to_string(),
            detail: source.to_string(),
        })
}

pub(crate) fn parse_time(value: &str) -> Result<OffsetDateTime, SandboxStoreError> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|source| SandboxStoreError::Object {
        path: "manifest.sobj".to_string(),
        detail: source.to_string(),
    })
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SandboxStoreError {
    #[error("sandbox instance already exists: {sandbox_id}")]
    AlreadyExists { sandbox_id: SandboxId },
    #[error("sandbox instance not found: {sandbox_id}")]
    NotFound { sandbox_id: SandboxId },
    #[error("sandbox checkpoint not found: {sandbox_id}:{checkpoint_id}")]
    CheckpointNotFound {
        sandbox_id: SandboxId,
        checkpoint_id: SandboxCheckpointId,
    },
    #[error("sandbox path error: {0}")]
    Path(#[from] SandboxPathError),
    #[error("sandbox store object error at {path}: {detail}")]
    Object { path: String, detail: String },
    #[error("sandbox store I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
}

impl From<SandboxFsError> for SandboxStoreError {
    fn from(error: SandboxFsError) -> Self {
        match error {
            SandboxFsError::Path(error) => Self::Path(error),
            SandboxFsError::NotFound { path } => Self::Io {
                path: path.to_string(),
                detail: "sandbox filesystem path not found".to_string(),
            },
            SandboxFsError::AlreadyExists { path } => Self::Io {
                path: path.to_string(),
                detail: "sandbox filesystem path already exists".to_string(),
            },
            SandboxFsError::ReadOnly { path } => Self::Io {
                path: path.to_string(),
                detail: "sandbox filesystem path is read-only".to_string(),
            },
            SandboxFsError::NotDirectory { path } => Self::Io {
                path: path.to_string(),
                detail: "sandbox filesystem path is not a directory".to_string(),
            },
            SandboxFsError::IsDirectory { path } => Self::Io {
                path: path.to_string(),
                detail: "sandbox filesystem path is a directory".to_string(),
            },
            SandboxFsError::NotSymlink { path } => Self::Io {
                path: path.to_string(),
                detail: "sandbox filesystem path is not a symbolic link".to_string(),
            },
            SandboxFsError::DirectoryNotEmpty { path } => Self::Io {
                path: path.to_string(),
                detail: "sandbox filesystem directory is not empty".to_string(),
            },
            SandboxFsError::Unsupported { operation } => Self::Io {
                path: "sandbox filesystem".to_string(),
                detail: format!("unsupported operation: {operation}"),
            },
            SandboxFsError::QuotaExceeded {
                kind,
                actual,
                limit,
            } => Self::Io {
                path: "sandbox filesystem".to_string(),
                detail: format!("quota exceeded for {kind}: actual {actual}, limit {limit}"),
            },
            SandboxFsError::Unavailable { reason } => Self::Io {
                path: "sandbox filesystem".to_string(),
                detail: reason,
            },
        }
    }
}
