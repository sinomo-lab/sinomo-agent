use crate::internal::*;

use super::serde::encode_toolchain_manifest;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolFidelityResult {
    pub case_name: String,
    pub command: String,
    pub passed: bool,
    pub exit_status: i32,
    pub stdout: String,
    pub stderr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mismatch: Option<String>,
}

#[derive(Debug, Error)]
pub enum SandboxToolFidelityError {
    #[error(transparent)]
    Command(#[from] SandboxCommandExecutionError),
    #[error("sandbox tool fidelity failed: {failed} of {total} cases failed")]
    Failed {
        failed: usize,
        total: usize,
        results: Vec<SandboxToolFidelityResult>,
    },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SandboxToolchainError {
    #[error("invalid sandbox toolchain manifest: {detail}")]
    InvalidManifest { detail: String },
    #[error("invalid sandbox toolchain command name: {name}")]
    InvalidCommandName { name: String },
    #[error("invalid sandbox toolchain artifact path: {path}")]
    InvalidArtifactPath { path: String },
    #[error("duplicate sandbox toolchain command: {name}")]
    DuplicateCommand { name: String },
    #[error("duplicate sandbox toolchain artifact: {path}")]
    DuplicateArtifact { path: String },
    #[error("sandbox toolchain command not found: {toolchain_id}:{name}")]
    CommandNotFound {
        toolchain_id: SandboxToolchainId,
        name: String,
    },
    #[error("sandbox toolchain artifact not found: {toolchain_id}:{path}")]
    ArtifactNotFound {
        toolchain_id: SandboxToolchainId,
        path: String,
    },
    #[error("sandbox toolchain not found: {toolchain_id}")]
    NotFound { toolchain_id: SandboxToolchainId },
    #[error("sandbox toolchain object error at {path}: {detail}")]
    Object { path: String, detail: String },
    #[error("sandbox toolchain I/O error at {path}: {detail}")]
    Io { path: String, detail: String },
}

pub fn sandbox_module_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

pub fn pack_toolchain_artifact_bundle(
    entries: &[(String, Vec<u8>)],
) -> Result<Vec<u8>, SandboxToolchainError> {
    let mut builder = tar::Builder::new(Vec::new());
    for (relative_path, bytes) in entries {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, relative_path, bytes.as_slice())
            .map_err(|source| SandboxToolchainError::Io {
                path: "toolchain artifact bundle".to_string(),
                detail: source.to_string(),
            })?;
    }
    builder
        .into_inner()
        .map_err(|source| SandboxToolchainError::Io {
            path: "toolchain artifact bundle".to_string(),
            detail: source.to_string(),
        })
}

pub fn unpack_toolchain_artifact_bundle(
    bytes: &[u8],
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
    let mut entries = Vec::new();
    let archive_entries =
        archive
            .entries()
            .map_err(|source| SandboxToolchainError::InvalidManifest {
                detail: format!("invalid toolchain artifact bundle: {source}"),
            })?;
    for entry in archive_entries {
        let mut entry = entry.map_err(|source| SandboxToolchainError::InvalidManifest {
            detail: format!("invalid toolchain artifact bundle entry: {source}"),
        })?;
        if !entry.header().entry_type().is_file() {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: "toolchain artifact bundle contains a non-file entry".to_string(),
            });
        }
        let path = entry
            .path()
            .map_err(|source| SandboxToolchainError::InvalidManifest {
                detail: format!("invalid toolchain artifact bundle path: {source}"),
            })?
            .to_string_lossy()
            .to_string();
        let relative_path = validate_toolchain_artifact_relative_path(&path)?;
        let mut entry_bytes = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut entry_bytes).map_err(|source| {
            SandboxToolchainError::InvalidManifest {
                detail: format!("failed to read toolchain artifact bundle entry: {source}"),
            }
        })?;
        entries.push((relative_path, entry_bytes));
    }
    Ok(entries)
}

#[derive(Debug)]
pub struct SandboxModuleHasher {
    hasher: blake3::Hasher,
}

impl SandboxModuleHasher {
    pub fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    pub fn finalize(self) -> String {
        format!("blake3:{}", self.hasher.finalize().to_hex())
    }
}

impl Default for SandboxModuleHasher {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn validate_toolchain_command_name(name: &str) -> Result<(), SandboxToolchainError> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+'))
    {
        return Err(SandboxToolchainError::InvalidCommandName {
            name: name.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_toolchain_artifact_relative_path(
    path: &str,
) -> Result<String, SandboxToolchainError> {
    let trimmed = path.trim();
    if trimmed.is_empty()
        || trimmed.starts_with('/')
        || trimmed.starts_with('\\')
        || trimmed.contains('\\')
    {
        return Err(SandboxToolchainError::InvalidArtifactPath {
            path: path.to_string(),
        });
    }
    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || !component
                .bytes()
                .all(|byte| matches!(byte, b' '..=b'~') && !matches!(byte, b'/' | b'\\'))
        {
            return Err(SandboxToolchainError::InvalidArtifactPath {
                path: path.to_string(),
            });
        }
        components.push(component);
    }
    match components.first().copied() {
        Some(
            "bin" | "lib" | "share" | "include" | "etc" | "usr" | "var" | "Lib" | "cross-build"
            | "sysroot",
        ) if components.len() > 1 => Ok(components.join("/")),
        _ => Err(SandboxToolchainError::InvalidArtifactPath {
            path: path.to_string(),
        }),
    }
}

pub(crate) fn sandbox_toolchain_root_from_path(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_else(|| path.to_string())
}

pub(crate) fn write_toolchain_manifest(
    path: &Path,
    manifest: &SandboxToolchainManifest,
) -> Result<(), SandboxToolchainError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| SandboxToolchainError::Io {
            path: parent.display().to_string(),
            detail: source.to_string(),
        })?;
    }
    let bytes = encode_toolchain_manifest(manifest, &path.display().to_string())?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("manifest.sobj");
    let temp_path = parent.join(format!(".{file_name}.tmp-{}", std::process::id()));
    fs::write(&temp_path, bytes).map_err(|source| SandboxToolchainError::Io {
        path: temp_path.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::rename(&temp_path, path).map_err(|source| SandboxToolchainError::Io {
        path: path.display().to_string(),
        detail: source.to_string(),
    })
}
