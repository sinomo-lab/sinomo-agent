use crate::{
    SIFS_SANDBOX_ROOT, SandboxCommandId, SandboxId, SandboxNetworkRequestId, SandboxToolchainError,
    SandboxToolchainId, validate_toolchain_artifact_relative_path,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

pub fn sandbox_instance_root(sandbox_id: &SandboxId) -> String {
    format!("{SIFS_SANDBOX_ROOT}/instances/{sandbox_id}")
}

/// Absolute path to this crate's bundled sandbox toolchain assets (busybox /
/// python / etc. wasm modules plus the Binaryen `wasm-opt` binary the WASIX
/// runner uses for asyncify). The assets live in the contracts crate's source
/// tree, so they are resolved relative to *its* manifest dir — the runtime crate
/// reaches them through this helper rather than its own `CARGO_MANIFEST_DIR`.
pub fn bundled_toolchains_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("toolchains")
}

pub fn sandbox_manifest_path(sandbox_id: &SandboxId) -> String {
    format!("{}/manifest.sobj", sandbox_control_root(sandbox_id))
}

pub fn sandbox_control_root(sandbox_id: &SandboxId) -> String {
    format!("{SIFS_SANDBOX_ROOT}/control/{sandbox_id}")
}

/// Immutable workspace generations built by authoritative sandbox transactions.
/// The stable instance path is an atomically replaceable symlink to one complete
/// generation; the manifest and transaction fence live outside that symlink.
pub fn sandbox_transaction_root(sandbox_id: &SandboxId) -> String {
    format!("{SIFS_SANDBOX_ROOT}/generations/{sandbox_id}")
}

pub fn sandbox_transaction_workspace_root(
    sandbox_id: &SandboxId,
    incarnation_id: &sinomo_core::Id,
    transaction_id: &sinomo_core::Id,
) -> String {
    format!(
        "{}/{incarnation_id}/{transaction_id}",
        sandbox_transaction_root(sandbox_id)
    )
}

pub fn sandbox_incarnation_generation_root(
    sandbox_id: &SandboxId,
    incarnation_id: &sinomo_core::Id,
) -> String {
    format!("{}/{incarnation_id}", sandbox_transaction_root(sandbox_id))
}

pub fn sandbox_transaction_swap_link(
    sandbox_id: &SandboxId,
    transaction_id: &sinomo_core::Id,
) -> String {
    format!("{SIFS_SANDBOX_ROOT}/instances/.swap-{sandbox_id}-{transaction_id}")
}

pub fn sandbox_legacy_backup_root(
    sandbox_id: &SandboxId,
    transaction_id: &sinomo_core::Id,
) -> String {
    format!("{SIFS_SANDBOX_ROOT}/legacy-backups/{sandbox_id}/{transaction_id}")
}

/// Checkpoints are control-plane state and deliberately stay outside the
/// swappable workspace generation. Publishing a new workspace pointer must
/// not orphan the checkpoint catalog used to recover it.
pub fn sandbox_checkpoint_root(sandbox_id: &SandboxId) -> String {
    format!("{}/checkpoints", sandbox_control_root(sandbox_id))
}

/// Persistent coordination root. Lock anchors deliberately live outside an
/// instance subtree so destroy/recreate cannot evade a still-live execution
/// lease on the previous instance generation.
pub fn sandbox_lock_root() -> String {
    format!("{SIFS_SANDBOX_ROOT}/locks")
}

pub fn sandbox_lock_anchor_path(sandbox_id: &SandboxId) -> String {
    format!("{}/{sandbox_id}.lock", sandbox_lock_root())
}

pub fn sandbox_toolchain_root(toolchain_id: &SandboxToolchainId) -> String {
    format!("{SIFS_SANDBOX_ROOT}/toolchains/{toolchain_id}")
}

pub fn sandbox_toolchain_manifest_path(toolchain_id: &SandboxToolchainId) -> String {
    format!("{}/manifest.sobj", sandbox_toolchain_root(toolchain_id))
}

pub fn sandbox_toolchain_command_path(
    toolchain_id: &SandboxToolchainId,
    command_name: &str,
) -> String {
    format!(
        "{}/bin/{command_name}.wasm",
        sandbox_toolchain_root(toolchain_id)
    )
}

pub fn sandbox_toolchain_artifact_path(
    toolchain_id: &SandboxToolchainId,
    relative_path: &str,
) -> Result<String, SandboxToolchainError> {
    let normalized = validate_toolchain_artifact_relative_path(relative_path)?;
    Ok(format!(
        "{}/{}",
        sandbox_toolchain_root(toolchain_id),
        normalized
    ))
}

pub fn sandbox_toolchain_artifact_bundle_path(
    toolchain_id: &SandboxToolchainId,
    bundle_name: &str,
) -> Result<String, SandboxToolchainError> {
    if bundle_name.is_empty()
        || bundle_name == "."
        || bundle_name == ".."
        || bundle_name.contains('/')
        || bundle_name.contains('\\')
        || !bundle_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(SandboxToolchainError::InvalidManifest {
            detail: format!("invalid toolchain artifact bundle name `{bundle_name}`"),
        });
    }
    Ok(format!(
        "{}/bundles/{bundle_name}.tar",
        sandbox_toolchain_root(toolchain_id)
    ))
}

pub fn sandbox_command_stdout_path(
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
) -> String {
    format!(
        "{}/logs/{command_id}.stdout",
        sandbox_instance_root(sandbox_id)
    )
}

pub fn sandbox_command_stderr_path(
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
) -> String {
    format!(
        "{}/logs/{command_id}.stderr",
        sandbox_instance_root(sandbox_id)
    )
}

pub fn sandbox_command_record_path(
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
) -> String {
    format!(
        "{}/logs/{command_id}.record.sobj",
        sandbox_instance_root(sandbox_id)
    )
}

pub fn sandbox_command_stdout_log_path(
    command_id: &SandboxCommandId,
) -> Result<SandboxPath, SandboxPathError> {
    SandboxPath::new(format!("/logs/{command_id}.stdout"))
}

pub fn sandbox_command_stderr_log_path(
    command_id: &SandboxCommandId,
) -> Result<SandboxPath, SandboxPathError> {
    SandboxPath::new(format!("/logs/{command_id}.stderr"))
}

pub fn sandbox_command_record_log_path(
    command_id: &SandboxCommandId,
) -> Result<SandboxPath, SandboxPathError> {
    SandboxPath::new(format!("/logs/{command_id}.record.sobj"))
}

pub fn sandbox_http_exchange_record_path(
    sandbox_id: &SandboxId,
    command_id: &SandboxCommandId,
    request_id: &SandboxNetworkRequestId,
) -> String {
    format!(
        "{}/logs/{command_id}.{request_id}.http.sobj",
        sandbox_instance_root(sandbox_id)
    )
}

pub fn sandbox_http_exchange_record_log_path(
    command_id: &SandboxCommandId,
    request_id: &SandboxNetworkRequestId,
) -> Result<SandboxPath, SandboxPathError> {
    SandboxPath::new(format!("/logs/{command_id}.{request_id}.http.sobj"))
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SandboxPath(String);

impl SandboxPath {
    pub fn new(path: impl AsRef<str>) -> Result<Self, SandboxPathError> {
        normalize_sandbox_path(path.as_ref()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn join(&self, child: impl AsRef<str>) -> Result<Self, SandboxPathError> {
        if child.as_ref().starts_with('/') {
            return Self::new(child);
        }
        let base = if self.0 == "/" {
            String::new()
        } else {
            self.0.clone()
        };
        Self::new(format!("{base}/{}", child.as_ref()))
    }

    /// The sandbox path naming a host path, for runtimes that run commands
    /// at host paths (the simple native runtime). A Windows drive path such
    /// as `C:\work` becomes `/C:/work`; a Unix path is used as is.
    pub fn for_host_path(path: &std::path::Path) -> Result<Self, SandboxPathError> {
        Self::new(host_path_to_sandbox_string(path)?)
    }

    /// The host path this sandbox path names for those runtimes: the inverse
    /// of [`Self::for_host_path`].
    pub fn host_path(&self) -> std::path::PathBuf {
        sandbox_string_to_host_path(&self.0)
    }
}

#[cfg(windows)]
fn host_path_to_sandbox_string(path: &std::path::Path) -> Result<String, SandboxPathError> {
    let raw = path.display().to_string();
    let raw = raw.strip_prefix(r"\\?\").unwrap_or(&raw);
    let normalized = raw.replace('\\', "/");
    let bytes = normalized.as_bytes();
    if bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
        Ok(format!("/{normalized}"))
    } else {
        Err(SandboxPathError::UnsupportedHostPath {
            path: path.display().to_string(),
        })
    }
}

#[cfg(not(windows))]
fn host_path_to_sandbox_string(path: &std::path::Path) -> Result<String, SandboxPathError> {
    Ok(path.display().to_string())
}

#[cfg(windows)]
fn sandbox_string_to_host_path(path: &str) -> std::path::PathBuf {
    if let Some(encoded) = path.strip_prefix('/') {
        let bytes = encoded.as_bytes();
        if bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
            let rest = encoded[2..].replace('/', "\\");
            let rest = if rest.is_empty() {
                "\\".to_string()
            } else {
                rest
            };
            return std::path::PathBuf::from(format!("{}:{rest}", bytes[0] as char));
        }
    }
    std::path::PathBuf::from(path)
}

#[cfg(not(windows))]
fn sandbox_string_to_host_path(path: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(path)
}

impl fmt::Display for SandboxPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl TryFrom<String> for SandboxPath {
    type Error = SandboxPathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SandboxPath> for String {
    fn from(path: SandboxPath) -> Self {
        path.0
    }
}

fn normalize_sandbox_path(path: &str) -> Result<String, SandboxPathError> {
    if path.is_empty() {
        return Err(SandboxPathError::Empty);
    }
    if !path.starts_with('/') {
        return Err(SandboxPathError::Relative {
            path: path.to_string(),
        });
    }
    if path.bytes().any(|byte| byte == 0) {
        return Err(SandboxPathError::NulByte);
    }

    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => {
                components.pop();
            }
            value => components.push(value),
        }
    }

    if components.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", components.join("/")))
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SandboxPathError {
    #[error("sandbox path cannot be empty")]
    Empty,
    #[error("sandbox path must be absolute: {path}")]
    Relative { path: String },
    #[error("sandbox path cannot contain nul bytes")]
    NulByte,
    #[error("sandbox path escapes sandbox root: {path}")]
    EscapesRoot { path: String },
    #[error("host path {path} is not a local drive path a sandbox can name")]
    UnsupportedHostPath { path: String },
}
