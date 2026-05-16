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

pub fn sandbox_manifest_path(sandbox_id: &SandboxId) -> String {
    format!("{}/manifest.sobj", sandbox_instance_root(sandbox_id))
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
                if components.pop().is_none() {
                    return Err(SandboxPathError::EscapesRoot {
                        path: path.to_string(),
                    });
                }
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
}
