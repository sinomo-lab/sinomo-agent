use crate::internal::*;

use super::SandboxToolchainError;
use super::support::{
    sandbox_module_hash, validate_toolchain_artifact_relative_path, validate_toolchain_command_name,
};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolchainCommand {
    pub name: String,
    pub module_path: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_abi: Option<SandboxWasiAbi>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub argv_prefix: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub default_env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    pub default_limits: SandboxResourceLimits,
}

impl SandboxToolchainCommand {
    pub fn new(
        toolchain_id: &SandboxToolchainId,
        name: impl Into<String>,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
    ) -> Result<Self, SandboxToolchainError> {
        let name = name.into();
        validate_toolchain_command_name(&name)?;
        Ok(Self {
            module_path: sandbox_toolchain_command_path(toolchain_id, &name),
            content_hash: sandbox_module_hash(module_bytes),
            target_abi: None,
            name,
            argv_prefix: Vec::new(),
            exec_name: None,
            default_env: BTreeMap::new(),
            required_capabilities: Vec::new(),
            default_limits,
        })
    }

    pub fn effective_argv(&self, argv: Vec<String>) -> Vec<String> {
        if self.argv_prefix.is_empty() && self.exec_name.is_none() {
            return argv;
        }
        let mut effective = Vec::with_capacity(argv.len() + self.argv_prefix.len());
        if let Some(argv0) = argv.first() {
            effective.push(self.exec_name.clone().unwrap_or_else(|| argv0.clone()));
            effective.extend(self.argv_prefix.iter().cloned());
            effective.extend(argv.into_iter().skip(1));
        }
        effective
    }

    pub fn effective_target_abi(&self, manifest: &SandboxToolchainManifest) -> SandboxWasiAbi {
        self.target_abi
            .clone()
            .unwrap_or_else(|| manifest.target_abi.clone())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolchainArtifact {
    pub relative_path: String,
    pub sifs_path: String,
    pub content_hash: String,
    pub byte_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolchainArtifactBundle {
    pub bundle_path: String,
    pub content_hash: String,
    pub byte_len: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<SandboxToolchainArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolchainPackageSpec {
    pub toolchain_id: SandboxToolchainId,
    pub version: String,
    #[serde(default = "default_sandbox_wasi_abi")]
    pub target_abi: SandboxWasiAbi,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<SandboxToolchainPackageCommand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<SandboxToolchainPackageArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolchainPackageCommand {
    pub name: String,
    pub module: String,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_abi: Option<SandboxWasiAbi>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub argv_prefix: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub default_env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub default_limits: SandboxResourceLimits,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolchainPackageArtifact {
    pub relative_path: String,
    pub file: String,
    pub content_hash: String,
}

impl SandboxToolchainArtifact {
    pub fn new(
        toolchain_id: &SandboxToolchainId,
        relative_path: impl Into<String>,
        bytes: &[u8],
    ) -> Result<Self, SandboxToolchainError> {
        let relative_path = validate_toolchain_artifact_relative_path(&relative_path.into())?;
        Ok(Self {
            sifs_path: sandbox_toolchain_artifact_path(toolchain_id, &relative_path)?,
            content_hash: sandbox_module_hash(bytes),
            byte_len: bytes.len() as u64,
            relative_path,
        })
    }
}

impl SandboxToolchainArtifactBundle {
    pub fn new(
        toolchain_id: &SandboxToolchainId,
        bundle_name: &str,
        bundle_bytes: &[u8],
        artifacts: Vec<SandboxToolchainArtifact>,
    ) -> Result<Self, SandboxToolchainError> {
        Ok(Self {
            bundle_path: sandbox_toolchain_artifact_bundle_path(toolchain_id, bundle_name)?,
            content_hash: sandbox_module_hash(bundle_bytes),
            byte_len: bundle_bytes.len() as u64,
            artifacts,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxToolchainManifest {
    pub toolchain_id: SandboxToolchainId,
    pub version: String,
    pub target_abi: SandboxWasiAbi,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub commands: BTreeMap<String, SandboxToolchainCommand>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub artifacts: BTreeMap<String, SandboxToolchainArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_bundles: Vec<SandboxToolchainArtifactBundle>,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

impl SandboxToolchainManifest {
    pub fn new(
        toolchain_id: SandboxToolchainId,
        version: impl Into<String>,
        target_abi: SandboxWasiAbi,
        provenance: impl Into<String>,
        created_at: OffsetDateTime,
    ) -> Result<Self, SandboxToolchainError> {
        let version = version.into();
        if version.trim().is_empty() {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: "toolchain version must be non-empty".to_string(),
            });
        }
        Ok(Self {
            toolchain_id,
            version,
            target_abi,
            commands: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            artifact_bundles: Vec::new(),
            provenance: provenance.into(),
            signature: None,
            created_at,
        })
    }

    pub fn insert_command(
        &mut self,
        command: SandboxToolchainCommand,
    ) -> Result<(), SandboxToolchainError> {
        validate_toolchain_command_name(&command.name)?;
        if let Some(exec_name) = &command.exec_name {
            validate_toolchain_command_name(exec_name)?;
        }
        validate_toolchain_command_argv_prefix(&command)?;
        validate_toolchain_command_default_env(&command)?;
        validate_toolchain_command_required_capabilities(&command)?;
        if self.commands.contains_key(&command.name) {
            return Err(SandboxToolchainError::DuplicateCommand { name: command.name });
        }
        self.commands.insert(command.name.clone(), command);
        Ok(())
    }

    pub fn command(&self, name: &str) -> Result<&SandboxToolchainCommand, SandboxToolchainError> {
        self.commands
            .get(name)
            .ok_or_else(|| SandboxToolchainError::CommandNotFound {
                toolchain_id: self.toolchain_id.clone(),
                name: name.to_string(),
            })
    }

    pub fn insert_artifact(
        &mut self,
        artifact: SandboxToolchainArtifact,
    ) -> Result<(), SandboxToolchainError> {
        validate_toolchain_artifact_relative_path(&artifact.relative_path)?;
        if self.artifacts.contains_key(&artifact.relative_path) {
            return Err(SandboxToolchainError::DuplicateArtifact {
                path: artifact.relative_path,
            });
        }
        self.artifacts
            .insert(artifact.relative_path.clone(), artifact);
        Ok(())
    }

    pub fn artifact(
        &self,
        relative_path: &str,
    ) -> Result<&SandboxToolchainArtifact, SandboxToolchainError> {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        self.artifacts
            .get(&relative_path)
            .ok_or_else(|| SandboxToolchainError::ArtifactNotFound {
                toolchain_id: self.toolchain_id.clone(),
                path: relative_path,
            })
    }

    pub fn insert_artifact_bundle(
        &mut self,
        bundle: SandboxToolchainArtifactBundle,
    ) -> Result<(), SandboxToolchainError> {
        for artifact in &bundle.artifacts {
            validate_toolchain_artifact_relative_path(&artifact.relative_path)?;
            if !self.artifacts.contains_key(&artifact.relative_path) {
                return Err(SandboxToolchainError::InvalidManifest {
                    detail: format!(
                        "artifact bundle references `{}` but the manifest has no matching artifact",
                        artifact.relative_path
                    ),
                });
            }
        }
        self.artifact_bundles.push(bundle);
        Ok(())
    }
}

fn validate_toolchain_command_default_env(
    command: &SandboxToolchainCommand,
) -> Result<(), SandboxToolchainError> {
    for (name, value) in &command.default_env {
        if !sandbox_env_name_is_valid(name) {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "toolchain command `{}` has an invalid default environment variable name `{name}`",
                    command.name
                ),
            });
        }
        if value.contains('\0') {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "toolchain command `{}` default environment variable `{name}` contains an embedded NUL byte",
                    command.name
                ),
            });
        }
    }
    Ok(())
}

fn validate_toolchain_command_argv_prefix(
    command: &SandboxToolchainCommand,
) -> Result<(), SandboxToolchainError> {
    for (index, arg) in command.argv_prefix.iter().enumerate() {
        if arg.contains('\0') {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "toolchain command `{}` argv_prefix[{index}] contains an embedded NUL byte",
                    command.name
                ),
            });
        }
    }
    Ok(())
}

fn validate_toolchain_command_required_capabilities(
    command: &SandboxToolchainCommand,
) -> Result<(), SandboxToolchainError> {
    for capability in &command.required_capabilities {
        if capability.trim().is_empty() {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "toolchain command `{}` required capability must be non-empty",
                    command.name
                ),
            });
        }
        if capability.contains('\0') {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "toolchain command `{}` required capability `{capability}` contains an embedded NUL byte",
                    command.name
                ),
            });
        }
    }
    Ok(())
}
