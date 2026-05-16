use crate::internal::*;

use crate::store::{format_time, parse_time};

use super::{
    SandboxToolchainArtifact, SandboxToolchainArtifactBundle, SandboxToolchainCommand,
    SandboxToolchainError, SandboxToolchainManifest,
};

pub(super) fn encode_toolchain_manifest(
    manifest: &SandboxToolchainManifest,
    path: &str,
) -> Result<Vec<u8>, SandboxToolchainError> {
    let stored = StoredSandboxToolchainManifest::try_from(manifest)?;
    sinomo_object_serde::to_bytes(
        &stored,
        SANDBOX_TOOLCHAIN_MANIFEST_OBJECT_VERSION,
        LengthMode::U32,
    )
    .map_err(|source| SandboxToolchainError::Object {
        path: path.to_string(),
        detail: source.to_string(),
    })
}

pub(super) fn decode_toolchain_manifest(
    bytes: &[u8],
    path: &str,
) -> Result<SandboxToolchainManifest, SandboxToolchainError> {
    let stored: StoredSandboxToolchainManifest =
        sinomo_object_serde::from_bytes(bytes).map_err(|source| SandboxToolchainError::Object {
            path: path.to_string(),
            detail: source.to_string(),
        })?;
    SandboxToolchainManifest::try_from(stored)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxToolchainManifest {
    toolchain_id: String,
    version: String,
    target_abi: SandboxWasiAbi,
    #[serde(default)]
    commands: Vec<StoredSandboxToolchainCommand>,
    #[serde(default)]
    artifacts: Vec<StoredSandboxToolchainArtifact>,
    #[serde(default)]
    artifact_bundles: Vec<StoredSandboxToolchainArtifactBundle>,
    provenance: String,
    signature: Option<String>,
    created_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxToolchainCommand {
    name: String,
    module_path: String,
    content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target_abi: Option<SandboxWasiAbi>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    argv_prefix: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exec_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    default_env: BTreeMap<String, String>,
    #[serde(default)]
    required_capabilities: Vec<String>,
    default_limits: SandboxResourceLimits,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxToolchainArtifact {
    relative_path: String,
    sifs_path: String,
    content_hash: String,
    byte_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredSandboxToolchainArtifactBundle {
    bundle_path: String,
    content_hash: String,
    byte_len: u64,
    #[serde(default)]
    artifacts: Vec<StoredSandboxToolchainArtifact>,
}

impl TryFrom<&SandboxToolchainManifest> for StoredSandboxToolchainManifest {
    type Error = SandboxToolchainError;

    fn try_from(manifest: &SandboxToolchainManifest) -> Result<Self, Self::Error> {
        Ok(Self {
            toolchain_id: manifest.toolchain_id.to_string(),
            version: manifest.version.clone(),
            target_abi: manifest.target_abi.clone(),
            commands: manifest
                .commands
                .values()
                .map(|command| StoredSandboxToolchainCommand {
                    name: command.name.clone(),
                    module_path: command.module_path.clone(),
                    content_hash: command.content_hash.clone(),
                    target_abi: command.target_abi.clone(),
                    argv_prefix: command.argv_prefix.clone(),
                    exec_name: command.exec_name.clone(),
                    default_env: command.default_env.clone(),
                    required_capabilities: command.required_capabilities.clone(),
                    default_limits: command.default_limits.clone(),
                })
                .collect(),
            artifacts: manifest
                .artifacts
                .values()
                .map(|artifact| StoredSandboxToolchainArtifact {
                    relative_path: artifact.relative_path.clone(),
                    sifs_path: artifact.sifs_path.clone(),
                    content_hash: artifact.content_hash.clone(),
                    byte_len: artifact.byte_len,
                })
                .collect(),
            artifact_bundles: manifest
                .artifact_bundles
                .iter()
                .map(|bundle| StoredSandboxToolchainArtifactBundle {
                    bundle_path: bundle.bundle_path.clone(),
                    content_hash: bundle.content_hash.clone(),
                    byte_len: bundle.byte_len,
                    artifacts: bundle
                        .artifacts
                        .iter()
                        .map(|artifact| StoredSandboxToolchainArtifact {
                            relative_path: artifact.relative_path.clone(),
                            sifs_path: artifact.sifs_path.clone(),
                            content_hash: artifact.content_hash.clone(),
                            byte_len: artifact.byte_len,
                        })
                        .collect(),
                })
                .collect(),
            provenance: manifest.provenance.clone(),
            signature: manifest.signature.clone(),
            created_at: format_time(manifest.created_at).map_err(|source| {
                SandboxToolchainError::Object {
                    path: "manifest.sobj".to_string(),
                    detail: source.to_string(),
                }
            })?,
        })
    }
}

impl TryFrom<StoredSandboxToolchainManifest> for SandboxToolchainManifest {
    type Error = SandboxToolchainError;

    fn try_from(stored: StoredSandboxToolchainManifest) -> Result<Self, Self::Error> {
        let toolchain_id = SandboxToolchainId::new(stored.toolchain_id).map_err(|source| {
            SandboxToolchainError::Object {
                path: "manifest.sobj".to_string(),
                detail: source.to_string(),
            }
        })?;
        let created_at =
            parse_time(&stored.created_at).map_err(|source| SandboxToolchainError::Object {
                path: "manifest.sobj".to_string(),
                detail: source.to_string(),
            })?;
        let mut manifest = SandboxToolchainManifest::new(
            toolchain_id,
            stored.version,
            stored.target_abi,
            stored.provenance,
            created_at,
        )?;
        manifest.signature = stored.signature;
        for command in stored.commands {
            manifest.insert_command(SandboxToolchainCommand {
                name: command.name,
                module_path: command.module_path,
                content_hash: command.content_hash,
                target_abi: command.target_abi,
                argv_prefix: command.argv_prefix,
                exec_name: command.exec_name,
                default_env: command.default_env,
                required_capabilities: command.required_capabilities,
                default_limits: command.default_limits,
            })?;
        }
        for artifact in stored.artifacts {
            manifest.insert_artifact(SandboxToolchainArtifact {
                relative_path: artifact.relative_path,
                sifs_path: artifact.sifs_path,
                content_hash: artifact.content_hash,
                byte_len: artifact.byte_len,
            })?;
        }
        for bundle in stored.artifact_bundles {
            let artifacts = bundle
                .artifacts
                .into_iter()
                .map(|artifact| SandboxToolchainArtifact {
                    relative_path: artifact.relative_path,
                    sifs_path: artifact.sifs_path,
                    content_hash: artifact.content_hash,
                    byte_len: artifact.byte_len,
                })
                .collect();
            manifest.insert_artifact_bundle(SandboxToolchainArtifactBundle {
                bundle_path: bundle.bundle_path,
                content_hash: bundle.content_hash,
                byte_len: bundle.byte_len,
                artifacts,
            })?;
        }
        Ok(manifest)
    }
}
