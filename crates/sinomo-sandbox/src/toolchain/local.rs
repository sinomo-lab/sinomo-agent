use crate::internal::*;

use super::serde::decode_toolchain_manifest;
use super::support::{
    sandbox_module_hash, validate_toolchain_artifact_relative_path, write_toolchain_manifest,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalSandboxToolchainLayout {
    pub root: PathBuf,
    pub manifest: PathBuf,
    pub bin: PathBuf,
    pub lib: PathBuf,
    pub share: PathBuf,
    pub include: PathBuf,
    pub etc: PathBuf,
}

impl LocalSandboxToolchainLayout {
    pub fn new(cache_root: impl Into<PathBuf>, toolchain_id: &SandboxToolchainId) -> Self {
        let root = cache_root
            .into()
            .join("toolchains")
            .join(toolchain_id.as_str());
        Self {
            manifest: root.join("manifest.sobj"),
            bin: root.join("bin"),
            lib: root.join("lib"),
            share: root.join("share"),
            include: root.join("include"),
            etc: root.join("etc"),
            root,
        }
    }

    pub fn command_module(&self, command_name: &str) -> PathBuf {
        self.bin.join(format!("{command_name}.wasm"))
    }

    pub fn artifact(&self, relative_path: &str) -> Result<PathBuf, SandboxToolchainError> {
        let relative_path = validate_toolchain_artifact_relative_path(relative_path)?;
        Ok(self.root.join(relative_path))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalSandboxToolchainStore {
    cache_root: PathBuf,
}

impl LocalSandboxToolchainStore {
    pub fn new(cache_root: impl Into<PathBuf>) -> Self {
        Self {
            cache_root: cache_root.into(),
        }
    }

    pub fn layout(&self, toolchain_id: &SandboxToolchainId) -> LocalSandboxToolchainLayout {
        LocalSandboxToolchainLayout::new(&self.cache_root, toolchain_id)
    }

    pub fn save_manifest(
        &self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<LocalSandboxToolchainLayout, SandboxToolchainError> {
        let layout = self.layout(&manifest.toolchain_id);
        fs::create_dir_all(&layout.bin).map_err(|source| SandboxToolchainError::Io {
            path: layout.bin.display().to_string(),
            detail: source.to_string(),
        })?;
        for dir in [&layout.lib, &layout.share, &layout.include, &layout.etc] {
            fs::create_dir_all(dir).map_err(|source| SandboxToolchainError::Io {
                path: dir.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        write_toolchain_manifest(&layout.manifest, manifest)?;
        Ok(layout)
    }

    pub fn load_manifest(
        &self,
        toolchain_id: &SandboxToolchainId,
    ) -> Result<(SandboxToolchainManifest, LocalSandboxToolchainLayout), SandboxToolchainError>
    {
        let layout = self.layout(toolchain_id);
        if !layout.manifest.exists() {
            return Err(SandboxToolchainError::NotFound {
                toolchain_id: toolchain_id.clone(),
            });
        }
        let bytes = fs::read(&layout.manifest).map_err(|source| SandboxToolchainError::Io {
            path: layout.manifest.display().to_string(),
            detail: source.to_string(),
        })?;
        let manifest = decode_toolchain_manifest(&bytes, &layout.manifest.display().to_string())?;
        Ok((manifest, layout))
    }

    pub fn put_command_module(
        &self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
    ) -> Result<LocalSandboxToolchainLayout, SandboxToolchainError> {
        self.put_command_module_with_capabilities(
            manifest,
            command_name,
            module_bytes,
            default_limits,
            Vec::new(),
        )
    }

    pub fn put_command_module_with_capabilities(
        &self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
        required_capabilities: Vec<String>,
    ) -> Result<LocalSandboxToolchainLayout, SandboxToolchainError> {
        self.put_command_module_with_capabilities_and_argv_prefix(
            manifest,
            command_name,
            module_bytes,
            default_limits,
            required_capabilities,
            Vec::new(),
            BTreeMap::new(),
        )
    }

    pub fn put_command_module_with_capabilities_and_argv_prefix(
        &self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
        required_capabilities: Vec<String>,
        argv_prefix: Vec<String>,
        default_env: BTreeMap<String, String>,
    ) -> Result<LocalSandboxToolchainLayout, SandboxToolchainError> {
        self.put_command_module_with_target_abi_capabilities_and_argv_prefix(
            manifest,
            command_name,
            module_bytes,
            default_limits,
            None,
            required_capabilities,
            argv_prefix,
            default_env,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_command_module_with_target_abi_capabilities_and_argv_prefix(
        &self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
        target_abi: Option<SandboxWasiAbi>,
        required_capabilities: Vec<String>,
        argv_prefix: Vec<String>,
        default_env: BTreeMap<String, String>,
    ) -> Result<LocalSandboxToolchainLayout, SandboxToolchainError> {
        let mut command = SandboxToolchainCommand::new(
            &manifest.toolchain_id,
            command_name,
            module_bytes,
            default_limits,
        )?;
        command.target_abi = target_abi;
        command.required_capabilities = required_capabilities;
        command.argv_prefix = argv_prefix;
        command.default_env = default_env;
        manifest.insert_command(command.clone())?;
        let layout = self.layout(&manifest.toolchain_id);
        fs::create_dir_all(&layout.bin).map_err(|source| SandboxToolchainError::Io {
            path: layout.bin.display().to_string(),
            detail: source.to_string(),
        })?;
        fs::write(layout.command_module(command_name), module_bytes).map_err(|source| {
            SandboxToolchainError::Io {
                path: layout.command_module(command_name).display().to_string(),
                detail: source.to_string(),
            }
        })?;
        let layout = self.save_manifest(manifest)?;
        Ok(layout)
    }

    pub fn put_artifact(
        &self,
        manifest: &mut SandboxToolchainManifest,
        relative_path: &str,
        bytes: &[u8],
    ) -> Result<LocalSandboxToolchainLayout, SandboxToolchainError> {
        let artifact = SandboxToolchainArtifact::new(&manifest.toolchain_id, relative_path, bytes)?;
        manifest.insert_artifact(artifact.clone())?;
        let layout = self.layout(&manifest.toolchain_id);
        let artifact_path = layout.artifact(&artifact.relative_path)?;
        if let Some(parent) = artifact_path.parent() {
            fs::create_dir_all(parent).map_err(|source| SandboxToolchainError::Io {
                path: parent.display().to_string(),
                detail: source.to_string(),
            })?;
        }
        fs::write(&artifact_path, bytes).map_err(|source| SandboxToolchainError::Io {
            path: artifact_path.display().to_string(),
            detail: source.to_string(),
        })?;
        self.save_manifest(manifest)
    }

    pub fn load_artifact(
        &self,
        manifest: &SandboxToolchainManifest,
        relative_path: &str,
    ) -> Result<Vec<u8>, SandboxToolchainError> {
        let artifact = manifest.artifact(relative_path)?;
        let layout = self.layout(&manifest.toolchain_id);
        let path = layout.artifact(&artifact.relative_path)?;
        let bytes = fs::read(&path).map_err(|source| SandboxToolchainError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        let actual_hash = sandbox_module_hash(&bytes);
        if actual_hash != artifact.content_hash {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "artifact `{}` hash mismatch: expected {}, actual {actual_hash}",
                    artifact.relative_path, artifact.content_hash
                ),
            });
        }
        Ok(bytes)
    }

    pub fn load_command_module(
        &self,
        manifest: &SandboxToolchainManifest,
        command_name: &str,
    ) -> Result<Vec<u8>, SandboxToolchainError> {
        let command = manifest.command(command_name)?;
        let layout = self.layout(&manifest.toolchain_id);
        let bytes = fs::read(layout.command_module(command_name)).map_err(|source| {
            SandboxToolchainError::Io {
                path: layout.command_module(command_name).display().to_string(),
                detail: source.to_string(),
            }
        })?;
        let actual_hash = sandbox_module_hash(&bytes);
        if actual_hash != command.content_hash {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "command `{command_name}` hash mismatch: expected {}, actual {actual_hash}",
                    command.content_hash
                ),
            });
        }
        Ok(bytes)
    }
}
