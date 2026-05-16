use crate::internal::*;

use super::serde::{decode_toolchain_manifest, encode_toolchain_manifest};
use super::support::{
    sandbox_module_hash, sandbox_toolchain_root_from_path, unpack_toolchain_artifact_bundle,
};

#[derive(Debug)]
pub struct SifsSandboxToolchainStore<T: NetworkTransport> {
    dfs: NetworkedDfsService<T>,
    replica_candidates: Vec<NodeId>,
    ensured_dirs: BTreeSet<String>,
}

impl<T: NetworkTransport> SifsSandboxToolchainStore<T> {
    pub fn new(dfs: NetworkedDfsService<T>) -> Self {
        let replica_candidates = vec![dfs.node_id().clone()];
        Self {
            dfs,
            replica_candidates,
            ensured_dirs: BTreeSet::new(),
        }
    }

    pub fn into_inner(self) -> NetworkedDfsService<T> {
        self.dfs
    }

    pub fn with_replica_candidates(mut self, replica_candidates: Vec<NodeId>) -> Self {
        let mut replica_candidates = replica_candidates;
        if !replica_candidates.contains(self.dfs.node_id()) {
            replica_candidates.insert(0, self.dfs.node_id().clone());
        }
        replica_candidates.sort();
        replica_candidates.dedup();
        self.replica_candidates = replica_candidates;
        self
    }

    pub fn ensure_toolchain_dirs(
        &mut self,
        toolchain_id: &SandboxToolchainId,
    ) -> Result<(), SandboxToolchainError> {
        for path in [
            sandbox_toolchain_root(toolchain_id),
            format!("{}/bin", sandbox_toolchain_root(toolchain_id)),
            format!("{}/lib", sandbox_toolchain_root(toolchain_id)),
            format!("{}/share", sandbox_toolchain_root(toolchain_id)),
            format!("{}/include", sandbox_toolchain_root(toolchain_id)),
            format!("{}/etc", sandbox_toolchain_root(toolchain_id)),
        ] {
            self.ensure_dir(path)?;
        }
        Ok(())
    }

    pub fn save_manifest(
        &mut self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<(), SandboxToolchainError> {
        self.ensure_toolchain_dirs(&manifest.toolchain_id)?;
        let path = sandbox_toolchain_manifest_path(&manifest.toolchain_id);
        let bytes = encode_toolchain_manifest(manifest, &path)?;
        self.dfs
            .posix_write_file(&path, &bytes, self.write_options())
            .map(|_| ())
            .map_err(|source| sandbox_toolchain_error_from_dfs(path, source))
    }

    pub fn load_manifest(
        &self,
        toolchain_id: &SandboxToolchainId,
    ) -> Result<SandboxToolchainManifest, SandboxToolchainError> {
        let path = sandbox_toolchain_manifest_path(toolchain_id);
        let bytes = self
            .dfs
            .posix_read_file(&path)
            .map_err(|source| match source {
                DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. } => {
                    SandboxToolchainError::NotFound {
                        toolchain_id: toolchain_id.clone(),
                    }
                }
                other => sandbox_toolchain_error_from_dfs(path.clone(), other),
            })?;
        decode_toolchain_manifest(&bytes, &path)
    }

    pub fn put_command_module(
        &mut self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
    ) -> Result<(), SandboxToolchainError> {
        self.put_command_module_with_capabilities(
            manifest,
            command_name,
            module_bytes,
            default_limits,
            Vec::new(),
        )
    }

    pub fn put_command_module_with_capabilities(
        &mut self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
        required_capabilities: Vec<String>,
    ) -> Result<(), SandboxToolchainError> {
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
        &mut self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
        required_capabilities: Vec<String>,
        argv_prefix: Vec<String>,
        default_env: BTreeMap<String, String>,
    ) -> Result<(), SandboxToolchainError> {
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
        &mut self,
        manifest: &mut SandboxToolchainManifest,
        command_name: &str,
        module_bytes: &[u8],
        default_limits: SandboxResourceLimits,
        target_abi: Option<SandboxWasiAbi>,
        required_capabilities: Vec<String>,
        argv_prefix: Vec<String>,
        default_env: BTreeMap<String, String>,
    ) -> Result<(), SandboxToolchainError> {
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
        self.write_command_module_file(&command, module_bytes)?;
        self.save_manifest(manifest)
    }

    pub fn put_artifact(
        &mut self,
        manifest: &mut SandboxToolchainManifest,
        relative_path: &str,
        bytes: &[u8],
    ) -> Result<(), SandboxToolchainError> {
        let artifact = SandboxToolchainArtifact::new(&manifest.toolchain_id, relative_path, bytes)?;
        manifest.insert_artifact(artifact.clone())?;
        self.write_artifact_file(&artifact, bytes)?;
        self.save_manifest(manifest)
    }

    pub fn write_command_module_file(
        &mut self,
        command: &SandboxToolchainCommand,
        module_bytes: &[u8],
    ) -> Result<(), SandboxToolchainError> {
        if sandbox_module_hash(module_bytes) != command.content_hash {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "command `{}` content hash does not match staged module bytes",
                    command.name
                ),
            });
        }
        self.ensure_dir(
            command
                .module_path
                .rsplit_once('/')
                .map(|(parent, _)| parent.to_string())
                .unwrap_or_else(|| sandbox_toolchain_root_from_path(&command.module_path)),
        )?;
        self.dfs
            .posix_write_file(&command.module_path, module_bytes, self.write_options())
            .map(|_| ())
            .map_err(|source| sandbox_toolchain_error_from_dfs(command.module_path.clone(), source))
    }

    pub fn write_artifact_file(
        &mut self,
        artifact: &SandboxToolchainArtifact,
        bytes: &[u8],
    ) -> Result<(), SandboxToolchainError> {
        if sandbox_module_hash(bytes) != artifact.content_hash {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "artifact `{}` content hash does not match staged artifact bytes",
                    artifact.relative_path
                ),
            });
        }
        if let Some(parent) = artifact
            .sifs_path
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
        {
            self.ensure_dir(parent)?;
        }
        self.dfs
            .posix_write_file(&artifact.sifs_path, bytes, self.write_options())
            .map(|_| ())
            .map_err(|source| sandbox_toolchain_error_from_dfs(artifact.sifs_path.clone(), source))
    }

    pub fn write_artifact_bundle_file(
        &mut self,
        bundle: &SandboxToolchainArtifactBundle,
        bytes: &[u8],
    ) -> Result<(), SandboxToolchainError> {
        if sandbox_module_hash(bytes) != bundle.content_hash {
            return Err(SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "artifact bundle `{}` content hash does not match staged bundle bytes",
                    bundle.bundle_path
                ),
            });
        }
        if let Some(parent) = bundle
            .bundle_path
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
        {
            self.ensure_dir(parent)?;
        }
        self.dfs
            .posix_write_file(&bundle.bundle_path, bytes, self.write_options())
            .map(|_| ())
            .map_err(|source| sandbox_toolchain_error_from_dfs(bundle.bundle_path.clone(), source))
    }

    pub fn load_artifact(
        &self,
        manifest: &SandboxToolchainManifest,
        relative_path: &str,
    ) -> Result<Vec<u8>, SandboxToolchainError> {
        let artifact = manifest.artifact(relative_path)?;
        let bytes = self
            .dfs
            .posix_read_file(&artifact.sifs_path)
            .map_err(|source| {
                sandbox_toolchain_error_from_dfs(artifact.sifs_path.clone(), source)
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

    pub fn load_artifacts(
        &self,
        manifest: &SandboxToolchainManifest,
    ) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
        let mut loaded = BTreeMap::new();
        for bundle in &manifest.artifact_bundles {
            let bundle_bytes = self
                .dfs
                .posix_read_file(&bundle.bundle_path)
                .map_err(|source| {
                    sandbox_toolchain_error_from_dfs(bundle.bundle_path.clone(), source)
                })?;
            let actual_hash = sandbox_module_hash(&bundle_bytes);
            if actual_hash != bundle.content_hash {
                return Err(SandboxToolchainError::InvalidManifest {
                    detail: format!(
                        "artifact bundle `{}` hash mismatch: expected {}, actual {actual_hash}",
                        bundle.bundle_path, bundle.content_hash
                    ),
                });
            }
            for (relative_path, bytes) in unpack_toolchain_artifact_bundle(&bundle_bytes)? {
                let artifact = bundle
                    .artifacts
                    .iter()
                    .find(|artifact| artifact.relative_path == relative_path)
                    .ok_or_else(|| SandboxToolchainError::InvalidManifest {
                        detail: format!(
                            "artifact bundle `{}` contains undeclared path `{relative_path}`",
                            bundle.bundle_path
                        ),
                    })?;
                let actual_hash = sandbox_module_hash(&bytes);
                if actual_hash != artifact.content_hash {
                    return Err(SandboxToolchainError::InvalidManifest {
                        detail: format!(
                            "artifact `{relative_path}` hash mismatch in bundle `{}`: expected {}, actual {actual_hash}",
                            bundle.bundle_path, artifact.content_hash
                        ),
                    });
                }
                loaded.insert(relative_path, bytes);
            }
        }
        for relative_path in manifest.artifacts.keys() {
            if !loaded.contains_key(relative_path) {
                loaded.insert(
                    relative_path.clone(),
                    self.load_artifact(manifest, relative_path)?,
                );
            }
        }
        Ok(loaded.into_iter().collect())
    }

    pub fn load_command_module(
        &self,
        manifest: &SandboxToolchainManifest,
        command_name: &str,
    ) -> Result<Vec<u8>, SandboxToolchainError> {
        let command = manifest.command(command_name)?;
        let bytes = self
            .dfs
            .posix_read_file(&command.module_path)
            .map_err(|source| {
                sandbox_toolchain_error_from_dfs(command.module_path.clone(), source)
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

    pub(crate) fn write_options(&self) -> DfsWriteOptions {
        DfsWriteOptions::replicated_eventual(self.replica_candidates.clone())
    }

    fn ensure_dir(&mut self, path: String) -> Result<(), SandboxToolchainError> {
        if self.ensured_dirs.contains(&path) {
            return Ok(());
        }
        self.dfs
            .posix_mkdir_all(&path)
            .map_err(|source| sandbox_toolchain_error_from_dfs(path.clone(), source))?;
        self.ensured_dirs.insert(path);
        Ok(())
    }
}
