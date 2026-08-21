use crate::*;
use sinomo_core::{EventEnvelope, node::NodeId};
use sinomo_dfs::DfsMessageTransport;
use std::path::{Component, Path, PathBuf};
use time::OffsetDateTime;

/// External effects used by clustered toolchain installation.
pub trait ClusteredSandboxToolchainHooks {
    type Error: From<SandboxToolchainError>;

    fn load_policy_config(&mut self) -> Result<SandboxClusterPolicyConfig, Self::Error>;
    fn append_event(&mut self, event: EventEnvelope) -> Result<(), Self::Error>;
    fn invalid(&mut self, detail: String) -> Self::Error;

    fn now(&mut self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// Owner-side policy and persistence workflow for clustered toolchains.
pub struct ClusteredSandboxToolchainRuntime<'a, H> {
    hooks: &'a mut H,
}

impl<'a, H> ClusteredSandboxToolchainRuntime<'a, H>
where
    H: ClusteredSandboxToolchainHooks,
{
    pub fn new(hooks: &'a mut H) -> Self {
        Self { hooks }
    }

    pub fn import_busybox<T>(
        &mut self,
        mut store: SifsSandboxToolchainStore<T>,
        request: SandboxImportBusyBoxToolchainRequest,
    ) -> Result<SandboxImportBusyBoxToolchainResponse, H::Error>
    where
        T: DfsMessageTransport,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.node_id().clone();
        let spec = SandboxBusyBoxToolchainSpec::new(
            request.version,
            request.provenance,
            self.hooks.now(),
        )?;
        let module_hash = sandbox_module_hash(&request.module_bytes);
        let candidate = spec.manifest(&request.module_bytes)?;
        self.validate_update_policy(
            &config.policy,
            &node_id,
            &candidate,
            "sandbox-toolchain-import-busybox",
        )?;
        let manifest =
            install_busybox_toolchain_to_sifs_store(&mut store, &spec, &request.module_bytes)?;
        let origin_node_id = store.into_inner().node_id().clone();
        self.hooks.append_event(sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            module_hash.clone(),
        ))?;
        Ok(SandboxImportBusyBoxToolchainResponse {
            manifest,
            module_hash,
        })
    }

    pub fn add_command<T>(
        &mut self,
        mut store: SifsSandboxToolchainStore<T>,
        request: SandboxAddToolchainCommandRequest,
    ) -> Result<SandboxAddToolchainCommandResponse, H::Error>
    where
        T: DfsMessageTransport,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.node_id().clone();
        let mut manifest = match store.load_manifest(&request.toolchain_id) {
            Ok(manifest) => manifest,
            Err(SandboxToolchainError::NotFound { .. }) => SandboxToolchainManifest::new(
                request.toolchain_id.clone(),
                request.version,
                request.target_abi.clone(),
                request.provenance,
                self.hooks.now(),
            )?,
            Err(error) => return Err(error.into()),
        };
        let module_hash = sandbox_module_hash(&request.module_bytes);
        let mut candidate = manifest.clone();
        let mut command = SandboxToolchainCommand::new(
            &candidate.toolchain_id,
            &request.command_name,
            &request.module_bytes,
            request.default_limits.clone(),
        )?;
        command.target_abi = Some(request.target_abi.clone());
        command.required_capabilities = request.required_capabilities.clone();
        candidate.insert_command(command)?;
        self.validate_update_policy(
            &config.policy,
            &node_id,
            &candidate,
            "sandbox-toolchain-add-command",
        )?;
        store.put_command_module_with_target_abi_capabilities_and_argv_prefix(
            &mut manifest,
            &request.command_name,
            &request.module_bytes,
            request.default_limits,
            Some(request.target_abi),
            request.required_capabilities,
            Vec::new(),
            std::collections::BTreeMap::new(),
        )?;
        let origin_node_id = store.into_inner().node_id().clone();
        self.hooks.append_event(sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            module_hash.clone(),
        ))?;
        Ok(SandboxAddToolchainCommandResponse {
            manifest,
            module_hash,
        })
    }

    pub fn import_package<T>(
        &mut self,
        mut store: SifsSandboxToolchainStore<T>,
        request: SandboxImportToolchainPackageRequest,
    ) -> Result<SandboxImportToolchainPackageResponse, H::Error>
    where
        T: DfsMessageTransport,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.node_id().clone();
        let mut manifest = match store.load_manifest(&request.toolchain_id) {
            Ok(manifest) => manifest,
            Err(SandboxToolchainError::NotFound { .. }) => SandboxToolchainManifest::new(
                request.toolchain_id.clone(),
                request.version,
                request.target_abi.clone(),
                request.provenance,
                self.hooks.now(),
            )?,
            Err(error) => return Err(error.into()),
        };
        for command in &request.commands {
            if manifest.commands.contains_key(&command.name) {
                return Err(SandboxToolchainError::DuplicateCommand {
                    name: command.name.clone(),
                }
                .into());
            }
        }
        for artifact in &request.artifacts {
            if manifest.artifacts.contains_key(&artifact.relative_path) {
                return Err(SandboxToolchainError::DuplicateArtifact {
                    path: artifact.relative_path.clone(),
                }
                .into());
            }
        }
        let mut candidate = manifest.clone();
        for command in &request.commands {
            let mut candidate_command = SandboxToolchainCommand::new(
                &candidate.toolchain_id,
                &command.name,
                &command.module_bytes,
                command.default_limits.clone(),
            )?;
            candidate_command.target_abi = Some(
                command
                    .target_abi
                    .clone()
                    .unwrap_or_else(|| request.target_abi.clone()),
            );
            candidate_command.required_capabilities = command.required_capabilities.clone();
            candidate_command.argv_prefix = command.argv_prefix.clone();
            candidate_command.exec_name = command.exec_name.clone();
            candidate_command.default_env = command.default_env.clone();
            candidate.insert_command(candidate_command)?;
        }
        for artifact in &request.artifacts {
            let candidate_artifact = SandboxToolchainArtifact::new(
                &candidate.toolchain_id,
                &artifact.relative_path,
                &artifact.bytes,
            )?;
            candidate.insert_artifact(candidate_artifact)?;
        }
        let artifact_bundle_bytes = if request.artifacts.is_empty() {
            None
        } else {
            Some(pack_toolchain_artifact_bundle(
                &request
                    .artifacts
                    .iter()
                    .map(|artifact| (artifact.relative_path.clone(), artifact.bytes.clone()))
                    .collect::<Vec<_>>(),
            )?)
        };
        let artifact_bundle = if let Some(bytes) = &artifact_bundle_bytes {
            let bundle_hash = sandbox_module_hash(bytes);
            let bundle_name = format!(
                "package-{}",
                bundle_hash
                    .strip_prefix("blake3:")
                    .unwrap_or(bundle_hash.as_str())
            );
            let mut bundled_artifacts = Vec::new();
            for artifact in &request.artifacts {
                bundled_artifacts.push(candidate.artifact(&artifact.relative_path)?.clone());
            }
            let bundle = SandboxToolchainArtifactBundle::new(
                &candidate.toolchain_id,
                &bundle_name,
                bytes,
                bundled_artifacts,
            )?;
            candidate.insert_artifact_bundle(bundle.clone())?;
            Some(bundle)
        } else {
            None
        };
        self.validate_update_policy(
            &config.policy,
            &node_id,
            &candidate,
            "sandbox-toolchain-import-package",
        )?;
        let mut package_hasher = SandboxModuleHasher::new();
        for command in &request.commands {
            package_hasher.update(command.name.as_bytes());
            if let Some(target_abi) = &command.target_abi {
                package_hasher.update(format!("{target_abi:?}").as_bytes());
            }
            for arg in &command.argv_prefix {
                package_hasher.update(arg.as_bytes());
            }
            if let Some(exec_name) = &command.exec_name {
                package_hasher.update(exec_name.as_bytes());
            }
            for (key, value) in &command.default_env {
                package_hasher.update(key.as_bytes());
                package_hasher.update(value.as_bytes());
            }
            package_hasher.update(&command.module_bytes);
            let manifest_command = candidate.command(&command.name)?;
            store.write_command_module_file(manifest_command, &command.module_bytes)?;
        }
        for artifact in &request.artifacts {
            package_hasher.update(artifact.relative_path.as_bytes());
            package_hasher.update(&artifact.bytes);
        }
        if let (Some(bundle), Some(bytes)) = (&artifact_bundle, &artifact_bundle_bytes) {
            store.write_artifact_bundle_file(bundle, bytes)?;
        }
        store.save_manifest(&candidate)?;
        manifest = candidate;
        let package_hash = package_hasher.finalize();
        let origin_node_id = store.into_inner().node_id().clone();
        self.hooks.append_event(sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            package_hash.clone(),
        ))?;
        Ok(SandboxImportToolchainPackageResponse {
            manifest,
            package_hash,
        })
    }

    pub fn add_artifact<T>(
        &mut self,
        mut store: SifsSandboxToolchainStore<T>,
        request: SandboxAddToolchainArtifactRequest,
    ) -> Result<SandboxAddToolchainArtifactResponse, H::Error>
    where
        T: DfsMessageTransport,
    {
        let config = self.hooks.load_policy_config()?;
        let node_id = store.node_id().clone();
        let mut manifest = match store.load_manifest(&request.toolchain_id) {
            Ok(manifest) => manifest,
            Err(SandboxToolchainError::NotFound { .. }) => SandboxToolchainManifest::new(
                request.toolchain_id.clone(),
                request.version,
                SandboxWasiAbi::Wasix,
                request.provenance,
                self.hooks.now(),
            )?,
            Err(error) => return Err(error.into()),
        };
        let artifact_hash = sandbox_module_hash(&request.bytes);
        let mut candidate = manifest.clone();
        let candidate_artifact = SandboxToolchainArtifact::new(
            &candidate.toolchain_id,
            &request.relative_path,
            &request.bytes,
        )?;
        candidate.insert_artifact(candidate_artifact)?;
        self.validate_update_policy(
            &config.policy,
            &node_id,
            &candidate,
            "sandbox-toolchain-add-artifact",
        )?;
        store.put_artifact(&mut manifest, &request.relative_path, &request.bytes)?;
        let origin_node_id = store.into_inner().node_id().clone();
        self.hooks.append_event(sandbox_toolchain_updated_event(
            origin_node_id,
            &manifest,
            artifact_hash.clone(),
        ))?;
        Ok(SandboxAddToolchainArtifactResponse {
            manifest,
            artifact_hash,
        })
    }

    fn validate_update_policy(
        &mut self,
        policy: &SandboxPolicy,
        node_id: &NodeId,
        manifest: &SandboxToolchainManifest,
        phase: &str,
    ) -> Result<(), H::Error> {
        if let Err(denial) = policy.validate_toolchain_update(manifest) {
            self.hooks.append_event(sandbox_policy_denied_event(
                node_id.clone(),
                None,
                phase,
                &denial,
            ))?;
            return Err(self.hooks.invalid(denial.to_string()));
        }
        Ok(())
    }
}

pub fn load_sandbox_toolchain_package_from_path(
    request: SandboxImportToolchainPackageFromPathRequest,
) -> Result<SandboxImportToolchainPackageRequest, SandboxToolchainError> {
    let package_path = PathBuf::from(&request.package_path);
    let package_dir = package_path
        .parent()
        .ok_or_else(|| SandboxToolchainError::InvalidManifest {
            detail: format!("package path has no parent: {}", package_path.display()),
        })?
        .to_path_buf();
    let spec_bytes = std::fs::read(&package_path).map_err(|source| SandboxToolchainError::Io {
        path: package_path.display().to_string(),
        detail: source.to_string(),
    })?;
    let spec: SandboxToolchainPackageSpec =
        serde_json::from_slice(&spec_bytes).map_err(|source| {
            SandboxToolchainError::InvalidManifest {
                detail: format!(
                    "failed to parse package manifest {}: {source}",
                    package_path.display()
                ),
            }
        })?;
    let mut commands = Vec::new();
    for command in &spec.commands {
        let module_path = sandbox_package_file_path(&package_dir, &command.module)?;
        let module_bytes =
            std::fs::read(&module_path).map_err(|source| SandboxToolchainError::Io {
                path: module_path.display().to_string(),
                detail: source.to_string(),
            })?;
        verify_sandbox_package_hash(&module_path, &module_bytes, &command.content_hash)?;
        commands.push(SandboxImportToolchainPackageCommand {
            name: command.name.clone(),
            module_bytes,
            target_abi: command.target_abi.clone(),
            argv_prefix: command.argv_prefix.clone(),
            exec_name: command.exec_name.clone(),
            default_env: command.default_env.clone(),
            required_capabilities: command.required_capabilities.clone(),
            default_limits: command.default_limits.clone(),
        });
    }
    let mut artifacts = Vec::new();
    for artifact in &spec.artifacts {
        let artifact_path = sandbox_package_file_path(&package_dir, &artifact.file)?;
        let bytes = std::fs::read(&artifact_path).map_err(|source| SandboxToolchainError::Io {
            path: artifact_path.display().to_string(),
            detail: source.to_string(),
        })?;
        verify_sandbox_package_hash(&artifact_path, &bytes, &artifact.content_hash)?;
        artifacts.push(SandboxImportToolchainPackageArtifact {
            relative_path: artifact.relative_path.clone(),
            bytes,
        });
    }
    Ok(SandboxImportToolchainPackageRequest {
        toolchain_id: request.toolchain_id.unwrap_or(spec.toolchain_id),
        version: spec.version,
        target_abi: spec.target_abi,
        provenance: spec.provenance,
        commands,
        artifacts,
    })
}

fn sandbox_package_file_path(
    package_dir: &Path,
    relative_path: &str,
) -> Result<PathBuf, SandboxToolchainError> {
    let path = Path::new(relative_path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(SandboxToolchainError::InvalidManifest {
            detail: format!("invalid package-relative path `{relative_path}`"),
        });
    }
    Ok(package_dir.join(path))
}

fn verify_sandbox_package_hash(
    path: &Path,
    bytes: &[u8],
    expected: &str,
) -> Result<(), SandboxToolchainError> {
    let actual = sandbox_module_hash(bytes);
    if actual != expected {
        return Err(SandboxToolchainError::InvalidManifest {
            detail: format!(
                "package file {} hash mismatch: expected {expected}, actual {actual}",
                path.display()
            ),
        });
    }
    Ok(())
}
