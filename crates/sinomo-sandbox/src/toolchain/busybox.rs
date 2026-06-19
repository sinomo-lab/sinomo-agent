use crate::internal::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxBusyBoxToolchainSpec {
    pub toolchain_id: SandboxToolchainId,
    pub version: String,
    pub provenance: String,
    pub commands: Vec<String>,
    pub default_limits: SandboxResourceLimits,
    pub created_at: OffsetDateTime,
}

impl SandboxBusyBoxToolchainSpec {
    pub fn new(
        version: impl Into<String>,
        provenance: impl Into<String>,
        created_at: OffsetDateTime,
    ) -> Result<Self, SandboxToolchainError> {
        Ok(Self {
            toolchain_id: SandboxToolchainId::from_legacy_named(BUSYBOX_TOOLCHAIN_LEGACY_ID)
                .map_err(|source| SandboxToolchainError::InvalidManifest {
                    detail: format!("invalid static BusyBox toolchain id: {source}"),
                })?,
            version: version.into(),
            provenance: provenance.into(),
            commands: BUSYBOX_TOOLCHAIN_COMMANDS
                .iter()
                .map(|command| command.to_string())
                .collect(),
            default_limits: SandboxResourceLimits::default(),
            created_at,
        })
    }

    pub fn manifest(
        &self,
        module_bytes: &[u8],
    ) -> Result<SandboxToolchainManifest, SandboxToolchainError> {
        let mut manifest = SandboxToolchainManifest::new(
            self.toolchain_id.clone(),
            self.version.clone(),
            default_sandbox_wasi_abi(),
            self.provenance.clone(),
            self.created_at,
        )?;
        for command in &self.commands {
            manifest.insert_command(SandboxToolchainCommand::new(
                &self.toolchain_id,
                command,
                module_bytes,
                self.default_limits.clone(),
            )?)?;
        }
        Ok(manifest)
    }
}

pub fn install_busybox_toolchain_to_local_store(
    store: &LocalSandboxToolchainStore,
    spec: &SandboxBusyBoxToolchainSpec,
    module_bytes: &[u8],
) -> Result<SandboxToolchainManifest, SandboxToolchainError> {
    let manifest = spec.manifest(module_bytes)?;
    let layout = store.layout(&manifest.toolchain_id);
    fs::create_dir_all(&layout.bin).map_err(|source| SandboxToolchainError::Io {
        path: layout.bin.display().to_string(),
        detail: source.to_string(),
    })?;
    for command in manifest.commands.values() {
        fs::write(layout.command_module(&command.name), module_bytes).map_err(|source| {
            SandboxToolchainError::Io {
                path: layout.command_module(&command.name).display().to_string(),
                detail: source.to_string(),
            }
        })?;
    }
    store.save_manifest(&manifest)?;
    Ok(manifest)
}

pub fn install_busybox_toolchain_to_sifs_store<T: DfsMessageTransport>(
    store: &mut SifsSandboxToolchainStore<T>,
    spec: &SandboxBusyBoxToolchainSpec,
    module_bytes: &[u8],
) -> Result<SandboxToolchainManifest, SandboxToolchainError> {
    let manifest = spec.manifest(module_bytes)?;
    store.ensure_toolchain_dirs(&manifest.toolchain_id)?;
    for command in manifest.commands.values() {
        store.write_command_module_file(command, module_bytes)?;
    }
    store.save_manifest(&manifest)?;
    Ok(manifest)
}
