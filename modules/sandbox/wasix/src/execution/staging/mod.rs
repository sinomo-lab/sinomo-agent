use crate::internal::*;

mod toolchain_cache;
pub(super) use toolchain_cache::stage_toolchain_runtime_to_local;
#[cfg(feature = "wasix-runtime")]
pub(super) use toolchain_cache::stage_toolchain_runtime_to_virtual;

pub(super) fn load_local_toolchain_artifacts(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .artifacts
        .keys()
        .map(|relative_path| {
            store
                .load_artifact(manifest, relative_path)
                .map(|bytes| (relative_path.clone(), bytes))
        })
        .collect()
}

pub(super) fn load_local_toolchain_commands(
    store: &LocalSandboxToolchainStore,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .commands
        .keys()
        .map(|command_name| {
            store
                .load_command_module(manifest, command_name)
                .map(|bytes| (command_name.clone(), bytes))
        })
        .collect()
}

pub(super) fn load_sifs_toolchain_artifacts<T: DfsFrameTransport>(
    store: &mut SifsSandboxToolchainStore<T>,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    store.load_artifacts(manifest)
}

pub(super) fn load_sifs_toolchain_commands<T: DfsFrameTransport>(
    store: &mut SifsSandboxToolchainStore<T>,
    manifest: &SandboxToolchainManifest,
) -> Result<Vec<(String, Vec<u8>)>, SandboxToolchainError> {
    manifest
        .commands
        .keys()
        .map(|command_name| {
            store
                .load_command_module(manifest, command_name)
                .map(|bytes| (command_name.clone(), bytes))
        })
        .collect()
}

#[cfg(test)]
mod tests;
