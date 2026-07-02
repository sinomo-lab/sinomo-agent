use crate::SANDBOX_STORAGE_MODULE;
use sinomo_core::storage::{StorageContractError, StorageLayout, StorageRing};

pub fn register_storage_paths(layout: &mut StorageLayout) -> Result<(), StorageContractError> {
    for (relative_path, reason) in [
        (
            "sandbox/cache/toolchains",
            "node-local cache for SIFS-replicated sandbox toolchain artifacts",
        ),
        (
            "sandbox/cache/modules",
            "node-local cache for compiled sandbox WASM modules",
        ),
        (
            "sandbox/module-artifacts",
            "node-local registered WASM and component artifacts for module replay",
        ),
        (
            "sandbox/runtime",
            "node-local sandbox runtime supervision state",
        ),
        (
            "sandbox/tmp",
            "node-local temporary scratch for active sandbox command execution",
        ),
    ] {
        layout.register_escape_path(
            SANDBOX_STORAGE_MODULE,
            StorageRing::R1,
            relative_path,
            reason,
        )?;
    }
    layout.register_sinomo_object(
        SANDBOX_STORAGE_MODULE,
        StorageRing::R1,
        "sandbox/module-wasm-artifacts.sobj",
        sinomo_core::storage::ObjectSchema::new(
            "SandboxWasmModuleArtifactRegistry",
            sinomo_core::storage::STORAGE_CONTRACT_OBJECT_VERSION,
        )
        .field("artifacts", "Vec<SandboxWasmModuleArtifact>"),
    )?;
    Ok(())
}
