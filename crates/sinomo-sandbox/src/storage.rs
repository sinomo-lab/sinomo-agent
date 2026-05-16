use crate::SANDBOX_STORAGE_MODULE;
use sinomo_storage::{StorageContractError, StorageLayout, StorageRing};

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
    Ok(())
}
