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
            "sandbox/agent-tools",
            "node-local sandbox instances and command evidence for agent tool execution",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_tool_sandbox_runtime_is_registered() {
        let mut layout = StorageLayout::new();
        register_storage_paths(&mut layout).expect("register sandbox storage");

        let owner = layout
            .owner_for("sandbox/agent-tools/instances/example/manifest.sobj")
            .expect("agent tool sandbox owner");
        let module = match owner {
            sinomo_core::storage::StoragePathOwner::File(file) => file.module.as_str(),
            sinomo_core::storage::StoragePathOwner::Escape(escape) => escape.module.as_str(),
        };
        assert_eq!(module, SANDBOX_STORAGE_MODULE);
    }
}
