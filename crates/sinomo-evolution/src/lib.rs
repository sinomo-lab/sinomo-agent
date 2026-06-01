//! Autonomous self-evolution planning skeleton.

mod engine;
pub mod persistence;
pub mod tools;
mod types;

pub use engine::*;
pub use persistence::*;
pub use tools::*;
pub use types::*;

pub type EvolutionJobId = sinomo_runtime::core::Id;
pub type TaskId = sinomo_runtime::core::Id;
pub type WorkspaceId = sinomo_runtime::core::Id;

pub fn register_storage_paths(
    layout: &mut sinomo_runtime::storage::StorageLayout,
) -> Result<(), sinomo_runtime::storage::StorageContractError> {
    layout.register_escape_path(
        "sinomo-evolution",
        sinomo_runtime::storage::StorageRing::R2,
        "evolution",
        "self-evolution jobs, campaigns, logs, and candidate workspaces",
    )
}
