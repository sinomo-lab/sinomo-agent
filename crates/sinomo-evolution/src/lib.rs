//! Autonomous self-evolution planning skeleton.

mod engine;
pub mod persistence;
pub mod tools;

pub use engine::*;
pub use persistence::*;
pub use tools::*;

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    layout.register_escape_path(
        "sinomo-evolution",
        sinomo_storage::StorageRing::R2,
        "evolution",
        "self-evolution jobs, campaigns, logs, and candidate workspaces",
    )
}
