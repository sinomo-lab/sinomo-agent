//! Owned incremental storage conversion, outside the current DFS runtime.
pub mod content;
pub mod namespace_counts;
pub use content::ContentConversionInspection;
pub mod authority;
pub use authority::{staged_import_component_factory, storage_preparation_provider};
mod legacy_prepare_codec;
mod legacy_wire;
mod local_state;
mod metadata;

/// Retained source records belong to the converter, never the current DFS reader.
pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::StorageRing;
    for (path, reason) in [
        ("dfs/migration-owner", "single migration owner's bounded unit/job references and discovery receipts"),
        ("dfs/.content-conversion-v2", "single-owner content publication checkpoint and private current targets"),
        ("dfs/.content-retained", "immutable original content generations retained by migration"),
    ] {
        layout.register_escape_path("sinomo-dfs-migration", StorageRing::R1, path, reason)?;
    }
    layout.register_binary(
        "sinomo-dfs-migration",
        StorageRing::R1,
        "dfs/local-state-converted-v8",
    )?;
    layout.register_binary(
        "sinomo-dfs-migration",
        StorageRing::R1,
        "dfs/content/legacy-readers.lock",
    )?;
    layout.register_escape_path(
        "sinomo-dfs-migration",
        StorageRing::R1,
        "dfs/content/blob-packs",
        "retained source packed blobs decoded only during content conversion",
    )?;
    layout.register_escape_path(
        "sinomo-dfs-migration",
        StorageRing::R1,
        "dfs/content/content-reservations",
        "retained source single-version reservations converted to current causal roots",
    )
}
