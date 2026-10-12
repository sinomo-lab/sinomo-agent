//! Owned incremental storage conversion, outside the current DFS runtime.
pub mod content;
pub use content::{ContentConversionInspection, ContentConverter};
pub mod authority;
pub use authority::content_work::MetadataImportedRetainedContent;
pub use authority::{
    staged_import_component_factory, standalone_storage_preparation_provider,
    storage_preparation_provider,
};
mod legacy_prepare_codec;
mod legacy_wire;
mod local_state;
mod metadata;
pub use metadata::{
    MetadataCapturedKvEntry, MetadataConverter, MetadataImportAuthority,
    MetadataImportAuthorityKind, MetadataImportCertificate, MetadataImportCertificateFacts,
    MetadataImportPlan, MetadataImportPlanFacts, MetadataImportProposal,
    MetadataImportProposalFacts, MetadataSourceContentLocator, MetadataSourceContentWorkScope,
    MetadataSourceCut, NativeMetadataImportCertificate, NativeMetadataImportPlan,
    NativeMetadataImportProposal, StandaloneMetadataImportCertificate,
    StandaloneMetadataImportPlan, StandaloneMetadataImportProposal, StandaloneSourceOwnerEvidence,
    discover_metadata_source,
};
pub use metadata::{
    MetadataSourceObjectWitness, MetadataSourceVersionEvidence, lookup_metadata_source_version,
    lookup_owned_metadata_source_version, owned_metadata_source_stage,
};

/// Retained source records belong to the converter, never the current DFS reader.
pub fn register_storage_paths(
    layout: &mut sinomo_core::storage::StorageLayout,
) -> Result<(), sinomo_core::storage::StorageContractError> {
    use sinomo_core::storage::StorageRing;
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
