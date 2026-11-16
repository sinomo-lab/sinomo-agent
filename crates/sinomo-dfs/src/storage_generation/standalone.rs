//! Current representation activation for the existing explicit local source
//! owner. Historical decoding/certificate policy stays in its migration owner.
use super::*;
#[cfg(feature = "storage-conversion")]
use std::fs::File;
#[cfg(feature = "storage-conversion")]
use std::sync::Arc;

/// Non-serializable adapter over an already-earned explicit source capability.
/// The migration implementation retains its actual data owner/source fence and
/// revalidates its original certificate and ancestry. A readiness callback or
/// caller JSON cannot supply these resources. This grants no native authority.
#[cfg(feature = "storage-conversion")]
pub trait DfsStandaloneStorageActivationWitness: Send + Sync {
    fn data_owner(&self) -> &Arc<crate::runtime_host::DfsDataOwnerLock>;
    fn source_fence(&self) -> &File;
    fn actual_source_root(&self) -> &Path;
    fn installed_target_root(&self) -> &Path;
    fn source_binding(&self) -> &str;
    fn file_system_ids(&self) -> &[DfsFileSystemId];
    fn revalidate_original_certificate(
        &self,
        image: &Path,
        identity: &DfsMetadataGenerationIdentity,
        seal: &DfsMetadataGenerationSeal,
    ) -> Result<()>;
}

pub(super) fn standalone_receipt(
    identity: &DfsMetadataGenerationIdentity,
    seal: &DfsMetadataGenerationSeal,
) -> Result<String> {
    if !matches!(
        identity.origin,
        DfsMetadataGenerationOrigin::StandaloneOwned { .. }
    ) || !seal.prefix_proof.is_well_formed()
        || seal.file_fingerprint.is_empty()
        || seal.state_fingerprint.is_empty()
    {
        return Err(invalid(
            "standalone receipt has no exact current generation seal",
        ));
    }
    let bytes = serde_json::to_vec(&("standalone-current-generation-v1", identity, seal))
        .map_err(invalid)?;
    Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}

/// Preserve explicit standalone eligibility. Missing native authority never
/// selects this path: the caller supplies the existing live source witness.
#[cfg(feature = "storage-conversion")]
pub fn activate_standalone_metadata_storage_generation(
    context: &sinomo_module::ComponentContext,
    root: &Path,
    identity: &DfsMetadataGenerationIdentity,
    seal: &DfsMetadataGenerationSeal,
    witness: &dyn DfsStandaloneStorageActivationWitness,
) -> Result<()> {
    if context.host_role() == sinomo_module::ComponentHostRole::Shadow
        || context.cluster_access().is_some()
        || crate::runtime_host::registered_component_cluster(context)
            .map_err(invalid)?
            .is_some()
    {
        return Err(invalid(
            "standalone activation requires the explicitly selected non-native owner",
        ));
    }
    identity.validate()?;
    let DfsMetadataGenerationOrigin::StandaloneOwned {
        owner_node_id,
        source_binding,
    } = &identity.origin
    else {
        return Err(invalid(
            "standalone witness cannot activate a native generation",
        ));
    };
    let actual_owner =
        crate::runtime_host::retain_component_data_owner(context).map_err(invalid)?;
    if !Arc::ptr_eq(&actual_owner, witness.data_owner())
        || source_binding != witness.source_binding()
    {
        return Err(invalid(
            "standalone source witness differs from the actual component owner",
        ));
    }
    let data_root = context.data_dir_root().canonicalize().map_err(invalid)?;
    let actual_root = root.canonicalize().map_err(invalid)?;
    let source_root = witness
        .actual_source_root()
        .canonicalize()
        .map_err(invalid)?;
    if actual_root
        != witness
            .installed_target_root()
            .canonicalize()
            .map_err(invalid)?
        || !actual_root.starts_with(&data_root)
        || !source_root.starts_with(&data_root)
        || actual_root == source_root
    {
        return Err(invalid(
            "standalone activation changes its actual source/installed target paths",
        ));
    }
    let local = sinomo_core::cluster::read_cluster_state_without_kv(
        &sinomo_core::network::ClusterPaths::new(context.data_dir()),
    )
    .map_err(invalid)?
    .local_node_id;
    if &local != owner_node_id {
        return Err(invalid(
            "standalone generation names another actual principal",
        ));
    }
    let fence_path = source_root.join("journal/write.lock");
    let named = File::open(&fence_path).map_err(invalid)?;
    if crate::runtime_host::storage_file_identity(&named).map_err(invalid)?
        != crate::runtime_host::storage_file_identity(witness.source_fence()).map_err(invalid)?
    {
        return Err(invalid(
            "standalone source fence no longer owns its actual archived source",
        ));
    }
    witness.revalidate_original_certificate(root, identity, seal)?;
    let _writer = crate::metadata_lock::metadata_write_lock(
        root.to_owned(),
        crate::metadata_cursor::journal_write_lock_path(root),
    )?;
    let inspection = crate::DfsMetadataCandidateInspection::capture(root)?;
    let actual = inspection.identity();
    if inspection.has_prepared_bodies()
        || actual.prefix_proof != seal.prefix_proof
        || actual.file_fingerprint != seal.file_fingerprint
        || actual.state_fingerprint != seal.state_fingerprint
    {
        return Err(invalid(
            "standalone installed image changes the original clean seal",
        ));
    }
    let snapshot = crate::read_metadata_candidate(root)?;
    let expected = witness
        .file_system_ids()
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let actual = snapshot
        .tables
        .file_systems
        .iter()
        .map(|fs| fs.file_system_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    if expected.is_empty()
        || expected.len() != witness.file_system_ids().len()
        || actual != expected
        || !expected.contains(&identity.file_system_id)
        || snapshot
            .tables
            .file_systems
            .iter()
            .any(|fs| fs.owner_node_id != local || fs.metadata_storage.is_some())
        || snapshot
            .tables
            .prepared_operation_evictions
            .iter()
            .any(|record| record.digest.owner_epoch != 0)
    {
        return Err(invalid(
            "standalone current image differs from its exact epoch-zero filesystem ownership",
        ));
    }
    let mut header = read_generation(root)?;
    let active = GenerationState::Active {
        activation_digest: standalone_receipt(identity, seal)?,
        standalone_seal: Some(seal.clone()),
    };
    if header.identity != *identity {
        return Err(invalid("standalone installed identity changed"));
    }
    match &header.state {
        GenerationState::Staged { seal: Some(actual) } if actual == seal => {}
        state if state == &active => return Ok(()),
        _ => {
            return Err(invalid(
                "standalone current generation has no exact staged seal",
            ));
        }
    }
    header.state = active;
    write_generation(root, &header)
}
fn invalid(error: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("explicit standalone storage generation: {error}"))
}
