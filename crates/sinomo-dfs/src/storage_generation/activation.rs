use super::*;

/// Activate only a current representation whose exact imported cut and target
/// generation have already been authorized by the native control plane. This
/// grants no publication authority; ordinary mutations still acquire their own
/// current native guard. The caller supplies DATA, never a configuration permit.
/// sinomo:kv-consistency: Bind representation activation to the native filesystem generation and exact committed import cut.
pub async fn activate_metadata_storage_generation(
    context: &sinomo_module::ComponentContext,
    root: &Path,
    identity: &DfsMetadataGenerationIdentity,
    seal: &DfsMetadataGenerationSeal,
) -> Result<()> {
    let _owner = require_owned_unit(context, root, identity)?;
    let DfsMetadataGenerationOrigin::StorageTransition { transition_id } = &identity.origin else {
        return Err(invalid(
            "import activation requires a storage transition identity",
        ));
    };
    let cluster = context
        .cluster_access()
        .ok_or_else(|| invalid("storage activation requires native cluster authority"))?;
    let fs_key = crate::file_system_config_key(&identity.file_system_id);
    let shard_key = crate::metadata_shard_key(&identity.file_system_id, &identity.shard_id.0);
    let proof_key = crate::metadata_shard_membership_handoff_proof_key(
        &identity.file_system_id,
        &identity.shard_id.0,
    );
    // Representation activation is an explicit global-consistency boundary.
    // One native exact-set cut binds configuration, assignment and import proof.
    let read = cluster
        .kv_get_authoritative_exact_set_async(vec![
            fs_key.clone(),
            shard_key.clone(),
            proof_key.clone(),
        ])
        .await
        .map_err(invalid)?;
    require_current_authority(&read.authority)?;
    if read.value.len() != 3 {
        return Err(invalid("storage activation exact-set cardinality changed"));
    }
    let entries = read.value;
    let fs: crate::DfsFileSystemConfig = entry(&entries[0], &fs_key)?.decode().map_err(invalid)?;
    let shard: crate::DfsMetadataShardConfig =
        entry(&entries[1], &shard_key)?.decode().map_err(invalid)?;
    let proof: crate::DfsMetadataShardMembershipHandoffProof =
        entry(&entries[2], &proof_key)?.decode().map_err(invalid)?;
    let local = sinomo_core::cluster::read_cluster_state_without_kv(
        &sinomo_core::network::ClusterPaths::new(context.data_dir()),
    )
    .map_err(invalid)?
    .local_node_id;
    let Some(crate::DfsMetadataStorageState::Active {
        generation,
        target_format,
        activation_digest,
        ..
    }) = &fs.metadata_storage
    else {
        return Err(invalid(
            "filesystem storage transition is not natively Active",
        ));
    };
    fs.metadata_storage
        .as_ref()
        .expect("matched active")
        .validate_at_catalog_generation(fs.catalog_generation)
        .map_err(invalid)?;
    let crate::DfsMetadataShardHandoffCause::StorageImport { cut } = &proof.cause else {
        return Err(invalid(
            "current unit has no exact storage import activation proof",
        ));
    };
    let acknowledged = proof.acknowledged_node_ids.iter().cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if acknowledged.len() != proof.acknowledged_node_ids.len()
        || !crate::control_plane::metadata_shard_membership_handoff_acknowledgements_are_safe(
            &proof.retiring_shard, &proof.desired_shard, &acknowledged,
        ) {
        return Err(invalid("native storage activation lacks its original joint quorum"));
    }
    // The committed quorum selects the exact generation/image. A configured
    // replica may install it later; activation still checks its actual owner,
    // current assignment and complete physical image under the writer fence.
    // The proof binds the shard's storage identity and the transition, not
    // the serving configuration: a later takeover, rotation, recruit or
    // removal leaves a lagging holder's staged copy activatable.
    if fs.file_system_id != identity.file_system_id
        || shard.file_system_id != identity.file_system_id
        || shard.shard_id != identity.shard_id.0
        || !crate::metadata_shard_storage_scope_matches(&proof.desired_shard, &shard)
        || !crate::metadata_shard_storage_scope_matches(&proof.retiring_shard, &shard)
        || proof.retiring_shard.epoch.checked_add(1) != Some(proof.desired_shard.epoch)
        || (shard.primary_node_id != local && !shard.replica_node_ids.contains(&local))
        || generation != &identity.generation
        || *target_format != u32::from(crate::METADATA_TABLE_SNAPSHOT_FORMAT_VERSION)
        || cut.storage_transition_id.as_ref() != Some(transition_id)
        || cut.target_prefix != seal.prefix_proof
        || cut.converted_image_fingerprint != seal.state_fingerprint
        || proof.journal_sequence != seal.prefix_proof.journal_sequence
        || proof.state_fingerprint != seal.state_fingerprint
    {
        return Err(invalid(
            "native storage activation differs from its exact candidate generation/cut",
        ));
    }
    let _writer = crate::metadata_lock::metadata_write_lock(
        root.to_owned(),
        crate::metadata_cursor::journal_write_lock_path(root),
    )?;
    let inspection = crate::DfsMetadataCandidateInspection::capture(root)?;
    inspection.validate_retiring_reservations(&proof.retiring_shard)?;
    let actual = inspection.identity();
    if actual.prefix_proof != seal.prefix_proof
        || actual.state_fingerprint != seal.state_fingerprint
        || actual.file_fingerprint != seal.file_fingerprint
    {
        return Err(invalid(
            "installed candidate differs from native activation seal",
        ));
    }
    let mut envelope = read_generation(root)?;
    if envelope.identity != *identity {
        return Err(invalid("installed storage generation identity changed"));
    }
    let active = GenerationState::Active {
        activation_digest: activation_digest.clone(),
        standalone_seal: None,
    };
    match &envelope.state {
        GenerationState::Staged { seal: Some(actual) } if actual == seal => {}
        state if state == &active => return Ok(()),
        _ => {
            return Err(invalid(
                "installed generation has no exact sealed candidate",
            ));
        }
    }
    envelope.state = active;
    write_generation(root, &envelope)
}

/// The checks every local activation makes before it reads authority: an
/// owned (not shadow) context, a valid identity whose exact unit root inside
/// the data directory is `root`, and the live data-owner lock, which the
/// caller holds for the whole activation.
pub(super) fn require_owned_unit(
    context: &sinomo_module::ComponentContext,
    root: &Path,
    identity: &DfsMetadataGenerationIdentity,
) -> Result<impl Sized + use<>> {
    if context.host_role() == sinomo_module::ComponentHostRole::Shadow {
        return Err(invalid(
            "shadow context cannot activate owned metadata storage",
        ));
    }
    identity.validate()?;
    let expected_root =
        crate::local_dfs_metadata_shard_path(context.data_dir(), &identity.shard_id);
    let actual_root = root.canonicalize().map_err(invalid)?;
    let data_root = context.data_dir().root().canonicalize().map_err(invalid)?;
    if actual_root != expected_root.canonicalize().map_err(invalid)?
        || !actual_root.starts_with(&data_root)
    {
        return Err(invalid(
            "storage activation root differs from the actual component unit",
        ));
    }
    crate::runtime_host::require_daemon_dfs_data_owner_lock(
        &crate::runtime_host::DfsRuntimePaths::new(context.data_dir()),
    )
    .map_err(invalid)
}

/// Require an authoritative read confirmed by a current-term voter quorum.
pub(super) fn require_current_authority(authority: &sinomo_core::kv::KvAuthorityProof) -> Result<()> {
    match authority.basis {
        sinomo_core::kv::KvAuthorityBasis::CurrentTermQuorum {
            voter_acks,
            required_quorum,
        } if authority.term != 0 && required_quorum > 0 && voter_acks >= required_quorum => Ok(()),
        _ => Err(invalid("storage activation lacks current native authority")),
    }
}

pub(super) fn invalid(error: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(format!("current storage generation activation: {error}"))
}
pub(super) fn entry<'a>(
    entry: &'a Option<sinomo_core::kv::KvEntry>,
    expected: &str,
) -> Result<&'a sinomo_core::kv::KvEntry> {
    entry
        .as_ref()
        .filter(|entry| entry.key == expected)
        .ok_or_else(|| invalid("native activation entry is missing or misbound"))
}
