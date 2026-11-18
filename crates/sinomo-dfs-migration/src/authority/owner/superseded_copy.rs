//! A replica's copy of a shard whose conversion activated without it.
//!
//! A per-shard conversion activates once its install rule is met, so a
//! replica that could not take the image when the primary installed it
//! keeps its old copy. No conversion of its own replaces that copy: the
//! shard's storage is Active under another node's attempt, and this replica
//! holds no staged image of it. The replica takes the activated image as an
//! ordinary authenticated snapshot from the shard's primary instead (the
//! core's snapshot repair), which the shard's next commit asks of every
//! member the commit needs.
//!
//! That repair writes only an absent or unreadable copy; while the migration
//! owner reports the old copy as a source it still owns, the repair is
//! refused and the replica never opens. So the replica's shard unit
//! [`supersede`]s the copy: it keeps the old copy where an installing member
//! keeps it, `.metadata-retained/shard-conversion-<attempt>`, where a
//! combined `root` copy stays found by the conversions of the shards whose
//! rows live only there, and records the activated generation beside it.
//! While the local copy does not hold that generation, the owner reports the
//! copy unreadable ([`awaits_snapshot`]), so the repair installs the image.
//! Once it does, the record is removed.
use super::*;
use sinomo_dfs::{DfsMetadataGenerationIdentity, DfsMetadataShardId, DfsMetadataShardStorage};

fn record_path(metadata_root: &Path, shard: &DfsMetadataShardId) -> PathBuf {
    metadata_root
        .join(".metadata-retained")
        .join(format!("superseded-copy-{}.json", shard.0))
}

/// Whether the copy at `destination` holds `generation`, activated.
fn holds(destination: &Path, generation: &DfsMetadataGenerationIdentity) -> bool {
    sinomo_dfs::inspect_metadata_storage_generation(destination).is_ok_and(|header| {
        header.identity == *generation
            && matches!(
                header.state,
                sinomo_dfs::DfsMetadataGenerationState::Active { .. }
            )
    })
}

fn read_record(path: &Path) -> Result<Option<DfsMetadataGenerationIdentity>> {
    match sinomo_core::storage::durable_file::read_object_file_bytes(path, 64 * 1024) {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(error),
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(cause) => Err(error(cause)),
    }
}

/// Whether the copy of `shard` at `destination` is superseded by its
/// shard's activated conversion and waits for the snapshot that replaces it.
pub(super) fn awaits_snapshot(destination: &Path, shard: &DfsMetadataShardId) -> Result<bool> {
    let metadata_root = destination
        .parent()
        .ok_or_else(|| error("metadata unit has no parent"))?;
    Ok(read_record(&record_path(metadata_root, shard))?
        .is_some_and(|generation| !holds(destination, &generation)))
}

/// Supersede this member's copy of `shard`, whose storage `record` is Active
/// under an attempt this node did not convert or no longer holds: keep the
/// old copy at the attempt's retained place and record the activated
/// generation, unless the copy already holds it, this node stages that
/// attempt's image (its generation unit installs it), or the copy is a
/// current generation of its own. Only a replica's copy is repaired: the
/// shard's primary serves the snapshot and has no member to take it from,
/// and a node that is no member keeps no copy. Returns whether the copy now
/// waits for repair.
pub(super) fn supersede(
    owner: &MigrationOwner,
    shard: &DfsMetadataShardConfig,
    record: &DfsMetadataShardStorage,
) -> Result<bool> {
    let Some((attempt, _)) = record.converted_activation() else {
        return Ok(false);
    };
    let physical = DfsMetadataShardId::new(&shard.shard_id);
    let destination = owner.metadata_root.join(&shard.shard_id);
    let marker = record_path(&owner.metadata_root, &physical);
    if holds(&destination, &record.generation) {
        sinomo_core::storage::durable_file::remove_file_if_exists_and_sync_parent(&marker)
            .map_err(error)?;
        return Ok(false);
    }
    let local = owner.authority.local_node()?;
    let replica = shard.primary_node_id != local && shard.replica_node_ids.contains(&local);
    let staged = super::conversion_receiver::conversion_stage_root(
        &owner.authority.receiver_root,
        &shard.file_system_id,
        &shard.shard_id,
        &attempt.attempt_id,
    )?
    .try_exists()
    .map_err(error)?;
    let current =
        sinomo_dfs::inspect_metadata_storage_generation(&destination).is_ok_and(|header| {
            matches!(
                header.state,
                sinomo_dfs::DfsMetadataGenerationState::Active { .. }
            )
        });
    if !replica || staged || current {
        return Ok(false);
    }
    if read_record(&marker)?.as_ref() == Some(&record.generation) {
        return Ok(true);
    }
    // A capture this node took of the copy ends here, so the copy is never
    // read as a source again.
    owner
        .metadata
        .finish_shard_conversion_capture(&destination, &physical)?;
    let retained = owner
        .metadata_root
        .join(".metadata-retained")
        .join(format!("shard-conversion-{}", attempt.attempt_id));
    if destination.try_exists().map_err(error)? && !retained.try_exists().map_err(error)? {
        let parent = retained
            .parent()
            .ok_or_else(|| error("retained copy has no parent"))?;
        sinomo_core::storage::durable_file::ensure_directory(parent).map_err(error)?;
        std::fs::rename(&destination, &retained).map_err(error)?;
        sinomo_core::storage::durable_file::sync_directory(&owner.metadata_root).map_err(error)?;
        sinomo_core::storage::durable_file::sync_directory(parent).map_err(error)?;
    }
    let bytes = serde_json::to_vec(&record.generation).map_err(error)?;
    sinomo_core::storage::durable_file::write_file_atomic(
        &marker,
        &bytes,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new(
            "superseded-metadata-copy",
            "tmp",
        )
        .sync_all(),
    )
    .map_err(error)?;
    // The copy's readiness changed: an open of it now meets the repair.
    owner
        .work
        .changed
        .send_modify(|generation| *generation = generation.wrapping_add(1));
    Ok(true)
}
