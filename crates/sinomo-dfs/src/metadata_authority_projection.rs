//! Interpretation of bounded authoritative DFS metadata projections.
//!
//! Process hosts obtain and retain the authoritative prefix proof. DFS owns
//! the key grammar and the conversion from that proof into shard and replica
//! authority contexts.

use crate::{
    DfsAuthoritativePrefixProjection, DfsError, DfsFileSystemId, DfsMetadataReplicaShardContext,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsMetadataShardMembershipHandoff,
};
use sinomo_core::node::NodeId;
use std::collections::{BTreeMap, BTreeSet};

pub type DfsMetadataShardsByFileSystem =
    BTreeMap<String, Result<Vec<DfsMetadataShardConfig>, String>>;

pub fn dfs_metadata_authority_key_is_exact(key: &str) -> bool {
    let mut segments = key.split('/');
    matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some(
                "metadata-shards"
                    | "metadata-write-sets"
                    | "metadata-head-watermarks"
                    | "metadata-membership-handoffs"
                    | "metadata-membership-handoff-proofs"
            ),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    ) || dfs_metadata_file_system_config_key_is_exact(key)
}

pub(crate) fn dfs_metadata_file_system_config_key_is_exact(key: &str) -> bool {
    key.strip_prefix("/dfs/filesystems/")
        .and_then(|suffix| suffix.strip_suffix("/config"))
        .is_some_and(|file_system_id| !file_system_id.is_empty() && !file_system_id.contains('/'))
}

/// Classify the owner of a committed configuration transition. Policies,
/// retention, display values and proof records update the authority projection;
/// they do not change which physical metadata stores the daemon must own.
/// Malformed structural configuration conservatively retires existing stores.
pub(crate) fn metadata_storage_configuration_changed(
    key: &str,
    previous: Option<&sinomo_core::kv::KvEntry>,
    current: Option<&sinomo_core::kv::KvEntry>,
) -> bool {
    if dfs_metadata_shard_config_key_is_exact(key) {
        return previous.map(|entry| &entry.value) != current.map(|entry| &entry.value);
    }
    if !dfs_metadata_file_system_config_key_is_exact(key) {
        return false;
    }
    let identity = |entry: &sinomo_core::kv::KvEntry| {
        entry.decode::<crate::DfsFileSystemConfig>().map(|config| {
            let crate::DfsFileSystemConfig {
                file_system_id,
                owner_node_id,
                metadata_storage,
                ..
            } = config;
            (file_system_id, owner_node_id, metadata_storage)
        })
    };
    match (previous, current) {
        (Some(previous), Some(current)) => match (identity(previous), identity(current)) {
            (Ok(previous), Ok(current)) => previous != current,
            _ => true,
        },
        (None, None) => false,
        _ => true,
    }
}

pub(crate) fn metadata_storage_configuration_changed_in_projection(
    previous: &[sinomo_core::kv::KvEntry],
    current: &[sinomo_core::kv::KvEntry],
) -> bool {
    let find = |entries: &[sinomo_core::kv::KvEntry], key: &str| {
        entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
    };
    previous.iter().any(|entry| {
        metadata_storage_configuration_changed(
            &entry.key,
            Some(entry),
            find(current, &entry.key).map(|index| &current[index]),
        )
    }) || current.iter().any(|entry| {
        find(previous, &entry.key).is_none()
            && metadata_storage_configuration_changed(&entry.key, None, Some(entry))
    })
}

pub fn dfs_metadata_shard_config_key_parts(key: &str) -> Option<(&str, &str)> {
    let mut segments = key.split('/');
    match (
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
        segments.next(),
    ) {
        (
            Some(""),
            Some("dfs"),
            Some("filesystems"),
            Some(file_system_id),
            Some("metadata-shards"),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty() => Some((file_system_id, shard_id)),
        _ => None,
    }
}

pub fn dfs_metadata_shard_config_key_is_exact(key: &str) -> bool {
    dfs_metadata_shard_config_key_parts(key).is_some()
}

pub fn metadata_shards_from_authority_projection(
    projection: &DfsAuthoritativePrefixProjection,
    file_system_id: &DfsFileSystemId,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    metadata_shards_from_authority_entries(projection.entries(), file_system_id)
}

/// Decode routing DATA for comparison with an already captured native context.
/// This does not manufacture a completeness certificate or an authority read.
pub(crate) fn metadata_shards_from_authority_entries(
    entries: &[sinomo_core::kv::KvEntry],
    file_system_id: &DfsFileSystemId,
) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
    let expected_file_system_id = file_system_id.to_string();
    let mut shards = entries
        .iter()
        .filter_map(|entry| {
            let (entry_file_system_id, entry_shard_id) =
                dfs_metadata_shard_config_key_parts(&entry.key)?;
            (entry_file_system_id == expected_file_system_id)
                .then_some((entry, entry_shard_id.to_string()))
        })
        .map(|(entry, entry_shard_id)| {
            let shard =
                entry
                    .decode::<DfsMetadataShardConfig>()
                    .map_err(|error| DfsError::Metadata {
                        message: format!(
                            "decode retained DFS metadata shard authority {:?}: {error}",
                            entry.key
                        ),
                    })?;
            if shard.file_system_id != *file_system_id || shard.shard_id != entry_shard_id {
                return Err(DfsError::Metadata {
                    message: format!(
                        "retained DFS metadata shard authority {:?} is bound to {}:{}",
                        entry.key, shard.file_system_id, shard.shard_id
                    ),
                });
            }
            Ok(shard)
        })
        .collect::<Result<Vec<_>, _>>()?;
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    Ok(shards)
}

/// Configured shards of `file_system_id` whose copy `local_node_id` does not
/// hold (see [`DfsMetadataReplicaShardContext::local_node_holds_copy`]).
pub fn metadata_shard_copies_to_discard(
    projection: &DfsAuthoritativePrefixProjection,
    local_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
) -> Result<BTreeSet<DfsMetadataShardId>, DfsError> {
    let mut discard = BTreeSet::new();
    for shard in metadata_shards_from_authority_projection(projection, file_system_id)? {
        let shard_id = DfsMetadataShardId::new(shard.shard_id);
        let holds_copy = metadata_replica_context_from_authority_entries(
            projection.entries(),
            local_node_id,
            file_system_id,
            &shard_id,
        )?
        .is_none_or(|context| context.local_node_holds_copy());
        if !holds_copy {
            discard.insert(shard_id);
        }
    }
    Ok(discard)
}

pub fn metadata_shard_index_from_authority_entries(
    entries: &[sinomo_core::kv::KvEntry],
) -> DfsMetadataShardsByFileSystem {
    let mut shards_by_file_system = DfsMetadataShardsByFileSystem::new();
    for entry in entries {
        let Some((key_file_system_id, key_shard_id)) =
            dfs_metadata_shard_config_key_parts(&entry.key)
        else {
            continue;
        };
        let decoded = entry.decode::<DfsMetadataShardConfig>().map_err(|error| {
            format!(
                "decode retained metadata shard authority for filesystem {key_file_system_id} shard {key_shard_id}: {error}"
            )
        });
        let slot = shards_by_file_system
            .entry(key_file_system_id.to_string())
            .or_insert_with(|| Ok(Vec::new()));
        match (slot, decoded) {
            (Ok(shards), Ok(shard))
                if shard.file_system_id.to_string() == key_file_system_id
                    && shard.shard_id == key_shard_id =>
            {
                shards.push(shard);
            }
            (slot @ Ok(_), Ok(shard)) => {
                *slot = Err(format!(
                    "retained metadata shard authority key {key_file_system_id}/{key_shard_id} contains filesystem {} shard {}",
                    shard.file_system_id, shard.shard_id,
                ));
            }
            (slot @ Ok(_), Err(error)) => *slot = Err(error),
            (Err(_), _) => {}
        }
    }
    shards_by_file_system
}

pub fn namespace_command_matches_authority_index(
    shards_by_file_system: &DfsMetadataShardsByFileSystem,
    command: &crate::DfsNamespaceCommand,
) -> Result<bool, DfsError> {
    let [stamped] = command.authority_shards.as_slice() else {
        return Ok(false);
    };
    if stamped.file_system_id != command.file_system_id || stamped.shard_id.trim().is_empty() {
        return Ok(false);
    }
    let file_system_id = command.file_system_id.to_string();
    let shards = match shards_by_file_system.get(&file_system_id) {
        Some(Ok(shards)) => shards.as_slice(),
        Some(Err(error)) => {
            return Err(DfsError::Metadata {
                message: error.clone(),
            });
        }
        None => &[],
    };
    let matches = match &command.operation {
        crate::DfsNamespaceOperation::LookupPath { path, .. } => {
            crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &command.file_system_id,
                shards,
                path,
            )
            .is_ok_and(|selected| selected.as_ref() == Some(stamped))
        }
        crate::DfsNamespaceOperation::LookupPaths { paths } if !paths.is_empty() => {
            paths.iter().all(|path| {
                crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    &command.file_system_id,
                    shards,
                    path,
                )
                .is_ok_and(|selected| selected.as_ref() == Some(stamped))
            })
        }
        crate::DfsNamespaceOperation::PollNamespaceChanges { request } => {
            request.cursor.file_system_id == command.file_system_id
                && request.cursor.shard_id.0 == stamped.shard_id
                && request.cursor.authority_epoch == stamped.epoch
                && crate::dfs_namespace_change_shards_for_subtree(
                    &command.file_system_id,
                    &request.root_path,
                    shards,
                )
                .is_ok_and(|selected| selected.iter().any(|shard| shard == stamped))
        }
        _ => false,
    };
    Ok(matches)
}

pub fn metadata_replica_context_matches_authority_entries(
    entries: &[sinomo_core::kv::KvEntry],
    context: &DfsMetadataReplicaShardContext,
) -> Result<bool, DfsError> {
    let projected = metadata_replica_context_from_authority_entries(
        entries,
        &context.local_node_id,
        &context.shard.file_system_id,
        &DfsMetadataShardId::new(context.shard.shard_id.clone()),
    )?;
    let current = projected.as_ref();
    let shard_matches = current.is_some_and(|current| current.shard == context.shard);
    let handoff_matches = current.is_some_and(|current| {
        current.pending_membership_handoff == context.pending_membership_handoff
    });
    let catalog_matches = context.namespace_catalog.as_ref()
        .map(|catalog| catalog.matches_entries(entries)).transpose()?.unwrap_or(true);
    let matches = shard_matches && handoff_matches && catalog_matches;
    if !matches && tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE) {
        // Observe the contexts already decoded for this exact comparison. The
        // fixed field set omits member arrays, paths, and storage-import payloads.
        // No new projection read, authorization decision, or authority cache is
        // introduced by these diagnostics.
        let expected_handoff = context.pending_membership_handoff.as_ref();
        let current_handoff =
            current.and_then(|current| current.pending_membership_handoff.as_ref());
        tracing::trace!(target: "sinomo.dfs.perf",
            current_context_present = current.is_some(), shard_matches, handoff_matches,
            expected_epoch = context.shard.epoch,
            current_epoch = ?current.map(|current| current.shard.epoch),
            expected_primary = %context.shard.primary_node_id,
            current_primary = ?current.map(|current| &current.shard.primary_node_id),
            file_system_matches = current.is_some_and(|current| current.shard.file_system_id == context.shard.file_system_id),
            shard_id_matches = current.is_some_and(|current| current.shard.shard_id == context.shard.shard_id),
            replica_node_ids_match = current.is_some_and(|current| current.shard.replica_node_ids == context.shard.replica_node_ids),
            expected_replica_count = context.shard.replica_node_ids.len(),
            current_replica_count = ?current.map(|current| current.shard.replica_node_ids.len()),
            assignment_matches = current.is_some_and(|current| current.shard.assignment == context.shard.assignment),
            default_policy_matches = current.is_some_and(|current| current.shard.default_policy == context.shard.default_policy),
            expected_handoff_id = ?expected_handoff.map(|handoff| &handoff.handoff_id),
            current_handoff_id = ?current_handoff.map(|handoff| &handoff.handoff_id),
            handoff_cause_matches = expected_handoff.map(|handoff| &handoff.cause) == current_handoff.map(|handoff| &handoff.cause),
            retiring_shard_matches = expected_handoff.map(|handoff| &handoff.retiring_shard) == current_handoff.map(|handoff| &handoff.retiring_shard),
            desired_shard_matches = expected_handoff.map(|handoff| &handoff.desired_shard) == current_handoff.map(|handoff| &handoff.desired_shard),
            expected_required_sequence = ?expected_handoff.map(|handoff| handoff.required_prepared_sequence),
            current_required_sequence = ?current_handoff.map(|handoff| handoff.required_prepared_sequence),
            required_watermark_matches = expected_handoff.map(|handoff| &handoff.required_head_watermark) == current_handoff.map(|handoff| &handoff.required_head_watermark),
            retiring_fence_matches = expected_handoff.map(|handoff| &handoff.retiring_primary_fence) == current_handoff.map(|handoff| &handoff.retiring_primary_fence),
            expected_fence_sequence = ?expected_handoff.and_then(|handoff| handoff.retiring_primary_fence.as_ref()).map(|fence| fence.journal_sequence),
            current_fence_sequence = ?current_handoff.and_then(|handoff| handoff.retiring_primary_fence.as_ref()).map(|fence| fence.journal_sequence),
            "DFS retained replica context comparison differs"
        );
    }
    Ok(matches)
}

pub fn metadata_replica_context_from_authority_projection(
    projection: &DfsAuthoritativePrefixProjection,
    local_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataReplicaShardContext>, DfsError> {
    let mut context = metadata_replica_context_from_authority_entries(
        projection.entries(),
        local_node_id,
        file_system_id,
        shard_id,
    )?;
    if let Some(context) = context.as_mut() {
        context.namespace_catalog = Some(crate::DfsMetadataNamespaceCatalog::capture(projection, file_system_id)?);
    }
    Ok(context)
}

pub fn metadata_replica_context_from_authority_entries(
    entries: &[sinomo_core::kv::KvEntry],
    local_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataReplicaShardContext>, DfsError> {
    let shard_key = crate::metadata_shard_key(file_system_id, &shard_id.0);
    let handoff_key = crate::metadata_shard_membership_handoff_key(file_system_id, &shard_id.0);
    let entry = |key: &str| {
        entries
            .binary_search_by(|entry| entry.key.as_str().cmp(key))
            .ok()
            .map(|index| &entries[index])
    };
    let shard = entry(&shard_key)
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .transpose()?;
    let Some(shard) = shard else {
        return Ok(None);
    };
    if shard.file_system_id != *file_system_id || shard.shard_id != shard_id.0 {
        return Err(DfsError::Metadata {
            message: format!(
                "retained DFS metadata shard authority {shard_key:?} is bound to {}:{}",
                shard.file_system_id, shard.shard_id
            ),
        });
    }
    let pending_membership_handoff = entry(&handoff_key)
        .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
        .transpose()?
        .filter(|handoff| {
            handoff.retiring_shard == shard
                && handoff.desired_shard.file_system_id == shard.file_system_id
                && handoff.desired_shard.shard_id == shard.shard_id
                && handoff.desired_shard.epoch == shard.epoch.saturating_add(1)
        });
    let write_set_key = crate::metadata_shard_write_set_key(file_system_id, &shard_id.0);
    // A write-set record that is stale or unreadable grants nothing: the
    // shard keeps the all-member rule for this member.
    let write_set = crate::metadata_shard_write_set_state(
        &shard,
        entry(&write_set_key).map(|entry| entry.value.as_slice()),
    )
    .switched()
    .cloned();
    Ok(Some(DfsMetadataReplicaShardContext {
        namespace_catalog: None, // Raw entries are routing DATA, not a complete captured input.
        local_node_id: local_node_id.clone(),
        shard,
        pending_membership_handoff,
        write_set,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_authority_key_grammar_excludes_descendants_and_empty_segments() {
        assert!(dfs_metadata_authority_key_is_exact(
            "/dfs/filesystems/sifs/config"
        ));
        assert!(dfs_metadata_authority_key_is_exact(
            "/dfs/filesystems/sifs/metadata-shards/root"
        ));
        assert!(!dfs_metadata_authority_key_is_exact(
            "/dfs/filesystems/sifs/metadata-shards/root/child"
        ));
        assert!(!dfs_metadata_authority_key_is_exact(
            "/dfs/filesystems//metadata-shards/root"
        ));
    }

    #[test]
    fn shard_key_parts_require_one_exact_shard_segment() {
        assert_eq!(
            dfs_metadata_shard_config_key_parts("/dfs/filesystems/sifs/metadata-shards/services"),
            Some(("sifs", "services"))
        );
        assert_eq!(
            dfs_metadata_shard_config_key_parts(
                "/dfs/filesystems/sifs/metadata-shards/services/child"
            ),
            None
        );
    }
}
