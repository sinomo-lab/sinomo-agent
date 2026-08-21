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
use std::collections::BTreeMap;

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
                    | "metadata-head-watermarks"
                    | "metadata-membership-handoffs"
                    | "metadata-membership-handoff-proofs"
            ),
            Some(shard_id),
            None,
        ) if !file_system_id.is_empty() && !shard_id.is_empty()
    ) || {
        let mut segments = key.split('/');
        matches!(
            (
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
                Some("config"),
                None,
            ) if !file_system_id.is_empty()
        )
    }
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
    let expected_file_system_id = file_system_id.to_string();
    let mut shards = projection
        .entries()
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
    Ok(projected.is_some_and(|projected| {
        projected.shard == context.shard
            && projected.pending_membership_handoff == context.pending_membership_handoff
    }))
}

pub fn metadata_replica_context_from_authority_projection(
    projection: &DfsAuthoritativePrefixProjection,
    local_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataReplicaShardContext>, DfsError> {
    metadata_replica_context_from_authority_entries(
        projection.entries(),
        local_node_id,
        file_system_id,
        shard_id,
    )
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
    Ok(Some(DfsMetadataReplicaShardContext {
        local_node_id: local_node_id.clone(),
        shard,
        pending_membership_handoff,
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
