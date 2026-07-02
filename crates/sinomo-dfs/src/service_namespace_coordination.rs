//! Namespace shard selection and cross-shard coordination helpers.
//!
//! Routed namespace requests, POSIX operations, and ownership checks all need
//! the same view of metadata-shard impact and in-flight cross-shard locks. This
//! module keeps that coordination logic out of the generic inbound protocol
//! dispatcher while preserving a crate-local service API for higher-level DFS
//! operations.

use std::collections::BTreeSet;

use sinomo_core::node::NodeId;

use crate::*;
use crate::{posix_model, posix_path};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Adoption operations install records minted by another shard authority;
    /// only the source path's configured primary may send them. Returns the
    /// rejection reason for an unauthorized sender, `None` when authorized or
    /// for non-adoption operations.
    pub(crate) fn adoption_source_authority_rejection(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        requester_node_id: &NodeId,
    ) -> Result<Option<String>, DfsError> {
        let source_path = match operation {
            DfsNamespaceOperation::AdoptObjectAtPath { source_path, .. }
            | DfsNamespaceOperation::AdoptHardlinkAlias { source_path, .. } => source_path,
            _ => return Ok(None),
        };
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let Some(normalized) = posix_path::normalized_posix_path_key(source_path) else {
            return Ok(Some(format!(
                "adoption carries an invalid source path: {source_path}"
            )));
        };
        let kv = self
            .control_plane
            .as_mut()
            .expect("control plane checked above");
        let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
            .select_metadata_shard_for_path_strict_default_sifs_split(file_system_id, &normalized)
            .map_err(DfsError::from)?
        else {
            return Ok(Some(format!(
                "adoption source path {normalized} has no metadata shard assignment"
            )));
        };
        if &shard.primary_node_id != requester_node_id {
            return Ok(Some(format!(
                "adoption sender {requester_node_id} is not the primary of source shard {} (primary {}, epoch {})",
                shard.shard_id, shard.primary_node_id, shard.epoch
            )));
        }
        Ok(None)
    }

    /// The metadata shards a namespace operation can mutate: the shards of
    /// every involved path plus their parents (parent records share the
    /// child's prefix shard except at shard-boundary anchors).
    pub(crate) fn affected_namespace_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if let DfsNamespaceOperation::ApplyTransaction { transaction } = operation {
            return self.affected_namespace_transaction_shards(file_system_id, transaction);
        }
        let mut shards: Vec<DfsMetadataShardConfig> = Vec::new();
        for path in operation.involved_paths() {
            for shard in self.namespace_shards_for_path(file_system_id, path)? {
                if !shards
                    .iter()
                    .any(|existing| existing.shard_id == shard.shard_id)
                {
                    shards.push(shard);
                }
            }
        }
        Ok(shards)
    }

    fn affected_namespace_transaction_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
        transaction: &DfsNamespaceTransaction,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if transaction.objects.is_empty() || self.control_plane.is_none() {
            return Ok(Vec::new());
        }
        let shards = {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked before namespace transaction shard selection");
            DfsControlPlaneStore::new(kv.as_mut())
                .list_metadata_shards(file_system_id)
                .map_err(DfsError::from)?
        };
        let mut candidate_paths = BTreeSet::new();
        for object in &transaction.objects {
            let normalized =
                posix_path::normalized_posix_path_key(&object.path).ok_or_else(|| {
                    DfsError::Distributed {
                        message: format!("invalid namespace transaction path: {}", object.path),
                    }
                })?;
            if let Some(parent) = posix_path::parent_posix_path(&normalized) {
                candidate_paths.insert(parent);
            }
            candidate_paths.insert(normalized);
        }

        let mut affected = Vec::new();
        for path in candidate_paths {
            let Some(shard) =
                crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    file_system_id,
                    &shards,
                    &path,
                )
                .map_err(DfsError::from)?
            else {
                continue;
            };
            if !affected
                .iter()
                .any(|existing: &DfsMetadataShardConfig| existing.shard_id == shard.shard_id)
            {
                affected.push(shard);
            }
        }
        Ok(affected)
    }

    /// The metadata shards covering a path and its parent directory.
    pub(crate) fn namespace_shards_for_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(Vec::new());
        }
        let normalized =
            posix_path::normalized_posix_path_key(path).ok_or_else(|| DfsError::Distributed {
                message: format!("invalid namespace operation path: {path}"),
            })?;
        let mut candidates = vec![normalized.clone()];
        if let Some(parent) = posix_path::parent_posix_path(&normalized) {
            candidates.push(parent);
        }
        let mut shards: Vec<DfsMetadataShardConfig> = Vec::new();
        for candidate in candidates {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked above");
            let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
                .select_metadata_shard_for_path_strict_default_sifs_split(
                    file_system_id,
                    &candidate,
                )
                .map_err(DfsError::from)?
            else {
                continue;
            };
            if !shards
                .iter()
                .any(|existing| existing.shard_id == shard.shard_id)
            {
                shards.push(shard);
            }
        }
        Ok(shards)
    }

    /// Validates the source side of a coordinated rename and resolves the
    /// single foreign destination primary before handing off to the
    /// cross-shard coordinator.
    pub(crate) fn namespace_coordinator_dispatch_rename(
        &mut self,
        file_system_id: &DfsFileSystemId,
        from: &str,
        to: &str,
    ) -> posix_model::Result<()> {
        if self.routed_namespace_settle_depth >= DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH {
            return Err(posix_model::DfsPosixError::Dfs(DfsError::Busy {
                path: from.to_string(),
            }));
        }
        if self.resume_running_cross_shard_namespace_job_for_paths(
            file_system_id,
            from,
            to,
            &[
                DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
            ],
        )? {
            return Ok(());
        }
        let destination_primary =
            self.namespace_coordinator_foreign_primary(file_system_id, from, to)?;
        let Some(destination_primary) = destination_primary else {
            return self.posix_rename_local(from, to).map(drop);
        };
        self.coordinate_cross_shard_rename(from, to, destination_primary)
    }

    pub(crate) fn namespace_coordinator_dispatch_hardlink(
        &mut self,
        file_system_id: &DfsFileSystemId,
        existing_path: &str,
        link_path: &str,
    ) -> posix_model::Result<()> {
        if self.routed_namespace_settle_depth >= DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH {
            return Err(posix_model::DfsPosixError::Dfs(DfsError::Busy {
                path: existing_path.to_string(),
            }));
        }
        if self.resume_running_cross_shard_namespace_job_for_paths(
            file_system_id,
            existing_path,
            link_path,
            &[DfsNamespaceJobKind::HardlinkAcrossShardPrimaries],
        )? {
            return Ok(());
        }
        let destination_primary =
            self.namespace_coordinator_foreign_primary(file_system_id, existing_path, link_path)?;
        let Some(destination_primary) = destination_primary else {
            return self
                .posix_hardlink_local(existing_path, link_path)
                .map(drop);
        };
        self.coordinate_cross_shard_hardlink(existing_path, link_path, destination_primary)
    }

    /// For a two-path coordinated operation: every shard on the source side
    /// must be owned by this node, and the destination side must resolve to
    /// at most one foreign primary.
    fn namespace_coordinator_foreign_primary(
        &mut self,
        file_system_id: &DfsFileSystemId,
        source_path: &str,
        destination_path: &str,
    ) -> posix_model::Result<Option<NodeId>> {
        let source_shards = self.namespace_shards_for_path(file_system_id, source_path)?;
        if let Some(shard) = source_shards
            .iter()
            .find(|shard| shard.primary_node_id != self.node_id)
        {
            return Err(posix_model::DfsPosixError::Dfs(DfsError::StaleOwnerEpoch {
                file_system_id: file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id.clone(),
                epoch: shard.epoch,
            }));
        }
        let destination_shards =
            self.namespace_shards_for_path(file_system_id, destination_path)?;
        let mut foreign_primary: Option<NodeId> = None;
        for shard in destination_shards {
            if shard.primary_node_id == self.node_id {
                continue;
            }
            match &foreign_primary {
                Some(existing) if existing != &shard.primary_node_id => {
                    return Err(posix_model::DfsPosixError::InvalidPath {
                        path: format!(
                            "{destination_path} spans metadata shards with different primaries"
                        ),
                    });
                }
                _ => foreign_primary = Some(shard.primary_node_id),
            }
        }
        Ok(foreign_primary)
    }

    /// Marks a path as participating in an in-flight cross-shard move so
    /// concurrent mutations cannot fork the object between shards. Directory
    /// moves lock their whole subtree: the check matches descendants and
    /// ancestors of the locked path, not just the path itself.
    pub(crate) fn lock_cross_shard_namespace_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let Some(normalized) = posix_path::normalized_posix_path_key(path) else {
            return;
        };
        self.cross_shard_namespace_locks
            .insert((file_system_id.clone(), normalized));
    }

    pub(crate) fn unlock_cross_shard_namespace_path(
        &mut self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) {
        let Some(normalized) = posix_path::normalized_posix_path_key(path) else {
            return;
        };
        self.cross_shard_namespace_locks
            .remove(&(file_system_id.clone(), normalized));
    }

    pub(crate) fn ensure_path_not_in_cross_shard_flight(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<(), DfsError> {
        if self.cross_shard_namespace_locks.is_empty() {
            return Ok(());
        }
        let Some(normalized) = posix_path::normalized_posix_path_key(path) else {
            return Ok(());
        };
        let blocked =
            self.cross_shard_namespace_locks
                .iter()
                .any(|(locked_file_system_id, locked_path)| {
                    locked_file_system_id == file_system_id
                        && (posix_path::posix_path_is_at_or_under_prefix(&normalized, locked_path)
                            || posix_path::posix_path_is_at_or_under_prefix(
                                locked_path,
                                &normalized,
                            ))
                });
        if blocked {
            return Err(DfsError::Busy {
                path: path.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn ensure_object_not_in_cross_shard_flight(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.ensure_path_not_in_cross_shard_flight(&object.file_system_id, &object.path)
    }

    /// Whether this node is the metadata write authority for the object's
    /// path: the configured metadata-shard primary, or the local service
    /// itself when no cluster shard configuration exists.
    pub(crate) fn is_local_metadata_primary_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        Ok(match self.select_metadata_shard_for_object(object)? {
            Some(shard) => shard.primary_node_id == self.node_id,
            None => true,
        })
    }
}
