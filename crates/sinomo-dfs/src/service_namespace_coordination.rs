//! Namespace shard selection and cross-shard coordination helpers.
//!
//! Routed namespace requests, POSIX operations, and ownership checks all need
//! the same view of metadata-shard impact and in-flight cross-shard locks. This
//! module keeps that coordination logic out of the generic inbound protocol
//! dispatcher while preserving a crate-local service API for higher-level DFS
//! operations.

use sinomo_core::node::NodeId;
use std::collections::BTreeSet;

use crate::service_state::DfsNamespaceOperationContext;
use crate::*;
use crate::{posix_model, posix_path};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(crate) fn replace_namespace_operation_context(
        &mut self,
        file_system_id: DfsFileSystemId,
        shards: Vec<DfsMetadataShardConfig>,
    ) -> Option<DfsNamespaceOperationContext> {
        self.active_namespace_operation_context
            .replace(DfsNamespaceOperationContext {
                file_system_id,
                shards,
                placement_config: None,
            })
    }

    pub(crate) fn restore_namespace_operation_context(
        &mut self,
        previous: Option<DfsNamespaceOperationContext>,
    ) {
        self.active_namespace_operation_context = previous;
    }

    pub(crate) fn metadata_shard_config_snapshot(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>, DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        DfsControlPlaneStore::new(kv.as_mut())
            .list_metadata_shards(file_system_id)
            .map(Some)
            .map_err(DfsError::from)
    }

    pub(crate) fn namespace_shards_for_normalized_paths_from_snapshot<'a>(
        file_system_id: &DfsFileSystemId,
        shards: &[DfsMetadataShardConfig],
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        let mut selected = Vec::new();
        for path in paths {
            let Some(shard) =
                crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                    file_system_id,
                    shards,
                    path,
                )
                .map_err(DfsError::from)?
            else {
                continue;
            };
            if !selected
                .iter()
                .any(|existing: &DfsMetadataShardConfig| existing.shard_id == shard.shard_id)
            {
                selected.push(shard);
            }
        }
        Ok(selected)
    }

    fn normalized_namespace_paths<'a>(
        paths: impl IntoIterator<Item = &'a str>,
        include_parents: bool,
        operation: &'static str,
    ) -> Result<Vec<String>, DfsError> {
        let mut seen = BTreeSet::new();
        let mut candidates = Vec::new();
        for path in paths {
            let normalized = posix_path::normalized_posix_path_key(path).ok_or_else(|| {
                DfsError::Distributed {
                    message: format!("invalid namespace {operation} path: {path}"),
                }
            })?;
            if seen.insert(normalized.clone()) {
                candidates.push(normalized.clone());
            }
            if include_parents
                && let Some(parent) = posix_path::parent_posix_path(&normalized)
                && seen.insert(parent.clone())
            {
                candidates.push(parent);
            }
        }
        Ok(candidates)
    }

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
            DfsNamespaceOperation::AdvisoryLockMigrationPrepare { source_path, .. }
            | DfsNamespaceOperation::AdoptObjectAtPath { source_path, .. }
            | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { source_path, .. }
            | DfsNamespaceOperation::VerifyObjectFamilyAdoption { source_path, .. }
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
        if self.control_plane.is_none() {
            return Ok(Vec::new());
        }
        let Some(shards) = self.metadata_shard_config_snapshot(file_system_id)? else {
            return Ok(Vec::new());
        };
        Self::affected_namespace_shards_from_snapshot(file_system_id, operation, &shards)
    }

    /// Resolves an operation against one immutable shard configuration view.
    ///
    /// Routed handlers use this after validating the request's authority
    /// stamps, then retain the same snapshot through path planning and the
    /// durable prepare. This prevents one operation from repeatedly rebuilding
    /// the same cluster-KV projection and also prevents it from mixing epochs.
    pub(crate) fn affected_namespace_shards_from_snapshot(
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        let candidates = match operation {
            DfsNamespaceOperation::LookupPath { path }
            | DfsNamespaceOperation::SyncPath { path }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. }
            | DfsNamespaceOperation::AdvisoryLock { path, .. }
            | DfsNamespaceOperation::FileSync { path, .. }
            | DfsNamespaceOperation::DirectorySync { path, .. } => {
                Self::normalized_namespace_paths([path.as_str()], false, "operation")?
            }
            DfsNamespaceOperation::VerifyObjectFamilyAdoption { object, .. } => {
                Self::normalized_namespace_paths([object.path.as_str()], false, "operation")?
            }
            DfsNamespaceOperation::LookupPaths { paths } => Self::normalized_namespace_paths(
                paths.iter().map(String::as_str),
                false,
                "operation",
            )?,
            DfsNamespaceOperation::CommitLocalEventualBatch { transaction, .. } => {
                return Self::affected_namespace_transaction_shards_from_snapshot(
                    file_system_id,
                    transaction,
                    shards,
                );
            }
            _ => Self::normalized_namespace_paths(operation.involved_paths(), true, "operation")?,
        };
        Self::namespace_shards_for_normalized_paths_from_snapshot(
            file_system_id,
            shards,
            candidates.iter().map(String::as_str),
        )
    }

    pub(crate) fn affected_namespace_transaction_shards_from_snapshot(
        file_system_id: &DfsFileSystemId,
        transaction: &DfsNamespaceTransaction,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if transaction.objects.is_empty() {
            return Ok(Vec::new());
        }
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

        Self::namespace_shards_for_normalized_paths_from_snapshot(
            file_system_id,
            shards,
            candidate_paths.iter().map(String::as_str),
        )
    }

    /// Resolve the shard that stores the transaction's exact object records.
    ///
    /// Unlike affected-operation routing, this deliberately does not add each
    /// object's parent. A shard-boundary parent touch is deferred to that
    /// parent's authority and is not part of the child transaction.
    pub(crate) fn namespace_transaction_shards_from_snapshot(
        file_system_id: &DfsFileSystemId,
        transaction: &DfsNamespaceTransaction,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        let candidates = Self::normalized_namespace_paths(
            transaction
                .objects
                .iter()
                .map(|object| object.path.as_str()),
            false,
            "transaction",
        )?;
        Self::namespace_shards_for_normalized_paths_from_snapshot(
            file_system_id,
            shards,
            candidates.iter().map(String::as_str),
        )
    }

    /// The shard(s) directly covering read-only namespace paths.
    ///
    /// Lookup and sync operations do not mutate parent directory records. If a
    /// split-prefix path and its parent are owned by different primaries, using
    /// the broader mutation shard set can make primaries redirect a read-only
    /// request back and forth forever.
    pub(crate) fn namespace_shards_for_exact_paths<'a>(
        &mut self,
        file_system_id: &DfsFileSystemId,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<DfsMetadataShardConfig>, DfsError> {
        if self.control_plane.is_none() {
            return Ok(Vec::new());
        }
        let candidates = Self::normalized_namespace_paths(paths, false, "operation")?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let Some(shards) = self.metadata_shard_config_snapshot(file_system_id)? else {
            return Ok(Vec::new());
        };
        Self::namespace_shards_for_normalized_paths_from_snapshot(
            file_system_id,
            &shards,
            candidates.iter().map(String::as_str),
        )
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
        self.namespace_coordinator_dispatch_rename_with_pending_replay(
            file_system_id,
            from,
            to,
            None,
        )
    }

    pub(crate) fn namespace_coordinator_dispatch_rename_with_pending_replay(
        &mut self,
        file_system_id: &DfsFileSystemId,
        from: &str,
        to: &str,
        pending_request: Option<&DfsNamespaceRequestRecord>,
    ) -> posix_model::Result<()> {
        if self.routed_namespace_settle_depth >= DFS_MAX_NESTED_NAMESPACE_COORDINATION_DEPTH {
            return Err(posix_model::DfsPosixError::Dfs(DfsError::Busy {
                path: from.to_string(),
            }));
        }
        // A shard transfer may have committed through the control-plane Raft
        // store while this service still holds an older local KV view. Refresh
        // before deciding which side owns the source; otherwise two stale
        // primaries can redirect a cross-shard rename back and forth.
        self.refresh_control_plane_latest()?;
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
        if let Some(request) = pending_request
            && self.completed_cross_shard_namespace_job_for_paths_since(
                file_system_id,
                from,
                to,
                &[
                    DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                    DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
                ],
                request.created_at,
                &DfsNamespaceJobOrigin::from_request(request),
            )?
        {
            // The source-side operation can finish its durable saga before a
            // response retry observes and completes the outer durable request.
            // A completed job created by this same pending admission is the
            // canonical success witness; do not reinterpret the exact replay
            // as a fresh rename after the source path has been evicted.
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
        self.refresh_control_plane_latest()?;
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
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let source_paths = Self::normalized_namespace_paths([source_path], false, "operation")?;
        let destination_paths =
            Self::normalized_namespace_paths([destination_path], true, "operation")?;
        let Some(shards) = self.metadata_shard_config_snapshot(file_system_id)? else {
            return Ok(None);
        };
        let source_shards = Self::namespace_shards_for_normalized_paths_from_snapshot(
            file_system_id,
            &shards,
            source_paths.iter().map(String::as_str),
        )?;
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
        let destination_shards = Self::namespace_shards_for_normalized_paths_from_snapshot(
            file_system_id,
            &shards,
            destination_paths.iter().map(String::as_str),
        )?;
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
