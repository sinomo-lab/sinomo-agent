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
        self.namespace_execution
            .replace_context(Some(DfsNamespaceOperationContext {
                file_system_id,
                shards,
                placement_config: None,
            }))
    }

    pub(crate) fn restore_namespace_operation_context(
        &mut self,
        previous: Option<DfsNamespaceOperationContext>,
    ) {
        self.namespace_execution.replace_context(previous);
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

    pub(crate) fn normalized_namespace_paths<'a>(
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
    /// the source primary or the exact bound source offer's coordinator sends
    /// them. The offer still needs native source/job custody at publication.
    /// Returns the rejection reason for an unauthorized sender, `None` when authorized or
    /// for non-adoption operations.
    pub(crate) fn adoption_source_authority_rejection(
        &mut self,
        file_system_id: &DfsFileSystemId,
        operation: &DfsNamespaceOperation,
        requester_node_id: &NodeId,
    ) -> Result<Option<String>, DfsError> {
        let source_path = match operation {
            // This validates only the exact already-published destination and
            // absence of active transfers. It cannot install or finalize data.
            DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. } => return Ok(None),
            DfsNamespaceOperation::AdoptObjectAtPath { source_path, .. }
            | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { source_path, .. }
            | DfsNamespaceOperation::InstallHardlinkTargetFence { source_path, .. }
            | DfsNamespaceOperation::CommitHardlinkTargetFence { source_path, .. }
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
            if let DfsNamespaceOperation::AdoptObjectAtPath {
                object,
                source_offer,
                ..
            } = operation
                && source_offer.source_config == shard
                && source_offer.source_node_id == shard.primary_node_id
                && source_offer.object_id == object.object_id
                && source_offer.destination == *object
                && &source_offer.job_offer.coordinator_node_id == requester_node_id
                && source_offer.job_offer.job_config.primary_node_id == *requester_node_id
                && source_offer.job_offer.job_config.file_system_id == *file_system_id
                && source_offer.job_id == source_offer.job_offer.job_id
            {
                // This is only sender admission. The target's ordinary prepare
                // must claim the live source offer and validate its full original
                // job, payload, epoch, and held writers before any effect.
                return Ok(None);
            }
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
    #[cfg(test)]
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
            DfsNamespaceOperation::BeginContentCopy { path, .. } => {
                Self::normalized_namespace_paths([path.as_str()], false, "file restore")?
            }
            DfsNamespaceOperation::ContentReference { request } => {
                Self::normalized_namespace_paths([request.path.as_str()], false, "file history")?
            }
            DfsNamespaceOperation::LookupPath { path, .. }
            | DfsNamespaceOperation::SyncPath { path }
            | DfsNamespaceOperation::ReconcilePlacement { path, .. }
            | DfsNamespaceOperation::AbortPendingWrite { path, .. }
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
                // The batch already includes every parent record it changes.
                // Adding those records' parents again would incorrectly pull
                // the root shard into a batch touching the /services directory.
                Self::normalized_namespace_paths(
                    transaction
                        .objects
                        .iter()
                        .map(|object| object.path.as_str()),
                    false,
                    "local-eventual batch",
                )?
            }
            _ => Self::normalized_namespace_paths(operation.involved_paths(), true, "operation")?,
        };
        Self::namespace_shards_for_normalized_paths_from_snapshot(
            file_system_id,
            shards,
            candidates.iter().map(String::as_str),
        )
    }

    /// Resolve the shard that stores the transaction's exact object records.
    ///
    /// Unlike affected-operation routing, this deliberately does not add each
    /// object's parent. A shard-boundary parent touch is deferred to that
    /// parent's authority and is not part of the child transaction.
    /// `shards` is the request's already validated authority subset, so it
    /// need not contain sibling or parent shards from the complete catalog.
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
        let mut selected = Vec::new();
        for path in candidates {
            let Some(shard) = crate::control_plane::select_metadata_shard_config_for_path(
                file_system_id,
                shards,
                &path,
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

    /// The shard(s) directly covering read-only namespace paths.
    ///
    /// Lookup and sync operations do not mutate parent directory records. If a
    /// split-prefix path and its parent are owned by different primaries, using
    /// the broader mutation shard set can make primaries redirect a read-only
    /// request back and forth forever.
    pub fn namespace_shards_for_exact_paths<'a>(
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
            #[cfg(test)]
            if let Some((root, _)) = self.native_publication_owner_for_observation() {
                crate::native_boundary_diagnostics::record(
                    &root,
                    "hardlink-coordination-depth",
                    || {
                        serde_json::json!({
                            "node": self.node_id, "existing_path": existing_path, "link_path": link_path,
                            "depth": self.routed_namespace_settle_depth,
                        })
                    },
                    true,
                );
            }
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
        if foreign_primary.is_none()
            && let Some(destination) = Self::namespace_shards_for_normalized_paths_from_snapshot(
                file_system_id,
                &shards,
                destination_paths.first().map(String::as_str),
            )?
            .first()
            && source_shards
                .iter()
                .any(|source| source.shard_id != destination.shard_id)
        {
            // One process may own different physical metadata shards. Their
            // writers and journal cuts still require the existing move saga;
            // sharing a node cannot make one local transaction atomic.
            return Ok(Some(destination.primary_node_id.clone()));
        }
        Ok(foreign_primary)
    }

    // Main callers retain an inspection-only job projection. The existing
    // authoritative read request checks the resident owner's current guards;
    // Core and served reads keep the local guard below.
    pub(crate) fn ensure_distributed_read_path_not_in_cross_shard_flight(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<(), DfsError> {
        if self.transport.delegates_local_namespace_authority() {
            return Ok(());
        }
        self.ensure_path_not_in_cross_shard_flight(file_system_id, path)
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
        let blocked = self
            .cross_shard_namespace_locks
            .blocks(file_system_id, &normalized);
        if blocked {
            #[cfg(test)]
            if let Some((root, _)) = self.native_publication_owner_for_observation() {
                crate::native_boundary_diagnostics::record(
                    &root,
                    "namespace-path-blocked",
                    || {
                        serde_json::json!({
                            "node": self.node_id, "file_system_id": file_system_id, "path": path,
                            "locks": format!("{:?}", self.cross_shard_namespace_locks),
                        })
                    },
                    true,
                );
            }
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
