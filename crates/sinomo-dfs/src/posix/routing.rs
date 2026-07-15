//! Routed namespace helpers for the POSIX facade.
//!
//! These helpers keep SIFS metadata-shard routing, bounded read freshness, and
//! routed mutation replay together. The parent `posix` module owns local
//! namespace operations and cross-shard sagas; this module decides when those
//! operations should run locally or be sent to another shard primary.

use super::{POSIX_READ_FRESHNESS_SYNC_TIMEOUT, POSIX_ROUTED_SYNC_RETRY_ROUNDS};
use crate::namespace::{SIFS_SERVICES_PATH, SIFS_USERS_PATH};
use crate::posix_model::{DfsPosixError, DfsStat, Result, posix_error_for_namespace_rejection};
use crate::posix_path::{
    normalized_posix_path_key, parent_posix_path, posix_path_is_at_or_under_prefix,
};
use crate::{
    DFS_ROUTED_NAMESPACE_OP_TIMEOUT, DfsControlPlaneStore, DfsError, DfsMessageTransport,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceJobKind, DfsNamespaceJobRecord,
    DfsNamespaceOperation, DfsNamespaceResult, DfsObjectKind, DfsObjectMetadata,
    DfsPosixCoordinationResult, NetworkedDfsService, PendingRoutedNamespaceResult,
    sifs_file_system_id,
};
use sinomo_core::node::NodeId;
use std::time::{Duration, Instant};

/// Routed-operation attempts when authority rejections redirect to the
/// current shard primary (a stale local shard view names a former primary).
const POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT: usize = 3;
const POSIX_ROUTED_DEEP_SYNC_ROUNDS: usize = 16;
const POSIX_READ_FRESHNESS_SYNC_CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RoutedNamespaceNetworkRecovery {
    Completed,
    Retry,
    Unchanged,
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// The metadata-shard primary to route a namespace mutation to, or `None`
    /// when this node is itself the namespace authority for the path (or no
    /// cluster shard configuration exists).
    pub(super) fn routed_namespace_primary_for_path(
        &mut self,
        path: &str,
    ) -> Result<Option<NodeId>> {
        Ok(self
            .routed_namespace_shard_for_path(path)?
            .map(|shard| shard.primary_node_id))
    }

    pub(super) fn routed_namespace_shard_for_path(
        &mut self,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>> {
        let shard = self.select_sifs_metadata_shard_for_path_fresh(path)?;
        Ok(match shard {
            Some(shard) if shard.primary_node_id != self.node_id => Some(shard),
            _ => None,
        })
    }

    fn select_sifs_metadata_shard_for_path_fresh(
        &mut self,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>> {
        let Some(shards) = self.refresh_sifs_metadata_shards()? else {
            return Ok(None);
        };
        self.select_sifs_metadata_shard_for_path_from_snapshot(path, &shards)
    }

    pub(super) fn refresh_sifs_metadata_shards(
        &mut self,
    ) -> Result<Option<Vec<DfsMetadataShardConfig>>> {
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let shards = {
            let kv = self
                .control_plane
                .as_mut()
                .expect("control plane checked before metadata shard refresh");
            DfsControlPlaneStore::new(kv.as_mut())
                .list_metadata_shards(&sifs_file_system_id())
                .map_err(DfsError::from)?
        };
        Ok(Some(shards))
    }

    pub(super) fn select_sifs_metadata_shard_for_path_from_snapshot(
        &self,
        path: &str,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Option<DfsMetadataShardConfig>> {
        let selected =
            crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &sifs_file_system_id(),
                shards,
                path,
            )
            .map_err(DfsError::from)?;
        self.reject_stale_default_sifs_root_fallback(path, selected.as_ref())?;
        Ok(selected)
    }

    fn reject_stale_default_sifs_root_fallback(
        &self,
        path: &str,
        selected: Option<&DfsMetadataShardConfig>,
    ) -> Result<()> {
        let Some((expected_shard_id, expected_prefix)) = default_sifs_split_shard_for_path(path)
        else {
            return Ok(());
        };
        if selected.is_some_and(|shard| shard.shard_id == expected_shard_id) {
            return Ok(());
        }
        let Some(metadata_backend) = self.metadata_backend.as_deref() else {
            return Ok(());
        };
        let expected_file_system_id = sifs_file_system_id();
        let local_has_expected_shard =
            metadata_backend
                .metadata_shard_statuses()?
                .iter()
                .any(|status| {
                    status.shard_id.0.as_str() == expected_shard_id
                        && status.file_system_id.as_ref() == Some(&expected_file_system_id)
                });
        if !local_has_expected_shard {
            return Ok(());
        }
        Err(DfsError::Distributed {
            message: format!(
                "raft node is busy at metadata shard config incomplete for {path}: missing default {expected_shard_id} shard for {expected_prefix}"
            ),
        }
        .into())
    }

    pub(super) fn routed_namespace_primary_for_path_from_snapshot(
        &self,
        path: &str,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Option<NodeId>> {
        Ok(
            match self.select_sifs_metadata_shard_for_path_from_snapshot(path, shards)? {
                Some(shard) if shard.primary_node_id != self.node_id => Some(shard.primary_node_id),
                _ => None,
            },
        )
    }

    fn routed_namespace_shard_for_path_from_snapshot(
        &self,
        path: &str,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Option<DfsMetadataShardConfig>> {
        Ok(
            match self.select_sifs_metadata_shard_for_path_from_snapshot(path, shards)? {
                Some(shard) if shard.primary_node_id != self.node_id => Some(shard),
                _ => None,
            },
        )
    }

    /// Resolve every path in one compound namespace decision from the same
    /// freshly loaded shard-config snapshot. A rename or hardlink must not pay
    /// one full control-plane prefix scan per side, and mixing revisions while
    /// classifying the two sides can produce a routing decision that never
    /// existed in the committed configuration.
    pub(crate) fn routed_namespace_primaries_for_paths_fresh<'a>(
        &mut self,
        paths: impl IntoIterator<Item = &'a str>,
    ) -> Result<Vec<Option<NodeId>>> {
        let paths = paths.into_iter().collect::<Vec<_>>();
        let Some(shards) = self.refresh_sifs_metadata_shards()? else {
            return Ok(vec![None; paths.len()]);
        };
        paths
            .into_iter()
            .map(|path| self.routed_namespace_primary_for_path_from_snapshot(path, &shards))
            .collect()
    }

    fn should_attempt_read_freshness_sync(&mut self, shard: &DfsMetadataShardConfig) -> bool {
        let now = Instant::now();
        self.recent_namespace_freshness_syncs
            .retain(|_, attempted_at| {
                now.duration_since(*attempted_at) < POSIX_READ_FRESHNESS_SYNC_CACHE_TTL
            });
        let key = (
            shard.file_system_id.clone(),
            DfsMetadataShardId::new(shard.shard_id.clone()),
            shard.primary_node_id.clone(),
        );
        match self.recent_namespace_freshness_syncs.get(&key) {
            Some(attempted_at)
                if now.duration_since(*attempted_at) < POSIX_READ_FRESHNESS_SYNC_CACHE_TTL =>
            {
                false
            }
            _ => {
                self.recent_namespace_freshness_syncs.insert(key, now);
                true
            }
        }
    }

    pub(super) fn is_local_metadata_replica_for_shard(
        &self,
        shard: &DfsMetadataShardConfig,
    ) -> bool {
        shard.primary_node_id == self.node_id || shard.replica_node_ids.contains(&self.node_id)
    }

    /// The local journal cursor of the shard owning `path`, used to detect
    /// whether a sync round actually advanced the local mirror.
    fn metadata_shard_cursor_for_path_from_snapshot(
        &self,
        path: &str,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<u64> {
        let Some(shard) = self.select_sifs_metadata_shard_for_path_from_snapshot(path, shards)?
        else {
            return Ok(0);
        };
        let shard_id = DfsMetadataShardId::new(shard.shard_id);
        Ok(self
            .local_metadata_journal_cursors
            .get(&shard_id)
            .copied()
            .unwrap_or(0))
    }

    /// Distributed path lookup with bounded mirror catch-up. Reads on a
    /// non-primary shard route a read-only lookup to the metadata primary
    /// before trusting a local replica hit, because the local replica may lag
    /// journal publication after another writer advances the path.
    /// Nodes outside a shard's replica set only use the SyncPath fallback when
    /// the read-only lookup cannot answer, keeping ordinary cold reads from
    /// materializing metadata locally.
    pub(super) fn lookup_sifs_path_distributed_synced(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<(String, Option<DfsObjectMetadata>)> {
        let mut resolved = self.resolve_existing_posix_path_distributed(path, follow_final)?;
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        for _ in 0..POSIX_ROUTED_SYNC_RETRY_ROUNDS {
            let local_object = self.lookup_sifs_path(&resolved)?;
            let Some(shard) = shard_snapshot
                .as_deref()
                .map(|shards| self.routed_namespace_shard_for_path_from_snapshot(&resolved, shards))
                .transpose()?
                .flatten()
            else {
                if let Some(object) = local_object
                    && object.kind != DfsObjectKind::Tombstone
                {
                    return Ok((resolved, Some(object)));
                }
                break;
            };
            if shard.primary_node_id != self.node_id {
                match self.route_namespace_lookup_path_to_metadata_primary(
                    sifs_file_system_id(),
                    shard.primary_node_id.clone(),
                    resolved.clone(),
                    POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
                ) {
                    Ok(Some(object)) if object.kind != DfsObjectKind::Tombstone => {
                        return Ok((resolved, Some(object)));
                    }
                    Ok(Some(object)) => return Ok((resolved, Some(object))),
                    Ok(None) => return Ok((resolved, None)),
                    Err(error) if namespace_lookup_error_is_uncorroborated_redirect(&error) => {
                        // The response was discarded, so refresh membership
                        // again and use no response-derived state. A current
                        // replica may preserve availability only for an
                        // already-live local hit; outsiders and missing or
                        // tombstoned local state must surface the integrity
                        // error rather than laundering it into NotFound.
                        let current_shard =
                            self.select_sifs_metadata_shard_for_path_fresh(&resolved)?;
                        if current_shard.as_ref().is_some_and(|current| {
                            self.is_local_metadata_replica_for_shard(current)
                        }) && local_object
                            .as_ref()
                            .is_some_and(|object| object.kind != DfsObjectKind::Tombstone)
                        {
                            return Ok((resolved, local_object));
                        }
                        return Err(error.into());
                    }
                    Err(error)
                        if self.is_local_metadata_replica_for_shard(&shard)
                            && !namespace_lookup_error_allows_local_replica_fallback(&error) =>
                    {
                        return Err(error.into());
                    }
                    Err(_) => {}
                }
            }
            if self.is_local_metadata_replica_for_shard(&shard)
                && let Some(object) = local_object.as_ref()
                && object.kind != DfsObjectKind::Tombstone
            {
                return Ok((resolved, local_object));
            }
            if let Some(object) = local_object
                && object.kind != DfsObjectKind::Tombstone
            {
                return Ok((resolved, Some(object)));
            }
            if self.is_local_metadata_replica_for_shard(&shard) {
                break;
            }
            if !self.should_attempt_read_freshness_sync(&shard) {
                break;
            }
            let cursor_before = self.metadata_shard_cursor_for_path_from_snapshot(
                &resolved,
                shard_snapshot.as_deref().unwrap_or_default(),
            )?;
            if self
                .route_namespace_operation_to_metadata_primary_with_timeout(
                    sifs_file_system_id(),
                    shard.primary_node_id,
                    DfsNamespaceOperation::SyncPath {
                        path: resolved.clone(),
                    },
                    POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
                )
                .is_err()
            {
                break;
            }
            if self.metadata_shard_cursor_for_path_from_snapshot(
                &resolved,
                shard_snapshot.as_deref().unwrap_or_default(),
            )? <= cursor_before
            {
                break;
            }
            resolved = self.resolve_existing_posix_path_distributed(path, follow_final)?;
        }
        let object = self.lookup_sifs_path(&resolved)?;
        Ok((resolved, object))
    }

    /// Routes a namespace mutation to the shard primary, blocks for the
    /// response, and maps a structured rejection back to the same
    /// `DfsPosixError` a local execution would have produced. On success the
    /// response journal batches are already replayed locally.
    ///
    /// A node routing on a stale shard view can address a former primary;
    /// the authority rejection names the current primary, and the operation
    /// is retried against it (bounded) instead of failing.
    pub(super) fn route_namespace_operation(
        &mut self,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<()> {
        let file_system_id = sifs_file_system_id();
        let mut primary = primary_node_id;
        for _ in 0..POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT {
            let routed = match self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                    true,
                    true,
                ) {
                Ok(result) => result,
                Err(error @ DfsError::Network { .. }) => {
                    match self.refresh_routed_namespace_primary_after_network_error(
                        &operation,
                        &mut primary,
                    )? {
                        RoutedNamespaceNetworkRecovery::Completed => return Ok(()),
                        RoutedNamespaceNetworkRecovery::Retry => continue,
                        RoutedNamespaceNetworkRecovery::Unchanged => {
                            return Err(DfsPosixError::Dfs(error));
                        }
                    }
                }
                Err(error) => return Err(DfsPosixError::Dfs(error)),
            };
            match routed.result.clone() {
                DfsNamespaceResult::Applied => {
                    validate_advisory_lock_migration_receipt(
                        &operation,
                        routed.posix_coordination_result.as_ref(),
                    )?;
                    self.finish_routed_namespace_catch_up_if_needed(&operation, &routed)?;
                    return Ok(());
                }
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    current_primary_node_id,
                } => match current_primary_node_id {
                    Some(current) if current == self.node_id => {
                        // Authority moved to this node; execute locally.
                        return self
                            .execute_posix_coordination_operation_local(&operation)
                            .map(drop);
                    }
                    Some(current) if current != primary => {
                        primary = self
                            .refreshed_primary_after_authority_redirect(
                                &file_system_id,
                                &operation,
                                &primary,
                                current,
                            )
                            .map_err(DfsPosixError::Dfs)?;
                        if primary == self.node_id {
                            return self
                                .execute_posix_coordination_operation_local(&operation)
                                .map(drop);
                        }
                    }
                    _ => return Err(posix_error_for_namespace_rejection(reason, posix_error)),
                },
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "routed namespace operation exceeded {POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        }))
    }

    pub(super) fn route_namespace_job_operation(
        &mut self,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<DfsNamespaceJobRecord> {
        let routed = self
            .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                sifs_file_system_id(),
                primary_node_id,
                operation.clone(),
                DFS_ROUTED_NAMESPACE_OP_TIMEOUT,
                true,
                true,
            )
            .map_err(DfsPosixError::Dfs)?;
        match routed.result.clone() {
            DfsNamespaceResult::Applied => {
                self.finish_routed_namespace_catch_up_if_needed(&operation, &routed)?;
                match routed.posix_coordination_result {
                    Some(DfsPosixCoordinationResult::NamespaceJob(job)) => Ok(job),
                    _ => Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: "bounded namespace job response omitted its typed receipt"
                            .to_string(),
                    })),
                }
            }
            DfsNamespaceResult::Rejected {
                reason,
                posix_error,
                ..
            } => Err(posix_error_for_namespace_rejection(reason, posix_error)),
        }
    }

    fn finish_routed_namespace_catch_up_if_needed(
        &mut self,
        operation: &DfsNamespaceOperation,
        routed: &PendingRoutedNamespaceResult,
    ) -> Result<()> {
        if !routed.journal_batch_limit_reached
            || matches!(
                operation,
                DfsNamespaceOperation::SyncPath { .. }
                    | DfsNamespaceOperation::LookupPath { .. }
                    | DfsNamespaceOperation::LookupPaths { .. }
            )
        {
            return Ok(());
        }
        let Some(path) = operation.routing_path() else {
            return Ok(());
        };
        self.sync_remote_namespace_path_until_quiet(path)
    }

    pub(super) fn route_namespace_operation_without_local_replay(
        &mut self,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<()> {
        let file_system_id = sifs_file_system_id();
        let mut primary = primary_node_id;
        for _ in 0..POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT {
            let routed = match self
                .route_namespace_operation_to_metadata_primary_without_local_replay(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                ) {
                Ok(result) => result,
                Err(error @ DfsError::Network { .. }) => {
                    match self.refresh_routed_namespace_primary_after_network_error(
                        &operation,
                        &mut primary,
                    )? {
                        RoutedNamespaceNetworkRecovery::Completed => return Ok(()),
                        RoutedNamespaceNetworkRecovery::Retry => continue,
                        RoutedNamespaceNetworkRecovery::Unchanged => {
                            return Err(DfsPosixError::Dfs(error));
                        }
                    }
                }
                Err(error) => return Err(DfsPosixError::Dfs(error)),
            };
            match routed {
                DfsNamespaceResult::Applied => return Ok(()),
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    current_primary_node_id,
                } => match current_primary_node_id {
                    Some(current) if current == self.node_id => {
                        return self
                            .execute_posix_coordination_operation_local(&operation)
                            .map(drop);
                    }
                    Some(current) if current != primary => {
                        primary = self
                            .refreshed_primary_after_authority_redirect(
                                &file_system_id,
                                &operation,
                                &primary,
                                current,
                            )
                            .map_err(DfsPosixError::Dfs)?;
                        if primary == self.node_id {
                            return self
                                .execute_posix_coordination_operation_local(&operation)
                                .map(drop);
                        }
                    }
                    _ => return Err(posix_error_for_namespace_rejection(reason, posix_error)),
                },
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "routed namespace operation exceeded {POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT} authority redirects"
            ),
        }))
    }

    fn refresh_routed_namespace_primary_after_network_error(
        &mut self,
        operation: &DfsNamespaceOperation,
        primary: &mut NodeId,
    ) -> Result<RoutedNamespaceNetworkRecovery> {
        let Some(path) = operation.routing_path() else {
            return Ok(RoutedNamespaceNetworkRecovery::Unchanged);
        };
        let Some(shard) = self.select_sifs_metadata_shard_for_path_fresh(path)? else {
            return Ok(RoutedNamespaceNetworkRecovery::Unchanged);
        };
        if shard.primary_node_id == self.node_id {
            self.execute_posix_coordination_operation_local(operation)?;
            return Ok(RoutedNamespaceNetworkRecovery::Completed);
        }
        if shard.primary_node_id != *primary {
            *primary = shard.primary_node_id;
            return Ok(RoutedNamespaceNetworkRecovery::Retry);
        }
        Ok(RoutedNamespaceNetworkRecovery::Unchanged)
    }

    /// Executes a routed namespace operation on the namespace authority.
    /// Callers have already validated shard primaryship; dispatching to the
    /// `_local` variants keeps execution from re-routing.
    pub(crate) fn execute_namespace_operation_local(
        &mut self,
        operation: &DfsNamespaceOperation,
    ) -> Result<()> {
        match operation {
            DfsNamespaceOperation::Rename { from, to } => {
                if self.resume_running_cross_shard_namespace_job_for_paths(
                    &sifs_file_system_id(),
                    from,
                    to,
                    &[
                        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                        DfsNamespaceJobKind::RenameDirectoryAcrossShardPrimaries,
                    ],
                )? {
                    return Ok(());
                }
            }
            DfsNamespaceOperation::Hardlink {
                existing_path,
                link_path,
            } => {
                if self.resume_running_cross_shard_namespace_job_for_paths(
                    &sifs_file_system_id(),
                    existing_path,
                    link_path,
                    &[DfsNamespaceJobKind::HardlinkAcrossShardPrimaries],
                )? {
                    return Ok(());
                }
            }
            _ => {}
        }
        // Mutations must not interleave with an in-flight cross-shard move of
        // the same path; the coordinator releases the lock when the move
        // completes or aborts.
        for path in operation.involved_paths() {
            if let Some(normalized) = normalized_posix_path_key(path) {
                self.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), &normalized)?;
            }
        }
        match operation {
            DfsNamespaceOperation::Mkdir { path } => self.posix_mkdir_local(path).map(drop),
            DfsNamespaceOperation::MkdirWithAttrs { path, attrs } => self
                .posix_mkdir_local_with_create_attrs(path, *attrs)
                .map(drop),
            DfsNamespaceOperation::MkdirAll { path } => self.posix_mkdir_all_local(path),
            DfsNamespaceOperation::Rmdir { path } => self.posix_rmdir_local(path).map(drop),
            DfsNamespaceOperation::Unlink { path } => self.posix_unlink_local(path).map(drop),
            DfsNamespaceOperation::Rename { from, to } => {
                self.posix_rename_local(from, to).map(drop)
            }
            DfsNamespaceOperation::RenameSingleNamespaceAtomic {
                from,
                to,
                expected_shard_id,
                expected_shard_epoch,
            } => self
                .posix_rename_single_namespace_atomic_local(
                    from,
                    to,
                    Some((expected_shard_id, *expected_shard_epoch)),
                )
                .map(drop),
            DfsNamespaceOperation::Hardlink {
                existing_path,
                link_path,
            } => self
                .posix_hardlink_local(existing_path, link_path)
                .map(drop),
            DfsNamespaceOperation::SetMode { path, mode } => {
                self.posix_set_mode_local(path, *mode).map(drop)
            }
            DfsNamespaceOperation::SetOwner { path, uid, gid } => {
                self.posix_set_owner_local(path, *uid, *gid).map(drop)
            }
            DfsNamespaceOperation::SetSymlinkOwner { path, uid, gid } => self
                .posix_set_symlink_owner_local(path, *uid, *gid)
                .map(drop),
            DfsNamespaceOperation::SetTimes {
                path,
                accessed_at_unix_nanos,
                modified_at_unix_nanos,
                follow_symlink,
            } => {
                if *follow_symlink {
                    self.posix_set_times_local(
                        path,
                        *accessed_at_unix_nanos,
                        *modified_at_unix_nanos,
                    )
                    .map(drop)
                } else {
                    self.posix_set_symlink_times_local(
                        path,
                        *accessed_at_unix_nanos,
                        *modified_at_unix_nanos,
                    )
                    .map(drop)
                }
            }
            DfsNamespaceOperation::TouchDirectoryModified { path } => {
                self.touch_directory_modified(path)
            }
            DfsNamespaceOperation::ClearDirRecursive { path } => {
                self.posix_clear_dir_recursive_local(path)
            }
            DfsNamespaceOperation::SyncPath { .. }
            | DfsNamespaceOperation::LookupPath { .. }
            | DfsNamespaceOperation::LookupPaths { .. } => Ok(()),
            DfsNamespaceOperation::ApplyTransaction { transaction } => {
                if !transaction.advisory_lock_states.is_empty() {
                    return Err(DfsPosixError::Dfs(DfsError::Distributed {
                        message: "generic namespace transactions may not install advisory-lock state; use the typed migration protocol"
                            .to_string(),
                    }));
                }
                self.apply_namespace_transaction(transaction.clone())
                    .map_err(DfsPosixError::from)
            }
            DfsNamespaceOperation::AdvisoryLock { .. }
            | DfsNamespaceOperation::FileSync { .. }
            | DfsNamespaceOperation::DirectorySync { .. }
            | DfsNamespaceOperation::RenameDirectoryBounded { .. }
            | DfsNamespaceOperation::ResumeNamespaceJobBounded { .. }
            | DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. } => {
                Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: "POSIX coordination operation bypassed its typed executor".to_string(),
                }))
            }
            DfsNamespaceOperation::AdoptObjectAtPath {
                object,
                versions,
                receipts,
                intents,
                conflicts,
                advisory_lock_state,
                source_path: _,
            } => self.adopt_object_at_path_local(
                object,
                versions,
                receipts,
                intents,
                conflicts,
                advisory_lock_state.as_ref(),
            ),
            DfsNamespaceOperation::AdoptHardlinkAlias {
                alias,
                source_path: _,
            } => self.adopt_hardlink_alias_local(alias),
        }
    }

    /// Local namespace mutations must not interleave with an in-flight
    /// cross-shard move of the same resolved path (the move's saga may be
    /// suspended awaiting recovery, with its lock still held).
    pub(super) fn ensure_resolved_path_not_in_cross_shard_flight(&self, path: &str) -> Result<()> {
        self.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), path)?;
        Ok(())
    }

    /// Stats the outcome of a routed namespace mutation. Bounded response
    /// replay may not have covered a deep catch-up; pull further journal
    /// batches from the path's primary until the mutation is visible.
    pub(super) fn stat_routed_namespace_result(
        &mut self,
        path: &str,
        follow: bool,
    ) -> Result<DfsStat> {
        for _ in 0..POSIX_ROUTED_SYNC_RETRY_ROUNDS {
            self.check_cancelled("routed stat sync")?;
            let attempt = if follow {
                self.posix_stat(path)
            } else {
                self.posix_lstat(path)
            };
            match attempt {
                Err(DfsPosixError::NotFound { .. }) => {
                    self.sync_remote_namespace_path(path)?;
                }
                result => return result,
            }
        }
        if follow {
            self.posix_stat(path)
        } else {
            self.posix_lstat(path)
        }
    }

    /// Tombstone stat for routed unlink/rmdir results, with the same bounded
    /// catch-up retry as `stat_routed_namespace_result`.
    pub(super) fn stat_replayed_tombstone_synced(&mut self, path: &str) -> Result<DfsStat> {
        for _ in 0..POSIX_ROUTED_SYNC_RETRY_ROUNDS {
            match self.stat_replayed_tombstone(path) {
                Err(DfsPosixError::NotFound { .. }) => {
                    self.sync_remote_namespace_path(path)?;
                }
                result => return result,
            }
        }
        self.stat_replayed_tombstone(path)
    }

    /// Replays a foreign primary's shard journal for a path this node just
    /// mutated through a coordinated cross-shard operation, restoring
    /// read-your-write for the foreign half.
    pub(super) fn sync_remote_namespace_path(&mut self, path: &str) -> Result<()> {
        if let Some(primary) = self.routed_namespace_primary_for_path(path)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SyncPath {
                    path: path.to_string(),
                },
            )?;
        }
        Ok(())
    }

    pub(super) fn sync_remote_namespace_path_until_quiet(&mut self, path: &str) -> Result<()> {
        let operation = DfsNamespaceOperation::SyncPath {
            path: path.to_string(),
        };
        let file_system_id = sifs_file_system_id();
        let mut primary = match self.routed_namespace_primary_for_path(path)? {
            Some(primary) => primary,
            None => return Ok(()),
        };
        let deadline = Instant::now() + POSIX_READ_FRESHNESS_SYNC_TIMEOUT;
        for _ in 0..POSIX_ROUTED_DEEP_SYNC_ROUNDS {
            self.check_cancelled("routed namespace sync")?;
            let now = Instant::now();
            if now >= deadline {
                return Err(DfsPosixError::Dfs(DfsError::Network {
                    message: format!(
                        "routed namespace sync for {path} did not settle within {}ms",
                        POSIX_READ_FRESHNESS_SYNC_TIMEOUT.as_millis()
                    ),
                }));
            }
            let routed = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    deadline.saturating_duration_since(now),
                    true,
                    true,
                )
                .map_err(DfsPosixError::Dfs)?;
            match routed.result {
                DfsNamespaceResult::Applied => {
                    if !routed.journal_batch_limit_reached {
                        return Ok(());
                    }
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current != primary => {
                    primary = self
                        .refreshed_primary_after_authority_redirect(
                            &file_system_id,
                            &operation,
                            &primary,
                            current,
                        )
                        .map_err(DfsPosixError::Dfs)?;
                    if primary == self.node_id {
                        return self.execute_namespace_operation_local(&operation);
                    }
                }
                DfsNamespaceResult::Rejected {
                    current_primary_node_id: Some(current),
                    ..
                } if current == self.node_id => {
                    return self.execute_namespace_operation_local(&operation);
                }
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    ..
                } => return Err(posix_error_for_namespace_rejection(reason, posix_error)),
            }
        }
        Err(DfsPosixError::Dfs(DfsError::Network {
            message: format!(
                "routed namespace sync for {path} reached {POSIX_ROUTED_DEEP_SYNC_ROUNDS} batches before the journal settled"
            ),
        }))
    }

    /// Touches the parent directory mtime when this node is the metadata
    /// authority for the parent's shard; foreign-shard parents are journaled
    /// by their own primaries.
    pub(super) fn touch_parent_directory_if_local_primary(&mut self, path: &str) -> Result<()> {
        let Some(parent) = parent_posix_path(path) else {
            return Ok(());
        };
        if self.routed_namespace_primary_for_path(&parent)?.is_some() {
            return Ok(());
        }
        self.touch_directory_modified(&parent)
    }
}

fn default_sifs_split_shard_for_path(path: &str) -> Option<(&'static str, &'static str)> {
    if posix_path_is_at_or_under_prefix(path, SIFS_SERVICES_PATH) {
        Some(("services", SIFS_SERVICES_PATH))
    } else if posix_path_is_at_or_under_prefix(path, SIFS_USERS_PATH) {
        Some(("users", SIFS_USERS_PATH))
    } else {
        None
    }
}

fn namespace_lookup_error_allows_local_replica_fallback(error: &DfsError) -> bool {
    matches!(
        error,
        DfsError::Distributed { message }
            if message.starts_with("routed namespace lookup exceeded ")
                && message.ends_with(" authority redirects")
    )
}

fn namespace_lookup_error_is_uncorroborated_redirect(error: &DfsError) -> bool {
    matches!(
        error,
        DfsError::Distributed { message }
            if message.starts_with("routed namespace authority redirect is uncorroborated:")
    )
}

fn validate_advisory_lock_migration_receipt(
    operation: &DfsNamespaceOperation,
    result: Option<&DfsPosixCoordinationResult>,
) -> Result<()> {
    if let DfsNamespaceOperation::AdvisoryLockMigrationPrepare {
        object_id,
        state_fingerprint,
        ..
    } = operation
    {
        if matches!(
            result,
            Some(DfsPosixCoordinationResult::AdvisoryLockMigrationPrepared {
                object_id: prepared_object_id,
                state_fingerprint: prepared_fingerprint,
            }) if prepared_object_id == object_id && prepared_fingerprint == state_fingerprint
        ) {
            return Ok(());
        }
        return Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "destination does not support exact advisory-lock migration preparation for {object_id}"
            ),
        }));
    }
    let DfsNamespaceOperation::AdoptObjectAtPath {
        object,
        advisory_lock_state: Some(source_state),
        ..
    } = operation
    else {
        return Ok(());
    };
    let (_, expected_fingerprint) =
        crate::advisory_lock::prepare_advisory_lock_state_for_migration(source_state).map_err(
            |source| {
                DfsPosixError::Dfs(DfsError::Serialization {
                    message: format!(
                        "encode advisory-lock migration receipt for {}: {source}",
                        object.object_id
                    ),
                })
            },
        )?;
    if matches!(
        result,
        Some(DfsPosixCoordinationResult::AdvisoryLockMigration {
            object_id,
            state_fingerprint,
            durable_through_sequence,
        }) if object_id == &object.object_id
            && state_fingerprint == &expected_fingerprint
            && *durable_through_sequence > 0
    ) {
        return Ok(());
    }
    Err(DfsPosixError::Dfs(DfsError::Distributed {
        message: format!(
            "destination did not return an exact quorum-durable advisory-lock migration receipt for {}; retry after every involved metadata authority supports lock-state adoption",
            object.object_id
        ),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsAdvisoryLockStateRecord, directory_object};

    #[test]
    fn lock_bearing_adoption_requires_an_exact_typed_receipt() {
        let owner = NodeId::from_legacy_named("node_lock_migration_receipt").expect("node");
        let object = directory_object("/moved", owner);
        let state = DfsAdvisoryLockStateRecord::empty(
            object.file_system_id.clone(),
            object.object_id.clone(),
            7,
        );
        let operation = DfsNamespaceOperation::AdoptObjectAtPath {
            object: object.clone(),
            source_path: "/source".to_string(),
            versions: Vec::new(),
            receipts: Vec::new(),
            intents: Vec::new(),
            conflicts: Vec::new(),
            advisory_lock_state: Some(state.clone()),
        };
        assert!(validate_advisory_lock_migration_receipt(&operation, None).is_err());

        let mut rebound = state;
        rebound.authority_rebind_required = true;
        let fingerprint = crate::advisory_lock::advisory_lock_state_fingerprint(&rebound)
            .expect("state fingerprint");
        validate_advisory_lock_migration_receipt(
            &operation,
            Some(&DfsPosixCoordinationResult::AdvisoryLockMigration {
                object_id: object.object_id,
                state_fingerprint: fingerprint,
                durable_through_sequence: 1,
            }),
        )
        .expect("exact receipt");
    }
}
