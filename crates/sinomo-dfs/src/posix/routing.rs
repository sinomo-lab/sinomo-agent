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
    DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceJobKind, DfsNamespaceOperation,
    DfsNamespaceResult, DfsObjectKind, DfsObjectMetadata, NetworkedDfsService,
    PendingRoutedNamespaceResult, sifs_file_system_id,
};
use sinomo_core::node::NodeId;
use std::time::{Duration, Instant};

const POSIX_SIFS_METADATA_SHARD_CACHE_TTL: Duration = Duration::from_secs(1);
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

    fn routed_namespace_shard_for_path_cached(
        &mut self,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>> {
        let shard = self.select_sifs_metadata_shard_for_path_cached(path)?;
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
        self.cached_sifs_metadata_shards = Some(crate::CachedSifsMetadataShards {
            recorded_at: Instant::now(),
            shards: shards.clone(),
        });
        Ok(Some(shards))
    }

    fn select_sifs_metadata_shard_for_path_from_snapshot(
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

    fn select_sifs_metadata_shard_for_path_cached(
        &mut self,
        path: &str,
    ) -> Result<Option<DfsMetadataShardConfig>> {
        if self.control_plane.is_none() {
            return Ok(None);
        }
        let now = Instant::now();
        let shards = if let Some(cache) = self.cached_sifs_metadata_shards.as_ref()
            && now.duration_since(cache.recorded_at) < POSIX_SIFS_METADATA_SHARD_CACHE_TTL
        {
            cache.shards.clone()
        } else {
            let shards = {
                let kv = self
                    .control_plane
                    .as_mut()
                    .expect("control plane checked before metadata shard cache refresh");
                DfsControlPlaneStore::new(kv.as_mut())
                    .list_metadata_shards(&sifs_file_system_id())
                    .map_err(DfsError::from)?
            };
            self.cached_sifs_metadata_shards = Some(crate::CachedSifsMetadataShards {
                recorded_at: now,
                shards: shards.clone(),
            });
            shards
        };
        Ok(
            crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
                &sifs_file_system_id(),
                &shards,
                path,
            )
            .map_err(DfsError::from)?,
        )
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
    fn metadata_shard_cursor_for_path(&mut self, path: &str) -> Result<u64> {
        let Some(shard) = self.select_sifs_metadata_shard_for_path_cached(path)? else {
            return Ok(0);
        };
        let shard_id = DfsMetadataShardId::new(shard.shard_id);
        Ok(self
            .local_metadata_shard_cursors()?
            .into_iter()
            .find(|cursor| cursor.shard_id == shard_id)
            .map(|cursor| cursor.after_sequence)
            .unwrap_or(0))
    }

    /// Distributed path lookup with bounded mirror catch-up. Reads serve local
    /// hits directly and use a read-only primary lookup for local misses.
    /// Nodes outside a shard's replica set only use the SyncPath fallback when
    /// the read-only lookup cannot answer, keeping ordinary cold reads from
    /// materializing metadata locally.
    pub(super) fn lookup_sifs_path_distributed_synced(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<(String, Option<DfsObjectMetadata>)> {
        let mut resolved = self.resolve_existing_posix_path_distributed(path, follow_final)?;
        // Non-replica mirrors can be stale in BOTH directions because they do
        // not receive the pushed journal stream. A bounded best-effort pull
        // remains available there, but configured replicas keep ordinary
        // reads free of local metadata writes.
        if let Some(shard) = self.routed_namespace_shard_for_path_cached(&resolved)?
            && !self.is_local_metadata_replica_for_shard(&shard)
            && self.should_attempt_read_freshness_sync(&shard)
        {
            if self
                .route_namespace_operation_to_metadata_primary_with_timeout(
                    sifs_file_system_id(),
                    shard.primary_node_id,
                    DfsNamespaceOperation::SyncPath {
                        path: resolved.clone(),
                    },
                    POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
                )
                .is_ok()
            {
                resolved = self.resolve_existing_posix_path_distributed(path, follow_final)?;
            }
        }
        for _ in 0..POSIX_ROUTED_SYNC_RETRY_ROUNDS {
            let local_object = self.lookup_sifs_path(&resolved)?;
            let Some(shard) = self.routed_namespace_shard_for_path_cached(&resolved)? else {
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
                    Err(error)
                        if self.is_local_metadata_replica_for_shard(&shard)
                            && !namespace_lookup_error_allows_local_replica_fallback(&error) =>
                    {
                        return Err(error.into());
                    }
                    Err(_) => {}
                }
            }
            if let Some(object) = local_object
                && object.kind != DfsObjectKind::Tombstone
            {
                return Ok((resolved, Some(object)));
            }
            if self.is_local_metadata_replica_for_shard(&shard) {
                break;
            }
            let cursor_before = self.metadata_shard_cursor_for_path(&resolved)?;
            self.sync_remote_namespace_path(&resolved)?;
            if self.metadata_shard_cursor_for_path(&resolved)? <= cursor_before {
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
                        return self.execute_namespace_operation_local(&operation);
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
                            return self.execute_namespace_operation_local(&operation);
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
        let Some(path) = namespace_operation_routing_path(operation) else {
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
                        return self.execute_namespace_operation_local(&operation);
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
                            return self.execute_namespace_operation_local(&operation);
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
        self.cached_sifs_metadata_shards = None;
        let Some(path) = namespace_operation_routing_path(operation) else {
            return Ok(RoutedNamespaceNetworkRecovery::Unchanged);
        };
        let Some(shard) = self.select_sifs_metadata_shard_for_path_fresh(path)? else {
            return Ok(RoutedNamespaceNetworkRecovery::Unchanged);
        };
        if shard.primary_node_id == self.node_id {
            self.execute_namespace_operation_local(operation)?;
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
            DfsNamespaceOperation::MkdirAll { path } => self.posix_mkdir_all_local(path),
            DfsNamespaceOperation::Rmdir { path } => self.posix_rmdir_local(path).map(drop),
            DfsNamespaceOperation::Unlink { path } => self.posix_unlink_local(path).map(drop),
            DfsNamespaceOperation::Rename { from, to } => {
                self.posix_rename_local(from, to).map(drop)
            }
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
            DfsNamespaceOperation::ApplyTransaction { transaction } => self
                .apply_namespace_transaction(transaction.clone())
                .map_err(DfsPosixError::from),
            DfsNamespaceOperation::AdoptObjectAtPath {
                object,
                versions,
                receipts,
                intents,
                conflicts,
                source_path: _,
            } => self.adopt_object_at_path_local(object, versions, receipts, intents, conflicts),
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
        for _ in 0..POSIX_ROUTED_DEEP_SYNC_ROUNDS {
            let routed = self
                .route_namespace_operation_to_metadata_primary_with_timeout_and_options(
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
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
        Ok(())
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

fn namespace_operation_routing_path(operation: &DfsNamespaceOperation) -> Option<&str> {
    match operation {
        DfsNamespaceOperation::Mkdir { path }
        | DfsNamespaceOperation::MkdirAll { path }
        | DfsNamespaceOperation::Rmdir { path }
        | DfsNamespaceOperation::Unlink { path }
        | DfsNamespaceOperation::SetMode { path, .. }
        | DfsNamespaceOperation::SetOwner { path, .. }
        | DfsNamespaceOperation::SetTimes { path, .. }
        | DfsNamespaceOperation::TouchDirectoryModified { path }
        | DfsNamespaceOperation::ClearDirRecursive { path }
        | DfsNamespaceOperation::SyncPath { path }
        | DfsNamespaceOperation::LookupPath { path } => Some(path.as_str()),
        DfsNamespaceOperation::LookupPaths { paths } => paths.first().map(String::as_str),
        DfsNamespaceOperation::Rename { from, .. } => Some(from.as_str()),
        DfsNamespaceOperation::Hardlink { existing_path, .. } => Some(existing_path.as_str()),
        DfsNamespaceOperation::ApplyTransaction { transaction } => transaction
            .objects
            .first()
            .map(|object| object.path.as_str()),
        DfsNamespaceOperation::AdoptObjectAtPath { object, .. } => Some(object.path.as_str()),
        DfsNamespaceOperation::AdoptHardlinkAlias { alias, .. } => Some(alias.path.as_str()),
    }
}
