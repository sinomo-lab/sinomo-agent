//! Routed namespace helpers for the POSIX facade.
//!
//! These helpers keep SIFS metadata-shard routing, bounded read freshness, and
//! routed mutation replay together. The parent `posix` module owns local
//! namespace operations and cross-shard sagas; this module decides when those
//! operations should run locally or be sent to another shard primary.

use super::POSIX_READ_FRESHNESS_SYNC_TIMEOUT;
use crate::namespace::{SIFS_SERVICES_PATH, SIFS_USERS_PATH};
use crate::posix_model::{DfsPosixError, DfsStat, Result, posix_error_for_namespace_rejection};
use crate::posix_path::{
    normalize_posix_path, normalized_posix_path_key, parent_posix_path,
    posix_path_is_at_or_under_prefix,
};
use crate::{
    DFS_NO_CALLER_BUDGET, DfsControlPlaneStore, DfsError, DfsMessageTransport,
    DfsMetadataShardConfig, DfsNamespaceJobKind, DfsNamespaceJobRecord, DfsNamespaceOperation,
    DfsNamespaceResult, DfsObjectKind, DfsObjectMetadata, DfsPosixCoordinationResult,
    DfsReadRangeData, NetworkedDfsService, PendingRoutedNamespaceResult, sifs_file_system_id,
};
use sinomo_core::node::NodeId;

/// Routed-operation attempts when authority rejections redirect to the
/// current shard primary (a stale local shard view names a former primary).
const POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RoutedNamespaceNetworkRecovery {
    Completed,
    Retry,
    Unchanged,
}

/// Possible owners of an immediate directory child under one exact catalog.
/// A prefix boundary can own that child; deeper disjoint prefixes cannot.
/// Hash assignments may select any child and retain the root fallback.
pub(crate) fn directory_child_authority_shards<'a>(
    path: &str,
    configs: &'a [DfsMetadataShardConfig],
) -> Result<Vec<&'a DfsMetadataShardConfig>> {
    let path = normalize_posix_path(path)?;
    let directory_owner = crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
        &sifs_file_system_id(),
        configs,
        &path,
    )
    .map_err(DfsError::from)?
    .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;
    Ok(configs
        .iter()
        .filter(|shard| {
            shard.shard_id == directory_owner.shard_id
                || match &shard.assignment {
                    crate::DfsMetadataShardAssignment::PathPrefix { prefix } => {
                        normalize_posix_path(prefix).is_ok_and(|prefix| {
                            parent_posix_path(&prefix).as_deref() == Some(path.as_str())
                        })
                    }
                    crate::DfsMetadataShardAssignment::NameHashRange { .. } => true,
                    crate::DfsMetadataShardAssignment::Root => matches!(
                        &directory_owner.assignment,
                        crate::DfsMetadataShardAssignment::NameHashRange { .. }
                    ),
                }
        })
        .collect())
}

fn namespace_operation_uses_stable_admission(operation: &DfsNamespaceOperation) -> bool {
    matches!(
        operation,
        DfsNamespaceOperation::Mkdir { .. }
            | DfsNamespaceOperation::MkdirWithAttrs { .. }
            | DfsNamespaceOperation::MkdirAtNoLinks { .. }
            | DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(_)
            | DfsNamespaceOperation::MkdirAll { .. }
            | DfsNamespaceOperation::ConditionalUnlink { .. }
            | DfsNamespaceOperation::ConditionalRmdir { .. }
            | DfsNamespaceOperation::ConditionalRename { .. }
    )
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// A live assignment-boundary directory needs no child/parent mutation.
    /// Observe it at its current owner before requesting that wider footprint.
    /// None leaves ordinary paths unchanged; Some(false) is authoritative
    /// evidence against accepting a cached local directory after a failure.
    pub(super) fn routed_mkdir_boundary_directory_exists(
        &mut self,
        path: &str,
        shard: &DfsMetadataShardConfig,
    ) -> Result<Option<bool>> {
        let crate::DfsMetadataShardAssignment::PathPrefix { prefix } = &shard.assignment else {
            return Ok(None);
        };
        if normalize_posix_path(prefix)? != path {
            return Ok(None);
        }
        let object = self.route_namespace_lookup_path_to_metadata_primary(
            sifs_file_system_id(),
            shard.primary_node_id.clone(),
            path.to_owned(),
            POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
        )?;
        Ok(Some(object.is_some_and(|object| {
            object.kind == DfsObjectKind::Directory
        })))
    }

    /// Run one local-primary read against a single durable metadata cut.
    ///
    /// A short-lived client backend may share shard files with the resident
    /// daemon. The first snapshot refresh observes any commit that completed
    /// before this read, after which the component walk can safely reuse the
    /// resident tables. Without this scope, every path component repeats the
    /// checkpoint/snapshot/log generation probes; that is especially costly
    /// on NTFS. Remote-primary reads retain their authority lookup unchanged.
    pub(super) fn with_local_primary_metadata_read_snapshot<R>(
        &mut self,
        path: &str,
        operation: impl FnOnce(&mut Self, Option<&[DfsMetadataShardConfig]>) -> Result<R>,
    ) -> Result<R> {
        self.with_local_primary_read_snapshot(path, false, operation)
    }

    pub(super) fn with_local_primary_stat_read_snapshot<R>(
        &mut self,
        path: &str,
        operation: impl FnOnce(&mut Self, Option<&[DfsMetadataShardConfig]>) -> Result<R>,
    ) -> Result<R> {
        self.with_local_primary_read_snapshot(path, true, operation)
    }

    fn with_local_primary_read_snapshot<R>(
        &mut self,
        path: &str,
        stat: bool,
        operation: impl FnOnce(&mut Self, Option<&[DfsMetadataShardConfig]>) -> Result<R>,
    ) -> Result<R> {
        let path = normalize_posix_path(path)?;
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        let local_primary = match shard_snapshot.as_deref() {
            Some(shards) => self
                .routed_namespace_primary_for_path_from_snapshot(&path, shards)?
                .is_none(),
            None => true,
        };
        let pinned = local_primary && self.metadata_backend.is_some();
        if pinned {
            let backend = self
                .metadata_backend
                .as_mut()
                .expect("metadata backend presence checked before read snapshot");
            if stat {
                backend.begin_stat_read_snapshot_for_path(&sifs_file_system_id(), &path)
            } else {
                backend.begin_consistent_read_snapshot_for_path(&sifs_file_system_id(), &path)
            }
            .map_err(DfsPosixError::from)?;
        }

        let enclosing = std::mem::replace(&mut self.read_shard_snapshot, shard_snapshot.clone());
        let result = operation(self, shard_snapshot.as_deref());
        self.read_shard_snapshot = enclosing;
        if pinned {
            let backend = self
                .metadata_backend
                .as_mut()
                .expect("pinned metadata backend remains installed");
            if stat {
                backend.end_stat_read_snapshot_for_path(&sifs_file_system_id(), &path);
            } else {
                backend.end_consistent_read_snapshot_for_path(&sifs_file_system_id(), &path);
            }
        }
        result
    }

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
            Some(shard)
                if shard.primary_node_id != self.node_id
                    || self.transport.delegates_local_namespace_authority() =>
            {
                Some(shard)
            }
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
        // An explicit nested prefix or hash assignment is a valid authority.
        // This compatibility guard rejects only a fallback to the root when
        // the default split is known locally but missing from the projection.
        if selected.is_some_and(|shard| {
            shard.shard_id == expected_shard_id
                || !matches!(shard.assignment, crate::DfsMetadataShardAssignment::Root)
        }) {
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
                Some(shard)
                    if shard.primary_node_id != self.node_id
                        || self.transport.delegates_local_namespace_authority() =>
                {
                    Some(shard.primary_node_id)
                }
                _ => None,
            },
        )
    }

    pub(super) fn routed_namespace_shard_for_path_from_snapshot(
        &self,
        path: &str,
        shards: &[DfsMetadataShardConfig],
    ) -> Result<Option<DfsMetadataShardConfig>> {
        Ok(
            match self.select_sifs_metadata_shard_for_path_from_snapshot(path, shards)? {
                Some(shard)
                    if shard.primary_node_id != self.node_id
                        || self.transport.delegates_local_namespace_authority() =>
                {
                    Some(shard)
                }
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

    pub(super) fn is_local_metadata_replica_for_shard(
        &self,
        shard: &DfsMetadataShardConfig,
    ) -> bool {
        (shard.primary_node_id == self.node_id || shard.replica_node_ids.contains(&self.node_id))
            && !self.metadata_lease_omits_shard(&shard.shard_id)
    }

    /// Whether this service answers reads of `path` from its own metadata:
    /// the path's shard is one it replicates, or no shard covers the path.
    pub(crate) fn local_metadata_answers_path_from_snapshot(
        &self,
        path: &str,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<bool> {
        Ok(shard_snapshot
            .map(|shards| self.routed_namespace_shard_for_path_from_snapshot(path, shards))
            .transpose()?
            .flatten()
            .is_none_or(|shard| self.reads_shard_from_local_metadata(&shard)))
    }

    /// Whether a write to `path` plans against this service's own metadata.
    /// Only a service that reads the path's shard locally does; any other
    /// reads the current object and parent from the shard's authority.
    pub(super) fn plans_write_from_local_metadata(&mut self, path: &str) -> Result<bool> {
        if self.metadata_backend.is_none() && self.transport.delegates_local_namespace_authority() {
            return Ok(self.control_plane.is_none());
        }
        let shards = self.refresh_sifs_metadata_shards()?;
        self.local_metadata_answers_path_from_snapshot(path, shards.as_deref())
    }

    /// Whether this service answers reads of `shard` from its own metadata.
    /// Only the primary and replicas follow the shard's journal; any other
    /// node, and a caller delegating to its Main, reads through the primary.
    pub(crate) fn reads_shard_from_local_metadata(&self, shard: &DfsMetadataShardConfig) -> bool {
        !self.transport.delegates_local_namespace_authority()
            && self.is_local_metadata_replica_for_shard(shard)
    }

    /// Distributed path lookup. A replica that is not the primary asks the
    /// primary before trusting its local hit, because the replica may lag
    /// journal publication after another writer advances the path. A node
    /// outside the shard's replica set answers only from the primary.
    pub(super) fn lookup_sifs_path_distributed_synced(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<(String, Option<DfsObjectMetadata>)> {
        self.with_metadata_lookup_cache_mut(|service| {
            service
                .lookup_sifs_path_distributed_synced_with_cache(path, follow_final, None)
                .map(|(path, object, _)| (path, object))
        })
    }

    pub(super) fn lookup_sifs_path_distributed_synced_from_snapshot(
        &mut self,
        path: &str,
        follow_final: bool,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<(String, Option<DfsObjectMetadata>)> {
        self.with_metadata_lookup_cache_mut(|service| {
            service
                .lookup_sifs_path_distributed_synced_with_cache_from_snapshot(
                    path,
                    follow_final,
                    None,
                    shard_snapshot,
                )
                .map(|(path, object, _)| (path, object))
        })
    }

    pub(super) fn lookup_sifs_path_distributed_synced_with_inline_range_from_snapshot(
        &mut self,
        path: &str,
        follow_final: bool,
        offset: u64,
        max_bytes: usize,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<(String, Option<DfsObjectMetadata>, Option<DfsReadRangeData>)> {
        self.with_metadata_lookup_cache_mut(|service| {
            service.lookup_sifs_path_distributed_synced_with_cache_from_snapshot(
                path,
                follow_final,
                Some((offset, max_bytes)),
                shard_snapshot,
            )
        })
    }

    fn lookup_sifs_path_distributed_synced_with_cache(
        &mut self,
        path: &str,
        follow_final: bool,
        inline_range: Option<(u64, usize)>,
    ) -> Result<(String, Option<DfsObjectMetadata>, Option<DfsReadRangeData>)> {
        let resolved = self.resolve_existing_posix_path_distributed(path, follow_final)?;
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        self.lookup_sifs_path_distributed_synced_with_resolved_snapshot(
            path,
            follow_final,
            inline_range,
            resolved,
            shard_snapshot.as_deref(),
        )
    }

    fn lookup_sifs_path_distributed_synced_with_cache_from_snapshot(
        &mut self,
        path: &str,
        follow_final: bool,
        inline_range: Option<(u64, usize)>,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<(String, Option<DfsObjectMetadata>, Option<DfsReadRangeData>)> {
        let resolved = self.resolve_existing_posix_path_distributed(path, follow_final)?;
        self.lookup_sifs_path_distributed_synced_with_resolved_snapshot(
            path,
            follow_final,
            inline_range,
            resolved,
            shard_snapshot,
        )
    }

    fn lookup_sifs_path_distributed_synced_with_resolved_snapshot(
        &mut self,
        _path: &str,
        _follow_final: bool,
        inline_range: Option<(u64, usize)>,
        resolved: String,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<(String, Option<DfsObjectMetadata>, Option<DfsReadRangeData>)> {
        let Some(shard) = shard_snapshot
            .map(|shards| self.routed_namespace_shard_for_path_from_snapshot(&resolved, shards))
            .transpose()?
            .flatten()
        else {
            if self.transport.delegates_local_namespace_authority() {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "Main caller lookup has no current metadata shard authority".into(),
                }));
            }
            let object = self.lookup_sifs_path_without_shard_authority(&resolved)?;
            return Ok((resolved, object, None));
        };
        if !self.reads_shard_from_local_metadata(&shard) {
            let (object, range) = self.route_namespace_lookup_to_metadata_primary(
                shard.primary_node_id.clone(),
                &resolved,
                inline_range,
            )?;
            let object = self.record_selected_synced_path(&resolved, object);
            return Ok((resolved, object, range));
        }
        let local_object = self.lookup_sifs_path_speculatively(&resolved)?;
        if shard.primary_node_id != self.node_id {
            match self.route_namespace_lookup_to_metadata_primary(
                shard.primary_node_id.clone(),
                &resolved,
                inline_range,
            ) {
                Ok((object, range)) => {
                    let object = self.record_selected_synced_path(&resolved, object);
                    return Ok((resolved, object, range));
                }
                Err(error) => {
                    if self.replica_redirect_keeps_local_object(&resolved, &error, &local_object)? {
                        let object = self.record_selected_synced_path(&resolved, local_object);
                        return Ok((resolved, object, None));
                    }
                    if !namespace_lookup_error_allows_local_replica_fallback(&error) {
                        return Err(error.into());
                    }
                }
            }
        }
        if let Some(object) = local_object
            .as_ref()
            .filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            let object = self.record_selected_synced_path(&resolved, Some(object.clone()));
            return Ok((resolved, object, None));
        }
        let object = self.lookup_sifs_path(&resolved)?;
        Ok((resolved, object, None))
    }

    /// One read-only lookup at the shard primary, optionally carrying an
    /// inline range of the object's content.
    fn route_namespace_lookup_to_metadata_primary(
        &mut self,
        primary_node_id: NodeId,
        path: &str,
        inline_range: Option<(u64, usize)>,
    ) -> std::result::Result<(Option<DfsObjectMetadata>, Option<DfsReadRangeData>), DfsError> {
        if let Some((offset, max_bytes)) = inline_range {
            self.route_namespace_lookup_path_range_to_metadata_primary(
                sifs_file_system_id(),
                primary_node_id,
                path.to_string(),
                offset,
                max_bytes,
                POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
            )
        } else {
            self.route_namespace_lookup_path_to_metadata_primary(
                sifs_file_system_id(),
                primary_node_id,
                path.to_string(),
                POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
            )
            .map(|object| (object, None))
        }
    }

    /// A replica's local read, taken outside the caller's planning predicate
    /// so an unused fallback does not constrain the request.
    fn lookup_sifs_path_speculatively(&self, path: &str) -> Result<Option<DfsObjectMetadata>> {
        let speculative = self.namespace_execution.begin_independent_planning();
        let local = self.lookup_sifs_path(path);
        drop(speculative);
        local
    }

    /// Without shard authority for the path, the node-local namespace answers.
    fn lookup_sifs_path_without_shard_authority(
        &self,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let local_object = self.lookup_sifs_path_speculatively(path)?;
        if let Some(object) = local_object.filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            return Ok(self.record_selected_synced_path(path, Some(object)));
        }
        self.lookup_sifs_path(path)
    }

    /// An uncorroborated redirect from the primary is honored only by a node
    /// that is still a current replica and holds a live local object. The
    /// response was discarded, so membership is refreshed again and no
    /// response-derived state is used; missing or tombstoned local state must
    /// surface the integrity error rather than becoming NotFound.
    fn replica_redirect_keeps_local_object(
        &mut self,
        path: &str,
        error: &DfsError,
        local_object: &Option<DfsObjectMetadata>,
    ) -> Result<bool> {
        if !namespace_lookup_error_is_uncorroborated_redirect(error)
            || !local_object
                .as_ref()
                .is_some_and(|object| object.kind != DfsObjectKind::Tombstone)
        {
            return Ok(false);
        }
        let current_shard = self.select_sifs_metadata_shard_for_path_fresh(path)?;
        Ok(current_shard
            .as_ref()
            .is_some_and(|current| self.is_local_metadata_replica_for_shard(current)))
    }

    /// Fetch one exact normalized namespace path from its current metadata
    /// authority without applying POSIX symbolic- or hard-link resolution.
    ///
    /// Capability-style callers use this as the primitive for an `openat2`
    /// `RESOLVE_NO_SYMLINKS` walk. Keeping the freshness/routing behavior here
    /// prevents those callers from falling back to a stale local mirror merely
    /// to avoid the ordinary path resolver.
    pub(super) fn lookup_sifs_path_direct_distributed_synced(
        &mut self,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        self.lookup_sifs_path_direct_distributed_synced_from_snapshot(
            path,
            shard_snapshot.as_deref(),
        )
    }

    /// Fetch the components of one path walk that only their shard authority
    /// can answer, one batched lookup per shard instead of one routed lookup
    /// per component. Answers are retained for the active lookup-cache scope
    /// and consumed by the exact-path lookup, which still records each
    /// component the walk actually selects. The final component is included
    /// even when the walk does not follow it, because its caller selects that
    /// exact entry next. This is purely an optimization:
    /// without a scope, with dot components, or when a batch fails, the walk
    /// routes its components individually and reports their errors itself.
    pub(super) fn prefetch_routed_walk_components(
        &mut self,
        components: &[String],
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) {
        let Some(shards) = shard_snapshot else {
            return;
        };
        if !self.metadata_lookup_cache_is_active()
            || components
                .iter()
                .any(|component| matches!(component.as_str(), "." | ".."))
        {
            return;
        }
        let mut by_shard = std::collections::BTreeMap::<String, (NodeId, Vec<String>)>::new();
        let mut candidate = "/".to_string();
        for component in components {
            candidate = crate::posix_path::join_posix_path(&candidate, component);
            if self.cached_routed_lookup(&candidate).is_some() {
                continue;
            }
            let Ok(Some(shard)) =
                self.routed_namespace_shard_for_path_from_snapshot(&candidate, shards)
            else {
                continue;
            };
            if self.reads_shard_from_local_metadata(&shard) {
                continue;
            }
            by_shard
                .entry(shard.shard_id)
                .or_insert_with(|| (shard.primary_node_id, Vec::new()))
                .1
                .push(candidate.clone());
        }
        let file_system_id = sifs_file_system_id();
        for (_, (primary_node_id, paths)) in by_shard {
            if paths.len() < 2 {
                continue;
            }
            let Ok(objects) = self.route_namespace_lookup_paths_to_metadata_primary(
                file_system_id.clone(),
                primary_node_id,
                paths.clone(),
                POSIX_READ_FRESHNESS_SYNC_TIMEOUT,
            ) else {
                continue;
            };
            // The reply also carries hardlink-target routing hints; only a
            // record at a requested path answers that path, and a requested
            // path without one is absent at its authority.
            let mut found = std::collections::BTreeMap::new();
            for object in objects {
                if object.file_system_id == file_system_id {
                    found
                        .entry(crate::namespace::metadata_path(&object.path))
                        .or_insert(object);
                }
            }
            for path in paths {
                let object = found.remove(&crate::namespace::metadata_path(&path));
                self.remember_routed_lookup(&path, &object);
            }
        }
    }

    /// Record only the lookup result used by the caller. A speculative local
    /// fallback does not replace the original predicate of a previous read.
    pub(super) fn record_selected_synced_path(
        &self,
        path: &str,
        object: Option<DfsObjectMetadata>,
    ) -> Option<DfsObjectMetadata> {
        let fs = sifs_file_system_id();
        self.namespace_execution
            .observe_path(&fs, path, object.as_ref());
        if self.metadata_lookup_cache_is_active() {
            if let Ok(mut cache) = self.metadata_lookup_cache.lock() {
                cache.insert((fs, crate::namespace::metadata_path(path)), object.clone());
            }
        }
        object
    }

    /// Exact-path variant that reuses a caller's request-scoped topology.
    ///
    /// No-link capability walks call this once per component. Keeping the
    /// snapshot outside the component loop avoids repeated cluster-KV catalog
    /// reads and ensures the whole walk uses one routing generation.
    pub(super) fn lookup_sifs_path_direct_distributed_synced_from_snapshot(
        &mut self,
        path: &str,
        shard_snapshot: Option<&[DfsMetadataShardConfig]>,
    ) -> Result<Option<DfsObjectMetadata>> {
        let path = normalize_posix_path(path)?;
        let Some(shard) = shard_snapshot
            .map(|shards| self.routed_namespace_shard_for_path_from_snapshot(&path, shards))
            .transpose()?
            .flatten()
        else {
            if self.transport.delegates_local_namespace_authority() {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "Main caller lookup has no current metadata shard authority".into(),
                }));
            }
            return self.lookup_sifs_path_without_shard_authority(&path);
        };
        if !self.reads_shard_from_local_metadata(&shard) {
            // One request resolves, validates its parent and selects its
            // target from the same authority observation instead of routing
            // each step again. The observation remains a planning predicate:
            // the authority re-checks it when it admits the mutation.
            if let Some(object) = self.cached_routed_lookup(&path) {
                return Ok(self.record_selected_synced_path(&path, object));
            }
            let (object, _) = self.route_namespace_lookup_to_metadata_primary(
                shard.primary_node_id.clone(),
                &path,
                None,
            )?;
            self.remember_routed_lookup(&path, &object);
            return Ok(self.record_selected_synced_path(&path, object));
        }
        let local_object = self.lookup_sifs_path_speculatively(&path)?;
        if shard.primary_node_id != self.node_id {
            match self.route_namespace_lookup_to_metadata_primary(
                shard.primary_node_id.clone(),
                &path,
                None,
            ) {
                Ok((object, _)) => return Ok(self.record_selected_synced_path(&path, object)),
                Err(error) => {
                    if self.replica_redirect_keeps_local_object(&path, &error, &local_object)? {
                        return Ok(self.record_selected_synced_path(&path, local_object));
                    }
                    if !namespace_lookup_error_allows_local_replica_fallback(&error) {
                        return Err(error.into());
                    }
                }
            }
        }
        if let Some(object) = local_object.filter(|object| object.kind != DfsObjectKind::Tombstone)
        {
            return Ok(self.record_selected_synced_path(&path, Some(object)));
        }
        self.lookup_sifs_path(&path)
    }

    /// Routes a namespace mutation to the shard primary, blocks for the
    /// response, and maps a structured rejection back to the same
    /// `DfsPosixError` a local execution would have produced. On success the
    /// response journal batches are already replayed locally.
    ///
    /// A node routing on a stale shard view can address a former primary;
    /// the authority rejection names the current primary, and the operation
    /// is retried against it (bounded) instead of failing.
    pub(crate) fn route_namespace_operation(
        &mut self,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<()> {
        let operation_id = self.next_request_id();
        self.route_namespace_operation_with_request_id(operation_id, primary_node_id, operation)
    }

    pub(crate) fn route_namespace_operation_with_result(
        &mut self,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<Option<DfsPosixCoordinationResult>> {
        let operation_id = self.next_request_id();
        self.route_namespace_operation_with_request_id_and_result(
            operation_id,
            primary_node_id,
            operation,
        )
    }

    pub(crate) fn route_namespace_operation_with_request_id(
        &mut self,
        operation_id: String,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<()> {
        self.route_namespace_operation_with_request_id_and_result(
            operation_id,
            primary_node_id,
            operation,
        )
        .map(drop)
    }

    pub(crate) fn route_namespace_operation_with_request_id_and_result(
        &mut self,
        operation_id: String,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<Option<DfsPosixCoordinationResult>> {
        let file_system_id = sifs_file_system_id();
        let replay = !matches!(operation, DfsNamespaceOperation::WriteFile { .. });
        let mut primary = primary_node_id;
        for _ in 0..POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT {
            let routed = match self
                .route_namespace_operation_to_metadata_primary_with_request_id_and_options(
                    operation_id.clone(),
                    file_system_id.clone(),
                    primary.clone(),
                    operation.clone(),
                    DFS_NO_CALLER_BUDGET,
                    replay,
                    replay,
                ) {
                Ok(result) => result,
                // A stable request id makes a retry after a possibly applied
                // dispatch safe: the primary answers it from durable admission.
                Err(
                    error @ (DfsError::Network { .. }
                    | DfsError::NetworkDelivery {
                        delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
                        ..
                    }),
                ) => {
                    let recovery = self.refresh_routed_namespace_primary_after_network_error(
                        Some(&operation_id),
                        &operation,
                        &mut primary,
                    )?;
                    match recovery {
                        RoutedNamespaceNetworkRecovery::Completed => {
                            validate_cross_shard_adoption_receipt(&operation, None)?;
                            validate_cross_shard_adoption_source_pin_set(
                                &operation,
                                self.metadata_backend.as_deref(),
                            )?;
                            return Ok(None);
                        }
                        RoutedNamespaceNetworkRecovery::Retry => continue,
                        RoutedNamespaceNetworkRecovery::Unchanged => {
                            return Err(DfsPosixError::Dfs(error));
                        }
                    }
                }
                Err(error @ DfsError::NetworkDelivery { .. }) => {
                    self.post_routed_namespace_delivery_hint(&operation);
                    return Err(DfsPosixError::Dfs(error));
                }
                Err(error) => return Err(DfsPosixError::Dfs(error)),
            };
            match routed.result.clone() {
                DfsNamespaceResult::Applied => {
                    validate_cross_shard_adoption_receipt(
                        &operation,
                        routed.posix_coordination_result.as_ref(),
                    )?;
                    validate_cross_shard_adoption_source_pin_set(
                        &operation,
                        self.metadata_backend.as_deref(),
                    )?;
                    return Ok(routed.posix_coordination_result);
                }
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    current_primary_node_id,
                } => match current_primary_node_id {
                    Some(current)
                        if current == self.node_id
                            && !self.transport.delegates_local_namespace_authority() =>
                    {
                        // Authority moved to this node; execute locally.
                        return self.execute_routed_namespace_operation_locally(
                            Some(&operation_id),
                            &operation,
                        );
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
                        if primary == self.node_id
                            && !self.transport.delegates_local_namespace_authority()
                        {
                            return self.execute_routed_namespace_operation_locally(
                                Some(&operation_id),
                                &operation,
                            );
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

    /// Keep caller-owned stable admission when routing discovers local
    /// authority, including after a lost response. Direct execution would lose
    /// the original durable identity and could create an untracked mutation.
    fn execute_routed_namespace_operation_locally(
        &mut self,
        operation_id: Option<&str>,
        operation: &DfsNamespaceOperation,
    ) -> Result<Option<DfsPosixCoordinationResult>> {
        if let (Some(id), DfsNamespaceOperation::WriteFile { .. }) = (operation_id, operation) {
            return self.local_browser_write_reply(id.to_owned(), operation.clone());
        }
        if let Some(operation_id) =
            operation_id.filter(|_| namespace_operation_uses_stable_admission(operation))
        {
            self.execute_local_primary_namespace_operation_with_request_id(
                operation_id.to_string(),
                operation.clone(),
            )?;
            return Ok(None);
        }
        self.execute_posix_coordination_operation_local(operation)
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
                DFS_NO_CALLER_BUDGET,
                true,
                true,
            )
            .map_err(DfsPosixError::Dfs)?;
        match routed.result.clone() {
            DfsNamespaceResult::Applied => match routed.posix_coordination_result {
                Some(DfsPosixCoordinationResult::NamespaceJob(job)) => Ok(job),
                _ => Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: "bounded namespace job response omitted its typed receipt".to_string(),
                })),
            },
            DfsNamespaceResult::Rejected {
                reason,
                posix_error,
                ..
            } => Err(posix_error_for_namespace_rejection(reason, posix_error)),
        }
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
                // Each attempt here has a new request id, so only an error
                // before dispatch is retried. A possibly applied dispatch is
                // returned unchanged, never repeated under a new id.
                Err(error @ DfsError::Network { .. }) => {
                    match self.refresh_routed_namespace_primary_after_network_error(
                        None,
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
                Err(error @ DfsError::NetworkDelivery { .. }) => {
                    self.post_routed_namespace_delivery_hint(&operation);
                    return Err(DfsPosixError::Dfs(error));
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
                    Some(current)
                        if current == self.node_id
                            && !self.transport.delegates_local_namespace_authority() =>
                    {
                        return self
                            .execute_routed_namespace_operation_locally(None, &operation)
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
                        if primary == self.node_id
                            && !self.transport.delegates_local_namespace_authority()
                        {
                            return self
                                .execute_routed_namespace_operation_locally(None, &operation)
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
        operation_id: Option<&str>,
        operation: &DfsNamespaceOperation,
        primary: &mut NodeId,
    ) -> Result<RoutedNamespaceNetworkRecovery> {
        let Some(path) = operation.routing_path() else {
            return Ok(RoutedNamespaceNetworkRecovery::Unchanged);
        };
        let Some(shard) = self.select_sifs_metadata_shard_for_path_fresh(path)? else {
            return Ok(RoutedNamespaceNetworkRecovery::Unchanged);
        };
        if shard.primary_node_id == self.node_id
            && !self.transport.delegates_local_namespace_authority()
        {
            self.execute_routed_namespace_operation_locally(operation_id, operation)?;
            return Ok(RoutedNamespaceNetworkRecovery::Completed);
        }
        if shard.primary_node_id != *primary {
            *primary = shard.primary_node_id;
            return Ok(RoutedNamespaceNetworkRecovery::Retry);
        }
        post_namespace_primary_unreachable(&self.node_id, &shard);
        Ok(RoutedNamespaceNetworkRecovery::Unchanged)
    }

    /// A request that could not be delivered to the shard primary is a hint
    /// for this node's candidate role, which asks the network to verify the
    /// primary when this node may take the shard over.
    fn post_routed_namespace_delivery_hint(&mut self, operation: &DfsNamespaceOperation) {
        if let Some(path) = operation.routing_path()
            && let Ok(Some(shard)) = self.select_sifs_metadata_shard_for_path_fresh(path)
        {
            post_namespace_primary_unreachable(&self.node_id, &shard);
        }
    }

    /// Executes a routed namespace operation on the namespace authority.
    /// Callers have already validated shard primaryship; dispatching to the
    /// `_local` variants keeps execution from re-routing.
    pub(crate) fn execute_namespace_operation_local(
        &mut self,
        operation: &DfsNamespaceOperation,
    ) -> Result<()> {
        self.execute_namespace_operation_local_with_placement(operation, None)
    }

    pub(crate) fn execute_namespace_operation_local_with_placement(
        &mut self,
        operation: &DfsNamespaceOperation,
        placement: Option<crate::placement::AdmittedPlacement>,
    ) -> Result<()> {
        let _source_scope = match operation {
            DfsNamespaceOperation::AdoptObjectAtPath { source_offer, .. } => {
                Some(self.source_adoption.install(source_offer.clone()))
            }
            _ => None,
        };
        if let DfsNamespaceOperation::ContentReference { request } = operation {
            return self
                .apply_content_reference_local(request)
                .map_err(DfsPosixError::from);
        }
        match operation {
            DfsNamespaceOperation::Unlink { path }
            | DfsNamespaceOperation::ConditionalUnlink { path, .. } => {
                if self.resume_or_replay_cross_shard_mutation_for_active_request(
                    &sifs_file_system_id(),
                    path,
                    None,
                    &[
                        DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries,
                        DfsNamespaceJobKind::RenameObjectAcrossShardPrimaries,
                    ],
                )? {
                    return Ok(());
                }
            }
            DfsNamespaceOperation::Rename { from, to }
            | DfsNamespaceOperation::ConditionalRename { from, to, .. }
                if self.resume_or_replay_cross_shard_mutation_for_active_request(
                    &sifs_file_system_id(),
                    from,
                    Some(to),
                    &[DfsNamespaceJobKind::HardlinkAliasMutationAcrossShardPrimaries],
                )? =>
            {
                return Ok(());
            }
            _ => {}
        }
        match operation {
            DfsNamespaceOperation::Rename { from, to }
            | DfsNamespaceOperation::ConditionalRename { from, to, .. } => {
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
            } if self.resume_running_cross_shard_namespace_job_for_paths(
                &sifs_file_system_id(),
                existing_path,
                link_path,
                &[DfsNamespaceJobKind::HardlinkAcrossShardPrimaries],
            )? =>
            {
                return Ok(());
            }
            _ => {}
        }
        // These authenticated phase commands are the only operations allowed
        // through the exact target fence they own. Ordinary lookup and
        // mutation traffic still reaches the guard below and returns Busy.
        match operation {
            DfsNamespaceOperation::ParentCountParticipant { coordinator_job, participant, action } => {
                return self.execute_parent_count_participant(coordinator_job, participant, action).map(drop);
            }
            DfsNamespaceOperation::InstallHardlinkTargetFence { fence, source_path } => {
                return self.install_hardlink_target_fence_local(fence, source_path);
            }
            DfsNamespaceOperation::ReleaseDirectoryMoveDestination { reservation } => {
                return Ok(self.release_directory_move_destination_local(reservation)?);
            }
            DfsNamespaceOperation::CommitHardlinkTargetFence {
                fence_job_id,
                source_path,
                target_path,
                expected_target,
                committed_target,
            } => {
                return self.commit_hardlink_target_fence_local(
                    fence_job_id,
                    source_path,
                    target_path,
                    expected_target,
                    committed_target,
                );
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
            DfsNamespaceOperation::MkdirAtNoLinks {
                path,
                expected_parent_object_id,
                expected_parent_version,
            } => self
                .posix_mkdir_at_no_links_local(
                    path,
                    expected_parent_object_id,
                    expected_parent_version,
                )
                .map(drop),
            DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(command) => self
                .posix_mkdir_at_no_links_local_with_attrs(
                    &command.path,
                    &command.expected_parent_object_id,
                    &command.expected_parent_version,
                    command.attrs,
                )
                .map(drop),
            DfsNamespaceOperation::MkdirAll { path } => self.posix_mkdir_all_local(path),
            DfsNamespaceOperation::Rmdir { path } => self.posix_rmdir_local(path).map(drop),
            DfsNamespaceOperation::ConditionalRmdir { path, expected } => self
                .posix_rmdir_local_if_generation(path, expected)
                .map(drop),
            DfsNamespaceOperation::Unlink { path } => self.posix_unlink_local(path).map(drop),
            DfsNamespaceOperation::ConditionalUnlink { path, expected } => self
                .posix_unlink_local_if_generation(path, expected)
                .map(drop),
            DfsNamespaceOperation::Rename { from, to } => {
                self.posix_rename_local(from, to).map(drop)
            }
            DfsNamespaceOperation::ConditionalRename {
                from,
                to,
                expected_source,
                expected_destination,
            } => self
                .posix_rename_local_if_generation(from, to, expected_source, expected_destination)
                .map(drop),
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
            DfsNamespaceOperation::InstallHardlinkTargetFence { .. }
            | DfsNamespaceOperation::ParentCountParticipant { .. }
            | DfsNamespaceOperation::ReleaseDirectoryMoveDestination { .. }
            | DfsNamespaceOperation::CommitHardlinkTargetFence { .. } => {
                unreachable!("hardlink target fence phases return before generic path guards")
            }
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
            DfsNamespaceOperation::RecordAccess { path, object_id } => {
                self.record_posix_access_local(object_id, path)
            }
            DfsNamespaceOperation::TouchDirectoryModified { path } => {
                self.touch_directory_modified(path)
            }
            DfsNamespaceOperation::ClearDirRecursive { path } => {
                self.posix_clear_dir_recursive_local(path)
            }
            DfsNamespaceOperation::MetadataQuery { .. }
            | DfsNamespaceOperation::LookupPath { .. }
            | DfsNamespaceOperation::LookupPaths { .. } => Ok(()),
            DfsNamespaceOperation::ContentReference { .. } => {
                unreachable!("history handled before path guards")
            }
            DfsNamespaceOperation::CommitLocalEventualBatch { .. } => {
                Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: "local-eventual batches require authenticated routed handling"
                        .to_string(),
                }))
            }
            DfsNamespaceOperation::ReconcilePlacement {
                job_id,
                path,
                object_id,
                expected_version,
                object_policy,
                replication_policy,
                desired_replica_node_ids,
            } => match placement {
                Some(cut) => match cut.job? {
                    Some((job, config)) => self.apply_admitted_placement(
                        &job,
                        &config,
                        path,
                        object_id,
                        expected_version,
                        object_policy.clone(),
                        replication_policy.clone(),
                        desired_replica_node_ids,
                    ),
                    None => Ok(()),
                },
                None => self.reconcile_object_placement_local(
                    job_id,
                    path,
                    object_id,
                    expected_version,
                    object_policy.clone(),
                    replication_policy.clone(),
                    desired_replica_node_ids,
                ),
            }
            .map_err(DfsPosixError::from),
            DfsNamespaceOperation::WriteFile { .. }
            | DfsNamespaceOperation::BeginContentCopy { .. }
            | DfsNamespaceOperation::AbortPendingWrite { .. }
            | DfsNamespaceOperation::PollNamespaceChanges { .. }
            | DfsNamespaceOperation::AdvisoryLock { .. }
            | DfsNamespaceOperation::FileSync { .. }
            | DfsNamespaceOperation::DirectorySync { .. }
            | DfsNamespaceOperation::RenameDirectoryBounded { .. }
            | DfsNamespaceOperation::ResumeNamespaceJobBounded { .. }
            | DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. }
            | DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. } => {
                Err(DfsPosixError::Dfs(DfsError::Distributed {
                    message: "POSIX coordination operation bypassed its typed executor".to_string(),
                }))
            }
            DfsNamespaceOperation::AdoptObjectAtPath {
                source_offer: _,
                object,
                source_path,
                pin_transfer_id,
                versions,
                version_pins,
                version_pin_set_fingerprint,
                receipts,
                intents,
                intent_receipts,
                intent_receipt_retention,
                content_reference_outcomes,
                content_reference_outcome_retention,
                advisory_lock_state,
            } => self
                .adopt_object_at_path_local(
                    object,
                    source_path,
                    pin_transfer_id.as_deref(),
                    versions,
                    version_pins,
                    version_pin_set_fingerprint.as_deref(),
                    receipts,
                    intents,
                    intent_receipts,
                    intent_receipt_retention,
                    content_reference_outcomes,
                    content_reference_outcome_retention.as_ref(),
                    advisory_lock_state.as_ref(),
                )
                .map(drop),
            DfsNamespaceOperation::AdoptHardlinkAlias {
                alias,
                source_path: _,
            } => self.adopt_hardlink_alias_local(alias),
        }
    }

    /// Local namespace mutations must not interleave with an in-flight
    /// cross-shard move of the same resolved path (the move's saga may be
    /// suspended awaiting recovery, with its lock still held), nor land in
    /// the destination subtree a cross-shard directory move reserves.
    pub(super) fn ensure_resolved_path_not_in_cross_shard_flight(
        &mut self,
        path: &str,
    ) -> Result<()> {
        self.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), path)?;
        self.ensure_path_not_reserved_by_directory_move(&sifs_file_system_id(), path)?;
        Ok(())
    }

    /// Stats the outcome of a routed namespace mutation as its metadata
    /// authority now reports it, so the caller reads its own change without
    /// waiting for a local copy to catch up.
    pub(super) fn stat_routed_namespace_result(
        &mut self,
        path: &str,
        follow: bool,
    ) -> Result<DfsStat> {
        if follow {
            self.posix_stat_distributed(path)
        } else {
            self.posix_lstat_distributed(path)
        }
    }

    /// Tombstone stat for routed unlink/rmdir results, read from the authority
    /// of the tombstoned entry.
    pub(super) fn stat_replayed_tombstone_synced(&mut self, path: &str) -> Result<DfsStat> {
        let Some(shards) = self.refresh_sifs_metadata_shards()? else {
            if self.transport.delegates_local_namespace_authority() {
                return Err(DfsPosixError::Dfs(DfsError::Metadata {
                    message: "Main caller lookup has no current metadata shard authority".into(),
                }));
            }
            return self.stat_replayed_tombstone(path);
        };
        // The authority tombstones the canonical entry after following parent
        // links. Resolve that result through one current routing cut, while
        // preserving the final entry itself instead of following it.
        let resolved = self.resolve_posix_path_distributed_from_snapshot(path, false, &shards)?;
        let object = self
            .lookup_sifs_path_direct_distributed_synced_from_snapshot(&resolved, Some(&shards))?
            .ok_or(DfsPosixError::NotFound { path: resolved })?;
        self.stat_object(&object)
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

pub(super) fn validate_cross_shard_adoption_receipt(
    operation: &DfsNamespaceOperation,
    result: Option<&DfsPosixCoordinationResult>,
) -> Result<()> {
    if let DfsNamespaceOperation::FinalizeObjectFamilyAdoption {
        object_id,
        pin_transfer_id,
        ..
    } = operation
    {
        if matches!(
            result,
            Some(DfsPosixCoordinationResult::ObjectFamilyAdoptionFinalized {
                object_id: finalized_object_id,
                pin_transfer_id: finalized_transfer_id,
                durable_through_sequence,
            }) if finalized_object_id == object_id
                && finalized_transfer_id == pin_transfer_id
                && *durable_through_sequence > 0
        ) {
            return Ok(());
        }
        return Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "destination did not return a quorum-durable pin-transfer finalization receipt for {object_id}"
            ),
        }));
    }
    if let DfsNamespaceOperation::AdoptObjectAtPath {
        object,
        version_pins,
        version_pin_set_fingerprint: Some(expected_pin_fingerprint),
        pin_transfer_id,
        advisory_lock_state,
        content_reference_outcomes,
        content_reference_outcome_retention,
        ..
    } = operation
    {
        let canonical_pin_fingerprint =
            crate::protocol::cross_shard_version_pin_set_fingerprint(object, version_pins)
                .map_err(DfsPosixError::from)?;
        if &canonical_pin_fingerprint != expected_pin_fingerprint {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "cross-shard adoption of {} carried a non-canonical durable version-pin fingerprint",
                    object.object_id
                ),
            }));
        }
        let expected_lock_fingerprint = advisory_lock_state
            .as_ref()
            .map(|source_state| {
                crate::advisory_lock::prepare_advisory_lock_state_for_migration(source_state)
                    .map(|(_, fingerprint)| fingerprint)
                    .map_err(|source| {
                        DfsPosixError::Dfs(DfsError::Serialization {
                            message: format!(
                                "encode advisory-lock migration receipt for {}: {source}",
                                object.object_id
                            ),
                        })
                    })
            })
            .transpose()?;
        let expected_outcome_fingerprint =
            crate::protocol::content_reference_outcome_family_fingerprint(
                object,
                content_reference_outcomes,
                content_reference_outcome_retention.as_ref(),
            )
            .map_err(DfsPosixError::from)?;
        if let Some(DfsPosixCoordinationResult::ObjectFamilyAdoption {
            object_id,
            target_version,
            pin_transfer_id: acknowledged_transfer_id,
            version_pin_set_fingerprint,
            advisory_lock_state_fingerprint,
            content_reference_outcome_fingerprint,
            durable_through_sequence,
            ..
        }) = result
        {
            if object_id == &object.object_id
                && target_version == &object.version
                && pin_transfer_id.as_ref() == Some(acknowledged_transfer_id)
                && version_pin_set_fingerprint == &canonical_pin_fingerprint
                && advisory_lock_state_fingerprint == &expected_lock_fingerprint
                && content_reference_outcome_fingerprint == &expected_outcome_fingerprint
                && *durable_through_sequence > 0
            {
                return Ok(());
            }
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "destination returned a mismatched object-family adoption receipt for {}",
                    object.object_id
                ),
            }));
        }
        if pin_transfer_id.as_deref().is_none_or(str::is_empty) {
            return Err(DfsPosixError::Dfs(DfsError::Distributed {
                message: format!(
                    "pin-aware cross-shard adoption of {} omitted its stable transfer identity",
                    object.object_id
                ),
            }));
        }
        return Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "destination did not return an exact current object-family adoption receipt for {}",
                object.object_id
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

fn validate_cross_shard_adoption_source_pin_set(
    operation: &DfsNamespaceOperation,
    metadata_backend: Option<&dyn crate::DfsMetadataBackend>,
) -> Result<()> {
    let DfsNamespaceOperation::AdoptObjectAtPath {
        object,
        version_pin_set_fingerprint: Some(_),
        ..
    } = operation
    else {
        return Ok(());
    };
    let current_pins = metadata_backend
        .map(|backend| backend.version_pins_for_object(&object.object_id))
        .transpose()?
        .unwrap_or_default();
    validate_cross_shard_adoption_source_pin_records(operation, &current_pins)
}

fn validate_cross_shard_adoption_source_pin_records(
    operation: &DfsNamespaceOperation,
    current_pins: &[crate::DfsMetadataVersionPinRecord],
) -> Result<()> {
    let DfsNamespaceOperation::AdoptObjectAtPath {
        object,
        version_pins,
        version_pin_set_fingerprint: Some(expected_fingerprint),
        ..
    } = operation
    else {
        return Ok(());
    };
    let captured_fingerprint =
        crate::protocol::cross_shard_version_pin_set_fingerprint(object, version_pins)
            .map_err(DfsPosixError::from)?;
    if &captured_fingerprint != expected_fingerprint {
        return Err(DfsPosixError::Dfs(DfsError::Distributed {
            message: format!(
                "source cross-shard adoption payload for {} has a non-canonical durable version-pin fingerprint",
                object.object_id
            ),
        }));
    }
    let current_fingerprint =
        crate::protocol::cross_shard_version_pin_set_fingerprint(object, current_pins)
            .map_err(DfsPosixError::from)?;
    if &current_fingerprint == expected_fingerprint {
        return Ok(());
    }
    Err(DfsPosixError::Dfs(DfsError::Distributed {
        message: format!(
            "source durable version-pin set changed while adopting {}; retry the object-family copy before source eviction",
            object.object_id
        ),
    }))
}

impl<T> NetworkedDfsService<T>
where
    T: crate::AsyncDfsMessageTransport + crate::DfsTransportIdentity + DfsMessageTransport,
{
    pub(crate) async fn route_placement_namespace_operation_with_request_id_and_result_async(
        &mut self,
        operation_id: String,
        primary_node_id: NodeId,
        operation: DfsNamespaceOperation,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let DfsNamespaceOperation::ReconcilePlacement {
            path, object_id, ..
        } = &operation
        else {
            return Err(DfsPosixError::Dfs(DfsError::Metadata {
                message: "async placement routing requires ReconcilePlacement".into(),
            }));
        };
        let file_system_id = sifs_file_system_id();
        let mut primary = primary_node_id;
        for _ in 0..POSIX_ROUTED_AUTHORITY_REDIRECT_LIMIT {
            if primary == self.node_id && !self.transport.delegates_local_namespace_authority() {
                self.execute_posix_coordination_operation_local(&operation)?;
                return self
                    .current_read_object_metadata(object_id)
                    .map(|object| vec![object])
                    .map_err(DfsPosixError::Dfs);
            }
            // Placement consumes the owner's committed object result. It does
            // not serve a requester read and has no journal-replay obligation.
            let mut routed = match self
                .route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
                    operation_id.clone(), file_system_id.clone(), primary.clone(), operation.clone(),
                    DFS_NO_CALLER_BUDGET, false, false,
                ).await {
                Ok(result) => result,
                Err(error @ DfsError::Network { .. }) => {
                    match self.refresh_routed_namespace_primary_after_network_error(None, &operation, &mut primary)? {
                        RoutedNamespaceNetworkRecovery::Completed => {
                            if primary == self.node_id { continue; }
                            // Recovery proved completion, but the current
                            // object result still comes from its owning primary.
                            PendingRoutedNamespaceResult {
                                result: DfsNamespaceResult::Applied,
                                posix_coordination_result: None,
                                synced_path_object: None,
                                synced_path_objects: Vec::new(),
                                metadata_journal_has_more: false,
                            }
                        }
                        RoutedNamespaceNetworkRecovery::Retry => continue,
                        RoutedNamespaceNetworkRecovery::Unchanged => return Err(DfsPosixError::Dfs(error)),
                    }
                }
                Err(error) => return Err(DfsPosixError::Dfs(error)),
            };
            if routed.result == DfsNamespaceResult::Applied && routed.synced_path_objects.is_empty()
            {
                // Older peers omit placement objects. Confirm their result
                // through the existing authoritative read, without journal
                // replay or installation into the coordinator's replica.
                let request_id = self.next_request_id();
                routed = self.route_placement_namespace_operation_to_metadata_primary_with_request_id_and_options(
                    request_id, file_system_id.clone(), primary.clone(),
                    DfsNamespaceOperation::LookupPaths { paths: vec![path.clone()] },
                    DFS_NO_CALLER_BUDGET, false, false,
                ).await.map_err(DfsPosixError::Dfs)?;
            }
            match routed.result {
                DfsNamespaceResult::Applied => {
                    if routed
                        .synced_path_objects
                        .iter()
                        .any(|object| object.file_system_id != file_system_id)
                        || (!routed.synced_path_objects.is_empty()
                            && !routed
                                .synced_path_objects
                                .iter()
                                .any(|object| &object.path == path))
                    {
                        return Err(DfsPosixError::Dfs(DfsError::Metadata {
                            message:
                                "placement response does not describe the requested filesystem/path"
                                    .into(),
                        }));
                    }
                    return Ok(routed.synced_path_objects);
                }
                DfsNamespaceResult::Rejected {
                    reason,
                    posix_error,
                    current_primary_node_id,
                } => match current_primary_node_id {
                    Some(current) if current == self.node_id => primary = current,
                    Some(current) if current != primary => {
                        primary = self
                            .refreshed_primary_after_authority_redirect(
                                &file_system_id,
                                &operation,
                                &primary,
                                current,
                            )
                            .map_err(DfsPosixError::Dfs)?;
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
}

/// Tell this node's candidate role it could not deliver to `shard`'s primary.
fn post_namespace_primary_unreachable(local: &NodeId, shard: &DfsMetadataShardConfig) {
    crate::write_set_keeping_signals::post_commit_demand(
        local,
        &shard.file_system_id,
        &crate::DfsMetadataShardId::new(shard.shard_id.clone()),
        crate::write_set_keeping_signals::DfsWriteSetCommitDemand::PrimaryUnreachable,
    );
}
