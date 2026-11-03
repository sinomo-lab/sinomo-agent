//! POSIX namespace query helpers.
//!
//! This module owns read-only namespace surfaces such as stat, lstat,
//! readlink, and directory listing. It intentionally reuses the parent POSIX
//! facade's path-resolution and metadata helpers so mutation code and query
//! code share one interpretation of tombstones, hardlinks, and directories.

use super::{POSIX_LIST_DIR_COLLECT_PAGE_SIZE, now_unix_nanos};
use crate::metadata_pages::try_walk_metadata_pages;
use crate::namespace::next_version;
use crate::posix_model::{DfsDirEntry, DfsDirEntryPage, DfsPosixError, DfsStat, Result};
use crate::posix_path::{join_posix_path, normalize_posix_path, posix_path_is_at_or_under_prefix};
use crate::{
    DfsDirectoryPageCursor, DfsMessageTransport, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    DfsReadData, NetworkedDfsReadOutcome, NetworkedDfsService, sifs_file_system_id,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Open one exact namespace object beneath `root` without following any
    /// symbolic-link or DFS hardlink record in either the root or target path.
    ///
    /// Every component is fetched directly from its current metadata
    /// authority. The returned stat therefore acts as an immutable opened
    /// identity for content reads; callers must not resolve `path` again after
    /// this succeeds.
    pub fn posix_open_beneath_no_links_distributed(
        &mut self,
        root: &str,
        path: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.check_cancelled("distributed no-link beneath open")?;
        let root = normalize_posix_path(root)?;
        let path = normalize_posix_path(path)?;
        if !posix_path_is_at_or_under_prefix(&path, &root) {
            return Err(DfsPosixError::InvalidPath { path });
        }

        let components = path
            .trim_start_matches('/')
            .split('/')
            .filter(|component| !component.is_empty())
            .collect::<Vec<_>>();
        // One capability walk observes one immutable shard catalog. Refreshing
        // the control-plane catalog for every component made deep-path lookup
        // cost proportional to both path depth and catalog size, and could
        // route different components through different topology generations.
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        let mut current = "/".to_string();
        // The filesystem root is part of every capability path. Validate it
        // explicitly instead of assuming the ordinary resolver's invariant.
        let file_system_root = self
            .lookup_sifs_path_direct_distributed_synced_from_snapshot(
                &current,
                shard_snapshot.as_deref(),
            )?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: current.clone(),
            })?;
        if file_system_root.kind != DfsObjectKind::Directory {
            return Err(
                if matches!(
                    &file_system_root.kind,
                    DfsObjectKind::Symlink | DfsObjectKind::Hardlink
                ) {
                    DfsPosixError::InvalidPath {
                        path: current.clone(),
                    }
                } else {
                    DfsPosixError::NotDirectory {
                        path: current.clone(),
                    }
                },
            );
        }
        let mut final_object = Some(file_system_root);

        if !components.is_empty() {
            for (chunk_index, chunk) in components.chunks(64).enumerate() {
                let paths = chunk
                    .iter()
                    .map(|component| {
                        current = join_posix_path(&current, component);
                        current.clone()
                    })
                    .collect::<Vec<_>>();
                let mut prefetched =
                    self.prefetch_no_link_path_components(&paths, shard_snapshot.as_deref())?;
                for (index, current) in paths.into_iter().enumerate() {
                    let object = match prefetched.remove(&current) {
                        Some(object) => object,
                        None => self.lookup_sifs_path_direct_distributed_synced_from_snapshot(
                            &current,
                            shard_snapshot.as_deref(),
                        )?,
                    };
                    // Preserve the original parent/path observations when an
                    // enclosing mutation uses this capability walk as its plan.
                    self.namespace_execution.observe_path(
                        &sifs_file_system_id(),
                        &current,
                        object.as_ref(),
                    );
                    let object = object.ok_or_else(|| DfsPosixError::NotFound {
                        path: current.clone(),
                    })?;
                    if object.kind == DfsObjectKind::Tombstone {
                        return Err(DfsPosixError::NotFound {
                            path: current.clone(),
                        });
                    }
                    if matches!(
                        &object.kind,
                        DfsObjectKind::Symlink | DfsObjectKind::Hardlink
                    ) {
                        return Err(DfsPosixError::InvalidPath {
                            path: current.clone(),
                        });
                    }
                    let final_component = chunk_index * 64 + index + 1 == components.len();
                    let scoped_root_component = current == root;
                    if (!final_component || scoped_root_component)
                        && object.kind != DfsObjectKind::Directory
                    {
                        return Err(DfsPosixError::NotDirectory {
                            path: current.clone(),
                        });
                    }
                    final_object = Some(object);
                }
            }
        }

        let object = final_object.expect("a normalized absolute path has a final object");
        if self.transport.delegates_local_namespace_authority() && self.control_plane.is_some() {
            // Preserve the no-link walk's exact identity and routing cut while
            // deriving counts through the existing native authority queries.
            // A private replica aggregate would decode every shard again.
            return self.with_stat_deadline(|service| {
                service.with_stat_read_rounds(|service| {
                    service.stat_object_from_authorities(&object, &path, shard_snapshot.as_deref())
                })
            });
        }
        self.namespace_lstat_object_for_path(&object, &path)
    }

    /// Group exact lstat queries without following links. Every component is
    /// still validated in path order by the caller. One frozen routing snapshot
    /// and the existing validated LookupPaths response keep authority checks
    /// unchanged while avoiding one RPC for every directory in a deep path.
    fn prefetch_no_link_path_components(
        &mut self,
        paths: &[String],
        shards: Option<&[crate::DfsMetadataShardConfig]>,
    ) -> Result<std::collections::BTreeMap<String, Option<crate::DfsObjectMetadata>>> {
        let mut prefetched = std::collections::BTreeMap::new();
        let Some(shards) = shards else {
            return Ok(prefetched);
        };
        let mut groups = std::collections::BTreeMap::<_, std::collections::BTreeSet<String>>::new();
        for path in paths {
            if let Some(shard) = self.routed_namespace_shard_for_path_from_snapshot(path, shards)?
                && (shard.primary_node_id != self.node_id
                    || self.transport.delegates_local_namespace_authority())
            {
                groups
                    .entry(shard.primary_node_id)
                    .or_default()
                    .insert(path.clone());
            }
        }
        for (primary, requested) in groups {
            match self.route_authoritative_bulk_lookup_paths(
                &sifs_file_system_id(),
                primary,
                requested.clone(),
            ) {
                Ok(mut lookup) => {
                    for path in requested {
                        let object = lookup.exact_objects_by_path.remove(&path);
                        prefetched.insert(path, object);
                    }
                }
                // Retain the existing per-component bounded transport fallback.
                // Malformed/foreign responses remain errors, never cached absence.
                Err(DfsPosixError::Dfs(
                    crate::DfsError::Network { .. } | crate::DfsError::NetworkDelivery { .. },
                )) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(prefetched)
    }

    pub fn posix_readlink(&self, path: &str) -> Result<String> {
        let path = self.resolve_existing_posix_path(path, false)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink { path });
        }
        match self.read_local_object_with_state(&object.object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath { path }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
        }
    }

    pub fn posix_readlink_distributed(&mut self, path: &str) -> Result<String> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (path, object) = if self.metadata_backend.is_none()
            && self.transport.delegates_local_namespace_authority()
            && self.control_plane.is_some()
        {
            self.lookup_sifs_path_distributed_synced(path, false)?
        } else {
            let path = self.resolve_existing_posix_path_distributed(path, false)?;
            let object = self.lookup_sifs_path(&path)?;
            (path, object)
        };
        let object = object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target_distributed(&object, None)?;
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink { path });
        }
        let outcome = self.read_object_metadata_or_fetch_remote_without_cache(&object)?;
        super::metadata::symlink_target_from_distributed_read_outcome(&path, outcome)
    }

    pub fn posix_readlink_update_access(&mut self, path: &str) -> Result<String> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let resolved_path = self.resolve_existing_posix_path_distributed(path, false)?;
        let target = self.posix_readlink_distributed(path)?;
        let object =
            self.lookup_sifs_path(&resolved_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: resolved_path.clone(),
                })?;
        let mut object = self.resolve_hardlink_target(&object)?;
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: resolved_path,
            });
        }
        if !self.is_local_metadata_primary_for_object(&object)? {
            return Ok(target);
        }
        object.accessed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(target)
    }

    pub fn posix_stat(&self, path: &str) -> Result<DfsStat> {
        self.with_metadata_lookup_cache(|service| {
            let requested = self.resolve_posix_dot_components(path)?;
            service.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), &requested)?;
            let path = service.resolve_existing_posix_path(path, true)?;
            service.stat_resolved_posix_path(&path)
        })
    }

    /// Resolve the same local namespace target as stat without projecting content
    /// size or link counts. Mutation admission still revalidates these hints.
    pub(crate) fn posix_object_metadata_hint(&self, path: &str) -> Result<DfsObjectMetadata> {
        self.with_metadata_lookup_cache(|service| {
            let requested = service.resolve_posix_dot_components(path)?;
            service.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), &requested)?;
            let path = service.resolve_existing_posix_path(path, true)?;
            service.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), &path)?;
            service.stat_target_for_path(&path)
        })
    }

    /// Policy of the object a write to `path` would replace, or `None` when
    /// nothing exists there yet. A node whose durable store replicates the
    /// path's shard uses that view; any other asks the shard's authority, since
    /// a missing local record does not mean the path is new.
    pub fn posix_existing_object_policy(
        &mut self,
        path: &str,
    ) -> Result<Option<sinomo_core::DfsObjectPolicy>> {
        let requested = normalize_posix_path(path)?;
        let local_store_replicates = match self.refresh_sifs_metadata_shards()? {
            Some(shards) => self
                .select_sifs_metadata_shard_for_path_from_snapshot(&requested, &shards)?
                .is_none_or(|shard| {
                    self.metadata_backend.is_some()
                        && crate::service_routed_journal::shard_lists_node(&shard, &self.node_id)
                }),
            None => true,
        };
        if local_store_replicates {
            return Ok(self
                .posix_object_metadata_hint(path)
                .ok()
                .map(|object| object.policy));
        }
        match self.with_stat_deadline(|service| service.file_history_capture_object(path)) {
            Ok(object) => Ok(Some(object.policy)),
            Err(DfsPosixError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub fn posix_stat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        self.with_stat_deadline(|service| service.posix_stat_authoritative(path))
    }

    fn posix_stat_authoritative(&mut self, path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_posix_dot_components_distributed(path)?;
        let (path, snapshot) =
            self.with_local_primary_stat_read_snapshot(&path, |service, shard_snapshot| {
                service.with_metadata_lookup_cache_mut(|service| {
                    service.check_cancelled("distributed stat")?;
                    service.ensure_distributed_read_path_not_in_cross_shard_flight(
                        &sifs_file_system_id(),
                        &path,
                    )?;
                    let path = service.resolve_existing_posix_path_distributed(&path, true)?;
                    service.ensure_distributed_read_path_not_in_cross_shard_flight(
                        &sifs_file_system_id(),
                        &path,
                    )?;
                    Ok((path, shard_snapshot.map(<[_]>::to_vec)))
                })
            })?;
        // Release lookup snapshots before collecting and fencing authority
        // views, so a local authority revalidation cannot reuse a pinned past.
        self.stat_path_from_authorities(&path, snapshot.as_deref())
    }

    pub(crate) fn file_history_capture_object(
        &mut self,
        path: &str,
    ) -> Result<crate::DfsObjectMetadata> {
        let path = self.resolve_posix_dot_components_distributed(path)?;
        self.with_local_primary_metadata_read_snapshot(&path, |service, shard_snapshot| {
            service.with_metadata_lookup_cache_mut(|service| {
                let path = service.resolve_existing_posix_path_distributed(&path, true)?;
                service.ensure_distributed_read_path_not_in_cross_shard_flight(
                    &sifs_file_system_id(),
                    &path,
                )?;
                let object = service
                    .lookup_sifs_path_direct_distributed_synced_from_snapshot(
                        &path,
                        shard_snapshot,
                    )?
                    .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                let object =
                    service.resolve_hardlink_target_distributed(&object, shard_snapshot)?;
                if object.kind == DfsObjectKind::Tombstone {
                    return Err(DfsPosixError::NotFound { path });
                }
                Ok(object)
            })
        })
    }

    pub fn posix_lstat(&self, path: &str) -> Result<DfsStat> {
        self.with_metadata_lookup_cache(|service| {
            let requested = self.resolve_posix_dot_components(path)?;
            service.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), &requested)?;
            let path = service.resolve_existing_posix_path(path, false)?;
            service.stat_resolved_posix_path(&path)
        })
    }

    fn stat_resolved_posix_path(&self, path: &str) -> Result<DfsStat> {
        self.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), path)?;
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        self.stat_object_for_path(&object, path)
    }

    pub fn posix_lstat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        self.with_stat_deadline(|service| service.posix_lstat_authoritative(path))
    }

    fn posix_lstat_authoritative(&mut self, path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let requested = self.resolve_posix_dot_components_distributed(path)?;
        self.ensure_distributed_read_path_not_in_cross_shard_flight(
            &sifs_file_system_id(),
            &requested,
        )?;
        let path = self.resolve_existing_posix_path_distributed(&requested, false)?;
        let snapshot = self.refresh_sifs_metadata_shards()?;
        self.ensure_distributed_read_path_not_in_cross_shard_flight(&sifs_file_system_id(), &path)?;
        self.stat_path_from_authorities(&path, snapshot.as_deref())
    }

    /// Inspect the final DFS namespace record without resolving either a
    /// symbolic link or the DFS hardlink marker to its target.
    ///
    /// Ordinary POSIX `lstat` still projects hardlinks as their shared inode.
    /// This stricter surface exists for capability boundaries that must reject
    /// all link records before performing a follow operation.
    pub fn posix_namespace_lstat(&self, path: &str) -> Result<DfsStat> {
        self.with_metadata_lookup_cache(|service| {
            let requested = self.resolve_posix_dot_components(path)?;
            service.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), &requested)?;
            let path = service.resolve_existing_posix_path(path, false)?;
            service.ensure_path_not_in_cross_shard_flight(&sifs_file_system_id(), &path)?;
            let object = service
                .lookup_sifs_path(&path)?
                .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
            if object.kind == DfsObjectKind::Tombstone {
                return Err(DfsPosixError::NotFound { path });
            }
            service.namespace_lstat_object_for_path(&object, &path)
        })
    }

    pub fn posix_namespace_lstat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        if self.metadata_backend.is_none()
            && self.transport.delegates_local_namespace_authority()
            && self.control_plane.is_some()
        {
            return self.with_stat_deadline(|service| {
                let _namespace_plan = service.namespace_execution.begin_planning();
                let requested = service.resolve_posix_dot_components_distributed(path)?;
                service.ensure_distributed_read_path_not_in_cross_shard_flight(
                    &sifs_file_system_id(),
                    &requested,
                )?;
                let snapshot = service.refresh_sifs_metadata_shards()?;
                let (path, object) = service.lookup_sifs_path_distributed_synced_from_snapshot(
                    path,
                    false,
                    snapshot.as_deref(),
                )?;
                service.ensure_distributed_read_path_not_in_cross_shard_flight(
                    &sifs_file_system_id(),
                    &path,
                )?;
                let object =
                    object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                if object.kind == DfsObjectKind::Tombstone {
                    return Err(DfsPosixError::NotFound { path });
                }
                service.with_stat_read_rounds(|service| {
                    let stat = service.stat_object_from_authorities(
                        &object,
                        &path,
                        snapshot.as_deref(),
                    )?;
                    if object.kind == DfsObjectKind::Hardlink {
                        // Count at the canonical inode, but keep the captured
                        // dentry identity so namespace callers still see a link.
                        super::stat::stat_from_projection(
                            service,
                            object.clone(),
                            &path,
                            stat.link_count,
                        )
                    } else {
                        Ok(stat)
                    }
                })
            });
        }
        let _namespace_plan = self.namespace_execution.begin_planning();
        let requested = self.resolve_posix_dot_components_distributed(path)?;
        self.ensure_distributed_read_path_not_in_cross_shard_flight(
            &sifs_file_system_id(),
            &requested,
        )?;
        let (path, object) = self.lookup_sifs_path_distributed_synced(path, false)?;
        self.ensure_distributed_read_path_not_in_cross_shard_flight(&sifs_file_system_id(), &path)?;
        let object = object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        self.namespace_lstat_object_for_path(&object, &path)
    }

    pub fn posix_stat_object(&mut self, object_id: &DfsObjectId) -> Result<Option<DfsStat>> {
        let source = self.object_read_source(object_id, None)?;
        let from_authority = matches!(
            source,
            crate::service_read::DfsObjectReadSource::Authority { .. }
        );
        let object = match self.current_read_object_metadata_from_source(object_id, source) {
            Ok(object) => object,
            Err(crate::DfsError::UnknownObject { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if object.kind == DfsObjectKind::Tombstone {
            return Ok(None);
        }
        if !from_authority {
            return Ok(Some(self.stat_object(&object)?));
        }
        // An object read from its authority takes its link count there too.
        let path = object.path.clone();
        self.with_stat_deadline(|service| {
            service.with_stat_read_rounds(|service| {
                service.stat_object_from_authorities(&object, &path, None)
            })
        })
        .map(Some)
    }

    pub fn posix_list_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        self.with_metadata_lookup_cache(|service| {
            let path = service.resolve_existing_posix_path(path, true)?;
            service.posix_collect_resolved_dir(&path)
        })
    }

    pub fn posix_list_dir_distributed(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
        if !self.transport.delegates_local_namespace_authority() {
            let _namespace_plan = self.namespace_execution.begin_planning();
            let path = self.resolve_posix_dot_components_distributed(path)?;
            let local = self.with_local_primary_metadata_read_snapshot(
                &path,
                |service, shard_snapshot| {
                    service.check_cancelled("distributed list")?;
                    let path = service.resolve_existing_posix_path_distributed(&path, true)?;
                    if service
                        .local_metadata_can_list_resolved_dir_from_snapshot(&path, shard_snapshot)?
                    {
                        return service.posix_collect_resolved_dir(&path).map(Some);
                    }
                    Ok(None)
                },
            )?;
            if let Some(entries) = local {
                return Ok(entries);
            }
        }
        let mut entries = Vec::new();
        try_walk_metadata_pages(|after| {
            let page = self.posix_list_dir_distributed_page(
                path,
                after,
                POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
            )?;
            entries.extend(page.entries);
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    pub fn posix_list_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        self.with_metadata_lookup_cache(|service| {
            let path = service.resolve_existing_posix_path(path, true)?;
            service.posix_list_resolved_dir_page(&path, after, max_entries)
        })
    }

    pub fn posix_list_dir_distributed_page(
        &mut self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        if self.local_metadata_can_list_resolved_dir(&path)? {
            return self.posix_list_resolved_dir_page(&path, after, max_entries);
        }
        let opened = self.posix_namespace_lstat_distributed(&path)?;
        self.owner_directory_page(&opened, after, max_entries, true)
    }

    /// List one bounded page from an exact no-link directory open beneath
    /// `root`. Child hardlink records are returned as links rather than being
    /// projected as their targets, and the opened path is never resolved
    /// again after the component walk.
    pub fn posix_list_dir_beneath_no_links_distributed_page(
        &mut self,
        root: &str,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<(DfsStat, DfsDirEntryPage)> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let opened = self.posix_open_beneath_no_links_distributed(root, path)?;
        if opened.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: opened.path.clone(),
            });
        }
        let local = match self.routed_namespace_shard_for_path(&opened.path)? {
            Some(shard) => self.reads_shard_from_local_metadata(&shard),
            None => !self.transport.delegates_local_namespace_authority(),
        };
        let page = if local {
            self.posix_list_opened_dir_no_links_page(&opened, after, max_entries)?
        } else {
            self.owner_directory_page(&opened, after, max_entries, false)?
        };
        Ok((opened, page))
    }

    fn owner_directory_page(
        &mut self,
        opened: &DfsStat,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
        project_hardlinks: bool,
    ) -> Result<DfsDirEntryPage> {
        let path = opened.path.as_str();
        if opened.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        if max_entries == 0 {
            return Ok(DfsDirEntryPage {
                entries: Vec::new(),
                next_cursor: None,
            });
        }
        let fs = sifs_file_system_id();
        let configs = self.metadata_shard_authorities(&fs)?;
        let shards = super::routing::directory_child_authority_shards(path, &configs)?
            .into_iter()
            .map(|shard| {
                (
                    crate::DfsMetadataShardId::new(shard.shard_id.clone()),
                    shard.clone(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        if after.is_some_and(|cursor| {
            cursor
                .shard_id
                .as_ref()
                .is_none_or(|id| !shards.contains_key(id))
                || normalize_posix_path(&cursor.path).map_or(true, |child| {
                    crate::posix_path::parent_posix_path(&child).as_deref() != Some(path)
                })
        }) {
            return Err(DfsPosixError::InvalidPath {
                path: path.to_string(),
            });
        }
        let pairs = crate::metadata_shard_pages::collect_resumed_sharded_page_entries(
            &shards,
            after,
            after.and_then(|cursor| cursor.shard_id.as_ref()),
            max_entries,
            crate::metadata_keys::local_directory_page_cursor,
            |_, shard, local_after, limit| {
                let result = self
                    .query_metadata_authority_impl(
                        shard,
                        crate::DfsMetadataSelection::DirectoryPage {
                            path: path.to_string(),
                            after: local_after.cloned(),
                            limit,
                        },
                    )
                    .map_err(|error| crate::MetadataBackendError::Serde(error.to_string()))?;
                let crate::DfsMetadataQueryResult::DirectoryPage {
                    entries,
                    after: next,
                } = result
                else {
                    return Err(crate::MetadataBackendError::Serde(
                        "directory authority returned another query kind".into(),
                    ));
                };
                let mut previous = local_after.cloned();
                for (object, cursor) in &entries {
                    let child = normalize_posix_path(&object.path)
                        .map_err(|error| crate::MetadataBackendError::Serde(error.to_string()))?;
                    if object.file_system_id != fs
                        || cursor.object_id != object.object_id
                        || cursor.path != object.path
                        || cursor.shard_id.is_some()
                        || crate::posix_path::parent_posix_path(&child).as_deref() != Some(path)
                        || previous.as_ref().is_some_and(|old| {
                            super::metadata::compare_directory_page_cursors(old, cursor)
                                != std::cmp::Ordering::Less
                        })
                    {
                        return Err(crate::MetadataBackendError::Serde(
                            "directory owner page changes exact scope or cursor order".into(),
                        ));
                    }
                    previous = Some(cursor.clone());
                }
                if entries.len() > limit
                    || next
                        .as_ref()
                        .is_some_and(|next| entries.last().is_none_or(|(_, cursor)| cursor != next))
                {
                    return Err(crate::MetadataBackendError::Serde(
                        "directory owner page changes its exact continuation".into(),
                    ));
                }
                Ok((entries, next))
            },
            |shard_id, _, (object, cursor)| {
                let selected =
                    crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
                        &fs,
                        &configs,
                        &object.path,
                    )
                    .map_err(|error| crate::MetadataBackendError::Serde(error.to_string()))?;
                if object.kind == DfsObjectKind::Tombstone
                    || selected
                        .as_ref()
                        .is_none_or(|owner| owner.shard_id != shard_id.0)
                {
                    return Ok(None);
                }
                Ok(Some((
                    object,
                    crate::metadata_keys::directory_page_cursor_with_shard(cursor, shard_id),
                )))
            },
        )?;
        let (objects, next_cursor) = crate::metadata_pages::finish_cursor_page(pairs, max_entries);
        let mut entries = Vec::with_capacity(objects.len());
        for object in objects {
            let target = if project_hardlinks
                && self.metadata_backend.is_none()
                && self.transport.delegates_local_namespace_authority()
                && self.control_plane.is_some()
            {
                self.resolve_hardlink_target_distributed(&object, Some(&configs))?
            } else if project_hardlinks {
                super::metadata::resolve_hardlink_target_with(&object, |id| {
                    self.current_object_from_authority(&fs, id)
                        .map_err(Into::into)
                })?
            } else {
                object.clone()
            };
            entries.push(Self::directory_entry_projection(&object, &target)?);
        }
        Ok(DfsDirEntryPage {
            entries,
            next_cursor,
        })
    }

    fn local_metadata_can_list_resolved_dir(&mut self, path: &str) -> Result<bool> {
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        self.local_metadata_can_list_resolved_dir_from_snapshot(path, shard_snapshot.as_deref())
    }

    /// Whether this service lists `path` from its own metadata. Every other
    /// listing, including a replica that does not yet hold the directory, is
    /// read page by page from the directory's owner.
    fn local_metadata_can_list_resolved_dir_from_snapshot(
        &mut self,
        path: &str,
        shard_snapshot: Option<&[crate::DfsMetadataShardConfig]>,
    ) -> Result<bool> {
        if self.transport.delegates_local_namespace_authority() {
            return Ok(false);
        }
        let Some(shard) = shard_snapshot
            .map(|shards| self.routed_namespace_shard_for_path_from_snapshot(path, shards))
            .transpose()?
            .flatten()
        else {
            return Ok(true);
        };
        if !self.reads_shard_from_local_metadata(&shard) {
            return Ok(false);
        }
        Ok(self
            .lookup_sifs_path(path)?
            .is_some_and(|object| object.kind == DfsObjectKind::Directory))
    }

    fn posix_collect_resolved_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let mut entries = Vec::new();
        try_walk_metadata_pages(|cursor| {
            let page =
                self.posix_list_resolved_dir_page(path, cursor, POSIX_LIST_DIR_COLLECT_PAGE_SIZE)?;
            entries.extend(page.entries);
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    fn directory_entry_projection(
        object: &DfsObjectMetadata,
        target: &DfsObjectMetadata,
    ) -> Result<DfsDirEntry> {
        let path = normalize_posix_path(&object.path)?;
        Ok(DfsDirEntry {
            name: path
                .rsplit('/')
                .next()
                .filter(|name| !name.is_empty())
                .unwrap_or("/")
                .to_string(),
            path: path,
            kind: target.kind.clone(),
            object_id: target.object_id.clone(),
            version: target.version.clone(),
            // Directory pages stay metadata-only: do not turn a list into one manifest or
            // version lookup per child. Current metadata supplies the visible size;
            // an absent metadata size remains unknown.
            size_bytes: target.content_size_bytes,
            modified_at_unix_nanos: target.modified_at_unix_nanos,
            policy: target.policy.clone(),
            current_locations: target.current_locations.clone(),
        })
    }

    fn posix_list_resolved_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        let object = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        let mut entries = Vec::new();
        let page = self.child_objects_for_sifs_dir_page(path, after, max_entries)?;
        for object in page.objects {
            if object.kind == DfsObjectKind::Tombstone {
                continue;
            }
            let target = self.resolve_hardlink_target(&object)?;
            entries.push(Self::directory_entry_projection(&object, &target)?);
        }
        Ok(DfsDirEntryPage {
            entries,
            next_cursor: page.next_cursor,
        })
    }

    fn posix_list_opened_dir_no_links_page(
        &self,
        opened: &DfsStat,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        if opened.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: opened.path.clone(),
            });
        }
        let path = normalize_posix_path(&opened.path)?;
        let page = self.child_objects_for_sifs_dir_page(&path, after, max_entries)?;
        let mut entries = Vec::with_capacity(page.objects.len());
        for object in page.objects {
            if object.kind == DfsObjectKind::Tombstone {
                continue;
            }
            entries.push(Self::directory_entry_projection(&object, &object)?);
        }
        Ok(DfsDirEntryPage {
            entries,
            next_cursor: page.next_cursor,
        })
    }

    pub fn posix_list_dir_update_access(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let resolved_path = self.resolve_existing_posix_path_distributed(path, true)?;
        let entries = self.posix_list_dir(&resolved_path)?;
        let object =
            self.lookup_sifs_path(&resolved_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: resolved_path.clone(),
                })?;
        let mut object = self.resolve_hardlink_target(&object)?;
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: resolved_path,
            });
        }
        if !self.is_local_metadata_primary_for_object(&object)? {
            return Ok(entries);
        }
        object.accessed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(entries)
    }
}
