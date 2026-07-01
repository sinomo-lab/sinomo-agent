//! Shared POSIX metadata, path, and stat helpers.
//!
//! These helpers are the internal backbone for POSIX namespace code: path
//! resolution, metadata lookup, hardlink resolution, directory validation,
//! directory timestamp touches, and `DfsStat` projection. They stay private to
//! the POSIX facade so writes, reads, routed operations, jobs, and queries all
//! share the same interpretation without adding to the crate-level API.

use super::{POSIX_LINK_COUNT_PAGE_SIZE, POSIX_LIST_DIR_COLLECT_PAGE_SIZE, now_unix_nanos};
use crate::metadata_pages::try_walk_metadata_pages;
use crate::namespace::{metadata_path, next_version};
use crate::posix_model::{DfsPosixError, DfsStat, Result, read_conflict_error};
use crate::posix_object::{object_mode_or_default, object_timestamp_or_default};
use crate::posix_path::{
    append_posix_components, join_posix_path, normalize_posix_path,
    normalize_posix_path_with_trailing_directory, parent_posix_path,
    posix_path_is_at_or_under_prefix, resolve_posix_symlink_target,
};
use crate::{
    DfsDirectoryPage, DfsDirectoryPageCursor, DfsFileSystemId, DfsMessageTransport,
    DfsNamespaceOperation, DfsObjectId, DfsObjectKind, DfsObjectMetadata, DfsReadData,
    NetworkedDfsReadOutcome, NetworkedDfsService, sifs_file_system_id,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub(super) fn lookup_sifs_path(&self, path: &str) -> Result<Option<DfsObjectMetadata>> {
        self.lookup_metadata_path(&sifs_file_system_id(), path)
    }

    pub(super) fn resolve_posix_path(&self, path: &str, follow_final: bool) -> Result<String> {
        let mut current = normalize_posix_path(path)?;
        for _ in 0..40 {
            let components = current
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut resolved = "/".to_string();
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let candidate = join_posix_path(&resolved, component);
                let final_component = index + 1 == components.len();
                if final_component && !follow_final {
                    resolved = candidate;
                    continue;
                }
                let Some(object) = self.lookup_sifs_path(&candidate)? else {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                };
                if object.kind == DfsObjectKind::Tombstone {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                }
                let object = self.resolve_hardlink_target(&object)?;
                if object.kind == DfsObjectKind::Symlink {
                    let target = self.symlink_target_for_object(&candidate, &object)?;
                    current = resolve_posix_symlink_target(&candidate, &target)?;
                    for rest in &components[index + 1..] {
                        current = join_posix_path(&current, rest);
                    }
                    current = normalize_posix_path(&current)?;
                    restarted = true;
                    break;
                }
                resolved = candidate;
            }
            if !restarted {
                return Ok(resolved);
            }
        }
        Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        })
    }

    pub(super) fn resolve_posix_path_distributed(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        let mut current = normalize_posix_path(path)?;
        for _ in 0..40 {
            let components = current
                .trim_start_matches('/')
                .split('/')
                .filter(|component| !component.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let mut resolved = "/".to_string();
            let mut restarted = false;
            for (index, component) in components.iter().enumerate() {
                let candidate = join_posix_path(&resolved, component);
                let final_component = index + 1 == components.len();
                if final_component && !follow_final {
                    resolved = candidate;
                    continue;
                }
                let Some(object) = self.lookup_sifs_path(&candidate)? else {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                };
                if object.kind == DfsObjectKind::Tombstone {
                    resolved = append_posix_components(candidate, &components[index + 1..])?;
                    return Ok(resolved);
                }
                let object = self.resolve_hardlink_target(&object)?;
                if object.kind == DfsObjectKind::Symlink {
                    let target = self.symlink_target_for_object_distributed(&candidate, &object)?;
                    current = resolve_posix_symlink_target(&candidate, &target)?;
                    for rest in &components[index + 1..] {
                        current = join_posix_path(&current, rest);
                    }
                    current = normalize_posix_path(&current)?;
                    restarted = true;
                    break;
                }
                resolved = candidate;
            }
            if !restarted {
                return Ok(resolved);
            }
        }
        Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        })
    }

    pub(super) fn resolve_existing_posix_path(
        &self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path(&path, follow_final || requires_directory)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory(&path)?;
        }
        Ok(path)
    }

    pub(super) fn resolve_existing_posix_path_distributed(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<String> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path =
            self.resolve_posix_path_distributed(&path, follow_final || requires_directory)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory(&path)?;
        }
        Ok(path)
    }

    fn symlink_target_for_object(&self, path: &str, object: &DfsObjectMetadata) -> Result<String> {
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: path.to_string(),
            });
        }
        match self.read_local_object_with_state(&object.object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path.to_string(), conflicts))
            }
        }
    }

    fn symlink_target_for_object_distributed(
        &mut self,
        path: &str,
        object: &DfsObjectMetadata,
    ) -> Result<String> {
        if object.kind != DfsObjectKind::Symlink {
            return Err(DfsPosixError::NotSymlink {
                path: path.to_string(),
            });
        }
        match self.read_object_metadata_or_fetch_remote_without_cache(object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound {
                path: path.to_string(),
            }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path.to_string(), conflicts))
            }
        }
    }

    pub(super) fn lookup_metadata_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        if self.has_tombstone_ancestor(file_system_id, path)? {
            return Ok(None);
        }
        self.lookup_metadata_path_raw(file_system_id, path)
    }

    pub(super) fn lookup_metadata_path_raw(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        if let Some(tombstone) = self
            .tombstones_by_metadata_path
            .get(&(file_system_id.clone(), metadata_path(path)))
        {
            return Ok(Some(tombstone.clone()));
        }
        let memory_object = self
            .metadata
            .object_by_normalized_posix_path(file_system_id, path)
            .cloned();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let backend_object =
                backend.get_object_by_path(file_system_id, &metadata_path(path))?;
            let object = match (memory_object.as_ref(), backend_object.as_ref()) {
                (Some(memory_object), Some(backend_object)) => {
                    Some(self.resolve_authoritative_object_metadata(
                        &backend_object.object_id,
                        Some(memory_object),
                        Some(backend_object),
                    )?)
                }
                (Some(memory_object), None) => Some(memory_object.clone()),
                (_, Some(backend_object)) => Some(backend_object.clone()),
                (None, None) => None,
            };
            return object
                .map(|object| self.object_metadata_with_receipt_locations(object))
                .transpose()
                .map_err(Into::into);
        }
        Ok(memory_object)
    }

    fn has_tombstone_ancestor(&self, file_system_id: &DfsFileSystemId, path: &str) -> Result<bool> {
        let path = normalize_posix_path(path)?;
        if path == "/" {
            return Ok(false);
        }
        let mut parent = path
            .rsplit_once('/')
            .map(|(parent, _)| parent)
            .unwrap_or("/");
        if parent.is_empty() {
            parent = "/";
        }
        loop {
            if let Some(object) = self.lookup_metadata_path_raw(file_system_id, parent)?
                && object.kind == DfsObjectKind::Tombstone
            {
                return Ok(true);
            }
            if parent == "/" {
                return Ok(false);
            }
            parent = parent
                .rsplit_once('/')
                .map(|(ancestor, _)| if ancestor.is_empty() { "/" } else { ancestor })
                .unwrap_or("/");
        }
    }

    pub(super) fn child_objects_for_sifs_dir(&self, path: &str) -> Result<Vec<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut children = Vec::new();
            try_walk_metadata_pages(|cursor| {
                let page = backend.child_objects_for_directory_page(
                    &file_system_id,
                    path,
                    cursor,
                    POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
                )?;
                children.extend(page.objects);
                Ok::<_, DfsPosixError>(page.next_cursor)
            })?;
            return Ok(children);
        }
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        Ok(self
            .metadata
            .objects
            .values()
            .filter(|object| {
                if object.file_system_id != file_system_id
                    || object.kind == DfsObjectKind::Tombstone
                {
                    return false;
                }
                let Ok(object_path) = normalize_posix_path(&object.path) else {
                    return false;
                };
                if object_path == path || !object_path.starts_with(&prefix) {
                    return false;
                }
                let rest = object_path.trim_start_matches(&prefix);
                !rest.is_empty() && !rest.contains('/')
            })
            .cloned()
            .collect())
    }

    pub(super) fn child_objects_for_sifs_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirectoryPage> {
        if max_entries == 0 {
            return Ok(DfsDirectoryPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        }
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.child_objects_for_directory_page(
                &file_system_id,
                path,
                after,
                max_entries,
            )?);
        }
        let mut objects = self.child_objects_for_sifs_dir(path)?;
        objects.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        if let Some(after) = after {
            objects.retain(|object| {
                (&object.path, &object.object_id) > (&after.path, &after.object_id)
            });
        }
        let has_more = objects.len() > max_entries;
        objects.truncate(max_entries);
        let next_cursor = if has_more {
            objects.last().map(|object| DfsDirectoryPageCursor {
                path: object.path.clone(),
                object_id: object.object_id.clone(),
                dentry: None,
                shard_id: None,
            })
        } else {
            None
        };
        Ok(DfsDirectoryPage {
            objects,
            next_cursor,
        })
    }

    pub(super) fn sifs_dir_has_children(&self, path: &str) -> Result<bool> {
        Ok(!self
            .child_objects_for_sifs_dir_page(path, None, 1)?
            .objects
            .is_empty())
    }

    pub(super) fn hardlink_count_to_object(&self, target_object_id: &DfsObjectId) -> Result<usize> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.hardlink_count_to_object(&file_system_id, target_object_id)?);
        }
        Ok(self
            .metadata
            .objects
            .values()
            .filter(|candidate| {
                candidate.file_system_id == file_system_id
                    && candidate.kind == DfsObjectKind::Hardlink
                    && candidate.link_target_object_id.as_ref() == Some(target_object_id)
            })
            .count())
    }

    pub(super) fn first_hardlink_to_object(
        &self,
        target_object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.first_hardlink_to_object(&file_system_id, target_object_id)?);
        }
        let mut first = None::<DfsObjectMetadata>;
        for candidate in self.metadata.objects.values() {
            if candidate.file_system_id != file_system_id
                || candidate.kind != DfsObjectKind::Hardlink
                || candidate.link_target_object_id.as_ref() != Some(target_object_id)
            {
                continue;
            }
            if first.as_ref().is_none_or(|current| {
                candidate
                    .path
                    .cmp(&current.path)
                    .then_with(|| candidate.object_id.cmp(&current.object_id))
                    .is_lt()
            }) {
                first = Some(candidate.clone());
            }
        }
        Ok(first)
    }

    fn child_directory_count_for_sifs_dir(&self, path: &str) -> Result<u64> {
        let mut count = 0u64;
        try_walk_metadata_pages(|cursor| {
            let page =
                self.child_objects_for_sifs_dir_page(path, cursor, POSIX_LINK_COUNT_PAGE_SIZE)?;
            count = count.saturating_add(
                page.objects
                    .iter()
                    .filter(|child| child.kind == DfsObjectKind::Directory)
                    .count() as u64,
            );
            Ok::<_, DfsPosixError>(page.next_cursor)
        })?;
        Ok(count)
    }

    pub(super) fn object_by_id(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsObjectMetadata>> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(object) = backend.get_object(object_id)?
        {
            return self
                .object_metadata_with_receipt_locations(object)
                .map(Some)
                .map_err(Into::into);
        }
        self.metadata
            .object(object_id)
            .cloned()
            .map(|object| self.object_metadata_with_receipt_locations(object))
            .transpose()
            .map_err(Into::into)
    }

    pub(crate) fn resolve_hardlink_target(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<DfsObjectMetadata> {
        let mut current = object.clone();
        for _ in 0..16 {
            if current.kind != DfsObjectKind::Hardlink {
                return Ok(current);
            }
            let Some(target_id) = current.link_target_object_id.clone() else {
                return Err(DfsPosixError::NotFound { path: current.path });
            };
            current = self
                .object_by_id(&target_id)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: current.path.clone(),
                })?;
        }
        Err(DfsPosixError::InvalidPath {
            path: object.path.clone(),
        })
    }

    /// Stat for a path that a routed unlink/rmdir just tombstoned; the
    /// response journal batches were replayed before this runs, so the local
    /// lookup observes the authoritative tombstone like a local removal does.
    pub(super) fn stat_replayed_tombstone(&self, path: &str) -> Result<DfsStat> {
        let resolved = self.resolve_posix_path(path, false)?;
        let object = self
            .lookup_sifs_path(&resolved)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: resolved.clone(),
            })?;
        self.stat_object(&object)
    }

    pub(super) fn stat_target_for_path(&self, path: &str) -> Result<DfsObjectMetadata> {
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
        self.resolve_hardlink_target(&object)
    }

    pub(super) fn ensure_parent_directory(&self, path: &str) -> Result<()> {
        let Some(parent) = path.rsplit_once('/').map(|(parent, _)| parent) else {
            return Ok(());
        };
        let parent = if parent.is_empty() { "/" } else { parent };
        match self.lookup_sifs_path(parent)? {
            Some(object) if object.kind == DfsObjectKind::Directory => Ok(()),
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                Err(DfsPosixError::MissingParent {
                    path: parent.to_string(),
                })
            }
            Some(_) => Err(DfsPosixError::NotDirectory {
                path: parent.to_string(),
            }),
            None => Err(DfsPosixError::MissingParent {
                path: parent.to_string(),
            }),
        }
    }

    pub(super) fn ensure_resolved_path_is_directory(&self, path: &str) -> Result<()> {
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
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            Ok(())
        } else {
            Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            })
        }
    }

    pub(super) fn ensure_resolved_path_is_directory_no_follow(&self, path: &str) -> Result<()> {
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
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            Ok(())
        } else {
            Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            })
        }
    }

    pub(crate) fn touch_parent_directory_modified(&mut self, path: &str) -> Result<()> {
        let Some(parent) = parent_posix_path(path) else {
            return Ok(());
        };
        self.touch_directory_modified(&parent)
    }

    pub fn posix_touch_directory_modified(&mut self, path: &str) -> Result<()> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&path)? {
            return self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::TouchDirectoryModified { path },
            );
        }
        self.touch_directory_modified(&path)
    }

    pub fn posix_touch_directory_modified_without_local_replay(
        &mut self,
        path: &str,
    ) -> Result<()> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&path)? {
            return self.route_namespace_operation_without_local_replay(
                primary,
                DfsNamespaceOperation::TouchDirectoryModified { path },
            );
        }
        self.touch_directory_modified(&path)
    }

    pub(super) fn touch_directory_modified(&mut self, path: &str) -> Result<()> {
        let mut object =
            self.lookup_sifs_path(path)?
                .ok_or_else(|| DfsPosixError::MissingParent {
                    path: path.to_string(),
                })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::MissingParent {
                path: path.to_string(),
            });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        let now = now_unix_nanos()
            .max(object.modified_at_unix_nanos.saturating_add(1))
            .max(object.changed_at_unix_nanos.saturating_add(1));
        object.modified_at_unix_nanos = now;
        object.changed_at_unix_nanos = now;
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(())
    }

    pub(super) fn stat_object(&self, object: &DfsObjectMetadata) -> Result<DfsStat> {
        self.stat_object_for_path(object, &object.path)
    }

    pub(super) fn stat_object_for_path(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
    ) -> Result<DfsStat> {
        self.stat_object_for_path_with_link_count_hint(object, path, None)
    }

    pub(super) fn stat_object_for_path_with_link_count_hint(
        &self,
        object: &DfsObjectMetadata,
        path: &str,
        link_count_hint: Option<u64>,
    ) -> Result<DfsStat> {
        let object = self.resolve_hardlink_target(object)?;
        let size_bytes = self.object_content_size_hint(&object)?;
        let link_count = match link_count_hint {
            Some(count)
                if object.kind != DfsObjectKind::Directory
                    && object.kind != DfsObjectKind::Hardlink =>
            {
                count
            }
            _ => self.link_count_for_object(&object)?,
        };
        Ok(DfsStat {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: normalize_posix_path(path)?,
            kind: object.kind.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes,
            link_count,
            mode: object_mode_or_default(&object),
            uid: object.uid,
            gid: object.gid,
            accessed_at_unix_nanos: object_timestamp_or_default(object.accessed_at_unix_nanos),
            modified_at_unix_nanos: object_timestamp_or_default(object.modified_at_unix_nanos),
            created_at_unix_nanos: object_timestamp_or_default(object.created_at_unix_nanos),
            changed_at_unix_nanos: object_timestamp_or_default(object.changed_at_unix_nanos),
            policy: object.policy.clone(),
            current_locations: object.current_locations.clone(),
        })
    }

    pub(super) fn link_count_for_object(&self, object: &DfsObjectMetadata) -> Result<u64> {
        if object.kind == DfsObjectKind::Hardlink {
            let target = self.resolve_hardlink_target(object)?;
            return self.link_count_for_object(&target);
        }
        if object.kind != DfsObjectKind::Directory {
            let count = self.hardlink_count_to_object(&object.object_id)?;
            return Ok(1u64.saturating_add(count as u64));
        }
        let path = normalize_posix_path(&object.path)?;
        let child_directory_count = self.child_directory_count_for_sifs_dir(&path)?;
        Ok(2u64.saturating_add(child_directory_count))
    }

    pub(super) fn first_hardlink_alias_outside_prefix(
        &self,
        object: &DfsObjectMetadata,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let prefix = normalize_posix_path(prefix)?;
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.first_hardlink_to_object_outside_prefix(
                &file_system_id,
                &object.object_id,
                &prefix,
            )?);
        }
        let mut first = None::<DfsObjectMetadata>;
        for candidate in self.metadata.objects.values() {
            if candidate.file_system_id != file_system_id
                || candidate.kind != DfsObjectKind::Hardlink
                || candidate.link_target_object_id.as_ref() != Some(&object.object_id)
            {
                continue;
            }
            let Ok(path) = normalize_posix_path(&candidate.path) else {
                continue;
            };
            if posix_path_is_at_or_under_prefix(&path, &prefix) {
                continue;
            }
            if first.as_ref().is_none_or(|current| {
                candidate
                    .path
                    .cmp(&current.path)
                    .then_with(|| candidate.object_id.cmp(&current.object_id))
                    .is_lt()
            }) {
                first = Some(candidate.clone());
            }
        }
        Ok(first)
    }
}
