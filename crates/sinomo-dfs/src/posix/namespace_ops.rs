//! POSIX namespace mutation helpers.
//!
//! This module owns unlink, rmdir, and same-shard rename. These operations
//! share tombstone transaction handling, hardlink promotion, parent directory
//! timestamp updates, and routed namespace wrappers.

use super::{POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS, now_unix_nanos};
use crate::namespace::{namespace_transaction_id_for_path, next_version};
use crate::posix_model::{DfsPosixError, DfsStat, Result};
use crate::posix_path::{
    hidden_alias_tombstone_path, normalize_posix_path,
    normalize_posix_path_with_trailing_directory, parent_posix_path,
};
use crate::{
    DfsMessageTransport, DfsNamespaceJobStatus, DfsNamespaceOperation, DfsNamespaceTransaction,
    DfsObjectId, DfsObjectKind, DfsObjectMetadata, NetworkedDfsService, sifs_file_system_id,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn posix_unlink(&mut self, path: &str) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::Unlink {
                    path: path.to_string(),
                },
            )?;
            return self.stat_replayed_tombstone_synced(&normalized);
        }
        self.posix_unlink_local(path)
    }

    pub(crate) fn posix_unlink_local(&mut self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        let mut object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if object.kind == DfsObjectKind::Hardlink {
            if let Some(target_object_id) = object.link_target_object_id.clone()
                && let Some(mut target) = self.object_by_id(&target_object_id)?
            {
                target.changed_at_unix_nanos = now_unix_nanos();
                target.version = next_version(&target.version);
                self.add_object_metadata(target)?;
            }
            object = self.tombstone_path_object(object);
            self.add_object_metadata(object.clone())?;
            self.touch_parent_directory_modified(&path)?;
            return self.stat_object(&object);
        }
        if self.link_count_for_object(&object)? > 1 {
            // Unlinking a canonical record promotes one of its hardlink
            // aliases. When the chosen alias lives in a shard owned by
            // another primary, the promotion is a cross-shard move of the
            // canonical record onto the alias path.
            if let Some(alias) = self.first_hardlink_to_object(&object.object_id)? {
                let alias_path = normalize_posix_path(&alias.path)?;
                if let Some(destination_primary) =
                    self.routed_namespace_primary_for_path(&alias_path)?
                {
                    self.coordinate_cross_shard_rename(&path, &alias_path, destination_primary)?;
                    return self.stat_replayed_tombstone(&path);
                }
            }
            return self.unlink_canonical_with_hardlinks(object, &path);
        }
        object = self.tombstone_path_object(object);
        self.add_object_metadata(object.clone())?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_rmdir(&mut self, path: &str) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::Rmdir {
                    path: path.to_string(),
                },
            )?;
            return self.stat_replayed_tombstone_synced(&normalized);
        }
        self.posix_rmdir_local(path)
    }

    pub(crate) fn posix_rmdir_local(&mut self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        if path == "/" {
            return Err(DfsPosixError::InvalidPath { path });
        }
        let mut object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path });
        }
        if self.sifs_dir_has_children(&path)? {
            return Err(DfsPosixError::DirectoryNotEmpty { path });
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.current_locations = vec![self.node_id.clone()];
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_path("rmdir", &object.object_id, &path),
            file_system_id: object.file_system_id.clone(),
            objects: vec![object.clone()],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_rename(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let normalized_from = normalize_posix_path(from)?;
        let normalized_to = normalize_posix_path(to)?;
        let from_primary = self.routed_namespace_primary_for_path(&normalized_from)?;
        let to_primary = self.routed_namespace_primary_for_path(&normalized_to)?;
        match (from_primary, to_primary) {
            (None, None) => self.posix_rename_local(from, to),
            (None, Some(_)) => {
                // This node owns the source side and coordinates the
                // cross-shard move itself.
                self.namespace_coordinator_dispatch_rename(&sifs_file_system_id(), from, to)?;
                self.stat_routed_namespace_result(&normalized_to, true)
            }
            (Some(source_primary), to_primary) => {
                // The source-side shard primary coordinates; it adopts the
                // record into the destination shard when that lives
                // elsewhere.
                self.route_namespace_operation(
                    source_primary.clone(),
                    DfsNamespaceOperation::Rename {
                        from: from.to_string(),
                        to: to.to_string(),
                    },
                )?;
                if to_primary.is_some_and(|to_primary| to_primary != source_primary) {
                    self.sync_remote_namespace_path(&normalized_to)?;
                }
                self.stat_routed_namespace_result(&normalized_to, true)
            }
        }
    }

    pub(crate) fn posix_rename_local(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let (from, from_requires_directory) = normalize_posix_path_with_trailing_directory(from)?;
        let (to, to_requires_directory) = normalize_posix_path_with_trailing_directory(to)?;
        let from = self.resolve_posix_path_distributed(&from, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&from)?;
        if from_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&from)?;
        }
        let to = self.resolve_posix_path_distributed(&to, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&to)?;
        if from == "/" || to == "/" {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{from} -> {to}"),
            });
        }
        if from == to {
            return self.posix_stat(&from);
        }
        let mut source = self
            .lookup_sifs_path(&from)?
            .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: from });
        }
        if to_requires_directory && source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path: to });
        }
        self.ensure_parent_directory(&to)?;
        if source.kind == DfsObjectKind::Directory {
            let child_prefix = format!("{}/", from.trim_end_matches('/'));
            if to.starts_with(&child_prefix) {
                return Err(DfsPosixError::InvalidPath {
                    path: format!("{from} -> {to}"),
                });
            }
            let mut job = self.posix_rename_directory_bounded_resolved(
                &from,
                &to,
                source.clone(),
                POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
            )?;
            while job.status != DfsNamespaceJobStatus::Completed {
                job = self.posix_resume_namespace_job_bounded(
                    &job.job_id,
                    POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
                )?;
            }
            return self.posix_stat(&to);
        }

        let old_source_path = source.path.clone();
        let mut moved_children = Vec::new();
        let mut hardlink_target_updates = Vec::new();

        if let Some(mut destination) = self.lookup_sifs_path(&to)? {
            if destination.kind != DfsObjectKind::Tombstone {
                let source_target = self.resolve_hardlink_target(&source)?;
                let mut destination_target = self.resolve_hardlink_target(&destination)?;
                if source_target.object_id == destination_target.object_id {
                    return self.posix_stat(&to);
                }
                match (&source.kind, &destination.kind) {
                    (DfsObjectKind::Directory, DfsObjectKind::Directory) => {
                        if self.sifs_dir_has_children(&to)? {
                            return Err(DfsPosixError::DirectoryNotEmpty { path: to });
                        }
                    }
                    (DfsObjectKind::Directory, _) => {
                        return Err(DfsPosixError::NotDirectory { path: to });
                    }
                    (_, DfsObjectKind::Directory) => {
                        return Err(DfsPosixError::IsDirectory { path: to });
                    }
                    _ => {}
                }
                let destination_target_changed_at = now_unix_nanos();
                if !matches!(
                    destination.kind,
                    DfsObjectKind::Hardlink | DfsObjectKind::Directory
                ) && self.link_count_for_object(&destination_target)? > 1
                {
                    let alias = self
                        .first_hardlink_to_object(&destination.object_id)?
                        .ok_or_else(|| DfsPosixError::NotFound { path: to.clone() })?;
                    // Demoting the replaced destination onto its alias would
                    // write into the alias's shard; that shard's primary must
                    // be this node, or the promotion would fork its journal.
                    let alias_path = normalize_posix_path(&alias.path)?;
                    if self
                        .routed_namespace_primary_for_path(&alias_path)?
                        .is_some()
                    {
                        return Err(DfsPosixError::InvalidPath {
                            path: format!(
                                "{to} has a hardlink alias at {alias_path} owned by another metadata shard primary"
                            ),
                        });
                    }
                    let alias_tombstone = self.tombstone_path_object_at(
                        alias.clone(),
                        hidden_alias_tombstone_path(&alias.object_id),
                    );

                    destination.path = alias.path.clone();
                    destination.changed_at_unix_nanos = destination_target_changed_at;
                    destination.version = next_version(&destination.version);
                    moved_children.push(alias_tombstone);
                    moved_children.push(destination);
                } else {
                    if destination.kind == DfsObjectKind::Hardlink {
                        destination_target.changed_at_unix_nanos = destination_target_changed_at;
                        destination_target.version = next_version(&destination_target.version);
                        hardlink_target_updates.push(destination_target);
                    }
                    moved_children.push(self.tombstone_path_object(destination));
                }
            }
        }

        if source.kind == DfsObjectKind::Hardlink
            && let Some(target_object_id) = source.link_target_object_id.as_ref()
            && let Some(mut source_target) = self.object_by_id(target_object_id)?
        {
            source_target.changed_at_unix_nanos = now_unix_nanos();
            source_target.version = next_version(&source_target.version);
            if !hardlink_target_updates
                .iter()
                .any(|object: &DfsObjectMetadata| object.object_id == source_target.object_id)
            {
                hardlink_target_updates.push(source_target);
            }
        }

        let old_path_tombstone = self.tombstone_path_object_at(
            {
                let mut object = source.clone();
                object.object_id = DfsObjectId::generate();
                object
            },
            old_source_path,
        );

        source.path = to;
        source.changed_at_unix_nanos = now_unix_nanos();
        source.version = next_version(&source.version);
        let mut transaction_objects = vec![old_path_tombstone, source.clone()];
        transaction_objects.extend(moved_children);
        transaction_objects.extend(hardlink_target_updates);
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_path(
                "rename",
                &source.object_id,
                &source.path,
            ),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_modified(&from)?;
        if parent_posix_path(&from) != parent_posix_path(&source.path) {
            self.touch_parent_directory_modified(&source.path)?;
        }
        self.stat_object(&source)
    }

    fn unlink_canonical_with_hardlinks(
        &mut self,
        mut object: DfsObjectMetadata,
        old_path: &str,
    ) -> Result<DfsStat> {
        let alias = self
            .first_hardlink_to_object(&object.object_id)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: old_path.to_string(),
            })?;
        let alias_tombstone = self
            .tombstone_path_object_at(alias.clone(), hidden_alias_tombstone_path(&alias.object_id));

        let old_path_tombstone = self.tombstone_path_object_at(
            {
                let mut object = object.clone();
                object.object_id = DfsObjectId::generate();
                object
            },
            old_path.to_string(),
        );

        object.path = alias.path.clone();
        object.changed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);

        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_path(
                "unlink-hardlink",
                &object.object_id,
                old_path,
            ),
            file_system_id: object.file_system_id.clone(),
            objects: vec![old_path_tombstone.clone(), alias_tombstone, object],
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_modified(old_path)?;
        self.stat_object(&old_path_tombstone)
    }

    fn tombstone_path_object(&self, object: DfsObjectMetadata) -> DfsObjectMetadata {
        self.tombstone_path_object_at(object, String::new())
    }

    fn tombstone_path_object_at(
        &self,
        mut object: DfsObjectMetadata,
        path: String,
    ) -> DfsObjectMetadata {
        if !path.is_empty() {
            object.path = path;
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.content_hash = None;
        object.content_size_bytes = None;
        object.link_target_object_id = None;
        object.current_locations = vec![self.node_id.clone()];
        object
    }
}
