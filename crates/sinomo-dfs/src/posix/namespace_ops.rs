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
    DfsObjectId, DfsObjectKind, DfsObjectMetadata, NetworkedDfsService,
    namespace_transaction_id_for_batch, object_version_record, sifs_file_system_id,
};
use time::OffsetDateTime;

fn ensure_same_atomic_namespace_shard(
    from_shard_id: Option<&str>,
    to_shard_id: Option<&str>,
    from_path: &str,
    to_path: &str,
) -> Result<()> {
    if from_shard_id != to_shard_id {
        return Err(DfsPosixError::InvalidPath {
            path: format!(
                "atomic namespace rename crosses metadata shards: {from_path} ({from_shard_id:?}) -> {to_path} ({to_shard_id:?})"
            ),
        });
    }
    Ok(())
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn posix_unlink(&mut self, path: &str) -> Result<DfsStat> {
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_unlink_with_current_deadline(path)
        })
    }

    fn posix_unlink_with_current_deadline(&mut self, path: &str) -> Result<DfsStat> {
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
        let mut objects = vec![object.clone()];
        if let Some(parent_path) = parent_posix_path(&path) {
            objects.push(self.directory_modified_metadata(&parent_path)?);
        }
        let versions = objects
            .iter()
            .map(|object| object_version_record(object, object.content_size_bytes))
            .collect::<Vec<_>>();
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_batch(
                "unlink",
                &object.file_system_id,
                &objects,
                &versions,
            ),
            file_system_id: object.file_system_id.clone(),
            objects,
            versions,
            advisory_lock_states: Vec::new(),
            created_at: OffsetDateTime::now_utc(),
        })?;
        self.stat_object(&object)
    }

    pub fn posix_rmdir(&mut self, path: &str) -> Result<DfsStat> {
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_rmdir_with_current_deadline(path)
        })
    }

    fn posix_rmdir_with_current_deadline(&mut self, path: &str) -> Result<DfsStat> {
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
        object = self.tombstone_path_object(object);
        let mut objects = vec![object.clone()];
        if let Some(parent_path) = parent_posix_path(&path) {
            objects.push(self.directory_modified_metadata(&parent_path)?);
        }
        let versions = objects
            .iter()
            .map(|object| object_version_record(object, object.content_size_bytes))
            .collect::<Vec<_>>();
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id_for_batch(
                "rmdir",
                &object.file_system_id,
                &objects,
                &versions,
            ),
            file_system_id: object.file_system_id.clone(),
            objects,
            versions,
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.stat_object(&object)
    }

    pub fn posix_rename(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let normalized_from = normalize_posix_path(from)?;
        let normalized_to = normalize_posix_path(to)?;
        let mut primaries = self
            .routed_namespace_primaries_for_paths_fresh([
                normalized_from.as_str(),
                normalized_to.as_str(),
            ])?
            .into_iter();
        let from_primary = primaries.next().unwrap_or(None);
        let to_primary = primaries.next().unwrap_or(None);
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

    /// Atomically replace one non-directory namespace entry without crossing a
    /// metadata shard boundary. This is the publish primitive for stable
    /// symlink pointers: callers get one namespace transaction or a fail-closed
    /// rejection, never the cross-shard adoption protocol or recursive job.
    pub fn posix_rename_single_namespace_atomic(
        &mut self,
        from: &str,
        to: &str,
    ) -> Result<DfsStat> {
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_rename_single_namespace_atomic_with_current_deadline(from, to)
        })
    }

    fn posix_rename_single_namespace_atomic_with_current_deadline(
        &mut self,
        from: &str,
        to: &str,
    ) -> Result<DfsStat> {
        let normalized_from = normalize_posix_path(from)?;
        let normalized_to = normalize_posix_path(to)?;
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        let selected_shard = if let Some(shards) = shard_snapshot.as_deref() {
            let from_shard =
                self.select_sifs_metadata_shard_for_path_from_snapshot(&normalized_from, shards)?;
            let to_shard =
                self.select_sifs_metadata_shard_for_path_from_snapshot(&normalized_to, shards)?;
            ensure_same_atomic_namespace_shard(
                from_shard.as_ref().map(|shard| shard.shard_id.as_str()),
                to_shard.as_ref().map(|shard| shard.shard_id.as_str()),
                &normalized_from,
                &normalized_to,
            )?;
            from_shard
        } else {
            None
        };
        let source =
            self.posix_lstat_distributed(&normalized_from)
                .map_err(|error| match error {
                    DfsPosixError::NotFound { .. } => DfsPosixError::NotFound {
                        path: normalized_from.clone(),
                    },
                    other => other,
                })?;
        if source.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::InvalidPath {
                path: format!(
                    "atomic single-entry rename refuses directory source: {normalized_from}"
                ),
            });
        }
        match selected_shard {
            Some(shard) if shard.primary_node_id != self.node_id => {
                self.route_namespace_operation(
                    shard.primary_node_id,
                    DfsNamespaceOperation::RenameSingleNamespaceAtomic {
                        from: normalized_from,
                        to: normalized_to.clone(),
                        expected_shard_id: shard.shard_id,
                        expected_shard_epoch: shard.epoch,
                    },
                )?;
                self.stat_routed_namespace_result(&normalized_to, true)
            }
            Some(shard) => self.posix_rename_single_namespace_atomic_local(
                &normalized_from,
                &normalized_to,
                Some((&shard.shard_id, shard.epoch)),
            ),
            None => self.posix_rename_single_namespace_atomic_local(
                &normalized_from,
                &normalized_to,
                None,
            ),
        }
    }

    pub(crate) fn posix_rename_single_namespace_atomic_local(
        &mut self,
        from: &str,
        to: &str,
        expected_shard: Option<(&str, u64)>,
    ) -> Result<DfsStat> {
        let shard_snapshot = self.refresh_sifs_metadata_shards()?;
        let actual_shard = if let Some(shards) = shard_snapshot.as_deref() {
            let from_shard =
                self.select_sifs_metadata_shard_for_path_from_snapshot(from, shards)?;
            let to_shard = self.select_sifs_metadata_shard_for_path_from_snapshot(to, shards)?;
            ensure_same_atomic_namespace_shard(
                from_shard.as_ref().map(|shard| shard.shard_id.as_str()),
                to_shard.as_ref().map(|shard| shard.shard_id.as_str()),
                from,
                to,
            )?;
            from_shard
        } else {
            None
        };
        match (expected_shard, actual_shard.as_ref()) {
            (None, None) => {}
            (Some((expected_id, expected_epoch)), Some(actual))
                if actual.shard_id == expected_id
                    && actual.epoch == expected_epoch
                    && actual.primary_node_id == self.node_id => {}
            (expected, actual) => {
                return Err(DfsPosixError::InvalidPath {
                    path: format!(
                        "atomic namespace authority changed before commit for {from} -> {to}: expected {expected:?}, current {:?}",
                        actual.map(|shard| (&shard.shard_id, shard.epoch, &shard.primary_node_id))
                    ),
                });
            }
        }
        self.posix_rename_local(from, to)
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

    pub(super) fn tombstone_path_object(&self, object: DfsObjectMetadata) -> DfsObjectMetadata {
        self.tombstone_path_object_at(object, String::new())
    }

    pub(super) fn tombstone_path_object_at(
        &self,
        mut object: DfsObjectMetadata,
        path: String,
    ) -> DfsObjectMetadata {
        if !path.is_empty() {
            object.path = path;
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.changed_at_unix_nanos =
            now_unix_nanos().max(object.changed_at_unix_nanos.saturating_add(1));
        object.content_hash = None;
        object.content_size_bytes = None;
        object.link_target_object_id = None;
        object.current_locations = vec![self.node_id.clone()];
        object
    }
}

#[cfg(test)]
mod atomic_namespace_tests {
    use super::*;
    use crate::{
        DfsControlPlaneStore, DfsFileSystemConfig, DfsMetadataMutation, DfsMetadataShardAssignment,
        DfsMetadataShardConfig, DfsMetadataShardId, DfsNamespaceCommand, DfsNamespaceResult,
        DfsRoutedCall, DfsRoutedMessage, DfsTransportIdentity, LocalContentStore,
        LocalSobjMetadataBackend, NetworkedDfsServiceEvent, SharedDfsControlPlaneKvClient,
        distributed_sifs, open_local_sharded_metadata_backend,
    };
    use sinomo_core::DfsObjectPolicy;
    use sinomo_core::kv::InProcessKvStore;
    use sinomo_core::network::{InProcessNetwork, NetworkError};
    use sinomo_core::node::NodeId;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[derive(Debug)]
    struct DeadlineTransport {
        local_node_id: NodeId,
        operation_deadline: Option<Instant>,
        observed_deadlines: Arc<Mutex<Vec<Option<Instant>>>>,
    }

    impl DfsTransportIdentity for DeadlineTransport {
        fn local_node_id(&self) -> &NodeId {
            &self.local_node_id
        }
    }

    impl DfsMessageTransport for DeadlineTransport {
        fn send_routed_call(
            &mut self,
            _target_node_id: &NodeId,
            _call: DfsRoutedCall,
        ) -> std::result::Result<(), NetworkError> {
            Ok(())
        }

        fn drain_incoming_messages(
            &mut self,
        ) -> std::result::Result<Vec<DfsRoutedMessage>, NetworkError> {
            Ok(Vec::new())
        }

        fn pending_incoming_count(&self) -> std::result::Result<Option<usize>, NetworkError> {
            Ok(Some(0))
        }

        fn set_operation_deadline(&mut self, deadline: Option<Instant>) -> Option<Instant> {
            self.observed_deadlines
                .lock()
                .expect("deadline observations")
                .push(deadline);
            std::mem::replace(&mut self.operation_deadline, deadline)
        }

        fn operation_deadline(&self) -> Option<Instant> {
            self.operation_deadline
        }
    }

    fn single_primary_shards(primary_node_id: &NodeId) -> Vec<DfsMetadataShardConfig> {
        [
            ("root", DfsMetadataShardAssignment::Root),
            (
                "users",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/users".to_string(),
                },
            ),
            (
                "services",
                DfsMetadataShardAssignment::PathPrefix {
                    prefix: "/services".to_string(),
                },
            ),
        ]
        .into_iter()
        .map(|(shard_id, assignment)| DfsMetadataShardConfig {
            file_system_id: sifs_file_system_id(),
            shard_id: shard_id.to_string(),
            primary_node_id: primary_node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 1,
            assignment,
            default_policy: DfsObjectPolicy::ReplicatedEventual,
        })
        .collect()
    }

    #[test]
    fn atomic_namespace_rename_accepts_the_same_metadata_shard() {
        ensure_same_atomic_namespace_shard(Some("shard-a"), Some("shard-a"), "/from", "/to")
            .expect("same shard is atomic");
    }

    #[test]
    fn atomic_namespace_rename_rejects_distinct_shards_even_with_one_owner() {
        let error =
            ensure_same_atomic_namespace_shard(Some("shard-a"), Some("shard-b"), "/from", "/to")
                .expect_err("different shard ids cannot share one atomic transaction");
        assert!(matches!(error, DfsPosixError::InvalidPath { .. }));
    }

    #[test]
    fn atomic_namespace_rename_rejects_configured_to_unconfigured_boundary() {
        let error = ensure_same_atomic_namespace_shard(Some("shard-a"), None, "/from", "/to")
            .expect_err("configured and fallback namespaces are distinct");
        assert!(matches!(error, DfsPosixError::InvalidPath { .. }));
    }

    #[test]
    fn rmdir_delete_recreate_delete_uses_atomic_generation_transactions() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-rmdir-reincarnation");
        let owner = NodeId::from_legacy_named("node_rmdir_reincarnation").expect("node");
        let metadata_root = root.path().join("metadata");
        let content_root = root.path().join("content");
        let network = InProcessNetwork::new();
        let backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("metadata backend");
        let mut service = NetworkedDfsService::new(
            network.add_node(owner.clone()),
            LocalContentStore::new(&content_root).expect("content store"),
        )
        .with_metadata_backend(backend)
        .expect("metadata-backed service");
        service
            .bootstrap_distributed_sifs()
            .expect("bootstrap distributed sifs");

        let parent_path = "/services/rmdir-reincarnation";
        let directory_path = "/services/rmdir-reincarnation/child";
        service.posix_mkdir_all(parent_path).expect("parent");
        let original = service.posix_mkdir(directory_path).expect("first mkdir");
        let journal_after = service
            .metadata_backend()
            .expect("metadata backend")
            .journal_since(0)
            .expect("initial journal")
            .last()
            .map(|entry| entry.sequence)
            .unwrap_or(0);

        let first_tombstone = service.posix_rmdir(directory_path).expect("first rmdir");
        let recreated = service.posix_mkdir(directory_path).expect("recreate");
        assert_eq!(recreated.object_id, original.object_id);
        let second_tombstone = service.posix_rmdir(directory_path).expect("second rmdir");
        assert_eq!(second_tombstone.object_id, original.object_id);
        assert_ne!(second_tombstone.version, first_tombstone.version);
        assert!(matches!(
            service.posix_stat(directory_path),
            Err(DfsPosixError::NotFound { .. })
        ));

        let journal = service
            .metadata_backend()
            .expect("metadata backend")
            .journal_since(journal_after)
            .expect("rmdir journal");
        let rmdir_transactions = journal
            .iter()
            .filter_map(|entry| match &entry.mutation {
                DfsMetadataMutation::ApplyNamespaceTransaction {
                    transaction: Some(transaction),
                    ..
                } if transaction.transaction_id.starts_with("namespace_rmdir_") => {
                    Some(transaction)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(rmdir_transactions.len(), 2);
        assert_ne!(
            rmdir_transactions[0].transaction_id, rmdir_transactions[1].transaction_id,
            "each pathname incarnation needs a distinct exact-retry identity"
        );
        for transaction in &rmdir_transactions {
            assert_eq!(transaction.objects.len(), 2);
            assert_eq!(transaction.versions.len(), 2);
            let removed = transaction
                .objects
                .iter()
                .find(|object| object.path == directory_path)
                .expect("directory tombstone");
            let parent = transaction
                .objects
                .iter()
                .find(|object| object.path == parent_path)
                .expect("parent touch");
            assert_eq!(removed.kind, DfsObjectKind::Tombstone);
            assert_eq!(parent.kind, DfsObjectKind::Directory);
            assert!(transaction.versions.iter().any(|version| {
                version.object_id == removed.object_id && version.version == removed.version
            }));
            assert!(transaction.versions.iter().any(|version| {
                version.object_id == parent.object_id && version.version == parent.version
            }));
        }
        let expected_parent = service.posix_stat(parent_path).expect("parent after rmdir");

        drop(service);
        let reopened_backend =
            LocalSobjMetadataBackend::new(DfsMetadataShardId::new("root"), &metadata_root)
                .expect("reopen metadata");
        let reopened_network = InProcessNetwork::new();
        let reopened = NetworkedDfsService::new(
            reopened_network.add_node(owner),
            LocalContentStore::new(content_root).expect("reopen content"),
        )
        .with_metadata_backend(reopened_backend)
        .expect("reopened service");
        assert!(matches!(
            reopened.posix_stat(directory_path),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            reopened.posix_stat(parent_path).expect("reopened parent"),
            expected_parent
        );
    }

    #[test]
    fn rmdir_installs_and_restores_the_commit_deadline() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-rmdir-deadline");
        let owner = NodeId::from_legacy_named("node_rmdir_deadline").expect("node");
        let observed_deadlines = Arc::new(Mutex::new(Vec::new()));
        let backend = LocalSobjMetadataBackend::new(
            DfsMetadataShardId::new("root"),
            root.path().join("metadata"),
        )
        .expect("metadata backend");
        let mut service = NetworkedDfsService::new(
            DeadlineTransport {
                local_node_id: owner,
                operation_deadline: None,
                observed_deadlines: Arc::clone(&observed_deadlines),
            },
            LocalContentStore::new(root.path().join("content")).expect("content store"),
        )
        .with_metadata_backend(backend)
        .expect("metadata-backed service");
        service
            .bootstrap_distributed_sifs()
            .expect("bootstrap distributed sifs");
        service
            .posix_mkdir("/services/rmdir-deadline")
            .expect("mkdir");
        observed_deadlines
            .lock()
            .expect("deadline observations")
            .clear();

        let started_at = Instant::now();
        service
            .posix_rmdir("/services/rmdir-deadline")
            .expect("rmdir");

        let observed = observed_deadlines.lock().expect("deadline observations");
        assert_eq!(observed.len(), 2, "install then restore the deadline");
        let installed = observed[0].expect("installed deadline");
        assert!(installed > started_at + Duration::from_secs(40));
        assert!(installed <= started_at + Duration::from_secs(46));
        assert_eq!(observed[1], None);
    }

    #[test]
    fn routed_rmdir_exact_retry_replays_after_restart_without_a_second_mutation() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-routed-rmdir-replay");
        let primary = NodeId::from_legacy_named("node_routed_rmdir_primary").expect("primary");
        let requester =
            NodeId::from_legacy_named("node_routed_rmdir_requester").expect("requester");
        let shards = single_primary_shards(&primary);
        let control_plane = SharedDfsControlPlaneKvClient::new(InProcessKvStore::new());
        control_plane.with_client(|kv| {
            let mut control = DfsControlPlaneStore::new(kv);
            control
                .put_file_system_config(&DfsFileSystemConfig::from(&distributed_sifs(
                    primary.clone(),
                )))
                .expect("filesystem config");
            for shard in &shards {
                control.put_metadata_shard(shard).expect("metadata shard");
            }
        });
        let network = InProcessNetwork::new();
        let _requester_transport = network.add_node(requester.clone());
        let metadata_root = root.path().join("metadata");
        let content_root = root.path().join("content");
        let mut service = NetworkedDfsService::new(
            network.add_node(primary.clone()),
            LocalContentStore::new(&content_root).expect("content store"),
        )
        .with_control_plane(control_plane.clone())
        .with_metadata_backend(
            open_local_sharded_metadata_backend(&metadata_root, shards.clone())
                .expect("sharded metadata"),
        )
        .expect("metadata-backed service");
        service
            .bootstrap_distributed_sifs()
            .expect("bootstrap distributed sifs");
        let path = "/services/routed-rmdir-replay";
        service.posix_mkdir(path).expect("seed directory");
        let command = DfsNamespaceCommand {
            request_id: "dfs_net_req_routed_rmdir_replay".to_string(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: Vec::new(),
            operation: DfsNamespaceOperation::Rmdir {
                path: path.to_string(),
            },
        };

        let first = service
            .handle_namespace_request(requester.clone(), command.clone())
            .expect("first rmdir request");
        assert!(matches!(
            first,
            NetworkedDfsServiceEvent::RemoteNamespaceServed {
                result: DfsNamespaceResult::Applied,
                mutates_metadata: true,
                ..
            }
        ));
        let journal_len = service
            .metadata_backend()
            .expect("metadata backend")
            .journal_since(0)
            .expect("journal")
            .len();
        let primary_transport = service.transport.clone();
        drop(service);

        let mut restarted = NetworkedDfsService::new(
            primary_transport,
            LocalContentStore::new(content_root).expect("reopen content"),
        )
        .with_control_plane(control_plane)
        .with_metadata_backend(
            open_local_sharded_metadata_backend(metadata_root, shards)
                .expect("reopen sharded metadata"),
        )
        .expect("restarted service");
        let replay = restarted
            .handle_namespace_request(requester.clone(), command.clone())
            .expect("exact retry");
        assert!(matches!(
            replay,
            NetworkedDfsServiceEvent::RemoteNamespaceServed {
                result: DfsNamespaceResult::Applied,
                mutates_metadata: false,
                ..
            }
        ));
        assert_eq!(
            restarted
                .metadata_backend()
                .expect("metadata backend")
                .journal_since(0)
                .expect("replayed journal")
                .len(),
            journal_len,
            "a lost-response retry must not journal a second rmdir"
        );
        assert!(matches!(
            restarted.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ));

        let reused = restarted
            .handle_namespace_request(
                requester,
                DfsNamespaceCommand {
                    operation: DfsNamespaceOperation::Rmdir {
                        path: "/services/different-rmdir".to_string(),
                    },
                    ..command
                },
            )
            .expect("request-id reuse response");
        assert!(matches!(
            reused,
            NetworkedDfsServiceEvent::RemoteNamespaceServed {
                result: DfsNamespaceResult::Rejected { ref reason, .. },
                mutates_metadata: false,
                ..
            } if reason.contains("already used for a different command")
        ));
    }
}
