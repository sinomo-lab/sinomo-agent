//! POSIX namespace mutation helpers.
//!
//! This module owns unlink, rmdir, and same-shard rename. These operations
//! share tombstone transaction handling, hardlink promotion, parent directory
//! timestamp updates, and routed namespace wrappers.

use super::{POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS, now_unix_nanos};
use crate::namespace::{namespace_transaction_id_for_versioned_path, next_version};
use crate::posix_model::{DfsPosixError, DfsStat, Result};
use crate::posix_path::{
    normalize_posix_path, normalize_posix_path_with_trailing_directory, parent_posix_path,
};
use crate::protocol::{DfsNamespaceDestinationPrecondition, DfsNamespaceObjectPrecondition};
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

fn namespace_generation_conflict(
    path: &str,
    expected: &DfsNamespaceObjectPrecondition,
    actual: Option<&DfsObjectMetadata>,
) -> DfsPosixError {
    let actual = actual
        .map(|object| {
            format!(
                "{}@{} ({:?})",
                object.object_id, object.version, object.kind
            )
        })
        .unwrap_or_else(|| "absent".to_string());
    DfsPosixError::Conflict {
        path: path.to_string(),
        reason: format!(
            "namespace generation changed: expected {}@{}, current {actual}; reconcile operation outcome before retrying",
            expected.object_id, expected.version
        ),
    }
}

fn conditional_delete_result(path: &str, stat: DfsStat) -> Result<DfsStat> {
    if stat.kind == DfsObjectKind::Tombstone {
        return Ok(stat);
    }
    Err(DfsPosixError::Conflict {
        path: path.to_string(),
        reason: "durable delete was applied previously, but the path now names a live generation; reconcile the stored outcome"
            .to_string(),
    })
}

fn conditional_rename_result(
    path: &str,
    expected_source: &DfsNamespaceObjectPrecondition,
    stat: DfsStat,
) -> Result<DfsStat> {
    if stat.object_id == expected_source.object_id && stat.kind != DfsObjectKind::Tombstone {
        return Ok(stat);
    }
    Err(DfsPosixError::Conflict {
        path: path.to_string(),
        reason: "durable rename was applied previously, but the destination now names a different generation; reconcile the stored outcome"
            .to_string(),
    })
}

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    fn hardlink_alias_target_requires_cross_shard_coordination(
        &mut self,
        alias_path: &str,
        target_path: &str,
    ) -> Result<bool> {
        let Some(shards) = self.refresh_sifs_metadata_shards()? else {
            return Ok(false);
        };
        let alias_shard =
            self.select_sifs_metadata_shard_for_path_from_snapshot(alias_path, &shards)?;
        let target_shard =
            self.select_sifs_metadata_shard_for_path_from_snapshot(target_path, &shards)?;
        Ok(match (alias_shard, target_shard) {
            (Some(alias), Some(target)) => alias.shard_id != target.shard_id,
            _ => false,
        })
    }

    pub fn posix_unlink(&mut self, path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            service.with_metadata_lookup_cache_mut(|service| {
                // The CAS protects the namespace entry being removed. Ordinary POSIX lstat
                // intentionally projects a hardlink alias as its shared target inode, whose
                // object/version cannot match the alias record checked by the authority.
                // Reuse the exact path observations during this one serialized operation;
                // namespace publication invalidates the cache before any later lookup.
                let observed = service.posix_namespace_lstat_distributed(path)?;
                let operation_id = service.next_request_id();
                service.posix_unlink_if_generation_with_current_deadline(
                    path,
                    operation_id,
                    DfsNamespaceObjectPrecondition {
                        object_id: observed.object_id,
                        version: observed.version,
                    },
                )
            })
        })
    }

    /// Unlink an exact namespace generation under a caller-owned durable
    /// operation id. A caller retrying after an ambiguous response must reuse
    /// both the id and the precondition unchanged.
    pub fn posix_unlink_if_generation(
        &mut self,
        path: &str,
        operation_id: String,
        expected: DfsNamespaceObjectPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_unlink_if_generation_with_current_deadline(path, operation_id, expected)
        })
    }

    fn posix_unlink_if_generation_with_current_deadline(
        &mut self,
        path: &str,
        operation_id: String,
        expected: DfsNamespaceObjectPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation_with_request_id(
                operation_id,
                primary,
                DfsNamespaceOperation::ConditionalUnlink {
                    path: path.to_string(),
                    expected,
                },
            )?;
            // The mutation ran on another request shell, so it cannot clear
            // this caller's operation-scoped lookup cache. Observe the
            // durable outcome from fresh metadata instead of replaying the
            // live generation captured before the unlink.
            self.clear_metadata_lookup_cache();
            return conditional_delete_result(
                &normalized,
                self.stat_replayed_tombstone_synced(&normalized)?,
            );
        }
        if self.control_plane.is_some() {
            self.execute_local_primary_namespace_operation_with_request_id(
                operation_id,
                DfsNamespaceOperation::ConditionalUnlink {
                    path: path.to_string(),
                    expected,
                },
            )?;
            self.clear_metadata_lookup_cache();
            return conditional_delete_result(
                &normalized,
                self.stat_replayed_tombstone(&normalized)?,
            );
        }
        self.posix_unlink_local_if_generation(path, &expected)
    }

    pub(crate) fn posix_unlink_local(&mut self, path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        self.posix_unlink_resolved_object(path, object)
    }

    fn posix_unlink_resolved_object(
        &mut self,
        path: String,
        object: DfsObjectMetadata,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if object.kind == DfsObjectKind::Hardlink {
            let target_object_id = object
                .link_target_object_id
                .clone()
                .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
            let mut target = self
                .object_by_id(&target_object_id)?
                .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
            if target.kind == DfsObjectKind::Tombstone || target.kind == DfsObjectKind::Hardlink {
                return Err(DfsPosixError::NotFound { path });
            }
            let tombstone = self.tombstone_path_object(object.clone());
            let mut alias_objects = vec![tombstone.clone()];
            if let Some(parent_path) = parent_posix_path(&path) {
                alias_objects.push(self.directory_modified_metadata(&parent_path)?);
            }
            let alias_transaction = DfsNamespaceTransaction {
                content_reference_identity: None,
                extension_expectations: Vec::new(),
                contribution_digest: None,
                file_version_pins: None,
                transaction_id: namespace_transaction_id_for_batch(
                    "unlink-hardlink-alias",
                    &object.file_system_id,
                    &alias_objects,
                    &[],
                ),
                file_system_id: object.file_system_id.clone(),
                objects: alias_objects,
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: OffsetDateTime::now_utc(),
            };
            if self.hardlink_alias_target_requires_cross_shard_coordination(&path, &target.path)? {
                self.coordinate_cross_primary_hardlink_alias_mutation(
                    &object,
                    &path,
                    alias_transaction,
                    "unlink",
                )?;
                return self.stat_replayed_tombstone(&path);
            }
            target.changed_at_unix_nanos =
                now_unix_nanos().max(target.changed_at_unix_nanos.saturating_add(1));
            target.version = next_version(&target.version);
            let mut objects = alias_transaction.objects;
            objects.push(target);
            self.apply_namespace_transaction(DfsNamespaceTransaction {
                content_reference_identity: None,
                extension_expectations: Vec::new(),
                contribution_digest: None,
                file_version_pins: None,
                transaction_id: namespace_transaction_id_for_batch(
                    "unlink-hardlink-alias-local",
                    &object.file_system_id,
                    &objects,
                    &[],
                ),
                file_system_id: object.file_system_id.clone(),
                objects,
                versions: Vec::new(),
                advisory_lock_states: Vec::new(),
                created_at: alias_transaction.created_at,
            })?;
            return self.stat_object(&tombstone);
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
        self.commit_namespace_tombstone("unlink", &path, object)
    }

    pub(crate) fn posix_unlink_local_if_generation(
        &mut self,
        path: &str,
        expected: &DfsNamespaceObjectPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let resolved = self.resolve_existing_posix_path_distributed(path, false)?;
        let current = self.lookup_sifs_path(&resolved)?;
        if current.as_ref().is_none_or(|object| {
            object.kind == DfsObjectKind::Tombstone
                || object.object_id != expected.object_id
                || object.version != expected.version
        }) {
            return Err(namespace_generation_conflict(
                &resolved,
                expected,
                current.as_ref(),
            ));
        }
        self.ensure_resolved_path_not_in_cross_shard_flight(&resolved)?;
        self.posix_unlink_resolved_object(
            resolved,
            current.expect("validated unlink generation is present"),
        )
    }

    pub fn posix_rmdir(&mut self, path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            let observed = service.posix_namespace_lstat_distributed(path)?;
            let operation_id = service.next_request_id();
            service.posix_rmdir_if_generation_with_current_deadline(
                path,
                operation_id,
                DfsNamespaceObjectPrecondition {
                    object_id: observed.object_id,
                    version: observed.version,
                },
            )
        })
    }

    /// Remove an exact directory generation under a caller-owned durable
    /// operation id.
    pub fn posix_rmdir_if_generation(
        &mut self,
        path: &str,
        operation_id: String,
        expected: DfsNamespaceObjectPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_rmdir_if_generation_with_current_deadline(path, operation_id, expected)
        })
    }

    fn posix_rmdir_if_generation_with_current_deadline(
        &mut self,
        path: &str,
        operation_id: String,
        expected: DfsNamespaceObjectPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation_with_request_id(
                operation_id,
                primary,
                DfsNamespaceOperation::ConditionalRmdir {
                    path: path.to_string(),
                    expected,
                },
            )?;
            return conditional_delete_result(
                &normalized,
                self.stat_replayed_tombstone_synced(&normalized)?,
            );
        }
        if self.control_plane.is_some() {
            self.execute_local_primary_namespace_operation_with_request_id(
                operation_id,
                DfsNamespaceOperation::ConditionalRmdir {
                    path: path.to_string(),
                    expected,
                },
            )?;
            return conditional_delete_result(
                &normalized,
                self.stat_replayed_tombstone(&normalized)?,
            );
        }
        self.posix_rmdir_local_if_generation(path, &expected)
    }

    pub(crate) fn posix_rmdir_local(&mut self, path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_posix_dot_components_distributed(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        if path == "/" {
            return Err(DfsPosixError::InvalidPath { path });
        }
        let object = self
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
        self.commit_namespace_tombstone("rmdir", &path, object)
    }

    /// Publish the removed generation and parent timestamps in one transaction.
    /// Each caller validates its unlink or directory-removal semantics first;
    /// hardlink promotion retains its separate multi-object transaction.
    fn commit_namespace_tombstone(
        &mut self,
        kind: &str,
        path: &str,
        object: DfsObjectMetadata,
    ) -> Result<DfsStat> {
        let object = self.tombstone_path_object(object);
        let mut objects = vec![object.clone()];
        if let Some(parent_path) = parent_posix_path(path) {
            objects.push(self.directory_modified_metadata(&parent_path)?);
        }
        let versions = objects
            .iter()
            .map(|object| object_version_record(object, object.content_size_bytes))
            .collect::<Vec<_>>();
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_batch(
                kind,
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

    pub(crate) fn posix_rmdir_local_if_generation(
        &mut self,
        path: &str,
        expected: &DfsNamespaceObjectPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let resolved = self.resolve_posix_path_distributed(&normalized, false)?;
        let current = self.lookup_sifs_path(&resolved)?;
        if current.as_ref().is_none_or(|object| {
            object.kind == DfsObjectKind::Tombstone
                || object.object_id != expected.object_id
                || object.version != expected.version
        }) {
            return Err(namespace_generation_conflict(
                &resolved,
                expected,
                current.as_ref(),
            ));
        }
        self.posix_rmdir_local(&resolved)
    }

    pub fn posix_rename(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized_from = self.resolve_posix_dot_components_distributed(from)?;
        let dispatch_from = crate::posix_path::path_after_dot_resolution(from, &normalized_from);
        let from = dispatch_from.as_ref();
        let normalized_to = self.resolve_posix_dot_components_distributed(to)?;
        let dispatch_to = crate::posix_path::path_after_dot_resolution(to, &normalized_to);
        let to = dispatch_to.as_ref();
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
            (Some(source_primary), _) => {
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
                self.stat_routed_namespace_result(&normalized_to, true)
            }
        }
    }

    /// Rename under a caller-owned durable operation identity.
    ///
    /// A caller retrying after an ambiguous transport result must reuse this
    /// exact id. The source metadata authority binds it to the rename command
    /// and replays the canonical terminal result instead of attempting a
    /// second rename after the source has already become a tombstone.
    pub fn posix_rename_with_operation_id(
        &mut self,
        from: &str,
        to: &str,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        crate::service_routed_namespace::validate_namespace_operation_id(operation_id)?;
        let normalized_from = self.resolve_posix_dot_components_distributed(from)?;
        let dispatch_from = crate::posix_path::path_after_dot_resolution(from, &normalized_from);
        let from = dispatch_from.as_ref();
        let normalized_to = self.resolve_posix_dot_components_distributed(to)?;
        let dispatch_to = crate::posix_path::path_after_dot_resolution(to, &normalized_to);
        let to = dispatch_to.as_ref();
        let operation = DfsNamespaceOperation::Rename {
            from: from.to_string(),
            to: to.to_string(),
        };
        let mut primaries = self
            .routed_namespace_primaries_for_paths_fresh([
                normalized_from.as_str(),
                normalized_to.as_str(),
            ])?
            .into_iter();
        let from_primary = primaries.next().unwrap_or(None);
        let to_primary = primaries.next().unwrap_or(None);
        match (from_primary, to_primary) {
            (None, None) if self.control_plane.is_some() => {
                self.execute_local_primary_namespace_operation_with_request_id(
                    operation_id.to_string(),
                    operation,
                )?;
                self.stat_routed_namespace_result(&normalized_to, true)
            }
            (None, None) => self.posix_rename_local(from, to),
            (None, Some(_)) => {
                // This node owns the source side and coordinates the
                // cross-shard move itself through durable admission.
                self.execute_local_primary_namespace_operation_with_request_id(
                    operation_id.to_string(),
                    operation,
                )?;
                self.stat_routed_namespace_result(&normalized_to, true)
            }
            (Some(source_primary), _) => {
                // The source-side shard primary coordinates; it adopts the
                // record into the destination shard when that lives
                // elsewhere.
                self.route_namespace_operation_with_request_id(
                    operation_id.to_string(),
                    source_primary.clone(),
                    operation,
                )?;
                self.stat_routed_namespace_result(&normalized_to, true)
            }
        }
    }

    /// Rename under exact source and destination generation fences.
    ///
    /// The current cross-shard adoption protocol cannot atomically carry a
    /// destination compare-and-swap, so this API rejects cross-shard moves
    /// instead of weakening the contract. Same-shard local and routed moves
    /// are one metadata transaction.
    pub fn posix_rename_if_generation(
        &mut self,
        from: &str,
        to: &str,
        operation_id: String,
        expected_source: DfsNamespaceObjectPrecondition,
        expected_destination: DfsNamespaceDestinationPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized_from = self.resolve_posix_dot_components_distributed(from)?;
        let dispatch_from = crate::posix_path::path_after_dot_resolution(from, &normalized_from);
        let from = dispatch_from.as_ref();
        let normalized_to = self.resolve_posix_dot_components_distributed(to)?;
        let dispatch_to = crate::posix_path::path_after_dot_resolution(to, &normalized_to);
        let to = dispatch_to.as_ref();
        let mut primaries = self
            .routed_namespace_primaries_for_paths_fresh([
                normalized_from.as_str(),
                normalized_to.as_str(),
            ])?
            .into_iter();
        let from_primary = primaries.next().unwrap_or(None);
        let to_primary = primaries.next().unwrap_or(None);
        match (from_primary, to_primary) {
            (None, None) if self.control_plane.is_some() => {
                self.execute_local_primary_namespace_operation_with_request_id(
                    operation_id,
                    DfsNamespaceOperation::ConditionalRename {
                        from: from.to_string(),
                        to: to.to_string(),
                        expected_source: expected_source.clone(),
                        expected_destination,
                    },
                )?;
                conditional_rename_result(
                    &normalized_to,
                    &expected_source,
                    self.posix_stat(&normalized_to)?,
                )
            }
            (None, None) => self.posix_rename_local_if_generation(
                from,
                to,
                &expected_source,
                &expected_destination,
            ),
            (Some(source_primary), Some(destination_primary))
                if source_primary == destination_primary =>
            {
                self.route_namespace_operation_with_request_id(
                    operation_id,
                    source_primary,
                    DfsNamespaceOperation::ConditionalRename {
                        from: from.to_string(),
                        to: to.to_string(),
                        expected_source: expected_source.clone(),
                        expected_destination,
                    },
                )?;
                conditional_rename_result(
                    &normalized_to,
                    &expected_source,
                    self.stat_routed_namespace_result(&normalized_to, true)?,
                )
            }
            _ => Err(DfsPosixError::Conflict {
                path: format!("{normalized_from} -> {normalized_to}"),
                reason: "conditional rename crosses metadata shard primaries; the destination CAS cannot be weakened"
                    .to_string(),
            }),
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
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            service.posix_rename_single_namespace_atomic_with_current_deadline(from, to)
        })
    }

    fn posix_rename_single_namespace_atomic_with_current_deadline(
        &mut self,
        from: &str,
        to: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized_from = self.resolve_posix_dot_components_distributed(from)?;
        let normalized_to = self.resolve_posix_dot_components_distributed(to)?;
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let _namespace_plan = self.namespace_execution.begin_planning();
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
        let source_generation = source.clone();
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
        let mut local_hardlink_target_updates = Vec::new();

        if let Some(mut destination) = self.lookup_sifs_path(&to)?
            && destination.kind != DfsObjectKind::Tombstone
        {
            let source_target = self.resolve_hardlink_target(&source)?;
            let destination_target = self.resolve_hardlink_target(&destination)?;
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
            if destination.kind == DfsObjectKind::Hardlink {
                if self.hardlink_alias_target_requires_cross_shard_coordination(
                    &destination.path,
                    &destination_target.path,
                )? {
                    return Err(DfsPosixError::InvalidPath {
                        path: format!(
                            "replacing hardlink destination {to} whose canonical target {} is on another metadata shard requires a multi-target alias job",
                            destination_target.path
                        ),
                    });
                }
                let mut updated_target = destination_target.clone();
                updated_target.changed_at_unix_nanos = destination_target_changed_at
                    .max(updated_target.changed_at_unix_nanos.saturating_add(1));
                updated_target.version = next_version(&updated_target.version);
                local_hardlink_target_updates.push(updated_target);
            }
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
                let hidden_path = self.hidden_alias_tombstone_path_on_destination_shard(
                    &alias_path,
                    &alias.object_id,
                )?;
                let alias_tombstone = self.tombstone_path_object_at(alias.clone(), hidden_path);

                destination.path = alias.path.clone();
                destination.changed_at_unix_nanos = destination_target_changed_at;
                destination.version = next_version(&destination.version);
                moved_children.push(alias_tombstone);
                moved_children.push(destination);
            } else {
                moved_children.push(self.tombstone_path_object(destination));
            }
        }

        let source_hardlink_target = if source.kind == DfsObjectKind::Hardlink {
            Some(self.resolve_hardlink_target(&source)?)
        } else {
            None
        };
        let source_hardlink_requires_coordination = match source_hardlink_target.as_ref() {
            Some(target) => self.hardlink_alias_target_requires_cross_shard_coordination(
                &source.path,
                &target.path,
            )?,
            None => false,
        };
        if let Some(target) = source_hardlink_target.as_ref()
            && !source_hardlink_requires_coordination
        {
            let mut updated_target = target.clone();
            updated_target.changed_at_unix_nanos =
                now_unix_nanos().max(updated_target.changed_at_unix_nanos.saturating_add(1));
            updated_target.version = next_version(&updated_target.version);
            local_hardlink_target_updates.push(updated_target);
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
        transaction_objects.extend(local_hardlink_target_updates);
        if source_hardlink_requires_coordination {
            if let Some(parent_path) = parent_posix_path(&from) {
                transaction_objects.push(self.directory_modified_metadata(&parent_path)?);
            }
            if parent_posix_path(&from) != parent_posix_path(&source.path)
                && let Some(parent_path) = parent_posix_path(&source.path)
            {
                transaction_objects.push(self.directory_modified_metadata(&parent_path)?);
            }
        }
        let transaction = DfsNamespaceTransaction {
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_versioned_path(
                "rename",
                &source.object_id,
                &source.version,
                &source.path,
            ),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            versions: Vec::new(),
            advisory_lock_states: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        };
        if source_hardlink_requires_coordination {
            self.coordinate_cross_primary_hardlink_alias_mutation(
                &source_generation,
                &source.path,
                transaction,
                "rename",
            )?;
            return self.posix_stat_distributed(&source.path);
        }
        self.apply_namespace_transaction(transaction)?;
        self.touch_parent_directory_modified(&from)?;
        if parent_posix_path(&from) != parent_posix_path(&source.path) {
            self.touch_parent_directory_modified(&source.path)?;
        }
        self.stat_object(&source)
    }

    pub(crate) fn posix_rename_local_if_generation(
        &mut self,
        from: &str,
        to: &str,
        expected_source: &DfsNamespaceObjectPrecondition,
        expected_destination: &DfsNamespaceDestinationPrecondition,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (from, _) = normalize_posix_path_with_trailing_directory(from)?;
        let (to, _) = normalize_posix_path_with_trailing_directory(to)?;
        let from = self.resolve_posix_path_distributed(&from, false)?;
        let to = self.resolve_posix_path_distributed(&to, false)?;
        let source = self.lookup_sifs_path(&from)?;
        if source.as_ref().is_none_or(|object| {
            object.kind == DfsObjectKind::Tombstone
                || object.object_id != expected_source.object_id
                || object.version != expected_source.version
        }) {
            return Err(namespace_generation_conflict(
                &from,
                expected_source,
                source.as_ref(),
            ));
        }
        let destination = self.lookup_sifs_path(&to)?;
        match expected_destination {
            DfsNamespaceDestinationPrecondition::Absent
                if destination
                    .as_ref()
                    .is_some_and(|object| object.kind != DfsObjectKind::Tombstone) =>
            {
                return Err(DfsPosixError::Conflict {
                    path: to,
                    reason: "rename destination was created after it was observed absent; reconcile operation outcome before retrying"
                        .to_string(),
                });
            }
            DfsNamespaceDestinationPrecondition::Exact(expected)
                if destination.as_ref().is_none_or(|object| {
                    object.kind == DfsObjectKind::Tombstone
                        || object.object_id != expected.object_id
                        || object.version != expected.version
                }) =>
            {
                return Err(namespace_generation_conflict(
                    &to,
                    expected,
                    destination.as_ref(),
                ));
            }
            _ => {}
        }
        self.posix_rename_local(&from, &to)
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
        let alias_path = normalize_posix_path(&alias.path)?;
        let hidden_path =
            self.hidden_alias_tombstone_path_on_destination_shard(&alias_path, &alias.object_id)?;
        let alias_tombstone = self.tombstone_path_object_at(alias.clone(), hidden_path);

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
            content_reference_identity: None,
            extension_expectations: Vec::new(),
            contribution_digest: None,
            file_version_pins: None,
            transaction_id: namespace_transaction_id_for_versioned_path(
                "unlink-hardlink",
                &object.object_id,
                &object.version,
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
    use crate::{DfsMessageCommand, DfsMessageResponse, DfsNetworkMessage};
    use crate::{
        DfsMetadataMutation, DfsNamespaceCommand, DfsNamespacePosixError, DfsNamespaceResult,
        DfsRoutedMessage,
    };

    use std::time::{Duration, Instant};

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
        let owner =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-rmdir-reincarnation", 1)
                .remove(0);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-rmdir-reincarnation", owner.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let mut service = runtime.client(&owner).unwrap();

        let parent_path = "/services/rmdir-reincarnation";
        let directory_path = "/services/rmdir-reincarnation/child";
        service.posix_mkdir_all(parent_path).expect("parent");
        let original = service.posix_mkdir(directory_path).expect("first mkdir");

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
            .journal_since(0)
            .expect("rmdir journal");
        let rmdir_transactions = journal
            .iter()
            .filter_map(|entry| match &entry.mutation {
                DfsMetadataMutation::PrepareOperationV7 { record, .. }
                    if record
                        .transaction
                        .transaction_id
                        .starts_with("namespace_rmdir_")
                        && record.transaction.objects.iter().any(|object| {
                            object.object_id == original.object_id && object.path == directory_path
                        }) =>
                {
                    Some(&record.transaction)
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
        runtime.stop_node(&owner).unwrap();
        runtime.reopen_node(owner.clone()).unwrap();
        let reopened = runtime.client(&owner).unwrap();
        assert!(matches!(
            reopened.posix_stat(directory_path),
            Err(DfsPosixError::NotFound { .. })
        ));
        assert_eq!(
            reopened.posix_stat(parent_path).expect("reopened parent"),
            expected_parent
        );
        drop(reopened);
        runtime.close().unwrap();
    }

    #[test]
    fn conditional_rmdir_never_deletes_a_recreated_generation() {
        let owner =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-rmdir-cas-recreate", 1)
                .remove(0);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-rmdir-cas-recreate", owner.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let mut service = runtime.client(&owner).unwrap();

        let path = "/services/rmdir-cas-recreate";
        let original = service.posix_mkdir(path).expect("original directory");
        let expected = DfsNamespaceObjectPrecondition {
            object_id: original.object_id,
            version: original.version,
        };
        service
            .posix_rmdir_if_generation(path, "rmdir-cas-operation".to_string(), expected.clone())
            .expect("conditional rmdir");
        let recreated = service.posix_mkdir(path).expect("recreated directory");

        let stale = service
            .posix_rmdir_if_generation(path, "rmdir-cas-operation".to_string(), expected)
            .expect_err("stale generation must be fenced");
        assert!(matches!(stale, DfsPosixError::Conflict { .. }));
        assert_eq!(
            service
                .posix_stat(path)
                .expect("recreated directory remains"),
            recreated
        );
        drop(service);
        runtime.close().unwrap();
    }

    #[test]
    fn conditional_rename_never_replaces_a_destination_created_after_observation() {
        let owner = crate::test_runtime::DfsTestRuntime::ordered_node_ids(
            "native-rename-cas-destination",
            1,
        )
        .remove(0);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(
            "native-rename-cas-destination",
            owner.clone(),
        )
        .unwrap();
        runtime.start_nodes().unwrap();
        let mut service = runtime.client(&owner).unwrap();

        let source_path = "/services/rename-cas-source";
        let destination_path = "/services/rename-cas-destination";
        let source = service.posix_mkdir(source_path).expect("source");
        let expected_source = DfsNamespaceObjectPrecondition {
            object_id: source.object_id.clone(),
            version: source.version.clone(),
        };
        let destination = service
            .posix_mkdir(destination_path)
            .expect("concurrent destination");

        let stale = service
            .posix_rename_if_generation(
                source_path,
                destination_path,
                "rename-cas-operation".to_string(),
                expected_source,
                DfsNamespaceDestinationPrecondition::Absent,
            )
            .expect_err("new destination must be fenced");
        assert!(matches!(stale, DfsPosixError::Conflict { .. }));
        assert_eq!(
            service.posix_stat(source_path).expect("source remains"),
            source
        );
        assert_eq!(
            service
                .posix_stat(destination_path)
                .expect("destination remains"),
            destination
        );
        drop(service);
        runtime.close().unwrap();
    }

    #[test]
    fn rmdir_installs_and_restores_the_commit_deadline() {
        let node =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-rmdir-deadline", 1)
                .remove(0);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-rmdir-deadline", node.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let mut service = runtime.client(&node).unwrap();
        service.posix_mkdir("/services/rmdir-deadline").unwrap();
        assert_eq!(service.transport.operation_deadline(), None);
        let started = Instant::now();
        let mut installed = None;
        service
            .with_posix_commit_operation_deadline(|service| {
                installed = service.transport.operation_deadline();
                // Invoke the actual ordinary namespace mutation. Its nested scope
                // must preserve the current caller deadline and restore that scope.
                service.posix_rmdir("/services/rmdir-deadline")?;
                assert_eq!(service.transport.operation_deadline(), installed);
                Ok(())
            })
            .unwrap();
        let installed = installed.expect("normal caller deadline was installed");
        assert!(installed > started + Duration::from_secs(40));
        assert!(installed <= started + Duration::from_secs(46));
        assert_eq!(
            service.transport.operation_deadline(),
            None,
            "normal caller scope restored"
        );
        assert!(matches!(
            service.posix_lstat("/services/rmdir-deadline"),
            Err(DfsPosixError::NotFound { .. })
        ));
        drop(service);
        runtime.close().unwrap();
    }

    #[test]
    fn routed_rmdir_exact_retry_replays_after_restart_without_a_second_mutation() {
        let name = "native-routed-rmdir-replay";
        let (mut runtime, mut service, config) =
            crate::test_support::current_native_metadata_service(name, 2, "/services").unwrap();
        let primary = config.primary_node_id.clone();
        let requester = crate::test_runtime::DfsTestRuntime::ordered_node_ids(name, 2)
            .into_iter()
            .find(|node| *node != primary)
            .unwrap();
        let path = "/services/routed-rmdir-replay";
        let original = service.posix_mkdir(path).unwrap();
        let command = DfsNamespaceCommand {
            request_id: "dfs_net_req_routed_rmdir_replay".into(),
            file_system_id: sifs_file_system_id(),
            requesting_node_id: requester.clone(),
            include_metadata_journal_batches: false,
            metadata_journal_after_sequences: Vec::new(),
            authority_shards: vec![config],
            operation: DfsNamespaceOperation::ConditionalRmdir {
                path: path.into(),
                expected: DfsNamespaceObjectPrecondition {
                    object_id: original.object_id,
                    version: original.version,
                },
            },
        };
        let call = |runtime: &crate::test_runtime::DfsTestRuntime, command: DfsNamespaceCommand| {
            let request = crate::dfs_message_rpc_request(&DfsMessageCommand {
                request_id: command.request_id.clone(),
                routed_message: DfsRoutedMessage {
                    source_node_id: requester.clone(),
                    target_node_id: primary.clone(),
                    frame_id: 1,
                    message_id: 1,
                    message: DfsNetworkMessage::NamespaceRequest(command),
                },
            })
            .unwrap();
            let response = runtime
                .rpc_request(&requester, &primary, request, Duration::from_secs(5))
                .unwrap();
            let decoded: DfsMessageResponse =
                sinomo_core::network::SinomoObjectCodec::network_frame()
                    .decode(&response.body)
                    .unwrap();
            decoded
                .outbound_messages
                .into_iter()
                .find_map(|message| match message.message {
                    DfsNetworkMessage::NamespaceResponse(response) => Some(response),
                    _ => None,
                })
                .expect("actual native namespace response")
        };
        let before = service
            .metadata_backend()
            .unwrap()
            .journal_since(0)
            .unwrap()
            .len();
        let first = call(&runtime, command.clone());
        assert!(matches!(first.result, DfsNamespaceResult::Applied));
        assert!(
            service
                .metadata_backend()
                .unwrap()
                .journal_since(0)
                .unwrap()
                .len()
                > before
        );
        let local_path = "/services/local-primary-rmdir-replay";
        let local_original = service.posix_mkdir(local_path).unwrap();
        let local_expected = DfsNamespaceObjectPrecondition {
            object_id: local_original.object_id,
            version: local_original.version,
        };
        service
            .posix_rmdir_if_generation(
                local_path,
                "local-primary-rmdir-operation".into(),
                local_expected.clone(),
            )
            .unwrap();
        let journal_len = service
            .metadata_backend()
            .unwrap()
            .journal_since(0)
            .unwrap()
            .len();
        drop(service);
        runtime.stop_node(&primary).unwrap();
        runtime.reopen_node(primary.clone()).unwrap();
        let mut restarted = runtime.client(&primary).unwrap();
        let replay = call(&runtime, command.clone());
        assert!(matches!(replay.result, DfsNamespaceResult::Applied));
        assert_eq!(
            restarted
                .metadata_backend()
                .unwrap()
                .journal_since(0)
                .unwrap()
                .len(),
            journal_len,
            "an exact lost-response retry adds no second namespace mutation"
        );
        assert!(matches!(
            restarted.posix_stat(path),
            Err(DfsPosixError::NotFound { .. })
        ));
        let local_recreated = restarted.posix_mkdir(local_path).unwrap();
        let local_replay = restarted
            .posix_rmdir_if_generation(
                local_path,
                "local-primary-rmdir-operation".into(),
                local_expected,
            )
            .unwrap_err();
        assert!(matches!(local_replay, DfsPosixError::Conflict { .. }));
        assert_eq!(restarted.posix_stat(local_path).unwrap(), local_recreated);
        let recreated = restarted.posix_mkdir(path).unwrap();
        let before_replay = restarted
            .metadata_backend()
            .unwrap()
            .journal_since(0)
            .unwrap()
            .len();
        let replay_after_recreate = call(&runtime, command.clone());
        assert!(matches!(
            replay_after_recreate.result,
            DfsNamespaceResult::Applied
        ));
        assert_eq!(
            restarted
                .metadata_backend()
                .unwrap()
                .journal_since(0)
                .unwrap()
                .len(),
            before_replay
        );
        assert_eq!(
            restarted.posix_stat(path).unwrap(),
            recreated,
            "durable replay cannot consume a newer pathname generation"
        );
        let reused = call(
            &runtime,
            DfsNamespaceCommand {
                operation: DfsNamespaceOperation::Rmdir {
                    path: "/services/different-rmdir".into(),
                },
                ..command
            },
        );
        assert!(matches!(reused.result, DfsNamespaceResult::Rejected {
            reason, posix_error: Some(DfsNamespacePosixError::Conflict { .. }), ..
        } if reason.contains("already used for a different command")));
        drop(restarted);
        runtime.close().unwrap();
    }
}
