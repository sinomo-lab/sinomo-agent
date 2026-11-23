//! POSIX symlink and hardlink creation helpers.
//!
//! This module owns creating link records. Readlink/stat behavior lives in the
//! query module, and unlink/rename remain with namespace mutation code because
//! they share hardlink promotion and tombstone transaction handling.

use super::now_unix_nanos;
use crate::namespace::{initial_version, next_version};
use crate::DfsNamespaceTransaction;
use crate::intent::{stable_create_object_id, stable_write_target_version};
use crate::posix_model::{DfsPosixCreateAttrs, DfsPosixError, DfsStat, DfsWriteOptions, Result};
use crate::posix_object::{
    apply_posix_create_attrs, new_posix_metadata_object, recreated_posix_metadata_object,
};
use crate::posix_path::{
    normalize_posix_path_with_trailing_directory, validate_posix_symlink_target,
};
use crate::{
    DfsMessageTransport, DfsNamespaceOperation, DfsObjectId, DfsObjectKind, DfsObjectMetadata,
    NetworkedDfsService, NetworkedDfsWriteOutcome, sifs_file_system_id,
};
use sinomo_core::DfsWriteRequest;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn posix_symlink(
        &mut self,
        target: &str,
        link_path: &str,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_symlink_with_create_attrs(
            target,
            link_path,
            options,
            DfsPosixCreateAttrs::symlink_default(),
        )
    }

    pub fn posix_symlink_with_create_attrs(
        &mut self,
        target: &str,
        link_path: &str,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.with_posix_commit_operation_deadline(|service| {
            service.with_metadata_lookup_cache_mut(|service| {
                service.posix_symlink_with_create_attrs_with_current_deadline(
                    target, link_path, options, attrs, None,
                )
            })
        })
    }

    /// [`Self::posix_symlink_with_create_attrs`] as the caller's operation
    /// `operation_id`. The link's object and version are the ones the id
    /// selects, so every attempt makes the same link, the primary answers a
    /// repeat with what the first attempt did, and an attempt made after the
    /// link was created finds it created. A mount mints one id per syscall
    /// and keeps it across the syscall's runs.
    pub fn posix_symlink_with_create_attrs_and_operation_id(
        &mut self,
        target: &str,
        link_path: &str,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
        operation_id: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        crate::service_routed_write::validate_write_operation_id(operation_id)?;
        // Every change goes under the caller's id: a mounted call's next run
        // answers it again, and its record waits are handed back.
        self.with_replayable_mounted_effects(|service| {
            service.with_posix_commit_operation_deadline(|service| {
                service.with_metadata_lookup_cache_mut(|service| {
                    service.posix_symlink_with_create_attrs_with_current_deadline(
                        target,
                        link_path,
                        options,
                        attrs,
                        Some(operation_id),
                    )
                })
            })
        })
    }

    fn posix_symlink_with_create_attrs_with_current_deadline(
        &mut self,
        target: &str,
        link_path: &str,
        options: DfsWriteOptions,
        attrs: DfsPosixCreateAttrs,
        operation_id: Option<&str>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        validate_posix_symlink_target(target)?;
        let (link_path, requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path_distributed(&link_path, false)?;
        // A service that does not plan writes from its own metadata checks
        // the entry and its parent at their authority.
        let plans_locally = self.plans_write_from_local_metadata(&link_path)?;
        if requires_directory {
            if plans_locally {
                self.ensure_resolved_path_is_directory_no_follow(&link_path)?;
            } else {
                match self.lookup_sifs_path_direct_distributed_synced(&link_path)? {
                    Some(object) if object.kind == DfsObjectKind::Directory => {}
                    Some(object) if object.kind != DfsObjectKind::Tombstone => {
                        return Err(DfsPosixError::NotDirectory { path: link_path });
                    }
                    _ => return Err(DfsPosixError::NotFound { path: link_path }),
                }
            }
            return Err(DfsPosixError::AlreadyExists { path: link_path });
        }
        if link_path == "/" {
            return Err(DfsPosixError::InvalidPath { path: link_path });
        }
        if plans_locally {
            self.ensure_parent_directory(&link_path)?;
        } else {
            self.posix_write_parent_from_main(&link_path)?;
        }
        // Resolving parent links deliberately skips the final entry. Its local
        // replica may still precede an acknowledged unlink, so select that
        // exact entry from its authority without following a dangling target.
        let stable_object_id = operation_id
            .map(|operation_id| stable_create_object_id(&self.node_id, &link_path, operation_id));
        let mut object = match self.lookup_sifs_path_direct_distributed_synced(&link_path)? {
            Some(object)
                if object.kind == DfsObjectKind::Symlink
                    && stable_object_id.as_ref() == Some(&object.object_id)
                    && operation_id.is_some_and(|operation_id| {
                        object.version
                            == stable_write_target_version(
                                &self.node_id,
                                &object.object_id,
                                operation_id,
                            )
                    }) =>
            {
                // A repeat of this link's creation that committed.
                return self.committed_write_stat(&link_path);
            }
            Some(object) if object.kind != DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::AlreadyExists { path: link_path });
            }
            Some(_) => {
                let mut object = recreated_posix_metadata_object(
                    &self.node_id,
                    &link_path,
                    DfsObjectKind::Symlink,
                );
                let attrs = self.posix_create_attrs_for_object_authority(&object, attrs, false)?;
                apply_posix_create_attrs(&mut object, attrs);
                if let Some(object_id) = stable_object_id.clone() {
                    object.object_id = object_id;
                }
                self.add_transient_object_metadata(object.clone())?;
                object
            }
            None => {
                let mut object =
                    new_posix_metadata_object(&self.node_id, &link_path, DfsObjectKind::Symlink);
                let attrs = self.posix_create_attrs_for_object_authority(&object, attrs, false)?;
                apply_posix_create_attrs(&mut object, attrs);
                if let Some(object_id) = stable_object_id.clone() {
                    object.object_id = object_id;
                }
                self.add_transient_object_metadata(object.clone())?;
                object
            }
        };
        if let Err(error) =
            self.apply_managed_policy_to_new_object(&mut object, options.replica_candidates.len())
        {
            self.remove_transient_object_metadata(&object.object_id);
            return Err(error.into());
        }
        let target_version = operation_id
            .map(|operation_id| {
                stable_write_target_version(&self.node_id, &object.object_id, operation_id)
            })
            .unwrap_or_else(|| next_version(&object.version));
        let candidates = match self.posix_replica_candidates_for_write(&object, &options) {
            Ok(candidates) => candidates,
            Err(error) => {
                self.remove_transient_object_metadata(&object.object_id);
                return Err(error.into());
            }
        };
        self.writes
            .follow_write(&object.object_id, &target_version, options.wait_for_commit);
        let outcome = self.write_distributed_eventual_with_candidates_and_parent_touch(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version,
                requesting_node_id: self.node_id.clone(),
            },
            target.as_bytes().to_vec(),
            candidates,
            options.policy,
            options.foreground_replica_repair,
            operation_id.map(str::to_owned),
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.remove_transient_object_metadata(&object.object_id);
                return Err(error.into());
            }
        };
        let outcome = if options.wait_for_commit {
            match self.settle_ready_write_outcome(outcome, options.foreground_replica_repair) {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.remove_transient_object_metadata(&object.object_id);
                    return Err(error.into());
                }
            }
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => self.committed_write_stat(&link_path),
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                self.remove_transient_object_metadata(&object.object_id);
                Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                })
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => {
                self.remove_transient_object_metadata(&object.object_id);
                Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                })
            }
        }
    }

    pub fn posix_hardlink(&mut self, existing_path: &str, link_path: &str) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized_existing = self.resolve_posix_dot_components_distributed(existing_path)?;
        let dispatch_existing_path =
            crate::posix_path::path_after_dot_resolution(existing_path, &normalized_existing);
        let existing_path = dispatch_existing_path.as_ref();
        let normalized_link = self.resolve_posix_dot_components_distributed(link_path)?;
        let dispatch_link_path =
            crate::posix_path::path_after_dot_resolution(link_path, &normalized_link);
        let link_path = dispatch_link_path.as_ref();
        let mut primaries = self
            .routed_namespace_primaries_for_paths_fresh([
                normalized_existing.as_str(),
                normalized_link.as_str(),
            ])?
            .into_iter();
        let existing_primary = primaries.next().unwrap_or(None);
        let link_primary = primaries.next().unwrap_or(None);
        match (existing_primary, link_primary) {
            (None, None) => self.posix_hardlink_local(existing_path, link_path),
            (None, Some(_)) => {
                // This node owns the target's shard and coordinates the
                // cross-shard alias adoption itself.
                self.namespace_coordinator_dispatch_hardlink(
                    &sifs_file_system_id(),
                    existing_path,
                    link_path,
                )?;
                self.stat_routed_namespace_result(&normalized_link, false)
            }
            (Some(target_primary), _) => {
                // The link target's shard primary coordinates; it adopts the
                // alias into the link path's shard when that lives elsewhere.
                self.route_namespace_operation(
                    target_primary.clone(),
                    DfsNamespaceOperation::Hardlink {
                        existing_path: existing_path.to_string(),
                        link_path: link_path.to_string(),
                    },
                )?;
                self.stat_routed_namespace_result(&normalized_link, false)
            }
        }
    }

    pub(crate) fn posix_hardlink_local(
        &mut self,
        existing_path: &str,
        link_path: &str,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let (existing_path, existing_requires_directory) =
            normalize_posix_path_with_trailing_directory(existing_path)?;
        let existing_path = self.resolve_posix_path_distributed(&existing_path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&existing_path)?;
        if existing_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&existing_path)?;
        }
        let (link_path, link_requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path_distributed(&link_path, false)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&link_path)?;
        if link_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&link_path)?;
            return Err(DfsPosixError::AlreadyExists { path: link_path });
        }
        if link_path == "/" {
            return Err(DfsPosixError::InvalidPath { path: link_path });
        }
        self.ensure_parent_directory(&link_path)?;
        match self.lookup_sifs_path(&link_path)? {
            Some(object) if object.kind != DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::AlreadyExists { path: link_path });
            }
            _ => {}
        }
        let existing =
            self.lookup_sifs_path(&existing_path)?
                .ok_or_else(|| DfsPosixError::NotFound {
                    path: existing_path.clone(),
                })?;
        if existing.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: existing_path,
            });
        }
        let target = self.resolve_hardlink_target_held_or_at_primary(&existing)?;
        match target.kind {
            DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory {
                    path: existing_path,
                });
            }
            DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::NotFound {
                    path: existing_path,
                });
            }
            _ => {}
        }
        if let Some(primary) = self.routed_namespace_primary_for_path(&target.path)? {
            self.route_namespace_operation(primary, DfsNamespaceOperation::Hardlink {
                existing_path: target.path.clone(), link_path: link_path.clone(),
            })?;
            return self.stat_routed_namespace_result(&link_path, false);
        }
        if self.hardlink_alias_target_requires_cross_shard_coordination(&link_path, &target.path)? {
            self.coordinate_cross_shard_hardlink(&target.path, &link_path, self.node_id.clone())?;
            return self.stat_routed_namespace_result(&link_path, false);
        }
        let mut target = target;
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        target.namespace_link_count = Some(super::metadata::updated_namespace_link_count(&target, 1)?);
        // A path-derived id may still belong to an inode renamed elsewhere.
        // Each new directory entry needs its own identity, even at a tombstone.
        let alias_object_id = DfsObjectId::generate();
        let alias = DfsObjectMetadata {
            object_id: alias_object_id,
            file_system_id: target.file_system_id.clone(),
            path: link_path.clone(),
            kind: DfsObjectKind::Hardlink,
            namespace_link_count: None,
            tombstone_provenance: None,
            owner_node_id: self.node_id.clone(),
            // The directory entry refers to the canonical object's generation;
            // it does not own a second generation or a copied durability grant.
            current_locations: Vec::new(),
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: Some(target.object_id.clone()),
            mode: target.mode,
            uid: target.uid,
            gid: target.gid,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            version: initial_version(),
            policy: target.policy.clone(),
            created_by_event_id: None,
            last_modified_by_event_id: None,
        };
        // Alias, canonical count and parent timestamp are one physical-shard
        // publication. A failed preparation cannot leave a standalone count bump.
        let parent = self.parent_directory_touch_metadata_for_create(&alias)?;
        let mut objects = vec![target.clone(), alias];
        objects.extend(parent);
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            content_reference_identity: None, extension_expectations: Vec::new(),
            contribution_digest: None, file_version_pins: None,
            transaction_id: super::namespace_transaction_id_for_batch(
                "hardlink-create", &target.file_system_id, &objects, &[],
            ),
            file_system_id: target.file_system_id.clone(), objects, versions: Vec::new(),
            advisory_lock_states: Vec::new(), created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.stat_object_for_path(&target, &link_path)
    }
}
