//! POSIX symlink and hardlink creation helpers.
//!
//! This module owns creating link records. Readlink/stat behavior lives in the
//! query module, and unlink/rename remain with namespace mutation code because
//! they share hardlink promotion and tombstone transaction handling.

use super::now_unix_nanos;
use crate::namespace::{initial_version, next_version, object_id_for_path};
use crate::posix_model::{DfsPosixCreateAttrs, DfsPosixError, DfsStat, DfsWriteOptions, Result};
use crate::posix_object::{
    apply_posix_create_attrs, new_posix_metadata_object, recreated_posix_metadata_object,
};
use crate::posix_path::{
    normalize_posix_path, normalize_posix_path_with_trailing_directory,
    validate_posix_symlink_target,
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
        validate_posix_symlink_target(target)?;
        let (link_path, requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path(&link_path, false)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&link_path)?;
            return Err(DfsPosixError::AlreadyExists { path: link_path });
        }
        if link_path == "/" {
            return Err(DfsPosixError::InvalidPath { path: link_path });
        }
        self.ensure_parent_directory(&link_path)?;
        let mut object = match self.lookup_sifs_path(&link_path)? {
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
                self.add_transient_object_metadata(object.clone())?;
                object
            }
            None => {
                let mut object =
                    new_posix_metadata_object(&self.node_id, &link_path, DfsObjectKind::Symlink);
                let attrs = self.posix_create_attrs_for_object_authority(&object, attrs, false)?;
                apply_posix_create_attrs(&mut object, attrs);
                self.add_transient_object_metadata(object.clone())?;
                object
            }
        };
        self.apply_managed_policy_to_new_object(&mut object, options.replica_candidates.len())?;
        let target_version = next_version(&object.version);
        let candidates = self.posix_replica_candidates_for_write(&object, &options)?;
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
            NetworkedDfsWriteOutcome::Committed { .. } => {
                let object =
                    self.lookup_sifs_path(&link_path)?
                        .ok_or_else(|| DfsPosixError::NotFound {
                            path: link_path.clone(),
                        })?;
                self.stat_object(&object)
            }
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
            NetworkedDfsWriteOutcome::Conflict { conflict } => {
                self.remove_transient_object_metadata(&object.object_id);
                Err(DfsPosixError::Conflict {
                    path: link_path,
                    reason: conflict.reason,
                })
            }
        }
    }

    pub fn posix_hardlink(&mut self, existing_path: &str, link_path: &str) -> Result<DfsStat> {
        let normalized_existing = normalize_posix_path(existing_path)?;
        let normalized_link = normalize_posix_path(link_path)?;
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
            (Some(target_primary), link_primary) => {
                // The link target's shard primary coordinates; it adopts the
                // alias into the link path's shard when that lives elsewhere.
                self.route_namespace_operation(
                    target_primary.clone(),
                    DfsNamespaceOperation::Hardlink {
                        existing_path: existing_path.to_string(),
                        link_path: link_path.to_string(),
                    },
                )?;
                if link_primary.is_some_and(|link_primary| link_primary != target_primary) {
                    self.sync_remote_namespace_path(&normalized_link)?;
                }
                self.stat_routed_namespace_result(&normalized_link, false)
            }
        }
    }

    pub(crate) fn posix_hardlink_local(
        &mut self,
        existing_path: &str,
        link_path: &str,
    ) -> Result<DfsStat> {
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
        let target = self.resolve_hardlink_target(&existing)?;
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
        let mut target = target;
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        let alias_object_id = {
            let candidate = object_id_for_path(&link_path);
            if candidate == target.object_id {
                DfsObjectId::generate()
            } else {
                candidate
            }
        };
        let alias = DfsObjectMetadata {
            object_id: alias_object_id,
            file_system_id: target.file_system_id.clone(),
            path: link_path.clone(),
            kind: DfsObjectKind::Hardlink,
            owner_node_id: self.node_id.clone(),
            current_locations: target.current_locations.clone(),
            content_hash: target.content_hash.clone(),
            content_size_bytes: target.content_size_bytes,
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
        self.add_object_metadata(alias.clone())?;
        self.touch_parent_directory_modified(&link_path)?;
        self.stat_object_for_path(&target, &link_path)
    }
}
