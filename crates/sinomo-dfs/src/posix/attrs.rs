//! POSIX metadata attribute mutation helpers.
//!
//! Mode, owner, and timestamp updates all share the same local workflow:
//! resolve the visible path, reject cross-shard in-flight moves, resolve the
//! hardlink target, apply the attribute change, bump ctime/version when a
//! mutation occurs, and project a `DfsStat` for the caller.

use super::now_unix_nanos;
use crate::namespace::next_version;
use crate::posix_model::{DfsPosixCreateAttrs, DfsPosixError, DfsStat, Result};
use crate::posix_object::inherit_posix_create_attrs;
use crate::posix_path::{normalize_posix_path, parent_posix_path};
use crate::{
    DfsMessageTransport, DfsNamespaceOperation, DfsObjectKind, DfsObjectMetadata,
    NetworkedDfsService,
};

#[cfg(test)]
mod strict_directory_native_tests;

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    /// Inspect an existing directory without creating it or following links.
    pub fn posix_inspect_directory_beneath_no_links_with_attrs(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<Option<DfsStat>> {
        let path = normalize_posix_path(path)?;
        let stat = match self.posix_open_beneath_no_links_distributed("/", &path) {
            Ok(stat) => stat,
            Err(DfsPosixError::NotFound { .. } | DfsPosixError::MissingParent { .. }) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        validate_directory_attrs(&path, &stat, attrs)?;
        Ok(Some(stat))
    }

    /// Create an absent directory with its final attributes. Existing entries
    /// must already match; this never changes their owner or mode.
    pub fn posix_ensure_directory_beneath_no_links_with_attrs(
        &mut self,
        path: &str,
        attrs: DfsPosixCreateAttrs,
    ) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let attrs = DfsPosixCreateAttrs::new(attrs.mode, attrs.uid, attrs.gid);
        if let Some(stat) =
            self.posix_inspect_directory_beneath_no_links_with_attrs(&path, attrs)?
        {
            return Ok(stat);
        }
        let parent = parent_posix_path(&path)
            .ok_or_else(|| DfsPosixError::InvalidPath { path: path.clone() })?;
        self.posix_mkdir_all_beneath_no_links("/", &parent)?;
        let opened_parent = self.posix_open_beneath_no_links_distributed("/", &parent)?;
        if inherit_posix_create_attrs(attrs, true, opened_parent.mode, opened_parent.gid) != attrs {
            return Err(DfsPosixError::Conflict {
                path,
                reason:
                    "parent inheritance is incompatible with the required directory owner and mode"
                        .into(),
            });
        }
        let child = path.rsplit_once('/').expect("canonical absolute path").1;
        let stat = match self.posix_mkdir_at_opened_directory_no_links_with_attrs(
            &opened_parent,
            child,
            attrs,
        ) {
            Ok(stat) => stat,
            Err(DfsPosixError::AlreadyExists { .. }) => {
                self.posix_open_beneath_no_links_distributed("/", &path)?
            }
            Err(error) => return Err(error),
        };
        validate_directory_attrs(&path, &stat, attrs)?;
        Ok(stat)
    }

    pub fn posix_set_mode(&mut self, path: &str, mode: u32) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetMode {
                    path: path.to_string(),
                    mode,
                },
            )?;
            return self.stat_routed_namespace_result(&normalized, true);
        }
        self.posix_set_mode_local(path, mode)
    }

    pub(crate) fn posix_set_mode_local(&mut self, path: &str, mode: u32) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_update_metadata_target(path, true, |target| {
            let mode = mode & 0o7777;
            if target.mode == mode {
                return false;
            }
            target.mode = mode;
            true
        })
    }

    pub fn posix_set_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let resolved = self.resolve_existing_posix_path_distributed(path, true)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&resolved)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetOwner {
                    path: resolved.clone(),
                    uid,
                    gid,
                },
            )?;
            return self.posix_stat_distributed(&resolved);
        }
        self.posix_set_owner_local(&resolved, uid, gid)
    }

    pub fn posix_set_symlink_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetSymlinkOwner {
                    path: path.to_string(),
                    uid,
                    gid,
                },
            )?;
            return self.posix_lstat_distributed(&normalized);
        }
        self.posix_set_symlink_owner_local(path, uid, gid)
    }

    pub(crate) fn posix_set_owner_local(
        &mut self,
        path: &str,
        uid: u32,
        gid: u32,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_update_metadata_target(path, true, |target| {
            if target.uid == uid && target.gid == gid {
                return false;
            }
            target.uid = uid;
            target.gid = gid;
            true
        })
    }

    pub(crate) fn posix_set_symlink_owner_local(
        &mut self,
        path: &str,
        uid: u32,
        gid: u32,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_update_metadata_target(path, false, |target| {
            if target.uid == uid && target.gid == gid {
                return false;
            }
            target.uid = uid;
            target.gid = gid;
            true
        })
    }

    pub fn posix_set_times(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetTimes {
                    path: path.to_string(),
                    accessed_at_unix_nanos,
                    modified_at_unix_nanos,
                    follow_symlink: true,
                },
            )?;
            return self.stat_routed_namespace_result(&normalized, true);
        }
        self.posix_set_times_local(path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub(crate) fn posix_set_times_local(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_set_times_with_follow(path, true, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub fn posix_set_symlink_times(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let normalized = self.resolve_posix_dot_components_distributed(path)?;
        let dispatch_path = crate::posix_path::path_after_dot_resolution(path, &normalized);
        let path = dispatch_path.as_ref();
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetTimes {
                    path: path.to_string(),
                    accessed_at_unix_nanos,
                    modified_at_unix_nanos,
                    follow_symlink: false,
                },
            )?;
            return self.stat_routed_namespace_result(&normalized, false);
        }
        self.posix_set_symlink_times_local(path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub(crate) fn posix_set_symlink_times_local(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_set_times_with_follow(
            path,
            false,
            accessed_at_unix_nanos,
            modified_at_unix_nanos,
        )
    }

    fn posix_set_times_with_follow(
        &mut self,
        path: &str,
        follow_final: bool,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let _namespace_plan = self.namespace_execution.begin_planning();
        self.posix_update_metadata_target(path, follow_final, |target| {
            let mut changed = false;
            if let Some(accessed_at_unix_nanos) = accessed_at_unix_nanos
                && target.accessed_at_unix_nanos != accessed_at_unix_nanos
            {
                target.accessed_at_unix_nanos = accessed_at_unix_nanos;
                changed = true;
            }
            if let Some(modified_at_unix_nanos) = modified_at_unix_nanos
                && target.modified_at_unix_nanos != modified_at_unix_nanos
            {
                target.modified_at_unix_nanos = modified_at_unix_nanos;
                changed = true;
            }
            changed
        })
    }

    fn posix_update_metadata_target<F>(
        &mut self,
        path: &str,
        follow_final: bool,
        update: F,
    ) -> Result<DfsStat>
    where
        F: FnOnce(&mut DfsObjectMetadata) -> bool,
    {
        let _namespace_plan = self.namespace_execution.begin_planning();
        let path = self.resolve_existing_posix_path_distributed(path, follow_final)?;
        self.ensure_resolved_path_not_in_cross_shard_flight(&path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let mut target = self.resolve_hardlink_target(&object)?;
        if target.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if !update(&mut target) {
            return self.stat_object_for_path(&target, &path);
        }
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        self.stat_object_for_path(&target, &path)
    }
}

fn validate_directory_attrs(path: &str, stat: &DfsStat, attrs: DfsPosixCreateAttrs) -> Result<()> {
    if stat.kind != DfsObjectKind::Directory {
        return Err(DfsPosixError::NotDirectory { path: path.into() });
    }
    if stat.uid != attrs.uid || stat.gid != attrs.gid || stat.mode != attrs.mode & 0o7777 {
        return Err(DfsPosixError::Conflict {
            path: path.into(),
            reason: format!(
                "SIFS directory {path} has owner {}:{} mode {:04o}, expected {}:{} mode {:04o}",
                stat.uid,
                stat.gid,
                stat.mode,
                attrs.uid,
                attrs.gid,
                attrs.mode & 0o7777,
            ),
        });
    }
    Ok(())
}
