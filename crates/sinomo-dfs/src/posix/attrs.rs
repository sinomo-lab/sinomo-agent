//! POSIX metadata attribute mutation helpers.
//!
//! Mode, owner, and timestamp updates all share the same local workflow:
//! resolve the visible path, reject cross-shard in-flight moves, resolve the
//! hardlink target, apply the attribute change, bump ctime/version when a
//! mutation occurs, and project a `DfsStat` for the caller.

use super::now_unix_nanos;
use crate::namespace::next_version;
use crate::posix_model::{DfsPosixError, DfsStat, Result};
use crate::posix_path::normalize_posix_path;
use crate::{
    DfsMessageTransport, DfsNamespaceOperation, DfsObjectKind, DfsObjectMetadata,
    NetworkedDfsService,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
    pub fn posix_set_mode(&mut self, path: &str, mode: u32) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetMode {
                    path: path.to_string(),
                    mode,
                },
            )?;
            return self.posix_stat(&normalized);
        }
        self.posix_set_mode_local(path, mode)
    }

    pub(crate) fn posix_set_mode_local(&mut self, path: &str, mode: u32) -> Result<DfsStat> {
        self.posix_update_metadata_target(path, true, |target| {
            target.mode = mode & 0o7777;
            true
        })
    }

    pub fn posix_set_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetOwner {
                    path: path.to_string(),
                    uid,
                    gid,
                },
            )?;
            return self.posix_stat(&normalized);
        }
        self.posix_set_owner_local(path, uid, gid)
    }

    pub fn posix_set_symlink_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
        if let Some(primary) = self.routed_namespace_primary_for_path(&normalized)? {
            self.route_namespace_operation(
                primary,
                DfsNamespaceOperation::SetSymlinkOwner {
                    path: path.to_string(),
                    uid,
                    gid,
                },
            )?;
            return self.posix_lstat(&normalized);
        }
        self.posix_set_symlink_owner_local(path, uid, gid)
    }

    pub(crate) fn posix_set_owner_local(
        &mut self,
        path: &str,
        uid: u32,
        gid: u32,
    ) -> Result<DfsStat> {
        self.posix_update_metadata_target(path, true, |target| {
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
        self.posix_update_metadata_target(path, false, |target| {
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
        let normalized = normalize_posix_path(path)?;
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
            return self.posix_stat(&normalized);
        }
        self.posix_set_times_local(path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub(crate) fn posix_set_times_local(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        self.posix_set_times_with_follow(path, true, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub fn posix_set_symlink_times(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let normalized = normalize_posix_path(path)?;
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
            return self.posix_lstat(&normalized);
        }
        self.posix_set_symlink_times_local(path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub(crate) fn posix_set_symlink_times_local(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
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
        self.posix_update_metadata_target(path, follow_final, |target| {
            let mut changed = false;
            if let Some(accessed_at_unix_nanos) = accessed_at_unix_nanos {
                target.accessed_at_unix_nanos = accessed_at_unix_nanos;
                changed = true;
            }
            if let Some(modified_at_unix_nanos) = modified_at_unix_nanos {
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
