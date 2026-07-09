//! POSIX namespace query helpers.
//!
//! This module owns read-only namespace surfaces such as stat, lstat,
//! readlink, and directory listing. It intentionally reuses the parent POSIX
//! facade's path-resolution and metadata helpers so mutation code and query
//! code share one interpretation of tombstones, hardlinks, and directories.

use super::{POSIX_LIST_DIR_COLLECT_PAGE_SIZE, now_unix_nanos};
use crate::metadata_pages::try_walk_metadata_pages;
use crate::namespace::next_version;
use crate::posix_model::{
    DfsDirEntry, DfsDirEntryPage, DfsPosixError, DfsStat, Result, read_conflict_error,
};
use crate::posix_path::normalize_posix_path;
use crate::{
    DfsDirectoryPageCursor, DfsMessageTransport, DfsObjectId, DfsObjectKind, DfsReadData,
    NetworkedDfsReadOutcome, NetworkedDfsService,
};

impl<T: DfsMessageTransport> NetworkedDfsService<T> {
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
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_readlink_distributed(&mut self, path: &str) -> Result<String> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
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
        match self.read_object_metadata_or_fetch_remote_without_cache(&object)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => String::from_utf8(bytes).map_err(|_| DfsPosixError::InvalidPath { path }),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_readlink_update_access(&mut self, path: &str) -> Result<String> {
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
            let path = service.resolve_existing_posix_path(path, true)?;
            service.posix_lstat(&path)
        })
    }

    pub fn posix_stat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        self.check_cancelled("distributed stat")?;
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_lstat_distributed(&path)
    }

    pub fn posix_lstat(&self, path: &str) -> Result<DfsStat> {
        self.with_metadata_lookup_cache(|service| {
            let path = service.resolve_existing_posix_path(path, false)?;
            let object = service
                .lookup_sifs_path(&path)?
                .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
            if object.kind == DfsObjectKind::Tombstone {
                return Err(DfsPosixError::NotFound { path });
            }
            service.stat_object_for_path(&object, &path)
        })
    }

    pub fn posix_lstat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        let (path, object) = self.lookup_sifs_path_distributed_synced(path, false)?;
        let object = object.ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        self.stat_object_for_path(&object, &path)
    }

    pub fn posix_stat_object(&self, object_id: &DfsObjectId) -> Result<Option<DfsStat>> {
        let Some(object) = self.object_by_id(object_id)? else {
            return Ok(None);
        };
        if object.kind == DfsObjectKind::Tombstone {
            return Ok(None);
        }
        Ok(Some(self.stat_object(&object)?))
    }

    pub fn posix_list_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        self.with_metadata_lookup_cache(|service| {
            let path = service.resolve_existing_posix_path(path, true)?;
            service.posix_collect_resolved_dir(&path)
        })
    }

    pub fn posix_list_dir_distributed(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
        self.check_cancelled("distributed list")?;
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        if self.local_metadata_replica_can_list_resolved_dir(&path)? {
            return self.posix_collect_resolved_dir(&path);
        }
        self.sync_remote_namespace_path_until_quiet(&path)?;
        self.posix_collect_resolved_dir(&path)
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
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        if self.local_metadata_replica_can_list_resolved_dir(&path)? {
            return self.posix_list_resolved_dir_page(&path, after, max_entries);
        }
        self.sync_remote_namespace_path_until_quiet(&path)?;
        self.posix_list_resolved_dir_page(&path, after, max_entries)
    }

    fn local_metadata_replica_can_list_resolved_dir(&mut self, path: &str) -> Result<bool> {
        let Some(shard) = self.routed_namespace_shard_for_path(path)? else {
            return Ok(true);
        };
        if !self.is_local_metadata_replica_for_shard(&shard) {
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
            let object_path = normalize_posix_path(&object.path)?;
            let listed_object_id = object.object_id.clone();
            let Some(object) = self.lookup_sifs_path(&object_path)? else {
                continue;
            };
            if object.kind == DfsObjectKind::Tombstone || object.object_id != listed_object_id {
                continue;
            }
            let target = self.resolve_hardlink_target(&object)?;
            entries.push(DfsDirEntry {
                name: object_path
                    .rsplit('/')
                    .next()
                    .filter(|name| !name.is_empty())
                    .unwrap_or("/")
                    .to_string(),
                path: object_path,
                kind: target.kind,
                object_id: target.object_id,
            });
        }
        Ok(DfsDirEntryPage {
            entries,
            next_cursor: page.next_cursor,
        })
    }

    pub fn posix_list_dir_update_access(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
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
