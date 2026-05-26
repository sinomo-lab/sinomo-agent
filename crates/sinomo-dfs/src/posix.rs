//! POSIX-like DFS namespace facade.
//!
//! The facade provides path-based operations without Unix user/group permission
//! semantics. It is intended for services such as static web servers and
//! rollout/self-evolution tools that need stable mkdir/list/stat/read/write
//! behavior over SIFS while the lower DFS layers handle placement, receipts,
//! repair, and metadata journaling.

use crate::namespace::{initial_version, metadata_path, next_version, object_id_for_path};
use crate::{
    DfsError, DfsFileSystem, DfsNamespaceTransaction, DfsObjectKind, DfsObjectMetadata,
    DfsReadData, DfsReplicationPolicy, NetworkedDfsReadOutcome, NetworkedDfsService,
    NetworkedDfsWriteOutcome, sifs_file_system_id,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_network::NetworkTransport;
use sinomo_node::NodeId;
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DfsPosixError {
    #[error("DFS error: {0}")]
    Dfs(#[from] DfsError),
    #[error("metadata backend error: {0}")]
    Metadata(#[from] crate::MetadataBackendError),
    #[error("not found: {path}")]
    NotFound { path: String },
    #[error("already exists: {path}")]
    AlreadyExists { path: String },
    #[error("not a directory: {path}")]
    NotDirectory { path: String },
    #[error("is a directory: {path}")]
    IsDirectory { path: String },
    #[error("not a symbolic link: {path}")]
    NotSymlink { path: String },
    #[error("parent directory does not exist: {path}")]
    MissingParent { path: String },
    #[error("invalid DFS path: {path}")]
    InvalidPath { path: String },
    #[error(
        "write did not commit yet: {intent_id} ({durable_receipts}/{required_receipts} receipts)"
    )]
    PendingWrite {
        intent_id: String,
        durable_receipts: usize,
        required_receipts: usize,
    },
    #[error("write conflicted at {path}: {reason}")]
    Conflict { path: String, reason: String },
    #[error("directory is not empty: {path}")]
    DirectoryNotEmpty { path: String },
}

pub type Result<T> = std::result::Result<T, DfsPosixError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsStat {
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub path: String,
    pub kind: DfsObjectKind,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub size_bytes: Option<u64>,
    pub link_count: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub accessed_at_unix_nanos: u64,
    pub modified_at_unix_nanos: u64,
    pub created_at_unix_nanos: u64,
    pub changed_at_unix_nanos: u64,
    pub policy: DfsObjectPolicy,
    pub current_locations: Vec<NodeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsDirEntry {
    pub name: String,
    pub path: String,
    pub kind: DfsObjectKind,
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsWriteOptions {
    pub policy: DfsReplicationPolicy,
    pub replica_candidates: Vec<NodeId>,
    pub wait_for_commit: bool,
}

impl DfsWriteOptions {
    pub fn replicated_eventual(replica_candidates: Vec<NodeId>) -> Self {
        let desired = replica_candidates.len().max(1).min(2);
        Self {
            policy: DfsReplicationPolicy::new(1, desired).expect("valid default policy"),
            replica_candidates,
            wait_for_commit: true,
        }
    }
}

impl<T: NetworkTransport> NetworkedDfsService<T> {
    pub fn bootstrap_distributed_sifs(&mut self) -> Result<()> {
        let file_system = crate::namespace::distributed_sifs(self.node_id.clone());
        self.add_file_system(file_system)?;
        for object in crate::namespace::distributed_sifs_bootstrap_objects(self.node_id.clone()) {
            if self
                .lookup_metadata_path(&object.file_system_id, &object.path)?
                .is_none()
            {
                self.add_object_metadata(object)?;
            }
        }
        Ok(())
    }

    pub fn posix_mkdir(&mut self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
        if let Some(mut object) = self.lookup_sifs_path(&path)? {
            if object.kind != DfsObjectKind::Tombstone {
                return Err(DfsPosixError::AlreadyExists { path });
            }
            object.kind = DfsObjectKind::Directory;
            object.content_hash = None;
            object.content_size_bytes = None;
            object.link_target_object_id = None;
            object.current_locations = vec![self.node_id.clone()];
            set_object_create_timestamps(&mut object);
            object.version = next_version(&object.version);
            self.add_object_metadata(object.clone())?;
            self.touch_parent_directory_modified(&path)?;
            return self.stat_object(&object);
        }
        self.ensure_parent_directory(&path)?;
        let object = self.metadata_object_for_new_path(&path, DfsObjectKind::Directory);
        self.add_object_metadata(object.clone())?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_mkdir_all(&mut self, path: &str) -> Result<()> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if path == "/" {
            return Ok(());
        }
        let mut current = String::new();
        for part in path.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            match self.lookup_sifs_path(&current)? {
                Some(object) if object.kind == DfsObjectKind::Directory => {}
                Some(object) if object.kind == DfsObjectKind::Tombstone => {
                    let mut object = object;
                    object.kind = DfsObjectKind::Directory;
                    object.content_hash = None;
                    object.content_size_bytes = None;
                    object.link_target_object_id = None;
                    object.current_locations = vec![self.node_id.clone()];
                    set_object_create_timestamps(&mut object);
                    object.version = next_version(&object.version);
                    self.add_object_metadata(object)?;
                    self.touch_parent_directory_modified(&current)?;
                }
                Some(_) => return Err(DfsPosixError::NotDirectory { path: current }),
                None => {
                    self.posix_mkdir(&current)?;
                }
            }
        }
        Ok(())
    }

    pub fn posix_write_file(
        &mut self,
        path: &str,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        self.ensure_parent_directory(&path)?;
        let mut transient_create = false;
        let object = match self.lookup_sifs_path(&path)? {
            Some(object) if object.kind == DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory { path });
            }
            Some(object) if object.kind == DfsObjectKind::Hardlink => {
                self.resolve_hardlink_target(&object)?
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let mut object = object;
                object.kind = DfsObjectKind::File;
                object.content_hash = None;
                object.content_size_bytes = None;
                object.link_target_object_id = None;
                object.current_locations = vec![self.node_id.clone()];
                set_object_create_timestamps(&mut object);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
            Some(object) => object,
            None => {
                let object = self.metadata_object_for_new_path(&path, DfsObjectKind::File);
                self.add_transient_object_metadata(object.clone())?;
                transient_create = true;
                object
            }
        };
        let target_version = next_version(&object.version);
        let mut candidates = options.replica_candidates;
        if !candidates.contains(&self.node_id) {
            candidates.insert(0, self.node_id.clone());
        }
        let outcome = self.write_distributed_eventual_with_candidates(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version,
                requesting_node_id: self.node_id.clone(),
            },
            bytes.to_vec(),
            candidates,
            options.policy,
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                return Err(error.into());
            }
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                if transient_create {
                    self.touch_parent_directory_modified(&path)?;
                }
                let object = self.stat_target_for_path(&path)?;
                self.stat_object_for_path(&object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                    return Err(DfsPosixError::PendingWrite {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                    });
                }
                let object = self.stat_target_for_path(&path)?;
                let mut stat = self.stat_object(&object)?;
                stat.path = path.clone();
                stat.size_bytes = Some(bytes.len() as u64);
                let _ = intent_id;
                let _ = durable_receipts;
                let _ = required_receipts;
                Ok(stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                Err(DfsPosixError::PendingWrite {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                })
            }
            NetworkedDfsWriteOutcome::Conflict { conflict } => {
                if transient_create {
                    self.remove_transient_object_metadata(&object.object_id);
                }
                Err(DfsPosixError::Conflict {
                    path,
                    reason: conflict.reason,
                })
            }
        }
    }

    pub fn posix_write_file_at(
        &mut self,
        path: &str,
        offset: u64,
        bytes: &[u8],
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        let base_version = object.version.clone();
        let mut current = match self.read_current_object_or_fetch_remote(&object.object_id)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => bytes,
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => return Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                return Err(read_conflict_error(path, conflicts));
            }
        };
        let start = usize::try_from(offset).map_err(|_| DfsPosixError::InvalidPath {
            path: format!("{path}@{offset}"),
        })?;
        let end = start
            .checked_add(bytes.len())
            .ok_or_else(|| DfsPosixError::InvalidPath {
                path: format!("{path}@{offset}+{}", bytes.len()),
            })?;
        if current.len() < end {
            current.resize(end, 0);
        }
        current[start..end].copy_from_slice(bytes);
        let result_size = current.len() as u64;
        let target_version = next_version(&object.version);
        let mut candidates = options.replica_candidates;
        if !candidates.contains(&self.node_id) {
            candidates.insert(0, self.node_id.clone());
        }
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version,
            requesting_node_id: self.node_id.clone(),
        };
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidates, options.policy.clone())?;
        let outcome = self.write_distributed_eventual_with_base(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
        )?;
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                let object = self.stat_target_for_path(&path)?;
                self.stat_object_for_path(&object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                let object = self.stat_target_for_path(&path)?;
                let mut stat = self.stat_object_for_path(&object, &path)?;
                stat.size_bytes = Some(result_size);
                let _ = intent_id;
                let _ = durable_receipts;
                let _ = required_receipts;
                Ok(stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => Err(DfsPosixError::PendingWrite {
                intent_id,
                durable_receipts,
                required_receipts,
            }),
            NetworkedDfsWriteOutcome::Conflict { conflict } => Err(DfsPosixError::Conflict {
                path,
                reason: conflict.reason,
            }),
        }
    }

    pub fn posix_truncate_file(
        &mut self,
        path: &str,
        len: u64,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        if requires_directory {
            return match self.ensure_resolved_path_is_directory(&path) {
                Ok(()) => Err(DfsPosixError::IsDirectory { path }),
                Err(DfsPosixError::NotDirectory { path }) => {
                    Err(DfsPosixError::NotDirectory { path })
                }
                Err(DfsPosixError::NotFound { path }) => Err(DfsPosixError::NotDirectory { path }),
                Err(error) => Err(error),
            };
        }
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        let base_version = object.version.clone();
        let mut current = match self.read_current_object_or_fetch_remote(&object.object_id)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => bytes,
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => return Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                return Err(read_conflict_error(path, conflicts));
            }
        };
        let len = usize::try_from(len).map_err(|_| DfsPosixError::InvalidPath {
            path: format!("{path}@len={len}"),
        })?;
        current.resize(len, 0);
        let target_version = next_version(&object.version);
        let mut candidates = options.replica_candidates;
        if !candidates.contains(&self.node_id) {
            candidates.insert(0, self.node_id.clone());
        }
        let request = DfsWriteRequest {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            target_version,
            requesting_node_id: self.node_id.clone(),
        };
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidates, options.policy.clone())?;
        let outcome = self.write_distributed_eventual_with_base(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
        )?;
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                let object = self.stat_target_for_path(&path)?;
                self.stat_object_for_path(&object, &path)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } if !options.wait_for_commit => {
                let object = self.stat_target_for_path(&path)?;
                let mut stat = self.stat_object_for_path(&object, &path)?;
                stat.size_bytes = Some(len as u64);
                let _ = intent_id;
                let _ = durable_receipts;
                let _ = required_receipts;
                Ok(stat)
            }
            NetworkedDfsWriteOutcome::Pending {
                intent_id,
                durable_receipts,
                required_receipts,
            } => Err(DfsPosixError::PendingWrite {
                intent_id,
                durable_receipts,
                required_receipts,
            }),
            NetworkedDfsWriteOutcome::Conflict { conflict } => Err(DfsPosixError::Conflict {
                path,
                reason: conflict.reason,
            }),
        }
    }

    pub fn posix_read_file(&self, path: &str) -> Result<Vec<u8>> {
        let path = self.resolve_existing_posix_path(path, true)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        match self.read_local_object_with_state(&object.object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => Ok(bytes),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_read_file_distributed(&mut self, path: &str) -> Result<Vec<u8>> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        match self.read_current_object_or_fetch_remote(&object.object_id)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => Ok(bytes),
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_read_file_update_access(&mut self, path: &str) -> Result<Vec<u8>> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        match self.read_current_object_or_fetch_remote(&object.object_id)? {
            NetworkedDfsReadOutcome::Found {
                data: DfsReadData { bytes, .. },
                ..
            } => {
                let mut object = self
                    .object_by_id(&object.object_id)?
                    .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                object = self.resolve_hardlink_target(&object)?;
                if object.kind == DfsObjectKind::Directory {
                    return Err(DfsPosixError::IsDirectory { path });
                }
                object.accessed_at_unix_nanos = now_unix_nanos();
                object.version = next_version(&object.version);
                self.add_object_metadata(object)?;
                Ok(bytes)
            }
            NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    fn read_current_object_or_fetch_remote(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<NetworkedDfsReadOutcome> {
        let initial = self.read_local_object_with_state(object_id, false)?;
        if !matches!(initial, NetworkedDfsReadOutcome::PendingReplication { .. }) {
            return Ok(initial);
        }
        let requested = self.request_remote_object_read(object_id)?;
        if requested.is_empty() {
            return Ok(initial);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.process_incoming()?;
            let current = self.read_local_object_with_state(object_id, false)?;
            if !matches!(current, NetworkedDfsReadOutcome::PendingReplication { .. }) {
                return Ok(current);
            }
            if !self.has_pending_remote_read_for_current_object(object_id)? {
                if Instant::now() >= deadline {
                    return Ok(current);
                }
                if self.request_remote_object_read(object_id)?.is_empty() {
                    return Ok(current);
                }
            }
            if Instant::now() >= deadline {
                return Ok(current);
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    pub fn posix_request_remote_read(&mut self, path: &str) -> Result<Vec<NodeId>> {
        self.posix_request_remote_read_with_follow(path, true)
    }

    pub fn posix_request_remote_read_no_follow(&mut self, path: &str) -> Result<Vec<NodeId>> {
        self.posix_request_remote_read_with_follow(path, false)
    }

    fn posix_request_remote_read_with_follow(
        &mut self,
        path: &str,
        follow_final: bool,
    ) -> Result<Vec<NodeId>> {
        let path = self.resolve_existing_posix_path(path, follow_final)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        let object = self.resolve_hardlink_target(&object)?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        match self.read_local_object_with_state(&object.object_id, false)? {
            NetworkedDfsReadOutcome::Found { .. } => Ok(Vec::new()),
            NetworkedDfsReadOutcome::PendingReplication { .. } => self
                .request_remote_object_read(&object.object_id)
                .map_err(DfsPosixError::from),
            NetworkedDfsReadOutcome::NotConfirmed { .. } | NetworkedDfsReadOutcome::NotFound => {
                Err(DfsPosixError::NotFound { path })
            }
            NetworkedDfsReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_symlink(
        &mut self,
        target: &str,
        link_path: &str,
        options: DfsWriteOptions,
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
        let object = match self.lookup_sifs_path(&link_path)? {
            Some(object) if object.kind != DfsObjectKind::Tombstone => {
                return Err(DfsPosixError::AlreadyExists { path: link_path });
            }
            Some(object) => {
                let mut object = object;
                object.kind = DfsObjectKind::Symlink;
                object.content_hash = None;
                object.content_size_bytes = None;
                object.link_target_object_id = None;
                object.current_locations = vec![self.node_id.clone()];
                set_object_create_timestamps(&mut object);
                self.add_transient_object_metadata(object.clone())?;
                object
            }
            None => {
                let object = self.metadata_object_for_new_path(&link_path, DfsObjectKind::Symlink);
                self.add_transient_object_metadata(object.clone())?;
                object
            }
        };
        let target_version = next_version(&object.version);
        let mut candidates = options.replica_candidates;
        if !candidates.contains(&self.node_id) {
            candidates.insert(0, self.node_id.clone());
        }
        let outcome = self.write_distributed_eventual_with_candidates(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version,
                requesting_node_id: self.node_id.clone(),
            },
            target.as_bytes().to_vec(),
            candidates,
            options.policy,
        );
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.remove_transient_object_metadata(&object.object_id);
                return Err(error.into());
            }
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed { .. } => {
                self.touch_parent_directory_modified(&link_path)?;
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
        let (existing_path, existing_requires_directory) =
            normalize_posix_path_with_trailing_directory(existing_path)?;
        let existing_path = self.resolve_posix_path_distributed(&existing_path, false)?;
        if existing_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&existing_path)?;
        }
        let (link_path, link_requires_directory) =
            normalize_posix_path_with_trailing_directory(link_path)?;
        let link_path = self.resolve_posix_path_distributed(&link_path, false)?;
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

    pub fn posix_set_mode(&mut self, path: &str, mode: u32) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
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
        target.mode = mode & 0o7777;
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        self.stat_object_for_path(&target, &path)
    }

    pub fn posix_set_owner(&mut self, path: &str, uid: u32, gid: u32) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
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
        target.uid = uid;
        target.gid = gid;
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        self.stat_object_for_path(&target, &path)
    }

    pub fn posix_set_times(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_set_times_at_path(&path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    pub fn posix_set_symlink_times(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
        self.posix_set_times_at_path(&path, accessed_at_unix_nanos, modified_at_unix_nanos)
    }

    fn posix_set_times_at_path(
        &mut self,
        path: &str,
        accessed_at_unix_nanos: Option<u64>,
        modified_at_unix_nanos: Option<u64>,
    ) -> Result<DfsStat> {
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        let mut target = self.resolve_hardlink_target(&object)?;
        if target.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        if accessed_at_unix_nanos.is_none() && modified_at_unix_nanos.is_none() {
            return self.stat_object_for_path(&target, path);
        }
        if let Some(accessed_at_unix_nanos) = accessed_at_unix_nanos {
            target.accessed_at_unix_nanos = accessed_at_unix_nanos;
        }
        if let Some(modified_at_unix_nanos) = modified_at_unix_nanos {
            target.modified_at_unix_nanos = modified_at_unix_nanos;
        }
        target.changed_at_unix_nanos = now_unix_nanos();
        target.version = next_version(&target.version);
        self.add_object_metadata(target.clone())?;
        self.stat_object_for_path(&target, path)
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
        match self.read_current_object_or_fetch_remote(&object.object_id)? {
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
        object.accessed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(target)
    }

    pub fn posix_unlink(&mut self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
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
            object.kind = DfsObjectKind::Tombstone;
            object.version = next_version(&object.version);
            object.content_hash = None;
            object.content_size_bytes = None;
            object.link_target_object_id = None;
            object.current_locations = vec![self.node_id.clone()];
            self.add_object_metadata(object.clone())?;
            self.touch_parent_directory_modified(&path)?;
            return self.stat_object(&object);
        }
        if self.link_count_for_object(&object)? > 1 {
            return self.unlink_canonical_with_hardlinks(object, &path);
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.content_hash = None;
        object.content_size_bytes = None;
        object.link_target_object_id = None;
        object.current_locations = vec![self.node_id.clone()];
        self.add_object_metadata(object.clone())?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_rmdir(&mut self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, false)?;
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
        if !self.posix_list_dir(&path)?.is_empty() {
            return Err(DfsPosixError::DirectoryNotEmpty { path });
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.current_locations = vec![self.node_id.clone()];
        self.add_object_metadata(object.clone())?;
        self.touch_parent_directory_modified(&path)?;
        self.stat_object(&object)
    }

    pub fn posix_clear_dir_recursive(&mut self, path: &str) -> Result<()> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        let directory = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if directory.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if directory.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path });
        }
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        let mut descendant_paths = self
            .descendant_objects_for_sifs_dir(&path)?
            .into_iter()
            .filter_map(|object| normalize_posix_path(&object.path).ok())
            .collect::<Vec<_>>();
        descendant_paths.sort();
        descendant_paths.dedup();
        descendant_paths.sort_by(|left_path, right_path| {
            right_path
                .len()
                .cmp(&left_path.len())
                .then_with(|| right_path.cmp(left_path))
        });
        for object_path in descendant_paths {
            for _ in 0..64 {
                let Some(object) = self.lookup_sifs_path(&object_path)? else {
                    break;
                };
                if object.kind == DfsObjectKind::Tombstone {
                    break;
                }
                if !matches!(
                    object.kind,
                    DfsObjectKind::Hardlink | DfsObjectKind::Directory | DfsObjectKind::Tombstone
                ) && self.link_count_for_object(&object)? > 1
                    && let Some(alias) =
                        self.first_hardlink_alias_outside_prefix(&object, &prefix)?
                {
                    let mut alias_tombstone = alias.clone();
                    alias_tombstone.path = hidden_alias_tombstone_path(&alias.object_id);
                    alias_tombstone.kind = DfsObjectKind::Tombstone;
                    alias_tombstone.version = next_version(&alias.version);
                    alias_tombstone.content_hash = None;
                    alias_tombstone.content_size_bytes = None;
                    alias_tombstone.link_target_object_id = None;
                    alias_tombstone.current_locations = vec![self.node_id.clone()];

                    let mut old_path_tombstone = object.clone();
                    old_path_tombstone.object_id = DfsObjectId::generate();
                    old_path_tombstone.kind = DfsObjectKind::Tombstone;
                    old_path_tombstone.version = next_version(&object.version);
                    old_path_tombstone.content_hash = None;
                    old_path_tombstone.content_size_bytes = None;
                    old_path_tombstone.link_target_object_id = None;
                    old_path_tombstone.current_locations = vec![self.node_id.clone()];

                    let mut promoted = object.clone();
                    promoted.path = alias.path.clone();
                    promoted.changed_at_unix_nanos = now_unix_nanos();
                    promoted.version = next_version(&promoted.version);
                    self.apply_namespace_transaction(DfsNamespaceTransaction {
                        transaction_id: namespace_transaction_id(
                            "clear-hardlink",
                            &promoted.object_id,
                            &promoted.path,
                        ),
                        file_system_id: promoted.file_system_id.clone(),
                        objects: vec![old_path_tombstone, alias_tombstone, promoted],
                        created_at: time::OffsetDateTime::now_utc(),
                    })?;
                    continue;
                }
                let tombstone = DfsObjectMetadata {
                    object_id: DfsObjectId::generate(),
                    file_system_id: object.file_system_id.clone(),
                    path: object.path.clone(),
                    kind: DfsObjectKind::Tombstone,
                    owner_node_id: self.node_id.clone(),
                    current_locations: vec![self.node_id.clone()],
                    content_hash: None,
                    content_size_bytes: None,
                    link_target_object_id: None,
                    mode: 0,
                    uid: 0,
                    gid: 0,
                    accessed_at_unix_nanos: 0,
                    modified_at_unix_nanos: 0,
                    created_at_unix_nanos: 0,
                    changed_at_unix_nanos: 0,
                    version: next_version(&object.version),
                    policy: object.policy.clone(),
                    created_by_event_id: object.created_by_event_id.clone(),
                    last_modified_by_event_id: object.last_modified_by_event_id.clone(),
                };
                self.add_object_metadata(tombstone)?;
            }
        }
        self.touch_directory_modified(&path)?;
        Ok(())
    }

    pub fn posix_rename(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let (from, from_requires_directory) = normalize_posix_path_with_trailing_directory(from)?;
        let (to, to_requires_directory) = normalize_posix_path_with_trailing_directory(to)?;
        let from = self.resolve_posix_path_distributed(&from, false)?;
        if from_requires_directory {
            self.ensure_resolved_path_is_directory_no_follow(&from)?;
        }
        let to = self.resolve_posix_path_distributed(&to, false)?;
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
        }

        let old_source_path = source.path.clone();
        let old_prefix = format!("{}/", from.trim_end_matches('/'));
        let new_prefix = format!("{}/", to.trim_end_matches('/'));
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
                        if !self.posix_list_dir(&to)?.is_empty() {
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
                        .hardlinks_to_object(&destination.object_id)?
                        .into_iter()
                        .min_by(|left, right| left.path.cmp(&right.path))
                        .ok_or_else(|| DfsPosixError::NotFound { path: to.clone() })?;
                    let mut alias_tombstone = alias.clone();
                    alias_tombstone.path = hidden_alias_tombstone_path(&alias.object_id);
                    alias_tombstone.kind = DfsObjectKind::Tombstone;
                    alias_tombstone.version = next_version(&alias.version);
                    alias_tombstone.content_hash = None;
                    alias_tombstone.content_size_bytes = None;
                    alias_tombstone.link_target_object_id = None;
                    alias_tombstone.current_locations = vec![self.node_id.clone()];

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
                    destination.kind = DfsObjectKind::Tombstone;
                    destination.version = next_version(&destination.version);
                    destination.content_hash = None;
                    destination.content_size_bytes = None;
                    destination.link_target_object_id = None;
                    destination.current_locations = vec![self.node_id.clone()];
                    moved_children.push(destination);
                }
            }
        }

        if source.kind == DfsObjectKind::Directory {
            for mut object in self.descendant_objects_for_sifs_dir(&from)? {
                let object_path = normalize_posix_path(&object.path)?;
                let listed_object_id = object.object_id.clone();
                if object.object_id == source.object_id
                    || object.kind == DfsObjectKind::Tombstone
                    || !object_path.starts_with(&old_prefix)
                {
                    continue;
                }
                let Some(visible_object) = self.lookup_sifs_path(&object_path)? else {
                    continue;
                };
                if visible_object.kind == DfsObjectKind::Tombstone
                    || visible_object.object_id != listed_object_id
                {
                    continue;
                }
                let suffix = object_path.trim_start_matches(&old_prefix);
                object.path = format!("{new_prefix}{suffix}");
                moved_children.push(object);
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

        let mut old_path_tombstone = source.clone();
        old_path_tombstone.object_id = DfsObjectId::generate();
        old_path_tombstone.path = old_source_path;
        old_path_tombstone.kind = DfsObjectKind::Tombstone;
        old_path_tombstone.version = next_version(&source.version);
        old_path_tombstone.content_hash = None;
        old_path_tombstone.content_size_bytes = None;
        old_path_tombstone.link_target_object_id = None;
        old_path_tombstone.current_locations = vec![self.node_id.clone()];

        source.path = to;
        source.changed_at_unix_nanos = now_unix_nanos();
        source.version = next_version(&source.version);
        let mut transaction_objects = vec![old_path_tombstone, source.clone()];
        transaction_objects.extend(moved_children);
        transaction_objects.extend(hardlink_target_updates);
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id("rename", &source.object_id, &source.path),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_modified(&from)?;
        if parent_posix_path(&from) != parent_posix_path(&source.path) {
            self.touch_parent_directory_modified(&source.path)?;
        }
        self.stat_object(&source)
    }

    pub fn posix_stat(&self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path(path, true)?;
        self.posix_lstat(&path)
    }

    pub fn posix_stat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_lstat_distributed(&path)
    }

    pub fn posix_lstat(&self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path(path, false)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        self.stat_object_for_path(&object, &path)
    }

    pub fn posix_lstat_distributed(&mut self, path: &str) -> Result<DfsStat> {
        let path = self.resolve_existing_posix_path_distributed(path, false)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
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
        let path = self.resolve_existing_posix_path(path, true)?;
        self.posix_list_resolved_dir(&path)
    }

    pub fn posix_list_dir_distributed(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_list_resolved_dir(&path)
    }

    fn posix_list_resolved_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let object = self
            .lookup_sifs_path(&path)?
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
        for object in self.child_objects_for_sifs_dir(path)? {
            let object_path = normalize_posix_path(&object.path)?;
            let listed_object_id = object.object_id.clone();
            let Some(object) = self.lookup_sifs_path(&object_path)? else {
                continue;
            };
            if object.kind == DfsObjectKind::Tombstone || object.object_id != listed_object_id {
                continue;
            }
            entries.push(DfsDirEntry {
                name: object_path
                    .rsplit('/')
                    .next()
                    .filter(|name| !name.is_empty())
                    .unwrap_or("/")
                    .to_string(),
                path: object_path,
                kind: self.resolve_hardlink_target(&object)?.kind,
                object_id: self.resolve_hardlink_target(&object)?.object_id,
            });
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
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
        object.accessed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(entries)
    }

    fn lookup_sifs_path(&self, path: &str) -> Result<Option<DfsObjectMetadata>> {
        self.lookup_metadata_path(&sifs_file_system_id(), path)
    }

    fn resolve_posix_path(&self, path: &str, follow_final: bool) -> Result<String> {
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

    fn resolve_posix_path_distributed(&mut self, path: &str, follow_final: bool) -> Result<String> {
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

    fn resolve_existing_posix_path(&self, path: &str, follow_final: bool) -> Result<String> {
        let (path, requires_directory) = normalize_posix_path_with_trailing_directory(path)?;
        let path = self.resolve_posix_path(&path, follow_final || requires_directory)?;
        if requires_directory {
            self.ensure_resolved_path_is_directory(&path)?;
        }
        Ok(path)
    }

    fn resolve_existing_posix_path_distributed(
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
        match self.read_current_object_or_fetch_remote(&object.object_id)? {
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

    fn lookup_metadata_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        if self.has_tombstone_ancestor(file_system_id, path)? {
            return Ok(None);
        }
        self.lookup_metadata_path_raw(file_system_id, path)
    }

    fn lookup_metadata_path_raw(
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
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.get_object_by_path(file_system_id, &metadata_path(path))?);
        }
        Ok(self
            .metadata
            .objects
            .values()
            .find(|object| {
                &object.file_system_id == file_system_id
                    && object.kind != DfsObjectKind::Tombstone
                    && normalize_posix_path(&object.path)
                        .map(|normalized| normalized == path)
                        .unwrap_or(false)
            })
            .cloned())
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
            if let Some(object) = self.lookup_metadata_path_raw(file_system_id, parent)? {
                if object.kind == DfsObjectKind::Tombstone {
                    return Ok(true);
                }
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

    fn child_objects_for_sifs_dir(&self, path: &str) -> Result<Vec<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.child_objects_for_directory(&file_system_id, path)?);
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

    fn hardlinks_to_object(
        &self,
        target_object_id: &DfsObjectId,
    ) -> Result<Vec<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.hardlinks_to_object(&file_system_id, target_object_id)?);
        }
        let mut hardlinks = self
            .metadata
            .objects
            .values()
            .filter(|candidate| {
                candidate.file_system_id == file_system_id
                    && candidate.kind == DfsObjectKind::Hardlink
                    && candidate.link_target_object_id.as_ref() == Some(target_object_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        hardlinks.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(hardlinks)
    }

    fn descendant_objects_for_sifs_dir(&self, path: &str) -> Result<Vec<DfsObjectMetadata>> {
        let mut descendants = Vec::new();
        let mut pending_dirs = vec![normalize_posix_path(path)?];
        while let Some(directory_path) = pending_dirs.pop() {
            for object in self.child_objects_for_sifs_dir(&directory_path)? {
                if object.kind == DfsObjectKind::Tombstone {
                    continue;
                }
                if object.kind == DfsObjectKind::Directory {
                    pending_dirs.push(normalize_posix_path(&object.path)?);
                }
                descendants.push(object);
            }
        }
        descendants.sort_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.object_id.cmp(&right.object_id))
        });
        Ok(descendants)
    }

    fn object_by_id(&self, object_id: &DfsObjectId) -> Result<Option<DfsObjectMetadata>> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(object) = backend.get_object(object_id)?
        {
            return Ok(Some(object));
        }
        Ok(self.metadata.object(object_id).cloned())
    }

    fn resolve_hardlink_target(&self, object: &DfsObjectMetadata) -> Result<DfsObjectMetadata> {
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

    fn stat_target_for_path(&self, path: &str) -> Result<DfsObjectMetadata> {
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

    fn ensure_parent_directory(&self, path: &str) -> Result<()> {
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

    fn ensure_resolved_path_is_directory(&self, path: &str) -> Result<()> {
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

    fn ensure_resolved_path_is_directory_no_follow(&self, path: &str) -> Result<()> {
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

    fn touch_parent_directory_modified(&mut self, path: &str) -> Result<()> {
        let Some(parent) = parent_posix_path(path) else {
            return Ok(());
        };
        self.touch_directory_modified(&parent)
    }

    fn touch_directory_modified(&mut self, path: &str) -> Result<()> {
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
        let now = now_unix_nanos();
        object.modified_at_unix_nanos = now;
        object.changed_at_unix_nanos = now;
        object.version = next_version(&object.version);
        self.add_object_metadata(object)?;
        Ok(())
    }

    fn metadata_object_for_new_path(&self, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
        let mode = default_mode_for_kind(&kind);
        DfsObjectMetadata {
            object_id: object_id_for_path(path),
            file_system_id: sifs_file_system_id(),
            path: path.to_string(),
            kind,
            owner_node_id: self.node_id.clone(),
            current_locations: vec![self.node_id.clone()],
            content_hash: None,
            content_size_bytes: None,
            link_target_object_id: None,
            mode,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: now_unix_nanos(),
            modified_at_unix_nanos: now_unix_nanos(),
            created_at_unix_nanos: now_unix_nanos(),
            changed_at_unix_nanos: now_unix_nanos(),
            version: initial_version(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn stat_object(&self, object: &DfsObjectMetadata) -> Result<DfsStat> {
        self.stat_object_for_path(object, &object.path)
    }

    fn stat_object_for_path(&self, object: &DfsObjectMetadata, path: &str) -> Result<DfsStat> {
        let object = self.resolve_hardlink_target(object)?;
        let size_bytes = self.object_content_size_hint(&object)?;
        let link_count = self.link_count_for_object(&object)?;
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

    fn link_count_for_object(&self, object: &DfsObjectMetadata) -> Result<u64> {
        if object.kind == DfsObjectKind::Hardlink {
            let target = self.resolve_hardlink_target(object)?;
            return self.link_count_for_object(&target);
        }
        if object.kind != DfsObjectKind::Directory {
            let count = self.hardlinks_to_object(&object.object_id)?.len();
            return Ok(1u64.saturating_add(count as u64));
        }
        let path = normalize_posix_path(&object.path)?;
        let child_directory_count = self
            .child_objects_for_sifs_dir(&path)?
            .into_iter()
            .filter(|child| child.kind == DfsObjectKind::Directory)
            .count() as u64;
        Ok(2u64.saturating_add(child_directory_count))
    }

    fn unlink_canonical_with_hardlinks(
        &mut self,
        mut object: DfsObjectMetadata,
        old_path: &str,
    ) -> Result<DfsStat> {
        let alias = self
            .hardlinks_to_object(&object.object_id)?
            .into_iter()
            .min_by(|left, right| left.path.cmp(&right.path))
            .ok_or_else(|| DfsPosixError::NotFound {
                path: old_path.to_string(),
            })?;
        let mut alias_tombstone = alias.clone();
        alias_tombstone.path = hidden_alias_tombstone_path(&alias.object_id);
        alias_tombstone.kind = DfsObjectKind::Tombstone;
        alias_tombstone.version = next_version(&alias.version);
        alias_tombstone.content_hash = None;
        alias_tombstone.content_size_bytes = None;
        alias_tombstone.link_target_object_id = None;
        alias_tombstone.current_locations = vec![self.node_id.clone()];

        let mut old_path_tombstone = object.clone();
        old_path_tombstone.object_id = DfsObjectId::generate();
        old_path_tombstone.path = old_path.to_string();
        old_path_tombstone.kind = DfsObjectKind::Tombstone;
        old_path_tombstone.version = next_version(&object.version);
        old_path_tombstone.content_hash = None;
        old_path_tombstone.content_size_bytes = None;
        old_path_tombstone.link_target_object_id = None;
        old_path_tombstone.current_locations = vec![self.node_id.clone()];

        object.path = alias.path.clone();
        object.changed_at_unix_nanos = now_unix_nanos();
        object.version = next_version(&object.version);

        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id(
                "unlink-hardlink",
                &object.object_id,
                old_path,
            ),
            file_system_id: object.file_system_id.clone(),
            objects: vec![old_path_tombstone.clone(), alias_tombstone, object],
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.touch_parent_directory_modified(old_path)?;
        self.stat_object(&old_path_tombstone)
    }

    fn first_hardlink_alias_outside_prefix(
        &self,
        object: &DfsObjectMetadata,
        prefix: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        let mut aliases = self
            .hardlinks_to_object(&object.object_id)?
            .into_iter()
            .filter(|candidate| {
                normalize_posix_path(&candidate.path)
                    .map(|path| !path.starts_with(prefix))
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        aliases.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(aliases.into_iter().next())
    }
}

fn object_timestamp_or_default(value: u64) -> u64 {
    if value == 0 { 1_000_000_000 } else { value }
}

fn object_mode_or_default(object: &DfsObjectMetadata) -> u32 {
    object.mode & 0o7777
}

fn default_mode_for_kind(kind: &DfsObjectKind) -> u32 {
    match kind {
        DfsObjectKind::Directory => 0o755,
        DfsObjectKind::Symlink => 0o777,
        DfsObjectKind::Tombstone => 0,
        _ => 0o644,
    }
}

fn read_conflict_error(path: String, conflicts: Vec<crate::DfsConflictReadView>) -> DfsPosixError {
    let reason = conflicts
        .into_iter()
        .map(|conflict| conflict.reason)
        .next()
        .unwrap_or_else(|| "object has an open DFS conflict".to_string());
    DfsPosixError::Conflict { path, reason }
}

fn parent_posix_path(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    let (parent, _) = path.rsplit_once('/')?;
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
}

fn join_posix_path(parent: &str, child: &str) -> String {
    if parent == "/" {
        format!("/{child}")
    } else {
        format!("{}/{}", parent.trim_end_matches('/'), child)
    }
}

fn append_posix_components(path: String, components: &[String]) -> Result<String> {
    let mut current = path;
    for component in components {
        current = join_posix_path(&current, component);
    }
    normalize_posix_path(&current)
}

fn resolve_posix_symlink_target(link_path: &str, target: &str) -> Result<String> {
    validate_posix_symlink_target(target)?;
    if target.starts_with('/') {
        return normalize_posix_path(target);
    }
    let parent = parent_posix_path(link_path).unwrap_or_else(|| "/".to_string());
    normalize_posix_path(&join_posix_path(&parent, target))
}

fn set_object_create_timestamps(object: &mut DfsObjectMetadata) {
    let now = now_unix_nanos();
    object.accessed_at_unix_nanos = now;
    object.modified_at_unix_nanos = now;
    object.created_at_unix_nanos = now;
    object.changed_at_unix_nanos = now;
}

fn now_unix_nanos() -> u64 {
    time::OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(1_000_000_000)
}

fn normalize_posix_path(path: &str) -> Result<String> {
    if !path.starts_with('/') {
        return Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        });
    }
    if path.bytes().any(|byte| byte == 0) {
        return Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        });
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn normalize_posix_path_with_trailing_directory(path: &str) -> Result<(String, bool)> {
    let normalized = normalize_posix_path(path)?;
    let requires_directory = normalized != "/" && path.ends_with('/');
    Ok((normalized, requires_directory))
}

fn validate_posix_symlink_target(target: &str) -> Result<()> {
    if target.is_empty() || target.bytes().any(|byte| byte == 0) {
        return Err(DfsPosixError::InvalidPath {
            path: target.to_string(),
        });
    }
    Ok(())
}

fn namespace_transaction_id(kind: &str, object_id: &DfsObjectId, path: &str) -> String {
    format!(
        "namespace_{}_{}",
        kind,
        blake3::hash(format!("{object_id}\0{path}").as_bytes()).to_hex()
    )
}

fn hidden_alias_tombstone_path(object_id: &DfsObjectId) -> String {
    format!("/.sinomo/deleted-hardlinks/{object_id}")
}

#[allow(dead_code)]
fn _assert_filesystem_send_sync<T: Send + Sync>(_: &DfsFileSystem) {}
