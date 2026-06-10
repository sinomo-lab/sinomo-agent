//! POSIX-like DFS namespace facade.
//!
//! The facade provides path-based operations without Unix user/group permission
//! semantics. It is intended for services such as static web servers and
//! rollout and higher-level automation tools that need stable mkdir/list/stat/read/write
//! behavior over SIFS while the lower DFS layers handle placement, receipts,
//! repair, and metadata journaling.

use crate::namespace::{initial_version, metadata_path, next_version, object_id_for_path};
use crate::{
    DfsControlPlaneStore, DfsDirectoryPage, DfsDirectoryPageCursor, DfsError, DfsFileSystem,
    DfsFrameTransport, DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobRecord,
    DfsNamespaceJobStatus, DfsNamespaceTransaction, DfsObjectKind, DfsObjectMetadata, DfsReadData,
    DfsReadRangeData, DfsReplicationPolicy, NetworkedDfsRangeReadOutcome,
    NetworkedDfsRangeReadStreamOutcome, NetworkedDfsReadOutcome, NetworkedDfsReadStreamOutcome,
    NetworkedDfsService, NetworkedDfsWriteOutcome, ObjectVersionReader, sifs_file_system_id,
};
use crate::{DfsFileSystemId, DfsObjectId, DfsVersion};
use sinomo_core::{DfsObjectPolicy, DfsWriteRequest};
use sinomo_events::EventId;
use sinomo_node::NodeId;
use std::io::{Cursor, Read};
use std::time::{Duration, Instant};
use thiserror::Error;

const POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT: usize = 8;
const POSIX_WAIT_FOR_COMMIT_IDLE_SLEEP: Duration = Duration::from_millis(10);
const POSIX_WAIT_FOR_COMMIT_TIMEOUT: Duration = Duration::from_secs(5);
const POSIX_LIST_DIR_COLLECT_PAGE_SIZE: usize = 4096;
const POSIX_LINK_COUNT_PAGE_SIZE: usize = 4096;
const POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS: usize = 512;
const POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE: usize = 128;
const POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE: usize = 1024;
const POSIX_REMOTE_READ_IDLE_WAIT_INITIAL: Duration = Duration::from_millis(2);
const POSIX_REMOTE_READ_IDLE_WAIT_MAX: Duration = Duration::from_millis(50);

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
pub struct DfsDirEntryPage {
    pub entries: Vec<DfsDirEntry>,
    pub next_cursor: Option<DfsDirectoryPageCursor>,
}

#[derive(Debug)]
pub struct DfsPosixFileReadStream {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub size_bytes: u64,
    pub reader: ObjectVersionReader,
}

#[derive(Debug)]
pub struct DfsPosixFileRangeReadStream {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: Option<String>,
    pub offset: u64,
    pub total_size_bytes: u64,
    pub range_size_bytes: u64,
    pub reader: ObjectVersionReader,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsWriteOptions {
    pub policy: DfsReplicationPolicy,
    pub replica_candidates: Vec<NodeId>,
    pub wait_for_commit: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsNamespaceJobMaintenanceReport {
    pub scanned_job_count: usize,
    pub resumed_job_count: usize,
    pub completed_job_count: usize,
    pub failed_job_count: usize,
    pub remaining_running_job_count: usize,
    pub failures: Vec<DfsNamespaceJobFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsNamespaceJobFailure {
    pub job_id: String,
    pub kind: DfsNamespaceJobKind,
    pub reason: String,
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

impl<T: DfsFrameTransport> NetworkedDfsService<T> {
    fn settle_ready_write_outcome(
        &mut self,
        outcome: NetworkedDfsWriteOutcome,
    ) -> std::result::Result<NetworkedDfsWriteOutcome, DfsError> {
        let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = &outcome else {
            return Ok(outcome);
        };
        let mut intent_id = intent_id.clone();
        let started_at = Instant::now();
        let mut ready_drains = 0;
        loop {
            let events = self.process_incoming()?;
            let outcome = self.write_outcome(&intent_id)?;
            if let NetworkedDfsWriteOutcome::Pending {
                intent_id: next_intent_id,
                ..
            } = &outcome
            {
                intent_id = next_intent_id.clone();
            }
            if !matches!(outcome, NetworkedDfsWriteOutcome::Pending { .. }) {
                return Ok(outcome);
            }
            if events.is_empty() || ready_drains >= POSIX_WAIT_FOR_COMMIT_READY_DRAIN_LIMIT {
                let elapsed = started_at.elapsed();
                if elapsed >= POSIX_WAIT_FOR_COMMIT_TIMEOUT {
                    return Ok(outcome);
                }
                std::thread::sleep(
                    POSIX_WAIT_FOR_COMMIT_IDLE_SLEEP
                        .min(POSIX_WAIT_FOR_COMMIT_TIMEOUT.saturating_sub(elapsed)),
                );
                ready_drains = 0;
            } else {
                ready_drains += 1;
            }
        }
    }

    pub fn bootstrap_distributed_sifs(&mut self) -> Result<()> {
        let fallback_file_system = crate::namespace::distributed_sifs(self.node_id.clone());
        let file_system_id = fallback_file_system.file_system_id.clone();
        let file_system = if let Some(kv) = self.control_plane.as_mut() {
            DfsControlPlaneStore::new(kv.as_mut())
                .get_file_system_config(&file_system_id)
                .map_err(DfsError::from)?
                .map(DfsFileSystem::from)
                .unwrap_or(fallback_file_system)
        } else {
            fallback_file_system
        };
        let bootstrap_owner_node_id = file_system.owner_node_id.clone();
        self.add_file_system(file_system)?;
        for object in
            crate::namespace::distributed_sifs_bootstrap_objects(bootstrap_owner_node_id.clone())
        {
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
        self.posix_write_file_from_reader(path, Cursor::new(bytes), options)
    }

    pub fn posix_write_file_from_reader<R: Read>(
        &mut self,
        path: &str,
        reader: R,
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
        let target_version_for_stat = target_version.clone();
        let mut candidates = options.replica_candidates;
        if !candidates.contains(&self.node_id) {
            candidates.insert(0, self.node_id.clone());
        }
        let committed_locations = candidates.clone();
        let outcome = self.write_distributed_eventual_from_reader_with_candidates(
            DfsWriteRequest {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                target_version,
                requesting_node_id: self.node_id.clone(),
            },
            reader,
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
        let outcome = if options.wait_for_commit {
            match self.settle_ready_write_outcome(outcome) {
                Ok(outcome) => outcome,
                Err(error) => {
                    if transient_create {
                        self.remove_transient_object_metadata(&object.object_id);
                    }
                    return Err(error.into());
                }
            }
        } else {
            outcome
        };
        match outcome {
            NetworkedDfsWriteOutcome::Committed {
                version,
                content_hash,
                ..
            } => {
                let mut committed_object = object.clone();
                committed_object.version = version;
                committed_object.content_hash = Some(content_hash);
                committed_object.current_locations = committed_locations;
                committed_object.content_size_bytes =
                    self.object_content_size_hint(&committed_object)?;
                let now = crate::dfs_now_unix_nanos();
                committed_object.modified_at_unix_nanos = now;
                committed_object.changed_at_unix_nanos = now;
                committed_object.last_modified_by_event_id = Some(EventId::generate());
                self.add_transient_object_metadata(committed_object.clone())?;
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
                if let Ok(manifest) = self
                    .content
                    .read_manifest(object.object_id.as_str(), target_version_for_stat.as_str())
                {
                    stat.size_bytes = Some(manifest.size_bytes);
                }
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

    pub fn posix_write_file_at_from_reader<R: Read>(
        &mut self,
        path: &str,
        offset: u64,
        mut reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsStat> {
        let mut first_byte = [0_u8; 1];
        let first_len = reader
            .read(&mut first_byte)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to read POSIX write_at stream for {path}: {source}"),
            })?;
        if first_len == 0 {
            return self.posix_write_file_at(path, offset, &[], options);
        }
        let mut reader = Cursor::new(first_byte[..first_len].to_vec()).chain(reader);
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
        let target_version = next_version(&object.version);
        let mut candidates = options.replica_candidates.clone();
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
        if object
            .content_size_bytes
            .is_some_and(|current_size| offset <= current_size)
            && let Some(outcome) = self.write_distributed_eventual_patch_from_reader_with_base(
                request,
                base_version,
                offset,
                &mut reader,
                plan.desired_replica_nodes,
                options.policy.clone(),
            )?
        {
            let outcome = if options.wait_for_commit {
                self.settle_ready_write_outcome(outcome)?
            } else {
                outcome
            };
            return match outcome {
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
                    stat.size_bytes = object.content_size_bytes;
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
            };
        }

        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|source| DfsError::ContentStore {
                message: format!("failed to buffer POSIX write_at fallback for {path}: {source}"),
            })?;
        self.posix_write_file_at(&path, offset, &bytes, options)
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
        let target_version = next_version(&object.version);
        let mut candidates = options.replica_candidates.clone();
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
        let patch_end =
            offset
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| DfsPosixError::InvalidPath {
                    path: format!("{path}@{offset}+{}", bytes.len()),
                })?;
        if object
            .content_size_bytes
            .is_some_and(|current_size| offset <= current_size)
            && let Some(outcome) = self.write_distributed_eventual_patch_with_base(
                request.clone(),
                base_version.clone(),
                offset,
                bytes,
                plan.desired_replica_nodes.clone(),
                options.policy.clone(),
            )?
        {
            let outcome = if options.wait_for_commit {
                self.settle_ready_write_outcome(outcome)?
            } else {
                outcome
            };
            match outcome {
                NetworkedDfsWriteOutcome::Committed { .. } => {
                    let object = self.stat_target_for_path(&path)?;
                    return self.stat_object_for_path(&object, &path);
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } if !options.wait_for_commit => {
                    let object = self.stat_target_for_path(&path)?;
                    let mut stat = self.stat_object_for_path(&object, &path)?;
                    stat.size_bytes = object.content_size_bytes.map(|size| size.max(patch_end));
                    let _ = intent_id;
                    let _ = durable_receipts;
                    let _ = required_receipts;
                    return Ok(stat);
                }
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } => {
                    return Err(DfsPosixError::PendingWrite {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                    });
                }
                NetworkedDfsWriteOutcome::Conflict { conflict } => {
                    return Err(DfsPosixError::Conflict {
                        path,
                        reason: conflict.reason,
                    });
                }
            }
        }
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
        let outcome = self.write_distributed_eventual_with_base(
            request,
            Some(base_version),
            current,
            plan.desired_replica_nodes,
            options.policy,
        )?;
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome)?
        } else {
            outcome
        };
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
        let outcome = if options.wait_for_commit {
            self.settle_ready_write_outcome(outcome)?
        } else {
            outcome
        };
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

    pub fn posix_read_file_range(
        &self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
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
        match self.read_local_object_range_with_state(
            &object.object_id,
            offset,
            max_bytes,
            false,
        )? {
            NetworkedDfsRangeReadOutcome::Found {
                data: DfsReadRangeData { bytes, .. },
                ..
            } => Ok(bytes),
            NetworkedDfsRangeReadOutcome::PendingReplication { .. }
            | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
            | NetworkedDfsRangeReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsRangeReadOutcome::Conflicted { conflicts, .. } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_open_file_stream(&self, path: &str) -> Result<DfsPosixFileReadStream> {
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
        match self.open_local_current_object_stream_with_state(&object.object_id)? {
            NetworkedDfsReadStreamOutcome::Found(stream) => Ok(DfsPosixFileReadStream {
                object_id: stream.data.object_id,
                version: stream.data.version,
                content_hash: stream.data.content_hash,
                size_bytes: stream.total_size_bytes,
                reader: stream.reader,
            }),
            NetworkedDfsReadStreamOutcome::PendingReplication
            | NetworkedDfsReadStreamOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsReadStreamOutcome::Conflicted { conflicts } => {
                Err(read_conflict_error(path, conflicts))
            }
        }
    }

    pub fn posix_open_file_range_stream(
        &self,
        path: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<DfsPosixFileRangeReadStream> {
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
        match self.open_local_current_object_range_stream_with_state(
            &object.object_id,
            offset,
            max_bytes,
            0,
        )? {
            NetworkedDfsRangeReadStreamOutcome::Found(stream) => Ok(DfsPosixFileRangeReadStream {
                object_id: stream.data.object_id,
                version: stream.data.version,
                content_hash: stream.data.content_hash,
                offset: stream.data.offset,
                total_size_bytes: stream.data.total_size_bytes,
                range_size_bytes: stream.range_size_bytes,
                reader: stream.reader,
            }),
            NetworkedDfsRangeReadStreamOutcome::PendingReplication
            | NetworkedDfsRangeReadStreamOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
            NetworkedDfsRangeReadStreamOutcome::Conflicted { conflicts } => {
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
        let mut idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
        loop {
            let events = self.process_incoming()?;
            if !events.is_empty() {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            }
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
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(current);
            }
            let remaining = deadline.saturating_duration_since(now);
            let until_retry = self
                .next_pending_remote_read_retry_for_current_object(object_id)?
                .map(|next_retry_at| next_retry_at.saturating_duration_since(now))
                .unwrap_or(remaining);
            let wait = idle_wait.min(remaining).min(until_retry);
            if wait.is_zero() {
                continue;
            }
            if self
                .transport
                .wait_for_incoming(wait)
                .map_err(DfsError::from_network)?
            {
                idle_wait = POSIX_REMOTE_READ_IDLE_WAIT_INITIAL;
            } else {
                idle_wait = idle_wait
                    .saturating_mul(2)
                    .min(POSIX_REMOTE_READ_IDLE_WAIT_MAX);
            }
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
        let outcome = if options.wait_for_commit {
            match self.settle_ready_write_outcome(outcome) {
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
        if self.sifs_dir_has_children(&path)? {
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
        let mut job =
            self.posix_clear_dir_recursive_bounded(path, POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS)?;
        while job.status != DfsNamespaceJobStatus::Completed {
            job = self.posix_resume_namespace_job_bounded(
                &job.job_id,
                POSIX_RECURSIVE_CLEAR_JOB_BATCH_STEPS,
            )?;
        }
        Ok(())
    }

    pub fn posix_clear_dir_recursive_bounded(
        &mut self,
        path: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let path = normalize_posix_path(path)?;
        let path = self.resolve_posix_path_distributed(&path, true)?;
        let job = self.clear_dir_recursive_job_for_path(&path)?;
        self.persist_namespace_job(&job)?;
        self.run_clear_dir_recursive_job_batch(job, max_steps)
    }

    pub fn posix_resume_namespace_job_bounded(
        &mut self,
        job_id: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        let Some(job) = self.namespace_job(job_id)? else {
            return Err(DfsPosixError::NotFound {
                path: job_id.to_string(),
            });
        };
        if job.status != DfsNamespaceJobStatus::Running {
            return Ok(job);
        }
        match job.kind {
            DfsNamespaceJobKind::ClearDirectoryRecursive => {
                self.run_clear_dir_recursive_job_batch(job, max_steps)
            }
            DfsNamespaceJobKind::RenameDirectory => {
                self.run_rename_directory_job_batch(job, max_steps)
            }
        }
    }

    pub fn posix_resume_namespace_jobs_bounded(
        &mut self,
        max_jobs: usize,
        max_steps_per_job: usize,
    ) -> Result<DfsNamespaceJobMaintenanceReport> {
        if self.metadata_backend.is_none() {
            return Ok(DfsNamespaceJobMaintenanceReport::default());
        }
        let mut cursor = None;
        let mut report = DfsNamespaceJobMaintenanceReport::default();
        loop {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .list_namespace_jobs_page(
                    cursor.as_ref(),
                    POSIX_NAMESPACE_JOB_MAINTENANCE_PAGE_SIZE,
                )?;
            report.scanned_job_count = report.scanned_job_count.saturating_add(page.jobs.len());
            for job in page.jobs {
                if job.status != DfsNamespaceJobStatus::Running {
                    continue;
                }
                if report.resumed_job_count >= max_jobs {
                    report.remaining_running_job_count =
                        report.remaining_running_job_count.saturating_add(1);
                    continue;
                }
                report.resumed_job_count = report.resumed_job_count.saturating_add(1);
                match self.posix_resume_namespace_job_bounded(&job.job_id, max_steps_per_job) {
                    Ok(updated) => match updated.status {
                        DfsNamespaceJobStatus::Completed => {
                            report.completed_job_count =
                                report.completed_job_count.saturating_add(1);
                        }
                        DfsNamespaceJobStatus::Running => {
                            report.remaining_running_job_count =
                                report.remaining_running_job_count.saturating_add(1);
                        }
                        DfsNamespaceJobStatus::Failed => {
                            report.failed_job_count = report.failed_job_count.saturating_add(1);
                        }
                    },
                    Err(error) => {
                        let reason = error.to_string();
                        let failed = self.fail_namespace_job(job, reason.clone())?;
                        report.failed_job_count = report.failed_job_count.saturating_add(1);
                        report.failures.push(DfsNamespaceJobFailure {
                            job_id: failed.job_id,
                            kind: failed.kind,
                            reason,
                        });
                    }
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(report)
    }

    pub fn posix_rename_directory_bounded(
        &mut self,
        from: &str,
        to: &str,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
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
            let source = self
                .lookup_sifs_path(&from)?
                .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
            if source.kind != DfsObjectKind::Directory {
                return Err(DfsPosixError::NotDirectory { path: from });
            }
            return self.rename_directory_job_for_paths(&from, &to, &source);
        }
        let source = self
            .lookup_sifs_path(&from)?
            .ok_or_else(|| DfsPosixError::NotFound { path: from.clone() })?;
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path: from });
        }
        if source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path: from });
        }
        let _ = to_requires_directory;
        self.posix_rename_directory_bounded_resolved(&from, &to, source, max_steps)
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
            versions: Vec::new(),
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
        self.posix_collect_resolved_dir(&path)
    }

    pub fn posix_list_dir_distributed(&mut self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_collect_resolved_dir(&path)
    }

    pub fn posix_list_dir_page(
        &self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        let path = self.resolve_existing_posix_path(path, true)?;
        self.posix_list_resolved_dir_page(&path, after, max_entries)
    }

    pub fn posix_list_dir_distributed_page(
        &mut self,
        path: &str,
        after: Option<&DfsDirectoryPageCursor>,
        max_entries: usize,
    ) -> Result<DfsDirEntryPage> {
        let path = self.resolve_existing_posix_path_distributed(path, true)?;
        self.posix_list_resolved_dir_page(&path, after, max_entries)
    }

    fn posix_collect_resolved_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.posix_list_resolved_dir_page(
                path,
                cursor.as_ref(),
                POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
            )?;
            entries.extend(page.entries);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
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
        let memory_object = self
            .metadata
            .object_by_normalized_posix_path(file_system_id, path)
            .cloned();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let backend_object =
                backend.get_object_by_path(file_system_id, &metadata_path(path))?;
            return Ok(match (memory_object, backend_object) {
                (Some(memory_object), Some(backend_object))
                    if memory_object.version > backend_object.version =>
                {
                    Some(memory_object)
                }
                (Some(memory_object), None) => Some(memory_object),
                (_, backend_object) => backend_object,
            });
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
            let mut children = Vec::new();
            let mut cursor = None;
            loop {
                let page = backend.child_objects_for_directory_page(
                    &file_system_id,
                    path,
                    cursor.as_ref(),
                    POSIX_LIST_DIR_COLLECT_PAGE_SIZE,
                )?;
                children.extend(page.objects);
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
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

    fn child_objects_for_sifs_dir_page(
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

    fn sifs_dir_has_children(&self, path: &str) -> Result<bool> {
        Ok(!self
            .child_objects_for_sifs_dir_page(path, None, 1)?
            .objects
            .is_empty())
    }

    fn hardlink_count_to_object(&self, target_object_id: &DfsObjectId) -> Result<usize> {
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

    fn first_hardlink_to_object(
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
        let mut cursor = None;
        loop {
            let page = self.child_objects_for_sifs_dir_page(
                path,
                cursor.as_ref(),
                POSIX_LINK_COUNT_PAGE_SIZE,
            )?;
            count = count.saturating_add(
                page.objects
                    .iter()
                    .filter(|child| child.kind == DfsObjectKind::Directory)
                    .count() as u64,
            );
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(count)
    }

    fn clear_dir_recursive_job_for_path(&self, path: &str) -> Result<DfsNamespaceJobRecord> {
        let directory = self
            .lookup_sifs_path(path)?
            .ok_or_else(|| DfsPosixError::NotFound {
                path: path.to_string(),
            })?;
        if directory.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: path.to_string(),
            });
        }
        if directory.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: path.to_string(),
            });
        }
        let job_id = namespace_job_id(
            "clear-dir-recursive",
            &directory.file_system_id,
            path,
            &directory.object_id,
            &directory.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return Ok(job);
        }
        let now = time::OffsetDateTime::now_utc();
        Ok(DfsNamespaceJobRecord {
            job_id,
            file_system_id: directory.file_system_id,
            kind: DfsNamespaceJobKind::ClearDirectoryRecursive,
            status: DfsNamespaceJobStatus::Running,
            root_path: path.to_string(),
            target_path: None,
            pending_frames: vec![DfsNamespaceJobFrame::ScanDirectory {
                path: path.to_string(),
                cursor: None,
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        })
    }

    fn namespace_job(&self, job_id: &str) -> Result<Option<DfsNamespaceJobRecord>> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        Ok(backend.get_namespace_job(job_id)?)
    }

    fn persist_namespace_job(&mut self, job: &DfsNamespaceJobRecord) -> Result<()> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_namespace_job(job)?;
        }
        Ok(())
    }

    fn fail_namespace_job(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        reason: String,
    ) -> Result<DfsNamespaceJobRecord> {
        job.status = DfsNamespaceJobStatus::Failed;
        job.failure_reason = Some(reason);
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    fn run_clear_dir_recursive_job_batch(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        if job.status == DfsNamespaceJobStatus::Completed {
            return Ok(job);
        }
        if max_steps == 0 {
            job.updated_at = time::OffsetDateTime::now_utc();
            self.persist_namespace_job(&job)?;
            return Ok(job);
        }
        let root_path = normalize_posix_path(&job.root_path)?;
        let prefix = subtree_prefix(&root_path);
        let mut steps = 0usize;
        while steps < max_steps {
            let Some(frame) = job.pending_frames.pop() else {
                self.touch_directory_modified(&root_path)?;
                let now = time::OffsetDateTime::now_utc();
                job.status = DfsNamespaceJobStatus::Completed;
                job.updated_at = now;
                job.completed_at = Some(now);
                self.persist_namespace_job(&job)?;
                return Ok(job);
            };
            match frame {
                DfsNamespaceJobFrame::ScanDirectory { path, cursor } => {
                    let page_size = (max_steps - steps)
                        .max(1)
                        .min(POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE);
                    let page =
                        self.child_objects_for_sifs_dir_page(&path, cursor.as_ref(), page_size)?;
                    let mut child_dirs = Vec::new();
                    for object in page.objects {
                        steps = steps.saturating_add(1);
                        let object_path = normalize_posix_path(&object.path)?;
                        let listed_object_id = object.object_id.clone();
                        let Some(visible_object) = self.lookup_sifs_path(&object_path)? else {
                            continue;
                        };
                        if visible_object.kind == DfsObjectKind::Tombstone
                            || visible_object.object_id != listed_object_id
                        {
                            continue;
                        }
                        if visible_object.kind == DfsObjectKind::Directory {
                            child_dirs.push(object_path);
                            continue;
                        }
                        if self.clear_visible_object_for_recursive_job(&object_path, &prefix)? {
                            job.processed_object_count =
                                job.processed_object_count.saturating_add(1);
                        }
                    }
                    if let Some(next_cursor) = page.next_cursor {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::ScanDirectory {
                                path: path.clone(),
                                cursor: Some(next_cursor),
                            });
                    } else if path != root_path {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::CompleteDirectory { path });
                    }
                    for child_path in child_dirs.into_iter().rev() {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::ScanDirectory {
                                path: child_path,
                                cursor: None,
                            });
                    }
                }
                DfsNamespaceJobFrame::CompleteDirectory { path } => {
                    steps = steps.saturating_add(1);
                    if path != root_path
                        && self.clear_visible_object_for_recursive_job(&path, &prefix)?
                    {
                        job.processed_object_count = job.processed_object_count.saturating_add(1);
                    }
                }
                DfsNamespaceJobFrame::RenameDirectoryStart { .. }
                | DfsNamespaceJobFrame::RenameDirectoryScan { .. }
                | DfsNamespaceJobFrame::RenameDirectoryComplete { .. } => {
                    return Err(DfsPosixError::InvalidPath {
                        path: job.job_id.clone(),
                    });
                }
            }
        }
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    fn clear_visible_object_for_recursive_job(
        &mut self,
        object_path: &str,
        prefix: &str,
    ) -> Result<bool> {
        for _ in 0..64 {
            let Some(object) = self.lookup_sifs_path(object_path)? else {
                return Ok(false);
            };
            if object.kind == DfsObjectKind::Tombstone {
                return Ok(false);
            }
            if !matches!(
                object.kind,
                DfsObjectKind::Hardlink | DfsObjectKind::Directory | DfsObjectKind::Tombstone
            ) && self.link_count_for_object(&object)? > 1
                && let Some(alias) = self.first_hardlink_alias_outside_prefix(&object, prefix)?
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
                    versions: Vec::new(),
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
            return Ok(true);
        }
        Ok(false)
    }

    fn posix_rename_directory_bounded_resolved(
        &mut self,
        from: &str,
        to: &str,
        source: DfsObjectMetadata,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        self.ensure_parent_directory(to)?;
        let child_prefix = subtree_prefix(from);
        if to.starts_with(&child_prefix) {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{from} -> {to}"),
            });
        }
        self.validate_directory_rename_destination(from, to, &source)?;
        let job = self.rename_directory_job_for_paths(from, to, &source)?;
        self.persist_namespace_job(&job)?;
        self.run_rename_directory_job_batch(job, max_steps)
    }

    fn validate_directory_rename_destination(
        &self,
        from: &str,
        to: &str,
        source: &DfsObjectMetadata,
    ) -> Result<()> {
        if source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: from.to_string(),
            });
        }
        if let Some(destination) = self.lookup_sifs_path(to)?
            && destination.kind != DfsObjectKind::Tombstone
        {
            if destination.object_id == source.object_id {
                return Ok(());
            }
            match destination.kind {
                DfsObjectKind::Directory => {
                    if self.sifs_dir_has_children(to)? {
                        return Err(DfsPosixError::DirectoryNotEmpty {
                            path: to.to_string(),
                        });
                    }
                }
                _ => {
                    return Err(DfsPosixError::NotDirectory {
                        path: to.to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn rename_directory_job_for_paths(
        &self,
        from: &str,
        to: &str,
        source: &DfsObjectMetadata,
    ) -> Result<DfsNamespaceJobRecord> {
        let job_id = namespace_job_id(
            "rename-directory",
            &source.file_system_id,
            &format!("{from}->{to}"),
            &source.object_id,
            &source.version,
        );
        if let Some(job) = self.namespace_job(&job_id)? {
            return Ok(job);
        }
        let now = time::OffsetDateTime::now_utc();
        Ok(DfsNamespaceJobRecord {
            job_id,
            file_system_id: source.file_system_id.clone(),
            kind: DfsNamespaceJobKind::RenameDirectory,
            status: DfsNamespaceJobStatus::Running,
            root_path: from.to_string(),
            target_path: Some(to.to_string()),
            pending_frames: vec![DfsNamespaceJobFrame::RenameDirectoryStart {
                source_object_id: source.object_id.clone(),
            }],
            processed_object_count: 0,
            created_at: now,
            updated_at: now,
            completed_at: None,
            failure_reason: None,
        })
    }

    fn run_rename_directory_job_batch(
        &mut self,
        mut job: DfsNamespaceJobRecord,
        max_steps: usize,
    ) -> Result<DfsNamespaceJobRecord> {
        if job.status == DfsNamespaceJobStatus::Completed {
            return Ok(job);
        }
        if max_steps == 0 {
            job.updated_at = time::OffsetDateTime::now_utc();
            self.persist_namespace_job(&job)?;
            return Ok(job);
        }
        let source_root = normalize_posix_path(&job.root_path)?;
        let target_root = job
            .target_path
            .as_ref()
            .ok_or_else(|| DfsPosixError::InvalidPath {
                path: job.job_id.clone(),
            })
            .and_then(|path| normalize_posix_path(path))?;
        let source_prefix = subtree_prefix(&source_root);
        let target_prefix = subtree_prefix(&target_root);
        let mut steps = 0usize;
        while steps < max_steps {
            let Some(frame) = job.pending_frames.pop() else {
                self.touch_parent_directory_modified(&source_root)?;
                if parent_posix_path(&source_root) != parent_posix_path(&target_root) {
                    self.touch_parent_directory_modified(&target_root)?;
                }
                let now = time::OffsetDateTime::now_utc();
                job.status = DfsNamespaceJobStatus::Completed;
                job.updated_at = now;
                job.completed_at = Some(now);
                self.persist_namespace_job(&job)?;
                return Ok(job);
            };
            match frame {
                DfsNamespaceJobFrame::RenameDirectoryStart { source_object_id } => {
                    steps = steps.saturating_add(1);
                    self.start_rename_directory_job(&source_object_id, &source_root, &target_root)?;
                    job.pending_frames
                        .push(DfsNamespaceJobFrame::RenameDirectoryScan {
                            source_object_id,
                            source_path: source_root.clone(),
                            target_path: target_root.clone(),
                            cursor: None,
                        });
                    job.processed_object_count = job.processed_object_count.saturating_add(1);
                }
                DfsNamespaceJobFrame::RenameDirectoryScan {
                    source_object_id,
                    source_path,
                    target_path,
                    cursor,
                } => {
                    let page_size = (max_steps - steps)
                        .max(1)
                        .min(POSIX_RECURSIVE_CLEAR_JOB_PAGE_SIZE);
                    let page = self.child_objects_for_sifs_dir_page(
                        &source_path,
                        cursor.as_ref(),
                        page_size,
                    )?;
                    let mut child_dirs = Vec::new();
                    for object in page.objects {
                        steps = steps.saturating_add(1);
                        let object_path = normalize_posix_path(&object.path)?;
                        if !object_path.starts_with(&source_prefix) {
                            continue;
                        }
                        let suffix = object_path.trim_start_matches(&source_prefix);
                        let target_child_path = format!("{target_prefix}{suffix}");
                        let Some(current) = self.object_by_id(&object.object_id)? else {
                            continue;
                        };
                        if current.kind == DfsObjectKind::Tombstone {
                            continue;
                        }
                        let current_path = normalize_posix_path(&current.path)?;
                        if current.kind == DfsObjectKind::Directory {
                            child_dirs.push((
                                current.object_id.clone(),
                                object_path.clone(),
                                target_child_path.clone(),
                            ));
                            continue;
                        }
                        if current_path == target_child_path {
                            continue;
                        }
                        if current_path != object_path {
                            continue;
                        }
                        if self.move_object_for_rename_job(&current, &target_child_path)? {
                            job.processed_object_count =
                                job.processed_object_count.saturating_add(1);
                        }
                    }
                    if let Some(next_cursor) = page.next_cursor {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::RenameDirectoryScan {
                                source_object_id,
                                source_path: source_path.clone(),
                                target_path: target_path.clone(),
                                cursor: Some(next_cursor),
                            });
                    } else if source_path != source_root {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::RenameDirectoryComplete {
                                source_object_id,
                                source_path: source_path.clone(),
                                target_path: target_path.clone(),
                            });
                    }
                    for (child_object_id, child_source_path, child_target_path) in
                        child_dirs.into_iter().rev()
                    {
                        job.pending_frames
                            .push(DfsNamespaceJobFrame::RenameDirectoryScan {
                                source_object_id: child_object_id,
                                source_path: child_source_path,
                                target_path: child_target_path,
                                cursor: None,
                            });
                    }
                }
                DfsNamespaceJobFrame::RenameDirectoryComplete {
                    source_object_id,
                    source_path,
                    target_path,
                } => {
                    steps = steps.saturating_add(1);
                    if self.complete_rename_directory_job(
                        &source_object_id,
                        &source_path,
                        &target_path,
                    )? {
                        job.processed_object_count = job.processed_object_count.saturating_add(1);
                    }
                }
                DfsNamespaceJobFrame::ScanDirectory { .. }
                | DfsNamespaceJobFrame::CompleteDirectory { .. } => {
                    return Err(DfsPosixError::InvalidPath {
                        path: job.job_id.clone(),
                    });
                }
            }
        }
        job.updated_at = time::OffsetDateTime::now_utc();
        self.persist_namespace_job(&job)?;
        Ok(job)
    }

    fn start_rename_directory_job(
        &mut self,
        source_object_id: &DfsObjectId,
        source_root: &str,
        target_root: &str,
    ) -> Result<()> {
        let Some(mut source) = self.object_by_id(source_object_id)? else {
            return Err(DfsPosixError::NotFound {
                path: source_root.to_string(),
            });
        };
        if source.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound {
                path: source_root.to_string(),
            });
        }
        if source.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: source_root.to_string(),
            });
        }
        let source_path = normalize_posix_path(&source.path)?;
        if source_path == target_root {
            return Ok(());
        }
        if source_path != source_root {
            return Err(DfsPosixError::InvalidPath {
                path: format!("{source_path} -> {target_root}"),
            });
        }

        let mut transaction_objects = Vec::new();
        if let Some(mut destination) = self.lookup_sifs_path(target_root)?
            && destination.kind != DfsObjectKind::Tombstone
            && destination.object_id != source.object_id
        {
            if destination.kind != DfsObjectKind::Directory {
                return Err(DfsPosixError::NotDirectory {
                    path: target_root.to_string(),
                });
            }
            if self.sifs_dir_has_children(target_root)? {
                return Err(DfsPosixError::DirectoryNotEmpty {
                    path: target_root.to_string(),
                });
            }
            destination.kind = DfsObjectKind::Tombstone;
            destination.version = next_version(&destination.version);
            destination.content_hash = None;
            destination.content_size_bytes = None;
            destination.link_target_object_id = None;
            destination.current_locations = vec![self.node_id.clone()];
            transaction_objects.push(destination);
        }

        let mut old_path_tombstone = source.clone();
        old_path_tombstone.object_id = DfsObjectId::generate();
        old_path_tombstone.path = source_root.to_string();
        old_path_tombstone.kind = DfsObjectKind::Tombstone;
        old_path_tombstone.version = next_version(&source.version);
        old_path_tombstone.content_hash = None;
        old_path_tombstone.content_size_bytes = None;
        old_path_tombstone.link_target_object_id = None;
        old_path_tombstone.current_locations = vec![self.node_id.clone()];

        source.path = target_root.to_string();
        source.changed_at_unix_nanos = now_unix_nanos();
        source.version = next_version(&source.version);
        transaction_objects.insert(0, old_path_tombstone);
        transaction_objects.insert(1, source.clone());
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id("rename", &source.object_id, &source.path),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            versions: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        Ok(())
    }

    fn complete_rename_directory_job(
        &mut self,
        source_object_id: &DfsObjectId,
        source_path: &str,
        target_path: &str,
    ) -> Result<bool> {
        let Some(current) = self.object_by_id(source_object_id)? else {
            return Ok(false);
        };
        if current.kind == DfsObjectKind::Tombstone {
            return Ok(false);
        }
        if current.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory {
                path: source_path.to_string(),
            });
        }
        self.move_object_for_rename_job(&current, target_path)
    }

    fn move_object_for_rename_job(
        &mut self,
        object: &DfsObjectMetadata,
        target_path: &str,
    ) -> Result<bool> {
        let current_path = normalize_posix_path(&object.path)?;
        if current_path == target_path {
            return Ok(false);
        }
        let mut moved = object.clone();
        moved.path = target_path.to_string();
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id("rename-child", &moved.object_id, &moved.path),
            file_system_id: moved.file_system_id.clone(),
            objects: vec![moved],
            versions: Vec::new(),
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        Ok(true)
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
            let count = self.hardlink_count_to_object(&object.object_id)?;
            return Ok(1u64.saturating_add(count as u64));
        }
        let path = normalize_posix_path(&object.path)?;
        let child_directory_count = self.child_directory_count_for_sifs_dir(&path)?;
        Ok(2u64.saturating_add(child_directory_count))
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
            versions: Vec::new(),
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

/// Returns the canonical POSIX path key used to index objects by path, or
/// `None` when the path cannot be normalized. This must match the normalization
/// applied when looking objects up by path so the index and lookups agree.
pub(crate) fn normalized_posix_path_key(path: &str) -> Option<String> {
    normalize_posix_path(path).ok()
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

fn namespace_job_id(
    kind: &str,
    file_system_id: &DfsFileSystemId,
    path: &str,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> String {
    format!(
        "namespace_job_{}_{}",
        kind,
        blake3::hash(format!("{file_system_id}\0{path}\0{object_id}\0{version}").as_bytes())
            .to_hex()
    )
}

fn subtree_prefix(path: &str) -> String {
    if path == "/" {
        "/".to_string()
    } else {
        format!("{}/", path.trim_end_matches('/'))
    }
}

fn posix_path_is_at_or_under_prefix(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn hidden_alias_tombstone_path(object_id: &DfsObjectId) -> String {
    format!("/.sinomo/deleted-hardlinks/{object_id}")
}

#[allow(dead_code)]
fn _assert_filesystem_send_sync<T: Send + Sync>(_: &DfsFileSystem) {}
