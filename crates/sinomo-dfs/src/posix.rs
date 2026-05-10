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
        if self.lookup_sifs_path(&path)?.is_some() {
            return Err(DfsPosixError::AlreadyExists { path });
        }
        self.ensure_parent_directory(&path)?;
        let object = self.metadata_object_for_new_path(&path, DfsObjectKind::Directory);
        self.add_object_metadata(object.clone())?;
        self.stat_object(&object)
    }

    pub fn posix_mkdir_all(&mut self, path: &str) -> Result<()> {
        let path = normalize_posix_path(path)?;
        if path == "/" {
            return Ok(());
        }
        let mut current = String::new();
        for part in path.trim_matches('/').split('/') {
            current.push('/');
            current.push_str(part);
            match self.lookup_sifs_path(&current)? {
                Some(object) if object.kind == DfsObjectKind::Directory => {}
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
        let path = normalize_posix_path(path)?;
        self.ensure_parent_directory(&path)?;
        let mut transient_create = false;
        let object = match self.lookup_sifs_path(&path)? {
            Some(object) if object.kind == DfsObjectKind::Directory => {
                return Err(DfsPosixError::IsDirectory { path });
            }
            Some(object) if object.kind == DfsObjectKind::Tombstone => {
                let mut object = object;
                object.kind = DfsObjectKind::File;
                object.version = initial_version();
                object.content_hash = None;
                object.current_locations = vec![self.node_id.clone()];
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
                let object = self
                    .lookup_sifs_path(&path)?
                    .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                self.stat_object(&object)
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
                let object = self
                    .lookup_sifs_path(&path)?
                    .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
                let mut stat = self.stat_object(&object)?;
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

    pub fn posix_read_file(&self, path: &str) -> Result<Vec<u8>> {
        let path = normalize_posix_path(path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
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
            | NetworkedDfsReadOutcome::Conflicted { .. }
            | NetworkedDfsReadOutcome::NotFound => Err(DfsPosixError::NotFound { path }),
        }
    }

    pub fn posix_unlink(&mut self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let mut object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Directory {
            return Err(DfsPosixError::IsDirectory { path });
        }
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.content_hash = None;
        object.current_locations = vec![self.node_id.clone()];
        self.add_object_metadata(object.clone())?;
        self.stat_object(&object)
    }

    pub fn posix_rmdir(&mut self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
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
            return Err(DfsPosixError::AlreadyExists { path });
        }
        object.kind = DfsObjectKind::Tombstone;
        object.version = next_version(&object.version);
        object.current_locations = vec![self.node_id.clone()];
        self.add_object_metadata(object.clone())?;
        self.stat_object(&object)
    }

    pub fn posix_rename(&mut self, from: &str, to: &str) -> Result<DfsStat> {
        let from = normalize_posix_path(from)?;
        let to = normalize_posix_path(to)?;
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
        self.ensure_parent_directory(&to)?;
        if source.kind == DfsObjectKind::Directory {
            let child_prefix = format!("{}/", from.trim_end_matches('/'));
            if to.starts_with(&child_prefix) {
                return Err(DfsPosixError::InvalidPath {
                    path: format!("{from} -> {to}"),
                });
            }
        }

        let old_prefix = format!("{}/", from.trim_end_matches('/'));
        let new_prefix = format!("{}/", to.trim_end_matches('/'));
        let mut moved_children = Vec::new();

        if let Some(mut destination) = self.lookup_sifs_path(&to)? {
            if destination.kind != DfsObjectKind::Tombstone {
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
                destination.kind = DfsObjectKind::Tombstone;
                destination.version = next_version(&destination.version);
                destination.content_hash = None;
                destination.current_locations = vec![self.node_id.clone()];
                moved_children.push(destination);
            }
        }

        if source.kind == DfsObjectKind::Directory {
            for mut object in self.objects_for_sifs()? {
                let object_path = normalize_posix_path(&object.path)?;
                if object.object_id == source.object_id
                    || object.kind == DfsObjectKind::Tombstone
                    || !object_path.starts_with(&old_prefix)
                {
                    continue;
                }
                let suffix = object_path.trim_start_matches(&old_prefix);
                object.path = format!("{new_prefix}{suffix}");
                moved_children.push(object);
            }
        }

        source.path = to;
        let mut transaction_objects = vec![source.clone()];
        transaction_objects.extend(moved_children);
        self.apply_namespace_transaction(DfsNamespaceTransaction {
            transaction_id: namespace_transaction_id("rename", &source.object_id, &source.path),
            file_system_id: source.file_system_id.clone(),
            objects: transaction_objects,
            created_at: time::OffsetDateTime::now_utc(),
        })?;
        self.stat_object(&source)
    }

    pub fn posix_stat(&self, path: &str) -> Result<DfsStat> {
        let path = normalize_posix_path(path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        self.stat_object(&object)
    }

    pub fn posix_list_dir(&self, path: &str) -> Result<Vec<DfsDirEntry>> {
        let path = normalize_posix_path(path)?;
        let object = self
            .lookup_sifs_path(&path)?
            .ok_or_else(|| DfsPosixError::NotFound { path: path.clone() })?;
        if object.kind == DfsObjectKind::Tombstone {
            return Err(DfsPosixError::NotFound { path });
        }
        if object.kind != DfsObjectKind::Directory {
            return Err(DfsPosixError::NotDirectory { path });
        }
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        let mut entries = Vec::new();
        for object in self.objects_for_sifs()? {
            let object_path = normalize_posix_path(&object.path)?;
            if object.kind == DfsObjectKind::Tombstone {
                continue;
            }
            if object_path == path || !object_path.starts_with(&prefix) {
                continue;
            }
            let rest = object_path.trim_start_matches(&prefix);
            if rest.is_empty() || rest.contains('/') {
                continue;
            }
            entries.push(DfsDirEntry {
                name: rest.to_string(),
                path: object_path,
                kind: object.kind,
                object_id: object.object_id,
            });
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(entries)
    }

    fn lookup_sifs_path(&self, path: &str) -> Result<Option<DfsObjectMetadata>> {
        self.lookup_metadata_path(&sifs_file_system_id(), path)
    }

    fn lookup_metadata_path(
        &self,
        file_system_id: &DfsFileSystemId,
        path: &str,
    ) -> Result<Option<DfsObjectMetadata>> {
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.get_object_by_path(file_system_id, &metadata_path(path))?);
        }
        Ok(self
            .metadata
            .objects
            .values()
            .find(|object| {
                &object.file_system_id == file_system_id
                    && normalize_posix_path(&object.path)
                        .map(|normalized| normalized == path)
                        .unwrap_or(false)
            })
            .cloned())
    }

    fn objects_for_sifs(&self) -> Result<Vec<DfsObjectMetadata>> {
        let file_system_id = sifs_file_system_id();
        if let Some(backend) = self.metadata_backend.as_ref() {
            return Ok(backend.objects_for_file_system(&file_system_id)?);
        }
        Ok(self
            .metadata
            .objects
            .values()
            .filter(|object| object.file_system_id == file_system_id)
            .cloned()
            .collect())
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

    fn metadata_object_for_new_path(&self, path: &str, kind: DfsObjectKind) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: object_id_for_path(path),
            file_system_id: sifs_file_system_id(),
            path: path.to_string(),
            kind,
            owner_node_id: self.node_id.clone(),
            current_locations: vec![self.node_id.clone()],
            content_hash: None,
            version: initial_version(),
            policy: DfsObjectPolicy::ReplicatedEventual,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn stat_object(&self, object: &DfsObjectMetadata) -> Result<DfsStat> {
        let size_bytes = if object.kind == DfsObjectKind::File {
            self.content
                .read_manifest(object.object_id.as_str(), object.version.as_str())
                .ok()
                .map(|manifest| manifest.size_bytes)
        } else {
            None
        };
        Ok(DfsStat {
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: normalize_posix_path(&object.path)?,
            kind: object.kind.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            size_bytes,
            policy: object.policy.clone(),
            current_locations: object.current_locations.clone(),
        })
    }
}

fn normalize_posix_path(path: &str) -> Result<String> {
    if !path.starts_with('/') {
        return Err(DfsPosixError::InvalidPath {
            path: path.to_string(),
        });
    }
    let mut parts = Vec::new();
    let normalized = path.replace('\\', "/");
    for part in normalized.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                return Err(DfsPosixError::InvalidPath {
                    path: path.to_string(),
                });
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

fn namespace_transaction_id(kind: &str, object_id: &DfsObjectId, path: &str) -> String {
    format!(
        "namespace_{}_{}",
        kind,
        blake3::hash(format!("{object_id}\0{path}").as_bytes()).to_hex()
    )
}

#[allow(dead_code)]
fn _assert_filesystem_send_sync<T: Send + Sync>(_: &DfsFileSystem) {}
