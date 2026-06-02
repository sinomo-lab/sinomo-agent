//! Local-first distributed filesystem metadata skeleton.

pub mod content_store;
pub mod control_plane;
pub mod discovery;
pub mod distributed;
pub mod local_owned;
pub mod metadata_backend;
pub mod namespace;
pub mod operator;
pub mod posix;
pub mod protocol;
mod transfer;

pub use content_store::{
    BlobRangeReader, BlobReader, ChunkedBlobAssembler, ContentManifest, ContentStoreError,
    ContentStoreGcOptions, ContentStoreGcReport, ContentStoreQuota, ContentStoreQuotaResource,
    ContentStoreQuotaUsage, ContentStoreScrubIssue, ContentStoreScrubReport, ContentStoreStats,
    ContentStoreVersionGcOptions, ContentStoreVersionGcReport, LocalContentStore,
    ObjectVersionReader,
};
pub use control_plane::{
    DfsControlPlaneKvClient, DfsControlPlaneStore, DfsFileSystemConfig, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, SharedDfsControlPlaneKvClient, file_system_config_key,
    metadata_shard_key, metadata_shards_prefix,
};
pub use discovery::{
    DfsClusterView, DfsNodeAdvertisement, content_service_key, content_services_prefix,
    metadata_service_key, metadata_services_prefix,
};
pub use distributed::{
    DfsDistributedError, DfsReplicaCandidate, DfsReplicaPlacementPlan, DfsReplicaPlacementRequest,
    DfsReplicaReceiptState, DfsReplicationPolicy, DfsWriteIntentState,
    DistributedEventualCoordinator, DistributedRepairJob, DistributedReplicaReceipt,
    DistributedWriteIntent, plan_replicas, validate_replication_plan,
};
pub use local_owned::{
    DfsExposeAddRequest, DfsExposeConfig, DfsExposeEntry, DfsExposeFileInfo, DfsExposeStore,
    DfsExposeStoreError, DfsExposedDirectory, LocalOwnedDfsError,
};
pub use metadata_backend::{
    DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT, DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
    DfsConflictPage, DfsConflictPageCursor, DfsConflictRecord, DfsConflictStatus,
    DfsDentryPageCursor, DfsDirFragmentId, DfsDirFragmentRecord, DfsDirectoryPage,
    DfsDirectoryPageCursor, DfsInodeId, DfsInodeRecord, DfsMetadataBackend,
    DfsMetadataJournalBatch, DfsMetadataJournalEntry, DfsMetadataMutation,
    DfsMetadataReplayCursorStatus, DfsMetadataReplicaLagStatus, DfsMetadataShardId,
    DfsMetadataShardStatus, DfsMetadataSubscription, DfsMetadataSummary, DfsMetadataTableSnapshot,
    DfsNamespaceJobFrame, DfsNamespaceJobKind, DfsNamespaceJobRecord, DfsNamespaceJobStatus,
    DfsNamespaceTransaction, DfsObjectPage, DfsObjectPageCursor, DfsObjectVersionPage,
    DfsObjectVersionPageCursor, DfsObjectVersionRecord, DfsPathRecord, DfsReplicaReceiptPage,
    DfsReplicaReceiptPageCursor, DfsReplicaReceiptRecord, DfsTombstoneCompactionOptions,
    DfsTombstoneCompactionReport, DfsWriteIntentPage, DfsWriteIntentPageCursor,
    DfsWriteIntentRecord, DfsWriteIntentStatus, LocalJsonMetadataBackend,
    LocalShardedMetadataBackend, MetadataBackendError,
};
use namespace::metadata_path;
pub use namespace::{
    SIFS_NODES_PATH, SIFS_ROOT_PATH, SIFS_SERVICES_PATH, SIFS_SINOMO_PATH, SIFS_USERS_PATH,
    directory_object, distributed_sifs, distributed_sifs_bootstrap_objects, sifs_node_files_path,
    sifs_node_path,
};
pub use operator::{
    DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsAgentFileSystemSummary,
    DfsAgentObjectState, DfsAgentObjectSummary, DfsOperatorConflictPage, DfsOperatorFileSystemView,
    DfsOperatorInspectionCursor, DfsOperatorInspectionItemCursor, DfsOperatorInspector,
    DfsOperatorObjectView, DfsOperatorRepairPage, DfsOperatorRepairView, DfsOperatorStatus,
    DfsPinAddRequest, DfsPinMode, DfsPinRecord, DfsPinStoreError, LocalDfsPinStore,
};
pub use posix::{
    DfsDirEntry, DfsDirEntryPage, DfsNamespaceJobFailure, DfsNamespaceJobMaintenanceReport,
    DfsPosixError, DfsStat, DfsWriteOptions,
};
pub use protocol::{
    DfsBlobTransferResult, DfsCommitVersionCommand, DfsCommitVersionResponse,
    DfsDistributedWriteCommand, DfsDistributedWriteResponse, DfsDistributedWriteResult,
    DfsFetchBlobCommand, DfsFetchBlobResponse, DfsFetchBlobResponseChunk, DfsFetchBlobResult,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsMetadataSnapshotInstallResult, DfsNetworkMessage, DfsProtocolFrameCommand,
    DfsProtocolFrameResponse, DfsReadData, DfsReadRangeData, DfsReadRequest, DfsReadResponse,
    DfsReadResponseChunk, DfsReadResult, DfsRepairReplicaChunkCommand, DfsRepairReplicaCommand,
    DfsRepairReplicaResponse, DfsStageBlobChunkCommand, DfsStageBlobCommand, DfsStageBlobResponse,
    DfsStagePatchCommand, DfsWriteCommand, DfsWriteResponse, DfsWriteResult,
};

pub type DfsFileSystemId = sinomo_core::Id;
pub type DfsObjectId = sinomo_core::Id;
pub type DfsVersion = sinomo_core::Id;

use serde::{Deserialize, Serialize};
use sinomo_core::{
    Confidence, DfsObjectPolicy, DfsWriteReceipt, DfsWriteRequest, EventEnvelope, EventKind,
    LatencyClass, PrivacyClass, RoutingMetadata, RoutingScope, Salience, Source,
};
use sinomo_events::EventId;
use sinomo_kv::KvError;
use sinomo_local::{LocalDataError, SinomoDataDir};
use sinomo_network::{
    MeshMessage, MeshMessageBody, MeshMessageId, NetworkError, NetworkFrame, NetworkFrameId,
    NetworkTransport, ProtocolMessage,
};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thiserror::Error;
use time::OffsetDateTime;
#[cfg(test)]
use transfer::RepairRetryState;
use transfer::{
    InProgressBlobTransfer, InProgressFetchBlobTransfer, InProgressReadTransfer,
    PendingNetworkedRead, PendingNetworkedWrite, PendingNetworkedWritePatch, blob_chunk_count,
    blob_chunk_count_for_size, blob_transfer_key, chunk_hash, validate_chunk_bytes,
    validate_chunk_hash, validate_chunked_transfer_shape,
};

pub const DFS_PROTOCOL: &str = "sinomo.dfs.v1";
pub const DEFAULT_DFS_NODE_ID: &str = "node_local";
pub const SIFS_FILE_SYSTEM_ID: &str = "sifs";
const DFS_BLOB_INLINE_LIMIT_BYTES: usize = 64 * 1024;
const DFS_BLOB_CHUNK_BYTES: usize = 64 * 1024;
const DFS_STAGE_CHUNK_WINDOW_SIZE: u32 = 64;
const DFS_READ_CHUNK_WINDOW_SIZE: u32 = 64;
const DFS_FETCH_CHUNK_WINDOW_SIZE: u32 = 64;
const DFS_REMOTE_READ_RETRY_DELAY: Duration = Duration::from_secs(1);
const DFS_CHUNK_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE: usize = 4096;
const DFS_METADATA_OBJECT_VERSION_PAGE_SIZE: usize = 1024;
const DFS_METADATA_OBJECT_INTENT_PAGE_SIZE: usize = 1024;
const DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE: usize = 1024;
const DFS_ANTI_ENTROPY_DEFAULT_OBJECT_PAGE_SIZE: usize = 1024;
const DFS_ANTI_ENTROPY_DEFAULT_ISSUE_LIMIT: usize = 1024;
#[cfg(not(test))]
const DFS_CONFLICT_SCAN_PAGE_SIZE: usize = 1024;
#[cfg(test)]
const DFS_CONFLICT_SCAN_PAGE_SIZE: usize = 2;

pub fn register_storage_paths(
    layout: &mut sinomo_storage::StorageLayout,
) -> Result<(), sinomo_storage::StorageContractError> {
    use sinomo_storage::StorageRing;

    for (relative_path, reason) in [
        ("dfs/content/blobs", "content-addressed DFS blob bytes"),
        (
            "dfs/content/objects",
            "dynamic DFS object manifests stored as sinomo-object files",
        ),
        ("dfs/content/staging", "temporary DFS staging bytes"),
        (
            "dfs/metadata/records",
            "dynamic DFS metadata records stored as sinomo-object files",
        ),
        (
            "dfs/metadata/journal",
            "dynamic DFS metadata journal entries stored as sinomo-object files",
        ),
        (
            "dfs/metadata/root",
            "default DFS root metadata shard with object records and journal entries",
        ),
        (
            "dfs/metadata/shard-index",
            "dynamic DFS object-to-metadata-shard index records",
        ),
        (
            "dfs/metadata-replication-cursors",
            "durable per-replica DFS metadata journal replication cursors",
        ),
        (
            "dfs/operator",
            "dynamic DFS operator intent records stored as sinomo-object files",
        ),
        (
            "dfs/pins",
            "dynamic DFS pin records stored as sinomo-object files",
        ),
        ("files", "local-owned user files exposed into DFS"),
    ] {
        layout.register_escape_path("sinomo-dfs", StorageRing::R1, relative_path, reason)?;
    }
    Ok(())
}

pub fn fallback_dfs_node_id() -> NodeId {
    NodeId::from_legacy_named(DEFAULT_DFS_NODE_ID).expect("valid fallback DFS node id")
}

pub fn sifs_file_system_id() -> DfsFileSystemId {
    DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("valid sifs filesystem id")
}

pub fn default_dfs_metadata_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("metadata").join("root")
}

pub fn default_dfs_metadata_shards_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("metadata")
}

pub fn resolve_dfs_metadata_root(explicit: Option<PathBuf>) -> Result<PathBuf, LocalDataError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(default_dfs_metadata_root(&data_dir))
}

pub fn default_dfs_pin_root(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_dir().join("pins")
}

pub fn resolve_dfs_pin_root(explicit: Option<PathBuf>) -> Result<PathBuf, LocalDataError> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    Ok(default_dfs_pin_root(&data_dir))
}

pub fn default_dfs_expose_config_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir.dfs_expose_config_file()
}

pub fn dfs_expose_config_path(explicit: Option<PathBuf>, data_dir: &SinomoDataDir) -> PathBuf {
    explicit.unwrap_or_else(|| default_dfs_expose_config_path(data_dir))
}

pub fn ensure_default_dfs_expose_config(
    config_path: impl AsRef<Path>,
    data_dir: impl AsRef<Path>,
    node_id: NodeId,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf())
        .ensure_default(data_dir.as_ref(), node_id)
}

pub fn load_dfs_expose_config(
    config_path: impl AsRef<Path>,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).load()
}

pub fn add_dfs_expose_entry(
    config_path: impl AsRef<Path>,
    request: DfsExposeAddRequest,
) -> Result<DfsExposeConfig, DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).add_entry(request)
}

pub fn remove_dfs_expose_entry(
    config_path: impl AsRef<Path>,
    id: &str,
) -> Result<(DfsExposeConfig, bool), DfsExposeStoreError> {
    DfsExposeStore::new(config_path.as_ref().to_path_buf()).remove_entry(id)
}

pub fn list_local_dfs_pins(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
) -> Result<Vec<DfsPinRecord>, DfsPinStoreError> {
    LocalDfsPinStore::new(root).list_pins(file_system_id)
}

pub fn add_local_dfs_pin(
    root: impl Into<PathBuf>,
    request: DfsPinAddRequest,
) -> Result<DfsPinRecord, DfsPinStoreError> {
    LocalDfsPinStore::new(root).add_pin(request)
}

pub fn remove_local_dfs_pin(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
    path: &str,
    node_id: Option<&NodeId>,
) -> Result<bool, DfsPinStoreError> {
    LocalDfsPinStore::new(root).remove_pin(file_system_id, path, node_id)
}

pub fn open_root_local_metadata_backend(
    root: impl Into<PathBuf>,
) -> Result<LocalJsonMetadataBackend, MetadataBackendError> {
    LocalJsonMetadataBackend::new(DfsMetadataShardId::new("root"), root.into())
}

pub fn open_local_sharded_metadata_backend(
    root: impl Into<PathBuf>,
    shards: Vec<DfsMetadataShardConfig>,
) -> Result<LocalShardedMetadataBackend, MetadataBackendError> {
    LocalShardedMetadataBackend::new(root, shards)
}

pub fn inspect_dfs_status(
    root: impl Into<PathBuf>,
) -> Result<DfsOperatorStatus, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).status()
}

pub fn inspect_dfs_file_system(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
) -> Result<Option<DfsOperatorFileSystemView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).file_system(file_system_id)
}

pub fn inspect_dfs_object(
    root: impl Into<PathBuf>,
    object_id: &DfsObjectId,
) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).object(object_id)
}

pub fn inspect_dfs_path(
    root: impl Into<PathBuf>,
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> Result<Option<DfsOperatorObjectView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).object_by_path(file_system_id, path)
}

pub fn inspect_dfs_conflicts(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
    object_id: Option<&DfsObjectId>,
) -> Result<Vec<DfsConflictRecord>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).conflicts(file_system_id, object_id)
}

pub fn inspect_dfs_repairs(
    root: impl Into<PathBuf>,
    file_system_id: Option<&DfsFileSystemId>,
    object_id: Option<&DfsObjectId>,
) -> Result<Vec<DfsOperatorRepairView>, MetadataBackendError> {
    let backend = open_root_local_metadata_backend(root)?;
    DfsOperatorInspector::new(&backend).repairs(file_system_id, object_id)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFileSystem {
    pub file_system_id: DfsFileSystemId,
    pub display_name: String,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtree_policies: Vec<DfsSubtreePolicy>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsSubtreePolicy {
    pub path_prefix: String,
    pub policy: DfsObjectPolicy,
}

impl DfsFileSystem {
    pub fn sifs(owner_node_id: NodeId) -> Self {
        Self {
            file_system_id: sifs_file_system_id(),
            display_name: "sifs".to_string(),
            owner_node_id,
            subtree_policies: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsObjectMetadata {
    pub object_id: DfsObjectId,
    pub file_system_id: DfsFileSystemId,
    pub path: String,
    pub kind: DfsObjectKind,
    pub owner_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub current_locations: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_target_object_id: Option<DfsObjectId>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub mode: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub uid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub gid: u32,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub accessed_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub modified_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub created_at_unix_nanos: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub changed_at_unix_nanos: u64,
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectKind {
    File,
    Directory,
    Symlink,
    Hardlink,
    Artifact,
    Mount,
    Tombstone,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteIntent {
    pub request: DfsWriteRequest,
    pub target_path: String,
    pub status: WriteIntentStatus,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteIntentStatus {
    PendingOwner,
    Committed,
    Rejected,
    Conflict,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsPolicyViolation {
    pub requesting_node_id: NodeId,
    pub executing_node_id: NodeId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_system_id: Option<DfsFileSystemId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<DfsObjectId>,
    pub reason: String,
}

impl DfsPolicyViolation {
    pub fn event(&self) -> EventEnvelope {
        EventEnvelope::new(
            EventId::generate(),
            OffsetDateTime::now_utc(),
            self.executing_node_id.clone(),
            Source::new("dfs.policy").expect("valid source"),
            EventKind::new("dfs.policy.violation").expect("valid event kind"),
            Salience::new(0.9).expect("valid salience"),
            Confidence::new(0.96).expect("valid confidence"),
            RoutingMetadata::new(
                RoutingScope::TrustedNetwork,
                LatencyClass::Background,
                PrivacyClass::Derived,
            ),
        )
        .with_target_node(self.executing_node_id.clone())
        .with_payload(serde_json::json!({
            "requesting_node_id": self.requesting_node_id,
            "executing_node_id": self.executing_node_id,
            "file_system_id": self.file_system_id,
            "object_id": self.object_id,
            "reason": self.reason,
        }))
    }
}

#[derive(Clone, Debug, Default)]
pub struct DfsMetadataStore {
    filesystems: BTreeMap<DfsFileSystemId, DfsFileSystem>,
    objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
    /// Index from `(file_system_id, normalized posix path)` to the object ids at
    /// that path, so path lookups avoid scanning every object. A path can hold
    /// more than one object id transiently (e.g. a tombstone alongside its
    /// replacement), so the set is resolved to the first non-tombstone object at
    /// lookup time, matching the previous linear scan exactly.
    objects_by_path: BTreeMap<(DfsFileSystemId, String), BTreeSet<DfsObjectId>>,
    conflicts: BTreeMap<DfsObjectId, BTreeMap<String, DfsConflictRecord>>,
}

impl DfsMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) {
        self.filesystems
            .insert(file_system.file_system_id.clone(), file_system);
    }

    pub fn add_object(&mut self, mut object: DfsObjectMetadata) -> Result<(), DfsError> {
        if !self.filesystems.contains_key(&object.file_system_id) {
            return Err(DfsError::UnknownFileSystem {
                file_system_id: object.file_system_id,
            });
        }
        normalize_current_locations(&mut object.current_locations);
        let new_key = path_index_key(&object.file_system_id, &object.path);
        let previous_key = self
            .objects
            .get(&object.object_id)
            .and_then(|previous| path_index_key(&previous.file_system_id, &previous.path));
        if previous_key != new_key {
            if let Some(previous_key) = previous_key {
                self.unindex_path(&previous_key, &object.object_id);
            }
            if let Some(new_key) = new_key {
                self.objects_by_path
                    .entry(new_key)
                    .or_default()
                    .insert(object.object_id.clone());
            }
        }
        self.objects.insert(object.object_id.clone(), object);
        Ok(())
    }

    fn remove_object(&mut self, object_id: &DfsObjectId) -> Option<DfsObjectMetadata> {
        let removed = self.objects.remove(object_id)?;
        if let Some(key) = path_index_key(&removed.file_system_id, &removed.path) {
            self.unindex_path(&key, object_id);
        }
        Some(removed)
    }

    fn unindex_path(&mut self, key: &(DfsFileSystemId, String), object_id: &DfsObjectId) {
        if let Some(object_ids) = self.objects_by_path.get_mut(key) {
            object_ids.remove(object_id);
            if object_ids.is_empty() {
                self.objects_by_path.remove(key);
            }
        }
    }

    pub fn object(&self, object_id: &DfsObjectId) -> Option<&DfsObjectMetadata> {
        self.objects.get(object_id)
    }

    /// Resolves the first non-tombstone object at a normalized posix path within
    /// a filesystem, using the path index instead of a full object scan.
    pub fn object_by_normalized_posix_path(
        &self,
        file_system_id: &DfsFileSystemId,
        normalized_path: &str,
    ) -> Option<&DfsObjectMetadata> {
        self.objects_by_path
            .get(&(file_system_id.clone(), normalized_path.to_string()))?
            .iter()
            .filter_map(|object_id| self.objects.get(object_id))
            .find(|object| object.kind != DfsObjectKind::Tombstone)
    }

    pub fn add_conflict(&mut self, conflict: DfsConflictRecord) {
        self.conflicts
            .entry(conflict.object_id.clone())
            .or_default()
            .insert(conflict.conflict_id.clone(), conflict);
    }

    pub fn conflicts_for_object(&self, object_id: &DfsObjectId) -> Vec<DfsConflictRecord> {
        self.conflicts
            .get(object_id)
            .map(|conflicts| conflicts.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn open_conflicts_for_object(&self, object_id: &DfsObjectId) -> Vec<DfsConflictRecord> {
        self.conflicts_for_object(object_id)
            .into_iter()
            .filter(|conflict| conflict.status == DfsConflictStatus::Open)
            .collect()
    }

    pub fn route_read(
        &self,
        object_id: &DfsObjectId,
        requester_node_id: &NodeId,
    ) -> Result<ReadRoute, DfsError> {
        let object = self
            .objects
            .get(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;

        let action = match &object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    ReadRouteAction::ReadLocally
                } else {
                    ReadRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            DfsObjectPolicy::ReplicatedConfirmed | DfsObjectPolicy::ReplicatedEventual => {
                if object.current_locations.contains(requester_node_id) {
                    ReadRouteAction::ReadLocally
                } else {
                    ReadRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            _ => {
                return Err(DfsError::UnsupportedPolicy {
                    policy: object.policy.clone(),
                });
            }
        };

        Ok(ReadRoute {
            object_id: object.object_id.clone(),
            action,
        })
    }

    pub fn route_write(
        &self,
        request: DfsWriteRequest,
        requester_node_id: &NodeId,
    ) -> Result<WriteRoute, DfsError> {
        let object =
            self.objects
                .get(&request.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: request.object_id.clone(),
                })?;

        let action = match &object.policy {
            DfsObjectPolicy::LocalOwned => {
                if &object.owner_node_id == requester_node_id {
                    WriteRouteAction::CommitLocally
                } else {
                    WriteRouteAction::RouteToOwner {
                        owner_node_id: object.owner_node_id.clone(),
                    }
                }
            }
            DfsObjectPolicy::ReplicatedConfirmed => WriteRouteAction::RequireReplicaReceipts,
            DfsObjectPolicy::ReplicatedEventual => WriteRouteAction::CommitThenReplicate,
            DfsObjectPolicy::AppendOnly => WriteRouteAction::AppendNewVersion,
            DfsObjectPolicy::EphemeralLocal => WriteRouteAction::CommitLocally,
        };

        Ok(WriteRoute {
            request,
            object_id: object.object_id.clone(),
            action,
        })
    }

    pub fn commit_write(
        &mut self,
        receipt: DfsWriteReceipt,
        content_hash: Option<String>,
    ) -> Result<(), DfsError> {
        let object =
            self.objects
                .get_mut(&receipt.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: receipt.object_id.clone(),
                })?;
        object.version = receipt.committed_version;
        object.last_modified_by_event_id = Some(receipt.event_id);
        object.content_hash = content_hash;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReadRoute {
    pub object_id: DfsObjectId,
    pub action: ReadRouteAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ReadRouteAction {
    ReadLocally,
    RouteToOwner { owner_node_id: NodeId },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WriteRoute {
    pub request: DfsWriteRequest,
    pub object_id: DfsObjectId,
    pub action: WriteRouteAction,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum WriteRouteAction {
    CommitLocally,
    RouteToOwner { owner_node_id: NodeId },
    RequireReplicaReceipts,
    CommitThenReplicate,
    AppendNewVersion,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsReadOutcome {
    Local {
        object_id: DfsObjectId,
        data: Option<DfsReadData>,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsWriteOutcome {
    Local {
        receipt: DfsWriteReceipt,
    },
    Routed {
        request_id: String,
        owner_node_id: NodeId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsServiceEvent {
    ReadRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsReadResult,
    },
    ReadResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsReadResult,
    },
    WriteRequestHandled {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsWriteResult,
    },
    WriteResponseReceived {
        request_id: String,
        object_id: DfsObjectId,
        result: DfsWriteResult,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsOperationalMetrics {
    pub node_id: NodeId,
    pub file_system_count: usize,
    pub object_count: usize,
    pub open_conflict_count: usize,
    pub coordinator_intent_count: usize,
    pub coordinator_receipt_count: usize,
    pub coordinator_committed_version_count: usize,
    pub repair_job_count: usize,
    pub pending_write_count: usize,
    pub pending_read_count: usize,
    pub stage_retry_count: usize,
    pub repair_retry_count: usize,
    pub stage_inflight_count: usize,
    pub repair_inflight_count: usize,
    pub fetch_inflight_count: usize,
    pub fetch_failed_source_count: usize,
    pub incoming_blob_transfer_count: usize,
    pub incoming_fetch_transfer_count: usize,
    pub incoming_read_transfer_count: usize,
    pub incoming_transfer_bytes: u64,
    pub active_replica_stage_count: usize,
    pub metadata_replication_cursor_count: usize,
    pub pending_metadata_replication_count: usize,
    pub pending_routed_write_count: usize,
    pub pending_remote_write_response_count: usize,
    pub metadata_backend_configured: bool,
    pub content: ContentStoreStats,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAntiEntropyOptions {
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<DfsObjectPageCursor>,
    pub max_objects: usize,
    pub max_issues: usize,
    #[serde(default)]
    pub repair_metadata: bool,
}

impl DfsAntiEntropyOptions {
    pub fn new(file_system_id: DfsFileSystemId) -> Self {
        Self {
            file_system_id,
            after: None,
            max_objects: DFS_ANTI_ENTROPY_DEFAULT_OBJECT_PAGE_SIZE,
            max_issues: DFS_ANTI_ENTROPY_DEFAULT_ISSUE_LIMIT,
            repair_metadata: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAntiEntropyReport {
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<DfsObjectPageCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectPageCursor>,
    pub metadata_backend_configured: bool,
    pub scanned_object_count: u64,
    pub checked_local_location_count: u64,
    pub stale_local_location_count: u64,
    pub repaired_location_count: u64,
    pub issue_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<DfsAntiEntropyIssue>,
    pub max_issues_reached: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAntiEntropyIssue {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub path: String,
    pub content_hash: String,
    pub reason: String,
    pub repaired: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsLifecycleMaintenanceOptions {
    #[serde(default)]
    pub scrub_content: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anti_entropy: Option<DfsAntiEntropyOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_gc: Option<ContentStoreVersionGcOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orphan_gc: Option<ContentStoreGcOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_compaction: Option<DfsTombstoneCompactionOptions>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsLifecycleMaintenanceReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_scrub: Option<ContentStoreScrubReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anti_entropy: Option<DfsAntiEntropyReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_gc: Option<ContentStoreVersionGcReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orphan_gc: Option<ContentStoreGcReport>,
    pub metadata_backend_configured: bool,
    pub tombstone_compaction_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tombstone_compaction: Option<DfsTombstoneCompactionReport>,
}

#[derive(Clone, Debug)]
pub struct LocalOwnedDfsService<T> {
    node_id: NodeId,
    metadata: DfsMetadataStore,
    content: BTreeMap<DfsObjectId, Vec<u8>>,
    pending_reads: BTreeMap<String, DfsObjectId>,
    pending_writes: BTreeMap<String, DfsObjectId>,
    transport: T,
    next_frame_id: u64,
    next_message_id: u64,
    next_request_id: u64,
}

impl<T: NetworkTransport> LocalOwnedDfsService<T> {
    pub fn new(transport: T) -> Self {
        let node_id = transport.local_node_id().clone();
        let mut metadata = DfsMetadataStore::new();
        metadata.add_file_system(DfsFileSystem::sifs(node_id.clone()));
        Self {
            node_id,
            metadata,
            content: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            transport,
            next_frame_id: 1,
            next_message_id: 1,
            next_request_id: 1,
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn metadata(&self) -> &DfsMetadataStore {
        &self.metadata
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) {
        self.metadata.add_file_system(file_system);
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.metadata.add_object(object)
    }

    pub fn add_object_metadata_batch(
        &mut self,
        objects: impl IntoIterator<Item = DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        for object in objects {
            self.metadata.add_object(object)?;
        }
        Ok(())
    }

    pub fn put_local_object(
        &mut self,
        mut object: DfsObjectMetadata,
        bytes: Vec<u8>,
    ) -> Result<(), DfsError> {
        if object.owner_node_id != self.node_id {
            return Err(DfsError::NotObjectOwner {
                object_id: object.object_id,
                owner_node_id: object.owner_node_id,
                local_node_id: self.node_id.clone(),
            });
        }

        object.content_hash = Some(content_hash(&bytes));
        object.content_size_bytes = Some(bytes.len() as u64);
        object.current_locations = vec![self.node_id.clone()];
        let object_id = object.object_id.clone();
        self.metadata.add_object(object)?;
        self.content.insert(object_id, bytes);
        Ok(())
    }

    pub fn read_object(&mut self, object_id: &DfsObjectId) -> Result<DfsReadOutcome, DfsError> {
        let route = self.metadata.route_read(object_id, &self.node_id)?;
        match route.action {
            ReadRouteAction::ReadLocally => Ok(DfsReadOutcome::Local {
                object_id: object_id.clone(),
                data: self.local_read(object_id)?,
            }),
            ReadRouteAction::RouteToOwner { owner_node_id } => {
                let request_id = self.next_request_id();
                self.pending_reads
                    .insert(request_id.clone(), object_id.clone());
                self.send_dfs_message(
                    &owner_node_id,
                    DfsNetworkMessage::ReadRequest(DfsReadRequest {
                        request_id: request_id.clone(),
                        object_id: object_id.clone(),
                        range_offset: None,
                        range_max_bytes: None,
                        start_chunk_index: 0,
                        max_chunks: 0,
                    }),
                )?;
                Ok(DfsReadOutcome::Routed {
                    request_id,
                    owner_node_id,
                })
            }
        }
    }

    pub fn write_object(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
    ) -> Result<DfsWriteOutcome, DfsError> {
        let route = self.metadata.route_write(request.clone(), &self.node_id)?;
        match route.action {
            WriteRouteAction::CommitLocally => {
                let receipt = self.commit_local_write(request, bytes)?;
                Ok(DfsWriteOutcome::Local { receipt })
            }
            WriteRouteAction::RouteToOwner { owner_node_id } => {
                let request_id = self.next_request_id();
                self.pending_writes
                    .insert(request_id.clone(), request.object_id.clone());
                self.send_dfs_message(
                    &owner_node_id,
                    DfsNetworkMessage::WriteRequest(DfsWriteCommand {
                        request_id: request_id.clone(),
                        request,
                        bytes,
                    }),
                )?;
                Ok(DfsWriteOutcome::Routed {
                    request_id,
                    owner_node_id,
                })
            }
            _ => Err(DfsError::UnsupportedPolicy {
                policy: self
                    .metadata
                    .object(&route.object_id)
                    .map(|object| object.policy.clone())
                    .unwrap_or(DfsObjectPolicy::LocalOwned),
            }),
        }
    }

    pub fn process_incoming(&mut self) -> Result<Vec<DfsServiceEvent>, DfsError> {
        let inbound_frames = self
            .transport
            .drain_incoming()
            .map_err(DfsError::from_network)?;
        let mut events = Vec::new();

        for inbound in inbound_frames {
            let source_node_id = inbound.frame.source_node_id.clone();
            let MeshMessageBody::Protocol(protocol_message) = inbound.frame.message.body else {
                continue;
            };

            if protocol_message.protocol != DFS_PROTOCOL {
                continue;
            }

            let Ok(message) = protocol_message.decode::<DfsNetworkMessage>() else {
                continue;
            };

            match message {
                DfsNetworkMessage::ReadRequest(request) => {
                    let event = self.handle_read_request(source_node_id, request)?;
                    events.push(event);
                }
                DfsNetworkMessage::ReadResponse(response) => {
                    self.pending_reads.remove(&response.request_id);
                    events.push(DfsServiceEvent::ReadResponseReceived {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        result: response.result,
                    });
                }
                DfsNetworkMessage::WriteRequest(command) => {
                    let event = self.handle_write_request(source_node_id, command)?;
                    events.push(event);
                }
                DfsNetworkMessage::WriteResponse(response) => {
                    self.pending_writes.remove(&response.request_id);
                    if let DfsWriteResult::Committed {
                        receipt,
                        content_hash,
                    } = &response.result
                    {
                        self.metadata
                            .commit_write(receipt.clone(), content_hash.clone())?;
                    }
                    events.push(DfsServiceEvent::WriteResponseReceived {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        result: response.result,
                    });
                }
                DfsNetworkMessage::StageBlob(_)
                | DfsNetworkMessage::StageBlobChunk(_)
                | DfsNetworkMessage::StagePatch(_)
                | DfsNetworkMessage::StageBlobResponse(_)
                | DfsNetworkMessage::CommitVersion(_)
                | DfsNetworkMessage::CommitVersionResponse(_)
                | DfsNetworkMessage::RepairReplica(_)
                | DfsNetworkMessage::RepairReplicaChunk(_)
                | DfsNetworkMessage::RepairReplicaResponse(_)
                | DfsNetworkMessage::FetchBlob(_)
                | DfsNetworkMessage::FetchBlobResponse(_)
                | DfsNetworkMessage::FetchBlobResponseChunk(_)
                | DfsNetworkMessage::DistributedWriteRequest(_)
                | DfsNetworkMessage::DistributedWriteResponse(_)
                | DfsNetworkMessage::MetadataJournalBatch(_)
                | DfsNetworkMessage::MetadataJournalBatchResponse(_)
                | DfsNetworkMessage::MetadataSnapshotInstall(_)
                | DfsNetworkMessage::MetadataSnapshotInstallResponse(_)
                | DfsNetworkMessage::ReadResponseChunk(_) => {}
            }
        }

        Ok(events)
    }

    fn handle_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsReadRequest,
    ) -> Result<DfsServiceEvent, DfsError> {
        let result = match self.metadata.object(&request.object_id) {
            Some(object) if object.owner_node_id != self.node_id => DfsReadResult::Rejected {
                reason: format!(
                    "node {} does not own object {}",
                    self.node_id, request.object_id
                ),
            },
            Some(_) => match (request.range_offset, request.range_max_bytes) {
                (Some(offset), Some(max_bytes)) => self
                    .local_read(&request.object_id)?
                    .map(|data| {
                        let total_size_bytes = data.bytes.len() as u64;
                        let start = usize::try_from(offset)
                            .ok()
                            .map(|offset| offset.min(data.bytes.len()))
                            .unwrap_or(data.bytes.len());
                        let end = start.saturating_add(max_bytes).min(data.bytes.len());
                        DfsReadResult::RangeFound(DfsReadRangeData {
                            object_id: data.object_id,
                            version: data.version,
                            content_hash: data.content_hash,
                            offset: offset.min(total_size_bytes),
                            total_size_bytes,
                            bytes: data.bytes[start..end].to_vec(),
                        })
                    })
                    .unwrap_or(DfsReadResult::NotFound),
                (None, None) => self
                    .local_read(&request.object_id)?
                    .map(DfsReadResult::Found)
                    .unwrap_or(DfsReadResult::NotFound),
                _ => DfsReadResult::Rejected {
                    reason: "DFS read range request must include both offset and max bytes"
                        .to_string(),
                },
            },
            None => DfsReadResult::NotFound,
        };

        self.send_dfs_message(
            &requester_node_id,
            DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: request.request_id.clone(),
                object_id: request.object_id.clone(),
                result: result.clone(),
            }),
        )?;

        Ok(DfsServiceEvent::ReadRequestHandled {
            request_id: request.request_id,
            object_id: request.object_id,
            requester_node_id,
            result,
        })
    }

    fn handle_write_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsWriteCommand,
    ) -> Result<DfsServiceEvent, DfsError> {
        let object_id = command.request.object_id.clone();
        let result = match self.commit_local_write(command.request, command.bytes) {
            Ok(receipt) => DfsWriteResult::Committed {
                content_hash: self
                    .metadata
                    .object(&object_id)
                    .and_then(|object| object.content_hash.clone()),
                receipt,
            },
            Err(DfsError::UnknownObject { .. }) => DfsWriteResult::NotFound,
            Err(error) => DfsWriteResult::Rejected {
                reason: error.to_string(),
            },
        };

        self.send_dfs_message(
            &requester_node_id,
            DfsNetworkMessage::WriteResponse(DfsWriteResponse {
                request_id: command.request_id.clone(),
                object_id: object_id.clone(),
                result: result.clone(),
            }),
        )?;

        Ok(DfsServiceEvent::WriteRequestHandled {
            request_id: command.request_id,
            object_id,
            requester_node_id,
            result,
        })
    }

    fn local_read(&self, object_id: &DfsObjectId) -> Result<Option<DfsReadData>, DfsError> {
        let object = self
            .metadata
            .object(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;

        Ok(self.content.get(object_id).map(|bytes| DfsReadData {
            object_id: object_id.clone(),
            version: object.version.clone(),
            content_hash: object.content_hash.clone(),
            bytes: bytes.clone(),
        }))
    }

    fn commit_local_write(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
    ) -> Result<DfsWriteReceipt, DfsError> {
        let object =
            self.metadata
                .object(&request.object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: request.object_id.clone(),
                })?;

        if object.policy != DfsObjectPolicy::LocalOwned {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }

        if object.owner_node_id != self.node_id {
            return Err(DfsError::NotObjectOwner {
                object_id: object.object_id.clone(),
                owner_node_id: object.owner_node_id.clone(),
                local_node_id: self.node_id.clone(),
            });
        }

        let content_hash = Some(content_hash(&bytes));
        let receipt = DfsWriteReceipt {
            file_system_id: request.file_system_id,
            object_id: request.object_id.clone(),
            committed_version: request.target_version,
            owner_node_id: self.node_id.clone(),
            event_id: EventId::generate(),
        };
        self.content.insert(request.object_id, bytes);
        self.metadata
            .commit_write(receipt.clone(), content_hash.clone())?;
        Ok(receipt)
    }

    fn send_dfs_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<(), DfsError> {
        let protocol_message =
            ProtocolMessage::encode(DFS_PROTOCOL, &message).map_err(DfsError::from_network)?;
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.node_id.clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.node_id.clone(),
                target_node_id: target_node_id.clone(),
                body: MeshMessageBody::Protocol(protocol_message),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;

        self.transport
            .send(target_node_id, frame)
            .map_err(DfsError::from_network)
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsWriteOutcome {
    Committed {
        intent_id: String,
        version: DfsVersion,
        content_hash: String,
        repair_jobs: Vec<DistributedRepairJob>,
    },
    Pending {
        intent_id: String,
        durable_receipts: usize,
        required_receipts: usize,
    },
    Conflict {
        conflict: DfsConflictRecord,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsConflictReadView {
    pub conflict_id: String,
    pub base_version: Option<DfsVersion>,
    pub local_version: DfsVersion,
    pub remote_version: DfsVersion,
    pub detected_by_node_id: NodeId,
    pub reason: String,
    pub local_content_hash: Option<String>,
    pub remote_content_hash: Option<String>,
    pub status: DfsConflictStatus,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsReadOutcome {
    Found {
        data: DfsReadData,
        freshness: NetworkedDfsReadFreshness,
    },
    NotConfirmed {
        object_id: DfsObjectId,
        version: DfsVersion,
        confirmed_locations: Vec<NodeId>,
        required_locations: Vec<NodeId>,
    },
    PendingReplication {
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: Option<String>,
        current_locations: Vec<NodeId>,
    },
    Conflicted {
        object_id: DfsObjectId,
        visible_version: DfsVersion,
        visible_content_hash: Option<String>,
        conflicts: Vec<DfsConflictReadView>,
    },
    NotFound,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsRangeReadOutcome {
    Found {
        data: DfsReadRangeData,
        freshness: NetworkedDfsReadFreshness,
    },
    NotConfirmed {
        object_id: DfsObjectId,
        version: DfsVersion,
        confirmed_locations: Vec<NodeId>,
        required_locations: Vec<NodeId>,
    },
    PendingReplication {
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: Option<String>,
        current_locations: Vec<NodeId>,
    },
    Conflicted {
        object_id: DfsObjectId,
        visible_version: DfsVersion,
        visible_content_hash: Option<String>,
        conflicts: Vec<DfsConflictReadView>,
    },
    NotFound,
}

struct NetworkedDfsReadStream {
    data: DfsReadData,
    total_size_bytes: u64,
    reader: ObjectVersionReader,
}

enum NetworkedDfsReadStreamOutcome {
    Found(NetworkedDfsReadStream),
    PendingReplication,
    Conflicted { conflicts: Vec<DfsConflictReadView> },
    NotFound,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StageBlobSendOutcome {
    sent: bool,
    complete: bool,
    next_chunk_index: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkedDfsReadFreshness {
    Current,
    Confirmed {
        replica_node_ids: Vec<NodeId>,
    },
    Stale {
        current_version: DfsVersion,
        current_content_hash: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsConfirmedTimeoutAction {
    KeepPending,
    Abort,
    DowngradeToEventual { min_commit_replicas: usize },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsConfirmedTimeoutOutcome {
    Committed(NetworkedDfsWriteOutcome),
    StillPending {
        intent_id: String,
        durable_receipts: usize,
        required_receipts: usize,
    },
    Aborted {
        intent_id: String,
    },
    Conflict {
        conflict: DfsConflictRecord,
    },
    Downgraded(NetworkedDfsWriteOutcome),
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReconnectReconciliation {
    pub object_id: DfsObjectId,
    pub refreshed_receipts: usize,
    pub conflicts: Vec<DfsConflictRecord>,
    pub missing_current: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsOwnerHandoffOutcome {
    Transferred {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_owner_node_id: NodeId,
        new_owner_node_id: NodeId,
        previous_epoch: u64,
        new_epoch: u64,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum DfsMetadataReplicationOutcome {
    Sent {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        entries: usize,
        next_after_sequence: u64,
    },
    InFlight {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
    },
    NoChanges {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    Unroutable {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingMetadataReplication {
    target_node_id: NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingRoutedWrite {
    object_id: DfsObjectId,
    primary_node_id: NodeId,
    remote_intent_id: Option<String>,
    durable_receipts: usize,
    required_receipts: usize,
    result: Option<Result<NetworkedDfsWriteOutcome, DfsError>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingRemoteWriteResponse {
    requester_node_id: NodeId,
    request_id: String,
    object_id: DfsObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum NetworkedDfsServiceEvent {
    BlobStaged {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        content_hash: String,
    },
    BlobStageRejected {
        intent_id: String,
        object_id: DfsObjectId,
        replica_node_id: NodeId,
        reason: String,
    },
    VersionCommitted {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
    },
    ReplicaRepaired {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        replica_node_id: NodeId,
        content_hash: String,
    },
    BlobFetched {
        intent_id: String,
        object_id: DfsObjectId,
        version: DfsVersion,
        source_node_id: NodeId,
        content_hash: String,
    },
    BlobFetchRejected {
        intent_id: String,
        object_id: DfsObjectId,
        source_node_id: NodeId,
        reason: String,
    },
    RemoteReadServed {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsReadResult,
    },
    RemoteReadCompleted {
        request_id: String,
        object_id: DfsObjectId,
        source_node_id: NodeId,
        result: DfsReadResult,
    },
    RemoteWriteServed {
        request_id: String,
        object_id: DfsObjectId,
        requester_node_id: NodeId,
        result: DfsDistributedWriteResult,
    },
    RemoteWriteCompleted {
        request_id: String,
        object_id: DfsObjectId,
        primary_node_id: NodeId,
        result: DfsDistributedWriteResult,
    },
    RepairRejected {
        intent_id: String,
        object_id: DfsObjectId,
        replica_node_id: NodeId,
        reason: String,
    },
    WriteConflictCreated {
        conflict: DfsConflictRecord,
    },
    MetadataJournalBatchApplied {
        request_id: String,
        source_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        applied_entries: usize,
        next_after_sequence: u64,
    },
    MetadataJournalBatchRejected {
        request_id: String,
        source_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    OwnerHandoffCompleted {
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        previous_owner_node_id: NodeId,
        new_owner_node_id: NodeId,
        previous_epoch: u64,
        new_epoch: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ActiveReplicaStage {
    intent_id: String,
    version: DfsVersion,
    content_hash: String,
    requester_node_id: NodeId,
}

#[derive(Debug)]
pub struct NetworkedDfsService<T> {
    node_id: NodeId,
    metadata: DfsMetadataStore,
    tombstones_by_metadata_path: BTreeMap<(DfsFileSystemId, String), DfsObjectMetadata>,
    content: LocalContentStore,
    coordinator: DistributedEventualCoordinator,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    metadata_backend: Option<Box<dyn DfsMetadataBackend>>,
    active_replica_stages: BTreeMap<DfsObjectId, ActiveReplicaStage>,
    pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    pending_reads: BTreeMap<String, PendingNetworkedRead>,
    incoming_read_transfers: BTreeMap<String, InProgressReadTransfer>,
    incoming_fetch_transfers: BTreeMap<String, InProgressFetchBlobTransfer>,
    incoming_blob_transfers: BTreeMap<String, InProgressBlobTransfer>,
    metadata_replication_cursors: BTreeMap<(NodeId, DfsMetadataShardId), u64>,
    pending_metadata_replications: BTreeMap<String, PendingMetadataReplication>,
    pending_routed_writes: BTreeMap<String, PendingRoutedWrite>,
    pending_remote_write_responses: BTreeMap<String, PendingRemoteWriteResponse>,
    last_events: Vec<NetworkedDfsServiceEvent>,
    transport: T,
    repair_retry_tick: u64,
    next_frame_id: u64,
    next_message_id: u64,
    next_request_id: u64,
}

impl<T: NetworkTransport> NetworkedDfsService<T> {
    pub fn new(transport: T, content: LocalContentStore) -> Self {
        let node_id = transport.local_node_id().clone();
        let mut metadata = DfsMetadataStore::new();
        metadata.add_file_system(DfsFileSystem::sifs(node_id.clone()));
        Self {
            node_id,
            metadata,
            tombstones_by_metadata_path: BTreeMap::new(),
            content,
            coordinator: DistributedEventualCoordinator::new(),
            control_plane: None,
            metadata_backend: None,
            active_replica_stages: BTreeMap::new(),
            pending_writes: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            incoming_read_transfers: BTreeMap::new(),
            incoming_fetch_transfers: BTreeMap::new(),
            incoming_blob_transfers: BTreeMap::new(),
            metadata_replication_cursors: BTreeMap::new(),
            pending_metadata_replications: BTreeMap::new(),
            pending_routed_writes: BTreeMap::new(),
            pending_remote_write_responses: BTreeMap::new(),
            last_events: Vec::new(),
            transport,
            repair_retry_tick: 0,
            next_frame_id: 1,
            next_message_id: 1,
            next_request_id: 1,
        }
    }

    pub fn with_control_plane(mut self, kv: impl DfsControlPlaneKvClient + Send + 'static) -> Self {
        self.control_plane = Some(Box::new(kv));
        self
    }

    pub fn with_metadata_backend(
        mut self,
        backend: impl DfsMetadataBackend + Send + 'static,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(Box::new(backend));
        self.hydrate_from_metadata_backend()?;
        Ok(self)
    }

    pub fn with_boxed_metadata_backend(
        mut self,
        backend: Box<dyn DfsMetadataBackend>,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(backend);
        self.hydrate_from_metadata_backend()?;
        Ok(self)
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn metadata(&self) -> &DfsMetadataStore {
        &self.metadata
    }

    pub fn content(&self) -> &LocalContentStore {
        &self.content
    }

    pub fn last_events(&self) -> &[NetworkedDfsServiceEvent] {
        &self.last_events
    }

    pub fn into_transport(self) -> T {
        self.transport
    }

    pub fn coordinator(&self) -> &DistributedEventualCoordinator {
        &self.coordinator
    }

    pub fn metadata_backend(&self) -> Option<&dyn DfsMetadataBackend> {
        self.metadata_backend.as_deref()
    }

    pub fn operational_metrics(&self) -> Result<DfsOperationalMetrics, DfsError> {
        let open_conflict_count = self
            .metadata
            .conflicts
            .values()
            .map(|conflicts| {
                conflicts
                    .values()
                    .filter(|conflict| conflict.status == DfsConflictStatus::Open)
                    .count()
            })
            .sum();
        let stage_retry_count = self
            .pending_writes
            .values()
            .map(|pending| pending.stage_retries.len())
            .sum();
        let repair_retry_count = self
            .pending_writes
            .values()
            .map(|pending| pending.repair_retries.len())
            .sum();
        let stage_inflight_count = self
            .pending_writes
            .values()
            .map(|pending| pending.stage_inflight_request_ids.len())
            .sum();
        let repair_inflight_count = self
            .pending_writes
            .values()
            .map(|pending| pending.repair_inflight_request_ids.len())
            .sum();
        let fetch_inflight_count = self
            .pending_writes
            .values()
            .map(|pending| pending.fetch_inflight_from.len())
            .sum();
        let fetch_failed_source_count = self
            .pending_writes
            .values()
            .map(|pending| pending.fetch_failed_from.len())
            .sum();
        let incoming_transfer_bytes = self
            .incoming_blob_transfers
            .values()
            .map(|transfer| transfer.total_size_bytes)
            .chain(
                self.incoming_fetch_transfers
                    .values()
                    .map(|transfer| transfer.total_size_bytes),
            )
            .chain(
                self.incoming_read_transfers
                    .values()
                    .map(|transfer| transfer.total_size_bytes),
            )
            .fold(0_u64, |total, size| total.saturating_add(size));

        Ok(DfsOperationalMetrics {
            node_id: self.node_id.clone(),
            file_system_count: self.metadata.filesystems.len(),
            object_count: self.metadata.objects.len(),
            open_conflict_count,
            coordinator_intent_count: self.coordinator.intent_count(),
            coordinator_receipt_count: self.coordinator.receipt_count(),
            coordinator_committed_version_count: self.coordinator.committed_version_count(),
            repair_job_count: self.coordinator.repair_jobs().len(),
            pending_write_count: self.pending_writes.len(),
            pending_read_count: self.pending_reads.len(),
            stage_retry_count,
            repair_retry_count,
            stage_inflight_count,
            repair_inflight_count,
            fetch_inflight_count,
            fetch_failed_source_count,
            incoming_blob_transfer_count: self.incoming_blob_transfers.len(),
            incoming_fetch_transfer_count: self.incoming_fetch_transfers.len(),
            incoming_read_transfer_count: self.incoming_read_transfers.len(),
            incoming_transfer_bytes,
            active_replica_stage_count: self.active_replica_stages.len(),
            metadata_replication_cursor_count: self.metadata_replication_cursors.len(),
            pending_metadata_replication_count: self.pending_metadata_replications.len(),
            pending_routed_write_count: self.pending_routed_writes.len(),
            pending_remote_write_response_count: self.pending_remote_write_responses.len(),
            metadata_backend_configured: self.metadata_backend.is_some(),
            content: self.content.stats()?,
        })
    }

    pub fn run_local_content_anti_entropy(
        &mut self,
        options: DfsAntiEntropyOptions,
    ) -> Result<DfsAntiEntropyReport, DfsError> {
        let mut report = DfsAntiEntropyReport {
            file_system_id: options.file_system_id.clone(),
            after: options.after.clone(),
            next_cursor: None,
            metadata_backend_configured: self.metadata_backend.is_some(),
            scanned_object_count: 0,
            checked_local_location_count: 0,
            stale_local_location_count: 0,
            repaired_location_count: 0,
            issue_count: 0,
            issues: Vec::new(),
            max_issues_reached: false,
        };
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(report);
        };
        let page = backend.objects_for_file_system_page(
            &options.file_system_id,
            options.after.as_ref(),
            options.max_objects.max(1),
        )?;
        report.next_cursor = page.next_cursor;
        let local_node_id = self.node_id.clone();

        for object in page.objects {
            report.scanned_object_count = report.scanned_object_count.saturating_add(1);
            if !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink) {
                continue;
            }
            if !object.current_locations.contains(&local_node_id) {
                continue;
            }
            let Some(content_hash) = object.content_hash.clone() else {
                continue;
            };
            report.checked_local_location_count =
                report.checked_local_location_count.saturating_add(1);
            let Some(reason) = self.local_content_anti_entropy_issue_reason(&object, &content_hash)
            else {
                continue;
            };
            report.stale_local_location_count = report.stale_local_location_count.saturating_add(1);
            let repaired = if options.repair_metadata {
                self.mark_remote_read_location_missing(
                    &object.object_id,
                    &object.version,
                    &object.content_hash,
                    &local_node_id,
                )?;
                report.repaired_location_count = report.repaired_location_count.saturating_add(1);
                true
            } else {
                false
            };
            record_anti_entropy_issue(
                &mut report,
                options.max_issues,
                DfsAntiEntropyIssue {
                    object_id: object.object_id,
                    version: object.version,
                    path: object.path,
                    content_hash,
                    reason,
                    repaired,
                },
            );
        }

        Ok(report)
    }

    pub fn run_lifecycle_maintenance(
        &mut self,
        options: DfsLifecycleMaintenanceOptions,
    ) -> Result<DfsLifecycleMaintenanceReport, DfsError> {
        let content_scrub = if options.scrub_content {
            Some(self.content.scrub()?)
        } else {
            None
        };
        let anti_entropy = options
            .anti_entropy
            .map(|options| self.run_local_content_anti_entropy(options))
            .transpose()?;
        let version_gc = options
            .version_gc
            .map(|options| self.content.collect_versions(options))
            .transpose()?;
        let orphan_gc = options
            .orphan_gc
            .map(|options| self.content.collect_garbage(options))
            .transpose()?;
        let tombstone_compaction_requested = options.tombstone_compaction.is_some();
        let metadata_backend_configured = self.metadata_backend.is_some();
        let tombstone_compaction =
            match (options.tombstone_compaction, self.metadata_backend.as_mut()) {
                (Some(options), Some(backend)) => Some(backend.compact_tombstones(options)?),
                (Some(_), None) | (None, _) => None,
            };

        Ok(DfsLifecycleMaintenanceReport {
            content_scrub,
            anti_entropy,
            version_gc,
            orphan_gc,
            metadata_backend_configured,
            tombstone_compaction_requested,
            tombstone_compaction,
        })
    }

    fn local_content_anti_entropy_issue_reason(
        &self,
        object: &DfsObjectMetadata,
        expected_content_hash: &str,
    ) -> Option<String> {
        let manifest = match self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return Some(format!(
                    "local object-version manifest is missing or unreadable: {error}"
                ));
            }
        };
        if manifest.content_hash != expected_content_hash {
            return Some(format!(
                "local object-version manifest hash mismatch: expected {expected_content_hash}, actual {}",
                manifest.content_hash
            ));
        }
        if let Some(expected_size) = object.content_size_bytes
            && manifest.size_bytes != expected_size
        {
            return Some(format!(
                "local object-version manifest size mismatch: expected {expected_size}, actual {}",
                manifest.size_bytes
            ));
        }
        match self.content.manifest_content_available(&manifest) {
            Ok(true) => None,
            Ok(false) => Some(
                "local object-version manifest references missing or corrupt content".to_string(),
            ),
            Err(error) => Some(format!(
                "local object-version manifest content is unreadable: {error}"
            )),
        }
    }

    pub fn send_metadata_journal_since(
        &mut self,
        target_node_id: &NodeId,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(batch) = self.metadata_journal_batch_since(after_sequence)? else {
            return Ok(None);
        };
        let request_id = self.next_request_id();
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                request_id,
                source_node_id: Some(self.node_id.clone()),
                batch: batch.clone(),
            }),
        )?;
        Ok(sent.then_some(batch))
    }

    pub fn replicate_metadata_shard_to_configured_replicas(
        &mut self,
        file_system_id: &DfsFileSystemId,
        shard_id: &str,
    ) -> Result<Vec<DfsMetadataReplicationOutcome>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Vec::new());
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(file_system_id, shard_id)?
        else {
            return Ok(Vec::new());
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: shard.file_system_id,
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }

        let shard_id = DfsMetadataShardId::new(shard.shard_id);
        let local_node_id = self.node_id.clone();
        let mut outcomes = Vec::new();
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != &local_node_id)
        {
            let cursor_key = (target_node_id.clone(), shard_id.clone());
            let after_sequence =
                if let Some(after_sequence) = self.metadata_replication_cursors.get(&cursor_key) {
                    *after_sequence
                } else if let Some(backend) = self.metadata_backend.as_ref() {
                    let after_sequence =
                        backend.metadata_replication_cursor(&target_node_id, &shard_id)?;
                    self.metadata_replication_cursors
                        .insert(cursor_key.clone(), after_sequence);
                    after_sequence
                } else {
                    0
                };
            let Some(batch) =
                self.metadata_journal_batch_for_shard_since(&shard_id, after_sequence)?
            else {
                outcomes.push(DfsMetadataReplicationOutcome::NoChanges {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
                continue;
            };
            let pending_next_after_sequence = self
                .pending_metadata_replications
                .values()
                .filter(|pending| {
                    pending.target_node_id == target_node_id && pending.shard_id == shard_id
                })
                .map(|pending| pending.next_after_sequence)
                .max();
            if let Some(pending_next_after_sequence) = pending_next_after_sequence
                && pending_next_after_sequence >= batch.next_after_sequence
            {
                outcomes.push(DfsMetadataReplicationOutcome::InFlight {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    next_after_sequence: pending_next_after_sequence,
                });
                continue;
            }
            let entries = batch.entries.len();
            let next_after_sequence = batch.next_after_sequence;
            let request_id = self.next_request_id();
            self.pending_metadata_replications.insert(
                request_id.clone(),
                PendingMetadataReplication {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                },
            );
            let sent = self.send_dfs_message_if_routable(
                &target_node_id,
                DfsNetworkMessage::MetadataJournalBatch(DfsMetadataJournalBatchCommand {
                    request_id: request_id.clone(),
                    source_node_id: Some(self.node_id.clone()),
                    batch,
                }),
            )?;
            if sent {
                outcomes.push(DfsMetadataReplicationOutcome::Sent {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    entries,
                    next_after_sequence,
                });
            } else {
                self.pending_metadata_replications.remove(&request_id);
                outcomes.push(DfsMetadataReplicationOutcome::Unroutable {
                    target_node_id,
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
            }
        }
        Ok(outcomes)
    }

    pub fn replicate_configured_metadata_shards(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Vec<DfsMetadataReplicationOutcome>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(Vec::new());
        };
        let shard_ids = DfsControlPlaneStore::new(control_plane.as_mut())
            .list_metadata_shards(file_system_id)?
            .into_iter()
            .filter(|shard| shard.primary_node_id == self.node_id)
            .map(|shard| shard.shard_id)
            .collect::<Vec<_>>();

        let mut outcomes = Vec::new();
        for shard_id in shard_ids {
            outcomes.extend(
                self.replicate_metadata_shard_to_configured_replicas(file_system_id, &shard_id)?,
            );
        }
        Ok(outcomes)
    }

    pub fn hydrate_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        if self.metadata_backend.is_none() {
            return Ok(());
        };

        let file_systems = self
            .metadata_backend
            .as_ref()
            .expect("metadata backend checked above")
            .list_file_systems()?;
        let mut intents = Vec::new();
        let mut receipts_by_intent =
            BTreeMap::<(DfsObjectId, String), Vec<DfsReplicaReceiptRecord>>::new();

        self.metadata = DfsMetadataStore::new();
        self.tombstones_by_metadata_path.clear();
        self.coordinator = DistributedEventualCoordinator::new();
        self.pending_writes.clear();
        self.pending_reads.clear();
        self.incoming_read_transfers.clear();
        self.incoming_fetch_transfers.clear();
        self.incoming_blob_transfers.clear();

        for file_system in &file_systems {
            self.metadata.add_file_system(file_system.clone());
        }
        if !self
            .metadata
            .filesystems
            .contains_key(&sifs_file_system_id())
        {
            self.metadata
                .add_file_system(DfsFileSystem::sifs(self.node_id.clone()));
        }

        for file_system in &file_systems {
            let mut cursor = None;
            loop {
                let page = self
                    .metadata_backend
                    .as_ref()
                    .expect("metadata backend checked above")
                    .objects_for_file_system_page(
                        &file_system.file_system_id,
                        cursor.as_ref(),
                        DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE,
                    )?;
                for object in page.objects {
                    self.coordinator.set_committed_version(
                        object.file_system_id.clone(),
                        object.object_id.clone(),
                        object.version.clone(),
                    );
                    self.track_hydrated_metadata_tombstone(&object);
                    self.metadata.add_object(object.clone())?;

                    self.add_backend_conflict_pages_to_memory(&object.object_id)?;
                    let mut intent_cursor = None;
                    loop {
                        let intent_page = self
                            .metadata_backend
                            .as_ref()
                            .expect("metadata backend checked above")
                            .intents_for_object_page(
                                &object.object_id,
                                intent_cursor.as_ref(),
                                DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                            )?;
                        for intent in intent_page.intents {
                            let mut receipt_cursor = None;
                            let mut receipts = Vec::new();
                            loop {
                                let receipt_page = self
                                    .metadata_backend
                                    .as_ref()
                                    .expect("metadata backend checked above")
                                    .receipts_for_version_page(
                                        &object.object_id,
                                        &intent.target_version,
                                        receipt_cursor.as_ref(),
                                        DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                                    )?;
                                receipts.extend(receipt_page.receipts);
                                let Some(next_cursor) = receipt_page.next_cursor else {
                                    break;
                                };
                                receipt_cursor = Some(next_cursor);
                            }
                            receipts_by_intent.insert(
                                (object.object_id.clone(), intent.intent_id.clone()),
                                receipts,
                            );
                            intents.push(intent);
                        }
                        let Some(next_cursor) = intent_page.next_cursor else {
                            break;
                        };
                        intent_cursor = Some(next_cursor);
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                cursor = Some(next_cursor);
            }
        }

        intents.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.intent_id.cmp(&right.intent_id))
        });
        let mut pending_write_records = Vec::new();
        for mut intent_record in intents {
            self.repair_visible_committed_pending_intent_record(&mut intent_record)?;
            let intent = distributed_intent_from_record(&intent_record);
            match self.coordinator.restore_intent(intent) {
                Ok(()) => {}
                Err(error @ DfsDistributedError::PendingIntentConflict { .. })
                    if matches!(
                        intent_record.status,
                        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                    ) =>
                {
                    let object = self.current_read_object_metadata(&intent_record.object_id)?;
                    self.create_write_conflict(
                        &object,
                        intent_record.base_version.clone(),
                        intent_record.target_version.clone(),
                        intent_record.coordinator_node_id.clone(),
                        intent_record.content_hash.clone(),
                        error.to_string(),
                        Some(intent_record.intent_id.clone()),
                    )?;
                    let mut conflict_record = intent_record.clone();
                    conflict_record.status = DfsWriteIntentStatus::Conflict;
                    conflict_record.updated_at = Some(OffsetDateTime::now_utc());
                    if let Some(backend) = self.metadata_backend.as_mut() {
                        backend.put_intent(&conflict_record)?;
                    }
                    self.coordinator
                        .restore_intent(distributed_intent_from_record(&conflict_record))?;
                    continue;
                }
                Err(error @ DfsDistributedError::DuplicateIntentId { .. }) => {
                    if matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                        && self
                            .current_read_object_metadata(&intent_record.object_id)
                            .is_ok_and(|object| {
                                object.version == intent_record.target_version
                                    && intent_record.content_hash.as_deref().is_none_or(
                                        |content_hash| {
                                            object.content_hash.as_deref() == Some(content_hash)
                                        },
                                    )
                            })
                    {
                        self.restore_visible_committed_object_location_receipts(&intent_record)?;
                        continue;
                    }
                    let object = self.current_read_object_metadata(&intent_record.object_id)?;
                    self.create_write_conflict(
                        &object,
                        intent_record.base_version.clone(),
                        intent_record.target_version.clone(),
                        intent_record.coordinator_node_id.clone(),
                        intent_record.content_hash.clone(),
                        error.to_string(),
                        Some(intent_record.intent_id.clone()),
                    )?;
                    let mut conflict_record = intent_record.clone();
                    conflict_record.status = DfsWriteIntentStatus::Conflict;
                    conflict_record.updated_at = Some(OffsetDateTime::now_utc());
                    if let Some(backend) = self.metadata_backend.as_mut() {
                        backend.put_intent(&conflict_record)?;
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
            if let Some(receipts) = receipts_by_intent.get(&(
                intent_record.object_id.clone(),
                intent_record.intent_id.clone(),
            )) {
                for receipt in receipts {
                    if let Some(expected_hash) = intent_record.content_hash.as_deref()
                        && receipt.content_hash.as_deref() != Some(expected_hash)
                    {
                        continue;
                    }
                    match self.coordinator.restore_durable_receipt(
                        &intent_record.intent_id,
                        receipt.replica_node_id.clone(),
                    ) {
                        Ok(()) => {}
                        Err(
                            DfsDistributedError::UnexpectedReplica { .. }
                            | DfsDistributedError::IntentClosed { .. },
                        ) => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            self.restore_visible_committed_object_location_receipts(&intent_record)?;
            if matches!(
                intent_record.status,
                DfsWriteIntentStatus::Staging
                    | DfsWriteIntentStatus::ReceiptsPending
                    | DfsWriteIntentStatus::Committed
            ) {
                pending_write_records.push(intent_record);
            }
        }

        let repair_intent_ids: BTreeSet<String> = self
            .coordinator
            .repair_jobs()
            .iter()
            .map(|job| job.intent_id.clone())
            .collect();
        let mut recovered_commit_ready_intent_ids = Vec::new();
        for intent_record in pending_write_records {
            let Some(restored_intent) = self.coordinator.intent(&intent_record.intent_id) else {
                continue;
            };
            if !distributed_intent_matches_record(restored_intent, &intent_record) {
                continue;
            }
            let needs_pending_write =
                matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) || (matches!(intent_record.status, DfsWriteIntentStatus::Committed)
                    && repair_intent_ids.contains(&intent_record.intent_id));
            if needs_pending_write && let Some(content_hash) = intent_record.content_hash.clone() {
                let size_bytes = self
                    .pending_write_size_hint(
                        &intent_record.object_id,
                        &intent_record.target_version,
                        &content_hash,
                    )?
                    .unwrap_or(0);
                self.pending_writes.insert(
                    intent_record.intent_id.clone(),
                    pending_networked_write(
                        intent_record.object_id.clone(),
                        intent_record.target_version.clone(),
                        content_hash,
                        size_bytes,
                    ),
                );
                if matches!(
                    intent_record.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) && self
                    .coordinator
                    .is_commit_ready(&intent_record.intent_id)
                    .unwrap_or(false)
                {
                    recovered_commit_ready_intent_ids.push(intent_record.intent_id.clone());
                }
            }
        }
        for intent_id in recovered_commit_ready_intent_ids {
            self.apply_committed_intent(&intent_id)?;
            self.cleanup_committed_pending_write_if_repaired(&intent_id);
        }

        Ok(())
    }

    fn repair_visible_committed_pending_intent_record(
        &mut self,
        intent_record: &mut DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !matches!(
            intent_record.status,
            DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
        ) {
            return Ok(());
        }
        let Some(object) = self.metadata.object(&intent_record.object_id) else {
            return Ok(());
        };
        if object.file_system_id != intent_record.file_system_id
            || object.version != intent_record.target_version
        {
            return Ok(());
        }
        if let Some(content_hash) = intent_record.content_hash.as_deref()
            && object.content_hash.as_deref() != Some(content_hash)
        {
            return Ok(());
        }

        intent_record.status = DfsWriteIntentStatus::Committed;
        intent_record.updated_at = Some(OffsetDateTime::now_utc());
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_intent(intent_record)?;
        }
        Ok(())
    }

    fn restore_visible_committed_object_location_receipts(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<(), DfsError> {
        if !matches!(intent_record.status, DfsWriteIntentStatus::Committed) {
            return Ok(());
        }
        let Some(object) = self.metadata.object(&intent_record.object_id).cloned() else {
            return Ok(());
        };
        if object.file_system_id != intent_record.file_system_id
            || object.version != intent_record.target_version
        {
            return Ok(());
        }
        if let Some(content_hash) = intent_record.content_hash.as_deref()
            && object.content_hash.as_deref() != Some(content_hash)
        {
            return Ok(());
        }
        let Some(content_hash) = object
            .content_hash
            .as_deref()
            .or(intent_record.content_hash.as_deref())
        else {
            return Ok(());
        };

        let mut existing_receipts = BTreeSet::new();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    &object.object_id,
                    &object.version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                for receipt in page.receipts {
                    if receipt.content_hash.as_deref() != Some(content_hash) {
                        continue;
                    }
                    existing_receipts.insert(receipt.replica_node_id.clone());
                    match self
                        .coordinator
                        .restore_durable_receipt(&intent_record.intent_id, receipt.replica_node_id)
                    {
                        Ok(()) => {}
                        Err(
                            DfsDistributedError::UnexpectedReplica { .. }
                            | DfsDistributedError::IntentClosed { .. },
                        ) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
        }

        let owner_node_id = object.owner_node_id.clone();
        for replica_node_id in object.current_locations {
            if !intent_record
                .desired_replica_node_ids
                .contains(&replica_node_id)
            {
                continue;
            }
            let has_verified_local_blob = replica_node_id == self.node_id
                && self.content.has_blob(content_hash).unwrap_or(false);
            if !existing_receipts.contains(&replica_node_id)
                && has_verified_local_blob
                && let Some(backend) = self.metadata_backend.as_mut()
            {
                backend.put_receipt(&DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: intent_record.file_system_id.clone(),
                        object_id: intent_record.object_id.clone(),
                        committed_version: intent_record.target_version.clone(),
                        owner_node_id: owner_node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.to_string()),
                    received_at: OffsetDateTime::now_utc(),
                })?;
                existing_receipts.insert(replica_node_id.clone());
            }
            if existing_receipts.contains(&replica_node_id) {
                match self
                    .coordinator
                    .restore_durable_receipt(&intent_record.intent_id, replica_node_id)
                {
                    Ok(()) => {}
                    Err(
                        DfsDistributedError::UnexpectedReplica { .. }
                        | DfsDistributedError::IntentClosed { .. },
                    ) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(())
    }

    fn apply_metadata_journal_batch_to_service_state(
        &mut self,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<(), DfsError> {
        let mut needs_full_hydration = false;
        for entry in &batch.entries {
            if self.apply_metadata_journal_entry_to_service_state(entry)? {
                needs_full_hydration = true;
                break;
            }
        }
        if needs_full_hydration {
            self.hydrate_from_metadata_backend()?;
        }
        Ok(())
    }

    /// Returns `true` when the entry cannot be applied from replay payloads alone
    /// and the caller should fall back to a full backend hydration.
    fn apply_metadata_journal_entry_to_service_state(
        &mut self,
        entry: &DfsMetadataJournalEntry,
    ) -> Result<bool, DfsError> {
        match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system, .. } => {
                let Some(file_system) = file_system.as_ref() else {
                    return Ok(true);
                };
                self.metadata.add_file_system(file_system.clone());
            }
            DfsMetadataMutation::PutObject { object, .. } => {
                let Some(object) = object.as_ref() else {
                    return Ok(true);
                };
                self.apply_metadata_object_to_service_state(object.clone())?;
            }
            DfsMetadataMutation::PutVersion { version_record, .. } => {
                let Some(version_record) = version_record.as_ref() else {
                    return Ok(true);
                };
                self.apply_metadata_version_to_service_state(version_record);
            }
            DfsMetadataMutation::PutReceipt {
                object_id,
                version,
                replica_node_id,
                receipt,
                ..
            } => {
                if receipt.is_none() {
                    return Ok(true);
                }
                self.apply_metadata_receipt_to_service_state(object_id, version, replica_node_id)?;
            }
            DfsMetadataMutation::PutIntent { intent, .. } => {
                let Some(intent) = intent.as_ref() else {
                    return Ok(true);
                };
                if self.apply_metadata_intent_to_service_state(intent)? {
                    return Ok(true);
                }
            }
            DfsMetadataMutation::CommitVisibleVersion {
                committed_object,
                visible_version,
                ..
            } => {
                let Some(committed_object) = committed_object.as_ref() else {
                    return Ok(true);
                };
                let Some(visible_version) = visible_version.as_ref() else {
                    return Ok(true);
                };
                self.apply_metadata_object_to_service_state(committed_object.clone())?;
                self.apply_metadata_version_to_service_state(visible_version);
            }
            DfsMetadataMutation::PutConflict { conflict, .. } => {
                let Some(conflict) = conflict.as_ref() else {
                    return Ok(true);
                };
                self.metadata.add_conflict(conflict.clone());
            }
            DfsMetadataMutation::ApplyNamespaceTransaction { transaction, .. } => {
                let Some(transaction) = transaction.as_ref() else {
                    return Ok(true);
                };
                let mut objects = transaction.objects.clone();
                objects.sort_by(|left, right| {
                    let left_is_tombstone = left.kind == DfsObjectKind::Tombstone;
                    let right_is_tombstone = right.kind == DfsObjectKind::Tombstone;
                    right_is_tombstone
                        .cmp(&left_is_tombstone)
                        .then_with(|| left.path.cmp(&right.path))
                        .then_with(|| left.object_id.cmp(&right.object_id))
                });
                for object in objects {
                    self.apply_metadata_object_to_service_state(object)?;
                }
                for version in &transaction.versions {
                    self.apply_metadata_version_to_service_state(version);
                }
            }
        }
        Ok(false)
    }

    fn apply_metadata_object_to_service_state(
        &mut self,
        mut object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        normalize_current_locations(&mut object.current_locations);
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            self.metadata.add_file_system(DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            });
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)?;
        Ok(())
    }

    fn apply_metadata_version_to_service_state(&mut self, version: &DfsObjectVersionRecord) {
        if !version.visible {
            return;
        }
        let Some(object) = self.metadata.objects.get_mut(&version.object_id) else {
            return;
        };
        if object.version != version.version {
            return;
        }
        if object.content_hash.is_none() {
            object.content_hash = version.content_hash.clone();
        }
        if version.size_bytes.is_some() {
            object.content_size_bytes = version.size_bytes;
        }
        if !version.locations.is_empty() {
            object.current_locations = version.locations.clone();
            normalize_current_locations(&mut object.current_locations);
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
    }

    fn apply_metadata_intent_to_service_state(
        &mut self,
        intent_record: &DfsWriteIntentRecord,
    ) -> Result<bool, DfsError> {
        let intent = distributed_intent_from_record(intent_record);
        match self.coordinator.restore_intent(intent) {
            Ok(()) => {}
            Err(error @ DfsDistributedError::PendingIntentConflict { .. }) => {
                eprintln!(
                    "warning: falling back to full DFS metadata hydration after journal intent conflict: {error}"
                );
                return Ok(true);
            }
            Err(error @ DfsDistributedError::DuplicateIntentId { .. }) => {
                if self
                    .coordinator
                    .intent(&intent_record.intent_id)
                    .is_some_and(|intent| distributed_intent_matches_record(intent, intent_record))
                {
                    // Same intent replayed after local state had already advanced.
                } else {
                    eprintln!(
                        "warning: falling back to full DFS metadata hydration after duplicate journal intent: {error}"
                    );
                    return Ok(true);
                }
            }
            Err(error) => return Err(error.into()),
        }

        if matches!(
            intent_record.status,
            DfsWriteIntentStatus::Staging
                | DfsWriteIntentStatus::ReceiptsPending
                | DfsWriteIntentStatus::Committed
        ) && let Some(content_hash) = intent_record.content_hash.clone()
            && !self.pending_writes.contains_key(&intent_record.intent_id)
        {
            let size_bytes = self
                .pending_write_size_hint(
                    &intent_record.object_id,
                    &intent_record.target_version,
                    &content_hash,
                )?
                .unwrap_or(0);
            self.pending_writes.insert(
                intent_record.intent_id.clone(),
                pending_networked_write(
                    intent_record.object_id.clone(),
                    intent_record.target_version.clone(),
                    content_hash,
                    size_bytes,
                ),
            );
        }
        Ok(false)
    }

    fn apply_metadata_receipt_to_service_state(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        let mut intent_ids = Vec::new();
        if let Some(backend) = self.metadata_backend.as_ref() {
            let mut intent_cursor = None;
            loop {
                let page = backend.intents_for_object_page(
                    object_id,
                    intent_cursor.as_ref(),
                    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                )?;
                intent_ids.extend(
                    page.intents
                        .into_iter()
                        .filter(|intent| intent.target_version == *version)
                        .map(|intent| intent.intent_id),
                );
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                intent_cursor = Some(next_cursor);
            }
        }
        for intent_id in intent_ids {
            match self
                .coordinator
                .restore_durable_receipt(&intent_id, replica_node_id.clone())
            {
                Ok(()) => {}
                Err(
                    DfsDistributedError::UnknownIntent { .. }
                    | DfsDistributedError::UnexpectedReplica { .. }
                    | DfsDistributedError::IntentClosed { .. },
                ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn pending_write_size_hint(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<Option<u64>, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes.is_some()
        {
            return Ok(version_record.size_bytes);
        }
        match self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
        {
            Ok(manifest) if manifest.content_hash == content_hash => Ok(Some(manifest.size_bytes)),
            Ok(_) => Ok(None),
            Err(ContentStoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) -> Result<(), DfsError> {
        if let Some(kv) = self.control_plane.as_mut() {
            let file_system_config = DfsFileSystemConfig::from(&file_system);
            let mut store = DfsControlPlaneStore::new(kv.as_mut());
            if store.get_file_system_config(&file_system.file_system_id)?
                != Some(file_system_config.clone())
            {
                store.put_file_system_config(&file_system_config)?;
            }
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            backend.put_file_system(&file_system)?;
        }
        self.metadata.add_file_system(file_system);
        Ok(())
    }

    pub fn add_object_metadata(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        self.put_visible_object_in_metadata_backend(&object)?;
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)
    }

    pub fn add_object_metadata_batch(
        &mut self,
        objects: impl IntoIterator<Item = DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let mut grouped =
            BTreeMap::<DfsFileSystemId, Vec<(DfsObjectMetadata, DfsObjectVersionRecord)>>::new();
        for mut object in objects {
            normalize_current_locations(&mut object.current_locations);
            let size_bytes = self.object_content_size_hint(&object)?;
            let version = object_version_record(&object, size_bytes);
            grouped
                .entry(object.file_system_id.clone())
                .or_default()
                .push((object, version));
        }
        if let Some(backend) = self.metadata_backend.as_mut() {
            for (file_system_id, records) in &grouped {
                if backend.get_file_system(file_system_id)?.is_none() {
                    let Some((object, _)) = records.first() else {
                        continue;
                    };
                    backend.put_file_system(&DfsFileSystem {
                        file_system_id: file_system_id.clone(),
                        display_name: file_system_id.to_string(),
                        owner_node_id: object.owner_node_id.clone(),
                        subtree_policies: Vec::new(),
                    })?;
                }

                let mut transaction_objects = Vec::new();
                let mut transaction_versions = Vec::new();
                for (object, version) in records {
                    if backend.get_object(&object.object_id)?.as_ref() != Some(object) {
                        transaction_objects.push(object.clone());
                    }
                    let mut version = version.clone();
                    if let Some(existing) =
                        backend.get_version(&version.object_id, &version.version)?
                    {
                        version.created_at = existing.created_at;
                        if existing == version {
                            continue;
                        }
                    }
                    transaction_versions.push(version);
                }
                if transaction_objects.is_empty() && transaction_versions.is_empty() {
                    continue;
                }
                backend.apply_namespace_transaction(&DfsNamespaceTransaction {
                    transaction_id: namespace_transaction_id_for_batch(
                        "visible-batch",
                        file_system_id,
                        &transaction_objects,
                        &transaction_versions,
                    ),
                    file_system_id: file_system_id.clone(),
                    objects: transaction_objects,
                    versions: transaction_versions,
                    created_at: OffsetDateTime::now_utc(),
                })?;
            }
        }
        for (_, records) in grouped {
            for (object, _) in records {
                self.coordinator.set_committed_version(
                    object.file_system_id.clone(),
                    object.object_id.clone(),
                    object.version.clone(),
                );
                self.track_metadata_tombstone(&object);
                self.metadata.add_object(object)?;
            }
        }
        Ok(())
    }

    pub fn add_transient_object_metadata(
        &mut self,
        object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.metadata.add_object(object)
    }

    fn track_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        let key = (object.file_system_id.clone(), metadata_path(&object.path));
        if object.kind == DfsObjectKind::Tombstone {
            let replace = self
                .tombstones_by_metadata_path
                .get(&key)
                .is_none_or(|existing| existing.version < object.version);
            if replace {
                self.tombstones_by_metadata_path.insert(key, object.clone());
            }
            return;
        }
        self.tombstones_by_metadata_path.remove(&key);
    }

    fn track_hydrated_metadata_tombstone(&mut self, object: &DfsObjectMetadata) {
        if object.kind != DfsObjectKind::Tombstone {
            self.track_metadata_tombstone(object);
            return;
        }
        let live_path = path_index_key(&object.file_system_id, &object.path).map(|(_, path)| path);
        if self
            .metadata
            .object_by_normalized_posix_path(
                &object.file_system_id,
                live_path.as_deref().unwrap_or(&object.path),
            )
            .is_none()
        {
            self.track_metadata_tombstone(object);
        }
    }

    pub fn remove_transient_object_metadata(&mut self, object_id: &DfsObjectId) {
        self.metadata.remove_object(object_id);
    }

    pub fn apply_namespace_transaction(
        &mut self,
        transaction: DfsNamespaceTransaction,
    ) -> Result<(), DfsError> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            if backend
                .get_file_system(&transaction.file_system_id)?
                .is_none()
            {
                backend.put_file_system(&DfsFileSystem {
                    file_system_id: transaction.file_system_id.clone(),
                    display_name: transaction.file_system_id.to_string(),
                    owner_node_id: self.node_id.clone(),
                    subtree_policies: Vec::new(),
                })?;
            }
            backend.apply_namespace_transaction(&transaction)?;
        }
        for object in transaction
            .objects
            .iter()
            .filter(|object| object.kind == DfsObjectKind::Tombstone)
            .chain(
                transaction
                    .objects
                    .iter()
                    .filter(|object| object.kind != DfsObjectKind::Tombstone),
            )
            .cloned()
        {
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            self.track_metadata_tombstone(&object);
            self.metadata.add_object(object)?;
        }
        Ok(())
    }

    pub fn put_local_object_version(
        &mut self,
        object: DfsObjectMetadata,
        bytes: &[u8],
    ) -> Result<String, DfsError> {
        let manifest = self.content.put_object_version(
            object.object_id.as_str(),
            object.version.as_str(),
            bytes,
        )?;
        self.publish_local_object_manifest(object, manifest)
    }

    pub fn put_local_object_version_from_reader<R: Read>(
        &mut self,
        object: DfsObjectMetadata,
        reader: R,
    ) -> Result<String, DfsError> {
        let manifest = self.content.put_object_version_from_reader(
            object.object_id.as_str(),
            object.version.as_str(),
            reader,
        )?;
        self.publish_local_object_manifest(object, manifest)
    }

    fn publish_local_object_manifest(
        &mut self,
        mut object: DfsObjectMetadata,
        manifest: ContentManifest,
    ) -> Result<String, DfsError> {
        object.content_hash = Some(manifest.content_hash.clone());
        object.content_size_bytes = Some(manifest.size_bytes);
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.put_visible_object_in_metadata_backend(&object)?;
        self.put_object_version_size_in_metadata_backend(&object, Some(manifest.size_bytes))?;
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        self.track_metadata_tombstone(&object);
        self.metadata.add_object(object)?;
        Ok(manifest.content_hash)
    }

    pub fn read_local_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Option<DfsReadData>, DfsError> {
        Ok(match self.read_local_object_with_state(object_id, false)? {
            NetworkedDfsReadOutcome::Found {
                data,
                freshness:
                    NetworkedDfsReadFreshness::Current | NetworkedDfsReadFreshness::Confirmed { .. },
            } => Some(data),
            NetworkedDfsReadOutcome::Found { .. }
            | NetworkedDfsReadOutcome::NotConfirmed { .. }
            | NetworkedDfsReadOutcome::PendingReplication { .. }
            | NetworkedDfsReadOutcome::Conflicted { .. }
            | NetworkedDfsReadOutcome::NotFound => None,
        })
    }

    pub fn request_remote_object_read(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<NodeId>, DfsError> {
        self.request_remote_object_read_inner(object_id, None)
    }

    pub fn request_remote_object_range_read(
        &mut self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
    ) -> Result<Vec<NodeId>, DfsError> {
        if max_bytes > DFS_BLOB_INLINE_LIMIT_BYTES {
            return Err(DfsError::Distributed {
                message: format!(
                    "remote DFS range read window {max_bytes} exceeds inline limit {DFS_BLOB_INLINE_LIMIT_BYTES}"
                ),
            });
        }
        self.request_remote_object_read_inner(object_id, Some((offset, max_bytes)))
    }

    fn request_remote_object_read_inner(
        &mut self,
        object_id: &DfsObjectId,
        range: Option<(u64, usize)>,
    ) -> Result<Vec<NodeId>, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        self.prune_stale_pending_remote_reads_for_object(&object);
        if object.content_hash.is_none() {
            return Ok(Vec::new());
        }

        let mut requested = Vec::new();
        let now = Instant::now();
        let mut seen_locations = BTreeSet::new();
        let mut expired_candidates = Vec::new();
        for node_id in &object.current_locations {
            if node_id == &self.node_id {
                continue;
            }
            if !seen_locations.insert(node_id.clone()) {
                continue;
            }
            if let Some((request_id, pending)) =
                self.pending_remote_read_for_object_target(&object, node_id, range)
            {
                if pending.next_retry_at > now {
                    requested.push(node_id.clone());
                    break;
                }
                if self.pending_remote_read_has_resume_state(&request_id, &pending)
                    && self.retry_remote_read_request_to(&request_id, &pending, node_id, now)?
                {
                    requested.push(node_id.clone());
                    break;
                }
                self.pending_reads.remove(&request_id);
                self.incoming_read_transfers.remove(&request_id);
                expired_candidates.push(node_id.clone());
                continue;
            }
            if self.send_remote_read_request_to(&object, node_id, now, range)? {
                requested.push(node_id.clone());
                break;
            }
        }
        if requested.is_empty() {
            for node_id in expired_candidates {
                if self.send_remote_read_request_to(&object, &node_id, now, range)? {
                    requested.push(node_id);
                    break;
                }
            }
        }
        Ok(requested)
    }

    fn send_remote_read_request_to(
        &mut self,
        object: &DfsObjectMetadata,
        target_node_id: &NodeId,
        now: Instant,
        range: Option<(u64, usize)>,
    ) -> Result<bool, DfsError> {
        let request_id = self.next_request_id();
        let (start_chunk_index, max_chunks, window_end_chunk_index) = match range {
            Some(_) => (0, 0, None),
            None => (
                0,
                DFS_READ_CHUNK_WINDOW_SIZE,
                Self::read_window_end_chunk_index(0, DFS_READ_CHUNK_WINDOW_SIZE),
            ),
        };
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: request_id.clone(),
                object_id: object.object_id.clone(),
                range_offset: range.map(|(offset, _)| offset),
                range_max_bytes: range.map(|(_, max_bytes)| max_bytes),
                start_chunk_index,
                max_chunks,
            }),
        )?;
        if sent {
            self.pending_reads.insert(
                request_id,
                PendingNetworkedRead {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: object.content_hash.clone(),
                    content_size_bytes: object.content_size_bytes,
                    range_offset: range.map(|(offset, _)| offset),
                    range_max_bytes: range.map(|(_, max_bytes)| max_bytes),
                    next_chunk_index: 0,
                    window_end_chunk_index,
                    target_node_id: target_node_id.clone(),
                    next_retry_at: now + DFS_REMOTE_READ_RETRY_DELAY,
                },
            );
        }
        Ok(sent)
    }

    fn pending_remote_read_has_resume_state(
        &self,
        request_id: &str,
        pending: &PendingNetworkedRead,
    ) -> bool {
        pending.range_offset.is_none()
            && pending.range_max_bytes.is_none()
            && (pending.next_chunk_index > 0
                || self.incoming_read_transfers.contains_key(request_id))
    }

    fn retry_remote_read_request_to(
        &mut self,
        request_id: &str,
        pending: &PendingNetworkedRead,
        target_node_id: &NodeId,
        now: Instant,
    ) -> Result<bool, DfsError> {
        let start_chunk_index = pending.next_chunk_index;
        let max_chunks = DFS_READ_CHUNK_WINDOW_SIZE;
        let sent = self.send_dfs_message_if_routable(
            target_node_id,
            DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: request_id.to_string(),
                object_id: pending.object_id.clone(),
                range_offset: None,
                range_max_bytes: None,
                start_chunk_index,
                max_chunks,
            }),
        )?;
        if sent && let Some(pending) = self.pending_reads.get_mut(request_id) {
            pending.target_node_id = target_node_id.clone();
            pending.next_retry_at = now + DFS_REMOTE_READ_RETRY_DELAY;
            pending.window_end_chunk_index =
                Self::read_window_end_chunk_index(start_chunk_index, max_chunks);
        }
        Ok(sent)
    }

    fn prune_stale_pending_remote_reads_for_object(&mut self, object: &DfsObjectMetadata) {
        let request_ids = self
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == object.object_id
                    && (pending.version != object.version
                        || pending.content_hash != object.content_hash
                        || pending.content_size_bytes != object.content_size_bytes
                        || !object.current_locations.contains(&pending.target_node_id))
            })
            .map(|(request_id, _)| request_id.clone())
            .collect::<Vec<_>>();
        for request_id in request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
        }
    }

    pub(crate) fn has_pending_remote_read_for_current_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        Ok(self.pending_reads.values().any(|pending| {
            pending.object_id == *object_id
                && pending.version == object.version
                && pending.content_hash == object.content_hash
                && pending.content_size_bytes == object.content_size_bytes
                && pending.range_offset.is_none()
                && pending.range_max_bytes.is_none()
                && object.current_locations.contains(&pending.target_node_id)
                && pending.next_retry_at > Instant::now()
        }))
    }

    fn pending_remote_read_for_object_target(
        &self,
        object: &DfsObjectMetadata,
        target_node_id: &NodeId,
        range: Option<(u64, usize)>,
    ) -> Option<(String, PendingNetworkedRead)> {
        let range_offset = range.map(|(offset, _)| offset);
        let range_max_bytes = range.map(|(_, max_bytes)| max_bytes);
        self.pending_reads
            .iter()
            .find(|(_, pending)| {
                pending.object_id == object.object_id
                    && pending.target_node_id == *target_node_id
                    && pending.version == object.version
                    && pending.content_hash == object.content_hash
                    && pending.content_size_bytes == object.content_size_bytes
                    && pending.range_offset == range_offset
                    && pending.range_max_bytes == range_max_bytes
            })
            .map(|(request_id, pending)| (request_id.clone(), pending.clone()))
    }

    fn current_read_object_metadata(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let memory_object = self.metadata.object(object_id).cloned();
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            None
        };
        Ok(self.resolve_authoritative_object_metadata(
            object_id,
            memory_object.as_ref(),
            backend_object.as_ref(),
        )?)
    }

    fn resolve_authoritative_object_metadata(
        &self,
        object_id: &DfsObjectId,
        memory_object: Option<&DfsObjectMetadata>,
        backend_object: Option<&DfsObjectMetadata>,
    ) -> Result<DfsObjectMetadata, DfsError> {
        match (memory_object, backend_object) {
            (Some(memory_object), Some(backend_object))
                if self.metadata_backend.is_some()
                    && self.local_write_view_overrides_backend(
                        object_id,
                        memory_object,
                        Some(backend_object),
                    ) =>
            {
                Ok(memory_object.clone())
            }
            (Some(memory_object), Some(backend_object))
                if self.metadata_backend.is_none()
                    && memory_object.version > backend_object.version =>
            {
                Ok(memory_object.clone())
            }
            (_, Some(backend_object)) => Ok(backend_object.clone()),
            (Some(memory_object), None) => Ok(memory_object.clone()),
            (None, None) => Err(DfsError::UnknownObject {
                object_id: object_id.clone(),
            }),
        }
    }

    fn local_write_view_overrides_backend(
        &self,
        object_id: &DfsObjectId,
        memory_object: &DfsObjectMetadata,
        backend_object: Option<&DfsObjectMetadata>,
    ) -> bool {
        match backend_object {
            None => true,
            Some(backend_object) if memory_object.version > backend_object.version => {
                self.has_pending_write_targeting(object_id, &memory_object.version)
                    || self.has_awaiting_intent_targeting(object_id, &memory_object.version)
            }
            Some(backend_object)
                if memory_object.kind != DfsObjectKind::Tombstone
                    && backend_object.kind == DfsObjectKind::Tombstone
                    && memory_object.version >= backend_object.version =>
            {
                self.has_pending_write_targeting(object_id, &memory_object.version)
                    || self.has_awaiting_intent_targeting(object_id, &memory_object.version)
                    || memory_object.kind != DfsObjectKind::Tombstone
            }
            _ => false,
        }
    }

    fn has_pending_write_targeting(&self, object_id: &DfsObjectId, version: &DfsVersion) -> bool {
        self.pending_writes
            .values()
            .any(|pending| &pending.object_id == object_id && &pending.version == version)
    }

    fn has_awaiting_intent_targeting(&self, object_id: &DfsObjectId, version: &DfsVersion) -> bool {
        self.coordinator
            .has_awaiting_intent_targeting(object_id, version)
    }

    pub fn read_local_object_with_state(
        &self,
        object_id: &DfsObjectId,
        allow_stale: bool,
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let open_conflicts = self.open_conflicts_for_object(object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsReadOutcome::Conflicted {
                object_id: object.object_id.clone(),
                visible_version: object.version.clone(),
                visible_content_hash: object.content_hash.clone(),
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok((manifest, bytes)) = self
            .content
            .read_object_version(object.object_id.as_str(), object.version.as_str())
            && object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            return Ok(NetworkedDfsReadOutcome::Found {
                data: DfsReadData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(manifest.content_hash),
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(bytes) = self.content.read_blob(expected_hash)
        {
            return Ok(NetworkedDfsReadOutcome::Found {
                data: DfsReadData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(expected_hash.clone()),
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if allow_stale {
            let stale_read = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .filter(|manifest| manifest.version != object.version.as_str())
                .find_map(|manifest| {
                    self.content
                        .read_object_version(&manifest.object_id, &manifest.version)
                        .ok()
                });
            if let Some((manifest, bytes)) = stale_read {
                return Ok(NetworkedDfsReadOutcome::Found {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: DfsVersion::deserialize_migrating_legacy(manifest.version.clone())
                            .map_err(|error| DfsError::ContentStore {
                                message: error.to_string(),
                            })?,
                        content_hash: Some(manifest.content_hash.clone()),
                        bytes,
                    },
                    freshness: NetworkedDfsReadFreshness::Stale {
                        current_version: object.version.clone(),
                        current_content_hash: object.content_hash.clone(),
                    },
                });
            }
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            })
        } else {
            Ok(NetworkedDfsReadOutcome::NotFound)
        }
    }

    fn open_local_current_object_stream_with_state(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<NetworkedDfsReadStreamOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let open_conflicts = self.open_conflicts_for_object(object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsReadStreamOutcome::Conflicted {
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok((manifest, reader)) = self
            .content
            .open_object_version_stream(object.object_id.as_str(), object.version.as_str())
            && object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(manifest.content_hash),
                        bytes: Vec::new(),
                    },
                    total_size_bytes: manifest.size_bytes,
                    reader,
                },
            ));
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(reader) = self.content.open_blob_stream(expected_hash)
            && object
                .content_size_bytes
                .is_none_or(|expected_size| expected_size == reader.size_bytes())
        {
            let total_size_bytes = reader.size_bytes();
            let reader = ObjectVersionReader::from_blob(reader);
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(expected_hash.clone()),
                        bytes: Vec::new(),
                    },
                    total_size_bytes,
                    reader,
                },
            ));
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsReadStreamOutcome::PendingReplication)
        } else {
            Ok(NetworkedDfsReadStreamOutcome::NotFound)
        }
    }

    fn open_local_current_object_read_window_stream_with_state(
        &self,
        object_id: &DfsObjectId,
        start_chunk_index: u32,
        max_chunks: u32,
    ) -> Result<NetworkedDfsReadStreamOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let open_conflicts = self.open_conflicts_for_object(object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsReadStreamOutcome::Conflicted {
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
            && object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            let range_len =
                read_response_window_range_len(start_chunk_index, manifest.size_bytes, max_chunks)?;
            let (manifest, reader) = self.content.open_object_version_range_stream(
                object.object_id.as_str(),
                object.version.as_str(),
                u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64,
                range_len,
            )?;
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(manifest.content_hash),
                        bytes: Vec::new(),
                    },
                    total_size_bytes: manifest.size_bytes,
                    reader,
                },
            ));
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(reader) = self.content.open_blob_stream(expected_hash)
            && object
                .content_size_bytes
                .is_none_or(|expected_size| expected_size == reader.size_bytes())
        {
            let total_size_bytes = reader.size_bytes();
            let range_len =
                read_response_window_range_len(start_chunk_index, total_size_bytes, max_chunks)?;
            let reader = self.content.open_blob_range_stream(
                expected_hash,
                u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64,
                range_len,
            )?;
            return Ok(NetworkedDfsReadStreamOutcome::Found(
                NetworkedDfsReadStream {
                    data: DfsReadData {
                        object_id: object.object_id.clone(),
                        version: object.version.clone(),
                        content_hash: Some(expected_hash.clone()),
                        bytes: Vec::new(),
                    },
                    total_size_bytes,
                    reader: ObjectVersionReader::from_blob_range(reader),
                },
            ));
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsReadStreamOutcome::PendingReplication)
        } else {
            Ok(NetworkedDfsReadStreamOutcome::NotFound)
        }
    }

    pub fn read_local_object_range_with_state(
        &self,
        object_id: &DfsObjectId,
        offset: u64,
        max_bytes: usize,
        allow_stale: bool,
    ) -> Result<NetworkedDfsRangeReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let open_conflicts = self.open_conflicts_for_object(object_id)?;
        if !open_conflicts.is_empty() {
            return Ok(NetworkedDfsRangeReadOutcome::Conflicted {
                object_id: object.object_id.clone(),
                visible_version: object.version.clone(),
                visible_content_hash: object.content_hash.clone(),
                conflicts: open_conflicts
                    .into_iter()
                    .map(DfsConflictReadView::from)
                    .collect(),
            });
        }

        if let Ok((manifest, bytes)) = self.content.read_object_version_range(
            object.object_id.as_str(),
            object.version.as_str(),
            offset,
            max_bytes,
        ) && object
            .content_hash
            .as_ref()
            .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
        {
            return Ok(NetworkedDfsRangeReadOutcome::Found {
                data: DfsReadRangeData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(manifest.content_hash),
                    offset: offset.min(manifest.size_bytes),
                    total_size_bytes: manifest.size_bytes,
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Ok(mut reader) =
                self.content
                    .open_blob_range_stream(expected_hash, offset, max_bytes)
        {
            let total_size_bytes = reader.size_bytes();
            let range_offset = reader.offset();
            let mut bytes =
                Vec::with_capacity(usize::try_from(reader.remaining_bytes()).unwrap_or(usize::MAX));
            reader
                .read_to_end(&mut bytes)
                .map_err(|source| DfsError::ContentStore {
                    message: source.to_string(),
                })?;
            return Ok(NetworkedDfsRangeReadOutcome::Found {
                data: DfsReadRangeData {
                    object_id: object.object_id.clone(),
                    version: object.version.clone(),
                    content_hash: Some(expected_hash.clone()),
                    offset: range_offset,
                    total_size_bytes,
                    bytes,
                },
                freshness: NetworkedDfsReadFreshness::Current,
            });
        }

        if allow_stale {
            let stale_read = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .filter(|manifest| manifest.version != object.version.as_str())
                .find_map(|manifest| {
                    self.content
                        .read_object_version_range(
                            &manifest.object_id,
                            &manifest.version,
                            offset,
                            max_bytes,
                        )
                        .ok()
                });
            if let Some((manifest, bytes)) = stale_read {
                return Ok(NetworkedDfsRangeReadOutcome::Found {
                    data: DfsReadRangeData {
                        object_id: object.object_id.clone(),
                        version: DfsVersion::deserialize_migrating_legacy(manifest.version.clone())
                            .map_err(|error| DfsError::ContentStore {
                                message: error.to_string(),
                            })?,
                        content_hash: Some(manifest.content_hash.clone()),
                        offset: offset.min(manifest.size_bytes),
                        total_size_bytes: manifest.size_bytes,
                        bytes,
                    },
                    freshness: NetworkedDfsReadFreshness::Stale {
                        current_version: object.version.clone(),
                        current_content_hash: object.content_hash.clone(),
                    },
                });
            }
        }

        if object.content_hash.is_some() {
            Ok(NetworkedDfsRangeReadOutcome::PendingReplication {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                current_locations: object.current_locations.clone(),
            })
        } else {
            Ok(NetworkedDfsRangeReadOutcome::NotFound)
        }
    }

    pub fn read_local_object_confirmed(
        &self,
        object_id: &DfsObjectId,
        required_replica_node_ids: &[NodeId],
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }

        let confirmed_locations =
            self.confirmed_locations_for_object(&object, required_replica_node_ids)?;
        let confirmed_set: BTreeSet<&NodeId> = confirmed_locations.iter().collect();
        let missing_required = required_replica_node_ids
            .iter()
            .any(|node_id| !confirmed_set.contains(node_id));
        if missing_required {
            return Ok(NetworkedDfsReadOutcome::NotConfirmed {
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                confirmed_locations,
                required_locations: required_replica_node_ids.to_vec(),
            });
        }

        match self.read_local_object_with_state(object_id, false)? {
            NetworkedDfsReadOutcome::Found { data, .. } => Ok(NetworkedDfsReadOutcome::Found {
                data,
                freshness: NetworkedDfsReadFreshness::Confirmed {
                    replica_node_ids: confirmed_locations,
                },
            }),
            other => Ok(other),
        }
    }

    pub fn plan_distributed_eventual_replicas(
        &self,
        request: &DfsWriteRequest,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<DfsReplicaPlacementPlan, DfsError> {
        let candidates = candidate_nodes
            .into_iter()
            .map(DfsReplicaCandidate::new)
            .collect();
        Ok(plan_replicas(&DfsReplicaPlacementRequest {
            file_system_id: request.file_system_id.clone(),
            object_id: request.object_id.clone(),
            target_version: request.target_version.clone(),
            coordinator_node_id: self.node_id.clone(),
            policy,
            candidates,
        })?)
    }

    pub fn write_distributed_eventual_with_candidates(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual(request, bytes, plan.desired_replica_nodes, policy)
    }

    pub fn write_distributed_eventual_from_reader_with_candidates<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        candidate_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual_from_reader(
            request,
            reader,
            plan.desired_replica_nodes,
            policy,
        )
    }

    pub fn write_replicated_confirmed(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.ensure_replicated_confirmed_object(&request.object_id)?;
        let policy = DfsReplicationPolicy::confirmed(desired_replica_nodes.len())?;
        self.write_distributed_eventual(request, bytes, desired_replica_nodes, policy)
    }

    pub fn write_replicated_confirmed_with_candidates(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        candidate_nodes: Vec<NodeId>,
        desired_replicas: usize,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        self.ensure_replicated_confirmed_object(&request.object_id)?;
        let policy = DfsReplicationPolicy::confirmed(desired_replicas)?;
        let plan =
            self.plan_distributed_eventual_replicas(&request, candidate_nodes, policy.clone())?;
        self.write_distributed_eventual(request, bytes, plan.desired_replica_nodes, policy)
    }

    pub fn write_distributed_eventual(
        &mut self,
        request: DfsWriteRequest,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        self.write_distributed_eventual_with_base(
            request,
            Some(object.version),
            bytes,
            desired_replica_nodes,
            policy,
        )
    }

    pub fn write_distributed_eventual_from_reader<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        self.write_distributed_eventual_from_reader_with_base(
            request,
            Some(object.version),
            reader,
            desired_replica_nodes,
            policy,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_distributed_eventual_patch_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        bytes: &[u8],
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if let Some(outcome) = self.write_self_only_eventual_patch_with_base(
            request.clone(),
            expected_base_version.clone(),
            offset,
            bytes,
            desired_replica_nodes.clone(),
            policy.clone(),
        )? {
            return Ok(Some(outcome));
        }
        if bytes.is_empty() || bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES {
            return Ok(None);
        }

        let object = self.current_object_for_write(&request.object_id)?;
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Ok(None);
        }
        if expected_base_version != object.version {
            return Ok(None);
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            DfsReplicationPolicy::new(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => return Ok(None),
            Some(shard) => shard.epoch,
            None => 0,
        };

        if self
            .ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )
            .is_err()
        {
            return Ok(None);
        }
        if self
            .coordinator
            .validate_new_intent(
                &request.file_system_id,
                &request.object_id,
                Some(&expected_base_version),
                &request.target_version,
                &desired_replica_nodes,
                &policy,
            )
            .is_err()
        {
            return Ok(None);
        }

        let manifest = match self.content.put_object_version_patch(
            request.object_id.as_str(),
            expected_base_version.as_str(),
            request.target_version.as_str(),
            offset,
            bytes,
        ) {
            Ok(manifest) => manifest,
            Err(_) => return Ok(None),
        };
        let intent = match self.coordinator.create_intent_at_epoch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            Some(expected_base_version.clone()),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_creates_conflict(&error) =>
            {
                let conflict = self.create_write_conflict(
                    &object,
                    Some(expected_base_version),
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(manifest.content_hash),
                    error.to_string(),
                    None,
                )?;
                return Ok(Some(NetworkedDfsWriteOutcome::Conflict { conflict }));
            }
            Err(error) => return Err(error.into()),
        };
        self.put_metadata_intent(&intent, Some(&manifest.content_hash))?;
        let patch_hash = content_hash(bytes);
        self.pending_writes.insert(
            intent.intent_id.clone(),
            pending_networked_write_with_patch(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
                Some(PendingNetworkedWritePatch {
                    base_version: expected_base_version.clone(),
                    offset,
                    patch_hash: patch_hash.clone(),
                    bytes: bytes.to_vec(),
                    full_stage_fallback_nodes: BTreeSet::new(),
                }),
            ),
        );

        let updated_intent = if desired_replica_nodes.contains(&self.node_id) {
            let local_node_id = self.node_id.clone();
            self.put_metadata_receipt(
                &request.file_system_id,
                &request.object_id,
                &request.target_version,
                &local_node_id,
                &manifest.content_hash,
            )?;
            self.coordinator
                .record_durable_receipt(&intent.intent_id, self.node_id.clone())?
        } else {
            intent
        };

        self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;
        if !matches!(
            self.coordinator
                .intent(&updated_intent.intent_id)
                .map(|intent| &intent.state),
            Some(DfsWriteIntentState::Committed)
        ) {
            let stage_targets = desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id)
                .cloned()
                .collect::<Vec<_>>();
            for replica_node_id in stage_targets {
                let request_id = self.next_request_id();
                let sent = self.send_stage_patch_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &updated_intent.intent_id,
                    &request.file_system_id,
                    &request.object_id,
                    &expected_base_version,
                    &request.target_version,
                    &manifest.content_hash,
                    offset,
                    &patch_hash,
                    bytes,
                    Some(object.clone()),
                )?;
                if sent {
                    self.record_stage_inflight(
                        &updated_intent.intent_id,
                        &replica_node_id,
                        request_id,
                    );
                } else {
                    self.record_stage_retry_scheduled(&updated_intent.intent_id, &replica_node_id);
                }
            }
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(Some(outcome))
    }

    pub(crate) fn write_self_only_eventual_patch_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: DfsVersion,
        offset: u64,
        bytes: &[u8],
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<Option<NetworkedDfsWriteOutcome>, DfsError> {
        if bytes.is_empty() {
            return Ok(None);
        }
        let object = self.current_object_for_write(&request.object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedEventual {
            return Ok(None);
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            DfsReplicationPolicy::new(
                policy.min_commit_replicas.min(desired_replicas),
                desired_replicas,
            )?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if policy.min_commit_replicas != 1
            || desired_replica_nodes.len() != 1
            || desired_replica_nodes[0] != self.node_id
            || object.owner_node_id != self.node_id
        {
            return Ok(None);
        }
        if self
            .select_metadata_shard_for_object(&object)?
            .is_some_and(|shard| shard.primary_node_id != self.node_id)
        {
            return Ok(None);
        }
        if expected_base_version != object.version {
            return Ok(None);
        }
        if self
            .ensure_no_backend_pending_write_intent(
                &request.object_id,
                &request.target_version,
                None,
            )
            .is_err()
        {
            return Ok(None);
        }
        if self
            .coordinator
            .validate_new_intent(
                &request.file_system_id,
                &request.object_id,
                Some(&expected_base_version),
                &request.target_version,
                &desired_replica_nodes,
                &policy,
            )
            .is_err()
        {
            return Ok(None);
        }

        let manifest = match self.content.put_object_version_patch(
            request.object_id.as_str(),
            expected_base_version.as_str(),
            request.target_version.as_str(),
            offset,
            bytes,
        ) {
            Ok(manifest) => manifest,
            Err(_) => return Ok(None),
        };
        let mut committed_object = object.clone();
        let write_time = dfs_now_unix_nanos();
        committed_object.version = request.target_version.clone();
        committed_object.content_hash = Some(manifest.content_hash.clone());
        committed_object.content_size_bytes = Some(manifest.size_bytes);
        committed_object.modified_at_unix_nanos = write_time;
        committed_object.changed_at_unix_nanos = write_time;
        committed_object.current_locations = vec![self.node_id.clone()];
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        self.put_visible_object_in_metadata_backend(&committed_object)?;
        self.put_object_version_size_in_metadata_backend(
            &committed_object,
            Some(manifest.size_bytes),
        )?;
        self.coordinator.set_committed_version(
            committed_object.file_system_id.clone(),
            committed_object.object_id.clone(),
            committed_object.version.clone(),
        );
        self.track_metadata_tombstone(&committed_object);
        self.metadata.add_object(committed_object)?;
        Ok(Some(NetworkedDfsWriteOutcome::Committed {
            intent_id: format!(
                "local-eventual-patch:{}:{}",
                request.object_id, request.target_version
            ),
            version: request.target_version,
            content_hash: manifest.content_hash,
            repair_jobs: Vec::new(),
        }))
    }

    pub fn write_distributed_eventual_from_reader_with_base<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            DfsReplicationPolicy::new(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => {
                return self.route_distributed_write_from_reader_to_metadata_primary(
                    request,
                    expected_base_version,
                    reader,
                    desired_replica_nodes,
                    policy,
                    object,
                    shard,
                );
            }
            Some(shard) => shard.epoch,
            None => 0,
        };

        if expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &object.version)
        {
            let local_content_hash =
                self.store_conflicting_write_reader_content_hash(&request, reader)?;
            let conflict = self.create_write_conflict(
                &object,
                expected_base_version,
                request.target_version.clone(),
                request.requesting_node_id.clone(),
                local_content_hash,
                "base version conflict for distributed write".to_string(),
                None,
            )?;
            return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
        }

        if let Err(error) = self.ensure_no_backend_pending_write_intent(
            &request.object_id,
            &request.target_version,
            None,
        ) {
            if object.policy == DfsObjectPolicy::ReplicatedEventual
                && distributed_error_creates_conflict(&error)
            {
                let local_content_hash =
                    self.store_conflicting_write_reader_content_hash(&request, reader)?;
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    local_content_hash,
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            return Err(error.into());
        }

        if let Err(error) = self.coordinator.validate_new_intent(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
        ) {
            if object.policy == DfsObjectPolicy::ReplicatedEventual
                && distributed_error_creates_conflict(&error)
            {
                let local_content_hash =
                    self.store_conflicting_write_reader_content_hash(&request, reader)?;
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    local_content_hash,
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            return Err(error.into());
        }

        let manifest = self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        )?;

        if object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
            && object.owner_node_id == self.node_id
        {
            let mut committed_object = object.clone();
            let write_time = dfs_now_unix_nanos();
            committed_object.version = request.target_version.clone();
            committed_object.content_hash = Some(manifest.content_hash.clone());
            committed_object.content_size_bytes = Some(manifest.size_bytes);
            committed_object.modified_at_unix_nanos = write_time;
            committed_object.changed_at_unix_nanos = write_time;
            committed_object.current_locations = vec![self.node_id.clone()];
            committed_object.last_modified_by_event_id = Some(EventId::generate());
            self.put_visible_object_in_metadata_backend(&committed_object)?;
            self.put_object_version_size_in_metadata_backend(
                &committed_object,
                Some(manifest.size_bytes),
            )?;
            self.coordinator.set_committed_version(
                committed_object.file_system_id.clone(),
                committed_object.object_id.clone(),
                committed_object.version.clone(),
            );
            self.track_metadata_tombstone(&committed_object);
            self.metadata.add_object(committed_object)?;
            return Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: format!(
                    "local-eventual:{}:{}",
                    request.object_id, request.target_version
                ),
                version: request.target_version,
                content_hash: manifest.content_hash,
                repair_jobs: Vec::new(),
            });
        }

        let intent = match self.coordinator.create_intent_at_epoch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            expected_base_version.clone(),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_creates_conflict(&error) =>
            {
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(manifest.content_hash.clone()),
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            Err(error) => return Err(error.into()),
        };
        self.put_metadata_intent(&intent, Some(&manifest.content_hash))?;
        self.pending_writes.insert(
            intent.intent_id.clone(),
            pending_networked_write(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
            ),
        );

        let updated_intent = if desired_replica_nodes.contains(&self.node_id) {
            let local_node_id = self.node_id.clone();
            self.put_metadata_receipt(
                &request.file_system_id,
                &request.object_id,
                &request.target_version,
                &local_node_id,
                &manifest.content_hash,
            )?;
            self.coordinator
                .record_durable_receipt(&intent.intent_id, self.node_id.clone())?
        } else {
            intent
        };

        self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;

        if !matches!(
            self.coordinator
                .intent(&updated_intent.intent_id)
                .map(|intent| &intent.state),
            Some(DfsWriteIntentState::Committed)
        ) {
            let stage_targets: Vec<NodeId> = desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id)
                .cloned()
                .collect();
            for replica_node_id in stage_targets {
                let request_id = self.next_request_id();
                let outcome = self.send_stage_blob_window_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &updated_intent.intent_id,
                    &request.file_system_id,
                    &request.object_id,
                    updated_intent.base_version.clone(),
                    &request.target_version,
                    &manifest.content_hash,
                    manifest.size_bytes,
                    0,
                    DFS_STAGE_CHUNK_WINDOW_SIZE,
                    Some(object.clone()),
                )?;
                if outcome.sent {
                    self.record_stage_window_progress(
                        &updated_intent.intent_id,
                        &replica_node_id,
                        request_id,
                        outcome,
                    );
                } else {
                    self.record_stage_retry_scheduled(&updated_intent.intent_id, &replica_node_id);
                }
            }
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(outcome)
    }

    fn write_distributed_eventual_from_remote_manifest_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        content_hash: String,
        content_size_bytes: u64,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes);
        if !desired_replica_nodes.contains(&request.requesting_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "reader-backed routed write for {} must include requesting node {} as a desired replica",
                    request.object_id, request.requesting_node_id
                ),
            });
        }
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            DfsReplicationPolicy::new(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => {
                return Err(DfsError::StaleOwnerEpoch {
                    file_system_id: object.file_system_id,
                    local_node_id: self.node_id.clone(),
                    primary_node_id: shard.primary_node_id,
                    epoch: shard.epoch,
                });
            }
            Some(shard) => shard.epoch,
            None => 0,
        };

        if expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &object.version)
        {
            let conflict = self.create_write_conflict(
                &object,
                expected_base_version,
                request.target_version.clone(),
                request.requesting_node_id.clone(),
                Some(content_hash),
                "base version conflict for distributed write".to_string(),
                None,
            )?;
            return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
        }

        if let Err(error) = self.ensure_no_backend_pending_write_intent(
            &request.object_id,
            &request.target_version,
            None,
        ) {
            if object.policy == DfsObjectPolicy::ReplicatedEventual
                && distributed_error_creates_conflict(&error)
            {
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(content_hash),
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            return Err(error.into());
        }

        if let Err(error) = self.coordinator.validate_new_intent(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
        ) {
            if object.policy == DfsObjectPolicy::ReplicatedEventual
                && distributed_error_creates_conflict(&error)
            {
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(content_hash),
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            return Err(error.into());
        }

        let intent = match self.coordinator.create_intent_at_epoch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            expected_base_version.clone(),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_creates_conflict(&error) =>
            {
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(content_hash),
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            Err(error) => return Err(error.into()),
        };
        self.put_metadata_intent(&intent, Some(&content_hash))?;
        self.pending_writes.insert(
            intent.intent_id.clone(),
            pending_networked_write(
                request.object_id.clone(),
                request.target_version.clone(),
                content_hash.clone(),
                content_size_bytes,
            ),
        );

        self.put_metadata_receipt(
            &request.file_system_id,
            &request.object_id,
            &request.target_version,
            &request.requesting_node_id,
            &content_hash,
        )?;
        let updated_intent = self
            .coordinator
            .record_durable_receipt(&intent.intent_id, request.requesting_node_id.clone())?;
        self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;

        let is_committed = matches!(
            self.coordinator
                .intent(&updated_intent.intent_id)
                .map(|intent| &intent.state),
            Some(DfsWriteIntentState::Committed)
        );
        let local_desired_copy_missing = desired_replica_nodes.contains(&self.node_id)
            && self
                .local_object_version_manifest_size_if_matches(
                    &request.object_id,
                    &request.target_version,
                    &content_hash,
                )
                .is_none();
        if (!is_committed || local_desired_copy_missing)
            && let Some(pending) = self.pending_writes.get(&updated_intent.intent_id).cloned()
        {
            let _ = self.request_stage_blob_from_alternate(&updated_intent, &pending)?;
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(outcome)
    }

    pub fn write_distributed_eventual_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self.current_object_for_write(&request.object_id)?;
        if !matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy,
            });
        }
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        let desired_replica_nodes =
            self.normalize_desired_replica_nodes(&object, desired_replica_nodes);
        let policy = if desired_replica_nodes.len() > policy.desired_replicas {
            let desired_replicas = desired_replica_nodes.len();
            // Replicated-confirmed requires every desired replica to confirm before
            // commit. When normalization widens the replica set (e.g. by injecting
            // the owner node), the commit threshold must grow with it; otherwise the
            // write would commit before all desired replicas are durable and silently
            // degrade to eventual semantics.
            let min_commit_replicas = if object.policy == DfsObjectPolicy::ReplicatedConfirmed {
                desired_replicas
            } else {
                policy.min_commit_replicas.min(desired_replicas)
            };
            DfsReplicationPolicy::new(min_commit_replicas, desired_replicas)?
        } else {
            policy
        };
        validate_replication_plan(&desired_replica_nodes, &policy)?;
        if object.policy == DfsObjectPolicy::ReplicatedConfirmed && !policy.is_strict_confirmed() {
            return Err(DfsError::InvalidReplicationPolicy {
                message:
                    "replicated-confirmed writes require min_commit_replicas == desired_replicas"
                        .to_string(),
            });
        }
        let owner_epoch = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if shard.primary_node_id != self.node_id => {
                return self.route_distributed_write_to_metadata_primary(
                    request,
                    expected_base_version,
                    bytes,
                    desired_replica_nodes,
                    policy,
                    object,
                    shard,
                );
            }
            Some(shard) => shard.epoch,
            None => 0,
        };

        if expected_base_version
            .as_ref()
            .is_some_and(|expected| expected != &object.version)
        {
            let local_content_hash = self.store_conflicting_write_content_hash(&request, &bytes)?;
            let conflict = self.create_write_conflict(
                &object,
                expected_base_version,
                request.target_version.clone(),
                request.requesting_node_id.clone(),
                local_content_hash,
                "base version conflict for distributed write".to_string(),
                None,
            )?;
            return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
        }

        if let Err(error) = self.ensure_no_backend_pending_write_intent(
            &request.object_id,
            &request.target_version,
            None,
        ) {
            if object.policy == DfsObjectPolicy::ReplicatedEventual
                && distributed_error_creates_conflict(&error)
            {
                let local_content_hash =
                    self.store_conflicting_write_content_hash(&request, &bytes)?;
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    local_content_hash,
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            return Err(error.into());
        }

        if let Err(error) = self.coordinator.validate_new_intent(
            &request.file_system_id,
            &request.object_id,
            expected_base_version.as_ref(),
            &request.target_version,
            &desired_replica_nodes,
            &policy,
        ) {
            if object.policy == DfsObjectPolicy::ReplicatedEventual
                && distributed_error_creates_conflict(&error)
            {
                let local_content_hash =
                    self.store_conflicting_write_content_hash(&request, &bytes)?;
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    local_content_hash,
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            return Err(error.into());
        }

        if object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
            && object.owner_node_id == self.node_id
        {
            let mut committed_object = object.clone();
            let manifest = self.content.put_object_version(
                request.object_id.as_str(),
                request.target_version.as_str(),
                &bytes,
            )?;
            let write_time = dfs_now_unix_nanos();
            committed_object.version = request.target_version.clone();
            committed_object.content_hash = Some(manifest.content_hash.clone());
            committed_object.content_size_bytes = Some(manifest.size_bytes);
            committed_object.modified_at_unix_nanos = write_time;
            committed_object.changed_at_unix_nanos = write_time;
            committed_object.current_locations = vec![self.node_id.clone()];
            committed_object.last_modified_by_event_id = Some(EventId::generate());
            self.put_visible_object_in_metadata_backend(&committed_object)?;
            self.put_object_version_size_in_metadata_backend(
                &committed_object,
                Some(manifest.size_bytes),
            )?;
            self.coordinator.set_committed_version(
                committed_object.file_system_id.clone(),
                committed_object.object_id.clone(),
                committed_object.version.clone(),
            );
            self.track_metadata_tombstone(&committed_object);
            self.metadata.add_object(committed_object)?;
            return Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: format!(
                    "local-eventual:{}:{}",
                    request.object_id, request.target_version
                ),
                version: request.target_version,
                content_hash: manifest.content_hash,
                repair_jobs: Vec::new(),
            });
        }

        let manifest = self.content.put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            &bytes,
        )?;
        let intent = match self.coordinator.create_intent_at_epoch(
            request.file_system_id.clone(),
            request.object_id.clone(),
            expected_base_version.clone(),
            request.target_version.clone(),
            request.requesting_node_id.clone(),
            owner_epoch,
            desired_replica_nodes.clone(),
            policy,
        ) {
            Ok(intent) => intent,
            Err(error)
                if object.policy == DfsObjectPolicy::ReplicatedEventual
                    && distributed_error_creates_conflict(&error) =>
            {
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(manifest.content_hash.clone()),
                    error.to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }
            Err(error) => return Err(error.into()),
        };
        self.put_metadata_intent(&intent, Some(&manifest.content_hash))?;
        self.pending_writes.insert(
            intent.intent_id.clone(),
            pending_networked_write(
                request.object_id.clone(),
                request.target_version.clone(),
                manifest.content_hash.clone(),
                manifest.size_bytes,
            ),
        );

        let updated_intent = if desired_replica_nodes.contains(&self.node_id) {
            let local_node_id = self.node_id.clone();
            self.put_metadata_receipt(
                &request.file_system_id,
                &request.object_id,
                &request.target_version,
                &local_node_id,
                &manifest.content_hash,
            )?;
            self.coordinator
                .record_durable_receipt(&intent.intent_id, self.node_id.clone())?
        } else {
            intent
        };

        self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;

        if !matches!(
            self.coordinator
                .intent(&updated_intent.intent_id)
                .map(|intent| &intent.state),
            Some(DfsWriteIntentState::Committed)
        ) {
            let stage_targets: Vec<NodeId> = desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id)
                .cloned()
                .collect();
            for replica_node_id in stage_targets {
                let request_id = self.next_request_id();
                let sent = self.send_stage_blob_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &updated_intent.intent_id,
                    &request.file_system_id,
                    &request.object_id,
                    updated_intent.base_version.clone(),
                    &request.target_version,
                    &manifest.content_hash,
                    &bytes,
                    Some(object.clone()),
                )?;
                if sent {
                    self.record_stage_inflight(
                        &updated_intent.intent_id,
                        &replica_node_id,
                        request_id,
                    );
                } else {
                    self.record_stage_retry_scheduled(&updated_intent.intent_id, &replica_node_id);
                }
            }
        }

        let outcome = self.write_outcome(&updated_intent.intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(&updated_intent.intent_id);
        Ok(outcome)
    }

    fn route_distributed_write_to_metadata_primary(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let request_id = self.next_request_id();
        let primary_node_id = shard.primary_node_id.clone();
        let sent = self.send_dfs_message_if_routable(
            &primary_node_id,
            DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
                request_id: request_id.clone(),
                request,
                expected_base_version,
                bytes,
                content_hash: None,
                content_size_bytes: None,
                desired_replica_nodes,
                policy,
                object_metadata: Some(object.clone()),
            }),
        )?;
        if !sent {
            return Err(DfsError::Network {
                message: format!(
                    "metadata shard {} primary {} is not routable",
                    shard.shard_id, primary_node_id
                ),
            });
        }
        self.pending_routed_writes.insert(
            request_id.clone(),
            PendingRoutedWrite {
                object_id: object.object_id,
                primary_node_id,
                remote_intent_id: None,
                durable_receipts: 0,
                required_receipts: 1,
                result: None,
            },
        );
        Ok(NetworkedDfsWriteOutcome::Pending {
            intent_id: request_id,
            durable_receipts: 0,
            required_receipts: 1,
        })
    }

    fn route_distributed_write_from_reader_to_metadata_primary<R: Read>(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        reader: R,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
        object: DfsObjectMetadata,
        shard: DfsMetadataShardConfig,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if !desired_replica_nodes.contains(&request.requesting_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "reader-backed routed write for {} must include requesting node {} as a desired replica",
                    request.object_id, request.requesting_node_id
                ),
            });
        }
        let manifest = self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        )?;
        let request_id = self.next_request_id();
        let primary_node_id = shard.primary_node_id.clone();
        let sent = self.send_dfs_message_if_routable(
            &primary_node_id,
            DfsNetworkMessage::DistributedWriteRequest(DfsDistributedWriteCommand {
                request_id: request_id.clone(),
                request,
                expected_base_version,
                bytes: Vec::new(),
                content_hash: Some(manifest.content_hash),
                content_size_bytes: Some(manifest.size_bytes),
                desired_replica_nodes,
                policy,
                object_metadata: Some(object.clone()),
            }),
        )?;
        if !sent {
            return Err(DfsError::Network {
                message: format!(
                    "metadata shard {} primary {} is not routable",
                    shard.shard_id, primary_node_id
                ),
            });
        }
        self.pending_routed_writes.insert(
            request_id.clone(),
            PendingRoutedWrite {
                object_id: object.object_id,
                primary_node_id,
                remote_intent_id: None,
                durable_receipts: 0,
                required_receipts: 1,
                result: None,
            },
        );
        Ok(NetworkedDfsWriteOutcome::Pending {
            intent_id: request_id,
            durable_receipts: 0,
            required_receipts: 1,
        })
    }

    pub fn process_incoming(&mut self) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
        self.prune_idle_chunk_transfers();
        let inbound_frames = self
            .transport
            .drain_incoming()
            .map_err(DfsError::from_network)?;
        let mut events = Vec::new();

        for inbound in inbound_frames {
            let source_node_id = inbound.frame.source_node_id.clone();
            let MeshMessageBody::Protocol(protocol_message) = inbound.frame.message.body else {
                continue;
            };
            if protocol_message.protocol != DFS_PROTOCOL {
                continue;
            }
            let Ok(message) = protocol_message.decode::<DfsNetworkMessage>() else {
                continue;
            };

            match message {
                DfsNetworkMessage::StageBlob(command) => {
                    events.push(self.handle_stage_blob(source_node_id, command)?);
                }
                DfsNetworkMessage::StageBlobChunk(command) => {
                    if let Some(event) = self.handle_stage_blob_chunk(source_node_id, command)? {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::StagePatch(command) => {
                    events.push(self.handle_stage_patch(source_node_id, command)?);
                }
                DfsNetworkMessage::StageBlobResponse(response) => {
                    if let Some(event) =
                        self.handle_stage_blob_response(source_node_id, response)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::CommitVersion(command) => {
                    events.push(self.handle_commit_version(source_node_id, command)?);
                }
                DfsNetworkMessage::RepairReplica(command) => {
                    events.push(self.handle_repair_replica(source_node_id, command)?);
                }
                DfsNetworkMessage::RepairReplicaChunk(command) => {
                    if let Some(event) =
                        self.handle_repair_replica_chunk(source_node_id, command)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::RepairReplicaResponse(response) => {
                    events.push(self.handle_repair_replica_response(source_node_id, response)?);
                }
                DfsNetworkMessage::FetchBlob(command) => {
                    events.push(self.handle_fetch_blob(source_node_id, command)?);
                }
                DfsNetworkMessage::FetchBlobResponse(response) => {
                    events.push(self.handle_fetch_blob_response(source_node_id.clone(), response)?);
                }
                DfsNetworkMessage::FetchBlobResponseChunk(chunk) => {
                    if let Some(event) =
                        self.handle_fetch_blob_response_chunk(source_node_id.clone(), chunk)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::ReadRequest(request) => {
                    events.push(self.handle_networked_read_request(source_node_id, request)?);
                }
                DfsNetworkMessage::ReadResponse(response) => {
                    events.push(self.handle_networked_read_response(source_node_id, response)?);
                }
                DfsNetworkMessage::ReadResponseChunk(chunk) => {
                    if let Some(event) =
                        self.handle_networked_read_response_chunk(source_node_id, chunk)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::DistributedWriteRequest(command) => {
                    events.push(self.handle_distributed_write_request(source_node_id, command)?);
                }
                DfsNetworkMessage::DistributedWriteResponse(response) => {
                    events.push(self.handle_distributed_write_response(source_node_id, response)?);
                }
                DfsNetworkMessage::MetadataJournalBatch(command) => {
                    events.push(self.handle_metadata_journal_batch(source_node_id, command)?);
                }
                DfsNetworkMessage::MetadataJournalBatchResponse(response) => {
                    if let Some(event) =
                        self.handle_metadata_journal_batch_response(source_node_id, response)?
                    {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::CommitVersionResponse(_)
                | DfsNetworkMessage::WriteRequest(_)
                | DfsNetworkMessage::WriteResponse(_)
                | DfsNetworkMessage::MetadataSnapshotInstall(_)
                | DfsNetworkMessage::MetadataSnapshotInstallResponse(_) => {}
            }
            self.flush_completed_pending_remote_write_responses(&mut events)?;
        }

        self.prune_expired_pending_remote_reads();
        self.last_events = events.clone();
        Ok(events)
    }

    fn prune_expired_pending_remote_reads(&mut self) {
        if self.pending_reads.is_empty() {
            return;
        }
        let now = Instant::now();
        let expired_request_ids = self
            .pending_reads
            .iter()
            .filter_map(|(request_id, pending)| {
                let prune_after = pending
                    .next_retry_at
                    .checked_add(DFS_CHUNK_TRANSFER_IDLE_TIMEOUT)
                    .unwrap_or(pending.next_retry_at);
                (prune_after <= now).then_some(request_id.clone())
            })
            .collect::<Vec<_>>();
        for request_id in expired_request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
        }
    }

    fn handle_distributed_write_request(
        &mut self,
        requester_node_id: NodeId,
        command: DfsDistributedWriteCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let request_id = command.request_id.clone();
        let object_id = command.request.object_id.clone();
        let result = if requester_node_id != command.request.requesting_node_id {
            DfsDistributedWriteResult::Rejected {
                reason: format!(
                    "distributed write requester mismatch: frame source {}, payload requester {}",
                    requester_node_id, command.request.requesting_node_id
                ),
            }
        } else {
            match self
                .ensure_routed_write_object_metadata(
                    &command.request,
                    command.object_metadata.as_ref(),
                )
                .and_then(|()| {
                    if let Some(content_hash) = command.content_hash {
                        let content_size_bytes =
                            command.content_size_bytes.ok_or_else(|| DfsError::Distributed {
                                message: format!(
                                    "routed reader-backed write for {} missing content size",
                                    command.request.object_id
                                ),
                            })?;
                        if !command.bytes.is_empty() {
                            return Err(DfsError::Distributed {
                                message: format!(
                                    "routed reader-backed write for {} must not include inline bytes",
                                    command.request.object_id
                                ),
                            });
                        }
                        self.write_distributed_eventual_from_remote_manifest_with_base(
                            command.request,
                            command.expected_base_version,
                            content_hash,
                            content_size_bytes,
                            command.desired_replica_nodes,
                            command.policy,
                        )
                    } else {
                        self.write_distributed_eventual_with_base(
                            command.request,
                            command.expected_base_version,
                            command.bytes,
                            command.desired_replica_nodes,
                            command.policy,
                        )
                    }
                }) {
                Ok(outcome) => {
                    if let NetworkedDfsWriteOutcome::Pending { intent_id, .. } = &outcome {
                        self.pending_remote_write_responses.insert(
                            intent_id.clone(),
                            PendingRemoteWriteResponse {
                                requester_node_id: requester_node_id.clone(),
                                request_id: request_id.clone(),
                                object_id: object_id.clone(),
                            },
                        );
                    }
                    DfsDistributedWriteResult::Outcome { outcome }
                }
                Err(error) => DfsDistributedWriteResult::Rejected {
                    reason: error.to_string(),
                },
            }
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                request_id: request_id.clone(),
                object_id: object_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(NetworkedDfsServiceEvent::RemoteWriteServed {
            request_id,
            object_id,
            requester_node_id,
            result,
        })
    }

    fn handle_distributed_write_response(
        &mut self,
        primary_node_id: NodeId,
        response: DfsDistributedWriteResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let pending = self
            .pending_routed_writes
            .get_mut(&response.request_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "received distributed write response for unknown request {}",
                    response.request_id
                ),
            })?;
        if pending.primary_node_id != primary_node_id {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: primary_node_id,
                reason: format!(
                    "distributed write response for {} did not come from primary {}",
                    response.request_id, pending.primary_node_id
                ),
            });
        }
        if pending.object_id != response.object_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "distributed write response object mismatch for {}: expected {}, actual {}",
                    response.request_id, pending.object_id, response.object_id
                ),
            });
        }
        let mut remote_intent_alias = None;
        match &response.result {
            DfsDistributedWriteResult::Outcome {
                outcome:
                    NetworkedDfsWriteOutcome::Pending {
                        intent_id,
                        durable_receipts,
                        required_receipts,
                        ..
                    },
            } => {
                pending.remote_intent_id = Some(intent_id.clone());
                pending.durable_receipts = *durable_receipts;
                pending.required_receipts = *required_receipts;
                pending.result = None;
                if intent_id != &response.request_id {
                    remote_intent_alias = Some((intent_id.clone(), pending.clone()));
                }
            }
            DfsDistributedWriteResult::Outcome { outcome } => {
                pending.result = Some(Ok(outcome.clone()));
            }
            DfsDistributedWriteResult::Rejected { reason } => {
                pending.result = Some(Err(DfsError::Distributed {
                    message: reason.clone(),
                }));
            }
        }
        if let Some((intent_id, pending)) = remote_intent_alias {
            self.pending_routed_writes
                .entry(intent_id)
                .or_insert(pending);
        }
        Ok(NetworkedDfsServiceEvent::RemoteWriteCompleted {
            request_id: response.request_id,
            object_id: response.object_id,
            primary_node_id,
            result: response.result,
        })
    }

    fn flush_completed_pending_remote_write_responses(
        &mut self,
        events: &mut Vec<NetworkedDfsServiceEvent>,
    ) -> Result<(), DfsError> {
        if self.pending_remote_write_responses.is_empty() {
            return Ok(());
        }
        let intent_ids = self
            .pending_remote_write_responses
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for intent_id in intent_ids {
            let result = match self.write_outcome(&intent_id) {
                Ok(NetworkedDfsWriteOutcome::Pending { .. }) => continue,
                Ok(outcome) => DfsDistributedWriteResult::Outcome { outcome },
                Err(error) => DfsDistributedWriteResult::Rejected {
                    reason: error.to_string(),
                },
            };
            let Some(pending) = self.pending_remote_write_responses.remove(&intent_id) else {
                continue;
            };
            self.send_dfs_message_if_routable(
                &pending.requester_node_id,
                DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                    request_id: pending.request_id.clone(),
                    object_id: pending.object_id.clone(),
                    result: result.clone(),
                }),
            )?;
            events.push(NetworkedDfsServiceEvent::RemoteWriteServed {
                request_id: pending.request_id,
                object_id: pending.object_id,
                requester_node_id: pending.requester_node_id,
                result,
            });
        }
        Ok(())
    }

    fn ensure_routed_write_object_metadata(
        &mut self,
        request: &DfsWriteRequest,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        match self.current_object_for_write(&request.object_id) {
            Ok(_) => return Ok(()),
            Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        let Some(object) = object_metadata else {
            return Err(DfsError::UnknownObject {
                object_id: request.object_id.clone(),
            });
        };
        if object.object_id != request.object_id || object.file_system_id != request.file_system_id
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write metadata mismatch for object {}",
                    request.object_id
                ),
            });
        }
        let Some(shard) = self.select_metadata_shard_for_object(object)? else {
            return Err(DfsError::Distributed {
                message: format!(
                    "routed write for {} has no metadata shard assignment",
                    request.object_id
                ),
            });
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: object.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        self.add_transient_object_metadata(object.clone())
    }

    fn handle_metadata_journal_batch_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsMetadataJournalBatchResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let pending = self
            .pending_metadata_replications
            .remove(&response.request_id);
        let Some(pending) = pending else {
            return Ok(None);
        };
        if pending.target_node_id != source_node_id || pending.shard_id != response.shard_id {
            return Ok(Some(
                NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason: "metadata journal batch response does not match pending replication"
                        .to_string(),
                },
            ));
        }

        Ok(match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                let cursor_key = (source_node_id.clone(), response.shard_id.clone());
                let cursor = self
                    .metadata_replication_cursors
                    .entry(cursor_key)
                    .or_insert(0);
                *cursor = (*cursor)
                    .max(response.next_after_sequence)
                    .max(pending.next_after_sequence);
                if let Some(backend) = self.metadata_backend.as_mut() {
                    backend.put_metadata_replication_cursor(
                        &source_node_id,
                        &response.shard_id,
                        *cursor,
                    )?;
                }
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    applied_entries: response.applied_entries,
                    next_after_sequence: *cursor,
                })
            }
            DfsMetadataJournalBatchResult::Rejected { reason } => {
                Some(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: response.request_id,
                    source_node_id,
                    shard_id: response.shard_id,
                    reason,
                })
            }
        })
    }

    fn metadata_journal_batch_since(
        &self,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_since_limited(
                after_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .map_err(Into::into)
    }

    fn metadata_journal_batch_for_shard_since(
        &self,
        shard_id: &DfsMetadataShardId,
        after_sequence: u64,
    ) -> Result<Option<DfsMetadataJournalBatch>, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(None);
        };
        backend
            .journal_batch_for_shard_since_limited(
                shard_id,
                after_sequence,
                DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
                DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            )
            .map_err(Into::into)
    }

    fn handle_metadata_journal_batch(
        &mut self,
        source_node_id: NodeId,
        command: DfsMetadataJournalBatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let shard_id = command.batch.shard_id.clone();
        let next_after_sequence = command.batch.next_after_sequence;
        let source_rejection = if command
            .source_node_id
            .as_ref()
            .is_some_and(|claimed| claimed != &source_node_id)
        {
            Some("metadata journal batch source does not match transport source".to_string())
        } else {
            self.validate_metadata_journal_batch_source(&source_node_id, &command.batch)?
        };
        if let Some(reason) = source_rejection {
            let _ = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    applied_entries: 0,
                    result: DfsMetadataJournalBatchResult::Rejected {
                        reason: reason.clone(),
                    },
                }),
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason,
            });
        }
        let Some(backend) = self.metadata_backend.as_mut() else {
            let reason = "local DFS metadata backend is not configured".to_string();
            let _ = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::MetadataJournalBatchResponse(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence,
                    applied_entries: 0,
                    result: DfsMetadataJournalBatchResult::Rejected {
                        reason: reason.clone(),
                    },
                }),
            )?;
            return Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                request_id: command.request_id,
                source_node_id,
                shard_id,
                reason,
            });
        };

        match backend.apply_journal_batch(&command.batch) {
            Ok(applied_entries) => {
                self.apply_metadata_journal_batch_to_service_state(&command.batch)?;
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries,
                            result: DfsMetadataJournalBatchResult::Applied,
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    applied_entries,
                    next_after_sequence,
                })
            }
            Err(error) => {
                let reason = error.to_string();
                let _ = self.send_dfs_message_if_routable(
                    &source_node_id,
                    DfsNetworkMessage::MetadataJournalBatchResponse(
                        DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: shard_id.clone(),
                            next_after_sequence,
                            applied_entries: 0,
                            result: DfsMetadataJournalBatchResult::Rejected {
                                reason: reason.clone(),
                            },
                        },
                    ),
                )?;
                Ok(NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
                    request_id: command.request_id,
                    source_node_id,
                    shard_id,
                    reason,
                })
            }
        }
    }

    fn validate_metadata_journal_batch_source(
        &mut self,
        source_node_id: &NodeId,
        batch: &DfsMetadataJournalBatch,
    ) -> Result<Option<String>, DfsError> {
        let Some(control_plane) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        let Some(file_system_id) = metadata_journal_batch_file_system_id(batch) else {
            return Ok(None);
        };
        let Some(shard) = DfsControlPlaneStore::new(control_plane.as_mut())
            .get_metadata_shard(&file_system_id, &batch.shard_id.0)?
        else {
            return Ok(Some(format!(
                "metadata journal batch has no configured shard {}",
                batch.shard_id.0
            )));
        };
        if shard.primary_node_id != *source_node_id {
            return Ok(Some(format!(
                "metadata journal source {source_node_id} is not shard {} primary {}",
                shard.shard_id, shard.primary_node_id
            )));
        }
        if shard.primary_node_id == self.node_id {
            return Ok(Some(
                "metadata primary must not apply a replica journal batch".to_string(),
            ));
        }
        if !shard.replica_node_ids.contains(&self.node_id) {
            return Ok(Some(format!(
                "local node {} is not configured as a replica for metadata shard {}",
                self.node_id, shard.shard_id
            )));
        }
        Ok(None)
    }

    fn prune_idle_chunk_transfers(&mut self) {
        if !self.has_incoming_chunk_transfer_state() {
            return;
        }
        let now = Instant::now();
        self.incoming_blob_transfers.retain(|_, transfer| {
            now.duration_since(transfer.last_updated_at) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT
        });
        self.incoming_read_transfers.retain(|request_id, transfer| {
            let keep =
                now.duration_since(transfer.last_updated_at) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;
            if !keep {
                self.pending_reads.remove(request_id);
            }
            keep
        });

        let mut expired_fetches = Vec::new();
        self.incoming_fetch_transfers
            .retain(|request_id, transfer| {
                let keep =
                    now.duration_since(transfer.last_updated_at) <= DFS_CHUNK_TRANSFER_IDLE_TIMEOUT;
                if !keep {
                    expired_fetches.push(request_id.clone());
                }
                keep
            });
        if expired_fetches.is_empty() {
            return;
        }
        let expired = expired_fetches.into_iter().collect::<BTreeSet<_>>();
        for pending in self.pending_writes.values_mut() {
            let expired_sources = pending
                .fetch_inflight_request_ids
                .iter()
                .filter_map(|(source, request_id)| {
                    expired.contains(request_id).then_some(source.clone())
                })
                .collect::<Vec<_>>();
            let has_expired_source = !expired_sources.is_empty();
            for source in expired_sources {
                pending.fetch_inflight_from.remove(&source);
                pending.fetch_inflight_request_ids.remove(&source);
                pending.fetch_inflight_started_ticks.remove(&source);
                pending.fetch_next_chunk_indexes.remove(&source);
                pending.fetch_window_end_chunk_indexes.remove(&source);
                pending.fetch_failed_from.insert(source);
            }
            if has_expired_source {
                for state in pending.stage_retries.values_mut() {
                    state.next_retry_tick = 0;
                    state.inflight = false;
                }
                for state in pending.repair_retries.values_mut() {
                    state.next_retry_tick = 0;
                    state.inflight = false;
                }
            }
        }
    }

    fn has_incoming_chunk_transfer_state(&self) -> bool {
        !self.incoming_blob_transfers.is_empty()
            || !self.incoming_read_transfers.is_empty()
            || !self.incoming_fetch_transfers.is_empty()
    }

    pub fn retry_pending_repairs(&mut self) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.prune_idle_chunk_transfers();
        if self.pending_writes.is_empty() {
            return Ok(Vec::new());
        }
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
        self.expire_stale_fetch_requests();
        self.retry_pending_stages()?;
        let repair_jobs = self.coordinator.repair_jobs().to_vec();
        let mut scheduled = Vec::new();

        for job in repair_jobs {
            if job.missing_replica_node_id == self.node_id {
                continue;
            }
            let Some(pending) = self.pending_writes.get(&job.intent_id).cloned() else {
                continue;
            };
            if !self.repair_retry_is_due(&job.intent_id, &job.missing_replica_node_id) {
                continue;
            }

            if self.open_pending_write_blob_stream(&pending).is_err() {
                let fetch_requested = self.request_repair_blob_from_alternate(&job, &pending)?;
                if !fetch_requested {
                    self.record_repair_retry_scheduled(
                        &job.intent_id,
                        &job.missing_replica_node_id,
                    );
                }
                continue;
            }
            let start_chunk_index = pending
                .repair_next_chunk_indexes
                .get(&job.missing_replica_node_id)
                .copied()
                .unwrap_or(0);
            let request_id = pending
                .repair_inflight_request_ids
                .get(&job.missing_replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let outcome = self.send_repair_replica_window_if_routable(
                &job.missing_replica_node_id,
                request_id.clone(),
                &job.intent_id,
                &job.file_system_id,
                &pending.object_id,
                self.coordinator
                    .intent(&job.intent_id)
                    .and_then(|intent| intent.base_version.clone()),
                &pending.version,
                &pending.content_hash,
                pending.size_bytes,
                start_chunk_index,
                DFS_STAGE_CHUNK_WINDOW_SIZE,
                object_metadata,
            )?;

            if outcome.sent {
                self.record_repair_window_progress(
                    &job.intent_id,
                    &job.missing_replica_node_id,
                    request_id,
                    outcome,
                );
                scheduled.push(job);
            } else {
                self.record_repair_retry_scheduled(&job.intent_id, &job.missing_replica_node_id);
            }
        }

        Ok(scheduled)
    }

    pub fn handle_confirmed_write_timeout(
        &mut self,
        intent_id: &str,
        action: DfsConfirmedTimeoutAction,
    ) -> Result<DfsConfirmedTimeoutOutcome, DfsError> {
        match action {
            DfsConfirmedTimeoutAction::KeepPending => match self.write_outcome(intent_id)? {
                NetworkedDfsWriteOutcome::Pending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                } => Ok(DfsConfirmedTimeoutOutcome::StillPending {
                    intent_id,
                    durable_receipts,
                    required_receipts,
                }),
                outcome @ NetworkedDfsWriteOutcome::Committed { .. } => {
                    Ok(DfsConfirmedTimeoutOutcome::Committed(outcome))
                }
                NetworkedDfsWriteOutcome::Conflict { conflict } => {
                    Ok(DfsConfirmedTimeoutOutcome::Conflict { conflict })
                }
            },
            DfsConfirmedTimeoutAction::Abort => {
                self.abort_pending_write(intent_id)?;
                Ok(DfsConfirmedTimeoutOutcome::Aborted {
                    intent_id: intent_id.to_string(),
                })
            }
            DfsConfirmedTimeoutAction::DowngradeToEventual {
                min_commit_replicas,
            } => {
                let intent = self.coordinator.intent(intent_id).cloned().ok_or_else(|| {
                    DfsError::Distributed {
                        message: format!("unknown intent: {intent_id}"),
                    }
                })?;
                let policy =
                    DfsReplicationPolicy::new(min_commit_replicas, intent.policy.desired_replicas)?;
                let updated_intent = self
                    .coordinator
                    .downgrade_intent_policy(intent_id, policy)?;
                let content_hash = self
                    .pending_writes
                    .get(intent_id)
                    .map(|pending| pending.content_hash.clone());
                self.put_metadata_intent(&updated_intent, content_hash.as_deref())?;
                self.try_apply_committed_intent_if_ready(intent_id)?;
                Ok(DfsConfirmedTimeoutOutcome::Downgraded(
                    self.write_outcome(intent_id)?,
                ))
            }
        }
    }

    fn current_object_for_write(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let memory_object = self.metadata.object(object_id).cloned();
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(backend_object) = backend.get_object(object_id)?
        {
            if let Some(memory_object) = memory_object.as_ref()
                && self.local_write_view_overrides_backend(
                    object_id,
                    memory_object,
                    Some(&backend_object),
                )
            {
                return Ok(memory_object.clone());
            }
            self.metadata.add_object(backend_object.clone())?;
            return Ok(backend_object);
        }
        if let Some(memory_object) = memory_object {
            return Ok(memory_object);
        }
        self.metadata
            .object(object_id)
            .cloned()
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })
    }

    pub fn abort_pending_write(
        &mut self,
        intent_id: &str,
    ) -> Result<DistributedWriteIntent, DfsError> {
        let content_hash = self
            .pending_writes
            .get(intent_id)
            .map(|pending| pending.content_hash.clone());
        let intent = self.coordinator.abort_intent(intent_id)?;
        self.put_metadata_intent(&intent, content_hash.as_deref())?;
        self.remove_pending_write_state(intent_id);
        Ok(intent)
    }

    fn remove_pending_write_state(&mut self, intent_id: &str) {
        let Some(pending) = self.pending_writes.remove(intent_id) else {
            return;
        };
        for request_id in pending.fetch_inflight_request_ids.values() {
            self.incoming_fetch_transfers.remove(request_id);
        }
        self.incoming_blob_transfers
            .retain(|_, transfer| transfer.intent_id != intent_id);
    }

    fn cleanup_committed_pending_write_if_repaired(&mut self, intent_id: &str) {
        let is_committed = self
            .coordinator
            .intent(intent_id)
            .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed));
        if !is_committed {
            return;
        }
        let has_repair_jobs = self
            .coordinator
            .repair_jobs()
            .iter()
            .any(|job| job.intent_id == intent_id);
        if has_repair_jobs {
            return;
        }
        self.remove_pending_write_state(intent_id);
    }

    pub fn conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        let mut conflicts: BTreeMap<String, DfsConflictRecord> = self
            .metadata
            .conflicts_for_object(object_id)
            .into_iter()
            .map(|conflict| (conflict.conflict_id.clone(), conflict))
            .collect();
        self.merge_backend_conflict_pages(object_id, &mut conflicts, |_| true)?;
        Ok(conflicts.into_values().collect())
    }

    pub fn open_conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        let mut conflicts: BTreeMap<String, DfsConflictRecord> = self
            .metadata
            .open_conflicts_for_object(object_id)
            .into_iter()
            .map(|conflict| (conflict.conflict_id.clone(), conflict))
            .collect();
        self.merge_backend_conflict_pages(object_id, &mut conflicts, |conflict| {
            conflict.status == DfsConflictStatus::Open
        })?;
        Ok(conflicts.into_values().collect())
    }

    pub fn update_conflict_status(
        &mut self,
        object_id: &DfsObjectId,
        conflict_id: &str,
        status: DfsConflictStatus,
    ) -> Result<Option<DfsConflictRecord>, DfsError> {
        let updated = if let Some(backend) = self.metadata_backend.as_mut() {
            backend.update_conflict_status(object_id, conflict_id, status.clone())?
        } else {
            self.metadata
                .conflicts_for_object(object_id)
                .into_iter()
                .find(|conflict| conflict.conflict_id == conflict_id)
                .map(|mut conflict| {
                    conflict.status = status;
                    conflict
                })
        };
        if let Some(conflict) = updated.clone() {
            self.metadata.add_conflict(conflict);
        }
        Ok(updated)
    }

    pub fn reconcile_local_manifests(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<DfsReconnectReconciliation, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let manifests = self
            .content
            .manifests_for_object(object.object_id.as_str())?;
        let mut refreshed_receipts = 0;
        let mut conflicts = Vec::new();
        let mut has_current_manifest = false;

        for manifest in manifests {
            let manifest_version = DfsVersion::deserialize_migrating_legacy(
                manifest.version.clone(),
            )
            .map_err(|error| DfsError::ContentStore {
                message: error.to_string(),
            })?;
            if manifest_version == object.version {
                if object
                    .content_hash
                    .as_ref()
                    .is_some_and(|hash| hash == &manifest.content_hash)
                {
                    if self
                        .content
                        .has_blob(&manifest.content_hash)
                        .unwrap_or(false)
                    {
                        has_current_manifest = true;
                        let local_node_id = self.node_id.clone();
                        let receipt_inserted = self.put_metadata_receipt(
                            &object.file_system_id,
                            &object.object_id,
                            &object.version,
                            &local_node_id,
                            &manifest.content_hash,
                        )?;
                        self.mark_repaired_location(
                            &object.object_id,
                            &object.version,
                            &local_node_id,
                            &manifest.content_hash,
                        )?;
                        if receipt_inserted {
                            refreshed_receipts += 1;
                        }
                    }
                } else {
                    conflicts.push(self.create_write_conflict(
                        &object,
                        Some(object.version.clone()),
                        manifest_version,
                        self.node_id.clone(),
                        Some(manifest.content_hash),
                        "local current-version manifest hash diverged during reconnect".to_string(),
                        None,
                    )?);
                }
            } else if !self.local_manifest_is_known_historical_version(
                &object.object_id,
                &manifest_version,
                &manifest.content_hash,
            )? {
                conflicts.push(self.create_write_conflict(
                    &object,
                    Some(object.version.clone()),
                    manifest_version,
                    self.node_id.clone(),
                    Some(manifest.content_hash),
                    "local non-visible manifest diverged during reconnect".to_string(),
                    None,
                )?);
            }
        }

        Ok(DfsReconnectReconciliation {
            object_id: object.object_id.clone(),
            refreshed_receipts,
            conflicts,
            missing_current: object.content_hash.is_some() && !has_current_manifest,
        })
    }

    fn local_manifest_is_known_historical_version(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(false);
        };
        Ok(backend
            .get_version(object_id, version)?
            .is_some_and(|record| record.content_hash.as_deref() == Some(content_hash)))
    }

    pub fn handoff_object_owner(
        &mut self,
        object_id: &DfsObjectId,
        shard_id: &str,
        expected_epoch: u64,
    ) -> Result<DfsOwnerHandoffOutcome, DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        let previous_owner_node_id = object.owner_node_id.clone();
        let Some(expected_hash) = object.content_hash.as_deref() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason:
                    "candidate cannot prove latest content for an object without a visible hash"
                        .to_string(),
            });
        };
        let manifest = match self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return Ok(DfsOwnerHandoffOutcome::Rejected {
                    reason: format!("candidate is missing latest manifest: {error}"),
                });
            }
        };
        if manifest.content_hash != expected_hash {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "candidate manifest hash does not match visible object hash".to_string(),
            });
        }

        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "owner handoff requires control-plane shard fencing".to_string(),
            });
        };
        let transferred = DfsControlPlaneStore::new(kv.as_mut()).transfer_metadata_shard_primary(
            &object.file_system_id,
            shard_id,
            expected_epoch,
            self.node_id.clone(),
        )?;
        let Some(transferred) = transferred else {
            return Ok(DfsOwnerHandoffOutcome::Rejected {
                reason: "control-plane epoch compare failed".to_string(),
            });
        };

        let file_system_to_persist =
            if let Some(file_system) = self.metadata.filesystems.get_mut(&object.file_system_id) {
                file_system.owner_node_id = self.node_id.clone();
                Some(file_system.clone())
            } else {
                None
            };
        if let Some(file_system) = file_system_to_persist.as_ref() {
            if let Some(backend) = self.metadata_backend.as_mut() {
                backend.put_file_system(file_system)?;
            }
            if let Some(kv) = self.control_plane.as_mut() {
                DfsControlPlaneStore::new(kv.as_mut())
                    .put_file_system_config(&DfsFileSystemConfig::from(file_system))?;
            }
        }

        let mut updated_object = object.clone();
        updated_object.owner_node_id = self.node_id.clone();
        if !updated_object.current_locations.contains(&self.node_id) {
            updated_object.current_locations.push(self.node_id.clone());
        }
        self.metadata
            .objects
            .insert(updated_object.object_id.clone(), updated_object.clone());
        self.put_visible_object_in_metadata_backend(&updated_object)?;

        Ok(DfsOwnerHandoffOutcome::Transferred {
            file_system_id: updated_object.file_system_id,
            object_id: updated_object.object_id,
            previous_owner_node_id,
            new_owner_node_id: self.node_id.clone(),
            previous_epoch: expected_epoch,
            new_epoch: transferred.epoch,
        })
    }

    fn select_metadata_shard_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<DfsMetadataShardConfig>, DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(None);
        };
        DfsControlPlaneStore::new(kv.as_mut())
            .select_metadata_shard_for_path(&object.file_system_id, &object.path)
            .map_err(DfsError::from)
    }

    fn ensure_intent_epoch_current(
        &mut self,
        intent: &DistributedWriteIntent,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(&intent.object_id)?;
        let Some(shard) = self.select_metadata_shard_for_object(&object)? else {
            return Ok(());
        };
        if shard.primary_node_id != self.node_id || shard.epoch != intent.owner_epoch {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: intent.file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(())
    }

    fn create_write_conflict(
        &mut self,
        object: &DfsObjectMetadata,
        base_version: Option<DfsVersion>,
        local_version: DfsVersion,
        detected_by_node_id: NodeId,
        local_content_hash: Option<String>,
        reason: String,
        created_by_intent_id: Option<String>,
    ) -> Result<DfsConflictRecord, DfsError> {
        let conflict_id = conflict_id_for(
            &object.object_id,
            &local_version,
            local_content_hash.as_deref(),
            &detected_by_node_id,
        );
        if let Some(existing) =
            self.get_conflict_from_memory_or_backend(&object.object_id, &conflict_id)?
        {
            return Ok(existing);
        }
        let conflict = DfsConflictRecord {
            conflict_id,
            file_system_id: object.file_system_id.clone(),
            object_id: object.object_id.clone(),
            path: object.path.clone(),
            base_version,
            local_version,
            remote_version: object.version.clone(),
            detected_by_node_id,
            reason,
            local_content_hash,
            remote_content_hash: object.content_hash.clone(),
            created_by_intent_id,
            strategy: Some("branch".to_string()),
            status: DfsConflictStatus::Open,
            detected_at: OffsetDateTime::now_utc(),
        };
        self.put_conflict_record(&conflict)?;
        Ok(conflict)
    }

    fn put_conflict_record(&mut self, conflict: &DfsConflictRecord) -> Result<(), DfsError> {
        if let Some(backend) = self.metadata_backend.as_mut() {
            if backend
                .get_conflict(&conflict.object_id, &conflict.conflict_id)?
                .as_ref()
                == Some(conflict)
            {
                self.metadata.add_conflict(conflict.clone());
                return Ok(());
            }
            backend.put_conflict(conflict)?;
        }
        self.metadata.add_conflict(conflict.clone());
        Ok(())
    }

    fn add_backend_conflict_pages_to_memory(
        &mut self,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        if self.metadata_backend.is_none() {
            return Ok(());
        }
        let mut cursor = None;
        loop {
            let page = self
                .metadata_backend
                .as_ref()
                .expect("metadata backend checked above")
                .conflicts_for_object_page(
                    object_id,
                    cursor.as_ref(),
                    DFS_CONFLICT_SCAN_PAGE_SIZE,
                )?;
            for conflict in page.conflicts {
                self.metadata.add_conflict(conflict);
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn merge_backend_conflict_pages<F>(
        &self,
        object_id: &DfsObjectId,
        conflicts: &mut BTreeMap<String, DfsConflictRecord>,
        mut include_conflict: F,
    ) -> Result<(), DfsError>
    where
        F: FnMut(&DfsConflictRecord) -> bool,
    {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut cursor = None;
        loop {
            let page = backend.conflicts_for_object_page(
                object_id,
                cursor.as_ref(),
                DFS_CONFLICT_SCAN_PAGE_SIZE,
            )?;
            for conflict in page.conflicts {
                if include_conflict(&conflict) {
                    conflicts.insert(conflict.conflict_id.clone(), conflict);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn get_conflict_from_memory_or_backend(
        &self,
        object_id: &DfsObjectId,
        conflict_id: &str,
    ) -> Result<Option<DfsConflictRecord>, DfsError> {
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(conflict) = backend.get_conflict(object_id, conflict_id)?
        {
            return Ok(Some(conflict));
        }
        Ok(self
            .metadata
            .conflicts_for_object(object_id)
            .into_iter()
            .find(|conflict| conflict.conflict_id == conflict_id))
    }

    fn handle_stage_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "stage",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "stage",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id.clone(),
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Committed { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn handle_stage_patch(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStagePatchCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "stage-patch",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "stage-patch",
                    &command.object_id,
                    Some(&command.base_version),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.store_command_patch(
                    &command.object_id,
                    &command.base_version,
                    &command.version,
                    &command.content_hash,
                    command.offset,
                    &command.patch_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id.clone(),
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Durable { content_hash }
            | DfsBlobTransferResult::Committed { content_hash } => {
                NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn handle_stage_blob_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "stage blob chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_stage_blob_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS blob transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "stage",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        if let Err(error) = validate_chunk_hash(
            "chunked DFS blob transfer",
            command.chunk_index,
            &command.bytes,
            command.chunk_hash.as_deref(),
        ) {
            self.incoming_blob_transfers.remove(&transfer_key);
            return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
        }
        if !self.incoming_blob_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) =
                self.ensure_replica_object_metadata(command.object_metadata.as_ref())
            {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) = self.ensure_command_base_matches_current(
                "stage",
                &command.object_id,
                command.expected_base_version.as_ref(),
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if let Err(error) = self.ensure_no_conflicting_pending_replica_write(
                "stage",
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                &command.version,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            let local_node_id = self.node_id.clone();
            if let Err(error) = self.ensure_metadata_receipt_accepts(
                &command.object_id,
                &command.version,
                &local_node_id,
                &command.content_hash,
            ) {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
            if self.local_object_version_manifest_matches(
                &command.object_id,
                &command.version,
                &command.content_hash,
                command.total_size_bytes,
            ) {
                let local_node_id = self.node_id.clone();
                if let Err(error) = self.put_replica_receipt_if_authoritative(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                ) {
                    return self.reject_stage_blob_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
                self.record_active_replica_stage(
                    &command.object_id,
                    &command.intent_id,
                    &command.version,
                    &command.content_hash,
                    &requester_node_id,
                );
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id,
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: self.node_id.clone(),
                        result: DfsBlobTransferResult::Durable {
                            content_hash: command.content_hash.clone(),
                        },
                    }),
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash: command.content_hash,
                }));
            }
            let assembler = match self.content.begin_chunked_object_version(
                command.object_id.as_str(),
                command.version.as_str(),
                &command.content_hash,
                command.total_size_bytes,
                command.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self.reject_stage_blob_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
            };
            let transfer = InProgressBlobTransfer::new(
                command.request_id.clone(),
                command.intent_id.clone(),
                command.file_system_id.clone(),
                command.object_id.clone(),
                command.expected_base_version.clone(),
                command.version.clone(),
                command.content_hash.clone(),
                command.total_size_bytes,
                command.total_chunks,
                assembler,
            )?;
            self.incoming_blob_transfers
                .insert(transfer_key.clone(), transfer);
        }
        let transfer = self
            .incoming_blob_transfers
            .get_mut(&transfer_key)
            .expect("transfer inserted");
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.expected_base_version != command.expected_base_version
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            let reason = format!(
                "conflicting stage blob chunk metadata for {}",
                command.intent_id
            );
            return self.reject_stage_blob_chunk(requester_node_id, command, reason);
        }
        let transfer_complete = match transfer.insert_chunk(
            command.chunk_index,
            command.total_chunks,
            command.bytes.clone(),
        ) {
            Ok(true) => true,
            Ok(false) => return Ok(None),
            Err(error) => {
                self.incoming_blob_transfers.remove(&transfer_key);
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
        };
        if !transfer_complete {
            return Ok(None);
        }
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return self.reject_stage_blob_chunk(requester_node_id, command, error.to_string());
            }
        };
        let local_node_id = self.node_id.clone();
        self.put_replica_receipt_if_authoritative(
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.version,
            &local_node_id,
            &manifest.content_hash,
            None,
        )?;
        self.record_active_replica_stage(
            &transfer.object_id,
            &transfer.intent_id,
            &transfer.version,
            &manifest.content_hash,
            &requester_node_id,
        );
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: transfer.request_id,
                intent_id: transfer.intent_id.clone(),
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id.clone(),
                version: transfer.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Durable {
                    content_hash: manifest.content_hash.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::BlobStaged {
            intent_id: transfer.intent_id,
            object_id: transfer.object_id,
            version: transfer.version,
            replica_node_id: self.node_id.clone(),
            content_hash: manifest.content_hash,
        }))
    }

    fn reject_stage_blob_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "stage",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_blob_transfers.remove(&transfer_key);
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::StageBlobResponse(DfsStageBlobResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version,
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: reason.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
            intent_id: command.intent_id,
            object_id: command.object_id,
            replica_node_id: self.node_id.clone(),
            reason,
        }))
    }

    fn handle_stage_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsStageBlobResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS stage response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            }));
        }
        match response.result {
            DfsBlobTransferResult::Durable { ref content_hash }
            | DfsBlobTransferResult::Committed { ref content_hash } => {
                if self
                    .coordinator
                    .intent(&response.intent_id)
                    .is_some_and(|intent| {
                        matches!(
                            intent.state,
                            DfsWriteIntentState::Aborted
                                | DfsWriteIntentState::Rejected
                                | DfsWriteIntentState::Conflict
                        )
                    })
                {
                    let intent_id = response.intent_id.clone();
                    return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: format!("missing pending write for stage response: {intent_id}"),
                    }));
                }
                self.restore_pending_write_for_intent_if_missing(
                    &response.intent_id,
                    &response.object_id,
                )?;
                if let Err(error) =
                    self.ensure_stage_response_matches_pending(&response, content_hash)
                {
                    return Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }));
                }
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                let updated_intent = self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                let event = NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: response.intent_id.clone(),
                    object_id: response.object_id.clone(),
                    version: response.version.clone(),
                    replica_node_id: response.replica_node_id.clone(),
                    content_hash: content_hash.clone(),
                };
                self.try_apply_committed_intent_if_ready(&updated_intent.intent_id)?;
                if self
                    .coordinator
                    .intent(&updated_intent.intent_id)
                    .is_some_and(|intent| matches!(intent.state, DfsWriteIntentState::Committed))
                    && updated_intent.requesting_node_id != self.node_id
                    && !self
                        .pending_remote_write_responses
                        .contains_key(&updated_intent.intent_id)
                {
                    let outcome = self.write_outcome(&updated_intent.intent_id)?;
                    self.send_dfs_message_if_routable(
                        &updated_intent.requesting_node_id,
                        DfsNetworkMessage::DistributedWriteResponse(DfsDistributedWriteResponse {
                            request_id: updated_intent.intent_id.clone(),
                            object_id: response.object_id.clone(),
                            result: DfsDistributedWriteResult::Outcome { outcome },
                        }),
                    )?;
                }
                if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
                    pending
                        .stage_inflight_request_ids
                        .remove(&response.replica_node_id);
                    pending
                        .stage_next_chunk_indexes
                        .remove(&response.replica_node_id);
                    pending.stage_retries.remove(&response.replica_node_id);
                }
                self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                Ok(Some(event))
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                match self.ensure_stage_response_request_matches_pending(&response) {
                    Ok(()) => {
                        self.record_stage_patch_fallback(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        self.record_stage_retry_scheduled(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        }))
                    }
                    Err(error) => Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    })),
                }
            }
        }
    }

    fn handle_commit_version(
        &mut self,
        requester_node_id: NodeId,
        command: DfsCommitVersionCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "commit",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.content
                    .read_manifest(command.object_id.as_str(), command.version.as_str())
                    .map_err(DfsError::from)
            })
            .and_then(|manifest| {
                if manifest.content_hash != command.content_hash {
                    return Err(DfsError::ContentHashMismatch {
                        expected: command.content_hash.clone(),
                        actual: manifest.content_hash,
                    });
                }
                if !self.content.manifest_content_available(&manifest)? {
                    return Err(DfsError::ContentStore {
                        message: format!(
                            "committed content bytes are missing for {} {}",
                            command.object_id, command.version
                        ),
                    });
                }
                Ok(())
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "commit",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )
            }) {
            Ok(()) => {
                // The version is now durably committed on this replica, so the
                // in-flight stage guard for it is no longer "active". Clearing it
                // prevents a stale entry from rejecting a later write of a new
                // version of the same object to this replica.
                self.clear_active_replica_stage(&command.object_id, &command.version);
                DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::CommitVersionResponse(DfsCommitVersionResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::VersionCommitted {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn ensure_command_base_matches_current(
        &self,
        operation: &str,
        object_id: &DfsObjectId,
        expected_base_version: Option<&DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let current = if let Some(backend) = self.metadata_backend.as_ref() {
            backend.get_object(object_id)?
        } else {
            self.metadata.object(object_id).cloned()
        };
        let Some(current) = current else {
            return Ok(());
        };
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash == content_hash
        {
            return Ok(());
        }
        if let Some(expected_base_version) = expected_base_version
            && &current.version != expected_base_version
        {
            // Content is addressed by hash, so a repair/stage carrying the exact
            // bytes the object *currently* references is never stale in a harmful
            // way — even when the version label advanced past `version`. This
            // happens when a metadata-only change bumps the version (chmod, chown,
            // utimes, hardlink/rename) while leaving content unchanged: the
            // content repair queued for the older version would otherwise be
            // rejected here as a stale base, dropping the only copy of the
            // content-addressed blob the replica needs to serve the current
            // version. Accept it; a genuinely stale write carries a different hash
            // and is still rejected below.
            if current.content_hash.as_deref() != Some(content_hash) {
                return Err(DfsError::Distributed {
                    message: format!(
                        "stale DFS {operation} base for {object_id}: expected {}, current {}, command {}",
                        expected_base_version, current.version, version
                    ),
                });
            }
        }
        if current.version == *version
            && let Some(current_hash) = current.content_hash.as_deref()
            && current_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: current_hash.to_string(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    fn ensure_no_conflicting_pending_replica_write(
        &self,
        operation: &str,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        if let Some(stage) = self.active_replica_stages.get(object_id) {
            if stage.intent_id != intent_id
                && (stage.version != *version || stage.content_hash != content_hash)
            {
                return Err(DfsError::Distributed {
                    message: format!(
                        "conflicting active DFS {operation} for {object_id}: local stage {} from {} targets {} ({}) while remote intent {intent_id} from {} targets {} ({})",
                        stage.intent_id,
                        stage.requester_node_id,
                        stage.version,
                        stage.content_hash,
                        requester_node_id,
                        version,
                        content_hash
                    ),
                });
            }
        }
        for (pending_intent_id, pending) in &self.pending_writes {
            if pending.object_id != *object_id {
                continue;
            }
            let Some(intent) = self.coordinator.intent(pending_intent_id) else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            if pending_intent_id == intent_id && intent.requesting_node_id == *requester_node_id {
                continue;
            }
            if pending.version == *version && pending.content_hash == content_hash {
                continue;
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "conflicting pending DFS {operation} for {object_id}: local intent {} from {} targets {} ({}) while remote intent {intent_id} from {} targets {} ({})",
                    pending_intent_id,
                    intent.requesting_node_id,
                    pending.version,
                    pending.content_hash,
                    requester_node_id,
                    version,
                    content_hash
                ),
            });
        }
        Ok(())
    }

    fn ensure_stage_response_request_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
    ) -> Result<(), DfsError> {
        let Some(pending) = self.pending_writes.get(&response.intent_id) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "missing pending write for stage response: {}",
                    response.intent_id
                ),
            });
        };
        let expected_request_id = pending
            .stage_inflight_request_ids
            .get(&response.replica_node_id);
        if let Some(expected_request_id) = expected_request_id {
            if expected_request_id != &response.request_id {
                return Err(DfsError::Distributed {
                    message: format!(
                        "unexpected DFS stage response request id for intent {} from {}: expected {}, actual {}",
                        response.intent_id,
                        response.replica_node_id,
                        expected_request_id,
                        response.request_id
                    ),
                });
            }
            return Ok(());
        }
        if !pending.stage_inflight_request_ids.is_empty() {
            return Err(DfsError::Distributed {
                message: format!(
                    "unexpected DFS stage response source for intent {}: {}",
                    response.intent_id, response.replica_node_id
                ),
            });
        }
        Ok(())
    }

    fn ensure_stage_response_matches_pending(
        &self,
        response: &DfsStageBlobResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_stage_response_request_matches_pending(response)?;
        let pending = self
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "stage",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }

    fn handle_repair_replica(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
            .ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            )
            .and_then(|()| self.ensure_replica_object_metadata(command.object_metadata.as_ref()))
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_no_conflicting_pending_replica_write(
                    "repair",
                    &requester_node_id,
                    &command.intent_id,
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.ensure_command_base_matches_current(
                    "repair",
                    &command.object_id,
                    command.expected_base_version.as_ref(),
                    &command.version,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                let local_node_id = self.node_id.clone();
                self.ensure_metadata_receipt_accepts(
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                match self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                ) {
                    Ok(()) => DfsBlobTransferResult::Committed {
                        content_hash: command.content_hash.clone(),
                    },
                    Err(error) => DfsBlobTransferResult::Rejected {
                        reason: error.to_string(),
                    },
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: result.clone(),
            }),
        )?;
        Ok(match result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsBlobTransferResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::RepairRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    replica_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    fn ensure_repair_response_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        self.ensure_repair_response_request_matches_pending(response)?;
        let pending = self
            .pending_writes
            .get(&response.intent_id)
            .expect("pending write validated");
        self.ensure_response_matches_pending_write(
            "repair",
            &response.intent_id,
            &response.file_system_id,
            &response.object_id,
            &response.version,
            &response.replica_node_id,
            content_hash,
            pending,
        )
    }

    fn ensure_repair_response_request_matches_pending(
        &self,
        response: &DfsRepairReplicaResponse,
    ) -> Result<(), DfsError> {
        let Some(pending) = self.pending_writes.get(&response.intent_id) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "missing pending write for repair response: {}",
                    response.intent_id
                ),
            });
        };
        let expected_request_id = pending
            .repair_inflight_request_ids
            .get(&response.replica_node_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!(
                    "unexpected DFS repair response source for intent {}: {}",
                    response.intent_id, response.replica_node_id
                ),
            })?;
        if expected_request_id != &response.request_id {
            return Err(DfsError::Distributed {
                message: format!(
                    "unexpected DFS repair response request id for intent {} from {}: expected {}, actual {}",
                    response.intent_id,
                    response.replica_node_id,
                    expected_request_id,
                    response.request_id
                ),
            });
        }
        Ok(())
    }

    fn ensure_response_matches_pending_write(
        &self,
        label: &str,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        pending: &PendingNetworkedWrite,
    ) -> Result<(), DfsError> {
        let intent = self
            .coordinator
            .intent(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("unknown DFS {label} response intent: {intent_id}"),
            })?;
        if &intent.file_system_id != file_system_id
            || &intent.object_id != object_id
            || &intent.target_version != version
        {
            return Err(DfsError::Distributed {
                message: format!("DFS {label} response metadata does not match intent {intent_id}"),
            });
        }
        if !intent.desired_replica_nodes.contains(replica_node_id) {
            return Err(DfsError::Distributed {
                message: format!(
                    "DFS {label} response from undesired replica {replica_node_id} for intent {intent_id}"
                ),
            });
        }
        if &pending.object_id != object_id
            || &pending.version != version
            || pending.content_hash != content_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: pending.content_hash.clone(),
                actual: content_hash.to_string(),
            });
        }
        Ok(())
    }

    fn handle_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            let reason = format!(
                "repair replica chunk transfer {} has no chunks",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS blob transfer",
            command.total_size_bytes,
            command.chunk_index,
            command.total_chunks,
            &command.bytes,
        ) {
            return self.reject_repair_replica_chunk(requester_node_id, command, error.to_string());
        }
        let transfer_key = blob_transfer_key(
            "repair",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        if let Err(error) = validate_chunk_hash(
            "chunked DFS blob transfer",
            command.chunk_index,
            &command.bytes,
            command.chunk_hash.as_deref(),
        ) {
            self.incoming_blob_transfers.remove(&transfer_key);
            return self.reject_repair_replica_chunk(requester_node_id, command, error.to_string());
        }
        if !self.incoming_blob_transfers.contains_key(&transfer_key) {
            if let Err(error) = self.ensure_replica_command_authorized_with_metadata(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
                command.object_metadata.as_ref(),
            ) {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if let Err(error) = self
                .ensure_replica_object_metadata(command.object_metadata.as_ref())
                .and_then(|()| {
                    self.ensure_replica_command_hash_matches_intent(
                        &command.intent_id,
                        &command.object_id,
                        &command.content_hash,
                    )
                })
                .and_then(|()| {
                    self.ensure_no_conflicting_pending_replica_write(
                        "repair",
                        &requester_node_id,
                        &command.intent_id,
                        &command.object_id,
                        &command.version,
                        &command.content_hash,
                    )
                })
                .and_then(|()| {
                    self.ensure_command_base_matches_current(
                        "repair",
                        &command.object_id,
                        command.expected_base_version.as_ref(),
                        &command.version,
                        &command.content_hash,
                    )
                })
            {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
            if self.local_object_version_manifest_matches(
                &command.object_id,
                &command.version,
                &command.content_hash,
                command.total_size_bytes,
            ) {
                let local_node_id = self.node_id.clone();
                self.commit_replica_metadata_cache(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                    command.object_metadata.as_ref(),
                )?;
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        file_system_id: command.file_system_id,
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        replica_node_id: self.node_id.clone(),
                        result: DfsBlobTransferResult::Committed {
                            content_hash: command.content_hash.clone(),
                        },
                    }),
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    replica_node_id: self.node_id.clone(),
                    content_hash: command.content_hash,
                }));
            }
            let assembler = match self.content.begin_chunked_object_version(
                command.object_id.as_str(),
                command.version.as_str(),
                &command.content_hash,
                command.total_size_bytes,
                command.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self.reject_repair_replica_chunk(
                        requester_node_id,
                        command,
                        error.to_string(),
                    );
                }
            };
            let transfer = InProgressBlobTransfer::new(
                command.request_id.clone(),
                command.intent_id.clone(),
                command.file_system_id.clone(),
                command.object_id.clone(),
                command.expected_base_version.clone(),
                command.version.clone(),
                command.content_hash.clone(),
                command.total_size_bytes,
                command.total_chunks,
                assembler,
            )?;
            self.incoming_blob_transfers
                .insert(transfer_key.clone(), transfer);
        }
        let transfer = self
            .incoming_blob_transfers
            .get_mut(&transfer_key)
            .expect("transfer inserted");
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.expected_base_version != command.expected_base_version
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            let reason = format!(
                "conflicting repair replica chunk metadata for {}",
                command.intent_id
            );
            return self.reject_repair_replica_chunk(requester_node_id, command, reason);
        }
        let transfer_complete = match transfer.insert_chunk(
            command.chunk_index,
            command.total_chunks,
            command.bytes.clone(),
        ) {
            Ok(true) => true,
            Ok(false) => return Ok(None),
            Err(error) => {
                self.incoming_blob_transfers.remove(&transfer_key);
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        if !transfer_complete {
            return Ok(None);
        }
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                return self.reject_repair_replica_chunk(
                    requester_node_id,
                    command,
                    error.to_string(),
                );
            }
        };
        let local_node_id = self.node_id.clone();
        self.commit_replica_metadata_cache(
            &transfer.file_system_id,
            &transfer.object_id,
            &transfer.version,
            &local_node_id,
            &manifest.content_hash,
            None,
        )?;
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: transfer.request_id,
                intent_id: transfer.intent_id.clone(),
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id.clone(),
                version: transfer.version.clone(),
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Committed {
                    content_hash: manifest.content_hash.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::ReplicaRepaired {
            intent_id: transfer.intent_id,
            object_id: transfer.object_id,
            version: transfer.version,
            replica_node_id: self.node_id.clone(),
            content_hash: manifest.content_hash,
        }))
    }

    fn reject_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let transfer_key = blob_transfer_key(
            "repair",
            &requester_node_id,
            &command.intent_id,
            &command.request_id,
        );
        self.incoming_blob_transfers.remove(&transfer_key);
        self.send_dfs_message_if_routable(
            &requester_node_id,
            DfsNetworkMessage::RepairReplicaResponse(DfsRepairReplicaResponse {
                request_id: command.request_id,
                intent_id: command.intent_id.clone(),
                file_system_id: command.file_system_id,
                object_id: command.object_id.clone(),
                version: command.version,
                replica_node_id: self.node_id.clone(),
                result: DfsBlobTransferResult::Rejected {
                    reason: reason.clone(),
                },
            }),
        )?;
        Ok(Some(NetworkedDfsServiceEvent::RepairRejected {
            intent_id: command.intent_id,
            object_id: command.object_id,
            replica_node_id: self.node_id.clone(),
            reason,
        }))
    }

    fn handle_repair_replica_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsRepairReplicaResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.replica_node_id {
            let reason = format!(
                "DFS repair response source mismatch: frame source {}, payload replica {}",
                frame_source_node_id, response.replica_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RepairRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                replica_node_id: frame_source_node_id,
                reason,
            });
        }
        match response.result {
            DfsBlobTransferResult::Committed { ref content_hash }
            | DfsBlobTransferResult::Durable { ref content_hash } => {
                if let Err(error) =
                    self.ensure_repair_response_matches_pending(&response, content_hash)
                {
                    return Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    });
                }
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                self.mark_repaired_location(
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    content_hash,
                )?;
                self.clear_repair_retry(&response.intent_id, &response.replica_node_id);
                self.cleanup_committed_pending_write_if_repaired(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    replica_node_id: response.replica_node_id,
                    content_hash: content_hash.clone(),
                })
            }
            DfsBlobTransferResult::Rejected { ref reason } => {
                match self.ensure_repair_response_request_matches_pending(&response) {
                    Ok(()) => {
                        self.record_repair_retry_scheduled(
                            &response.intent_id,
                            &response.replica_node_id,
                        );
                        Ok(NetworkedDfsServiceEvent::RepairRejected {
                            intent_id: response.intent_id,
                            object_id: response.object_id,
                            replica_node_id: response.replica_node_id,
                            reason: reason.clone(),
                        })
                    }
                    Err(error) => Ok(NetworkedDfsServiceEvent::RepairRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        replica_node_id: response.replica_node_id,
                        reason: error.to_string(),
                    }),
                }
            }
        }
    }

    fn handle_fetch_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsFetchBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let manifest_result = self
            .ensure_blob_fetch_authorized(
                &requester_node_id,
                &command.intent_id,
                &command.object_id,
            )
            .and_then(|()| {
                self.ensure_replica_command_hash_matches_intent(
                    &command.intent_id,
                    &command.object_id,
                    &command.content_hash,
                )
            })
            .and_then(|()| {
                self.content
                    .read_manifest(command.object_id.as_str(), command.version.as_str())
                    .map_err(DfsError::from)
            })
            .and_then(|manifest| {
                if manifest.content_hash != command.content_hash {
                    return Err(DfsError::ContentHashMismatch {
                        expected: command.content_hash.clone(),
                        actual: manifest.content_hash,
                    });
                }
                Ok(manifest)
            });

        let mut sent_chunked_response = false;
        let result = match manifest_result {
            Ok(manifest) => match self
                .content
                .open_object_version_stream(&manifest.object_id, &manifest.version)
            {
                Ok((stream_manifest, reader))
                    if stream_manifest.content_hash == command.content_hash
                        && stream_manifest.size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 =>
                {
                    let reader = if command.start_chunk_index == 0 {
                        reader
                    } else {
                        self.open_fetch_response_chunk_reader(&stream_manifest, &command)?
                    };
                    self.send_fetch_blob_response_chunks_from_reader(
                        &requester_node_id,
                        &command,
                        reader,
                        stream_manifest.size_bytes,
                    )?;
                    sent_chunked_response = true;
                    DfsFetchBlobResult::Found {
                        content_hash: command.content_hash.clone(),
                        bytes: Vec::new(),
                    }
                }
                Ok((stream_manifest, mut reader))
                    if stream_manifest.content_hash == command.content_hash =>
                {
                    if command.start_chunk_index > 0 {
                        match self.open_fetch_response_chunk_reader(&stream_manifest, &command) {
                            Ok(reader) => {
                                self.send_fetch_blob_response_chunks_from_reader(
                                    &requester_node_id,
                                    &command,
                                    reader,
                                    stream_manifest.size_bytes,
                                )?;
                                sent_chunked_response = true;
                                DfsFetchBlobResult::Found {
                                    content_hash: command.content_hash.clone(),
                                    bytes: Vec::new(),
                                }
                            }
                            Err(error) => DfsFetchBlobResult::Rejected {
                                reason: error.to_string(),
                            },
                        }
                    } else {
                        match read_small_blob_from_reader(
                            &command.content_hash,
                            stream_manifest.size_bytes,
                            &mut reader,
                        ) {
                            Ok(bytes) => DfsFetchBlobResult::Found {
                                content_hash: command.content_hash.clone(),
                                bytes,
                            },
                            Err(error) => DfsFetchBlobResult::Rejected {
                                reason: error.to_string(),
                            },
                        }
                    }
                }
                Ok((stream_manifest, _)) => DfsFetchBlobResult::Rejected {
                    reason: format!(
                        "object manifest hash changed while serving fetch: expected {}, actual {}",
                        command.content_hash, stream_manifest.content_hash
                    ),
                },
                Err(_) => DfsFetchBlobResult::NotFound,
            },
            Err(DfsError::ContentStore { .. }) => DfsFetchBlobResult::NotFound,
            Err(error) => DfsFetchBlobResult::Rejected {
                reason: error.to_string(),
            },
        };
        match &result {
            DfsFetchBlobResult::Found { .. } if sent_chunked_response => {}
            _ => {
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
                        request_id: command.request_id,
                        intent_id: command.intent_id.clone(),
                        object_id: command.object_id.clone(),
                        version: command.version.clone(),
                        source_node_id: self.node_id.clone(),
                        result: result.clone(),
                    }),
                )?;
            }
        }
        Ok(match result {
            DfsFetchBlobResult::Found { content_hash, .. } => {
                NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    version: command.version,
                    source_node_id: self.node_id.clone(),
                    content_hash,
                }
            }
            DfsFetchBlobResult::NotFound => NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: command.intent_id,
                object_id: command.object_id,
                source_node_id: self.node_id.clone(),
                reason: "blob not found".to_string(),
            },
            DfsFetchBlobResult::Rejected { reason } => {
                NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: command.intent_id,
                    object_id: command.object_id,
                    source_node_id: self.node_id.clone(),
                    reason,
                }
            }
        })
    }

    #[cfg(test)]
    fn send_fetch_blob_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(bytes)?;
        let end_chunk_index =
            fetch_response_window_end(command.start_chunk_index, total_chunks, command.max_chunks);
        for (chunk_index, chunk) in bytes
            .chunks(DFS_BLOB_CHUNK_BYTES)
            .enumerate()
            .skip(command.start_chunk_index as usize)
            .take(end_chunk_index.saturating_sub(command.start_chunk_index) as usize)
        {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    source_node_id: self.node_id.clone(),
                    content_hash: command.content_hash.clone(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn send_fetch_blob_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        command: &DfsFetchBlobCommand,
        mut reader: ObjectVersionReader,
        total_size_bytes: u64,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        let end_chunk_index =
            fetch_response_window_end(command.start_chunk_index, total_chunks, command.max_chunks);
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in command.start_chunk_index..end_chunk_index {
            let chunk_offset = u64::from(chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
            let chunk_len = (total_size_bytes - chunk_offset).min(DFS_BLOB_CHUNK_BYTES as u64);
            let chunk_len = usize::try_from(chunk_len).map_err(|_| DfsError::Distributed {
                message: format!("DFS blob chunk is too large to fit in memory: {chunk_len} bytes"),
            })?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {} chunk {}: {}",
                        command.content_hash, chunk_index, source
                    ),
                })?;
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                    request_id: command.request_id.clone(),
                    intent_id: command.intent_id.clone(),
                    object_id: command.object_id.clone(),
                    version: command.version.clone(),
                    source_node_id: self.node_id.clone(),
                    content_hash: command.content_hash.clone(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&buffer[..chunk_len])),
                    bytes: buffer[..chunk_len].to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn open_fetch_response_chunk_reader(
        &self,
        manifest: &ContentManifest,
        command: &DfsFetchBlobCommand,
    ) -> Result<ObjectVersionReader, DfsError> {
        let total_chunks = blob_chunk_count_for_size(manifest.size_bytes)?;
        if command.start_chunk_index >= total_chunks {
            return Err(DfsError::Distributed {
                message: format!(
                    "DFS fetch resume chunk {} is outside total chunks {} for {} {}",
                    command.start_chunk_index, total_chunks, command.object_id, command.version
                ),
            });
        }
        let start_offset = u64::from(command.start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
        let remaining_bytes = manifest.size_bytes.saturating_sub(start_offset);
        let remaining_bytes = usize::try_from(remaining_bytes).map_err(|_| {
            DfsError::Distributed {
                message: format!(
                    "DFS fetch response range is too large to fit in memory window: {remaining_bytes} bytes"
                ),
            }
        })?;
        let (range_manifest, reader) = self
            .content
            .open_object_version_range_stream(
                command.object_id.as_str(),
                command.version.as_str(),
                start_offset,
                remaining_bytes,
            )
            .map_err(DfsError::from)?;
        if range_manifest.content_hash != command.content_hash
            || range_manifest.size_bytes != manifest.size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "object manifest changed while opening fetch resume range for {} {}",
                    command.object_id, command.version
                ),
            });
        }
        Ok(reader)
    }

    fn fetch_window_end_chunk_index(start_chunk_index: u32, max_chunks: u32) -> Option<u32> {
        (max_chunks > 0).then(|| start_chunk_index.saturating_add(max_chunks))
    }

    fn read_window_end_chunk_index(start_chunk_index: u32, max_chunks: u32) -> Option<u32> {
        (max_chunks > 0).then(|| start_chunk_index.saturating_add(max_chunks))
    }

    fn handle_fetch_blob_response(
        &mut self,
        frame_source_node_id: NodeId,
        response: DfsFetchBlobResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if frame_source_node_id != response.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, response.source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason,
            });
        }
        let Some(pending) = self.pending_writes.get(&response.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob: {}",
                response.intent_id
            );
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: response.source_node_id,
                reason,
            });
        };
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            });
        }
        if pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
            != Some(&response.request_id)
        {
            return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: response.intent_id,
                object_id: response.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            });
        }
        if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
            pending.fetch_inflight_from.remove(&frame_source_node_id);
            pending
                .fetch_inflight_request_ids
                .remove(&frame_source_node_id);
            pending
                .fetch_inflight_started_ticks
                .remove(&frame_source_node_id);
        }
        match response.result {
            DfsFetchBlobResult::Found {
                content_hash,
                bytes,
            } => {
                if pending.object_id != response.object_id
                    || pending.version != response.version
                    || pending.content_hash != content_hash
                {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    let reason = DfsError::ContentHashMismatch {
                        expected: pending.content_hash,
                        actual: content_hash,
                    }
                    .to_string();
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason,
                    });
                }
                if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                ) {
                    self.record_fetched_pending_write_size(
                        &response.intent_id,
                        &response.object_id,
                        &response.version,
                        &content_hash,
                        local_size_bytes,
                    )?;
                    self.record_local_pending_write_receipt_if_desired(
                        &response.intent_id,
                        &response.object_id,
                        &response.version,
                        &content_hash,
                    )?;
                    self.wake_pending_retries_after_fetch_success(&response.intent_id);
                    return Ok(NetworkedDfsServiceEvent::BlobFetched {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        version: response.version,
                        source_node_id: frame_source_node_id,
                        content_hash,
                    });
                }
                if let Err(error) = self.store_command_bytes(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    &bytes,
                ) {
                    self.record_fetch_failed_source_and_wake_retries(
                        &response.intent_id,
                        &response.source_node_id,
                    );
                    return Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                        intent_id: response.intent_id,
                        object_id: response.object_id,
                        source_node_id: response.source_node_id,
                        reason: error.to_string(),
                    });
                }
                self.record_fetched_pending_write_size(
                    &response.intent_id,
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    bytes.len() as u64,
                )?;
                self.record_local_pending_write_receipt_if_desired(
                    &response.intent_id,
                    &response.object_id,
                    &response.version,
                    &content_hash,
                )?;
                self.wake_pending_retries_after_fetch_success(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    source_node_id: frame_source_node_id,
                    content_hash,
                })
            }
            DfsFetchBlobResult::NotFound => {
                self.record_fetch_failed_source_and_wake_retries(
                    &response.intent_id,
                    &response.source_node_id,
                );
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason: "blob not found".to_string(),
                })
            }
            DfsFetchBlobResult::Rejected { reason } => {
                self.record_fetch_failed_source_and_wake_retries(
                    &response.intent_id,
                    &response.source_node_id,
                );
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason,
                })
            }
        }
    }

    fn handle_fetch_blob_response_chunk(
        &mut self,
        frame_source_node_id: NodeId,
        chunk: DfsFetchBlobResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if frame_source_node_id != chunk.source_node_id {
            let reason = format!(
                "DFS fetch response source mismatch: frame source {}, payload source {}",
                frame_source_node_id, chunk.source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason,
            }));
        }
        let Some(pending) = self.pending_writes.get(&chunk.intent_id).cloned() else {
            let reason = format!(
                "distributed DFS error: missing pending write for fetched blob chunk: {}",
                chunk.intent_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: chunk.source_node_id,
                reason,
            }));
        };
        if !pending.fetch_inflight_from.contains(&frame_source_node_id) {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response source".to_string(),
            }));
        }
        if pending
            .fetch_inflight_request_ids
            .get(&frame_source_node_id)
            != Some(&chunk.request_id)
        {
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                source_node_id: frame_source_node_id,
                reason: "unexpected DFS fetch response request id".to_string(),
            }));
        }
        if pending.object_id != chunk.object_id
            || pending.version != chunk.version
            || pending.content_hash != chunk.content_hash
        {
            let reason = DfsError::ContentHashMismatch {
                expected: pending.content_hash,
                actual: chunk.content_hash.clone(),
            }
            .to_string();
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if let Some(local_size_bytes) = self.local_object_version_manifest_size_if_matches(
            &chunk.object_id,
            &chunk.version,
            &chunk.content_hash,
        ) {
            self.record_fetched_pending_write_size(
                &chunk.intent_id,
                &chunk.object_id,
                &chunk.version,
                &chunk.content_hash,
                local_size_bytes,
            )?;
            self.record_local_pending_write_receipt_if_desired(
                &chunk.intent_id,
                &chunk.object_id,
                &chunk.version,
                &chunk.content_hash,
            )?;
            self.clear_fetch_response_transfer_state(
                &chunk.intent_id,
                &chunk.source_node_id,
                &chunk.request_id,
            );
            self.wake_pending_retries_after_fetch_success(&chunk.intent_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
                intent_id: chunk.intent_id,
                object_id: chunk.object_id,
                version: chunk.version,
                source_node_id: frame_source_node_id,
                content_hash: chunk.content_hash,
            }));
        }
        if chunk.total_chunks == 0 {
            let reason = format!(
                "chunked DFS fetch response {} has no chunks",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if pending.size_bytes != 0 && chunk.total_size_bytes != pending.size_bytes {
            let reason = format!(
                "chunked DFS fetch response size does not match pending write: expected {}, actual {}",
                pending.size_bytes, chunk.total_size_bytes
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        if let Err(error) = validate_chunk_bytes(
            "chunked DFS fetch response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS fetch response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.incoming_fetch_transfers.remove(&chunk.request_id);
            return self.reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
        }

        if !self
            .incoming_fetch_transfers
            .contains_key(&chunk.request_id)
        {
            let assembler = match self.content.begin_chunked_object_version(
                chunk.object_id.as_str(),
                chunk.version.as_str(),
                &chunk.content_hash,
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
            let transfer = match InProgressFetchBlobTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk.content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                assembler,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
            self.incoming_fetch_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_fetch_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk.content_hash
            || transfer.total_size_bytes != chunk.total_size_bytes
        {
            let reason = format!(
                "conflicting DFS fetch response chunk metadata for {}",
                chunk.request_id
            );
            return self.reject_fetch_response_chunk_and_fail_source(chunk, reason);
        }
        let request_id = chunk.request_id.clone();
        let intent_id = chunk.intent_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let source_node_id = chunk.source_node_id.clone();
        let content_hash = chunk.content_hash.clone();
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes.clone())
            {
                Ok(true) => true,
                Ok(false) => false,
                Err(error) => {
                    return self
                        .reject_fetch_response_chunk_and_fail_source(chunk, error.to_string());
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_writes.get_mut(&intent_id) {
            let entry = pending
                .fetch_next_chunk_indexes
                .entry(source_node_id.clone())
                .or_insert(0);
            *entry = (*entry).max(next_chunk_index);
        }
        if !transfer_complete {
            self.request_next_fetch_window_if_due(
                &intent_id,
                &source_node_id,
                next_chunk_index,
                chunk.total_chunks,
            )?;
            return Ok(None);
        }
        let transfer = self
            .incoming_fetch_transfers
            .remove(&request_id)
            .expect("completed fetch transfer");
        if let Some(pending) = self.pending_writes.get_mut(&intent_id) {
            pending.fetch_inflight_from.remove(&source_node_id);
            pending.fetch_inflight_request_ids.remove(&source_node_id);
            pending.fetch_inflight_started_ticks.remove(&source_node_id);
            pending.fetch_next_chunk_indexes.remove(&source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(&source_node_id);
        }
        if let Some(local_size_bytes) =
            self.local_object_version_manifest_size_if_matches(&object_id, &version, &content_hash)
        {
            self.record_fetched_pending_write_size(
                &intent_id,
                &object_id,
                &version,
                &content_hash,
                local_size_bytes,
            )?;
            self.record_local_pending_write_receipt_if_desired(
                &intent_id,
                &object_id,
                &version,
                &content_hash,
            )?;
            self.wake_pending_retries_after_fetch_success(&intent_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
                intent_id,
                object_id,
                version,
                source_node_id,
                content_hash,
            }));
        }
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                self.record_fetch_failed_source_and_wake_retries(&intent_id, &source_node_id);
                return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id,
                    object_id,
                    source_node_id,
                    reason: error.to_string(),
                }));
            }
        };
        if manifest.content_hash != content_hash {
            self.record_fetch_failed_source_and_wake_retries(&intent_id, &source_node_id);
            return Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
                intent_id,
                object_id,
                source_node_id,
                reason: DfsError::ContentHashMismatch {
                    expected: content_hash,
                    actual: manifest.content_hash,
                }
                .to_string(),
            }));
        }
        self.record_fetched_pending_write_size(
            &intent_id,
            &object_id,
            &version,
            &manifest.content_hash,
            manifest.size_bytes,
        )?;
        self.record_local_pending_write_receipt_if_desired(
            &intent_id,
            &object_id,
            &version,
            &manifest.content_hash,
        )?;
        self.wake_pending_retries_after_fetch_success(&intent_id);
        Ok(Some(NetworkedDfsServiceEvent::BlobFetched {
            intent_id,
            object_id,
            version,
            source_node_id,
            content_hash: manifest.content_hash,
        }))
    }

    fn reject_fetch_response_chunk_and_fail_source(
        &mut self,
        chunk: DfsFetchBlobResponseChunk,
        reason: String,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        self.clear_fetch_response_transfer_state(
            &chunk.intent_id,
            &chunk.source_node_id,
            &chunk.request_id,
        );
        self.record_fetch_failed_source_and_wake_retries(&chunk.intent_id, &chunk.source_node_id);
        Ok(Some(NetworkedDfsServiceEvent::BlobFetchRejected {
            intent_id: chunk.intent_id,
            object_id: chunk.object_id,
            source_node_id: chunk.source_node_id,
            reason,
        }))
    }

    fn record_fetched_pending_write_size(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        size_bytes: u64,
    ) -> Result<(), DfsError> {
        if let Some(pending) = self.pending_writes.get_mut(intent_id)
            && pending.size_bytes == 0
        {
            pending.size_bytes = size_bytes;
        }
        if let Some(backend) = self.metadata_backend.as_mut()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash.as_deref() == Some(content_hash)
            && version_record.size_bytes != Some(size_bytes)
        {
            version_record.size_bytes = Some(size_bytes);
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    fn record_local_pending_write_receipt_if_desired(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Ok(());
        };
        if !intent.desired_replica_nodes.contains(&self.node_id) {
            return Ok(());
        }
        if self
            .coordinator
            .receipts_for_intent(intent_id)
            .iter()
            .any(|receipt| receipt.replica_node_id == self.node_id)
        {
            return Ok(());
        }
        if self
            .local_object_version_manifest_size_if_matches(object_id, version, content_hash)
            .is_none()
        {
            return Ok(());
        }
        let local_node_id = self.node_id.clone();
        self.put_metadata_receipt(
            &intent.file_system_id,
            object_id,
            version,
            &local_node_id,
            content_hash,
        )?;
        self.coordinator
            .record_durable_receipt(intent_id, local_node_id)?;
        self.try_apply_committed_intent_if_ready(intent_id)?;
        self.cleanup_committed_pending_write_if_repaired(intent_id);
        Ok(())
    }

    fn wake_pending_retries_after_fetch_success(&mut self, intent_id: &str) {
        let stale_request_ids = self
            .pending_writes
            .get(intent_id)
            .map(|pending| {
                pending
                    .fetch_inflight_request_ids
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for request_id in stale_request_ids {
            self.incoming_fetch_transfers.remove(&request_id);
        }
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            for state in pending.repair_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            pending.fetch_inflight_from.clear();
            pending.fetch_inflight_request_ids.clear();
            pending.fetch_inflight_started_ticks.clear();
            pending.fetch_next_chunk_indexes.clear();
            pending.fetch_window_end_chunk_indexes.clear();
            pending.fetch_failed_from.clear();
        }
    }

    fn clear_fetch_response_transfer_state(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
        request_id: &str,
    ) {
        self.incoming_fetch_transfers.remove(request_id);
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_request_ids.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_next_chunk_indexes.remove(source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(source_node_id);
        }
    }

    fn record_fetch_failed_source_and_wake_retries(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
    ) {
        let stale_request_id = self
            .pending_writes
            .get_mut(intent_id)
            .and_then(|pending| pending.fetch_inflight_request_ids.remove(source_node_id));
        if let Some(request_id) = stale_request_id {
            self.incoming_fetch_transfers.remove(&request_id);
        }
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            pending.fetch_next_chunk_indexes.remove(source_node_id);
            pending
                .fetch_window_end_chunk_indexes
                .remove(source_node_id);
            pending.fetch_failed_from.insert(source_node_id.clone());
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
        }
    }

    fn handle_networked_read_request(
        &mut self,
        requester_node_id: NodeId,
        request: DfsReadRequest,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let requested_range = match (request.range_offset, request.range_max_bytes) {
            (Some(offset), Some(max_bytes)) if max_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES => {
                Ok(Some((offset, max_bytes)))
            }
            (Some(_), Some(max_bytes)) => Err(format!(
                "remote DFS range read window {max_bytes} exceeds inline limit {DFS_BLOB_INLINE_LIMIT_BYTES}"
            )),
            (None, None) => Ok(None),
            _ => Err("DFS read range request must include both offset and max bytes".to_string()),
        };
        let requested_range = if requested_range.as_ref().is_ok_and(Option::is_some)
            && (request.start_chunk_index != 0 || request.max_chunks != 0)
        {
            Err("DFS read range request cannot include chunk window fields".to_string())
        } else {
            requested_range
        };
        let mut sent_chunked_response = false;
        let result = match requested_range {
            Err(reason) => DfsReadResult::Rejected { reason },
            Ok(range) => match self.current_read_object_metadata(&request.object_id) {
                Ok(object) if !object.current_locations.contains(&self.node_id) => {
                    DfsReadResult::Rejected {
                        reason: format!(
                            "node {} is not a current location for DFS object {}",
                            self.node_id, request.object_id
                        ),
                    }
                }
                Ok(_) => match range {
                    Some((offset, max_bytes)) => {
                        match self.read_local_object_range_with_state(
                            &request.object_id,
                            offset,
                            max_bytes,
                            false,
                        )? {
                            NetworkedDfsRangeReadOutcome::Found { data, .. } => {
                                DfsReadResult::RangeFound(data)
                            }
                            NetworkedDfsRangeReadOutcome::Conflicted { conflicts, .. } => {
                                DfsReadResult::Rejected {
                                    reason: read_conflicts_reason(conflicts),
                                }
                            }
                            NetworkedDfsRangeReadOutcome::PendingReplication { .. }
                            | NetworkedDfsRangeReadOutcome::NotConfirmed { .. }
                            | NetworkedDfsRangeReadOutcome::NotFound => DfsReadResult::NotFound,
                        }
                    }
                    None => {
                        let stream_outcome =
                            self.open_local_current_object_stream_with_state(&request.object_id)?;
                        match stream_outcome {
                            NetworkedDfsReadStreamOutcome::Found(stream)
                                if stream.total_size_bytes > DFS_BLOB_INLINE_LIMIT_BYTES as u64 =>
                            {
                                let window_outcome = if request.start_chunk_index == 0
                                    && request.max_chunks == 0
                                {
                                    Ok(NetworkedDfsReadStreamOutcome::Found(stream))
                                } else {
                                    self.open_local_current_object_read_window_stream_with_state(
                                        &request.object_id,
                                        request.start_chunk_index,
                                        request.max_chunks,
                                    )
                                };
                                match window_outcome {
                                    Ok(NetworkedDfsReadStreamOutcome::Found(stream)) => {
                                        self.send_networked_read_response_chunks_from_reader(
                                            &requester_node_id,
                                            &request,
                                            &stream.data,
                                            stream.total_size_bytes,
                                            stream.reader,
                                        )?;
                                        sent_chunked_response = true;
                                        DfsReadResult::Found(stream.data)
                                    }
                                    Ok(NetworkedDfsReadStreamOutcome::Conflicted { conflicts }) => {
                                        DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        }
                                    }
                                    Ok(NetworkedDfsReadStreamOutcome::PendingReplication)
                                    | Ok(NetworkedDfsReadStreamOutcome::NotFound) => {
                                        DfsReadResult::NotFound
                                    }
                                    Err(error) => DfsReadResult::Rejected {
                                        reason: error.to_string(),
                                    },
                                }
                            }
                            NetworkedDfsReadStreamOutcome::Found(_) => {
                                if request.start_chunk_index != 0 {
                                    DfsReadResult::Rejected {
                                        reason: format!(
                                            "DFS read chunk window starts outside inline object: {}",
                                            request.start_chunk_index
                                        ),
                                    }
                                } else {
                                    match self
                                        .read_local_object_with_state(&request.object_id, false)?
                                    {
                                        NetworkedDfsReadOutcome::Found { data, .. } => {
                                            DfsReadResult::Found(data)
                                        }
                                        NetworkedDfsReadOutcome::Conflicted {
                                            conflicts, ..
                                        } => DfsReadResult::Rejected {
                                            reason: read_conflicts_reason(conflicts),
                                        },
                                        NetworkedDfsReadOutcome::PendingReplication { .. }
                                        | NetworkedDfsReadOutcome::NotConfirmed { .. }
                                        | NetworkedDfsReadOutcome::NotFound => {
                                            DfsReadResult::NotFound
                                        }
                                    }
                                }
                            }
                            NetworkedDfsReadStreamOutcome::Conflicted { conflicts } => {
                                DfsReadResult::Rejected {
                                    reason: read_conflicts_reason(conflicts),
                                }
                            }
                            NetworkedDfsReadStreamOutcome::PendingReplication
                            | NetworkedDfsReadStreamOutcome::NotFound => DfsReadResult::NotFound,
                        }
                    }
                },
                Err(DfsError::UnknownObject { .. }) => DfsReadResult::NotFound,
                Err(error) => return Err(error),
            },
        };
        match &result {
            DfsReadResult::Found(_) if sent_chunked_response => {}
            DfsReadResult::Found(data) if data.bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES => {
                self.send_networked_read_response_chunks(&requester_node_id, &request, data)?;
            }
            _ => {
                self.send_dfs_message_if_routable(
                    &requester_node_id,
                    DfsNetworkMessage::ReadResponse(DfsReadResponse {
                        request_id: request.request_id.clone(),
                        object_id: request.object_id.clone(),
                        result: result.clone(),
                    }),
                )?;
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadServed {
            request_id: request.request_id,
            object_id: request.object_id,
            requester_node_id,
            result,
        })
    }

    fn send_networked_read_response_chunks(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count(&data.bytes)?;
        let end_chunk_index =
            fetch_response_window_end(request.start_chunk_index, total_chunks, request.max_chunks);
        for (chunk_index, chunk) in data
            .bytes
            .chunks(DFS_BLOB_CHUNK_BYTES)
            .enumerate()
            .skip(request.start_chunk_index as usize)
            .take(end_chunk_index.saturating_sub(request.start_chunk_index) as usize)
        {
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    total_size_bytes: data.bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn send_networked_read_response_chunks_from_reader(
        &mut self,
        requester_node_id: &NodeId,
        request: &DfsReadRequest,
        data: &DfsReadData,
        total_size_bytes: u64,
        mut reader: ObjectVersionReader,
    ) -> Result<(), DfsError> {
        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        let end_chunk_index =
            fetch_response_window_end(request.start_chunk_index, total_chunks, request.max_chunks);
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in request.start_chunk_index..end_chunk_index {
            let chunk_offset = u64::from(chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
            let chunk_len = (total_size_bytes - chunk_offset).min(DFS_BLOB_CHUNK_BYTES as u64);
            let chunk_len = usize::try_from(chunk_len).map_err(|_| DfsError::Distributed {
                message: format!("DFS read chunk is too large to fit in memory: {chunk_len} bytes"),
            })?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {:?} chunk {}: {}",
                        data.content_hash, chunk_index, source
                    ),
                })?;
            let sent = self.send_dfs_message_if_routable(
                requester_node_id,
                DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                    request_id: request.request_id.clone(),
                    object_id: request.object_id.clone(),
                    version: data.version.clone(),
                    content_hash: data.content_hash.clone(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&buffer[..chunk_len])),
                    bytes: buffer[..chunk_len].to_vec(),
                }),
            )?;
            if !sent {
                break;
            }
        }
        Ok(())
    }

    fn handle_networked_read_response(
        &mut self,
        source_node_id: NodeId,
        response: DfsReadResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let Some(pending) = self.pending_reads.get(&response.request_id).cloned() else {
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            });
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            });
        }
        self.pending_reads.remove(&response.request_id);
        if pending.object_id != response.object_id {
            self.incoming_read_transfers.remove(&response.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: response.request_id,
                object_id: response.object_id.clone(),
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "unexpected DFS read response object: expected {}, actual {}",
                        pending.object_id, response.object_id
                    ),
                },
            });
        }
        match &response.result {
            DfsReadResult::Found(data) => {
                if pending.range_offset.is_some() || pending.range_max_bytes.is_some() {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "unexpected full DFS read response for range request"
                                .to_string(),
                        },
                    });
                }
                if data.object_id != pending.object_id {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "unexpected DFS read response payload object: expected {}, actual {}",
                                pending.object_id, data.object_id
                            ),
                        },
                    });
                }
                if self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                if !self.pending_read_source_still_current(&pending, &source_node_id)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason:
                                "stale DFS read response: source is no longer a current location"
                                    .to_string(),
                        },
                    });
                }
                if data.version != pending.version {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected version {}, actual {}",
                                pending.version, data.version
                            ),
                        },
                    });
                }
                if data.content_hash != pending.content_hash {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS read response: expected content hash {:?}, actual {:?}",
                                pending.content_hash, data.content_hash
                            ),
                        },
                    });
                }
                if let Err(error) = self.store_remote_read_data(&source_node_id, data) {
                    if matches!(error, DfsError::ContentHashMismatch { .. }) {
                        self.mark_remote_read_location_missing(
                            &pending.object_id,
                            &pending.version,
                            &pending.content_hash,
                            &source_node_id,
                        )?;
                        return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                            request_id: response.request_id,
                            object_id: response.object_id,
                            source_node_id,
                            result: DfsReadResult::Rejected {
                                reason: error.to_string(),
                            },
                        });
                    }
                    return Err(error);
                }
                self.clear_pending_remote_reads_for_object_version(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                );
            }
            DfsReadResult::RangeFound(data) => {
                let (Some(requested_offset), Some(requested_max_bytes)) =
                    (pending.range_offset, pending.range_max_bytes)
                else {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "unexpected range DFS read response for full-object request"
                                .to_string(),
                        },
                    });
                };
                if data.object_id != pending.object_id {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "unexpected DFS range read response payload object: expected {}, actual {}",
                                pending.object_id, data.object_id
                            ),
                        },
                    });
                }
                if self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                if !self.pending_read_source_still_current(&pending, &source_node_id)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason:
                                "stale DFS read response: source is no longer a current location"
                                    .to_string(),
                        },
                    });
                }
                if data.version != pending.version {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS range read response: expected version {}, actual {}",
                                pending.version, data.version
                            ),
                        },
                    });
                }
                if data.content_hash != pending.content_hash {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "stale DFS range read response: expected content hash {:?}, actual {:?}",
                                pending.content_hash, data.content_hash
                            ),
                        },
                    });
                }
                if let Some(expected_size) = pending.content_size_bytes
                    && expected_size != data.total_size_bytes
                {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response size does not match object metadata: expected {expected_size}, actual {}",
                                data.total_size_bytes
                            ),
                        },
                    });
                }
                let expected_offset = requested_offset.min(data.total_size_bytes);
                if data.offset != expected_offset {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response offset mismatch: expected {expected_offset}, actual {}",
                                data.offset
                            ),
                        },
                    });
                }
                if data.bytes.len() > requested_max_bytes {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response exceeds requested window: expected at most {requested_max_bytes}, actual {}",
                                data.bytes.len()
                            ),
                        },
                    });
                }
                let Some(range_end) = data.offset.checked_add(data.bytes.len() as u64) else {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "DFS range read response offset overflow".to_string(),
                        },
                    });
                };
                if range_end > data.total_size_bytes {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: format!(
                                "DFS range read response exceeds total size: end {range_end}, total {}",
                                data.total_size_bytes
                            ),
                        },
                    });
                }
            }
            DfsReadResult::NotFound => {
                if self.pending_read_metadata_advanced(&pending)? {
                    return Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: response.request_id,
                        object_id: response.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: "stale DFS read response: object metadata advanced".to_string(),
                        },
                    });
                }
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
            }
            DfsReadResult::Rejected { reason } => {
                if remote_read_rejection_prunes_location(reason) {
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                }
            }
        }
        Ok(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: response.request_id,
            object_id: response.object_id,
            source_node_id,
            result: response.result,
        })
    }

    fn mark_remote_read_location_missing(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
        source_node_id: &NodeId,
    ) -> Result<(), DfsError> {
        if let Some(object) = self.metadata.objects.get_mut(object_id) {
            if object.version == *version && object.content_hash == *content_hash {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
            }
        }
        let backend_object = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut object) = backend.get_object(object_id)?
            && object.version == *version
            && object.content_hash == *content_hash
        {
            if !object.current_locations.contains(source_node_id) {
                None
            } else {
                object
                    .current_locations
                    .retain(|node_id| node_id != source_node_id);
                Some(object)
            }
        } else {
            None
        };
        let backend_version = if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(mut version_record) = backend.get_version(object_id, version)?
            && version_record.content_hash == *content_hash
        {
            if !version_record.locations.contains(source_node_id) {
                None
            } else {
                version_record
                    .locations
                    .retain(|node_id| node_id != source_node_id);
                Some(version_record)
            }
        } else {
            None
        };
        if let Some(object) = backend_object {
            self.put_visible_object_in_metadata_backend(&object)?;
        } else if let Some(version_record) = backend_version
            && let Some(backend) = self.metadata_backend.as_mut()
        {
            backend.put_version(&version_record)?;
        }
        Ok(())
    }

    fn handle_networked_read_response_chunk(
        &mut self,
        source_node_id: NodeId,
        chunk: DfsReadResponseChunk,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        let Some(pending) = self.pending_reads.get(&chunk.request_id).cloned() else {
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unknown DFS read request".to_string(),
                },
            }));
        };
        if source_node_id != pending.target_node_id {
            let reason = format!(
                "unexpected DFS read response source: expected {}, actual {}",
                pending.target_node_id, source_node_id
            );
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.object_id != chunk.object_id {
            let reason = format!(
                "unexpected DFS read response object: expected {}, actual {}",
                pending.object_id, chunk.object_id
            );
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected { reason },
            }));
        }
        if pending.range_offset.is_some() || pending.range_max_bytes.is_some() {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "unexpected chunked DFS read response for range request".to_string(),
                },
            }));
        }
        if pending.version != chunk.version || pending.content_hash != chunk.content_hash {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response chunk".to_string(),
                },
            }));
        }
        if self.pending_read_metadata_advanced(&pending)? {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: object metadata advanced".to_string(),
                },
            }));
        }
        if !self.pending_read_source_still_current(&pending, &source_node_id)? {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "stale DFS read response: source is no longer a current location"
                        .to_string(),
                },
            }));
        }
        if let Some(expected_size) = pending.content_size_bytes
            && expected_size != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response size does not match object metadata: expected {expected_size}, actual {}",
                        chunk.total_size_bytes
                    ),
                },
            }));
        }
        if chunk.total_chunks == 0 {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "chunked DFS read response {} has no chunks",
                        chunk.request_id
                    ),
                },
            }));
        }

        let Some(chunk_content_hash) = chunk.content_hash.clone() else {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: "chunked DFS read response is missing content hash".to_string(),
                },
            }));
        };

        if let Err(error) = validate_chunk_bytes(
            "chunked DFS read response",
            chunk.total_size_bytes,
            chunk.chunk_index,
            chunk.total_chunks,
            &chunk.bytes,
        )
        .and_then(|()| {
            validate_chunk_hash(
                "chunked DFS read response",
                chunk.chunk_index,
                &chunk.bytes,
                chunk.chunk_hash.as_deref(),
            )
        }) {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id,
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: error.to_string(),
                },
            }));
        }

        if !self.incoming_read_transfers.contains_key(&chunk.request_id) {
            if let Err(error) = validate_chunked_transfer_shape(
                "chunked DFS read response",
                chunk.total_size_bytes,
                chunk.total_chunks,
            ) {
                self.clear_read_response_transfer_state(&chunk.request_id);
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id: chunk.request_id,
                    object_id: chunk.object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            let assembler = match self.content.begin_chunked_object_version(
                chunk.object_id.as_str(),
                chunk.version.as_str(),
                &chunk_content_hash,
                chunk.total_size_bytes,
                chunk.total_chunks,
                DFS_BLOB_CHUNK_BYTES,
            ) {
                Ok(assembler) => assembler,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            let transfer = match InProgressReadTransfer::new(
                chunk.object_id.clone(),
                chunk.version.clone(),
                chunk_content_hash.clone(),
                chunk.total_size_bytes,
                chunk.total_chunks,
                assembler,
            ) {
                Ok(transfer) => transfer,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id: chunk.object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
            self.incoming_read_transfers
                .insert(chunk.request_id.clone(), transfer);
        }
        let transfer = self
            .incoming_read_transfers
            .get_mut(&chunk.request_id)
            .expect("transfer inserted");
        if transfer.object_id != chunk.object_id
            || transfer.version != chunk.version
            || transfer.content_hash != chunk_content_hash
            || transfer.total_size_bytes != chunk.total_size_bytes
        {
            self.pending_reads.remove(&chunk.request_id);
            self.incoming_read_transfers.remove(&chunk.request_id);
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id: chunk.request_id.clone(),
                object_id: chunk.object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: format!(
                        "conflicting DFS read response chunk metadata for {}",
                        chunk.request_id
                    ),
                },
            }));
        }
        let request_id = chunk.request_id.clone();
        let object_id = chunk.object_id.clone();
        let version = chunk.version.clone();
        let content_hash = chunk_content_hash.clone();
        let transfer_complete =
            match transfer.insert_chunk(chunk.chunk_index, chunk.total_chunks, chunk.bytes) {
                Ok(true) => true,
                Ok(false) => false,
                Err(error) => {
                    self.clear_read_response_transfer_state(&chunk.request_id);
                    self.mark_remote_read_location_missing(
                        &pending.object_id,
                        &pending.version,
                        &pending.content_hash,
                        &source_node_id,
                    )?;
                    return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                        request_id: chunk.request_id,
                        object_id,
                        source_node_id,
                        result: DfsReadResult::Rejected {
                            reason: error.to_string(),
                        },
                    }));
                }
            };
        let next_chunk_index = transfer.contiguous_received_chunk_count();
        if let Some(pending) = self.pending_reads.get_mut(&request_id) {
            pending.next_chunk_index = pending.next_chunk_index.max(next_chunk_index);
        }
        if !transfer_complete {
            return self.request_next_read_window_if_due(
                &request_id,
                &source_node_id,
                next_chunk_index,
                chunk.total_chunks,
            );
        }
        let transfer = self
            .incoming_read_transfers
            .remove(&request_id)
            .expect("completed read transfer");
        self.pending_reads.remove(&request_id);
        let manifest = match self
            .content
            .commit_chunked_object_version(transfer.assembler)
        {
            Ok(manifest) => manifest,
            Err(error) => {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
        };
        if manifest.content_hash != content_hash {
            self.mark_remote_read_location_missing(
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &source_node_id,
            )?;
            return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                request_id,
                object_id,
                source_node_id,
                result: DfsReadResult::Rejected {
                    reason: DfsError::ContentHashMismatch {
                        expected: content_hash,
                        actual: manifest.content_hash,
                    }
                    .to_string(),
                },
            }));
        }
        let data = DfsReadData {
            object_id: object_id.clone(),
            version,
            content_hash: Some(manifest.content_hash),
            bytes: Vec::new(),
        };
        if let Err(error) = self.store_remote_read_manifest(&data, manifest.size_bytes) {
            if matches!(error, DfsError::ContentHashMismatch { .. }) {
                self.mark_remote_read_location_missing(
                    &pending.object_id,
                    &pending.version,
                    &pending.content_hash,
                    &source_node_id,
                )?;
                return Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
                    request_id,
                    object_id,
                    source_node_id,
                    result: DfsReadResult::Rejected {
                        reason: error.to_string(),
                    },
                }));
            }
            return Err(error);
        }
        self.clear_pending_remote_reads_for_object_version(
            &data.object_id,
            &data.version,
            &data.content_hash,
        );
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id,
            object_id,
            source_node_id,
            result: DfsReadResult::Found(data),
        }))
    }

    fn request_next_read_window_if_due(
        &mut self,
        request_id: &str,
        source_node_id: &NodeId,
        next_chunk_index: u32,
        total_chunks: u32,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if next_chunk_index >= total_chunks {
            return Ok(None);
        }
        let Some(pending) = self.pending_reads.get(request_id).cloned() else {
            return Ok(None);
        };
        if pending.range_offset.is_some() || pending.range_max_bytes.is_some() {
            return Ok(None);
        }
        let Some(window_end) = pending.window_end_chunk_index else {
            return Ok(None);
        };
        if next_chunk_index < window_end {
            return Ok(None);
        }
        let sent = self.send_dfs_message_if_routable(
            source_node_id,
            DfsNetworkMessage::ReadRequest(DfsReadRequest {
                request_id: request_id.to_string(),
                object_id: pending.object_id.clone(),
                range_offset: None,
                range_max_bytes: None,
                start_chunk_index: next_chunk_index,
                max_chunks: DFS_READ_CHUNK_WINDOW_SIZE,
            }),
        )?;
        if sent {
            if let Some(pending) = self.pending_reads.get_mut(request_id) {
                pending.next_chunk_index = next_chunk_index;
                pending.window_end_chunk_index =
                    Self::read_window_end_chunk_index(next_chunk_index, DFS_READ_CHUNK_WINDOW_SIZE);
                pending.next_retry_at = Instant::now() + DFS_REMOTE_READ_RETRY_DELAY;
            }
            return Ok(None);
        }

        self.clear_read_response_transfer_state(request_id);
        self.mark_remote_read_location_missing(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
            source_node_id,
        )?;
        Ok(Some(NetworkedDfsServiceEvent::RemoteReadCompleted {
            request_id: request_id.to_string(),
            object_id: pending.object_id,
            source_node_id: source_node_id.clone(),
            result: DfsReadResult::Rejected {
                reason: "unable to route DFS read continuation request".to_string(),
            },
        }))
    }

    fn clear_read_response_transfer_state(&mut self, request_id: &str) {
        self.pending_reads.remove(request_id);
        self.incoming_read_transfers.remove(request_id);
    }

    fn pending_read_metadata_advanced(
        &self,
        pending: &PendingNetworkedRead,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version != pending.version
            || object.content_hash != pending.content_hash
            || object.content_size_bytes != pending.content_size_bytes)
    }

    fn pending_read_source_still_current(
        &self,
        pending: &PendingNetworkedRead,
        source_node_id: &NodeId,
    ) -> Result<bool, DfsError> {
        let object = self.current_read_object_metadata(&pending.object_id)?;
        Ok(object.version == pending.version
            && object.content_hash == pending.content_hash
            && object.content_size_bytes == pending.content_size_bytes
            && object.current_locations.contains(source_node_id))
    }

    fn clear_pending_remote_reads_for_object_version(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &Option<String>,
    ) {
        let request_ids: Vec<String> = self
            .pending_reads
            .iter()
            .filter(|(_, pending)| {
                pending.object_id == *object_id
                    && pending.version == *version
                    && pending.content_hash == *content_hash
            })
            .map(|(request_id, _)| request_id.clone())
            .collect();
        for request_id in request_ids {
            self.pending_reads.remove(&request_id);
            self.incoming_read_transfers.remove(&request_id);
        }
    }

    fn store_remote_read_data(
        &mut self,
        _source_node_id: &NodeId,
        data: &DfsReadData,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(&data.object_id)?;
        if object.version != data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: object.version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = object.content_hash.clone() else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != &expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash.clone(),
            });
        }
        self.store_command_bytes(&data.object_id, &data.version, &expected_hash, &data.bytes)?;
        self.store_remote_read_manifest(data, data.bytes.len() as u64)
    }

    fn store_remote_read_manifest(
        &mut self,
        data: &DfsReadData,
        size_bytes: u64,
    ) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(&data.object_id)?;
        if object.version != data.version {
            return Err(DfsError::ContentHashMismatch {
                expected: object.version.to_string(),
                actual: data.version.to_string(),
            });
        }
        let Some(expected_hash) = object.content_hash.clone() else {
            return Err(DfsError::ContentHashMismatch {
                expected: "committed content hash".to_string(),
                actual: data
                    .content_hash
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            });
        };
        if let Some(actual_hash) = data.content_hash.as_ref()
            && actual_hash != &expected_hash
        {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash,
                actual: actual_hash.clone(),
            });
        }
        if let Some(expected_size) = object.content_size_bytes
            && expected_size != size_bytes
        {
            return Err(DfsError::Distributed {
                message: format!(
                    "remote read size does not match object metadata: expected {expected_size}, actual {size_bytes}"
                ),
            });
        }
        let mut object = object;
        object.content_size_bytes = Some(size_bytes);
        self.put_object_version_size_in_metadata_backend(&object, Some(size_bytes))?;
        self.metadata.add_object(object)?;
        Ok(())
    }

    fn retry_pending_stages(&mut self) -> Result<(), DfsError> {
        let pending_intent_ids = self.pending_writes.keys().cloned().collect::<Vec<_>>();
        for intent_id in pending_intent_ids {
            let Some(intent) = self.coordinator.intent(&intent_id).cloned() else {
                continue;
            };
            if !matches!(intent.state, DfsWriteIntentState::AwaitingReceipts) {
                continue;
            }
            let Some(pending) = self.pending_writes.get(&intent_id).cloned() else {
                continue;
            };
            let durable_nodes = self
                .coordinator
                .receipts_for_intent(&intent_id)
                .into_iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<BTreeSet<_>>();
            let stage_targets = intent
                .desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id && !durable_nodes.contains(*node_id))
                .filter(|node_id| self.stage_retry_is_due(&intent_id, node_id))
                .cloned()
                .collect::<Vec<_>>();
            if stage_targets.is_empty() {
                continue;
            }
            let object_metadata = Some(self.current_object_for_write(&pending.object_id)?);
            let mut full_stage_targets = Vec::new();
            for replica_node_id in stage_targets {
                let request_id = pending
                    .stage_inflight_request_ids
                    .get(&replica_node_id)
                    .cloned()
                    .unwrap_or_else(|| self.next_request_id());
                match self.send_pending_stage_patch_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &intent,
                    &pending,
                    object_metadata.clone(),
                )? {
                    Some(true) => {
                        self.record_stage_inflight(&intent_id, &replica_node_id, request_id);
                    }
                    Some(false) => {
                        self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                    }
                    None => full_stage_targets.push((replica_node_id, request_id)),
                }
            }
            if full_stage_targets.is_empty() {
                continue;
            }
            if self.open_pending_write_blob_stream(&pending).is_err() {
                let fetch_requested = self.request_stage_blob_from_alternate(&intent, &pending)?;
                if !fetch_requested {
                    for (replica_node_id, _) in full_stage_targets {
                        self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                    }
                }
                continue;
            }
            for (replica_node_id, request_id) in full_stage_targets {
                let start_chunk_index = pending
                    .stage_next_chunk_indexes
                    .get(&replica_node_id)
                    .copied()
                    .unwrap_or(0);
                let outcome = self.send_stage_blob_window_if_routable(
                    &replica_node_id,
                    request_id.clone(),
                    &intent.intent_id,
                    &intent.file_system_id,
                    &intent.object_id,
                    intent.base_version.clone(),
                    &intent.target_version,
                    &pending.content_hash,
                    pending.size_bytes,
                    start_chunk_index,
                    DFS_STAGE_CHUNK_WINDOW_SIZE,
                    object_metadata.clone(),
                )?;
                if outcome.sent {
                    self.record_stage_window_progress(
                        &intent_id,
                        &replica_node_id,
                        request_id,
                        outcome,
                    );
                } else {
                    self.record_stage_retry_scheduled(&intent_id, &replica_node_id);
                }
            }
        }
        Ok(())
    }

    fn send_pending_stage_patch_if_routable(
        &mut self,
        replica_node_id: &NodeId,
        request_id: String,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Option<bool>, DfsError> {
        let Some(patch) = pending.patch.as_ref() else {
            return Ok(None);
        };
        if patch.full_stage_fallback_nodes.contains(replica_node_id) {
            return Ok(None);
        }
        let Some(base_version) = intent.base_version.as_ref() else {
            return Ok(None);
        };
        if base_version != &patch.base_version {
            return Ok(None);
        }
        let sent = self.send_stage_patch_if_routable(
            replica_node_id,
            request_id,
            &intent.intent_id,
            &intent.file_system_id,
            &intent.object_id,
            base_version,
            &intent.target_version,
            &pending.content_hash,
            patch.offset,
            &patch.patch_hash,
            &patch.bytes,
            object_metadata,
        )?;
        Ok(Some(sent))
    }

    fn open_pending_write_blob_stream(
        &self,
        pending: &PendingNetworkedWrite,
    ) -> Result<ObjectVersionReader, DfsError> {
        if let Ok((manifest, reader)) = self
            .content
            .open_object_version_stream(pending.object_id.as_str(), pending.version.as_str())
        {
            if manifest.content_hash == pending.content_hash
                && (pending.size_bytes == 0 || manifest.size_bytes == pending.size_bytes)
            {
                return Ok(reader);
            }
            return Err(DfsError::Distributed {
                message: format!(
                    "pending write manifest mismatch for {} {}: expected hash {} size {}, actual hash {} size {}",
                    pending.object_id,
                    pending.version,
                    pending.content_hash,
                    pending.size_bytes,
                    manifest.content_hash,
                    manifest.size_bytes
                ),
            });
        }

        let reader = self.content.open_blob_stream(&pending.content_hash)?;
        if pending.size_bytes != 0 && reader.size_bytes() != pending.size_bytes {
            return Err(DfsError::Distributed {
                message: format!(
                    "pending write blob size mismatch for {} {}: expected {}, actual {}",
                    pending.object_id,
                    pending.version,
                    pending.size_bytes,
                    reader.size_bytes()
                ),
            });
        }
        Ok(ObjectVersionReader::from_blob(reader))
    }

    fn request_stage_blob_from_alternate(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        for source_node_id in self.stage_blob_sources(&intent.intent_id) {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let start_chunk_index = pending
                .fetch_next_chunk_indexes
                .get(&source_node_id)
                .copied()
                .unwrap_or(0);
            let max_chunks = DFS_FETCH_CHUNK_WINDOW_SIZE;
            let sent = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: intent.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    start_chunk_index,
                    max_chunks,
                }),
            )?;
            if sent {
                self.record_fetch_inflight(
                    &intent.intent_id,
                    source_node_id,
                    request_id,
                    start_chunk_index,
                    max_chunks,
                );
                return Ok(true);
            }
        }
        if skipped_failed_source
            && let Some(pending) = self.pending_writes.get_mut(&intent.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    fn stage_blob_sources(&self, intent_id: &str) -> Vec<NodeId> {
        self.coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .filter_map(|receipt| {
                (receipt.replica_node_id != self.node_id).then_some(receipt.replica_node_id.clone())
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn record_fetch_inflight(
        &mut self,
        intent_id: &str,
        source_node_id: NodeId,
        request_id: String,
        start_chunk_index: u32,
        max_chunks: u32,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.insert(source_node_id.clone());
            pending
                .fetch_inflight_request_ids
                .insert(source_node_id.clone(), request_id);
            pending
                .fetch_inflight_started_ticks
                .insert(source_node_id.clone(), tick);
            if let Some(window_end) =
                Self::fetch_window_end_chunk_index(start_chunk_index, max_chunks)
            {
                pending
                    .fetch_window_end_chunk_indexes
                    .insert(source_node_id.clone(), window_end);
            } else {
                pending
                    .fetch_window_end_chunk_indexes
                    .remove(&source_node_id);
            }
        }
    }

    fn request_next_fetch_window_if_due(
        &mut self,
        intent_id: &str,
        source_node_id: &NodeId,
        next_chunk_index: u32,
        total_chunks: u32,
    ) -> Result<(), DfsError> {
        if next_chunk_index >= total_chunks {
            return Ok(());
        }
        let Some(pending) = self.pending_writes.get(intent_id).cloned() else {
            return Ok(());
        };
        let Some(window_end) = pending
            .fetch_window_end_chunk_indexes
            .get(source_node_id)
            .copied()
        else {
            return Ok(());
        };
        if next_chunk_index < window_end {
            return Ok(());
        }
        let request_id = pending
            .fetch_inflight_request_ids
            .get(source_node_id)
            .cloned()
            .unwrap_or_else(|| self.next_request_id());
        let max_chunks = DFS_FETCH_CHUNK_WINDOW_SIZE;
        let sent = self.send_dfs_message_if_routable(
            source_node_id,
            DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                request_id: request_id.clone(),
                intent_id: intent_id.to_string(),
                object_id: pending.object_id,
                version: pending.version,
                content_hash: pending.content_hash,
                start_chunk_index: next_chunk_index,
                max_chunks,
            }),
        )?;
        if sent {
            self.record_fetch_inflight(
                intent_id,
                source_node_id.clone(),
                request_id,
                next_chunk_index,
                max_chunks,
            );
        } else {
            self.record_fetch_window_retry_needed(intent_id, source_node_id);
        }
        Ok(())
    }

    fn record_fetch_window_retry_needed(&mut self, intent_id: &str, source_node_id: &NodeId) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.fetch_inflight_from.remove(source_node_id);
            pending.fetch_inflight_started_ticks.remove(source_node_id);
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
        }
    }

    fn expire_stale_fetch_requests(&mut self) {
        let tick = self.repair_retry_tick;
        for pending in self.pending_writes.values_mut() {
            let expired_sources = pending
                .fetch_inflight_started_ticks
                .iter()
                .filter_map(|(source_node_id, started_tick)| {
                    let expires_at = started_tick.saturating_add(fetch_inflight_timeout_ticks());
                    (expires_at <= tick).then_some(source_node_id.clone())
                })
                .collect::<Vec<_>>();
            if expired_sources.is_empty() {
                continue;
            }
            for source_node_id in expired_sources {
                pending.fetch_inflight_from.remove(&source_node_id);
                pending.fetch_inflight_started_ticks.remove(&source_node_id);
                pending.fetch_failed_from.insert(source_node_id);
            }
            for state in pending.stage_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
            for state in pending.repair_retries.values_mut() {
                state.next_retry_tick = 0;
                state.inflight = false;
            }
        }
    }

    fn stage_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.pending_writes
            .get(intent_id)
            .and_then(|pending| pending.stage_retries.get(replica_node_id))
            .is_none_or(|state| state.next_retry_tick <= self.repair_retry_tick)
    }

    fn record_stage_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick =
                tick.saturating_add(stage_inflight_retry_delay_ticks(state.attempts));
            pending
                .stage_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
        }
    }

    fn record_stage_window_progress(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
        outcome: StageBlobSendOutcome,
    ) {
        if outcome.complete {
            self.record_stage_inflight(intent_id, replica_node_id, request_id);
            if let Some(pending) = self.pending_writes.get_mut(intent_id) {
                pending.stage_next_chunk_indexes.remove(replica_node_id);
            }
            return;
        }

        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = false;
            state.next_retry_tick = tick;
            pending
                .stage_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
            pending
                .stage_next_chunk_indexes
                .insert(replica_node_id.clone(), outcome.next_chunk_index);
        }
    }

    fn record_stage_retry_scheduled(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .stage_retries
                .entry(replica_node_id.clone())
                .or_default();
            if !state.inflight {
                state.attempts = state.attempts.saturating_add(1);
            }
            state.inflight = false;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
            pending.stage_inflight_request_ids.remove(replica_node_id);
            pending.stage_next_chunk_indexes.remove(replica_node_id);
        }
    }

    fn record_stage_patch_fallback(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id)
            && let Some(patch) = pending.patch.as_mut()
        {
            patch
                .full_stage_fallback_nodes
                .insert(replica_node_id.clone());
        }
    }

    fn repair_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.pending_writes
            .get(intent_id)
            .and_then(|pending| pending.repair_retries.get(replica_node_id))
            .is_none_or(|state| state.next_retry_tick <= self.repair_retry_tick)
    }

    fn record_repair_inflight(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
    ) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
            pending
                .repair_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
            pending.repair_next_chunk_indexes.remove(replica_node_id);
        }
    }

    fn record_repair_window_progress(
        &mut self,
        intent_id: &str,
        replica_node_id: &NodeId,
        request_id: String,
        outcome: StageBlobSendOutcome,
    ) {
        if outcome.complete {
            self.record_repair_inflight(intent_id, replica_node_id, request_id);
            if let Some(pending) = self.pending_writes.get_mut(intent_id) {
                pending.repair_next_chunk_indexes.remove(replica_node_id);
            }
            return;
        }

        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = false;
            state.next_retry_tick = tick;
            pending
                .repair_inflight_request_ids
                .insert(replica_node_id.clone(), request_id);
            pending
                .repair_next_chunk_indexes
                .insert(replica_node_id.clone(), outcome.next_chunk_index);
        }
    }

    fn record_repair_retry_scheduled(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            if !state.inflight {
                state.attempts = state.attempts.saturating_add(1);
            }
            state.inflight = false;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
            pending.repair_inflight_request_ids.remove(replica_node_id);
            pending.repair_next_chunk_indexes.remove(replica_node_id);
        }
    }

    fn clear_repair_retry(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.repair_retries.remove(replica_node_id);
            pending.repair_inflight_request_ids.remove(replica_node_id);
            pending.repair_next_chunk_indexes.remove(replica_node_id);
        }
    }

    fn request_repair_blob_from_alternate(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        let mut skipped_failed_source = false;
        for source_node_id in self.repair_blob_sources(job, pending)? {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            if pending.fetch_failed_from.contains(&source_node_id) {
                skipped_failed_source = true;
                continue;
            }
            let request_id = pending
                .fetch_inflight_request_ids
                .get(&source_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let start_chunk_index = pending
                .fetch_next_chunk_indexes
                .get(&source_node_id)
                .copied()
                .unwrap_or(0);
            let max_chunks = DFS_FETCH_CHUNK_WINDOW_SIZE;
            let sent = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id: request_id.clone(),
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    start_chunk_index,
                    max_chunks,
                }),
            )?;
            if sent {
                self.record_fetch_inflight(
                    &job.intent_id,
                    source_node_id,
                    request_id,
                    start_chunk_index,
                    max_chunks,
                );
                return Ok(true);
            }
        }
        if skipped_failed_source && let Some(pending) = self.pending_writes.get_mut(&job.intent_id)
        {
            pending.fetch_failed_from.clear();
        }
        Ok(false)
    }

    fn repair_blob_sources(
        &self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut candidates = BTreeSet::new();
        for receipt in self.coordinator.receipts_for_intent(&job.intent_id) {
            if receipt.replica_node_id != self.node_id
                && receipt.replica_node_id != job.missing_replica_node_id
            {
                candidates.insert(receipt.replica_node_id.clone());
            }
        }
        match self.current_read_object_metadata(&pending.object_id) {
            Ok(object) => {
                for node_id in &object.current_locations {
                    if node_id != &self.node_id && node_id != &job.missing_replica_node_id {
                        candidates.insert(node_id.clone());
                    }
                }
            }
            Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(candidates.into_iter().collect())
    }

    fn apply_committed_intent(&mut self, intent_id: &str) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Err(DfsError::Distributed {
                message: format!("unknown committed intent: {intent_id}"),
            });
        };
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            self.clear_active_replica_stage(&intent.object_id, &intent.target_version);
            return Ok(());
        }
        if !self.coordinator.is_commit_ready(intent_id)? {
            return Err(DfsError::Distributed {
                message: format!("intent {intent_id} is not ready to commit"),
            });
        }
        self.ensure_intent_epoch_current(&intent)?;
        let pending =
            self.pending_writes
                .get(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing pending write for committed intent: {intent_id}"),
                })?;
        if intent.object_id != pending.object_id || intent.target_version != pending.version {
            return Err(DfsError::Distributed {
                message: format!(
                    "committed intent {intent_id} state does not match pending write: intent {} {}, pending {} {}",
                    intent.object_id, intent.target_version, pending.object_id, pending.version
                ),
            });
        }
        let durable_receipts: Vec<DistributedReplicaReceipt> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .cloned()
            .collect();
        for receipt in durable_receipts {
            self.put_metadata_receipt(
                &intent.file_system_id,
                &receipt.object_id,
                &receipt.version,
                &receipt.replica_node_id,
                &pending.content_hash,
            )?;
        }
        let write_time = dfs_now_unix_nanos();
        self.commit_visible_version_in_metadata_backend(&intent, &pending, write_time)?;
        self.coordinator
            .commit_intent(intent_id)
            .map_err(DfsError::from)?;
        let committed_intent =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing committed intent after finalize: {intent_id}"),
                })?;
        self.put_metadata_intent(&committed_intent, Some(&pending.content_hash))?;
        self.commit_local_metadata(
            &pending.object_id,
            &pending.version,
            &pending.content_hash,
            write_time,
        )?;
        self.clear_active_replica_stage(&pending.object_id, &pending.version);

        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        if let Some(object) = self.metadata.objects.get_mut(&pending.object_id) {
            object.current_locations = durable_nodes.iter().cloned().collect();
        }

        let commit_targets: Vec<NodeId> = durable_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id)
            .cloned()
            .collect();
        let committed_metadata = Some(self.current_read_object_metadata(&pending.object_id)?);
        for replica_node_id in commit_targets {
            let request_id = self.next_request_id();
            self.send_best_effort_message_if_routable(
                &replica_node_id,
                DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: pending.object_id.clone(),
                    expected_base_version: intent.base_version.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                    object_metadata: committed_metadata.clone(),
                }),
            )?;
        }

        let repair_targets: Vec<NodeId> = self
            .coordinator
            .repair_jobs()
            .iter()
            .filter(|job| job.intent_id == intent_id)
            .map(|job| job.missing_replica_node_id.clone())
            .collect();
        for replica_node_id in repair_targets {
            if replica_node_id == self.node_id
                || !self.repair_retry_is_due(intent_id, &replica_node_id)
            {
                continue;
            }
            if self.open_pending_write_blob_stream(&pending).is_err() {
                self.record_repair_retry_scheduled(intent_id, &replica_node_id);
                continue;
            }
            let start_chunk_index = pending
                .repair_next_chunk_indexes
                .get(&replica_node_id)
                .copied()
                .unwrap_or(0);
            let request_id = pending
                .repair_inflight_request_ids
                .get(&replica_node_id)
                .cloned()
                .unwrap_or_else(|| self.next_request_id());
            let outcome = self.send_repair_replica_window_if_routable(
                &replica_node_id,
                request_id.clone(),
                intent_id,
                &intent.file_system_id,
                &pending.object_id,
                intent.base_version.clone(),
                &pending.version,
                &pending.content_hash,
                pending.size_bytes,
                start_chunk_index,
                DFS_STAGE_CHUNK_WINDOW_SIZE,
                committed_metadata.clone(),
            )?;
            if outcome.sent {
                self.record_repair_window_progress(
                    intent_id,
                    &replica_node_id,
                    request_id,
                    outcome,
                );
            } else {
                self.record_repair_retry_scheduled(intent_id, &replica_node_id);
            }
        }
        Ok(())
    }

    fn try_apply_committed_intent_if_ready(&mut self, intent_id: &str) -> Result<(), DfsError> {
        if self.coordinator.is_commit_ready(intent_id)? {
            self.apply_committed_intent(intent_id)?;
        }
        Ok(())
    }

    fn normalize_desired_replica_nodes(
        &self,
        object: &DfsObjectMetadata,
        desired_replica_nodes: Vec<NodeId>,
    ) -> Vec<NodeId> {
        let mut normalized = desired_replica_nodes;
        if matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) && !normalized.contains(&object.owner_node_id)
        {
            normalized.insert(0, object.owner_node_id.clone());
        }
        let mut deduped = Vec::new();
        for node_id in normalized {
            if !deduped.contains(&node_id) {
                deduped.push(node_id);
            }
        }
        deduped
    }

    fn ensure_no_backend_pending_write_intent(
        &self,
        object_id: &DfsObjectId,
        target_version: &DfsVersion,
        excluding_intent_id: Option<&str>,
    ) -> Result<(), DfsDistributedError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut intent_cursor = None;
        loop {
            let page = backend
                .intents_for_object_page(
                    object_id,
                    intent_cursor.as_ref(),
                    DFS_METADATA_OBJECT_INTENT_PAGE_SIZE,
                )
                .map_err(|error| DfsDistributedError::InvalidPolicy {
                    reason: error.to_string(),
                })?;
            for intent in page.intents {
                if excluding_intent_id == Some(intent.intent_id.as_str()) {
                    continue;
                }
                if matches!(
                    intent.status,
                    DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending
                ) {
                    return Err(DfsDistributedError::PendingIntentConflict {
                        object_id: object_id.clone(),
                        pending_intent_id: intent.intent_id.clone(),
                        pending_target_version: intent.target_version.clone(),
                        requested_target_version: target_version.clone(),
                    });
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            intent_cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn record_active_replica_stage(
        &mut self,
        object_id: &DfsObjectId,
        intent_id: &str,
        version: &DfsVersion,
        content_hash: &str,
        requester_node_id: &NodeId,
    ) {
        self.active_replica_stages.insert(
            object_id.clone(),
            ActiveReplicaStage {
                intent_id: intent_id.to_string(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                requester_node_id: requester_node_id.clone(),
            },
        );
    }

    fn clear_active_replica_stage(&mut self, object_id: &DfsObjectId, version: &DfsVersion) {
        if self
            .active_replica_stages
            .get(object_id)
            .is_some_and(|stage| &stage.version == version)
        {
            self.active_replica_stages.remove(object_id);
        }
    }

    fn restore_pending_write_for_intent_if_missing(
        &mut self,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        if self.pending_writes.contains_key(intent_id) {
            return Ok(());
        }
        let Some(record) = (match self.metadata_backend.as_ref() {
            Some(backend) => backend.get_intent(object_id, intent_id)?,
            None => None,
        }) else {
            let _ = self
                .coordinator
                .intent(intent_id)
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("unknown pending write intent: {intent_id}"),
                })?;
            return Ok(());
        };
        if self.coordinator.intent(intent_id).is_none() {
            self.coordinator
                .restore_intent(distributed_intent_from_record(&record))?;
            if let Some(backend) = self.metadata_backend.as_ref() {
                let mut receipt_cursor = None;
                loop {
                    let page = backend.receipts_for_version_page(
                        &record.object_id,
                        &record.target_version,
                        receipt_cursor.as_ref(),
                        DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                    )?;
                    for receipt in page.receipts {
                        if let Some(expected_hash) = record.content_hash.as_deref()
                            && receipt.content_hash.as_deref() != Some(expected_hash)
                        {
                            continue;
                        }
                        match self
                            .coordinator
                            .restore_durable_receipt(intent_id, receipt.replica_node_id)
                        {
                            Ok(()) => {}
                            Err(
                                DfsDistributedError::UnexpectedReplica { .. }
                                | DfsDistributedError::IntentClosed { .. },
                            ) => continue,
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let Some(next_cursor) = page.next_cursor else {
                        break;
                    };
                    receipt_cursor = Some(next_cursor);
                }
            }
        }
        let intent =
            self.coordinator
                .intent(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("unknown pending write intent: {intent_id}"),
                })?;
        if !distributed_intent_matches_record(&intent, &record) {
            return Err(DfsError::Distributed {
                message: format!(
                    "stored intent record does not match coordinator state for {intent_id}"
                ),
            });
        }
        if self
            .current_read_object_metadata(&record.object_id)
            .is_err()
            && let Some(object) = record.object_metadata.clone()
        {
            self.add_transient_object_metadata(object)?;
        }
        let Some(content_hash) = record.content_hash.clone() else {
            return Ok(());
        };
        let size_bytes = self
            .pending_write_size_hint(&record.object_id, &record.target_version, &content_hash)?
            .unwrap_or(0);
        self.pending_writes.insert(
            intent_id.to_string(),
            pending_networked_write(
                record.object_id,
                record.target_version,
                content_hash,
                size_bytes,
            ),
        );
        Ok(())
    }

    fn commit_local_metadata(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        write_time_unix_nanos: u64,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let content_size_bytes = self
            .content
            .read_manifest(object_id.as_str(), version.as_str())
            .ok()
            .filter(|manifest| manifest.content_hash == content_hash)
            .map(|manifest| manifest.size_bytes);
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(backend_object) = backend.get_object(object_id)?
        {
            self.metadata.add_object(backend_object)?;
        }
        let object =
            self.metadata
                .objects
                .get_mut(object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
        let content_unchanged = object
            .content_hash
            .as_ref()
            .is_some_and(|existing| existing == content_hash);
        let already_current = object.version == *version && content_unchanged;
        object.version = version.clone();
        object.content_hash = Some(content_hash.to_string());
        if content_size_bytes.is_some() {
            object.content_size_bytes = content_size_bytes;
        }
        if !content_unchanged {
            object.modified_at_unix_nanos = write_time_unix_nanos;
            object.changed_at_unix_nanos = write_time_unix_nanos;
        }
        if !already_current {
            object.last_modified_by_event_id = Some(EventId::generate());
        }
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
        let object = object.clone();
        self.track_metadata_tombstone(&object);
        Ok(object.clone())
    }

    fn store_conflicting_write_content_hash(
        &self,
        request: &DfsWriteRequest,
        bytes: &[u8],
    ) -> Result<Option<String>, DfsError> {
        match self.content.put_object_version(
            request.object_id.as_str(),
            request.target_version.as_str(),
            bytes,
        ) {
            Ok(manifest) => Ok(Some(manifest.content_hash)),
            Err(ContentStoreError::VersionHashConflict { new_hash, .. }) => Ok(Some(new_hash)),
            Err(error) => Err(error.into()),
        }
    }

    fn store_conflicting_write_reader_content_hash<R: Read>(
        &self,
        request: &DfsWriteRequest,
        reader: R,
    ) -> Result<Option<String>, DfsError> {
        match self.content.put_object_version_from_reader(
            request.object_id.as_str(),
            request.target_version.as_str(),
            reader,
        ) {
            Ok(manifest) => Ok(Some(manifest.content_hash)),
            Err(ContentStoreError::VersionHashConflict { new_hash, .. }) => Ok(Some(new_hash)),
            Err(error) => Err(error.into()),
        }
    }

    fn ensure_replicated_confirmed_object(&self, object_id: &DfsObjectId) -> Result<(), DfsError> {
        let object = self.current_read_object_metadata(object_id)?;
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }
        Ok(())
    }

    fn confirmed_locations_for_object(
        &self,
        object: &DfsObjectMetadata,
        required_replica_node_ids: &[NodeId],
    ) -> Result<Vec<NodeId>, DfsError> {
        let mut confirmed_locations: BTreeSet<NodeId> = object
            .current_locations
            .iter()
            .filter(|node_id| required_replica_node_ids.contains(node_id))
            .cloned()
            .collect();

        if let Some(backend) = self.metadata_backend.as_ref() {
            confirmed_locations.clear();
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    &object.object_id,
                    &object.version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                for receipt in page.receipts {
                    if !required_replica_node_ids.contains(&receipt.replica_node_id) {
                        continue;
                    }
                    if let Some(expected_hash) = object.content_hash.as_deref() {
                        if receipt.content_hash.as_deref() != Some(expected_hash) {
                            continue;
                        }
                    }
                    confirmed_locations.insert(receipt.replica_node_id);
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
        }

        Ok(confirmed_locations.into_iter().collect())
    }

    fn mark_repaired_location(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let mut object = self.current_read_object_metadata(object_id)?;
        if &object.version != version {
            return Ok(());
        }
        if let Some(expected_hash) = object.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        if object.current_locations.contains(replica_node_id) {
            return Ok(());
        }
        object.current_locations.push(replica_node_id.clone());
        self.metadata.add_object(object.clone())?;
        self.put_visible_object_in_metadata_backend(&object)?;
        Ok(())
    }

    fn put_metadata_receipt(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<bool, DfsError> {
        self.ensure_metadata_receipt_accepts(object_id, version, replica_node_id, content_hash)?;
        let object = self.current_read_object_metadata(object_id).ok();
        let inserted = {
            let Some(backend) = self.metadata_backend.as_mut() else {
                return Ok(false);
            };
            let mut existing_matching_receipt = false;
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    object_id,
                    version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                for receipt in page.receipts {
                    if receipt.replica_node_id != *replica_node_id {
                        continue;
                    }
                    if receipt.content_hash.as_deref() == Some(content_hash) {
                        existing_matching_receipt = true;
                    }
                    break;
                }
                if existing_matching_receipt {
                    break;
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
            if existing_matching_receipt {
                false
            } else {
                let receipt = DfsReplicaReceiptRecord {
                    receipt: DfsWriteReceipt {
                        file_system_id: file_system_id.clone(),
                        object_id: object_id.clone(),
                        committed_version: version.clone(),
                        owner_node_id: self.node_id.clone(),
                        event_id: EventId::generate(),
                    },
                    replica_node_id: replica_node_id.clone(),
                    content_hash: Some(content_hash.to_string()),
                    received_at: OffsetDateTime::now_utc(),
                };
                backend.put_receipt_for_object(object.as_ref(), &receipt)?;
                true
            }
        };
        match self.mark_repaired_location(object_id, version, replica_node_id, content_hash) {
            Ok(()) | Err(DfsError::UnknownObject { .. }) => {}
            Err(error) => return Err(error),
        }
        Ok(inserted)
    }

    fn ensure_metadata_receipt_accepts(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let mut receipt_cursor = None;
        loop {
            let page = backend.receipts_for_version_page(
                object_id,
                version,
                receipt_cursor.as_ref(),
                DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
            )?;
            for receipt in page.receipts {
                if receipt.replica_node_id != *replica_node_id {
                    continue;
                }
                if receipt.content_hash.as_deref() == Some(content_hash) {
                    return Ok(());
                }
                return Err(DfsError::Distributed {
                    message: format!(
                        "conflicting DFS replica receipt for {object_id} {version} from {replica_node_id}: existing {:?}, new {content_hash}",
                        receipt.content_hash
                    ),
                });
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            receipt_cursor = Some(next_cursor);
        }
        Ok(())
    }

    fn put_metadata_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: Option<&str>,
    ) -> Result<(), DfsError> {
        let object = self.current_object_for_write(&intent.object_id).ok();
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let mut record = write_intent_record(intent, content_hash, object.clone());
        if let Some(existing) = backend.get_intent(&intent.object_id, &intent.intent_id)? {
            record.created_at = existing.created_at;
            let mut comparable_record = record.clone();
            comparable_record.updated_at = existing.updated_at;
            if comparable_record == existing {
                return Ok(());
            }
        }
        backend.put_intent_for_object(object.as_ref(), &record)?;
        Ok(())
    }

    fn put_visible_object_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        let mut object = object.clone();
        normalize_current_locations(&mut object.current_locations);
        let size_bytes = self.object_content_size_hint(&object)?;
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        if backend.get_file_system(&object.file_system_id)?.is_none() {
            backend.put_file_system(&DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            })?;
        }
        if backend.get_object(&object.object_id)?.as_ref() != Some(&object) {
            backend.put_object(&object)?;
        }
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(&object, size_bytes),
        )?;
        Ok(())
    }

    fn put_object_version_size_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
        size_bytes: Option<u64>,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        put_visible_version_record_if_changed(
            backend.as_mut(),
            object_version_record(object, size_bytes),
        )?;
        Ok(())
    }

    pub(crate) fn object_content_size_hint(
        &self,
        object: &DfsObjectMetadata,
    ) -> Result<Option<u64>, DfsError> {
        if !matches!(object.kind, DfsObjectKind::File | DfsObjectKind::Symlink) {
            return Ok(None);
        }
        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            return Ok(Some(manifest.size_bytes));
        }
        if object.content_size_bytes.is_some() {
            return Ok(object.content_size_bytes);
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(version) = backend.get_version(&object.object_id, &object.version)?
            && version.size_bytes.is_some()
        {
            return Ok(version.size_bytes);
        }
        if let Some(expected_hash) = object.content_hash.as_ref()
            && let Some(manifest) = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .find(|manifest| &manifest.content_hash == expected_hash)
        {
            return Ok(Some(manifest.size_bytes));
        }
        if let Some(backend) = self.metadata_backend.as_ref() {
            if let Some(expected_hash) = object.content_hash.as_ref() {
                let mut version_cursor = None;
                let mut matching_size_bytes = None;
                loop {
                    let page = backend.versions_for_object_page(
                        &object.object_id,
                        version_cursor.as_ref(),
                        DFS_METADATA_OBJECT_VERSION_PAGE_SIZE,
                    )?;
                    for version in page.versions {
                        if version.content_hash.as_ref() == Some(expected_hash) {
                            matching_size_bytes = Some(version.size_bytes);
                        }
                    }
                    let Some(next_cursor) = page.next_cursor else {
                        break;
                    };
                    version_cursor = Some(next_cursor);
                }
                if let Some(size_bytes) = matching_size_bytes {
                    return Ok(size_bytes);
                }
            }
        }
        Ok(None)
    }

    fn commit_visible_version_in_metadata_backend(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
        write_time_unix_nanos: u64,
    ) -> Result<(), DfsError> {
        let expected_object = self.current_object_for_write(&pending.object_id)?;
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let durable_locations: Vec<NodeId> = durable_nodes.iter().cloned().collect();
        if expected_object.version == pending.version
            && expected_object.content_hash.as_deref() == Some(pending.content_hash.as_str())
        {
            let mut current_locations = expected_object.current_locations.clone();
            normalize_current_locations(&mut current_locations);
            let size_is_current = pending.size_bytes == 0
                || expected_object.content_size_bytes == Some(pending.size_bytes);
            if current_locations == durable_locations && size_is_current {
                return Ok(());
            }
            let mut repaired_object = expected_object.clone();
            if pending.size_bytes != 0 {
                repaired_object.content_size_bytes = Some(pending.size_bytes);
            }
            repaired_object.current_locations = durable_locations;
            self.put_visible_object_in_metadata_backend(&repaired_object)?;
            self.metadata.add_object(repaired_object)?;
            return Ok(());
        }
        let mut committed_object = expected_object.clone();
        committed_object.version = pending.version.clone();
        committed_object.content_hash = Some(pending.content_hash.clone());
        committed_object.content_size_bytes = Some(pending.size_bytes);
        committed_object.modified_at_unix_nanos = write_time_unix_nanos;
        committed_object.changed_at_unix_nanos = write_time_unix_nanos;
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        committed_object.current_locations = durable_locations;
        let visible_version = object_version_record(&committed_object, Some(pending.size_bytes));
        let required_replica_node_ids: Vec<NodeId> = durable_nodes.into_iter().collect();
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        if backend.get_object(&pending.object_id)?.is_none() {
            backend.apply_namespace_transaction(&DfsNamespaceTransaction {
                transaction_id: namespace_transaction_id_for_object(
                    "create-visible",
                    &pending.object_id,
                    &pending.version,
                ),
                file_system_id: committed_object.file_system_id.clone(),
                objects: vec![committed_object.clone()],
                versions: vec![visible_version],
                created_at: OffsetDateTime::now_utc(),
            })?;
            return Ok(());
        }
        backend.commit_visible_version(
            &expected_object.version,
            &committed_object,
            &visible_version,
            &required_replica_node_ids,
        )?;
        Ok(())
    }

    fn store_command_bytes(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let actual_hash = content_hash(bytes);
        if actual_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: actual_hash,
            });
        }
        let manifest =
            self.content
                .put_object_version(object_id.as_str(), version.as_str(), bytes)?;
        if manifest.content_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        Ok(())
    }

    fn store_command_patch(
        &self,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
        target_version: &DfsVersion,
        expected_hash: &str,
        offset: u64,
        expected_patch_hash: &str,
        bytes: &[u8],
    ) -> Result<(), DfsError> {
        let actual_patch_hash = content_hash(bytes);
        if actual_patch_hash != expected_patch_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_patch_hash.to_string(),
                actual: actual_patch_hash,
            });
        }
        let manifest = self.content.put_object_version_patch(
            object_id.as_str(),
            base_version.as_str(),
            target_version.as_str(),
            offset,
            bytes,
        )?;
        if manifest.content_hash != expected_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: expected_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        Ok(())
    }

    fn local_object_version_manifest_matches(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
        expected_size_bytes: u64,
    ) -> bool {
        self.content
            .read_manifest(object_id.as_str(), version.as_str())
            .is_ok_and(|manifest| {
                manifest.content_hash == expected_hash
                    && manifest.size_bytes == expected_size_bytes
                    && self
                        .content
                        .manifest_content_available(&manifest)
                        .unwrap_or(false)
            })
    }

    fn local_object_version_manifest_size_if_matches(
        &self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        expected_hash: &str,
    ) -> Option<u64> {
        self.content
            .read_manifest(object_id.as_str(), version.as_str())
            .ok()
            .filter(|manifest| {
                manifest.content_hash == expected_hash
                    && self
                        .content
                        .manifest_content_available(manifest)
                        .unwrap_or(false)
            })
            .map(|manifest| manifest.size_bytes)
    }

    fn ensure_replica_object_metadata(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let Some(object) = object else {
            return Ok(());
        };
        let persist_authoritative_metadata =
            self.local_metadata_backend_is_authoritative_for_object(object)?;
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            let file_system = DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            };
            if persist_authoritative_metadata {
                self.add_file_system(file_system)?;
            } else {
                self.metadata.add_file_system(file_system);
            }
        }
        if let Some(backend) = self.metadata_backend.as_ref()
            && let Some(existing) = backend.get_object(&object.object_id)?
        {
            self.metadata.add_object(existing)?;
        } else if self.metadata.object(&object.object_id).is_none() {
            if persist_authoritative_metadata {
                self.add_object_metadata(object.clone())?;
            } else {
                self.add_transient_object_metadata(object.clone())?;
            }
        }
        Ok(())
    }

    fn local_metadata_backend_is_authoritative_for_object(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<bool, DfsError> {
        Ok(self
            .select_metadata_shard_for_object(object)?
            .is_none_or(|shard| shard.primary_node_id == self.node_id))
    }

    fn replica_command_metadata_is_authoritative(
        &mut self,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        let object = match self.current_read_object_metadata(object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?
            }
            Err(error) => return Err(error),
        };
        self.local_metadata_backend_is_authoritative_for_object(&object)
    }

    fn put_replica_receipt_if_authoritative(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        if self.replica_command_metadata_is_authoritative(object_id, object_metadata)? {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        Ok(())
    }

    fn commit_replica_metadata_cache(
        &mut self,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        replica_node_id: &NodeId,
        content_hash: &str,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let persist_authoritative_metadata =
            self.replica_command_metadata_is_authoritative(object_id, object_metadata)?;
        if persist_authoritative_metadata {
            self.put_metadata_receipt(
                file_system_id,
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        } else {
            self.ensure_metadata_receipt_accepts(
                object_id,
                version,
                replica_node_id,
                content_hash,
            )?;
        }
        let object =
            self.commit_local_metadata(object_id, version, content_hash, dfs_now_unix_nanos())?;
        if persist_authoritative_metadata {
            self.put_visible_object_in_metadata_backend(&object)?;
        }
        Ok(())
    }

    fn ensure_replica_command_authorized_with_metadata(
        &mut self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
        object_metadata: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let object = match self.current_read_object_metadata(object_id) {
            Ok(object) => object,
            Err(DfsError::UnknownObject { .. }) => {
                object_metadata
                    .cloned()
                    .ok_or_else(|| DfsError::UnknownObject {
                        object_id: object_id.clone(),
                    })?
            }
            Err(error) => return Err(error),
        };
        let requester_is_metadata_primary = match self.select_metadata_shard_for_object(&object)? {
            Some(shard) if &shard.primary_node_id != requester_node_id => {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!(
                        "source is not current metadata shard primary for {}; primary is {} at epoch {}",
                        object.file_system_id, shard.primary_node_id, shard.epoch
                    ),
                });
            }
            Some(_) => true,
            None => false,
        };
        if let Some(backend) = self.metadata_backend.as_ref() {
            if let Some(intent) = backend.get_intent(object_id, intent_id)? {
                if &intent.coordinator_node_id != requester_node_id {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "source is not coordinator for DFS write intent {intent_id}"
                        ),
                    });
                }
                if !intent.desired_replica_node_ids.contains(&self.node_id) {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "local node is not a desired replica for DFS write intent {intent_id}"
                        ),
                    });
                }
                return Ok(());
            }
        }
        if requester_is_metadata_primary && object_metadata.is_some() {
            return Ok(());
        }

        if &object.owner_node_id == requester_node_id {
            return Ok(());
        }
        if matches!(
            object.policy,
            DfsObjectPolicy::ReplicatedEventual | DfsObjectPolicy::ReplicatedConfirmed
        ) && object.current_locations.contains(requester_node_id)
        {
            return Ok(());
        }

        Err(DfsError::UnauthorizedReplicaCommand {
            source_node_id: requester_node_id.clone(),
            reason: format!(
                "source is neither object owner nor current replica for {object_id}; owner is {}",
                object.owner_node_id
            ),
        })
    }

    fn ensure_replica_command_hash_matches_intent(
        &self,
        intent_id: &str,
        object_id: &DfsObjectId,
        content_hash: &str,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return Ok(());
        };
        if let Some(expected_hash) = intent.content_hash.as_deref() {
            if expected_hash != content_hash {
                return Err(DfsError::ContentHashMismatch {
                    expected: expected_hash.to_string(),
                    actual: content_hash.to_string(),
                });
            }
        }
        Ok(())
    }

    fn ensure_blob_fetch_authorized(
        &self,
        requester_node_id: &NodeId,
        intent_id: &str,
        object_id: &DfsObjectId,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };
        let Some(intent) = backend.get_intent(object_id, intent_id)? else {
            return Ok(());
        };
        if &intent.coordinator_node_id != requester_node_id
            && !intent.desired_replica_node_ids.contains(requester_node_id)
        {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not authorized to fetch DFS blob for intent {intent_id}"
                ),
            });
        }
        if !intent.desired_replica_node_ids.contains(&self.node_id) {
            let mut has_receipt = false;
            let mut receipt_cursor = None;
            loop {
                let page = backend.receipts_for_version_page(
                    object_id,
                    &intent.target_version,
                    receipt_cursor.as_ref(),
                    DFS_METADATA_OBJECT_RECEIPT_PAGE_SIZE,
                )?;
                if page
                    .receipts
                    .into_iter()
                    .any(|receipt| receipt.replica_node_id == self.node_id)
                {
                    has_receipt = true;
                    break;
                }
                let Some(next_cursor) = page.next_cursor else {
                    break;
                };
                receipt_cursor = Some(next_cursor);
            }
            if !has_receipt {
                return Err(DfsError::UnauthorizedReplicaCommand {
                    source_node_id: requester_node_id.clone(),
                    reason: format!(
                        "local node is not an expected or durable replica for DFS intent {intent_id}"
                    ),
                });
            }
        }
        Ok(())
    }

    fn write_outcome(&self, intent_id: &str) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        if let Some(pending) = self.pending_routed_writes.get(intent_id) {
            return match &pending.result {
                Some(Ok(outcome)) => Ok(outcome.clone()),
                Some(Err(error)) => Err(error.clone()),
                None => Ok(NetworkedDfsWriteOutcome::Pending {
                    intent_id: pending
                        .remote_intent_id
                        .clone()
                        .unwrap_or_else(|| intent_id.to_string()),
                    durable_receipts: pending.durable_receipts,
                    required_receipts: pending.required_receipts,
                }),
            };
        }
        let intent = self
            .coordinator
            .intent(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("unknown intent: {intent_id}"),
            })?;
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            let pending = self.pending_writes.get(intent_id);
            let content_hash = pending
                .map(|pending| pending.content_hash.clone())
                .or_else(|| {
                    self.metadata_backend
                        .as_ref()
                        .and_then(|backend| {
                            backend
                                .get_intent(&intent.object_id, intent_id)
                                .ok()
                                .flatten()
                        })
                        .and_then(|intent_record| intent_record.content_hash)
                })
                .or_else(|| {
                    self.current_read_object_metadata(&intent.object_id)
                        .ok()
                        .filter(|object| object.version == intent.target_version)
                        .and_then(|object| object.content_hash)
                })
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing content hash for committed intent: {intent_id}"),
                })?;
            Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: intent_id.to_string(),
                version: pending
                    .map(|pending| pending.version.clone())
                    .unwrap_or_else(|| intent.target_version.clone()),
                content_hash,
                repair_jobs: self
                    .coordinator
                    .repair_jobs()
                    .iter()
                    .filter(|job| job.intent_id == intent_id)
                    .cloned()
                    .collect(),
            })
        } else {
            let _pending =
                self.pending_writes
                    .get(intent_id)
                    .ok_or_else(|| DfsError::Distributed {
                        message: format!("missing pending write: {intent_id}"),
                    })?;
            Ok(NetworkedDfsWriteOutcome::Pending {
                intent_id: intent_id.to_string(),
                durable_receipts: self.coordinator.durable_receipt_count(intent_id),
                required_receipts: intent.policy.min_commit_replicas,
            })
        }
    }

    fn send_dfs_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        let protocol_message =
            ProtocolMessage::encode(DFS_PROTOCOL, &message).map_err(DfsError::from_network)?;
        let frame = NetworkFrame {
            frame_id: NetworkFrameId(self.next_frame_id),
            source_node_id: self.node_id.clone(),
            target_node_id: target_node_id.clone(),
            message: MeshMessage {
                message_id: MeshMessageId(self.next_message_id),
                origin_node_id: self.node_id.clone(),
                target_node_id: target_node_id.clone(),
                body: MeshMessageBody::Protocol(protocol_message),
            },
        };
        self.next_frame_id += 1;
        self.next_message_id += 1;
        match self.transport.send(target_node_id, frame) {
            Ok(()) => Ok(true),
            Err(NetworkError::UnknownNode { .. }) => Ok(false),
            Err(error) => Err(DfsError::from_network(error)),
        }
    }

    fn send_stage_blob_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        if bytes.len() <= DFS_BLOB_INLINE_LIMIT_BYTES {
            return self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: bytes.to_vec(),
                    object_metadata,
                }),
            );
        }

        let total_chunks = blob_chunk_count(bytes)?;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                    object_metadata: object_metadata.clone(),
                }),
            )?;
            if !sent {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn send_stage_patch_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        base_version: &DfsVersion,
        version: &DfsVersion,
        content_hash: &str,
        offset: u64,
        patch_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        if bytes.len() > DFS_BLOB_INLINE_LIMIT_BYTES {
            return Ok(false);
        }
        self.send_best_effort_message_if_routable(
            target_node_id,
            DfsNetworkMessage::StagePatch(DfsStagePatchCommand {
                request_id,
                intent_id: intent_id.to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                base_version: base_version.clone(),
                version: version.clone(),
                content_hash: content_hash.to_string(),
                offset,
                patch_hash: patch_hash.to_string(),
                bytes: bytes.to_vec(),
                object_metadata,
            }),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn send_stage_blob_window_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let (_, mut reader) = self.content.open_object_version_range_stream(
                object_id.as_str(),
                version.as_str(),
                0,
                usize::try_from(total_size_bytes).unwrap_or(usize::MAX),
            )?;
            let bytes = read_small_blob_from_reader(content_hash, total_size_bytes, &mut reader)?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes,
                    object_metadata,
                }),
            )?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if start_chunk_index >= total_chunks {
            return Ok(StageBlobSendOutcome {
                sent: true,
                complete: true,
                next_chunk_index: total_chunks,
            });
        }
        let max_chunks = max_chunks.max(1);
        let end_chunk_index = start_chunk_index
            .saturating_add(max_chunks)
            .min(total_chunks);
        let start_offset = u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
        let end_offset = if end_chunk_index >= total_chunks {
            total_size_bytes
        } else {
            u64::from(end_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64
        };
        let range_len = usize::try_from(end_offset.saturating_sub(start_offset)).map_err(|_| {
            DfsError::Distributed {
                message: format!(
                    "DFS stage chunk window is too large: {start_offset}..{end_offset}"
                ),
            }
        })?;
        let (manifest, mut reader) = self.content.open_object_version_range_stream(
            object_id.as_str(),
            version.as_str(),
            start_offset,
            range_len,
        )?;
        if manifest.content_hash != content_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: content_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {content_hash} stage chunk {chunk_index}: {source}"
                    ),
                })?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&buffer[..chunk_len])),
                    bytes: buffer[..chunk_len].to_vec(),
                    object_metadata: object_metadata.clone(),
                }),
            )?;
            if !sent {
                return Ok(StageBlobSendOutcome {
                    sent: false,
                    complete: false,
                    next_chunk_index: chunk_index,
                });
            }
        }
        Ok(StageBlobSendOutcome {
            sent: true,
            complete: end_chunk_index >= total_chunks,
            next_chunk_index: end_chunk_index,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn send_repair_replica_window_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        total_size_bytes: u64,
        start_chunk_index: u32,
        max_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<StageBlobSendOutcome, DfsError> {
        if total_size_bytes <= DFS_BLOB_INLINE_LIMIT_BYTES as u64 {
            let (_, mut reader) = self.content.open_object_version_range_stream(
                object_id.as_str(),
                version.as_str(),
                0,
                usize::try_from(total_size_bytes).unwrap_or(usize::MAX),
            )?;
            let bytes = read_small_blob_from_reader(content_hash, total_size_bytes, &mut reader)?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes,
                    object_metadata,
                }),
            )?;
            return Ok(StageBlobSendOutcome {
                sent,
                complete: sent,
                next_chunk_index: 0,
            });
        }

        let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
        if start_chunk_index >= total_chunks {
            return Ok(StageBlobSendOutcome {
                sent: true,
                complete: true,
                next_chunk_index: total_chunks,
            });
        }
        let max_chunks = max_chunks.max(1);
        let end_chunk_index = start_chunk_index
            .saturating_add(max_chunks)
            .min(total_chunks);
        let start_offset = u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
        let end_offset = if end_chunk_index >= total_chunks {
            total_size_bytes
        } else {
            u64::from(end_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64
        };
        let range_len = usize::try_from(end_offset.saturating_sub(start_offset)).map_err(|_| {
            DfsError::Distributed {
                message: format!(
                    "DFS repair chunk window is too large: {start_offset}..{end_offset}"
                ),
            }
        })?;
        let (manifest, mut reader) = self.content.open_object_version_range_stream(
            object_id.as_str(),
            version.as_str(),
            start_offset,
            range_len,
        )?;
        if manifest.content_hash != content_hash {
            return Err(DfsError::ContentHashMismatch {
                expected: content_hash.to_string(),
                actual: manifest.content_hash,
            });
        }
        let mut buffer = vec![0_u8; DFS_BLOB_CHUNK_BYTES];
        for chunk_index in start_chunk_index..end_chunk_index {
            let chunk_len = blob_chunk_len(total_size_bytes, chunk_index)?;
            reader
                .read_exact(&mut buffer[..chunk_len])
                .map_err(|source| DfsError::ContentStore {
                    message: format!(
                        "failed to read blob {content_hash} repair chunk {chunk_index}: {source}"
                    ),
                })?;
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes,
                    chunk_index,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(&buffer[..chunk_len])),
                    bytes: buffer[..chunk_len].to_vec(),
                    object_metadata: object_metadata.clone(),
                }),
            )?;
            if !sent {
                return Ok(StageBlobSendOutcome {
                    sent: false,
                    complete: false,
                    next_chunk_index: chunk_index,
                });
            }
        }
        Ok(StageBlobSendOutcome {
            sent: true,
            complete: end_chunk_index >= total_chunks,
            next_chunk_index: end_chunk_index,
        })
    }

    fn send_best_effort_message_if_routable(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<bool, DfsError> {
        match self.send_dfs_message_if_routable(target_node_id, message) {
            Ok(sent) => Ok(sent),
            Err(DfsError::Network { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    fn send_repair_replica_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        expected_base_version: Option<DfsVersion>,
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        if bytes.len() <= DFS_BLOB_INLINE_LIMIT_BYTES {
            return self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version,
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: bytes.to_vec(),
                    object_metadata,
                }),
            );
        }

        let total_chunks = blob_chunk_count(bytes)?;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_best_effort_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    expected_base_version: expected_base_version.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    chunk_hash: Some(chunk_hash(chunk)),
                    bytes: chunk.to_vec(),
                    object_metadata: object_metadata.clone(),
                }),
            )?;
            if !sent {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_net_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}

fn blob_chunk_len(total_size_bytes: u64, chunk_index: u32) -> Result<usize, DfsError> {
    let chunk_offset = u64::from(chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
    let chunk_len = total_size_bytes
        .saturating_sub(chunk_offset)
        .min(DFS_BLOB_CHUNK_BYTES as u64);
    usize::try_from(chunk_len).map_err(|_| DfsError::Distributed {
        message: format!("DFS blob chunk is too large to fit in memory: {chunk_len} bytes"),
    })
}

fn fetch_response_window_end(start_chunk_index: u32, total_chunks: u32, max_chunks: u32) -> u32 {
    if max_chunks == 0 {
        return total_chunks;
    }
    start_chunk_index
        .saturating_add(max_chunks)
        .min(total_chunks)
}

fn read_response_window_range_len(
    start_chunk_index: u32,
    total_size_bytes: u64,
    max_chunks: u32,
) -> Result<usize, DfsError> {
    let total_chunks = blob_chunk_count_for_size(total_size_bytes)?;
    if start_chunk_index >= total_chunks {
        return Err(DfsError::Distributed {
            message: format!(
                "DFS read chunk window starts outside object: start={}, total_chunks={total_chunks}",
                start_chunk_index
            ),
        });
    }
    let end_chunk_index = fetch_response_window_end(start_chunk_index, total_chunks, max_chunks);
    let start_offset = u64::from(start_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64;
    let end_offset = if end_chunk_index >= total_chunks {
        total_size_bytes
    } else {
        u64::from(end_chunk_index) * DFS_BLOB_CHUNK_BYTES as u64
    };
    usize::try_from(end_offset.saturating_sub(start_offset)).map_err(|_| DfsError::Distributed {
        message: format!("DFS read chunk window is too large: {start_offset}..{end_offset}"),
    })
}

fn record_anti_entropy_issue(
    report: &mut DfsAntiEntropyReport,
    max_issues: usize,
    issue: DfsAntiEntropyIssue,
) {
    report.issue_count = report.issue_count.saturating_add(1);
    if report.issues.len() < max_issues {
        report.issues.push(issue);
    } else {
        report.max_issues_reached = true;
    }
}

fn read_small_blob_from_reader(
    content_hash: &str,
    total_size_bytes: u64,
    reader: &mut ObjectVersionReader,
) -> Result<Vec<u8>, DfsError> {
    let size = usize::try_from(total_size_bytes).map_err(|_| DfsError::Distributed {
        message: format!("DFS blob is too large to inline: {total_size_bytes} bytes"),
    })?;
    let mut bytes = vec![0_u8; size];
    reader
        .read_exact(&mut bytes)
        .map_err(|source| DfsError::ContentStore {
            message: format!("failed to read inline blob {content_hash}: {source}"),
        })?;
    Ok(bytes)
}

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn metadata_journal_batch_file_system_id(
    batch: &DfsMetadataJournalBatch,
) -> Option<DfsFileSystemId> {
    batch
        .entries
        .iter()
        .find_map(|entry| match &entry.mutation {
            DfsMetadataMutation::PutFileSystem { file_system_id, .. }
            | DfsMetadataMutation::PutObject { file_system_id, .. }
            | DfsMetadataMutation::PutVersion { file_system_id, .. }
            | DfsMetadataMutation::PutReceipt { file_system_id, .. }
            | DfsMetadataMutation::PutIntent { file_system_id, .. }
            | DfsMetadataMutation::CommitVisibleVersion { file_system_id, .. }
            | DfsMetadataMutation::PutConflict { file_system_id, .. }
            | DfsMetadataMutation::ApplyNamespaceTransaction { file_system_id, .. } => {
                Some(file_system_id.clone())
            }
        })
}

fn read_conflicts_reason(conflicts: Vec<DfsConflictReadView>) -> String {
    conflicts
        .into_iter()
        .map(|conflict| format!("{}: {}", conflict.conflict_id, conflict.reason))
        .collect::<Vec<_>>()
        .join("; ")
}

fn dfs_now_unix_nanos() -> u64 {
    OffsetDateTime::now_utc()
        .unix_timestamp_nanos()
        .try_into()
        .unwrap_or(1_000_000_000)
}

fn object_version_record(
    object: &DfsObjectMetadata,
    size_bytes: Option<u64>,
) -> DfsObjectVersionRecord {
    let mut locations = object.current_locations.clone();
    normalize_current_locations(&mut locations);
    DfsObjectVersionRecord {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        size_bytes: size_bytes.or(object.content_size_bytes),
        locations,
        created_by_event_id: object.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::now_utc(),
        visible: true,
    }
}

fn put_visible_version_record_if_changed(
    backend: &mut dyn DfsMetadataBackend,
    mut version_record: DfsObjectVersionRecord,
) -> Result<(), DfsError> {
    if let Some(existing) =
        backend.get_version(&version_record.object_id, &version_record.version)?
    {
        version_record.created_at = existing.created_at;
        if existing == version_record {
            return Ok(());
        }
    }
    backend.put_version(&version_record)?;
    Ok(())
}

fn normalize_current_locations(locations: &mut Vec<NodeId>) {
    let mut seen = BTreeSet::new();
    locations.retain(|node_id| seen.insert(node_id.clone()));
}

/// Builds the `(file_system_id, normalized path)` key used to index an object by
/// path, or `None` when the object's path cannot be normalized (such objects are
/// not reachable by path lookup, matching the previous scan's behavior).
fn path_index_key(
    file_system_id: &DfsFileSystemId,
    path: &str,
) -> Option<(DfsFileSystemId, String)> {
    posix::normalized_posix_path_key(path).map(|normalized| (file_system_id.clone(), normalized))
}

fn remote_read_rejection_prunes_location(reason: &str) -> bool {
    reason.contains("is not a current location for DFS object")
}

fn pending_networked_write(
    object_id: DfsObjectId,
    version: DfsVersion,
    content_hash: String,
    size_bytes: u64,
) -> PendingNetworkedWrite {
    pending_networked_write_with_patch(object_id, version, content_hash, size_bytes, None)
}

fn pending_networked_write_with_patch(
    object_id: DfsObjectId,
    version: DfsVersion,
    content_hash: String,
    size_bytes: u64,
    patch: Option<PendingNetworkedWritePatch>,
) -> PendingNetworkedWrite {
    PendingNetworkedWrite {
        object_id,
        version,
        content_hash,
        size_bytes,
        patch,
        stage_retries: BTreeMap::new(),
        repair_retries: BTreeMap::new(),
        stage_inflight_request_ids: BTreeMap::new(),
        stage_next_chunk_indexes: BTreeMap::new(),
        repair_inflight_request_ids: BTreeMap::new(),
        repair_next_chunk_indexes: BTreeMap::new(),
        fetch_inflight_from: BTreeSet::new(),
        fetch_inflight_request_ids: BTreeMap::new(),
        fetch_inflight_started_ticks: BTreeMap::new(),
        fetch_next_chunk_indexes: BTreeMap::new(),
        fetch_window_end_chunk_indexes: BTreeMap::new(),
        fetch_failed_from: BTreeSet::new(),
    }
}

fn write_intent_record(
    intent: &DistributedWriteIntent,
    content_hash: Option<&str>,
    object_metadata: Option<DfsObjectMetadata>,
) -> DfsWriteIntentRecord {
    let now = OffsetDateTime::now_utc();
    DfsWriteIntentRecord {
        intent_id: intent.intent_id.clone(),
        file_system_id: intent.file_system_id.clone(),
        object_id: intent.object_id.clone(),
        base_version: intent.base_version.clone(),
        target_version: intent.target_version.clone(),
        coordinator_node_id: intent.requesting_node_id.clone(),
        owner_epoch: intent.owner_epoch,
        desired_replica_node_ids: intent.desired_replica_nodes.clone(),
        policy: intent.policy.clone(),
        status: write_intent_status(&intent.state),
        content_hash: content_hash.map(ToOwned::to_owned),
        object_metadata,
        created_at: now,
        updated_at: Some(now),
    }
}

fn write_intent_status(state: &DfsWriteIntentState) -> DfsWriteIntentStatus {
    match state {
        DfsWriteIntentState::AwaitingReceipts => DfsWriteIntentStatus::ReceiptsPending,
        DfsWriteIntentState::Committed => DfsWriteIntentStatus::Committed,
        DfsWriteIntentState::Rejected => DfsWriteIntentStatus::Rejected,
        DfsWriteIntentState::Conflict => DfsWriteIntentStatus::Conflict,
        DfsWriteIntentState::Aborted => DfsWriteIntentStatus::Aborted,
    }
}

fn distributed_intent_from_record(record: &DfsWriteIntentRecord) -> DistributedWriteIntent {
    DistributedWriteIntent {
        intent_id: record.intent_id.clone(),
        file_system_id: record.file_system_id.clone(),
        object_id: record.object_id.clone(),
        base_version: record.base_version.clone(),
        target_version: record.target_version.clone(),
        requesting_node_id: record.coordinator_node_id.clone(),
        owner_epoch: record.owner_epoch,
        desired_replica_nodes: record.desired_replica_node_ids.clone(),
        policy: record.policy.clone(),
        state: distributed_intent_state_from_status(&record.status),
    }
}

fn distributed_intent_matches_record(
    intent: &DistributedWriteIntent,
    record: &DfsWriteIntentRecord,
) -> bool {
    intent.file_system_id == record.file_system_id
        && intent.object_id == record.object_id
        && intent.target_version == record.target_version
}

fn distributed_intent_state_from_status(status: &DfsWriteIntentStatus) -> DfsWriteIntentState {
    match status {
        DfsWriteIntentStatus::Staging | DfsWriteIntentStatus::ReceiptsPending => {
            DfsWriteIntentState::AwaitingReceipts
        }
        DfsWriteIntentStatus::Committed => DfsWriteIntentState::Committed,
        DfsWriteIntentStatus::Rejected => DfsWriteIntentState::Rejected,
        DfsWriteIntentStatus::Conflict => DfsWriteIntentState::Conflict,
        DfsWriteIntentStatus::Aborted => DfsWriteIntentState::Aborted,
    }
}

fn repair_retry_delay_ticks(attempts: u32) -> u64 {
    1_u64 << attempts.saturating_sub(1).min(6)
}

fn stage_inflight_retry_delay_ticks(attempts: u32) -> u64 {
    repair_retry_delay_ticks(attempts).saturating_add(1)
}

fn fetch_inflight_timeout_ticks() -> u64 {
    stage_inflight_retry_delay_ticks(2)
}

fn namespace_transaction_id_for_object(
    kind: &str,
    object_id: &DfsObjectId,
    version: &DfsVersion,
) -> String {
    format!(
        "namespace_{}_{}",
        kind,
        blake3::hash(format!("{object_id}\0{version}").as_bytes()).to_hex()
    )
}

fn namespace_transaction_id_for_batch(
    kind: &str,
    file_system_id: &DfsFileSystemId,
    objects: &[DfsObjectMetadata],
    versions: &[DfsObjectVersionRecord],
) -> String {
    let mut input = format!("{file_system_id}\0{}\0{}\0", objects.len(), versions.len());
    for object in objects {
        input.push_str(object.object_id.as_str());
        input.push('\0');
        input.push_str(object.version.as_str());
        input.push('\0');
        input.push_str(&object.path);
        input.push('\0');
        input.push_str(&format!("{:?}", object.kind));
        input.push('\0');
        input.push_str(object.content_hash.as_deref().unwrap_or(""));
        input.push('\0');
        input.push_str(
            &object
                .content_size_bytes
                .map(|size| size.to_string())
                .unwrap_or_default(),
        );
        input.push('\0');
    }
    for version in versions {
        input.push_str(version.object_id.as_str());
        input.push('\0');
        input.push_str(version.version.as_str());
        input.push('\0');
        input.push_str(version.content_hash.as_deref().unwrap_or(""));
        input.push('\0');
        input.push_str(
            &version
                .size_bytes
                .map(|size| size.to_string())
                .unwrap_or_default(),
        );
        input.push('\0');
        input.push_str(if version.visible { "visible" } else { "hidden" });
        input.push('\0');
    }
    format!(
        "namespace_{kind}_{}",
        blake3::hash(input.as_bytes()).to_hex()
    )
}

impl From<DfsConflictRecord> for DfsConflictReadView {
    fn from(conflict: DfsConflictRecord) -> Self {
        Self {
            conflict_id: conflict.conflict_id,
            base_version: conflict.base_version,
            local_version: conflict.local_version,
            remote_version: conflict.remote_version,
            detected_by_node_id: conflict.detected_by_node_id,
            reason: conflict.reason,
            local_content_hash: conflict.local_content_hash,
            remote_content_hash: conflict.remote_content_hash,
            status: conflict.status,
        }
    }
}

fn distributed_error_creates_conflict(error: &DfsDistributedError) -> bool {
    matches!(
        error,
        DfsDistributedError::BaseVersionConflict { .. }
            | DfsDistributedError::DuplicateIntent { .. }
            | DfsDistributedError::PendingIntentConflict { .. }
    )
}

fn conflict_id_for(
    object_id: &DfsObjectId,
    local_version: &DfsVersion,
    local_content_hash: Option<&str>,
    detected_by_node_id: &NodeId,
) -> String {
    let hash_input = format!(
        "{}\0{}\0{}\0{}",
        object_id,
        local_version,
        local_content_hash.unwrap_or(""),
        detected_by_node_id
    );
    format!("conflict_{}", blake3::hash(hash_input.as_bytes()).to_hex())
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum DfsError {
    #[error("unknown filesystem: {file_system_id}")]
    UnknownFileSystem { file_system_id: DfsFileSystemId },
    #[error("unknown object: {object_id}")]
    UnknownObject { object_id: DfsObjectId },
    #[error("unsupported DFS policy for local-owned service: {policy:?}")]
    UnsupportedPolicy { policy: DfsObjectPolicy },
    #[error("node {local_node_id} cannot own object {object_id}; owner is {owner_node_id}")]
    NotObjectOwner {
        object_id: DfsObjectId,
        owner_node_id: NodeId,
        local_node_id: NodeId,
    },
    #[error("network error: {message}")]
    Network { message: String },
    #[error("serialization error: {message}")]
    Serialization { message: String },
    #[error("content store error: {message}")]
    ContentStore { message: String },
    #[error("distributed DFS error: {message}")]
    Distributed { message: String },
    #[error("metadata backend error: {message}")]
    Metadata { message: String },
    #[error("invalid DFS replication policy: {message}")]
    InvalidReplicationPolicy { message: String },
    #[error("content hash mismatch: expected {expected}, actual {actual}")]
    ContentHashMismatch { expected: String, actual: String },
    #[error("unauthorized DFS replica command from {source_node_id}: {reason}")]
    UnauthorizedReplicaCommand {
        source_node_id: NodeId,
        reason: String,
    },
    #[error(
        "stale owner epoch for filesystem {file_system_id}: local node {local_node_id}, primary {primary_node_id}, epoch {epoch}"
    )]
    StaleOwnerEpoch {
        file_system_id: DfsFileSystemId,
        local_node_id: NodeId,
        primary_node_id: NodeId,
        epoch: u64,
    },
}

impl DfsError {
    fn from_network(error: sinomo_network::NetworkError) -> Self {
        Self::Network {
            message: error.to_string(),
        }
    }
}

impl From<ContentStoreError> for DfsError {
    fn from(error: ContentStoreError) -> Self {
        Self::ContentStore {
            message: error.to_string(),
        }
    }
}

impl From<DfsDistributedError> for DfsError {
    fn from(error: DfsDistributedError) -> Self {
        Self::Distributed {
            message: error.to_string(),
        }
    }
}

impl From<MetadataBackendError> for DfsError {
    fn from(error: MetadataBackendError) -> Self {
        Self::Metadata {
            message: error.to_string(),
        }
    }
}

impl From<KvError> for DfsError {
    fn from(error: KvError) -> Self {
        Self::Distributed {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests;
