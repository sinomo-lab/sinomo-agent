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

pub use content_store::{ContentManifest, ContentStoreError, LocalContentStore};
pub use control_plane::{
    DfsControlPlaneKvClient, DfsControlPlaneStore, DfsFileSystemConfig, DfsMetadataShardAssignment,
    DfsMetadataShardConfig, SharedDfsControlPlaneKvClient, file_system_config_key,
    metadata_shard_key,
};
pub use discovery::{
    DfsClusterView, DfsNodeAdvertisement, content_service_key, content_services_prefix,
    metadata_service_key, metadata_services_prefix,
};
pub use distributed::{
    DfsDistributedError, DfsReplicaCandidate, DfsReplicaPlacementPlan, DfsReplicaPlacementRequest,
    DfsReplicaReceiptState, DfsReplicationPolicy, DfsWriteIntentState,
    DistributedEventualCoordinator, DistributedRepairJob, DistributedReplicaReceipt,
    DistributedWriteIntent, plan_replicas,
};
pub use local_owned::{
    DfsExposeAddRequest, DfsExposeConfig, DfsExposeEntry, DfsExposeFileInfo, DfsExposeStore,
    DfsExposeStoreError, DfsExposedDirectory, LocalOwnedDfsError,
};
pub use metadata_backend::{
    DfsConflictRecord, DfsConflictStatus, DfsDirFragmentId, DfsDirFragmentRecord, DfsInodeId,
    DfsInodeRecord, DfsMetadataBackend, DfsMetadataJournalBatch, DfsMetadataJournalEntry,
    DfsMetadataMutation, DfsMetadataShardId, DfsMetadataSubscription, DfsNamespaceTransaction,
    DfsObjectVersionRecord, DfsPathRecord, DfsReplicaReceiptRecord, DfsWriteIntentRecord,
    DfsWriteIntentStatus, LocalJsonMetadataBackend, MetadataBackendError,
};
use namespace::metadata_path;
pub use namespace::{
    SIFS_NODES_PATH, SIFS_ROOT_PATH, SIFS_SERVICES_PATH, SIFS_SINOMO_PATH, SIFS_USERS_PATH,
    directory_object, distributed_sifs, distributed_sifs_bootstrap_objects, sifs_node_files_path,
    sifs_node_path,
};
pub use operator::{
    DfsAgentFileSystemSummary, DfsAgentObjectState, DfsAgentObjectSummary,
    DfsOperatorFileSystemView, DfsOperatorInspector, DfsOperatorObjectView, DfsOperatorRepairView,
    DfsOperatorStatus, DfsPinAddRequest, DfsPinMode, DfsPinRecord, DfsPinStoreError,
    LocalDfsPinStore,
};
pub use posix::{DfsDirEntry, DfsPosixError, DfsStat, DfsWriteOptions};

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
use std::path::{Path, PathBuf};
use thiserror::Error;
use time::OffsetDateTime;

pub const DFS_PROTOCOL: &str = "sinomo.dfs.v1";
pub const DEFAULT_DFS_NODE_ID: &str = "node_local";
pub const SIFS_FILE_SYSTEM_ID: &str = "sifs";
const DFS_BLOB_INLINE_LIMIT_BYTES: usize = 64 * 1024;
const DFS_BLOB_CHUNK_BYTES: usize = 64 * 1024;

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
    pub version: DfsVersion,
    pub policy: DfsObjectPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_by_event_id: Option<EventId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsObjectKind {
    File,
    Directory,
    Symlink,
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

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadData {
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadRequest {
    pub request_id: String,
    pub object_id: DfsObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsReadResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsReadResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsReadResult {
    Found(DfsReadData),
    NotFound,
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteCommand {
    pub request_id: String,
    pub request: DfsWriteRequest,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsWriteResponse {
    pub request_id: String,
    pub object_id: DfsObjectId,
    pub result: DfsWriteResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsWriteResult {
    Committed {
        receipt: DfsWriteReceipt,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_hash: Option<String>,
    },
    NotFound,
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsStageBlobChunkCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCommitVersionCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsCommitVersionResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaResponse {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub replica_node_id: NodeId,
    pub result: DfsBlobTransferResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairReplicaChunkCommand {
    pub request_id: String,
    pub intent_id: String,
    pub file_system_id: DfsFileSystemId,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub total_size_bytes: u64,
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_metadata: Option<DfsObjectMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobCommand {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsFetchBlobResponse {
    pub request_id: String,
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub source_node_id: NodeId,
    pub result: DfsFetchBlobResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsFetchBlobResult {
    Found {
        content_hash: String,
        bytes: Vec<u8>,
    },
    NotFound,
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsBlobTransferResult {
    Durable { content_hash: String },
    Committed { content_hash: String },
    Rejected { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsNetworkMessage {
    ReadRequest(DfsReadRequest),
    ReadResponse(DfsReadResponse),
    WriteRequest(DfsWriteCommand),
    WriteResponse(DfsWriteResponse),
    StageBlob(DfsStageBlobCommand),
    StageBlobChunk(DfsStageBlobChunkCommand),
    StageBlobResponse(DfsStageBlobResponse),
    CommitVersion(DfsCommitVersionCommand),
    CommitVersionResponse(DfsCommitVersionResponse),
    RepairReplica(DfsRepairReplicaCommand),
    RepairReplicaChunk(DfsRepairReplicaChunkCommand),
    RepairReplicaResponse(DfsRepairReplicaResponse),
    FetchBlob(DfsFetchBlobCommand),
    FetchBlobResponse(DfsFetchBlobResponse),
}

#[derive(Clone, Debug, Default)]
pub struct DfsMetadataStore {
    filesystems: BTreeMap<DfsFileSystemId, DfsFileSystem>,
    objects: BTreeMap<DfsObjectId, DfsObjectMetadata>,
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

    pub fn add_object(&mut self, object: DfsObjectMetadata) -> Result<(), DfsError> {
        if !self.filesystems.contains_key(&object.file_system_id) {
            return Err(DfsError::UnknownFileSystem {
                file_system_id: object.file_system_id,
            });
        }
        self.objects.insert(object.object_id.clone(), object);
        Ok(())
    }

    pub fn object(&self, object_id: &DfsObjectId) -> Option<&DfsObjectMetadata> {
        self.objects.get(object_id)
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

            let message: DfsNetworkMessage =
                protocol_message.decode().map_err(DfsError::from_network)?;

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
                | DfsNetworkMessage::StageBlobResponse(_)
                | DfsNetworkMessage::CommitVersion(_)
                | DfsNetworkMessage::CommitVersionResponse(_)
                | DfsNetworkMessage::RepairReplica(_)
                | DfsNetworkMessage::RepairReplicaChunk(_)
                | DfsNetworkMessage::RepairReplicaResponse(_)
                | DfsNetworkMessage::FetchBlob(_)
                | DfsNetworkMessage::FetchBlobResponse(_) => {}
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
            Some(_) => self
                .local_read(&request.object_id)?
                .map(DfsReadResult::Found)
                .unwrap_or(DfsReadResult::NotFound),
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
    RepairRejected {
        intent_id: String,
        object_id: DfsObjectId,
        replica_node_id: NodeId,
        reason: String,
    },
    WriteConflictCreated {
        conflict: DfsConflictRecord,
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

#[derive(Clone, Debug)]
struct PendingNetworkedWrite {
    object_id: DfsObjectId,
    version: DfsVersion,
    content_hash: String,
    repair_retries: BTreeMap<NodeId, RepairRetryState>,
    fetch_inflight_from: BTreeSet<NodeId>,
}

#[derive(Clone, Debug, Default)]
struct RepairRetryState {
    attempts: u32,
    next_retry_tick: u64,
    inflight: bool,
}

#[derive(Clone, Debug)]
struct InProgressBlobTransfer {
    request_id: String,
    file_system_id: DfsFileSystemId,
    object_id: DfsObjectId,
    version: DfsVersion,
    content_hash: String,
    total_size_bytes: u64,
    object_metadata: Option<DfsObjectMetadata>,
    chunks: Vec<Option<Vec<u8>>>,
}

impl InProgressBlobTransfer {
    fn new(
        request_id: String,
        file_system_id: DfsFileSystemId,
        object_id: DfsObjectId,
        version: DfsVersion,
        content_hash: String,
        total_size_bytes: u64,
        total_chunks: u32,
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<Self, DfsError> {
        if total_chunks == 0 {
            return Err(DfsError::Distributed {
                message: "chunked DFS blob transfer has zero chunks".to_string(),
            });
        }
        Ok(Self {
            request_id,
            file_system_id,
            object_id,
            version,
            content_hash,
            total_size_bytes,
            object_metadata,
            chunks: vec![None; total_chunks as usize],
        })
    }

    fn insert_chunk(
        &mut self,
        chunk_index: u32,
        total_chunks: u32,
        bytes: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, DfsError> {
        if total_chunks as usize != self.chunks.len() {
            return Err(DfsError::Distributed {
                message: format!(
                    "chunked DFS blob transfer total changed from {} to {total_chunks}",
                    self.chunks.len()
                ),
            });
        }
        let Some(slot) = self.chunks.get_mut(chunk_index as usize) else {
            return Err(DfsError::Distributed {
                message: format!(
                    "chunked DFS blob transfer chunk index {chunk_index} out of {total_chunks}"
                ),
            });
        };
        *slot = Some(bytes);
        if self.chunks.iter().any(Option::is_none) {
            return Ok(None);
        }

        let mut assembled = Vec::with_capacity(self.total_size_bytes as usize);
        for chunk in &self.chunks {
            assembled.extend_from_slice(chunk.as_ref().expect("all chunks present"));
        }
        if assembled.len() as u64 != self.total_size_bytes {
            return Err(DfsError::Distributed {
                message: format!(
                    "chunked DFS blob transfer size mismatch: expected {}, actual {}",
                    self.total_size_bytes,
                    assembled.len()
                ),
            });
        }
        Ok(Some(assembled))
    }
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
    pending_writes: BTreeMap<String, PendingNetworkedWrite>,
    incoming_blob_transfers: BTreeMap<String, InProgressBlobTransfer>,
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
            pending_writes: BTreeMap::new(),
            incoming_blob_transfers: BTreeMap::new(),
            transport,
            repair_retry_tick: 0,
            next_frame_id: 1,
            next_message_id: 1,
            next_request_id: 1,
        }
    }

    pub fn with_control_plane(mut self, kv: impl DfsControlPlaneKvClient + 'static) -> Self {
        self.control_plane = Some(Box::new(kv));
        self
    }

    pub fn with_metadata_backend(
        mut self,
        backend: impl DfsMetadataBackend + 'static,
    ) -> Result<Self, DfsError> {
        self.metadata_backend = Some(Box::new(backend));
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

    pub fn coordinator(&self) -> &DistributedEventualCoordinator {
        &self.coordinator
    }

    pub fn metadata_backend(&self) -> Option<&dyn DfsMetadataBackend> {
        self.metadata_backend.as_deref()
    }

    pub fn hydrate_from_metadata_backend(&mut self) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_ref() else {
            return Ok(());
        };

        let file_systems = backend.list_file_systems()?;
        let mut objects = Vec::new();
        let mut conflicts = Vec::new();
        let mut intents = Vec::new();
        let mut receipts_by_intent = BTreeMap::<String, Vec<DfsReplicaReceiptRecord>>::new();

        for file_system in &file_systems {
            for object in backend.objects_for_file_system(&file_system.file_system_id)? {
                conflicts.extend(backend.conflicts_for_object(&object.object_id)?);
                for intent in backend.intents_for_object(&object.object_id)? {
                    let receipts =
                        backend.receipts_for_version(&object.object_id, &intent.target_version)?;
                    receipts_by_intent.insert(intent.intent_id.clone(), receipts);
                    intents.push(intent);
                }
                objects.push(object);
            }
        }

        self.metadata = DfsMetadataStore::new();
        self.tombstones_by_metadata_path.clear();
        self.coordinator = DistributedEventualCoordinator::new();
        self.pending_writes.clear();

        for file_system in file_systems {
            self.metadata.add_file_system(file_system);
        }
        if !self
            .metadata
            .filesystems
            .contains_key(&sifs_file_system_id())
        {
            self.metadata
                .add_file_system(DfsFileSystem::sifs(self.node_id.clone()));
        }

        for object in objects {
            self.coordinator.set_committed_version(
                object.file_system_id.clone(),
                object.object_id.clone(),
                object.version.clone(),
            );
            self.track_metadata_tombstone(&object);
            self.metadata.add_object(object)?;
        }

        for conflict in conflicts {
            self.metadata.add_conflict(conflict);
        }

        intents.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.intent_id.cmp(&right.intent_id))
        });
        for intent_record in intents {
            let intent = distributed_intent_from_record(&intent_record);
            self.coordinator.restore_intent(intent)?;
            if let Some(content_hash) = intent_record.content_hash.clone() {
                self.pending_writes.insert(
                    intent_record.intent_id.clone(),
                    PendingNetworkedWrite {
                        object_id: intent_record.object_id.clone(),
                        version: intent_record.target_version.clone(),
                        content_hash,
                        repair_retries: BTreeMap::new(),
                        fetch_inflight_from: BTreeSet::new(),
                    },
                );
            }
            if let Some(receipts) = receipts_by_intent.get(&intent_record.intent_id) {
                for receipt in receipts {
                    self.coordinator.restore_durable_receipt(
                        &intent_record.intent_id,
                        receipt.replica_node_id.clone(),
                    )?;
                }
            }
        }

        Ok(())
    }

    pub fn add_file_system(&mut self, file_system: DfsFileSystem) -> Result<(), DfsError> {
        if let Some(kv) = self.control_plane.as_mut() {
            DfsControlPlaneStore::new(kv.as_mut())
                .put_file_system_config(&DfsFileSystemConfig::from(&file_system))?;
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

    pub fn add_transient_object_metadata(
        &mut self,
        object: DfsObjectMetadata,
    ) -> Result<(), DfsError> {
        self.coordinator.set_committed_version(
            object.file_system_id.clone(),
            object.object_id.clone(),
            object.version.clone(),
        );
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
        if self
            .tombstones_by_metadata_path
            .get(&key)
            .is_some_and(|existing| existing.object_id == object.object_id)
        {
            self.tombstones_by_metadata_path.remove(&key);
        }
    }

    pub fn remove_transient_object_metadata(&mut self, object_id: &DfsObjectId) {
        self.metadata.objects.remove(object_id);
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
        for object in transaction.objects {
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
        mut object: DfsObjectMetadata,
        bytes: &[u8],
    ) -> Result<String, DfsError> {
        let manifest = self.content.put_object_version(
            object.object_id.as_str(),
            object.version.as_str(),
            bytes,
        )?;
        object.content_hash = Some(manifest.content_hash.clone());
        if !object.current_locations.contains(&self.node_id) {
            object.current_locations.push(self.node_id.clone());
        }
        self.put_visible_object_in_metadata_backend(&object)?;
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

    pub fn read_local_object_with_state(
        &self,
        object_id: &DfsObjectId,
        allow_stale: bool,
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let Some(object) = self.metadata.object(object_id) else {
            return Err(DfsError::UnknownObject {
                object_id: object_id.clone(),
            });
        };
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

        if let Ok(manifest) = self
            .content
            .read_manifest(object.object_id.as_str(), object.version.as_str())
        {
            if object
                .content_hash
                .as_ref()
                .is_none_or(|expected_hash| expected_hash == &manifest.content_hash)
            {
                let bytes = self.content.read_blob(&manifest.content_hash)?;
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
        }

        if allow_stale {
            let stale_manifest = self
                .content
                .manifests_for_object(object.object_id.as_str())?
                .into_iter()
                .rev()
                .find(|manifest| manifest.version != object.version.as_str());
            if let Some(manifest) = stale_manifest {
                let bytes = self.content.read_blob(&manifest.content_hash)?;
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

    pub fn read_local_object_confirmed(
        &self,
        object_id: &DfsObjectId,
        required_replica_node_ids: &[NodeId],
    ) -> Result<NetworkedDfsReadOutcome, DfsError> {
        let Some(object) = self.metadata.object(object_id) else {
            return Err(DfsError::UnknownObject {
                object_id: object_id.clone(),
            });
        };
        if object.policy != DfsObjectPolicy::ReplicatedConfirmed {
            return Err(DfsError::UnsupportedPolicy {
                policy: object.policy.clone(),
            });
        }

        let confirmed_locations =
            self.confirmed_locations_for_object(object, required_replica_node_ids)?;
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
        let object = self
            .metadata
            .object(&request.object_id)
            .cloned()
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: request.object_id.clone(),
            })?;
        self.write_distributed_eventual_with_base(
            request,
            Some(object.version),
            bytes,
            desired_replica_nodes,
            policy,
        )
    }

    pub fn write_distributed_eventual_with_base(
        &mut self,
        request: DfsWriteRequest,
        expected_base_version: Option<DfsVersion>,
        bytes: Vec<u8>,
        desired_replica_nodes: Vec<NodeId>,
        policy: DfsReplicationPolicy,
    ) -> Result<NetworkedDfsWriteOutcome, DfsError> {
        let object = self
            .metadata
            .object(&request.object_id)
            .cloned()
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: request.object_id.clone(),
            })?;
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
        let owner_epoch = self.ensure_file_system_write_authority(&request.file_system_id)?;

        if object.policy == DfsObjectPolicy::ReplicatedEventual
            && policy.min_commit_replicas == 1
            && desired_replica_nodes.len() == 1
            && desired_replica_nodes[0] == self.node_id
        {
            if expected_base_version
                .as_ref()
                .is_some_and(|expected| expected != &object.version)
            {
                let manifest = self.content.put_object_version(
                    request.object_id.as_str(),
                    request.target_version.as_str(),
                    &bytes,
                )?;
                let conflict = self.create_write_conflict(
                    &object,
                    expected_base_version,
                    request.target_version.clone(),
                    request.requesting_node_id.clone(),
                    Some(manifest.content_hash.clone()),
                    "base version conflict for local eventual write".to_string(),
                    None,
                )?;
                return Ok(NetworkedDfsWriteOutcome::Conflict { conflict });
            }

            let mut committed_object = object.clone();
            let manifest = self.content.put_object_version(
                request.object_id.as_str(),
                request.target_version.as_str(),
                &bytes,
            )?;
            committed_object.version = request.target_version.clone();
            committed_object.content_hash = Some(manifest.content_hash.clone());
            committed_object.current_locations = vec![self.node_id.clone()];
            committed_object.last_modified_by_event_id = Some(EventId::generate());
            self.put_visible_object_in_metadata_backend(&committed_object)?;
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
            PendingNetworkedWrite {
                object_id: request.object_id.clone(),
                version: request.target_version.clone(),
                content_hash: manifest.content_hash.clone(),
                repair_retries: BTreeMap::new(),
                fetch_inflight_from: BTreeSet::new(),
            },
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

        if matches!(updated_intent.state, DfsWriteIntentState::Committed) {
            self.apply_committed_intent(&updated_intent.intent_id)?;
        } else {
            let stage_targets: Vec<NodeId> = desired_replica_nodes
                .iter()
                .filter(|node_id| *node_id != &self.node_id)
                .cloned()
                .collect();
            for replica_node_id in stage_targets {
                let request_id = self.next_request_id();
                self.send_stage_blob_if_routable(
                    &replica_node_id,
                    request_id,
                    &updated_intent.intent_id,
                    &request.file_system_id,
                    &request.object_id,
                    &request.target_version,
                    &manifest.content_hash,
                    &bytes,
                    Some(object.clone()),
                )?;
            }
        }

        self.write_outcome(&updated_intent.intent_id)
    }

    pub fn process_incoming(&mut self) -> Result<Vec<NetworkedDfsServiceEvent>, DfsError> {
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
            let message: DfsNetworkMessage =
                protocol_message.decode().map_err(DfsError::from_network)?;

            match message {
                DfsNetworkMessage::StageBlob(command) => {
                    events.push(self.handle_stage_blob(source_node_id, command)?);
                }
                DfsNetworkMessage::StageBlobChunk(command) => {
                    if let Some(event) = self.handle_stage_blob_chunk(source_node_id, command)? {
                        events.push(event);
                    }
                }
                DfsNetworkMessage::StageBlobResponse(response) => {
                    if let Some(event) = self.handle_stage_blob_response(response)? {
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
                    events.push(self.handle_repair_replica_response(response)?);
                }
                DfsNetworkMessage::FetchBlob(command) => {
                    events.push(self.handle_fetch_blob(source_node_id, command)?);
                }
                DfsNetworkMessage::FetchBlobResponse(response) => {
                    events.push(self.handle_fetch_blob_response(response)?);
                }
                DfsNetworkMessage::CommitVersionResponse(_)
                | DfsNetworkMessage::ReadRequest(_)
                | DfsNetworkMessage::ReadResponse(_)
                | DfsNetworkMessage::WriteRequest(_)
                | DfsNetworkMessage::WriteResponse(_) => {}
            }
        }

        Ok(events)
    }

    pub fn retry_pending_repairs(&mut self) -> Result<Vec<DistributedRepairJob>, DfsError> {
        self.repair_retry_tick = self.repair_retry_tick.saturating_add(1);
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

            let bytes = match self.content.read_blob(&pending.content_hash) {
                Ok(bytes) => bytes,
                Err(_) => {
                    let fetch_requested =
                        self.request_repair_blob_from_alternate(&job, &pending)?;
                    if !fetch_requested {
                        self.record_repair_retry_scheduled(
                            &job.intent_id,
                            &job.missing_replica_node_id,
                        );
                    }
                    continue;
                }
            };
            let request_id = self.next_request_id();
            let sent = self.send_repair_replica_if_routable(
                &job.missing_replica_node_id,
                request_id,
                &job.intent_id,
                &job.file_system_id,
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &bytes,
                self.metadata.object(&pending.object_id).cloned(),
            )?;

            if sent {
                self.record_repair_inflight(&job.intent_id, &job.missing_replica_node_id);
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
                if matches!(updated_intent.state, DfsWriteIntentState::Committed) {
                    self.apply_committed_intent(intent_id)?;
                }
                Ok(DfsConfirmedTimeoutOutcome::Downgraded(
                    self.write_outcome(intent_id)?,
                ))
            }
        }
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
        self.pending_writes.remove(intent_id);
        Ok(intent)
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
        if let Some(backend) = self.metadata_backend.as_ref() {
            for conflict in backend.conflicts_for_object(object_id)? {
                conflicts.insert(conflict.conflict_id.clone(), conflict);
            }
        }
        Ok(conflicts.into_values().collect())
    }

    pub fn open_conflicts_for_object(
        &self,
        object_id: &DfsObjectId,
    ) -> Result<Vec<DfsConflictRecord>, DfsError> {
        Ok(self
            .conflicts_for_object(object_id)?
            .into_iter()
            .filter(|conflict| conflict.status == DfsConflictStatus::Open)
            .collect())
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
        let object =
            self.metadata
                .object(object_id)
                .cloned()
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
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
                    has_current_manifest = true;
                    let local_node_id = self.node_id.clone();
                    self.put_metadata_receipt(
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
                    refreshed_receipts += 1;
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
            } else {
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

    pub fn handoff_object_owner(
        &mut self,
        object_id: &DfsObjectId,
        shard_id: &str,
        expected_epoch: u64,
    ) -> Result<DfsOwnerHandoffOutcome, DfsError> {
        let object =
            self.metadata
                .object(object_id)
                .cloned()
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
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

    fn ensure_file_system_write_authority(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<u64, DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(0);
        };
        let Some(shard) =
            DfsControlPlaneStore::new(kv.as_mut()).get_metadata_shard(file_system_id, "root")?
        else {
            return Ok(0);
        };
        if shard.primary_node_id != self.node_id {
            return Err(DfsError::StaleOwnerEpoch {
                file_system_id: file_system_id.clone(),
                local_node_id: self.node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
        }
        Ok(shard.epoch)
    }

    fn ensure_intent_epoch_current(
        &mut self,
        intent: &DistributedWriteIntent,
    ) -> Result<(), DfsError> {
        let Some(kv) = self.control_plane.as_mut() else {
            return Ok(());
        };
        let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
            .get_metadata_shard(&intent.file_system_id, "root")?
        else {
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
        let conflict = DfsConflictRecord {
            conflict_id: conflict_id_for(
                &object.object_id,
                &local_version,
                local_content_hash.as_deref(),
                &detected_by_node_id,
            ),
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
            backend.put_conflict(conflict)?;
        }
        self.metadata.add_conflict(conflict.clone());
        Ok(())
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
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => {
                let local_node_id = self.node_id.clone();
                self.put_metadata_receipt(
                    &command.file_system_id,
                    &command.object_id,
                    &command.version,
                    &local_node_id,
                    &command.content_hash,
                )?;
                DfsBlobTransferResult::Durable {
                    content_hash: command.content_hash.clone(),
                }
            }
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message(
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

    fn handle_stage_blob_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsStageBlobChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            return Err(DfsError::Distributed {
                message: format!(
                    "stage blob chunk transfer {} has no chunks",
                    command.intent_id
                ),
            });
        }
        let transfer_key = blob_transfer_key("stage", &requester_node_id, &command.intent_id);
        let transfer = self
            .incoming_blob_transfers
            .entry(transfer_key.clone())
            .or_insert_with(|| {
                InProgressBlobTransfer::new(
                    command.request_id.clone(),
                    command.file_system_id.clone(),
                    command.object_id.clone(),
                    command.version.clone(),
                    command.content_hash.clone(),
                    command.total_size_bytes,
                    command.total_chunks,
                    command.object_metadata.clone(),
                )
                .expect("validated nonzero chunk count")
            });
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            return Err(DfsError::Distributed {
                message: format!(
                    "conflicting stage blob chunk metadata for {}",
                    command.intent_id
                ),
            });
        }
        let Some(bytes) =
            transfer.insert_chunk(command.chunk_index, command.total_chunks, command.bytes)?
        else {
            return Ok(None);
        };
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        Ok(Some(self.handle_stage_blob(
            requester_node_id,
            DfsStageBlobCommand {
                request_id: transfer.request_id,
                intent_id: command.intent_id,
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id,
                version: transfer.version,
                content_hash: transfer.content_hash,
                bytes,
                object_metadata: transfer.object_metadata,
            },
        )?))
    }

    fn handle_stage_blob_response(
        &mut self,
        response: DfsStageBlobResponse,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        match response.result {
            DfsBlobTransferResult::Durable { content_hash }
            | DfsBlobTransferResult::Committed { content_hash } => {
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    &content_hash,
                )?;
                let updated_intent = self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                let event = NetworkedDfsServiceEvent::BlobStaged {
                    intent_id: response.intent_id.clone(),
                    object_id: response.object_id,
                    version: response.version,
                    replica_node_id: response.replica_node_id,
                    content_hash,
                };
                if matches!(updated_intent.state, DfsWriteIntentState::Committed) {
                    self.apply_committed_intent(&updated_intent.intent_id)?;
                }
                Ok(Some(event))
            }
            DfsBlobTransferResult::Rejected { reason } => {
                Ok(Some(NetworkedDfsServiceEvent::BlobStageRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    replica_node_id: response.replica_node_id,
                    reason,
                }))
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
                Ok(())
            })
            .and_then(|()| {
                self.commit_local_metadata(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                )
                .and_then(|object| {
                    self.put_visible_object_in_metadata_backend(&object)?;
                    Ok(())
                })
            }) {
            Ok(()) => DfsBlobTransferResult::Committed {
                content_hash: command.content_hash.clone(),
            },
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message(
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
                self.store_command_bytes(
                    &command.object_id,
                    &command.version,
                    &command.content_hash,
                    &command.bytes,
                )
            }) {
            Ok(()) => match self
                .commit_local_metadata(&command.object_id, &command.version, &command.content_hash)
                .and_then(|object| {
                    self.put_visible_object_in_metadata_backend(&object)?;
                    let local_node_id = self.node_id.clone();
                    self.put_metadata_receipt(
                        &command.file_system_id,
                        &command.object_id,
                        &command.version,
                        &local_node_id,
                        &command.content_hash,
                    )?;
                    Ok(())
                }) {
                Ok(()) => DfsBlobTransferResult::Committed {
                    content_hash: command.content_hash.clone(),
                },
                Err(error) => DfsBlobTransferResult::Rejected {
                    reason: error.to_string(),
                },
            },
            Err(error) => DfsBlobTransferResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message(
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

    fn handle_repair_replica_chunk(
        &mut self,
        requester_node_id: NodeId,
        command: DfsRepairReplicaChunkCommand,
    ) -> Result<Option<NetworkedDfsServiceEvent>, DfsError> {
        if command.total_chunks == 0 {
            return Err(DfsError::Distributed {
                message: format!(
                    "repair replica chunk transfer {} has no chunks",
                    command.intent_id
                ),
            });
        }
        let transfer_key = blob_transfer_key("repair", &requester_node_id, &command.intent_id);
        let transfer = self
            .incoming_blob_transfers
            .entry(transfer_key.clone())
            .or_insert_with(|| {
                InProgressBlobTransfer::new(
                    command.request_id.clone(),
                    command.file_system_id.clone(),
                    command.object_id.clone(),
                    command.version.clone(),
                    command.content_hash.clone(),
                    command.total_size_bytes,
                    command.total_chunks,
                    command.object_metadata.clone(),
                )
                .expect("validated nonzero chunk count")
            });
        if transfer.file_system_id != command.file_system_id
            || transfer.object_id != command.object_id
            || transfer.version != command.version
            || transfer.content_hash != command.content_hash
        {
            self.incoming_blob_transfers.remove(&transfer_key);
            return Err(DfsError::Distributed {
                message: format!(
                    "conflicting repair replica chunk metadata for {}",
                    command.intent_id
                ),
            });
        }
        let Some(bytes) =
            transfer.insert_chunk(command.chunk_index, command.total_chunks, command.bytes)?
        else {
            return Ok(None);
        };
        let transfer = self
            .incoming_blob_transfers
            .remove(&transfer_key)
            .expect("completed transfer");
        Ok(Some(self.handle_repair_replica(
            requester_node_id,
            DfsRepairReplicaCommand {
                request_id: transfer.request_id,
                intent_id: command.intent_id,
                file_system_id: transfer.file_system_id,
                object_id: transfer.object_id,
                version: transfer.version,
                content_hash: transfer.content_hash,
                bytes,
                object_metadata: transfer.object_metadata,
            },
        )?))
    }

    fn handle_repair_replica_response(
        &mut self,
        response: DfsRepairReplicaResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        match response.result {
            DfsBlobTransferResult::Committed { content_hash }
            | DfsBlobTransferResult::Durable { content_hash } => {
                self.clear_repair_retry(&response.intent_id, &response.replica_node_id);
                self.put_metadata_receipt(
                    &response.file_system_id,
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    &content_hash,
                )?;
                self.coordinator.record_durable_receipt(
                    &response.intent_id,
                    response.replica_node_id.clone(),
                )?;
                self.mark_repaired_location(
                    &response.object_id,
                    &response.version,
                    &response.replica_node_id,
                    &content_hash,
                )?;
                Ok(NetworkedDfsServiceEvent::ReplicaRepaired {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    replica_node_id: response.replica_node_id,
                    content_hash,
                })
            }
            DfsBlobTransferResult::Rejected { reason } => {
                self.record_repair_retry_scheduled(&response.intent_id, &response.replica_node_id);
                Ok(NetworkedDfsServiceEvent::RepairRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    replica_node_id: response.replica_node_id,
                    reason,
                })
            }
        }
    }

    fn handle_fetch_blob(
        &mut self,
        requester_node_id: NodeId,
        command: DfsFetchBlobCommand,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        let result = match self
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
                let bytes = self.content.read_blob(&command.content_hash)?;
                Ok(bytes)
            }) {
            Ok(bytes) => DfsFetchBlobResult::Found {
                content_hash: command.content_hash.clone(),
                bytes,
            },
            Err(DfsError::ContentStore { .. }) => DfsFetchBlobResult::NotFound,
            Err(error) => DfsFetchBlobResult::Rejected {
                reason: error.to_string(),
            },
        };
        self.send_dfs_message(
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

    fn handle_fetch_blob_response(
        &mut self,
        response: DfsFetchBlobResponse,
    ) -> Result<NetworkedDfsServiceEvent, DfsError> {
        if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
            pending.fetch_inflight_from.remove(&response.source_node_id);
        }
        match response.result {
            DfsFetchBlobResult::Found {
                content_hash,
                bytes,
            } => {
                let Some(pending) = self.pending_writes.get(&response.intent_id).cloned() else {
                    return Err(DfsError::Distributed {
                        message: format!(
                            "missing pending write for fetched blob: {}",
                            response.intent_id
                        ),
                    });
                };
                if pending.object_id != response.object_id
                    || pending.version != response.version
                    || pending.content_hash != content_hash
                {
                    return Err(DfsError::ContentHashMismatch {
                        expected: pending.content_hash,
                        actual: content_hash,
                    });
                }
                self.store_command_bytes(
                    &response.object_id,
                    &response.version,
                    &content_hash,
                    &bytes,
                )?;
                if let Some(pending) = self.pending_writes.get_mut(&response.intent_id) {
                    for state in pending.repair_retries.values_mut() {
                        state.next_retry_tick = 0;
                        state.inflight = false;
                    }
                }
                Ok(NetworkedDfsServiceEvent::BlobFetched {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    version: response.version,
                    source_node_id: response.source_node_id,
                    content_hash,
                })
            }
            DfsFetchBlobResult::NotFound => {
                self.schedule_all_repair_retries(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason: "blob not found".to_string(),
                })
            }
            DfsFetchBlobResult::Rejected { reason } => {
                self.schedule_all_repair_retries(&response.intent_id);
                Ok(NetworkedDfsServiceEvent::BlobFetchRejected {
                    intent_id: response.intent_id,
                    object_id: response.object_id,
                    source_node_id: response.source_node_id,
                    reason,
                })
            }
        }
    }

    fn repair_retry_is_due(&self, intent_id: &str, replica_node_id: &NodeId) -> bool {
        self.pending_writes
            .get(intent_id)
            .and_then(|pending| pending.repair_retries.get(replica_node_id))
            .is_none_or(|state| state.next_retry_tick <= self.repair_retry_tick)
    }

    fn record_repair_inflight(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = true;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
        }
    }

    fn record_repair_retry_scheduled(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        let tick = self.repair_retry_tick;
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            let state = pending
                .repair_retries
                .entry(replica_node_id.clone())
                .or_default();
            state.attempts = state.attempts.saturating_add(1);
            state.inflight = false;
            state.next_retry_tick = tick.saturating_add(repair_retry_delay_ticks(state.attempts));
        }
    }

    fn clear_repair_retry(&mut self, intent_id: &str, replica_node_id: &NodeId) {
        if let Some(pending) = self.pending_writes.get_mut(intent_id) {
            pending.repair_retries.remove(replica_node_id);
        }
    }

    fn schedule_all_repair_retries(&mut self, intent_id: &str) {
        let missing_replicas = self
            .coordinator
            .repair_jobs()
            .iter()
            .filter(|job| job.intent_id == intent_id)
            .map(|job| job.missing_replica_node_id.clone())
            .collect::<Vec<_>>();
        for replica_node_id in missing_replicas {
            self.record_repair_retry_scheduled(intent_id, &replica_node_id);
        }
    }

    fn request_repair_blob_from_alternate(
        &mut self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Result<bool, DfsError> {
        for source_node_id in self.repair_blob_sources(job, pending) {
            if pending.fetch_inflight_from.contains(&source_node_id) {
                continue;
            }
            let request_id = self.next_request_id();
            let sent = self.send_dfs_message_if_routable(
                &source_node_id,
                DfsNetworkMessage::FetchBlob(DfsFetchBlobCommand {
                    request_id,
                    intent_id: job.intent_id.clone(),
                    object_id: pending.object_id.clone(),
                    version: pending.version.clone(),
                    content_hash: pending.content_hash.clone(),
                }),
            )?;
            if sent {
                if let Some(pending) = self.pending_writes.get_mut(&job.intent_id) {
                    pending.fetch_inflight_from.insert(source_node_id);
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn repair_blob_sources(
        &self,
        job: &DistributedRepairJob,
        pending: &PendingNetworkedWrite,
    ) -> Vec<NodeId> {
        let mut candidates = BTreeSet::new();
        for receipt in self.coordinator.receipts_for_intent(&job.intent_id) {
            if receipt.replica_node_id != self.node_id
                && receipt.replica_node_id != job.missing_replica_node_id
            {
                candidates.insert(receipt.replica_node_id.clone());
            }
        }
        if let Some(object) = self.metadata.object(&pending.object_id) {
            for node_id in &object.current_locations {
                if node_id != &self.node_id && node_id != &job.missing_replica_node_id {
                    candidates.insert(node_id.clone());
                }
            }
        }
        candidates.into_iter().collect()
    }

    fn apply_committed_intent(&mut self, intent_id: &str) -> Result<(), DfsError> {
        let Some(intent) = self.coordinator.intent(intent_id).cloned() else {
            return Err(DfsError::Distributed {
                message: format!("unknown committed intent: {intent_id}"),
            });
        };
        self.ensure_intent_epoch_current(&intent)?;
        let pending =
            self.pending_writes
                .get(intent_id)
                .cloned()
                .ok_or_else(|| DfsError::Distributed {
                    message: format!("missing pending write for committed intent: {intent_id}"),
                })?;
        self.commit_visible_version_in_metadata_backend(&intent, &pending)?;
        self.put_metadata_intent(&intent, Some(&pending.content_hash))?;
        self.commit_local_metadata(&pending.object_id, &pending.version, &pending.content_hash)?;

        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        if let Some(object) = self.metadata.objects.get_mut(&pending.object_id) {
            for node_id in &durable_nodes {
                if !object.current_locations.contains(node_id) {
                    object.current_locations.push(node_id.clone());
                }
            }
        }

        let commit_targets: Vec<NodeId> = durable_nodes
            .iter()
            .filter(|node_id| *node_id != &self.node_id)
            .cloned()
            .collect();
        let committed_metadata = self.metadata.object(&pending.object_id).cloned();
        for replica_node_id in commit_targets {
            let request_id = self.next_request_id();
            self.send_dfs_message(
                &replica_node_id,
                DfsNetworkMessage::CommitVersion(DfsCommitVersionCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: intent.file_system_id.clone(),
                    object_id: pending.object_id.clone(),
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
            let bytes = self.content.read_blob(&pending.content_hash)?;
            let request_id = self.next_request_id();
            let sent = self.send_repair_replica_if_routable(
                &replica_node_id,
                request_id,
                intent_id,
                &intent.file_system_id,
                &pending.object_id,
                &pending.version,
                &pending.content_hash,
                &bytes,
                committed_metadata.clone(),
            )?;
            if !sent {
                self.record_repair_retry_scheduled(intent_id, &replica_node_id);
            } else {
                self.record_repair_inflight(intent_id, &replica_node_id);
            }
        }
        Ok(())
    }

    fn commit_local_metadata(
        &mut self,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
    ) -> Result<DfsObjectMetadata, DfsError> {
        let object =
            self.metadata
                .objects
                .get_mut(object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
        object.version = version.clone();
        object.content_hash = Some(content_hash.to_string());
        object.last_modified_by_event_id = Some(EventId::generate());
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

    fn ensure_replicated_confirmed_object(&self, object_id: &DfsObjectId) -> Result<(), DfsError> {
        let object = self
            .metadata
            .object(object_id)
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;
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
            let receipts = backend.receipts_for_version(&object.object_id, &object.version)?;
            for receipt in receipts {
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
        let object =
            self.metadata
                .objects
                .get_mut(object_id)
                .ok_or_else(|| DfsError::UnknownObject {
                    object_id: object_id.clone(),
                })?;
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
        if !object.current_locations.contains(replica_node_id) {
            object.current_locations.push(replica_node_id.clone());
        }
        let object = object.clone();
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
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        backend.put_receipt(&DfsReplicaReceiptRecord {
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
        })?;
        Ok(())
    }

    fn put_metadata_intent(
        &mut self,
        intent: &DistributedWriteIntent,
        content_hash: Option<&str>,
    ) -> Result<(), DfsError> {
        let Some(backend) = self.metadata_backend.as_mut() else {
            return Ok(());
        };
        let mut record = write_intent_record(intent, content_hash);
        if let Some(existing) = backend.get_intent(&intent.object_id, &intent.intent_id)? {
            record.created_at = existing.created_at;
        }
        backend.put_intent(&record)?;
        Ok(())
    }

    fn put_visible_object_in_metadata_backend(
        &mut self,
        object: &DfsObjectMetadata,
    ) -> Result<(), DfsError> {
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
        backend.put_object(object)?;
        backend.put_version(&object_version_record(object))?;
        Ok(())
    }

    fn commit_visible_version_in_metadata_backend(
        &mut self,
        intent: &DistributedWriteIntent,
        pending: &PendingNetworkedWrite,
    ) -> Result<(), DfsError> {
        let expected_object = self
            .metadata
            .object(&pending.object_id)
            .cloned()
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: pending.object_id.clone(),
            })?;
        let durable_nodes: BTreeSet<NodeId> = self
            .coordinator
            .receipts_for_intent(&intent.intent_id)
            .into_iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect();
        let mut committed_object = expected_object.clone();
        committed_object.version = pending.version.clone();
        committed_object.content_hash = Some(pending.content_hash.clone());
        committed_object.last_modified_by_event_id = Some(EventId::generate());
        for node_id in &durable_nodes {
            if !committed_object.current_locations.contains(node_id) {
                committed_object.current_locations.push(node_id.clone());
            }
        }
        let visible_version = object_version_record(&committed_object);
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
                created_at: OffsetDateTime::now_utc(),
            })?;
            backend.put_version(&visible_version)?;
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

    fn ensure_replica_object_metadata(
        &mut self,
        object: Option<&DfsObjectMetadata>,
    ) -> Result<(), DfsError> {
        let Some(object) = object else {
            return Ok(());
        };
        if !self
            .metadata
            .filesystems
            .contains_key(&object.file_system_id)
        {
            self.add_file_system(DfsFileSystem {
                file_system_id: object.file_system_id.clone(),
                display_name: object.file_system_id.to_string(),
                owner_node_id: object.owner_node_id.clone(),
                subtree_policies: Vec::new(),
            })?;
        }
        if self.metadata.object(&object.object_id).is_none() {
            self.add_object_metadata(object.clone())?;
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
        let object = self
            .metadata
            .object(object_id)
            .cloned()
            .or_else(|| object_metadata.cloned())
            .ok_or_else(|| DfsError::UnknownObject {
                object_id: object_id.clone(),
            })?;
        if &object.owner_node_id != requester_node_id {
            return Err(DfsError::UnauthorizedReplicaCommand {
                source_node_id: requester_node_id.clone(),
                reason: format!(
                    "source is not object owner for {object_id}; owner is {}",
                    object.owner_node_id
                ),
            });
        }
        if let Some(kv) = self.control_plane.as_mut() {
            if let Some(shard) = DfsControlPlaneStore::new(kv.as_mut())
                .get_metadata_shard(&object.file_system_id, "root")?
            {
                if &shard.primary_node_id != requester_node_id {
                    return Err(DfsError::UnauthorizedReplicaCommand {
                        source_node_id: requester_node_id.clone(),
                        reason: format!(
                            "source is not current metadata shard primary for {}; primary is {} at epoch {}",
                            object.file_system_id, shard.primary_node_id, shard.epoch
                        ),
                    });
                }
            }
        }
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
            }
        }
        Ok(())
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
            let has_receipt = backend
                .receipts_for_version(object_id, &intent.target_version)?
                .into_iter()
                .any(|receipt| receipt.replica_node_id == self.node_id);
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
        let intent = self
            .coordinator
            .intent(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("unknown intent: {intent_id}"),
            })?;
        let pending = self
            .pending_writes
            .get(intent_id)
            .ok_or_else(|| DfsError::Distributed {
                message: format!("missing pending write: {intent_id}"),
            })?;
        if matches!(intent.state, DfsWriteIntentState::Committed) {
            Ok(NetworkedDfsWriteOutcome::Committed {
                intent_id: intent_id.to_string(),
                version: pending.version.clone(),
                content_hash: pending.content_hash.clone(),
                repair_jobs: self
                    .coordinator
                    .repair_jobs()
                    .iter()
                    .filter(|job| job.intent_id == intent_id)
                    .cloned()
                    .collect(),
            })
        } else {
            Ok(NetworkedDfsWriteOutcome::Pending {
                intent_id: intent_id.to_string(),
                durable_receipts: self.coordinator.durable_receipt_count(intent_id),
                required_receipts: intent.policy.min_commit_replicas,
            })
        }
    }

    fn send_dfs_message(
        &mut self,
        target_node_id: &NodeId,
        message: DfsNetworkMessage,
    ) -> Result<(), DfsError> {
        if self.send_dfs_message_if_routable(target_node_id, message)? {
            Ok(())
        } else {
            Err(DfsError::Network {
                message: format!("unknown node: {target_node_id}"),
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
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        if bytes.len() <= DFS_BLOB_INLINE_LIMIT_BYTES {
            return self.send_dfs_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: bytes.to_vec(),
                    object_metadata,
                }),
            );
        }

        let total_chunks = blob_chunk_count(bytes)?;
        let mut all_sent = true;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_dfs_message_if_routable(
                target_node_id,
                DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    bytes: chunk.to_vec(),
                    object_metadata: (chunk_index == 0)
                        .then(|| object_metadata.clone())
                        .flatten(),
                }),
            )?;
            all_sent &= sent;
        }
        Ok(all_sent)
    }

    fn send_repair_replica_if_routable(
        &mut self,
        target_node_id: &NodeId,
        request_id: String,
        intent_id: &str,
        file_system_id: &DfsFileSystemId,
        object_id: &DfsObjectId,
        version: &DfsVersion,
        content_hash: &str,
        bytes: &[u8],
        object_metadata: Option<DfsObjectMetadata>,
    ) -> Result<bool, DfsError> {
        if bytes.len() <= DFS_BLOB_INLINE_LIMIT_BYTES {
            return self.send_dfs_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplica(DfsRepairReplicaCommand {
                    request_id,
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    bytes: bytes.to_vec(),
                    object_metadata,
                }),
            );
        }

        let total_chunks = blob_chunk_count(bytes)?;
        let mut all_sent = true;
        for (chunk_index, chunk) in bytes.chunks(DFS_BLOB_CHUNK_BYTES).enumerate() {
            let sent = self.send_dfs_message_if_routable(
                target_node_id,
                DfsNetworkMessage::RepairReplicaChunk(DfsRepairReplicaChunkCommand {
                    request_id: request_id.clone(),
                    intent_id: intent_id.to_string(),
                    file_system_id: file_system_id.clone(),
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: content_hash.to_string(),
                    total_size_bytes: bytes.len() as u64,
                    chunk_index: chunk_index as u32,
                    total_chunks,
                    bytes: chunk.to_vec(),
                    object_metadata: (chunk_index == 0)
                        .then(|| object_metadata.clone())
                        .flatten(),
                }),
            )?;
            all_sent &= sent;
        }
        Ok(all_sent)
    }

    fn next_request_id(&mut self) -> String {
        let request_id = format!("dfs_net_req_{}", self.next_request_id);
        self.next_request_id += 1;
        request_id
    }
}

fn content_hash(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

fn object_version_record(object: &DfsObjectMetadata) -> DfsObjectVersionRecord {
    DfsObjectVersionRecord {
        file_system_id: object.file_system_id.clone(),
        object_id: object.object_id.clone(),
        version: object.version.clone(),
        content_hash: object.content_hash.clone(),
        locations: object.current_locations.clone(),
        created_by_event_id: object.last_modified_by_event_id.clone(),
        created_at: OffsetDateTime::now_utc(),
        visible: true,
    }
}

fn write_intent_record(
    intent: &DistributedWriteIntent,
    content_hash: Option<&str>,
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

fn blob_chunk_count(bytes: &[u8]) -> Result<u32, DfsError> {
    let chunks = bytes.len().div_ceil(DFS_BLOB_CHUNK_BYTES);
    u32::try_from(chunks).map_err(|_| DfsError::Distributed {
        message: format!("DFS blob is too large to transfer in {DFS_BLOB_CHUNK_BYTES}-byte chunks"),
    })
}

fn blob_transfer_key(kind: &str, source_node_id: &NodeId, intent_id: &str) -> String {
    format!("{kind}:{source_node_id}:{intent_id}")
}

fn repair_retry_delay_ticks(attempts: u32) -> u64 {
    1_u64 << attempts.saturating_sub(1).min(6)
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
mod tests {
    use super::*;
    use sinomo_network::InProcessNetwork;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn file_system(owner: NodeId) -> DfsFileSystem {
        DfsFileSystem {
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            display_name: "repo".to_string(),
            owner_node_id: owner,
            subtree_policies: Vec::new(),
        }
    }

    fn file_metadata(owner: NodeId) -> DfsObjectMetadata {
        DfsObjectMetadata {
            object_id: DfsObjectId::from_legacy_named("obj_file").expect("object"),
            file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
            path: "src/lib.rs".to_string(),
            kind: DfsObjectKind::File,
            owner_node_id: owner.clone(),
            current_locations: vec![owner],
            content_hash: None,
            version: DfsVersion::from_legacy_named("version_1").expect("version"),
            policy: DfsObjectPolicy::LocalOwned,
            created_by_event_id: None,
            last_modified_by_event_id: None,
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "sinomo-dfs-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    fn networked_sifs_with_backend(
        root: &Path,
        node_id: NodeId,
    ) -> NetworkedDfsService<sinomo_network::InProcessTransport> {
        let network = InProcessNetwork::new();
        let content = LocalContentStore::new(root.join("content")).expect("content");
        let metadata = open_root_local_metadata_backend(root.join("metadata")).expect("metadata");
        let mut dfs = NetworkedDfsService::new(network.add_node(node_id), content)
            .with_metadata_backend(metadata)
            .expect("metadata backend");
        dfs.bootstrap_distributed_sifs().expect("bootstrap sifs");
        dfs
    }

    #[test]
    fn local_owned_write_routes_to_owner() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let file_system_id = DfsFileSystemId::from_legacy_named("sifs").expect("filesystem");
        let object_id = DfsObjectId::from_legacy_named("obj_file").expect("object");
        let mut store = DfsMetadataStore::new();
        store.add_file_system(file_system(owner.clone()));
        store
            .add_object(DfsObjectMetadata {
                object_id: object_id.clone(),
                file_system_id,
                path: "src/lib.rs".to_string(),
                kind: DfsObjectKind::File,
                owner_node_id: owner.clone(),
                current_locations: vec![owner.clone()],
                content_hash: None,
                version: DfsVersion::from_legacy_named("version_1").expect("version"),
                policy: DfsObjectPolicy::LocalOwned,
                created_by_event_id: None,
                last_modified_by_event_id: None,
            })
            .expect("add object");

        let route = store
            .route_write(
                DfsWriteRequest {
                    file_system_id: DfsFileSystemId::from_legacy_named("sifs").expect("filesystem"),
                    object_id,
                    target_version: DfsVersion::from_legacy_named("version_2").expect("version"),
                    requesting_node_id: remote.clone(),
                },
                &remote,
            )
            .expect("route write");

        assert_eq!(
            route.action,
            WriteRouteAction::RouteToOwner {
                owner_node_id: owner
            }
        );
    }

    #[test]
    fn sifs_recursive_clear_survives_metadata_backend_reopen() {
        let root = temp_root("sifs-recursive-clear-reopen");
        let node_id = NodeId::from_legacy_named("node_sifs_clear").expect("node");
        let file_path = "/services/sandbox/instances/sandbox_clear/tmp/0/session-cache.txt";
        let clear_root = "/services/sandbox/instances/sandbox_clear/tmp/0";

        {
            let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
            dfs.posix_mkdir_all(clear_root).expect("mkdir");
            dfs.posix_write_file(
                file_path,
                b"tmp",
                DfsWriteOptions::replicated_eventual(vec![node_id.clone()]),
            )
            .expect("write");
            assert_eq!(dfs.posix_read_file(file_path).expect("read"), b"tmp");
            dfs.posix_clear_dir_recursive(clear_root)
                .expect("clear recursive");
            assert!(matches!(
                dfs.posix_read_file(file_path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }

        {
            let dfs = networked_sifs_with_backend(&root, node_id.clone());
            assert!(matches!(
                dfs.posix_read_file(file_path),
                Err(DfsPosixError::NotFound { .. })
            ));
        }

        {
            let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
            dfs.posix_mkdir_all(clear_root)
                .expect("recreate cleared root");
            dfs.posix_write_file(
                file_path,
                b"fresh",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("rewrite after clear");
            assert_eq!(
                dfs.posix_read_file(file_path).expect("fresh read"),
                b"fresh"
            );
        }

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn sifs_self_only_eventual_write_uses_local_fast_path() {
        let root = temp_root("sifs-self-only-eventual-fast-path");
        let node_id = NodeId::from_legacy_named("node_sifs_fast_path").expect("node");
        let file_path = "/services/sandbox/instances/sandbox_fast/work/site.py";

        let mut dfs = networked_sifs_with_backend(&root, node_id.clone());
        dfs.posix_mkdir_all("/services/sandbox/instances/sandbox_fast/work")
            .expect("mkdir");
        let stat = dfs
            .posix_write_file(
                file_path,
                b"print('fast path')\n",
                DfsWriteOptions::replicated_eventual(vec![node_id]),
            )
            .expect("write");

        assert_eq!(stat.size_bytes, Some(19));
        assert_eq!(
            dfs.posix_read_file(file_path).expect("read"),
            b"print('fast path')\n"
        );
        assert!(
            dfs.pending_writes.is_empty(),
            "self-only eventual writes should not leave distributed intents"
        );
        assert!(dfs.coordinator.repair_jobs().is_empty());

        let journal_count = std::fs::read_dir(root.join("metadata/journal"))
            .expect("journal dir")
            .count();
        assert!(
            journal_count < 64,
            "self-only write should not emit per-file distributed intent/receipt/commit journals"
        );

        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn local_owned_read_is_routed_to_owner_node() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
        let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
        let metadata = file_metadata(owner.clone());
        let object_id = metadata.object_id.clone();

        owner_dfs.add_file_system(file_system(owner.clone()));
        remote_dfs.add_file_system(file_system(owner.clone()));
        owner_dfs
            .put_local_object(metadata.clone(), b"owned bytes".to_vec())
            .expect("seed owner object");
        remote_dfs
            .add_object_metadata(metadata)
            .expect("replicate metadata");

        let read = remote_dfs.read_object(&object_id).expect("read routes");
        let request_id = match read {
            DfsReadOutcome::Routed {
                request_id,
                owner_node_id,
            } => {
                assert_eq!(owner_node_id, owner);
                request_id
            }
            DfsReadOutcome::Local { .. } => panic!("remote read should route to owner"),
        };

        let owner_events = owner_dfs.process_incoming().expect("owner handles read");
        assert!(matches!(
            &owner_events[..],
            [DfsServiceEvent::ReadRequestHandled {
                result: DfsReadResult::Found(_),
                ..
            }]
        ));

        let remote_events = remote_dfs
            .process_incoming()
            .expect("remote receives read response");
        assert!(matches!(
            &remote_events[..],
            [DfsServiceEvent::ReadResponseReceived {
                request_id: received_request_id,
                result: DfsReadResult::Found(DfsReadData { bytes, .. }),
                ..
            }] if received_request_id == &request_id && bytes == b"owned bytes"
        ));
    }

    #[test]
    fn local_owned_write_is_routed_to_owner_node_and_committed() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let network = InProcessNetwork::new();
        let mut owner_dfs = LocalOwnedDfsService::new(network.add_node(owner.clone()));
        let mut remote_dfs = LocalOwnedDfsService::new(network.add_node(remote.clone()));
        let metadata = file_metadata(owner.clone());
        let object_id = metadata.object_id.clone();
        let file_system_id = metadata.file_system_id.clone();

        owner_dfs.add_file_system(file_system(owner.clone()));
        remote_dfs.add_file_system(file_system(owner.clone()));
        owner_dfs
            .put_local_object(metadata.clone(), b"old bytes".to_vec())
            .expect("seed owner object");
        remote_dfs
            .add_object_metadata(metadata)
            .expect("replicate metadata");

        let target_version = DfsVersion::from_legacy_named("version_2").expect("version");
        let write = remote_dfs
            .write_object(
                DfsWriteRequest {
                    file_system_id,
                    object_id: object_id.clone(),
                    target_version: target_version.clone(),
                    requesting_node_id: remote.clone(),
                },
                b"updated bytes".to_vec(),
            )
            .expect("write routes");
        let request_id = match write {
            DfsWriteOutcome::Routed {
                request_id,
                owner_node_id,
            } => {
                assert_eq!(owner_node_id, owner);
                request_id
            }
            DfsWriteOutcome::Local { .. } => panic!("remote write should route to owner"),
        };

        let owner_events = owner_dfs.process_incoming().expect("owner handles write");
        assert!(matches!(
            &owner_events[..],
            [DfsServiceEvent::WriteRequestHandled {
                result: DfsWriteResult::Committed { .. },
                ..
            }]
        ));

        let owner_read = owner_dfs
            .read_object(&object_id)
            .expect("owner reads local");
        assert!(matches!(
            owner_read,
            DfsReadOutcome::Local {
                data: Some(DfsReadData { bytes, version, .. }),
                ..
            } if bytes == b"updated bytes" && version == target_version
        ));

        let remote_events = remote_dfs
            .process_incoming()
            .expect("remote receives write response");
        assert!(matches!(
            &remote_events[..],
            [DfsServiceEvent::WriteResponseReceived {
                request_id: received_request_id,
                result: DfsWriteResult::Committed { receipt, .. },
                ..
            }] if received_request_id == &request_id
                && receipt.committed_version == target_version
                && receipt.owner_node_id == owner
        ));
        assert_eq!(
            remote_dfs
                .metadata()
                .object(&object_id)
                .expect("metadata updated")
                .version,
            target_version
        );
    }

    #[test]
    fn dfs_policy_violation_event_is_structured_for_supervision() {
        let owner = NodeId::from_legacy_named("node_desktop").expect("node");
        let remote = NodeId::from_legacy_named("node_mobile").expect("node");
        let event = DfsPolicyViolation {
            requesting_node_id: remote,
            executing_node_id: owner.clone(),
            file_system_id: Some(DfsFileSystemId::from_legacy_named("sifs").expect("filesystem")),
            object_id: Some(DfsObjectId::from_legacy_named("obj_file").expect("object")),
            reason: "write attempted against non-owner".to_string(),
        }
        .event();

        assert_eq!(event.kind.as_str(), "dfs.policy.violation");
        assert_eq!(event.target_node_id, Some(owner));
        assert_eq!(
            event.payload["file_system_id"],
            DfsFileSystemId::from_legacy_named("sifs")
                .expect("filesystem")
                .as_str()
        );
    }

    #[test]
    fn explicit_dfs_roots_are_preserved() {
        let root = PathBuf::from("/tmp/sinomo-dfs-explicit-root");

        assert_eq!(
            resolve_dfs_metadata_root(Some(root.join("metadata"))).expect("metadata root"),
            root.join("metadata")
        );
        assert_eq!(
            resolve_dfs_pin_root(Some(root.join("pins"))).expect("pin root"),
            root.join("pins")
        );
    }
}
