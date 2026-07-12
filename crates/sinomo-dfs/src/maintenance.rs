//! DFS lifecycle maintenance, anti-entropy, and repair tick orchestration.
//!
//! This module owns both the reusable maintenance host surface and the
//! cluster-aware entrypoints that open DFS services, bind transports, and run a
//! maintenance tick against configured metadata shards.

use serde::{Deserialize, Serialize};
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::ClusterPaths;
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::{
    AsyncDfsMessageTransport, AsyncDfsMetadataJournalReplicationTransport,
    AsyncDfsMetadataNodeTransport, AsyncDfsPeerTransport, ContentStoreGcOptions,
    ContentStoreGcReport, ContentStoreScrubReport, ContentStoreStagingGcOptions,
    ContentStoreStagingGcReport, ContentStoreVersionGcOptions, ContentStoreVersionGcReport,
    DFS_PENDING_TRANSFER_STALE_AFTER, DfsControlPlaneKvClient, DfsError, DfsFileSystemId,
    DfsMessageSender, DfsMessageTransport, DfsMetadataBackend, DfsMetadataJournalBatchSender,
    DfsMetadataJournalPublisherHost, DfsMetadataJournalReplicationOptions,
    DfsMetadataJournalReplicationReport, DfsMetadataJournalReplicationTransport,
    DfsMetadataNodeTransport, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataSnapshotInstallSender, DfsNamespaceJobMaintenanceReport, DfsObjectId,
    DfsObjectPageCursor, DfsPeerTransport, DfsTombstoneCompactionOptions,
    DfsTombstoneCompactionReport, DfsTransportIdentity, DfsVersion, DistributedRepairJob,
    LocalDfsMetadataJournalPublisherHost, NetworkedDfsService, NetworkedDfsServiceEvent,
    async_dfs_metadata_journal_batch_sender_from_peer_transport,
    async_dfs_metadata_snapshot_install_sender_from_peer_transport, cluster_dfs_metadata_shards,
    cluster_dfs_peer_node_ids, dfs_message_sender_from_peer_transport,
    dfs_metadata_journal_batch_sender_from_peer_transport, dfs_metadata_replication_notice_lines,
    dfs_metadata_replication_sent_count, dfs_metadata_snapshot_install_sender_from_peer_transport,
    dfs_now_unix_nanos, dfs_path_safe_segment, distributed_sifs,
    open_cluster_dfs_service_with_message_sender,
    open_cluster_dfs_service_with_peer_transports_for_nodes,
    publish_dfs_metadata_journal_with_async_host, publish_dfs_metadata_journal_with_host,
    refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport,
};

#[cfg(test)]
pub(crate) fn run_cluster_dfs_maintenance_tick(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    message_sender: DfsMessageSender,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    run_cluster_dfs_maintenance_tick_with_message_sender(
        data_dir,
        cluster_paths,
        message_sender,
        metadata_sender,
        snapshot_sender,
        control_plane,
        options,
    )
}

pub(crate) fn run_cluster_dfs_maintenance_tick_with_message_sender(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    message_sender: DfsMessageSender,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Ok(None);
    }

    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    let local_node_id = state.local_node_id.clone();
    let file_system = distributed_sifs(local_node_id.clone());
    let metadata_shards = cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?;
    let peer_node_ids = cluster_dfs_peer_node_ids(&state);
    let mut service = open_cluster_dfs_service_with_message_sender(
        data_dir,
        cluster_paths,
        local_node_id.clone(),
        peer_node_ids.clone(),
        message_sender,
        control_plane,
    )?;
    let mut maintenance_host = LocalDfsMaintenanceHost::new(data_dir.clone());
    let mut metadata_publisher_host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let mut metadata_transport =
        DfsMetadataNodeTransport::new(peer_node_ids, metadata_sender, snapshot_sender);
    run_dfs_maintenance_tick_with_host(
        &mut service,
        &mut maintenance_host,
        file_system.file_system_id,
        &local_node_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        options,
    )
    .map(Some)
}

pub async fn run_cluster_dfs_maintenance_tick_with_async_peer_transport(
    runtime: &tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Ok(None);
    }

    refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
        &cluster_paths,
        sync_peer_transport.as_ref(),
    )?;

    let state = sinomo_core::cluster::read_cluster_state_without_kv_async(runtime, &cluster_paths)
        .await
        .map_err(DfsError::from_network)?;
    let local_node_id = state.local_node_id.clone();
    let file_system = distributed_sifs(local_node_id.clone());
    let metadata_shards = cluster_dfs_metadata_shards(&cluster_paths, &file_system.file_system_id)?;
    let peer_node_ids = cluster_dfs_peer_node_ids(&state);
    let mut service = open_cluster_dfs_service_with_peer_transports_for_nodes(
        &data_dir,
        &cluster_paths,
        local_node_id.clone(),
        peer_node_ids.clone(),
        sync_peer_transport,
        Arc::clone(&peer_transport),
        control_plane,
    )?;
    let mut maintenance_host = LocalDfsMaintenanceHost::new(data_dir.clone());
    let mut metadata_publisher_host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let mut metadata_transport = AsyncDfsMetadataNodeTransport::new(
        peer_node_ids,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );
    run_dfs_maintenance_tick_with_async_host(
        &mut service,
        &mut maintenance_host,
        &data_dir,
        file_system.file_system_id,
        &local_node_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        options,
    )
    .await
    .map(Some)
}

pub fn run_cluster_dfs_maintenance_tick_with_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    run_cluster_dfs_maintenance_tick_with_message_sender(
        data_dir,
        cluster_paths,
        dfs_message_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        control_plane,
        options,
    )
}

const DFS_ANTI_ENTROPY_DEFAULT_OBJECT_PAGE_SIZE: usize = 1024;
const DFS_ANTI_ENTROPY_DEFAULT_ISSUE_LIMIT: usize = 1024;

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

pub(crate) fn record_anti_entropy_issue(
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsLifecycleMaintenanceOptions {
    #[serde(default)]
    pub scrub_content: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_staging_gc: Option<ContentStoreStagingGcOptions>,
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
    pub stale_staging_gc: Option<ContentStoreStagingGcReport>,
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

pub const DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_OBJECTS: usize = 1024;
pub const DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_ISSUES: usize = 64;
pub const DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_JOBS: usize = 16;
pub const DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_STEPS_PER_JOB: usize = 1024;
pub const DEFAULT_DFS_CONFLICT_MAINTENANCE_MAX_RECORDS: usize = 128;
pub const DEFAULT_DFS_WRITE_INTENT_MAINTENANCE_MAX_RECORDS: usize = 256;
pub const DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsAntiEntropyCursorRecord {
    pub file_system_id: DfsFileSystemId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<DfsObjectPageCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairAttemptRecord {
    pub intent_id: String,
    pub object_id: DfsObjectId,
    pub version: DfsVersion,
    pub content_hash: String,
    pub missing_replica_node_id: NodeId,
    pub attempts: u32,
    pub in_flight: bool,
    pub last_attempt_unix_nanos: u64,
    pub next_retry_unix_nanos: u64,
    /// Stable request identity for an in-flight logical transfer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsConflictMaintenanceReport {
    pub scanned_open_conflict_count: usize,
    pub resolved_conflict_count: usize,
    pub superseded_conflict_count: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsWriteIntentMaintenanceReport {
    pub scanned_active_intent_count: usize,
    pub committed_visible_intent_count: usize,
    pub aborted_missing_object_count: usize,
    pub aborted_superseded_intent_count: usize,
    pub skipped_fresh_stale_candidate_count: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsRepairAttemptSnapshot {
    #[serde(default)]
    pub records: Vec<DfsRepairAttemptRecord>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataReplicationSuppressionRecord {
    pub target_node_id: NodeId,
    pub shard_id: DfsMetadataShardId,
    pub shard_epoch: u64,
    pub reason: String,
    pub updated_at_unix_nanos: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataReplicationSuppressionSnapshot {
    #[serde(default)]
    pub records: Vec<DfsMetadataReplicationSuppressionRecord>,
}

#[derive(Clone, Debug)]
pub struct LocalDfsMaintenanceHost {
    data_dir: SinomoDataDir,
}

impl LocalDfsMaintenanceHost {
    pub fn new(data_dir: SinomoDataDir) -> Self {
        Self { data_dir }
    }

    fn cursor_path(&self, file_system_id: &DfsFileSystemId) -> PathBuf {
        default_dfs_anti_entropy_cursor_path(&self.data_dir, file_system_id)
    }

    fn repair_attempt_path(&self) -> PathBuf {
        default_dfs_repair_attempt_snapshot_path(&self.data_dir)
    }
}

impl DfsMaintenanceHost for LocalDfsMaintenanceHost {
    fn load_anti_entropy_cursor(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsAntiEntropyCursorRecord>, DfsError> {
        read_dfs_anti_entropy_cursor(&self.cursor_path(file_system_id))
    }

    fn store_anti_entropy_cursor(
        &mut self,
        cursor: &DfsAntiEntropyCursorRecord,
    ) -> Result<(), DfsError> {
        write_dfs_anti_entropy_cursor(&self.cursor_path(&cursor.file_system_id), cursor)
    }

    fn clear_anti_entropy_cursor(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<(), DfsError> {
        clear_dfs_anti_entropy_cursor(&self.cursor_path(file_system_id))
    }

    fn load_repair_attempt_records(&mut self) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        read_dfs_repair_attempt_snapshot(&self.repair_attempt_path()).map(|snapshot| {
            snapshot
                .map(|snapshot| snapshot.records)
                .unwrap_or_default()
        })
    }

    fn store_repair_attempt_records(
        &mut self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<(), DfsError> {
        write_dfs_repair_attempt_snapshot(
            &self.repair_attempt_path(),
            &DfsRepairAttemptSnapshot {
                records: records.to_vec(),
            },
        )
    }
}

pub fn default_dfs_anti_entropy_cursor_path(
    data_dir: &SinomoDataDir,
    file_system_id: &DfsFileSystemId,
) -> PathBuf {
    data_dir
        .dfs_dir()
        .join("anti-entropy-cursors")
        .join(format!(
            "{}.sobj",
            dfs_path_safe_segment(&file_system_id.to_string())
        ))
}

pub fn default_dfs_repair_attempt_snapshot_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .dfs_dir()
        .join("maintenance")
        .join("repair-attempts.sobj")
}

pub fn default_dfs_metadata_replication_suppression_snapshot_path(
    data_dir: &SinomoDataDir,
) -> PathBuf {
    data_dir
        .dfs_dir()
        .join("maintenance")
        .join("metadata-replication-suppressions.sobj")
}

pub fn read_dfs_repair_attempt_snapshot(
    path: &Path,
) -> Result<Option<DfsRepairAttemptSnapshot>, DfsError> {
    match std::fs::read(path) {
        Ok(bytes) => sinomo_object_serde::from_bytes(&bytes)
            .map(Some)
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "DFS repair attempt snapshot object error at {}: {source}",
                    path.display()
                ),
            }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS repair attempt snapshot {}: {source}",
                path.display()
            ),
        }),
    }
}

pub fn write_dfs_repair_attempt_snapshot(
    path: &Path,
    snapshot: &DfsRepairAttemptSnapshot,
) -> Result<(), DfsError> {
    if snapshot.records.is_empty() {
        return clear_dfs_cursor_file(path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| DfsError::Metadata {
            message: format!(
                "could not create DFS repair attempt snapshot directory {}: {source}",
                parent.display()
            ),
        })?;
    }
    let bytes = sinomo_object_serde::to_bytes(snapshot, 1, LengthMode::U32).map_err(|source| {
        DfsError::Serialization {
            message: format!(
                "DFS repair attempt snapshot object error at {}: {source}",
                path.display()
            ),
        }
    })?;
    std::fs::write(path, bytes).map_err(|source| DfsError::Metadata {
        message: format!(
            "could not write DFS repair attempt snapshot {}: {source}",
            path.display()
        ),
    })
}

pub fn read_dfs_metadata_replication_suppression_snapshot(
    path: &Path,
) -> Result<Option<DfsMetadataReplicationSuppressionSnapshot>, DfsError> {
    match std::fs::read(path) {
        Ok(bytes) => sinomo_object_serde::from_bytes(&bytes)
            .map(Some)
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "DFS metadata replication suppression snapshot object error at {}: {source}",
                    path.display()
                ),
            }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS metadata replication suppression snapshot {}: {source}",
                path.display()
            ),
        }),
    }
}

pub fn write_dfs_metadata_replication_suppression_snapshot(
    path: &Path,
    snapshot: &DfsMetadataReplicationSuppressionSnapshot,
) -> Result<(), DfsError> {
    if snapshot.records.is_empty() {
        return clear_dfs_cursor_file(path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| DfsError::Metadata {
            message: format!(
                "could not create DFS metadata replication suppression snapshot directory {}: {source}",
                parent.display()
            ),
        })?;
    }
    let bytes = sinomo_object_serde::to_bytes(snapshot, 1, LengthMode::U32).map_err(|source| {
        DfsError::Serialization {
            message: format!(
                "DFS metadata replication suppression snapshot object error at {}: {source}",
                path.display()
            ),
        }
    })?;
    std::fs::write(path, bytes).map_err(|source| DfsError::Metadata {
        message: format!(
            "could not write DFS metadata replication suppression snapshot {}: {source}",
            path.display()
        ),
    })
}

pub fn put_local_dfs_metadata_replication_suppression(
    data_dir: &SinomoDataDir,
    record: DfsMetadataReplicationSuppressionRecord,
) -> Result<(), DfsError> {
    let path = default_dfs_metadata_replication_suppression_snapshot_path(data_dir);
    let mut snapshot = read_dfs_metadata_replication_suppression_snapshot(&path)?
        .unwrap_or_else(DfsMetadataReplicationSuppressionSnapshot::default);
    snapshot.records.retain(|existing| {
        existing.target_node_id != record.target_node_id || existing.shard_id != record.shard_id
    });
    snapshot.records.push(record);
    snapshot.records.sort_by(|left, right| {
        left.shard_id
            .cmp(&right.shard_id)
            .then_with(|| left.target_node_id.cmp(&right.target_node_id))
    });
    write_dfs_metadata_replication_suppression_snapshot(&path, &snapshot)
}

pub fn legacy_dfs_anti_entropy_cursor_path(path: &Path) -> PathBuf {
    path.with_extension("json")
}

pub fn read_dfs_anti_entropy_cursor(
    path: &Path,
) -> Result<Option<DfsAntiEntropyCursorRecord>, DfsError> {
    match std::fs::read(path) {
        Ok(bytes) => sinomo_object_serde::from_bytes(&bytes)
            .map(Some)
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "DFS anti-entropy cursor object error at {}: {source}",
                    path.display()
                ),
            }),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            read_legacy_dfs_anti_entropy_cursor(&legacy_dfs_anti_entropy_cursor_path(path))
        }
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS anti-entropy cursor {}: {source}",
                path.display()
            ),
        }),
    }
}

fn read_legacy_dfs_anti_entropy_cursor(
    path: &Path,
) -> Result<Option<DfsAntiEntropyCursorRecord>, DfsError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|source| DfsError::Serialization {
                    message: format!(
                        "DFS anti-entropy legacy cursor JSON error at {}: {source}",
                        path.display()
                    ),
                })
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS anti-entropy legacy cursor {}: {source}",
                path.display()
            ),
        }),
    }
}

pub fn write_dfs_anti_entropy_cursor(
    path: &Path,
    cursor: &DfsAntiEntropyCursorRecord,
) -> Result<(), DfsError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| DfsError::Metadata {
            message: format!(
                "could not create DFS anti-entropy cursor directory {}: {source}",
                parent.display()
            ),
        })?;
    }
    let bytes = sinomo_object_serde::to_bytes(cursor, 1, LengthMode::U32).map_err(|source| {
        DfsError::Serialization {
            message: format!(
                "DFS anti-entropy cursor object error at {}: {source}",
                path.display()
            ),
        }
    })?;
    std::fs::write(path, bytes).map_err(|source| DfsError::Metadata {
        message: format!(
            "could not write DFS anti-entropy cursor {}: {source}",
            path.display()
        ),
    })?;
    clear_dfs_cursor_file(&legacy_dfs_anti_entropy_cursor_path(path))
}

pub fn clear_dfs_anti_entropy_cursor(path: &Path) -> Result<(), DfsError> {
    clear_dfs_cursor_file(path)?;
    clear_dfs_cursor_file(&legacy_dfs_anti_entropy_cursor_path(path))
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LegacyDfsMetadataReplicationCursor {
    pub target_node_id: NodeId,
    pub shard_id: DfsMetadataShardId,
    pub next_after_sequence: u64,
}

pub fn migrate_legacy_dfs_metadata_replication_cursor(
    backend: &mut dyn DfsMetadataBackend,
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
) -> Result<u64, DfsError> {
    let mut after_sequence = backend.metadata_replication_cursor(target_node_id, shard_id)?;
    let legacy_path =
        legacy_dfs_metadata_replication_cursor_path(data_dir, target_node_id, shard_id);
    let Some(legacy_cursor) = read_legacy_dfs_metadata_replication_cursor(&legacy_path)? else {
        return Ok(after_sequence);
    };
    if legacy_cursor.target_node_id != *target_node_id || legacy_cursor.shard_id != *shard_id {
        return Err(DfsError::Metadata {
            message: format!(
                "legacy DFS metadata replication cursor {} does not match target {target_node_id} shard {}",
                legacy_path.display(),
                shard_id.0,
            ),
        });
    }
    let legacy_after_sequence = legacy_cursor.next_after_sequence;
    if legacy_after_sequence > after_sequence {
        backend.put_metadata_replication_cursor(target_node_id, shard_id, legacy_after_sequence)?;
        after_sequence = legacy_after_sequence;
    }
    clear_dfs_cursor_file(&legacy_path)?;
    Ok(after_sequence)
}

pub fn legacy_dfs_metadata_replication_cursor_path(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
) -> PathBuf {
    data_dir
        .dfs_dir()
        .join("metadata-replication-cursors")
        .join(dfs_path_safe_segment(&target_node_id.to_string()))
        .join(format!("{}.json", dfs_path_safe_segment(&shard_id.0)))
}

fn read_legacy_dfs_metadata_replication_cursor(
    path: &Path,
) -> Result<Option<LegacyDfsMetadataReplicationCursor>, DfsError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|source| DfsError::Serialization {
                    message: format!(
                        "DFS metadata replication legacy cursor JSON error at {}: {source}",
                        path.display()
                    ),
                })
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS metadata replication legacy cursor {}: {source}",
                path.display()
            ),
        }),
    }
}

#[cfg(test)]
pub(crate) fn write_legacy_dfs_metadata_replication_cursor(
    path: &Path,
    cursor: &LegacyDfsMetadataReplicationCursor,
) -> Result<(), DfsError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| DfsError::Metadata {
            message: format!(
                "could not create DFS metadata replication legacy cursor directory {}: {source}",
                parent.display()
            ),
        })?;
    }
    let bytes = serde_json::to_vec_pretty(cursor).map_err(|source| DfsError::Serialization {
        message: format!(
            "DFS metadata replication legacy cursor JSON error at {}: {source}",
            path.display()
        ),
    })?;
    std::fs::write(path, bytes).map_err(|source| DfsError::Metadata {
        message: format!(
            "could not write DFS metadata replication legacy cursor {}: {source}",
            path.display()
        ),
    })
}

fn clear_dfs_cursor_file(path: &Path) -> Result<(), DfsError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(DfsError::Metadata {
            message: format!("could not remove DFS cursor {}: {source}", path.display()),
        }),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMaintenanceRunOptions {
    pub anti_entropy_max_objects: usize,
    pub anti_entropy_max_issues: usize,
    pub repair_metadata: bool,
    pub cleanup_stale_staging: bool,
    pub stale_staging_min_age: Duration,
}

impl Default for DfsMaintenanceRunOptions {
    fn default() -> Self {
        Self {
            anti_entropy_max_objects: DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_OBJECTS,
            anti_entropy_max_issues: DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_ISSUES,
            repair_metadata: true,
            cleanup_stale_staging: true,
            stale_staging_min_age: DFS_PENDING_TRANSFER_STALE_AFTER,
        }
    }
}

pub trait DfsMaintenanceHost: Send {
    fn load_anti_entropy_cursor(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<Option<DfsAntiEntropyCursorRecord>, DfsError>;

    fn store_anti_entropy_cursor(
        &mut self,
        cursor: &DfsAntiEntropyCursorRecord,
    ) -> Result<(), DfsError>;

    fn clear_anti_entropy_cursor(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<(), DfsError>;

    fn load_repair_attempt_records(&mut self) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        Ok(Vec::new())
    }

    fn store_repair_attempt_records(
        &mut self,
        _records: &[DfsRepairAttemptRecord],
    ) -> Result<(), DfsError> {
        Ok(())
    }
}

pub fn run_dfs_lifecycle_maintenance_with_host<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    host: &mut dyn DfsMaintenanceHost,
    options: DfsMaintenanceRunOptions,
) -> Result<Vec<DfsLifecycleMaintenanceReport>, DfsError> {
    let Some(metadata_backend) = service.metadata_backend() else {
        return Ok(Vec::new());
    };
    let file_systems = metadata_backend.list_file_systems()?;
    let mut reports = Vec::with_capacity(file_systems.len());
    let mut stale_staging_gc = options
        .cleanup_stale_staging
        .then(|| ContentStoreStagingGcOptions::delete_eligible(options.stale_staging_min_age));

    for file_system in file_systems {
        let saved_cursor = host.load_anti_entropy_cursor(&file_system.file_system_id)?;
        if let Some(saved_cursor) = saved_cursor.as_ref()
            && saved_cursor.file_system_id != file_system.file_system_id
        {
            return Err(DfsError::Metadata {
                message: format!(
                    "DFS anti-entropy cursor for {} does not match file system {}",
                    saved_cursor.file_system_id, file_system.file_system_id,
                ),
            });
        }
        let after = saved_cursor.and_then(|cursor| cursor.next_cursor);
        let report = service.run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
            stale_staging_gc: stale_staging_gc.take(),
            anti_entropy: Some(DfsAntiEntropyOptions {
                file_system_id: file_system.file_system_id.clone(),
                after,
                max_objects: options.anti_entropy_max_objects,
                max_issues: options.anti_entropy_max_issues,
                repair_metadata: options.repair_metadata,
            }),
            ..DfsLifecycleMaintenanceOptions::default()
        })?;

        if let Some(anti_entropy) = report.anti_entropy.as_ref() {
            if let Some(next_cursor) = anti_entropy.next_cursor.clone() {
                host.store_anti_entropy_cursor(&DfsAntiEntropyCursorRecord {
                    file_system_id: file_system.file_system_id.clone(),
                    next_cursor: Some(next_cursor),
                })?;
            } else {
                host.clear_anti_entropy_cursor(&file_system.file_system_id)?;
            }
        }
        reports.push(report);
    }

    Ok(reports)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMaintenanceTickOptions {
    pub namespace_job_max_jobs: usize,
    pub namespace_job_max_steps_per_job: usize,
    pub conflict_max_records: usize,
    pub write_intent_max_records: usize,
    pub stale_write_intent_min_age: Duration,
    pub repair_max_jobs: usize,
    pub lifecycle: DfsMaintenanceRunOptions,
    pub metadata_replication: DfsMetadataJournalReplicationOptions,
}

impl Default for DfsMaintenanceTickOptions {
    fn default() -> Self {
        Self {
            namespace_job_max_jobs: DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_JOBS,
            namespace_job_max_steps_per_job:
                DEFAULT_DFS_NAMESPACE_JOB_MAINTENANCE_MAX_STEPS_PER_JOB,
            conflict_max_records: DEFAULT_DFS_CONFLICT_MAINTENANCE_MAX_RECORDS,
            write_intent_max_records: DEFAULT_DFS_WRITE_INTENT_MAINTENANCE_MAX_RECORDS,
            stale_write_intent_min_age: DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE,
            repair_max_jobs: usize::MAX,
            lifecycle: DfsMaintenanceRunOptions::default(),
            metadata_replication: DfsMetadataJournalReplicationOptions::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMaintenanceTickReport {
    pub namespace_jobs: DfsNamespaceJobMaintenanceReport,
    pub conflicts: DfsConflictMaintenanceReport,
    pub write_intents: DfsWriteIntentMaintenanceReport,
    pub lifecycle_reports: Vec<DfsLifecycleMaintenanceReport>,
    pub scheduled_repairs: Vec<DistributedRepairJob>,
    pub followup_events: Vec<NetworkedDfsServiceEvent>,
    pub repair_attempts_before: Vec<DfsRepairAttemptRecord>,
    pub repair_attempts_after: Vec<DfsRepairAttemptRecord>,
    pub metadata_replication: DfsMetadataJournalReplicationReport,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsMaintenanceTickObservations {
    pub metadata_replications_sent: usize,
    pub repair_attempt_count: usize,
    pub repair_attempt_in_flight_count: usize,
    pub repair_attempt_backoff_count: usize,
    pub repair_attempt_suppressed_count: usize,
    pub notice_lines: Vec<String>,
}

pub fn dfs_maintenance_tick_observations(
    report: &DfsMaintenanceTickReport,
) -> DfsMaintenanceTickObservations {
    let mut notice_lines = dfs_namespace_job_maintenance_notice_lines(&report.namespace_jobs);
    notice_lines.extend(dfs_conflict_maintenance_notice_lines(&report.conflicts));
    notice_lines.extend(dfs_write_intent_maintenance_notice_lines(
        &report.write_intents,
    ));
    notice_lines.extend(dfs_lifecycle_maintenance_notice_lines(
        &report.lifecycle_reports,
    ));
    notice_lines.extend(dfs_repair_maintenance_notice_lines(
        &report.scheduled_repairs,
        &report.followup_events,
        &report.repair_attempts_after,
    ));
    notice_lines.extend(dfs_metadata_replication_notice_lines(
        &report.metadata_replication,
    ));
    let repair_attempt_summary =
        dfs_repair_attempt_observation_counts(&report.repair_attempts_after);

    DfsMaintenanceTickObservations {
        metadata_replications_sent: dfs_metadata_replication_sent_count(
            &report.metadata_replication,
        ),
        repair_attempt_count: report.repair_attempts_after.len(),
        repair_attempt_in_flight_count: repair_attempt_summary.in_flight,
        repair_attempt_backoff_count: repair_attempt_summary.backoff,
        repair_attempt_suppressed_count: repair_attempt_summary.suppressed,
        notice_lines,
    }
}

pub fn dfs_conflict_maintenance_notice_lines(report: &DfsConflictMaintenanceReport) -> Vec<String> {
    if report.resolved_conflict_count == 0 && report.superseded_conflict_count == 0 {
        return Vec::new();
    }
    vec![format!(
        "DFS conflicts: scanned_open={}, resolved={}, superseded={}",
        report.scanned_open_conflict_count,
        report.resolved_conflict_count,
        report.superseded_conflict_count
    )]
}

pub fn dfs_write_intent_maintenance_notice_lines(
    report: &DfsWriteIntentMaintenanceReport,
) -> Vec<String> {
    if report.committed_visible_intent_count == 0
        && report.aborted_missing_object_count == 0
        && report.aborted_superseded_intent_count == 0
        && report.skipped_fresh_stale_candidate_count == 0
    {
        return Vec::new();
    }
    vec![format!(
        "DFS write intents: scanned_active={}, committed_visible={}, aborted_missing_object={}, aborted_superseded={}, skipped_fresh={}",
        report.scanned_active_intent_count,
        report.committed_visible_intent_count,
        report.aborted_missing_object_count,
        report.aborted_superseded_intent_count,
        report.skipped_fresh_stale_candidate_count
    )]
}

pub fn dfs_namespace_job_maintenance_notice_lines(
    report: &DfsNamespaceJobMaintenanceReport,
) -> Vec<String> {
    if report.resumed_job_count == 0 && report.failed_job_count == 0 {
        return Vec::new();
    }

    let mut lines = vec![format!(
        "DFS namespace jobs: scanned={}, resumed={}, completed={}, failed={}, remaining={}",
        report.scanned_job_count,
        report.resumed_job_count,
        report.completed_job_count,
        report.failed_job_count,
        report.remaining_running_job_count
    )];
    lines.extend(report.failures.iter().map(|failure| {
        format!(
            "DFS namespace job {} ({:?}) failed: {}",
            failure.job_id, failure.kind, failure.reason
        )
    }));
    lines
}

pub fn dfs_lifecycle_maintenance_notice_lines(
    reports: &[DfsLifecycleMaintenanceReport],
) -> Vec<String> {
    let mut lines = Vec::new();
    for report in reports {
        if let Some(staging_gc) = report.stale_staging_gc.as_ref()
            && (staging_gc.deleted_staging_count > 0 || staging_gc.max_deletions_reached)
        {
            lines.push(format!(
                "DFS staging GC: scanned={} eligible={} deleted={} bytes_deleted={} dry_run={} max_deletions_reached={}",
                staging_gc.scanned_staging_count,
                staging_gc.eligible_staging_count,
                staging_gc.deleted_staging_count,
                staging_gc.deleted_staging_bytes,
                staging_gc.dry_run,
                staging_gc.max_deletions_reached
            ));
        }
        let Some(anti_entropy) = report.anti_entropy.as_ref() else {
            continue;
        };
        if anti_entropy.stale_local_location_count == 0
            && anti_entropy.repaired_location_count == 0
            && anti_entropy.issue_count == 0
            && !anti_entropy.max_issues_reached
        {
            continue;
        }
        lines.push(format!(
            "DFS anti-entropy {}: scanned={}, checked_local={}, stale_local={}, repaired={}, issues={}, next_cursor={}",
            anti_entropy.file_system_id,
            anti_entropy.scanned_object_count,
            anti_entropy.checked_local_location_count,
            anti_entropy.stale_local_location_count,
            anti_entropy.repaired_location_count,
            anti_entropy.issue_count,
            anti_entropy.next_cursor.is_some()
        ));
        lines.extend(anti_entropy.issues.iter().map(|issue| {
            format!(
                "DFS anti-entropy issue {} {} {}: {} (repaired={})",
                issue.object_id, issue.version, issue.path, issue.reason, issue.repaired
            )
        }));
        if anti_entropy.max_issues_reached {
            lines.push(format!(
                "DFS anti-entropy {} stopped after reaching the issue limit",
                anti_entropy.file_system_id
            ));
        }
    }
    lines
}

pub fn dfs_repair_maintenance_notice_lines(
    scheduled_repairs: &[DistributedRepairJob],
    followup_events: &[NetworkedDfsServiceEvent],
    repair_attempts: &[DfsRepairAttemptRecord],
) -> Vec<String> {
    let repair_followups = followup_events
        .iter()
        .filter(|event| {
            matches!(
                event,
                NetworkedDfsServiceEvent::ReplicaRepaired { .. }
                    | NetworkedDfsServiceEvent::RepairRejected { .. }
                    | NetworkedDfsServiceEvent::RepairRetryFailed { .. }
                    | NetworkedDfsServiceEvent::BlobFetched { .. }
                    | NetworkedDfsServiceEvent::BlobFetchRejected { .. }
            )
        })
        .count();
    if scheduled_repairs.is_empty() && repair_followups == 0 && repair_attempts.is_empty() {
        return Vec::new();
    }

    let mut lines = vec![format!(
        "DFS repairs: scheduled={}, followups={}",
        scheduled_repairs.len(),
        repair_followups
    )];
    if !repair_attempts.is_empty() {
        let summary = dfs_repair_attempt_observation_counts(repair_attempts);
        let next_retry = summary
            .next_retry_unix_nanos
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string());
        lines.push(format!(
            "DFS repair attempts: active={}, in_flight={}, due={}, backoff={}, suppressed={}, next_retry_unix_nanos={}",
            repair_attempts.len(),
            summary.in_flight,
            summary.due,
            summary.backoff,
            summary.suppressed,
            next_retry
        ));
    }
    lines.extend(scheduled_repairs.iter().map(|job| {
        format!(
            "DFS repair scheduled {} {} {} -> {}",
            job.intent_id, job.object_id, job.version, job.missing_replica_node_id
        )
    }));
    lines.extend(followup_events.iter().filter_map(|event| match event {
        NetworkedDfsServiceEvent::ReplicaRepaired {
            intent_id,
            object_id,
            version,
            replica_node_id,
            ..
        } => Some(format!(
            "DFS repair completed {intent_id} {object_id} {version} on {replica_node_id}"
        )),
        NetworkedDfsServiceEvent::RepairRejected {
            intent_id,
            object_id,
            replica_node_id,
            reason,
        } => Some(format!(
            "DFS repair rejected {intent_id} {object_id} by {replica_node_id}: {reason}"
        )),
        NetworkedDfsServiceEvent::RepairRetryFailed { reason } => {
            Some(format!("DFS repair retry failed: {reason}"))
        }
        NetworkedDfsServiceEvent::BlobFetched {
            intent_id,
            object_id,
            version,
            source_node_id,
            ..
        } => Some(format!(
            "DFS repair fetched {intent_id} {object_id} {version} from {source_node_id}"
        )),
        NetworkedDfsServiceEvent::BlobFetchRejected {
            intent_id,
            object_id,
            source_node_id,
            reason,
        } => Some(format!(
            "DFS repair fetch rejected {intent_id} {object_id} from {source_node_id}: {reason}"
        )),
        _ => None,
    }));
    lines
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DfsRepairAttemptObservationCounts {
    in_flight: usize,
    due: usize,
    backoff: usize,
    suppressed: usize,
    next_retry_unix_nanos: Option<u64>,
}

fn dfs_repair_attempt_observation_counts(
    records: &[DfsRepairAttemptRecord],
) -> DfsRepairAttemptObservationCounts {
    let now = dfs_now_unix_nanos();
    let mut counts = DfsRepairAttemptObservationCounts::default();
    for record in records {
        if record.in_flight {
            counts.in_flight += 1;
            continue;
        }
        if record.next_retry_unix_nanos == u64::MAX {
            counts.suppressed += 1;
            continue;
        }
        if record.next_retry_unix_nanos <= now {
            counts.due += 1;
        } else {
            counts.backoff += 1;
            counts.next_retry_unix_nanos = Some(
                counts
                    .next_retry_unix_nanos
                    .map(|current| current.min(record.next_retry_unix_nanos))
                    .unwrap_or(record.next_retry_unix_nanos),
            );
        }
    }
    counts
}

pub fn run_dfs_maintenance_tick_with_host<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    maintenance_host: &mut dyn DfsMaintenanceHost,
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    metadata_shards: Vec<DfsMetadataShardConfig>,
    metadata_publisher_host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMaintenanceTickOptions,
) -> Result<DfsMaintenanceTickReport, DfsError> {
    let namespace_jobs = service
        .posix_resume_namespace_jobs_bounded(
            options.namespace_job_max_jobs,
            options.namespace_job_max_steps_per_job,
        )
        .map_err(|error| DfsError::Metadata {
            message: error.to_string(),
        })?;
    let conflicts = service.reconcile_stale_conflicts_bounded(options.conflict_max_records)?;
    let write_intents = service.reconcile_stale_write_intents_bounded(
        options.write_intent_max_records,
        options.stale_write_intent_min_age,
    )?;
    let lifecycle_reports =
        run_dfs_lifecycle_maintenance_with_host(service, maintenance_host, options.lifecycle)?;
    let repair_attempts_before = maintenance_host.load_repair_attempt_records()?;
    let scheduled_repairs = service.retry_pending_repairs_bounded_with_attempts(
        options.repair_max_jobs,
        &repair_attempts_before,
    )?;
    let followup_events = service.process_incoming_until_quiescent()?;
    service.observe_repair_maintenance_events(&followup_events);
    let repair_attempts_after = service.active_repair_attempt_records();
    maintenance_host.store_repair_attempt_records(&repair_attempts_after)?;
    let metadata_replication = publish_dfs_metadata_journal_with_host(
        file_system_id,
        local_node_id,
        metadata_shards,
        metadata_publisher_host,
        metadata_transport,
        options.metadata_replication,
    )?;

    Ok(DfsMaintenanceTickReport {
        namespace_jobs,
        conflicts,
        write_intents,
        lifecycle_reports,
        scheduled_repairs,
        followup_events,
        repair_attempts_before,
        repair_attempts_after,
        metadata_replication,
    })
}

pub async fn run_dfs_maintenance_tick_with_async_host<T>(
    service: &mut NetworkedDfsService<T>,
    maintenance_host: &mut dyn DfsMaintenanceHost,
    data_dir: &SinomoDataDir,
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    metadata_shards: Vec<DfsMetadataShardConfig>,
    metadata_publisher_host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMaintenanceTickOptions,
) -> Result<DfsMaintenanceTickReport, DfsError>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    let namespace_jobs = service
        .posix_resume_namespace_jobs_bounded(
            options.namespace_job_max_jobs,
            options.namespace_job_max_steps_per_job,
        )
        .map_err(|error| DfsError::Metadata {
            message: error.to_string(),
        })?;
    let conflicts = service.reconcile_stale_conflicts_bounded(options.conflict_max_records)?;
    let write_intents = service.reconcile_stale_write_intents_bounded(
        options.write_intent_max_records,
        options.stale_write_intent_min_age,
    )?;
    let lifecycle_reports =
        run_dfs_lifecycle_maintenance_with_host(service, maintenance_host, options.lifecycle)?;
    let repair_attempts_before = maintenance_host.load_repair_attempt_records()?;
    let scheduled_repairs = service
        .retry_pending_repairs_async_bounded_with_attempts(
            options.repair_max_jobs,
            &repair_attempts_before,
        )
        .await?;
    let followup_events = service.process_incoming_async_until_quiescent().await?;
    service.observe_repair_maintenance_events(&followup_events);
    let repair_attempts_after = service.active_repair_attempt_records();
    maintenance_host.store_repair_attempt_records(&repair_attempts_after)?;
    let metadata_replication = publish_dfs_metadata_journal_with_async_host(
        file_system_id,
        local_node_id,
        metadata_shards,
        data_dir,
        metadata_publisher_host,
        metadata_transport,
        options.metadata_replication,
    )
    .await?;

    Ok(DfsMaintenanceTickReport {
        namespace_jobs,
        conflicts,
        write_intents,
        lifecycle_reports,
        scheduled_repairs,
        followup_events,
        repair_attempts_before,
        repair_attempts_after,
        metadata_replication,
    })
}
