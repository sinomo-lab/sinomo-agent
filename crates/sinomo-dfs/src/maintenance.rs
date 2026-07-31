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
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::{
    AsyncDfsMessageTransport, AsyncDfsMetadataJournalReplicationTransport,
    AsyncDfsMetadataNodeTransport, AsyncDfsPeerTransport, ContentStoreGcOptions,
    ContentStoreGcReport, ContentStoreReservationReconciliationOptions,
    ContentStoreReservationReconciliationReport, ContentStoreScrubReport,
    ContentStoreStagingGcOptions, ContentStoreStagingGcReport, ContentStoreVersionGcOptions,
    ContentStoreVersionGcReport, DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES,
    DFS_PENDING_TRANSFER_STALE_AFTER, DfsCommittedWorkTracker,
    DfsContentRootCatalogCollectionValidation, DfsContentRootCatalogIncompleteReason,
    DfsControlPlaneKvClient, DfsDistributedWriteResult, DfsError, DfsFileSystemConfig,
    DfsFileSystemId, DfsMessageSender, DfsMessageTransport, DfsMetadataBackend,
    DfsMetadataHistoryGcOptions, DfsMetadataHistoryGcReport, DfsMetadataJournalBatchSender,
    DfsMetadataJournalPublisherHost, DfsMetadataJournalReplicationOptions,
    DfsMetadataJournalReplicationReport, DfsMetadataJournalReplicationTransport,
    DfsMetadataNodeTransport, DfsMetadataShardConfig, DfsMetadataShardHeadWatermark,
    DfsMetadataShardId, DfsMetadataShardMembershipHandoff, DfsMetadataSnapshotInstallSender,
    DfsNamespaceJobMaintenanceReport, DfsNodeDetachedSendState, DfsObjectId, DfsObjectPageCursor,
    DfsPeerTransport, DfsPlacementReconciliationJob, DfsTombstoneCompactionOptions,
    DfsTombstoneCompactionReport, DfsTransportIdentity, DfsVersion, DistributedRepairJob,
    LocalDfsMetadataJournalPublisherHost, NetworkedDfsService, NetworkedDfsServiceEvent,
    NetworkedDfsWriteOutcome, async_dfs_metadata_journal_batch_sender_from_peer_transport,
    async_dfs_metadata_snapshot_install_sender_from_peer_transport,
    cluster_dfs_all_file_system_configs, cluster_dfs_all_metadata_shard_head_watermarks,
    cluster_dfs_all_metadata_shard_membership_handoffs, cluster_dfs_all_metadata_shards,
    cluster_dfs_metadata_shards, cluster_dfs_peer_node_ids, default_dfs_content_root_catalog_store,
    dfs_message_sender_from_peer_transport, dfs_metadata_journal_batch_sender_from_peer_transport,
    dfs_metadata_replication_notice_lines, dfs_metadata_replication_sent_count,
    dfs_metadata_snapshot_install_sender_from_peer_transport, dfs_now_unix_nanos,
    dfs_path_safe_segment, distributed_sifs, open_cluster_dfs_service_with_message_sender,
    open_cluster_dfs_service_with_peer_transports_for_nodes,
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state,
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state_until_cancelled,
    publish_dfs_metadata_journal_with_host,
    refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport,
};

/// Returns true when an authority-side message commit created durable DFS work
/// which must wake the reactive maintenance classifier.
///
/// A routed write requester observes `RemoteWriteCompleted`, but only the
/// metadata authority serving `RemoteWriteServed` owns the pending intent or
/// repair jobs. Keeping that distinction here prevents requester-side traffic
/// from waking broad maintenance while guaranteeing that asynchronous W=1
/// commits cannot park until an unrelated notification or safety reprobe.
pub fn dfs_events_should_wake_repair_maintenance(events: &[NetworkedDfsServiceEvent]) -> bool {
    events.iter().any(|event| {
        let NetworkedDfsServiceEvent::RemoteWriteServed {
            result: DfsDistributedWriteResult::Outcome { outcome },
            ..
        } = event
        else {
            return false;
        };
        match outcome {
            NetworkedDfsWriteOutcome::Pending { .. } => true,
            NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } => !repair_jobs.is_empty(),
            NetworkedDfsWriteOutcome::Conflict { .. } => false,
        }
    })
}

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
    let content_lifecycle = options.lifecycle;
    let mut metadata_options = options;
    metadata_options.lifecycle = dfs_metadata_lifecycle_options(metadata_options.lifecycle);
    let mut report = run_dfs_maintenance_tick_with_host(
        &mut service,
        &mut maintenance_host,
        file_system.file_system_id,
        &local_node_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        metadata_options,
    )?;
    if dfs_content_gc_requested(content_lifecycle) {
        let expected_file_systems = cluster_dfs_all_file_system_configs(cluster_paths)?;
        let expected_shards = cluster_dfs_all_metadata_shards(cluster_paths)?;
        let pending_handoffs = cluster_dfs_all_metadata_shard_membership_handoffs(cluster_paths)?;
        let head_watermarks = cluster_dfs_all_metadata_shard_head_watermarks(cluster_paths)?;
        if let Some(content_gc) = run_dfs_content_gc_with_catalog(
            &mut service,
            &data_dir.dfs_dir().join("content"),
            &expected_file_systems,
            &expected_shards,
            &pending_handoffs,
            &head_watermarks,
            content_lifecycle,
        )? {
            report.lifecycle_reports.push(content_gc);
        }
    }
    Ok(Some(report))
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
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_optional_detached_state(
        runtime,
        data_dir,
        cluster_paths,
        sync_peer_transport,
        peer_transport,
        None,
        DfsMaintenanceExecutionExclusions::default(),
        None,
        None,
        control_plane,
        options,
    )
    .await
}

/// Run cluster DFS maintenance while rebuilding durable post-response work
/// into a caller-owned detached outbox.
///
/// The normal service hydrate and promotion recovery share one backend scan;
/// the supplied state may also be held by served-message lanes, allowing the
/// maintenance worker to reconstruct crash-window work without moving full
/// hydration or replica RPC latency onto their hot path.
#[allow(clippy::too_many_arguments)]
pub async fn run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state(
    runtime: &tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions(
        runtime,
        data_dir,
        cluster_paths,
        sync_peer_transport,
        peer_transport,
        detached_send_state,
        DfsMaintenanceExecutionExclusions::default(),
        control_plane,
        options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions(
    runtime: &tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    execution_exclusions: DfsMaintenanceExecutionExclusions,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_optional_detached_state(
        runtime,
        data_dir,
        cluster_paths,
        sync_peer_transport,
        peer_transport,
        Some(detached_send_state),
        execution_exclusions,
        None,
        None,
        control_plane,
        options,
    )
    .await
}

/// Run cancellable cluster DFS maintenance while publishing every durable
/// follow-up edge into caller-retained state.
///
/// The tracker outlives the backend-bearing service generation. A caller can
/// therefore transfer committed work after success, error, or cancellation
/// without making broad maintenance compete with retained publisher lanes.
#[allow(clippy::too_many_arguments)]
pub async fn run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions_and_committed_work_tracker(
    runtime: &tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    execution_exclusions: DfsMaintenanceExecutionExclusions,
    committed_work_tracker: DfsCommittedWorkTracker,
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_optional_detached_state(
        runtime,
        data_dir,
        cluster_paths,
        sync_peer_transport,
        peer_transport,
        Some(detached_send_state),
        execution_exclusions,
        Some(committed_work_tracker),
        None,
        control_plane,
        options,
    )
    .await
}

/// Run cluster DFS maintenance with cooperative cancellation during the
/// synchronous recovery hydrate which precedes the async maintenance phases.
///
/// Cancellation leaves the partially hydrated service owned by this call and
/// destroys it before returning. Durable recovery mutations are replay-safe;
/// the caller-retained committed-work tracker preserves any follow-up edges.
#[allow(clippy::too_many_arguments)]
pub async fn run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_detached_state_and_execution_exclusions_and_committed_work_tracker_until_hydration_cancelled(
    runtime: &tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: DfsNodeDetachedSendState,
    execution_exclusions: DfsMaintenanceExecutionExclusions,
    committed_work_tracker: DfsCommittedWorkTracker,
    should_cancel_hydration: &(dyn Fn() -> bool + Sync),
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError> {
    run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_optional_detached_state(
        runtime,
        data_dir,
        cluster_paths,
        sync_peer_transport,
        peer_transport,
        Some(detached_send_state),
        execution_exclusions,
        Some(committed_work_tracker),
        Some(should_cancel_hydration),
        control_plane,
        options,
    )
    .await
}

/// Refresh cluster KV for maintenance and return the exact resulting bootstrap
/// state.
///
/// A reusable backend must be selected only after this refresh so topology
/// changes observed while maintenance waited for admission cannot run against
/// a stale service generation.
pub async fn refresh_cluster_dfs_maintenance_state_with_peer_transport(
    runtime: &tokio::runtime::Handle,
    cluster_paths: &ClusterPaths,
    peer_transport: &dyn DfsPeerTransport,
) -> Result<sinomo_core::ClusterBootstrapState, DfsError> {
    refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
        cluster_paths,
        peer_transport,
    )?;
    sinomo_core::cluster::read_cluster_state_without_kv_async(runtime, cluster_paths)
        .await
        .map_err(DfsError::from_network)
}

/// Run one cluster maintenance tick against a reusable service after the
/// caller refreshed cluster KV and captured the returned bootstrap state.
///
/// Keeping refresh and execution as distinct steps lets the daemon validate
/// the cached backend generation against the exact post-refresh authority
/// image before it transfers ownership into this cancellable future.
#[allow(clippy::too_many_arguments)]
pub async fn run_cluster_dfs_maintenance_tick_with_reusable_service_until_hydration_cancelled<T>(
    service: &mut NetworkedDfsService<T>,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    state: sinomo_core::ClusterBootstrapState,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    execution_exclusions: DfsMaintenanceExecutionExclusions,
    committed_work_tracker: DfsCommittedWorkTracker,
    should_cancel_hydration: &(dyn Fn() -> bool + Sync),
    options: DfsMaintenanceTickOptions,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Ok(None);
    }

    service.refresh_control_plane_latest()?;
    let local_node_id = state.local_node_id.clone();
    let file_system = distributed_sifs(local_node_id.clone());
    let metadata_shards = cluster_dfs_metadata_shards(&cluster_paths, &file_system.file_system_id)?;
    let peer_node_ids = cluster_dfs_peer_node_ids(&state);

    service.set_committed_work_tracker(committed_work_tracker);
    if !service.hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(
        should_cancel_hydration,
    )? {
        return Ok(None);
    }

    let mut maintenance_host = LocalDfsMaintenanceHost::new(data_dir.clone());
    let mut metadata_publisher_host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let mut metadata_transport = AsyncDfsMetadataNodeTransport::new(
        peer_node_ids,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );
    run_dfs_maintenance_tick_with_async_host_and_execution_exclusions(
        service,
        &mut maintenance_host,
        &data_dir,
        file_system.file_system_id,
        &local_node_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        &execution_exclusions,
        options,
    )
    .await
    .map(Some)
}

/// Run bounded, queue-driven cluster maintenance against an already-open
/// daemon service lane without reconstructing the complete DFS projection.
///
/// This is the normal production healing path. It consumes durable namespace,
/// placement, handoff, and repair queues plus the service lane's exact
/// foreground state. Full metadata/content hydration is intentionally reserved
/// for an explicitly requested disaster-recovery pass.
#[allow(clippy::too_many_arguments)]
pub async fn run_cluster_dfs_reactive_maintenance_tick_with_reusable_service<T>(
    service: &mut NetworkedDfsService<T>,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    state: sinomo_core::ClusterBootstrapState,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    execution_exclusions: DfsMaintenanceExecutionExclusions,
    committed_work_tracker: DfsCommittedWorkTracker,
    options: DfsMaintenanceTickOptions,
) -> Result<DfsMaintenanceTickReport, DfsError>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    if !cluster_paths.state.exists() {
        return Err(DfsError::Metadata {
            message: "reactive DFS maintenance requires initialized cluster state".to_string(),
        });
    }

    service.refresh_control_plane_latest()?;
    service.set_committed_work_tracker(committed_work_tracker);
    let local_node_id = state.local_node_id.clone();
    let file_system = distributed_sifs(local_node_id.clone());
    let metadata_shards = cluster_dfs_metadata_shards(&cluster_paths, &file_system.file_system_id)?;
    let peer_node_ids = cluster_dfs_peer_node_ids(&state);
    let mut maintenance_host = LocalDfsMaintenanceHost::new(data_dir.clone());
    let mut metadata_publisher_host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let mut metadata_transport = AsyncDfsMetadataNodeTransport::new(
        peer_node_ids,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );

    run_dfs_maintenance_tick_with_async_host_and_execution_exclusions(
        service,
        &mut maintenance_host,
        &data_dir,
        file_system.file_system_id,
        &local_node_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        &execution_exclusions,
        options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_cluster_dfs_maintenance_tick_with_async_peer_transport_and_optional_detached_state(
    runtime: &tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    detached_send_state: Option<DfsNodeDetachedSendState>,
    execution_exclusions: DfsMaintenanceExecutionExclusions,
    committed_work_tracker: Option<DfsCommittedWorkTracker>,
    should_cancel_hydration: Option<&(dyn Fn() -> bool + Sync)>,
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
    let mut service = match (detached_send_state, should_cancel_hydration) {
        (Some(detached_send_state), Some(should_cancel_hydration)) => {
            let Some(service) =
                open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state_until_cancelled(
                    &data_dir,
                    &cluster_paths,
                    local_node_id.clone(),
                    peer_node_ids.clone(),
                    sync_peer_transport,
                    Arc::clone(&peer_transport),
                    detached_send_state,
                    control_plane,
                    committed_work_tracker.clone(),
                    should_cancel_hydration,
                )?
            else {
                return Ok(None);
            };
            service
        }
        (Some(detached_send_state), None) => {
            open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state(
                &data_dir,
                &cluster_paths,
                local_node_id.clone(),
                peer_node_ids.clone(),
                sync_peer_transport,
                Arc::clone(&peer_transport),
                detached_send_state,
                control_plane,
            )?
        }
        (None, _) => open_cluster_dfs_service_with_peer_transports_for_nodes(
            &data_dir,
            &cluster_paths,
            local_node_id.clone(),
            peer_node_ids.clone(),
            sync_peer_transport,
            Arc::clone(&peer_transport),
            control_plane,
        )?,
    };
    if let Some(committed_work_tracker) = committed_work_tracker {
        service.set_committed_work_tracker(committed_work_tracker);
    }
    let mut maintenance_host = LocalDfsMaintenanceHost::new(data_dir.clone());
    let mut metadata_publisher_host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let mut metadata_transport = AsyncDfsMetadataNodeTransport::new(
        peer_node_ids,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );
    run_dfs_maintenance_tick_with_async_host_and_execution_exclusions(
        &mut service,
        &mut maintenance_host,
        &data_dir,
        file_system.file_system_id,
        &local_node_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        &execution_exclusions,
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
    /// Exact, current catalog validation produced after metadata maintenance.
    ///
    /// Destructive content collectors require `collection_authorized`; a
    /// configured metadata backend by itself is never a reachability proof.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_root_catalog_validation: Option<DfsContentRootCatalogCollectionValidation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_reservation_reconciliation: Option<ContentStoreReservationReconciliationOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_history_gc: Option<DfsMetadataHistoryGcOptions>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_history_gc: Option<DfsMetadataHistoryGcReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_root_catalog_validation: Option<DfsContentRootCatalogCollectionValidation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_reservation_reconciliation: Option<ContentStoreReservationReconciliationReport>,
    #[serde(default)]
    pub durable_content_reservation_count: u64,
    /// Content GC was requested but no metadata backend could provide the
    /// complete, current catalog needed to make deletion safe. Destructive
    /// content GC is skipped in this state; any returned dry-run report is an
    /// explicitly unprotected upper-bound projection.
    #[serde(default)]
    pub content_gc_metadata_protection_unavailable: bool,
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
pub const DEFAULT_DFS_PLACEMENT_RECONCILIATION_MAX_OBJECTS: usize = 256;
pub const DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_DFS_TOMBSTONE_COMPACTION_MIN_AGE: Duration =
    Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS: u64 = 256;
pub const DEFAULT_DFS_METADATA_HISTORY_GC_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS: u64 = 2;
pub const DEFAULT_DFS_METADATA_HISTORY_GC_MAX_RECORDS: u64 = 256;
pub const DEFAULT_DFS_CONTENT_VERSION_GC_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_DFS_CONTENT_VERSION_GC_MAX_RECORDS: u64 = 256;
pub const DEFAULT_DFS_CONTENT_ORPHAN_GC_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_DFS_CONTENT_ORPHAN_GC_MAX_RECORDS: u64 = 256;
pub const DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_AGE: Duration = Duration::from_secs(5 * 60);
pub const DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_ROOTS_PER_SHARD: usize = 1024 * 1024;
// Keep the historical 256-entry pagination bound when sizing this safety
// limit. The adaptive byte budget can make a page smaller than the preferred
// 1,024 entries, so using the preferred size here would reduce the catalog's
// usable coverage and leave destructive content GC permanently fail-closed on
// otherwise supported large shards.
pub const DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_PAGES_PER_SHARD: usize =
    DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_ROOTS_PER_SHARD
        .div_ceil(DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES);
pub const DEFAULT_DFS_CONTENT_RESERVATION_REJECTION_GRACE: Duration =
    Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS: u64 = 256;

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

const DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY: &str =
    "repair execution blocked: target has no currently usable cluster route";

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
    repair_queue_store: crate::repair_queue_store::LocalDfsRepairQueueStore,
    repair_queue_head_sequence: Option<u64>,
}

impl LocalDfsMaintenanceHost {
    pub fn new(data_dir: SinomoDataDir) -> Self {
        let repair_queue_store = crate::repair_queue_store::LocalDfsRepairQueueStore::new(
            default_dfs_repair_attempt_snapshot_path(&data_dir),
        );
        repair_queue_store.reload_from_disk_on_next_access();
        Self {
            data_dir,
            repair_queue_store,
            repair_queue_head_sequence: None,
        }
    }

    fn cursor_path(&self, file_system_id: &DfsFileSystemId) -> PathBuf {
        default_dfs_anti_entropy_cursor_path(&self.data_dir, file_system_id)
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
        let view = self.repair_queue_store.view()?;
        self.repair_queue_head_sequence = Some(view.head_sequence);
        Ok(view.records)
    }

    fn store_repair_attempt_records(
        &mut self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<(), DfsError> {
        let expected_head_sequence = match self.repair_queue_head_sequence {
            Some(sequence) => sequence,
            None => self.repair_queue_store.view()?.head_sequence,
        };
        match self
            .repair_queue_store
            .reconcile(expected_head_sequence, records)?
        {
            crate::repair_queue_store::DfsRepairQueueReconcileOutcome::Applied {
                head_sequence,
                ..
            } => {
                self.repair_queue_head_sequence = Some(head_sequence);
                self.repair_queue_store.compact_if_needed(
                    crate::repair_queue_store::DfsRepairQueueCompactionOptions::default(),
                )?;
                Ok(())
            }
            crate::repair_queue_store::DfsRepairQueueReconcileOutcome::GenerationChanged {
                current_head_sequence,
            } => {
                self.repair_queue_head_sequence = Some(current_head_sequence);
                Err(DfsError::Metadata {
                    message: format!(
                        "DFS repair queue changed during maintenance: expected sequence {expected_head_sequence}, found {current_head_sequence}"
                    ),
                })
            }
        }
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
    let view = crate::repair_queue_store::LocalDfsRepairQueueStore::new(path).view()?;
    Ok(
        (view.head_sequence > 0 || !view.records.is_empty() || path.exists()).then_some(
            DfsRepairAttemptSnapshot {
                records: view.records,
            },
        ),
    )
}

pub fn write_dfs_repair_attempt_snapshot(
    path: &Path,
    snapshot: &DfsRepairAttemptSnapshot,
) -> Result<(), DfsError> {
    let store = crate::repair_queue_store::LocalDfsRepairQueueStore::new(path);
    store.replace_records(&snapshot.records)?;
    store.compact_if_needed(crate::repair_queue_store::DfsRepairQueueCompactionOptions::force())?;
    Ok(())
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
    pub compact_tombstones: bool,
    pub tombstone_compaction_min_age: Duration,
    pub tombstone_compaction_max_records: u64,
    pub collect_metadata_history: bool,
    pub metadata_history_gc_min_age: Duration,
    pub metadata_history_gc_keep_latest_versions: u64,
    pub metadata_history_gc_max_records: u64,
    pub collect_content_versions: bool,
    pub content_version_gc_min_age: Duration,
    pub content_version_gc_keep_latest_versions: u64,
    pub content_version_gc_max_records: u64,
    pub collect_orphan_content: bool,
    pub content_orphan_gc_min_age: Duration,
    pub content_orphan_gc_max_records: u64,
    pub content_root_catalog_max_age: Duration,
    pub content_root_catalog_max_pages_per_shard: usize,
    pub reconcile_content_reservations: bool,
    pub content_reservation_rejection_grace: Duration,
    pub content_reservation_reconciliation_max_records: u64,
}

impl Default for DfsMaintenanceRunOptions {
    fn default() -> Self {
        Self {
            anti_entropy_max_objects: DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_OBJECTS,
            anti_entropy_max_issues: DEFAULT_DFS_MAINTENANCE_ANTI_ENTROPY_MAX_ISSUES,
            repair_metadata: true,
            cleanup_stale_staging: true,
            stale_staging_min_age: DFS_PENDING_TRANSFER_STALE_AFTER,
            compact_tombstones: false,
            tombstone_compaction_min_age: DEFAULT_DFS_TOMBSTONE_COMPACTION_MIN_AGE,
            tombstone_compaction_max_records: DEFAULT_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS,
            collect_metadata_history: false,
            metadata_history_gc_min_age: DEFAULT_DFS_METADATA_HISTORY_GC_MIN_AGE,
            metadata_history_gc_keep_latest_versions:
                DEFAULT_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS,
            metadata_history_gc_max_records: DEFAULT_DFS_METADATA_HISTORY_GC_MAX_RECORDS,
            collect_content_versions: false,
            content_version_gc_min_age: DEFAULT_DFS_CONTENT_VERSION_GC_MIN_AGE,
            content_version_gc_keep_latest_versions:
                DEFAULT_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS,
            content_version_gc_max_records: DEFAULT_DFS_CONTENT_VERSION_GC_MAX_RECORDS,
            collect_orphan_content: false,
            content_orphan_gc_min_age: DEFAULT_DFS_CONTENT_ORPHAN_GC_MIN_AGE,
            content_orphan_gc_max_records: DEFAULT_DFS_CONTENT_ORPHAN_GC_MAX_RECORDS,
            content_root_catalog_max_age: DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_AGE,
            content_root_catalog_max_pages_per_shard:
                DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_PAGES_PER_SHARD,
            reconcile_content_reservations: false,
            content_reservation_rejection_grace: DEFAULT_DFS_CONTENT_RESERVATION_REJECTION_GRACE,
            content_reservation_reconciliation_max_records:
                DEFAULT_DFS_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS,
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
    let maintenance_requested = options.repair_metadata
        || options.cleanup_stale_staging
        || options.compact_tombstones
        || options.collect_metadata_history
        || options.collect_content_versions
        || options.collect_orphan_content;
    if !maintenance_requested {
        // An anti-entropy walk is an O(total-metadata) inventory even when
        // `repair_metadata` is false. Routine/reactive cluster work must never
        // enter this helper merely to obtain an empty lifecycle report.
        return Ok(Vec::new());
    }
    let Some(metadata_backend) = service.metadata_backend() else {
        return Ok(Vec::new());
    };
    let file_systems = metadata_backend.list_file_systems()?;
    let mut reports = Vec::with_capacity(file_systems.len());
    let mut stale_staging_gc = options
        .cleanup_stale_staging
        .then(|| ContentStoreStagingGcOptions::delete_eligible(options.stale_staging_min_age));
    let mut tombstone_compaction = options.compact_tombstones.then(|| {
        DfsTombstoneCompactionOptions::compact_eligible(options.tombstone_compaction_min_age)
            .with_max_compactions(options.tombstone_compaction_max_records)
    });
    let mut metadata_history_gc = options.collect_metadata_history.then(|| {
        DfsMetadataHistoryGcOptions::delete_eligible(
            options.metadata_history_gc_min_age,
            options.metadata_history_gc_keep_latest_versions,
        )
        .with_max_removals(options.metadata_history_gc_max_records)
    });
    let mut version_gc = options.collect_content_versions.then(|| {
        ContentStoreVersionGcOptions::delete_eligible(
            options.content_version_gc_min_age,
            options.content_version_gc_keep_latest_versions,
        )
        .with_max_deletions(options.content_version_gc_max_records)
    });
    let mut orphan_gc = options.collect_orphan_content.then(|| {
        ContentStoreGcOptions::delete_eligible(options.content_orphan_gc_min_age)
            .with_max_deletions(options.content_orphan_gc_max_records)
    });

    if file_systems.is_empty() {
        let shared_maintenance_requested = stale_staging_gc.is_some()
            || metadata_history_gc.is_some()
            || version_gc.is_some()
            || orphan_gc.is_some()
            || tombstone_compaction.is_some();
        if shared_maintenance_requested {
            reports.push(
                service.run_lifecycle_maintenance(DfsLifecycleMaintenanceOptions {
                    stale_staging_gc,
                    metadata_history_gc,
                    version_gc,
                    orphan_gc,
                    tombstone_compaction,
                    ..DfsLifecycleMaintenanceOptions::default()
                })?,
            );
        }
        return Ok(reports);
    }

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
            anti_entropy: options.repair_metadata.then_some(DfsAntiEntropyOptions {
                file_system_id: file_system.file_system_id.clone(),
                after,
                max_objects: options.anti_entropy_max_objects,
                max_issues: options.anti_entropy_max_issues,
                repair_metadata: true,
            }),
            metadata_history_gc: metadata_history_gc.take(),
            version_gc: version_gc.take(),
            orphan_gc: orphan_gc.take(),
            tombstone_compaction: tombstone_compaction.take(),
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

fn dfs_content_gc_requested(options: DfsMaintenanceRunOptions) -> bool {
    options.collect_content_versions
        || options.collect_orphan_content
        || options.reconcile_content_reservations
}

fn dfs_metadata_lifecycle_options(
    mut options: DfsMaintenanceRunOptions,
) -> DfsMaintenanceRunOptions {
    options.collect_content_versions = false;
    options.collect_orphan_content = false;
    options.reconcile_content_reservations = false;
    options
}

fn dfs_content_gc_lifecycle_options(
    options: DfsMaintenanceRunOptions,
    validation: DfsContentRootCatalogCollectionValidation,
) -> DfsLifecycleMaintenanceOptions {
    let catalog_observation = validation
        .is_collection_authorized()
        .then(|| {
            validation
                .catalog_generation()
                .map(str::to_string)
                .zip(validation.catalog_generated_at_unix_nanos())
                .zip(validation.catalog_installed_at_unix_nanos())
                .map(
                    |((generation, generated_at_unix_nanos), installed_at_unix_nanos)| {
                        (generation, generated_at_unix_nanos, installed_at_unix_nanos)
                    },
                )
        })
        .flatten();
    let version_gc = options.collect_content_versions.then(|| {
        let version_gc = ContentStoreVersionGcOptions::delete_eligible(
            options.content_version_gc_min_age,
            options.content_version_gc_keep_latest_versions,
        )
        .with_max_deletions(options.content_version_gc_max_records)
        .with_min_absence_age(options.content_version_gc_min_age);
        match catalog_observation.as_ref() {
            Some((generation, _, installed_at_unix_nanos)) => {
                // First-absence state is local and durable. Use the sealed
                // local installation time so comparisons do not depend on a
                // remote primary's wall clock.
                version_gc.with_catalog_observation(generation.clone(), *installed_at_unix_nanos)
            }
            None => version_gc,
        }
    });
    let orphan_gc = options.collect_orphan_content.then(|| {
        let orphan_gc = ContentStoreGcOptions::delete_eligible(options.content_orphan_gc_min_age)
            .with_max_deletions(options.content_orphan_gc_max_records);
        match catalog_observation.as_ref() {
            Some((generation, _, installed_at_unix_nanos)) => {
                orphan_gc.with_catalog_observation(generation.clone(), *installed_at_unix_nanos)
            }
            None => orphan_gc,
        }
    });
    let reconcile_content_reservations = options.reconcile_content_reservations
        || options.collect_content_versions
        || options.collect_orphan_content;
    let content_reservation_reconciliation = reconcile_content_reservations
        .then(|| {
            catalog_observation
                .as_ref()
                .map(|(generation, _, installed_at_unix_nanos)| {
                    // The reconciliation option predates the distinction between
                    // remote generation time and local installation time. Its
                    // timestamp is causal evidence, so pass the sealed local
                    // installation observation.
                    ContentStoreReservationReconciliationOptions::bounded(
                        *installed_at_unix_nanos,
                        options.content_reservation_rejection_grace,
                        options.content_reservation_reconciliation_max_records,
                    )
                    .with_catalog_generation(generation.clone())
                })
        })
        .flatten();

    DfsLifecycleMaintenanceOptions {
        version_gc,
        orphan_gc,
        content_root_catalog_validation: Some(validation),
        content_reservation_reconciliation,
        ..DfsLifecycleMaintenanceOptions::default()
    }
}

fn run_dfs_content_reservation_reconciliation_with_installed_catalog<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    content_root: &Path,
    options: DfsMaintenanceRunOptions,
) -> Result<Option<DfsLifecycleMaintenanceReport>, DfsError> {
    let catalog = match default_dfs_content_root_catalog_store(content_root).load() {
        Ok(Some(catalog)) => catalog,
        Ok(None) => {
            crate::dfs_perf_trace(format_args!(
                "content-reservation-catalog-reuse-miss reason=not-installed"
            ));
            return Ok(None);
        }
        Err(error) => {
            crate::dfs_perf_trace(format_args!(
                "content-reservation-catalog-reuse-miss reason=load-error error={error}"
            ));
            return Ok(None);
        }
    };
    if !catalog.is_durably_installed() || !catalog.is_complete() {
        crate::dfs_perf_trace(format_args!(
            "content-reservation-catalog-reuse-miss reason=incomplete"
        ));
        return Ok(None);
    }
    let catalog_generation = catalog.generation_fingerprint()?;
    let catalog_versions = catalog
        .protected_version_pairs()
        .expect("complete installed content-root catalog has version roots");
    let report = service.reconcile_content_reservations_from_installed_catalog(
        &catalog_versions,
        &catalog_generation,
        options.content_reservation_reconciliation_max_records,
    )?;
    let reconciliation = report
        .content_reservation_reconciliation
        .as_ref()
        .expect("reservation-only reconciliation returns its report");
    crate::dfs_perf_trace(format_args!(
        "content-reservation-catalog-reuse catalog_generation={} catalog_versions={} examined={} released_covered={} remaining={} has_more={}",
        catalog_generation,
        catalog_versions.len(),
        reconciliation.examined_reservation_count,
        reconciliation.released_catalog_covered_count,
        reconciliation.remaining_after_batch_count,
        reconciliation.has_more
    ));
    Ok(Some(report))
}

fn run_dfs_content_gc_with_catalog<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    content_root: &Path,
    expected_file_systems: &[DfsFileSystemConfig],
    expected_shards: &[DfsMetadataShardConfig],
    pending_handoffs: &[DfsMetadataShardMembershipHandoff],
    head_watermarks: &[DfsMetadataShardHeadWatermark],
    options: DfsMaintenanceRunOptions,
) -> Result<Option<DfsLifecycleMaintenanceReport>, DfsError> {
    if !dfs_content_gc_requested(options) {
        return Ok(None);
    }

    let destructive_content_gc_requested =
        options.collect_content_versions || options.collect_orphan_content;
    if !destructive_content_gc_requested {
        return run_dfs_content_reservation_reconciliation_with_installed_catalog(
            service,
            content_root,
            options,
        );
    }

    let validation = match service.refresh_content_root_catalog(
        expected_file_systems,
        expected_shards,
        pending_handoffs,
        content_root,
        head_watermarks,
        options.content_root_catalog_max_pages_per_shard,
    ) {
        // The authenticated current-primary fragment is the authority
        // proof. A quorum head watermark strengthens that proof when one
        // exists, but a quiet shard or offline replica must not block
        // content GC indefinitely merely because no operation-bound
        // watermark has yet been published.
        Ok(catalog) => catalog.validate_for_collection_with_head_watermark_policy(
            expected_file_systems,
            expected_shards,
            pending_handoffs,
            head_watermarks,
            false,
            dfs_now_unix_nanos(),
            options.content_root_catalog_max_age,
        )?,
        Err(error) => DfsContentRootCatalogCollectionValidation::incomplete_for_expected_shards(
            vec![
                DfsContentRootCatalogIncompleteReason::CatalogRefreshFailed {
                    reason: format!("content-root catalog refresh failed: {error}"),
                },
            ],
            expected_shards.len(),
        ),
    };
    service
        .run_lifecycle_maintenance(dfs_content_gc_lifecycle_options(options, validation))
        .map(Some)
}

fn run_dfs_content_gc_without_catalog<T: DfsMessageTransport>(
    service: &mut NetworkedDfsService<T>,
    options: DfsMaintenanceRunOptions,
) -> Result<Option<DfsLifecycleMaintenanceReport>, DfsError> {
    if !dfs_content_gc_requested(options) {
        return Ok(None);
    }
    let validation = DfsContentRootCatalogCollectionValidation::incomplete(vec![
        DfsContentRootCatalogIncompleteReason::CatalogNotDurablyInstalled,
    ]);
    service
        .run_lifecycle_maintenance(dfs_content_gc_lifecycle_options(options, validation))
        .map(Some)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMaintenanceTickOptions {
    pub namespace_job_max_jobs: usize,
    pub namespace_job_max_steps_per_job: usize,
    pub conflict_max_records: usize,
    pub write_intent_max_records: usize,
    pub stale_write_intent_min_age: Duration,
    pub placement_reconciliation_max_objects: usize,
    pub repair_max_jobs: usize,
    pub lifecycle: DfsMaintenanceRunOptions,
    /// Whether this broad maintenance pass owns metadata-journal publication.
    /// Reactive runtimes with exact per-shard publishers disable this to keep
    /// a single durable owner for each replication cursor.
    pub publish_metadata: bool,
    pub metadata_replication: DfsMetadataJournalReplicationOptions,
}

/// Targets that one maintenance execution must leave parked.
///
/// This is deliberately separate from durable DFS topology and retry
/// suppression. A later pass with a different reachability view can execute
/// the same durable repair or metadata-replication backlog immediately.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsMaintenanceExecutionExclusions {
    target_node_ids: BTreeSet<NodeId>,
}

impl DfsMaintenanceExecutionExclusions {
    pub fn new(target_node_ids: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            target_node_ids: target_node_ids.into_iter().collect(),
        }
    }

    pub fn contains(&self, target_node_id: &NodeId) -> bool {
        self.target_node_ids.contains(target_node_id)
    }

    pub fn is_empty(&self) -> bool {
        self.target_node_ids.is_empty()
    }

    /// Never defer a newly committed repair solely from a cached reachability
    /// projection. Its first bounded send is the evidence which establishes
    /// whether the target is actually blocked; older work with a concrete
    /// failure remains parked until the routing generation changes.
    fn allowing_unattempted_repairs(&self, records: &[DfsRepairAttemptRecord]) -> Self {
        let first_attempt_targets = records
            .iter()
            .filter(|record| record.attempts == 0)
            .map(|record| record.missing_replica_node_id.clone())
            .collect::<BTreeSet<_>>();
        Self::new(
            self.target_node_ids
                .iter()
                .filter(|target| !first_attempt_targets.contains(*target))
                .cloned(),
        )
    }
}

fn mark_connectivity_blocked_dfs_repair_attempt_records(
    records: &mut [DfsRepairAttemptRecord],
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
) {
    for record in records {
        if !execution_exclusions.contains(&record.missing_replica_node_id)
            || record.next_retry_unix_nanos == u64::MAX
        {
            continue;
        }
        record.in_flight = false;
        record.next_retry_unix_nanos = u64::MAX;
        record.transfer_request_id = None;
        record.last_error = Some(DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY.to_string());
    }
}

pub(crate) fn merge_parked_dfs_repair_attempt_records(
    active_records: Vec<DfsRepairAttemptRecord>,
    parked_records: Vec<DfsRepairAttemptRecord>,
    active_repair_keys: &BTreeSet<(String, NodeId)>,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
) -> Vec<DfsRepairAttemptRecord> {
    let mut records = active_records
        .into_iter()
        .filter(|record| !execution_exclusions.contains(&record.missing_replica_node_id))
        .collect::<Vec<_>>();
    records.extend(parked_records.into_iter().filter(|record| {
        active_repair_keys.contains(&(
            record.intent_id.clone(),
            record.missing_replica_node_id.clone(),
        ))
    }));
    records.sort_by(|left, right| {
        left.intent_id.cmp(&right.intent_id).then_with(|| {
            left.missing_replica_node_id
                .cmp(&right.missing_replica_node_id)
        })
    });
    records
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
            placement_reconciliation_max_objects: DEFAULT_DFS_PLACEMENT_RECONCILIATION_MAX_OBJECTS,
            repair_max_jobs: usize::MAX,
            lifecycle: DfsMaintenanceRunOptions::default(),
            publish_metadata: true,
            metadata_replication: DfsMetadataJournalReplicationOptions::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMaintenanceTickReport {
    pub namespace_jobs: DfsNamespaceJobMaintenanceReport,
    pub conflicts: DfsConflictMaintenanceReport,
    pub write_intents: DfsWriteIntentMaintenanceReport,
    pub placement_reconciliation: Option<DfsPlacementReconciliationJob>,
    pub lifecycle_reports: Vec<DfsLifecycleMaintenanceReport>,
    pub scheduled_repairs: Vec<DistributedRepairJob>,
    pub followup_events: Vec<NetworkedDfsServiceEvent>,
    pub repair_attempts_before: Vec<DfsRepairAttemptRecord>,
    pub repair_attempts_after: Vec<DfsRepairAttemptRecord>,
    /// This bounded pass stopped while another repair record was immediately
    /// runnable. The daemon may continue without its ordinary retry gate.
    pub repair_budget_exhausted: bool,
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
        if report.content_gc_metadata_protection_unavailable {
            if let Some(catalog) = report.content_root_catalog_validation.as_ref() {
                lines.push(format!(
                    "DFS content GC catalog blocked: covered_shards={}/{} age_nanos={:?} journal_lag={} reservations={} reasons={:?}; destructive manifest/blob collection skipped",
                    catalog.covered_shard_count(),
                    catalog.expected_shard_count(),
                    catalog.catalog_age_nanos(),
                    catalog.max_journal_lag_entries(),
                    report.durable_content_reservation_count,
                    catalog.reasons(),
                ));
            } else {
                lines.push(format!(
                    "DFS content GC catalog unavailable: covered_shards=0/0 reservations={}; destructive manifest/blob collection skipped; explicit dry-run projections remain unprotected",
                    report.durable_content_reservation_count,
                ));
            }
        }
        if let Some(reconciliation) = report.content_reservation_reconciliation.as_ref()
            && (reconciliation.released_catalog_covered_count > 0
                || reconciliation.released_rejected_count > 0
                || reconciliation.has_more)
        {
            lines.push(format!(
                "DFS content reservations: total={} examined={} released_covered={} released_rejected={} retained_catalog_too_old={} retained_grace={} remaining={} has_more={}",
                reconciliation.reservation_count,
                reconciliation.examined_reservation_count,
                reconciliation.released_catalog_covered_count,
                reconciliation.released_rejected_count,
                reconciliation.retained_catalog_too_old_count,
                reconciliation.retained_rejection_grace_count,
                reconciliation.remaining_after_batch_count,
                reconciliation.has_more,
            ));
        }
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
        if let Some(metadata_gc) = report.metadata_history_gc.as_ref()
            && (metadata_gc.applied_record_count() > 0 || metadata_gc.max_removals_reached)
        {
            lines.push(format!(
                "DFS metadata history GC: applied={} versions={} replica_receipts={} intent_bodies={} intent_receipts={} conflicts={} visible_commits={} namespace_bodies={} namespace_receipts={} dry_run={} max_removals_reached={}",
                metadata_gc.applied_record_count(),
                metadata_gc.removed_version_count,
                metadata_gc.removed_receipt_count,
                metadata_gc.compacted_intent_count,
                metadata_gc.removed_intent_receipt_count,
                metadata_gc.removed_conflict_count,
                metadata_gc.removed_visible_commit_count,
                metadata_gc
                    .compacted_namespace_job_count
                    .saturating_add(metadata_gc.compacted_namespace_transaction_count)
                    .saturating_add(metadata_gc.compacted_namespace_prepare_count)
                    .saturating_add(metadata_gc.compacted_namespace_request_count),
                metadata_gc
                    .removed_namespace_job_receipt_count
                    .saturating_add(metadata_gc.removed_namespace_transaction_receipt_count)
                    .saturating_add(metadata_gc.removed_namespace_prepare_receipt_count)
                    .saturating_add(metadata_gc.removed_namespace_request_receipt_count),
                metadata_gc.dry_run,
                metadata_gc.max_removals_reached,
            ));
        }
        if let Some(version_gc) = report.version_gc.as_ref()
            && (version_gc.deleted_version_count > 0 || version_gc.max_deletions_reached)
        {
            lines.push(format!(
                "DFS content version GC: scanned={} eligible={} deleted={} logical_bytes_deleted={} protected={} dry_run={} max_deletions_reached={}",
                version_gc.scanned_version_count,
                version_gc.eligible_version_count,
                version_gc.deleted_version_count,
                version_gc.deleted_logical_bytes,
                version_gc.retained_protected_version_count,
                version_gc.dry_run,
                version_gc.max_deletions_reached,
            ));
        }
        if let Some(orphan_gc) = report.orphan_gc.as_ref()
            && (orphan_gc.deleted_orphan_count > 0 || orphan_gc.max_deletions_reached)
        {
            lines.push(format!(
                "DFS orphan content GC: eligible={} deleted={} bytes_deleted={} protected={} dry_run={} max_deletions_reached={}",
                orphan_gc.eligible_orphan_count,
                orphan_gc.deleted_orphan_count,
                orphan_gc.deleted_orphan_bytes,
                orphan_gc.retained_protected_orphan_count,
                orphan_gc.dry_run,
                orphan_gc.max_deletions_reached,
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
    // A durable pending membership freeze deliberately rejects every
    // authoritative metadata mutation. Reconcile it before namespace,
    // conflict, intent, or lifecycle maintenance so a marker surviving restart
    // cannot prevent the only production path that activates or rebinds its
    // authority generation.
    service.reconcile_pending_metadata_shard_membership_handoffs(
        &metadata_shards,
        options.metadata_replication.target_timeout,
    )?;
    // A cluster KV snapshot can become visible before its DFS shard topology
    // is bootstrapped. Scanning with a zero execution budget preserves durable
    // running jobs for the first tick that observes committed authority instead
    // of converting that transient startup state into a permanent failure.
    let namespace_job_max_jobs = if metadata_shards.is_empty() {
        0
    } else {
        options.namespace_job_max_jobs
    };
    let _namespace_requests = service.recover_pending_namespace_requests_bounded(
        crate::service_routed_namespace::DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT,
    )?;
    let namespace_jobs = service
        .posix_resume_namespace_jobs_bounded(
            namespace_job_max_jobs,
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
    let placement_reconciliation = service
        .run_placement_reconciliation_bounded(options.placement_reconciliation_max_objects)?;
    let mut lifecycle_reports = run_dfs_lifecycle_maintenance_with_host(
        service,
        maintenance_host,
        dfs_metadata_lifecycle_options(options.lifecycle),
    )?;
    if let Some(content_gc) = run_dfs_content_gc_without_catalog(service, options.lifecycle)? {
        lifecycle_reports.push(content_gc);
    }
    let durable_repair_queue_before = maintenance_host.load_repair_attempt_records()?;
    let repair_queue_count_before = durable_repair_queue_before.len();
    let scheduled_repairs = service.retry_pending_repairs_bounded_with_attempts(
        options.repair_max_jobs,
        &durable_repair_queue_before,
    )?;
    let repair_attempts_before = durable_repair_queue_before
        .into_iter()
        .filter(|record| record.attempts > 0)
        .collect();
    let followup_events = service.process_incoming_until_quiescent()?;
    service.observe_repair_maintenance_events(&followup_events);
    let repair_attempts_after = service.active_repair_attempt_records();
    let durable_repair_queue_after = service.durable_repair_queue_records();
    let repair_budget_exhausted = options.repair_max_jobs > 0
        && options.repair_max_jobs != usize::MAX
        && repair_queue_count_before >= options.repair_max_jobs
        && durable_repair_queue_after.iter().any(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= dfs_now_unix_nanos()
        });
    maintenance_host.store_repair_attempt_records(&durable_repair_queue_after)?;
    let metadata_replication = if options.publish_metadata {
        publish_dfs_metadata_journal_with_host(
            file_system_id,
            local_node_id,
            metadata_shards.clone(),
            metadata_publisher_host,
            metadata_transport,
            options.metadata_replication,
        )?
    } else {
        DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        }
    };
    Ok(DfsMaintenanceTickReport {
        namespace_jobs,
        conflicts,
        write_intents,
        placement_reconciliation,
        lifecycle_reports,
        scheduled_repairs,
        followup_events,
        repair_attempts_before,
        repair_attempts_after,
        repair_budget_exhausted,
        metadata_replication,
    })
}

#[cfg(test)]
mod causal_time_tests {
    use super::*;
    use crate::{
        DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION, DfsContentRootCatalog,
        DfsContentRootCatalogCoverage, DfsContentRootCatalogFragment,
        DfsContentRootCatalogShardIdentity, DfsMetadataShardAssignment,
        LocalDfsContentRootCatalogStore,
        dfs_content_root_catalog_topology_digest_with_file_systems,
    };
    use sinomo_core::DfsObjectPolicy;

    fn repair_attempt_for_connectivity_test(
        target: NodeId,
        attempts: u32,
    ) -> DfsRepairAttemptRecord {
        DfsRepairAttemptRecord {
            intent_id: "intent-reactive-connectivity".to_string(),
            object_id: DfsObjectId::from_legacy_named("object-reactive-connectivity")
                .expect("object id"),
            version: DfsVersion::from_legacy_named("version-reactive-connectivity")
                .expect("version"),
            content_hash: "blake3:reactive-connectivity".to_string(),
            missing_replica_node_id: target,
            attempts,
            in_flight: false,
            last_attempt_unix_nanos: 0,
            next_retry_unix_nanos: dfs_now_unix_nanos(),
            transfer_request_id: None,
            last_error: None,
        }
    }

    #[test]
    fn connectivity_projection_cannot_defer_the_first_bounded_repair_attempt() {
        let target = NodeId::from_legacy_named("node-reactive-connectivity").expect("target node");
        let exclusions = DfsMaintenanceExecutionExclusions::new([target.clone()]);
        let unattempted = repair_attempt_for_connectivity_test(target.clone(), 0);
        let effective = exclusions.allowing_unattempted_repairs(std::slice::from_ref(&unattempted));

        assert!(
            !effective.contains(&target),
            "cached reachability alone must not park newly committed repair work"
        );

        let attempted = repair_attempt_for_connectivity_test(target.clone(), 1);
        let effective = exclusions.allowing_unattempted_repairs(std::slice::from_ref(&attempted));
        assert!(
            effective.contains(&target),
            "a target with an actual failed attempt may remain connectivity-parked"
        );

        let mut records = vec![attempted];
        mark_connectivity_blocked_dfs_repair_attempt_records(&mut records, &exclusions);
        assert_eq!(records[0].next_retry_unix_nanos, u64::MAX);
        assert!(!records[0].in_flight);
        assert_eq!(records[0].transfer_request_id, None);
        assert_eq!(
            records[0].last_error.as_deref(),
            Some(DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY),
            "a deferred record must carry its concrete blocker"
        );
    }

    #[test]
    fn default_catalog_page_budget_preserves_the_bounded_root_capacity() {
        assert_eq!(DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_PAGES_PER_SHARD, 4096);
        let root_capacity = DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_PAGES_PER_SHARD
            * DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES;
        assert!(root_capacity >= DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_ROOTS_PER_SHARD);
        assert!(
            root_capacity - DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_ROOTS_PER_SHARD
                < DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES
        );
        assert!(
            crate::DEFAULT_DFS_CONTENT_ROOT_CATALOG_PAGE_ENTRIES
                > DFS_CONTENT_ROOT_CATALOG_MAX_PAGE_ENTRIES,
            "the request target may grow without silently reducing the safety page budget"
        );
    }

    #[test]
    fn content_gc_uses_local_install_time_for_durable_content_observations() {
        let file_system_id =
            DfsFileSystemId::from_legacy_named("causal-time-fs").expect("filesystem");
        let primary_node_id =
            NodeId::from_legacy_named("causal-time-primary").expect("primary node");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "causal-time-shard".to_string(),
            primary_node_id: primary_node_id.clone(),
            replica_node_ids: Vec::new(),
            epoch: 4,
            assignment: DfsMetadataShardAssignment::Root,
            namespace_prepare_codec: Default::default(),
            default_policy: DfsObjectPolicy::ReplicatedConfirmed,
        };
        let file_system = DfsFileSystemConfig {
            file_system_id,
            display_name: "Causal time filesystem".to_string(),
            owner_node_id: primary_node_id,
            subtree_policies: Vec::new(),
        };
        let topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(
            std::slice::from_ref(&file_system),
            std::slice::from_ref(&shard),
        )
        .expect("topology digest");
        let identity = DfsContentRootCatalogShardIdentity::from_config(&shard);
        let generated_at_unix_nanos = 10;
        let installed_at_unix_nanos = 100;
        let state_fingerprint = format!(
            "dfs-metadata-prefix-v1:blake3:{}",
            blake3::hash(b"causal-time-state").to_hex()
        );
        let catalog = DfsContentRootCatalog::from_fragments(
            topology_digest.clone(),
            vec![identity.clone()],
            generated_at_unix_nanos,
            vec![DfsContentRootCatalogFragment {
                identity,
                topology_digest,
                generated_at_unix_nanos,
                coverage: DfsContentRootCatalogCoverage::Complete {
                    prefix_proof_version: DFS_CONTENT_ROOT_CATALOG_PREFIX_PROOF_VERSION,
                    journal_sequence: 1,
                    state_fingerprint,
                },
                entries: Vec::new(),
            }],
        )
        .expect("catalog");
        let root = sinomo_testdir::TestDir::new("dfs-content-root-catalog-causal-time");
        let validation = LocalDfsContentRootCatalogStore::new(&root)
            .install_test_catalog_at(catalog, installed_at_unix_nanos)
            .expect("install catalog")
            .validate_for_collection_with_head_watermark_policy(
                std::slice::from_ref(&file_system),
                std::slice::from_ref(&shard),
                &[],
                &[],
                false,
                installed_at_unix_nanos + 1,
                Duration::from_nanos(100),
            )
            .expect("validate catalog");
        let content_gc_options = DfsMaintenanceRunOptions {
            collect_content_versions: true,
            reconcile_content_reservations: true,
            ..DfsMaintenanceRunOptions::default()
        };
        assert!(validation.is_collection_authorized());
        assert_eq!(
            validation.catalog_generated_at_unix_nanos(),
            Some(generated_at_unix_nanos)
        );
        assert_eq!(
            validation.catalog_installed_at_unix_nanos(),
            Some(installed_at_unix_nanos)
        );

        let options = dfs_content_gc_lifecycle_options(content_gc_options, validation);
        assert_eq!(
            options
                .version_gc
                .expect("version GC")
                .catalog_generated_at_unix_nanos,
            Some(installed_at_unix_nanos),
            "manifest first-absence observations use the causally ordered local installation time"
        );
        assert_eq!(
            options
                .content_reservation_reconciliation
                .expect("reservation reconciliation")
                .catalog_generated_at_unix_nanos,
            installed_at_unix_nanos,
            "reservation release uses the causally ordered local installation time"
        );
    }
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
    run_dfs_maintenance_tick_with_async_host_and_execution_exclusions(
        service,
        maintenance_host,
        data_dir,
        file_system_id,
        local_node_id,
        metadata_shards,
        metadata_publisher_host,
        metadata_transport,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_dfs_maintenance_tick_with_async_host_and_execution_exclusions<T>(
    service: &mut NetworkedDfsService<T>,
    maintenance_host: &mut dyn DfsMaintenanceHost,
    data_dir: &SinomoDataDir,
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    metadata_shards: Vec<DfsMetadataShardConfig>,
    metadata_publisher_host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMaintenanceTickOptions,
) -> Result<DfsMaintenanceTickReport, DfsError>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    // Keep the async tick's safety ordering identical to the synchronous tick:
    // handoff activation/rebind must precede every freeze-sensitive mutation.
    service.reconcile_pending_metadata_shard_membership_handoffs(
        &metadata_shards,
        options.metadata_replication.target_timeout,
    )?;
    // Keep the asynchronous production path identical to the synchronous
    // path: an absent committed topology is retryable startup state, not a
    // reason to fail durable namespace work.
    let namespace_job_max_jobs = if metadata_shards.is_empty() {
        0
    } else {
        options.namespace_job_max_jobs
    };
    let _namespace_requests = service.recover_pending_namespace_requests_bounded(
        crate::service_routed_namespace::DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT,
    )?;
    let namespace_jobs = service
        .posix_resume_namespace_jobs_bounded(
            namespace_job_max_jobs,
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
    let placement_reconciliation = service
        .run_placement_reconciliation_bounded(options.placement_reconciliation_max_objects)?;
    let mut lifecycle_reports = run_dfs_lifecycle_maintenance_with_host(
        service,
        maintenance_host,
        dfs_metadata_lifecycle_options(options.lifecycle),
    )?;
    if dfs_content_gc_requested(options.lifecycle) {
        // Metadata history removal is authoritative and journaled above. Build
        // the node-wide content-root catalog only after that durable cut, and
        // cover every filesystem whose manifests share this content store.
        let cluster_paths = ClusterPaths::new(data_dir);
        let expected_file_systems = cluster_dfs_all_file_system_configs(&cluster_paths)?;
        let expected_shards = cluster_dfs_all_metadata_shards(&cluster_paths)?;
        let pending_handoffs = cluster_dfs_all_metadata_shard_membership_handoffs(&cluster_paths)?;
        let head_watermarks = cluster_dfs_all_metadata_shard_head_watermarks(&cluster_paths)?;
        if let Some(content_gc) = run_dfs_content_gc_with_catalog(
            service,
            &data_dir.dfs_dir().join("content"),
            &expected_file_systems,
            &expected_shards,
            &pending_handoffs,
            &head_watermarks,
            options.lifecycle,
        )? {
            lifecycle_reports.push(content_gc);
        }
    }
    let durable_repair_queue_before = maintenance_host.load_repair_attempt_records()?;
    let repair_execution_exclusions =
        execution_exclusions.allowing_unattempted_repairs(&durable_repair_queue_before);
    let (parked_repair_attempts, eligible_repair_attempts): (Vec<_>, Vec<_>) =
        durable_repair_queue_before
            .iter()
            .cloned()
            .partition(|record| {
                repair_execution_exclusions.contains(&record.missing_replica_node_id)
            });
    let scheduled_repairs = service
        .retry_pending_repairs_async_bounded_with_attempts_and_execution_exclusions(
            options.repair_max_jobs,
            &eligible_repair_attempts,
            &repair_execution_exclusions,
        )
        .await?;
    let followup_events = service.process_incoming_async_until_quiescent().await?;
    service.observe_repair_maintenance_events(&followup_events);
    let active_repair_keys = service
        .coordinator
        .repair_jobs()
        .iter()
        .map(|job| (job.intent_id.clone(), job.missing_replica_node_id.clone()))
        .collect::<BTreeSet<_>>();
    let mut durable_repair_queue_after = merge_parked_dfs_repair_attempt_records(
        service.durable_repair_queue_records(),
        parked_repair_attempts.clone(),
        &active_repair_keys,
        &repair_execution_exclusions,
    );
    let mut repair_attempts_after = merge_parked_dfs_repair_attempt_records(
        service.active_repair_attempt_records(),
        parked_repair_attempts,
        &active_repair_keys,
        &repair_execution_exclusions,
    );
    mark_connectivity_blocked_dfs_repair_attempt_records(
        &mut durable_repair_queue_after,
        execution_exclusions,
    );
    mark_connectivity_blocked_dfs_repair_attempt_records(
        &mut repair_attempts_after,
        execution_exclusions,
    );
    let repair_budget_exhausted = options.repair_max_jobs > 0
        && options.repair_max_jobs != usize::MAX
        && eligible_repair_attempts.len() >= options.repair_max_jobs
        && durable_repair_queue_after.iter().any(|record| {
            record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= dfs_now_unix_nanos()
        });
    let repair_attempts_before = durable_repair_queue_before
        .into_iter()
        .filter(|record| record.attempts > 0)
        .collect();
    maintenance_host.store_repair_attempt_records(&durable_repair_queue_after)?;
    let metadata_replication = if options.publish_metadata {
        crate::publish_dfs_metadata_journal_with_async_host_and_execution_exclusions(
            file_system_id,
            local_node_id,
            metadata_shards.clone(),
            data_dir,
            metadata_publisher_host,
            metadata_transport,
            execution_exclusions,
            options.metadata_replication,
        )
        .await?
    } else {
        DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        }
    };
    Ok(DfsMaintenanceTickReport {
        namespace_jobs,
        conflicts,
        write_intents,
        placement_reconciliation,
        lifecycle_reports,
        scheduled_repairs,
        followup_events,
        repair_attempts_before,
        repair_attempts_after,
        repair_budget_exhausted,
        metadata_replication,
    })
}
