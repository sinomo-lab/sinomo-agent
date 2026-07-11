//! DFS metadata journal replication and default metadata materialization.
//!
//! This module owns the metadata publish pipeline: deciding when service events
//! require journal publication, routing journal/snapshot replication to peer
//! nodes, validating incoming replica updates, and materializing the default
//! SIFS metadata records. Durable metadata storage itself remains owned by
//! `metadata_backend`.

use serde::{Deserialize, Serialize};
use sinomo_core::ClusterBootstrapState;
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{ClusterPaths, NetworkError};
use sinomo_core::node::NodeId;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

use crate::maintenance::migrate_legacy_dfs_metadata_replication_cursor;
use crate::{
    AsyncDfsPeerTransport, DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, DfsDistributedWriteResult, DfsError, DfsFileSystem,
    DfsFileSystemId, DfsMetadataBackend, DfsMetadataJournalBatch, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsMetadataReplicationSuppressionRecord, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsMetadataSnapshotInstallResult, DfsNamespaceResult, DfsObjectRpcSendOptions,
    DfsObjectVersionRecord, DfsPeerTransport, DfsPeerTransportFuture, LocalShardedMetadataBackend,
    NetworkedDfsServiceEvent, cluster_dfs_metadata_shards, cluster_dfs_peer_node_ids,
    decode_dfs_metadata_journal_batch_rpc_response,
    decode_dfs_metadata_snapshot_install_rpc_response,
    default_dfs_metadata_replication_suppression_snapshot_path,
    dfs_metadata_journal_batch_rpc_request, dfs_metadata_snapshot_install_rpc_request,
    dfs_now_unix_nanos, distributed_sifs, distributed_sifs_bootstrap_objects_for_nodes,
    local_dfs_metadata_shard_path, metadata_shards_prefix, object_rpc_request_from_kv_payload,
    open_local_metadata_shard_backend, put_local_dfs_metadata_replication_suppression,
    read_dfs_metadata_replication_suppression_snapshot, sifs_file_system_id,
};
use crate::{metadata_cursor, metadata_lock};

pub const DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;

pub fn publish_cluster_dfs_metadata_journal(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        });
    }

    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let shards = cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?;

    let peer_node_ids = cluster_dfs_peer_node_ids(&state);
    let mut transport = DfsMetadataNodeTransport::new(peer_node_ids, sender, snapshot_sender);
    let mut host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    publish_dfs_metadata_journal_with_host(
        file_system.file_system_id,
        &state.local_node_id,
        shards,
        &mut host,
        &mut transport,
        options,
    )
}

pub async fn publish_cluster_dfs_metadata_journal_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let Some(plan) =
        prepare_cluster_dfs_metadata_journal_publish_async(data_dir, cluster_paths, options)
            .await?
    else {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        });
    };
    let mut transport =
        AsyncDfsMetadataNodeTransport::new(plan.peer_node_ids.clone(), sender, snapshot_sender);
    execute_prepared_dfs_metadata_journal_publish_async(plan, &mut transport, options).await
}

pub fn publish_cluster_dfs_metadata_journal_with_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
        cluster_paths,
        peer_transport.as_ref(),
    )?;
    publish_cluster_dfs_metadata_journal(
        data_dir,
        cluster_paths,
        dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        options,
    )
}

pub async fn publish_cluster_dfs_metadata_journal_with_async_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    publish_cluster_dfs_metadata_journal_async(
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        options,
    )
    .await
}

pub async fn publish_cluster_dfs_metadata_journal_with_peer_transports_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let refresh_cluster_paths = cluster_paths.clone();
    tokio::task::spawn_blocking(move || {
        refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
            &refresh_cluster_paths,
            sync_peer_transport.as_ref(),
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata publish KV refresh task failed: {source}"),
    })??;

    publish_cluster_dfs_metadata_journal_async(
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        options,
    )
    .await
}

pub fn refresh_cluster_dfs_kv_snapshot_with_peer_transport(
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> Result<(), DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    if sinomo_core::kv::cluster_kv_raft_local_is_voter(&state) {
        return Ok(());
    }
    refresh_cluster_dfs_kv_snapshot_with_peer_transport_for_state(
        cluster_paths,
        &state,
        peer_transport.as_ref(),
    )
}

pub(crate) fn refresh_cluster_dfs_kv_snapshot_before_metadata_publish_with_peer_transport(
    cluster_paths: &ClusterPaths,
    peer_transport: &dyn DfsPeerTransport,
) -> Result<(), DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    if sinomo_core::kv::cluster_kv_raft_local_is_voter(&state) {
        return Ok(());
    }
    let file_system = distributed_sifs(state.local_node_id.clone());
    let shards = cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?;
    if !shards
        .iter()
        .any(|shard| shard.primary_node_id == state.local_node_id)
    {
        return Ok(());
    }
    refresh_cluster_dfs_kv_snapshot_with_peer_transport_for_state(
        cluster_paths,
        &state,
        peer_transport,
    )
}

fn refresh_cluster_dfs_kv_snapshot_with_peer_transport_for_state(
    cluster_paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    peer_transport: &dyn DfsPeerTransport,
) -> Result<(), DfsError> {
    let payload = sinomo_core::kv::cluster_kv_snapshot_rpc_payload()?;
    let request = object_rpc_request_from_kv_payload(payload);
    let options = sinomo_core::cluster::ClusterControlPlaneTimeouts::default_daemon_transport()
        .kv_snapshot_refresh_options();
    sinomo_core::cluster::refresh_current_cluster_kv_store_with_local_storage_and_transport(
        cluster_paths,
        state,
        options,
        |target_node_id, timeout| {
            let response = peer_transport
                .call_object_rpc(
                    target_node_id,
                    &request,
                    DfsObjectRpcSendOptions::default()
                        .with_overall_timeout(timeout)
                        .with_observe_connectivity(false),
                )
                .map_err(|source| NetworkError::Io {
                    operation: "DFS cluster KV snapshot refresh".to_string(),
                    detail: source.to_string(),
                })?;
            response.error_detail()?;
            sinomo_core::kv::decode_cluster_kv_snapshot_rpc_body(&response.body)
                .map_err(sinomo_core::network::NetworkError::directory)
        },
    )
    .map_err(DfsError::from_network)?;
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataReplicaShardContext {
    pub local_node_id: NodeId,
    pub shard: DfsMetadataShardConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMetadataJournalReplicationOptions {
    pub batch_entry_limit: usize,
    pub batch_byte_limit: usize,
    pub max_batches_per_target: usize,
    pub attempt_timeout: Duration,
    pub target_timeout: Duration,
}

impl Default for DfsMetadataJournalReplicationOptions {
    fn default() -> Self {
        Self {
            batch_entry_limit: DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            batch_byte_limit: DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            max_batches_per_target: DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
            attempt_timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            target_timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        }
    }
}

fn dfs_metadata_replication_attempt_timeout(
    options: DfsMetadataJournalReplicationOptions,
    target_started_at: Instant,
) -> Option<Duration> {
    let remaining = options
        .target_timeout
        .checked_sub(target_started_at.elapsed())?;
    if remaining.is_zero() {
        return None;
    }
    Some(options.attempt_timeout.min(remaining))
}

#[cfg(test)]
mod replication_budget_tests {
    use super::*;

    #[test]
    fn metadata_replication_defaults_bound_attempt_and_target_time() {
        let options = DfsMetadataJournalReplicationOptions::default();
        assert_eq!(
            options.attempt_timeout,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
        );
        assert_eq!(
            options.target_timeout,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT
        );
    }

    #[test]
    fn metadata_replication_attempt_uses_remaining_target_budget() {
        let options = DfsMetadataJournalReplicationOptions {
            attempt_timeout: Duration::from_secs(10),
            target_timeout: Duration::from_secs(15),
            ..DfsMetadataJournalReplicationOptions::default()
        };

        let fresh = dfs_metadata_replication_attempt_timeout(options, Instant::now())
            .expect("fresh attempt budget");
        assert!(fresh > Duration::from_secs(9));
        assert!(fresh <= Duration::from_secs(10));

        let partly_spent = dfs_metadata_replication_attempt_timeout(
            options,
            Instant::now() - Duration::from_secs(9),
        )
        .expect("remaining target budget");
        assert!(partly_spent > Duration::from_secs(5));
        assert!(partly_spent <= Duration::from_secs(6));

        assert!(
            dfs_metadata_replication_attempt_timeout(
                options,
                Instant::now() - Duration::from_secs(15),
            )
            .is_none()
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsMetadataJournalReplicationReport {
    pub outcomes: Vec<DfsMetadataJournalReplicationOutcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum DfsMetadataJournalReplicationOutcome {
    MissingShard {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    },
    NotPrimary {
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        local_node_id: NodeId,
        primary_node_id: NodeId,
        epoch: u64,
    },
    Applied {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        entries: usize,
        next_after_sequence: u64,
    },
    NoChanges {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    MissingRoute {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
    },
    TopologyMismatchSuppressed {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        epoch: u64,
        reason: String,
    },
    Rejected {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    SnapshotInstalled {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        next_after_sequence: u64,
        archived_existing: bool,
    },
    Failed {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        reason: String,
    },
    BacklogRemaining {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        after_sequence: u64,
        max_batches: usize,
    },
}

pub fn dfs_metadata_replication_sent_count(report: &DfsMetadataJournalReplicationReport) -> usize {
    report
        .outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                DfsMetadataJournalReplicationOutcome::Applied { .. }
                    | DfsMetadataJournalReplicationOutcome::SnapshotInstalled { .. }
            )
        })
        .count()
}

pub fn dfs_metadata_replication_notice_lines(
    report: &DfsMetadataJournalReplicationReport,
) -> Vec<String> {
    report
        .outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id,
                entries,
                ..
            } => Some(format!(
                "replicated {entries} DFS metadata journal entries to {target_node_id}"
            )),
            DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id,
                next_after_sequence,
                ..
            } => Some(format!(
                "repaired DFS metadata replica {target_node_id} with snapshot through {next_after_sequence}"
            )),
            DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id,
                reason,
                ..
            }
            | DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id,
                reason,
                ..
            } => Some(format!(
                "DFS metadata replication to {target_node_id} failed: {reason}"
            )),
            DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                max_batches,
                ..
            } => Some(format!(
                "DFS metadata replication to {target_node_id} still has backlog after {max_batches} batches"
            )),
            DfsMetadataJournalReplicationOutcome::NoChanges { .. }
            | DfsMetadataJournalReplicationOutcome::MissingRoute { .. }
            | DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed { .. }
            | DfsMetadataJournalReplicationOutcome::MissingShard { .. }
            | DfsMetadataJournalReplicationOutcome::NotPrimary { .. } => None,
        })
        .collect()
}

pub fn dfs_events_should_publish_metadata(events: &[NetworkedDfsServiceEvent]) -> bool {
    events.iter().any(|event| match event {
        NetworkedDfsServiceEvent::BlobStaged { .. }
        | NetworkedDfsServiceEvent::VersionCommitted { .. }
        | NetworkedDfsServiceEvent::ReplicaRepaired { .. }
        | NetworkedDfsServiceEvent::WriteConflictCreated { .. }
        | NetworkedDfsServiceEvent::OwnerHandoffCompleted { .. } => true,
        NetworkedDfsServiceEvent::RemoteWriteServed { result, .. }
        | NetworkedDfsServiceEvent::RemoteWriteCompleted { result, .. } => {
            matches!(result, DfsDistributedWriteResult::Outcome { .. })
        }
        // The primary journals routed namespace mutations and must publish
        // them to replicas; the requester-side completion only replays.
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result,
            mutates_metadata,
            ..
        } => *mutates_metadata && matches!(result, DfsNamespaceResult::Applied),
        NetworkedDfsServiceEvent::RemoteNamespaceCompleted { .. }
        | NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. } => false,
        NetworkedDfsServiceEvent::BlobStageRejected { .. }
        | NetworkedDfsServiceEvent::BlobFetched { .. }
        | NetworkedDfsServiceEvent::BlobFetchRejected { .. }
        | NetworkedDfsServiceEvent::RemoteReadServed { .. }
        | NetworkedDfsServiceEvent::RemoteBulkReadServed { .. }
        | NetworkedDfsServiceEvent::RemoteRangeReadChunkReceived { .. }
        | NetworkedDfsServiceEvent::RemoteReadCompleted { .. }
        | NetworkedDfsServiceEvent::RemoteBulkReadCompleted { .. }
        | NetworkedDfsServiceEvent::RepairRejected { .. }
        | NetworkedDfsServiceEvent::RepairRetryFailed { .. }
        | NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
        | NetworkedDfsServiceEvent::MetadataJournalBatchRejected { .. }
        | NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. } => false,
    })
}

pub fn dfs_metadata_replication_report_needs_retry(
    report: &DfsMetadataJournalReplicationReport,
) -> bool {
    report.outcomes.iter().any(|outcome| {
        matches!(
            outcome,
            DfsMetadataJournalReplicationOutcome::Failed { .. }
                | DfsMetadataJournalReplicationOutcome::BacklogRemaining { .. }
        )
    })
}

pub fn run_dfs_metadata_publish_after_events<P, R, E>(
    events: &[NetworkedDfsServiceEvent],
    mut publish: P,
    mut on_report: R,
    mut on_error: E,
) -> bool
where
    P: FnMut() -> Result<DfsMetadataJournalReplicationReport, String>,
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    if !dfs_events_should_publish_metadata(events) {
        return false;
    }
    match publish() {
        Ok(report) => on_report(report),
        Err(error) => on_error(error),
    }
    true
}

pub async fn run_dfs_metadata_publish_after_events_async<P, Fut, R, E>(
    events: &[NetworkedDfsServiceEvent],
    publish: P,
    mut on_report: R,
    mut on_error: E,
) -> bool
where
    P: FnOnce() -> Fut,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>>,
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    if !dfs_events_should_publish_metadata(events) {
        return false;
    }
    match publish().await {
        Ok(report) => on_report(report),
        Err(error) => on_error(error),
    }
    true
}

pub fn schedule_dfs_metadata_publish_after_events_async_on_runtime<P, Fut, R, E>(
    scheduler: &'static DfsMetadataPublishScheduler,
    runtime: tokio::runtime::Handle,
    events: &[NetworkedDfsServiceEvent],
    options: DfsMetadataPublishScheduleOptions,
    publish: P,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    if !dfs_events_should_publish_metadata(events) {
        return None;
    }
    Some(scheduler.request_publish_async_on_runtime(runtime, options, publish, on_report, on_error))
}

static DEFAULT_DFS_METADATA_PUBLISH_SCHEDULER: DfsMetadataPublishScheduler =
    DfsMetadataPublishScheduler::new();

pub fn schedule_default_dfs_metadata_publish_after_events_async_on_runtime<P, Fut, R, E>(
    runtime: tokio::runtime::Handle,
    events: &[NetworkedDfsServiceEvent],
    options: DfsMetadataPublishScheduleOptions,
    publish: P,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_dfs_metadata_publish_after_events_async_on_runtime(
        &DEFAULT_DFS_METADATA_PUBLISH_SCHEDULER,
        runtime,
        events,
        options,
        publish,
        on_report,
        on_error,
    )
}

pub fn run_cluster_dfs_metadata_publish_after_events<R, E>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    metadata_sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
    events: &[NetworkedDfsServiceEvent],
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> bool
where
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    run_dfs_metadata_publish_after_events(
        events,
        || {
            publish_cluster_dfs_metadata_journal(
                data_dir,
                cluster_paths,
                Arc::clone(&metadata_sender),
                Arc::clone(&snapshot_sender),
                replication_options,
            )
            .map_err(|error| error.to_string())
        },
        on_report,
        on_error,
    )
}

pub fn run_cluster_dfs_metadata_publish_after_events_with_peer_transport<R, E>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> bool
where
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    run_cluster_dfs_metadata_publish_after_events(
        data_dir,
        cluster_paths,
        dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        events,
        replication_options,
        on_report,
        on_error,
    )
}

pub async fn run_cluster_dfs_metadata_publish_after_events_with_async_peer_transport<R, E>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> bool
where
    R: FnMut(DfsMetadataJournalReplicationReport),
    E: FnMut(String),
{
    run_dfs_metadata_publish_after_events_async(
        events,
        || async {
            publish_cluster_dfs_metadata_journal_with_async_peer_transport(
                data_dir,
                cluster_paths,
                peer_transport,
                replication_options,
            )
            .await
            .map_err(|error| error.to_string())
        },
        on_report,
        on_error,
    )
    .await
}

pub fn schedule_cluster_dfs_metadata_publish_after_events_async_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    metadata_sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    events: &[NetworkedDfsServiceEvent],
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_default_dfs_metadata_publish_after_events_async_on_runtime(
        runtime,
        events,
        schedule_options,
        move || {
            let data_dir = data_dir.clone();
            let cluster_paths = cluster_paths.clone();
            let metadata_sender = Arc::clone(&metadata_sender);
            let snapshot_sender = Arc::clone(&snapshot_sender);
            async move {
                publish_cluster_dfs_metadata_journal_async(
                    &data_dir,
                    &cluster_paths,
                    metadata_sender,
                    snapshot_sender,
                    replication_options,
                )
                .await
                .map_err(|error| error.to_string())
            }
        },
        on_report,
        on_error,
    )
}

pub fn schedule_cluster_dfs_metadata_publish_after_events_with_async_peer_transport_on_runtime<
    R,
    E,
>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_cluster_dfs_metadata_publish_after_events_async_on_runtime(
        runtime,
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        events,
        schedule_options,
        replication_options,
        on_report,
        on_error,
    )
}

pub fn schedule_cluster_dfs_metadata_publish_after_events_with_peer_transports_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    events: &[NetworkedDfsServiceEvent],
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> Option<DfsMetadataPublishScheduleStart>
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_default_dfs_metadata_publish_after_events_async_on_runtime(
        runtime,
        events,
        schedule_options,
        move || {
            let data_dir = data_dir.clone();
            let cluster_paths = cluster_paths.clone();
            let sync_peer_transport = Arc::clone(&sync_peer_transport);
            let peer_transport = Arc::clone(&peer_transport);
            async move {
                publish_cluster_dfs_metadata_journal_with_peer_transports_async(
                    &data_dir,
                    &cluster_paths,
                    sync_peer_transport,
                    peer_transport,
                    replication_options,
                )
                .await
                .map_err(|error| error.to_string())
            }
        },
        on_report,
        on_error,
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataPublishScheduleOptions {
    pub max_retry_rounds: usize,
    pub retry_delay: Duration,
}

impl Default for DfsMetadataPublishScheduleOptions {
    fn default() -> Self {
        Self {
            max_retry_rounds: 3,
            retry_delay: Duration::from_millis(750),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsMetadataPublishScheduleStart {
    Started,
    AlreadyRunning,
}

#[derive(Debug)]
pub struct DfsMetadataPublishScheduler {
    pending: AtomicBool,
    running: AtomicBool,
}

impl Default for DfsMetadataPublishScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl DfsMetadataPublishScheduler {
    pub const fn new() -> Self {
        Self {
            pending: AtomicBool::new(false),
            running: AtomicBool::new(false),
        }
    }

    fn claim_publish_start(&'static self) -> DfsMetadataPublishScheduleStart {
        self.pending.store(true, Ordering::Release);
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            DfsMetadataPublishScheduleStart::AlreadyRunning
        } else {
            DfsMetadataPublishScheduleStart::Started
        }
    }

    pub fn request_publish_async_on_runtime<P, Fut, R, E>(
        &'static self,
        runtime: tokio::runtime::Handle,
        options: DfsMetadataPublishScheduleOptions,
        publish: P,
        on_report: R,
        on_error: E,
    ) -> DfsMetadataPublishScheduleStart
    where
        P: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
        R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
        E: FnMut(String) + Send + 'static,
    {
        let start = self.claim_publish_start();
        if start == DfsMetadataPublishScheduleStart::Started {
            runtime.spawn(async move {
                self.run_publish_loop_async(options, publish, on_report, on_error)
                    .await;
            });
        }
        start
    }

    async fn run_publish_loop_async<P, Fut, R, E>(
        &'static self,
        options: DfsMetadataPublishScheduleOptions,
        mut publish: P,
        mut on_report: R,
        mut on_error: E,
    ) where
        P: FnMut() -> Fut,
        Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>>,
        R: FnMut(DfsMetadataJournalReplicationReport),
        E: FnMut(String),
    {
        let mut retry_rounds = 0usize;
        loop {
            self.pending.store(false, Ordering::Release);
            let needs_retry = match publish().await {
                Ok(report) => {
                    let needs_retry = dfs_metadata_replication_report_needs_retry(&report);
                    on_report(report);
                    needs_retry
                }
                Err(error) => {
                    on_error(error);
                    true
                }
            };
            if self.pending.load(Ordering::Acquire) {
                retry_rounds = 0;
                continue;
            }
            if needs_retry && retry_rounds < options.max_retry_rounds {
                retry_rounds += 1;
                tokio::time::sleep(options.retry_delay).await;
                continue;
            }
            self.running.store(false, Ordering::Release);
            if self.pending.load(Ordering::Acquire)
                && self
                    .running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                retry_rounds = 0;
                continue;
            }
            break;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMetadataJournalReplicationTransportError {
    MissingRoute,
    Failed { reason: String },
}

pub trait DfsMetadataJournalReplicationTransport {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool;

    fn send_metadata_journal_batch(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>;

    fn send_metadata_snapshot_install(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>;
}

pub type DfsMetadataJournalReplicationTransportFuture<'a, T> =
    Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait AsyncDfsMetadataJournalReplicationTransport: Send {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool;

    fn send_metadata_journal_batch_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>,
    >;

    fn send_metadata_snapshot_install_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>,
    >;
}

pub const DFS_METADATA_REPLICATION_MAX_NODE_ROUTES_PER_TARGET: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataPeerTransportError {
    detail: String,
}

impl DfsMetadataPeerTransportError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for DfsMetadataPeerTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for DfsMetadataPeerTransportError {}

pub type DfsMetadataJournalBatchSender = Arc<
    dyn Fn(
            &NodeId,
            &DfsMetadataJournalBatchCommand,
            Duration,
        ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataPeerTransportError>
        + Send
        + Sync,
>;

pub type DfsMetadataSnapshotInstallSender = Arc<
    dyn Fn(
            &NodeId,
            &DfsMetadataSnapshotInstallCommand,
            Duration,
        ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataPeerTransportError>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct DfsMetadataNodeTransport {
    peer_node_ids: BTreeSet<NodeId>,
    sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
}

impl std::fmt::Debug for DfsMetadataNodeTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DfsMetadataNodeTransport")
            .field("peer_node_ids", &self.peer_node_ids)
            .finish_non_exhaustive()
    }
}

impl DfsMetadataNodeTransport {
    pub fn new(
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: DfsMetadataJournalBatchSender,
        snapshot_sender: DfsMetadataSnapshotInstallSender,
    ) -> Self {
        Self {
            peer_node_ids: peer_node_ids.into_iter().collect(),
            sender,
            snapshot_sender,
        }
    }

    fn has_peer_node(&self, target_node_id: &NodeId) -> bool {
        self.peer_node_ids.contains(target_node_id)
    }
}

fn validate_dfs_metadata_journal_batch_response(
    target_node_id: &NodeId,
    command: &DfsMetadataJournalBatchCommand,
    response: &DfsMetadataJournalBatchResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != command.batch.shard_id
        || response.next_after_sequence != command.batch.next_after_sequence
    {
        return Err(format!(
            "{target_node_id} returned mismatched metadata journal response"
        ));
    }
    Ok(())
}

fn validate_dfs_metadata_journal_replication_response(
    command: &DfsMetadataJournalBatchCommand,
    shard_id: &DfsMetadataShardId,
    response: &DfsMetadataJournalBatchResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != *shard_id
        || response.next_after_sequence != command.batch.next_after_sequence
    {
        return Err("metadata journal replication returned mismatched response".to_string());
    }
    Ok(())
}

fn validate_dfs_metadata_snapshot_install_response(
    target_node_id: &NodeId,
    command: &DfsMetadataSnapshotInstallCommand,
    response: &DfsMetadataSnapshotInstallResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != command.shard_id
        || response.next_after_sequence != command.next_after_sequence
    {
        return Err(format!(
            "{target_node_id} returned mismatched metadata snapshot install response"
        ));
    }
    Ok(())
}

fn validate_dfs_metadata_snapshot_install_replication_response(
    command: &DfsMetadataSnapshotInstallCommand,
    response: &DfsMetadataSnapshotInstallResponse,
) -> Result<(), String> {
    if response.request_id != command.request_id
        || response.shard_id != command.shard_id
        || response.next_after_sequence != command.next_after_sequence
    {
        return Err("metadata snapshot install returned mismatched response".to_string());
    }
    Ok(())
}

impl DfsMetadataJournalReplicationTransport for DfsMetadataNodeTransport {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool {
        self.has_peer_node(target_node_id)
    }

    fn send_metadata_journal_batch(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError> {
        if !self.has_peer_node(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        match (self.sender)(target_node_id, command, timeout) {
            Ok(response) => {
                validate_dfs_metadata_journal_batch_response(target_node_id, command, &response)
                    .map_err(
                        |reason| DfsMetadataJournalReplicationTransportError::Failed { reason },
                    )?;
                Ok(response)
            }
            Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                reason: error.to_string(),
            }),
        }
    }

    fn send_metadata_snapshot_install(
        &mut self,
        target_node_id: &NodeId,
        command: &DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>
    {
        if !self.has_peer_node(target_node_id) {
            return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
        }
        match (self.snapshot_sender)(target_node_id, command, timeout) {
            Ok(response) => {
                validate_dfs_metadata_snapshot_install_response(target_node_id, command, &response)
                    .map_err(
                        |reason| DfsMetadataJournalReplicationTransportError::Failed { reason },
                    )?;
                Ok(response)
            }
            Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                reason: error.to_string(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct AsyncDfsMetadataNodeTransport {
    peer_node_ids: BTreeSet<NodeId>,
    sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
}

impl std::fmt::Debug for AsyncDfsMetadataNodeTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AsyncDfsMetadataNodeTransport")
            .field("peer_node_ids", &self.peer_node_ids)
            .finish_non_exhaustive()
    }
}

impl AsyncDfsMetadataNodeTransport {
    pub fn new(
        peer_node_ids: impl IntoIterator<Item = NodeId>,
        sender: AsyncDfsMetadataJournalBatchSender,
        snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    ) -> Self {
        Self {
            peer_node_ids: peer_node_ids.into_iter().collect(),
            sender,
            snapshot_sender,
        }
    }

    fn has_peer_node(&self, target_node_id: &NodeId) -> bool {
        self.peer_node_ids.contains(target_node_id)
    }
}

impl AsyncDfsMetadataJournalReplicationTransport for AsyncDfsMetadataNodeTransport {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool {
        self.has_peer_node(target_node_id)
    }

    fn send_metadata_journal_batch_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>,
    > {
        Box::pin(async move {
            if !self.has_peer_node(target_node_id) {
                return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
            }
            match (self.sender)(target_node_id, command, timeout).await {
                Ok(response) => {
                    validate_dfs_metadata_journal_batch_response(
                        target_node_id,
                        command,
                        &response,
                    )
                    .map_err(|reason| {
                        DfsMetadataJournalReplicationTransportError::Failed { reason }
                    })?;
                    Ok(response)
                }
                Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                    reason: error.to_string(),
                }),
            }
        })
    }

    fn send_metadata_snapshot_install_async<'a>(
        &'a mut self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataSnapshotInstallCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataSnapshotInstallResponse, DfsMetadataJournalReplicationTransportError>,
    > {
        Box::pin(async move {
            if !self.has_peer_node(target_node_id) {
                return Err(DfsMetadataJournalReplicationTransportError::MissingRoute);
            }
            match (self.snapshot_sender)(target_node_id, command, timeout).await {
                Ok(response) => {
                    validate_dfs_metadata_snapshot_install_response(
                        target_node_id,
                        command,
                        &response,
                    )
                    .map_err(|reason| {
                        DfsMetadataJournalReplicationTransportError::Failed { reason }
                    })?;
                    Ok(response)
                }
                Err(error) => Err(DfsMetadataJournalReplicationTransportError::Failed {
                    reason: error.to_string(),
                }),
            }
        })
    }
}

pub trait DfsMetadataJournalPublisherHost: Send {
    fn open_primary_shard_backend(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Box<dyn DfsMetadataBackend>, DfsError>;

    fn metadata_replication_cursor(
        &mut self,
        backend: &mut dyn DfsMetadataBackend,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64, DfsError> {
        Ok(backend.metadata_replication_cursor(target_node_id, shard_id)?)
    }
}

#[derive(Clone, Debug)]
pub struct LocalDfsMetadataJournalPublisherHost {
    data_dir: SinomoDataDir,
}

impl LocalDfsMetadataJournalPublisherHost {
    pub fn new(data_dir: SinomoDataDir) -> Self {
        Self { data_dir }
    }
}

impl DfsMetadataJournalPublisherHost for LocalDfsMetadataJournalPublisherHost {
    fn open_primary_shard_backend(
        &mut self,
        shard_id: &DfsMetadataShardId,
    ) -> Result<Box<dyn DfsMetadataBackend>, DfsError> {
        Ok(Box::new(open_local_metadata_shard_backend(
            &self.data_dir,
            shard_id.clone(),
        )?))
    }

    fn metadata_replication_cursor(
        &mut self,
        backend: &mut dyn DfsMetadataBackend,
        target_node_id: &NodeId,
        shard_id: &DfsMetadataShardId,
    ) -> Result<u64, DfsError> {
        migrate_legacy_dfs_metadata_replication_cursor(
            backend,
            &self.data_dir,
            target_node_id,
            shard_id,
        )
    }
}

#[derive(Clone, Debug)]
struct DfsPreparedClusterMetadataJournalPublish {
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    outcomes: Vec<DfsMetadataJournalReplicationOutcome>,
    targets: Vec<DfsPreparedMetadataJournalTargetPublish>,
}

#[derive(Clone, Debug)]
pub(crate) struct DfsPreparedMetadataJournalTargetPublish {
    pub(crate) target_node_id: NodeId,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) shard_epoch: u64,
    pub(crate) after_sequence: u64,
    pub(crate) commands: Vec<DfsMetadataJournalBatchCommand>,
    pub(crate) backlog_after_sequence: Option<u64>,
}

fn prepare_cluster_dfs_metadata_journal_publish(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }

    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DfsError::from_network)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let shards = cluster_dfs_metadata_shards(&cluster_paths, &file_system.file_system_id)?;
    let peer_node_ids = cluster_dfs_peer_node_ids(&state);
    let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
        &default_dfs_metadata_replication_suppression_snapshot_path(&data_dir),
    )?
    .map(|snapshot| snapshot.records)
    .unwrap_or_default();
    let mut host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system.file_system_id,
        &state.local_node_id,
        shards,
        &mut host,
        &metadata_suppressions,
        options,
    )?;
    Ok(Some(DfsPreparedClusterMetadataJournalPublish {
        data_dir,
        local_node_id: state.local_node_id,
        peer_node_ids,
        outcomes,
        targets,
    }))
}

async fn prepare_cluster_dfs_metadata_journal_publish_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    let data_dir = data_dir.clone();
    let cluster_paths = cluster_paths.clone();
    tokio::task::spawn_blocking(move || {
        prepare_cluster_dfs_metadata_journal_publish(data_dir, cluster_paths, options)
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata publish planning task failed: {source}"),
    })?
}

pub(crate) fn prepare_dfs_metadata_journal_publish_with_host(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    mut shards: Vec<DfsMetadataShardConfig>,
    host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_suppressions: &[DfsMetadataReplicationSuppressionRecord],
    options: DfsMetadataJournalReplicationOptions,
) -> Result<
    (
        Vec<DfsMetadataJournalReplicationOutcome>,
        Vec<DfsPreparedMetadataJournalTargetPublish>,
    ),
    DfsError,
> {
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok((
            vec![DfsMetadataJournalReplicationOutcome::MissingShard {
                file_system_id,
                shard_id: DfsMetadataShardId::new("root"),
            }],
            Vec::new(),
        ));
    }

    let mut outcomes = Vec::new();
    let mut targets = Vec::new();
    for shard in shards {
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        if shard.primary_node_id != *local_node_id {
            outcomes.push(DfsMetadataJournalReplicationOutcome::NotPrimary {
                file_system_id: shard.file_system_id,
                shard_id,
                local_node_id: local_node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
            continue;
        }

        let mut backend = host.open_primary_shard_backend(&shard_id)?;
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != local_node_id)
        {
            if let Some(suppression) = dfs_metadata_replication_suppression_for(
                metadata_suppressions,
                &target_node_id,
                &shard_id,
                shard.epoch,
            ) {
                outcomes.push(
                    DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed {
                        target_node_id,
                        shard_id: shard_id.clone(),
                        epoch: shard.epoch,
                        reason: suppression.reason.clone(),
                    },
                );
                continue;
            }
            let after_sequence =
                host.metadata_replication_cursor(&mut *backend, &target_node_id, &shard_id)?;
            match prepare_dfs_metadata_journal_target_publish(
                &mut *backend,
                local_node_id,
                target_node_id,
                shard.file_system_id.clone(),
                shard_id.clone(),
                shard.epoch,
                after_sequence,
                options,
            )? {
                DfsPreparedMetadataJournalTargetPublishResult::NoChanges { outcome } => {
                    outcomes.push(outcome);
                }
                DfsPreparedMetadataJournalTargetPublishResult::Publish { target } => {
                    targets.push(target);
                }
            }
        }
    }

    Ok((outcomes, targets))
}

enum DfsPreparedMetadataJournalTargetPublishResult {
    NoChanges {
        outcome: DfsMetadataJournalReplicationOutcome,
    },
    Publish {
        target: DfsPreparedMetadataJournalTargetPublish,
    },
}

fn prepare_dfs_metadata_journal_target_publish(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: NodeId,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    shard_epoch: u64,
    after_sequence: u64,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsPreparedMetadataJournalTargetPublishResult, DfsError> {
    let mut next_after_sequence = after_sequence;
    let mut commands = Vec::new();
    let mut backlog_after_sequence = None;
    loop {
        let Some(batch) = backend.journal_batch_since_limited(
            next_after_sequence,
            options.batch_entry_limit,
            options.batch_byte_limit,
        )?
        else {
            if commands.is_empty() {
                return Ok(DfsPreparedMetadataJournalTargetPublishResult::NoChanges {
                    outcome: DfsMetadataJournalReplicationOutcome::NoChanges {
                        target_node_id,
                        shard_id,
                        after_sequence,
                    },
                });
            }
            break;
        };

        next_after_sequence = batch.next_after_sequence;
        commands.push(dfs_metadata_journal_batch_command(
            local_node_id,
            &target_node_id,
            batch,
        ));
        if commands.len() >= options.max_batches_per_target {
            if backend
                .journal_batch_since_limited(
                    next_after_sequence,
                    options.batch_entry_limit,
                    options.batch_byte_limit,
                )?
                .is_some()
            {
                backlog_after_sequence = Some(next_after_sequence);
            }
            break;
        }
    }

    Ok(DfsPreparedMetadataJournalTargetPublishResult::Publish {
        target: DfsPreparedMetadataJournalTargetPublish {
            target_node_id,
            file_system_id,
            shard_id,
            shard_epoch,
            after_sequence,
            commands,
            backlog_after_sequence,
        },
    })
}

fn dfs_metadata_replication_suppression_for<'a>(
    records: &'a [DfsMetadataReplicationSuppressionRecord],
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
) -> Option<&'a DfsMetadataReplicationSuppressionRecord> {
    records.iter().find(|record| {
        record.target_node_id == *target_node_id
            && record.shard_id == *shard_id
            && record.shard_epoch == shard_epoch
    })
}

async fn put_local_dfs_metadata_replication_suppression_async(
    data_dir: SinomoDataDir,
    record: DfsMetadataReplicationSuppressionRecord,
) -> Result<(), DfsError> {
    tokio::task::spawn_blocking(move || {
        put_local_dfs_metadata_replication_suppression(&data_dir, record)
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata replication suppression write task failed: {source}"),
    })?
}

async fn record_dfs_metadata_replication_topology_mismatch_async(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    reason: &str,
) -> Result<(), DfsError> {
    if !metadata_replication_rejection_is_topology_mismatch(reason) {
        return Ok(());
    }
    put_local_dfs_metadata_replication_suppression_async(
        data_dir.clone(),
        DfsMetadataReplicationSuppressionRecord {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            shard_epoch,
            reason: reason.to_owned(),
            updated_at_unix_nanos: dfs_now_unix_nanos(),
        },
    )
    .await
}

async fn execute_prepared_dfs_metadata_journal_publish_async(
    plan: DfsPreparedClusterMetadataJournalPublish,
    transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let mut outcomes = plan.outcomes;
    for target in plan.targets {
        execute_prepared_dfs_metadata_journal_target_publish_async(
            &plan.data_dir,
            &plan.local_node_id,
            target,
            transport,
            options,
            &mut outcomes,
        )
        .await?;
    }
    Ok(DfsMetadataJournalReplicationReport { outcomes })
}

async fn execute_prepared_dfs_metadata_journal_target_publish_async(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target: DfsPreparedMetadataJournalTargetPublish,
    transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    let DfsPreparedMetadataJournalTargetPublish {
        target_node_id,
        file_system_id,
        shard_id,
        shard_epoch,
        mut after_sequence,
        commands,
        backlog_after_sequence,
    } = target;
    if !transport.has_metadata_replication_route(&target_node_id) {
        outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id,
            shard_id,
            after_sequence,
        });
        return Ok(());
    }

    let target_started_at = Instant::now();
    let mut completed_plan = true;
    for command in commands {
        let Some(attempt_timeout) =
            dfs_metadata_replication_attempt_timeout(options, target_started_at)
        else {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
            completed_plan = false;
            break;
        };
        let response = match transport
            .send_metadata_journal_batch_async(&target_node_id, &command, attempt_timeout)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                push_dfs_metadata_transport_error_outcome(
                    outcomes,
                    &target_node_id,
                    &shard_id,
                    after_sequence,
                    error,
                );
                completed_plan = false;
                break;
            }
        };
        if let Err(reason) =
            validate_dfs_metadata_journal_replication_response(&command, &shard_id, &response)
        {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
            completed_plan = false;
            break;
        }
        match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                let next_after_sequence = command.batch.next_after_sequence;
                put_local_dfs_metadata_replication_cursor_async(
                    data_dir.clone(),
                    target_node_id.clone(),
                    shard_id.clone(),
                    next_after_sequence,
                )
                .await?;
                after_sequence = next_after_sequence;
                outcomes.push(DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    entries: command.batch.entries.len(),
                    next_after_sequence,
                });
            }
            DfsMetadataJournalBatchResult::Rejected { reason } => {
                if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                    execute_prepared_dfs_metadata_snapshot_repair_async(
                        data_dir,
                        local_node_id,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        &file_system_id,
                        after_sequence,
                        reason,
                        transport,
                        options,
                        target_started_at,
                        outcomes,
                    )
                    .await?;
                } else {
                    record_dfs_metadata_replication_topology_mismatch_async(
                        data_dir,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        &reason,
                    )
                    .await?;
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        reason,
                    });
                }
                completed_plan = false;
                break;
            }
        }
    }

    if completed_plan {
        if let Some(after_sequence) = backlog_after_sequence {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                shard_id,
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
        }
    }
    Ok(())
}

async fn execute_prepared_dfs_metadata_snapshot_repair_async(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    file_system_id: &DfsFileSystemId,
    after_sequence: u64,
    reason: String,
    transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    target_started_at: Instant,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    let command = build_local_dfs_metadata_snapshot_install_command_async(
        data_dir.clone(),
        local_node_id.clone(),
        target_node_id.clone(),
        file_system_id.clone(),
        shard_epoch,
        shard_id.clone(),
        reason.clone(),
    )
    .await?;
    let Some(attempt_timeout) =
        dfs_metadata_replication_attempt_timeout(options, target_started_at)
    else {
        outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            after_sequence,
            max_batches: options.max_batches_per_target,
        });
        return Ok(());
    };
    let response = match transport
        .send_metadata_snapshot_install_async(target_node_id, &command, attempt_timeout)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            push_dfs_metadata_snapshot_transport_error_outcome(
                outcomes,
                target_node_id,
                shard_id,
                after_sequence,
                reason,
                error,
            );
            return Ok(());
        }
    };
    if let Err(reason) =
        validate_dfs_metadata_snapshot_install_replication_response(&command, &response)
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            reason,
        });
        return Ok(());
    }
    match response.result {
        DfsMetadataSnapshotInstallResult::Installed { archived_existing } => {
            put_local_dfs_metadata_replication_cursor_async(
                data_dir.clone(),
                target_node_id.clone(),
                shard_id.clone(),
                command.next_after_sequence,
            )
            .await?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                archived_existing,
            });
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
        } => {
            let reason = format!("{reason}; snapshot repair rejected: {snapshot_reason}");
            record_dfs_metadata_replication_topology_mismatch_async(
                data_dir,
                target_node_id,
                shard_id,
                shard_epoch,
                &reason,
            )
            .await?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
        }
    }
    Ok(())
}

async fn build_local_dfs_metadata_snapshot_install_command_async(
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
    target_node_id: NodeId,
    file_system_id: DfsFileSystemId,
    authority_epoch: u64,
    shard_id: DfsMetadataShardId,
    reason: String,
) -> Result<DfsMetadataSnapshotInstallCommand, DfsError> {
    tokio::task::spawn_blocking(move || {
        let mut backend = open_local_metadata_shard_backend(&data_dir, shard_id.clone())?;
        dfs_metadata_snapshot_install_command(
            &mut backend,
            &local_node_id,
            &target_node_id,
            &file_system_id,
            authority_epoch,
            &shard_id,
            reason,
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata snapshot planning task failed: {source}"),
    })?
}

async fn put_local_dfs_metadata_replication_cursor_async(
    data_dir: SinomoDataDir,
    target_node_id: NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<(), DfsError> {
    tokio::task::spawn_blocking(move || {
        // Cursor acknowledgement is independent of materialized metadata
        // tables. Opening a complete shard backend here replays and decodes
        // those tables once per replicated batch, which can dominate publish
        // latency on large shards. Keep the same shard write lock and durable
        // monotonic cursor update without loading unrelated table state.
        let root = local_dfs_metadata_shard_path(&data_dir, &shard_id);
        let _lock = metadata_lock::metadata_write_lock(
            root.clone(),
            metadata_cursor::journal_write_lock_path(&root),
        )?;
        Ok(metadata_cursor::put_metadata_replication_cursor(
            &root,
            &target_node_id,
            &shard_id,
            next_after_sequence,
        )?)
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata cursor commit task failed: {source}"),
    })?
}

pub fn publish_dfs_metadata_journal_with_host(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    mut shards: Vec<DfsMetadataShardConfig>,
    host: &mut impl DfsMetadataJournalPublisherHost,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    shards.sort_by(|left, right| left.shard_id.cmp(&right.shard_id));
    if shards.is_empty() {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::MissingShard {
                file_system_id,
                shard_id: DfsMetadataShardId::new("root"),
            }],
        });
    }

    let mut outcomes = Vec::new();
    for shard in shards {
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        if shard.primary_node_id != *local_node_id {
            outcomes.push(DfsMetadataJournalReplicationOutcome::NotPrimary {
                file_system_id: shard.file_system_id,
                shard_id,
                local_node_id: local_node_id.clone(),
                primary_node_id: shard.primary_node_id,
                epoch: shard.epoch,
            });
            continue;
        }

        let mut backend = host.open_primary_shard_backend(&shard_id)?;
        for target_node_id in shard
            .replica_node_ids
            .into_iter()
            .filter(|target_node_id| target_node_id != local_node_id)
        {
            let after_sequence =
                host.metadata_replication_cursor(&mut *backend, &target_node_id, &shard_id)?;
            outcomes.extend(replicate_dfs_metadata_shard_to_target(
                &mut *backend,
                local_node_id,
                &target_node_id,
                &shard.file_system_id,
                shard.epoch,
                &shard_id,
                after_sequence,
                transport,
                options,
            )?);
        }
    }

    Ok(DfsMetadataJournalReplicationReport { outcomes })
}

pub async fn publish_dfs_metadata_journal_with_async_host(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    shards: Vec<DfsMetadataShardConfig>,
    data_dir: &SinomoDataDir,
    host: &mut impl DfsMetadataJournalPublisherHost,
    transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
        &default_dfs_metadata_replication_suppression_snapshot_path(data_dir),
    )?
    .map(|snapshot| snapshot.records)
    .unwrap_or_default();
    let (outcomes, targets) = prepare_dfs_metadata_journal_publish_with_host(
        file_system_id,
        local_node_id,
        shards,
        host,
        &metadata_suppressions,
        options,
    )?;
    execute_prepared_dfs_metadata_journal_publish_async(
        DfsPreparedClusterMetadataJournalPublish {
            data_dir: data_dir.clone(),
            local_node_id: local_node_id.clone(),
            peer_node_ids: Vec::new(),
            outcomes,
            targets,
        },
        transport,
        options,
    )
    .await
}

pub fn process_dfs_metadata_journal_batch(
    backend: &mut dyn DfsMetadataBackend,
    context: Option<&DfsMetadataReplicaShardContext>,
    command: DfsMetadataJournalBatchCommand,
) -> DfsMetadataJournalBatchResponse {
    if let Some(response) = validate_dfs_metadata_journal_batch_source(context, &command) {
        return response;
    }
    let shard_id = command.batch.shard_id.clone();
    let next_after_sequence = command.batch.next_after_sequence;
    if command.verify_prefix_only {
        return match verify_dfs_metadata_journal_prefix(backend, &command) {
            Ok(()) => DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: true,
                result: DfsMetadataJournalBatchResult::Applied,
            },
            Err(reason) => DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: false,
                result: DfsMetadataJournalBatchResult::Rejected { reason },
            },
        };
    }
    match backend.apply_journal_batch(&command.batch) {
        Ok(applied_entries) => DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries,
            verified_prefix_only: false,
            result: DfsMetadataJournalBatchResult::Applied,
        },
        Err(error) => DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries: 0,
            verified_prefix_only: false,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: error.to_string(),
            },
        },
    }
}

fn verify_dfs_metadata_journal_prefix(
    backend: &mut dyn DfsMetadataBackend,
    command: &DfsMetadataJournalBatchCommand,
) -> Result<(), String> {
    let batch = &command.batch;
    let Some(probe) = command.prefix_probe.as_ref() else {
        return Err(
            "metadata journal prefix probe is missing its exact checkpoint identity".to_string(),
        );
    };
    if !batch.entries.is_empty() {
        return Err(
            "metadata journal prefix probe must carry an empty rolling-upgrade-safe batch"
                .to_string(),
        );
    }
    if probe.barrier_id.is_empty()
        || probe.state_fingerprint.is_empty()
        || probe.shard_id != batch.shard_id
        || probe.journal_sequence != batch.next_after_sequence
    {
        return Err(format!(
            "metadata journal prefix probe identity mismatch for shard {} sequence {}",
            batch.shard_id.0, batch.next_after_sequence
        ));
    }
    let local_fingerprint = crate::service_sync::metadata_shard_state_fingerprint_at(
        backend,
        &probe.shard_id,
        probe.journal_sequence,
    )
    .map_err(|error| {
        format!(
            "metadata journal prefix probe state mismatch for shard {} sequence {}: {error}",
            batch.shard_id.0, probe.journal_sequence
        )
    })?;
    if local_fingerprint != probe.state_fingerprint {
        return Err(format!(
            "metadata journal prefix probe state mismatch for shard {} sequence {}",
            batch.shard_id.0, probe.journal_sequence
        ));
    }
    backend
        .put_metadata_prefix_ack_floor(&crate::DfsMetadataPrefixAckFloor {
            shard_id: probe.shard_id.clone(),
            journal_sequence: probe.journal_sequence,
            state_fingerprint: probe.state_fingerprint.clone(),
        })
        .map_err(|error| {
            format!(
                "persist metadata prefix acknowledgement floor for shard {} sequence {}: {error}",
                batch.shard_id.0, probe.journal_sequence
            )
        })?;
    Ok(())
}

pub fn cluster_dfs_metadata_replica_shard_context(
    cluster_paths: &ClusterPaths,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataReplicaShardContext>, DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    let mut shard = None;
    for entry in sinomo_core::kv::read_cluster_kv_prefix_entries(
        &cluster_paths.kv_paths(),
        &format!("{}/", metadata_shards_prefix(&sifs_file_system_id())),
    )? {
        let candidate: DfsMetadataShardConfig = entry.decode()?;
        if candidate.shard_id == shard_id.0.as_str() {
            shard = Some(candidate);
            break;
        }
    }
    Ok(shard.map(|shard| DfsMetadataReplicaShardContext {
        local_node_id: state.local_node_id,
        shard,
    }))
}

pub fn process_cluster_dfs_metadata_journal_batch(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let context =
        cluster_dfs_metadata_replica_shard_context(cluster_paths, &command.batch.shard_id)?;
    let mut backend = open_local_metadata_shard_backend(data_dir, command.batch.shard_id.clone())?;
    Ok(process_dfs_metadata_journal_batch(
        &mut backend,
        context.as_ref(),
        command,
    ))
}

pub fn process_dfs_metadata_snapshot_install(
    backend: &mut dyn DfsMetadataBackend,
    context: Option<&DfsMetadataReplicaShardContext>,
    command: DfsMetadataSnapshotInstallCommand,
) -> DfsMetadataSnapshotInstallResponse {
    if let Some(response) = validate_dfs_metadata_snapshot_install_source(context, &command) {
        return response;
    }
    let request_id = command.request_id.clone();
    let shard_id = command.shard_id.clone();
    let next_after_sequence = command.next_after_sequence;
    match validate_dfs_metadata_snapshot_install_state(backend, &command) {
        Ok(Some(reason)) => {
            return DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Rejected { reason },
            };
        }
        Err(error) => {
            return DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Rejected {
                    reason: error.to_string(),
                },
            };
        }
        Ok(None) => {}
    }
    match backend.replace_with_replicated_snapshot(
        command.shard_id,
        command.next_after_sequence,
        command.snapshot,
    ) {
        Ok(archived_existing) => DfsMetadataSnapshotInstallResponse {
            request_id,
            shard_id,
            next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Installed { archived_existing },
        },
        Err(error) => DfsMetadataSnapshotInstallResponse {
            request_id,
            shard_id,
            next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: error.to_string(),
            },
        },
    }
}

pub(crate) fn validate_dfs_metadata_snapshot_install_state(
    backend: &dyn DfsMetadataBackend,
    command: &DfsMetadataSnapshotInstallCommand,
) -> Result<Option<String>, DfsError> {
    if command.snapshot.materialized_through_sequence != Some(command.next_after_sequence) {
        return Ok(Some(format!(
            "metadata snapshot embedded sequence {:?} does not match install sequence {} for shard {}",
            command.snapshot.materialized_through_sequence,
            command.next_after_sequence,
            command.shard_id.0
        )));
    }
    let Some(floor) = backend.metadata_prefix_ack_floor(&command.shard_id)? else {
        return Ok(None);
    };
    if command.next_after_sequence < floor.journal_sequence {
        return Ok(Some(format!(
            "metadata snapshot sequence {} would roll shard {} behind locally acknowledged checkpoint {}",
            command.next_after_sequence, command.shard_id.0, floor.journal_sequence
        )));
    }
    if command.next_after_sequence == floor.journal_sequence {
        let fingerprint = crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(
            &command.snapshot,
        )
        .map_err(|source| DfsError::Serialization {
            message: format!(
                "encode metadata snapshot local acknowledgement check for {}: {source}",
                command.shard_id.0
            ),
        })?;
        if fingerprint != floor.state_fingerprint {
            return Ok(Some(format!(
                "metadata snapshot state does not match locally acknowledged checkpoint for shard {} sequence {}",
                command.shard_id.0, command.next_after_sequence
            )));
        }
    }
    Ok(None)
}

pub fn process_cluster_dfs_metadata_snapshot_install(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    command: DfsMetadataSnapshotInstallCommand,
) -> Result<DfsMetadataSnapshotInstallResponse, DfsError> {
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let context = cluster_dfs_metadata_replica_shard_context(cluster_paths, &command.shard_id)?;
    let mut backend = open_local_metadata_shard_backend(data_dir, command.shard_id.clone())?;
    Ok(process_dfs_metadata_snapshot_install(
        &mut backend,
        context.as_ref(),
        command,
    ))
}

pub fn materialize_default_sifs_metadata(
    backend: &mut dyn DfsMetadataBackend,
    file_system: &DfsFileSystem,
    bootstrap_owner_node_ids: impl IntoIterator<Item = NodeId>,
    fallback_owner_node_id: NodeId,
    created_at: OffsetDateTime,
) -> Result<(), DfsError> {
    backend.ensure_file_system(file_system)?;
    for object in distributed_sifs_bootstrap_objects_for_nodes(
        bootstrap_owner_node_ids,
        fallback_owner_node_id,
    ) {
        if backend
            .get_object_by_path(&object.file_system_id, object.path.trim_matches('/'))?
            .is_none()
        {
            backend.put_object(&object)?;
            backend.put_version(&DfsObjectVersionRecord {
                file_system_id: object.file_system_id.clone(),
                object_id: object.object_id.clone(),
                version: object.version.clone(),
                content_hash: object.content_hash.clone(),
                size_bytes: object.content_size_bytes,
                locations: object.current_locations.clone(),
                created_by_event_id: object.created_by_event_id.clone(),
                created_at,
                visible: true,
            })?;
        }
    }
    Ok(())
}

pub fn materialize_default_sifs_metadata_on_shards(
    backend: &mut LocalShardedMetadataBackend,
    file_system: &DfsFileSystem,
    bootstrap_owner_node_ids: impl IntoIterator<Item = NodeId>,
    fallback_owner_node_id: NodeId,
    created_at: OffsetDateTime,
) -> Result<(), DfsError> {
    backend.ensure_file_system_on_all_shards(file_system)?;
    materialize_default_sifs_metadata(
        backend,
        file_system,
        bootstrap_owner_node_ids,
        fallback_owner_node_id,
        created_at,
    )?;
    backend.migrate_objects_to_configured_shards(&file_system.file_system_id, usize::MAX)?;
    Ok(())
}

pub fn validate_dfs_metadata_journal_batch_source(
    context: Option<&DfsMetadataReplicaShardContext>,
    command: &DfsMetadataJournalBatchCommand,
) -> Option<DfsMetadataJournalBatchResponse> {
    let Some(context) = context else {
        return None;
    };
    if context.shard.shard_id != command.batch.shard_id.0 {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata journal batch has no local shard config".to_string(),
        ));
    }
    if command.verify_prefix_only
        && command.prefix_probe.as_ref().is_none_or(|probe| {
            probe.file_system_id.as_ref() != Some(&context.shard.file_system_id)
                || probe.authority_epoch != Some(context.shard.epoch)
        })
    {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata journal prefix probe authority does not match shard {} epoch {}",
                context.shard.shard_id, context.shard.epoch
            ),
        ));
    }
    let Some(source_node_id) = command.source_node_id.as_ref() else {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata journal batch is missing source node id".to_string(),
        ));
    };
    if source_node_id != &context.shard.primary_node_id {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata journal source {source_node_id} is not shard {} primary {}",
                context.shard.shard_id, context.shard.primary_node_id
            ),
        ));
    }
    if context.shard.primary_node_id == context.local_node_id {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata primary must not apply a replica journal batch".to_string(),
        ));
    }
    if !context
        .shard
        .replica_node_ids
        .contains(&context.local_node_id)
    {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "local node {} is not configured as a replica for metadata shard {}",
                context.local_node_id, context.shard.shard_id
            ),
        ));
    }
    None
}

pub fn validate_dfs_metadata_snapshot_install_source(
    context: Option<&DfsMetadataReplicaShardContext>,
    command: &DfsMetadataSnapshotInstallCommand,
) -> Option<DfsMetadataSnapshotInstallResponse> {
    let reject = |reason: String| DfsMetadataSnapshotInstallResponse {
        request_id: command.request_id.clone(),
        shard_id: command.shard_id.clone(),
        next_after_sequence: command.next_after_sequence,
        result: DfsMetadataSnapshotInstallResult::Rejected { reason },
    };

    let Some(context) = context else {
        return Some(reject(
            "metadata snapshot install has no local shard config".to_string(),
        ));
    };
    if context.shard.shard_id != command.shard_id.0 {
        return Some(reject(
            "metadata snapshot install has no local shard config".to_string(),
        ));
    }
    if command.file_system_id.as_ref() != Some(&context.shard.file_system_id)
        || command.authority_epoch != Some(context.shard.epoch)
    {
        return Some(reject(format!(
            "metadata snapshot authority identity does not match filesystem {} shard {} epoch {}",
            context.shard.file_system_id, context.shard.shard_id, context.shard.epoch
        )));
    }
    if context.shard.primary_node_id != command.source_node_id {
        return Some(reject(format!(
            "metadata snapshot source {} is not shard {} primary {}",
            command.source_node_id, context.shard.shard_id, context.shard.primary_node_id
        )));
    }
    if context.shard.primary_node_id == context.local_node_id {
        return Some(reject(
            "metadata primary must not install a replica snapshot".to_string(),
        ));
    }
    if !context
        .shard
        .replica_node_ids
        .contains(&context.local_node_id)
    {
        return Some(reject(format!(
            "local node {} is not configured as a replica for metadata shard {}",
            context.local_node_id, context.shard.shard_id
        )));
    }
    None
}

fn dfs_metadata_journal_rejection_response(
    command: &DfsMetadataJournalBatchCommand,
    reason: String,
) -> DfsMetadataJournalBatchResponse {
    DfsMetadataJournalBatchResponse {
        request_id: command.request_id.clone(),
        shard_id: command.batch.shard_id.clone(),
        next_after_sequence: command.batch.next_after_sequence,
        applied_entries: 0,
        verified_prefix_only: false,
        result: DfsMetadataJournalBatchResult::Rejected { reason },
    }
}

#[derive(Debug)]
enum DfsMetadataJournalReplicationStep {
    Continue {
        after_sequence: u64,
    },
    SnapshotRepair {
        reason: String,
        command: DfsMetadataSnapshotInstallCommand,
    },
    Terminal,
}

fn dfs_metadata_journal_batch_command(
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    batch: DfsMetadataJournalBatch,
) -> DfsMetadataJournalBatchCommand {
    let next_after_sequence = batch.next_after_sequence;
    DfsMetadataJournalBatchCommand {
        request_id: format!(
            "dfs_meta_{}_{}_{}",
            local_node_id, target_node_id, next_after_sequence
        ),
        source_node_id: Some(local_node_id.clone()),
        verify_prefix_only: false,
        prefix_probe: None,
        batch,
    }
}

fn dfs_metadata_snapshot_install_command(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    reason: String,
) -> Result<DfsMetadataSnapshotInstallCommand, DfsError> {
    let next_after_sequence = backend.journal_last_sequence()?;
    Ok(DfsMetadataSnapshotInstallCommand {
        request_id: format!(
            "dfs_meta_snapshot_{}_{}_{}",
            local_node_id, target_node_id, next_after_sequence
        ),
        source_node_id: local_node_id.clone(),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        shard_id: shard_id.clone(),
        next_after_sequence,
        snapshot: backend.export_table_snapshot()?,
        reason,
    })
}

fn push_dfs_metadata_transport_error_outcome(
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    after_sequence: u64,
    error: DfsMetadataJournalReplicationTransportError,
) {
    match error {
        DfsMetadataJournalReplicationTransportError::MissingRoute => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
            });
        }
        DfsMetadataJournalReplicationTransportError::Failed { reason } => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
        }
    }
}

fn handle_dfs_metadata_journal_batch_response(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    command: &DfsMetadataJournalBatchCommand,
    response: DfsMetadataJournalBatchResponse,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<DfsMetadataJournalReplicationStep, DfsError> {
    if let Err(reason) =
        validate_dfs_metadata_journal_replication_response(command, shard_id, &response)
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            reason,
        });
        return Ok(DfsMetadataJournalReplicationStep::Terminal);
    }

    let next_after_sequence = command.batch.next_after_sequence;
    match response.result {
        DfsMetadataJournalBatchResult::Applied => {
            backend.put_metadata_replication_cursor(
                target_node_id,
                shard_id,
                next_after_sequence,
            )?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::Applied {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                entries: command.batch.entries.len(),
                next_after_sequence,
            });
            Ok(DfsMetadataJournalReplicationStep::Continue {
                after_sequence: next_after_sequence,
            })
        }
        DfsMetadataJournalBatchResult::Rejected { reason } => {
            if metadata_journal_rejection_allows_snapshot_repair(&reason) {
                let command = dfs_metadata_snapshot_install_command(
                    backend,
                    local_node_id,
                    target_node_id,
                    file_system_id,
                    authority_epoch,
                    shard_id,
                    reason.clone(),
                )?;
                Ok(DfsMetadataJournalReplicationStep::SnapshotRepair { reason, command })
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    reason,
                });
                Ok(DfsMetadataJournalReplicationStep::Terminal)
            }
        }
    }
}

fn handle_dfs_metadata_snapshot_install_response(
    backend: &mut dyn DfsMetadataBackend,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    repair_reason: String,
    command: &DfsMetadataSnapshotInstallCommand,
    response: DfsMetadataSnapshotInstallResponse,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<DfsMetadataJournalReplicationStep, DfsError> {
    if let Err(reason) =
        validate_dfs_metadata_snapshot_install_replication_response(command, &response)
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            reason,
        });
        return Ok(DfsMetadataJournalReplicationStep::Terminal);
    }

    match response.result {
        DfsMetadataSnapshotInstallResult::Installed { archived_existing } => {
            backend.put_metadata_replication_cursor(
                target_node_id,
                shard_id,
                command.next_after_sequence,
            )?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                archived_existing,
            });
            Ok(DfsMetadataJournalReplicationStep::Continue {
                after_sequence: command.next_after_sequence,
            })
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
        } => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason: format!("{repair_reason}; snapshot repair rejected: {snapshot_reason}"),
            });
            Ok(DfsMetadataJournalReplicationStep::Terminal)
        }
    }
}

fn push_dfs_metadata_snapshot_transport_error_outcome(
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    after_sequence: u64,
    repair_reason: String,
    error: DfsMetadataJournalReplicationTransportError,
) {
    match error {
        DfsMetadataJournalReplicationTransportError::MissingRoute => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
            });
        }
        DfsMetadataJournalReplicationTransportError::Failed { reason } => {
            outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason: format!("{repair_reason}; snapshot repair failed: {reason}"),
            });
        }
    }
}

fn push_dfs_metadata_backlog_if_remaining(
    backend: &mut dyn DfsMetadataBackend,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    after_sequence: u64,
    options: DfsMetadataJournalReplicationOptions,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    if backend
        .journal_batch_since_limited(
            after_sequence,
            options.batch_entry_limit,
            options.batch_byte_limit,
        )?
        .is_some()
    {
        outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            after_sequence,
            max_batches: options.max_batches_per_target,
        });
    }
    Ok(())
}

pub fn replicate_dfs_metadata_shard_to_target(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    mut after_sequence: u64,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Vec<DfsMetadataJournalReplicationOutcome>, DfsError> {
    let mut outcomes = Vec::new();
    let mut sent_batches = 0usize;
    let target_started_at = Instant::now();
    loop {
        let Some(batch) = backend.journal_batch_since_limited(
            after_sequence,
            options.batch_entry_limit,
            options.batch_byte_limit,
        )?
        else {
            if sent_batches == 0 {
                outcomes.push(DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence,
                });
            }
            break;
        };
        if !transport.has_metadata_replication_route(target_node_id) {
            outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
            });
            break;
        }
        if sent_batches >= options.max_batches_per_target {
            push_dfs_metadata_backlog_if_remaining(
                backend,
                target_node_id,
                shard_id,
                after_sequence,
                options,
                &mut outcomes,
            )?;
            break;
        }

        let command = dfs_metadata_journal_batch_command(local_node_id, target_node_id, batch);
        let Some(attempt_timeout) =
            dfs_metadata_replication_attempt_timeout(options, target_started_at)
        else {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
            break;
        };
        let response = match transport.send_metadata_journal_batch(
            target_node_id,
            &command,
            attempt_timeout,
        ) {
            Ok(response) => response,
            Err(error) => {
                push_dfs_metadata_transport_error_outcome(
                    &mut outcomes,
                    target_node_id,
                    shard_id,
                    after_sequence,
                    error,
                );
                break;
            }
        };
        match handle_dfs_metadata_journal_batch_response(
            backend,
            local_node_id,
            target_node_id,
            file_system_id,
            authority_epoch,
            shard_id,
            &command,
            response,
            &mut outcomes,
        )? {
            DfsMetadataJournalReplicationStep::Continue {
                after_sequence: next_after_sequence,
            } => {
                after_sequence = next_after_sequence;
                sent_batches += 1;
            }
            DfsMetadataJournalReplicationStep::SnapshotRepair {
                reason,
                command: snapshot_command,
            } => {
                let Some(snapshot_attempt_timeout) =
                    dfs_metadata_replication_attempt_timeout(options, target_started_at)
                else {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence,
                        max_batches: options.max_batches_per_target,
                    });
                    break;
                };
                match transport.send_metadata_snapshot_install(
                    target_node_id,
                    &snapshot_command,
                    snapshot_attempt_timeout,
                ) {
                    Ok(snapshot_response) => {
                        match handle_dfs_metadata_snapshot_install_response(
                            backend,
                            target_node_id,
                            shard_id,
                            reason,
                            &snapshot_command,
                            snapshot_response,
                            &mut outcomes,
                        )? {
                            DfsMetadataJournalReplicationStep::Continue {
                                after_sequence: next_after_sequence,
                            } => {
                                after_sequence = next_after_sequence;
                                sent_batches += 1;
                            }
                            DfsMetadataJournalReplicationStep::Terminal => break,
                            DfsMetadataJournalReplicationStep::SnapshotRepair { .. } => {
                                unreachable!("snapshot install response cannot request a snapshot")
                            }
                        }
                    }
                    Err(error) => {
                        push_dfs_metadata_snapshot_transport_error_outcome(
                            &mut outcomes,
                            target_node_id,
                            shard_id,
                            after_sequence,
                            reason,
                            error,
                        );
                        break;
                    }
                }
            }
            DfsMetadataJournalReplicationStep::Terminal => break,
        };
        if sent_batches >= options.max_batches_per_target {
            push_dfs_metadata_backlog_if_remaining(
                backend,
                target_node_id,
                shard_id,
                after_sequence,
                options,
                &mut outcomes,
            )?;
            break;
        }
    }
    Ok(outcomes)
}

pub(crate) fn metadata_journal_rejection_allows_snapshot_repair(reason: &str) -> bool {
    (reason.contains("metadata journal sequence")
        && reason.contains("already exists with a different entry"))
        || reason.contains("failed to decode segmented journal entry")
        || (reason.contains("metadata journal prefix probe") && reason.contains("state mismatch"))
}

pub(crate) fn metadata_replication_rejection_is_topology_mismatch(reason: &str) -> bool {
    reason.contains("metadata journal batch has no local shard config")
        || reason.contains("metadata snapshot install has no local shard config")
        || reason.contains("is not configured as a replica for metadata shard")
        || (reason.contains("metadata journal source")
            && reason.contains("is not shard")
            && reason.contains("primary"))
        || (reason.contains("metadata snapshot source")
            && reason.contains("is not shard")
            && reason.contains("primary"))
}

pub type AsyncDfsMetadataJournalBatchSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsMetadataJournalBatchCommand,
            Duration,
        ) -> DfsPeerTransportFuture<
            'a,
            Result<DfsMetadataJournalBatchResponse, DfsMetadataPeerTransportError>,
        > + Send
        + Sync,
>;

pub type AsyncDfsMetadataSnapshotInstallSender = Arc<
    dyn for<'a> Fn(
            &'a NodeId,
            &'a DfsMetadataSnapshotInstallCommand,
            Duration,
        ) -> DfsPeerTransportFuture<
            'a,
            Result<DfsMetadataSnapshotInstallResponse, DfsMetadataPeerTransportError>,
        > + Send
        + Sync,
>;

pub(crate) fn async_dfs_metadata_journal_batch_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMetadataJournalBatchSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let request = dfs_metadata_journal_batch_rpc_request(command)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    &request,
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            decode_dfs_metadata_journal_batch_rpc_response(&response)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn async_dfs_metadata_snapshot_install_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMetadataSnapshotInstallSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let request = dfs_metadata_snapshot_install_rpc_request(command)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    &request,
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            decode_dfs_metadata_snapshot_install_rpc_response(&response)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn dfs_metadata_journal_batch_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMetadataJournalBatchSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let request = dfs_metadata_journal_batch_rpc_request(command)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = peer_transport
            .call_object_rpc(
                target_node_id,
                &request,
                DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
            )
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        decode_dfs_metadata_journal_batch_rpc_response(&response)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
    })
}

pub(crate) fn dfs_metadata_snapshot_install_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMetadataSnapshotInstallSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let request = dfs_metadata_snapshot_install_rpc_request(command)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = peer_transport
            .call_object_rpc(
                target_node_id,
                &request,
                DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
            )
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        decode_dfs_metadata_snapshot_install_rpc_response(&response)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
    })
}

#[cfg(test)]
mod cursor_update_tests {
    use super::*;

    #[test]
    fn async_cursor_commit_does_not_open_materialized_tables() {
        let temp = sinomo_testdir::TestDir::new("sinomo-dfs-replication-cursor-direct");
        let data_dir = SinomoDataDir::new(temp.path());
        let shard_id = DfsMetadataShardId::new("services");
        let root = local_dfs_metadata_shard_path(&data_dir, &shard_id);
        let table_dir = root.join("records").join("tables");
        std::fs::create_dir_all(&table_dir).expect("table dir");
        std::fs::write(table_dir.join("materialized.slog"), b"not-a-table-log")
            .expect("corrupt table fixture");
        let target_node_id = NodeId::from_legacy_named("cursor-target").expect("node id");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");

        runtime
            .block_on(put_local_dfs_metadata_replication_cursor_async(
                data_dir.clone(),
                target_node_id.clone(),
                shard_id.clone(),
                42,
            ))
            .expect("cursor commit");
        runtime
            .block_on(put_local_dfs_metadata_replication_cursor_async(
                data_dir,
                target_node_id.clone(),
                shard_id.clone(),
                12,
            ))
            .expect("monotonic cursor commit");

        assert_eq!(
            metadata_cursor::metadata_replication_cursor(&root, &target_node_id, &shard_id)
                .expect("cursor read"),
            42
        );
    }
}
