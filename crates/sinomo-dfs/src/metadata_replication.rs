//! DFS metadata journal replication and default metadata materialization.
//!
//! This module owns the metadata publish pipeline: deciding when service events
//! require journal publication, routing journal/snapshot replication to peer
//! nodes, validating incoming replica updates, and materializing the default
//! SIFS metadata records. Durable metadata storage itself remains owned by
//! `metadata_backend`.

use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterConnectivityState, ClusterPaths, NetworkError,
    ObjectRpcResponse, ObjectRpcSessionCall, ObjectRpcSessionResult, cluster_directory_for,
    cluster_route_next_hop,
};
use sinomo_core::node::NodeId;
use sinomo_core::{ClusterBootstrapState, ClusterEndpointRecord};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

use crate::maintenance::migrate_legacy_dfs_metadata_replication_cursor;
use crate::{
    AsyncDfsPeerTransport, DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, DfsDistributedWriteResult, DfsError, DfsFileSystem,
    DfsFileSystemId, DfsMetadataBackend, DfsMetadataJournalBatch, DfsMetadataJournalBatchCommand,
    DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult, DfsMetadataReplicaIngestPermit,
    DfsMetadataReplicationSuppressionRecord, DfsMetadataShardConfig, DfsMetadataShardId,
    DfsMetadataShardMembershipHandoff, DfsMetadataSnapshotInstallCommand,
    DfsMetadataSnapshotInstallResponse, DfsMetadataSnapshotInstallResult, DfsNamespaceResult,
    DfsObjectRpcSendOptions, DfsObjectVersionRecord, DfsPeerTransport, DfsPeerTransportFuture,
    LocalShardedMetadataBackend, LocalSobjMetadataBackend, NetworkedDfsServiceEvent,
    cluster_dfs_metadata_shards, cluster_dfs_peer_node_ids,
    decode_dfs_metadata_journal_batch_rpc_response,
    decode_dfs_metadata_snapshot_install_rpc_response,
    default_dfs_metadata_replication_suppression_snapshot_path,
    dfs_metadata_journal_batch_rpc_request, dfs_metadata_snapshot_install_rpc_request,
    dfs_now_unix_nanos, distributed_sifs, distributed_sifs_bootstrap_objects_for_nodes,
    local_dfs_metadata_shard_path, metadata_shard_key, metadata_shard_membership_handoff_key,
    object_rpc_request_from_kv_payload, open_local_metadata_shard_backend,
    put_local_dfs_metadata_replication_suppression,
    read_dfs_metadata_replication_suppression_snapshot, sifs_file_system_id,
};
use crate::{metadata_cursor, metadata_lock};

type ClusterMetadataBackendCache =
    BTreeMap<(PathBuf, DfsMetadataShardId), Arc<Mutex<LocalSobjMetadataBackend>>>;
static DFS_METADATA_REPLICATION_SUPPRESSION_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// A topology change may leave legacy objects on their former metadata shard.
/// Limit cross-shard rewrites per mandatory pass while still scanning the full
/// catalog: a scan cursor is not durable yet, so stopping the scan itself could
/// permanently starve a misplaced object behind an already-correct prefix.
const DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_MUTATION_LIMIT: usize = 256;

fn cached_cluster_metadata_backend(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> Result<Arc<Mutex<LocalSobjMetadataBackend>>, DfsError> {
    static BACKENDS: OnceLock<Mutex<ClusterMetadataBackendCache>> = OnceLock::new();
    let key = (data_dir.root(), shard_id.clone());
    let mut backends = BACKENDS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DfsError::Metadata {
            message: "cluster metadata backend cache lock is poisoned".to_string(),
        })?;
    if let Some(backend) = backends.get(&key) {
        return Ok(Arc::clone(backend));
    }
    let backend = Arc::new(Mutex::new(open_local_metadata_shard_backend(
        data_dir,
        shard_id.clone(),
    )?));
    backends.insert(key, Arc::clone(&backend));
    Ok(backend)
}

pub const DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;
const DEFAULT_DFS_METADATA_REPLICATION_MAX_CONCURRENT_TARGET_NODES: usize = 8;

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
                    ObjectRpcSessionCall::value(request.clone()),
                    DfsObjectRpcSendOptions::default()
                        .with_overall_timeout(timeout)
                        .with_observe_connectivity(false),
                )
                .map_err(|source| NetworkError::Io {
                    operation: "DFS cluster KV snapshot refresh".to_string(),
                    detail: source.to_string(),
                })?;
            if !response.body.is_empty() {
                return Err(NetworkError::Serialization {
                    detail: "DFS cluster KV snapshot returned an unexpected attachment".to_string(),
                });
            }
            let response = response.response;
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
    pub pending_membership_handoff: Option<DfsMetadataShardMembershipHandoff>,
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
    // A target-level failure does not independently drive scheduler retries:
    // its durable cursor remains behind for periodic DFS maintenance or a new
    // mutation request. A mixed report can revisit that lane while another
    // target's confirmed backlog is drained, but those extra rounds stay
    // bounded by the schedule options.
    report.outcomes.iter().any(|outcome| {
        matches!(
            outcome,
            DfsMetadataJournalReplicationOutcome::BacklogRemaining { .. }
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

pub fn schedule_dfs_metadata_publish_async_on_runtime<P, Fut, R, E>(
    scheduler: &'static DfsMetadataPublishScheduler,
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
    scheduler.request_publish_async_on_runtime(runtime, options, publish, on_report, on_error)
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
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_dfs_metadata_publish_async_on_runtime(
            scheduler, runtime, options, publish, on_report, on_error,
        )
    })
}

static DEFAULT_DFS_METADATA_PUBLISH_SCHEDULER: DfsMetadataPublishScheduler =
    DfsMetadataPublishScheduler::new();

pub fn schedule_default_dfs_metadata_publish_async_on_runtime<P, Fut, R, E>(
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
    schedule_dfs_metadata_publish_async_on_runtime(
        &DEFAULT_DFS_METADATA_PUBLISH_SCHEDULER,
        runtime,
        options,
        publish,
        on_report,
        on_error,
    )
}

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
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_default_dfs_metadata_publish_async_on_runtime(
            runtime, options, publish, on_report, on_error,
        )
    })
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

pub fn schedule_cluster_dfs_metadata_publish_async_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    metadata_sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_default_dfs_metadata_publish_async_on_runtime(
        runtime,
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
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_cluster_dfs_metadata_publish_async_on_runtime(
            runtime,
            data_dir,
            cluster_paths,
            metadata_sender,
            snapshot_sender,
            schedule_options,
            replication_options,
            on_report,
            on_error,
        )
    })
}

pub fn schedule_cluster_dfs_metadata_publish_with_async_peer_transport_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_cluster_dfs_metadata_publish_async_on_runtime(
        runtime,
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(Arc::clone(&peer_transport)),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        schedule_options,
        replication_options,
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
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_cluster_dfs_metadata_publish_with_async_peer_transport_on_runtime(
            runtime,
            data_dir,
            cluster_paths,
            peer_transport,
            schedule_options,
            replication_options,
            on_report,
            on_error,
        )
    })
}

pub fn schedule_cluster_dfs_metadata_publish_with_peer_transports_on_runtime<R, E>(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    schedule_options: DfsMetadataPublishScheduleOptions,
    replication_options: DfsMetadataJournalReplicationOptions,
    on_report: R,
    on_error: E,
) -> DfsMetadataPublishScheduleStart
where
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    schedule_default_dfs_metadata_publish_async_on_runtime(
        runtime,
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
    dfs_events_should_publish_metadata(events).then(|| {
        schedule_cluster_dfs_metadata_publish_with_peer_transports_on_runtime(
            runtime,
            data_dir,
            cluster_paths,
            sync_peer_transport,
            peer_transport,
            schedule_options,
            replication_options,
            on_report,
            on_error,
        )
    })
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

struct DfsMetadataPublishRunGuard {
    scheduler: &'static DfsMetadataPublishScheduler,
    owns_running: bool,
}

impl DfsMetadataPublishRunGuard {
    fn new(scheduler: &'static DfsMetadataPublishScheduler) -> Self {
        Self {
            scheduler,
            owns_running: true,
        }
    }

    fn release_or_reclaim_pending(&mut self) -> bool {
        debug_assert!(self.owns_running);
        // Disarm before publishing the idle state so this guard cannot clear a
        // concurrent request's newly acquired claim when the loop exits.
        self.owns_running = false;
        self.scheduler.running.store(false, Ordering::Release);
        if self.scheduler.pending.load(Ordering::Acquire)
            && self
                .scheduler
                .running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.owns_running = true;
            true
        } else {
            false
        }
    }
}

impl Drop for DfsMetadataPublishRunGuard {
    fn drop(&mut self) {
        if self.owns_running {
            self.scheduler.running.store(false, Ordering::Release);
        }
    }
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
            let run_guard = DfsMetadataPublishRunGuard::new(self);
            runtime.spawn(async move {
                self.run_publish_loop_async(options, publish, on_report, on_error, run_guard)
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
        mut run_guard: DfsMetadataPublishRunGuard,
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
            if run_guard.release_or_reclaim_pending() {
                retry_rounds = 0;
                continue;
            }
            break;
        }
    }
}

#[cfg(test)]
mod publish_scheduler_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn failed_publish_report() -> DfsMetadataJournalReplicationReport {
        DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id: NodeId::from_legacy_named("node_metadata_publish_scheduler_failed")
                    .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                reason: "target unavailable".to_string(),
            }],
        }
    }

    fn backlog_publish_report() -> DfsMetadataJournalReplicationReport {
        DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: NodeId::from_legacy_named(
                    "node_metadata_publish_scheduler_backlog",
                )
                .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                after_sequence: 7,
                max_batches: 1,
            }],
        }
    }

    #[test]
    fn retry_classifier_defers_target_failures_to_periodic_maintenance() {
        assert!(!dfs_metadata_replication_report_needs_retry(
            &failed_publish_report()
        ));
        assert!(!dfs_metadata_replication_report_needs_retry(
            &DfsMetadataJournalReplicationReport {
                outcomes: vec![DfsMetadataJournalReplicationOutcome::MissingRoute {
                    target_node_id: NodeId::from_legacy_named(
                        "node_metadata_publish_scheduler_missing",
                    )
                    .expect("target node"),
                    shard_id: DfsMetadataShardId::new("services"),
                    after_sequence: 7,
                }],
            }
        ));
        assert!(dfs_metadata_replication_report_needs_retry(
            &backlog_publish_report()
        ));
        let mut mixed = failed_publish_report();
        mixed.outcomes.extend(backlog_publish_report().outcomes);
        assert!(
            dfs_metadata_replication_report_needs_retry(&mixed),
            "confirmed backlog keeps the bounded report-wide retry active"
        );
    }

    #[test]
    fn failed_target_report_is_deferred_without_retry() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler: &'static DfsMetadataPublishScheduler =
            Box::leak(Box::new(DfsMetadataPublishScheduler::new()));
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
            },
            move || {
                publish_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(failed_publish_report()))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(scheduler),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn backlog_report_runs_initial_plus_configured_retry_rounds() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler: &'static DfsMetadataPublishScheduler =
            Box::leak(Box::new(DfsMetadataPublishScheduler::new()));
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
            },
            move || {
                publish_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(backlog_publish_report()))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(scheduler),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn pending_request_after_deferred_failure_runs_one_fresh_publish() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler: &'static DfsMetadataPublishScheduler =
            Box::leak(Box::new(DfsMetadataPublishScheduler::new()));
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    assert_eq!(
                        scheduler.claim_publish_start(),
                        DfsMetadataPublishScheduleStart::AlreadyRunning
                    );
                }
                std::future::ready(Ok(failed_publish_report()))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(scheduler),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn unconditional_publish_runs_and_coalesces_pending_requests() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Box::leak(Box::new(DfsMetadataPublishScheduler::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let discarded_calls = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let (report_sender, report_receiver) = std::sync::mpsc::channel();

        let publish_calls = Arc::clone(&calls);
        let publish_started = Arc::clone(&first_started);
        let publish_release = Arc::clone(&release_first);
        let start = schedule_dfs_metadata_publish_async_on_runtime(
            scheduler,
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                let publish_started = Arc::clone(&publish_started);
                let publish_release = Arc::clone(&publish_release);
                async move {
                    publish_started.notify_one();
                    if call == 0 {
                        publish_release.notified().await;
                    }
                    Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    })
                }
            },
            move |report| {
                report_sender.send(report).expect("report receiver");
            },
            |_| {},
        );
        assert_eq!(start, DfsMetadataPublishScheduleStart::Started);
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), first_started.notified())
                .await
                .expect("first publish started");
        });

        let second_publish_calls = Arc::clone(&discarded_calls);
        let second_start = schedule_dfs_metadata_publish_async_on_runtime(
            scheduler,
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions::default(),
            move || {
                second_publish_calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    })
                }
            },
            |_| {},
            |_| {},
        );
        assert_eq!(
            second_start,
            DfsMetadataPublishScheduleStart::AlreadyRunning
        );

        release_first.notify_one();
        report_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("first report");
        report_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("coalesced report");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(discarded_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cancelled_publish_task_releases_running_claim() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Box::leak(Box::new(DfsMetadataPublishScheduler::new()));
        let (started_sender, started_receiver) = std::sync::mpsc::channel();

        let start = schedule_dfs_metadata_publish_async_on_runtime(
            scheduler,
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
            },
            move || {
                let started_sender = started_sender.clone();
                async move {
                    started_sender.send(()).expect("start receiver");
                    std::future::pending::<Result<DfsMetadataJournalReplicationReport, String>>()
                        .await
                }
            },
            |_| {},
            |_| {},
        );
        assert_eq!(start, DfsMetadataPublishScheduleStart::Started);
        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("publish task started");

        drop(runtime);
        assert!(!scheduler.running.load(Ordering::Acquire));

        let recovery_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("recovery runtime");
        let (report_sender, report_receiver) = std::sync::mpsc::channel();
        let recovery_start = schedule_dfs_metadata_publish_async_on_runtime(
            scheduler,
            recovery_runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
            },
            || async {
                Ok(DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                })
            },
            move |report| report_sender.send(report).expect("report receiver"),
            |_| {},
        );
        assert_eq!(recovery_start, DfsMetadataPublishScheduleStart::Started);
        recovery_runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if let Ok(report) = report_receiver.try_recv() {
                        break report;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("recovery publish report");
        });
    }

    #[test]
    fn event_gated_scheduler_skips_non_publishing_events() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Box::leak(Box::new(DfsMetadataPublishScheduler::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        let start = schedule_dfs_metadata_publish_after_events_async_on_runtime(
            scheduler,
            runtime.handle().clone(),
            &[],
            DfsMetadataPublishScheduleOptions::default(),
            move || {
                publish_calls.fetch_add(1, Ordering::SeqCst);
                async {
                    Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    })
                }
            },
            |_| {},
            |_| {},
        );

        assert_eq!(start, None);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsMetadataJournalReplicationTransportError {
    MissingRoute,
    Failed { reason: String },
}

pub trait DfsMetadataJournalReplicationTransport: Send {
    /// Returns an independent transport handle for one target-node lane.
    /// Implementations without a cheap, thread-safe fork retain serial
    /// execution through the default `None` result.
    fn fork_target_lane(&self) -> Option<Box<dyn DfsMetadataJournalReplicationTransport>> {
        None
    }

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

pub trait AsyncDfsMetadataJournalReplicationTransport: Send + Sync {
    fn has_metadata_replication_route(&self, target_node_id: &NodeId) -> bool;

    fn send_metadata_journal_batch_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        command: &'a DfsMetadataJournalBatchCommand,
        timeout: Duration,
    ) -> DfsMetadataJournalReplicationTransportFuture<
        'a,
        Result<DfsMetadataJournalBatchResponse, DfsMetadataJournalReplicationTransportError>,
    >;

    fn send_metadata_snapshot_install_async<'a>(
        &'a self,
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
    fn fork_target_lane(&self) -> Option<Box<dyn DfsMetadataJournalReplicationTransport>> {
        Some(Box::new(self.clone()))
    }

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
        &'a self,
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
        &'a self,
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
    fn local_data_dir(&self) -> Option<&SinomoDataDir> {
        None
    }

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
    fn local_data_dir(&self) -> Option<&SinomoDataDir> {
        Some(&self.data_dir)
    }

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
    let trace_started = Instant::now();
    let data_dir = data_dir.clone();
    let cluster_paths = cluster_paths.clone();
    let prepared = tokio::task::spawn_blocking(move || {
        prepare_cluster_dfs_metadata_journal_publish(data_dir, cluster_paths, options)
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata publish planning task failed: {source}"),
    })??;
    let (outcomes, targets) = prepared
        .as_ref()
        .map(|prepared| (prepared.outcomes.len(), prepared.targets.len()))
        .unwrap_or_default();
    let traceparent = crate::current_dfs_traceparent().unwrap_or_default();
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent,
        outcomes,
        targets,
        total_ms = trace_started.elapsed().as_millis(),
        "metadata-publish-plan"
    );
    Ok(prepared)
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
            &file_system_id,
            shard_epoch,
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

fn record_dfs_metadata_replication_topology_mismatch(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    reason: &str,
) -> Result<(), DfsError> {
    if !metadata_replication_rejection_is_topology_mismatch(reason) {
        return Ok(());
    }
    let _write_guard = DFS_METADATA_REPLICATION_SUPPRESSION_WRITE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| DfsError::Metadata {
            message: "DFS metadata replication suppression write lock is poisoned".to_string(),
        })?;
    put_local_dfs_metadata_replication_suppression(
        data_dir,
        DfsMetadataReplicationSuppressionRecord {
            target_node_id: target_node_id.clone(),
            shard_id: shard_id.clone(),
            shard_epoch,
            reason: reason.to_owned(),
            updated_at_unix_nanos: dfs_now_unix_nanos(),
        },
    )
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
    let data_dir = data_dir.clone();
    let target_node_id = target_node_id.clone();
    let shard_id = shard_id.clone();
    let reason = reason.to_owned();
    tokio::task::spawn_blocking(move || {
        record_dfs_metadata_replication_topology_mismatch(
            &data_dir,
            &target_node_id,
            &shard_id,
            shard_epoch,
            &reason,
        )
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("DFS metadata replication suppression write task failed: {source}"),
    })?
}

fn prepared_dfs_metadata_target_lanes(
    targets: Vec<DfsPreparedMetadataJournalTargetPublish>,
) -> VecDeque<Vec<(usize, DfsPreparedMetadataJournalTargetPublish)>> {
    let mut target_lanes =
        BTreeMap::<NodeId, Vec<(usize, DfsPreparedMetadataJournalTargetPublish)>>::new();
    for (target_index, target) in targets.into_iter().enumerate() {
        target_lanes
            .entry(target.target_node_id.clone())
            .or_default()
            .push((target_index, target));
    }
    target_lanes.into_values().collect()
}

/// Treat a node as temporarily ineligible only when every endpoint currently
/// advertised by the cluster directory has an explicit local `Unreachable`
/// record and the connectivity graph has no relay path to it. An unobserved
/// directory-only or module endpoint therefore keeps the target eligible.
/// Re-reading the graph for each publish also makes a connectivity transition
/// immediately restore the target without waiting for a replication timer.
fn dfs_metadata_target_is_explicitly_unreachable(
    graph: &ClusterConnectivityGraph,
    advertised_endpoints: &[ClusterEndpointRecord],
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    if cluster_route_next_hop(graph, local_node_id, target_node_id).is_some() {
        return false;
    }

    let mut target_endpoints = advertised_endpoints
        .iter()
        .filter(|endpoint| &endpoint.node_id == target_node_id)
        .peekable();
    target_endpoints.peek().is_some()
        && target_endpoints.all(|endpoint| {
            matches!(
                graph.state_for_endpoint(endpoint),
                Some(ClusterConnectivityState::Unreachable)
            )
        })
}

fn defer_explicitly_unreachable_dfs_metadata_targets(
    plan: &mut DfsPreparedClusterMetadataJournalPublish,
) {
    if plan.targets.is_empty() {
        return;
    }
    let cluster_paths = ClusterPaths::new(&plan.data_dir);
    if !cluster_paths.state.exists() {
        return;
    }
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths) else {
        return;
    };
    if state.local_node_id != plan.local_node_id {
        return;
    }

    // Fail open when the directory is not installed (for example, an
    // embedded/pre-cluster caller). State-member endpoints alone are not
    // sufficient here because the directory may own additional module or
    // dynamically published endpoints that must remain routable.
    let Ok(advertised_endpoints) = cluster_directory_for(&cluster_paths).endpoints() else {
        return;
    };
    if advertised_endpoints.is_empty() {
        return;
    }
    let Ok(graph) = ClusterConnectivityGraph::try_read_for_local(&cluster_paths, &state) else {
        return;
    };

    defer_explicitly_unreachable_dfs_metadata_targets_with_connectivity(
        plan,
        &graph,
        &advertised_endpoints,
    );
}

fn defer_explicitly_unreachable_dfs_metadata_targets_with_connectivity(
    plan: &mut DfsPreparedClusterMetadataJournalPublish,
    graph: &ClusterConnectivityGraph,
    advertised_endpoints: &[ClusterEndpointRecord],
) {
    let target_node_ids = plan
        .targets
        .iter()
        .map(|target| target.target_node_id.clone())
        .collect::<BTreeSet<_>>();
    let deferred_node_ids = target_node_ids
        .into_iter()
        .filter(|target_node_id| {
            dfs_metadata_target_is_explicitly_unreachable(
                graph,
                advertised_endpoints,
                &plan.local_node_id,
                target_node_id,
            )
        })
        .collect::<BTreeSet<_>>();
    if deferred_node_ids.is_empty() {
        return;
    }

    let mut deferred_outcomes = Vec::new();
    plan.targets.retain(|target| {
        if !deferred_node_ids.contains(&target.target_node_id) {
            return true;
        }
        deferred_outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id: target.target_node_id.clone(),
            shard_id: target.shard_id.clone(),
            after_sequence: target.after_sequence,
        });
        false
    });
    plan.outcomes.extend(deferred_outcomes);
}

#[cfg(test)]
mod explicitly_unreachable_target_tests {
    use super::*;
    use sinomo_core::network::ClusterConnectivityRecord;
    use sinomo_core::node::ClusterId;
    use sinomo_core::{ClusterEndpointPrivacy, ClusterEndpointSource, ClusterTransportKind, Id};

    fn test_node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn test_endpoint(
        name: &str,
        node_id: &NodeId,
        transport: ClusterTransportKind,
        address: &str,
        source: ClusterEndpointSource,
    ) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: Id::from_legacy_named(name).expect("endpoint id"),
            node_id: node_id.clone(),
            transport,
            address: address.to_string(),
            source,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        }
    }

    fn connectivity_record(
        cluster_id: &ClusterId,
        source_node_id: &NodeId,
        endpoint: &ClusterEndpointRecord,
        state: ClusterConnectivityState,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        let is_healthy = state == ClusterConnectivityState::Healthy;
        ClusterConnectivityRecord {
            cluster_id: cluster_id.clone(),
            source_node_id: source_node_id.clone(),
            target_node_id: endpoint.node_id.clone(),
            endpoint_id: endpoint.endpoint_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            state,
            latency_ms: is_healthy.then_some(4),
            consecutive_failures: if is_healthy { 0 } else { 3 },
            last_success_at: None,
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::hours(1),
        }
    }

    fn graph(
        local_node_id: &NodeId,
        records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    ) -> ClusterConnectivityGraph {
        ClusterConnectivityGraph::from_records(local_node_id.clone(), records)
    }

    #[test]
    fn explicitly_unreachable_requires_every_advertised_endpoint_to_be_unreachable() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_metadata_offline_all").expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_all_local");
        let target_node_id = test_node("node_dfs_metadata_offline_all_target");
        let tcp_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_all_tcp",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.20.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let iroh_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_all_iroh",
            &target_node_id,
            ClusterTransportKind::Iroh,
            "iroh-offline-target",
            ClusterEndpointSource::Rendezvous,
        );
        let advertised_endpoints = vec![tcp_endpoint.clone(), iroh_endpoint.clone()];
        let graph = graph(
            &local_node_id,
            [
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &tcp_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &iroh_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
            ],
        );

        assert!(dfs_metadata_target_is_explicitly_unreachable(
            &graph,
            &advertised_endpoints,
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_fails_open_for_unobserved_module_endpoint() {
        let cluster_id = ClusterId::from_legacy_named("cluster_dfs_metadata_offline_unobserved")
            .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_unobserved_local");
        let target_node_id = test_node("node_dfs_metadata_offline_unobserved_target");
        let tcp_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_unobserved_tcp",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.21.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let module_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_unobserved_module",
            &target_node_id,
            ClusterTransportKind::Iroh,
            "iroh-module-target",
            ClusterEndpointSource::Rendezvous,
        );
        let graph = graph(
            &local_node_id,
            [connectivity_record(
                &cluster_id,
                &local_node_id,
                &tcp_endpoint,
                ClusterConnectivityState::Unreachable,
            )],
        );

        assert!(!dfs_metadata_target_is_explicitly_unreachable(
            &graph,
            &[tcp_endpoint, module_endpoint],
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_recovers_immediately_on_connectivity_transition() {
        let cluster_id = ClusterId::from_legacy_named("cluster_dfs_metadata_offline_recovery")
            .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_recovery_local");
        let target_node_id = test_node("node_dfs_metadata_offline_recovery_target");
        let endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_recovery",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.22.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let unreachable_graph = graph(
            &local_node_id,
            [connectivity_record(
                &cluster_id,
                &local_node_id,
                &endpoint,
                ClusterConnectivityState::Unreachable,
            )],
        );
        let healthy_graph = graph(
            &local_node_id,
            [connectivity_record(
                &cluster_id,
                &local_node_id,
                &endpoint,
                ClusterConnectivityState::Healthy,
            )],
        );

        assert!(dfs_metadata_target_is_explicitly_unreachable(
            &unreachable_graph,
            std::slice::from_ref(&endpoint),
            &local_node_id,
            &target_node_id,
        ));
        assert!(!dfs_metadata_target_is_explicitly_unreachable(
            &healthy_graph,
            std::slice::from_ref(&endpoint),
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_keeps_target_with_live_relay_path() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_metadata_offline_relay").expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_relay_local");
        let relay_node_id = test_node("node_dfs_metadata_offline_relay_bridge");
        let target_node_id = test_node("node_dfs_metadata_offline_relay_target");
        let direct_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_relay_direct",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.23.3:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_relay_bridge",
            &relay_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.23.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_target_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_relay_bridge_target",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.23.3:21353",
            ClusterEndpointSource::Observed,
        );
        let graph = graph(
            &local_node_id,
            [
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &direct_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &relay_endpoint,
                    ClusterConnectivityState::Healthy,
                ),
                connectivity_record(
                    &cluster_id,
                    &relay_node_id,
                    &relay_target_endpoint,
                    ClusterConnectivityState::Healthy,
                ),
            ],
        );

        assert!(!dfs_metadata_target_is_explicitly_unreachable(
            &graph,
            &[direct_endpoint],
            &local_node_id,
            &target_node_id,
        ));
    }

    #[test]
    fn explicitly_unreachable_target_is_deferred_without_dropping_healthy_target() {
        let cluster_id = ClusterId::from_legacy_named("cluster_dfs_metadata_offline_filter")
            .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_filter_local");
        let offline_node_id = test_node("node_dfs_metadata_offline_filter_offline");
        let healthy_node_id = test_node("node_dfs_metadata_offline_filter_healthy");
        let offline_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_filter_offline",
            &offline_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.24.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let healthy_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_filter_healthy",
            &healthy_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.24.3:21353",
            ClusterEndpointSource::KvMembership,
        );
        let graph = graph(
            &local_node_id,
            [
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &offline_endpoint,
                    ClusterConnectivityState::Unreachable,
                ),
                connectivity_record(
                    &cluster_id,
                    &local_node_id,
                    &healthy_endpoint,
                    ClusterConnectivityState::Healthy,
                ),
            ],
        );
        let file_system_id = DfsFileSystemId::from_legacy_named("fs_dfs_metadata_offline_filter")
            .expect("file system id");
        let shard_id = DfsMetadataShardId::new("services");
        let target = |target_node_id: NodeId| DfsPreparedMetadataJournalTargetPublish {
            target_node_id,
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            shard_epoch: 4,
            after_sequence: 19,
            commands: Vec::new(),
            backlog_after_sequence: None,
        };
        let mut plan = DfsPreparedClusterMetadataJournalPublish {
            data_dir: SinomoDataDir::new(std::env::temp_dir()),
            local_node_id: local_node_id.clone(),
            peer_node_ids: vec![offline_node_id.clone(), healthy_node_id.clone()],
            outcomes: Vec::new(),
            targets: vec![
                target(offline_node_id.clone()),
                target(healthy_node_id.clone()),
            ],
        };

        defer_explicitly_unreachable_dfs_metadata_targets_with_connectivity(
            &mut plan,
            &graph,
            &[offline_endpoint, healthy_endpoint],
        );

        assert_eq!(plan.targets.len(), 1);
        assert_eq!(plan.targets[0].target_node_id, healthy_node_id);
        assert!(matches!(
            plan.outcomes.as_slice(),
            [DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id,
                shard_id: deferred_shard_id,
                after_sequence: 19,
            }] if target_node_id == &offline_node_id && deferred_shard_id == &shard_id
        ));
    }
}

fn execute_prepared_dfs_metadata_journal_publish(
    mut plan: DfsPreparedClusterMetadataJournalPublish,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    defer_explicitly_unreachable_dfs_metadata_targets(&mut plan);
    let DfsPreparedClusterMetadataJournalPublish {
        data_dir,
        local_node_id,
        peer_node_ids: _,
        mut outcomes,
        targets,
    } = plan;
    let Some(first_lane_transport) = transport.fork_target_lane() else {
        for target in targets {
            execute_prepared_dfs_metadata_journal_target_publish(
                &data_dir,
                &local_node_id,
                target,
                transport,
                options,
                &mut outcomes,
            )?;
        }
        return Ok(DfsMetadataJournalReplicationReport { outcomes });
    };

    let target_count = targets.len();
    let mut first_lane_transport = Some(first_lane_transport);
    let mut pending_lanes = prepared_dfs_metadata_target_lanes(targets)
        .into_iter()
        .map(|target_lane| {
            let lane_transport = first_lane_transport.take().unwrap_or_else(|| {
                transport
                    .fork_target_lane()
                    .expect("metadata transport stopped supporting target lane forks")
            });
            (target_lane, lane_transport)
        })
        .collect::<VecDeque<_>>();
    let mut ordered_target_results = std::iter::repeat_with(|| None)
        .take(target_count)
        .collect::<Vec<_>>();
    while !pending_lanes.is_empty() {
        let lane_count = pending_lanes
            .len()
            .min(DEFAULT_DFS_METADATA_REPLICATION_MAX_CONCURRENT_TARGET_NODES);
        let lanes = pending_lanes.drain(..lane_count).collect::<Vec<_>>();
        let completed_lanes = std::thread::scope(|scope| {
            lanes
                .into_iter()
                .map(|(target_lane, mut lane_transport)| {
                    let data_dir = &data_dir;
                    let local_node_id = &local_node_id;
                    scope.spawn(move || {
                        let mut lane_results = Vec::with_capacity(target_lane.len());
                        for (target_index, target) in target_lane {
                            let mut target_outcomes = Vec::new();
                            let result = execute_prepared_dfs_metadata_journal_target_publish(
                                &data_dir,
                                &local_node_id,
                                target,
                                lane_transport.as_mut(),
                                options,
                                &mut target_outcomes,
                            )
                            .map(|()| target_outcomes);
                            let failed = result.is_err();
                            lane_results.push((target_index, result));
                            if failed {
                                break;
                            }
                        }
                        lane_results
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| handle.join().expect("metadata target lane panicked"))
                .collect::<Vec<_>>()
        });
        for completed in completed_lanes {
            for (target_index, result) in completed {
                ordered_target_results[target_index] = Some(result);
            }
        }
    }

    for result in ordered_target_results {
        let result = result.expect("metadata target result missing without an earlier error");
        match result {
            Ok(mut target_outcomes) => outcomes.append(&mut target_outcomes),
            Err(error) => return Err(error),
        }
    }
    Ok(DfsMetadataJournalReplicationReport { outcomes })
}

async fn execute_prepared_dfs_metadata_journal_publish_async(
    mut plan: DfsPreparedClusterMetadataJournalPublish,
    transport: &dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    defer_explicitly_unreachable_dfs_metadata_targets(&mut plan);
    let DfsPreparedClusterMetadataJournalPublish {
        data_dir,
        local_node_id,
        peer_node_ids: _,
        mut outcomes,
        targets,
    } = plan;
    let target_count = targets.len();
    // One lane owns all shard plans for a target node, preserving the
    // established per-target batch/shard ordering. Independent node lanes run
    // concurrently so an offline replica cannot head-of-line block healthy
    // replicas. Results are reassembled by their original plan index below,
    // keeping report/error ordering deterministic rather than completion-timed.
    let mut pending_lanes = prepared_dfs_metadata_target_lanes(targets);
    let mut in_flight = Vec::<DfsMetadataTargetLaneFuture<'_>>::new();
    let mut ordered_target_results = std::iter::repeat_with(|| None)
        .take(target_count)
        .collect::<Vec<_>>();
    while !pending_lanes.is_empty() || !in_flight.is_empty() {
        while in_flight.len() < DEFAULT_DFS_METADATA_REPLICATION_MAX_CONCURRENT_TARGET_NODES {
            let Some(target_lane) = pending_lanes.pop_front() else {
                break;
            };
            let data_dir = &data_dir;
            let local_node_id = &local_node_id;
            in_flight.push(Box::pin(async move {
                let mut lane_results = Vec::with_capacity(target_lane.len());
                for (target_index, target) in target_lane {
                    let mut target_outcomes = Vec::new();
                    let result = execute_prepared_dfs_metadata_journal_target_publish_async(
                        &data_dir,
                        &local_node_id,
                        target,
                        transport,
                        options,
                        &mut target_outcomes,
                    )
                    .await
                    .map(|()| target_outcomes);
                    let failed = result.is_err();
                    lane_results.push((target_index, result));
                    if failed {
                        break;
                    }
                }
                lane_results
            }));
        }

        let completed = next_completed_dfs_metadata_target_lane(&mut in_flight).await;
        for (target_index, result) in completed {
            ordered_target_results[target_index] = Some(result);
        }
    }

    for result in ordered_target_results {
        let result = result.expect("metadata target result missing without an earlier error");
        match result {
            Ok(mut target_outcomes) => outcomes.append(&mut target_outcomes),
            Err(error) => return Err(error),
        }
    }
    Ok(DfsMetadataJournalReplicationReport { outcomes })
}

type DfsMetadataTargetPublishResult = Result<Vec<DfsMetadataJournalReplicationOutcome>, DfsError>;
type DfsMetadataTargetLaneResult = Vec<(usize, DfsMetadataTargetPublishResult)>;
type DfsMetadataTargetLaneFuture<'a> =
    Pin<Box<dyn Future<Output = DfsMetadataTargetLaneResult> + Send + 'a>>;

async fn next_completed_dfs_metadata_target_lane(
    in_flight: &mut Vec<DfsMetadataTargetLaneFuture<'_>>,
) -> DfsMetadataTargetLaneResult {
    let (completed_index, result) = std::future::poll_fn(|context| {
        for (index, future) in in_flight.iter_mut().enumerate() {
            if let Poll::Ready(result) = future.as_mut().poll(context) {
                return Poll::Ready((index, result));
            }
        }
        Poll::Pending
    })
    .await;
    drop(in_flight.swap_remove(completed_index));
    result
}

fn execute_prepared_dfs_metadata_journal_target_publish(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target: DfsPreparedMetadataJournalTargetPublish,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
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
        let response =
            match transport.send_metadata_journal_batch(&target_node_id, &command, attempt_timeout)
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
                put_local_dfs_metadata_replication_cursor(
                    data_dir,
                    &target_node_id,
                    &shard_id,
                    next_after_sequence,
                )?;
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
                    execute_prepared_dfs_metadata_snapshot_repair(
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
                    )?;
                } else {
                    record_dfs_metadata_replication_topology_mismatch(
                        data_dir,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        &reason,
                    )?;
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

#[allow(clippy::too_many_arguments)]
fn execute_prepared_dfs_metadata_snapshot_repair(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    file_system_id: &DfsFileSystemId,
    after_sequence: u64,
    reason: String,
    transport: &mut dyn DfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    target_started_at: Instant,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    let command = build_local_dfs_metadata_snapshot_install_command(
        data_dir,
        local_node_id,
        target_node_id,
        file_system_id,
        shard_epoch,
        shard_id,
        reason.clone(),
    )?;
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
    let response =
        match transport.send_metadata_snapshot_install(target_node_id, &command, attempt_timeout) {
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
            put_local_dfs_metadata_replication_cursor(
                data_dir,
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
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
        } => {
            let reason = format!("{reason}; snapshot repair rejected: {snapshot_reason}");
            record_dfs_metadata_replication_topology_mismatch(
                data_dir,
                target_node_id,
                shard_id,
                shard_epoch,
                &reason,
            )?;
            outcomes.push(DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                reason,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod concurrent_target_publish_tests {
    use super::*;

    fn prepared_target(
        local_node_id: &NodeId,
        target_node_id: NodeId,
        file_system_id: DfsFileSystemId,
        shard_id: &str,
    ) -> DfsPreparedMetadataJournalTargetPublish {
        let shard_id = DfsMetadataShardId::new(shard_id);
        DfsPreparedMetadataJournalTargetPublish {
            target_node_id: target_node_id.clone(),
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
            shard_epoch: 1,
            after_sequence: 0,
            commands: vec![DfsMetadataJournalBatchCommand {
                request_id: format!("publish-{target_node_id}-{}", shard_id.0),
                source_node_id: Some(local_node_id.clone()),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(1),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch: DfsMetadataJournalBatch {
                    shard_id,
                    entries: Vec::new(),
                    next_after_sequence: 1,
                },
            }],
            backlog_after_sequence: None,
        }
    }

    fn snapshot_sender() -> AsyncDfsMetadataSnapshotInstallSender {
        Arc::new(move |_target_node_id, command, _timeout| {
            Box::pin(async move {
                Ok(DfsMetadataSnapshotInstallResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.shard_id.clone(),
                    next_after_sequence: command.next_after_sequence,
                    result: DfsMetadataSnapshotInstallResult::Installed {
                        archived_existing: false,
                    },
                })
            })
        })
    }

    fn sync_snapshot_sender() -> DfsMetadataSnapshotInstallSender {
        Arc::new(move |_target_node_id, command, _timeout| {
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id: command.request_id.clone(),
                shard_id: command.shard_id.clone(),
                next_after_sequence: command.next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::Installed {
                    archived_existing: false,
                },
            })
        })
    }

    #[test]
    fn sync_healthy_target_publish_does_not_wait_behind_offline_target() {
        let test_dir = sinomo_testdir::TestDir::new("dfs-metadata-sync-concurrent-targets");
        let data_dir = SinomoDataDir::new(test_dir.path());
        data_dir.ensure_root_layout().expect("root layout");
        let local_node_id =
            NodeId::from_legacy_named("node_metadata_sync_concurrent_local").expect("local node");
        let offline_node_id =
            NodeId::from_legacy_named("node_metadata_sync_concurrent_offline").expect("offline");
        let healthy_node_id =
            NodeId::from_legacy_named("node_metadata_sync_concurrent_healthy").expect("healthy");
        let file_system_id = distributed_sifs(local_node_id.clone()).file_system_id;
        let (offline_started_sender, offline_started_receiver) = std::sync::mpsc::channel();
        let (release_offline_sender, release_offline_receiver) = std::sync::mpsc::channel();
        let release_offline_receiver = Arc::new(Mutex::new(release_offline_receiver));
        let (healthy_completed_sender, healthy_completed_receiver) = std::sync::mpsc::channel();
        let sender: DfsMetadataJournalBatchSender = {
            let offline_node_id = offline_node_id.clone();
            let release_offline_receiver = Arc::clone(&release_offline_receiver);
            Arc::new(move |target_node_id, command, _timeout| {
                if *target_node_id == offline_node_id {
                    offline_started_sender.send(()).expect("offline started");
                    release_offline_receiver
                        .lock()
                        .expect("offline release lock")
                        .recv()
                        .expect("offline release");
                    return Err(DfsMetadataPeerTransportError::new("offline"));
                }
                healthy_completed_sender
                    .send(())
                    .expect("healthy completed");
                Ok(DfsMetadataJournalBatchResponse {
                    request_id: command.request_id.clone(),
                    shard_id: command.batch.shard_id.clone(),
                    next_after_sequence: command.batch.next_after_sequence,
                    applied_entries: command.batch.entries.len(),
                    verified_prefix_only: command.verify_prefix_only,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::Applied,
                })
            })
        };
        let mut transport = DfsMetadataNodeTransport::new(
            vec![offline_node_id.clone(), healthy_node_id.clone()],
            sender,
            sync_snapshot_sender(),
        );
        let plan = DfsPreparedClusterMetadataJournalPublish {
            data_dir,
            local_node_id: local_node_id.clone(),
            peer_node_ids: vec![offline_node_id.clone(), healthy_node_id.clone()],
            outcomes: Vec::new(),
            targets: vec![
                prepared_target(
                    &local_node_id,
                    offline_node_id.clone(),
                    file_system_id.clone(),
                    "root",
                ),
                prepared_target(
                    &local_node_id,
                    healthy_node_id.clone(),
                    file_system_id,
                    "root",
                ),
            ],
        };

        let publish = std::thread::spawn(move || {
            execute_prepared_dfs_metadata_journal_publish(
                plan,
                &mut transport,
                DfsMetadataJournalReplicationOptions {
                    attempt_timeout: Duration::from_secs(5),
                    target_timeout: Duration::from_secs(5),
                    ..DfsMetadataJournalReplicationOptions::default()
                },
            )
        });
        offline_started_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("offline publish started");
        healthy_completed_receiver
            .recv_timeout(Duration::from_millis(250))
            .expect("healthy publish must complete while offline target is blocked");
        assert!(!publish.is_finished());

        release_offline_sender.send(()).expect("release offline");
        let report = publish
            .join()
            .expect("publish thread")
            .expect("publish report");
        assert!(matches!(
            &report.outcomes[0],
            DfsMetadataJournalReplicationOutcome::Failed { target_node_id, .. }
                if target_node_id == &offline_node_id
        ));
        assert!(matches!(
            &report.outcomes[1],
            DfsMetadataJournalReplicationOutcome::Applied { target_node_id, .. }
                if target_node_id == &healthy_node_id
        ));
    }

    #[test]
    fn healthy_target_publish_does_not_wait_behind_offline_target() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let test_dir = sinomo_testdir::TestDir::new("dfs-metadata-concurrent-targets");
            let data_dir = SinomoDataDir::new(test_dir.path());
            data_dir.ensure_root_layout().expect("root layout");
            let local_node_id =
                NodeId::from_legacy_named("node_metadata_concurrent_local").expect("local node");
            let offline_node_id =
                NodeId::from_legacy_named("node_metadata_concurrent_offline").expect("offline");
            let healthy_node_id =
                NodeId::from_legacy_named("node_metadata_concurrent_healthy").expect("healthy");
            let file_system_id = distributed_sifs(local_node_id.clone()).file_system_id;
            let offline_started = Arc::new(tokio::sync::Notify::new());
            let release_offline = Arc::new(tokio::sync::Notify::new());
            let healthy_completed = Arc::new(tokio::sync::Notify::new());
            let sender: AsyncDfsMetadataJournalBatchSender = {
                let offline_node_id = offline_node_id.clone();
                let offline_started = Arc::clone(&offline_started);
                let release_offline = Arc::clone(&release_offline);
                let healthy_completed = Arc::clone(&healthy_completed);
                Arc::new(move |target_node_id, command, _timeout| {
                    let is_offline = *target_node_id == offline_node_id;
                    let offline_started = Arc::clone(&offline_started);
                    let release_offline = Arc::clone(&release_offline);
                    let healthy_completed = Arc::clone(&healthy_completed);
                    Box::pin(async move {
                        if is_offline {
                            offline_started.notify_one();
                            release_offline.notified().await;
                            return Err(DfsMetadataPeerTransportError::new("offline"));
                        }
                        healthy_completed.notify_one();
                        Ok(DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: command.batch.shard_id.clone(),
                            next_after_sequence: command.batch.next_after_sequence,
                            applied_entries: command.batch.entries.len(),
                            verified_prefix_only: command.verify_prefix_only,
                            verified_authority_probe_id: None,
                            result: DfsMetadataJournalBatchResult::Applied,
                        })
                    })
                })
            };
            let mut transport = AsyncDfsMetadataNodeTransport::new(
                vec![offline_node_id.clone(), healthy_node_id.clone()],
                sender,
                snapshot_sender(),
            );
            let plan = DfsPreparedClusterMetadataJournalPublish {
                data_dir,
                local_node_id: local_node_id.clone(),
                peer_node_ids: vec![offline_node_id.clone(), healthy_node_id.clone()],
                outcomes: Vec::new(),
                targets: vec![
                    prepared_target(
                        &local_node_id,
                        offline_node_id.clone(),
                        file_system_id.clone(),
                        "root",
                    ),
                    prepared_target(
                        &local_node_id,
                        healthy_node_id.clone(),
                        file_system_id,
                        "root",
                    ),
                ],
            };

            let publish = tokio::spawn(async move {
                execute_prepared_dfs_metadata_journal_publish_async(
                    plan,
                    &mut transport,
                    DfsMetadataJournalReplicationOptions {
                        attempt_timeout: Duration::from_secs(5),
                        target_timeout: Duration::from_secs(5),
                        ..DfsMetadataJournalReplicationOptions::default()
                    },
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), offline_started.notified())
                .await
                .expect("offline publish started");
            tokio::time::timeout(Duration::from_millis(250), healthy_completed.notified())
                .await
                .expect("healthy publish must complete while offline target is blocked");
            assert!(!publish.is_finished());

            release_offline.notify_one();
            let report = tokio::time::timeout(Duration::from_secs(1), publish)
                .await
                .expect("publish completed after offline release")
                .expect("publish task")
                .expect("publish report");
            assert!(matches!(
                &report.outcomes[0],
                DfsMetadataJournalReplicationOutcome::Failed { target_node_id, .. }
                    if target_node_id == &offline_node_id
            ));
            assert!(matches!(
                &report.outcomes[1],
                DfsMetadataJournalReplicationOutcome::Applied { target_node_id, .. }
                    if target_node_id == &healthy_node_id
            ));
        });
    }

    #[test]
    fn one_target_node_lane_preserves_shard_plan_order() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let test_dir = sinomo_testdir::TestDir::new("dfs-metadata-target-order");
            let data_dir = SinomoDataDir::new(test_dir.path());
            data_dir.ensure_root_layout().expect("root layout");
            let local_node_id =
                NodeId::from_legacy_named("node_metadata_order_local").expect("local node");
            let target_node_id =
                NodeId::from_legacy_named("node_metadata_order_target").expect("target node");
            let file_system_id = distributed_sifs(local_node_id.clone()).file_system_id;
            let first_started = Arc::new(tokio::sync::Notify::new());
            let release_first = Arc::new(tokio::sync::Notify::new());
            let second_started = Arc::new(tokio::sync::Notify::new());
            let sender: AsyncDfsMetadataJournalBatchSender = {
                let first_started = Arc::clone(&first_started);
                let release_first = Arc::clone(&release_first);
                let second_started = Arc::clone(&second_started);
                Arc::new(move |_target_node_id, command, _timeout| {
                    let is_first = command.batch.shard_id.0 == "root";
                    let first_started = Arc::clone(&first_started);
                    let release_first = Arc::clone(&release_first);
                    let second_started = Arc::clone(&second_started);
                    Box::pin(async move {
                        if is_first {
                            first_started.notify_one();
                            release_first.notified().await;
                        } else {
                            second_started.notify_one();
                        }
                        Ok(DfsMetadataJournalBatchResponse {
                            request_id: command.request_id.clone(),
                            shard_id: command.batch.shard_id.clone(),
                            next_after_sequence: command.batch.next_after_sequence,
                            applied_entries: command.batch.entries.len(),
                            verified_prefix_only: command.verify_prefix_only,
                            verified_authority_probe_id: None,
                            result: DfsMetadataJournalBatchResult::Applied,
                        })
                    })
                })
            };
            let mut transport = AsyncDfsMetadataNodeTransport::new(
                vec![target_node_id.clone()],
                sender,
                snapshot_sender(),
            );
            let plan = DfsPreparedClusterMetadataJournalPublish {
                data_dir,
                local_node_id: local_node_id.clone(),
                peer_node_ids: vec![target_node_id.clone()],
                outcomes: Vec::new(),
                targets: vec![
                    prepared_target(
                        &local_node_id,
                        target_node_id.clone(),
                        file_system_id.clone(),
                        "root",
                    ),
                    prepared_target(&local_node_id, target_node_id, file_system_id, "services"),
                ],
            };

            let publish = tokio::spawn(async move {
                execute_prepared_dfs_metadata_journal_publish_async(
                    plan,
                    &mut transport,
                    DfsMetadataJournalReplicationOptions::default(),
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), first_started.notified())
                .await
                .expect("first shard publish started");
            assert!(
                tokio::time::timeout(Duration::from_millis(100), second_started.notified())
                    .await
                    .is_err(),
                "the second shard must stay queued behind the first for the same node"
            );
            release_first.notify_one();
            tokio::time::timeout(Duration::from_secs(1), second_started.notified())
                .await
                .expect("second shard started after first completed");
            let report = tokio::time::timeout(Duration::from_secs(1), publish)
                .await
                .expect("ordered publish completed")
                .expect("publish task")
                .expect("publish report");
            assert_eq!(report.outcomes.len(), 2);
            assert!(matches!(
                &report.outcomes[0],
                DfsMetadataJournalReplicationOutcome::Applied { shard_id, .. }
                    if shard_id.0 == "root"
            ));
            assert!(matches!(
                &report.outcomes[1],
                DfsMetadataJournalReplicationOutcome::Applied { shard_id, .. }
                    if shard_id.0 == "services"
            ));
        });
    }
}

async fn execute_prepared_dfs_metadata_journal_target_publish_async(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target: DfsPreparedMetadataJournalTargetPublish,
    transport: &dyn AsyncDfsMetadataJournalReplicationTransport,
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
    transport: &dyn AsyncDfsMetadataJournalReplicationTransport,
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
        build_local_dfs_metadata_snapshot_install_command(
            &data_dir,
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

fn build_local_dfs_metadata_snapshot_install_command(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    shard_id: &DfsMetadataShardId,
    reason: String,
) -> Result<DfsMetadataSnapshotInstallCommand, DfsError> {
    let mut backend = open_local_metadata_shard_backend(data_dir, shard_id.clone())?;
    dfs_metadata_snapshot_install_command(
        &mut backend,
        local_node_id,
        target_node_id,
        file_system_id,
        authority_epoch,
        shard_id,
        reason,
    )
}

fn put_local_dfs_metadata_replication_cursor(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<(), DfsError> {
    // Cursor acknowledgement is independent of materialized metadata tables.
    // Keep the same shard write lock and durable monotonic cursor update
    // without loading unrelated table state.
    let root = local_dfs_metadata_shard_path(data_dir, shard_id);
    let _lock = metadata_lock::metadata_write_lock(
        root.clone(),
        metadata_cursor::journal_write_lock_path(&root),
    )?;
    Ok(metadata_cursor::put_metadata_replication_cursor(
        &root,
        target_node_id,
        shard_id,
        next_after_sequence,
    )?)
}

async fn put_local_dfs_metadata_replication_cursor_async(
    data_dir: SinomoDataDir,
    target_node_id: NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
) -> Result<(), DfsError> {
    tokio::task::spawn_blocking(move || {
        put_local_dfs_metadata_replication_cursor(
            &data_dir,
            &target_node_id,
            &shard_id,
            next_after_sequence,
        )
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
    if let Some(data_dir) = host.local_data_dir().cloned() {
        let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
            &default_dfs_metadata_replication_suppression_snapshot_path(&data_dir),
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
        return execute_prepared_dfs_metadata_journal_publish(
            DfsPreparedClusterMetadataJournalPublish {
                data_dir,
                local_node_id: local_node_id.clone(),
                peer_node_ids: Vec::new(),
                outcomes,
                targets,
            },
            transport,
            options,
        );
    }

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
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::Applied,
            },
            Err(reason) => DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: false,
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::Rejected { reason },
            },
        };
    }
    if command.authority_probe.is_some()
        && let Err(reason) = validate_contiguous_metadata_authority_batch(backend, &command.batch)
    {
        return dfs_metadata_journal_rejection_response(&command, reason);
    }
    let context = context.expect("validated metadata replica ingest has a shard context");
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        context.shard.file_system_id.clone(),
        shard_id.clone(),
        context.shard.primary_node_id.clone(),
        context.shard.epoch,
    );
    match backend.apply_journal_batch_with_replica_permit(&permit, &command.batch) {
        Ok(applied_entries) => {
            let verified_authority_probe_id = if let Some(probe) = command.authority_probe.as_ref()
            {
                let confirmation_id =
                    match crate::service_replica::verify_persisted_metadata_authority_probe(
                        backend,
                        &context.shard,
                        probe,
                    ) {
                        Ok(confirmation_id) => confirmation_id,
                        Err(error) => {
                            return dfs_metadata_journal_rejection_response(
                                &command,
                                error.to_string(),
                            );
                        }
                    };
                if let Err(error) =
                    backend.put_metadata_authority_ack_floor(&crate::DfsMetadataAuthorityAckFloor {
                        shard_id: command.batch.shard_id.clone(),
                        journal_sequence: command.batch.next_after_sequence,
                    })
                {
                    return dfs_metadata_journal_rejection_response(
                        &command,
                        format!(
                            "persist metadata authority acknowledgement floor for shard {} sequence {}: {error}",
                            command.batch.shard_id.0, command.batch.next_after_sequence
                        ),
                    );
                }
                Some(confirmation_id)
            } else {
                None
            };
            DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries,
                verified_prefix_only: false,
                // Echo the prefix-bound confirmation only after the exact
                // hidden operation and complete primary history are durable.
                verified_authority_probe_id,
                result: DfsMetadataJournalBatchResult::Applied,
            }
        }
        Err(error) => DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries: 0,
            verified_prefix_only: false,
            verified_authority_probe_id: None,
            result: DfsMetadataJournalBatchResult::Rejected {
                reason: error.to_string(),
            },
        },
    }
}

fn validate_contiguous_metadata_authority_batch(
    backend: &dyn DfsMetadataBackend,
    batch: &DfsMetadataJournalBatch,
) -> Result<(), String> {
    let Some(last_entry) = batch.entries.last() else {
        return Err(format!(
            "metadata authority batch for shard {} must contain the exact hidden operation prepare",
            batch.shard_id.0
        ));
    };
    if last_entry.sequence != batch.next_after_sequence {
        return Err(format!(
            "metadata authority batch for shard {} ends at sequence {} but names sequence {}",
            batch.shard_id.0, last_entry.sequence, batch.next_after_sequence
        ));
    }
    let replayed_through_sequence =
        backend
            .metadata_replay_cursor(&batch.shard_id)
            .map_err(|error| {
                format!(
                    "read metadata replay cursor for authority batch shard {}: {error}",
                    batch.shard_id.0
                )
            })?;
    let mut previous_sequence: Option<u64> = None;
    let mut expected_unseen_sequence = replayed_through_sequence.saturating_add(1);
    for entry in &batch.entries {
        if entry.shard_id != batch.shard_id {
            return Err(format!(
                "metadata authority batch for shard {} contains entry from shard {}",
                batch.shard_id.0, entry.shard_id.0
            ));
        }
        if let Some(previous_sequence) = previous_sequence
            && entry.sequence != previous_sequence.saturating_add(1)
        {
            return Err(format!(
                "metadata authority batch for shard {} is not contiguous between sequences {} and {}",
                batch.shard_id.0, previous_sequence, entry.sequence
            ));
        }
        previous_sequence = Some(entry.sequence);
        if entry.sequence <= replayed_through_sequence {
            continue;
        }
        if entry.sequence != expected_unseen_sequence {
            return Err(format!(
                "metadata authority batch for shard {} starts unseen history at sequence {} but durable replay requires sequence {}",
                batch.shard_id.0, entry.sequence, expected_unseen_sequence
            ));
        }
        expected_unseen_sequence = expected_unseen_sequence.saturating_add(1);
    }
    Ok(())
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
    backend
        .refresh_metadata_shard_durable_view(&probe.shard_id)
        .map_err(|error| {
            format!(
                "refresh durable metadata view for prefix probe shard {} sequence {}: {error}",
                batch.shard_id.0, probe.journal_sequence
            )
        })?;
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
    let shard = sinomo_core::kv::read_cluster_kv_entry(
        &cluster_paths.kv_paths(),
        &metadata_shard_key(&sifs_file_system_id(), &shard_id.0),
    )?
    .map(|entry| entry.decode::<DfsMetadataShardConfig>())
    .transpose()?;
    let pending_membership_handoff = shard
        .as_ref()
        .map(|shard| {
            sinomo_core::kv::read_cluster_kv_entry(
                &cluster_paths.kv_paths(),
                &metadata_shard_membership_handoff_key(&shard.file_system_id, &shard.shard_id),
            )?
            .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
            .transpose()
        })
        .transpose()?
        .flatten();
    Ok(shard.map(|shard| DfsMetadataReplicaShardContext {
        local_node_id: state.local_node_id,
        pending_membership_handoff: pending_membership_handoff.filter(|handoff| {
            handoff.retiring_shard == shard
                && handoff.desired_shard.file_system_id == shard.file_system_id
                && handoff.desired_shard.shard_id == shard.shard_id
                && handoff.desired_shard.primary_node_id == shard.primary_node_id
                && handoff.desired_shard.epoch == shard.epoch.saturating_add(1)
        }),
        shard,
    }))
}

pub fn process_cluster_dfs_metadata_journal_batch(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, DfsError> {
    if command.source_node_id.as_ref() != Some(authenticated_source_node_id) {
        return Ok(dfs_metadata_journal_rejection_response(
            &command,
            format!(
                "authenticated metadata journal caller {authenticated_source_node_id} does not match claimed source {}",
                command
                    .source_node_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<missing>".to_string())
            ),
        ));
    }
    let context =
        cluster_dfs_metadata_replica_shard_context(cluster_paths, &command.batch.shard_id)?;
    if let Some(response) = validate_dfs_metadata_journal_batch_source(context.as_ref(), &command) {
        return Ok(response);
    }
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    let backend = cached_cluster_metadata_backend(data_dir, &command.batch.shard_id)?;
    let mut backend = backend.lock().map_err(|_| DfsError::Metadata {
        message: format!(
            "cluster metadata backend lock is poisoned for shard {}",
            command.batch.shard_id.0
        ),
    })?;
    Ok(process_dfs_metadata_journal_batch(
        &mut *backend,
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
    let context = context.expect("validated metadata snapshot ingest has a shard context");
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        context.shard.file_system_id.clone(),
        shard_id.clone(),
        context.shard.primary_node_id.clone(),
        context.shard.epoch,
    );
    match backend.replace_with_replicated_snapshot_with_replica_permit(
        &permit,
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
    if let Some(proof) = command.snapshot.prefix_proof.as_ref() {
        if !proof.is_well_formed() || proof.journal_sequence != command.next_after_sequence {
            return Ok(Some(format!(
                "metadata snapshot prefix proof does not name exact install sequence {} for shard {}",
                command.next_after_sequence, command.shard_id.0
            )));
        }
        let Some(expected_semantic_fingerprint) =
            command.snapshot.semantic_state_fingerprint.as_ref()
        else {
            return Ok(Some(format!(
                "metadata snapshot prefix proof for shard {} sequence {} is missing its semantic checksum",
                command.shard_id.0, command.next_after_sequence
            )));
        };
        let actual_semantic_fingerprint =
            crate::metadata_records::dfs_metadata_table_snapshot_state_fingerprint(
                &command.snapshot,
            )
            .map_err(|source| DfsError::Serialization {
                message: format!(
                    "encode metadata snapshot semantic checksum for {}: {source}",
                    command.shard_id.0
                ),
            })?;
        if &actual_semantic_fingerprint != expected_semantic_fingerprint {
            return Ok(Some(format!(
                "metadata snapshot semantic checksum does not match its prefix proof transfer for shard {} sequence {}",
                command.shard_id.0, command.next_after_sequence
            )));
        }
    }
    if let Some(authority_floor) = backend.metadata_authority_ack_floor(&command.shard_id)?
        && command.next_after_sequence <= authority_floor.journal_sequence
    {
        return Ok(Some(format!(
            "metadata snapshot sequence {} would replace shard {} at or behind locally acknowledged authority operation {}",
            command.next_after_sequence, command.shard_id.0, authority_floor.journal_sequence
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
        let matches = crate::metadata_records::dfs_metadata_table_snapshot_matches_checkpoint(
            &command.snapshot,
            command.next_after_sequence,
            &floor.state_fingerprint,
        )
        .map_err(|source| DfsError::Serialization {
            message: format!(
                "encode metadata snapshot local acknowledgement check for {}: {source}",
                command.shard_id.0
            ),
        })?;
        if !matches {
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
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataSnapshotInstallCommand,
) -> Result<DfsMetadataSnapshotInstallResponse, DfsError> {
    if command.source_node_id != *authenticated_source_node_id {
        return Ok(DfsMetadataSnapshotInstallResponse {
            request_id: command.request_id,
            shard_id: command.shard_id,
            next_after_sequence: command.next_after_sequence,
            result: DfsMetadataSnapshotInstallResult::Rejected {
                reason: format!(
                    "authenticated metadata snapshot caller {authenticated_source_node_id} does not match claimed source {}",
                    command.source_node_id
                ),
            },
        });
    }
    let context = cluster_dfs_metadata_replica_shard_context(cluster_paths, &command.shard_id)?;
    if let Some(response) =
        validate_dfs_metadata_snapshot_install_source(context.as_ref(), &command)
    {
        return Ok(response);
    }
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    match open_local_metadata_shard_backend(data_dir, command.shard_id.clone()) {
        Ok(mut backend) => Ok(process_dfs_metadata_snapshot_install(
            &mut backend,
            context.as_ref(),
            command,
        )),
        Err(open_error) => {
            let request_id = command.request_id.clone();
            let shard_id = command.shard_id.clone();
            let next_after_sequence = command.next_after_sequence;
            let root = local_dfs_metadata_shard_path(data_dir, &shard_id);
            let context = context
                .as_ref()
                .expect("validated metadata snapshot ingest has a shard context");
            let permit = DfsMetadataReplicaIngestPermit::new_validated(
                context.shard.file_system_id.clone(),
                shard_id.clone(),
                context.shard.primary_node_id.clone(),
                context.shard.epoch,
            );
            let result = LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
                &permit,
                shard_id.clone(),
                root,
                command.shard_id,
                command.next_after_sequence,
                command.snapshot,
            );
            let result = match result {
                Ok((_backend, archived_existing)) => {
                    DfsMetadataSnapshotInstallResult::Installed { archived_existing }
                }
                Err(repair_error) => DfsMetadataSnapshotInstallResult::Rejected {
                    reason: format!(
                        "cannot open metadata shard for snapshot install: {open_error}; unreadable-shard snapshot replacement failed: {repair_error}"
                    ),
                },
            };
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result,
            })
        }
    }
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
    backend.migrate_objects_to_configured_shards(
        &file_system.file_system_id,
        DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_MUTATION_LIMIT,
    )?;
    Ok(())
}

pub fn validate_dfs_metadata_journal_batch_source(
    context: Option<&DfsMetadataReplicaShardContext>,
    command: &DfsMetadataJournalBatchCommand,
) -> Option<DfsMetadataJournalBatchResponse> {
    let Some(context) = context else {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata journal ingest requires an exact committed cluster shard context".to_string(),
        ));
    };
    if context.shard.shard_id != command.batch.shard_id.0 {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            "metadata journal batch has no local shard config".to_string(),
        ));
    }
    let ordinary_applied_batch = !command.verify_prefix_only && command.authority_probe.is_none();
    if ordinary_applied_batch
        && (command.file_system_id.as_ref() != Some(&context.shard.file_system_id)
            || command.authority_epoch != Some(context.shard.epoch))
    {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata journal authority identity does not match filesystem {} shard {} epoch {}",
                context.shard.file_system_id, context.shard.shard_id, context.shard.epoch
            ),
        ));
    }
    if let Some(probe) = command.authority_probe.as_ref()
        && (command.verify_prefix_only
            || command.prefix_probe.is_some()
            || command.batch.entries.is_empty()
            || probe.probe_id.is_empty()
            || probe.expected_shard != context.shard
            || !crate::service_replica::metadata_authority_operation_is_well_formed_for_shard(
                &probe.operation,
                &context.shard,
            )
            || crate::service_replica::metadata_authority_operation_prepare_sequence(
                &probe.operation,
            ) != command.batch.next_after_sequence)
    {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "metadata authority probe does not match the exact committed assignment for shard {} epoch {}",
                context.shard.shard_id, context.shard.epoch
            ),
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
    let configured_replica = context
        .shard
        .replica_node_ids
        .contains(&context.local_node_id);
    // A desired-only member can start behind the retiring prefix. The exact
    // pending handoff is a scoped replica-ingest permit for both the ordinary
    // contiguous suffix and its subsequent exact-prefix proof. Restricting
    // this to the proof alone deadlocks: the sender suppresses that proof
    // until its durable cursor says the suffix has landed.
    let handoff_catch_up_target = metadata_membership_handoff_allows_local_target(context)
        && command.authority_probe.is_none()
        && any_handoff_catch_up_command(command, ordinary_applied_batch);
    if !configured_replica && !handoff_catch_up_target {
        return Some(dfs_metadata_journal_rejection_response(
            command,
            format!(
                "local node {} is neither a configured replica nor an exact pending membership-handoff catch-up target for metadata shard {}",
                context.local_node_id, context.shard.shard_id
            ),
        ));
    }
    None
}

fn any_handoff_catch_up_command(
    command: &DfsMetadataJournalBatchCommand,
    ordinary_applied_batch: bool,
) -> bool {
    (ordinary_applied_batch && command.prefix_probe.is_none())
        || (command.verify_prefix_only && command.prefix_probe.is_some())
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
        && !metadata_membership_handoff_allows_local_target(context)
    {
        return Some(reject(format!(
            "local node {} is neither a configured replica nor a pending membership-handoff catch-up target for metadata shard {}",
            context.local_node_id, context.shard.shard_id
        )));
    }
    None
}

fn metadata_membership_handoff_allows_local_target(
    context: &DfsMetadataReplicaShardContext,
) -> bool {
    context
        .pending_membership_handoff
        .as_ref()
        .is_some_and(|handoff| {
            !handoff.handoff_id.is_empty()
                && handoff.retiring_shard == context.shard
                && handoff.desired_shard.file_system_id == context.shard.file_system_id
                && handoff.desired_shard.shard_id == context.shard.shard_id
                && handoff.desired_shard.primary_node_id == context.shard.primary_node_id
                && handoff.desired_shard.epoch == context.shard.epoch.saturating_add(1)
                && std::iter::once(&handoff.desired_shard.primary_node_id)
                    .chain(handoff.desired_shard.replica_node_ids.iter())
                    .any(|node_id| node_id == &context.local_node_id)
        })
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
        verified_authority_probe_id: None,
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
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    batch: DfsMetadataJournalBatch,
) -> DfsMetadataJournalBatchCommand {
    let next_after_sequence = batch.next_after_sequence;
    DfsMetadataJournalBatchCommand {
        request_id: format!(
            "dfs_meta_{}_{}_{}",
            local_node_id, target_node_id, next_after_sequence
        ),
        source_node_id: Some(local_node_id.clone()),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
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

        let command = dfs_metadata_journal_batch_command(
            local_node_id,
            target_node_id,
            file_system_id,
            authority_epoch,
            batch,
        );
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
        || ((reason.contains("metadata journal batch")
            || reason.contains("metadata authority batch"))
            && reason.contains("starts unseen history")
            && reason.contains("durable replay requires"))
        || (reason.contains("metadata shard")
            && reason.contains("cannot extend prefix")
            && reason.contains("with journal entry"))
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

fn dfs_metadata_value_response(
    result: ObjectRpcSessionResult,
) -> Result<ObjectRpcResponse, DfsMetadataPeerTransportError> {
    let (response, body) = result.into_parts();
    if !body.is_empty() {
        return Err(DfsMetadataPeerTransportError::new(format!(
            "DFS metadata route {} returned an unexpected attachment",
            response.route
        )));
    }
    Ok(response)
}

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
                    ObjectRpcSessionCall::value(request),
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            let response = dfs_metadata_value_response(response)?;
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
                    ObjectRpcSessionCall::value(request),
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            let response = dfs_metadata_value_response(response)?;
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
                ObjectRpcSessionCall::value(request),
                DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
            )
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = dfs_metadata_value_response(response)?;
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
                ObjectRpcSessionCall::value(request),
                DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
            )
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = dfs_metadata_value_response(response)?;
        decode_dfs_metadata_snapshot_install_rpc_response(&response)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
    })
}

#[cfg(test)]
mod cursor_update_tests {
    use super::*;

    #[test]
    fn default_sifs_bootstrap_shard_migration_is_bounded() {
        assert_eq!(DEFAULT_SIFS_BOOTSTRAP_SHARD_MIGRATION_MUTATION_LIMIT, 256);
        let source = include_str!("metadata_replication.rs");
        assert!(!source.contains(concat!(
            "migrate_objects_to_configured_shards(&file_system.file_system_id, ",
            "usize::MAX"
        )));
    }

    #[test]
    fn journal_discontinuity_rejection_allows_snapshot_repair() {
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata serialization error: metadata journal batch for shard services starts unseen history at sequence 7722 but durable replay requires 1"
        ));
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata authority batch for shard root starts unseen history at sequence 9 but durable replay requires sequence 2"
        ));
        assert!(metadata_journal_rejection_allows_snapshot_repair(
            "metadata serialization error: metadata shard root cannot extend prefix 2216 with journal entry root:2216"
        ));
        assert!(!metadata_journal_rejection_allows_snapshot_repair(
            "metadata journal batch for shard services contains entry from shard root"
        ));
    }

    #[test]
    fn unauthorized_cluster_replica_ingress_does_not_create_local_layout() {
        let target = sinomo_testdir::TestDir::new("sinomo-dfs-replica-ingress-no-layout");
        let data_dir = SinomoDataDir::new(target.path());
        let cluster_paths = ClusterPaths::new(&data_dir);
        let source_node_id =
            NodeId::from_legacy_named("replica-ingress-source").expect("source node id");
        let shard_id = DfsMetadataShardId::new("unconfigured");
        let file_system_id = sifs_file_system_id();
        assert!(!data_dir.dfs_dir().exists());

        let journal_response = process_cluster_dfs_metadata_journal_batch(
            &data_dir,
            &cluster_paths,
            &source_node_id,
            DfsMetadataJournalBatchCommand {
                request_id: "unauthorized-journal".to_string(),
                source_node_id: Some(source_node_id.clone()),
                file_system_id: Some(file_system_id.clone()),
                authority_epoch: Some(1),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                batch: DfsMetadataJournalBatch {
                    shard_id: shard_id.clone(),
                    entries: Vec::new(),
                    next_after_sequence: 1,
                },
            },
        )
        .expect("journal rejection response");
        assert!(matches!(
            journal_response.result,
            DfsMetadataJournalBatchResult::Rejected { .. }
        ));
        assert!(!data_dir.dfs_dir().exists());

        let snapshot_source =
            sinomo_testdir::TestDir::new("sinomo-dfs-replica-ingress-snapshot-source");
        let snapshot = LocalSobjMetadataBackend::new(shard_id.clone(), snapshot_source.path())
            .expect("snapshot source backend")
            .export_table_snapshot();
        let snapshot_response = process_cluster_dfs_metadata_snapshot_install(
            &data_dir,
            &cluster_paths,
            &source_node_id,
            DfsMetadataSnapshotInstallCommand {
                request_id: "unauthorized-snapshot".to_string(),
                source_node_id: source_node_id.clone(),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(1),
                shard_id,
                next_after_sequence: 1,
                snapshot,
                reason: "test unauthorized ingress".to_string(),
            },
        )
        .expect("snapshot rejection response");
        assert!(matches!(
            snapshot_response.result,
            DfsMetadataSnapshotInstallResult::Rejected { .. }
        ));
        assert!(!data_dir.dfs_dir().exists());
    }

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
