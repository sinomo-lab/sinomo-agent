//! DFS metadata journal replication and default metadata materialization.
//!
//! This module owns the metadata publish pipeline: deciding when service events
//! require journal publication, routing journal/snapshot replication to peer
//! nodes, validating incoming replica updates, and materializing the default
//! SIFS metadata records. Durable metadata storage itself remains owned by
//! `metadata_backend`.

use crate::{DfsMetadataRejection, DfsMetadataRejectionCode};

use serde::{Deserialize, Serialize};
use sinomo_core::cluster::ClusterKvPathsExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterConnectivityState, ClusterPaths, ObjectRpcResponse,
    ObjectRpcResult, cluster_directory_for, cluster_nodes_share_healthy_component,
};
use sinomo_core::node::NodeId;
use sinomo_core::{ClusterBootstrapState, ClusterEndpointRecord};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::Poll;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

#[cfg(test)]
use crate::cluster_dfs_peer_node_ids;
use crate::maintenance::DfsMaintenanceExecutionExclusions;
use crate::metadata_sharded_backend::ResidentMetadataReplicationCursorStatusObserver;
use crate::node_transport::value_response_from_rpc_result;
use crate::{
    AsyncDfsPeerTransport, DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
    DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT, DfsDistributedWriteResult, DfsError, DfsFileSystem,
    DfsFileSystemId, DfsMetadataBackend, DfsMetadataDurableReplication, DfsMetadataJournalBatch,
    DfsMetadataJournalBatchCommand, DfsMetadataJournalBatchResponse, DfsMetadataJournalBatchResult,
    DfsMetadataJournalFrameRecord, DfsMetadataPrefixProbe, DfsMetadataReplicaApplyReport,
    DfsMetadataReplicaIngestPermit, DfsMetadataReplicationSuppressionRecord,
    DfsMetadataShardConfig, DfsMetadataShardId, DfsMetadataShardMembershipHandoff,
    DfsMetadataSnapshotInstallCommand, DfsMetadataSnapshotInstallResponse,
    DfsMetadataSnapshotInstallResult, DfsNamespaceResult, DfsObjectKind, DfsObjectRpcSendOptions,
    DfsObjectVersionRecord, DfsPeerTransport, DfsPeerTransportFuture, DfsVersion,
    LocalShardedMetadataBackend, LocalSobjMetadataBackend, NetworkedDfsServiceEvent,
    ResidentLocalMetadataShardLease, cluster_dfs_metadata_shards,
    decode_dfs_metadata_journal_batch_rpc_response,
    decode_dfs_metadata_snapshot_install_rpc_response,
    default_dfs_metadata_replication_suppression_snapshot_path,
    dfs_metadata_journal_batch_rpc_call, dfs_metadata_snapshot_install_rpc_call,
    dfs_now_unix_nanos, discovered_cluster_dfs_peer_node_ids, distributed_sifs,
    distributed_sifs_bootstrap_objects_for_nodes, local_dfs_metadata_shard_path,
    metadata_shard_key, metadata_shard_membership_handoff_key, open_local_metadata_shard_backend,
    put_local_dfs_metadata_replication_suppression,
    read_dfs_metadata_replication_suppression_snapshot, sifs_file_system_id,
};
use crate::{metadata_cursor, metadata_lock};

static DFS_METADATA_REPLICATION_SUPPRESSION_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
pub(crate) const DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON: &str =
    "metadata replica durable state is unreadable and requires authoritative snapshot repair";

pub(crate) fn metadata_backend_open_error_allows_snapshot_repair(
    error: &crate::MetadataBackendError,
) -> bool {
    if error.rejection_code().allows_snapshot_repair() {
        return true;
    }
    matches!(error, crate::MetadataBackendError::Serde(_))
}

pub const DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;
const DEFAULT_DFS_METADATA_REPLICATION_MAX_CONCURRENT_TARGET_NODES: usize = 8;

fn read_cluster_dfs_metadata_publish_state(
    cluster_paths: &ClusterPaths,
) -> Result<ClusterBootstrapState, DfsError> {
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    if !sinomo_core::kv::cluster_kv_raft_local_is_voter(&state) {
        return Err(DfsError::Metadata {
            message: "non-voter DFS metadata publication requires an explicit retained authority projection"
                .to_string(),
        });
    }
    Ok(state)
}

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

    let state = read_cluster_dfs_metadata_publish_state(cluster_paths)?;
    let file_system = distributed_sifs(state.local_node_id.clone());
    let shards = cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?;

    let peer_node_ids = discovered_cluster_dfs_peer_node_ids(cluster_paths, &state)?;
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
    publish_cluster_dfs_metadata_journal_async_with_execution_exclusions(
        data_dir,
        cluster_paths,
        sender,
        snapshot_sender,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
    .await
}

pub async fn publish_cluster_dfs_metadata_journal_async_with_execution_exclusions(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    sender: AsyncDfsMetadataJournalBatchSender,
    snapshot_sender: AsyncDfsMetadataSnapshotInstallSender,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let Some(plan) = prepare_cluster_dfs_metadata_journal_publish_async(
        data_dir,
        cluster_paths,
        execution_exclusions,
        None,
        options,
    )
    .await?
    else {
        return Ok(DfsMetadataJournalReplicationReport {
            outcomes: Vec::new(),
        });
    };
    let transport =
        AsyncDfsMetadataNodeTransport::new(plan.peer_node_ids.clone(), sender, snapshot_sender);
    execute_prepared_dfs_metadata_journal_publish_async(plan, &transport, options).await
}

pub fn publish_cluster_dfs_metadata_journal_with_peer_transport(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_transport: Arc<dyn DfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    publish_cluster_dfs_metadata_journal(
        data_dir,
        cluster_paths,
        dfs_metadata_journal_batch_sender_from_peer_transport(
            data_dir,
            Arc::clone(&peer_transport),
        ),
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
        async_dfs_metadata_journal_batch_sender_from_peer_transport(
            data_dir,
            Arc::clone(&peer_transport),
        ),
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
    publish_cluster_dfs_metadata_journal_with_peer_transports_async_and_execution_exclusions(
        data_dir,
        cluster_paths,
        sync_peer_transport,
        peer_transport,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
    .await
}

pub async fn publish_cluster_dfs_metadata_journal_with_peer_transports_async_and_execution_exclusions(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    _sync_peer_transport: Arc<dyn DfsPeerTransport>,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    publish_cluster_dfs_metadata_journal_async_with_execution_exclusions(
        data_dir,
        cluster_paths,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(
            data_dir,
            Arc::clone(&peer_transport),
        ),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        execution_exclusions,
        options,
    )
    .await
}

/// Capture one shard's durable publication plan without performing any peer
/// I/O.
///
/// This compatibility path is voter-local and fails closed for non-voters;
/// it never refreshes or materializes the complete cluster KV store. Daemon
/// callers should use the explicit-authority resident-backend entry point,
/// holding exact-shard admission only around bounded local plan capture and
/// releasing it before the potentially slow peer execution phase.
#[allow(clippy::too_many_arguments)]
pub fn prepare_cluster_dfs_metadata_shard_publish_with_execution_exclusions(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    let trace_started = Instant::now();
    let prepared = prepare_cluster_dfs_metadata_journal_publish(
        data_dir.clone(),
        cluster_paths.clone(),
        execution_exclusions,
        Some((file_system_id, shard_id)),
        options,
    )?;
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

/// Capture one shard's durable publication plan from an already resident
/// metadata backend.
///
/// The daemon uses this entry point while holding exact-shard admission so
/// publication observes the same backend instance as foreground mutations.
/// Standalone callers can continue using
/// [`prepare_cluster_dfs_metadata_shard_publish_with_execution_exclusions`],
/// which opens a bounded local backend for the planning call.
#[allow(clippy::too_many_arguments)]
pub fn prepare_cluster_dfs_metadata_shard_publish_with_backend_and_execution_exclusions(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    backend: &mut ResidentLocalMetadataShardLease,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    if !cluster_paths.state.exists() {
        return Err(DfsError::Metadata {
            message: "exact DFS metadata publish lacks cluster state".to_string(),
        });
    }

    let state = read_cluster_dfs_metadata_publish_state(cluster_paths)?;
    let peer_node_ids = discovered_cluster_dfs_peer_node_ids(cluster_paths, &state)?;
    let shard = cluster_dfs_metadata_shards(cluster_paths, &file_system_id)?
        .into_iter()
        .find(|shard| shard.shard_id == shard_id.0);
    prepare_cluster_dfs_metadata_shard_publish_with_backend_and_authority(
        data_dir,
        &state.local_node_id,
        peer_node_ids,
        file_system_id,
        shard_id,
        shard,
        backend,
        execution_exclusions,
        options,
    )
}

/// Capture one shard publication plan from an explicit, coherent authority
/// projection. Daemon callers use this after binding the resident backend to
/// the same retained generation, so retries never fetch or materialize the
/// complete cluster KV snapshot.
#[allow(clippy::too_many_arguments)]
pub fn prepare_cluster_dfs_metadata_shard_publish_with_backend_and_authority(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    peer_node_ids: Vec<NodeId>,
    file_system_id: DfsFileSystemId,
    shard_id: DfsMetadataShardId,
    authoritative_shard: Option<DfsMetadataShardConfig>,
    backend: &mut ResidentLocalMetadataShardLease,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    let trace_started = Instant::now();
    let expected_shard_root = local_dfs_metadata_shard_path(data_dir, &shard_id);
    if backend.file_system_id() != &file_system_id
        || backend.shard_id() != &shard_id
        || backend.shard_root() != expected_shard_root
    {
        return Err(DfsError::Metadata {
            message: format!(
                "resident DFS metadata lease {}:{} at {} does not match requested {}:{} at {}",
                backend.file_system_id(),
                backend.shard_id().0,
                backend.shard_root().display(),
                file_system_id,
                shard_id.0,
                expected_shard_root.display(),
            ),
        });
    }
    if authoritative_shard
        .as_ref()
        .is_some_and(|shard| shard.file_system_id != file_system_id || shard.shard_id != shard_id.0)
    {
        return Err(DfsError::Metadata {
            message: format!(
                "authoritative DFS metadata shard does not match requested {}:{}",
                file_system_id, shard_id.0,
            ),
        });
    }
    let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
        &default_dfs_metadata_replication_suppression_snapshot_path(data_dir),
    )?
    .map(|snapshot| snapshot.records)
    .unwrap_or_default();

    let (outcomes, targets) = match authoritative_shard {
        None => (
            vec![DfsMetadataJournalReplicationOutcome::MissingShard {
                file_system_id,
                shard_id,
            }],
            Vec::new(),
        ),
        Some(shard) if shard.primary_node_id != *local_node_id => {
            let primary_node_id = shard.primary_node_id;
            let epoch = shard.epoch;
            (
                vec![DfsMetadataJournalReplicationOutcome::NotPrimary {
                    file_system_id,
                    shard_id,
                    local_node_id: local_node_id.clone(),
                    primary_node_id,
                    epoch,
                }],
                Vec::new(),
            )
        }
        Some(shard) => {
            backend.refresh_metadata_shard_durable_view(&shard_id)?;
            prepare_dfs_metadata_journal_publish_for_primary_shard(
                &mut **backend,
                local_node_id,
                shard,
                &metadata_suppressions,
                execution_exclusions,
                options,
                |backend, target_node_id, shard_id| {
                    let cursor = backend.metadata_replication_cursor(target_node_id, shard_id)?;
                    // A retry can find a previously published hint already at
                    // the head and plan NoChanges. Discharge a registered
                    // selection wait from that same observation; the hint is
                    // still not an activation or durability certificate.
                    crate::default_reconciliation::notify_primary_transfer_checkpoint(
                        data_dir,
                        target_node_id,
                        shard_id,
                        cursor,
                    );
                    Ok(cursor)
                },
            )?
        }
    };
    let prepared = Some(DfsPreparedClusterMetadataJournalPublish {
        data_dir: data_dir.clone(),
        local_node_id: local_node_id.clone(),
        peer_node_ids,
        outcomes,
        targets,
        resident_cursor_status_observer: Some(
            backend.metadata_replication_cursor_status_observer(),
        ),
    });
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
        "metadata-publish-plan-resident"
    );
    Ok(prepared)
}

/// Execute an already captured cluster metadata-publication plan.
///
/// This phase performs peer I/O and durable cursor checkpointing, but does not
/// reopen the primary journal or recalculate the captured suffix.
pub async fn execute_prepared_cluster_dfs_metadata_publish_with_async_peer_transport(
    plan: DfsPreparedClusterMetadataJournalPublish,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let transport = AsyncDfsMetadataNodeTransport::new(
        plan.peer_node_ids.clone(),
        async_dfs_metadata_journal_batch_sender_from_peer_transport(
            &plan.data_dir,
            Arc::clone(&peer_transport),
        ),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );
    execute_prepared_dfs_metadata_journal_publish_async(plan, &transport, options).await
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
    /// Send one empty authenticated batch to a replica whose durable sender
    /// cursor is already at the primary head. Intended for periodic
    /// maintenance and explicit repair commands, not foreground operations.
    pub probe_caught_up_replicas: bool,
    /// Only asynchronous publishers opt in. Synchronous barriers keep the
    /// default foreground priority to break cross-node admission dependencies.
    pub background_publication: bool,
}

impl Default for DfsMetadataJournalReplicationOptions {
    fn default() -> Self {
        Self {
            batch_entry_limit: DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT,
            batch_byte_limit: DEFAULT_METADATA_JOURNAL_BATCH_BYTE_LIMIT,
            max_batches_per_target: DEFAULT_DFS_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
            attempt_timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            target_timeout: sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            probe_caught_up_replicas: false,
            background_publication: false,
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
        assert!(!options.probe_caught_up_replicas);
        assert!(!options.background_publication);
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
    CursorDurabilityInconsistencyDetected {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        cursor_after_sequence: u64,
        durable_head_sequence: u64,
    },
    CursorDurabilityInconsistencyHealed {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        previous_cursor_after_sequence: u64,
        next_after_sequence: u64,
    },
    CursorDurabilityInconsistencySnapshotInstalled {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        previous_cursor_after_sequence: u64,
        next_after_sequence: u64,
        archived_existing: bool,
    },
    CursorDurabilityInconsistencyUnhealed {
        target_node_id: NodeId,
        shard_id: DfsMetadataShardId,
        cursor_after_sequence: u64,
        durable_head_sequence: u64,
        reason: String,
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
                    | DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencySnapshotInstalled { .. }
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
                shard_id,
                reason,
                ..
            }
            | DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id,
                shard_id,
                reason,
                ..
            } => {
                if dfs_metadata_report_has_cursor_durability_inconsistency(
                    report,
                    target_node_id,
                    shard_id,
                ) {
                    None
                } else {
                    Some(format!(
                        "DFS metadata replication to {target_node_id} failed: {reason}"
                    ))
                }
            }
            DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                shard_id,
                max_batches,
                ..
            } => {
                if dfs_metadata_report_has_cursor_durability_inconsistency(
                    report,
                    target_node_id,
                    shard_id,
                ) {
                    None
                } else {
                    Some(format!(
                        "DFS metadata replication to {target_node_id} still has backlog after {max_batches} batches"
                    ))
                }
            }
            DfsMetadataJournalReplicationOutcome::NoChanges { .. }
            | DfsMetadataJournalReplicationOutcome::MissingRoute { .. }
            | DfsMetadataJournalReplicationOutcome::TopologyMismatchSuppressed { .. }
            | DfsMetadataJournalReplicationOutcome::MissingShard { .. }
            | DfsMetadataJournalReplicationOutcome::NotPrimary { .. }
            | DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyDetected {
                ..
            }
            | DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyHealed {
                ..
            }
            | DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencySnapshotInstalled {
                ..
            }
            | DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyUnhealed {
                ..
            } => None,
        })
        .collect()
}

fn dfs_metadata_report_has_cursor_durability_inconsistency(
    report: &DfsMetadataJournalReplicationReport,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
) -> bool {
    report.outcomes.iter().any(|outcome| {
        matches!(
            outcome,
            DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyDetected {
                target_node_id: detected_target,
                shard_id: detected_shard,
                ..
            } if detected_target == target_node_id && detected_shard == shard_id
        )
    })
}

pub fn dfs_metadata_replication_fault_notice_lines(
    report: &DfsMetadataJournalReplicationReport,
) -> Vec<String> {
    report
        .outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyDetected {
                target_node_id,
                shard_id,
                cursor_after_sequence,
                durable_head_sequence,
            } => Some(format!(
                "DFS metadata fault healing detected an outbound cursor/durability inconsistency for shard {} target {target_node_id}: cursor={cursor_after_sequence}, durable_head={durable_head_sequence}",
                shard_id.0
            )),
            DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyHealed {
                target_node_id,
                shard_id,
                previous_cursor_after_sequence,
                next_after_sequence,
            } => Some(format!(
                "DFS metadata fault healing reconstructed the outbound cursor for shard {} target {target_node_id} after exact-prefix verification: cursor={previous_cursor_after_sequence}->{next_after_sequence}",
                shard_id.0
            )),
            DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencySnapshotInstalled {
                target_node_id,
                shard_id,
                previous_cursor_after_sequence,
                next_after_sequence,
                archived_existing,
            } => Some(format!(
                "DFS metadata fault healing repaired shard {} target {target_node_id} with an authoritative snapshot and reconstructed its outbound cursor: cursor={previous_cursor_after_sequence}->{next_after_sequence}, archived_existing={archived_existing}",
                shard_id.0
            )),
            DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyUnhealed {
                target_node_id,
                shard_id,
                cursor_after_sequence,
                durable_head_sequence,
                reason,
            } => Some(format!(
                "DFS metadata fault healing could not repair the outbound cursor/durability inconsistency for shard {} target {target_node_id}: cursor={cursor_after_sequence}, durable_head={durable_head_sequence}, reason={reason}",
                shard_id.0
            )),
            DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id,
                shard_id,
                reason,
            }
            | DfsMetadataJournalReplicationOutcome::Failed {
                target_node_id,
                shard_id,
                reason,
            } if dfs_metadata_report_has_cursor_durability_inconsistency(
                report,
                target_node_id,
                shard_id,
            ) => Some(format!(
                "DFS metadata fault healing for shard {} target {target_node_id} failed: {reason}",
                shard_id.0
            )),
            DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                shard_id,
                after_sequence,
                max_batches,
            } if dfs_metadata_report_has_cursor_durability_inconsistency(
                report,
                target_node_id,
                shard_id,
            ) => Some(format!(
                "DFS metadata fault healing for shard {} target {target_node_id} was deferred at sequence {after_sequence} after {max_batches} batches",
                shard_id.0
            )),
            DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id,
                shard_id,
                ..
            } if dfs_metadata_report_has_cursor_durability_inconsistency(
                report,
                target_node_id,
                shard_id,
            ) => Some(format!(
                "DFS metadata fault healing for shard {} target {target_node_id} is waiting for a replication route",
                shard_id.0
            )),
            _ => None,
        })
        .collect()
}

pub fn dfs_events_should_publish_metadata(events: &[NetworkedDfsServiceEvent]) -> bool {
    events.iter().any(|event| match event {
        NetworkedDfsServiceEvent::OwnerHandoffCompleted { .. } => true,
        NetworkedDfsServiceEvent::RemoteWriteServed { result, .. } => {
            matches!(result, DfsDistributedWriteResult::Outcome { .. })
        }
        // The primary journals routed mutations and must publish them to
        // replicas. Requester-side completions only replay returned metadata
        // and, for committed writes, install a blob-only content manifest;
        // they never append the requester's metadata-shard journal.
        NetworkedDfsServiceEvent::RemoteWriteCompleted { .. }
        | NetworkedDfsServiceEvent::RemoteNamespaceCompleted { .. }
        | NetworkedDfsServiceEvent::StableReconciliationServed { .. }
        | NetworkedDfsServiceEvent::StableReconciliationCompleted { .. }
        | NetworkedDfsServiceEvent::RemoteResponseDiscarded { .. } => false,
        NetworkedDfsServiceEvent::RemoteNamespaceServed {
            result,
            mutates_metadata,
            ..
        } => *mutates_metadata && matches!(result, DfsNamespaceResult::Applied),
        // Content-replica results are notifications, not metadata authorship.
        // The owning journal mutation records its publication edge separately.
        NetworkedDfsServiceEvent::BlobStaged { .. }
        | NetworkedDfsServiceEvent::VersionCommitted { .. }
        | NetworkedDfsServiceEvent::ReplicaRepaired { .. }
        | NetworkedDfsServiceEvent::BlobStageRejected { .. }
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
        | NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. }
        | NetworkedDfsServiceEvent::ContentRootCatalogPageServed { .. }
        | NetworkedDfsServiceEvent::ContentRootCatalogPageReceived { .. } => false,
    })
}

/// Returns true only when an accepted replica operation produced evidence that
/// the local metadata replica advanced. Rejected or stale-authority traffic is
/// deliberately excluded so it cannot churn higher-level reactive consumers.
pub fn dfs_events_advance_metadata_replica(events: &[NetworkedDfsServiceEvent]) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            NetworkedDfsServiceEvent::MetadataJournalBatchApplied { .. }
                | NetworkedDfsServiceEvent::MetadataSnapshotInstalled { .. }
        )
    })
}

pub fn dfs_metadata_replication_report_needs_retry(
    report: &DfsMetadataJournalReplicationReport,
) -> bool {
    // These outcomes all leave a durable target cursor behind the primary.
    // Keep the exact shard lane dirty until it catches up instead of relying on
    // a process-wide maintenance scan to rediscover the work. Authority and
    // topology outcomes are terminal for this lane generation; their own
    // topology/change events schedule a fresh lane.
    dfs_metadata_publish_retry_class(report) != DfsMetadataPublishRetryClass::Complete
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DfsMetadataPublishRetryClass {
    Complete,
    /// A bounded publish round advanced normally but has more journal pages.
    Progress,
    /// A transport was attempted and failed; retry with exponential backoff.
    Failed,
    /// Current topology proves there is no route; retain the lane but wait for
    /// connectivity/new-input generation (plus one sparse safety reprobe).
    MissingRoute,
}

fn dfs_metadata_publish_retry_class(
    report: &DfsMetadataJournalReplicationReport,
) -> DfsMetadataPublishRetryClass {
    if report.outcomes.iter().any(|outcome| {
        matches!(
            outcome,
            DfsMetadataJournalReplicationOutcome::Failed { .. }
                | DfsMetadataJournalReplicationOutcome::Rejected { .. }
                | DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyUnhealed { .. }
        )
    }) {
        return DfsMetadataPublishRetryClass::Failed;
    }
    let backlog = report
        .outcomes
        .iter()
        .filter_map(|outcome| match outcome {
            DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id,
                shard_id,
                ..
            } => Some((target_node_id, shard_id)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if !backlog.is_empty() {
        let every_backlog_advanced = backlog.iter().all(|(target_node_id, shard_id)| {
            report.outcomes.iter().any(|outcome| match outcome {
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: applied_target,
                    shard_id: applied_shard,
                    entries,
                    ..
                } => {
                    applied_target == *target_node_id && applied_shard == *shard_id && *entries > 0
                }
                DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                    target_node_id: installed_target,
                    shard_id: installed_shard,
                    ..
                } => installed_target == *target_node_id && installed_shard == *shard_id,
                DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencySnapshotInstalled {
                    target_node_id: installed_target,
                    shard_id: installed_shard,
                    ..
                } => installed_target == *target_node_id && installed_shard == *shard_id,
                _ => false,
            })
        });
        return if every_backlog_advanced {
            DfsMetadataPublishRetryClass::Progress
        } else {
            // A target budget can expire before any command is sent, and a
            // zero batch budget also reports backlog. Yield-retrying either
            // case would be a control-plane CPU loop, so retain the lane under
            // ordinary failure backoff until new input or its retry deadline.
            DfsMetadataPublishRetryClass::Failed
        };
    }
    if report.outcomes.iter().any(|outcome| {
        matches!(
            outcome,
            DfsMetadataJournalReplicationOutcome::MissingRoute { .. }
        )
    }) {
        return DfsMetadataPublishRetryClass::MissingRoute;
    }
    DfsMetadataPublishRetryClass::Complete
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
    scheduler: Arc<DfsMetadataPublishScheduler>,
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

pub(crate) fn schedule_dfs_metadata_publish_async_on_runtime_owned<P, Fut, R, E>(
    scheduler: Arc<DfsMetadataPublishScheduler>,
    runtime: tokio::runtime::Handle,
    options: DfsMetadataPublishScheduleOptions,
    publish: P,
    on_report: R,
    on_error: E,
) -> (
    DfsMetadataPublishScheduleStart,
    Option<tokio::task::JoinHandle<()>>,
)
where
    P: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
    R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
    E: FnMut(String) + Send + 'static,
{
    scheduler.request_publish_async_on_runtime_owned(runtime, options, publish, on_report, on_error)
}

pub fn schedule_dfs_metadata_publish_after_events_async_on_runtime<P, Fut, R, E>(
    scheduler: Arc<DfsMetadataPublishScheduler>,
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

fn default_dfs_metadata_publish_scheduler() -> Arc<DfsMetadataPublishScheduler> {
    // This compatibility API carries no data-root or shard identity, so it
    // cannot safely coalesce independent callers: the winning task would own
    // one caller's closure and discard the other's. Identity-aware production
    // paths use the keyed cluster/daemon registries; anonymous callers receive
    // a private lane and therefore never lose work through closure aliasing.
    Arc::new(DfsMetadataPublishScheduler::new())
}

fn cluster_dfs_metadata_materialization_scheduler(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> Arc<DfsMetadataPublishScheduler> {
    static SCHEDULERS: OnceLock<
        Mutex<BTreeMap<(PathBuf, DfsMetadataShardId), Weak<DfsMetadataPublishScheduler>>>,
    > = OnceLock::new();
    let key = (data_dir.root(), shard_id.clone());
    let Ok(mut schedulers) = SCHEDULERS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    else {
        return Arc::new(DfsMetadataPublishScheduler::new());
    };
    if let Some(scheduler) = schedulers.get(&key).and_then(Weak::upgrade) {
        return scheduler;
    }
    schedulers.retain(|_, scheduler| scheduler.strong_count() > 0);
    let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
    schedulers.insert(key, Arc::downgrade(&scheduler));
    scheduler
}

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
        default_dfs_metadata_publish_scheduler(),
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
        dfs_metadata_journal_batch_sender_from_peer_transport(
            data_dir,
            Arc::clone(&peer_transport),
        ),
        dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
        events,
        replication_options,
        on_report,
        on_error,
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMetadataPublishScheduleOptions {
    /// Number of low-latency retries before the retained lane starts backing
    /// off. Zero disables autonomous retries while preserving coalescing.
    pub max_retry_rounds: usize,
    pub retry_delay: Duration,
    /// Upper bound for a retained dirty lane's exponential retry delay.
    pub max_retry_delay: Duration,
}

// Route-generation notifications are the fast path, but they are deliberately
// coalesced process-local hints rather than a durable journal. Bound the safety
// reprobe so a missed edge cannot strand metadata catch-up for minutes.
const DFS_METADATA_MISSING_ROUTE_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

impl Default for DfsMetadataPublishScheduleOptions {
    fn default() -> Self {
        Self {
            max_retry_rounds: 3,
            retry_delay: Duration::from_millis(750),
            max_retry_delay: Duration::from_secs(5 * 60),
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
    wake: tokio::sync::Notify,
}

struct DfsMetadataPublishRunGuard {
    scheduler: Arc<DfsMetadataPublishScheduler>,
    owns_running: bool,
}

impl DfsMetadataPublishRunGuard {
    fn new(scheduler: Arc<DfsMetadataPublishScheduler>) -> Self {
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
            wake: tokio::sync::Notify::const_new(),
        }
    }

    /// Return whether another request arrived after the active pass claimed
    /// its current input edge.
    ///
    /// Specialized retained workers use this to debounce rebuildable work
    /// while preserving the scheduler's ordinary pending/running ownership.
    /// The run loop remains the only consumer which clears this bit.
    pub fn has_pending_request(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }

    /// Consume queued input edges after the worker owns the producer's exact
    /// admission gate. The guarded operation must cover all input accepted
    /// before this claim; later producers will enqueue another edge.
    pub(crate) fn claim_pending_requests(&self) {
        self.pending.store(false, Ordering::Release);
    }

    fn claim_publish_start(&self) -> DfsMetadataPublishScheduleStart {
        self.pending.store(true, Ordering::Release);
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Wake a retained lane immediately for new journal or
            // connectivity input instead of waiting for its backoff timer.
            self.wake.notify_one();
            DfsMetadataPublishScheduleStart::AlreadyRunning
        } else {
            DfsMetadataPublishScheduleStart::Started
        }
    }

    pub fn request_publish_async_on_runtime<P, Fut, R, E>(
        self: Arc<Self>,
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
        self.request_publish_async_on_runtime_owned(runtime, options, publish, on_report, on_error)
            .0
    }

    pub(crate) fn request_publish_async_on_runtime_owned<P, Fut, R, E>(
        self: Arc<Self>,
        runtime: tokio::runtime::Handle,
        options: DfsMetadataPublishScheduleOptions,
        publish: P,
        on_report: R,
        on_error: E,
    ) -> (
        DfsMetadataPublishScheduleStart,
        Option<tokio::task::JoinHandle<()>>,
    )
    where
        P: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<DfsMetadataJournalReplicationReport, String>> + Send + 'static,
        R: FnMut(DfsMetadataJournalReplicationReport) + Send + 'static,
        E: FnMut(String) + Send + 'static,
    {
        let start = self.claim_publish_start();
        let task = if start == DfsMetadataPublishScheduleStart::Started {
            let run_guard = DfsMetadataPublishRunGuard::new(Arc::clone(&self));
            Some(runtime.spawn(async move {
                self.run_publish_loop_async(options, publish, on_report, on_error, run_guard)
                    .await;
            }))
        } else {
            None
        };
        (start, task)
    }

    async fn run_publish_loop_async<P, Fut, R, E>(
        self: &Arc<Self>,
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
            self.claim_pending_requests();
            let retry_class = match publish().await {
                Ok(report) => {
                    let retry_class = dfs_metadata_publish_retry_class(&report);
                    on_report(report);
                    retry_class
                }
                Err(error) => {
                    on_error(error);
                    DfsMetadataPublishRetryClass::Failed
                }
            };
            if self.pending.load(Ordering::Acquire) {
                retry_rounds = 0;
                continue;
            }
            match retry_class {
                DfsMetadataPublishRetryClass::Progress if options.max_retry_rounds > 0 => {
                    // Successful bounded progress is not a failure streak. Give
                    // other ready lanes one scheduler turn, then keep draining.
                    retry_rounds = 0;
                    tokio::task::yield_now().await;
                    continue;
                }
                DfsMetadataPublishRetryClass::Failed if options.max_retry_rounds > 0 => {
                    retry_rounds = retry_rounds.saturating_add(1);
                    let delay = dfs_metadata_publish_retry_delay(&options, retry_rounds);
                    self.wait_for_retry_or_input(delay).await;
                    continue;
                }
                DfsMetadataPublishRetryClass::MissingRoute if options.max_retry_rounds > 0 => {
                    retry_rounds = 0;
                    self.wait_for_retry_or_input(dfs_metadata_missing_route_retry_delay(&options))
                        .await;
                    continue;
                }
                DfsMetadataPublishRetryClass::Complete
                | DfsMetadataPublishRetryClass::Progress
                | DfsMetadataPublishRetryClass::Failed
                | DfsMetadataPublishRetryClass::MissingRoute => {}
            }
            if run_guard.release_or_reclaim_pending() {
                retry_rounds = 0;
                continue;
            }
            break;
        }
    }

    async fn wait_for_retry_or_input(&self, delay: Duration) {
        if self.pending.load(Ordering::Acquire) {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = self.wake.notified() => {}
        }
    }
}

fn dfs_metadata_publish_retry_delay(
    options: &DfsMetadataPublishScheduleOptions,
    retry_round: usize,
) -> Duration {
    if retry_round <= options.max_retry_rounds {
        return options.retry_delay;
    }
    let backoff_round = retry_round.saturating_sub(options.max_retry_rounds).min(31) as u32;
    let multiplier = 1u32 << backoff_round;
    options
        .retry_delay
        .saturating_mul(multiplier)
        .min(options.max_retry_delay.max(options.retry_delay))
}

fn dfs_metadata_missing_route_retry_delay(options: &DfsMetadataPublishScheduleOptions) -> Duration {
    options
        .max_retry_delay
        .min(DFS_METADATA_MISSING_ROUTE_RETRY_MAX_DELAY)
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

    fn rejected_publish_report() -> DfsMetadataJournalReplicationReport {
        DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::Rejected {
                target_node_id: NodeId::from_legacy_named(
                    "node_metadata_publish_scheduler_rejected",
                )
                .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                reason: "metadata journal batch has no local shard config".to_string(),
            }],
        }
    }

    fn backlog_publish_report() -> DfsMetadataJournalReplicationReport {
        let target_node_id = NodeId::from_legacy_named("node_metadata_publish_scheduler_backlog")
            .expect("target node");
        let shard_id = DfsMetadataShardId::new("services");
        DfsMetadataJournalReplicationReport {
            outcomes: vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    entries: 1,
                    next_after_sequence: 7,
                },
                DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                    target_node_id,
                    shard_id,
                    after_sequence: 7,
                    max_batches: 1,
                },
            ],
        }
    }

    fn stalled_backlog_publish_report() -> DfsMetadataJournalReplicationReport {
        DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: NodeId::from_legacy_named(
                    "node_metadata_publish_scheduler_stalled_backlog",
                )
                .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                after_sequence: 7,
                max_batches: 0,
            }],
        }
    }

    #[test]
    fn only_accepted_replica_events_signal_progress() {
        let source = NodeId::from_legacy_named("node_metadata_replica_progress").expect("node");
        let shard_id = DfsMetadataShardId::new("services");
        let rejected = NetworkedDfsServiceEvent::MetadataJournalBatchRejected {
            request_id: "request-rejected".to_string(),
            source_node_id: source.clone(),
            shard_id: shard_id.clone(),
            reason: "stale authority".to_string(),
        };
        assert!(!dfs_events_advance_metadata_replica(&[rejected]));

        let applied = NetworkedDfsServiceEvent::MetadataJournalBatchApplied {
            request_id: "request-applied".to_string(),
            source_node_id: source.clone(),
            shard_id: shard_id.clone(),
            applied_entries: 0,
            next_after_sequence: 7,
            verified_prefix_only: true,
        };
        assert!(dfs_events_advance_metadata_replica(&[applied]));

        let snapshot = NetworkedDfsServiceEvent::MetadataSnapshotInstalled {
            request_id: "request-snapshot".to_string(),
            source_node_id: source,
            shard_id,
            next_after_sequence: 11,
        };
        assert!(dfs_events_advance_metadata_replica(&[snapshot]));
    }

    #[test]
    fn anonymous_default_publish_callers_never_alias_closures() {
        let first = default_dfs_metadata_publish_scheduler();
        let second = default_dfs_metadata_publish_scheduler();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "an identity-free compatibility API cannot safely coalesce independent closures"
        );
    }

    #[test]
    fn retry_classifier_retains_durable_target_failures() {
        assert_eq!(
            dfs_metadata_publish_retry_class(&backlog_publish_report()),
            DfsMetadataPublishRetryClass::Progress
        );
        assert_eq!(
            dfs_metadata_publish_retry_class(&failed_publish_report()),
            DfsMetadataPublishRetryClass::Failed
        );
        assert_eq!(
            dfs_metadata_publish_retry_class(&rejected_publish_report()),
            DfsMetadataPublishRetryClass::Failed,
            "a receiver which is still hydrating must remain under bounded retry"
        );
        assert_eq!(
            dfs_metadata_publish_retry_class(&stalled_backlog_publish_report()),
            DfsMetadataPublishRetryClass::Failed,
            "backlog without a proven cursor advance must not yield-spin"
        );
        assert!(dfs_metadata_replication_report_needs_retry(
            &failed_publish_report()
        ));
        let missing_route = DfsMetadataJournalReplicationReport {
            outcomes: vec![DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id: NodeId::from_legacy_named(
                    "node_metadata_publish_scheduler_missing",
                )
                .expect("target node"),
                shard_id: DfsMetadataShardId::new("services"),
                after_sequence: 7,
            }],
        };
        assert_eq!(
            dfs_metadata_publish_retry_class(&missing_route),
            DfsMetadataPublishRetryClass::MissingRoute
        );
        assert!(dfs_metadata_replication_report_needs_retry(&missing_route));
        assert!(dfs_metadata_replication_report_needs_retry(
            &backlog_publish_report()
        ));
        let mut mixed = failed_publish_report();
        mixed.outcomes.extend(backlog_publish_report().outcomes);
        assert_eq!(
            dfs_metadata_publish_retry_class(&mixed),
            DfsMetadataPublishRetryClass::Failed,
            "a progressing target must not mask another target's failure"
        );
        assert!(
            dfs_metadata_replication_report_needs_retry(&mixed),
            "any durable lag keeps the exact shard lane active"
        );
    }

    #[test]
    fn failed_target_report_remains_scheduled_until_clean() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(if call < 4 {
                    failed_publish_report()
                } else {
                    DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    }
                }))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn backlog_report_remains_scheduled_until_clean() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(if call < 4 {
                    backlog_publish_report()
                } else {
                    DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    }
                }))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
        ));

        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn stalled_backlog_waits_instead_of_spinning() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(async {
            let result = tokio::time::timeout(
                Duration::from_millis(50),
                scheduler.run_publish_loop_async(
                    DfsMetadataPublishScheduleOptions {
                        max_retry_rounds: 3,
                        retry_delay: Duration::from_secs(60 * 60),
                        max_retry_delay: Duration::from_secs(60 * 60),
                    },
                    move || {
                        publish_calls.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(Ok(stalled_backlog_publish_report()))
                    },
                    |_| {},
                    |_| {},
                    DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
                ),
            )
            .await;
            assert!(result.is_err(), "the retained lane should be sleeping");
        });

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no-progress backlog must not consume a worker in a yield loop"
        );
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn healthy_backlog_progress_never_enters_failure_backoff() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);

        runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_secs(1),
                scheduler.run_publish_loop_async(
                    DfsMetadataPublishScheduleOptions {
                        max_retry_rounds: 3,
                        retry_delay: Duration::from_secs(60 * 60),
                        max_retry_delay: Duration::from_secs(60 * 60),
                    },
                    move || {
                        let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                        std::future::ready(Ok(if call < 128 {
                            backlog_publish_report()
                        } else {
                            DfsMetadataJournalReplicationReport {
                                outcomes: Vec::new(),
                            }
                        }))
                    },
                    |_| {},
                    |_| {},
                    DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
                ),
            )
            .await
            .expect("healthy progress must not sleep on failure backoff");
        });

        assert_eq!(calls.load(Ordering::SeqCst), 129);
        assert!(!scheduler.running.load(Ordering::Acquire));
    }

    #[test]
    fn retained_retry_delay_backs_off_to_a_hard_cap() {
        let options = DfsMetadataPublishScheduleOptions {
            max_retry_rounds: 2,
            retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(50),
        };

        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 1),
            Duration::from_millis(10)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 2),
            Duration::from_millis(10)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 3),
            Duration::from_millis(20)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 4),
            Duration::from_millis(40)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, 5),
            Duration::from_millis(50)
        );
        assert_eq!(
            dfs_metadata_publish_retry_delay(&options, usize::MAX),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn missing_route_safety_reprobe_has_a_short_hard_cap() {
        let mut options = DfsMetadataPublishScheduleOptions {
            max_retry_delay: Duration::from_secs(5 * 60),
            ..DfsMetadataPublishScheduleOptions::default()
        };
        assert_eq!(
            dfs_metadata_missing_route_retry_delay(&options),
            Duration::from_secs(5)
        );
        options.max_retry_delay = Duration::from_millis(250);
        assert_eq!(
            dfs_metadata_missing_route_retry_delay(&options),
            Duration::from_millis(250),
            "an explicitly tighter retry remains authoritative"
        );
    }

    #[test]
    fn pending_request_during_failure_runs_one_fresh_publish() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        scheduler.running.store(true, Ordering::Release);
        let calls = Arc::new(AtomicUsize::new(0));
        let publish_calls = Arc::clone(&calls);
        let request_scheduler = Arc::clone(&scheduler);

        runtime.block_on(scheduler.run_publish_loop_async(
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 3,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
            },
            move || {
                let call = publish_calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    assert_eq!(
                        request_scheduler.claim_publish_start(),
                        DfsMetadataPublishScheduleStart::AlreadyRunning
                    );
                }
                std::future::ready(Ok(if call == 0 {
                    failed_publish_report()
                } else {
                    DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    }
                }))
            },
            |_| {},
            |_| {},
            DfsMetadataPublishRunGuard::new(Arc::clone(&scheduler)),
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
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let discarded_calls = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let (report_sender, report_receiver) = std::sync::mpsc::channel();

        let publish_calls = Arc::clone(&calls);
        let publish_started = Arc::clone(&first_started);
        let publish_release = Arc::clone(&release_first);
        let start = schedule_dfs_metadata_publish_async_on_runtime(
            Arc::clone(&scheduler),
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
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
            Arc::clone(&scheduler),
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
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
        let (started_sender, started_receiver) = std::sync::mpsc::channel();

        let start = schedule_dfs_metadata_publish_async_on_runtime(
            Arc::clone(&scheduler),
            runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
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
            Arc::clone(&scheduler),
            recovery_runtime.handle().clone(),
            DfsMetadataPublishScheduleOptions {
                max_retry_rounds: 0,
                retry_delay: Duration::ZERO,
                max_retry_delay: Duration::ZERO,
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
        let scheduler = Arc::new(DfsMetadataPublishScheduler::new());
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
    if matches!(&response.result, DfsMetadataJournalBatchResult::Applied)
        && command
            .durable_replication
            .as_ref()
            .is_some_and(|replication| {
                response.verified_authority_probe_id.as_deref()
                    != Some(replication.confirmation_id.as_str())
            })
    {
        return Err(
            "ordinary durable metadata replication returned no matching prefix confirmation"
                .to_string(),
        );
    }
    Ok(())
}

fn dfs_metadata_journal_batch_command_entry_count(
    command: &DfsMetadataJournalBatchCommand,
) -> usize {
    command.batch.journal_frames.len()
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
        Ok(backend.metadata_replication_cursor(target_node_id, shard_id)?)
    }
}

/// Immutable metadata-publication work captured from one durable cluster view.
///
/// The fields remain private so callers cannot alter authority evidence,
/// journal bounds, or target ordering between planning and execution.
#[derive(Clone, Debug)]
pub struct DfsPreparedClusterMetadataJournalPublish {
    data_dir: SinomoDataDir,
    local_node_id: NodeId,
    peer_node_ids: Vec<NodeId>,
    outcomes: Vec<DfsMetadataJournalReplicationOutcome>,
    targets: Vec<DfsPreparedMetadataJournalTargetPublish>,
    resident_cursor_status_observer: Option<ResidentMetadataReplicationCursorStatusObserver>,
}

#[derive(Clone, Debug)]
pub(crate) struct DfsPreparedMetadataJournalTargetPublish {
    pub(crate) target_node_id: NodeId,
    pub(crate) file_system_id: DfsFileSystemId,
    pub(crate) shard_id: DfsMetadataShardId,
    pub(crate) shard_epoch: u64,
    pub(crate) after_sequence: u64,
    /// Durable source head represented by this immutable preparation.
    pub(crate) prepared_source_head: u64,
    pub(crate) commands: Vec<DfsMetadataJournalBatchCommand>,
    pub(crate) backlog_after_sequence: Option<u64>,
    pub(crate) caught_up_probe: bool,
    cursor_durability_inconsistency: Option<DfsMetadataCursorDurabilityInconsistency>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DfsMetadataCursorDurabilityInconsistency {
    cursor_after_sequence: u64,
    durable_head_sequence: u64,
}

fn validate_prepared_caught_up_probe(
    target: &DfsPreparedMetadataJournalTargetPublish,
) -> Result<(), DfsError> {
    if !target.caught_up_probe {
        return Ok(());
    }
    let expected_probe_sequence = target
        .cursor_durability_inconsistency
        .map(|inconsistency| inconsistency.durable_head_sequence)
        .unwrap_or(target.after_sequence);
    let valid_inconsistency = target
        .cursor_durability_inconsistency
        .is_none_or(|inconsistency| {
            inconsistency.cursor_after_sequence == target.after_sequence
                && inconsistency.cursor_after_sequence < inconsistency.durable_head_sequence
        });
    let valid = valid_inconsistency && target.commands.len() == 1 && {
        let command = &target.commands[0];
        command.batch.journal_frames.is_empty()
            && command.batch.shard_id == target.shard_id
            && command.batch.next_after_sequence == expected_probe_sequence
            && command.verify_prefix_only
            && command.prefix_probe.as_ref().is_some_and(|probe| {
                probe.file_system_id.as_ref() == Some(&target.file_system_id)
                    && probe.authority_epoch == Some(target.shard_epoch)
                    && probe.shard_id == target.shard_id
                    && probe.journal_sequence == expected_probe_sequence
                    && !probe.barrier_id.is_empty()
                    && !probe.state_fingerprint.is_empty()
                    && probe.primary_handoff_id.is_none()
            })
            && command.authority_probe.is_none()
    };
    if valid {
        Ok(())
    } else {
        Err(DfsError::Metadata {
            message: format!(
                "caught-up metadata probe for shard {} must contain exactly one empty exact-prefix batch at sequence {}",
                target.shard_id.0, expected_probe_sequence
            ),
        })
    }
}

fn prepare_cluster_dfs_metadata_journal_publish(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    only_lane: Option<(DfsFileSystemId, DfsMetadataShardId)>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    if !cluster_paths.state.exists() && only_lane.is_none() {
        return Ok(None);
    }
    if !cluster_paths.state.exists() {
        return Err(DfsError::Metadata {
            message: "exact DFS metadata publish lacks cluster state".to_string(),
        });
    }

    let state = read_cluster_dfs_metadata_publish_state(&cluster_paths)?;
    let file_system_id = only_lane
        .as_ref()
        .map(|(file_system_id, _)| file_system_id.clone())
        .unwrap_or_else(|| distributed_sifs(state.local_node_id.clone()).file_system_id);
    let mut shards = cluster_dfs_metadata_shards(&cluster_paths, &file_system_id)?;
    if let Some((_, only_shard_id)) = only_lane.as_ref() {
        shards.retain(|shard| shard.shard_id == only_shard_id.0);
    }
    let peer_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)?;
    let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
        &default_dfs_metadata_replication_suppression_snapshot_path(&data_dir),
    )?
    .map(|snapshot| snapshot.records)
    .unwrap_or_default();
    let (outcomes, targets) =
        prepare_dfs_metadata_journal_publish_with_local_backends_and_execution_exclusions(
            &data_dir,
            file_system_id,
            &state.local_node_id,
            shards,
            &metadata_suppressions,
            execution_exclusions,
            options,
        )?;
    Ok(Some(DfsPreparedClusterMetadataJournalPublish {
        data_dir,
        local_node_id: state.local_node_id,
        peer_node_ids,
        outcomes,
        targets,
        resident_cursor_status_observer: None,
    }))
}

#[cfg(test)]
mod metadata_publish_membership_tests {
    use super::*;

    #[test]
    fn native_metadata_execution_exclusions_park_only_the_exact_offline_target() {
        let nodes =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-exclusion-plan", 3);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-exclusion-plan", nodes[0].clone())
                .unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let offline = shard.replica_node_ids[0].clone();
        let healthy = shard.replica_node_ids[1].clone();
        let data_dir = runtime.data_dir(&shard.primary_node_id).clone();
        let resident = {
            let cache_slot = crate::runtime_host::daemon_dfs_message_service_cache()
                .root(&data_dir.root())
                .expect("actual root cache slot");
            let cache = cache_slot.lock().unwrap();
            cache
                .as_ref()
                .unwrap()
                .bounded_status_resident_current()
                .unwrap()
        };
        let handle = resident
            .scope_shard(
                &shard.file_system_id,
                &DfsMetadataShardId::new(shard.shard_id.clone()),
            )
            .unwrap();
        let mut lease = handle.checkout().unwrap();
        let before = lease.export_table_snapshot();
        let plan = crate::metadata_replication::prepare_cluster_dfs_metadata_shard_publish_with_backend_and_authority(
        &data_dir, &shard.primary_node_id, shard.replica_node_ids.clone(), shard.file_system_id.clone(),
        DfsMetadataShardId::new(shard.shard_id.clone()), Some(shard.clone()), &mut lease,
        &DfsMaintenanceExecutionExclusions::new([offline.clone()]),
        DfsMetadataJournalReplicationOptions { probe_caught_up_replicas: true, ..Default::default() },
    ).unwrap().unwrap();
        assert_eq!(
            lease.export_table_snapshot(),
            before,
            "planning does not author visible state"
        );
        drop(lease);
        let (_, peer) = runtime.peer_transports(&shard.primary_node_id).unwrap();
        let transport = AsyncDfsMetadataNodeTransport::new(
        shard.replica_node_ids.clone(),
        crate::metadata_replication::async_dfs_metadata_journal_batch_sender_from_peer_transport(
            &data_dir, Arc::clone(&peer),
        ),
        crate::metadata_replication::async_dfs_metadata_snapshot_install_sender_from_peer_transport(
            peer,
        ),
    );
        let executor = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = executor
            .block_on(super::execute_prepared_dfs_metadata_journal_publish_async(
                plan,
                &transport,
                DfsMetadataJournalReplicationOptions::default(),
            ))
            .unwrap();
        assert!(report.outcomes.iter().any(|outcome| matches!(outcome,
        DfsMetadataJournalReplicationOutcome::MissingRoute { target_node_id, .. } if target_node_id == &offline)));
        assert!(report.outcomes.iter().any(|outcome| matches!(outcome,
        DfsMetadataJournalReplicationOutcome::NoChanges { target_node_id, .. } | DfsMetadataJournalReplicationOutcome::Applied { target_node_id, .. } if target_node_id == &healthy)));
        drop(executor);
        drop(transport);
        drop(handle);
        drop(resident);
        runtime.close().unwrap();
    }

    #[test]
    fn resident_publish_planning_reuses_exact_shard_and_rejects_wrong_root() {
        let root = sinomo_testdir::TestDir::new("dfs-resident-publish-identity");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local_node_id =
            NodeId::from_legacy_named("node_dfs_resident_publish_local").expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_dfs_resident_publish_identity",
            )
            .expect("cluster id"),
            "resident publish identity",
            local_node_id.clone(),
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        let snapshot =
            sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
                .expect("initial cluster KV snapshot");
        let file_system_id = sifs_file_system_id();
        let shard_id = DfsMetadataShardId::new("root");
        let shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.0.clone(),
            primary_node_id: local_node_id.clone(),
            replica_node_ids: vec![local_node_id],
            epoch: 1,
            assignment: crate::DfsMetadataShardAssignment::Root,

            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut accepted =
            sinomo_core::kv::InProcessKvStore::restore(snapshot).expect("accepted KV store");
        let shard_key = metadata_shard_key(&file_system_id, &shard_id.0);
        accepted
            .put_serde(&shard_key, &shard, None)
            .expect("metadata shard");
        sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
            &cluster_paths.kv_paths(),
            &accepted.try_snapshot().expect("accepted KV snapshot"),
            |_| false,
        )
        .expect("import shard snapshot");

        let metadata_root = crate::default_dfs_metadata_shards_root(&data_dir);
        let resident = LocalShardedMetadataBackend::new(&metadata_root, vec![shard.clone()])
            .expect("resident backend")
            .into_resident();
        let handle = resident
            .scope_shard(&file_system_id, &shard_id)
            .expect("resident shard handle");
        let mut lease = handle.checkout().expect("resident shard lease");
        crate::metadata_backend::reset_metadata_backend_open_calls();
        let plan =
            prepare_cluster_dfs_metadata_shard_publish_with_backend_and_execution_exclusions(
                &data_dir,
                &cluster_paths,
                file_system_id.clone(),
                shard_id.clone(),
                &mut lease,
                &DfsMaintenanceExecutionExclusions::default(),
                DfsMetadataJournalReplicationOptions::default(),
            )
            .expect("resident publish plan")
            .expect("cluster plan");
        assert!(plan.targets.is_empty());
        assert_eq!(
            crate::metadata_backend::metadata_backend_open_calls(),
            0,
            "resident publish planning must not reopen the shard"
        );
        drop(lease);

        let wrong_resident =
            LocalShardedMetadataBackend::new(root.path().join("wrong-metadata-root"), vec![shard])
                .expect("wrong-root resident backend")
                .into_resident();
        let wrong_handle = wrong_resident
            .scope_shard(&file_system_id, &shard_id)
            .expect("wrong-root handle");
        let mut wrong_lease = wrong_handle.checkout().expect("wrong-root lease");
        let error =
            prepare_cluster_dfs_metadata_shard_publish_with_backend_and_execution_exclusions(
                &data_dir,
                &cluster_paths,
                file_system_id,
                shard_id,
                &mut wrong_lease,
                &DfsMaintenanceExecutionExclusions::default(),
                DfsMetadataJournalReplicationOptions::default(),
            )
            .expect_err("wrong-root resident lease must be rejected");
        assert!(error.to_string().contains("does not match requested"));
    }

    #[test]
    fn final_native_metadata_ack_clears_resident_status_lag() {
        let nodes =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-resident-ack-status", 3);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(
            "native-resident-ack-status",
            nodes[0].clone(),
        )
        .unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let primary = shard.primary_node_id.clone();
        let data_dir = runtime.data_dir(&primary).clone();
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let captures = nodes
            .iter()
            .map(|node| {
                (
                    node.clone(),
                    crate::native_shard_trace::capture_native_shard_trace(
                        &runtime.data_dir(node).root(),
                        shard_id.clone(),
                    )
                    .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let publication_deadline =
            std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let mut writer = runtime.client(&primary).unwrap();
        writer
            .posix_write_file(
                "/native-resident-ack.bin",
                b"real durable publication",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![primary.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        drop(writer);
        let expected_prefix = {
            let inspector = runtime
                .metadata_primary_service("/native-resident-ack.bin")
                .unwrap();
            inspector
                .metadata_backend()
                .unwrap()
                .metadata_prefix_proof(&shard_id)
                .unwrap()
                .expect("the actual current committed source prefix")
        };
        let tick = runtime
            .maintenance_tick(
                &primary,
                publication_deadline.saturating_duration_since(std::time::Instant::now()),
            )
            .expect("the ordinary Main publisher completes within the original publication budget");
        assert!(
            tick.outcome.is_ok(),
            "actual Main publication outcome: {:?}",
            tick.outcome
        );
        assert!(std::time::Instant::now() <= publication_deadline);
        let observed = captures
            .iter()
            .map(|(node, capture)| {
                let mut bytes = Vec::new();
                capture.export_raw(&mut bytes).unwrap();
                (
                    node.clone(),
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let source = observed.get(&primary).unwrap();
        let expected = serde_json::to_value(&expected_prefix).unwrap();
        for target in &shard.replica_node_ids {
            let returned = source["boundary_packets"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| &row["facts"])
                .find(|fact| {
                    fact["phase"] == "source-journal-response-accepted"
                        && fact["authenticated_source_node_id"] == target.as_str()
                        && fact["expected_target_node_id"] == target.as_str()
                        && fact["response"]["shard_id"] == shard.shard_id
                        && fact["response"]["next_after_sequence"].as_u64()
                            == Some(expected_prefix.journal_sequence)
                        && fact["response"]["result"] == "applied"
                })
                .expect("the actual source accepted this target's exact final prefix ACK");
            let receiver = observed.get(target).unwrap();
            assert!(
                receiver["boundary_packets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| &row["facts"])
                    .any(|fact| fact["phase"] == "replica-journal-persisted"
                        && fact["command"]["request_id"] == returned["response"]["request_id"]
                        && fact["command"]["source_node_id"] == primary.as_str()
                        && fact["command"]["file_system_id"] == shard.file_system_id.as_str()
                        && fact["command"]["batch"]["shard_id"] == shard.shard_id
                        && fact["command"]["batch"]["next_after_sequence"].as_u64()
                            == Some(expected_prefix.journal_sequence)
                        && fact["durable_prefix"] == expected),
                "the genuine final ACK correlates with that receiver's exact durable frame prefix"
            );
        }
        let resident = {
            let cache_slot = crate::runtime_host::daemon_dfs_message_service_cache()
                .root(&data_dir.root())
                .expect("actual root cache slot");
            let cache = cache_slot.lock().unwrap();
            cache
                .as_ref()
                .unwrap()
                .bounded_status_resident_current()
                .unwrap()
        };
        let status = resident.bounded_status_snapshot().unwrap();
        let actual = status
            .metadata_shards
            .iter()
            .find(|row| row.shard_id.0 == shard.shard_id)
            .unwrap();
        assert!(actual.journal_last_sequence > 0);
        for target in &shard.replica_node_ids {
            let lag = actual
                .replica_lag
                .iter()
                .find(|row| &row.target_node_id == target)
                .unwrap();
            assert_eq!(
                lag.lag_entries, 0,
                "the actual durable ACK clears resident lag"
            );
            assert_eq!(
                lag.replicated_through_sequence,
                actual.journal_last_sequence
            );
            assert_eq!(
                metadata_cursor::metadata_replication_cursor(
                    &local_dfs_metadata_shard_path(
                        &data_dir,
                        &DfsMetadataShardId::new(shard.shard_id.clone())
                    ),
                    target,
                    &DfsMetadataShardId::new(shard.shard_id.clone()),
                )
                .unwrap(),
                actual.journal_last_sequence
            );
        }
        drop(resident);
        for (_, capture) in &captures {
            capture.close();
        }
        runtime.close().unwrap();
        for (_, capture) in captures {
            let mut bytes = Vec::new();
            capture.export_raw(&mut bytes).unwrap();
            let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            for field in ["dropped", "incomplete", "in_flight"] {
                assert_eq!(raw[field], "0", "all actual ACK observations are complete");
            }
            assert_eq!(raw["actual_owner_stopped"], true);
        }
    }

    #[test]
    fn native_replica_membership_handoff_rebinds_status_and_preserves_bytes() {
        let nodes =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-resident-membership", 3);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(
            "native-resident-membership",
            nodes[0].clone(),
        )
        .unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let original = runtime.root_shard().unwrap();
        let mut writer = runtime.client(&original.primary_node_id).unwrap();
        let stat = writer
            .posix_write_file(
                "/native-membership.bin",
                b"preserve through native handoff",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![original.primary_node_id.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        drop(writer);
        let successor = nodes
            .iter()
            .find(|node| **node != original.primary_node_id)
            .unwrap()
            .clone();
        // Shrinking the three-member ring and transferring its primary are
        // distinct native activations. Verify each exact one-epoch boundary.
        assert_eq!(original.replica_node_ids.len(), 2);
        let reduced = runtime
            .reassign_metadata_shard(
                &original.file_system_id,
                &original.shard_id,
                original.primary_node_id.clone(),
                vec![successor.clone()],
            )
            .unwrap();
        assert_eq!(reduced.epoch, original.epoch + 1);
        assert_eq!(reduced.primary_node_id, original.primary_node_id);
        assert_eq!(reduced.replica_node_ids, vec![successor.clone()]);
        let control = runtime.shared_control_plane().unwrap();
        let reduction_proof = control
            .with_client(|kv| {
                crate::DfsControlPlaneStore::new(kv.as_mut())
                    .get_metadata_shard_membership_handoff_proof(
                        &original.file_system_id,
                        &original.shard_id,
                    )
            })
            .unwrap()
            .unwrap();
        assert_eq!(reduction_proof.retiring_shard, original);
        assert_eq!(reduction_proof.desired_shard, reduced);
        let desired = runtime
            .reassign_metadata_shard(
                &original.file_system_id,
                &original.shard_id,
                successor.clone(),
                vec![original.primary_node_id.clone()],
            )
            .unwrap();
        assert_eq!(desired.epoch, reduced.epoch + 1);
        let role_proof = control
            .with_client(|kv| {
                crate::DfsControlPlaneStore::new(kv.as_mut())
                    .get_metadata_shard_membership_handoff_proof(
                        &original.file_system_id,
                        &original.shard_id,
                    )
            })
            .unwrap()
            .unwrap();
        assert_eq!(role_proof.retiring_shard, reduced);
        assert_eq!(role_proof.desired_shard, desired);
        assert_ne!(role_proof.handoff_id, reduction_proof.handoff_id);
        assert_eq!(desired.primary_node_id, successor);
        assert_eq!(
            desired.replica_node_ids,
            vec![original.primary_node_id.clone()]
        );
        let mut reader = runtime.client(&successor).unwrap();
        assert_eq!(
            reader
                .posix_read_file_distributed("/native-membership.bin")
                .unwrap(),
            b"preserve through native handoff"
        );
        assert_eq!(
            reader
                .posix_stat_distributed("/native-membership.bin")
                .unwrap()
                .object_id,
            stat.object_id
        );
        drop(reader);
        runtime.stop_node(&successor).unwrap();
        runtime.reopen_node(successor.clone()).unwrap();
        let mut reader = runtime.client(&successor).unwrap();
        assert_eq!(
            reader
                .posix_read_file_distributed("/native-membership.bin")
                .unwrap(),
            b"preserve through native handoff"
        );
        drop(reader);
        runtime.close().unwrap();
    }

    #[test]
    fn generic_metadata_publish_does_not_import_materialized_membership() {
        let root = sinomo_testdir::TestDir::new("dfs-metadata-publish-latest-membership");
        let data_dir = SinomoDataDir::new(root.path());
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_metadata_publish_latest_membership",
        )
        .expect("cluster id");
        let local_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_publish_local").expect("local node");
        let joined_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_publish_joined").expect("joined node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "metadata publish latest membership",
            local_node_id.clone(),
            "local",
            "pk-local",
            now,
        );
        let capabilities = sinomo_core::node::main_daemon_capabilities(&state.members[0].role);
        state.members[0].capabilities = capabilities.clone();
        state.members[0].allowed_capabilities = capabilities;
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("initial cluster KV snapshot");

        let stale_bootstrap = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
            .expect("bootstrap-only state");
        assert_eq!(
            cluster_dfs_peer_node_ids(&stale_bootstrap),
            vec![local_node_id.clone()],
            "the regression requires bootstrap membership to trail KV membership"
        );

        let mut joined_member = sinomo_core::ClusterMemberRecord::pending_learner(
            cluster_id,
            joined_node_id.clone(),
            "joined",
            "pk-joined",
            now,
        );
        joined_member.state = sinomo_core::ClusterMemberState::Learner;
        let mut accepted = sinomo_core::kv::InProcessKvStore::restore(
            sinomo_core::kv::read_cluster_kv_cached_snapshot(&cluster_paths.kv_paths())
                .expect("cached cluster KV snapshot"),
        )
        .expect("accepted KV store");
        accepted
            .put_serde(
                sinomo_core::node::cluster_member_key(&joined_node_id),
                &joined_member,
                None,
            )
            .expect("joined member");
        let accepted = accepted.try_snapshot().expect("accepted KV snapshot");
        sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
            &cluster_paths.kv_paths(),
            &accepted,
            |_| false,
        )
        .expect("import accepted KV snapshot");

        let publish_state = read_cluster_dfs_metadata_publish_state(&cluster_paths)
            .expect("metadata publish state");
        assert_eq!(
            cluster_dfs_peer_node_ids(&publish_state)
                .into_iter()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([local_node_id]),
            "generic publication must not reconstruct or import the materialized KV snapshot"
        );
        assert_ne!(publish_state.local_node_id, joined_node_id);
    }

    #[test]
    fn generic_metadata_publish_fails_closed_for_non_voter() {
        let root = sinomo_testdir::TestDir::new("dfs-metadata-publish-non-voter");
        let data_dir = SinomoDataDir::new(root.path());
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let local_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_publish_non_voter").expect("local node");
        let mut state = ClusterBootstrapState::initialized_locally(
            sinomo_core::node::ClusterId::from_legacy_named(
                "cluster_dfs_metadata_publish_non_voter",
            )
            .expect("cluster id"),
            "metadata publish non-voter",
            local_node_id,
            "local",
            "pk-local",
            OffsetDateTime::now_utc(),
        );
        state.members[0].role = sinomo_core::ClusterMemberRole::Learner;
        state.members[0].state = sinomo_core::ClusterMemberState::Learner;
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");

        let error = read_cluster_dfs_metadata_publish_state(&cluster_paths)
            .expect_err("non-voter generic publication must fail closed");
        assert!(error.to_string().contains("explicit retained authority"));
    }

    #[test]
    fn generic_metadata_replica_ingress_rejects_retired_primary_from_stale_local_raft() {
        let root = sinomo_testdir::TestDir::new("dfs-metadata-ingress-latest-authority");
        let data_dir = SinomoDataDir::new(root.path());
        data_dir.ensure_root_layout().expect("data root layout");
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_metadata_ingress_latest_authority",
        )
        .expect("cluster id");
        let receiver_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_ingress_receiver").expect("receiver");
        let retired_primary_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_ingress_retired_primary")
                .expect("retired primary");
        let current_primary_node_id =
            NodeId::from_legacy_named("node_dfs_metadata_ingress_current_primary")
                .expect("current primary");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "metadata ingress latest authority",
            receiver_node_id.clone(),
            "receiver",
            "pk-receiver",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&cluster_paths, &state).expect("cluster state");
        sinomo_core::cluster::sync_cluster_state_to_local_kv_unchecked(&cluster_paths, &state)
            .expect("initial cluster KV snapshot");

        let file_system_id = sifs_file_system_id();
        let shard_key = metadata_shard_key(&file_system_id, "root");
        let retired_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: retired_primary_node_id.clone(),
            replica_node_ids: vec![receiver_node_id.clone()],
            epoch: 8,
            assignment: crate::DfsMetadataShardAssignment::Root,

            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let retired_snapshot = sinomo_core::cluster::append_cluster_raft_command(
            &cluster_paths,
            &receiver_node_id,
            sinomo_core::kv::KvCommand::Op(
                sinomo_core::kv::KvOp::put_serde(&shard_key, &retired_shard, None)
                    .expect("retired shard operation"),
            ),
        )
        .expect("persist retired local Raft authority");
        let current_shard = DfsMetadataShardConfig {
            file_system_id: file_system_id.clone(),
            shard_id: "root".to_string(),
            primary_node_id: current_primary_node_id,
            replica_node_ids: vec![receiver_node_id.clone()],
            epoch: 9,
            assignment: crate::DfsMetadataShardAssignment::Root,

            default_policy: sinomo_core::DfsObjectPolicy::ReplicatedEventual,
        };
        let mut accepted = sinomo_core::kv::InProcessKvStore::restore(retired_snapshot)
            .expect("accepted KV store");
        accepted
            .put_serde(&shard_key, &current_shard, None)
            .expect("current shard authority");
        let accepted = accepted.try_snapshot().expect("accepted KV snapshot");
        sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
            &cluster_paths.kv_paths(),
            &accepted,
            |_| false,
        )
        .expect("import current KV snapshot");

        let local_shard =
            sinomo_core::cluster::read_cluster_kv_local_snapshot(&cluster_paths, &state);
        let local_shard = local_shard
            .expect("local Raft snapshot")
            .value
            .get(&shard_key)
            .expect("retired local shard")
            .decode::<DfsMetadataShardConfig>()
            .expect("decode retired local shard");
        assert_eq!(
            local_shard, retired_shard,
            "the regression requires a coherent but retired local Raft assignment"
        );

        let response = process_cluster_dfs_metadata_journal_batch(
            &data_dir,
            &cluster_paths,
            &retired_primary_node_id,
            DfsMetadataJournalBatchCommand {
                request_id: "retired-primary-empty-batch".to_string(),
                source_node_id: Some(retired_primary_node_id.clone()),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(retired_shard.epoch),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch: crate::DfsMetadataJournalFrameBatch {
                    shard_id: DfsMetadataShardId::new("root"),
                    journal_frames: Vec::new(),
                    next_after_sequence: 0,
                },
            },
        )
        .expect("replica ingress response");
        assert!(matches!(
            response.result,
            DfsMetadataJournalBatchResult::Rejected { .. }
        ));
        assert!(
            !local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("root")).exists(),
            "a retired-primary request must not create replica metadata state"
        );
        assert!(
            !local_dfs_metadata_shard_path(&data_dir, &DfsMetadataShardId::new("root")).exists(),
            "an unproven generic replica request must not create local metadata state"
        );
    }
}

async fn prepare_cluster_dfs_metadata_journal_publish_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    only_lane: Option<(DfsFileSystemId, DfsMetadataShardId)>,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<Option<DfsPreparedClusterMetadataJournalPublish>, DfsError> {
    let trace_started = Instant::now();
    let data_dir = data_dir.clone();
    let cluster_paths = cluster_paths.clone();
    let execution_exclusions = execution_exclusions.clone();
    let prepared = tokio::task::spawn_blocking(move || {
        prepare_cluster_dfs_metadata_journal_publish(
            data_dir,
            cluster_paths,
            &execution_exclusions,
            only_lane,
            options,
        )
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

fn prepare_dfs_metadata_journal_publish_for_primary_shard(
    backend: &mut dyn DfsMetadataBackend,
    local_node_id: &NodeId,
    shard: DfsMetadataShardConfig,
    metadata_suppressions: &[DfsMetadataReplicationSuppressionRecord],
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
    mut replication_cursor: impl FnMut(
        &mut dyn DfsMetadataBackend,
        &NodeId,
        &DfsMetadataShardId,
    ) -> Result<u64, DfsError>,
) -> Result<
    (
        Vec<DfsMetadataJournalReplicationOutcome>,
        Vec<DfsPreparedMetadataJournalTargetPublish>,
    ),
    DfsError,
> {
    let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
    let mut outcomes = Vec::new();
    let mut targets = Vec::new();
    let mut payloads_by_after_sequence = BTreeMap::new();
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
        let after_sequence = replication_cursor(backend, &target_node_id, &shard_id)?;
        if execution_exclusions.contains(&target_node_id) {
            outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
                target_node_id,
                shard_id: shard_id.clone(),
                after_sequence,
            });
            continue;
        }
        match prepare_dfs_metadata_journal_target_publish(
            backend,
            local_node_id,
            target_node_id,
            shard.file_system_id.clone(),
            shard_id.clone(),
            shard.epoch,
            after_sequence,
            options,
            &mut payloads_by_after_sequence,
        )? {
            DfsPreparedMetadataJournalTargetPublishResult::NoChanges { outcome } => {
                outcomes.push(outcome);
            }
            DfsPreparedMetadataJournalTargetPublishResult::Publish { target } => {
                targets.push(target);
            }
        }
    }
    Ok((outcomes, targets))
}

pub(crate) fn prepare_dfs_metadata_journal_publish_with_host(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    shards: Vec<DfsMetadataShardConfig>,
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
    prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions(
        file_system_id,
        local_node_id,
        shards,
        host,
        metadata_suppressions,
        &DfsMaintenanceExecutionExclusions::default(),
        options,
    )
}

pub(crate) fn prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    mut shards: Vec<DfsMetadataShardConfig>,
    host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_suppressions: &[DfsMetadataReplicationSuppressionRecord],
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
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
        let (shard_outcomes, shard_targets) =
            prepare_dfs_metadata_journal_publish_for_primary_shard(
                &mut *backend,
                local_node_id,
                shard,
                metadata_suppressions,
                execution_exclusions,
                options,
                |backend, target_node_id, shard_id| {
                    host.metadata_replication_cursor(backend, target_node_id, shard_id)
                },
            )?;
        outcomes.extend(shard_outcomes);
        targets.extend(shard_targets);
    }

    Ok((outcomes, targets))
}

fn prepare_dfs_metadata_journal_publish_with_local_backends_and_execution_exclusions(
    data_dir: &SinomoDataDir,
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    mut shards: Vec<DfsMetadataShardConfig>,
    metadata_suppressions: &[DfsMetadataReplicationSuppressionRecord],
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
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

        let root = local_dfs_metadata_shard_path(data_dir, &shard_id);
        let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root)?;
        backend.refresh_metadata_shard_durable_view(&shard_id)?;
        let (shard_outcomes, shard_targets) =
            prepare_dfs_metadata_journal_publish_for_primary_shard(
                &mut backend,
                local_node_id,
                shard,
                metadata_suppressions,
                execution_exclusions,
                options,
                |backend, target_node_id, shard_id| {
                    Ok(backend.metadata_replication_cursor(target_node_id, shard_id)?)
                },
            )?;
        outcomes.extend(shard_outcomes);
        targets.extend(shard_targets);
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

#[derive(Clone, Debug)]
enum DfsPreparedMetadataJournalPayload {
    NoChanges,
    CaughtUp {
        state_fingerprint: String,
    },
    CursorDurabilityInconsistency {
        durable_head_sequence: u64,
        state_fingerprint: String,
    },
    CursorDurabilityInconsistencyUnhealed {
        durable_head_sequence: u64,
        reason: String,
    },
    Batches {
        batches: Vec<DfsPreparedMetadataJournalFrameBatch>,
        backlog_after_sequence: Option<u64>,
    },
}

pub(crate) type DfsPreparedMetadataJournalFrameBatch = crate::DfsMetadataJournalFrameBatch;

pub(crate) fn prepare_dfs_metadata_journal_frame_batch(
    backend: &dyn DfsMetadataBackend,
    batch: &DfsMetadataJournalBatch,
) -> Result<DfsPreparedMetadataJournalFrameBatch, DfsError> {
    let journal_frames = backend.journal_frames_for_batch(batch)?;
    if journal_frames.len() != batch.entries.len() || journal_frames.is_empty() {
        return Err(DfsError::Metadata {
            message: format!(
                "metadata replication frame selection for shard {} returned {} frames for {} entries",
                batch.shard_id.0,
                journal_frames.len(),
                batch.entries.len()
            ),
        });
    }
    Ok(DfsPreparedMetadataJournalFrameBatch {
        shard_id: batch.shard_id.clone(),
        next_after_sequence: batch.next_after_sequence,
        journal_frames,
    })
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
    payloads_by_after_sequence: &mut BTreeMap<u64, DfsPreparedMetadataJournalPayload>,
) -> Result<DfsPreparedMetadataJournalTargetPublishResult, DfsError> {
    let payload = match payloads_by_after_sequence.get(&after_sequence) {
        Some(payload) => payload.clone(),
        None => {
            let mut next_after_sequence = after_sequence;
            let mut batches = Vec::new();
            let mut backlog_after_sequence = None;
            let payload = loop {
                let Some(batch) = backend.journal_batch_for_shard_since_limited(
                    &shard_id,
                    next_after_sequence,
                    options.batch_entry_limit,
                    options.batch_byte_limit,
                )?
                else {
                    if batches.is_empty() {
                        let durable_head_sequence = backend.journal_last_sequence()?;
                        if after_sequence < durable_head_sequence {
                            let state_fingerprint =
                                crate::service_sync::metadata_shard_state_fingerprint_at(
                                    backend,
                                    &shard_id,
                                    durable_head_sequence,
                                )?;
                            break DfsPreparedMetadataJournalPayload::CursorDurabilityInconsistency {
                                durable_head_sequence,
                                state_fingerprint,
                            };
                        }
                        if after_sequence > durable_head_sequence {
                            break DfsPreparedMetadataJournalPayload::CursorDurabilityInconsistencyUnhealed {
                                durable_head_sequence,
                                reason: "the outbound cursor is ahead of the local durable metadata head; refusing to lower it without stronger recovery evidence".to_string(),
                            };
                        }
                        if !options.probe_caught_up_replicas {
                            break DfsPreparedMetadataJournalPayload::NoChanges;
                        }
                        let state_fingerprint =
                            crate::service_sync::metadata_shard_state_fingerprint_at(
                                backend,
                                &shard_id,
                                after_sequence,
                            )?;
                        break DfsPreparedMetadataJournalPayload::CaughtUp { state_fingerprint };
                    }
                    break DfsPreparedMetadataJournalPayload::Batches {
                        batches,
                        backlog_after_sequence,
                    };
                };

                next_after_sequence = batch.next_after_sequence;
                batches.push(prepare_dfs_metadata_journal_frame_batch(backend, &batch)?);
                if batches.len() >= options.max_batches_per_target {
                    if backend
                        .journal_batch_for_shard_since_limited(
                            &shard_id,
                            next_after_sequence,
                            options.batch_entry_limit,
                            options.batch_byte_limit,
                        )?
                        .is_some()
                    {
                        backlog_after_sequence = Some(next_after_sequence);
                    }
                    break DfsPreparedMetadataJournalPayload::Batches {
                        batches,
                        backlog_after_sequence,
                    };
                }
            };
            payloads_by_after_sequence.insert(after_sequence, payload.clone());
            payload
        }
    };

    match payload {
        DfsPreparedMetadataJournalPayload::NoChanges => {
            Ok(DfsPreparedMetadataJournalTargetPublishResult::NoChanges {
                outcome: DfsMetadataJournalReplicationOutcome::NoChanges {
                    target_node_id,
                    shard_id,
                    after_sequence,
                },
            })
        }
        DfsPreparedMetadataJournalPayload::CaughtUp { state_fingerprint } => {
            let command = dfs_metadata_caught_up_probe_command(
                local_node_id,
                &target_node_id,
                &file_system_id,
                &shard_id,
                shard_epoch,
                after_sequence,
                state_fingerprint,
            );
            Ok(DfsPreparedMetadataJournalTargetPublishResult::Publish {
                target: DfsPreparedMetadataJournalTargetPublish {
                    target_node_id,
                    file_system_id,
                    shard_id,
                    shard_epoch,
                    after_sequence,
                    prepared_source_head: after_sequence,
                    commands: vec![command],
                    backlog_after_sequence: None,
                    caught_up_probe: true,
                    cursor_durability_inconsistency: None,
                },
            })
        }
        DfsPreparedMetadataJournalPayload::CursorDurabilityInconsistency {
            durable_head_sequence,
            state_fingerprint,
        } => {
            let command = dfs_metadata_caught_up_probe_command(
                local_node_id,
                &target_node_id,
                &file_system_id,
                &shard_id,
                shard_epoch,
                durable_head_sequence,
                state_fingerprint,
            );
            Ok(DfsPreparedMetadataJournalTargetPublishResult::Publish {
                target: DfsPreparedMetadataJournalTargetPublish {
                    target_node_id,
                    file_system_id,
                    shard_id,
                    shard_epoch,
                    after_sequence,
                    prepared_source_head: durable_head_sequence,
                    commands: vec![command],
                    backlog_after_sequence: None,
                    caught_up_probe: true,
                    cursor_durability_inconsistency: Some(
                        DfsMetadataCursorDurabilityInconsistency {
                            cursor_after_sequence: after_sequence,
                            durable_head_sequence,
                        },
                    ),
                },
            })
        }
        DfsPreparedMetadataJournalPayload::CursorDurabilityInconsistencyUnhealed {
            durable_head_sequence,
            reason,
        } => Ok(DfsPreparedMetadataJournalTargetPublishResult::NoChanges {
            outcome: DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyUnhealed {
                target_node_id,
                shard_id,
                cursor_after_sequence: after_sequence,
                durable_head_sequence,
                reason,
            },
        }),
        DfsPreparedMetadataJournalPayload::Batches {
            batches,
            backlog_after_sequence,
        } => {
            let commands: Vec<_> = batches
                .into_iter()
                .map(|batch| {
                    dfs_metadata_durable_journal_batch_command(
                        local_node_id,
                        &target_node_id,
                        &file_system_id,
                        shard_epoch,
                        batch,
                        options.background_publication,
                    )
                })
                .collect();
            Ok(DfsPreparedMetadataJournalTargetPublishResult::Publish {
                target: DfsPreparedMetadataJournalTargetPublish {
                    target_node_id,
                    file_system_id,
                    shard_id,
                    shard_epoch,
                    after_sequence,
                    prepared_source_head: backend.journal_last_sequence()?.max(
                        commands
                            .last()
                            .map_or(0, |command| command.batch.next_after_sequence),
                    ),
                    commands,
                    backlog_after_sequence,
                    caught_up_probe: false,
                    cursor_durability_inconsistency: None,
                },
            })
        }
    }
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
            // Older versions persisted transient receiver-startup failures as
            // epoch-long suppressions. Revalidate the reason so those records
            // cannot strand a restarted replica after upgrading.
            && record.code.unwrap_or(DfsMetadataRejectionCode::Other).suppresses_replication()
    })
}

fn record_dfs_metadata_replication_topology_mismatch(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    code: DfsMetadataRejectionCode,
    reason: &str,
) -> Result<(), DfsError> {
    if !code.suppresses_replication() {
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
            code: Some(code),
            updated_at_unix_nanos: dfs_now_unix_nanos(),
        },
    )
}

async fn record_dfs_metadata_replication_topology_mismatch_async(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    shard_id: &DfsMetadataShardId,
    shard_epoch: u64,
    code: DfsMetadataRejectionCode,
    reason: &str,
) -> Result<(), DfsError> {
    if !code.suppresses_replication() {
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
            code,
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
    if cluster_nodes_share_healthy_component(graph, local_node_id, target_node_id) {
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
    fn explicitly_unreachable_rejects_stale_degraded_relay_path() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_metadata_offline_degraded_relay")
                .expect("cluster id");
        let local_node_id = test_node("node_dfs_metadata_offline_degraded_relay_local");
        let relay_node_id = test_node("node_dfs_metadata_offline_degraded_relay_bridge");
        let target_node_id = test_node("node_dfs_metadata_offline_degraded_relay_target");
        let direct_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_degraded_relay_direct",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.25.3:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_degraded_relay_bridge",
            &relay_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.25.2:21353",
            ClusterEndpointSource::KvMembership,
        );
        let relay_target_endpoint = test_endpoint(
            "endpoint_dfs_metadata_offline_degraded_relay_bridge_target",
            &target_node_id,
            ClusterTransportKind::LanTcp,
            "10.90.25.3:21353",
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
                    ClusterConnectivityState::Degraded,
                ),
            ],
        );

        assert!(dfs_metadata_target_is_explicitly_unreachable(
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
            prepared_source_head: 19,
            commands: Vec::new(),
            backlog_after_sequence: None,
            caught_up_probe: false,
            cursor_durability_inconsistency: None,
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
            resident_cursor_status_observer: None,
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
        resident_cursor_status_observer: _,
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
                                data_dir,
                                local_node_id,
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
        resident_cursor_status_observer,
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
            let resident_cursor_status_observer = resident_cursor_status_observer.as_ref();
            in_flight.push(Box::pin(async move {
                let mut lane_results = Vec::with_capacity(target_lane.len());
                for (target_index, target) in target_lane {
                    let mut target_outcomes = Vec::new();
                    let result = execute_prepared_dfs_metadata_journal_target_publish_async(
                        data_dir,
                        local_node_id,
                        target,
                        resident_cursor_status_observer,
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
    validate_prepared_caught_up_probe(&target)?;
    let DfsPreparedMetadataJournalTargetPublish {
        target_node_id,
        file_system_id,
        shard_id,
        shard_epoch,
        mut after_sequence,
        prepared_source_head,
        commands,
        backlog_after_sequence,
        caught_up_probe,
        cursor_durability_inconsistency,
    } = target;
    if let Some(inconsistency) = cursor_durability_inconsistency {
        outcomes.push(
            DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyDetected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                cursor_after_sequence: inconsistency.cursor_after_sequence,
                durable_head_sequence: inconsistency.durable_head_sequence,
            },
        );
    }
    crate::catchup_diagnostics::replication_started(local_node_id, &target_node_id, &shard_id);
    if !transport.has_metadata_replication_route(&target_node_id) {
        crate::catchup_diagnostics::replication_unavailable(
            local_node_id,
            &target_node_id,
            &shard_id,
        );
        outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id,
            shard_id,
            after_sequence,
        });
        return Ok(());
    }

    let target_started_at = Instant::now();
    let mut completed_plan = true;
    let had_commands = !commands.is_empty();
    for command in commands {
        if !command.verify_prefix_only
            && crate::metadata_replica_observer::outbound_cursor(
                &local_dfs_metadata_shard_path(data_dir, &shard_id),
                &target_node_id,
            )
            .is_some_and(|confirmed| confirmed >= command.batch.next_after_sequence)
        {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
            completed_plan = false;
            break;
        }
        let entries = dfs_metadata_journal_batch_command_entry_count(&command);
        if entries >= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
            || (after_sequence == 0 && entries > 0)
        {
            crate::catchup_diagnostics::record(
                local_node_id,
                &target_node_id,
                &shard_id,
                "replica-publication",
                "journal-backfill",
                Some(after_sequence),
                Some(command.batch.next_after_sequence),
                "replica requires historical journal backfill",
            );
        }
        let Some(attempt_timeout) =
            dfs_metadata_replication_attempt_timeout(options, target_started_at)
        else {
            if caught_up_probe {
                outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    reason: "caught-up metadata probe exhausted its target budget".to_string(),
                });
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence,
                    max_batches: options.max_batches_per_target,
                });
            }
            completed_plan = false;
            break;
        };
        let response =
            match transport.send_metadata_journal_batch(&target_node_id, &command, attempt_timeout)
            {
                Ok(response) => response,
                Err(error) => {
                    crate::catchup_diagnostics::replication_unavailable(
                        local_node_id,
                        &target_node_id,
                        &shard_id,
                    );
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
        let verified_caught_up_probe = !caught_up_probe || response.verified_prefix_only;
        match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                if !verified_caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        reason: "metadata replica did not verify the exact caught-up prefix"
                            .to_string(),
                    });
                    completed_plan = false;
                    break;
                }
                #[cfg(any(test, feature = "test-support"))]
                if let Some(context) =
                    crate::native_shard_trace::direct_journal_attempt_context(data_dir, &command)
                {
                    context.accepted_direct_journal(&target_node_id, &command, &response);
                }
                let next_after_sequence = command.batch.next_after_sequence;
                put_local_dfs_metadata_replication_cursor(
                    data_dir,
                    &target_node_id,
                    &shard_id,
                    next_after_sequence,
                )?;
                after_sequence = next_after_sequence;
                if let Some(inconsistency) = cursor_durability_inconsistency {
                    outcomes.push(
                        DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyHealed {
                            target_node_id: target_node_id.clone(),
                            shard_id: shard_id.clone(),
                            previous_cursor_after_sequence: inconsistency.cursor_after_sequence,
                            next_after_sequence,
                        },
                    );
                } else if caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::NoChanges {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence: next_after_sequence,
                    });
                } else {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Applied {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        entries: dfs_metadata_journal_batch_command_entry_count(&command),
                        next_after_sequence,
                    });
                }
            }
            DfsMetadataJournalBatchResult::Rejected { reason, code } => {
                // Another foreground commit can advance the receiver beyond
                // this frozen plan. Rejecting old work is not evidence of a
                // damaged replica. Re-plan without inventing an ACK or moving
                // a cursor; unchanged mismatches retain snapshot recovery.
                if code == DfsMetadataRejectionCode::PrefixMismatch
                    && metadata_replication_plan_advanced(
                        data_dir,
                        &shard_id,
                        prepared_source_head,
                    )?
                {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence,
                        max_batches: options.max_batches_per_target,
                    });
                    completed_plan = false;
                    break;
                }
                if code.allows_snapshot_repair() {
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
                        cursor_durability_inconsistency,
                        outcomes,
                    )?;
                } else {
                    record_dfs_metadata_replication_topology_mismatch(
                        data_dir,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        code,
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

    if completed_plan && had_commands && backlog_after_sequence.is_none() {
        crate::catchup_diagnostics::replication_converged(
            local_node_id,
            &target_node_id,
            &shard_id,
        );
    }
    if completed_plan && let Some(after_sequence) = backlog_after_sequence {
        outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
            target_node_id,
            shard_id,
            after_sequence,
            max_batches: options.max_batches_per_target,
        });
    }
    Ok(())
}

/// This read can only retire work, never grant an acknowledgement or lower a
/// floor. Reuse journal admission under the writer lock: the optional on-disk
/// head hint can lag an owned append and must not select a repair decision.
fn metadata_replication_plan_advanced(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
    prepared_source_head: u64,
) -> Result<bool, DfsError> {
    let root = local_dfs_metadata_shard_path(data_dir, shard_id);
    let _lock = metadata_lock::metadata_write_lock(
        root.clone(),
        metadata_cursor::journal_write_lock_path(&root),
    )?;
    let current =
        crate::metadata_journal::admission::certify_for_write(&root, shard_id)?.saturating_sub(1);
    if current > prepared_source_head {
        crate::dfs_perf_trace(format_args!(
            "metadata-replication-plan-superseded shard={} prepared_head={} current_head={}",
            shard_id.0, prepared_source_head, current
        ));
        return Ok(true);
    }
    Ok(false)
}

async fn metadata_replication_plan_advanced_async(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
    prepared_source_head: u64,
) -> Result<bool, DfsError> {
    let data_dir = data_dir.clone();
    let shard_id = shard_id.clone();
    tokio::task::spawn_blocking(move || {
        metadata_replication_plan_advanced(&data_dir, &shard_id, prepared_source_head)
    })
    .await
    .map_err(|error| DfsError::Metadata {
        message: format!("metadata replication freshness task failed: {error}"),
    })?
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
    cursor_durability_inconsistency: Option<DfsMetadataCursorDurabilityInconsistency>,
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
            if let Some(inconsistency) = cursor_durability_inconsistency {
                outcomes.push(
                    DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencySnapshotInstalled {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        previous_cursor_after_sequence: inconsistency.cursor_after_sequence,
                        next_after_sequence: command.next_after_sequence,
                        archived_existing,
                    },
                );
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence: command.next_after_sequence,
                    archived_existing,
                });
            }
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
            code,
        } => {
            let reason = format!("{reason}; snapshot repair rejected: {snapshot_reason}");
            record_dfs_metadata_replication_topology_mismatch(
                data_dir,
                target_node_id,
                shard_id,
                shard_epoch,
                code,
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
            prepared_source_head: 1,
            commands: vec![DfsMetadataJournalBatchCommand {
                request_id: format!("publish-{target_node_id}-{}", shard_id.0),
                source_node_id: Some(local_node_id.clone()),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(1),
                verify_prefix_only: false,
                prefix_probe: None,
                authority_probe: None,
                durable_replication: None,
                batch: crate::DfsMetadataJournalFrameBatch {
                    shard_id,
                    journal_frames: Vec::new(),
                    next_after_sequence: 1,
                },
            }],
            backlog_after_sequence: None,
            caught_up_probe: false,
            cursor_durability_inconsistency: None,
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
                    applied_entries: command.batch.journal_frames.len(),
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
            resident_cursor_status_observer: None,
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
                            applied_entries: command.batch.journal_frames.len(),
                            verified_prefix_only: command.verify_prefix_only,
                            verified_authority_probe_id: None,
                            result: DfsMetadataJournalBatchResult::Applied,
                        })
                    })
                })
            };
            let transport = AsyncDfsMetadataNodeTransport::new(
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
                resident_cursor_status_observer: None,
            };

            let publish = tokio::spawn(async move {
                execute_prepared_dfs_metadata_journal_publish_async(
                    plan,
                    &transport,
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
                            applied_entries: command.batch.journal_frames.len(),
                            verified_prefix_only: command.verify_prefix_only,
                            verified_authority_probe_id: None,
                            result: DfsMetadataJournalBatchResult::Applied,
                        })
                    })
                })
            };
            let transport = AsyncDfsMetadataNodeTransport::new(
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
                resident_cursor_status_observer: None,
            };

            let publish = tokio::spawn(async move {
                execute_prepared_dfs_metadata_journal_publish_async(
                    plan,
                    &transport,
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
    resident_cursor_status_observer: Option<&ResidentMetadataReplicationCursorStatusObserver>,
    transport: &dyn AsyncDfsMetadataJournalReplicationTransport,
    options: DfsMetadataJournalReplicationOptions,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    validate_prepared_caught_up_probe(&target)?;
    let DfsPreparedMetadataJournalTargetPublish {
        target_node_id,
        file_system_id,
        shard_id,
        shard_epoch,
        mut after_sequence,
        prepared_source_head,
        commands,
        backlog_after_sequence,
        caught_up_probe,
        cursor_durability_inconsistency,
    } = target;
    if let Some(inconsistency) = cursor_durability_inconsistency {
        outcomes.push(
            DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyDetected {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                cursor_after_sequence: inconsistency.cursor_after_sequence,
                durable_head_sequence: inconsistency.durable_head_sequence,
            },
        );
    }
    crate::catchup_diagnostics::replication_started(local_node_id, &target_node_id, &shard_id);
    if !transport.has_metadata_replication_route(&target_node_id) {
        crate::catchup_diagnostics::replication_unavailable(
            local_node_id,
            &target_node_id,
            &shard_id,
        );
        outcomes.push(DfsMetadataJournalReplicationOutcome::MissingRoute {
            target_node_id,
            shard_id,
            after_sequence,
        });
        return Ok(());
    }

    let target_started_at = Instant::now();
    let mut completed_plan = true;
    let had_commands = !commands.is_empty();
    for command in commands {
        if !command.verify_prefix_only
            && crate::metadata_replica_observer::outbound_cursor(
                &local_dfs_metadata_shard_path(data_dir, &shard_id),
                &target_node_id,
            )
            .is_some_and(|confirmed| confirmed >= command.batch.next_after_sequence)
        {
            outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                target_node_id: target_node_id.clone(),
                shard_id: shard_id.clone(),
                after_sequence,
                max_batches: options.max_batches_per_target,
            });
            completed_plan = false;
            break;
        }
        let entries = dfs_metadata_journal_batch_command_entry_count(&command);
        if entries >= DEFAULT_METADATA_JOURNAL_BATCH_ENTRY_LIMIT
            || (after_sequence == 0 && entries > 0)
        {
            crate::catchup_diagnostics::record(
                local_node_id,
                &target_node_id,
                &shard_id,
                "replica-publication",
                "journal-backfill",
                Some(after_sequence),
                Some(command.batch.next_after_sequence),
                "replica requires historical journal backfill",
            );
        }
        let Some(attempt_timeout) =
            dfs_metadata_replication_attempt_timeout(options, target_started_at)
        else {
            if caught_up_probe {
                outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    reason: "caught-up metadata probe exhausted its target budget".to_string(),
                });
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    after_sequence,
                    max_batches: options.max_batches_per_target,
                });
            }
            completed_plan = false;
            break;
        };
        let response = match transport
            .send_metadata_journal_batch_async(&target_node_id, &command, attempt_timeout)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                crate::catchup_diagnostics::replication_unavailable(
                    local_node_id,
                    &target_node_id,
                    &shard_id,
                );
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
        let verified_caught_up_probe = !caught_up_probe || response.verified_prefix_only;
        match response.result {
            DfsMetadataJournalBatchResult::Applied => {
                if !verified_caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Failed {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        reason: "metadata replica did not verify the exact caught-up prefix"
                            .to_string(),
                    });
                    completed_plan = false;
                    break;
                }
                #[cfg(any(test, feature = "test-support"))]
                if let Some(context) =
                    crate::native_shard_trace::direct_journal_attempt_context(data_dir, &command)
                {
                    context.accepted_direct_journal(&target_node_id, &command, &response);
                }
                let next_after_sequence = command.batch.next_after_sequence;
                put_local_dfs_metadata_replication_cursor_async(
                    data_dir.clone(),
                    target_node_id.clone(),
                    shard_id.clone(),
                    next_after_sequence,
                    resident_cursor_status_observer.cloned(),
                )
                .await?;
                after_sequence = next_after_sequence;
                if let Some(inconsistency) = cursor_durability_inconsistency {
                    outcomes.push(
                        DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyHealed {
                            target_node_id: target_node_id.clone(),
                            shard_id: shard_id.clone(),
                            previous_cursor_after_sequence: inconsistency.cursor_after_sequence,
                            next_after_sequence,
                        },
                    );
                } else if caught_up_probe {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::NoChanges {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence: next_after_sequence,
                    });
                } else {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::Applied {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        entries: dfs_metadata_journal_batch_command_entry_count(&command),
                        next_after_sequence,
                    });
                }
            }
            DfsMetadataJournalBatchResult::Rejected { reason, code } => {
                // Another foreground commit can advance the receiver beyond
                // this frozen plan. Rejecting old work is not evidence of a
                // damaged replica. Re-plan without inventing an ACK or moving
                // a cursor; unchanged mismatches retain snapshot recovery.
                if code == DfsMetadataRejectionCode::PrefixMismatch
                    && metadata_replication_plan_advanced_async(
                        data_dir,
                        &shard_id,
                        prepared_source_head,
                    )
                    .await?
                {
                    outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        after_sequence,
                        max_batches: options.max_batches_per_target,
                    });
                    completed_plan = false;
                    break;
                }
                if code.allows_snapshot_repair() {
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
                        cursor_durability_inconsistency,
                        resident_cursor_status_observer,
                        outcomes,
                    )
                    .await?;
                } else {
                    record_dfs_metadata_replication_topology_mismatch_async(
                        data_dir,
                        &target_node_id,
                        &shard_id,
                        shard_epoch,
                        code,
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

    if completed_plan && had_commands && backlog_after_sequence.is_none() {
        crate::catchup_diagnostics::replication_converged(
            local_node_id,
            &target_node_id,
            &shard_id,
        );
    }
    if completed_plan && let Some(after_sequence) = backlog_after_sequence {
        outcomes.push(DfsMetadataJournalReplicationOutcome::BacklogRemaining {
            target_node_id,
            shard_id,
            after_sequence,
            max_batches: options.max_batches_per_target,
        });
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
    cursor_durability_inconsistency: Option<DfsMetadataCursorDurabilityInconsistency>,
    resident_cursor_status_observer: Option<&ResidentMetadataReplicationCursorStatusObserver>,
    outcomes: &mut Vec<DfsMetadataJournalReplicationOutcome>,
) -> Result<(), DfsError> {
    let mut command = build_local_dfs_metadata_snapshot_install_command_async(
        data_dir.clone(),
        local_node_id.clone(),
        target_node_id.clone(),
        file_system_id.clone(),
        shard_epoch,
        shard_id.clone(),
        reason.clone(),
    )
    .await?;
    bind_pending_primary_handoff_snapshot_for_cluster_target(
        data_dir,
        target_node_id,
        &mut command,
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
                resident_cursor_status_observer.cloned(),
            )
            .await?;
            if let Some(inconsistency) = cursor_durability_inconsistency {
                outcomes.push(
                    DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencySnapshotInstalled {
                        target_node_id: target_node_id.clone(),
                        shard_id: shard_id.clone(),
                        previous_cursor_after_sequence: inconsistency.cursor_after_sequence,
                        next_after_sequence: command.next_after_sequence,
                        archived_existing,
                    },
                );
            } else {
                outcomes.push(DfsMetadataJournalReplicationOutcome::SnapshotInstalled {
                    target_node_id: target_node_id.clone(),
                    shard_id: shard_id.clone(),
                    next_after_sequence: command.next_after_sequence,
                    archived_existing,
                });
            }
        }
        DfsMetadataSnapshotInstallResult::Rejected {
            reason: snapshot_reason,
            code,
        } => {
            let reason = format!("{reason}; snapshot repair rejected: {snapshot_reason}");
            record_dfs_metadata_replication_topology_mismatch_async(
                data_dir,
                target_node_id,
                shard_id,
                shard_epoch,
                code,
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

fn bind_pending_primary_handoff_snapshot_for_cluster_target(
    data_dir: &SinomoDataDir,
    target_node_id: &NodeId,
    command: &mut DfsMetadataSnapshotInstallCommand,
) -> Result<(), DfsError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let Some(context) = cluster_dfs_metadata_resident_shard_context_for_outbound_handoff(
        &cluster_paths,
        &command.shard_id,
    )?
    else {
        return Ok(());
    };
    bind_pending_primary_handoff_snapshot_for_target(&context, target_node_id, command);
    Ok(())
}

fn bind_pending_primary_handoff_snapshot_for_target(
    sender_context: &DfsMetadataReplicaShardContext,
    target_node_id: &NodeId,
    command: &mut DfsMetadataSnapshotInstallCommand,
) {
    if sender_context.local_node_id != command.source_node_id
        || sender_context.shard.shard_id != command.shard_id.0
    {
        return;
    }
    let Some(handoff) = sender_context.pending_membership_handoff.as_ref() else {
        return;
    };

    command.primary_handoff_id = Some(handoff.handoff_id.clone());
    let receiver_context = DfsMetadataReplicaShardContext {
        local_node_id: target_node_id.clone(),
        shard: sender_context.shard.clone(),
        pending_membership_handoff: Some(handoff.clone()),
    };
    if !matches!(
        validated_pending_primary_handoff_snapshot_id(&receiver_context, command),
        Ok(Some(_))
    ) {
        command.primary_handoff_id = None;
    }
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
    crate::catchup_diagnostics::record(
        local_node_id,
        target_node_id,
        shard_id,
        "replica-publication",
        "snapshot-repair",
        None,
        None,
        &reason,
    );
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
    // Keep the same shard write lock and monotonic atomic hint update
    // without loading unrelated table state.
    let root = local_dfs_metadata_shard_path(data_dir, shard_id);
    let _lock = metadata_lock::metadata_write_lock(
        root.clone(),
        metadata_cursor::journal_write_lock_path(&root),
    )?;
    let checkpoint = metadata_cursor::put_metadata_replication_cursor_and_observe(
        &root,
        target_node_id,
        shard_id,
        next_after_sequence,
    )?;
    drop(_lock);
    #[cfg(test)]
    run_metadata_cursor_checkpoint_test_hook(data_dir);
    // Stay inside the cancellation-independent checkpoint task. Only a current
    // registered transfer-selection dependency wakes default reconciliation.
    crate::default_reconciliation::notify_primary_transfer_checkpoint(
        data_dir,
        target_node_id,
        shard_id,
        checkpoint,
    );
    Ok(())
}

#[cfg(test)]
type MetadataCursorCheckpointHook = Box<dyn FnOnce() + Send>;
#[cfg(test)]
static METADATA_CURSOR_CHECKPOINT_HOOKS: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<std::path::PathBuf, MetadataCursorCheckpointHook>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
struct MetadataCursorCheckpointTestHookGuard(std::path::PathBuf);

#[cfg(test)]
impl Drop for MetadataCursorCheckpointTestHookGuard {
    fn drop(&mut self) {
        if let Some(hooks) = METADATA_CURSOR_CHECKPOINT_HOOKS.get() {
            hooks
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove(&self.0);
        }
    }
}

#[cfg(test)]
fn install_metadata_cursor_checkpoint_test_hook(
    data_dir: &SinomoDataDir,
    hook: impl FnOnce() + Send + 'static,
) -> MetadataCursorCheckpointTestHookGuard {
    let root = data_dir.root();
    let previous = METADATA_CURSOR_CHECKPOINT_HOOKS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(root.clone(), Box::new(hook));
    assert!(previous.is_none(), "one checkpoint hook per data root");
    MetadataCursorCheckpointTestHookGuard(root)
}

#[cfg(test)]
fn run_metadata_cursor_checkpoint_test_hook(data_dir: &SinomoDataDir) {
    let hook = METADATA_CURSOR_CHECKPOINT_HOOKS.get().and_then(|hooks| {
        hooks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&data_dir.root())
    });
    if let Some(hook) = hook {
        hook();
    }
}

async fn put_local_dfs_metadata_replication_cursor_async(
    data_dir: SinomoDataDir,
    target_node_id: NodeId,
    shard_id: DfsMetadataShardId,
    next_after_sequence: u64,
    resident_cursor_status_observer: Option<ResidentMetadataReplicationCursorStatusObserver>,
) -> Result<(), DfsError> {
    let observed_target_node_id = target_node_id.clone();
    let observed_shard_id = shard_id.clone();
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
    })??;
    if let Some(observer) = resident_cursor_status_observer {
        observer.observe(
            &observed_target_node_id,
            &observed_shard_id,
            next_after_sequence,
        );
    }
    Ok(())
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
                resident_cursor_status_observer: None,
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

pub async fn publish_dfs_metadata_journal_with_async_host_and_execution_exclusions(
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    shards: Vec<DfsMetadataShardConfig>,
    data_dir: &SinomoDataDir,
    host: &mut impl DfsMetadataJournalPublisherHost,
    transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMetadataJournalReplicationOptions,
) -> Result<DfsMetadataJournalReplicationReport, DfsError> {
    let metadata_suppressions = read_dfs_metadata_replication_suppression_snapshot(
        &default_dfs_metadata_replication_suppression_snapshot_path(data_dir),
    )?
    .map(|snapshot| snapshot.records)
    .unwrap_or_default();
    let (outcomes, targets) =
        prepare_dfs_metadata_journal_publish_with_host_and_execution_exclusions(
            file_system_id,
            local_node_id,
            shards,
            host,
            &metadata_suppressions,
            execution_exclusions,
            options,
        )?;
    execute_prepared_dfs_metadata_journal_publish_async(
        DfsPreparedClusterMetadataJournalPublish {
            data_dir: data_dir.clone(),
            local_node_id: local_node_id.clone(),
            peer_node_ids: Vec::new(),
            outcomes,
            targets,
            resident_cursor_status_observer: None,
        },
        transport,
        options,
    )
    .await
}

pub(crate) fn validate_durable_metadata_journal_frames(
    command: &DfsMetadataJournalBatchCommand,
) -> Result<(), String> {
    if command.verify_prefix_only {
        if command.prefix_probe.is_none()
            || command.authority_probe.is_some()
            || command.durable_replication.is_some()
            || !command.batch.journal_frames.is_empty()
        {
            return Err(
                "prefix verification requires one explicit prefix request and no frames".into(),
            );
        }
        return Ok(());
    }
    if command.prefix_probe.is_some()
        || (command.batch.journal_frames.is_empty()
            && (command.authority_probe.is_none() || command.durable_replication.is_some()))
    {
        return Err("current metadata ingest requires its one exact-frame carrier".into());
    }
    match (&command.durable_replication, &command.authority_probe) {
        (Some(replication), None) if !replication.confirmation_id.is_empty() => {}
        (None, Some(probe)) => {
            let prefix = &probe.prepared_prefix;
            if crate::service_replica::metadata_authority_probe_confirmation_id(probe).is_none()
                || prefix.journal_sequence != command.batch.next_after_sequence
            {
                return Err(
                    "metadata authority ingest requires a well-formed exact target prefix".into(),
                );
            }
        }
        _ => {
            return Err("metadata ingest requires exactly one current confirmation purpose".into());
        }
    }
    if command.batch.journal_frames.is_empty() {
        return Ok(());
    }
    validate_durable_metadata_journal_frame_order(
        "current metadata journal",
        &command.batch.journal_frames,
        command.batch.next_after_sequence,
    )
}

fn validate_durable_metadata_journal_frame_order(
    carrier: &str,
    frames: &[DfsMetadataJournalFrameRecord],
    next_after_sequence: u64,
) -> Result<(), String> {
    let mut previous_sequence = None;
    for frame in frames {
        if previous_sequence.is_some_and(|previous| frame.sequence <= previous) {
            return Err(format!(
                "{carrier} frames are not strictly sequence ordered"
            ));
        }
        previous_sequence = Some(frame.sequence);
    }
    if previous_sequence != Some(next_after_sequence) {
        return Err(format!(
            "{carrier} frames end at {:?}, not requested sequence {}",
            previous_sequence, next_after_sequence
        ));
    }
    Ok(())
}

#[cfg(test)]
mod durable_metadata_journal_frame_validation_tests {
    use super::*;

    #[test]
    fn durable_authority_without_prepared_prefix_is_rejected_before_ingest() {
        let primary = NodeId::from_legacy_named("node_missing_durable_prefix_primary").unwrap();
        let replica = NodeId::from_legacy_named("node_missing_durable_prefix_replica").unwrap();
        let fs = sifs_file_system_id();
        let wire = serde_json::json!({
            "request_id":"missing-durable-authority-prefix", "source_node_id":primary,
            "file_system_id":fs, "authority_epoch":3, "verify_prefix_only":false,
            "prefix_probe":null, "durable_replication":null,
            "authority_probe":{
                "probe_id":"missing-durable-authority-prefix-probe",
                "expected_shard":{
                    "file_system_id":fs, "shard_id":"root", "primary_node_id":primary,
                    "replica_node_ids":[replica], "epoch":3, "assignment":"root",
                    "default_policy":"replicated-eventual"
                },
                "operation":{
                    "kind":"prepared-operation", "operation_id":"missing-durable-authority-prefix-operation",
                    "file_system_id":fs,"shard_id":"root","owner_epoch":3,"prepare_sequence":1,
                    "payload_digest":"blake3:missing-durable-authority-prefix"
                }
            },
            "batch":{"shard_id":"root","journal_frames":[],"next_after_sequence":1}
        });
        assert!(
            serde_json::from_value::<DfsMetadataJournalBatchCommand>(wire).is_err(),
            "a missing exact prepared prefix cannot become an ingest command"
        );
    }
}

pub(crate) fn persist_durable_metadata_replication_ack(
    backend: &mut dyn DfsMetadataBackend,
    command: &DfsMetadataJournalBatchCommand,
    apply_report: &DfsMetadataReplicaApplyReport,
) -> Result<Option<String>, String> {
    let Some(replication) = command.durable_replication.as_ref() else {
        return Ok(None);
    };
    let durable_prefix = apply_report
        .durable_prefix_proof()
        .filter(|proof| {
            proof.is_well_formed() && proof.journal_sequence == command.batch.next_after_sequence
        })
        .cloned()
        .ok_or_else(|| {
            format!(
                "ordinary durable metadata replication did not establish shard {} prefix {}",
                command.batch.shard_id.0, command.batch.next_after_sequence
            )
        })?;
    backend
        .put_metadata_prefix_ack_floor(&crate::DfsMetadataPrefixAckFloor {
            shard_id: command.batch.shard_id.clone(),
            journal_sequence: command.batch.next_after_sequence,
            state_fingerprint: String::new(),
            durable_prefix_proof: durable_prefix,
        })
        .map_err(|error| {
            format!(
                "persist ordinary metadata replication prefix floor for shard {} sequence {}: {error}",
                command.batch.shard_id.0, command.batch.next_after_sequence
            )
        })?;
    Ok(Some(replication.confirmation_id.clone()))
}

pub fn process_dfs_metadata_journal_batch(
    backend: &mut dyn DfsMetadataBackend,
    context: Option<&DfsMetadataReplicaShardContext>,
    command: DfsMetadataJournalBatchCommand,
) -> DfsMetadataJournalBatchResponse {
    if let Err(reason) = validate_durable_metadata_journal_frames(&command) {
        return dfs_metadata_journal_rejection_response(&command, reason);
    }
    if let Some(response) = validate_dfs_metadata_journal_batch_source(context, &command) {
        return response;
    }
    let shard_id = command.batch.shard_id.clone();
    let next_after_sequence = command.batch.next_after_sequence;
    if command.verify_prefix_only {
        let context = context.expect("validated metadata prefix probe has a shard context");
        let source_node_id = command
            .source_node_id
            .as_ref()
            .expect("validated metadata prefix probe has a source node");
        #[cfg(any(test, feature = "test-support"))]
        let mut native_boundary = crate::native_shard_trace::current_boundary_interest();
        #[cfg(any(test, feature = "test-support"))]
        let native_input = native_boundary.as_ref().map(|_| command.clone());
        #[cfg(any(test, feature = "test-support"))]
        let native_before = native_boundary.as_ref().and_then(|_| {
            backend.native_held_snapshot_for_scope(&context.shard.file_system_id, &shard_id)
        });
        let response =
            match verify_dfs_metadata_journal_prefix(backend, context, source_node_id, &command) {
                Ok(verified_primary_handoff_id) => DfsMetadataJournalBatchResponse {
                    request_id: command.request_id,
                    shard_id,
                    next_after_sequence,
                    applied_entries: 0,
                    verified_prefix_only: true,
                    verified_authority_probe_id: verified_primary_handoff_id,
                    result: DfsMetadataJournalBatchResult::Applied,
                },
                Err(reason) => DfsMetadataJournalBatchResponse {
                    request_id: command.request_id,
                    shard_id,
                    next_after_sequence,
                    applied_entries: 0,
                    verified_prefix_only: false,
                    verified_authority_probe_id: None,
                    result: DfsMetadataJournalBatchResult::from_rejection(reason),
                },
            };
        #[cfg(any(test, feature = "test-support"))]
        if matches!(&response.result, DfsMetadataJournalBatchResult::Applied) {
            if let (Some(trace), Some(input)) = (native_boundary.as_mut(), native_input) {
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::ReplicaPrefixProbeAccepted {
                        local_node_id: context.local_node_id.clone(),
                        actual_source_config: context.shard.clone(),
                        actual_pending_handoff: context.pending_membership_handoff.clone(),
                        command: input,
                        response: response.clone(),
                    },
                    native_before,
                    backend.native_held_snapshot_for_scope(
                        &context.shard.file_system_id,
                        &response.shard_id,
                    ),
                );
            }
        }
        return response;
    }
    if command.batch.journal_frames.is_empty() {
        // Confirmation only: no ingest, repair, fabricated frame or cursor ACK.
        // The authenticated current shard context above is still mandatory.
        let context = context.expect("validated authority query has a shard context");
        let probe = command
            .authority_probe
            .as_ref()
            .expect("validated authority query purpose");
        let confirmation = crate::service_replica::verify_persisted_metadata_authority_probe_inner(
            backend,
            &context.shard,
            probe,
            false,
        );
        let confirmation = match confirmation {
            Ok(value) => value,
            Err(error) => {
                return dfs_metadata_journal_rejection_response(&command, error.to_string());
            }
        };
        let floor = crate::DfsMetadataAuthorityAckFloor {
            shard_id: shard_id.clone(),
            journal_sequence: next_after_sequence,
            durable_prefix_proof: probe.prepared_prefix.clone(),
        };
        if let Err(error) = backend.put_metadata_authority_ack_floor(&floor) {
            return dfs_metadata_journal_rejection_response(&command, error.to_string());
        }
        return DfsMetadataJournalBatchResponse {
            request_id: command.request_id,
            shard_id,
            next_after_sequence,
            applied_entries: 0,
            verified_prefix_only: false,
            verified_authority_probe_id: Some(confirmation),
            result: DfsMetadataJournalBatchResult::Applied,
        };
    }
    let context = context.expect("validated metadata replica ingest has a shard context");
    let permit = DfsMetadataReplicaIngestPermit::new_validated(
        context.shard.file_system_id.clone(),
        shard_id.clone(),
        context.shard.primary_node_id.clone(),
        context.shard.epoch,
    );
    #[cfg(any(test, feature = "test-support"))]
    let mut native_trace = crate::native_shard_trace::current_boundary_interest();
    #[cfg(any(test, feature = "test-support"))]
    let native_before = native_trace.as_ref().and_then(|_| {
        backend.native_held_snapshot_for_scope(&context.shard.file_system_id, &shard_id)
    });
    let header = command.batch.header();
    let apply_result = if let Some(probe) = command.authority_probe.as_ref() {
        backend.persist_authority_journal_frames_with_replica_permit_report(
            &permit,
            &header,
            &command.batch.journal_frames,
            &probe.prepared_prefix,
            &probe.operation,
        )
    } else {
        backend.persist_journal_frames_with_replica_permit_report(
            &permit,
            &header,
            &command.batch.journal_frames,
        )
    };
    match apply_result {
        Ok(apply_report) => {
            let applied_entries = apply_report.applied_entries();
            #[cfg(any(test, feature = "test-support"))]
            if let Some(trace) = native_trace.as_mut() {
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::ReplicaJournalPersisted {
                        local_node_id: context.local_node_id.clone(),
                        actual_source_config: context.shard.clone(),
                        actual_pending_handoff: context.pending_membership_handoff.clone(),
                        command: command.clone(),
                        applied_entries,
                        permit: crate::native_shard_trace::NativeReplicaPermitTupleV1::observed(
                            &permit,
                        ),
                        durable_prefix: apply_report.durable_prefix_proof().cloned(),
                        materialization_pending: apply_report.materialization_pending(),
                    },
                    native_before,
                    backend
                        .native_held_snapshot_for_scope(&context.shard.file_system_id, &shard_id),
                );
            }

            let verified_durable_replication_id =
                match persist_durable_metadata_replication_ack(backend, &command, &apply_report) {
                    Ok(confirmation_id) => confirmation_id,
                    Err(reason) => {
                        return dfs_metadata_journal_rejection_response(&command, reason);
                    }
                };
            let verified_authority_probe_id = if verified_durable_replication_id.is_some() {
                verified_durable_replication_id
            } else if let Some(probe) = command.authority_probe.as_ref() {
                let confirmation_id =
                    match crate::service_replica::verify_persisted_metadata_authority_probe_after_apply(
                        backend,
                        &context.shard,
                        probe,
                        &apply_report,
                    ) {
                        Ok(confirmation_id) => confirmation_id,
                        Err(error) => {
                            return dfs_metadata_journal_rejection_response(
                                &command,
                                error.to_string(),
                            );
                        }
                    };
                let Some(durable_prefix_proof) = apply_report.durable_prefix_proof().cloned()
                else {
                    return dfs_metadata_journal_rejection_response(
                        &command,
                        "authority acknowledgement has no exact durable prefix",
                    );
                };
                if let Err(error) =
                    backend.put_metadata_authority_ack_floor(&crate::DfsMetadataAuthorityAckFloor {
                        shard_id: command.batch.shard_id.clone(),
                        journal_sequence: command.batch.next_after_sequence,
                        durable_prefix_proof,
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
            let response = DfsMetadataJournalBatchResponse {
                request_id: command.request_id.clone(),
                shard_id,
                next_after_sequence,
                applied_entries,
                verified_prefix_only: false,
                // Echo the prefix-bound confirmation only after the exact
                // hidden operation and complete primary history are durable.
                verified_authority_probe_id,
                result: DfsMetadataJournalBatchResult::Applied,
            };
            #[cfg(any(test, feature = "test-support"))]
            if let Some(trace) = native_trace.as_mut() {
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::ReplicaJournalResponseConstructed {
                        response: response.clone(),
                    }, None, None,
                );
            }
            response
        }
        Err(error) => {
            #[cfg(any(test, feature = "test-support"))]
            if let Some(trace) = native_trace.as_mut() {
                trace.observe(
                    crate::native_shard_trace::NativeBoundaryFactsV1::ReplicaJournalPersistenceFailed {
                        local_node_id: context.local_node_id.clone(),
                        actual_source_config: context.shard.clone(),
                        command: command.clone(), error_detail: error.to_string(),
                    },
                    native_before,
                    backend.native_held_snapshot_for_scope(&context.shard.file_system_id, &shard_id),
                );
                trace.uncertain();
            }
            DfsMetadataJournalBatchResponse {
                request_id: command.request_id,
                shard_id,
                next_after_sequence,
                applied_entries: 0,
                verified_prefix_only: false,
                verified_authority_probe_id: None,
                result: DfsMetadataJournalBatchResult::from_rejection(error),
            }
        }
    }
}

pub(crate) fn verify_dfs_metadata_journal_prefix(
    backend: &mut dyn DfsMetadataBackend,
    context: &DfsMetadataReplicaShardContext,
    source_node_id: &NodeId,
    command: &DfsMetadataJournalBatchCommand,
) -> Result<Option<String>, DfsMetadataRejection> {
    let batch = &command.batch;
    let Some(probe) = command.prefix_probe.as_ref() else {
        return Err(
            ("metadata journal prefix probe is missing its exact checkpoint identity".to_string())
                .into(),
        );
    };
    if !batch.journal_frames.is_empty() {
        return Err("metadata prefix verification cannot carry journal frames"
            .to_string()
            .into());
    }
    if probe.barrier_id.is_empty()
        || probe.state_fingerprint.is_empty()
        || probe.shard_id != batch.shard_id
        || probe.journal_sequence != batch.next_after_sequence
    {
        return Err(DfsMetadataRejection::from(format!(
            "metadata journal prefix probe identity mismatch for shard {} sequence {}",
            batch.shard_id.0, batch.next_after_sequence
        )));
    }
    let verified_primary_handoff_id =
        validated_pending_primary_handoff_prefix_probe_id(context, source_node_id, probe)?;
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
        DfsMetadataRejection::new(
            DfsMetadataRejectionCode::PrefixMismatch,
            format!(
                "metadata journal prefix probe state mismatch for shard {} sequence {}: {error}",
                batch.shard_id.0, probe.journal_sequence
            ),
        )
    })?;
    if local_fingerprint != probe.state_fingerprint {
        return Err(DfsMetadataRejection::new(
            DfsMetadataRejectionCode::PrefixMismatch,
            format!(
                "metadata journal prefix probe state mismatch for shard {} sequence {}",
                batch.shard_id.0, probe.journal_sequence
            ),
        ));
    }
    let durable_prefix_proof = backend
        .metadata_prefix_proof(&probe.shard_id)
        .map_err(|error| {
            format!(
                "read durable metadata prefix for probe shard {} sequence {}: {error}",
                batch.shard_id.0, probe.journal_sequence
            )
        })?
        .filter(|proof| {
            proof.is_well_formed() && proof.journal_sequence == probe.journal_sequence
        })
        .ok_or_else(|| {
            format!(
                "metadata journal prefix probe has no exact durable prefix for shard {} sequence {}",
                batch.shard_id.0, probe.journal_sequence
            )
        })?;
    if let Some(primary_handoff_id) = verified_primary_handoff_id.as_deref() {
        let handoff = context
            .pending_membership_handoff
            .as_ref()
            .expect("validated primary handoff prefix probe has a pending handoff");
        let freeze = backend
            .freeze_metadata_shard_for_membership_handoff(
                &probe.shard_id,
                primary_handoff_id,
                handoff.retiring_shard.epoch,
            )
            .map_err(|error| {
                format!(
                    "persist metadata primary-handoff {primary_handoff_id} promoted-replica marker for shard {}: {error}",
                    probe.shard_id.0
                )
            })?
            .ok_or_else(|| {
                format!(
                    "metadata primary-handoff {primary_handoff_id} could not persist its promoted-replica marker for shard {}",
                    probe.shard_id.0
                )
            })?;
        if freeze.handoff_id != primary_handoff_id
            || freeze.retiring_epoch != handoff.retiring_shard.epoch
            || freeze.activated_epoch.is_some()
            || freeze.journal_sequence != probe.journal_sequence
            || freeze.state_fingerprint != probe.state_fingerprint
        {
            return Err(DfsMetadataRejection::from(format!(
                "metadata primary-handoff {primary_handoff_id} persisted a non-exact promoted-replica marker for shard {}",
                probe.shard_id.0
            )));
        }
    }
    backend
        .put_metadata_prefix_ack_floor(&crate::DfsMetadataPrefixAckFloor {
            shard_id: probe.shard_id.clone(),
            journal_sequence: probe.journal_sequence,
            state_fingerprint: probe.state_fingerprint.clone(),
            durable_prefix_proof,
        })
        .map_err(|error| {
            format!(
                "persist metadata prefix acknowledgement floor for shard {} sequence {}: {error}",
                batch.shard_id.0, probe.journal_sequence
            )
        })?;
    Ok(verified_primary_handoff_id)
}

/// Compatibility authority lookup for host-agnostic callers.
///
/// `ClusterPaths` exposes only resident local state, which cannot prove that a
/// shard assignment belongs to the current Raft term. Callers must obtain a
/// retained control-plane context or a current-term exact-set proof and use the
/// explicit-context ingress functions instead.
pub fn cluster_dfs_metadata_replica_shard_context(
    _cluster_paths: &ClusterPaths,
    _shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataReplicaShardContext>, DfsError> {
    Ok(None)
}

/// Read the resident local shard projection for an outbound handoff hint.
///
/// This projection is deliberately not an ingress authority proof: it can be
/// coherent but stale after the local node loses quorum or a primary retires.
/// Replica ingress must instead receive a retained control-plane context or a
/// current-term exact-set proof through `ClusterAccess`.
/// sinomo:kv-configuration: Bind an outbound handoff to the resident committed assignment and pending configuration proposal.
fn cluster_dfs_metadata_resident_shard_context_for_outbound_handoff(
    cluster_paths: &ClusterPaths,
    shard_id: &DfsMetadataShardId,
) -> Result<Option<DfsMetadataReplicaShardContext>, DfsError> {
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(DfsError::from_network)?;
    let file_system_id = sifs_file_system_id();
    let shard_key = metadata_shard_key(&file_system_id, &shard_id.0);
    let handoff_key = metadata_shard_membership_handoff_key(&file_system_id, &shard_id.0);
    let keys = vec![shard_key, handoff_key];
    let read = sinomo_core::kv::read_cluster_kv_resident_local_entries(
        &cluster_paths.kv_paths(),
        &state.local_node_id,
        &keys,
    )
    .map_err(DfsError::from)?;
    let mut values = read.value.into_iter();
    let shard = values
        .next()
        .expect("resident exact-key set preserves requested cardinality")
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .transpose()?;
    let pending_membership_handoff = values
        .next()
        .expect("resident exact-key set preserves requested cardinality")
        .map(|entry| entry.decode::<DfsMetadataShardMembershipHandoff>())
        .transpose()?;
    Ok(shard.map(|shard| DfsMetadataReplicaShardContext {
        local_node_id: state.local_node_id,
        pending_membership_handoff: pending_membership_handoff.filter(|handoff| {
            handoff.retiring_shard == shard
                && handoff.desired_shard.file_system_id == shard.file_system_id
                && handoff.desired_shard.shard_id == shard.shard_id
                && handoff.desired_shard.epoch == shard.epoch.saturating_add(1)
        }),
        shard,
    }))
}

pub fn process_cluster_dfs_metadata_journal_batch(
    data_dir: &SinomoDataDir,
    _cluster_paths: &ClusterPaths,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataJournalBatchCommand,
) -> Result<DfsMetadataJournalBatchResponse, DfsError> {
    // ClusterPaths alone cannot prove that its resident Raft projection is
    // current. The host-agnostic entry point therefore fails closed; service
    // ingress uses the `_with_context` path after establishing retained or
    // current-term exact-set authority.
    process_cluster_dfs_metadata_journal_batch_with_context(
        data_dir,
        authenticated_source_node_id,
        command,
        None,
    )
}

/// Test-support ingress for callers that have already established the exact
/// replica shard authority context used by the daemon data plane.
///
/// Production ingress must obtain this context from its retained control-plane
/// authority projection; this helper exists only so cross-crate tests exercise
/// the same validation and durable apply path without falling back to the
/// deliberately fail-closed host-agnostic entry point above.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn process_cluster_dfs_metadata_journal_batch_with_replica_context_for_test(
    data_dir: &SinomoDataDir,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataJournalBatchCommand,
    context: DfsMetadataReplicaShardContext,
) -> Result<DfsMetadataJournalBatchResponse, DfsError> {
    process_cluster_dfs_metadata_journal_batch_with_context(
        data_dir,
        authenticated_source_node_id,
        command,
        Some(context),
    )
}

pub(crate) fn process_cluster_dfs_metadata_journal_batch_with_context(
    data_dir: &SinomoDataDir,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataJournalBatchCommand,
    context: Option<DfsMetadataReplicaShardContext>,
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
    if let Some(response) = validate_dfs_metadata_journal_batch_source(context.as_ref(), &command) {
        return Ok(response);
    }
    data_dir
        .ensure_root_layout()
        .map_err(|source| DfsError::Metadata {
            message: source.to_string(),
        })?;
    // The daemon routes replica ingestion through its canonical resident
    // shard backend. This local route is retained for standalone callers and
    // deliberately owns the backend for only this bounded operation.
    let root = local_dfs_metadata_shard_path(data_dir, &command.batch.shard_id);
    let mut backend = match LocalSobjMetadataBackend::new(command.batch.shard_id.clone(), root) {
        Ok(backend) => backend,
        Err(error) if metadata_backend_open_error_allows_snapshot_repair(&error) => {
            return Ok(dfs_metadata_journal_rejection_response(
                &command,
                DfsMetadataRejection::new(
                    DfsMetadataRejectionCode::ReplicaUnreadable,
                    format!(
                        "{DFS_METADATA_UNREADABLE_REPLICA_SNAPSHOT_REPAIR_REASON} for shard {}: {error}",
                        command.batch.shard_id.0
                    ),
                ),
            ));
        }
        Err(error) => return Err(error.into()),
    };
    Ok(process_dfs_metadata_journal_batch(
        &mut backend,
        context.as_ref(),
        command,
    ))
}

pub(crate) fn materialize_cluster_dfs_metadata_replica_journal(
    data_dir: &SinomoDataDir,
    shard_id: &DfsMetadataShardId,
) -> Result<usize, DfsError> {
    // Standalone replica materialization reopens a bounded local view. Daemon
    // traffic uses the resident message host and never reaches this fallback.
    let root = local_dfs_metadata_shard_path(data_dir, shard_id);
    let mut backend = LocalSobjMetadataBackend::new(shard_id.clone(), root)?;
    let materialized = backend
        .materialize_pending_replica_journal()
        .map_err(DfsError::from)?;
    drop(backend);
    if materialized > 0 {
        crate::metadata_table_apply::release_metadata_allocator_pages();
    }
    Ok(materialized)
}

pub(crate) fn schedule_cluster_dfs_metadata_replica_materialization_on_runtime(
    runtime: tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    shard_id: DfsMetadataShardId,
) {
    let scheduler = cluster_dfs_metadata_materialization_scheduler(&data_dir, &shard_id);
    let report_shard_id = shard_id.clone();
    let error_shard_id = shard_id.clone();
    schedule_dfs_metadata_publish_async_on_runtime(
        scheduler,
        runtime,
        DfsMetadataPublishScheduleOptions {
            max_retry_rounds: 3,
            retry_delay: Duration::from_millis(50),
            max_retry_delay: Duration::from_secs(1),
        },
        move || {
            let data_dir = data_dir.clone();
            let shard_id = shard_id.clone();
            async move {
                tokio::task::spawn_blocking(move || {
                    materialize_cluster_dfs_metadata_replica_journal(&data_dir, &shard_id)
                })
                .await
                .map_err(|error| format!("DFS replica materialization task failed: {error}"))?
                .map(|_| DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                })
                .map_err(|error| error.to_string())
            }
        },
        move |_| {
            tracing::trace!(
                shard_id = %report_shard_id.0,
                "DFS service materialized a durably acknowledged replica journal"
            );
        },
        move |error| {
            tracing::warn!(
                shard_id = %error_shard_id.0,
                "DFS service could not materialize a durably acknowledged replica journal: {error}"
            );
        },
    );
}

pub(crate) fn metadata_snapshot_install_permit(
    context: &DfsMetadataReplicaShardContext,
    command: &DfsMetadataSnapshotInstallCommand,
) -> std::result::Result<DfsMetadataReplicaIngestPermit, String> {
    let handoff_id = validated_pending_primary_handoff_snapshot_id(context, command)?;
    Ok(if let Some(handoff_id) = handoff_id {
        DfsMetadataReplicaIngestPermit::new_validated_pending_primary_handoff(
            context.shard.file_system_id.clone(),
            command.shard_id.clone(),
            context.shard.primary_node_id.clone(),
            context.shard.epoch,
            handoff_id,
        )
    } else {
        DfsMetadataReplicaIngestPermit::new_validated(
            context.shard.file_system_id.clone(),
            command.shard_id.clone(),
            context.shard.primary_node_id.clone(),
            context.shard.epoch,
        )
    })
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
                result: DfsMetadataSnapshotInstallResult::from_rejection(reason),
            };
        }
        Err(error) => {
            return DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::from_rejection(error),
            };
        }
        Ok(None) => {}
    }
    let context = context.expect("validated metadata snapshot ingest has a shard context");
    let permit = match metadata_snapshot_install_permit(context, &command) {
        Ok(permit) => permit,
        Err(reason) => {
            return DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result: DfsMetadataSnapshotInstallResult::from_rejection(reason),
            };
        }
    };
    #[cfg(any(test, feature = "test-support"))]
    let mut native_boundary = crate::native_shard_trace::current_boundary_interest();
    #[cfg(any(test, feature = "test-support"))]
    let native_input = native_boundary.as_ref().map(|_| command.clone());
    #[cfg(any(test, feature = "test-support"))]
    let native_before = native_boundary.as_ref().and_then(|_| {
        backend.native_held_snapshot_for_scope(&context.shard.file_system_id, &shard_id)
    });
    let snapshot_image = command
        .image()
        .cloned()
        .expect("validated native snapshot resource");
    let response = match backend.replace_with_replicated_snapshot_with_replica_permit(
        &permit,
        command.shard_id,
        command.next_after_sequence,
        snapshot_image,
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
            result: DfsMetadataSnapshotInstallResult::from_rejection(error),
        },
    };
    #[cfg(any(test, feature = "test-support"))]
    if matches!(
        &response.result,
        DfsMetadataSnapshotInstallResult::Installed { .. }
    ) && let (Some(trace), Some(input)) = (native_boundary.as_mut(), native_input)
    {
        trace.observe(
            crate::native_shard_trace::NativeBoundaryFactsV1::ReplicaSnapshotAccepted {
                local_node_id: context.local_node_id.clone(),
                actual_source_config: context.shard.clone(),
                actual_pending_handoff: context.pending_membership_handoff.clone(),
                command: input,
                response: response.clone(),
                permit: crate::native_shard_trace::NativeReplicaPermitTupleV1::observed(&permit),
            },
            native_before,
            backend
                .native_held_snapshot_for_scope(&context.shard.file_system_id, &response.shard_id),
        );
    }
    response
}

pub(crate) fn validate_dfs_metadata_snapshot_install_state(
    backend: &dyn DfsMetadataBackend,
    command: &DfsMetadataSnapshotInstallCommand,
) -> Result<Option<String>, DfsError> {
    if command.snapshot.materialized_through_sequence != command.next_after_sequence {
        return Ok(Some(format!(
            "metadata snapshot embedded sequence {:?} does not match install sequence {} for shard {}",
            command.snapshot.materialized_through_sequence,
            command.next_after_sequence,
            command.shard_id.0
        )));
    }
    {
        let proof = &command.snapshot.prefix_proof;
        if !proof.is_well_formed() || proof.journal_sequence != command.next_after_sequence {
            return Ok(Some(format!(
                "metadata snapshot prefix proof does not name exact install sequence {} for shard {}",
                command.next_after_sequence, command.shard_id.0
            )));
        }
        if let Err(error) = command.image().and_then(|image| image.validate()) {
            return Ok(Some(format!(
                "native snapshot image failed verification: {error}"
            )));
        }
    }
    if let Some(authority_floor) = backend.metadata_authority_ack_floor(&command.shard_id)? {
        if command.next_after_sequence < authority_floor.journal_sequence {
            return Ok(Some(format!(
                "metadata snapshot sequence {} would replace shard {} behind locally acknowledged authority operation {}",
                command.next_after_sequence, command.shard_id.0, authority_floor.journal_sequence
            )));
        }
        if command.next_after_sequence == authority_floor.journal_sequence
            && authority_floor.durable_prefix_proof != command.snapshot.prefix_proof
        {
            return Ok(Some(format!(
                "metadata snapshot prefix conflicts with locally acknowledged authority operation for shard {} sequence {}",
                command.shard_id.0, command.next_after_sequence
            )));
        }
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
        if command.snapshot.prefix_proof != floor.durable_prefix_proof {
            return Ok(Some(format!(
                "metadata snapshot journal prefix conflicts with locally acknowledged checkpoint for shard {} sequence {}",
                command.shard_id.0, command.next_after_sequence
            )));
        }
        if !floor.state_fingerprint.is_empty() {
            let matches = command
                .snapshot
                .matches_checkpoint(command.next_after_sequence, &floor.state_fingerprint);
            if !matches {
                return Ok(Some(format!(
                    "metadata snapshot state does not match locally acknowledged checkpoint for shard {} sequence {}",
                    command.shard_id.0, command.next_after_sequence
                )));
            }
        }
    }
    Ok(None)
}

pub fn process_cluster_dfs_metadata_snapshot_install(
    data_dir: &SinomoDataDir,
    _cluster_paths: &ClusterPaths,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataSnapshotInstallCommand,
) -> Result<DfsMetadataSnapshotInstallResponse, DfsError> {
    // See the journal entry point above: resident local state is observational,
    // not a current-term authority proof.
    process_cluster_dfs_metadata_snapshot_install_with_context(
        data_dir,
        authenticated_source_node_id,
        command,
        None,
    )
}

pub(crate) fn process_cluster_dfs_metadata_snapshot_install_with_context(
    data_dir: &SinomoDataDir,
    authenticated_source_node_id: &NodeId,
    command: DfsMetadataSnapshotInstallCommand,
    context: Option<DfsMetadataReplicaShardContext>,
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
                code: DfsMetadataRejectionCode::Other,
            },
        });
    }
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
    let root = local_dfs_metadata_shard_path(data_dir, &command.shard_id);
    match LocalSobjMetadataBackend::new(command.shard_id.clone(), &root) {
        Ok(mut backend) => Ok(process_dfs_metadata_snapshot_install(
            &mut backend,
            context.as_ref(),
            command,
        )),
        Err(open_error) if metadata_backend_open_error_allows_snapshot_repair(&open_error) => {
            let request_id = command.request_id.clone();
            let shard_id = command.shard_id.clone();
            let next_after_sequence = command.next_after_sequence;
            let context = context
                .as_ref()
                .expect("validated metadata snapshot ingest has a shard context");
            let permit = metadata_snapshot_install_permit(context, &command)
                .map_err(|message| DfsError::Metadata { message })?;
            let snapshot_image = command.image().cloned().map_err(DfsError::from)?;
            let result = LocalSobjMetadataBackend::replace_unreadable_with_replicated_snapshot(
                &permit,
                shard_id.clone(),
                root,
                command.shard_id,
                command.next_after_sequence,
                snapshot_image,
            );
            let result = match result {
                Ok((_backend, archived_existing)) => {
                    DfsMetadataSnapshotInstallResult::Installed { archived_existing }
                }
                Err(repair_error) => DfsMetadataSnapshotInstallResult::Rejected {
                    reason: format!(
                        "cannot open metadata shard for snapshot install: {open_error}; unreadable-shard snapshot replacement failed: {repair_error}"
                    ),
                    code: DfsMetadataRejectionCode::Other,
                },
            };
            Ok(DfsMetadataSnapshotInstallResponse {
                request_id,
                shard_id,
                next_after_sequence,
                result,
            })
        }
        Err(open_error) => Err(open_error.into()),
    }
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
    if let Some(probe) = command.authority_probe.as_ref() {
        if command.verify_prefix_only
            || command.prefix_probe.is_some()
            || probe.probe_id.is_empty()
            || probe.expected_shard != context.shard
            || !crate::service_replica::metadata_authority_operation_is_well_formed_for_shard(
                &probe.operation,
                &context.shard,
            )
            || crate::service_replica::metadata_authority_operation_prepare_sequence(
                &probe.operation,
            ) > command.batch.next_after_sequence
            || crate::service_replica::metadata_authority_probe_confirmation_id(probe).is_none()
            || probe.prepared_prefix.journal_sequence != command.batch.next_after_sequence
        {
            return Some(dfs_metadata_journal_rejection_response(
                command,
                format!(
                    "metadata authority probe does not match the exact committed assignment for shard {} epoch {}",
                    context.shard.shard_id, context.shard.epoch
                ),
            ));
        }
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
            DfsMetadataRejection::new(
                DfsMetadataRejectionCode::PrimaryMismatch,
                format!(
                    "metadata journal source {source_node_id} is not shard {} primary {}",
                    context.shard.shard_id, context.shard.primary_node_id
                ),
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
    let reject = |reason: DfsMetadataRejection| DfsMetadataSnapshotInstallResponse {
        request_id: command.request_id.clone(),
        shard_id: command.shard_id.clone(),
        next_after_sequence: command.next_after_sequence,
        result: DfsMetadataSnapshotInstallResult::from_rejection(reason),
    };

    let Some(context) = context else {
        return Some(reject(
            ("metadata snapshot install has no local shard config".to_string()).into(),
        ));
    };
    if context.shard.shard_id != command.shard_id.0 {
        return Some(reject(
            ("metadata snapshot install has no local shard config".to_string()).into(),
        ));
    }
    if command.file_system_id.as_ref() != Some(&context.shard.file_system_id)
        || command.authority_epoch != Some(context.shard.epoch)
    {
        return Some(reject(DfsMetadataRejection::from(format!(
            "metadata snapshot authority identity does not match filesystem {} shard {} epoch {}",
            context.shard.file_system_id, context.shard.shard_id, context.shard.epoch
        ))));
    }
    if context.shard.primary_node_id != command.source_node_id {
        return Some(reject(DfsMetadataRejection::new(
            DfsMetadataRejectionCode::PrimaryMismatch,
            format!(
                "metadata snapshot source {} is not shard {} primary {}",
                command.source_node_id, context.shard.shard_id, context.shard.primary_node_id
            ),
        )));
    }
    if context.shard.primary_node_id == context.local_node_id {
        return Some(reject(
            ("metadata primary must not install a replica snapshot".to_string()).into(),
        ));
    }
    if !context
        .shard
        .replica_node_ids
        .contains(&context.local_node_id)
        && !metadata_membership_handoff_allows_local_target(context)
    {
        return Some(reject(DfsMetadataRejection::from(format!(
            "local node {} is neither a configured replica nor a pending membership-handoff catch-up target for metadata shard {}",
            context.local_node_id, context.shard.shard_id
        ))));
    }
    if let Err(reason) = validated_pending_primary_handoff_snapshot_id(context, command) {
        return Some(reject((reason).into()));
    }
    None
}

pub(crate) fn validated_pending_primary_handoff_snapshot_id(
    context: &DfsMetadataReplicaShardContext,
    command: &DfsMetadataSnapshotInstallCommand,
) -> Result<Option<String>, String> {
    let Some(primary_handoff_id) = command.primary_handoff_id.as_deref() else {
        return Ok(None);
    };
    let Some(handoff) = context.pending_membership_handoff.as_ref() else {
        return Err(format!(
            "metadata primary-handoff snapshot {primary_handoff_id} has no pending handoff"
        ));
    };
    let exact_fence = if let Some(fence) = handoff.retiring_primary_fence.as_ref() {
        fence.handoff_id == primary_handoff_id
            && fence.primary_node_id == command.source_node_id
            && fence.journal_sequence == command.next_after_sequence
            && command
                .snapshot
                .matches_checkpoint(fence.journal_sequence, &fence.state_fingerprint)
    } else {
        false
    };
    validated_pending_primary_handoff_role_rotation_id(
        context,
        primary_handoff_id,
        &command.source_node_id,
        command.file_system_id.as_ref(),
        command.authority_epoch,
        exact_fence,
        "snapshot",
    )
    .map(Some)
}

fn validated_pending_primary_handoff_prefix_probe_id(
    context: &DfsMetadataReplicaShardContext,
    source_node_id: &NodeId,
    probe: &DfsMetadataPrefixProbe,
) -> Result<Option<String>, String> {
    let Some(primary_handoff_id) = probe.primary_handoff_id.as_deref() else {
        return Ok(None);
    };
    let Some(handoff) = context.pending_membership_handoff.as_ref() else {
        return Err(format!(
            "metadata primary-handoff prefix probe {primary_handoff_id} has no pending handoff"
        ));
    };
    let exact_fence = handoff
        .retiring_primary_fence
        .as_ref()
        .is_some_and(|fence| {
            fence.handoff_id == primary_handoff_id
                && fence.primary_node_id == *source_node_id
                && fence.journal_sequence == probe.journal_sequence
                && fence.state_fingerprint == probe.state_fingerprint
        });
    validated_pending_primary_handoff_role_rotation_id(
        context,
        primary_handoff_id,
        source_node_id,
        probe.file_system_id.as_ref(),
        probe.authority_epoch,
        exact_fence,
        "prefix probe",
    )
    .map(Some)
}

fn validated_pending_primary_handoff_role_rotation_id(
    context: &DfsMetadataReplicaShardContext,
    primary_handoff_id: &str,
    source_node_id: &NodeId,
    file_system_id: Option<&DfsFileSystemId>,
    authority_epoch: Option<u64>,
    exact_fence: bool,
    operation: &str,
) -> Result<String, String> {
    let Some(handoff) = context.pending_membership_handoff.as_ref() else {
        return Err(format!(
            "metadata primary-handoff {operation} {primary_handoff_id} has no pending handoff"
        ));
    };
    let topology_preserving_replicas =
        crate::control_plane::topology_preserving_metadata_shard_replicas(
            &handoff.retiring_shard,
            &handoff.desired_shard.primary_node_id,
        );
    let exact_role_rotation = handoff.handoff_id == primary_handoff_id
        && handoff.retiring_shard == context.shard
        && handoff.retiring_shard.primary_node_id == *source_node_id
        && handoff
            .retiring_shard
            .replica_node_ids
            .contains(&context.local_node_id)
        && handoff.desired_shard.primary_node_id == context.local_node_id
        && handoff.desired_shard.epoch == handoff.retiring_shard.epoch.saturating_add(1)
        && topology_preserving_replicas == Some(handoff.desired_shard.replica_node_ids.clone())
        && file_system_id == Some(&handoff.retiring_shard.file_system_id)
        && authority_epoch == Some(handoff.retiring_shard.epoch)
        && exact_fence;
    if !exact_role_rotation {
        return Err(format!(
            "metadata primary-handoff {operation} {primary_handoff_id} is not bound to the exact fenced role rotation for shard {}",
            context.shard.shard_id
        ));
    }
    Ok(primary_handoff_id.to_string())
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
                && handoff.desired_shard.epoch == context.shard.epoch.saturating_add(1)
                && std::iter::once(&handoff.desired_shard.primary_node_id)
                    .chain(handoff.desired_shard.replica_node_ids.iter())
                    .any(|node_id| node_id == &context.local_node_id)
        })
}

fn dfs_metadata_journal_rejection_response(
    command: &DfsMetadataJournalBatchCommand,
    reason: impl Into<DfsMetadataRejection>,
) -> DfsMetadataJournalBatchResponse {
    DfsMetadataJournalBatchResponse {
        request_id: command.request_id.clone(),
        shard_id: command.batch.shard_id.clone(),
        next_after_sequence: command.batch.next_after_sequence,
        applied_entries: 0,
        verified_prefix_only: false,
        verified_authority_probe_id: None,
        result: DfsMetadataJournalBatchResult::from_rejection(reason),
    }
}

#[derive(Debug)]
enum DfsMetadataJournalReplicationStep {
    Continue {
        after_sequence: u64,
    },
    SnapshotRepair {
        reason: String,
        command: Box<DfsMetadataSnapshotInstallCommand>,
    },
    Terminal,
}

fn dfs_metadata_caught_up_probe_command(
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
    authority_epoch: u64,
    journal_sequence: u64,
    state_fingerprint: String,
) -> DfsMetadataJournalBatchCommand {
    let barrier_id = sinomo_core::Id::generate().to_string();
    DfsMetadataJournalBatchCommand {
        request_id: format!(
            "dfs_meta_probe_{}_{}_{}",
            local_node_id, target_node_id, barrier_id
        ),
        source_node_id: Some(local_node_id.clone()),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        verify_prefix_only: true,
        prefix_probe: Some(DfsMetadataPrefixProbe {
            barrier_id,
            file_system_id: Some(file_system_id.clone()),
            authority_epoch: Some(authority_epoch),
            shard_id: shard_id.clone(),
            journal_sequence,
            state_fingerprint,
            primary_handoff_id: None,
        }),
        authority_probe: None,
        durable_replication: None,
        batch: crate::DfsMetadataJournalFrameBatch {
            shard_id: shard_id.clone(),
            journal_frames: Vec::new(),
            next_after_sequence: journal_sequence,
        },
    }
}

#[cfg(test)]
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
        durable_replication: None,
        batch: crate::test_support::structural_journal_frame_batch(batch)
            .expect("encode structural current frame fixture"),
    }
}

pub(crate) fn dfs_metadata_durable_journal_batch_command(
    local_node_id: &NodeId,
    target_node_id: &NodeId,
    file_system_id: &DfsFileSystemId,
    authority_epoch: u64,
    batch: DfsPreparedMetadataJournalFrameBatch,
    background_publication: bool,
) -> DfsMetadataJournalBatchCommand {
    let confirmation_id = sinomo_core::Id::generate().to_string();
    DfsMetadataJournalBatchCommand {
        request_id: format!(
            "dfs_meta_{}_{}_{}",
            local_node_id, target_node_id, batch.next_after_sequence
        ),
        source_node_id: Some(local_node_id.clone()),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        verify_prefix_only: false,
        prefix_probe: None,
        authority_probe: None,
        durable_replication: Some(DfsMetadataDurableReplication {
            confirmation_id,
            background_publication,
        }),
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
    let image = backend
        .export_shard_snapshot_image(shard_id)?
        .ok_or_else(|| DfsError::Metadata {
            message: format!(
                "metadata shard {} cannot export its current snapshot image",
                shard_id.0
            ),
        })?;
    let next_after_sequence = image.materialized_through_sequence;
    Ok(DfsMetadataSnapshotInstallCommand {
        request_id: format!(
            "dfs_meta_snapshot_{}_{}_{}",
            local_node_id, target_node_id, next_after_sequence
        ),
        source_node_id: local_node_id.clone(),
        file_system_id: Some(file_system_id.clone()),
        authority_epoch: Some(authority_epoch),
        primary_handoff_id: None,
        shard_id: shard_id.clone(),
        next_after_sequence,
        snapshot: image.descriptor().clone(),
        image: Some(image),
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
                entries: dfs_metadata_journal_batch_command_entry_count(command),
                next_after_sequence,
            });
            Ok(DfsMetadataJournalReplicationStep::Continue {
                after_sequence: next_after_sequence,
            })
        }
        DfsMetadataJournalBatchResult::Rejected { reason, code } => {
            if code.allows_snapshot_repair() {
                let command = dfs_metadata_snapshot_install_command(
                    backend,
                    local_node_id,
                    target_node_id,
                    file_system_id,
                    authority_epoch,
                    shard_id,
                    reason.clone(),
                )?;
                Ok(DfsMetadataJournalReplicationStep::SnapshotRepair {
                    reason,
                    command: Box::new(command),
                })
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
            ..
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

        let batch = prepare_dfs_metadata_journal_frame_batch(backend, &batch)?;
        let command = dfs_metadata_durable_journal_batch_command(
            local_node_id,
            target_node_id,
            file_system_id,
            authority_epoch,
            batch,
            options.background_publication,
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
    result: ObjectRpcResult,
) -> Result<ObjectRpcResponse, DfsMetadataPeerTransportError> {
    value_response_from_rpc_result(result)
        .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
}

pub(crate) fn async_dfs_metadata_journal_batch_sender_from_peer_transport(
    _data_dir: &SinomoDataDir,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMetadataJournalBatchSender {
    #[cfg(any(test, feature = "test-support"))]
    let observer_data_dir = _data_dir.clone();
    Arc::new(move |target_node_id, command, operation_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        #[cfg(any(test, feature = "test-support"))]
        let observer_data_dir = observer_data_dir.clone();
        Box::pin(async move {
            let call = dfs_metadata_journal_batch_rpc_call(command)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            #[cfg(any(test, feature = "test-support"))]
            let mut native_attempt = crate::native_shard_trace::direct_journal_attempt_context(
                &observer_data_dir,
                command,
            )
            .and_then(|context| {
                context.begin_direct_journal(
                    target_node_id,
                    &call.request.route,
                    Arc::new(command.clone()),
                )
            });
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    call,
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| {
                    #[cfg(any(test, feature = "test-support"))]
                    if let Some(attempt) = native_attempt.as_mut() {
                        attempt.returned_direct_journal(
                            None,
                            Some(source.to_string()),
                            source.delivery_state(),
                        );
                    }
                    DfsMetadataPeerTransportError::new(source.to_string())
                })?;
            let decoded = response
                .decode_body::<DfsMetadataJournalBatchResponse>()
                .await;
            #[cfg(any(test, feature = "test-support"))]
            if let Some(attempt) = native_attempt.as_mut() {
                attempt.returned_direct_journal(
                    decoded.as_ref().ok(),
                    decoded.as_ref().err().map(ToString::to_string),
                    decoded
                        .as_ref()
                        .err()
                        .and_then(|error| error.delivery_state()),
                );
            }
            decoded.map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn async_dfs_metadata_snapshot_install_sender_from_peer_transport(
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
) -> AsyncDfsMetadataSnapshotInstallSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let peer_transport = Arc::clone(&peer_transport);
        Box::pin(async move {
            let call = dfs_metadata_snapshot_install_rpc_call(command)
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            let response = peer_transport
                .call_object_rpc_async(
                    target_node_id,
                    call,
                    DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
                )
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
            response
                .decode_body::<DfsMetadataSnapshotInstallResponse>()
                .await
                .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
        })
    })
}

pub(crate) fn dfs_metadata_journal_batch_sender_from_peer_transport(
    _data_dir: &SinomoDataDir,
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMetadataJournalBatchSender {
    #[cfg(any(test, feature = "test-support"))]
    let observer_data_dir = _data_dir.clone();
    Arc::new(move |target_node_id, command, operation_timeout| {
        let call = dfs_metadata_journal_batch_rpc_call(command)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        #[cfg(any(test, feature = "test-support"))]
        let mut native_attempt =
            crate::native_shard_trace::direct_journal_attempt_context(&observer_data_dir, command)
                .and_then(|context| {
                    context.begin_direct_journal(
                        target_node_id,
                        &call.request.route,
                        Arc::new(command.clone()),
                    )
                });
        let response = peer_transport
            .call_object_rpc(
                target_node_id,
                call,
                DfsObjectRpcSendOptions::default().with_overall_timeout(operation_timeout),
            )
            .map_err(|source| {
                #[cfg(any(test, feature = "test-support"))]
                if let Some(attempt) = native_attempt.as_mut() {
                    attempt.returned_direct_journal(
                        None,
                        Some(source.to_string()),
                        source.delivery_state(),
                    );
                }
                DfsMetadataPeerTransportError::new(source.to_string())
            })?;
        let response = dfs_metadata_value_response(response).map_err(|error| {
            #[cfg(any(test, feature = "test-support"))]
            if let Some(attempt) = native_attempt.as_mut() {
                attempt.returned_direct_journal(None, Some(error.to_string()), None);
            }
            error
        })?;
        let decoded = decode_dfs_metadata_journal_batch_rpc_response(&response);
        #[cfg(any(test, feature = "test-support"))]
        if let Some(attempt) = native_attempt.as_mut() {
            attempt.returned_direct_journal(
                decoded.as_ref().ok(),
                decoded.as_ref().err().map(ToString::to_string),
                decoded
                    .as_ref()
                    .err()
                    .and_then(|error| error.delivery_state()),
            );
        }
        decoded.map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))
    })
}

pub(crate) fn dfs_metadata_snapshot_install_sender_from_peer_transport(
    peer_transport: Arc<dyn DfsPeerTransport>,
) -> DfsMetadataSnapshotInstallSender {
    Arc::new(move |target_node_id, command, operation_timeout| {
        let call = dfs_metadata_snapshot_install_rpc_call(command)
            .map_err(|source| DfsMetadataPeerTransportError::new(source.to_string()))?;
        let response = peer_transport
            .call_object_rpc(
                target_node_id,
                call,
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

    // Shared negative physical fault on a genuine current snapshot record.
    fn corrupt_current_snapshot(path: &std::path::Path) -> Vec<u8> {
        use std::io::{Seek, Write};
        let reader =
            sinomo_core::storage::migration::stream::MigrationStreamReader::open(path).unwrap();
        let payload_offset = reader.cursor().offset + 8;
        drop(reader);
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(std::io::SeekFrom::Start(payload_offset)).unwrap();
        file.write_all(b"BAD").unwrap();
        file.sync_all().unwrap();
        drop(file);
        std::fs::read(path).unwrap()
    }

    #[test]
    fn native_real_receiver_ack_cannot_advance_a_failed_cursor_checkpoint() {
        let (runtime, delegated, config) = crate::test_support::current_native_metadata_service(
            "native-real-ack-cursor-checkpoint",
            3,
            "/services",
        )
        .unwrap();
        drop(delegated);
        let mut service = runtime.metadata_primary_service("/services").unwrap();
        let primary = config.primary_node_id.clone();
        let target = config.replica_node_ids[0].clone();
        let shard = DfsMetadataShardId::new(config.shard_id.clone());
        let data = runtime.data_dir(&primary).clone();
        let root = crate::local_state::local_dfs_metadata_shard_path(&data, &shard);
        let path = crate::metadata_cursor::metadata_replication_cursor_path(&root, &target, &shard);
        let backup = path.with_extension("actual-cursor-checkpoint-negative-backup");
        let before = service
            .metadata_backend()
            .unwrap()
            .metadata_replication_cursor(&target, &shard)
            .unwrap();
        let original_bytes = match std::fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("actual cursor before negative fault: {error}"),
        };
        let mut fs = service
            .metadata_backend()
            .unwrap()
            .get_file_system_on_shard(&shard, &config.file_system_id)
            .unwrap()
            .unwrap();
        fs.display_name.push_str("-hidden-cursor-checkpoint");
        let payload = crate::DfsPreparedOperationPayload::for_metadata_effects(
            crate::DfsPreparedOperationScope {
                file_system_id: config.file_system_id.clone(),
                shard_id: shard.clone(),
                owner_epoch: config.epoch,
                operation_id: format!("cursor-checkpoint:{}", sinomo_core::Id::generate()),
            },
            Vec::new(),
            Vec::new(),
            vec![crate::DfsPreparedMetadataEffect::PutFileSystem { file_system: fs }],
            time::OffsetDateTime::now_utc(),
        )
        .unwrap();
        let digest = service
            .metadata_backend_mut()
            .unwrap()
            .prepare_operation(payload, None)
            .unwrap();
        assert!(digest.prepare_sequence > before);
        let (peer, _) = runtime.peer_transports(&primary).unwrap();
        let actual =
            dfs_metadata_journal_batch_sender_from_peer_transport(&data, Arc::clone(&peer));
        let actual_snapshot = dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
        let acknowledged = Arc::new(std::sync::Mutex::new(Vec::new()));
        let replies = Arc::clone(&acknowledged);
        let faulted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fault = Arc::clone(&faulted);
        let selected = target.clone();
        let selected_shard = shard.clone();
        let fault_root = root.clone();
        let fault_path = path.clone();
        let fault_backup = backup.clone();
        let had_cursor = original_bytes.is_some();
        let fault_original_bytes = original_bytes.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        let snapshot: DfsMetadataSnapshotInstallSender = Arc::new(move |node, command, budget| {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| {
                    DfsMetadataPeerTransportError::new(
                        "original cursor test snapshot deadline elapsed",
                    )
                })?;
            actual_snapshot(node, command, budget.min(remaining))
        });
        let sender: DfsMetadataJournalBatchSender = Arc::new(move |node, command, budget| {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(|| {
                    DfsMetadataPeerTransportError::new(
                        "original cursor test publication deadline elapsed",
                    )
                })?;
            let response = actual(node, command, budget.min(remaining))?;
            if node == &selected && command.batch.shard_id == selected_shard {
                validate_dfs_metadata_journal_replication_response(
                    command,
                    &selected_shard,
                    &response,
                )
                .expect("genuine current receiver acknowledgement must match the whole request");
                assert!(command.durable_replication.is_some());
                assert!(matches!(
                    response.result,
                    DfsMetadataJournalBatchResult::Applied
                ));
                assert!(command.batch.next_after_sequence > before);
                replies
                    .lock()
                    .unwrap()
                    .push((command.clone(), response.clone()));
                if !fault.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    let _writer = crate::metadata_lock::metadata_write_lock(
                        fault_root.clone(),
                        crate::metadata_cursor::journal_write_lock_path(&fault_root),
                    )
                    .unwrap();
                    assert_eq!(
                        crate::metadata_cursor::metadata_replication_cursor(
                            &fault_root,
                            &selected,
                            &selected_shard,
                        )
                        .unwrap(),
                        before,
                    );
                    if let Some(bytes) = &fault_original_bytes {
                        assert!(
                            std::fs::symlink_metadata(&fault_path)
                                .unwrap()
                                .file_type()
                                .is_file()
                        );
                        assert_eq!(&std::fs::read(&fault_path).unwrap(), bytes);
                        std::fs::rename(&fault_path, &fault_backup).unwrap();
                    } else {
                        assert!(!fault_path.try_exists().unwrap());
                    }
                    std::fs::create_dir_all(&fault_path).unwrap();
                }
            }
            Ok(response)
        });
        let failed = publish_cluster_dfs_metadata_journal(
            &data,
            &ClusterPaths::new(&data),
            Arc::clone(&sender),
            Arc::clone(&snapshot),
            DfsMetadataJournalReplicationOptions {
                attempt_timeout: Duration::from_secs(5),
                target_timeout: Duration::from_secs(5),
                ..Default::default()
            },
        )
        .expect_err("a real ACK cannot hide failed local cursor durability");
        assert!(faulted.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!acknowledged.lock().unwrap().is_empty());
        assert!(
            service
                .metadata_backend()
                .unwrap()
                .metadata_replication_cursor(&target, &shard)
                .is_err()
        );
        let cold =
            crate::LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).unwrap();
        assert!(cold.metadata_replication_cursor(&target, &shard).is_err());
        drop(cold);
        {
            let _writer = crate::metadata_lock::metadata_write_lock(
                root.clone(),
                crate::metadata_cursor::journal_write_lock_path(&root),
            )
            .unwrap();
            std::fs::remove_dir(&path).unwrap();
            if had_cursor {
                assert_eq!(
                    std::fs::read(&backup).unwrap(),
                    original_bytes.clone().unwrap()
                );
                std::fs::rename(&backup, &path).unwrap();
            }
        }
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .metadata_replication_cursor(&target, &shard)
                .unwrap(),
            before
        );
        let cold =
            crate::LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).unwrap();
        assert_eq!(
            cold.metadata_replication_cursor(&target, &shard).unwrap(),
            before
        );
        drop(cold);
        let retry = publish_cluster_dfs_metadata_journal(
            &data,
            &ClusterPaths::new(&data),
            sender,
            snapshot,
            DfsMetadataJournalReplicationOptions {
                attempt_timeout: Duration::from_secs(5),
                target_timeout: Duration::from_secs(5),
                ..Default::default()
            },
        )
        .expect("normal replay and genuine acknowledgement persist the cursor after repair");
        let replies = acknowledged.lock().unwrap();
        assert!(
            replies.len() >= 2,
            "the retry must obtain another actual receiver acknowledgement: {retry:?}"
        );
        assert_eq!(replies[0].0.batch, replies[1].0.batch);
        let head = replies[1].0.batch.next_after_sequence;
        assert_eq!(
            service
                .metadata_backend()
                .unwrap()
                .metadata_replication_cursor(&target, &shard)
                .unwrap(),
            head
        );
        let cold =
            crate::LocalSobjMetadataBackend::open_for_inspection(shard.clone(), &root).unwrap();
        assert_eq!(
            cold.metadata_replication_cursor(&target, &shard).unwrap(),
            head
        );
        drop((cold, replies, service));
        assert!(
            Instant::now() < deadline,
            "publication and replay use the same original five-second window"
        );
        assert_eq!(runtime.observation_dropped_count(), 0);
        runtime
            .close()
            .expect("same normal owned source and receivers drain");
        assert!(
            failed.to_string().contains(&path.display().to_string()),
            "the actual cursor path must explain the persistence failure: {failed}"
        );
    }

    #[test]
    fn native_cursor_ahead_of_durable_head_is_reported_without_publication() {
        let nodes =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-invalid-cursor", 3);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-invalid-cursor", nodes[0].clone())
                .unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let mut client = runtime.client(&shard.primary_node_id).unwrap();
        let backend = client.metadata_backend_mut().unwrap();
        let before = backend
            .export_shard_table_snapshot(&shard_id)
            .unwrap()
            .unwrap();
        let head = backend.journal_last_sequence().unwrap();
        let outcome = match prepare_dfs_metadata_journal_target_publish(
            backend,
            &shard.primary_node_id,
            shard.replica_node_ids[0].clone(),
            shard.file_system_id.clone(),
            shard_id.clone(),
            shard.epoch,
            head + 1,
            DfsMetadataJournalReplicationOptions::default(),
            &mut BTreeMap::new(),
        )
        .unwrap()
        {
            DfsPreparedMetadataJournalTargetPublishResult::NoChanges { outcome } => outcome,
            DfsPreparedMetadataJournalTargetPublishResult::Publish { target } => {
                panic!("invalid cursor must not publish: {target:?}")
            }
        };
        let report = DfsMetadataJournalReplicationReport {
            outcomes: vec![outcome],
        };
        assert!(
            matches!(report.outcomes.as_slice(), [DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyUnhealed {
            cursor_after_sequence, durable_head_sequence, ..
        }] if *cursor_after_sequence == head + 1 && *durable_head_sequence == head)
        );
        assert!(dfs_metadata_replication_report_needs_retry(&report));
        assert_eq!(
            backend
                .export_shard_table_snapshot(&shard_id)
                .unwrap()
                .unwrap(),
            before
        );
        drop(client);
        runtime.close().unwrap();
    }

    #[test]
    fn cursor_fault_healing_notices_are_separate_from_normal_replication_notices() {
        let normal_target =
            NodeId::from_legacy_named("normal_replication_target").expect("normal target");
        let fault_target = NodeId::from_legacy_named("fault_healing_target").expect("fault target");
        let shard_id = DfsMetadataShardId::new("root");
        let report = DfsMetadataJournalReplicationReport {
            outcomes: vec![
                DfsMetadataJournalReplicationOutcome::Applied {
                    target_node_id: normal_target.clone(),
                    shard_id: shard_id.clone(),
                    entries: 4,
                    next_after_sequence: 8,
                },
                DfsMetadataJournalReplicationOutcome::CursorDurabilityInconsistencyDetected {
                    target_node_id: fault_target.clone(),
                    shard_id: shard_id.clone(),
                    cursor_after_sequence: 0,
                    durable_head_sequence: 8,
                },
                DfsMetadataJournalReplicationOutcome::Failed {
                    target_node_id: fault_target.clone(),
                    shard_id: shard_id.clone(),
                    reason: "exact-prefix probe timed out".to_string(),
                },
            ],
        };

        assert_eq!(
            dfs_metadata_replication_notice_lines(&report),
            vec![format!(
                "replicated 4 DFS metadata journal entries to {normal_target}"
            )]
        );
        assert_eq!(
            dfs_metadata_replication_fault_notice_lines(&report),
            vec![
                format!(
                    "DFS metadata fault healing detected an outbound cursor/durability inconsistency for shard root target {fault_target}: cursor=0, durable_head=8"
                ),
                format!(
                    "DFS metadata fault healing for shard root target {fault_target} failed: exact-prefix probe timed out"
                ),
            ]
        );
    }

    #[test]
    fn native_snapshot_install_and_corruption_recovery_preserve_actual_committed_state() {
        let nodes =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-snapshot-corruption", 3);
        let mut runtime = crate::test_runtime::DfsTestRuntime::new(
            "native-snapshot-corruption",
            nodes[0].clone(),
        )
        .unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let primary = shard.primary_node_id.clone();
        let receiver = shard.replica_node_ids[0].clone();
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let receiver_root = local_dfs_metadata_shard_path(runtime.data_dir(&receiver), &shard_id);
        let mut writer = runtime.client(&primary).unwrap();
        let stat = writer
            .posix_write_file(
                "/native-snapshot-recovery.bin",
                b"actual sealed bytes",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
                    replica_candidates: vec![primary.clone()],
                    wait_for_commit: true,
                    foreground_replica_repair: false,
                },
            )
            .unwrap();
        drop(writer);
        let mut primary_view = runtime
            .metadata_primary_service("/native-snapshot-recovery.bin")
            .unwrap();
        let committed = primary_view
            .metadata_backend()
            .unwrap()
            .get_object(&stat.object_id)
            .unwrap()
            .unwrap();
        assert_eq!(committed.version, stat.version);
        assert_eq!(committed.content_hash, stat.content_hash);
        let command = dfs_metadata_snapshot_install_command(
            primary_view.metadata_backend_mut().unwrap(),
            &primary,
            &receiver,
            &shard.file_system_id,
            shard.epoch,
            &shard_id,
            "native current snapshot replacement".into(),
        )
        .unwrap();
        let expected = primary_view
            .metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(command.snapshot.prefix_proof, expected.prefix_proof);
        assert_eq!(
            command.next_after_sequence,
            expected.materialized_through_sequence
        );
        drop(primary_view);
        let (peer, _) = runtime.peer_transports(&primary).unwrap();
        let sender = dfs_metadata_snapshot_install_sender_from_peer_transport(peer);
        let installed = sender(&receiver, &command, Duration::from_secs(5)).unwrap();
        assert!(
            matches!(
                installed.result,
                DfsMetadataSnapshotInstallResult::Installed { .. }
            ),
            "actual current snapshot install: {installed:?}"
        );
        runtime.stop_node(&receiver).unwrap();
        let snapshot_path =
            crate::metadata_table_store::metadata_table_snapshot_path(&receiver_root);
        let _corrupt_bytes = corrupt_current_snapshot(&snapshot_path);
        assert!(
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &receiver_root)
                .is_err(),
            "a genuine current-format corrupted snapshot cannot be accepted"
        );
        // The existing normal recovery route admits this pinned source image
        // before the corrupted receiver can complete its startup generation.
        // Reopen and repair both must succeed under the SAME original budget.
        runtime
            .reopen_node_with_startup_action(
                receiver.clone(),
                Box::new(|remaining| {
                    let installed = sender(&receiver, &command, remaining)
                        .map_err(|error| error.to_string())?;
                    if !matches!(
                        installed.result,
                        DfsMetadataSnapshotInstallResult::Installed {
                            archived_existing: true
                        }
                    ) {
                        return Err(format!(
                            "actual current corruption recovery rejected: {installed:?}"
                        ));
                    }
                    Ok(())
                }),
            )
            .unwrap();
        let mut receiver_view =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &receiver_root)
                .unwrap();
        receiver_view.begin_inspection_snapshot().unwrap();
        let object = receiver_view.get_object(&stat.object_id).unwrap().unwrap();
        assert_eq!(object.version, stat.version);
        assert_eq!(object.content_hash, stat.content_hash);
        assert_eq!(receiver_view.export_table_snapshot(), expected);
        assert!(receiver_root.join("replica-repair-archives").is_dir());
        receiver_view.end_inspection_snapshot();
        drop(receiver_view);
        drop(sender);
        runtime.stop_node(&receiver).unwrap();
        runtime.reopen_node(receiver).unwrap();
        let mut reader = runtime.client(&primary).unwrap();
        assert_eq!(
            reader
                .posix_read_file_distributed("/native-snapshot-recovery.bin")
                .unwrap(),
            b"actual sealed bytes"
        );
        drop(reader);
        runtime.close().unwrap();
    }

    #[test]
    fn native_stale_snapshot_cannot_roll_back_an_acknowledged_current_head() {
        let nodes =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-stale-snapshot", 3);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-stale-snapshot", nodes[0].clone())
                .unwrap();
        for node in &nodes[1..] {
            runtime.define_node(node.clone()).unwrap();
        }
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let primary = shard.primary_node_id.clone();
        let receiver = shard.replica_node_ids[0].clone();
        let shard_id = DfsMetadataShardId::new(shard.shard_id.clone());
        let options = crate::DfsWriteOptions {
            policy: crate::DfsReplicationPolicy::new(1, 1).unwrap(),
            replica_candidates: vec![primary.clone()],
            wait_for_commit: true,
            foreground_replica_repair: false,
        };
        let mut writer = runtime.client(&primary).unwrap();
        let old = writer
            .posix_write_file("/native-stale-cut.bin", b"old bytes", options.clone())
            .unwrap();
        let mut predecessor_view = runtime
            .metadata_primary_service("/native-stale-cut.bin")
            .unwrap();
        assert_eq!(
            predecessor_view
                .metadata_backend()
                .unwrap()
                .get_object(&old.object_id)
                .unwrap()
                .unwrap()
                .version,
            old.version
        );
        let stale = dfs_metadata_snapshot_install_command(
            predecessor_view.metadata_backend_mut().unwrap(),
            &primary,
            &receiver,
            &shard.file_system_id,
            shard.epoch,
            &shard_id,
            "native captured predecessor".into(),
        )
        .unwrap();
        drop(predecessor_view);
        let current = writer
            .posix_write_file("/native-stale-cut.bin", b"current bytes", options)
            .unwrap();
        drop(writer);
        let mut primary_view = runtime
            .metadata_primary_service("/native-stale-cut.bin")
            .unwrap();
        assert_eq!(
            primary_view
                .metadata_backend()
                .unwrap()
                .get_object(&current.object_id)
                .unwrap()
                .unwrap()
                .version,
            current.version
        );
        let expected = primary_view
            .metadata_backend()
            .unwrap()
            .export_shard_table_snapshot(&shard_id)
            .unwrap()
            .unwrap();
        assert!(stale.next_after_sequence < expected.materialized_through_sequence);
        assert!(expected.prefix_proof.is_well_formed());
        assert_eq!(
            expected.prefix_proof.journal_sequence,
            expected.materialized_through_sequence
        );
        let (peer, _) = runtime.peer_transports(&primary).unwrap();
        let sender = dfs_metadata_snapshot_install_sender_from_peer_transport(Arc::clone(&peer));
        let journal_sender =
            dfs_metadata_journal_batch_sender_from_peer_transport(runtime.data_dir(&primary), peer);
        let mut transport = DfsMetadataNodeTransport::new(
            shard.replica_node_ids.clone(),
            journal_sender,
            Arc::clone(&sender),
        );
        let convergence_started = Instant::now();
        let outcomes = replicate_dfs_metadata_shard_to_target(
            primary_view.metadata_backend_mut().unwrap(),
            &primary,
            &receiver,
            &shard.file_system_id,
            shard.epoch,
            &shard_id,
            0,
            &mut transport,
            DfsMetadataJournalReplicationOptions {
                attempt_timeout: Duration::from_secs(5),
                target_timeout: Duration::from_secs(5),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(outcomes.iter().any(|outcome| matches!(outcome,
            DfsMetadataJournalReplicationOutcome::Applied { target_node_id, next_after_sequence, .. }
                if target_node_id == &receiver && *next_after_sequence == expected.materialized_through_sequence
        )), "selected real receiver must durably acknowledge current head: {outcomes:?}");
        let remaining = Duration::from_secs(5).saturating_sub(convergence_started.elapsed());
        assert!(
            !remaining.is_zero(),
            "current-view convergence retains its five-second budget"
        );
        let probe = dfs_metadata_caught_up_probe_command(
            &primary,
            &receiver,
            &shard.file_system_id,
            &shard_id,
            shard.epoch,
            expected.materialized_through_sequence,
            expected.prefix_proof.digest.clone(),
        );
        let confirmed = transport
            .send_metadata_journal_batch(&receiver, &probe, remaining)
            .unwrap();
        assert!(
            matches!(confirmed.result, DfsMetadataJournalBatchResult::Applied),
            "actual current-view probe: {confirmed:?}"
        );
        assert!(confirmed.verified_prefix_only);
        assert_eq!(
            confirmed.next_after_sequence,
            expected.materialized_through_sequence
        );
        drop(transport);
        let current_snapshot = dfs_metadata_snapshot_install_command(
            primary_view.metadata_backend_mut().unwrap(),
            &primary,
            &receiver,
            &shard.file_system_id,
            shard.epoch,
            &shard_id,
            "actual current image after corrupt stale rejection".into(),
        )
        .unwrap();
        assert_eq!(
            current_snapshot.snapshot.prefix_proof,
            expected.prefix_proof
        );
        drop(primary_view);
        let receiver_root = local_dfs_metadata_shard_path(runtime.data_dir(&receiver), &shard_id);
        let before =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &receiver_root)
                .unwrap();
        let floor = before
            .metadata_prefix_ack_floor(&shard_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            floor.journal_sequence,
            expected.materialized_through_sequence
        );
        assert_eq!(floor.durable_prefix_proof, expected.prefix_proof);
        drop(before);
        let rejected = sender(&receiver, &stale, Duration::from_secs(5)).unwrap();
        assert!(
            matches!(
                rejected.result,
                DfsMetadataSnapshotInstallResult::Rejected { .. }
            ),
            "actual receiver authority floor forbids predecessor rollback"
        );
        let mut reader =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &receiver_root)
                .unwrap();
        reader.begin_inspection_snapshot().unwrap();
        let object = reader.get_object(&current.object_id).unwrap().unwrap();
        assert_eq!(object.version, current.version);
        let actual = reader.export_table_snapshot();
        assert_eq!(actual, expected);
        reader.end_inspection_snapshot();
        drop(reader);
        // Repeat the SAME genuine stale image against an unreadable replica.
        // The normal native slot must reach its durable floor check before archive.
        runtime.stop_node(&receiver).unwrap();
        let snapshot_path =
            crate::metadata_table_store::metadata_table_snapshot_path(&receiver_root);
        let damaged = corrupt_current_snapshot(&snapshot_path);
        assert!(
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &receiver_root)
                .is_err()
        );
        let archive = receiver_root.join("replica-repair-archives");
        assert!(
            !archive.exists(),
            "this negative cut starts without repair archives"
        );
        let prefix_floor =
            metadata_cursor::metadata_prefix_ack_floor(&receiver_root, &shard_id).unwrap();
        let authority_floor =
            metadata_cursor::metadata_authority_ack_floor(&receiver_root, &shard_id).unwrap();
        assert_eq!(prefix_floor.as_ref(), Some(&floor));
        let receiver_data = runtime.data_dir(&receiver).clone();
        runtime
            .reopen_node_with_startup_action(
                receiver.clone(),
                Box::new(|remaining| {
                    let began = Instant::now();
                    let rejection = sender(&receiver, &stale, remaining);
                    let reason = match rejection {
                        Ok(response) => match response.result {
                            DfsMetadataSnapshotInstallResult::Rejected { reason, .. } => reason,
                            other => panic!("corrupt receiver accepted stale snapshot: {other:?}"),
                        },
                        Err(error) => error.to_string(),
                    };
                    assert!(
                        reason.contains("behind locally acknowledged"),
                        "actual unreadable replacement must reach its durable floor: {reason}"
                    );
                    assert_eq!(
                        std::fs::read(&snapshot_path).unwrap(),
                        damaged,
                        "floor rejection preserves the exact corrupt record bytes"
                    );
                    assert!(
                        !archive.exists(),
                        "floor rejection cannot archive or publish replacement"
                    );
                    assert_eq!(
                        metadata_cursor::metadata_prefix_ack_floor(&receiver_root, &shard_id)
                            .unwrap(),
                        prefix_floor
                    );
                    assert_eq!(
                        metadata_cursor::metadata_authority_ack_floor(&receiver_root, &shard_id)
                            .unwrap(),
                        authority_floor
                    );
                    assert!(
                        LocalSobjMetadataBackend::open_for_inspection(
                            shard_id.clone(),
                            &receiver_root
                        )
                        .is_err()
                    );
                    let peer = crate::runtime_host::read_daemon_dfs_message_peer_snapshot(
                        &ClusterPaths::new(&receiver_data),
                    )
                    .unwrap()
                    .unwrap();
                    // The bounded cache observation can yield to another root's
                    // registry update. Join only that exact transient within
                    // this recovery action's existing remaining budget.
                    let resident = loop {
                        match crate::runtime_host::daemon_dfs_bounded_status_resident(
                            &receiver_data,
                            &peer,
                        ) {
                            Err(DfsError::Network { ref message })
                                if (message == "bounded DFS status unavailable while the resident generation cache is being updated"
                                    || message == "DFS runtime network error at DFS message cache root identity: bounded DFS status unavailable while the resident generation cache is being updated")
                                    && began.elapsed() < remaining =>
                            {
                                std::thread::sleep(Duration::from_millis(2)
                                    .min(remaining.saturating_sub(began.elapsed())));
                            }
                            result => break result,
                        }
                    }
                    .expect("actual corrupt receiver resident generation");
                    let status = resident.bounded_status_snapshot().unwrap();
                    assert!(
                        status
                            .bounded_omissions
                            .iter()
                            .any(|omission| matches!(omission,
                    crate::DfsOperatorStatusOmission::NonPrimaryResidentMetadataNotHydrated {
                        file_system_id, shard_id: unavailable
                    } if file_system_id == &shard.file_system_id && unavailable == &shard_id)),
                        "corrupt replica status cannot become an invented complete projection"
                    );
                    let remaining = remaining.saturating_sub(began.elapsed());
                    assert!(
                        !remaining.is_zero(),
                        "negative and actual recovery share the original budget"
                    );
                    let installed = sender(&receiver, &current_snapshot, remaining)
                        .map_err(|error| error.to_string())?;
                    assert!(
                        matches!(
                            installed.result,
                            DfsMetadataSnapshotInstallResult::Installed {
                                archived_existing: true
                            }
                        ),
                        "the same native ingress admits the genuine current image: {installed:?}"
                    );
                    Ok(())
                }),
            )
            .unwrap();
        let mut recovered =
            LocalSobjMetadataBackend::open_for_inspection(shard_id.clone(), &receiver_root)
                .unwrap();
        recovered.begin_inspection_snapshot().unwrap();
        assert_eq!(recovered.export_table_snapshot(), expected);
        assert_eq!(
            recovered
                .get_object(&current.object_id)
                .unwrap()
                .unwrap()
                .version,
            current.version
        );
        recovered.end_inspection_snapshot();
        drop(recovered);
        drop(sender);
        runtime.close().unwrap();
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
                durable_replication: None,
                batch: crate::DfsMetadataJournalFrameBatch {
                    shard_id: shard_id.clone(),
                    journal_frames: Vec::new(),
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
            .export_snapshot_image()
            .expect("native snapshot image");
        let snapshot_response = process_cluster_dfs_metadata_snapshot_install(
            &data_dir,
            &cluster_paths,
            &source_node_id,
            DfsMetadataSnapshotInstallCommand {
                request_id: "unauthorized-snapshot".to_string(),
                source_node_id: source_node_id.clone(),
                file_system_id: Some(file_system_id),
                authority_epoch: Some(1),
                primary_handoff_id: None,
                shard_id,
                next_after_sequence: 1,
                snapshot: snapshot.descriptor().clone(),
                image: Some(snapshot),
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
                None,
            ))
            .expect("cursor commit");
        runtime
            .block_on(put_local_dfs_metadata_replication_cursor_async(
                data_dir,
                target_node_id.clone(),
                shard_id.clone(),
                12,
                None,
            ))
            .expect("monotonic cursor commit");

        assert_eq!(
            metadata_cursor::metadata_replication_cursor(&root, &target_node_id, &shard_id)
                .expect("cursor read"),
            42
        );
    }
}

#[cfg(test)]
mod primary_transfer_wake_tests;

/// Resolve the immutable bodies referenced by the current commit markers.
/// Compact marker identity alone cannot classify visible effects.
pub(crate) fn committed_payloads_for_batch(
    backend: &dyn DfsMetadataBackend,
    batch: &DfsMetadataJournalBatch,
) -> Result<Vec<Arc<crate::DfsPreparedOperationPayload>>, DfsError> {
    let mut payloads = Vec::new();
    for entry in &batch.entries {
        let crate::DfsMetadataMutation::CommitPreparedOperationV7 { digest, .. } = &entry.mutation
        else {
            continue;
        };
        if digest.prepare_sequence >= entry.sequence || digest.shard_id != entry.shard_id {
            return Err(DfsError::Metadata {
                message: "commit marker differs from its actual durable prepare scope".into(),
            });
        }
        let inline = batch
            .entries
            .iter()
            .find_map(|prepare| match &prepare.mutation {
                crate::DfsMetadataMutation::PrepareOperationV7 { record, .. }
                    if record.digest == *digest && prepare.sequence == digest.prepare_sequence =>
                {
                    Some(Arc::clone(&record.payload))
                }
                _ => None,
            });
        let payload = match inline {
            Some(payload) => payload,
            None => {
                let stored = backend.prepared_operation_payload(&digest.operation_id)?;
                if let Some(payload) = stored {
                    payload
                } else {
                    let prepare = backend
                        .journal_batch_for_shard_since_limited(
                            &digest.shard_id,
                            digest.prepare_sequence.saturating_sub(1),
                            1,
                            usize::MAX,
                        )?
                        .and_then(|batch| {
                            batch
                                .entries
                                .into_iter()
                                .find(|entry| entry.sequence == digest.prepare_sequence)
                        });
                    match prepare.map(|entry| entry.mutation) {
                        Some(crate::DfsMetadataMutation::PrepareOperationV7 { record, .. })
                            if record.digest == *digest =>
                        {
                            record.payload
                        }
                        _ => return Err(DfsError::Metadata {
                            message:
                                "committed semantic body is unavailable; a scope rescan is required"
                                    .into(),
                        }),
                    }
                }
            }
        };
        digest.validate_payload(&payload)?;
        payloads.push(payload);
    }
    Ok(payloads)
}

pub(crate) fn payload_visible_paths(
    payload: &crate::DfsPreparedOperationPayload,
) -> BTreeSet<String> {
    payload
        .transaction
        .objects
        .iter()
        .map(|object| object.path.clone())
        .chain(
            payload
                .metadata_effects
                .iter()
                .filter_map(|effect| match effect {
                    crate::DfsPreparedMetadataEffect::PutObject { object }
                    | crate::DfsPreparedMetadataEffect::FinalizeCrossShardSourceEviction {
                        tombstone: object,
                        ..
                    } => Some(object.path.clone()),
                    _ => None,
                }),
        )
        .collect()
}
