//! DFS lifecycle maintenance, anti-entropy, and repair tick orchestration.
//!
//! This module owns both the reusable maintenance host surface and the
//! cluster-aware entrypoints that open DFS services, bind transports, and run a
//! maintenance tick against configured metadata shards.

mod progress;
pub(crate) use progress::DfsMaintenanceProgressState;

use serde::{Deserialize, Serialize};
use sinomo_core::ClusterNodeAvailability;
use sinomo_core::kv::KvEntry;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{
    ClusterConnectivityGraph, ClusterPaths, cluster_nodes_share_healthy_component,
};
use sinomo_core::node::NodeId;
use sinomo_core::object::{LengthMode, serde_ext as sinomo_object_serde};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Bounded process-local projection used to schedule DFS maintenance without
/// repeatedly running a broad maintenance pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsPendingMaintenanceWork {
    pub pending_write_count: usize,
    pub pending_repair_count: usize,
    pub repair_attempt_count: usize,
    pub due_repair_attempt_count: usize,
    pub unattempted_repair_count: usize,
    pub next_repair_retry_unix_nanos: Option<u64>,
    pub due_time_based_maintenance_count: usize,
    pub next_time_based_maintenance_unix_nanos: Option<u64>,
    pub deferred_work_count: usize,
    pub pending_namespace_job_count: usize,

    pub actionable_placement_reconciliation_count: usize,
    pub metadata_replica_lag_count: usize,
    pub pending_local_membership_handoff_count: usize,
    pub completed_local_primary_handoff_rebind_count: usize,
    pub mounted_mutation_live_count: usize,
    pub mounted_mutation_recoverable_exact_count: usize,
    pub mounted_mutation_recoverable_full_count: usize,
    pub mounted_mutation_overflow_count: usize,
    pub next_mounted_mutation_reprobe_unix_nanos: Option<u64>,
}

pub const DFS_RESIDENT_REPAIR_MAX_JOBS_PER_TICK: usize = 8;
pub const DFS_RESIDENT_TOMBSTONE_COMPACTION_MIN_AGE: Duration =
    Duration::from_secs(7 * 24 * 60 * 60);
pub const DFS_RESIDENT_TOMBSTONE_COMPACTION_MAX_RECORDS: u64 = 256;
pub const DFS_RESIDENT_METADATA_HISTORY_GC_MIN_AGE: Duration =
    Duration::from_secs(7 * 24 * 60 * 60);
pub const DFS_RESIDENT_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS: u64 = 0;
pub const DFS_RESIDENT_METADATA_HISTORY_GC_MAX_RECORDS: u64 = 256;
pub const DFS_RESIDENT_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS: u64 = 4_096;
pub const DFS_RESIDENT_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET: usize = 64;
pub const DFS_RESIDENT_METADATA_REPLICATION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DFS_RESIDENT_METADATA_REPLICATION_TARGET_TIMEOUT: Duration = Duration::from_secs(15);

impl DfsPendingMaintenanceWork {
    pub fn has_work(&self) -> bool {
        self.pending_write_count > 0
            || self.pending_repair_count > 0
            || self.pending_namespace_job_count > 0
            || self.actionable_placement_reconciliation_count > 0
            || self.metadata_replica_lag_count > 0
            || self.pending_local_membership_handoff_count > 0
            || self.completed_local_primary_handoff_rebind_count > 0
            || self.mounted_mutation_recoverable_exact_count > 0
            || self.mounted_mutation_recoverable_full_count > 0
            || self.due_time_based_maintenance_count > 0
    }

    pub fn should_trigger_background_maintenance(&self) -> bool {
        // Metadata replica lag is deliberately absent: retained exact-shard
        // publisher lanes own those durable cursors and connectivity wakes.
        self.pending_write_count > 0
            || self.pending_namespace_job_count > 0
            || self.actionable_placement_reconciliation_count > 0
            || self.pending_local_membership_handoff_count > 0
            || self.completed_local_primary_handoff_rebind_count > 0
            || self.mounted_mutation_recoverable_exact_count > 0
            || self.mounted_mutation_recoverable_full_count > 0
            || self.due_repair_attempt_count > 0
            || self.unattempted_repair_count > 0
            || self.due_time_based_maintenance_count > 0
    }

    pub fn next_maintenance_deadline_unix_nanos(&self) -> Option<u64> {
        self.next_repair_retry_unix_nanos
            .into_iter()
            .chain(self.next_time_based_maintenance_unix_nanos)
            .chain(self.next_mounted_mutation_reprobe_unix_nanos)
            .min()
    }

    pub fn has_deferred_work(&self) -> bool {
        self.deferred_work_count > 0
    }
}

/// Whether a maintenance pass itself proved that bounded work remains and
/// that the pass made progress toward it.
pub fn dfs_maintenance_report_reached_limit(report: &DfsMaintenanceTickReport) -> bool {
    let terminal_namespace_jobs = report
        .namespace_jobs
        .completed_job_count
        .saturating_add(report.namespace_jobs.failed_job_count);
    let resumed_namespace_jobs_still_running = report
        .namespace_jobs
        .resumed_job_count
        .saturating_sub(terminal_namespace_jobs);
    let skipped_namespace_jobs = report
        .namespace_jobs
        .remaining_running_job_count
        .saturating_sub(resumed_namespace_jobs_still_running);
    let namespace_job_limit_reached = terminal_namespace_jobs > 0 && skipped_namespace_jobs > 0;

    let placement_page_remaining = report.placement_reconciliation.as_ref().is_some_and(|job| {
        // Finishing a job or its scan is progress too. The post-pass work
        // classifier decides whether another job/verification pass is ready;
        // do not insert the routine timer between those productive phases.
        job.state.is_terminal()
            || (job.last_error.is_none()
                && (job.cursor.is_some()
                    || (job.state == crate::DfsPlacementReconciliationState::Repairing
                        && job.pending_repair_count == 0)))
    });
    let lifecycle_limit_reached = report.lifecycle_reports.iter().any(|lifecycle| {
        lifecycle
            .stale_staging_gc
            .as_ref()
            .is_some_and(|gc| gc.max_deletions_reached && gc.deleted_staging_count > 0)
            || lifecycle
                .version_gc
                .as_ref()
                .is_some_and(|gc| gc.max_deletions_reached && gc.deleted_version_count > 0)
            || lifecycle.orphan_gc.as_ref().is_some_and(|gc| {
                (gc.pending_inline_compaction_count != 0
                    && (gc.advanced_inline_compaction_steps != 0
                        || gc.reclaimed_inline_generation_bytes != 0))
                    || gc.max_deletions_reached
                        && gc
                            .deleted_orphan_count
                            .saturating_add(gc.deleted_stale_orphan_observation_count)
                            .saturating_add(gc.persisted_orphan_observation_count)
                            > 0
            })
            || lifecycle
                .metadata_history_gc
                .as_ref()
                .is_some_and(|gc| gc.max_removals_reached && gc.applied_record_count() > 0)
            || lifecycle
                .content_reservation_reconciliation
                .as_ref()
                .is_some_and(|reconciliation| {
                    reconciliation.has_more && reconciliation.examined_reservation_count > 0
                })
            || lifecycle
                .tombstone_compaction
                .as_ref()
                .is_some_and(|compaction| {
                    compaction.max_compactions_reached && compaction.compacted_tombstone_count > 0
                })
    });

    namespace_job_limit_reached
        || (report.write_intents.scan_has_more
            && report.write_intents.visited_intent_shard_count > 0)
        || placement_page_remaining
        || lifecycle_limit_reached
        || (report.repair_budget_exhausted
            && (!report.scheduled_repairs.is_empty()
                || report.repair_page_checkpoint.completed_record_count > 0
                || report.repair_page_checkpoint.operation_count > 0))
}

/// Bounded resident-daemon maintenance policy. Inventory-wide lifecycle work
/// remains disabled and is enabled only by explicit recovery options.
pub fn dfs_resident_routine_maintenance_tick_options() -> DfsMaintenanceTickOptions {
    DfsMaintenanceTickOptions {
        repair_max_jobs: DFS_RESIDENT_REPAIR_MAX_JOBS_PER_TICK,
        lifecycle: DfsMaintenanceRunOptions {
            repair_metadata: false,
            cleanup_stale_staging: false,
            compact_tombstones: false,
            tombstone_compaction_min_age: DFS_RESIDENT_TOMBSTONE_COMPACTION_MIN_AGE,
            tombstone_compaction_max_records: DFS_RESIDENT_TOMBSTONE_COMPACTION_MAX_RECORDS,
            collect_metadata_history: false,
            metadata_history_gc_min_age: DFS_RESIDENT_METADATA_HISTORY_GC_MIN_AGE,
            metadata_history_gc_keep_latest_versions:
                DFS_RESIDENT_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS,
            metadata_history_gc_max_records: DFS_RESIDENT_METADATA_HISTORY_GC_MAX_RECORDS,
            collect_content_versions: false,
            collect_orphan_content: false,
            reconcile_content_reservations: false,
            content_reservation_reconciliation_max_records:
                DFS_RESIDENT_CONTENT_RESERVATION_RECONCILIATION_MAX_RECORDS,
            ..DfsMaintenanceRunOptions::default()
        },
        publish_metadata: false,
        metadata_replication: dfs_resident_metadata_replication_options(),
        ..DfsMaintenanceTickOptions::default()
    }
}

pub fn dfs_explicit_disaster_recovery_tick_options(
    content_gc_requested: bool,
    content_reservation_reconciliation_requested: bool,
) -> DfsMaintenanceTickOptions {
    let mut options = dfs_resident_routine_maintenance_tick_options();
    let inventory_requested = content_gc_requested || content_reservation_reconciliation_requested;
    options.lifecycle.repair_metadata = inventory_requested;
    options.lifecycle.cleanup_stale_staging = inventory_requested;
    options.lifecycle.compact_tombstones = inventory_requested;
    options.lifecycle.collect_metadata_history = inventory_requested;
    options.lifecycle.collect_content_versions = content_gc_requested;
    options.lifecycle.collect_orphan_content = content_gc_requested;
    options.lifecycle.reconcile_content_reservations = inventory_requested;
    options
}

/// Finish the finite replay obligations of an orphaned mounted mutation.
/// Reconstruction transfers content repair into its durable queue. Adaptive
/// placement and ordinary repair dispatch continue under their retained owners;
/// their ongoing page budgets must not prevent the mount from becoming ready.
pub fn dfs_mounted_mutation_recovery_tick_options() -> DfsMaintenanceTickOptions {
    let mut options = dfs_explicit_disaster_recovery_tick_options(false, false);
    options.placement_reconciliation_max_objects = 0;
    options.repair_max_jobs = 0;
    options
}

pub fn dfs_resident_metadata_replication_options() -> DfsMetadataJournalReplicationOptions {
    DfsMetadataJournalReplicationOptions {
        max_batches_per_target: DFS_RESIDENT_METADATA_REPLICATION_MAX_BATCHES_PER_TARGET,
        attempt_timeout: DFS_RESIDENT_METADATA_REPLICATION_ATTEMPT_TIMEOUT,
        target_timeout: DFS_RESIDENT_METADATA_REPLICATION_TARGET_TIMEOUT,
        probe_caught_up_replicas: true,
        ..DfsMetadataJournalReplicationOptions::default()
    }
}

pub fn dfs_resident_smoke_options() -> crate::DfsSmokeOptions {
    crate::DfsSmokeOptions {
        stable_path: true,
        remote_replica_candidates: false,
        foreground_replica_repair: false,
        inspect_pending_repairs: false,
        publish_metadata: false,
        metadata_replication: DfsMetadataJournalReplicationOptions {
            max_batches_per_target: 0,
            ..dfs_resident_metadata_replication_options()
        },
        ..crate::DfsSmokeOptions::default()
    }
}

struct DfsConnectivityStatusProbe;

impl sinomo_core::cluster::ClusterStatusProbe for DfsConnectivityStatusProbe {
    fn probe_member_status(
        &self,
        _state: &sinomo_core::ClusterBootstrapState,
        _member: &sinomo_core::ClusterMemberRecord,
    ) -> Option<sinomo_core::ClusterNodeStatusRecord> {
        None
    }
}

pub fn dfs_node_is_offline_for_maintenance(
    availability: &ClusterNodeAvailability,
    connectivity: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    availability != &ClusterNodeAvailability::Online
        && local_node_id != target_node_id
        && !cluster_nodes_share_healthy_component(connectivity, local_node_id, target_node_id)
}

/// Build the bounded reachability exclusion set used by DFS repair and
/// detached-send scheduling.
pub fn dfs_offline_node_ids(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> BTreeSet<NodeId> {
    dfs_offline_node_ids_with_transport_availability(cluster_paths, state, &|_| {
        sinomo_core::network::RoutedAvailability::Unknown
    })
}

/// Route capability can resolve only an unknown reachability hint. Actual
/// membership state, explicit Offline, policy/storage and effect guards remain
/// independent; Candidate never becomes Online or a healthy graph edge.
pub(crate) fn dfs_transport_candidate_can_relax_unknown_route(
    availability: &ClusterNodeAvailability,
    member: &sinomo_core::ClusterMemberRecord,
    transport: sinomo_core::network::RoutedAvailability,
) -> bool {
    *availability == ClusterNodeAvailability::Unknown
        && matches!(
            member.state,
            sinomo_core::ClusterMemberState::Learner | sinomo_core::ClusterMemberState::Voter
        )
        && matches!(
            transport,
            sinomo_core::network::RoutedAvailability::Candidate
                | sinomo_core::network::RoutedAvailability::Connected
        )
}

pub(crate) fn dfs_offline_node_ids_with_transport_availability(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
    transport: &dyn Fn(&NodeId) -> sinomo_core::network::RoutedAvailability,
) -> BTreeSet<NodeId> {
    let connectivity = ClusterConnectivityGraph::read_for_local(cluster_paths, state);
    sinomo_core::cluster::cluster_health_rows_with_probe(
        cluster_paths,
        state,
        &DfsConnectivityStatusProbe,
        false,
        time::OffsetDateTime::now_utc(),
    )
    .unwrap_or_default()
    .into_iter()
    .filter_map(|row| {
        let excluded = dfs_node_is_offline_for_maintenance(
            &row.availability,
            &connectivity,
            &state.local_node_id,
            &row.member.node_id,
        ) && !(row.availability == ClusterNodeAvailability::Unknown
            && matches!(
                row.member.state,
                sinomo_core::ClusterMemberState::Learner | sinomo_core::ClusterMemberState::Voter
            )
            && dfs_transport_candidate_can_relax_unknown_route(
                &row.availability,
                &row.member,
                transport(&row.member.node_id),
            ));
        if excluded {
            // Record the actual advisory exclusion used by this plan. Do not
            // issue another transport/health read or turn it into authority.
            tracing::trace!(target: "sinomo.dfs.perf", node = %state.local_node_id,
                target = %row.member.node_id, availability = ?row.availability,
                member_state = ?row.member.state,
                "metadata-maintenance-target-excluded");
        }
        excluded.then_some(row.member.node_id)
    })
    .collect()
}

pub fn dfs_resident_maintenance_execution_exclusions(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> DfsMaintenanceExecutionExclusions {
    DfsMaintenanceExecutionExclusions::new(dfs_offline_node_ids(cluster_paths, state))
}

/// Count placement jobs that can advance without waiting on an already-owned
/// durable repair generation.
pub fn dfs_actionable_placement_reconciliation_count(
    jobs: &[crate::DfsPlacementReconciliationJob],
    pending_repair_count: usize,
) -> usize {
    dfs_actionable_placement_reconciliation_count_with_config(jobs, pending_repair_count, None)
}

pub(crate) fn dfs_actionable_placement_reconciliation_count_with_config(
    jobs: &[crate::DfsPlacementReconciliationJob],
    pending_repair_count: usize,
    current_config: Option<&crate::DfsFileSystemConfig>,
) -> usize {
    jobs.iter()
        .filter(|job| {
            dfs_placement_reconciliation_job_can_advance(job, pending_repair_count, current_config)
        })
        .count()
}

/// One scheduler/executor contract for a job waiting on the existing durable
/// repair owner. In-progress pages finish promptly; an empty queue owns the
/// final verification edge. This preserves the current global-count semantics.
pub(crate) fn dfs_placement_reconciliation_job_can_advance(
    job: &crate::DfsPlacementReconciliationJob,
    pending_repair_count: usize,
    current_config: Option<&crate::DfsFileSystemConfig>,
) -> bool {
    let runnable = match job.state {
        crate::DfsPlacementReconciliationState::Pending
        | crate::DfsPlacementReconciliationState::Scanning => true,
        crate::DfsPlacementReconciliationState::Repairing => {
            job.cursor.is_some() || job.pending_repair_count == 0 || pending_repair_count == 0
        }
        crate::DfsPlacementReconciliationState::Completed
        | crate::DfsPlacementReconciliationState::Failed => false,
    };
    runnable
        || (job.state == crate::DfsPlacementReconciliationState::Repairing
            && current_config.is_some_and(|current| {
                !crate::placement::placement_config_remains_authoritative_for_job(
                    &job.accepted_config,
                    current,
                    job,
                )
            }))
}

/// Count completed local-primary handoffs whose durable router generation has
/// not consumed the exact completion proof yet.
pub fn dfs_completed_local_primary_handoff_rebind_count(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    pending_handoffs: &[crate::DfsMetadataShardMembershipHandoff],
    active_shards: &[crate::DfsMetadataShardConfig],
    authority_entries: &[KvEntry],
) -> Result<usize, DfsError> {
    let pending_shards = pending_handoffs
        .iter()
        .map(|handoff| {
            (
                handoff.retiring_shard.file_system_id.clone(),
                handoff.retiring_shard.shard_id.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let mut count = 0usize;
    for active in active_shards
        .iter()
        .filter(|shard| shard.primary_node_id == *local_node_id)
    {
        if pending_shards.contains(&(active.file_system_id.clone(), active.shard_id.clone())) {
            continue;
        }
        let shard_id = crate::DfsMetadataShardId::new(active.shard_id.clone());
        let Some(freeze) = crate::read_local_dfs_metadata_shard_write_freeze(data_dir, &shard_id)?
        else {
            continue;
        };
        if freeze.activated_epoch.is_some() {
            continue;
        }
        let proof_key = crate::metadata_shard_membership_handoff_proof_key(
            &active.file_system_id,
            &active.shard_id,
        );
        let Some(proof) = authority_entries
            .binary_search_by(|entry| entry.key.as_str().cmp(proof_key.as_str()))
            .ok()
            .map(|index| {
                authority_entries[index].decode::<crate::DfsMetadataShardMembershipHandoffProof>()
            })
            .transpose()?
        else {
            continue;
        };
        if freeze.journal_sequence == proof.journal_sequence
            && freeze.state_fingerprint == proof.state_fingerprint
            && crate::completed_promoted_primary_handoff_matches(
                local_node_id,
                active,
                &freeze.handoff_id,
                freeze.retiring_epoch,
                &proof,
            )
        {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

/// Build the bounded production maintenance projection from the retained DFS
/// authority prefix and the durable repair queue. This never hydrates metadata
/// or enumerates content.
pub fn dfs_pending_reactive_maintenance_work_from_authority_entries(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    authority_entries: &[KvEntry],
    repair_attempt_records: &[DfsRepairAttemptRecord],
    now_unix_nanos: u64,
) -> Result<DfsPendingMaintenanceWork, DfsError> {
    let mounted_mutation_work =
        crate::mounted_sifs_mutation_pending_maintenance_work(data_dir, now_unix_nanos)?;
    let due_repair_attempt_count = repair_attempt_records
        .iter()
        .filter(|record| {
            !crate::service_repair::repair_record_has_unidentified_producer(record)
                && record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= now_unix_nanos
        })
        .count();
    let next_repair_retry_unix_nanos = repair_attempt_records
        .iter()
        .filter(|record| {
            !crate::service_repair::repair_record_has_unidentified_producer(record)
                && record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos > now_unix_nanos
        })
        .map(|record| record.next_retry_unix_nanos)
        .min();
    let placement_prefix = crate::placement_reconciliations_prefix(&crate::sifs_file_system_id());
    let placement_queue_key =
        crate::control_plane::placement_reconciliation_queue_key(&crate::sifs_file_system_id());
    let has_placement_work = authority_entries
        .iter()
        .any(|entry| entry.key.starts_with(&placement_prefix) || entry.key == placement_queue_key);
    let membership_handoffs = authority_entries
        .iter()
        .filter(|entry| crate::dfs_metadata_shard_membership_handoff_key_is_exact(&entry.key))
        .map(|entry| entry.decode::<crate::DfsMetadataShardMembershipHandoff>())
        .collect::<Result<Vec<_>, _>>()?;
    let active_shards = authority_entries
        .iter()
        .filter(|entry| crate::dfs_metadata_shard_config_key_is_exact(&entry.key))
        .map(|entry| entry.decode::<crate::DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()?;
    // Match the placement executor's deterministic root-primary ownership.
    // A remote job is not local runnable work: otherwise every replica scans
    // its historical intent table repeatedly while the coordinator is busy.
    let placement_shards = active_shards
        .iter()
        .filter(|shard| shard.file_system_id == crate::sifs_file_system_id())
        .cloned()
        .collect::<Vec<_>>();
    let placement_is_local = has_placement_work
        && crate::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &crate::sifs_file_system_id(),
            &placement_shards,
            "/",
        )?
        .is_none_or(|shard| shard.primary_node_id == *local_node_id);
    let actionable_placement_reconciliation_count = if placement_is_local {
        crate::control_plane::placement_reconciliation_pending_work_from_authority_entries(
            &crate::sifs_file_system_id(),
            authority_entries,
            repair_attempt_records.len(),
        )?
    } else {
        0
    };
    let pending_local_membership_handoff_count = membership_handoffs
        .iter()
        .filter(|handoff| handoff.retiring_shard.primary_node_id == *local_node_id)
        .count();
    let completed_local_primary_handoff_rebind_count =
        dfs_completed_local_primary_handoff_rebind_count(
            data_dir,
            local_node_id,
            &membership_handoffs,
            &active_shards,
            authority_entries,
        )?;

    Ok(DfsPendingMaintenanceWork {
        pending_repair_count: repair_attempt_records.len(),
        repair_attempt_count: repair_attempt_records.len(),
        due_repair_attempt_count,
        next_repair_retry_unix_nanos,
        actionable_placement_reconciliation_count,
        pending_local_membership_handoff_count,
        completed_local_primary_handoff_rebind_count,
        deferred_work_count: mounted_mutation_work.deferred_work_count,
        mounted_mutation_live_count: mounted_mutation_work.mounted_mutation_live_count,
        mounted_mutation_recoverable_exact_count: mounted_mutation_work
            .mounted_mutation_recoverable_exact_count,
        mounted_mutation_recoverable_full_count: mounted_mutation_work
            .mounted_mutation_recoverable_full_count,
        mounted_mutation_overflow_count: mounted_mutation_work.mounted_mutation_overflow_count,
        next_mounted_mutation_reprobe_unix_nanos: mounted_mutation_work
            .next_mounted_mutation_reprobe_unix_nanos,
        ..DfsPendingMaintenanceWork::default()
    })
}

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
    default_dfs_content_root_catalog_store, dfs_message_sender_from_peer_transport,
    dfs_metadata_journal_batch_sender_from_peer_transport, dfs_metadata_replication_notice_lines,
    dfs_metadata_replication_sent_count, dfs_metadata_snapshot_install_sender_from_peer_transport,
    dfs_now_unix_nanos, dfs_path_safe_segment, discovered_cluster_dfs_peer_node_ids,
    distributed_sifs, open_cluster_dfs_service_with_message_sender,
    open_cluster_dfs_service_with_peer_transports_for_nodes,
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state,
    open_cluster_dfs_service_with_peer_transports_for_nodes_and_recovery_state_until_cancelled,
    publish_dfs_metadata_journal_with_host,
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
    mut control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
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
    let authoritative_topology = control_plane
        .as_mut()
        .map(|control_plane| {
            crate::local_state::DfsClusterTopologyProjection::read_authoritative(
                control_plane.as_mut(),
            )
        })
        .transpose()?;
    let metadata_shards = authoritative_topology
        .as_ref()
        .map(|topology| topology.metadata_shards(&file_system.file_system_id))
        .transpose()?
        .unwrap_or_default();
    let peer_node_ids = discovered_cluster_dfs_peer_node_ids(cluster_paths, &state)?;
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
        let topology = authoritative_topology
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message:
                    "destructive DFS content collection requires authoritative cluster topology"
                        .to_string(),
            })?;
        topology.require_destructive_authority()?;
        let expected_file_systems = topology.all_file_system_configs()?;
        let expected_shards = topology.all_metadata_shards()?;
        let pending_handoffs = topology.all_metadata_shard_membership_handoffs()?;
        let head_watermarks = topology.all_metadata_shard_head_watermarks()?;
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
    authoritative_prefix: crate::DfsAuthoritativePrefixProjection,
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

    let local_node_id = state.local_node_id.clone();
    let file_system = distributed_sifs(local_node_id.clone());
    let authoritative_topology =
        crate::local_state::DfsClusterTopologyProjection::from_authoritative_prefix(
            authoritative_prefix.clone(),
        )?;
    let metadata_shards = authoritative_topology.metadata_shards(&file_system.file_system_id)?;
    let peer_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)?;

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
        async_dfs_metadata_journal_batch_sender_from_peer_transport(
            &data_dir,
            Arc::clone(&peer_transport),
        ),
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
        Some(authoritative_prefix),
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
    authoritative_prefix: crate::DfsAuthoritativePrefixProjection,
    peer_transport: Arc<dyn AsyncDfsPeerTransport>,
    peer_node_ids: Vec<NodeId>,
    execution_exclusions: DfsMaintenanceExecutionExclusions,
    committed_work_tracker: DfsCommittedWorkTracker,
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
        return Err(DfsError::Metadata {
            message: "reactive DFS maintenance requires initialized cluster state".to_string(),
        });
    }

    service.set_committed_work_tracker(committed_work_tracker);
    let local_node_id = state.local_node_id.clone();
    let file_system = distributed_sifs(local_node_id.clone());
    let authoritative_topology =
        crate::local_state::DfsClusterTopologyProjection::from_authoritative_prefix(
            authoritative_prefix.clone(),
        )?;
    let metadata_shards = authoritative_topology.metadata_shards(&file_system.file_system_id)?;
    let mut maintenance_host = LocalDfsMaintenanceHost::new(data_dir.clone());
    let mut metadata_publisher_host = LocalDfsMetadataJournalPublisherHost::new(data_dir.clone());
    let mut metadata_transport = AsyncDfsMetadataNodeTransport::new(
        peer_node_ids,
        async_dfs_metadata_journal_batch_sender_from_peer_transport(
            &data_dir,
            Arc::clone(&peer_transport),
        ),
        async_dfs_metadata_snapshot_install_sender_from_peer_transport(peer_transport),
    );

    let result = run_dfs_maintenance_turn_with_async_host_and_execution_exclusions(
        service,
        &mut maintenance_host,
        &data_dir,
        file_system.file_system_id,
        &local_node_id,
        metadata_shards,
        &mut metadata_publisher_host,
        &mut metadata_transport,
        &execution_exclusions,
        Some(authoritative_prefix),
        options,
        true,
    )
    .await;
    if result.is_err()
        && let Some(
            crate::repair_queue_store::DfsRepairQueuePageReconcileOutcome::GenerationChanged {
                current_head_sequence,
            },
        ) = maintenance_host.repair_page_generation_change
    {
        // The failed exact checkpoint did not publish this pass's private calls.
        // Defer to a fresh actual page; no scan/completion count is fabricated.
        tracing::debug!(target: "sinomo.dfs.maintenance", current_head_sequence,
            "DFS maintenance deferred after its observed repair queue generation changed");
        return Ok(None);
    }
    result
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

    let state = sinomo_core::cluster::read_cluster_state_without_kv_async(runtime, &cluster_paths)
        .await
        .map_err(DfsError::from_network)?;
    let local_node_id = state.local_node_id.clone();
    let file_system = distributed_sifs(local_node_id.clone());
    let (control_plane, authoritative_topology) =
        read_authoritative_topology_on_blocking_lane(control_plane).await?;
    let metadata_shards = match authoritative_topology.as_ref() {
        Some(prefix) => {
            crate::local_state::DfsClusterTopologyProjection::from_authoritative_prefix(
                prefix.clone(),
            )?
            .metadata_shards(&file_system.file_system_id)?
        }
        None => Vec::new(),
    };
    let peer_node_ids = discovered_cluster_dfs_peer_node_ids(&cluster_paths, &state)?;
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
        async_dfs_metadata_journal_batch_sender_from_peer_transport(
            &data_dir,
            Arc::clone(&peer_transport),
        ),
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
        authoritative_topology,
        options,
    )
    .await
    .map(Some)
}

async fn read_authoritative_topology_on_blocking_lane(
    control_plane: Option<Box<dyn DfsControlPlaneKvClient>>,
) -> Result<
    (
        Option<Box<dyn DfsControlPlaneKvClient>>,
        Option<crate::DfsAuthoritativePrefixProjection>,
    ),
    DfsError,
> {
    let Some(mut control_plane) = control_plane else {
        return Ok((None, None));
    };
    let (control_plane, projection) = tokio::task::spawn_blocking(move || {
        let projection = control_plane.range_prefix_authoritative("/dfs/filesystems/");
        (control_plane, projection)
    })
    .await
    .map_err(|source| DfsError::Metadata {
        message: format!("authoritative DFS topology task failed: {source}"),
    })?;
    Ok((Some(control_plane), Some(projection?)))
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
        dfs_metadata_journal_batch_sender_from_peer_transport(
            data_dir,
            Arc::clone(&peer_transport),
        ),
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

pub const DEFAULT_DFS_WRITE_INTENT_MAINTENANCE_MAX_RECORDS: usize = 256;
pub const DEFAULT_DFS_PLACEMENT_RECONCILIATION_MAX_OBJECTS: usize = 256;
pub const DEFAULT_DFS_STALE_WRITE_INTENT_MIN_AGE: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_DFS_TOMBSTONE_COMPACTION_MIN_AGE: Duration =
    Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_DFS_TOMBSTONE_COMPACTION_MAX_RECORDS: u64 = 256;
pub const DEFAULT_DFS_METADATA_HISTORY_GC_MIN_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_DFS_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS: u64 = 0;
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

pub use sinomo_api::dfs::maintenance::DfsRepairAttemptRecord;

pub(crate) const DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY: &str =
    "repair execution blocked: target has no currently usable cluster route";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsWriteIntentMaintenanceReport {
    /// Physical scan work, including terminal and ownership-rejected records.
    pub visited_intent_record_count: usize,
    pub visited_intent_shard_count: usize,
    /// More observation pages remain; no claim that active recovery completed.
    pub scan_has_more: bool,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<crate::DfsMetadataRejectionCode>,
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
    repair_queue_observation: Option<crate::repair_queue_store::DfsRepairQueueObservation>,
    repair_page_observation: Option<crate::repair_queue_store::DfsRepairQueuePageObservation>,
    // Actual owner result only; it cannot acknowledge records or grant dispatch.
    repair_page_generation_change:
        Option<crate::repair_queue_store::DfsRepairQueuePageReconcileOutcome>,
}

impl LocalDfsMaintenanceHost {
    pub fn new(data_dir: SinomoDataDir) -> Self {
        let repair_queue_store = crate::repair_queue_store::LocalDfsRepairQueueStore::new(
            default_dfs_repair_attempt_snapshot_path(&data_dir),
        );
        Self {
            data_dir,
            repair_queue_store,
            repair_queue_observation: None,
            repair_page_observation: None,
            repair_page_generation_change: None,
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

    fn load_repair_maintenance_page(
        &mut self,
        max_records: usize,
    ) -> Result<crate::DfsRepairQueueMaintenancePage, DfsError> {
        let (page, observed) = self
            .repair_queue_store
            .maintenance_page(max_records, &[], None)?;
        self.repair_page_observation = Some(observed);
        Ok(page)
    }

    fn enroll_repair_maintenance_page(
        &mut self,
        _page: &crate::DfsRepairQueueMaintenancePage,
        max_records: usize,
        captured: &[DfsRepairAttemptRecord],
    ) -> Result<crate::DfsRepairQueueMaintenancePage, DfsError> {
        let expected = self
            .repair_page_observation
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "repair page enrollment has no observation".into(),
            })?;
        let (page, observed) =
            self.repair_queue_store
                .maintenance_page(max_records, captured, Some(expected))?;
        self.repair_page_observation = Some(observed);
        Ok(page)
    }

    fn checkpoint_repair_maintenance_page(
        &mut self,
        page: &crate::DfsRepairQueueMaintenancePage,
        examined: &BTreeSet<(String, NodeId)>,
        desired: &[DfsRepairAttemptRecord],
        retired: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairMaintenancePageCheckpoint, DfsError> {
        self.repair_page_generation_change = None;
        let expected = self
            .repair_page_observation
            .as_ref()
            .ok_or_else(|| DfsError::Metadata {
                message: "repair page checkpoint has no observation".into(),
            })?;
        let examined_prefix = page
            .records
            .iter()
            .take_while(|record| {
                examined.contains(&(
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                ))
            })
            .count();
        let reconciled = page
            .records
            .iter()
            .chain(&page.enrollment_records)
            .filter(|record| {
                examined.contains(&(
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                ))
            })
            .cloned()
            .collect::<Vec<_>>();
        let result = self.repair_queue_store.reconcile_maintenance_page(
            expected,
            examined_prefix.min(expected.record_count()),
            &reconciled,
            desired,
            retired,
            &page.enrollment_records,
        )?;
        match result {
            crate::repair_queue_store::DfsRepairQueuePageReconcileOutcome::Applied {
                removed_target_count,
                operation_count,
                has_more,
                ..
            } => {
                self.repair_queue_store.compact_if_needed(
                    crate::repair_queue_store::DfsRepairQueueCompactionOptions::default(),
                )?;
                self.repair_page_observation = None;
                Ok(DfsRepairMaintenancePageCheckpoint {
                    examined_record_count: examined.len(),
                    completed_record_count: examined.len(),
                    removed_target_count,
                    operation_count,
                    has_more,
                })
            }
            outcome @ crate::repair_queue_store::DfsRepairQueuePageReconcileOutcome::GenerationChanged {
                current_head_sequence,
            } => {
                self.repair_page_generation_change = Some(outcome);
                Err(DfsError::Metadata {
                    message: format!(
                        "repair queue generation changed at page checkpoint (head {current_head_sequence})"
                    ),
                })
            }
        }
    }

    fn load_repair_attempt_records(&mut self) -> Result<Vec<DfsRepairAttemptRecord>, DfsError> {
        let (view, observation) = self.repair_queue_store.view_for_maintenance()?;
        self.repair_queue_observation = Some(observation);
        Ok(view.records)
    }

    fn store_repair_attempt_records(
        &mut self,
        records: &[DfsRepairAttemptRecord],
    ) -> Result<(), DfsError> {
        let observation = match self.repair_queue_observation.clone() {
            Some(observation) => observation,
            None => self.repair_queue_store.view_for_maintenance()?.1,
        };
        match self.repair_queue_store.reconcile_observed(&observation, records)? {
            crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::Applied(updated) => {
                self.repair_queue_observation = Some(updated);
                self.repair_queue_store.compact_if_needed(
                    crate::repair_queue_store::DfsRepairQueueCompactionOptions::default(),
                )?;
                Ok(())
            }
            crate::repair_queue_store::DfsRepairQueueObservedReconcileOutcome::GenerationChanged { current_head_sequence } => {
                // Keep the stale token until load is repeated. Merely learning
                // the new head must not authorize a stale desired record set.
                Err(DfsError::Metadata {
                    message: format!("DFS repair queue changed during maintenance: expected sequence {}, found {current_head_sequence}", observation.head_sequence),
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
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(DfsError::Metadata {
            message: format!(
                "could not read DFS anti-entropy cursor {}: {source}",
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
    Ok(())
}

pub fn clear_dfs_anti_entropy_cursor(path: &Path) -> Result<(), DfsError> {
    clear_dfs_cursor_file(path)
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

    fn load_repair_maintenance_page(
        &mut self,
        _max_records: usize,
    ) -> Result<crate::DfsRepairQueueMaintenancePage, DfsError>;

    fn enroll_repair_maintenance_page(
        &mut self,
        page: &crate::DfsRepairQueueMaintenancePage,
        _max_records: usize,
        captured: &[DfsRepairAttemptRecord],
    ) -> Result<crate::DfsRepairQueueMaintenancePage, DfsError>;

    fn checkpoint_repair_maintenance_page(
        &mut self,
        _page: &crate::DfsRepairQueueMaintenancePage,
        _examined: &BTreeSet<(String, NodeId)>,
        _desired: &[DfsRepairAttemptRecord],
        _retired: &[DfsRepairAttemptRecord],
    ) -> Result<DfsRepairMaintenancePageCheckpoint, DfsError>;

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
    let catalog_generation = validation.catalog_generation().map(str::to_owned);
    let scan_cut = validation.catalog_scan_cut().cloned();
    let version_gc = options.collect_content_versions.then(|| {
        let options = ContentStoreVersionGcOptions::delete_eligible(
            options.content_version_gc_min_age,
            options.content_version_gc_keep_latest_versions,
        )
        .with_max_deletions(options.content_version_gc_max_records)
        .with_min_absence_age(options.content_version_gc_min_age);
        match catalog_generation.as_ref().zip(scan_cut.as_ref()) {
            Some((generation, cut)) => {
                options.with_catalog_observation(generation.clone(), cut.started_at_unix_nanos())
            }
            None => options,
        }
    });
    let orphan_gc = options.collect_orphan_content.then(|| {
        let options = ContentStoreGcOptions::delete_eligible(options.content_orphan_gc_min_age)
            .with_max_deletions(options.content_orphan_gc_max_records);
        match catalog_generation.as_ref().zip(scan_cut.as_ref()) {
            Some((generation, cut)) => {
                options.with_catalog_observation(generation.clone(), cut.started_at_unix_nanos())
            }
            None => options,
        }
    });
    let reconcile = options.reconcile_content_reservations
        || options.collect_content_versions
        || options.collect_orphan_content;
    let content_reservation_reconciliation = reconcile
        .then(|| {
            catalog_generation.map(|generation| {
                ContentStoreReservationReconciliationOptions::bounded(
                    scan_cut
                        .as_ref()
                        .map_or(0, |cut| cut.started_at_unix_nanos()),
                    options.content_reservation_rejection_grace,
                    options.content_reservation_reconciliation_max_records,
                )
                .with_catalog_generation(generation)
                .with_catalog_scan_cut(scan_cut)
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
/// Required metadata propagation uses the ordinary bounded transport even
/// under these advisory hints; only idle metadata verification is parked.
/// An attempted send never substitutes for the receiver's validated ACK.
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
            || record.in_flight
            || record.transfer_request_id.is_some()
        {
            continue;
        }
        record.in_flight = false;
        record.next_retry_unix_nanos = u64::MAX;
        record.transfer_request_id = None;
        record.last_error = Some(DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY.to_string());
    }
}

fn dfs_repair_attempt_is_eligible_without_execution_projection(
    record: &DfsRepairAttemptRecord,
) -> bool {
    // The synchronous maintenance API has no current connectivity projection
    // with which to prove that a durable blocker cleared. A MAX deadline is
    // the explicit parked/suppressed marker, so it cannot contribute to the
    // bounded executable queue until a projection-aware path admits it.
    record.next_retry_unix_nanos != u64::MAX
}

fn dfs_repair_maintenance_budget_exhausted(
    repair_max_jobs: usize,
    eligible_repair_count_before: usize,
    scheduled_repair_count: usize,
    durable_repair_queue_after: &[DfsRepairAttemptRecord],
    now_unix_nanos: u64,
) -> bool {
    repair_max_jobs > 0
        && repair_max_jobs != usize::MAX
        && eligible_repair_count_before >= repair_max_jobs
        && scheduled_repair_count > 0
        && durable_repair_queue_after.iter().any(|record| {
            !crate::service_repair::repair_record_has_unidentified_producer(record)
                && record.next_retry_unix_nanos != u64::MAX
                && record.next_retry_unix_nanos <= now_unix_nanos
        })
}

#[cfg(test)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsRepairMaintenancePageCheckpoint {
    pub examined_record_count: usize,
    /// Keys whose scan/execution outcome was acknowledged by the queue owner.
    pub completed_record_count: usize,
    pub removed_target_count: usize,
    pub operation_count: usize,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsMaintenanceTickReport {
    pub namespace_jobs: DfsNamespaceJobMaintenanceReport,
    pub write_intents: DfsWriteIntentMaintenanceReport,
    pub placement_reconciliation: Option<DfsPlacementReconciliationJob>,
    pub lifecycle_reports: Vec<DfsLifecycleMaintenanceReport>,
    pub scheduled_repairs: Vec<DistributedRepairJob>,
    pub followup_events: Vec<NetworkedDfsServiceEvent>,
    pub repair_attempts_before: Vec<DfsRepairAttemptRecord>,
    pub repair_attempts_after: Vec<DfsRepairAttemptRecord>,
    /// This bounded pass scheduled at least one repair and stopped while
    /// another repair record was immediately runnable. The daemon may continue
    /// without its ordinary retry gate.
    pub repair_budget_exhausted: bool,
    /// Set only after the existing queue owner acknowledges this page.
    pub repair_page_checkpoint: DfsRepairMaintenancePageCheckpoint,
    pub metadata_replication: DfsMetadataJournalReplicationReport,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DfsMaintenanceTickObservations {
    pub metadata_replications_sent: usize,
    pub repair_attempt_count: usize,
    pub repair_attempt_in_flight_count: usize,
    pub repair_attempt_backoff_count: usize,
    pub repair_attempt_suppressed_count: usize,
    /// Informational repair summaries retained by the ordinary in-memory log sink.
    pub routine_notice_lines: Vec<String>,
    pub notice_lines: Vec<String>,
}

impl DfsMaintenanceTickObservations {
    /// Emit the daemon notices while retaining the caller's trace context.
    #[doc(hidden)]
    pub fn emit_daemon_notice_logs(&self) {
        for line in &self.routine_notice_lines {
            tracing::info!(target: "sinomo_dfs::runtime_host::maintenance_runtime", "sinomo daemon {line}");
        }
        for line in &self.notice_lines {
            tracing::warn!(target: "sinomo_dfs::runtime_host::maintenance_runtime", "sinomo daemon {line}");
        }
    }
}

pub fn dfs_maintenance_tick_observations(
    report: &DfsMaintenanceTickReport,
) -> DfsMaintenanceTickObservations {
    let mut notice_lines = dfs_namespace_job_maintenance_notice_lines(&report.namespace_jobs);
    notice_lines.extend(dfs_write_intent_maintenance_notice_lines(
        &report.write_intents,
    ));
    notice_lines.extend(dfs_lifecycle_maintenance_notice_lines(
        &report.lifecycle_reports,
    ));
    let (routine_notice_lines, repair_notices) = dfs_repair_maintenance_observation_lines(
        &report.scheduled_repairs,
        &report.followup_events,
        &report.repair_attempts_after,
    );
    notice_lines.extend(repair_notices);
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
        routine_notice_lines,
        notice_lines,
    }
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
                "DFS metadata history GC: applied={} versions={} replica_receipts={} intent_bodies={} intent_receipts={} prepared_operations={} namespace_bodies={} namespace_receipts={} dry_run={} max_removals_reached={}",
                metadata_gc.applied_record_count(),
                metadata_gc.removed_version_count,
                metadata_gc.removed_receipt_count,
                metadata_gc.compacted_intent_count,
                metadata_gc.removed_intent_receipt_count,
                metadata_gc.removed_prepared_operation_count,
                metadata_gc
                    .compacted_namespace_job_count
                    .saturating_add(metadata_gc.compacted_namespace_request_count),
                metadata_gc
                    .removed_namespace_job_receipt_count
                    .saturating_add(metadata_gc.removed_namespace_transaction_receipt_count)
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
    let (mut lines, events) = dfs_repair_maintenance_observation_lines(
        scheduled_repairs, followup_events, repair_attempts,
    );
    lines.extend(events);
    lines
}

fn dfs_repair_maintenance_observation_lines(
    scheduled_repairs: &[DistributedRepairJob],
    followup_events: &[NetworkedDfsServiceEvent],
    repair_attempts: &[DfsRepairAttemptRecord],
) -> (Vec<String>, Vec<String>) {
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
        return (Vec::new(), Vec::new());
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
    let routine_lines = lines;
    let mut lines = Vec::new();
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
    (routine_lines, lines)
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
    if service.remote_namespace_phase.is_some() {
        return Err(DfsError::Metadata {
            message: "owned namespace recovery requires its asynchronous host continuation".into(),
        });
    }
    let namespace_jobs = service
        .posix_resume_namespace_jobs_bounded(
            namespace_job_max_jobs,
            options.namespace_job_max_steps_per_job,
        )
        .map_err(|error| DfsError::Metadata {
            message: error.to_string(),
        })?;
    let write_intents = service.reconcile_stale_write_intents_bounded(
        options.write_intent_max_records,
        options.stale_write_intent_min_age,
    )?;
    // One snapshot from the durable repair owner supplies both placement
    // eligibility and the existing later repair phase under this same service
    // admission. Placement/lifecycle do not persist attempt records.
    let durable_repair_queue_before = maintenance_host.load_repair_attempt_records()?;
    let placement_reconciliation = if options.placement_reconciliation_max_objects == 0 {
        None
    } else {
        service.run_placement_reconciliation_bounded_with_pending_repairs(
            options.placement_reconciliation_max_objects,
            durable_repair_queue_before.len(),
        )?
    };
    let mut lifecycle_reports = run_dfs_lifecycle_maintenance_with_host(
        service,
        maintenance_host,
        dfs_metadata_lifecycle_options(options.lifecycle),
    )?;
    if let Some(content_gc) = run_dfs_content_gc_without_catalog(service, options.lifecycle)? {
        lifecycle_reports.push(content_gc);
    }
    let eligible_repair_count_before = durable_repair_queue_before
        .iter()
        .filter(|record| dfs_repair_attempt_is_eligible_without_execution_projection(record))
        .count();
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
    let repair_budget_exhausted = dfs_repair_maintenance_budget_exhausted(
        options.repair_max_jobs,
        eligible_repair_count_before,
        scheduled_repairs.len(),
        &durable_repair_queue_after,
        dfs_now_unix_nanos(),
    );
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
        write_intents,
        placement_reconciliation,
        lifecycle_reports,
        scheduled_repairs,
        followup_events,
        repair_attempts_before,
        repair_attempts_after,
        repair_budget_exhausted,
        repair_page_checkpoint: DfsRepairMaintenancePageCheckpoint::default(),
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
        None,
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
    authoritative_prefix: Option<crate::DfsAuthoritativePrefixProjection>,
    options: DfsMaintenanceTickOptions,
) -> Result<DfsMaintenanceTickReport, DfsError>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    run_dfs_maintenance_turn_with_async_host_and_execution_exclusions(
        service,
        maintenance_host,
        data_dir,
        file_system_id,
        local_node_id,
        metadata_shards,
        metadata_publisher_host,
        metadata_transport,
        execution_exclusions,
        authoritative_prefix,
        options,
        false,
    )
    .await?
    .ok_or_else(|| DfsError::Metadata {
        message: "full maintenance entry unexpectedly yielded a partial turn".into(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_dfs_maintenance_turn_with_async_host_and_execution_exclusions<T>(
    service: &mut NetworkedDfsService<T>,
    maintenance_host: &mut dyn DfsMaintenanceHost,
    data_dir: &SinomoDataDir,
    file_system_id: DfsFileSystemId,
    local_node_id: &NodeId,
    metadata_shards: Vec<DfsMetadataShardConfig>,
    metadata_publisher_host: &mut impl DfsMetadataJournalPublisherHost,
    metadata_transport: &mut dyn AsyncDfsMetadataJournalReplicationTransport,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    authoritative_prefix: Option<crate::DfsAuthoritativePrefixProjection>,
    options: DfsMaintenanceTickOptions,
    fair_resume: bool,
) -> Result<Option<DfsMaintenanceTickReport>, DfsError>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    let progress_owner = Arc::clone(&service.maintenance_progress);
    let result = async {
        use progress::Phase;
        let mut turn = service.maintenance_progress.begin(progress::Scope {
            file_system_id: file_system_id.clone(),
            node: local_node_id.clone(),
            shards: metadata_shards.clone(),
            options,
            projection_revision: authoritative_prefix
                .as_ref()
                .map(|prefix| prefix.revision()),
        })?;
        service.coordinator.ensure_message_repair_job_capture();
        let progress_owner = Arc::clone(&service.maintenance_progress);
        // Retry unacknowledged producer data before authoring another phase. The
        // same queue writer validates/merges these rows; they carry no authority.
        progress_owner.retry_births(maintenance_host)?;
        let authoritative_topology = authoritative_prefix
            .map(crate::local_state::DfsClusterTopologyProjection::from_authoritative_prefix)
            .transpose()?;
        if fair_resume && turn.needs_repair_turn() {
            let Some(repair) = run_maintenance_repair_phase(
                service,
                maintenance_host,
                execution_exclusions,
                options,
                false,
                fair_resume,
            )
            .await?
            else {
                return Ok(None);
            };
            repair.apply(turn.report());
            turn.defer_after_repair();
            return Ok(None);
        }
        // Persist phase position before each cooperative yield. Native handoff and
        // publication checks still run under the current admitted generation.
        if turn.phase() == Phase::Handoff {
            service.reconcile_pending_metadata_shard_membership_handoffs(
                &metadata_shards,
                options.metadata_replication.target_timeout,
            )?;
            turn.advance(Phase::NamespaceRequests);
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::NamespaceRequests {
            service.recover_pending_namespace_requests_bounded(
                crate::service_routed_namespace::DFS_NAMESPACE_REQUEST_RECOVERY_BATCH_LIMIT,
            )?;
            if service.remote_namespace_phase.is_some() {
                return Err(DfsError::Metadata {
                    message: "owned namespace recovery requires its asynchronous host continuation"
                        .into(),
                });
            }
            turn.advance(Phase::NamespaceJobs);
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::NamespaceJobs {
            let max_jobs = if metadata_shards.is_empty() {
                0
            } else {
                options.namespace_job_max_jobs
            };
            turn.report().namespace_jobs = service
                .posix_resume_namespace_jobs_bounded(
                    max_jobs,
                    options.namespace_job_max_steps_per_job,
                )
                .map_err(|error| DfsError::Metadata {
                    message: error.to_string(),
                })?;
            turn.advance(Phase::WriteIntents);
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::WriteIntents {
            turn.report().write_intents = service.reconcile_stale_write_intents_bounded(
                options.write_intent_max_records,
                options.stale_write_intent_min_age,
            )?;
            turn.advance(Phase::Placement);
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::Placement {
            let initial_repair_page = maintenance_host.load_repair_maintenance_page(0)?;
            tokio::task::yield_now().await;
            turn.report().placement_reconciliation =
                if options.placement_reconciliation_max_objects == 0 {
                    None
                } else {
                    service
                        .run_placement_reconciliation_bounded_async_with_pending_repairs(
                            options.placement_reconciliation_max_objects,
                            initial_repair_page.total_record_count,
                            &metadata_shards,
                        )
                        .await?
                };
            progress_owner.checkpoint_births(service, maintenance_host)?;
            turn.advance(Phase::MetadataLifecycle);
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::MetadataLifecycle {
            turn.report().lifecycle_reports = run_dfs_lifecycle_maintenance_with_host(
                service,
                maintenance_host,
                dfs_metadata_lifecycle_options(options.lifecycle),
            )?;
            progress_owner.checkpoint_births(service, maintenance_host)?;
            turn.advance(Phase::ContentLifecycle);
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::ContentLifecycle {
            if dfs_content_gc_requested(options.lifecycle) {
                let topology = authoritative_topology.as_ref().ok_or_else(|| {
                    DfsError::Metadata {
                    message:
                        "destructive DFS content collection requires authoritative cluster topology"
                            .into(),
                }
                })?;
                topology.require_destructive_authority()?;
                let expected_file_systems = topology.all_file_system_configs()?;
                let expected_shards = topology.all_metadata_shards()?;
                let pending_handoffs = topology.all_metadata_shard_membership_handoffs()?;
                let head_watermarks = topology.all_metadata_shard_head_watermarks()?;
                if let Some(content_gc) = run_dfs_content_gc_with_catalog(
                    service,
                    &data_dir.dfs_dir().join("content"),
                    &expected_file_systems,
                    &expected_shards,
                    &pending_handoffs,
                    &head_watermarks,
                    options.lifecycle,
                )? {
                    turn.report().lifecycle_reports.push(content_gc);
                }
            }
            progress_owner.checkpoint_births(service, maintenance_host)?;
            turn.advance(Phase::Repair);
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::Repair {
            let Some(repair) = run_maintenance_repair_phase(
                service,
                maintenance_host,
                execution_exclusions,
                options,
                true,
                fair_resume,
            )
            .await?
            else {
                return Ok(None);
            };
            repair.apply(turn.report());
            turn.advance(Phase::Publish);
            tokio::task::yield_now().await;
        }
        if turn.phase() == Phase::Publish {
            turn.report().metadata_replication = if options.publish_metadata {
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
            turn.advance(Phase::Complete);
        }
        turn.finish().map(Some)
    }
    .await;
    if result.is_err() {
        progress_owner.restart_after_error()?;
    }
    result
}

struct RepairPhaseReport {
    scheduled_repairs: Vec<DistributedRepairJob>,
    followup_events: Vec<NetworkedDfsServiceEvent>,
    repair_attempts_before: Vec<DfsRepairAttemptRecord>,
    repair_attempts_after: Vec<DfsRepairAttemptRecord>,
    repair_budget_exhausted: bool,
    repair_page_checkpoint: DfsRepairMaintenancePageCheckpoint,
}
impl RepairPhaseReport {
    fn apply(self, report: &mut DfsMaintenanceTickReport) {
        report.scheduled_repairs = self.scheduled_repairs;
        report.followup_events = self.followup_events;
        report.repair_attempts_before = self.repair_attempts_before;
        report.repair_attempts_after = self.repair_attempts_after;
        report.repair_budget_exhausted = self.repair_budget_exhausted;
        report.repair_page_checkpoint = self.repair_page_checkpoint;
    }
}
async fn run_maintenance_repair_phase<T>(
    service: &mut NetworkedDfsService<T>,
    maintenance_host: &mut dyn DfsMaintenanceHost,
    execution_exclusions: &DfsMaintenanceExecutionExclusions,
    options: DfsMaintenanceTickOptions,
    yield_before_followup: bool,
    yield_on_source_fence: bool,
) -> Result<Option<RepairPhaseReport>, DfsError>
where
    T: AsyncDfsMessageTransport + DfsTransportIdentity + DfsMessageTransport,
{
    let repair_page_limit = options
        .repair_max_jobs
        .min(crate::service_repair::DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE);
    // Enrollment alone proposes rows; it does not persist them. Keep producer
    // captures in the existing progress owner until the ordinary queue writer
    // acknowledges them, before any source read can yield or return an error.
    let progress_owner = Arc::clone(&service.maintenance_progress);
    progress_owner.checkpoint_births(service, maintenance_host)?;
    // Observe the same bounded execution page after that durable checkpoint.
    // Pending source admission cannot acknowledge or advance its scan cursor.
    let mut page = maintenance_host.load_repair_maintenance_page(repair_page_limit)?;
    // Own scheduling Set changes the durable queue generation. Capture a
    // current SAME bounded page before executing it, rather than retaining the
    // pre-wake observation and rejecting our own later checkpoint as stale.
    let mut removed_wake_keys = BTreeSet::new();
    if let Some(removed) =
        service.wake_connectivity_blocked_repair_records(&page.records, execution_exclusions)?
    {
        removed_wake_keys = removed;
        let captures = page
            .enrollment_records
            .iter()
            .filter(|row| {
                !removed_wake_keys
                    .contains(&(row.intent_id.clone(), row.missing_replica_node_id.clone()))
            })
            .cloned()
            .collect::<Vec<_>>();
        let current_page = maintenance_host.load_repair_maintenance_page(repair_page_limit)?;
        page = maintenance_host.enroll_repair_maintenance_page(
            &current_page,
            repair_page_limit,
            &captures,
        )?;
    }
    let (
        scheduled_repairs,
        followup_events,
        repair_attempts_before,
        repair_attempts_after,
        repair_budget_exhausted,
        repair_page_checkpoint,
    ) = {
        let repair_execution_exclusions =
            execution_exclusions.allowing_unattempted_repairs(&page.records);
        let mut scoped_targets = BTreeSet::new();
        let mut eligible = Vec::new();
        let mut parked = Vec::new();
        for record in page.records.iter().take(repair_page_limit) {
            let key = (
                record.intent_id.clone(),
                record.missing_replica_node_id.clone(),
            );
            if repair_execution_exclusions.contains(&record.missing_replica_node_id) {
                parked.push(record.clone());
            } else {
                scoped_targets.insert(key);
                eligible.push(record.clone());
            }
        }
        let mut progress = service
            .retry_pending_repair_page_with_resident_yield(
                options.repair_max_jobs,
                &eligible,
                &repair_execution_exclusions,
                Some(&scoped_targets),
                Some(&removed_wake_keys),
                true,
                yield_on_source_fence,
            )
            .await?;
        if progress.source_read_pending {
            // Earlier rows may have restored actual producer work before a
            // later source was busy. Retain those captures across cancellation
            // and generation retirement without claiming any page completion.
            progress_owner.checkpoint_births(service, maintenance_host)?;
            return Ok(None);
        }
        progress.examined.extend(parked.iter().map(|record| {
            (
                record.intent_id.clone(),
                record.missing_replica_node_id.clone(),
            )
        }));
        // Replacement/stage completion may author new repair identities while
        // processing this page. Enroll that exact capture in the SAME observed
        // checkpoint before cancellation can destroy the service generation.
        let repair_captures = service
            .take_message_committed_repair_queue_records()
            .into_iter()
            .filter(|row| {
                !removed_wake_keys
                    .contains(&(row.intent_id.clone(), row.missing_replica_node_id.clone()))
            })
            .collect::<Vec<_>>();
        service.coordinator.ensure_message_repair_job_capture();
        if !repair_captures.is_empty() {
            let mut captures = page.enrollment_records.clone();
            captures.extend(repair_captures);
            page = maintenance_host.enroll_repair_maintenance_page(
                &page,
                repair_page_limit,
                &captures,
            )?;
        }
        let deferred_repair_error = progress.error;
        let scheduled_repairs = progress.scheduled;
        let preserved_parked = page
            .records
            .iter()
            .filter(|record| {
                progress.parked.contains(&(
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                )) || crate::service_repair::repair_record_has_unidentified_producer(record)
            })
            .cloned()
            .collect::<Vec<_>>();
        let preserved_keys = preserved_parked
            .iter()
            .map(|record| {
                (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                )
            })
            .collect::<BTreeSet<_>>();
        let mut durable_after =
            service.repair_queue_records_for_targets(true, Some(&scoped_targets));
        durable_after.extend(parked.iter().cloned());
        durable_after.retain(|record| {
            !preserved_keys.contains(&(
                record.intent_id.clone(),
                record.missing_replica_node_id.clone(),
            ))
        });
        mark_connectivity_blocked_dfs_repair_attempt_records(
            &mut durable_after,
            execution_exclusions,
        );
        durable_after.extend(preserved_parked.iter().cloned());
        let mut repair_page_checkpoint = maintenance_host.checkpoint_repair_maintenance_page(
            &page,
            &progress.examined,
            &durable_after,
            &progress.retired_intents,
        )?;
        repair_page_checkpoint.examined_record_count =
            progress.examined_record_count + parked.len();
        // Durable attempt/retirement checkpoint precedes both payload handoff and
        // cancellation. No stale-only group can disappear with its service future.
        service
            .transport
            .checkpoint_background_replica_dispatch()
            .map_err(DfsError::from_network)?;
        if let Some(error) = deferred_repair_error {
            return Err(error);
        }
        if yield_before_followup {
            tokio::task::yield_now().await;
        }
        let mut followup_keys = scoped_targets.clone();
        followup_keys.retain(|key| !preserved_keys.contains(key));
        followup_keys.extend(
            page.enrollment_records
                .iter()
                .filter(|record| {
                    !preserved_keys.contains(&(
                        record.intent_id.clone(),
                        record.missing_replica_node_id.clone(),
                    ))
                })
                .map(|record| {
                    (
                        record.intent_id.clone(),
                        record.missing_replica_node_id.clone(),
                    )
                }),
        );
        let followup_before = service.repair_queue_records_for_targets(true, Some(&followup_keys));
        let followup_events = service.process_incoming_async_until_quiescent().await?;
        service.observe_repair_maintenance_events(&followup_events);
        let followup_captures = service
            .take_message_committed_repair_queue_records()
            .into_iter()
            .filter(|record| {
                !preserved_keys.contains(&(
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                ))
            })
            .collect::<Vec<_>>();
        service.coordinator.ensure_message_repair_job_capture();
        if !followup_events.is_empty() || !followup_captures.is_empty() {
            followup_keys.extend(followup_captures.iter().map(|record| {
                (
                    record.intent_id.clone(),
                    record.missing_replica_node_id.clone(),
                )
            }));
            let followup_page = maintenance_host.load_repair_maintenance_page(0)?;
            let followup_after =
                service.repair_queue_records_for_targets(true, Some(&followup_keys));
            let captures = followup_before
                .iter()
                .chain(&followup_after)
                .chain(&followup_captures)
                .cloned()
                .collect::<Vec<_>>();
            let followup_page =
                maintenance_host.enroll_repair_maintenance_page(&followup_page, 0, &captures)?;
            let keys = followup_before
                .iter()
                .map(|record| {
                    (
                        record.intent_id.clone(),
                        record.missing_replica_node_id.clone(),
                    )
                })
                .collect();
            let checkpoint = maintenance_host.checkpoint_repair_maintenance_page(
                &followup_page,
                &keys,
                &followup_after,
                &[],
            )?;
            repair_page_checkpoint.removed_target_count += checkpoint.removed_target_count;
            repair_page_checkpoint.operation_count += checkpoint.operation_count;
            repair_page_checkpoint.has_more |= checkpoint.has_more;
        }
        let repair_budget_exhausted = options.repair_max_jobs > 0
            && repair_page_checkpoint.has_more
            && (repair_page_checkpoint.completed_record_count > 0
                || repair_page_checkpoint.operation_count > 0);
        let repair_attempts_before = page
            .records
            .into_iter()
            .filter(|record| record.attempts > 0)
            .collect();
        let mut repair_attempts_after =
            service.repair_queue_records_for_targets(false, Some(&scoped_targets));
        repair_attempts_after.extend(parked.into_iter().filter(|record| record.attempts > 0));
        repair_attempts_after.retain(|record| {
            !preserved_keys.contains(&(
                record.intent_id.clone(),
                record.missing_replica_node_id.clone(),
            ))
        });
        mark_connectivity_blocked_dfs_repair_attempt_records(
            &mut repair_attempts_after,
            execution_exclusions,
        );
        repair_attempts_after.extend(
            preserved_parked
                .into_iter()
                .filter(|record| record.attempts > 0),
        );
        (
            scheduled_repairs,
            followup_events,
            repair_attempts_before,
            repair_attempts_after,
            repair_budget_exhausted,
            repair_page_checkpoint,
        )
    };
    Ok(Some(RepairPhaseReport {
        scheduled_repairs,
        followup_events,
        repair_attempts_before,
        repair_attempts_after,
        repair_budget_exhausted,
        repair_page_checkpoint,
    }))
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
            transfer_delivery_state: None,
            completed_transfer_request_id: None,
            transfer_kind: None,
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
    fn sync_repair_budget_excludes_durably_parked_records() {
        let (runtime, mut writer, shard) = crate::test_support::current_native_metadata_service(
            "native-sync-repair-budget",
            3,
            "/",
        )
        .unwrap();
        let owner = shard.primary_node_id.clone();
        let nodes = std::iter::once(owner.clone())
            .chain(shard.replica_node_ids.iter().cloned())
            .collect::<Vec<_>>();
        for target in &shard.replica_node_ids {
            for route in [
                crate::DFS_DATA_STAGE_BLOB_ROUTE,
                crate::DFS_DATA_REPAIR_REPLICA_ROUTE,
            ] {
                runtime.install_fault(crate::test_runtime::FaultRule {
                    source: None,
                    target: Some(target.clone()),
                    route: Some(route.into()),
                    call_id: None,
                    origin: None,
                    remaining_hits: 32,
                    action: crate::test_runtime::FaultAction::BudgetExhausted,
                });
            }
        }
        let stat = writer
            .posix_write_file(
                "/native-sync-budget",
                b"actual budget accounting inputs",
                crate::DfsWriteOptions {
                    policy: crate::DfsReplicationPolicy::new(1, 3).unwrap(),
                    replica_candidates: nodes,
                    wait_for_commit: true,
                    foreground_replica_repair: true,
                },
            )
            .unwrap();
        drop(writer);
        let queue = crate::LocalDfsRepairQueueStore::new(
            crate::default_dfs_repair_attempt_snapshot_path(runtime.data_dir(&owner)),
        );
        let deadline = std::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let originals = loop {
            let records = queue
                .view()
                .unwrap()
                .records
                .into_iter()
                .filter(|record| {
                    record.object_id == stat.object_id && record.version == stat.version
                })
                .collect::<Vec<_>>();
            if records.len() == 2
                && records.iter().all(|record| {
                    record.attempts > 0
                        && !record.in_flight
                        && !crate::service_repair::repair_record_has_unidentified_producer(record)
                        && record.transfer_delivery_state
                            == Some(sinomo_core::network::NetworkDeliveryState::DefinitelyNotSent)
                        && record.next_retry_unix_nanos != u64::MAX
                })
            {
                break records;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "actual failed transfers must checkpoint their producer facts: {records:?}"
            );
            runtime
                .maintenance_tick(&owner, remaining)
                .unwrap()
                .outcome
                .unwrap();
        };
        // These are read-only classifier inputs from actual failed native sends.
        // Evaluate at their recorded due time without advancing a runtime clock
        // or modifying the live durable queue. Only the second copied row parks.
        let now = originals
            .iter()
            .map(|record| record.next_retry_unix_nanos)
            .max()
            .unwrap();
        let runnable = originals[0].clone();
        let mut parked = originals[1].clone();
        let exclusions =
            DfsMaintenanceExecutionExclusions::new([parked.missing_replica_node_id.clone()]);
        mark_connectivity_blocked_dfs_repair_attempt_records(
            std::slice::from_mut(&mut parked),
            &exclusions,
        );
        assert_eq!(parked.next_retry_unix_nanos, u64::MAX);
        assert_eq!(
            parked.last_error.as_deref(),
            Some(DFS_REPAIR_EXECUTION_BLOCKED_BY_CONNECTIVITY)
        );
        let queue_before = [runnable.clone(), parked];
        let eligible_count = queue_before
            .iter()
            .filter(|record| dfs_repair_attempt_is_eligible_without_execution_projection(record))
            .count();

        assert_eq!(eligible_count, 1);
        assert!(
            !dfs_repair_maintenance_budget_exhausted(
                2,
                eligible_count,
                1,
                std::slice::from_ref(&runnable),
                now,
            ),
            "parked work cannot make a partially used sync budget look exhausted"
        );
        assert!(
            dfs_repair_maintenance_budget_exhausted(
                1,
                eligible_count,
                1,
                std::slice::from_ref(&runnable),
                now,
            ),
            "the same queue must report exhaustion when its executable work fills the budget"
        );
        let mut unidentified = runnable.clone();
        unidentified.transfer_kind = None;
        unidentified.transfer_delivery_state = None;
        unidentified.transfer_request_id = None;
        unidentified.completed_transfer_request_id = None;
        assert!(
            !dfs_repair_maintenance_budget_exhausted(1, 1, 1, &[unidentified], now),
            "unknown producer DATA cannot consume an executable retry budget"
        );
        println!(
            "actual repair budget records: {}",
            serde_json::to_string(&originals).unwrap()
        );
        runtime.clear_faults();
        runtime.close().unwrap();
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
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
    fn delayed_catalog_install_cannot_reject_a_reservation_renewed_after_its_root_cut() {
        let node =
            crate::test_runtime::DfsTestRuntime::ordered_node_ids("native-delayed-catalog", 1)
                .remove(0);
        let mut runtime =
            crate::test_runtime::DfsTestRuntime::new("native-delayed-catalog", node.clone())
                .unwrap();
        runtime.start_nodes().unwrap();
        let shard = runtime.root_shard().unwrap();
        let file_system = DfsFileSystemConfig::from(&runtime.file_system());
        let content_root = runtime.data_dir(&node).dfs_dir().join("content");
        let content = crate::LocalContentStore::new(&content_root).unwrap();
        let mut service = runtime.client(&node).unwrap();
        let object_id = DfsObjectId::generate();
        let version = DfsVersion::generate();
        let manifest = content
            .put_object_version(object_id.as_str(), version.as_str(), b"current bytes")
            .unwrap();
        // A real primary page is captured after staging, but before the write
        // reaches canonical metadata. Hold it as an in-flight catalog reply.
        let topology_digest = dfs_content_root_catalog_topology_digest_with_file_systems(
            std::slice::from_ref(&file_system),
            std::slice::from_ref(&shard),
        )
        .unwrap();
        let identity = DfsContentRootCatalogShardIdentity::from_config(&shard);
        let request = crate::DfsContentRootCatalogPageRequest {
            identity: identity.clone(),
            expected_topology_digest: topology_digest.clone(),
            minimum_journal_sequence: 0,
            minimum_state_fingerprint: None,
            after: None,
            max_entries: 256,
        };
        let scan_cut = content.begin_content_root_catalog_scan().unwrap();
        let page = service
            .metadata_backend_mut()
            .unwrap()
            .content_root_catalog_page(&request)
            .expect("capture primary root cut");
        let captured_at = page.generated_at_unix_nanos;
        let fragment = DfsContentRootCatalogFragment::from_pages(&request, &[page]).unwrap();
        let captured = DfsContentRootCatalog::from_fragments(
            topology_digest,
            vec![identity],
            captured_at,
            vec![fragment],
        )
        .unwrap();
        assert!(
            !captured
                .protected_version_pairs()
                .unwrap()
                .contains(&(object_id.to_string(), version.to_string()))
        );

        // A normal committed write advances metadata after this captured cut.
        service
            .posix_write_file(
                "/delayed-catalog-current.txt",
                b"current bytes",
                crate::DfsWriteOptions::replicated_eventual(vec![node.clone()]),
            )
            .unwrap();
        // Keep the existing generic completion renewal. This regression does
        // not depend on any proposed reservation no-op optimization.
        content
            .put_blob_only_manifest(
                object_id.as_str(),
                version.as_str(),
                &manifest.content_hash,
                manifest.size_bytes,
            )
            .expect("generic completion renewal");
        let renewed = content
            .content_reservation_roots()
            .unwrap()
            .reservations
            .into_iter()
            .find(|row| {
                row.identity.object_id == object_id.as_str()
                    && row.identity.version == version.as_str()
            })
            .unwrap();
        assert!(renewed.reserved_at_unix_nanos > captured_at);
        let installed_at = renewed.reserved_at_unix_nanos + 1;
        LocalDfsContentRootCatalogStore::new(&content_root)
            .install_scanned_at(captured, scan_cut, installed_at)
            .expect("deliver delayed catalog");
        drop(content);

        // Separate/restarted handles retain the same durable interleaving.
        let content = crate::LocalContentStore::new(&content_root).unwrap();
        let installed = LocalDfsContentRootCatalogStore::new(&content_root)
            .load()
            .unwrap()
            .unwrap();
        let catalog_versions = installed.protected_version_pairs().unwrap();
        let catalog_hashes = installed.protected_content_hashes().unwrap();
        let validation = installed
            .validate_for_collection_with_head_watermark_policy(
                std::slice::from_ref(&file_system),
                std::slice::from_ref(&shard),
                &[],
                &[],
                false,
                installed_at + 1,
                DEFAULT_DFS_CONTENT_ROOT_CATALOG_MAX_AGE,
            )
            .expect("validate delayed complete catalog");
        assert!(validation.is_collection_authorized());
        let options = dfs_content_gc_lifecycle_options(
            DfsMaintenanceRunOptions {
                collect_content_versions: true,
                reconcile_content_reservations: true,
                content_reservation_rejection_grace: Duration::ZERO,
                ..DfsMaintenanceRunOptions::default()
            },
            validation,
        )
        .content_reservation_reconciliation
        .unwrap();
        let report = content
            .reconcile_content_reservations_with_catalog(
                &catalog_versions,
                &catalog_hashes,
                options,
            )
            .expect("reconcile delayed root cut");
        assert_eq!(
            report.released_rejected_count, 0,
            "installing an older root cut after completion must not make it newer evidence than the renewed reservation"
        );
        assert!(
            content
                .content_reservation_roots()
                .unwrap()
                .reservations
                .iter()
                .any(|row| row.identity.object_id == object_id.as_str()
                    && row.identity.version == version.as_str())
        );
        drop(content);
        drop(service);
        runtime.close().unwrap();
    }

    #[test]
    fn content_gc_uses_local_root_cut_time_and_ordinal_for_content_observations() {
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

            default_policy: DfsObjectPolicy::ReplicatedConfirmed,
        };
        let file_system = DfsFileSystemConfig {
            retention_policies: Default::default(),
            module_metadata: Default::default(),
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
            "dfs-metadata-prefix-v2:blake3:{}",
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
        let content = crate::LocalContentStore::new(&root).unwrap();
        let scan_started_at = 90;
        let legacy_validation = LocalDfsContentRootCatalogStore::new(&root)
            .install_test_catalog_at(catalog.clone(), installed_at_unix_nanos)
            .unwrap()
            .validate_for_collection_with_head_watermark_policy(
                std::slice::from_ref(&file_system),
                std::slice::from_ref(&shard),
                &[],
                &[],
                false,
                installed_at_unix_nanos + 1,
                Duration::from_nanos(100),
            )
            .unwrap();
        assert!(
            legacy_validation.is_collection_authorized(),
            "legacy catalog retains positive roots"
        );
        assert!(legacy_validation.catalog_scan_cut().is_none());
        let legacy_options = dfs_content_gc_lifecycle_options(
            DfsMaintenanceRunOptions {
                collect_content_versions: true,
                collect_orphan_content: true,
                reconcile_content_reservations: true,
                ..Default::default()
            },
            legacy_validation,
        );
        assert!(
            legacy_options
                .version_gc
                .unwrap()
                .catalog_generated_at_unix_nanos
                .is_none()
        );
        assert!(
            legacy_options
                .orphan_gc
                .unwrap()
                .catalog_observed_at_unix_nanos
                .is_none()
        );
        assert_eq!(
            legacy_options
                .content_reservation_reconciliation
                .unwrap()
                .catalog_generated_at_unix_nanos,
            0
        );
        // Installation cannot relabel an old/future root cut as fresh.
        for (started, installed, now, age) in
            [(102, 100, 103, 100), (90, 100, 89, 100), (1, 100, 101, 99)]
        {
            let cut = content.begin_content_root_catalog_scan_at(started).unwrap();
            let validation = LocalDfsContentRootCatalogStore::new(&root)
                .install_scanned_at(catalog.clone(), cut, installed)
                .unwrap()
                .validate_for_collection_with_head_watermark_policy(
                    std::slice::from_ref(&file_system),
                    std::slice::from_ref(&shard),
                    &[],
                    &[],
                    false,
                    now,
                    Duration::from_nanos(age),
                )
                .unwrap();
            assert!(!validation.is_collection_authorized());
        }
        let cut = content
            .begin_content_root_catalog_scan_at(scan_started_at)
            .unwrap();
        let validation = LocalDfsContentRootCatalogStore::new(&root)
            .install_scanned_at(catalog, cut, installed_at_unix_nanos)
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
            Some(scan_started_at),
            "manifest first-absence observations use the original local scan start"
        );
        assert_eq!(
            options
                .content_reservation_reconciliation
                .expect("reservation reconciliation")
                .catalog_generated_at_unix_nanos,
            scan_started_at,
            "reservation age uses local scan start; the ordinal separately proves ordering"
        );
    }
}
