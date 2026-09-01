use super::*;

pub(crate) fn daemon_dfs_maintenance_notify() -> Arc<tokio::sync::Notify> {
    Arc::clone(DAEMON_DFS_MAINTENANCE_NOTIFY.get_or_init(|| Arc::new(tokio::sync::Notify::new())))
}

pub(crate) fn notify_daemon_dfs_maintenance_work() {
    #[cfg(test)]
    DAEMON_DFS_MAINTENANCE_WAKE_COUNT.fetch_add(1, Ordering::Relaxed);
    daemon_dfs_maintenance_notify().notify_one();
}

const DAEMON_DFS_MAINTENANCE_RECONCILIATION_MARKER: &str = "repair-queue-reconciled-v1";
const DAEMON_DFS_MAINTENANCE_RECONCILIATION_MARKER_CONTENT: &[u8] =
    b"sinomo-dfs-repair-queue-reconciled-v1\n";

fn daemon_dfs_maintenance_reconciliation_marker_path(data_dir: &SinomoDataDir) -> PathBuf {
    data_dir
        .dfs_dir()
        .join("maintenance")
        .join(DAEMON_DFS_MAINTENANCE_RECONCILIATION_MARKER)
}

fn daemon_dfs_maintenance_reconciliation_data_root(
    paths: &DaemonPaths,
) -> Result<PathBuf, DaemonError> {
    daemon_data_dir(paths).map(|path| path.to_path_buf())
}

/// Request one authoritative metadata reconciliation before the next bounded
/// repair pass. Requests are generation-counted so a manual request arriving
/// during a running pass cannot be cleared by the older completion.
pub(crate) fn request_daemon_dfs_maintenance_reconciliation(
    paths: &DaemonPaths,
) -> Result<u64, DaemonError> {
    let data_root = daemon_dfs_maintenance_reconciliation_data_root(paths)?;
    let generation = {
        let mut requests = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let generation = requests
            .get(&data_root)
            .copied()
            .unwrap_or_default()
            .wrapping_add(1)
            .max(1);
        requests.insert(data_root, generation);
        generation
    };
    notify_daemon_dfs_maintenance_work();
    Ok(generation)
}

pub(crate) fn daemon_dfs_maintenance_reconciliation_generation(paths: &DaemonPaths) -> Option<u64> {
    let data_root = daemon_dfs_maintenance_reconciliation_data_root(paths).ok()?;
    DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS
        .get()
        .and_then(|requests| {
            requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&data_root)
                .copied()
        })
}

/// Upgrade installations created before committed mutations durably owned
/// their repair-queue delta. The marker makes the inventory reconciliation a
/// one-time internal migration rather than work repeated at every restart.
pub(crate) fn schedule_daemon_dfs_maintenance_reconciliation_if_needed(
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_dfs_maintenance_reconciliation_data_root(paths)?);
    if daemon_dfs_maintenance_reconciliation_marker_path(&data_dir).is_file() {
        return Ok(false);
    }
    request_daemon_dfs_maintenance_reconciliation(paths).map(|_| true)
}

pub(super) fn complete_daemon_dfs_maintenance_reconciliation(
    paths: &DaemonPaths,
    generation: u64,
) -> Result<(), DaemonError> {
    let data_root = daemon_dfs_maintenance_reconciliation_data_root(paths)?;
    let data_dir = SinomoDataDir::new(data_root.clone());
    let marker = daemon_dfs_maintenance_reconciliation_marker_path(&data_dir);
    sinomo_core::storage::durable_file::write_file_atomic(
        &marker,
        DAEMON_DFS_MAINTENANCE_RECONCILIATION_MARKER_CONTENT,
        sinomo_core::storage::durable_file::DurableAtomicFileOptions::new(
            DAEMON_DFS_MAINTENANCE_RECONCILIATION_MARKER,
            "repair-queue-reconciliation",
        )
        .sync_all(),
    )
    .map_err(|source| DaemonError::Io {
        path: marker.display().to_string(),
        detail: source.to_string(),
    })?;
    if let Some(requests) = DAEMON_DFS_MAINTENANCE_RECONCILIATION_REQUESTS.get() {
        let mut requests = requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if requests.get(&data_root).copied() == Some(generation) {
            requests.remove(&data_root);
        }
    }
    Ok(())
}

pub(super) fn record_daemon_dfs_repair_queue_nonempty(data_dir: &SinomoDataDir, nonempty: bool) {
    DAEMON_DFS_REPAIR_QUEUE_NONEMPTY
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(data_dir.root(), nonempty);
}

pub(super) fn refresh_daemon_dfs_repair_queue_nonempty(
    data_dir: &SinomoDataDir,
) -> Result<bool, sinomo_dfs::DfsError> {
    let nonempty =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(data_dir))?
            .is_some_and(|snapshot| !snapshot.records.is_empty());
    record_daemon_dfs_repair_queue_nonempty(data_dir, nonempty);
    Ok(nonempty)
}

/// Return the process-local queue state sampled at startup, a committed repair
/// edge, or the end of the previous bounded maintenance pass.
///
/// Connectivity generation changes use this to wake only a repair executor
/// whose external route blocker may have cleared. An empty cluster must not
/// turn routine route refreshes into broad DFS maintenance passes.
pub(crate) fn daemon_dfs_repair_queue_work_is_known(paths: &DaemonPaths) -> bool {
    let Some(data_root) = paths.root.parent() else {
        return false;
    };
    DAEMON_DFS_REPAIR_QUEUE_NONEMPTY
        .get()
        .and_then(|states| {
            states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(data_root)
                .copied()
        })
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) fn daemon_dfs_maintenance_wake_count() -> u64 {
    DAEMON_DFS_MAINTENANCE_WAKE_COUNT.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(super) fn record_daemon_dfs_allocator_cleanup_request(data_dir: &SinomoDataDir) {
    *DAEMON_DFS_TEST_ALLOCATOR_CLEANUP_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(data_dir.root())
        .or_default() += 1;
}

#[cfg(test)]
pub(super) fn daemon_dfs_allocator_cleanup_request_count(data_dir: &SinomoDataDir) -> u64 {
    DAEMON_DFS_TEST_ALLOCATOR_CLEANUP_REQUESTS
        .get()
        .and_then(|counts| {
            counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&data_dir.root())
                .copied()
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(super) fn record_daemon_dfs_allocator_trim_completion(data_dir: &SinomoDataDir) {
    *DAEMON_DFS_TEST_ALLOCATOR_TRIM_COMPLETIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(data_dir.root())
        .or_default() += 1;
}

#[cfg(test)]
pub(super) fn daemon_dfs_allocator_trim_completion_count(data_dir: &SinomoDataDir) -> u64 {
    DAEMON_DFS_TEST_ALLOCATOR_TRIM_COMPLETIONS
        .get()
        .and_then(|counts| {
            counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&data_dir.root())
                .copied()
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(super) fn daemon_dfs_local_membership_handoff_keys(
    metadata_shard_statuses: &[DfsMetadataShardStatus],
    local_node_id: &NodeId,
) -> Vec<String> {
    metadata_shard_statuses
        .iter()
        .filter(|status| status.primary_node_id.as_ref() == Some(local_node_id))
        .filter_map(|status| {
            status.file_system_id.as_ref().map(|file_system_id| {
                sinomo_dfs::metadata_shard_membership_handoff_key(
                    file_system_id,
                    &status.shard_id.0,
                )
            })
        })
        .collect()
}

#[cfg(test)]
pub(super) fn daemon_dfs_node_is_offline_for_maintenance(
    availability: &ClusterNodeAvailability,
    connectivity: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    target_node_id: &NodeId,
) -> bool {
    sinomo_dfs::dfs_node_is_offline_for_maintenance(
        availability,
        connectivity,
        local_node_id,
        target_node_id,
    )
}

pub(super) fn daemon_dfs_offline_node_ids(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> BTreeSet<NodeId> {
    sinomo_dfs::dfs_offline_node_ids(cluster_paths, state)
}

pub(super) fn daemon_dfs_maintenance_execution_exclusions(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
) -> DfsMaintenanceExecutionExclusions {
    sinomo_dfs::dfs_resident_maintenance_execution_exclusions(cluster_paths, state)
}

/// Add newly committed repair identities to the bounded durable queue.
///
/// This runs at the mutation's committed edge, before its live service lane is
/// released. Existing retry/backoff state wins for an already queued target;
/// maintenance remains the sole owner of advancing or clearing that state.
/// The framed append is proportional only to this message's newly committed
/// targets and never reads or rewrites the accumulated queue.
pub(super) fn persist_daemon_dfs_repair_queue_records(
    data_dir: &SinomoDataDir,
    committed_records: Vec<DfsRepairAttemptRecord>,
) -> Result<usize, sinomo_dfs::DfsError> {
    let appended =
        sinomo_dfs::persist_default_dfs_repair_queue_records(data_dir, committed_records)?;
    if appended > 0 {
        record_daemon_dfs_repair_queue_nonempty(data_dir, true);
    }
    Ok(appended)
}

#[cfg(test)]
pub(super) fn fail_next_daemon_dfs_mounted_mutation_after_repair_persist(data_dir: &SinomoDataDir) {
    DAEMON_DFS_TEST_MOUNTED_MUTATION_FAIL_AFTER_REPAIR_PERSIST
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .expect("mounted-mutation crash-cut hooks")
        .insert(data_dir.root());
}

#[cfg(test)]
pub(super) fn inject_daemon_dfs_mounted_mutation_crash_cut_after_repair_persist(
    data_dir: &SinomoDataDir,
) -> Result<(), DaemonError> {
    let should_fail = DAEMON_DFS_TEST_MOUNTED_MUTATION_FAIL_AFTER_REPAIR_PERSIST
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .expect("mounted-mutation crash-cut hooks")
        .remove(&data_dir.root());
    if should_fail {
        return Err(DaemonError::Io {
            path: data_dir.root().display().to_string(),
            detail: "injected mounted-mutation crash after repair queue persistence".to_string(),
        });
    }
    Ok(())
}

pub(super) struct DaemonMountedSifsMutationHandoffHost<'a> {
    pub(super) data_dir: &'a SinomoDataDir,
    pub(super) cluster_paths: &'a ClusterPaths,
    pub(super) runtime: &'a tokio::runtime::Handle,
    pub(super) traceparent: Option<String>,
}

impl sinomo_dfs::MountedSifsMutationHandoffHost for DaemonMountedSifsMutationHandoffHost<'_> {
    fn repair_queue_persisted(&self, appended_record_count: usize) {
        if appended_record_count > 0 {
            record_daemon_dfs_repair_queue_nonempty(self.data_dir, true);
        }
    }

    fn after_repair_queue_persisted(&self) -> Result<(), sinomo_dfs::DfsError> {
        #[cfg(test)]
        inject_daemon_dfs_mounted_mutation_crash_cut_after_repair_persist(self.data_dir).map_err(
            |error| sinomo_dfs::DfsError::Metadata {
                message: error.to_string(),
            },
        )?;
        Ok(())
    }

    fn committed_work(&self, committed_work: DfsMessageCommittedWork) {
        emit_daemon_dfs_committed_edges(
            self.data_dir,
            self.cluster_paths,
            &DfsMessageExecutionScope::GlobalExclusive,
            self.runtime,
            self.traceparent.clone(),
            committed_work,
            &[],
        );
    }

    fn maintenance_needed(&self) {
        notify_daemon_dfs_maintenance_work();
    }
}

/// Main-daemon endpoint for the crash-safe mounted-filesystem mutation
/// handoff. Only Arm authors a precondition. Every post-mutation request first
/// attests the adapter's exact fsynced marker phase and payload.
pub(crate) fn handle_daemon_dfs_mounted_mutation_handoff_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    request: DaemonDfsMountedMutationHandoffRequest,
) -> Result<DaemonDfsMountedMutationHandoffResponse, DaemonError> {
    if paths.is_shadow() {
        return Err(DaemonError::Parse {
            detail: "mounted-mutation durability handoff is owned only by the main daemon"
                .to_string(),
        });
    }
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    sinomo_dfs::handle_mounted_sifs_mutation_handoff(
        &data_dir,
        &DaemonMountedSifsMutationHandoffHost {
            data_dir: &data_dir,
            cluster_paths: &cluster_paths,
            runtime,
            traceparent,
        },
        request,
    )
    .map_err(Into::into)
}

/// Count exact completed primary transfers whose durable local marker has not
/// yet been consumed by the promoted router generation.
///
/// This reads one bounded marker and one compact proof per current local
/// primary. It never opens, recovers, or scans a metadata backend. Successful
/// rebind advances `activated_epoch`, durably clearing this retry projection.
#[cfg(test)]
pub(super) fn daemon_dfs_completed_local_primary_handoff_rebind_count(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    pending_handoffs: &[DfsMetadataShardMembershipHandoff],
    active_shards: &[DfsMetadataShardConfig],
    authority_entries: &[KvEntry],
) -> Result<usize, DaemonError> {
    sinomo_dfs::dfs_completed_local_primary_handoff_rebind_count(
        data_dir,
        local_node_id,
        pending_handoffs,
        active_shards,
        authority_entries,
    )
    .map_err(Into::into)
}

/// Inspect only the bounded durable queues owned by normal reactive healing.
///
/// This function is safe on daemon startup and notification handling: it does
/// not open a DFS metadata backend, enumerate object/shard indexes, or inspect
/// content. Full metadata reconstruction belongs to explicit disaster
/// recovery, not to the automatic scheduler.
pub(super) fn daemon_dfs_mounted_mutation_pending_work(
    data_dir: &SinomoDataDir,
    now_unix_nanos: u64,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    sinomo_dfs::mounted_sifs_mutation_pending_maintenance_work(data_dir, now_unix_nanos)
        .map_err(Into::into)
}

pub(super) fn daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(
    paths: &DaemonPaths,
    authority_entries: &[KvEntry],
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let now_unix_nanos = daemon_dfs_now_unix_nanos();
    if !cluster_paths.state.exists() {
        return daemon_dfs_mounted_mutation_pending_work(&data_dir, now_unix_nanos);
    }
    let repair_attempt_records =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    record_daemon_dfs_repair_queue_nonempty(&data_dir, !repair_attempt_records.is_empty());
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    sinomo_dfs::dfs_pending_reactive_maintenance_work_from_authority_entries(
        &data_dir,
        &state.local_node_id,
        authority_entries,
        &repair_attempt_records,
        now_unix_nanos,
    )
    .map_err(Into::into)
}

pub(crate) fn daemon_dfs_pending_reactive_maintenance_work_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(paths, &[]);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        Some(runtime),
    )?;
    let projection =
        authority
            .current_complete_prefix()
            .ok_or_else(|| sinomo_dfs::DfsError::Network {
                message: "reactive DFS maintenance awaits a complete retained authority projection"
                    .to_string(),
            })?;
    daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(paths, projection.entries())
}

#[cfg(test)]
pub(crate) fn daemon_dfs_pending_reactive_maintenance_work(
    paths: &DaemonPaths,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let entries = if cluster_paths.state.exists() {
        sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
            &cluster_paths,
            DAEMON_DFS_METADATA_AUTHORITY_PREFIX,
        )?
    } else {
        Vec::new()
    };
    daemon_dfs_pending_reactive_maintenance_work_from_authority_entries(paths, &entries)
}

/// Full metadata classifier retained for authoritative reconciliation tests.
///
/// Unlike [`daemon_dfs_pending_reactive_maintenance_work`], this opens the
/// cluster-aware metadata image and is therefore O(total metadata). Production
/// daemon scheduling must never call it.
#[cfg(test)]
pub(crate) fn daemon_dfs_pending_maintenance_work(
    paths: &DaemonPaths,
) -> Result<DaemonDfsPendingMaintenanceWork, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsPendingMaintenanceWork::default());
    }
    let now_unix_nanos = daemon_dfs_now_unix_nanos();
    // Routine scheduling observes only indexed metadata work queues and their
    // bounded retry records. Content-store inventory (reservations, manifests,
    // versions, blobs, and orphan observations) is deliberately excluded:
    // those O(total-content) scans belong to authoritative reconciliation, never
    // to daemon startup, notification handling, or an idle safety poll.
    let DaemonDfsMaintenanceMetadataObservation {
        summary,
        metadata_shard_statuses,
        write_intents,
        repair_views,
        actionable_conflict_count,
        receipt_records,
        lookups,
    } = daemon_dfs_maintenance_metadata_observation(&data_dir, &cluster_paths)?;
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map_err(DaemonError::from)?;
    let pending_local_membership_handoff_count =
        daemon_dfs_local_membership_handoff_keys(&metadata_shard_statuses, &state.local_node_id)
            .into_iter()
            .try_fold(0usize, |count, key| {
                sinomo_core::cluster::read_cluster_kv_latest_observed_entry(&cluster_paths, &key)
                    .map(|entry| count.saturating_add(usize::from(entry.is_some())))
                    .map_err(DaemonError::from)
            })?;
    let membership_handoffs = cluster_dfs_all_metadata_shard_membership_handoffs(&cluster_paths)?;
    let authority_entries = sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
        &cluster_paths,
        DAEMON_DFS_METADATA_AUTHORITY_PREFIX,
    )?;
    let active_shards = authority_entries
        .iter()
        .filter(|entry| daemon_dfs_metadata_shard_config_key_is_exact(&entry.key))
        .map(|entry| entry.decode::<DfsMetadataShardConfig>())
        .collect::<Result<Vec<_>, _>>()?;
    let completed_local_primary_handoff_rebind_count =
        daemon_dfs_completed_local_primary_handoff_rebind_count(
            &data_dir,
            &state.local_node_id,
            &membership_handoffs,
            &active_shards,
            &authority_entries,
        )?;
    let repair_attempt_records =
        read_dfs_repair_attempt_snapshot(&default_dfs_repair_attempt_snapshot_path(&data_dir))?
            .map(|snapshot| snapshot.records)
            .unwrap_or_default();
    let placement_reconciliation_jobs =
        sinomo_core::cluster::read_cluster_kv_latest_observed_prefix_entries(
            &cluster_paths,
            &sinomo_dfs::placement_reconciliations_prefix(&sinomo_dfs::sifs_file_system_id()),
        )?
        .into_iter()
        .map(|entry| entry.decode::<DfsPlacementReconciliationJob>())
        .collect::<Result<Vec<_>, _>>()?;
    let actionable_placement_reconciliation_count =
        daemon_dfs_actionable_placement_reconciliation_count(
            &placement_reconciliation_jobs,
            summary.pending_repair_count,
        );
    let offline_node_ids = daemon_dfs_offline_node_ids(&cluster_paths, &state);
    let intents_without_recovery_source = daemon_dfs_intents_without_recovery_source(
        &data_dir,
        &lookups,
        &write_intents,
        &receipt_records,
        &offline_node_ids,
        &state.local_node_id,
    );
    let repairs_without_recovery_source = daemon_dfs_repairs_without_recovery_source(
        &data_dir,
        &lookups,
        &repair_views,
        &receipt_records,
        &offline_node_ids,
        &state.local_node_id,
    );
    let mut work = daemon_dfs_pending_work_from_records(
        summary.pending_write_count,
        summary.pending_repair_count,
        summary.pending_namespace_job_count,
        actionable_conflict_count,
        actionable_placement_reconciliation_count,
        &write_intents,
        &receipt_records,
        &metadata_shard_statuses,
        &repair_views,
        &repair_attempt_records,
        &intents_without_recovery_source,
        &repairs_without_recovery_source,
        &offline_node_ids,
        &state.local_node_id,
        pending_local_membership_handoff_count,
        now_unix_nanos,
    );
    work.completed_local_primary_handoff_rebind_count =
        completed_local_primary_handoff_rebind_count;
    Ok(work)
}
