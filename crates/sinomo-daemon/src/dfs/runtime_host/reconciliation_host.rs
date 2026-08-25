use super::*;

pub(super) fn notify_daemon_default_dfs_reconciliation_replica_progress(
    cluster_paths: &ClusterPaths,
) {
    sinomo_dfs::notify_default_dfs_reconciliation_replica_progress(cluster_paths);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DaemonDefaultDfsReconciliationStep {
    Idle,
    Progress,
    Parked,
    IdentityChanged,
}

/// Main-daemon owner for the derived default-DFS control-plane state.
///
/// One dirty startup pass covers state committed while this process was down.
/// Afterwards only semantic changes to the exact inputs consumed by default
/// DFS reconciliation wake the reactor. Derived service advertisements remain
/// in that semantic projection so deletion or corruption self-heals, while
/// their refresh-only `updated_at` field is excluded so a correct publication
/// cannot create a feedback loop.
pub(crate) struct DaemonDefaultDfsReconciliationReactor {
    pub(super) runtime: sinomo_dfs::DefaultDfsReconciliationRuntime,
    #[cfg(test)]
    pub(super) _projection_tx: tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
}

impl std::fmt::Debug for DaemonDefaultDfsReconciliationReactor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonDefaultDfsReconciliationReactor")
            .field("runtime", &self.runtime)
            .finish()
    }
}

impl DaemonDefaultDfsReconciliationReactor {
    #[cfg(test)]
    pub(crate) fn new(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
    ) -> Result<Self, DaemonError> {
        Self::new_with_runtime(cluster_paths, local_node_id, None)
    }

    pub(crate) fn new_on_runtime(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<Self, DaemonError> {
        Self::new_with_runtime(cluster_paths, local_node_id, Some(runtime))
    }

    fn new_with_runtime(
        cluster_paths: &ClusterPaths,
        local_node_id: &NodeId,
        runtime: Option<&tokio::runtime::Handle>,
    ) -> Result<Self, DaemonError> {
        let metadata_authority_generation = runtime
            .map(|runtime| {
                let state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)?;
                daemon_dfs_metadata_authority_generation_for_identity(
                    cluster_paths,
                    &state.cluster_id,
                    local_node_id,
                    Some(runtime),
                )
                .map_err(DaemonError::from)
            })
            .transpose()?;
        let owner_runtime = sinomo_dfs::DefaultDfsReconciliationRuntime::new(
            cluster_paths,
            local_node_id,
            metadata_authority_generation,
        )?;
        Ok(Self {
            #[cfg(test)]
            _projection_tx: owner_runtime.projection_sender(),
            runtime: owner_runtime,
        })
    }

    pub(crate) async fn changed(&mut self) {
        self.runtime.changed().await;
    }

    async fn changed_signal(&mut self) {
        self.runtime.changed_signal().await;
    }

    pub(super) fn exact_retry_generation(&self) -> DaemonDefaultDfsExactRetryGeneration {
        self.runtime.exact_retry_generation()
    }

    /// Completes the dirty startup generation before a settled solo daemon
    /// begins serving. The same retained reactor is then moved into the
    /// background task, so startup and committed-change handling share one
    /// ownership boundary and one observed generation.
    pub(crate) async fn reconcile_startup_until_idle_async(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        expected_cluster_id: &sinomo_core::Id,
        expected_node_id: &NodeId,
        runtime: &tokio::runtime::Handle,
    ) -> Result<(), DaemonError> {
        loop {
            match self
                .reconcile_pending_async(
                    paths,
                    options,
                    expected_cluster_id,
                    expected_node_id,
                    runtime,
                    false,
                )
                .await?
            {
                DaemonDefaultDfsReconciliationStep::Progress => continue,
                DaemonDefaultDfsReconciliationStep::Parked => return Ok(()),
                DaemonDefaultDfsReconciliationStep::Idle => return Ok(()),
                DaemonDefaultDfsReconciliationStep::IdentityChanged => {
                    return Err(DaemonError::ClusterOrphan);
                }
            }
        }
    }

    pub(super) fn pending_work(&mut self) -> Option<DaemonDefaultDfsReconciliationWork> {
        self.runtime.pending_work()
    }

    pub(super) fn complete_default_reconciliation_after_control_plane_observation(
        &mut self,
        work: &DaemonDefaultDfsReconciliationWork,
        local_node_id: &NodeId,
    ) -> bool {
        let promoted_primary_rebind = self
            .runtime
            .complete_after_control_plane_observation(work, local_node_id);
        if promoted_primary_rebind {
            // Completion is a control-plane edge, not a metadata mutation on
            // the promoted node. Wake the normal maintenance lane so it can
            // reuse the proof-bound durable-marker rebind before local status
            // or a foreground mutation has to discover the new generation.
            notify_daemon_dfs_maintenance_work();
        }
        promoted_primary_rebind
    }

    #[cfg(test)]
    pub(super) fn complete_default_reconciliation(
        &mut self,
        work: &DaemonDefaultDfsReconciliationWork,
    ) {
        self.runtime.complete_default_reconciliation(work);
    }

    pub(super) fn park_default_reconciliation(
        &mut self,
        work: &DaemonDefaultDfsReconciliationWork,
        handoff: DaemonDefaultDfsParkedHandoff,
    ) {
        self.runtime.park(work, handoff);
    }

    pub(super) fn complete_prior_process_recovery(
        &mut self,
        work: &DaemonDefaultDfsReconciliationWork,
    ) {
        self.runtime.complete_prior_process_recovery(work);
    }

    pub(super) async fn reconcile_pending_async(
        &mut self,
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        expected_cluster_id: &sinomo_core::Id,
        expected_node_id: &NodeId,
        runtime: &tokio::runtime::Handle,
        foreground_preemptible: bool,
    ) -> Result<DaemonDefaultDfsReconciliationStep, DaemonError> {
        let Some(work) = self.pending_work() else {
            return Ok(DaemonDefaultDfsReconciliationStep::Idle);
        };
        let Some(publish_cluster_control) = daemon_default_dfs_reconciliation_local_is_voter(
            paths,
            expected_cluster_id,
            expected_node_id,
            runtime,
        )
        .await?
        else {
            return Ok(DaemonDefaultDfsReconciliationStep::IdentityChanged);
        };

        let mut deferred_recovery_error = None;
        if work.recover_prior_process_permit {
            match recover_prior_process_default_dfs_placement_authority_async_on_runtime(
                paths,
                runtime,
                work.refresh_before_recovery,
            )
            .await
            {
                Ok(_) => self.complete_prior_process_recovery(&work),
                Err(error) if publish_cluster_control => return Err(error),
                Err(error) => {
                    tracing::warn!(
                        target: "sinomo.dfs.placement",
                        "prior-process SIFS placement authority recovery deferred: {error}"
                    );
                    deferred_recovery_error = Some(error);
                }
            }
        }

        if work.reconcile_defaults {
            // The placement writer can be the sole KV voter while the
            // retiring metadata primary is a learner. The learner deliberately
            // does not publish cluster-control state, so its lifecycle pass
            // returns `Completed` instead of rediscovering the authoritative
            // parked handoff. Transfer the already-committed exact handoff
            // edge to that node's local maintenance executor before consuming
            // the projection generation.
            let locally_owned_pending_handoff =
                daemon_default_dfs_locally_owned_pending_handoff(expected_node_id, &work.inputs);
            let lifecycle_outcome = match ensure_default_distributed_dfs_outcome_async_on_runtime(
                paths,
                options,
                publish_cluster_control,
                runtime.clone(),
                foreground_preemptible,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(lifecycle_error) => {
                    if let Some(recovery_error) = deferred_recovery_error {
                        tracing::warn!(
                            target: "sinomo.dfs.placement",
                            "default SIFS lifecycle also failed while prior-process placement authority recovery remains pending: {lifecycle_error}"
                        );
                        return Err(recovery_error);
                    }
                    return Err(lifecycle_error);
                }
            };
            match lifecycle_outcome {
                DaemonDefaultDfsLifecycleOutcome::Completed => {
                    if let Some(handoff) = locally_owned_pending_handoff {
                        self.park_default_reconciliation(&work, handoff);
                        notify_daemon_dfs_maintenance_work();
                        if let Some(error) = deferred_recovery_error {
                            return Err(error);
                        }
                        return Ok(DaemonDefaultDfsReconciliationStep::Parked);
                    }
                    self.complete_default_reconciliation_after_control_plane_observation(
                        &work,
                        expected_node_id,
                    );
                }
                DaemonDefaultDfsLifecycleOutcome::Parked(handoff) => {
                    let notify_local_retiring_primary =
                        handoff.is_owned_by_retiring_primary(expected_node_id, &work.inputs);
                    self.park_default_reconciliation(&work, handoff);
                    if notify_local_retiring_primary {
                        notify_daemon_dfs_maintenance_work();
                    }
                    if let Some(error) = deferred_recovery_error {
                        return Err(error);
                    }
                    return Ok(DaemonDefaultDfsReconciliationStep::Parked);
                }
            }
        }
        if let Some(error) = deferred_recovery_error {
            return Err(error);
        }
        Ok(DaemonDefaultDfsReconciliationStep::Progress)
    }
}

#[cfg(test)]
pub(super) fn observe_daemon_default_dfs_reconciliation(
    projection_tx: &tokio::sync::watch::Sender<DaemonDefaultDfsReconciliationProjection>,
    _local_node_id: &NodeId,
    snapshot: &KvSnapshot,
) {
    observe_default_dfs_reconciliation(projection_tx, snapshot, None);
}

async fn daemon_default_dfs_reconciliation_local_is_voter(
    paths: &DaemonPaths,
    expected_cluster_id: &sinomo_core::Id,
    expected_node_id: &NodeId,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<bool>, DaemonError> {
    let task_paths = paths.clone();
    let expected_cluster_id = expected_cluster_id.clone();
    let expected_node_id = expected_node_id.clone();
    runtime
        .spawn_blocking(move || {
            let data_dir = SinomoDataDir::new(daemon_data_dir(&task_paths)?);
            let cluster_paths = ClusterPaths::new(&data_dir);
            let state = sinomo_core::cluster::read_cluster_consensus_state(&cluster_paths)?;
            if state.cluster_id != expected_cluster_id || state.local_node_id != expected_node_id {
                return Ok(None);
            }
            Ok(Some(sinomo_core::kv::cluster_kv_raft_local_is_voter(
                &state,
            )))
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "default DFS reconciliation identity".to_string(),
            detail: format!("default DFS identity task failed: {source}"),
        })?
}

pub(super) async fn recover_prior_process_default_dfs_placement_authority_async_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    refresh_latest: bool,
) -> Result<bool, DaemonError> {
    let task_paths = paths.clone();
    let task_runtime = runtime.clone();
    runtime
        .spawn_blocking(move || {
            recover_prior_process_default_dfs_placement_authority_from_observation(
                &task_paths,
                &task_runtime,
                refresh_latest,
            )
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "default DFS placement-authority recovery".to_string(),
            detail: format!("default DFS recovery task failed: {source}"),
        })?
}

fn daemon_default_dfs_reconciliation_retry_delay(failure_streak: u32) -> Duration {
    let shift = failure_streak.saturating_sub(1).min(6);
    DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_INITIAL
        .saturating_mul(1_u32.checked_shl(shift).unwrap_or(64))
        .min(DAEMON_DEFAULT_DFS_RECONCILIATION_RETRY_MAX)
}

pub(crate) async fn run_daemon_default_dfs_reconciliation_reactor(
    mut reactor: DaemonDefaultDfsReconciliationReactor,
    paths: DaemonPaths,
    options: DaemonRunOptions,
    expected_cluster_id: sinomo_core::Id,
    expected_node_id: NodeId,
    runtime: tokio::runtime::Handle,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut failure_streak = 0_u32;
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        match reactor
            .reconcile_pending_async(
                &paths,
                &options,
                &expected_cluster_id,
                &expected_node_id,
                &runtime,
                true,
            )
            .await
        {
            Ok(DaemonDefaultDfsReconciliationStep::Progress) => {
                failure_streak = 0;
                continue;
            }
            Ok(DaemonDefaultDfsReconciliationStep::Parked) => {
                failure_streak = 0;
                // A durable handoff is waiting on exact state owners, not a
                // failed reconciliation attempt. It has no retry timer: only
                // the handoff/proof/shard generation or subscribed replica
                // progress can make the parked work runnable again.
                tokio::select! {
                    _ = reactor.changed() => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                }
            }
            Ok(DaemonDefaultDfsReconciliationStep::Idle) => {
                failure_streak = 0;
                tokio::select! {
                    _ = reactor.changed() => {}
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return;
                        }
                    }
                }
            }
            Ok(DaemonDefaultDfsReconciliationStep::IdentityChanged) => {
                tracing::info!(
                    target: "sinomo.dfs.maintenance",
                    "default DFS reconciliation stopped after daemon identity changed"
                );
                return;
            }
            Err(error) => {
                failure_streak = failure_streak.saturating_add(1);
                let retry_delay = daemon_default_dfs_reconciliation_retry_delay(failure_streak);
                tracing::warn!(
                    target: "sinomo.dfs.maintenance",
                    failure_streak,
                    retry_delay_ms = retry_delay.as_millis().min(u128::from(u64::MAX)) as u64,
                    "default DFS reconciliation failed; retrying after source change or backoff: {error}"
                );
                // A timer exists only while real work is failing. Preserve its
                // absolute deadline across ordinary placement/connectivity
                // churn: only an exact handoff, proof, shard-authority, permit,
                // or subscribed replica generation can make the failed work
                // runnable before that deadline.
                let failed_generation = reactor.exact_retry_generation();
                let retry_deadline = tokio::time::Instant::now() + retry_delay;
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep_until(retry_deadline) => break,
                        _ = reactor.changed_signal() => {
                            if reactor.exact_retry_generation() != failed_generation {
                                break;
                            }
                        }
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DaemonDefaultDfsLifecycleOutcome {
    Completed,
    Parked(DaemonDefaultDfsParkedHandoff),
}

#[cfg(test)]
pub(crate) async fn ensure_default_distributed_dfs_async_on_runtime(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    match ensure_default_distributed_dfs_outcome_async_on_runtime(
        paths,
        options,
        publish_cluster_control,
        runtime,
        false,
    )
    .await?
    {
        DaemonDefaultDfsLifecycleOutcome::Completed => Ok(()),
        DaemonDefaultDfsLifecycleOutcome::Parked(handoff) => {
            Err(sinomo_dfs::DfsError::MetadataShardMembershipHandoffParked {
                file_system_id: handoff.file_system_id,
                shard_id: handoff.shard_id.0,
                handoff_id: handoff.handoff_id,
                retiring_epoch: handoff.retiring_epoch,
                desired_epoch: handoff.desired_epoch,
            }
            .into())
        }
    }
}

async fn ensure_default_distributed_dfs_outcome_async_on_runtime(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
    foreground_preemptible: bool,
) -> Result<DaemonDefaultDfsLifecycleOutcome, DaemonError> {
    #[cfg(test)]
    record_daemon_default_dfs_reconciliation_run(paths);
    let data_dir = daemon_data_dir(paths)?;
    let lifecycle_data_dir = SinomoDataDir::new(data_dir);
    let lifecycle_cluster_paths = ClusterPaths::new(&lifecycle_data_dir);
    let metadata_authority_preflight = if publish_cluster_control {
        let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&lifecycle_cluster_paths)?
        else {
            return Err(DaemonError::ClusterOrphan);
        };
        let preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
            &lifecycle_cluster_paths,
            &peer_snapshot,
            &runtime,
        )
        .await?;
        Some((peer_snapshot, preflight))
    } else {
        // Learners and shadow/non-authoritative callers neither consume nor
        // publish shard topology. They must not hydrate a metadata backend or
        // enter Raft merely to determine that this branch has no authority.
        None
    };
    let lifecycle_gate = daemon_default_dfs_lifecycle_gate(data_dir)?;
    let mutation_scheduler = daemon_dfs_mutation_scheduler(&lifecycle_data_dir)?;
    // The dedicated reactor is the sole owner of default reconciliation. A
    // startup pass waits normally. Background passes publish maintenance
    // interest before either broad gate and destroy any queued global writer
    // as soon as foreground traffic asks to run; this avoids Tokio's
    // write-preferring RwLock stranding later exact-shard requests behind a
    // reconciliation generation which has not started yet.
    let (lifecycle_guard, mutation_admission, foreground_preemption) = if foreground_preemptible {
        loop {
            let foreground_preemption = mutation_scheduler.begin_maintenance_interest();
            let mut lifecycle_admission = Box::pin(Arc::clone(&lifecycle_gate).lock_owned());
            let lifecycle_guard = tokio::select! {
                biased;
                _ = foreground_preemption.requested() => None,
                guard = &mut lifecycle_admission => Some(guard),
            };
            drop(lifecycle_admission);
            let Some(lifecycle_guard) = lifecycle_guard else {
                drop(foreground_preemption);
                tokio::time::sleep(DAEMON_DEFAULT_DFS_RECONCILIATION_FOREGROUND_QUIET_PERIOD).await;
                continue;
            };
            let mut mutation_admission_future =
                Box::pin(mutation_scheduler.admit(&DfsMessageExecutionScope::GlobalExclusive));
            let mutation_admission = tokio::select! {
                biased;
                _ = foreground_preemption.requested() => None,
                admission = &mut mutation_admission_future => Some(admission?),
            };
            drop(mutation_admission_future);
            let Some(mutation_admission) = mutation_admission else {
                drop(lifecycle_guard);
                drop(foreground_preemption);
                tokio::time::sleep(DAEMON_DEFAULT_DFS_RECONCILIATION_FOREGROUND_QUIET_PERIOD).await;
                continue;
            };
            break (
                lifecycle_guard,
                mutation_admission,
                Some(foreground_preemption),
            );
        }
    } else {
        let lifecycle_guard = lifecycle_gate.lock_owned().await;
        let mutation_admission = mutation_scheduler
            .admit(&DfsMessageExecutionScope::GlobalExclusive)
            .await?;
        (lifecycle_guard, mutation_admission, None)
    };
    // Own admission inside the blocking task that polls the complete lifecycle
    // future. Dropping the JoinHandle cannot cancel a running blocking task, so
    // neither guard can be released while backend-bearing DFS work still runs.
    let task_paths = paths.clone();
    let task_options = options.clone();
    let task_runtime = runtime.clone();
    let task_data_dir = lifecycle_data_dir;
    let task_cluster_paths = lifecycle_cluster_paths;
    let task_data_owner_lock = daemon_dfs_data_owner_lock(paths)?;
    let lifecycle_result = runtime
        .spawn_blocking(move || {
            let _data_owner_lock = task_data_owner_lock;
            let _lifecycle_guard = lifecycle_guard;
            let _mutation_admission = mutation_admission;
            let resident_metadata_backend = match metadata_authority_preflight.as_ref() {
                Some((peer_snapshot, preflight)) => {
                    daemon_dfs_resident_metadata_backend_after_preflight(
                        &task_data_dir,
                        &task_cluster_paths,
                        peer_snapshot,
                        preflight,
                        &task_runtime,
                    )
                    .map_err(DaemonError::from)
                }
                None => Ok(None),
            };
            let lifecycle_result =
                resident_metadata_backend.and_then(|resident_metadata_backend| {
                    task_runtime.block_on(ensure_default_distributed_dfs_lifecycle_inner(
                        &task_paths,
                        &task_options,
                        publish_cluster_control,
                        task_runtime.clone(),
                        resident_metadata_backend,
                    ))
                });

            if sinomo_dfs::take_default_cluster_dfs_materialization_immediate_retry(&task_data_dir)
            {
                notify_daemon_default_dfs_reconciliation_replica_progress(&ClusterPaths::new(
                    &task_data_dir,
                ));
            }

            // The lifecycle may append before parking, failing, or losing its
            // outer waiter. Retain the publisher edge from inside the same
            // cancellation-independent closure and before releasing mutation
            // admission; the spawned lane will wait for this guard normally.
            let publish_result = if publish_cluster_control {
                schedule_daemon_dfs_metadata_publish_on_runtime(&task_paths, &task_runtime, None)
                    .map(|_| ())
            } else {
                // A non-voter cannot own an authoritative metadata shard, so
                // it cannot have a journal eligible for cluster publication.
                // Starting discovery here would perform an authoritative KV
                // read for work this process is forbidden to execute.
                Ok(())
            };
            match (lifecycle_result, publish_result) {
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
                (Ok(outcome), Ok(_)) => Ok(outcome),
            }
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "default DFS lifecycle".to_string(),
            detail: format!("default DFS lifecycle task failed: {source}"),
        })?;
    let foreground_waiting = foreground_preemption
        .as_ref()
        .is_some_and(DaemonDfsMaintenanceForegroundPreemption::is_requested);
    drop(foreground_preemption);
    if foreground_waiting {
        // Admission was released inside the completed blocking task. Delay
        // only the next bounded pass so the foreground burst which requested
        // preemption can drain without another global writer immediately
        // taking its place.
        tokio::time::sleep(DAEMON_DEFAULT_DFS_RECONCILIATION_FOREGROUND_QUIET_PERIOD).await;
    }
    lifecycle_result
}

async fn ensure_default_distributed_dfs_lifecycle_inner(
    paths: &DaemonPaths,
    _options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
    resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
) -> Result<DaemonDefaultDfsLifecycleOutcome, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    // The shadow shares the data root but never owns a second device sampler.
    let local_content_advertisement = (!crate::daemon_paths_are_shadow(paths))
        .then(|| {
            sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
                .ok()
                .map(|state| {
                    daemon_dfs_local_content_advertisement(
                        &data_dir,
                        &state.local_node_id,
                        daemon_dfs_local_zone(),
                    )
                })
        })
        .flatten();
    let control_plane = Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
        &cluster_paths,
        &runtime,
    ));
    match sinomo_dfs::ensure_default_cluster_dfs_async_with_resident_metadata(
        data_dir,
        cluster_paths,
        control_plane,
        resident_metadata_backend,
        DfsDefaultClusterBootstrapOptions {
            publish_cluster_control,
            materialize_local_metadata: true,
            local_zone: daemon_dfs_local_zone(),
            local_content_advertisement,
        },
    )
    .await
    {
        Ok(_) => Ok(DaemonDefaultDfsLifecycleOutcome::Completed),
        Err(sinomo_dfs::DfsError::MetadataShardMembershipHandoffParked {
            file_system_id,
            shard_id,
            handoff_id,
            retiring_epoch,
            desired_epoch,
        }) => Ok(DaemonDefaultDfsLifecycleOutcome::Parked(
            DaemonDefaultDfsParkedHandoff::new(
                file_system_id,
                shard_id,
                handoff_id,
                retiring_epoch,
                desired_epoch,
            ),
        )),
        Err(error) => Err(error.into()),
    }
}
