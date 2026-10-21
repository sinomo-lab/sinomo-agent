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
    WaitingForTopology,
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
    data_dir: SinomoDataDir,
    foreground_quiet: Option<(DaemonDefaultDfsExactRetryGeneration, Instant)>,
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
        let placement_authority_process_identity = runtime.and_then(|runtime| {
            let client = DaemonDfsControlPlaneKvClient::new_on_runtime(cluster_paths, runtime);
            sinomo_dfs::DfsControlPlaneKvClient::placement_authority_process_identity(&client)
        });
        let owner_runtime = sinomo_dfs::DefaultDfsReconciliationRuntime::new(
            cluster_paths,
            local_node_id,
            metadata_authority_generation,
            placement_authority_process_identity,
        )?;
        Ok(Self {
            foreground_quiet: None,
            data_dir: SinomoDataDir::new(
                cluster_paths
                    .root
                    .parent()
                    .ok_or(DaemonError::ClusterOrphan)?,
            ),
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
            let attempted_generation = self.exact_retry_generation();
            let result = self
                .reconcile_pending_async(
                    paths,
                    options,
                    expected_cluster_id,
                    expected_node_id,
                    runtime,
                    false,
                )
                .await;
            tracing::debug!(target: "sinomo.dfs.reconciliation",
                node = %expected_node_id, foreground_preemptible = false,
                attempted_local_generation = attempted_generation.local_generation,
                attempted_input_count = attempted_generation.inputs.len(),
                result = ?result, "default DFS startup reconciliation step returned");
            match result? {
                DaemonDefaultDfsReconciliationStep::Progress => continue,
                DaemonDefaultDfsReconciliationStep::WaitingForTopology => {
                    if self.exact_retry_generation() == attempted_generation {
                        self.changed_signal().await;
                    }
                }
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
            notify_daemon_dfs_maintenance_work(&self.data_dir);
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

    #[tracing::instrument(target = "sinomo.dfs.reconciliation", level = "trace", skip_all,
        fields(node = %expected_node_id, data_root = %self.data_dir.root().display()))]
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
            tracing::debug!(target: "sinomo.dfs.reconciliation",
                node = %expected_node_id, foreground_preemptible,
                "default DFS reconciliation has no pending work");
            return Ok(DaemonDefaultDfsReconciliationStep::Idle);
        };
        tracing::debug!(
            target: "sinomo.dfs.reconciliation",
            node = %expected_node_id,
            foreground_preemptible,
            reconcile_defaults = work.reconcile_defaults,
            recover_prior_process_permit = work.recover_prior_process_permit,
            refresh_before_recovery = work.refresh_before_recovery,
            local_generation = work.local_generation,
            input_count = work.inputs.len(),
            permit_modify_revision = work
                .placement_authority_permit
                .as_ref()
                .map(|permit| permit.modify_revision.0),
            "default DFS reconciliation generation started"
        );
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
        let publish_metadata = publish_cluster_control
            || daemon_default_dfs_reconciliation_local_owns_metadata_primary(
                expected_node_id,
                &work.inputs,
            );

        let mut deferred_recovery_error = None;
        if work.recover_prior_process_permit {
            if work.refresh_before_recovery {
                // Mount installs orchestration; the initial retained generation
                // owns bounded local-state preparation and startup edge replay.
                let task_owner = registered_dfs_runtime_host(&self.data_dir)?.ok_or_else(|| {
                    DaemonError::Network {
                        address: "DFS startup mounted-work replay".into(),
                        detail: "startup replay has no registered runtime host".into(),
                    }
                })?;
                let owner = require_daemon_dfs_data_owner_lock(paths)?;
                let task_paths = paths.clone();
                let task_runtime = runtime.clone();
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                let child = runtime.spawn_blocking(move || {
                    let _actual_owner = owner;
                    let result = replay_daemon_dfs_mounted_mutation_committed_work_on_runtime(
                        &task_paths,
                        &task_runtime,
                    );
                    let _ = result_tx.send(result);
                });
                // Register before awaiting: cancellation cannot detach storage
                // work from the normal component's actual child-task drain.
                task_owner.own_task(child);
                result_rx.await.map_err(|source| DaemonError::Network {
                    address: "DFS startup mounted-work replay".into(),
                    detail: format!("startup replay task stopped before returning: {source}"),
                })??;
            }
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
            let (lifecycle_outcome, foreground_waiting) =
                match ensure_default_distributed_dfs_outcome_async_on_runtime(
                    paths,
                    options,
                    expected_cluster_id,
                    expected_node_id,
                    publish_cluster_control,
                    publish_metadata,
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
            if foreground_waiting
                && !matches!(
                    &lifecycle_outcome,
                    DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission
                )
            {
                self.foreground_quiet = Some((
                    DaemonDefaultDfsExactRetryGeneration {
                        inputs: work.inputs.clone(),
                        placement_authority_permit: work.placement_authority_permit.clone(),
                        local_generation: work.local_generation,
                    },
                    Instant::now() + DAEMON_DEFAULT_DFS_RECONCILIATION_FOREGROUND_QUIET_PERIOD,
                ));
            }
            match lifecycle_outcome {
                DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission => {
                    return Ok(DaemonDefaultDfsReconciliationStep::Progress);
                }
                DaemonDefaultDfsLifecycleOutcome::WaitingForTopology => {
                    return Ok(DaemonDefaultDfsReconciliationStep::WaitingForTopology);
                }
                DaemonDefaultDfsLifecycleOutcome::Completed => {
                    let current = self.runtime.projection_sender().borrow().clone();
                    let locally_owned_pending_handoff =
                        daemon_default_dfs_locally_owned_pending_handoff(
                            expected_node_id,
                            &current.inputs,
                        );
                    if let Some(handoff) = locally_owned_pending_handoff {
                        let mut observed = work.clone();
                        observed.inputs = current.inputs;
                        self.park_default_reconciliation(&observed, handoff);
                        notify_daemon_dfs_maintenance_work(&self.data_dir);
                        if let Some(error) = deferred_recovery_error {
                            return Err(error);
                        }
                        return Ok(DaemonDefaultDfsReconciliationStep::Parked);
                    }
                    self.complete_default_reconciliation_after_control_plane_observation(
                        &work,
                        expected_node_id,
                    );
                    tracing::trace!(
                        target: "sinomo.dfs.reconciliation",
                        local_generation = work.local_generation,
                        "default DFS reconciliation generation completed"
                    );
                }
                DaemonDefaultDfsLifecycleOutcome::Parked(handoff) => {
                    // The lifecycle can commit this handoff after capturing
                    // `work`. Its ordered projection already contains that
                    // edge; the preceding generation cannot name its owner.
                    let current = self.runtime.projection_sender().borrow().clone();
                    let notify_local_retiring_primary =
                        handoff.is_owned_by_retiring_primary(expected_node_id, &current.inputs);
                    let mut observed = work.clone();
                    observed.inputs = current.inputs;
                    self.park_default_reconciliation(&observed, handoff);
                    if notify_local_retiring_primary {
                        notify_daemon_dfs_maintenance_work(&self.data_dir);
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
    tracing::debug!(target: "sinomo.dfs.reconciliation", node = %expected_node_id,
        "default DFS reconciliation reactor task started");
    let mut failure_streak = 0_u32;
    // Recheck the same owner's current obligation on every pass: consuming a
    // parked handoff can require normal startup admission after an earlier idle.
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        if let Some((generation, deadline)) = reactor.foreground_quiet.take() {
            // Quiet delays the NEXT pass, after completed effects and their
            // exact generation are recorded. An actual semantic projection or
            // local-progress change can make that captured generation obsolete.
            while reactor.runtime.foreground_quiet_generation().as_ref() == Some(&generation) {
                tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
                    _ = reactor.changed_signal() => {},
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() { return; }
                    }
                }
            }
        }
        let foreground_preemptible = !reactor.runtime.mandatory_generation_pending();
        let attempted_generation = reactor.exact_retry_generation();
        let result = reactor
            .reconcile_pending_async(
                &paths,
                &options,
                &expected_cluster_id,
                &expected_node_id,
                &runtime,
                foreground_preemptible,
            )
            .await;
        tracing::debug!(target: "sinomo.dfs.reconciliation",
            node = %expected_node_id, foreground_preemptible,
            attempted_local_generation = attempted_generation.local_generation,
            attempted_input_count = attempted_generation.inputs.len(),
            result = ?result, "default DFS background reconciliation step returned");
        match result {
            Ok(DaemonDefaultDfsReconciliationStep::Progress) => {
                failure_streak = 0;
                continue;
            }
            Ok(DaemonDefaultDfsReconciliationStep::WaitingForTopology) => {
                failure_streak = 0;
                // Storage has not run and this mandatory generation is still
                // pending. Its empty topology can only become usable through
                // the existing committed projection or owned refresh wake.
                // Compare the pre-attempt cut first so an update committed
                // during admission is never lost before registering the wait.
                // A refresh completion can make the same semantic topology
                // usable; reselect after that actual wake as well.
                if reactor.exact_retry_generation() == attempted_generation {
                    tokio::select! {
                        _ = reactor.changed_signal() => {},
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() { return; }
                        }
                    }
                }
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
            Err(DaemonError::StoragePreparationPending {
                root,
                processed_records,
            }) if processed_records > 0 => {
                // The provider processed a bounded batch. Keep this same
                // unfinished generation and continue its bounded work; Ready
                // and lifecycle completion still require the ordinary pass.
                failure_streak = 0;
                tracing::debug!(target: "sinomo.dfs.reconciliation",
                    root = %root.display(), processed_records,
                    "DFS storage preparation advanced; continuing the current generation");
                tokio::task::yield_now().await;
                continue;
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
    DeferredBeforeAdmission,
    WaitingForTopology,
}

#[cfg(test)]
pub(crate) async fn ensure_default_distributed_dfs_async_on_runtime(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    publish_cluster_control: bool,
    runtime: tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let state = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(
        &ClusterPaths::new(&data_dir),
    )?;
    match ensure_default_distributed_dfs_outcome_async_on_runtime(
        paths,
        options,
        &state.cluster_id,
        &state.local_node_id,
        publish_cluster_control,
        publish_cluster_control,
        runtime,
        false,
    )
    .await?
    .0
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
        DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission
        | DaemonDefaultDfsLifecycleOutcome::WaitingForTopology => Err(DaemonError::Parse {
            detail: "nonpreemptible startup lifecycle was deferred before admission".into(),
        }),
    }
}

fn default_dfs_lifecycle_admission_scope(
    publishes_control: bool,
    publishes_metadata: bool,
    retained_no_materialization: bool,
) -> DfsMessageExecutionScope {
    if !publishes_control && !publishes_metadata && retained_no_materialization {
        DfsMessageExecutionScope::ReadOnly
    } else {
        DfsMessageExecutionScope::GlobalExclusive
    }
}

// Foreground metadata replies may complete work already holding an exact
// generation fence. A queued broad writer must yield to that work even when
// the lifecycle generation itself is mandatory. No lifecycle effect has begun
// here; the caller leaves that generation unfinished and reselects normally.
async fn acquire_default_dfs_lifecycle_admission(
    lifecycle_gate: Arc<tokio::sync::Mutex<()>>,
    mutation_scheduler: &Arc<DaemonDfsMutationScheduler>,
    lifecycle_scope: &DfsMessageExecutionScope,
) -> Result<
    Option<(
        tokio::sync::OwnedMutexGuard<()>,
        DaemonDfsMutationAdmission,
        DaemonDfsMaintenanceForegroundPreemption,
    )>,
    DaemonError,
> {
    let foreground_preemption = mutation_scheduler.begin_maintenance_interest();
    let mut lifecycle_admission = Box::pin(lifecycle_gate.lock_owned());
    let lifecycle_guard = tokio::select! {
        biased;
        _ = foreground_preemption.requested() => None,
        guard = &mut lifecycle_admission => Some(guard),
    };
    drop(lifecycle_admission);
    let Some(lifecycle_guard) = lifecycle_guard else {
        return Ok(None);
    };
    let mut mutation_admission_future = Box::pin(mutation_scheduler.admit(lifecycle_scope));
    let mutation_admission = tokio::select! {
        biased;
        _ = foreground_preemption.requested() => None,
        admission = &mut mutation_admission_future => Some(admission?),
    };
    // Destroy a queued fair-RwLock writer before releasing the lifecycle gate.
    // Later exact-shard replies can then complete the existing owner normally.
    drop(mutation_admission_future);
    let Some(mutation_admission) = mutation_admission else {
        drop(lifecycle_guard);
        return Ok(None);
    };
    Ok(Some((
        lifecycle_guard,
        mutation_admission,
        foreground_preemption,
    )))
}

async fn ensure_default_distributed_dfs_outcome_async_on_runtime(
    paths: &DaemonPaths,
    options: &DaemonRunOptions,
    expected_cluster_id: &sinomo_core::Id,
    expected_node_id: &NodeId,
    publish_cluster_control: bool,
    publish_metadata: bool,
    runtime: tokio::runtime::Handle,
    foreground_preemptible: bool,
) -> Result<(DaemonDefaultDfsLifecycleOutcome, bool), DaemonError> {
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "authority-preflight", publish_metadata);
    let data_dir = daemon_data_dir(paths)?;
    let lifecycle_data_dir = SinomoDataDir::new(data_dir);
    let lifecycle_cluster_paths = ClusterPaths::new(&lifecycle_data_dir);
    let metadata_authority_preflight = if publish_metadata {
        let peer_deadline =
            tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot_before(
            &lifecycle_cluster_paths,
            &runtime,
            peer_deadline,
        )
        .await?
        else {
            return Err(DaemonError::ClusterOrphan);
        };
        let preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
            &lifecycle_cluster_paths,
            &peer_snapshot,
            &runtime,
            peer_deadline,
            #[cfg(test)]
            None,
        )
        .await?;
        Some((peer_snapshot, preflight))
    } else {
        // A node which owns no authoritative metadata shard must not hydrate a
        // backend or enter Raft merely to determine that it has no journal to
        // publish.
        None
    };
    // Preserve the original preflight for every metadata/control publisher.
    // A nonpublisher outside the actual bootstrap owner only maintains its
    // local KV advertisements, provided the same complete native generation
    // proves that it owns no metadata primary. Unknown topology stays global.
    let passive_role = if !publish_cluster_control && !publish_metadata {
        let state = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(
            &lifecycle_cluster_paths,
        )?;
        if state.cluster_id != *expected_cluster_id || state.local_node_id != *expected_node_id {
            return Err(DaemonError::ClusterOrphan);
        }
        if crate::default_cluster_sifs_owner(&state) == state.local_node_id {
            None
        } else {
            let authority = daemon_dfs_metadata_authority_generation_for_identity(
                &lifecycle_cluster_paths,
                expected_cluster_id,
                expected_node_id,
                Some(&runtime),
            )?;
            let generation = authority.generation();
            let shards = authority.cached_metadata_shards(
                &crate::sifs_file_system_id(),
                authority.projection_generation(),
            )?;
            shards
                .filter(|shards| {
                    shards
                        .iter()
                        .all(|shard| shard.primary_node_id != state.local_node_id)
                })
                .map(|shards| (state, authority, generation, shards))
        }
    } else {
        None
    };
    // The same mandatory owner covers startup, exact authority changes and
    // local replica progress. Passive nodes still own their physical storage;
    // a provider-only follow-up retains the existing read-only path.
    let prepare_passive_storage = passive_role.is_some() && !foreground_preemptible;
    let lifecycle_scope = default_dfs_lifecycle_admission_scope(
        publish_cluster_control,
        publish_metadata,
        passive_role.is_some() && !prepare_passive_storage,
    );
    let lifecycle_gate = daemon_default_dfs_lifecycle_gate(data_dir)?;
    let mutation_scheduler = daemon_dfs_mutation_scheduler(&lifecycle_data_dir)?;
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-admission", scope = ?lifecycle_scope);
    // The sole reactor retains mandatory startup/configuration work until it
    // completes. Before either broad gate is acquired, yield queued admission
    // to the existing foreground edge so reciprocal metadata replies are not
    // stranded behind a generation which has not started yet.
    let Some((lifecycle_guard, mutation_admission, queued_foreground_preemption)) =
        acquire_default_dfs_lifecycle_admission(
            lifecycle_gate,
            &mutation_scheduler,
            &lifecycle_scope,
        )
        .await?
    else {
        tracing::trace!(target: "sinomo.dfs.reconciliation",
            phase = "lifecycle-deferred-before-admission", foreground_preemptible);
        return Ok((
            DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission,
            true,
        ));
    };
    // A mandatory pass stays nonpreemptible once it owns actual admission.
    // Only the existing background path keeps its post-admission quiet signal.
    let foreground_preemption = if foreground_preemptible {
        Some(queued_foreground_preemption)
    } else {
        drop(queued_foreground_preemption);
        None
    };
    // Own admission inside the blocking task that polls the complete lifecycle
    // future. Dropping the JoinHandle cannot cancel a running blocking task, so
    // neither guard can be released while backend-bearing DFS work still runs.
    let task_paths = paths.clone();
    let task_options = *options;
    let task_runtime = runtime.clone();
    let task_data_dir = lifecycle_data_dir;
    let task_cluster_paths = lifecycle_cluster_paths;
    let task_data_owner_lock =
        daemon_dfs_data_owner_lock(paths)?.ok_or_else(|| crate::DfsError::Metadata {
            message: "native DFS reconciliation requires its actual data owner".into(),
        })?;
    let task_owner =
        registered_dfs_runtime_host(&task_data_dir)?.ok_or_else(|| DaemonError::Network {
            address: "default DFS lifecycle".into(),
            detail: "native DFS reconciliation has no registered component task owner".into(),
        })?;
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-admitted");
    let task_span = tracing::Span::current();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let child = runtime.spawn_blocking(move || {
        // This same started child owns the complete backend-bearing lifecycle.
        // Drop its owners before exposing the result, as the old join did.
        let lifecycle_result = (move || {
            let _span = task_span.enter();
            tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-started", scope = ?lifecycle_scope);
            let _lifecycle_guard = lifecycle_guard;
            if let Some((state, authority, generation, captured_shards)) = passive_role.as_ref() {
                let current_state = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(
                    &task_cluster_paths,
                )?;
                let current_shards = authority.cached_metadata_shards(
                    &crate::sifs_file_system_id(), authority.projection_generation(),
                )?;
                if current_state.cluster_id != state.cluster_id
                    || current_state.local_node_id != state.local_node_id
                    || crate::default_cluster_sifs_owner(&current_state) == current_state.local_node_id
                    || authority.retained_complete_prefix_revision_for_generation(*generation).is_none()
                    || current_shards.as_ref() != Some(captured_shards)
                {
                    // No effects ran. Reselect the actual changed identity/role;
                    // this old observation cannot complete its generation.
                    return Ok(DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission);
                }
            }
            let selected_preflight = metadata_authority_preflight
                .as_ref()
                .map(|(_, preflight)| preflight.for_admission(&mutation_admission));
            let publication_owners = crate::DfsPublicationExecutionOwners::from_runtime(
                task_data_dir.root(),
                lifecycle_scope,
                mutation_admission,
                task_data_owner_lock,
            );
            if prepare_passive_storage {
                let (_, authority, generation, captured_shards) =
                    passive_role.as_ref().expect("captured passive storage role");
                if captured_shards.is_empty() {
                    // An empty projection cannot prove that unknown physical
                    // metadata has completed preparation. Retain the unfinished
                    // generation and await its actual topology publication.
                    tracing::trace!(target: "sinomo.dfs.reconciliation",
                        phase = "lifecycle-topology-unavailable",
                        data_root = %task_data_dir.root().display());
                    return Ok(DaemonDefaultDfsLifecycleOutcome::WaitingForTopology);
                }
                let metadata_root = crate::default_dfs_metadata_shards_root(&task_data_dir);
                crate::storage_preparation::prepare_storage_root_before_open(
                    &metadata_root,
                    &crate::DfsStoragePreparationScope::MetadataRouting,
                )?;
                for shard in captured_shards {
                    crate::storage_preparation::prepare_storage_root_before_open(
                        &crate::local_dfs_metadata_shard_path(
                            &task_data_dir,
                            &DfsMetadataShardId::new(&shard.shard_id),
                        ),
                        &crate::DfsStoragePreparationScope::Metadata(
                            DfsMetadataShardId::new(&shard.shard_id),
                        ),
                    )?;
                }
                if authority
                    .retained_complete_prefix_revision_for_generation(*generation)
                    .is_none()
                {
                    return Ok(DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission);
                }
            }
            let resident_metadata_backend = match metadata_authority_preflight.as_ref() {
                Some((peer_snapshot, _)) => {
                    // This task owns global admission, so prior exact leases
                    // are drained. Reuse the existing certified successor
                    // rule when authority advanced while admission waited.
                    // An unavailable successor still fails before execution.
                    match try_daemon_dfs_resident_metadata_backend_after_preflight(
                        &task_data_dir,
                        &task_cluster_paths,
                        peer_snapshot,
                        selected_preflight.as_ref().expect("captured preflight"),
                        &task_runtime,
                    ) {
                        Ok(backend) => Ok(backend),
                        Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale))
                            if stale.current_generation != stale.expected_generation =>
                        {
                            tracing::trace!(target: "sinomo.dfs.reconciliation",
                                phase = "lifecycle-authority-advanced-before-execution",
                                expected_generation = stale.expected_generation,
                                current_generation = stale.current_generation,
                                current_ready = stale.current_ready);
                            // No lifecycle effect ran. The same retained reactor
                            // reselects its current role and configuration before
                            // trying this unfinished generation again.
                            return Ok(DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission);
                        }
                        Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale)) => {
                            Err(DaemonError::from(stale.into_dfs_error()))
                        }
                        Err(DaemonDfsAuthorityPreExecutionError::Failed(error)) => {
                            Err(DaemonError::from(error))
                        }
                    }
                }
                None => Ok(None),
            };
            let lifecycle_result =
                resident_metadata_backend.and_then(|resident_metadata_backend| {
                    task_runtime.block_on(ensure_default_distributed_dfs_lifecycle_inner(
                        &task_paths,
                        &task_options,
                        publish_cluster_control,
                        passive_role.is_none(),
                        task_runtime.clone(),
                        resident_metadata_backend,
                        Arc::clone(&publication_owners),
                    ))
                });

            tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-block-on-returned",
                data_root = %task_data_dir.root().display());
            tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-retry-take-started",
                data_root = %task_data_dir.root().display());
            let immediate_retry =
                sinomo_dfs::take_default_cluster_dfs_materialization_immediate_retry(&task_data_dir);
            tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-retry-take-completed",
                data_root = %task_data_dir.root().display(), immediate_retry);
            if immediate_retry {
                notify_daemon_default_dfs_reconciliation_replica_progress(&ClusterPaths::new(
                    &task_data_dir,
                ));
            }

            // The lifecycle may append before parking, failing, or losing its
            // outer waiter. Retain the publisher edge from inside the same
            // cancellation-independent closure and before releasing mutation
            // admission; the spawned lane will wait for this guard normally.
            let publish_result = if publish_metadata {
                schedule_daemon_dfs_metadata_publish_on_runtime(&task_paths, &task_runtime, None)
                    .map(|_| ())
            } else {
                // This node owns no authoritative metadata shard. Starting
                // discovery would perform an authoritative KV read for work it
                // cannot publish.
                Ok(())
            };
            tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-blocking-return",
                data_root = %task_data_dir.root().display(), publish_metadata);
            match (lifecycle_result, publish_result) {
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
                (Ok(outcome), Ok(_)) => Ok(outcome),
            }
        })();
        let _ = result_tx.send(lifecycle_result);
    });
    // Register before the first await: cancelling orchestration must not detach
    // a running blocking child from the normal component storage-owner drain.
    task_owner.own_task(child);
    let lifecycle_result = result_rx.await.map_err(|source| DaemonError::Network {
        address: "default DFS lifecycle".to_string(),
        detail: format!("default DFS lifecycle task stopped before returning: {source}"),
    })?;
    let foreground_waiting = foreground_preemption
        .as_ref()
        .is_some_and(DaemonDfsMaintenanceForegroundPreemption::is_requested);
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-post-join-preemption",
        foreground_preemptible, foreground_waiting,
        quiet_period_ms = DAEMON_DEFAULT_DFS_RECONCILIATION_FOREGROUND_QUIET_PERIOD.as_millis());
    drop(foreground_preemption);
    lifecycle_result.map(|outcome| (outcome, foreground_waiting))
}

#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.background.lifecycle",
    skip_all, fields(stage = "dfs.background.lifecycle", publish_cluster_control,
    outcome = tracing::field::Empty))]
async fn ensure_default_distributed_dfs_lifecycle_inner(
    paths: &DaemonPaths,
    _options: &DaemonRunOptions,
    publish_cluster_control: bool,
    materialize_local_metadata: bool,
    runtime: tokio::runtime::Handle,
    resident_metadata_backend: Option<ResidentLocalShardedMetadataBackend>,
    publication_owners: Arc<crate::DfsPublicationExecutionOwners>,
) -> Result<DaemonDefaultDfsLifecycleOutcome, DaemonError> {
    let mut observation = crate::background_observation::DfsBackgroundObservation::new(
        "lifecycle",
        "default-cluster",
    );
    let result = async {
        let data_dir = daemon_data_dir(paths)?;
        let data_dir = SinomoDataDir::new(data_dir);
        let cluster_paths = ClusterPaths::new(&data_dir);
        let local_zone = daemon_dfs_local_zone(&data_dir);
        // The shadow shares the data root but never owns a second device sampler.
        let local_content_advertisement = (!paths.is_shadow())
            .then(|| {
                sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
                    .ok()
                    .map(|state| {
                        cached_daemon_dfs_local_content_advertisement(
                            &data_dir,
                            &state.local_node_id,
                            local_zone.clone(),
                        )
                    })
            })
            .flatten();
        let control_plane = Box::new(DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(
            &cluster_paths,
            &runtime,
        ));
        match sinomo_dfs::ensure_default_cluster_dfs_async_with_resident_metadata(
            data_dir,
            cluster_paths,
            control_plane,
            resident_metadata_backend,
            publication_owners,
            DfsDefaultClusterBootstrapOptions {
                publish_cluster_control,
                materialize_local_metadata,
                local_zone,
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
    .await;
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-observation-finish-started");
    observation.finish(match &result {
        Ok(DaemonDefaultDfsLifecycleOutcome::Completed) => "complete",
        Ok(DaemonDefaultDfsLifecycleOutcome::Parked(_)) => "deferred",
        Ok(DaemonDefaultDfsLifecycleOutcome::DeferredBeforeAdmission)
        | Ok(DaemonDefaultDfsLifecycleOutcome::WaitingForTopology) => "deferred",
        Err(_) => "error",
    });
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-observation-finish-completed");
    drop(observation);
    tracing::trace!(target: "sinomo.dfs.reconciliation", phase = "lifecycle-observation-dropped");
    result
}

#[cfg(test)]
#[tokio::test]
async fn nonmaterializing_lifecycle_keeps_global_writers_exclusive_without_joining_them() {
    let scheduler = Arc::new(crate::DfsMutationScheduler::new());
    let writer = scheduler
        .admit(&DfsMessageExecutionScope::GlobalExclusive)
        .await
        .unwrap();
    let scope = default_dfs_lifecycle_admission_scope(false, false, true);
    let reader = std::future::poll_fn(|context| {
        let mut admission = Box::pin(scheduler.admit(&scope));
        match std::future::Future::poll(admission.as_mut(), context) {
            std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
            std::task::Poll::Pending => {
                panic!("no-metadata lifecycle must not queue on the global writer")
            }
        }
    })
    .await
    .unwrap();
    assert!(!reader.is_global() && !reader.is_exact());
    assert!(scheduler.global_is_exclusively_held());
    for (control, metadata, retained) in [
        (true, false, true),
        (false, true, true),
        (false, false, false),
    ] {
        assert!(matches!(
            default_dfs_lifecycle_admission_scope(control, metadata, retained),
            DfsMessageExecutionScope::GlobalExclusive
        ));
    }
    drop(reader);
    assert!(scheduler.global_is_exclusively_held());
    drop(writer);
}

#[cfg(test)]
#[tokio::test]
async fn mandatory_lifecycle_queued_writer_yields_to_reciprocal_metadata_admission() {
    use std::future::Future;
    use std::task::Poll;

    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let source_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: crate::sifs_file_system_id(),
        shard_id: DfsMetadataShardId::new("users"),
    };
    let reply_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: crate::sifs_file_system_id(),
        shard_id: DfsMetadataShardId::new("services"),
    };
    let lifecycle_scope = DfsMessageExecutionScope::GlobalExclusive;
    let source = scheduler.admit(&source_scope).await.unwrap();
    let mut lifecycle = Box::pin(acquire_default_dfs_lifecycle_admission(
        Arc::clone(&gate),
        &scheduler,
        &lifecycle_scope,
    ));
    std::future::poll_fn(|context| {
        assert!(matches!(lifecycle.as_mut().poll(context), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    assert_eq!(scheduler.maintenance_interest_count(), 1);

    // The existing normal admission API raises the foreground edge. Its
    // exact-shard reply is initially behind the actual queued global writer.
    let mut reply = Box::pin(admit_daemon_dfs_message(&scheduler, &reply_scope, true));
    std::future::poll_fn(|context| {
        assert!(matches!(reply.as_mut().poll(context), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    assert!(lifecycle.await.unwrap().is_none());
    assert_eq!(scheduler.maintenance_interest_count(), 0);
    let reply = tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, reply)
        .await
        .expect("reciprocal reply must proceed while the source read is still held")
        .unwrap();
    assert!(reply.is_exact());
    assert!(source.is_exact());
    drop(reply);
    drop(source);

    // The unfinished generation can reacquire actual exclusive admission.
    // Dropping its queue-only signal cannot cancel this already-held owner.
    let (lifecycle_gate, lifecycle_admission, foreground_preemption) =
        acquire_default_dfs_lifecycle_admission(Arc::clone(&gate), &scheduler, &lifecycle_scope)
            .await
            .unwrap()
            .unwrap();
    assert!(lifecycle_admission.is_global());
    drop(foreground_preemption);
    assert_eq!(scheduler.maintenance_interest_count(), 0);
    let mut later_reply = Box::pin(admit_daemon_dfs_message(&scheduler, &reply_scope, true));
    std::future::poll_fn(|context| {
        assert!(matches!(later_reply.as_mut().poll(context), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    assert!(gate.try_lock().is_err());
    drop(lifecycle_admission);
    drop(lifecycle_gate);
    let later_reply =
        tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, later_reply)
            .await
            .unwrap()
            .unwrap();
    assert!(later_reply.is_exact());
}

#[cfg(test)]
#[tokio::test]
async fn mandatory_lifecycle_yields_before_its_lifecycle_gate_without_releasing_the_owner() {
    use std::future::Future;
    use std::task::Poll;

    let scheduler = Arc::new(DaemonDfsMutationScheduler::new());
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    let held = Arc::clone(&gate).lock_owned().await;
    let scope = DfsMessageExecutionScope::GlobalExclusive;
    let mut lifecycle = Box::pin(acquire_default_dfs_lifecycle_admission(
        Arc::clone(&gate),
        &scheduler,
        &scope,
    ));
    std::future::poll_fn(|context| {
        assert!(matches!(lifecycle.as_mut().poll(context), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    scheduler.request_foreground_admission();
    assert!(lifecycle.await.unwrap().is_none());
    assert!(
        gate.try_lock().is_err(),
        "the original held owner stays exact"
    );
    assert_eq!(scheduler.maintenance_interest_count(), 0);
    drop(held);
    let (_gate, admission, _interest) =
        acquire_default_dfs_lifecycle_admission(gate, &scheduler, &scope)
            .await
            .unwrap()
            .unwrap();
    assert!(admission.is_global());
}
