use super::*;
use tracing::Instrument;

static NEXT_MAINTENANCE_TRACE_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

pub(crate) type DaemonDfsMaintenanceTickOutcome = sinomo_dfs::DfsMaintenanceTickOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DaemonDfsMaintenanceKind {
    Routine,
    MountedMutationRecovery,
    AuthoritativeReconciliation {
        content_gc_requested: bool,
        content_reservation_reconciliation_requested: bool,
        commit_promotion_recovery_requested: bool,
    },
}

impl DaemonDfsMaintenanceKind {
    pub(super) fn replica_dispatch_is_deferred(self) -> bool {
        matches!(
            self,
            Self::Routine
                | Self::MountedMutationRecovery
                | Self::AuthoritativeReconciliation {
                    content_gc_requested: false,
                    content_reservation_reconciliation_requested: false,
                    commit_promotion_recovery_requested: false,
                }
        )
    }
}

pub(super) type DaemonDfsMountedMutationRecoveryBatch =
    sinomo_dfs::MountedSifsMutationRecoveryBatch;

/// Replay the daemon-owned monotonic mounted-mutation latch once while the
/// main daemon is starting. The latch never clears, so a crash during this
/// replay merely causes the next daemon generation to schedule the same
/// idempotent owners again.
pub(crate) fn replay_daemon_dfs_mounted_mutation_committed_work_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    if paths.is_shadow() {
        return Ok(());
    }
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(());
    }
    let Some(committed_work) = sinomo_dfs::mounted_sifs_mutation_startup_committed_work(&data_dir)?
    else {
        return Ok(());
    };
    let edges = emit_daemon_dfs_committed_edges(
        &data_dir,
        &cluster_paths,
        &DfsMessageExecutionScope::GlobalExclusive,
        runtime,
        None,
        committed_work,
        &[],
    );
    if committed_work.metadata_replica_progress && !edges.replica_progress {
        // `invalidate_*` is conditional on a currently established dynamic
        // subscription. The startup latch is stronger: publish one retained
        // local generation even when reconciliation has not subscribed yet.
        notify_daemon_default_dfs_reconciliation_replica_progress(&cluster_paths);
    }
    Ok(())
}

pub(super) fn prepare_daemon_dfs_mounted_mutation_recovery_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsMountedMutationRecoveryBatch, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsMountedMutationRecoveryBatch::default());
    }
    let batch = DaemonDfsMountedMutationRecoveryBatch::prepare(&data_dir)?;
    if batch.appended_repair_record_count() > 0 {
        record_daemon_dfs_repair_queue_nonempty(&data_dir, true);
    }
    for committed_work in batch.committed_work().iter().copied() {
        emit_daemon_dfs_committed_edges(
            &data_dir,
            &cluster_paths,
            &DfsMessageExecutionScope::GlobalExclusive,
            runtime,
            None,
            committed_work,
            &[],
        );
    }
    Ok(batch)
}

pub(super) fn complete_daemon_dfs_mounted_mutation_recovery(
    batch: DaemonDfsMountedMutationRecoveryBatch,
    outcome: DaemonDfsMaintenanceTickOutcome,
) -> Result<(), DaemonError> {
    batch
        .complete(matches!(
            outcome,
            DaemonDfsMaintenanceTickOutcome::Completed {
                reached_limit: false
            }
        ))
        .map_err(Into::into)
}

pub(super) type DaemonDfsMaintenancePreemptionOutcome<T> =
    sinomo_dfs::DfsMaintenancePreemptionOutcome<T>;

pub(super) async fn acquire_daemon_dfs_maintenance_admission_until_foreground_waits(
    data_dir: &SinomoDataDir,
    foreground_preemption: &DaemonDfsMaintenanceForegroundPreemption,
) -> Result<Option<DaemonDfsMaintenanceAdmission>, DaemonError> {
    sinomo_dfs::acquire_dfs_maintenance_admission_until_foreground_waits(
        foreground_preemption,
        acquire_daemon_dfs_maintenance_admission(data_dir),
    )
    .await
}

pub(super) async fn run_daemon_dfs_maintenance_until_foreground_waits<F>(
    foreground_preemption: &DaemonDfsMaintenanceForegroundPreemption,
    maintenance: F,
) -> DaemonDfsMaintenancePreemptionOutcome<F::Output>
where
    F: std::future::Future,
{
    sinomo_dfs::run_dfs_maintenance_until_foreground_waits(foreground_preemption, maintenance).await
}

pub(crate) async fn run_daemon_dfs_maintenance_tick_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    node_resources: sinomo_device::NodeResourceSnapshot,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let mounted_mutation_recovery =
        prepare_daemon_dfs_mounted_mutation_recovery_on_runtime(paths, runtime)?;
    let reconciliation_generation = daemon_dfs_maintenance_reconciliation_generation(paths);
    let maintenance_kind = if mounted_mutation_recovery.requires_full_recovery() {
        DaemonDfsMaintenanceKind::MountedMutationRecovery
    } else if reconciliation_generation.is_some() {
        DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
            content_gc_requested: false,
            content_reservation_reconciliation_requested: false,
            commit_promotion_recovery_requested: false,
        }
    } else {
        DaemonDfsMaintenanceKind::Routine
    };
    let outcome = loop {
        let outcome = run_daemon_dfs_maintenance_tick_kind_on_runtime(
            paths,
            runtime,
            maintenance_kind,
            node_resources,
        )
        .await?;
        if mounted_mutation_recovery.requires_full_recovery()
            && matches!(
                outcome,
                DaemonDfsMaintenanceTickOutcome::Completed {
                    reached_limit: true
                }
            )
        {
            // Retain the exact orphan claims and their recovery generation.
            // Re-preparing them would republish their conservative work edges
            // between every bounded page, invalidating recovery's own progress.
            // Each pass still releases admission and yields to foreground work.
            tokio::task::yield_now().await;
            continue;
        }
        break outcome;
    };
    complete_daemon_dfs_mounted_mutation_recovery(mounted_mutation_recovery, outcome)?;
    complete_daemon_dfs_maintenance_reconciliation_after_tick(
        paths,
        reconciliation_generation,
        outcome,
    )?;
    Ok(outcome)
}

pub(super) fn complete_daemon_dfs_maintenance_reconciliation_after_tick(
    paths: &DaemonPaths,
    generation: Option<u64>,
    outcome: DaemonDfsMaintenanceTickOutcome,
) -> Result<(), DaemonError> {
    if let Some(generation) = generation
        && matches!(
            outcome,
            DaemonDfsMaintenanceTickOutcome::Completed {
                reached_limit: false
            }
        )
    {
        complete_daemon_dfs_maintenance_reconciliation(paths, generation)?;
    }
    Ok(())
}

#[tracing::instrument(target = "sinomo.perf", level = "trace", name = "dfs.background",
    skip_all, fields(stage = "dfs.background", worker = "maintenance",
    kind = ?maintenance_kind, outcome = tracing::field::Empty))]
pub(super) async fn run_daemon_dfs_maintenance_tick_kind_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    maintenance_kind: DaemonDfsMaintenanceKind,
    node_resources: sinomo_device::NodeResourceSnapshot,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let kind = match maintenance_kind {
        DaemonDfsMaintenanceKind::Routine => "routine",
        DaemonDfsMaintenanceKind::MountedMutationRecovery => "mounted-mutation-recovery",
        DaemonDfsMaintenanceKind::AuthoritativeReconciliation { .. } => "reconciliation",
    };
    let mut observation =
        crate::background_observation::DfsBackgroundObservation::new("maintenance", kind);
    let result = run_daemon_dfs_maintenance_tick_kind_inner(
        paths,
        runtime,
        maintenance_kind,
        node_resources,
    )
    .await;
    observation.finish(match &result {
        Ok(DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        }) => "complete",
        Ok(DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: true,
        }) => "continuation",
        Ok(DaemonDfsMaintenanceTickOutcome::Deferred) => "deferred",
        Err(_) => "error",
    });
    result
}

async fn run_daemon_dfs_maintenance_tick_kind_inner(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    maintenance_kind: DaemonDfsMaintenanceKind,
    node_resources: sinomo_device::NodeResourceSnapshot,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        });
    }
    let preflight_cluster_state =
        sinomo_core::cluster::read_cluster_state_without_kv_async(runtime, &cluster_paths).await?;
    // Keep the exact proof-bearing authority alive through admission and the
    // complete maintenance pass. The global registry intentionally stores a
    // weak reference, so dropping this guard before the resident cache opens
    // can otherwise replace a successfully warmed projection with a fresh,
    // empty generation under parallel cold roots.
    let initial_metadata_authority_preflight =
        warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &preflight_cluster_state.cluster_id,
            &preflight_cluster_state.local_node_id,
            runtime,
        )
        .await?;
    // Publication can advance the shared configuration's authority fence even
    // when every filesystem and physical store is unchanged. Complete it before
    // capturing the admitted generation. Retain the initial owner through this
    // step so pristine native single-node bootstrap keeps its verified anchor.
    let local_advertisement = cached_daemon_dfs_local_content_advertisement(
        &data_dir,
        &preflight_cluster_state.local_node_id,
        daemon_dfs_local_zone(),
    );
    let mut advertisement_control_plane =
        DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(&cluster_paths, runtime);
    match sinomo_dfs::publish_dfs_content_advertisement_if_needed_async(
        &mut advertisement_control_plane,
        &preflight_cluster_state.local_node_id,
        local_advertisement,
    )
    .await
    {
        Ok(true) => {
            let committed_revision = advertisement_control_plane
                .projection
                .lock()
                .map_err(|_| advertisement_control_plane.projection_lock_error())?
                .revision;
            initial_metadata_authority_preflight
                .authority
                .require_authoritative_revision(committed_revision);
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(
                target: "sinomo.dfs.placement",
                "could not refresh cached DFS storage advertisement: {error}"
            );
        }
    }
    // Archived goal dispatch consumes the ordinary current advertisement cut.
    // Publish it before running callbacks that need real current candidates;
    // callbacks remain outside service ownership and mutation admission.
    let namespace_work = super::maintenance_host::run_namespace_reconciliation_work(paths)?;
    if namespace_work.blocks_ordinary_maintenance {
        return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
    }
    // The normal preflight validates the resulting retained proof, including
    // concurrent filesystem/shard transitions and possibly applied publication.
    let mut metadata_authority_preflight = warm_daemon_dfs_metadata_authority_for_identity_async(
        &cluster_paths,
        &preflight_cluster_state.cluster_id,
        &preflight_cluster_state.local_node_id,
        runtime,
    )
    .await?;
    drop(advertisement_control_plane);
    drop(initial_metadata_authority_preflight);
    // Publish maintenance interest before cluster/local admission. A
    // foreground request that queues anywhere behind this pass can then ask it
    // to yield once its current synchronous poll reaches an await boundary.
    let trace_started = crate::dfs_perf_trace_enabled().then(Instant::now);
    let trace_id = trace_started
        .map(|_| NEXT_MAINTENANCE_TRACE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    let maintenance_span = trace_id.map_or_else(tracing::Span::none, |owner_id| {
        tracing::trace_span!(target: "sinomo.dfs.perf", "dfs-maintenance-owner",
            owner_id, node = %preflight_cluster_state.local_node_id,
            data_root = %data_dir.root().display(), kind = ?maintenance_kind, scope = "global")
    });
    if let Some(owner_id) = trace_id {
        tracing::trace!(target: "sinomo.dfs.perf", owner_id,
            node = %preflight_cluster_state.local_node_id, kind = ?maintenance_kind,
            scope = "global", phase = "requested", "dfs-maintenance-owner-state");
    }
    let mut authority_replans = 0;
    let mut completed_remote_phase = None;
    loop {
        let foreground_preemption =
            daemon_dfs_mutation_scheduler(&data_dir)?.begin_maintenance_interest();
        let Some(admission) = acquire_daemon_dfs_maintenance_admission_until_foreground_waits(
            &data_dir,
            &foreground_preemption,
        )
        .instrument(maintenance_span.clone())
        .await?
        else {
            metrics::counter!("sinomo.dfs.background.deferred", "reason" => "admission")
                .increment(1);
            return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
        };
        let DaemonDfsMaintenanceAdmission {
            _lifecycle_guard: lifecycle_guard,
            _mutation_admission: mutation_admission,
        } = admission;
        let mut retained_admission = Some(mutation_admission);
        let mut publication_scope = match daemon_dfs_data_owner_lock(&DaemonPaths::new(&data_dir))?
        {
            Some(owner) => Some(
                crate::publication_guard::DfsPublicationExecutionScope::enter(
                    data_dir.root(),
                    DfsMessageExecutionScope::GlobalExclusive,
                    &mut retained_admission,
                    owner,
                )?,
            ),
            None => None,
        };
        let trace_hold_started = trace_started.map(|_| Instant::now());
        if let Some(started) = trace_started {
            tracing::trace!(target: "sinomo.dfs.perf", owner_id = trace_id.unwrap_or_default(),
            node = %preflight_cluster_state.local_node_id, kind = ?maintenance_kind,
            scope = "global", phase = "acquired", elapsed_us = started.elapsed().as_micros() as u64,
            "dfs-maintenance-owner-state");
        }
        let mut deferred_work = None;
        let committed_work_tracker = DfsCommittedWorkTracker::new();
        let maintenance_result = run_daemon_dfs_maintenance_until_foreground_waits(
            &foreground_preemption,
            Box::pin(async {
                run_daemon_dfs_maintenance_tick_with_admission(
                    data_dir.clone(),
                    cluster_paths.clone(),
                    runtime,
                    &metadata_authority_preflight,
                    maintenance_kind,
                    node_resources,
                    committed_work_tracker.clone(),
                    &mut deferred_work,
                    &mut completed_remote_phase,
                    publication_scope
                        .as_ref()
                        .map(crate::publication_guard::DfsPublicationExecutionScope::owners),
                    &foreground_preemption,
                )
                .await
            }),
        )
        .instrument(maintenance_span.clone())
        .await;
        // The cancellable broad generation has been collapsed or destroyed at
        // this point. Return its freed pages while maintenance still owns message
        // admission: trimming after releasing admission can contend on glibc
        // arena locks with the foreground request this pass just admitted.
        let allocator_trim_started = Instant::now();
        let allocator_trimmed = release_daemon_dfs_idle_allocator_pages(&data_dir);
        tracing::trace!(
            target: "sinomo.dfs.perf",
            allocator_trimmed,
            allocator_trim_ms = allocator_trim_started.elapsed().as_millis(),
            "dfs maintenance allocator trim"
        );
        // The preemption helper has already completed or dropped the entire
        // maintenance future before global admission is released. Its return
        // owner restores the SAME live service through the checked cache boundary
        // on preemption/error; only quiescent state may have been reset on success.
        // The cancelled work and publication context are gone before handback,
        // while actual fetch/repair custody remains single-generation. Return
        // Deferred below so durable work is retried promptly.
        if let Some(scope) = publication_scope.take() {
            scope.finish()?;
        }
        drop(publication_scope);
        drop(retained_admission);
        drop(lifecycle_guard);
        drop(foreground_preemption);
        if let Some(started) = trace_hold_started {
            tracing::trace!(target: "sinomo.dfs.perf", owner_id = trace_id.unwrap_or_default(),
            node = %preflight_cluster_state.local_node_id, kind = ?maintenance_kind,
            scope = "global", phase = "released", elapsed_us = started.elapsed().as_micros() as u64,
            "dfs-maintenance-owner-state");
        }
        if let Some((detached_runtime, worker)) = deferred_work {
            schedule_daemon_dfs_message_deferred_work(
                runtime,
                &cluster_paths,
                detached_runtime,
                worker,
            );
        }
        // The tracker is owned outside the cancellable maintenance future and is
        // updated at each durable mutation boundary. Transfer those exact bits
        // only after destroying the backend-bearing generation and dropping its
        // guards, even when a later maintenance step failed or was preempted.
        emit_daemon_dfs_committed_edges(
            &data_dir,
            &cluster_paths,
            &DfsMessageExecutionScope::GlobalExclusive,
            runtime,
            None,
            committed_work_tracker.take(),
            &[],
        );
        let outcome = match maintenance_result {
            DaemonDfsMaintenancePreemptionOutcome::Finished(result) => match result? {
                DaemonDfsMaintenancePreExecution::AuthorityPreflightStale(stale) => {
                    if authority_replans >= 2 {
                        return Err(stale.into_dfs_error().into());
                    }
                    authority_replans += 1;
                    // The whole old backend-bearing future and every admission
                    // guard have been dropped above. Preserve this same requested
                    // maintenance kind while the existing authority owner refreshes.
                    daemon_dfs_record_authority_replan(
                        &data_dir,
                        "maintenance",
                        authority_replans,
                        stale,
                    )?;
                    metadata_authority_preflight =
                        warm_daemon_dfs_metadata_authority_for_identity_async(
                            &cluster_paths,
                            &preflight_cluster_state.cluster_id,
                            &preflight_cluster_state.local_node_id,
                            runtime,
                        )
                        .await?;
                    continue;
                }
                DaemonDfsMaintenancePreExecution::RemoteNamespacePhase(request, phase) => {
                    // This is the SAME normal owned phase dispatcher. The broad
                    // service, backend checkout and all admission guards ended above.
                    let completed =
                        super::message_host::dispatch_daemon_dfs_remote_namespace_phase(
                            &data_dir, phase, runtime,
                        )
                        .await?;
                    completed_remote_phase = Some((request, completed));
                    metadata_authority_preflight =
                        warm_daemon_dfs_metadata_authority_for_identity_async(
                            &cluster_paths,
                            &preflight_cluster_state.cluster_id,
                            &preflight_cluster_state.local_node_id,
                            runtime,
                        )
                        .await?;
                    continue;
                }
                DaemonDfsMaintenancePreExecution::Finished(Some(reached_limit)) => {
                    DaemonDfsMaintenanceTickOutcome::Completed { reached_limit }
                }
                DaemonDfsMaintenancePreExecution::Finished(None) => {
                    metrics::counter!("sinomo.dfs.background.deferred", "reason" => "hydration")
                        .increment(1);
                    DaemonDfsMaintenanceTickOutcome::Deferred
                }
            },
            DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting => {
                metrics::counter!("sinomo.dfs.background.deferred", "reason" => "foreground")
                    .increment(1);
                DaemonDfsMaintenanceTickOutcome::Deferred
            }
        };
        if let Err(error) = refresh_daemon_dfs_repair_queue_nonempty(&data_dir) {
            // Retain fail-open wake eligibility. A queue whose snapshot cannot be
            // observed must never be mistaken for proof that repair work is empty.
            record_daemon_dfs_repair_queue_nonempty(&data_dir, true);
            tracing::warn!(
                target: "sinomo.dfs.fault_healing",
                data_root = %data_dir.root().display(),
                error = %error,
                "could not refresh bounded DFS repair queue state after maintenance"
            );
        }
        if matches!(outcome, DaemonDfsMaintenanceTickOutcome::Completed { .. }) {
            // Lifecycle work can delete or compact content without producing a
            // served-message event. One coalesced dirty edge after a completed pass
            // keeps capacity evidence current without a periodic filesystem scan.
            notify_daemon_dfs_storage_environment_dirty(&data_dir);
        }
        return Ok(if namespace_work.complete {
            outcome
        } else {
            DaemonDfsMaintenanceTickOutcome::Deferred
        });
    }
}

#[derive(Debug)]
pub(super) enum DaemonDfsMaintenancePreExecution {
    Finished(Option<bool>),
    RemoteNamespacePhase(
        Box<crate::DfsNamespaceRequestRecord>,
        Box<crate::source_adoption::RemoteNamespacePhase>,
    ),
    AuthorityPreflightStale(DaemonDfsAuthorityPreflightStale),
}

// The cancellable future borrows this actual lane. Dropping that future must
// return live request custody before its outer global admission is released.
struct DaemonDfsMaintenanceServiceReturn<'a> {
    data_dir: &'a SinomoDataDir,
    cluster_paths: &'a ClusterPaths,
    runtime: &'a tokio::runtime::Handle,
    retained: Option<DaemonDfsMaintenanceServiceLane>,
}

impl DaemonDfsMaintenanceServiceReturn<'_> {
    fn restore(&mut self) -> Result<bool, sinomo_dfs::DfsError> {
        let Some(mut retained) = self.retained.take() else {
            return Ok(false);
        };
        // Cancellation bypasses the ordinary post-await cleanup. End only the
        // temporary dispatch mode; the outer worker still owns the same actual
        // checkpointed calls and starts after admission release.
        retained.lane.service.set_background_replica_dispatch(false);
        // This consumes the lane only at the existing checked handback. A
        // changed owner, storage generation or freeze still refuses reuse.
        restore_daemon_dfs_message_mutation_service_after_maintenance(
            self.data_dir,
            self.cluster_paths,
            self.runtime,
            retained,
        )
    }

    fn reset_to_lazy(&mut self) -> Result<(), sinomo_dfs::DfsError> {
        let mut retained = self.retained.take().expect("owned maintenance lane");
        // The caller's complete live-operation predicate has already proven
        // that no request custody is consumed by this existing lazy rebuild.
        retained.lane.service = retained.lane.service.into_lazy_metadata_backend_service()?;
        self.retained = Some(retained);
        Ok(())
    }
}

impl Drop for DaemonDfsMaintenanceServiceReturn<'_> {
    fn drop(&mut self) {
        if self.retained.is_none() {
            return;
        }
        match self.restore() {
            Ok(true) => tracing::debug!(
                data_root = %self.data_dir.root().display(),
                "returned the same daemon DFS service after cancelled or failed maintenance"
            ),
            Ok(false) => tracing::info!(
                data_root = %self.data_dir.root().display(),
                "maintenance service handback refused its changed owner or storage"
            ),
            Err(error) => tracing::warn!(
                data_root = %self.data_dir.root().display(),
                "maintenance service handback failed its existing validation: {error}"
            ),
        }
    }
}

pub(super) async fn run_daemon_dfs_maintenance_tick_with_admission(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    metadata_authority_preflight: &DaemonDfsMetadataAuthorityPreflight,
    maintenance_kind: DaemonDfsMaintenanceKind,
    node_resources: sinomo_device::NodeResourceSnapshot,
    committed_work_tracker: DfsCommittedWorkTracker,
    _deferred_work: &mut Option<(Arc<DaemonDfsDetachedSendRuntime>, DfsNodeDetachedSendWorker)>,
    completed_remote_phase: &mut Option<(
        Box<crate::DfsNamespaceRequestRecord>,
        Box<crate::source_adoption::CompletedRemoteNamespacePhase>,
    )>,
    publication_owners: Option<Arc<crate::publication_guard::DfsPublicationExecutionOwners>>,
    _foreground_preemption: &DaemonDfsMaintenanceForegroundPreemption,
) -> Result<DaemonDfsMaintenancePreExecution, DaemonError> {
    // The scheduler's retained read-only image is useful between probes, but a
    // broad writable generation will decode the same tables. Release it first
    // so accumulated metadata does not double peak RSS during recovery.
    sinomo_dfs::evict_cluster_aware_dfs_metadata_inspection_cache(&data_dir);
    if let Ok(host) = dfs_runtime_host(&data_dir)
        && let Some(activity) = host.activity.as_ref()
    {
        activity.increment(sinomo_module::COMPONENT_ACTIVITY_DISK_READ_DFS_METADATA, 1);
    }
    let (_peer_transport, async_peer_transport) =
        daemon_dfs_peer_transports_on_runtime(&data_dir, runtime);
    let cluster_state =
        sinomo_core::cluster::read_cluster_state_without_kv_async(runtime, &cluster_paths).await?;
    let execution_exclusions =
        daemon_dfs_maintenance_execution_exclusions(&cluster_paths, &cluster_state);
    // Transfer the mutation lane only after re-reading the admitted cluster
    // identity. Topology comes from the retained proof-bearing generation
    // below; maintenance must never refresh or materialize the complete KV
    // store while foreground lanes own the resident Raft node.
    if let Err(error) = daemon_dfs_require_retained_authority(
        &metadata_authority_preflight.authority,
        metadata_authority_preflight.generation,
    ) {
        return match error {
            DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale) => Ok(
                DaemonDfsMaintenancePreExecution::AuthorityPreflightStale(stale),
            ),
            DaemonDfsAuthorityPreExecutionError::Failed(error) => Err(error.into()),
        };
    }
    let retained_lane = match try_take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster_state,
        runtime,
        Some(metadata_authority_preflight),
    ) {
        Ok(lane) => lane,
        Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale)) => {
            return Ok(DaemonDfsMaintenancePreExecution::AuthorityPreflightStale(
                stale,
            ));
        }
        Err(DaemonDfsAuthorityPreExecutionError::Failed(error)) => return Err(error.into()),
    };
    // Declare the real return owner before the publication-context guard:
    // cancellation drops borrowed work and that context before cache handback.
    let mut service_return = DaemonDfsMaintenanceServiceReturn {
        data_dir: &data_dir,
        cluster_paths: &cluster_paths,
        runtime,
        retained: Some(retained_lane),
    };
    let retained = service_return
        .retained
        .as_mut()
        .expect("owned maintenance lane");
    let signature = retained.signature.clone();
    let metadata_authority_generation = Arc::clone(&retained.metadata_authority_generation);
    let peer_node_ids = retained.peer_node_ids.clone();
    let detached_send_runtime = Arc::clone(&retained.detached_send_runtime);
    let mut maintenance_service = &mut retained.lane.service;
    let publication_provider = publication_owners
        .as_ref()
        .map(|owners| {
            crate::DfsPublicationExecutionProvider::from_execution_owners(
                owners,
                Arc::clone(&metadata_authority_generation),
                runtime.clone(),
            )
        })
        .transpose()?;
    let publication_scope =
        publication_owners.map(|owners| maintenance_service.publication_execution.install(owners));
    // Release this strong owner before returning the reusable lane to cache.
    let publication_provider_scope = publication_provider
        .map(|provider| maintenance_service.publication_provider.install(provider));
    let placement_advertisements = maintenance_service
        .placement_content_candidates()
        .unwrap_or_default();
    refresh_daemon_dfs_placement_snapshot(
        &data_dir,
        &cluster_paths,
        &cluster_state.local_node_id,
        placement_advertisements,
    );
    let authority_generation_before_maintenance = signature.metadata_authority_generation;
    let authoritative_topology = match metadata_authority_generation
        .retained_complete_prefix_for_generation(authority_generation_before_maintenance)
    {
        Some(projection) => projection,
        None => {
            let error = daemon_dfs_stale_authority(
                &metadata_authority_generation,
                authority_generation_before_maintenance,
            );
            return match error {
                DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale) => Ok(
                    DaemonDfsMaintenancePreExecution::AuthorityPreflightStale(stale),
                ),
                DaemonDfsAuthorityPreExecutionError::Failed(error) => Err(error.into()),
            };
        }
    };
    maintenance_service.set_committed_work_tracker(committed_work_tracker.clone());
    let defer_replica_dispatch = maintenance_kind.replica_dispatch_is_deferred();
    maintenance_service.set_background_replica_dispatch(defer_replica_dispatch);
    if defer_replica_dispatch {
        // This holder lives outside the cancellable future. The transport
        // moves payloads into it synchronously after repair-queue persistence,
        // before yielding; the outer owner dispatches only after guard release.
        // Regular reconciliation first checkpoints its discovered recovery
        // work below, then uses the same transport handoff as routine repair.
        // Explicit GC/reservation/promotion recovery retains its synchronous
        // completion contract.
        *_deferred_work = Some((
            Arc::clone(&detached_send_runtime),
            maintenance_service.prepare_background_replica_worker(),
        ));
    }
    let mut recovery_handed_off = false;
    let maintenance_result = async {
        let options = match maintenance_kind {
            DaemonDfsMaintenanceKind::Routine => {
                daemon_dfs_routine_maintenance_tick_options(node_resources)
            }
            DaemonDfsMaintenanceKind::AuthoritativeReconciliation { .. }
            | DaemonDfsMaintenanceKind::MountedMutationRecovery => {
                let (content_gc_requested, content_reservation_reconciliation_requested, commit_promotion_recovery_requested) = match maintenance_kind {
                    DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
                        content_gc_requested, content_reservation_reconciliation_requested, commit_promotion_recovery_requested,
                    } => (content_gc_requested, content_reservation_reconciliation_requested, commit_promotion_recovery_requested),
                    DaemonDfsMaintenanceKind::MountedMutationRecovery => (false, false, false),
                    DaemonDfsMaintenanceKind::Routine => unreachable!("routine handled above"),
                };
                let recovery_options = if maintenance_kind == DaemonDfsMaintenanceKind::MountedMutationRecovery {
                    crate::dfs_mounted_mutation_recovery_tick_options()
                } else {
                    daemon_dfs_explicit_disaster_recovery_tick_options(
                        content_gc_requested,
                        content_reservation_reconciliation_requested,
                    )
                };
                let request_generation =
                    daemon_dfs_maintenance_reconciliation_generation(&DaemonPaths::new(&data_dir));
                recovery_handed_off = !content_gc_requested
                    && !content_reservation_reconciliation_requested
                    && !commit_promotion_recovery_requested
                    && super::maintenance_host::daemon_dfs_reconciliation_checkpoint_matches(
                        &DaemonPaths::new(&data_dir), request_generation,
                        authority_generation_before_maintenance,
                    );
                if !recovery_handed_off {
                    // Recovery can publish real prepares. Use the SAME normal
                    // freeze/ACK/joint-activation/rebind path before hydration.
                    let recovery_shards = crate::metadata_shards_from_authority_projection(
                        &authoritative_topology, &crate::sifs_file_system_id(),
                    ).map_err(DaemonError::from)?;
                    match maintenance_service.reconcile_pending_metadata_shard_membership_handoffs_until_cancelled(
                        &recovery_shards, recovery_options.metadata_replication.target_timeout,
                        &|| {
                            _foreground_preemption.is_requested()
                                || metadata_authority_generation
                                    .retained_complete_prefix_revision_for_generation(
                                        authority_generation_before_maintenance,
                                    )
                                    .is_none()
                        },
                    ) {
                        Ok(Some(_)) => {},
                        Ok(None) => return Ok(None),
                        Err(crate::DfsError::MetadataShardMembershipHandoffParked { .. }) => return Ok(None),
                        Err(error) => return Err(DaemonError::from(error)),
                    }
                    // Activation can invalidate the admitted topology cut.
                    // Leave this recovery generation pending for its normal wake.
                    if metadata_authority_generation
                        .retained_complete_prefix_revision_for_generation(authority_generation_before_maintenance)
                        .is_none()
                    {
                        return Ok(None);
                    }
                    let backend = maintenance_service.metadata_backend.as_ref()
                        .ok_or_else(|| DaemonError::from(crate::DfsError::Metadata {
                            message: "Main recovery requires its actual durable backend".into(),
                        }))?;
                    for shard in recovery_shards.iter().filter(|shard|
                        shard.primary_node_id == cluster_state.local_node_id)
                    {
                        if backend.metadata_shard_authoritative_writes_frozen(
                            &crate::DfsMetadataShardId::new(shard.shard_id.clone()),
                        ).map_err(DaemonError::from)? {
                            return Ok(None);
                        }
                    }
                    // Unrelated cluster revisions do not invalidate this scan. A
                    // DFS authority change, incomplete proof or foreground waiter
                    // still cancels before the completed local handoff.
                    if !maintenance_service
                        .hydrate_from_metadata_backend_for_commit_promotion_recovery_until_cancelled(
                            &|| {
                                _foreground_preemption.is_requested()
                                    || metadata_authority_generation
                                        .retained_complete_prefix_revision_for_generation(
                                            authority_generation_before_maintenance,
                                        )
                                        .is_none()
                            },
                        )
                        .map_err(DaemonError::from)?
                    {
                        return Ok(None);
                    }
                    recovery_handed_off =
                        super::maintenance_host::checkpoint_daemon_dfs_reconciliation(
                            &data_dir,
                            &metadata_authority_generation,
                            authority_generation_before_maintenance,
                            request_generation,
                            maintenance_service.durable_repair_queue_records(),
                        )?;
                    if !recovery_handed_off {
                        return Ok(None);
                    }
                    detached_send_runtime
                        .record_recovery_scan_generation(signature.metadata_storage_generation);
                }
                recovery_options
            }
        };
        if let Some((request, completed)) = completed_remote_phase.take() {
            maintenance_service.complete_remote_namespace_phase(*completed)?;
            maintenance_service.pending_namespace_request_recovery = Some(request);
            // Resume only the original durable request. Ending this continuation
            // leaves every other bounded phase for the next ordinary tick, rather
            // than resetting its work budgets after each remote response.
            maintenance_service.recover_pending_namespace_requests_bounded(1)?;
            return Ok(None);
        }
        // Both kinds now execute the same bounded phases. Recovery has already
        // durably transferred discovered work before this future can yield to
        // foreground RPCs (including replies from its own promotion outbox).
        sinomo_dfs::run_cluster_dfs_reactive_maintenance_tick_with_reusable_service(
            &mut maintenance_service,
            data_dir.clone(),
            cluster_paths.clone(),
            cluster_state,
            authoritative_topology,
            async_peer_transport,
            peer_node_ids.clone(),
            execution_exclusions,
            committed_work_tracker,
            options,
        )
        .await
        .map_err(DaemonError::from)
    }
    .await;
    let remote_namespace_phase =
        if let Some(phase) = maintenance_service.remote_namespace_phase.take() {
            let request = maintenance_service
                .pending_namespace_request_recovery
                .take()
                .ok_or_else(|| {
                    DaemonError::from(crate::DfsError::Metadata {
                        message: "owned recovery phase has no exact durable request continuation"
                            .into(),
                    })
                })?;
            Some((request, phase))
        } else {
            None
        };
    if matches!(&maintenance_result, Ok(Some(_)))
        && let Some((generation, jobs)) =
            super::maintenance_host::daemon_dfs_requested_namespace_job_compaction(
                &DaemonPaths::new(&data_dir),
            )
    {
        let options = crate::DfsMetadataHistoryGcOptions::delete_eligible(
            crate::DFS_RESIDENT_METADATA_HISTORY_GC_MIN_AGE,
            crate::DFS_RESIDENT_METADATA_HISTORY_GC_KEEP_LATEST_VERSIONS,
        )
        .with_max_removals(jobs.len() as u64)
        .with_namespace_job_ids(jobs);
        let report = maintenance_service.gc_metadata_history(options)?;
        if report.applied_record_count() > 0 {
            maintenance_service.record_message_committed_work(DfsMessageCommittedWork {
                publish_metadata: true,
                ..DfsMessageCommittedWork::default()
            });
        }
        tracing::info!(target: "sinomo.dfs.maintenance",
            compacted_namespace_jobs = report.compacted_namespace_job_count,
            "completed requested terminal namespace job retention pass");
        super::maintenance_host::complete_daemon_dfs_namespace_job_compaction(
            &DaemonPaths::new(&data_dir),
            generation,
        )?;
    }
    maintenance_service.set_background_replica_dispatch(false);
    if !recovery_handed_off
        && maintenance_result.is_err()
        && matches!(
            maintenance_kind,
            DaemonDfsMaintenanceKind::AuthoritativeReconciliation { .. }
                | DaemonDfsMaintenanceKind::MountedMutationRecovery
        )
    {
        detached_send_runtime.reset_recovery_scan_generation();
    }
    // The bounded repair phase has persisted its attempt reservations before
    // returning. Transfer only immutable payloads and request identities; the
    // outer owner schedules this worker after dropping every admission guard.
    if matches!(&maintenance_result, Ok(Some(_)))
        && maintenance_service
            .message_deferred_work_pending()
            .map_err(DaemonError::from)?
    {
        *_deferred_work = Some((
            Arc::clone(&detached_send_runtime),
            maintenance_service.detach_message_deferred_work(),
        ));
    }
    // Rebuild only a genuinely quiescent service. Explicit recovery is not a
    // process restart and cannot discard live fetch/repair or namespace custody.
    drop(publication_provider_scope);
    drop(publication_scope);
    let preserve_remote_continuation = remote_namespace_phase.is_some();
    let reset_to_lazy = !preserve_remote_continuation
        && matches!(&maintenance_result, Ok(Some(_)))
        && matches!(
            maintenance_service.authoritative_write_frozen_shard_count(),
            Ok(0)
        )
        && maintenance_service.can_release_hydrated_metadata_projection();
    if reset_to_lazy {
        if let Err(error) = service_return.reset_to_lazy() {
            tracing::warn!(
                data_root = %data_dir.root().display(),
                "dropping quiescent daemon DFS service after lazy reset failed: {error}"
            );
        }
    }
    if matches!(
        maintenance_kind,
        DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
            commit_promotion_recovery_requested: true,
            ..
        }
    ) {
        let promotion_recovery_started = Instant::now();
        match prepare_daemon_dfs_commit_promotion_recovery(&data_dir, &cluster_paths, runtime) {
            Ok(DaemonDfsCommitPromotionRecovery {
                detached_runtime,
                worker,
                scan_result,
            }) => {
                if _deferred_work.is_none() {
                    *_deferred_work = Some((detached_runtime, worker));
                }
                if let Err(error) = scan_result {
                    tracing::warn!(
                        "sinomo daemon DFS commit-promotion disaster-recovery scan remains pending: {error}"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    "sinomo daemon DFS commit-promotion reconciliation remains pending: {error}"
                );
            }
        }
        daemon_dfs_perf_trace(format_args!(
            "commit-promotion-disaster-recovery maintenance_ms={}",
            promotion_recovery_started.elapsed().as_millis()
        ));
    }
    let restored = service_return.restore();
    if preserve_remote_continuation {
        // No remote dispatch after a failed/foreign restoration. The existing
        // actor remains the physical source/job custody owner.
        if !restored? {
            return Err(DaemonError::from(crate::DfsError::Metadata {
                message: "owned namespace recovery could not restore its current service".into(),
            }));
        }
    } else {
        match restored {
            Ok(true) => tracing::debug!(
                data_root = %data_dir.root().display(),
                "restored daemon DFS mutation service after maintenance"
            ),
            Ok(false) => tracing::info!(
                data_root = %data_dir.root().display(),
                "dropped daemon DFS mutation service after maintenance identity changed"
            ),
            Err(error) => tracing::warn!(
                data_root = %data_dir.root().display(),
                "dropping daemon DFS mutation service after maintenance validation failed: {error}"
            ),
        }
    }
    if let Some((request, phase)) = remote_namespace_phase {
        return Ok(DaemonDfsMaintenancePreExecution::RemoteNamespacePhase(
            request, phase,
        ));
    }
    let report = maintenance_result?;
    let Some(report) = report else {
        // A cancellable hydrate which yields no report did not complete a
        // durable owner pass. Surface it as Deferred so mounted-mutation
        // recovery claims remain non-terminal.
        return Ok(DaemonDfsMaintenancePreExecution::Finished(None));
    };
    let reached_limit = daemon_dfs_maintenance_report_reached_limit(&report);
    let observations = sinomo_dfs::dfs_maintenance_tick_observations(&report);
    tracing::debug!(
        target: "sinomo.dfs.maintenance",
        metadata_replications_sent = observations.metadata_replications_sent,
        repair_attempt_count = observations.repair_attempt_count,
        repair_attempt_in_flight_count = observations.repair_attempt_in_flight_count,
        repair_attempt_backoff_count = observations.repair_attempt_backoff_count,
        repair_attempt_suppressed_count = observations.repair_attempt_suppressed_count,
        scheduled_repair_count = report.scheduled_repairs.len(),
        followup_event_count = report.followup_events.len(),
        namespace_jobs_scanned = report.namespace_jobs.scanned_job_count,
        namespace_jobs_resumed = report.namespace_jobs.resumed_job_count,
        intent_records_visited = report.write_intents.visited_intent_record_count,
        intent_scan_has_more = report.write_intents.scan_has_more,
        placement_state = ?report.placement_reconciliation.as_ref().map(|job| &job.state),
        bounded_continuation = reached_limit,
        "DFS maintenance tick completed"
    );
    if let Ok(host) = dfs_runtime_host(&data_dir)
        && let Some(activity) = host.activity.as_ref()
    {
        activity.increment(
            sinomo_module::COMPONENT_ACTIVITY_DFS_METADATA_REPLICATION_SENT,
            observations.metadata_replications_sent as u64,
        );
    }
    for line in observations.notice_lines {
        tracing::warn!("sinomo daemon {line}");
    }
    Ok(DaemonDfsMaintenancePreExecution::Finished(Some(
        reached_limit,
    )))
}

/// Whether this pass itself proved that it stopped with bounded, immediately
/// continuable work.
///
/// Hitting a scan count is not enough: an exact-sized workload and a newly
/// arrived record are indistinguishable from counts alone. Likewise, a
/// running namespace job may have made no progress while waiting on external
/// state. Only durable cursors or explicit retained-limit reports, paired with
/// evidence that this pass advanced, may bypass the ordinary maintenance gate.
pub(super) fn daemon_dfs_maintenance_report_reached_limit(
    report: &DfsMaintenanceTickReport,
) -> bool {
    sinomo_dfs::dfs_maintenance_report_reached_limit(report)
}

/// Run the complete DFS maintenance future from Tokio's blocking pool.
///
/// The reusable maintenance entrypoint is async because metadata replication
/// uses async transports, but it also performs substantial synchronous
/// backend recovery and metadata scans between awaits. Polling that future on
/// a daemon worker can starve unrelated control traffic on slower nodes.
pub(crate) async fn run_daemon_dfs_maintenance_tick_in_blocking_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    node_resources: sinomo_device::NodeResourceSnapshot,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let tick_runtime = runtime.clone();
    let data_owner_lock = daemon_dfs_data_owner_lock(&paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(&paths)?);
    let task_owner = registered_dfs_runtime_host(&data_dir)?.ok_or_else(|| DaemonError::Parse {
        detail: "DFS maintenance has no registered runtime task owner".into(),
    })?;
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let result = runtime.block_on(async move {
            if *shutdown.borrow() {
                return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
            }
            tokio::select! {
                result = run_daemon_dfs_maintenance_tick_on_runtime(
                    &paths,
                    &tick_runtime,
                    node_resources,
                ) => result,
                _ = shutdown.changed() => Ok(DaemonDfsMaintenanceTickOutcome::Deferred),
            }
        });
        // Match the prior JoinHandle result boundary only after the actual
        // maintenance future and its retained OS owner have finished.
        drop(data_owner_lock);
        let _ = result_tx.send(result);
    });
    // A stopped component's outer async task must not detach this running
    // blocking owner. Retain the same child in the normal host task drain.
    task_owner.own_task(task);
    result_rx.await.map_err(|source| DaemonError::Network {
        address: "DFS maintenance".to_string(),
        detail: format!("DFS maintenance blocking task ended without a result: {source}"),
    })?
}

pub(super) fn daemon_dfs_routine_maintenance_tick_options(
    node_resources: sinomo_device::NodeResourceSnapshot,
) -> DfsMaintenanceTickOptions {
    let mut options = sinomo_dfs::dfs_resident_routine_maintenance_tick_options();
    if daemon_dfs_background_network_work_deferred(node_resources) {
        // Local intent/namespace/handoff recovery remains active. Background
        // data-plane repair, placement migration, and metadata catch-up wait
        // for an exact resource-change wake or an explicit operator recovery.
        // Metered links receive the same protection even when battery and
        // thermal conditions alone would otherwise be merely constrained.
        options.repair_max_jobs = 0;
        options.placement_reconciliation_max_objects = 0;
        options.metadata_replication.max_batches_per_target = 0;
    } else if node_resources.mode == sinomo_device::NodeResourceMode::Constrained {
        // An unmetered node under battery, power-saver, congestion, or thermal
        // pressure can still make bounded healing progress without running a
        // workstation-sized maintenance batch.
        options.repair_max_jobs = options.repair_max_jobs.min(1);
        options.placement_reconciliation_max_objects =
            options.placement_reconciliation_max_objects.min(16);
        options.metadata_replication.max_batches_per_target =
            options.metadata_replication.max_batches_per_target.min(1);
    }
    options
}

pub(crate) fn daemon_dfs_background_network_work_deferred(
    node_resources: sinomo_device::NodeResourceSnapshot,
) -> bool {
    node_resources.mode == sinomo_device::NodeResourceMode::Restricted
        || node_resources.network_metering == sinomo_device::NodeNetworkMetering::Metered
}

pub(super) fn daemon_dfs_explicit_disaster_recovery_tick_options(
    content_gc_requested: bool,
    content_reservation_reconciliation_requested: bool,
) -> DfsMaintenanceTickOptions {
    sinomo_dfs::dfs_explicit_disaster_recovery_tick_options(
        content_gc_requested,
        content_reservation_reconciliation_requested,
    )
}
