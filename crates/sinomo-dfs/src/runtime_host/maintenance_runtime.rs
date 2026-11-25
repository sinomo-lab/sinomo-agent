use super::*;
use tracing::Instrument;

static NEXT_MAINTENANCE_TRACE_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

pub(crate) type DaemonDfsMaintenanceTickOutcome = sinomo_dfs::DfsMaintenanceTickOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DaemonDfsMaintenanceKind {
    Routine,
    /// Only the repair executor's pass, for a wake that concerns only it.
    RepairExecutor,
    MountedMutationRecovery,
    AuthoritativeReconciliation {
        content_gc_requested: bool,
        content_reservation_reconciliation_requested: bool,
        commit_promotion_recovery_requested: bool,
    },
}

impl DaemonDfsMaintenanceKind {
    fn retains_requested_recovery(self) -> bool {
        // These callers already own an unfinished reconciliation/recovery
        // request. Foreground preemption cannot convert it into idle routine
        // work merely because its first phase has not reached a checkpoint.
        !matches!(self, Self::Routine | Self::RepairExecutor)
    }

    pub(super) fn replica_dispatch_is_deferred(self) -> bool {
        matches!(
            self,
            Self::Routine
                | Self::RepairExecutor
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
        DaemonDfsMaintenanceKind::RepairExecutor => "repair-executor",
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
        Ok(DaemonDfsMaintenanceTickOutcome::Continued) => "retained-continuation",
        Ok(DaemonDfsMaintenanceTickOutcome::Deferred) => "deferred",
        Err(_) => "error",
    });
    result
}

/// The coordinator owns the KV job, while each read and mutation is admitted
/// by its shard owner. A remote primary can therefore call back into any shard
/// on this node while the page is outstanding.
async fn run_placement_page_without_admission(
    data: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    preflight: &DaemonDfsMetadataAuthorityPreflight,
    local: &NodeId,
    budget: usize,
) -> Result<bool, DaemonError> {
    let Some(snapshot) = preflight
        .authority
        .retained_complete_prefix_for_generation(preflight.generation)
    else {
        // Projection publication will wake maintenance again.
        return Ok(true);
    };
    let fs = crate::sifs_file_system_id();
    let shards = crate::metadata_shards_from_authority_projection(&snapshot, &fs)?;
    let root =
        crate::control_plane::select_metadata_shard_config_for_path_strict_default_sifs_split(
            &fs, &shards, "/",
        )?;
    if !root.is_some_and(|shard| &shard.primary_node_id == local) {
        return Ok(false);
    }
    let cache = daemon_dfs_message_service_cache().root(&data.root())?;
    let resident = cache
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?
        .as_ref()
        .filter(|entry| {
            Arc::ptr_eq(&entry.metadata_authority_generation, &preflight.authority)
                && entry.signature.metadata_authority_generation == preflight.generation
        })
        .and_then(|entry| {
            entry
                .resident_metadata_backend
                .clone()
                .map(|resident| (resident, entry.signature.metadata_storage_generation))
        });
    let Some((resident, storage_generation)) = resident else {
        return Ok(true);
    };
    let planning = crate::placement::DfsPlacementPageRead {
        resident,
        scheduler: daemon_dfs_mutation_scheduler(data)?,
        authority: Arc::clone(&preflight.authority),
        generation: preflight.generation,
        storage_generation,
    };
    let mut caller = open_dfs_runtime_service(data, Some(runtime))?;
    let pending = crate::repair_executor::repair_executor_work(&data.root()).need_count;
    let page = caller
        .run_placement_page_with_pending_repairs(budget, pending, &shards, Some(&planning))
        .await?;
    Ok(page
        .as_ref()
        .is_some_and(crate::maintenance::placement_page_can_continue))
}

/// A warm executor consumes the indexed work of this node's primary shards.
/// Cold cache initialization and explicit node recovery keep their normal owner.
async fn run_scoped_repair_turn(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
    preflight: &DaemonDfsMetadataAuthorityPreflight,
    authority_epoch: u64,
    resources: sinomo_device::NodeResourceSnapshot,
) -> Result<Option<DaemonDfsMaintenanceTickOutcome>, DaemonError> {
    let limit = daemon_dfs_routine_maintenance_tick_options(resources)
        .repair_max_jobs
        .min(crate::service_repair::DFS_REPAIR_EXAMINED_RECORDS_PER_PAGE);
    if limit == 0 {
        return Ok(Some(DaemonDfsMaintenanceTickOutcome::Completed {
            reached_limit: false,
        }));
    }
    let Some(projection) = preflight
        .authority
        .retained_complete_prefix_for_generation(preflight.generation)
    else {
        return Ok(None);
    };
    let shards = crate::metadata_shards_from_authority_projection(
        &projection,
        &crate::sifs_file_system_id(),
    )?;
    let primaries = crate::repair_executor::local_primary_shards(&shards, &state.local_node_id);
    #[cfg(test)]
    crate::maintenance::phase_runs::record(
        &data_dir.root(),
        crate::maintenance::phase_runs::EXECUTOR_TURN,
    );
    let work = crate::repair_executor::repair_turn_shards(&data_dir.root(), &primaries);
    if work.is_empty() {
        return Ok(
            if crate::repair_executor::repair_executor_work(&data_dir.root()).due {
                None // The final primary was lost. Retire the old scheduler state.
            } else {
                Some(DaemonDfsMaintenanceTickOutcome::Completed {
                    reached_limit: false,
                })
            },
        );
    }
    let keys = work
        .iter()
        .map(|id| DaemonDfsMutationLaneKey {
            file_system_id: crate::sifs_file_system_id(),
            shard_id: crate::DfsMetadataShardId::new(id.clone()),
        })
        .collect::<Vec<_>>();
    let Some(peer) = read_daemon_dfs_message_peer_snapshot(cluster_paths)? else {
        return Ok(None);
    };
    if peer.cluster_id != state.cluster_id || peer.local_node_id != state.local_node_id {
        return Ok(None);
    }
    let exclusions = daemon_dfs_maintenance_execution_exclusions(cluster_paths, state)
        .with_repair_authority_epoch(authority_epoch);
    let scheduler = daemon_dfs_mutation_scheduler(data_dir)?;
    let foreground = scheduler.begin_maintenance_interest();
    let Some(admission) = crate::acquire_dfs_maintenance_admission_until_foreground_waits(
        &foreground,
        scheduler.admit_shards(&keys),
    )
    .await?
    else {
        return Ok(Some(DaemonDfsMaintenanceTickOutcome::Deferred));
    };
    if admission.is_global() {
        return Ok(None);
    }
    for key in &keys {
        if daemon_dfs_message_cache_exact_admission_handle(
            data_dir,
            cluster_paths,
            &peer,
            key,
            runtime,
        )?
        .is_none()
        {
            return Ok(None);
        }
    }
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight,
            None,
            Some(preflight),
        )?;
    }
    let (handle, _) = daemon_dfs_message_lane(
        data_dir,
        cluster_paths,
        &peer,
        runtime,
        DaemonDfsMessageLaneKind::MutationShards(keys),
        false,
        Some(preflight),
    )?;
    let mut admission = Some(admission);
    let scope = DfsMessageExecutionScope::NamespacePaths {
        file_system_id: crate::sifs_file_system_id(),
        paths: Vec::new(),
    };
    let publication = crate::publication_guard::DfsPublicationExecutionScope::enter(
        data_dir.root(),
        scope.clone(),
        &mut admission,
        require_daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir))?,
    )?;
    let tracker = DfsCommittedWorkTracker::new();
    let mut lane = handle.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    let Some(metadata @ DaemonDfsResidentMetadataScope::Shards(..)) =
        lane.resident_metadata_scope.clone()
    else {
        return Ok(None);
    };
    let provider = crate::DfsPublicationExecutionProvider::from_execution_owners(
        &publication.owners(),
        Arc::clone(&preflight.authority),
        runtime.clone(),
    )?;
    lane.attach_metadata_scope(&metadata)?;
    let service = &mut lane.service;
    let provider = service.publication_provider.install(provider);
    let owners = service.publication_execution.install(publication.owners());
    let previous = service.committed_work_tracker.replace(tracker.clone());
    service.set_background_replica_dispatch(true);
    let result = service.run_repair_executor_pass_on_shards(
        &data_dir.root(),
        &shards,
        &exclusions,
        limit,
        Some(&work),
    );
    let worker = service
        .message_deferred_work_pending()
        .map(|pending| pending.then(|| service.detach_message_deferred_work()));

    service.set_background_replica_dispatch(false);
    service.committed_work_tracker = previous;
    drop(owners);
    drop(provider);
    let release = service.release_resident_metadata_backend();
    drop(lane);
    let finish = publication.finish();
    drop(admission);
    emit_daemon_dfs_committed_edges(
        data_dir,
        cluster_paths,
        &scope,
        runtime,
        None,
        tracker.take(),
        &[],
    );
    if let Some(worker) = worker? {
        schedule_daemon_dfs_message_deferred_work(
            runtime,
            cluster_paths,
            Arc::clone(handle.detached_send_runtime()),
            worker,
        );
    }
    release?;
    finish?;
    Ok(Some(DaemonDfsMaintenanceTickOutcome::Completed {
        reached_limit: result?.has_more,
    }))
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
    let executor_only = maintenance_kind == DaemonDfsMaintenanceKind::RepairExecutor;
    // Read before the turn captures its authority view: an authority event
    // counted after this is newer than the view the executor dispatches with.
    let repair_authority_epoch =
        crate::repair_executor::repair_executor_authority_epoch(&data_dir.root());
    // An executor-only turn serves the published assignment it finds. It does
    // not wait for a configuration that is not ready yet: it yields, and the
    // turn after the configuration's publication serves the same pending work.
    let initial_metadata_authority_preflight = if executor_only {
        match warm_daemon_dfs_repair_executor_authority(
            &cluster_paths,
            &preflight_cluster_state,
            runtime,
        )
        .await?
        {
            Some(preflight) => preflight,
            None => {
                crate::repair_executor::repair_executor_await_publication(&data_dir.root());
                return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
            }
        }
    } else {
        warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &preflight_cluster_state.cluster_id,
            &preflight_cluster_state.local_node_id,
            runtime,
        )
        .await?
    };
    if executor_only
        && let Some(outcome) = run_scoped_repair_turn(
            &data_dir,
            &cluster_paths,
            &preflight_cluster_state,
            runtime,
            &initial_metadata_authority_preflight,
            repair_authority_epoch,
            node_resources,
        )
        .await?
    {
        return Ok(outcome);
    }
    // Publication can advance the shared configuration's authority fence even
    // when every filesystem and physical store is unchanged. Complete it before
    // capturing the admitted generation. Retain the initial owner through this
    // step so pristine native single-node bootstrap keeps its verified anchor.
    let local_advertisement = cached_daemon_dfs_local_content_advertisement(
        &data_dir,
        &preflight_cluster_state.local_node_id,
        daemon_dfs_local_zone(&data_dir),
    );
    let mut advertisement_control_plane =
        DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(&cluster_paths, runtime);
    // An executor-only turn publishes nothing; the advertisement keeps the
    // triggers of the ordinary turns.
    match if executor_only {
        Ok(false)
    } else {
        sinomo_dfs::publish_dfs_content_advertisement_if_needed_async(
            &mut advertisement_control_plane,
            &preflight_cluster_state.local_node_id,
            local_advertisement,
        )
        .await
    } {
        Ok(true) => {
            let committed_revision = advertisement_control_plane.observed_revision();
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
    let namespace_work = if executor_only {
        super::maintenance_host::NamespaceReconciliationRun {
            complete: true,
            blocks_ordinary_maintenance: false,
        }
    } else {
        super::maintenance_host::run_namespace_reconciliation_work(paths)?
    };
    if namespace_work.blocks_ordinary_maintenance {
        return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
    }
    // The normal preflight validates the resulting retained proof, including
    // concurrent filesystem/shard transitions and possibly applied publication.
    let mut metadata_authority_preflight = if executor_only {
        match warm_daemon_dfs_repair_executor_authority(
            &cluster_paths,
            &preflight_cluster_state,
            runtime,
        )
        .await?
        {
            Some(preflight) => preflight,
            None => {
                crate::repair_executor::repair_executor_await_publication(&data_dir.root());
                return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
            }
        }
    } else {
        warm_daemon_dfs_metadata_authority_for_identity_async(
            &cluster_paths,
            &preflight_cluster_state.cluster_id,
            &preflight_cluster_state.local_node_id,
            runtime,
        )
        .await?
    };
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
        // Snapshot only the existing scheduling owner before waiting for
        // admission. No backend/lane/admission is retained by this handle.
        let saved_turn = retained_daemon_dfs_maintenance_turn(
            &data_dir,
            metadata_authority_preflight.generation,
        )?;
        let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
        let foreground_preemption = scheduler.begin_maintenance_interest();
        // The first admission released after the one this turn yields to, or
        // after this turn's own, is the earliest moment the foreground work it
        // gave way to can have finished.
        let admission_releases = scheduler.subscribe_admission_releases();
        let released_before_admission = *admission_releases.borrow();
        let Some(admission) = acquire_daemon_dfs_maintenance_admission_until_foreground_waits(
            &data_dir,
            &foreground_preemption,
        )
        .instrument(maintenance_span.clone())
        .await?
        else {
            metrics::counter!("sinomo.dfs.background.deferred", "reason" => "admission")
                .increment(1);
            wake_daemon_dfs_maintenance_after_admission_release(
                &data_dir,
                admission_releases,
                released_before_admission,
                runtime,
            );
            // Reads still preempt admission. Keep the caller's actual pending
            // recovery request, including before its first phase or after an
            // authority generation invalidated the old phase checkpoint.
            return Ok(
                if maintenance_kind.retains_requested_recovery()
                    || saved_turn
                        .as_ref()
                        .map(|owner| owner.has_unfinished_turn())
                        .transpose()?
                        .unwrap_or(false)
                {
                    DaemonDfsMaintenanceTickOutcome::Continued
                } else {
                    DaemonDfsMaintenanceTickOutcome::Deferred
                },
            );
        };
        // Nothing else releases an admission while this turn holds the node.
        let released_with_this_turn = *admission_releases.borrow() + 1;
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
        let mut handed_off_reads = Vec::new();
        let mut turn_progress = None;
        let mut placement_budget = 0;
        let committed_work_tracker = DfsCommittedWorkTracker::new();
        let maintenance_result = run_daemon_dfs_maintenance_until_foreground_waits(
            &foreground_preemption,
            Box::pin(async {
                run_daemon_dfs_maintenance_tick_with_admission(
                    data_dir.clone(),
                    cluster_paths.clone(),
                    preflight_cluster_state.clone(),
                    runtime,
                    &metadata_authority_preflight,
                    repair_authority_epoch,
                    maintenance_kind,
                    node_resources,
                    committed_work_tracker.clone(),
                    &mut deferred_work,
                    &mut turn_progress,
                    &mut placement_budget,
                    &mut completed_remote_phase,
                    &mut handed_off_reads,
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
        let retained_progress = match turn_progress.as_ref() {
            Some((owner, before)) => owner.unfinished_progress_since(*before),
            None => Ok(false),
        };
        let preempted_turn_pending = turn_progress
            .as_ref()
            .map(|(owner, _)| owner)
            .or(saved_turn.as_ref())
            .map(|owner| owner.has_unfinished_turn())
            .transpose()?
            .unwrap_or(false);
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
        let yielded_to_foreground = foreground_preemption.is_requested();
        drop(foreground_preemption);
        if yielded_to_foreground {
            wake_daemon_dfs_maintenance_after_admission_release(
                &data_dir,
                admission_releases,
                released_with_this_turn,
                runtime,
            );
        }
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
        send_daemon_dfs_handed_off_reads(&data_dir, handed_off_reads, runtime);
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
        // A bookkeeping error must not discard already-owned payload work or
        // committed edges. Propagate it only after their normal handoff.
        let retained_progress = retained_progress?;
        let outcome = match maintenance_result {
            DaemonDfsMaintenancePreemptionOutcome::Finished(result) => match result? {
                DaemonDfsMaintenancePreExecution::AuthorityPreflightStale(_)
                    if executor_only =>
                {
                    // The assignment moved during the turn: the next turn
                    // serves it from the newer publication.
                    crate::repair_executor::repair_executor_await_publication(&data_dir.root());
                    DaemonDfsMaintenanceTickOutcome::Deferred
                }
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
                            &data_dir, phase, None, runtime,
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
                    if retained_progress {
                        DaemonDfsMaintenanceTickOutcome::Continued
                    } else {
                        DaemonDfsMaintenanceTickOutcome::Deferred
                    }
                }
            },
            DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting => {
                metrics::counter!("sinomo.dfs.background.deferred", "reason" => "foreground")
                    .increment(1);
                if maintenance_kind.retains_requested_recovery()
                    || retained_progress
                    || preempted_turn_pending
                {
                    DaemonDfsMaintenanceTickOutcome::Continued
                } else {
                    DaemonDfsMaintenanceTickOutcome::Deferred
                }
            }
        };
        let outcome = if placement_budget > 0
            && run_placement_page_without_admission(
                &data_dir,
                runtime,
                &metadata_authority_preflight,
                &preflight_cluster_state.local_node_id,
                placement_budget,
            )
            .await?
        {
            match outcome {
                DaemonDfsMaintenanceTickOutcome::Completed { .. } => {
                    DaemonDfsMaintenanceTickOutcome::Completed {
                        reached_limit: true,
                    }
                }
                other => other,
            }
        } else {
            outcome
        };
        if !executor_only && matches!(outcome, DaemonDfsMaintenanceTickOutcome::Completed { .. }) {
            // Lifecycle work can delete or compact content without producing a
            // served-message event. One coalesced dirty edge after a completed pass
            // keeps capacity evidence current without a periodic filesystem scan.
            notify_daemon_dfs_storage_environment_dirty(&data_dir);
        }
        return Ok(
            if namespace_work.complete || outcome == DaemonDfsMaintenanceTickOutcome::Continued {
                outcome
            } else {
                DaemonDfsMaintenanceTickOutcome::Deferred
            },
        );
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

/// Work that foreground work cut short, or that is due for another reason, runs
/// again once an admission is released after `after`: on that release, not on
/// a later unrelated wake. Work the next turn's foreground work cuts short
/// again waits for the release after that one, so nothing runs while
/// foreground work is still waiting.
fn wake_daemon_dfs_maintenance_after_admission_release(
    data_dir: &SinomoDataDir,
    mut releases: tokio::sync::watch::Receiver<u64>,
    after: u64,
    runtime: &tokio::runtime::Handle,
) {
    if crate::read_handoff::due_count(&data_dir.root()) == 0 {
        return;
    }
    let Ok(Some(task_owner)) = registered_dfs_runtime_host(data_dir) else {
        return;
    };
    let data_dir = data_dir.clone();
    task_owner.own_task(runtime.spawn(async move {
        while *releases.borrow_and_update() <= after {
            if releases.changed().await.is_err() {
                return;
            }
        }
        notify_daemon_dfs_maintenance_work(&data_dir);
    }));
}

/// Send the reads this turn's work handed off, now that the turn holds no
/// admission. Each read is its owner's pending item until it ends. An answer
/// is kept for its owner and wakes a turn, which runs the owner with it. A
/// read that ends without one parks its owner until
/// [`daemon_dfs_parked_read_event`] comes, then wakes a turn that runs the
/// owner, which sends the read again.
fn send_daemon_dfs_handed_off_reads(
    data_dir: &SinomoDataDir,
    reads: Vec<(
        crate::read_handoff::DfsReadOwner,
        Box<crate::source_adoption::RemoteNamespacePhase>,
    )>,
    runtime: &tokio::runtime::Handle,
) {
    if reads.is_empty() {
        return;
    }
    // A host with no task owner is stopping: each owner sends its read again
    // when it next runs.
    let Ok(Some(task_owner)) = registered_dfs_runtime_host(data_dir) else {
        return;
    };
    for (owner, phase) in reads {
        let crate::source_adoption::RemoteNamespacePhase::ReadAtPrimary(read) = phase.as_ref()
        else {
            continue;
        };
        let mut sending = DaemonDfsHandedOffReadSend {
            root: data_dir.root(),
            owner,
            key: read.key.clone(),
            ended: false,
        };
        let primary = read.read.pending.primary_node_id.clone();
        let shard = read.read.pending.dispatch_authority_shard.clone();
        if !crate::read_handoff::begin_send(&sending.root, &sending.owner, &sending.key) {
            sending.ended = true;
            continue;
        }
        let data_dir = data_dir.clone();
        let handle = runtime.clone();
        task_owner.own_task(runtime.spawn(async move {
            let completed = super::message_host::dispatch_daemon_dfs_remote_namespace_phase(
                &data_dir, phase, None, &handle,
            )
            .await;
            if let Ok(crate::source_adoption::CompletedRemoteNamespacePhase::ReadAnswered {
                key,
                answer: Ok(answer),
            }) = completed.map(|completed| *completed)
                && key == sending.key
            {
                if sending.end(Some(Ok(answer))) {
                    notify_daemon_dfs_maintenance_work(&data_dir);
                }
                return;
            }
            // Unanswered: the owner stays quiet until the read is worth
            // sending again.
            let Some(stop) = sending.park() else {
                return;
            };
            let mut parked = DaemonDfsParkedRead {
                root: sending.root.clone(),
                owner: sending.owner.clone(),
                stop,
                ended: false,
            };
            if daemon_dfs_parked_read_event(
                &data_dir,
                &handle,
                &primary,
                shard.as_ref(),
                &parked.stop,
            )
            .await
                && parked.end(true)
            {
                notify_daemon_dfs_maintenance_work(&data_dir);
            }
        }));
    }
}

/// Wait for an event after which a handed-off read that ended unanswered is
/// worth sending again: this node's verdict for `primary` changes back to a
/// path after it could not be reached; an examination of `primary` that
/// concludes after the read ended finds a path; or the record of the read's
/// shard moves off the epoch and primary it was sent under. The wait first
/// leaves the keeper the hint that the primary was not reached, as an
/// undelivered read does, so the network examines it. Nothing else ends the
/// wait but `stop`, which returns false.
async fn daemon_dfs_parked_read_event(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    primary: &NodeId,
    shard: Option<&sinomo_dfs::DfsMetadataShardConfig>,
    stop: &tokio::sync::Notify,
) -> bool {
    use sinomo_core::network::PeerPathVerdict;
    let cluster_paths = ClusterPaths::new(data_dir);
    let Ok(state) =
        sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths)
    else {
        stop.notified().await;
        return false;
    };
    let mut verdicts = super::peer_verdicts::DfsPeerVerdicts::new(data_dir, &cluster_paths, &state);
    verdicts.retain(true);
    let mut published = daemon_dfs_metadata_authority_generation_for_identity(
        &cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        Some(runtime),
    )
    .ok()
    .map(|authority| authority.subscribe_configuration_snapshot());
    let examined_before = verdicts.current().concluded_generation(primary);
    if let Some(shard) = shard {
        crate::service_record_follow::post_primary_unreachable(&state.local_node_id, shard);
    }
    let record_moved =
        |published: &tokio::sync::watch::Receiver<Arc<sinomo_dfs::DfsConfigurationSnapshot>>| {
            let Some(shard) = shard else {
                return false;
            };
            let current = published.borrow();
            let Ok(Some(shards)) = current.metadata_shards(&shard.file_system_id) else {
                return false;
            };
            shards
                .iter()
                .find(|current| current.shard_id == shard.shard_id)
                .is_none_or(|current| {
                    current.epoch != shard.epoch || current.primary_node_id != shard.primary_node_id
                })
        };
    let mut unreachable = false;
    loop {
        if published.as_ref().is_some_and(record_moved) {
            return true;
        }
        let snapshot = verdicts.current();
        match snapshot.entry(primary) {
            (PeerPathVerdict::Path, false)
                if unreachable || snapshot.concluded_generation(primary) != examined_before =>
            {
                return true;
            }
            (PeerPathVerdict::NoPath, _) => unreachable = true,
            _ => {}
        }
        let configuration = async {
            match published.as_mut() {
                Some(published) => {
                    if published.changed().await.is_err() {
                        std::future::pending::<()>().await;
                    }
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            () = stop.notified() => return false,
            () = verdicts.changed() => {}
            () = configuration => {}
        }
    }
}

/// A handed-off read on its way to its primary. Ending without an answer and
/// without being parked, its task's abort included, leaves its owner to send
/// it again when it next runs.
struct DaemonDfsHandedOffReadSend {
    root: std::path::PathBuf,
    owner: crate::read_handoff::DfsReadOwner,
    key: String,
    ended: bool,
}

impl DaemonDfsHandedOffReadSend {
    fn end(&mut self, answer: Option<crate::read_handoff::DfsHandedOffReadAnswer>) -> bool {
        self.ended = true;
        crate::read_handoff::end_send(&self.root, &self.owner, &self.key, answer)
    }

    fn park(&mut self) -> Option<Arc<tokio::sync::Notify>> {
        self.ended = true;
        crate::read_handoff::park(&self.root, &self.owner, &self.key)
    }
}

impl Drop for DaemonDfsHandedOffReadSend {
    fn drop(&mut self) {
        if !self.ended {
            crate::read_handoff::end_send(&self.root, &self.owner, &self.key, None);
        }
    }
}

/// An owner parked on its unanswered read. A wait that its task's abort ends
/// leaves the owner to send the read again when it next runs.
struct DaemonDfsParkedRead {
    root: std::path::PathBuf,
    owner: crate::read_handoff::DfsReadOwner,
    stop: Arc<tokio::sync::Notify>,
    ended: bool,
}

impl DaemonDfsParkedRead {
    fn end(&mut self, due: bool) -> bool {
        self.ended = true;
        crate::read_handoff::unpark(&self.root, &self.owner, &self.stop, due)
    }
}

impl Drop for DaemonDfsParkedRead {
    fn drop(&mut self) {
        if !self.ended {
            self.end(false);
        }
    }
}

// The cancellable future borrows this actual lane. Dropping that future must
// return live request custody before its outer global admission is released.
struct DaemonDfsMaintenanceServiceReturn<'a> {
    data_dir: &'a SinomoDataDir,
    cluster_paths: &'a ClusterPaths,
    runtime: &'a tokio::runtime::Handle,
    retained: Option<DaemonDfsMaintenanceServiceLane>,
    previous_cancellation: Option<crate::DfsCancellationFlag>,
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
        retained.lane.service.foreign_live_write_intents.clear();
        // This turn's read hand-off ends with it. A read handed off but never
        // sent is sent again when its owner next runs.
        retained.lane.service.read_handoff = Default::default();
        // A foreground generation belongs to this maintenance turn. Never
        // leave its already-triggered cancellation source on the reused lane.
        retained.lane.service.set_cancellation(self.previous_cancellation.take());
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
    cluster_state: sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
    metadata_authority_preflight: &DaemonDfsMetadataAuthorityPreflight,
    repair_authority_epoch: u64,
    maintenance_kind: DaemonDfsMaintenanceKind,
    node_resources: sinomo_device::NodeResourceSnapshot,
    committed_work_tracker: DfsCommittedWorkTracker,
    _deferred_work: &mut Option<(Arc<DaemonDfsDetachedSendRuntime>, DfsNodeDetachedSendWorker)>,
    turn_progress: &mut Option<(Arc<crate::maintenance::DfsMaintenanceProgressState>, u64)>,
    placement_budget: &mut usize,
    completed_remote_phase: &mut Option<(
        Box<crate::DfsNamespaceRequestRecord>,
        Box<crate::source_adoption::CompletedRemoteNamespacePhase>,
    )>,
    handed_off_reads: &mut Vec<(
        crate::read_handoff::DfsReadOwner,
        Box<crate::source_adoption::RemoteNamespacePhase>,
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
    // Identity DATA was read before admission. The live peer projection and
    // retained authority checks below revalidate that identity before taking
    // the lane; no file-state wait may retain admission needed by its producer.
    let execution_exclusions =
        daemon_dfs_maintenance_execution_exclusions(&cluster_paths, &cluster_state)
            .with_repair_authority_epoch(repair_authority_epoch);
    // The shard lanes' unfinished-write phase below reuses these inputs.
    let write_phase_exclusions = execution_exclusions.clone();
    let write_phase_owners = publication_owners.clone();
    let write_phase_tracker = committed_work_tracker.clone();
    let write_phase_budget =
        daemon_dfs_routine_maintenance_tick_options(node_resources).repair_max_jobs;
    // Transfer the mutation lane only after validating the admitted native
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
    let mut retained_lane = match try_take_daemon_dfs_message_mutation_service_for_maintenance(
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
    let previous_cancellation = retained_lane.lane.service.cancellation.clone();
    retained_lane.lane.service.add_cancellation(_foreground_preemption.cancellation_flag());
    let mut service_return = DaemonDfsMaintenanceServiceReturn {
        data_dir: &data_dir,
        cluster_paths: &cluster_paths,
        runtime,
        retained: Some(retained_lane),
        previous_cancellation,
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
    // Unfinished routed writes stay with the shard lanes that serve them;
    // their own phase below advances them, not this service's recovery.
    maintenance_service.foreign_live_write_intents = daemon_dfs_live_write_lane_intents(&data_dir)?;
    *turn_progress = Some((
        Arc::clone(&maintenance_service.maintenance_progress),
        maintenance_service.maintenance_progress.epoch()?,
    ));
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
    if maintenance_kind != DaemonDfsMaintenanceKind::RepairExecutor {
        let placement_advertisements = maintenance_service
            .placement_content_candidates()
            .unwrap_or_default();
        refresh_daemon_dfs_placement_snapshot(
            &data_dir,
            &cluster_paths,
            &cluster_state.local_node_id,
            placement_advertisements,
        );
    }
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
    // This host sends the reads the turn's work hands off once the turn has
    // released admission.
    maintenance_service.read_handoff.maintenance_sends =
        maintenance_kind != DaemonDfsMaintenanceKind::RepairExecutor;
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
            DaemonDfsMaintenanceKind::RepairExecutor => {
                let options = daemon_dfs_routine_maintenance_tick_options(node_resources);
                let shards = crate::metadata_shards_from_authority_projection(
                    &authoritative_topology,
                    &crate::sifs_file_system_id(),
                )
                .map_err(DaemonError::from)?;
                return crate::maintenance::run_dfs_repair_executor_turn(
                    &mut *maintenance_service,
                    &data_dir.root(),
                    &shards,
                    &execution_exclusions,
                    options.repair_max_jobs,
                )
                .map(Some)
                .map_err(DaemonError::from);
            }
            DaemonDfsMaintenanceKind::AuthoritativeReconciliation { .. }
            | DaemonDfsMaintenanceKind::MountedMutationRecovery => {
                let (content_gc_requested, content_reservation_reconciliation_requested, commit_promotion_recovery_requested) = match maintenance_kind {
                    DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
                        content_gc_requested, content_reservation_reconciliation_requested, commit_promotion_recovery_requested,
                    } => (content_gc_requested, content_reservation_reconciliation_requested, commit_promotion_recovery_requested),
                    DaemonDfsMaintenanceKind::MountedMutationRecovery => (false, false, false),
                    DaemonDfsMaintenanceKind::Routine | DaemonDfsMaintenanceKind::RepairExecutor => {
                        unreachable!("routine and executor-only turns handled above")
                    }
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
                    #[cfg(test)]
                    crate::maintenance::phase_runs::record(
                        &data_dir.root(),
                        crate::maintenance::phase_runs::HANDOFF,
                    );
                    let handoff_result = maintenance_service.reconcile_pending_metadata_shard_membership_handoffs_until_cancelled(
                        &recovery_shards, recovery_options.metadata_replication.target_timeout,
                        &|| {
                            _foreground_preemption.is_requested()
                                || metadata_authority_generation
                                    .retained_complete_prefix_revision_for_generation(
                                        authority_generation_before_maintenance,
                                    )
                                    .is_none()
                        },
                    );
                    #[cfg(test)]
                    crate::default_cluster::bootstrap_observation::record(&data_dir.root(), &cluster_state.local_node_id, "maintenance-handoff-result", ||
                        serde_json::json!({"admitted_generation":authority_generation_before_maintenance,"shards":recovery_shards,
                            "result":handoff_result.as_ref().map(|v|*v).map_err(|e|e.to_string())}));
                    match handoff_result {
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
                        let shard_id = crate::DfsMetadataShardId::new(shard.shard_id.clone());
                        // A closed copy is left out, as by the recovery scan
                        // that follows: it holds no writes to recover yet.
                        if backend.metadata_shard_storage_is_closed(&shard_id).map_err(DaemonError::from)? {
                            continue;
                        }
                        if backend.metadata_shard_authoritative_writes_frozen(&shard_id)
                            .map_err(DaemonError::from)? {
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
            let resumed = maintenance_service.recover_pending_namespace_requests_bounded(1);
            // A completed repair wait's deadline belongs to that re-run only.
            maintenance_service.file_sync_wait_deadline = None;
            resumed?;
            return Ok(None);
        }
        // Both kinds now execute the same bounded phases. Recovery has already
        // durably transferred discovered work before this future can yield to
        // foreground RPCs (including replies from its own promotion outbox).
        let result = crate::maintenance::run_reactive_maintenance_phases(
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
            Some(placement_budget),
        )
        .await;
        match result {
            Err(error) if error.is_cancellation() && _foreground_preemption.is_requested() => {
                // The synchronous metadata wait observed the SAME generation
                // before Tokio could poll the outer biased select again.
                // Existing progress/queue owners retain the unfinished turn.
                Ok(None)
            }
            result => result.map_err(DaemonError::from),
        }
    }
    .await;
    // An executor-only turn wakes the detached worker only for sends of its
    // own; sends other producers queued keep their own wakes.
    if maintenance_kind == DaemonDfsMaintenanceKind::RepairExecutor
        && matches!(&maintenance_result, Ok(Some(report)) if report.scheduled_repairs.is_empty())
    {
        *_deferred_work = None;
    }
    // The host sends the reads this turn's work handed off once the turn has
    // released admission.
    *handed_off_reads = std::mem::take(&mut maintenance_service.read_handoff.reads);
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
        && maintenance_kind != DaemonDfsMaintenanceKind::RepairExecutor
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
        // This branch precedes remote dispatch. A rejected service restore
        // must not strand an exact unclaimed local source/job writer and then
        // wait for that same writer while opening the successor generation.
        match restored {
            Ok(true) => {}
            rejected => {
                if let Some((_, phase)) = remote_namespace_phase.as_ref() {
                    #[cfg(test)]
                    phase.trace_undispatched_custody();
                    if let Err(error) = phase.cancel_local_before_failed_restore() {
                        tracing::warn!(%error, "undispatched namespace source remains owned; exact unclaimed release was rejected");
                    }
                }
                if let Err(error) = rejected { return Err(DaemonError::from(error)); }
                return Err(DaemonError::from(crate::DfsError::Metadata {
                    message: "owned namespace recovery could not restore its current service".into(),
                }));
            }
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
    // A routed write stays on the shard lane that served it until it finishes.
    // Run that lane's unfinished-write phase as the repair phase ran it on the
    // global service: due stage retries, the replies they bring back, and the
    // terminal reply its requester is still owed. It runs whatever this
    // turn's own phases reported: a recovery pass that settled a lane's write
    // durably still owes that write's reply. The global service's backend
    // lease has been returned above, so each lane checks out its own shard
    // while this turn still holds the global admission.
    if maintenance_kind != DaemonDfsMaintenanceKind::RepairExecutor
        && let Some(owners) = write_phase_owners.as_ref()
    {
        run_daemon_dfs_unfinished_write_phase_on_shard_lanes(
            &data_dir,
            owners,
            &metadata_authority_generation,
            runtime,
            &write_phase_exclusions,
            write_phase_budget,
            &write_phase_tracker,
            _foreground_preemption,
            _deferred_work,
        )
        .await?;
    }
    // A completed old turn is not completion of a newer authority generation.
    // Its durable effects and queue checkpoints remain, but the next turn must
    // acquire the current proof before acknowledging reconciliation readiness.
    if let Err(error) = daemon_dfs_require_retained_authority(
        &metadata_authority_generation,
        authority_generation_before_maintenance,
    ) {
        return match error {
            DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale) => Ok(
                DaemonDfsMaintenancePreExecution::AuthorityPreflightStale(stale),
            ),
            DaemonDfsAuthorityPreExecutionError::Failed(error) => Err(error.into()),
        };
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
    observations.emit_daemon_notice_logs();
    Ok(DaemonDfsMaintenancePreExecution::Finished(Some(
        reached_limit,
    )))
}

// Dropping the cancellable maintenance future mid-phase must return the
// lane's live write custody to its cache slot, as the global service's own
// return owner does.
struct DaemonDfsMaintenanceWriteServiceReturn<'a> {
    data_dir: &'a SinomoDataDir,
    taken: Option<DaemonDfsMaintenanceWriteService>,
    previous_cancellation: Option<crate::DfsCancellationFlag>,
}

impl DaemonDfsMaintenanceWriteServiceReturn<'_> {
    fn restore(&mut self) -> Result<bool, sinomo_dfs::DfsError> {
        let Some(mut taken) = self.taken.take() else {
            return Ok(false);
        };
        taken
            .lane
            .service
            .set_cancellation(self.previous_cancellation.take());
        if taken.lane.resident_metadata_scope.is_some() {
            taken.lane.service.release_resident_metadata_backend()?;
        }
        restore_daemon_dfs_write_service_after_maintenance(self.data_dir, taken)
    }
}

impl Drop for DaemonDfsMaintenanceWriteServiceReturn<'_> {
    fn drop(&mut self) {
        if self.taken.is_none() {
            return;
        }
        if let Err(error) = self.restore() {
            tracing::warn!(
                data_root = %self.data_dir.root().display(),
                "routed-write service handback failed after interrupted maintenance: {error}"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_daemon_dfs_unfinished_write_phase_on_shard_lanes(
    data_dir: &SinomoDataDir,
    owners: &Arc<crate::publication_guard::DfsPublicationExecutionOwners>,
    metadata_authority_generation: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    runtime: &tokio::runtime::Handle,
    execution_exclusions: &crate::DfsMaintenanceExecutionExclusions,
    repair_max_jobs: usize,
    committed_work_tracker: &DfsCommittedWorkTracker,
    foreground_preemption: &DaemonDfsMaintenanceForegroundPreemption,
    deferred_work: &mut Option<(Arc<DaemonDfsDetachedSendRuntime>, DfsNodeDetachedSendWorker)>,
) -> Result<(), DaemonError> {
    for taken in take_daemon_dfs_unfinished_write_services(data_dir)? {
        let mut write_return = DaemonDfsMaintenanceWriteServiceReturn {
            data_dir,
            previous_cancellation: taken.lane.service.cancellation.clone(),
            taken: Some(taken),
        };
        let taken = write_return.taken.as_mut().expect("taken write service");
        let key = taken.key.clone();
        let detached_send_runtime = Arc::clone(&taken.detached_send_runtime);
        let lane = &mut taken.lane;
        let phase = async {
            if let Some(scope) = lane.resident_metadata_scope.clone() {
                lane.attach_metadata_scope(&scope)?;
            }
            let provider = crate::DfsPublicationExecutionProvider::from_execution_owners(
                owners,
                Arc::clone(metadata_authority_generation),
                runtime.clone(),
            )?;
            let _publication_scope = lane
                .service
                .publication_execution
                .install(Arc::clone(owners));
            let _provider_scope = lane.service.publication_provider.install(provider);
            lane.service
                .set_committed_work_tracker(committed_work_tracker.clone());
            lane.service
                .add_cancellation(foreground_preemption.cancellation_flag());
            // An outcome settled elsewhere is replied first; retries follow.
            let mut events = Vec::new();
            lane.service
                .flush_completed_pending_remote_write_responses(&mut events)?;
            lane.service
                .retry_pending_stages_async(repair_max_jobs, execution_exclusions)
                .await?;
            lane.service
                .process_incoming_async_until_quiescent()
                .await?;
            lane.service
                .flush_completed_pending_remote_write_responses(&mut events)?;
            Ok::<_, sinomo_dfs::DfsError>(())
        }
        .await;
        if lane.service.message_deferred_work_pending()? {
            let worker = lane.service.detach_message_deferred_work();
            match deferred_work.as_mut() {
                Some((_, existing)) => existing.retain_prepared_calls_from(&worker),
                None => {
                    *deferred_work = Some((detached_send_runtime, worker));
                }
            }
        }
        match write_return.restore() {
            Ok(true) => {}
            Ok(false) => tracing::info!(
                data_root = %data_dir.root().display(), fs = %key.file_system_id,
                shard = %key.shard_id.0,
                "dropped a routed-write service whose owner or storage changed during maintenance"
            ),
            Err(error) => tracing::warn!(
                data_root = %data_dir.root().display(), fs = %key.file_system_id,
                shard = %key.shard_id.0,
                "routed-write service handback failed after maintenance: {error}"
            ),
        }
        if let Err(error) = phase {
            if error.is_cancellation() && foreground_preemption.is_requested() {
                return Ok(());
            }
            tracing::warn!(
                data_root = %data_dir.root().display(), fs = %key.file_system_id,
                shard = %key.shard_id.0,
                "routed-write lane unfinished-write phase failed: {error}"
            );
        }
    }
    Ok(())
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

/// Clone only an existing turn under the normal short cache bookkeeping lock.
fn retained_daemon_dfs_maintenance_turn(
    data_dir: &SinomoDataDir,
    authority_generation: u64,
) -> Result<Option<Arc<crate::maintenance::DfsMaintenanceProgressState>>, DaemonError> {
    let Some(slot) =
        crate::runtime_message::dfs_runtime_message_cache().existing_root(&data_dir.root())?
    else {
        return Ok(None);
    };
    let cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    Ok(cache
        .as_ref()
        .filter(|entry| entry.signature.metadata_authority_generation == authority_generation)
        .map(|entry| Arc::clone(&entry.maintenance_progress)))
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
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    run_daemon_dfs_maintenance_in_blocking_task(paths, runtime, node_resources, shutdown, false)
        .await
}

/// Run one executor-only turn from Tokio's blocking pool, owned and cancelled
/// exactly like a maintenance tick.
pub(crate) async fn run_daemon_dfs_repair_executor_turn_in_blocking_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    node_resources: sinomo_device::NodeResourceSnapshot,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    run_daemon_dfs_maintenance_in_blocking_task(paths, runtime, node_resources, shutdown, true)
        .await
}

async fn run_daemon_dfs_maintenance_in_blocking_task(
    paths: DaemonPaths,
    runtime: tokio::runtime::Handle,
    node_resources: sinomo_device::NodeResourceSnapshot,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    executor_only: bool,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError> {
    let tick_runtime = runtime.clone();
    let data_owner_lock = daemon_dfs_data_owner_lock(&paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(&paths)?);
    let task_owner = registered_dfs_runtime_host(&data_dir)?.ok_or_else(|| DaemonError::Parse {
        detail: "DFS maintenance has no registered runtime task owner".into(),
    })?;
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let cancellation_owner = Arc::clone(&task_owner);
    let task = tokio::task::spawn_blocking(move || {
        let result = runtime.block_on(async move {
            if *shutdown.borrow() {
                return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
            }
            let shutdown_state = shutdown.clone();
            tokio::select! {
                biased;
                _ = cancellation_owner.wait_for_shutdown() => Ok(DaemonDfsMaintenanceTickOutcome::Deferred),
                _ = shutdown.changed() => Ok(DaemonDfsMaintenanceTickOutcome::Deferred),
                result = until_shutdown_observed(async {
                    if executor_only {
                        run_daemon_dfs_maintenance_tick_kind_on_runtime(
                            &paths,
                            &tick_runtime,
                            DaemonDfsMaintenanceKind::RepairExecutor,
                            node_resources,
                        )
                        .await
                    } else {
                        run_daemon_dfs_maintenance_tick_on_runtime(
                            &paths,
                            &tick_runtime,
                            node_resources,
                        )
                        .await
                    }
                }, &cancellation_owner, &shutdown_state) => result,
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

/// Poll a maintenance turn that runs long synchronous stretches on the calling
/// thread. A shutdown signalled during one of those stretches ends the turn
/// when that poll returns: the wake it delivered can arrive while the select
/// is already being polled, and the select would otherwise sleep until a
/// later wake, such as the turn's own readiness deadline.
async fn until_shutdown_observed<F>(
    turn: F,
    host: &DfsRuntimeHost,
    shutdown: &tokio::sync::watch::Receiver<bool>,
) -> Result<DaemonDfsMaintenanceTickOutcome, DaemonError>
where
    F: std::future::Future<Output = Result<DaemonDfsMaintenanceTickOutcome, DaemonError>>,
{
    let mut turn = std::pin::pin!(turn);
    std::future::poll_fn(|cx| match turn.as_mut().poll(cx) {
        std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
        std::task::Poll::Pending if host.is_shutting_down() || *shutdown.borrow() => {
            std::task::Poll::Ready(Ok(DaemonDfsMaintenanceTickOutcome::Deferred))
        }
        std::task::Poll::Pending => std::task::Poll::Pending,
    })
    .await
}

/// The current authority preflight if its configuration is ready now, without
/// waiting for a pending publication.
async fn warm_daemon_dfs_repair_executor_authority(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<DaemonDfsMetadataAuthorityPreflight>, DaemonError> {
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        Some(runtime),
    )?;
    Ok(
        warm_daemon_dfs_metadata_authority_before_deadline(authority, tokio::time::Instant::now())
            .await
            .ok(),
    )
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
