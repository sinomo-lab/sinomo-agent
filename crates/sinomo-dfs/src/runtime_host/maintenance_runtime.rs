use super::*;

pub(crate) type DaemonDfsMaintenanceTickOutcome = sinomo_dfs::DfsMaintenanceTickOutcome;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DaemonDfsMaintenanceKind {
    Routine,
    AuthoritativeReconciliation {
        content_gc_requested: bool,
        content_reservation_reconciliation_requested: bool,
        commit_promotion_recovery_requested: bool,
    },
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
    let maintenance_kind = if mounted_mutation_recovery.requires_full_recovery()
        || reconciliation_generation.is_some()
    {
        DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
            content_gc_requested: false,
            content_reservation_reconciliation_requested: false,
            commit_promotion_recovery_requested: false,
        }
    } else {
        DaemonDfsMaintenanceKind::Routine
    };
    let outcome = run_daemon_dfs_maintenance_tick_kind_on_runtime(
        paths,
        runtime,
        maintenance_kind,
        node_resources,
    )
    .await?;
    complete_daemon_dfs_mounted_mutation_recovery(mounted_mutation_recovery, outcome)?;
    if let Some(generation) = reconciliation_generation
        && matches!(outcome, DaemonDfsMaintenanceTickOutcome::Completed { .. })
    {
        complete_daemon_dfs_maintenance_reconciliation(paths, generation)?;
    }
    Ok(outcome)
}

pub(super) async fn run_daemon_dfs_maintenance_tick_kind_on_runtime(
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
    let metadata_authority_preflight = warm_daemon_dfs_metadata_authority_for_identity_async(
        &cluster_paths,
        &preflight_cluster_state.cluster_id,
        &preflight_cluster_state.local_node_id,
        runtime,
    )
    .await?;
    // Publish maintenance interest before cluster/local admission. A
    // foreground request that queues anywhere behind this pass can then ask it
    // to yield once its current synchronous poll reaches an await boundary.
    let foreground_preemption =
        daemon_dfs_mutation_scheduler(&data_dir)?.begin_maintenance_interest();
    let Some(admission) = acquire_daemon_dfs_maintenance_admission_until_foreground_waits(
        &data_dir,
        &foreground_preemption,
    )
    .await?
    else {
        return Ok(DaemonDfsMaintenanceTickOutcome::Deferred);
    };
    let DaemonDfsMaintenanceAdmission {
        _lifecycle_guard: lifecycle_guard,
        _mutation_admission: mutation_admission,
    } = admission;
    let mut deferred_work = None;
    let committed_work_tracker = DfsCommittedWorkTracker::new();
    let maintenance_result =
        run_daemon_dfs_maintenance_until_foreground_waits(&foreground_preemption, async {
            run_daemon_dfs_maintenance_tick_with_admission(
                data_dir.clone(),
                cluster_paths.clone(),
                runtime,
                &metadata_authority_preflight,
                maintenance_kind,
                node_resources,
                committed_work_tracker.clone(),
                &mut deferred_work,
                &foreground_preemption,
            )
            .await
        })
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
    // maintenance future before global admission is released. On success it may
    // have restored a reset backend-bearing request lane; on preemption/error
    // the owned lane was destroyed. Local mutation therefore remains
    // single-generation even when a repair or metadata RPC is cancelled to
    // admit foreground work. Those sends are replay-safe; return Deferred
    // below so durable work is retried promptly.
    drop(mutation_admission);
    drop(lifecycle_guard);
    drop(foreground_preemption);
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
            Some(reached_limit) => DaemonDfsMaintenanceTickOutcome::Completed { reached_limit },
            None => DaemonDfsMaintenanceTickOutcome::Deferred,
        },
        DaemonDfsMaintenancePreemptionOutcome::ForegroundWaiting => {
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
    Ok(outcome)
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
    _foreground_preemption: &DaemonDfsMaintenanceForegroundPreemption,
) -> Result<Option<bool>, DaemonError> {
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
    metadata_authority_preflight
        .authority
        .ensure_complete_prefix_for_generation_async(metadata_authority_preflight.generation)
        .await
        .filter(|_| {
            metadata_authority_preflight.authority.generation()
                == metadata_authority_preflight.generation
        })
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS metadata authority generation {} changed or remained incomplete before maintenance admission",
                metadata_authority_preflight.generation
            ),
        })?;
    let retained_lane = take_daemon_dfs_message_mutation_service_for_maintenance(
        &data_dir,
        &cluster_paths,
        &cluster_state,
        runtime,
        Some(metadata_authority_preflight),
    )
    .map_err(DaemonError::from)?;
    let DaemonDfsMaintenanceServiceLane {
        signature,
        metadata_authority_generation,
        peer_node_ids,
        lane,
        detached_send_runtime,
    } = retained_lane;
    let DaemonDfsMessageServiceLane {
        service: mut maintenance_service,
        resident_metadata_scope,
        ..
    } = lane;
    let local_advertisement = cached_daemon_dfs_local_content_advertisement(
        &data_dir,
        &cluster_state.local_node_id,
        daemon_dfs_local_zone(),
    );
    if let Err(error) =
        maintenance_service.publish_local_content_advertisement_if_needed(local_advertisement)
    {
        tracing::warn!(
            target: "sinomo.dfs.placement",
            "could not refresh cached DFS storage advertisement: {error}"
        );
    }
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
    let authoritative_topology = metadata_authority_generation
        .current_complete_prefix_for_generation(authority_generation_before_maintenance)
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message:
                "DFS maintenance requires a complete retained control-plane projection from the admitted backend generation"
                    .to_string(),
        })?;
    let authority_prefix_revision = authoritative_topology.revision();
    let maintenance_result = match maintenance_kind {
        DaemonDfsMaintenanceKind::Routine => {
            sinomo_dfs::run_cluster_dfs_reactive_maintenance_tick_with_reusable_service(
                &mut maintenance_service,
                data_dir.clone(),
                cluster_paths.clone(),
                cluster_state,
                authoritative_topology,
                async_peer_transport,
                execution_exclusions,
                committed_work_tracker,
                daemon_dfs_routine_maintenance_tick_options(node_resources),
            )
            .await
            .map(Some)
            .map_err(DaemonError::from)
        }
        DaemonDfsMaintenanceKind::AuthoritativeReconciliation {
            content_gc_requested,
            content_reservation_reconciliation_requested,
            ..
        } => {
            sinomo_dfs::run_cluster_dfs_maintenance_tick_with_reusable_service_until_hydration_cancelled(
                &mut maintenance_service,
                data_dir.clone(),
                cluster_paths.clone(),
                cluster_state,
                authoritative_topology,
                async_peer_transport,
                execution_exclusions,
                committed_work_tracker,
                &|| {
                    _foreground_preemption.is_requested()
                        || metadata_authority_generation.generation()
                            != authority_generation_before_maintenance
                        || metadata_authority_generation.current_complete_prefix_revision()
                            != Some(authority_prefix_revision)
                },
                daemon_dfs_explicit_disaster_recovery_tick_options(
                    content_gc_requested,
                    content_reservation_reconciliation_requested,
                ),
            )
            .await
            .map_err(DaemonError::from)
        }
    };
    match &maintenance_result {
        Ok(Some(_))
            if matches!(
                maintenance_kind,
                DaemonDfsMaintenanceKind::AuthoritativeReconciliation { .. }
            ) =>
        {
            let authority_generation_after_maintenance = metadata_authority_generation.generation();
            if authority_generation_after_maintenance == authority_generation_before_maintenance
                && metadata_authority_generation.current_complete_prefix_revision()
                    == Some(authority_prefix_revision)
            {
                // The broad maintenance service already performed the explicit
                // one-pass recovery hydrate into this shared outbox. Mark only
                // the exact unchanged topology it scanned; recovery preparation
                // below can wake the outbox without repeating the full scan.
                detached_send_runtime
                    .record_recovery_scan_generation(authority_generation_after_maintenance);
            }
        }
        Err(_)
            if matches!(
                maintenance_kind,
                DaemonDfsMaintenanceKind::AuthoritativeReconciliation { .. }
            ) =>
        {
            // A disaster-recovery hydrate can repair a durable intent before
            // failing later. Preserve that authoritative reconciliation obligation.
            detached_send_runtime.reset_recovery_scan_generation();
        }
        Ok(Some(_)) | Ok(None) | Err(_) => {}
    }
    // A completed maintenance pass retains only live request state. When no
    // in-flight repair, replication, or namespace transition owns the hydrated
    // mirror, collapse routine and disaster-recovery passes back to the same
    // lazy shell over the resident backend. Durable tables and the repair queue
    // reconstruct the released projection on demand.
    let reusable_service = if matches!(&maintenance_result, Ok(Some(_))) {
        match maintenance_service.authoritative_write_frozen_shard_count() {
            Ok(0) => {
                let reset_to_lazy = matches!(
                    maintenance_kind,
                    DaemonDfsMaintenanceKind::AuthoritativeReconciliation { .. }
                ) || maintenance_service
                    .can_release_hydrated_metadata_projection();
                if reset_to_lazy {
                    match maintenance_service.into_lazy_metadata_backend_service() {
                        Ok(service) => Some(service),
                        Err(error) => {
                            tracing::warn!(
                                data_root = %data_dir.root().display(),
                                "dropping daemon DFS disaster-recovery service after lazy reset failed: {error}"
                            );
                            None
                        }
                    }
                } else {
                    Some(maintenance_service)
                }
            }
            Ok(frozen_shard_count) => {
                tracing::info!(
                    data_root = %data_dir.root().display(),
                    frozen_shard_count,
                    "dropping daemon DFS maintenance service while an authoritative shard is frozen"
                );
                drop(maintenance_service);
                None
            }
            Err(error) => {
                tracing::warn!(
                    data_root = %data_dir.root().display(),
                    "dropping daemon DFS maintenance service after frozen-shard validation failed: {error}"
                );
                drop(maintenance_service);
                None
            }
        }
    } else {
        drop(maintenance_service);
        None
    };
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
                *_deferred_work = Some((detached_runtime, worker));
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
    if let Some(service) = reusable_service {
        let retained = DaemonDfsMaintenanceServiceLane {
            signature,
            metadata_authority_generation,
            peer_node_ids: peer_node_ids.clone(),
            lane: DaemonDfsMessageServiceLane {
                service,
                peer_node_ids,
                resident_metadata_scope,
            },
            detached_send_runtime,
        };
        match restore_daemon_dfs_message_mutation_service_after_maintenance(
            &data_dir,
            &cluster_paths,
            runtime,
            retained,
        ) {
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
    let report = maintenance_result?;
    let Some(report) = report else {
        // A cancellable hydrate which yields no report did not complete a
        // durable owner pass. Surface it as Deferred so mounted-mutation
        // recovery claims remain non-terminal.
        return Ok(None);
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
    Ok(Some(reached_limit))
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
    tokio::task::spawn_blocking(move || {
        let _data_owner_lock = data_owner_lock;
        runtime.block_on(async move {
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
        })
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS maintenance".to_string(),
        detail: format!("DFS maintenance blocking task failed: {source}"),
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
