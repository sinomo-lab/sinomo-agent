use super::*;

const DFS_MAINTENANCE_TASK_COMPLETION_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DFS_STORAGE_DIRTY_SETTLE_DELAY: Duration = Duration::from_secs(2);
const DFS_STORAGE_REFRESH_ERROR_RETRY_DELAY: Duration = Duration::from_secs(30);
const DFS_REMOTE_PLACEMENT_STARTUP_RECONCILE_INTERVAL: Duration = Duration::from_secs(2);
const DFS_REMOTE_PLACEMENT_STARTUP_RECONCILE_WINDOW: Duration = Duration::from_secs(60);

pub(crate) fn mount_dfs_runtime_tasks(
    context: &sinomo_module::ComponentContext,
    scope: &mut sinomo_module::ComponentScope,
    paths: DfsRuntimePaths,
    runtime: tokio::runtime::Handle,
    host_event_receivers: Vec<sinomo_module::ComponentEventReceiver>,
) -> Result<(), sinomo_module::ComponentError> {
    let cluster_paths = ClusterPaths::new(context.data_dir());
    let state =
        sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths).map_err(|error| {
            sinomo_module::ComponentError::Component {
                component_id: crate::SINOMO_DFS_MODULE_LEGACY_ID.to_string(),
                detail: error.to_string(),
            }
        })?;
    let startup_cluster_id = state.cluster_id.clone();
    let startup_node_id = state.local_node_id.clone();
    let reactor = Arc::new(tokio::sync::Mutex::new(Some(
        DaemonDefaultDfsReconciliationReactor::new_on_runtime(
            &cluster_paths,
            &startup_node_id,
            &runtime,
        )
        .map_err(|error| sinomo_module::ComponentError::Component {
            component_id: crate::SINOMO_DFS_MODULE_LEGACY_ID.to_string(),
            detail: error.to_string(),
        })?,
    )));
    let (prepared_tx, prepared_rx) = tokio::sync::watch::channel(false);

    let prepare_paths = paths.clone();
    let prepare_runtime = runtime.clone();
    let prepare_reactor = Arc::clone(&reactor);
    let prepare_cluster_id = startup_cluster_id.clone();
    let prepare_node_id = startup_node_id.clone();
    let prepare_cluster_paths = cluster_paths.clone();
    scope.on_host_prepare(move || {
        let paths = prepare_paths.clone();
        let runtime = prepare_runtime.clone();
        let reactor = Arc::clone(&prepare_reactor);
        let cluster_id = prepare_cluster_id.clone();
        let node_id = prepare_node_id.clone();
        let cluster_paths = prepare_cluster_paths.clone();
        let prepared_tx = prepared_tx.clone();
        async move {
            let settled_solo =
                sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths)
                    .map(|state| sinomo_core::network::cluster_is_settled_solo(&state))
                    .map_err(|error| error.to_string())?;
            if settled_solo {
                let mut reactor = reactor.lock().await;
                let reactor = reactor
                    .as_mut()
                    .ok_or_else(|| "DFS startup reactor is unavailable".to_string())?;
                match reactor
                    .reconcile_startup_until_idle_async(
                        &paths,
                        &DfsRuntimeOptions,
                        &cluster_id,
                        &node_id,
                        &runtime,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(DfsRuntimeError::StoragePreparationPending { root, processed_records }) => {
                        // This edge prepares orchestration, not physical storage.
                        // Keep the SAME unfinished reactor generation for its
                        // normal source-change/backoff path after host preparation.
                        // Cold opens still require the actual provider's Ready;
                        // no lifecycle generation has been completed here.
                        tracing::debug!(target: "sinomo.dfs.reconciliation",
                            root = %root.display(), processed_records,
                            "DFS eager startup storage preparation deferred to its retained reactor");
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            schedule_daemon_dfs_startup_repair_reconciliation(&paths)
                .map_err(|error| error.to_string())?;
            let _ = prepared_tx.send(true);
            Ok(())
        }
    });

    let shutdown = scope.shutdown_receiver();
    let reconciliation_task = runtime.spawn(run_component_reconciliation(
        reactor,
        paths.clone(),
        startup_cluster_id,
        startup_node_id,
        runtime.clone(),
        prepared_rx.clone(),
        shutdown.clone(),
    ));
    scope.own_task("default DFS reconciliation", reconciliation_task);

    let node_resources = context.node_resources().unwrap_or_default();
    scope.own_task(
        "DFS storage environment",
        runtime.spawn(run_component_storage_environment(
            paths.clone(),
            runtime.clone(),
            node_resources.clone(),
            prepared_rx.clone(),
            shutdown.clone(),
        )),
    );
    scope.own_task(
        "DFS remote placement observations",
        runtime.spawn(run_component_remote_placement_observation(
            paths.clone(),
            prepared_rx.clone(),
            shutdown.clone(),
        )),
    );
    let route_subscription = sinomo_core::network::__connectivity_store::subscribe_local_cluster_connectivity_routing_generation(
        &cluster_paths,
    );
    scope.own_task(
        "DFS metadata route generation",
        runtime.spawn(run_component_route_generation_watch(
            paths.clone(),
            runtime.clone(),
            route_subscription,
            prepared_rx.clone(),
            shutdown.clone(),
        )),
    );
    scope.own_task(
        "DFS maintenance",
        runtime.clone().spawn(run_component_maintenance(
            paths.clone(),
            runtime.clone(),
            node_resources,
            prepared_rx,
            shutdown.clone(),
        )),
    );
    for (index, receiver) in host_event_receivers.into_iter().enumerate() {
        scope.own_task(
            format!("DFS host event {index}"),
            runtime.spawn(run_component_host_events(
                paths.clone(),
                runtime.clone(),
                receiver,
                shutdown.clone(),
            )),
        );
    }
    let shutdown_paths = paths;
    scope.defer(move || {
        if let Err(error) = evict_daemon_dfs_message_service_cache_on_shutdown(&shutdown_paths) {
            tracing::warn!(%error, "failed to evict DFS resident cache during component shutdown");
        }
    });
    Ok(())
}

async fn run_component_host_events(
    paths: DfsRuntimePaths,
    runtime: tokio::runtime::Handle,
    mut receiver: sinomo_module::ComponentEventReceiver,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            event = receiver.recv() => {
                let Ok(event) = event else {
                    continue;
                };
                match event.envelope.kind.as_str() {
                    sinomo_module::COMPONENT_HOST_CLUSTER_CONTROL_CHANGED_EVENT_KIND => {
                        if let Ok(data_dir)=paths.data_dir() {
                            if let Ok(actor)=crate::source_adoption::actor_for_root(&data_dir.root()) {actor.wake_reconciliation();}
                        }
                        if let Err(error) = request_daemon_dfs_maintenance_reconciliation(&paths) {
                            tracing::warn!(%error, "DFS could not observe host cluster-control change");
                        }
                        notify_daemon_dfs_maintenance_work_for_paths(&paths);
                        let _ = schedule_daemon_dfs_metadata_publish_on_runtime(
                            &paths,
                            &runtime,
                            None,
                        );
                    }
                    sinomo_module::COMPONENT_HOST_CONNECTIVITY_CHANGED_EVENT_KIND => {
                        if let Ok(data_dir)=paths.data_dir() {
                            if let Ok(actor)=crate::source_adoption::actor_for_root(&data_dir.root()) {actor.wake_reconciliation();}
                        }
                        notify_daemon_dfs_maintenance_work_for_paths(&paths);
                        let _ = notify_daemon_dfs_metadata_publish_route_generation_on_runtime(
                            &paths,
                            &runtime,
                        );
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn wait_until_prepared(
    prepared: &mut tokio::sync::watch::Receiver<bool>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    loop {
        if *shutdown.borrow() {
            return false;
        }
        if *prepared.borrow() {
            return true;
        }
        tokio::select! {
            changed = prepared.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return false;
                }
            }
        }
    }
}

async fn run_component_reconciliation(
    reactor: Arc<tokio::sync::Mutex<Option<DaemonDefaultDfsReconciliationReactor>>>,
    paths: DfsRuntimePaths,
    cluster_id: sinomo_core::Id,
    node_id: NodeId,
    runtime: tokio::runtime::Handle,
    mut prepared: tokio::sync::watch::Receiver<bool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !wait_until_prepared(&mut prepared, &mut shutdown).await {
        return;
    }
    let Some(reactor) = reactor.lock().await.take() else {
        return;
    };
    run_daemon_default_dfs_reconciliation_reactor(
        reactor,
        paths,
        DfsRuntimeOptions,
        cluster_id,
        node_id,
        runtime,
        shutdown,
    )
    .await;
}

async fn run_component_storage_environment(
    paths: DfsRuntimePaths,
    runtime: tokio::runtime::Handle,
    mut node_resources: sinomo_device::NodeResourceMonitor,
    mut prepared: tokio::sync::watch::Receiver<bool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !wait_until_prepared(&mut prepared, &mut shutdown).await {
        return;
    }
    let Ok(data_dir) = paths.data_dir() else {
        return;
    };
    let mut current_resources = node_resources.take_latest();
    let notify = daemon_dfs_storage_environment_notify(&data_dir);
    let mut sampled_generation = 0;
    let mut sample_storage = true;
    loop {
        if *shutdown.borrow() {
            return;
        }
        if !sample_storage
            && daemon_dfs_storage_environment_dirty_generation(&data_dir) != sampled_generation
        {
            tokio::time::sleep(DFS_STORAGE_DIRTY_SETTLE_DELAY).await;
            sample_storage = true;
            continue;
        }
        let observed_generation =
            sample_storage.then(|| daemon_dfs_storage_environment_dirty_generation(&data_dir));
        let refresh_failed = match refresh_daemon_dfs_storage_environment_with_resources_on_runtime(
            &paths,
            &runtime,
            current_resources,
            sample_storage,
        )
        .await
        {
            Ok(_) => {
                if let Some(generation) = observed_generation {
                    sampled_generation = generation;
                    sample_storage = false;
                }
                false
            }
            Err(error) => {
                tracing::warn!(target: "sinomo.dfs.placement", %error, "DFS storage environment refresh failed");
                true
            }
        };
        if daemon_dfs_storage_environment_dirty_generation(&data_dir) != sampled_generation {
            tokio::time::sleep(DFS_STORAGE_DIRTY_SETTLE_DELAY).await;
            sample_storage = true;
            continue;
        }
        tokio::select! {
            _ = async {
                if refresh_failed {
                    tokio::time::sleep(DFS_STORAGE_REFRESH_ERROR_RETRY_DELAY).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
            _ = notify.notified() => {}
            resources = node_resources.changed() => current_resources = resources,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

async fn run_component_remote_placement_observation(
    paths: DfsRuntimePaths,
    mut prepared: tokio::sync::watch::Receiver<bool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !wait_until_prepared(&mut prepared, &mut shutdown).await {
        return;
    }
    let _subscription = match subscribe_daemon_dfs_remote_placement_observation(&paths) {
        Ok(subscription) => subscription,
        Err(error) => {
            tracing::warn!(target: "sinomo.dfs.placement", %error, "DFS remote placement subscription failed");
            None
        }
    };
    let reconcile_deadline = Instant::now() + DFS_REMOTE_PLACEMENT_STARTUP_RECONCILE_WINDOW;
    while let Some(remaining) = reconcile_deadline.checked_duration_since(Instant::now()) {
        tokio::select! {
            _ = tokio::time::sleep(
                DFS_REMOTE_PLACEMENT_STARTUP_RECONCILE_INTERVAL.min(remaining)
            ) => {
                if let Err(error) = refresh_daemon_dfs_remote_placement_observation(&paths) {
                    tracing::warn!(target: "sinomo.dfs.placement", %error, "DFS remote placement startup reconciliation failed");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

async fn run_component_route_generation_watch(
    paths: DfsRuntimePaths,
    runtime: tokio::runtime::Handle,
    mut subscription: sinomo_core::network::__connectivity_store::LocalClusterConnectivityRoutingGenerationSubscription,
    mut prepared: tokio::sync::watch::Receiver<bool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !wait_until_prepared(&mut prepared, &mut shutdown).await {
        return;
    }
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            generation = subscription.changed() => {
                if daemon_dfs_repair_queue_work_is_known(&paths) {
                    notify_daemon_dfs_maintenance_work_for_paths(&paths);
                }
                match notify_daemon_dfs_metadata_publish_route_generation_on_runtime(&paths, &runtime) {
                    Ok(0) => {}
                    Ok(lanes) => tracing::debug!(generation, lanes, "DFS metadata lanes observed route generation"),
                    Err(error) => tracing::warn!(generation, %error, "DFS metadata lanes could not observe route generation"),
                }
            }
        }
    }
}

fn pending_work_requires_local_recovery(work: &DaemonDfsPendingMaintenanceWork) -> bool {
    work.pending_write_count > 0
        || work.pending_namespace_job_count > 0
        || work.pending_local_membership_handoff_count > 0
        || work.completed_local_primary_handoff_rebind_count > 0
        || work.mounted_mutation_recoverable_exact_count > 0
        || work.mounted_mutation_recoverable_full_count > 0
        || work.due_time_based_maintenance_count > 0
}

fn resource_adjusted_decision(
    resources: sinomo_device::NodeResourceSnapshot,
    work: Option<&DaemonDfsPendingMaintenanceWork>,
    mut decision: crate::DfsMaintenanceProbeDecision,
) -> crate::DfsMaintenanceProbeDecision {
    if daemon_dfs_background_network_work_deferred(resources)
        && work.is_some_and(|work| !pending_work_requires_local_recovery(work))
    {
        decision.trigger_maintenance = false;
        decision.immediate_continuation = false;
        decision.next_probe_at = None;
    }
    decision
}

/// A consumed wake is not completion of the requested local reconciliation.
/// In particular, foreground preemption must retain its normal retry timer
/// even when no other local repair/placement work happens to keep us awake.
pub(super) fn retain_requested_reconciliation(
    paths: &DfsRuntimePaths,
    mut decision: crate::DfsMaintenanceProbeDecision,
) -> crate::DfsMaintenanceProbeDecision {
    if daemon_dfs_maintenance_reconciliation_generation(paths).is_some() {
        decision.trigger_maintenance = true;
    }
    decision
}

async fn run_component_maintenance(
    paths: DfsRuntimePaths,
    runtime: tokio::runtime::Handle,
    mut node_resources: sinomo_device::NodeResourceMonitor,
    mut prepared: tokio::sync::watch::Receiver<bool>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if !wait_until_prepared(&mut prepared, &mut shutdown).await {
        return;
    }
    let cluster_paths = match paths.data_dir() {
        Ok(data_dir) => ClusterPaths::new(&data_dir),
        Err(error) => {
            tracing::warn!(%error, "DFS maintenance has no data directory");
            return;
        }
    };
    let phase = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)
        .map(|state| crate::dfs_maintenance_member_phase(&state))
        .unwrap_or(Duration::ZERO);
    let started = Instant::now();
    let not_before = started
        .checked_add(crate::dfs_initial_maintenance_delay(phase, true))
        .unwrap_or(started);
    let mut last_maintenance = started
        .checked_sub(crate::DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL)
        .unwrap_or(started);
    let mut next_probe = Some(not_before);
    let mut triggered = false;
    let mut immediate = false;
    let mut resources = node_resources.take_latest();
    let notify =
        match daemon_dfs_maintenance_notify(&paths.data_dir().expect("validated data root")) {
            Ok(notify) => notify,
            Err(error) => {
                tracing::warn!(%error, "DFS maintenance has no registered runtime host");
                return;
            }
        };

    loop {
        if *shutdown.borrow() {
            return;
        }
        let now = Instant::now();
        let due = if triggered {
            crate::dfs_maintenance_due_in(now, not_before, last_maintenance, triggered, immediate)
        } else {
            next_probe.map(|deadline| deadline.saturating_duration_since(now))
        };
        tokio::select! {
            _ = async {
                match due {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending::<()>().await,
                }
            } => {}
            _ = notify.notified() => {
                let decision = if daemon_dfs_background_network_work_deferred(resources) {
                    crate::DfsMaintenanceProbeDecision {
                        trigger_maintenance: false,
                        immediate_continuation: false,
                        next_probe_at: Some(Instant::now()),
                    }
                } else {
                    crate::dfs_maintenance_notification_decision(
                        Instant::now(), triggered, immediate, next_probe,
                    )
                };
                triggered = decision.trigger_maintenance;
                immediate = decision.immediate_continuation;
                next_probe = decision.next_probe_at;
            }
            updated = node_resources.changed() => {
                resources = updated;
                if !daemon_dfs_background_network_work_deferred(resources) {
                    next_probe = Some(Instant::now());
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }

        if !triggered && crate::dfs_maintenance_probe_is_due(Instant::now(), next_probe) {
            let probe_paths = paths.clone();
            let probe_runtime = runtime.clone();
            let work = match tokio::task::spawn_blocking(move || {
                daemon_dfs_pending_reactive_maintenance_work_on_runtime(
                    &probe_paths,
                    &probe_runtime,
                )
            })
            .await
            {
                Ok(Ok(work)) => Some(work),
                Ok(Err(error)) => {
                    tracing::warn!(%error, "DFS maintenance probe failed");
                    None
                }
                Err(error) => {
                    tracing::warn!(%error, "DFS maintenance probe task failed");
                    None
                }
            };
            let work_for_policy = work.clone();
            let decision = retain_requested_reconciliation(
                &paths,
                resource_adjusted_decision(
                    resources,
                    work_for_policy.as_ref(),
                    crate::dfs_maintenance_probe_decision(
                        Instant::now(),
                        daemon_dfs_now_unix_nanos(),
                        work,
                    ),
                ),
            );
            triggered = decision.trigger_maintenance;
            immediate = decision.immediate_continuation;
            next_probe = decision.next_probe_at;
        }

        let launch = crate::dfs_maintenance_due_in(
            Instant::now(),
            not_before,
            last_maintenance,
            triggered,
            immediate,
        )
        .is_some_and(|delay| delay.is_zero());
        if launch && crate::dfs_maintenance_should_launch(launch, triggered) {
            let outcome = run_daemon_dfs_maintenance_tick_in_blocking_task(
                paths.clone(),
                runtime.clone(),
                resources,
                shutdown.clone(),
            )
            .await;
            let completed = Instant::now();
            match outcome {
                Ok(outcome) => {
                    let probe_paths = paths.clone();
                    let probe_runtime = runtime.clone();
                    let work = tokio::task::spawn_blocking(move || {
                        daemon_dfs_pending_reactive_maintenance_work_on_runtime(
                            &probe_paths,
                            &probe_runtime,
                        )
                    })
                    .await
                    .ok()
                    .and_then(Result::ok);
                    let work_for_policy = work.clone();
                    let decision = retain_requested_reconciliation(
                        &paths,
                        resource_adjusted_decision(
                            resources,
                            work_for_policy.as_ref(),
                            crate::dfs_maintenance_completion_decision(
                                outcome,
                                completed,
                                daemon_dfs_now_unix_nanos(),
                                work,
                                false,
                            ),
                        ),
                    );
                    triggered = decision.trigger_maintenance;
                    immediate = decision.immediate_continuation;
                    next_probe = decision.next_probe_at;
                    last_maintenance = if outcome == DaemonDfsMaintenanceTickOutcome::Deferred {
                        completed
                            .checked_sub(
                                crate::DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
                                    .saturating_sub(crate::DFS_MAINTENANCE_LEASE_RETRY_DELAY),
                            )
                            .unwrap_or(completed)
                    } else {
                        completed
                    };
                }
                Err(error) => {
                    tracing::warn!(%error, "DFS maintenance tick failed");
                    let decision = crate::dfs_failed_maintenance_decision(completed, next_probe);
                    triggered = decision.trigger_maintenance;
                    immediate = decision.immediate_continuation;
                    next_probe = decision.next_probe_at;
                    last_maintenance = completed;
                }
            }
        }

        if due.is_none() && !triggered {
            tokio::time::sleep(DFS_MAINTENANCE_TASK_COMPLETION_POLL_INTERVAL).await;
        }
    }
}
