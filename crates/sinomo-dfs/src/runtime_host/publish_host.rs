use super::*;

fn own_dfs_publish_task(data_dir: &SinomoDataDir, task: Option<tokio::task::JoinHandle<()>>) {
    let Some(task) = task else {
        return;
    };
    match dfs_runtime_host(data_dir) {
        Ok(host) => host.own_task(task),
        Err(_) => task.abort(),
    }
}

pub(super) fn daemon_dfs_metadata_replication_options() -> DfsMetadataJournalReplicationOptions {
    DfsMetadataJournalReplicationOptions {
        background_publication: true,
        ..sinomo_dfs::dfs_resident_metadata_replication_options()
    }
}

pub(super) fn daemon_dfs_smoke_options() -> sinomo_dfs::DfsSmokeOptions {
    sinomo_dfs::dfs_resident_smoke_options()
}

pub(super) fn daemon_dfs_metadata_publish_schedule_options() -> DfsMetadataPublishScheduleOptions {
    DfsMetadataPublishScheduleOptions {
        max_retry_rounds: DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS,
        retry_delay: DAEMON_DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL,
        max_retry_delay: DAEMON_DFS_METADATA_PUBLISH_RETRY_MAX_DELAY,
    }
}

pub(super) fn daemon_dfs_metadata_publish_discovery_schedule_options()
-> DfsMetadataPublishScheduleOptions {
    DfsMetadataPublishScheduleOptions {
        max_retry_rounds: DAEMON_DFS_METADATA_PUBLISH_MAX_RETRY_ROUNDS,
        retry_delay: DAEMON_DFS_METADATA_DISCOVERY_RETRY_INITIAL,
        max_retry_delay: DAEMON_DFS_METADATA_DISCOVERY_RETRY_MAX,
    }
}

pub(super) fn log_dfs_metadata_replication_report(report: DfsMetadataJournalReplicationReport) {
    for line in sinomo_dfs::dfs_metadata_replication_notice_lines(&report) {
        tracing::warn!("sinomo daemon {line}");
    }
    for line in sinomo_dfs::dfs_metadata_replication_fault_notice_lines(&report) {
        tracing::warn!(
            healing_class = "fault-inconsistency",
            "sinomo daemon {line}"
        );
    }
}

pub(crate) fn schedule_daemon_dfs_metadata_publish_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> Result<DfsMetadataPublishScheduleStart, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    Ok(
        schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
            data_dir,
            cluster_paths,
            runtime,
            traceparent,
            None,
        ),
    )
}

pub(super) fn schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    exact_lane: Option<DaemonDfsMutationLaneKey>,
) -> DfsMetadataPublishScheduleStart {
    // A served mutation already carries its authoritative filesystem/shard
    // identity. Mark that exact lane dirty before any fallible topology read;
    // planning inside the retained publisher will retry transient state or KV
    // projection failures without losing the committed journal edge.
    if let Some(lane) = exact_lane {
        return schedule_daemon_dfs_metadata_publish_lane_on_runtime(
            data_dir,
            cluster_paths,
            runtime,
            traceparent,
            lane,
        );
    }

    // Namespace/global operations and lifecycle materialization can append to
    // several shards. Retain the discovery edge before reading fallible
    // cluster state or topology; a transient read failure therefore retries
    // from this per-root lane without needing another DFS mutation.
    schedule_daemon_dfs_metadata_publish_discovery_on_runtime(
        data_dir,
        cluster_paths,
        runtime,
        traceparent,
    )
}

pub(super) fn schedule_daemon_dfs_allocator_cleanup_on_runtime(
    data_dir: SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    mutation_scheduler: Arc<DaemonDfsMutationScheduler>,
) {
    let cleanup_scheduler = match daemon_dfs_allocator_cleanup_scheduler(&data_dir) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::warn!(
                data_root = %data_dir.root().display(),
                "sinomo daemon DFS allocator cleanup could not be scheduled: {error}"
            );
            return;
        }
    };
    let run_scheduler = Arc::clone(&cleanup_scheduler);
    let report_data_root = data_dir.root();
    let error_data_root = report_data_root.clone();
    let owner_data_dir = data_dir.clone();
    let (_, task) = crate::schedule_dfs_metadata_publish_async_on_runtime_owned(
        Arc::clone(&cleanup_scheduler),
        runtime.clone(),
        DfsMetadataPublishScheduleOptions {
            max_retry_rounds: 0,
            ..daemon_dfs_metadata_publish_schedule_options()
        },
        move || {
            let data_dir = data_dir.clone();
            let mutation_scheduler = Arc::clone(&mutation_scheduler);
            let run_scheduler = Arc::clone(&run_scheduler);
            async move {
                loop {
                    // Publish interest before waiting for quiet. A foreground
                    // mutation can then invalidate this pass without queueing
                    // behind allocator work, even while it is still entering
                    // cluster or local admission.
                    let foreground_preemption = mutation_scheduler.begin_maintenance_interest();
                    tokio::select! {
                        biased;
                        _ = foreground_preemption.requested() => continue,
                        _ = tokio::time::sleep(DAEMON_DFS_ALLOCATOR_CLEANUP_QUIET_PERIOD) => {}
                    }
                    if run_scheduler.has_pending_request() {
                        // Let the retained scheduler consume the new cleanup
                        // edge and restart the quiet period from that request.
                        return Ok(DfsMetadataJournalReplicationReport {
                            outcomes: Vec::new(),
                        });
                    }
                    let Some(global_admission) = mutation_scheduler.try_admit_global() else {
                        // Never become a queued writer ahead of foreground
                        // exact-shard traffic. A later quiet pass retries.
                        continue;
                    };
                    if foreground_preemption.is_requested() {
                        drop(global_admission);
                        continue;
                    }

                    let trim_data_dir = data_dir.clone();
                    let trim_started = Instant::now();
                    let allocator_trimmed = tokio::task::spawn_blocking(move || {
                        release_daemon_dfs_idle_allocator_pages(&trim_data_dir)
                    })
                    .await
                    .map_err(|error| format!("DFS allocator cleanup task failed: {error}"))?;
                    daemon_dfs_perf_trace(format_args!(
                        "dfs-allocator-cleanup data_root={} trimmed={} elapsed_ms={}",
                        data_dir.root().display(),
                        allocator_trimmed,
                        trim_started.elapsed().as_millis(),
                    ));
                    drop(global_admission);
                    return Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    });
                }
            }
        },
        move |_| {
            daemon_dfs_perf_trace(format_args!(
                "dfs-allocator-cleanup-complete data_root={}",
                report_data_root.display()
            ));
        },
        move |error| {
            tracing::warn!(
                data_root = %error_data_root.display(),
                "sinomo daemon DFS allocator cleanup failed: {error}"
            );
        },
    );
    own_dfs_publish_task(&owner_data_dir, task);
}

pub(super) fn schedule_daemon_dfs_replica_materialization_on_runtime(
    data_dir: SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    shard: ResidentLocalMetadataShardHandle,
    data_owner_lock: Arc<DaemonDataOwnerLock>,
    generation_fence: DaemonDfsMutationGenerationFence,
) {
    let owner_data_dir = data_dir.clone();
    let task = start_daemon_dfs_replica_materialization_on_runtime(
        data_dir,
        runtime,
        shard,
        data_owner_lock,
        generation_fence,
        DAEMON_DFS_REPLICA_MATERIALIZATION_QUIET_PERIOD,
    );
    own_dfs_publish_task(&owner_data_dir, task);
}

pub(super) fn start_daemon_dfs_replica_materialization_on_runtime(
    data_dir: SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    shard: ResidentLocalMetadataShardHandle,
    data_owner_lock: Arc<DaemonDataOwnerLock>,
    generation_fence: DaemonDfsMutationGenerationFence,
    quiet_period: Duration,
) -> Option<tokio::task::JoinHandle<()>> {
    let file_system_id = shard.file_system_id().clone();
    let shard_id = shard.shard_id().clone();
    let scheduler = match daemon_dfs_metadata_materialization_lane_scheduler(
        &data_dir,
        &file_system_id,
        &shard_id,
    ) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::warn!(
                file_system_id = %file_system_id,
                shard_id = %shard_id.0,
                "sinomo daemon DFS replica materialization could not be scheduled: {error}"
            );
            return None;
        }
    };
    let mutation_scheduler = generation_fence.scheduler();
    // The accepted journal is durable. Retain its replay obligation by
    // identity, not by a generation read lock that blocks global foreground
    // work throughout the quiet period. Never reuse `shard` after this fence
    // is released: the materializer resolves the current canonical generation.
    drop(shard);
    drop(generation_fence);
    let lane = Arc::new(DaemonDfsReplicaMaterializationLane {
        data_dir,
        runtime: runtime.clone(),
        lane: DaemonDfsMutationLaneKey {
            file_system_id: file_system_id.clone(),
            shard_id: shard_id.clone(),
        },
        mutation_scheduler,
        _data_owner_lock: data_owner_lock,
        allocator_cleanup_needed: AtomicBool::new(false),
    });
    let report_file_system_id = file_system_id.clone();
    let report_shard_id = shard_id.clone();
    let error_file_system_id = file_system_id;
    let error_shard_id = shard_id;
    let (_, task) = crate::schedule_dfs_metadata_publish_async_on_runtime_owned(
        Arc::clone(&scheduler),
        runtime.clone(),
        // A transient authority rollover must not discard accepted replay.
        // Retry/backoff retains only identity and data ownership, never a
        // mutation admission or a stale resident handle.
        daemon_dfs_metadata_publish_schedule_options(),
        move || {
            let lane = Arc::clone(&lane);
            let scheduler = Arc::clone(&scheduler);
            async move {
                let materialized = loop {
                    let foreground_preemption = lane.begin_foreground_preemption();
                    tokio::select! {
                        biased;
                        _ = foreground_preemption.requested() => continue,
                        _ = tokio::time::sleep(quiet_period) => {}
                    }
                    if scheduler.has_pending_request() {
                        // The retained run loop consumes the new append edge
                        // and starts its quiet interval. A foreground-only
                        // edge above keeps this task alive even if that request
                        // is cancelled or performs no metadata append.
                        return Ok(DfsMetadataJournalReplicationReport {
                            outcomes: Vec::new(),
                        });
                    }
                    let cluster_paths = ClusterPaths::new(&lane.data_dir);
                    let peer_snapshot = ensure_daemon_dfs_metadata_publish_resident_generation(
                        Arc::clone(&lane.mutation_scheduler),
                        lane.data_dir.clone(),
                        cluster_paths.clone(),
                        lane.lane.clone(),
                        lane.runtime.clone(),
                        Arc::clone(&lane._data_owner_lock),
                    )
                    .await?;
                    if foreground_preemption.is_requested() {
                        continue;
                    }
                    let materialize_lane = Arc::clone(&lane);
                    break sinomo_dfs::run_metadata_publish_lane_planning_with_admission(
                        Arc::clone(&lane.mutation_scheduler),
                        lane.lane.clone(),
                        move || {
                            // This closure and the helper's admission move
                            // into the blocking task. Cancelling the scheduler
                            // cannot release either capability during replay.
                            let shard = daemon_dfs_message_cache_exact_admission_handle(
                                &materialize_lane.data_dir,
                                &cluster_paths,
                                &peer_snapshot,
                                &materialize_lane.lane,
                                &materialize_lane.runtime,
                            )
                            .map_err(|error| error.to_string())?
                            .ok_or_else(|| {
                                "DFS replica materialization resident generation changed".to_string()
                            })?;
                            let Some(backend) = shard.try_checkout().map_err(|error| error.to_string())?
                            else {
                                return Ok(sinomo_dfs::DfsMetadataPublishPlanningAttempt::WaitForResidentShard(shard));
                            };
                            let materialized = materialize_lane
                                .materialize(backend)
                                .map_err(|error| error.to_string())?;
                            Ok(sinomo_dfs::DfsMetadataPublishPlanningAttempt::Complete(materialized))
                        },
                    )
                    .await?;
                };
                daemon_dfs_perf_trace(format_args!(
                    "dfs-replica-materialization filesystem={} shard={} entries={materialized}",
                    lane.lane.file_system_id, lane.lane.shard_id.0,
                ));
                Ok(DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                })
            }
        },
        move |_| {
            daemon_dfs_perf_trace(format_args!(
                "dfs-replica-materialization-complete filesystem={} shard={}",
                report_file_system_id, report_shard_id.0
            ));
        },
        move |error| {
            tracing::warn!(
                file_system_id = %error_file_system_id,
                shard_id = %error_shard_id.0,
                "sinomo daemon DFS replica materialization failed: {error}"
            );
        },
    );
    task
}

/// Resolve a retained global publish edge without polling a synchronous cold
/// cache miss. The preflight owns the weak-registry authority generation until
/// lane discovery has consumed the exact proof; concurrent callers join its
/// single bounded ReadIndex and are woken by the refresh-owner guard.
pub(super) async fn daemon_dfs_metadata_publish_discovery_lanes_async(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<Vec<DaemonDfsMutationLaneKey>, String> {
    let state = sinomo_core::cluster::read_cluster_state_without_kv_async(runtime, cluster_paths)
        .await
        .map_err(|error| format!("DFS metadata lanes lack cluster state: {error}"))?;
    let preflight = warm_daemon_dfs_metadata_authority_for_identity_async(
        cluster_paths,
        &state.cluster_id,
        &state.local_node_id,
        runtime,
    )
    .await
    .map_err(|error| format!("DFS metadata lanes lack authority projection: {error}"))?;
    let authoritative = preflight
        .authority
        .retained_complete_prefix_for_generation(preflight.generation)
        .ok_or_else(|| {
            format!(
                "DFS metadata authority advanced after discovery preflight of generation {}",
                preflight.generation
            )
        })?;
    let lanes =
        sinomo_dfs::metadata_publish_lanes_from_authority(&state.local_node_id, &authoritative)?;
    if lanes.is_empty() {
        let peer_snapshot = read_daemon_dfs_message_peer_snapshot(cluster_paths)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "DFS metadata lanes lack a cluster peer snapshot".to_string())?;
        ensure_daemon_dfs_idle_resident_generation_after_preflight(
            data_dir.clone(),
            cluster_paths.clone(),
            peer_snapshot,
            preflight,
            runtime.clone(),
        )
        .await?;
    }
    Ok(lanes)
}

/// Keep a node with no local-primary publish lanes on the current resident
/// metadata generation. Without this authority-edge rollover, a node omitted
/// from every shard placement can retain its startup generation forever:
/// neither a primary publisher nor replica ingress exists to take the global
/// admission that replaces the stale cache entry.
async fn ensure_daemon_dfs_idle_resident_generation_after_preflight(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    peer_snapshot: DaemonDfsMessagePeerSnapshot,
    preflight: DaemonDfsMetadataAuthorityPreflight,
    runtime: tokio::runtime::Handle,
) -> Result<(), String> {
    let already_current = {
        let cache = daemon_dfs_message_service_cache()
            .lock()
            .map_err(|_| "DFS metadata resident cache is poisoned".to_string())?;
        cache.get(&data_dir.root()).is_some_and(|entry| {
            entry.signature.cluster_id == peer_snapshot.cluster_id
                && entry.signature.local_node_id == peer_snapshot.local_node_id
                && entry.signature.metadata_authority_generation == preflight.generation
                && Arc::ptr_eq(&entry.metadata_authority_generation, &preflight.authority)
                && entry.resident_metadata_initialized
                && entry.resident_metadata_backend.is_some()
        })
    };
    if already_current {
        return Ok(());
    }

    let data_owner_lock = DaemonDfsControlPlaneKvClient::discover_data_owner_lock(&cluster_paths)
        .ok_or_else(|| {
        "DFS metadata idle-generation rollover lacks the live data-owner capability".to_string()
    })?;
    let mutation_scheduler =
        daemon_dfs_mutation_scheduler(&data_dir).map_err(|error| error.to_string())?;
    let admission = mutation_scheduler
        .admit(&DfsMessageExecutionScope::GlobalExclusive)
        .await
        .map_err(|error| format!("DFS metadata idle-generation admission failed: {error}"))?;
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        let _data_owner_lock = data_owner_lock;
        daemon_dfs_resident_metadata_backend_after_preflight(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            &preflight,
            &runtime,
        )
        .map(drop)
        .map_err(|error| format!("DFS metadata idle-generation rollover failed: {error}"))
    })
    .await
    .map_err(|error| format!("DFS metadata idle-generation task failed: {error}"))?
}

pub(super) type DaemonDfsMetadataPublishDiscoveryFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Vec<DaemonDfsMutationLaneKey>, String>> + Send + 'a,
    >,
>;

/// Heap-erases the deep Raft-forwarding future below the retained publisher.
///
/// The discovery path can enter network route selection through an
/// authoritative prefix read. Keeping that generic future inline makes one
/// scheduler poll construct the complete nested transport stack on a Tokio
/// worker. Dynamic dispatch here bounds the scheduler's poll frame while the
/// future state itself remains owned by the task.
pub(super) fn daemon_dfs_metadata_publish_discovery_lanes_boxed_async<'a>(
    data_dir: &'a SinomoDataDir,
    cluster_paths: &'a ClusterPaths,
    runtime: &'a tokio::runtime::Handle,
) -> DaemonDfsMetadataPublishDiscoveryFuture<'a> {
    Box::pin(daemon_dfs_metadata_publish_discovery_lanes_async(
        data_dir,
        cluster_paths,
        runtime,
    ))
}

pub(super) async fn ensure_daemon_dfs_metadata_publish_resident_generation(
    mutation_scheduler: Arc<DaemonDfsMutationScheduler>,
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    lane: DaemonDfsMutationLaneKey,
    runtime: tokio::runtime::Handle,
    data_owner_lock: Arc<DaemonDataOwnerLock>,
) -> Result<DaemonDfsMessagePeerSnapshot, String> {
    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "DFS metadata publish lacks a cluster peer snapshot".to_string())?;
    let metadata_authority_preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
        &cluster_paths,
        &peer_snapshot,
        &runtime,
    )
    .await
    .map_err(|error| format!("DFS metadata publish authority preflight failed: {error}"))?;
    if daemon_dfs_message_cache_exact_admission_is_current(
        &data_dir,
        &cluster_paths,
        &peer_snapshot,
        &lane,
        &runtime,
    )
    .map_err(|error| error.to_string())?
    {
        return Ok(peer_snapshot);
    }

    // Opening or replacing the canonical resident generation is a global
    // topology operation. Most publishes take the fast path above; the first
    // publish after startup or an authority rollover performs this one fence
    // before returning to independent exact-shard planning.
    let admission = mutation_scheduler
        .admit(&DfsMessageExecutionScope::GlobalExclusive)
        .await
        .map_err(|error| format!("DFS metadata resident initialization failed: {error}"))?;
    let initialize_peer_snapshot = peer_snapshot.clone();
    let initialize_lane = lane.clone();
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        let _data_owner_lock = data_owner_lock;
        let resident = daemon_dfs_resident_metadata_backend_after_preflight(
            &data_dir,
            &cluster_paths,
            &initialize_peer_snapshot,
            &metadata_authority_preflight,
            &runtime,
        )
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "DFS metadata publish has no resident backend for filesystem {} shard {}",
                initialize_lane.file_system_id, initialize_lane.shard_id.0
            )
        })?;
        resident
            .scope_shard(&initialize_lane.file_system_id, &initialize_lane.shard_id)
            .map(|_| ())
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("DFS metadata resident initialization task failed: {error}"))??;
    Ok(peer_snapshot)
}

pub(super) fn schedule_daemon_dfs_metadata_publish_discovery_on_runtime(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> DfsMetadataPublishScheduleStart {
    let scheduler = match daemon_dfs_metadata_publish_discovery_scheduler(&data_dir) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::warn!("sinomo daemon DFS metadata discovery could not be scheduled: {error}");
            return DfsMetadataPublishScheduleStart::AlreadyRunning;
        }
    };
    let owner_data_dir = data_dir.clone();
    let publish_data_dir = data_dir;
    let publish_cluster_paths = cluster_paths;
    let publish_runtime = runtime.clone();
    let report_traceparent = traceparent.clone();
    let error_traceparent = traceparent;
    let (start, task) = crate::schedule_dfs_metadata_publish_async_on_runtime_owned(
        scheduler,
        runtime.clone(),
        daemon_dfs_metadata_publish_discovery_schedule_options(),
        move || {
            let data_dir = publish_data_dir.clone();
            let cluster_paths = publish_cluster_paths.clone();
            let runtime = publish_runtime.clone();
            async move {
                let discovered = daemon_dfs_metadata_publish_discovery_lanes_boxed_async(
                    &data_dir,
                    &cluster_paths,
                    &runtime,
                )
                .await;
                #[cfg(test)]
                record_daemon_dfs_metadata_publish_discovery_attempt(
                    &data_dir.root(),
                    discovered.is_ok(),
                );
                let lanes = discovered?;
                for lane in lanes {
                    schedule_daemon_dfs_metadata_publish_lane_on_runtime(
                        data_dir.clone(),
                        cluster_paths.clone(),
                        &runtime,
                        None,
                        lane,
                    );
                }
                Ok(DfsMetadataJournalReplicationReport {
                    outcomes: Vec::new(),
                })
            }
        },
        move |report| {
            daemon_dfs_perf_trace_with_traceparent(
                report_traceparent.as_deref(),
                format_args!(
                    "dfs-metadata-publish-discovery-complete outcomes={}",
                    report.outcomes.len()
                ),
            );
        },
        move |error| {
            daemon_dfs_perf_trace_with_traceparent(
                error_traceparent.as_deref(),
                format_args!("dfs-metadata-publish-discovery-failed error={error}"),
            );
            tracing::warn!("sinomo daemon DFS metadata lane discovery failed: {error}");
        },
    );
    own_dfs_publish_task(&owner_data_dir, task);
    start
}

/// Re-dirty only publisher lanes that are currently retained by durable
/// backlog/failure state for this data root. Connectivity owns the route
/// generation and calls this edge when its semantic projection changes; no
/// DFS-wide scan or steady-state timer is involved.
pub(crate) fn notify_daemon_dfs_metadata_publish_route_generation_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<usize, DaemonError> {
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let data_root = data_dir.root();
    let lanes = DAEMON_DFS_METADATA_PUBLISH_SCHEDULERS
        .get_or_init(|| DfsMetadataSchedulerRegistry::new("DFS metadata publish scheduler"))
        .live_keys()?
        .into_iter()
        .filter(|key| key.data_root == data_root)
        .map(|key| DaemonDfsMutationLaneKey {
            file_system_id: key.file_system_id,
            shard_id: key.shard_id,
        })
        .collect::<Vec<_>>();
    let cluster_paths = ClusterPaths::new(&data_dir);
    for lane in &lanes {
        schedule_daemon_dfs_metadata_publish_lane_on_runtime(
            data_dir.clone(),
            cluster_paths.clone(),
            runtime,
            None,
            lane.clone(),
        );
    }
    Ok(lanes.len())
}

pub(super) fn schedule_daemon_dfs_metadata_publish_lane_on_runtime(
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    lane: DaemonDfsMutationLaneKey,
) -> DfsMetadataPublishScheduleStart {
    let scheduler = match daemon_dfs_metadata_publish_lane_scheduler(
        &data_dir,
        &lane.file_system_id,
        &lane.shard_id,
    ) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::warn!(
                file_system_id = %lane.file_system_id,
                shard_id = %lane.shard_id.0,
                "sinomo daemon DFS metadata lane could not be scheduled: {error}"
            );
            return DfsMetadataPublishScheduleStart::AlreadyRunning;
        }
    };
    let Some(data_owner_lock) =
        DaemonDfsControlPlaneKvClient::discover_data_owner_lock(&cluster_paths)
    else {
        tracing::warn!(
            file_system_id = %lane.file_system_id,
            shard_id = %lane.shard_id.0,
            "sinomo daemon DFS metadata lane lacks the live data-owner capability"
        );
        return DfsMetadataPublishScheduleStart::AlreadyRunning;
    };
    #[cfg(test)]
    record_daemon_dfs_metadata_publish_lane_request(&data_dir, &lane);
    let mutation_scheduler = match daemon_dfs_mutation_scheduler(&data_dir) {
        Ok(scheduler) => scheduler,
        Err(error) => {
            tracing::warn!(
                file_system_id = %lane.file_system_id,
                shard_id = %lane.shard_id.0,
                "sinomo daemon DFS metadata lane lacks mutation admission: {error}"
            );
            return DfsMetadataPublishScheduleStart::AlreadyRunning;
        }
    };
    let (_peer_transport, async_peer_transport) =
        daemon_dfs_peer_transports_on_runtime(&data_dir, runtime);
    let owner_data_dir = data_dir.clone();
    let publish_data_dir = data_dir;
    let publish_cluster_paths = cluster_paths;
    let publish_file_system_id = lane.file_system_id.clone();
    let publish_shard_id = lane.shard_id.clone();
    let publish_runtime = runtime.clone();
    let report_lane = lane.clone();
    let error_lane = lane;
    let report_traceparent = traceparent.clone();
    let error_traceparent = traceparent;
    let (start, task) = crate::schedule_dfs_metadata_publish_async_on_runtime_owned(
        scheduler,
        runtime.clone(),
        daemon_dfs_metadata_publish_schedule_options(),
        move || {
            let data_dir = publish_data_dir.clone();
            let cluster_paths = publish_cluster_paths.clone();
            let exclusion_paths = cluster_paths.clone();
            let file_system_id = publish_file_system_id.clone();
            let shard_id = publish_shard_id.clone();
            let async_peer_transport = Arc::clone(&async_peer_transport);
            let runtime = publish_runtime.clone();
            let data_owner_lock = Arc::clone(&data_owner_lock);
            let mutation_scheduler = Arc::clone(&mutation_scheduler);
            let lane = DaemonDfsMutationLaneKey {
                file_system_id: file_system_id.clone(),
                shard_id: shard_id.clone(),
            };
            async move {
                let peer_snapshot = ensure_daemon_dfs_metadata_publish_resident_generation(
                    Arc::clone(&mutation_scheduler),
                    data_dir.clone(),
                    cluster_paths.clone(),
                    lane.clone(),
                    runtime.clone(),
                    Arc::clone(&data_owner_lock),
                )
                .await?;

                let planning_data_owner_lock = Arc::clone(&data_owner_lock);
                let replication_options = daemon_dfs_metadata_replication_options();
                let planning_data_dir = data_dir.clone();
                let planning_cluster_paths = cluster_paths.clone();
                let planning_runtime = runtime.clone();
                let plan = sinomo_dfs::run_metadata_publish_lane_planning_with_admission(
                    mutation_scheduler,
                    lane,
                    move || {
                        // The scheduler task can be cancelled while this
                        // blocking planner is running. Keep exact-shard
                        // admission and the OS-backed data capability in this
                        // closure through local durable plan capture. Admission
                        // is intentionally released before the slow peer
                        // transport and cursor-checkpoint phase below.
                        let _data_owner_lock = Arc::clone(&planning_data_owner_lock);
                        let execution_exclusions =
                            sinomo_core::cluster::read_cluster_state_without_kv(&exclusion_paths)
                                .map(|state| {
                                    daemon_dfs_maintenance_execution_exclusions(
                                        &exclusion_paths,
                                        &state,
                                    )
                                })
                                .unwrap_or_default();
                        let authority_generation =
                            daemon_dfs_metadata_authority_generation_for_identity(
                                &planning_cluster_paths,
                                &peer_snapshot.cluster_id,
                                &peer_snapshot.local_node_id,
                                Some(&planning_runtime),
                            )
                            .map_err(|error| error.to_string())?;
                        let authority_generation_id = authority_generation.generation();
                        let authority_projection = authority_generation
                            .current_complete_prefix_for_generation(authority_generation_id)
                            .ok_or_else(|| {
                                format!(
                                    "DFS metadata publish lacks retained authority generation {authority_generation_id}"
                                )
                            })?;
                        let context =
                            daemon_dfs_metadata_replica_context_from_authority_projection(
                                &authority_projection,
                                &peer_snapshot.local_node_id,
                                &file_system_id,
                                &shard_id,
                            )
                            .map_err(|error| error.to_string())?
                            .ok_or_else(|| {
                                format!(
                                    "DFS metadata publish authority is missing filesystem {} shard {}",
                                    file_system_id, shard_id.0
                                )
                            })?;
                        let shard = daemon_dfs_message_cache_exact_admission_handle_for_replica_context(
                            &planning_data_dir,
                            &planning_cluster_paths,
                            &peer_snapshot,
                            &context,
                            &planning_runtime,
                        )
                        .map_err(|error| error.to_string())?
                        .ok_or_else(|| {
                            format!(
                                "DFS metadata publish resident backend does not match retained authority for filesystem {} shard {}",
                                file_system_id, shard_id.0
                            )
                        })?;
                        let Some(mut backend) =
                            shard.try_checkout().map_err(|error| error.to_string())?
                        else {
                            return Ok(
                                sinomo_dfs::DfsMetadataPublishPlanningAttempt::WaitForResidentShard(
                                    shard,
                                ),
                            );
                        };
                        let plan = sinomo_dfs::prepare_cluster_dfs_metadata_shard_publish_with_backend_and_authority(
                            &planning_data_dir,
                            &peer_snapshot.local_node_id,
                            peer_snapshot.peer_node_ids.clone(),
                            file_system_id.clone(),
                            shard_id.clone(),
                            Some(context.shard),
                            &mut backend,
                            &execution_exclusions,
                            replication_options,
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(sinomo_dfs::DfsMetadataPublishPlanningAttempt::Complete(plan))
                    },
                )
                .await?;
                let Some(plan) = plan else {
                    return Ok(DfsMetadataJournalReplicationReport {
                        outcomes: Vec::new(),
                    });
                };

                // Keep the daemon's data-owner capability alive while the
                // immutable plan is in flight, without occupying mutation
                // admission for its potentially multi-second network budget.
                // The outer blocking closure retains that capability even if
                // the scheduler future is cancelled during runtime shutdown.
                sinomo_dfs::run_metadata_publish_blocking_execution(
                    runtime,
                    data_owner_lock,
                    async move {
                        sinomo_dfs::execute_prepared_cluster_dfs_metadata_publish_with_async_peer_transport(
                            plan,
                            async_peer_transport,
                            replication_options,
                        )
                        .await
                        .map_err(|error| error.to_string())
                    },
                )
                .await
            }
        },
        move |report| {
            daemon_dfs_perf_trace_with_traceparent(
                report_traceparent.as_deref(),
                format_args!(
                    "dfs-metadata-publish-complete filesystem={} shard={} outcomes={}",
                    report_lane.file_system_id,
                    report_lane.shard_id.0,
                    report.outcomes.len()
                ),
            );
            log_dfs_metadata_replication_report(report);
        },
        move |error| {
            daemon_dfs_perf_trace_with_traceparent(
                error_traceparent.as_deref(),
                format_args!(
                    "dfs-metadata-publish-failed filesystem={} shard={} error={error}",
                    error_lane.file_system_id, error_lane.shard_id.0
                ),
            );
            tracing::warn!(
                file_system_id = %error_lane.file_system_id,
                shard_id = %error_lane.shard_id.0,
                "sinomo daemon DFS scheduled metadata publish failed: {error}"
            );
        },
    );
    own_dfs_publish_task(&owner_data_dir, task);
    start
}

#[cfg(test)]
pub(super) fn publish_default_dfs_metadata_journal_with_sender(
    paths: &DaemonPaths,
    sender: DfsMetadataJournalBatchSender,
) -> Result<DfsMetadataJournalReplicationReport, DaemonError> {
    publish_default_dfs_metadata_journal_with_senders(
        paths,
        sender,
        daemon_dfs_metadata_snapshot_install_sender(),
    )
}

#[cfg(test)]
pub(super) fn publish_default_dfs_metadata_journal_with_senders(
    paths: &DaemonPaths,
    sender: DfsMetadataJournalBatchSender,
    snapshot_sender: DfsMetadataSnapshotInstallSender,
) -> Result<DfsMetadataJournalReplicationReport, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    Ok(sinomo_dfs::publish_cluster_dfs_metadata_journal(
        &data_dir,
        &cluster_paths,
        sender,
        snapshot_sender,
        DfsMetadataJournalReplicationOptions::default(),
    )?)
}

pub(super) async fn run_daemon_dfs_smoke_async(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_dfs::DfsSmokeReport, DaemonError> {
    let data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let metadata_authority_preflight = if cluster_paths.state.exists() {
        let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)? else {
            return Err(DaemonError::ClusterOrphan);
        };
        Some(
            warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
                &cluster_paths,
                &peer_snapshot,
                runtime,
            )
            .await?,
        )
    } else {
        None
    };
    let smoke_guard = DaemonDfsSmokeInFlightGuard::acquire()?;
    let runtime = runtime.clone();
    let smoke_task = tokio::task::spawn_blocking(move || {
        let _data_owner_lock = data_owner_lock;
        let _smoke_guard = smoke_guard;
        // Keep the warmed generation alive while the synchronous smoke client
        // borrows it from the weak process registry.
        let _metadata_authority_preflight = metadata_authority_preflight;
        let (peer_transport, _) = daemon_dfs_peer_transports_on_runtime(&data_dir, &runtime);
        sinomo_dfs::run_default_cluster_dfs_smoke_with_peer_transport(
            &data_dir,
            &cluster_paths,
            peer_transport,
            Box::new(DaemonDfsControlPlaneKvClient::new_on_runtime(
                &cluster_paths,
                &runtime,
            )),
            daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(&runtime)),
            DfsDefaultClusterSmokeOptions {
                bootstrap: DfsDefaultClusterBootstrapOptions {
                    publish_cluster_control: false,
                    materialize_local_metadata: false,
                    local_zone: None,
                    local_content_advertisement: None,
                },
                smoke: daemon_dfs_smoke_options(),
            },
        )
    });
    let Some(report) = tokio::time::timeout(DAEMON_DFS_SMOKE_TIMEOUT, smoke_task)
        .await
        .map_err(|_| DaemonError::Network {
            address: "DFS smoke".to_string(),
            detail: format!(
                "DFS smoke did not finish within {}ms",
                DAEMON_DFS_SMOKE_TIMEOUT.as_millis()
            ),
        })?
        .map_err(|source| DaemonError::Network {
            address: "DFS smoke".to_string(),
            detail: format!("DFS smoke task failed: {source}"),
        })??
    else {
        return Err(DaemonError::ClusterOrphan);
    };
    Ok(report)
}
