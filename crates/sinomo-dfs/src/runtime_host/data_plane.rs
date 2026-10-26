use super::*;
use crate::{DfsMessageResponse, DfsNetworkMessage, DfsRoutedMessage};

#[derive(Clone, Debug)]
pub(crate) struct DaemonDfsDataPlaneHost {
    pub(super) paths: DaemonPaths,
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) event_client: Option<ComponentEventClient>,
}

impl DaemonDfsDataPlaneHost {
    #[cfg(test)]
    pub(crate) fn new_on_runtime(paths: DaemonPaths, runtime: &tokio::runtime::Handle) -> Self {
        Self {
            paths,
            runtime: runtime.clone(),
            event_client: None,
        }
    }

    pub(crate) fn new_on_runtime_with_event_client(
        paths: DaemonPaths,
        runtime: &tokio::runtime::Handle,
        event_client: ComponentEventClient,
    ) -> Self {
        Self {
            paths,
            runtime: runtime.clone(),
            event_client: Some(event_client),
        }
    }

    pub(super) fn validate_data_dir(
        &self,
        data_dir: &SinomoDataDir,
    ) -> Result<(), DfsDataPlaneHostError> {
        let configured_data_root =
            daemon_data_dir(&self.paths).map_err(daemon_dfs_data_plane_error)?;
        if data_dir.root() == configured_data_root {
            return Ok(());
        }
        Err(DfsDataPlaneHostError::new(format!(
            "DFS service data root {} does not match daemon data root {}",
            data_dir.root().display(),
            configured_data_root.display()
        )))
    }

    pub(super) fn require_data_owner(
        &self,
    ) -> Result<Arc<DaemonDataOwnerLock>, DfsDataPlaneHostError> {
        require_daemon_dfs_data_owner_lock(&self.paths).map_err(daemon_dfs_data_plane_error)
    }
}

/// This comparison permits only a pre-execution retry, never execution under
/// the earlier unfenced receiver observation.
pub(super) fn replica_context_can_reselect_committed_fence(
    previous: &sinomo_dfs::DfsMetadataReplicaShardContext,
    current: &sinomo_dfs::DfsMetadataReplicaShardContext,
) -> bool {
    if previous.local_node_id != current.local_node_id || previous.shard != current.shard {
        return false;
    }
    let (Some(previous), Some(current)) = (
        previous.pending_membership_handoff.as_ref(),
        current.pending_membership_handoff.as_ref(),
    ) else {
        return false;
    };
    let Some(fence) = current.retiring_primary_fence.as_ref() else {
        return false;
    };
    if previous.retiring_primary_fence.is_some()
        || fence.handoff_id != current.handoff_id
        || fence.primary_node_id != current.retiring_shard.primary_node_id
        || fence.journal_sequence < current.required_prepared_sequence
        || fence.state_fingerprint.is_empty()
    {
        return false;
    }
    let mut same_handoff = previous.clone();
    same_handoff.retiring_primary_fence = Some(fence.clone());
    same_handoff == *current
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_daemon_dfs_resident_replica_operation<R, F>(
    paths: &DaemonPaths,
    data_dir: Arc<SinomoDataDir>,
    mut expected_context: sinomo_dfs::DfsMetadataReplicaShardContext,
    runtime: &tokio::runtime::Handle,
    preempt_maintenance: bool,
    materialize_after: bool,
    trace_request: (&str, &'static str),
    operation: F,
) -> Result<DaemonDfsResidentReplicaOperation<R>, DaemonError>
where
    R: Send + 'static,
    F: FnOnce(
            &sinomo_dfs::ResidentLocalMetadataShardHandle,
            &sinomo_dfs::DfsMetadataReplicaShardContext,
        ) -> Result<R, sinomo_dfs::MetadataBackendError>
        + Send
        + 'static,
{
    let data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    let cluster_paths = ClusterPaths::new(data_dir.as_ref());
    let peer_deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let Some(peer_snapshot) =
        read_daemon_dfs_message_peer_snapshot_before(&cluster_paths, runtime, peer_deadline)
            .await?
    else {
        return Err(DaemonError::ClusterOrphan);
    };
    if peer_snapshot.local_node_id != expected_context.local_node_id {
        return Ok(DaemonDfsResidentReplicaOperation::Rejected {
            reason: format!(
                "DFS replica authority context names local node {}, but the daemon identity is {}",
                expected_context.local_node_id, peer_snapshot.local_node_id
            ),
        });
    }
    let mut metadata_authority_preflight =
        warm_daemon_dfs_replica_authority_for_peer_snapshot_before(
            &cluster_paths,
            &peer_snapshot,
            runtime,
            peer_deadline,
        )
        .await?;
    let file_system_id = expected_context.shard.file_system_id.clone();
    let shard_id = DfsMetadataShardId::new(expected_context.shard.shard_id.clone());
    let exact_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let trace_enabled = tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE);
    let traceparent = trace_enabled
        .then(crate::service_trace::current_dfs_traceparent)
        .flatten();
    let mut admission_trace = DaemonDfsAdmissionTrace {
        label: "dfs-replica-admission",
        request: trace_request.0,
        kind: trace_request.1,
        attempt: 0,
        scope: &exact_scope,
        traceparent: traceparent.as_deref(),
        enabled: trace_enabled,
    };
    let scheduler = daemon_dfs_mutation_scheduler(data_dir.as_ref())?;
    let mut authority_refresh_retries = 0;
    let (admission, shard) = loop {
        admission_trace.attempt = authority_refresh_retries;
        let scheduler_started = admission_trace.start();
        let admitted = tracing::Instrument::instrument(
            admit_daemon_dfs_message(&scheduler, &exact_scope, preempt_maintenance),
            admission_trace.span("initial-scheduler"),
        )
        .await;
        admission_trace.record(
            "initial-scheduler",
            if admitted.is_ok() {
                "admitted"
            } else {
                "error"
            },
            scheduler_started,
            admitted.as_ref().ok(),
        );
        #[cfg(not(test))]
        let mut admission = admitted?;
        // Test observers borrow the actual guard's lifetime; they never acquire
        // another scheduler permit or require unrelated work to be idle.
        #[cfg(test)]
        let mut admission = Arc::new(admitted?);
        #[cfg(test)]
        if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir.as_ref()) {
            hook(
                DaemonDfsPreDispatchTestPoint::BeforeReplicaContextComparison,
                None,
                Some(&metadata_authority_preflight),
            )?;
        }
        // A ready successor with different shard/handoff DATA is an authority
        // rejection, not a retry grant. An incomplete successor is still handled
        // by the typed preexecution refresh below. Both checks borrow the existing
        // retained projection and perform no authority I/O under admission.
        let context_span = tracing::trace_span!(target: "sinomo.perf",
            "dfs.metadata.replica_context.compare", stage = "dfs.metadata.replica_context.compare",
            request = trace_request.0, kind = trace_request.1, attempt = authority_refresh_retries,
            traceparent = traceparent.as_deref().unwrap_or_default(), outcome = "error",
            node = %expected_context.local_node_id, data_root = %data_dir.root().display(),
            fs = %file_system_id, shard = %shard_id.0,
            preflight_generation = metadata_authority_preflight.generation,
            observed_generation = tracing::field::Empty, context_matches = tracing::field::Empty);
        let (observed_generation, context_matches) = context_span.in_scope(|| {
            metadata_authority_preflight
                .authority
                .replica_context_state(&expected_context)
        })?;
        context_span.record("observed_generation", observed_generation);
        context_span.record("context_matches", context_matches);
        context_span.record("outcome", "ok");
        if !context_matches
            && metadata_authority_preflight
                .authority
                .retained_complete_prefix_revision_for_generation(observed_generation)
                .is_some()
        {
            let current = metadata_authority_preflight
                .authority
                .retained_complete_prefix_for_generation(observed_generation)
                .map(|projection| {
                    daemon_dfs_metadata_replica_context_from_authority_projection(
                        &projection,
                        &peer_snapshot.local_node_id,
                        &file_system_id,
                        &shard_id,
                    )
                })
                .transpose()?
                .flatten();
            if !current.as_ref().is_some_and(|current| {
                replica_context_can_reselect_committed_fence(&expected_context, current)
            }) || authority_refresh_retries >= 2
            {
                return Ok(DaemonDfsResidentReplicaOperation::Rejected {
                    reason: format!(
                        "metadata replica authority context changed for filesystem {file_system_id} shard {}",
                        shard_id.0
                    ),
                });
            }
            // This is a new receiver observation before the FnOnce operation,
            // not permission to execute under the old unfenced context.
            #[cfg(test)]
            let previous_admission = Arc::downgrade(&admission);
            drop(admission);
            authority_refresh_retries += 1;
            tracing::trace!(target: "sinomo.dfs.perf",
                request = trace_request.0, kind = trace_request.1,
                data_root = %data_dir.root().display(), node = %peer_snapshot.local_node_id,
                fs = %file_system_id, shard = %shard_id.0,
                attempt = authority_refresh_retries, observed_generation,
                "DFS replica reselecting actual committed retirement fence before execution");
            #[cfg(test)]
            if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir.as_ref()) {
                hook(
                    DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased {
                        released: previous_admission.upgrade().is_none(),
                    },
                    None,
                    None,
                )?;
                hook(
                    DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh {
                        owner: "replica",
                        replan: authority_refresh_retries,
                    },
                    None,
                    None,
                )?;
            }
            metadata_authority_preflight =
                warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
                    &cluster_paths,
                    &peer_snapshot,
                    runtime,
                    peer_deadline,
                    #[cfg(test)]
                    None,
                )
                .await?;
            let projection = metadata_authority_preflight
                .authority
                .retained_complete_prefix_for_generation(metadata_authority_preflight.generation)
                .ok_or_else(|| crate::DfsError::Metadata {
                    message: "refreshed replica context has no complete retained projection".into(),
                })?;
            let refreshed = daemon_dfs_metadata_replica_context_from_authority_projection(
                &projection,
                &peer_snapshot.local_node_id,
                &file_system_id,
                &shard_id,
            )?;
            let Some(refreshed) = refreshed.filter(|context| {
                replica_context_can_reselect_committed_fence(&expected_context, context)
            }) else {
                return Ok(DaemonDfsResidentReplicaOperation::Rejected {
                    reason: "refreshed replica context changed its retiring handoff or shard"
                        .into(),
                });
            };
            expected_context = refreshed;
            continue;
        }
        let cache_started = admission
            .is_exact()
            .then(|| admission_trace.start())
            .flatten();
        let cached = admission_trace.span("exact-cache").in_scope(|| {
            Ok::<_, sinomo_dfs::DfsError>(if admission.is_exact() {
                daemon_dfs_message_cache_exact_admission_handle_for_replica_context(
                    data_dir.as_ref(),
                    &cluster_paths,
                    &peer_snapshot,
                    &expected_context,
                    runtime,
                )?
            } else {
                None
            })
        });
        admission_trace.record(
            "exact-cache",
            match &cached {
                Ok(Some(_)) => "eligible",
                Ok(None) => "ineligible",
                Err(_) => "error",
            },
            cache_started,
            Some(&admission),
        );
        let mut shard = cached?;
        if shard.is_none() {
            if !admission.is_global() {
                drop(admission);
                let upgrade_started = admission_trace.start();
                let upgraded = tracing::Instrument::instrument(
                    admit_daemon_dfs_message(
                        &scheduler,
                        &DfsMessageExecutionScope::GlobalExclusive,
                        preempt_maintenance,
                    ),
                    admission_trace.span("global-upgrade"),
                )
                .await;
                admission_trace.record(
                    "global-upgrade",
                    if upgraded.is_ok() {
                        "admitted"
                    } else {
                        "error"
                    },
                    upgrade_started,
                    upgraded.as_ref().ok(),
                );
                #[cfg(not(test))]
                {
                    admission = upgraded?;
                }
                #[cfg(test)]
                {
                    admission = Arc::new(upgraded?);
                }
            }
            // Generation creation and rollover require the global fence. Once the
            // resident generation is current, replica persistence checks out only
            // the exact shard and bypasses the routed service shell entirely.
            let resident_started = admission_trace.start();
            let resident = admission_trace.span("resident-refresh").in_scope(|| {
                let preflight = metadata_authority_preflight.for_admission(&admission);
                try_daemon_dfs_resident_metadata_backend_after_preflight(
                    data_dir.as_ref(),
                    &cluster_paths,
                    &peer_snapshot,
                    &preflight,
                    runtime,
                )
            });
            admission_trace.record(
                "resident-refresh",
                match &resident {
                    Ok(Some(_)) => "resident",
                    Ok(None) => "unhosted",
                    Err(_) => "error",
                },
                resident_started,
                Some(&admission),
            );
            let resident = match resident {
                Ok(resident) => resident,
                Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale)) => {
                    // No journal/snapshot operation has been invoked. Release the
                    // actual old admission before shared async proof refresh, then
                    // acquire and validate the same command/context afresh.
                    #[cfg(test)]
                    let previous_admission = Arc::downgrade(&admission);
                    drop(admission);
                    #[cfg(test)]
                    if let Some(hook) =
                        super::message_host::daemon_dfs_pre_dispatch_test_hook(data_dir.as_ref())
                    {
                        use super::message_host::DaemonDfsPreDispatchTestPoint;
                        hook(
                            DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased {
                                released: previous_admission.upgrade().is_none(),
                            },
                            None,
                            None,
                        )?;
                    }
                    if authority_refresh_retries >= 2 {
                        return Err(stale.into_dfs_error().into());
                    }
                    authority_refresh_retries += 1;
                    daemon_dfs_record_authority_replan(
                        data_dir.as_ref(),
                        "replica",
                        authority_refresh_retries,
                        stale,
                    )?;
                    metadata_authority_preflight =
                        warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
                            &cluster_paths,
                            &peer_snapshot,
                            runtime,
                            peer_deadline,
                            #[cfg(test)]
                            None,
                        )
                        .await?;
                    continue;
                }
                Err(DaemonDfsAuthorityPreExecutionError::Failed(error)) => {
                    return Err(error.into());
                }
            };
            if resident.is_none() {
                return Ok(DaemonDfsResidentReplicaOperation::Rejected {
                    reason: format!(
                        "this node does not host filesystem {file_system_id} metadata shard {}",
                        shard_id.0
                    ),
                });
            }
            let cache_started = admission_trace.start();
            let cached = admission_trace
                .span("exact-cache-after-refresh")
                .in_scope(|| {
                    daemon_dfs_message_cache_exact_admission_handle_for_replica_context(
                        data_dir.as_ref(),
                        &cluster_paths,
                        &peer_snapshot,
                        &expected_context,
                        runtime,
                    )
                });
            admission_trace.record(
                "exact-cache-after-refresh",
                match &cached {
                    Ok(Some(_)) => "eligible",
                    Ok(None) => "ineligible",
                    Err(_) => "error",
                },
                cache_started,
                Some(&admission),
            );
            shard = cached?;
        }
        let Some(shard) = shard else {
            return Ok(DaemonDfsResidentReplicaOperation::Rejected {
                reason: format!(
                    "metadata replica authority context is stale for filesystem {file_system_id} shard {}",
                    shard_id.0
                ),
            });
        };
        #[cfg(test)]
        let admission = Arc::try_unwrap(admission)
            .expect("preflight observers never retain actual mutation admission");
        break (admission, shard);
    };
    admission_trace.ready(&admission);
    let blocking_span = admission_trace.span("blocking");
    let blocking_context = trace_enabled.then(|| {
        (
            trace_request.0.to_owned(),
            traceparent.clone(),
            exact_scope.clone(),
        )
    });
    let request_kind = trace_request.1;
    let process_shard = shard.clone();
    let process_data_dir = data_dir.as_ref().clone();
    let process_data_owner_lock = Arc::clone(&data_owner_lock);
    let process_runtime = runtime.clone();
    let queue_started = admission_trace.start();
    let result = tokio::task::spawn_blocking(move || {
        let _trace_span = blocking_span.enter();
        let blocking_trace = blocking_context
            .as_ref()
            .map(|(request, traceparent, scope)| DaemonDfsAdmissionTrace {
                label: "dfs-replica-admission",
                request,
                kind: request_kind,
                attempt: 0,
                scope,
                traceparent: traceparent.as_deref(),
                enabled: true,
            });
        if let Some(trace) = blocking_trace.as_ref() {
            trace.record("blocking-queue", "started", queue_started, Some(&admission));
        }
        // Both capabilities live inside the blocking task: cancellation of the
        // object-RPC future cannot admit another mutation or invalidate daemon
        // data ownership while durable persistence is still running.
        let checkout_started = blocking_trace
            .as_ref()
            .and_then(DaemonDfsAdmissionTrace::start);
        if let Some(trace) = blocking_trace.as_ref() {
            trace.record(
                "backend-operation",
                "started",
                checkout_started,
                Some(&admission),
            );
        }
        #[cfg(any(test, feature = "test-support"))]
        let _native_boundary = crate::native_shard_trace::enter_resident_replica_boundary_context(
            &process_data_dir.root(),
            &process_data_owner_lock,
            &process_shard,
            &expected_context.local_node_id,
            &admission,
        );
        let result = operation(&process_shard, &expected_context).map_err(DaemonError::from)?;
        if materialize_after {
            // Register the replay edge while still owning producer admission.
            // The worker can then claim all accepted inputs under that same
            // shard gate without racing a late notification from this append.
            schedule_daemon_dfs_replica_materialization_on_runtime(
                process_data_dir,
                &process_runtime,
                process_shard,
                process_data_owner_lock,
                admission,
            );
        } else {
            drop(admission);
            drop(process_data_owner_lock);
        }
        Ok::<R, DaemonError>(result)
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS replica ingress".to_string(),
        detail: format!("DFS resident replica task failed: {source}"),
    })??;
    Ok(DaemonDfsResidentReplicaOperation::Completed { result })
}

pub(super) enum DaemonDfsResidentReplicaOperation<R> {
    Completed { result: R },
    Rejected { reason: String },
}

pub(super) async fn handle_daemon_dfs_inspection_async(
    paths: &DaemonPaths,
    request: sinomo_dfs::DaemonDfsInspectionRequest,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_dfs::DaemonDfsInspectionResponse, DaemonError> {
    let data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        let inspection_data_dir = data_dir.clone();
        return tokio::task::spawn_blocking(move || {
            sinomo_dfs::inspect_cluster_aware_dfs(
                &inspection_data_dir,
                &ClusterPaths::new(&inspection_data_dir),
                request,
            )
            .map_err(DaemonError::from)
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "DFS inspection".to_string(),
            detail: format!("DFS inspection task failed: {source}"),
        })?;
    }

    // Inspection borrows the mutation lane's backend, so reads and operator
    // mutations use the same global scheduler owner as maintenance and
    // foreground traffic. Holding admission inside the blocking task preserves
    // cancellation safety.
    let peer_deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let Some(preflight_peer_snapshot) =
        read_daemon_dfs_message_peer_snapshot_before(&cluster_paths, runtime, peer_deadline)
            .await?
    else {
        return Err(DaemonError::ClusterOrphan);
    };
    let metadata_authority_preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
        &cluster_paths,
        &preflight_peer_snapshot,
        runtime,
        peer_deadline,
        #[cfg(test)]
        None,
    )
    .await?;
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
    let admission =
        admit_daemon_dfs_foreground_message(&scheduler, &DfsMessageExecutionScope::GlobalExclusive)
            .await?;
    let inspection_runtime = runtime.clone();
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        let _data_owner_lock = data_owner_lock;
        let peer_snapshot = preflight_peer_snapshot;
        let (lane, _) = daemon_dfs_message_lane(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            &inspection_runtime,
            DaemonDfsMessageLaneKind::MutationGlobal,
            true,
            Some(&metadata_authority_preflight),
        )?;
        let mut lane = lane.lock().map_err(|_| DaemonError::Network {
            address: "DFS inspection".to_string(),
            detail: "DFS mutation service lane is poisoned".to_string(),
        })?;
        lane.refresh_peer_targets(peer_snapshot.peer_node_ids);
        let resident_scope = lane.resident_metadata_scope.clone();
        if let Some(scope) = resident_scope.as_ref() {
            scope.attach(&mut lane.service)?;
        }
        // A prior standalone inspection may have retained a duplicate decoded
        // generation. The daemon-owned lane is now the reusable source.
        sinomo_dfs::evict_cluster_aware_dfs_metadata_inspection_cache(&data_dir);
        let result = lane
            .service
            .metadata_backend_mut()
            .ok_or_else(|| DaemonError::Parse {
                detail: "DFS mutation service lane has no metadata backend".to_string(),
            })
            .and_then(|metadata| {
                let read_only = true;
                // The resident backend is also advanced by replica and
                // foreground lanes. Refresh its tables once, then make every
                // page in this inspection borrow that coherent generation.
                // Without this scope, the generic read path may decode and
                // retain a second complete table generation after a checkpoint
                // rotation.
                if read_only {
                    metadata
                        .begin_consistent_read_snapshot()
                        .map_err(DaemonError::from)?;
                }
                let result = sinomo_dfs::inspect_cluster_aware_dfs_from_metadata(
                    &data_dir,
                    &cluster_paths,
                    metadata,
                    request,
                )
                .map_err(DaemonError::from);
                if read_only {
                    metadata.end_consistent_read_snapshot();
                }
                result
            });
        let release_result = resident_scope
            .is_some()
            .then(|| lane.service.release_resident_metadata_backend())
            .transpose();
        drop(lane);
        let allocator_trim_started = Instant::now();
        let allocator_trimmed = release_daemon_dfs_idle_allocator_pages(&data_dir);
        tracing::trace!(
            target: "sinomo.dfs.perf",
            allocator_trimmed,
            allocator_trim_ms = allocator_trim_started.elapsed().as_millis(),
            "dfs inspection allocator trim"
        );
        release_result?;
        result
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS inspection".to_string(),
        detail: format!("DFS inspection task failed: {source}"),
    })?
}

pub(super) async fn handle_daemon_dfs_status_async(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_dfs::DfsOperatorStatus, DaemonError> {
    let data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return match handle_daemon_dfs_inspection_async(
            paths,
            sinomo_dfs::DaemonDfsInspectionRequest::Status,
            runtime,
        )
        .await?
        {
            sinomo_dfs::DaemonDfsInspectionResponse::Status { status } => Ok(status),
            _ => unreachable!("status inspection returned a non-status response"),
        };
    }

    // Cluster status is a read of the last complete resident generation. It
    // must not join the global mutation queue, initialize a missing backend,
    // or hydrate/scan shard tables merely to answer an operator probe.
    let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
    let admission =
        admit_daemon_dfs_foreground_message(&scheduler, &DfsMessageExecutionScope::ReadOnly)
            .await?;
    tokio::task::spawn_blocking(move || {
        let _admission = admission;
        let _data_owner_lock = data_owner_lock;
        let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)? else {
            return Err(DaemonError::ClusterOrphan);
        };
        let resident = match daemon_dfs_bounded_status_resident(&data_dir, &peer_snapshot) {
            Ok(resident) => resident,
            Err(error) => {
                return sinomo_dfs::inspect_incomplete_dfs_status(
                    &data_dir,
                    &cluster_paths,
                    error.to_string(),
                )
                .map_err(DaemonError::from);
            }
        };
        let snapshot = match resident.bounded_status_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return sinomo_dfs::inspect_incomplete_dfs_status(
                    &data_dir,
                    &cluster_paths,
                    format!("bounded resident metadata status unavailable: {error}"),
                )
                .map_err(DaemonError::from);
            }
        };
        sinomo_dfs::inspect_dfs_status_from_resident_snapshot(&data_dir, &cluster_paths, snapshot)
            .map_err(DaemonError::from)
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS bounded status".to_string(),
        detail: format!("DFS bounded status task failed: {source}"),
    })?
}

/// One explicitly requested local-fence effect, never a maintenance caller.
/// No native metadata/namespace mutation permit spans an authoritative read.
#[cfg(test)]
fn install_selected_local_recovery_rpc_owned(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
    owner: &Arc<DaemonDataOwnerLock>,
    expected_host: &Weak<DfsRuntimeHost>,
    command: crate::DfsMessageCommand,
    authenticated_source: NodeId,
    deadline: std::time::Instant,
) -> Result<crate::DfsMessageResponse, DfsDataPlaneHostError> {
    use crate::{
        DfsSelectedLocalRecoveryFenceRpcData as Data,
        DfsSelectedLocalRecoveryFenceRpcOutcome as Outcome,
        DfsSelectedLocalRecoveryFenceRpcResponse as Reply,
    };
    let crate::DfsNetworkMessage::SelectedLocalRecoveryFenceRequest(request) =
        &command.routed_message.message
    else {
        return Err(DfsDataPlaneHostError::new(
            "selected recovery ingress has no request",
        ));
    };
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data_dir))
        .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))?;
    let local = state.local_node_id;
    let response = |outcome| crate::DfsMessageResponse {
        request_id: command.request_id.clone(),
        outbound_messages: vec![crate::DfsRoutedMessage {
            source_node_id: local.clone(),
            target_node_id: command.routed_message.source_node_id.clone(),
            frame_id: command.routed_message.frame_id,
            message_id: command.routed_message.message_id,
            message: crate::DfsNetworkMessage::SelectedLocalRecoveryFenceResponse(Reply {
                reservation: request.reservation.clone(),
                acceptor_node_id: local.clone(),
                outcome,
            }),
        }],
    };
    let refused = |reason: String| response(Outcome::Refused { reason });
    if std::time::Instant::now() >= deadline {
        return Ok(refused(
            "selected recovery receiver deadline expired before publication".into(),
        ));
    }
    if command.request_id.is_empty()
        || command.routed_message.source_node_id != authenticated_source
        || authenticated_source != request.reservation.ballot.candidate_node_id
        || authenticated_source == local
        || command.routed_message.target_node_id != local
        || request.expected_target != local
    {
        return Ok(refused("selected recovery authenticated candidate/carrier/expected target differs from actual local owner".into()));
    }
    let check_host = || -> Result<(), DfsDataPlaneHostError> {
        let expected = expected_host.upgrade().ok_or_else(|| {
            DfsDataPlaneHostError::new("selected recovery original host disappeared")
        })?;
        let current = registered_dfs_runtime_host(data_dir)
            .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))?
            .ok_or_else(|| {
                DfsDataPlaneHostError::new("selected recovery registered host disappeared")
            })?;
        if !Arc::ptr_eq(&current, &expected) || current.shutting_down.is_cancelled() {
            return Err(DfsDataPlaneHostError::new(
                "selected recovery registered host changed or stopped",
            ));
        }
        let held = require_daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir))
            .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))?;
        if !Arc::ptr_eq(&held, owner) {
            return Err(DfsDataPlaneHostError::new(
                "selected recovery actual DATA owner changed",
            ));
        }
        Ok(())
    };
    if let Err(error) = check_host() {
        return Ok(refused(error.to_string()));
    }
    let layout = match owner.selected_original_data_layout() {
        Ok(Some(layout)) => layout,
        Ok(None) => {
            return Ok(refused(
                "default owner refuses selected local recovery fence RPC".into(),
            ));
        }
        Err(error) => return Ok(refused(error.to_string())),
    };
    if let Err(error) = layout.check_owner(owner) {
        return Ok(refused(error.to_string()));
    }
    let cluster_paths = ClusterPaths::new(data_dir);
    let Some(mut control) =
        daemon_dfs_control_plane_client_on_runtime(&cluster_paths, Some(runtime))
    else {
        return Ok(refused(
            "selected recovery receiver has no authoritative control owner".into(),
        ));
    };
    // Use the independently verified stored request, not a substituted wire
    // body. R42 validates the full deterministic request and fixed member set.
    let authoritative = match crate::DfsControlPlaneStore::new(control.as_mut())
        .verify_metadata_shard_recovery_ballot_before(&request.reservation, &local, deadline)
    {
        Ok(crate::control_plane::DfsRecoveryBallotVerify::Verified(actual)) => actual,
        Ok(crate::control_plane::DfsRecoveryBallotVerify::Refused(reason)) => {
            return Ok(refused(format!(
                "selected recovery reservation refused: {reason:?}"
            )));
        }
        Err(error) => return Ok(refused(error.to_string())),
    };
    drop(control);
    let mut service = match open_dfs_runtime_service(data_dir, Some(runtime)) {
        Ok(service) => service,
        Err(error) => return Ok(refused(error.to_string())),
    };
    if service.node_id() != &local || std::time::Instant::now() >= deadline {
        return Ok(refused("selected recovery actual service identity or original deadline changed before publication".into()));
    }
    let Some((_, service_owner)) = service.native_publication_owner_for_observation() else {
        return Ok(refused(
            "selected recovery service lost its actual DATA owner".into(),
        ));
    };
    if !Arc::ptr_eq(&service_owner, owner) {
        return Ok(refused(
            "selected recovery service belongs to another DATA owner".into(),
        ));
    }
    if let Err(error) = check_host().and_then(|_| {
        layout
            .check_owner(owner)
            .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))
    }) {
        return Ok(refused(error.to_string()));
    }
    // From this invocation onward every error is conservative uncertainty:
    // the atomic publisher may have made bytes visible before a later failure.
    let installed = match service.install_selected_local_recovery_fence(&authoritative, deadline) {
        Ok(installed) => installed,
        Err(error) => {
            return Ok(response(Outcome::Uncertain {
                reason: error.to_string(),
            }));
        }
    };
    let data = Data {
        fence: installed.data.fence,
        marker_digest: blake3::hash(&installed.data.marker_bytes)
            .to_hex()
            .to_string(),
        marker_bytes: installed.data.marker_bytes.len() as u64,
    };
    let post = check_host().and_then(|_| {
        layout
            .check_owner(owner)
            .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))
    });
    let reason = if !installed.reservation_current {
        Some(
            installed
                .post_verification_error
                .unwrap_or_else(|| "post-publication reservation unavailable".into()),
        )
    } else if std::time::Instant::now() >= deadline {
        Some("selected recovery receiver deadline expired after publication; stored DATA is not a Fenced response".into())
    } else {
        post.err().map(|error| error.to_string())
    };
    Ok(response(match reason {
        Some(reason) => Outcome::StoredButUnverified { data, reason },
        None => Outcome::Fenced { data },
    }))
}

impl DfsDataPlaneHost for DaemonDfsDataPlaneHost {
    #[cfg(test)]
    fn install_selected_local_recovery_fence_rpc<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        ingress: crate::service::AuthenticatedSelectedLocalRecoveryFenceIngress,
    ) -> DfsDataPlaneHostFuture<'a, crate::DfsMessageResponse> {
        let validation = self.validate_data_dir(&data_dir);
        let data_owner = self.require_data_owner();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            validation?;
            let owner = data_owner?;
            let task_owner = registered_dfs_runtime_host(&data_dir)
                .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))?
                .ok_or_else(|| {
                    DfsDataPlaneHostError::new(
                        "selected recovery receiver has no registered task owner",
                    )
                })?;
            if task_owner.shutting_down.is_cancelled() {
                return Err(DfsDataPlaneHostError::new(
                    "selected recovery receiver is shutting down",
                ));
            }
            let expected_host = Arc::downgrade(&task_owner);
            let (command, source, deadline) = ingress.into_parts();
            let process_runtime = runtime.clone();
            let (send, receive) = tokio::sync::oneshot::channel();
            let task = runtime.spawn_blocking(move || {
                // Actual DATA ownership remains inside the registered host's
                // task even if the calling RPC future is cancelled.
                let response = install_selected_local_recovery_rpc_owned(
                    &data_dir,
                    &process_runtime,
                    &owner,
                    &expected_host,
                    command,
                    source,
                    deadline,
                );
                let _ = send.send(response);
            });
            task_owner.own_task(task);
            receive.await.map_err(|error| {
                DfsDataPlaneHostError::new(format!(
                    "selected recovery owned receiver ended without a result (uncertain): {error}"
                ))
            })?
        })
    }

    fn status<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, sinomo_dfs::DfsOperatorStatus> {
        let data_owner = self.require_data_owner();
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let _data_owner = data_owner?;
            handle_daemon_dfs_status_async(&paths, &runtime)
                .await
                .map_err(daemon_dfs_data_plane_error)
        })
    }

    fn shared_content_store(
        &self,
        data_dir: &SinomoDataDir,
    ) -> Result<Option<Arc<sinomo_dfs::LocalContentStore>>, DfsDataPlaneHostError> {
        let _data_owner = self.require_data_owner()?;
        self.validate_data_dir(data_dir)?;
        let cluster_paths = ClusterPaths::new(data_dir);
        let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
            .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?
        else {
            return Ok(None);
        };
        let content_store = daemon_dfs_message_content_store(
            data_dir,
            &cluster_paths,
            &peer_snapshot,
            &self.runtime,
            #[cfg(test)]
            None,
        )
        .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?;
        Ok(Some(Arc::new(content_store)))
    }

    fn run_smoke<'a>(&'a self) -> DfsDataPlaneHostFuture<'a, sinomo_dfs::DfsSmokeReport> {
        let data_owner = self.require_data_owner();
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let _data_owner = data_owner?;
            run_daemon_dfs_smoke_async(&paths, &runtime)
                .await
                .map_err(daemon_dfs_data_plane_error)
        })
    }

    fn inspect<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        request: sinomo_dfs::DaemonDfsInspectionRequest,
    ) -> DfsDataPlaneHostFuture<'a, sinomo_dfs::DaemonDfsInspectionResponse> {
        let data_owner = self.require_data_owner();
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let _data_owner = data_owner?;
            self.validate_data_dir(data_dir.as_ref())?;
            handle_daemon_dfs_inspection_async(&paths, request, &runtime)
                .await
                .map_err(daemon_dfs_data_plane_error)
        })
    }

    fn retained_metadata_replica_shard_context<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
    ) -> Option<DfsDataPlaneHostFuture<'a, Option<sinomo_dfs::DfsMetadataReplicaShardContext>>>
    {
        let data_owner = self.require_data_owner();
        let validation = self.validate_data_dir(data_dir.as_ref());
        let runtime = self.runtime.clone();
        Some(Box::pin(async move {
            let _data_owner = data_owner?;
            validation?;
            let cluster_paths = ClusterPaths::new(data_dir.as_ref());
            let peer_deadline =
                tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
            let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot_before(
                &cluster_paths,
                &runtime,
                peer_deadline,
            )
            .await
            .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?
            else {
                return Ok(None);
            };
            let preflight = warm_daemon_dfs_replica_authority_for_peer_snapshot_before(
                &cluster_paths,
                &peer_snapshot,
                &runtime,
                peer_deadline,
            )
            .await
            .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?;
            let projection = preflight
                .authority
                .retained_complete_prefix_for_generation(preflight.generation)
                .ok_or_else(|| {
                    DfsDataPlaneHostError::new(
                        "daemon DFS metadata replica authority projection is not ready",
                    )
                })?;
            daemon_dfs_metadata_replica_context_from_authority_projection(
                &projection,
                &peer_snapshot.local_node_id,
                &file_system_id,
                &shard_id,
            )
            .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))
        }))
    }

    fn apply_metadata_journal_batch<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        authenticated_source_node_id: NodeId,
        command: sinomo_dfs::DfsMetadataJournalBatchCommand,
        context: Option<sinomo_dfs::DfsMetadataReplicaShardContext>,
    ) -> DfsDataPlaneHostFuture<'a, sinomo_dfs::DfsMetadataJournalBatchResponse> {
        // NEW selected-only receiver processing clock. Capture before the
        // async body; earlier service authority lookup is outside this cap.
        let selected_clock = command.prefix_probe.as_ref()
            .and_then(|probe| probe.repair_custody.as_ref())
            .filter(|page| page.cut.version == 2)
            .map(|_| {
                let captured = std::time::Instant::now();
                (captured, captured + Duration::from_secs(5))
            });
        let data_owner = self.require_data_owner();
        #[cfg(any(test, feature = "test-support"))]
        let selected_clock_observation = (|| -> Result<(), DfsDataPlaneHostError> {
            if let Some((captured, deadline)) = selected_clock {
                let owner = data_owner.as_ref().map_err(|error| DfsDataPlaneHostError::new(error.to_string()))?;
                if let Some(layout) = owner.selected_original_data_layout()
                    .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))?
                {
                    layout.observe_receiver_clock(&command.request_id, captured, deadline)
                        .map_err(|error| DfsDataPlaneHostError::new(error.to_string()))?;
                }
            }
            Ok(())
        })();
        let validation = self.validate_data_dir(data_dir.as_ref());
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let event_client = self.event_client.clone();
        Box::pin(async move {
            let _data_owner = data_owner?;
            validation?;
            #[cfg(any(test, feature = "test-support"))]
            selected_clock_observation?;
            #[cfg(any(test, feature = "test-support"))]
            let diagnostic = command.authority_probe.as_ref().filter(|probe| {
                matches!(&probe.operation, crate::DfsMetadataAuthorityOperation::PreparedOperation { operation_id, .. }
                    if crate::native_boundary_diagnostics::selected_operation(&data_dir.root(), operation_id))
            }).filter(|_| crate::native_boundary_diagnostics::enabled(&data_dir.root()))
                .map(|probe| (data_dir.root(), probe.clone(), std::time::Instant::now()));
            #[cfg(any(test, feature = "test-support"))]
            if let Some((root, probe, _)) = &diagnostic {
                crate::native_boundary_diagnostics::record(
                    root,
                    "replica-authority-ingress",
                    || {
                        serde_json::json!({
                            "authenticated_source": authenticated_source_node_id, "request_id": command.request_id,
                            "probe": probe, "context": format!("{context:?}"), "next_after_sequence": command.batch.next_after_sequence,
                            "journal_frame_count": command.batch.journal_frames.len(),
                        })
                    },
                    false,
                );
            }
            if command.source_node_id.as_ref() != Some(&authenticated_source_node_id) {
                return Ok(daemon_dfs_metadata_journal_rejection(
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
            let Some(context) = context else {
                return Ok(daemon_dfs_metadata_journal_rejection(
                    &command,
                    "authoritative metadata replica shard context is unavailable",
                ));
            };
            let exact_authority = command.file_system_id.as_ref()
                == Some(&context.shard.file_system_id)
                && command.batch.shard_id.0 == context.shard.shard_id
                && command.authority_epoch == Some(context.shard.epoch)
                && context.shard.primary_node_id == authenticated_source_node_id;
            if !exact_authority {
                return Ok(daemon_dfs_metadata_journal_rejection(
                    &command,
                    format!(
                        "metadata journal command does not match the authorized shard identity: received filesystem {:?} shard {} epoch {:?} primary {}; expected filesystem {} shard {} epoch {} primary {}",
                        command.file_system_id,
                        command.batch.shard_id.0,
                        command.authority_epoch,
                        authenticated_source_node_id,
                        context.shard.file_system_id,
                        context.shard.shard_id,
                        context.shard.epoch,
                        context.shard.primary_node_id,
                    ),
                ));
            }
            let event_origin_node_id = context.local_node_id.clone();
            let may_have_deferred_materialization =
                command.durable_replication.is_some() || command.authority_probe.is_some();
            // Asynchronous publication must not cancel the maintenance which
            // authored it. It queues through the same exact-shard admission
            // and validation. Synchronous durability/handoff catch-up and
            // foreground durability and authority commands retain preemption.
            let preempt_maintenance = !dfs_metadata_replication_can_wait_for_maintenance(
                &command,
                &authenticated_source_node_id,
                &context.local_node_id,
            );
            let rejection_request_id = command.request_id.clone();
            let rejection_shard_id = command.batch.shard_id.clone();
            let rejection_next_after_sequence = command.batch.next_after_sequence;
            let admitted_context = context.clone();
            #[cfg(any(test, feature = "test-support"))]
            let diagnostic_apply = diagnostic.clone();
            let operation = handle_daemon_dfs_resident_replica_operation(
                &paths,
                data_dir.clone(),
                admitted_context,
                &runtime,
                preempt_maintenance,
                may_have_deferred_materialization,
                (&rejection_request_id, "metadata-journal-batch"),
                move |shard, context| {
                    shard.with_backend(|backend| {
                        let logical = if command.verify_prefix_only {
                            Ok(command.batch.header())
                        } else {
                            command.batch.decoded()
                        };
                        let response = if let Some((captured, deadline)) = selected_clock {
                            crate::metadata_replication::process_dfs_metadata_journal_batch_with_selected_receiver_budget(
                                backend, Some(context), command, captured, deadline,
                            )
                        } else {
                            sinomo_dfs::process_dfs_metadata_journal_batch(backend, Some(context), command)
                        };
                        #[cfg(any(test, feature = "test-support"))]
                        if let Some((root, probe, started)) = &diagnostic_apply {
                            crate::native_boundary_diagnostics::record(root, "replica-authority-applied", || serde_json::json!({
                                "probe": probe, "context": format!("{context:?}"), "response": response,
                                "ingress_elapsed_us": started.elapsed().as_micros(),
                            }), false);
                        }
                        let namespace_paths = logical
                            .map_err(crate::DfsError::from)
                            .and_then(|batch| {
                                crate::metadata_replication::committed_payloads_for_batch(
                                    backend, &batch,
                                )
                            })
                            .map(|payloads| {
                                payloads
                                    .iter()
                                    .flat_map(|payload| {
                                        crate::metadata_replication::payload_visible_paths(payload)
                                    })
                                    .collect::<BTreeSet<_>>()
                            });
                        #[cfg(test)]
                        wait_at_daemon_dfs_message_committed_edge_test_hook(&response.request_id);
                        Ok((response, namespace_paths))
                    })
                },
            )
            .await;
            #[cfg(any(test, feature = "test-support"))]
            if let Some((root, probe, started)) = &diagnostic {
                crate::native_boundary_diagnostics::record(
                    root,
                    "replica-authority-operation-return",
                    || {
                        let result = match &operation {
                            Ok(DaemonDfsResidentReplicaOperation::Completed {
                                result: (response, _),
                            }) => serde_json::json!({"completed": response}),
                            Ok(DaemonDfsResidentReplicaOperation::Rejected { reason }) => {
                                serde_json::json!({"rejected": format!("{reason:?}")})
                            }
                            Err(error) => serde_json::json!({"error": format!("{error:?}")}),
                        };
                        serde_json::json!({"probe": probe, "request_id": rejection_request_id,
                        "result": result, "ingress_elapsed_us": started.elapsed().as_micros()})
                    },
                    false,
                );
            }
            let operation = operation.map_err(daemon_dfs_data_plane_error)?;
            let (response, namespace_paths) = match operation {
                DaemonDfsResidentReplicaOperation::Completed { result } => result,
                DaemonDfsResidentReplicaOperation::Rejected { reason } => {
                    return Ok(daemon_dfs_metadata_journal_rejection_for(
                        rejection_request_id,
                        rejection_shard_id,
                        rejection_next_after_sequence,
                        reason,
                    ));
                }
            };
            if response.applied_entries > 0
                && response.result == sinomo_dfs::DfsMetadataJournalBatchResult::Applied
                && let Some(event_client) = event_client
            {
                match namespace_paths {
                    Ok(paths) if !paths.is_empty() => {
                        publish_dfs_namespace_change(
                            &event_client,
                            event_origin_node_id,
                            "metadata-journal-applied",
                            paths.into_iter().collect(),
                        )
                        .await
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(target: "sinomo.dfs", %error, "committed namespace body unavailable; invalidating the observed scope");
                        publish_dfs_namespace_change(
                            &event_client,
                            event_origin_node_id,
                            "metadata-journal-rescan-required",
                            vec!["/".into()],
                        )
                        .await;
                    }
                }
            }
            if selected_clock.is_some_and(|(_, deadline)| std::time::Instant::now() >= deadline)
                && response.result == sinomo_dfs::DfsMetadataJournalBatchResult::Applied
            {
                return Ok(daemon_dfs_metadata_journal_rejection_for(
                    response.request_id, response.shard_id, rejection_next_after_sequence,
                    "selected receiver processing budget expired after processing; DATA retained without ACK",
                ));
            }
            Ok(response)
        })
    }

    fn install_metadata_snapshot<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        authenticated_source_node_id: NodeId,
        mut command: sinomo_dfs::DfsMetadataSnapshotInstallCommand,
        context: Option<sinomo_dfs::DfsMetadataReplicaShardContext>,
        body: sinomo_core::network::ObjectRpcIncomingBody,
    ) -> DfsDataPlaneHostFuture<'a, sinomo_dfs::DfsMetadataSnapshotInstallResponse> {
        let data_owner = self.require_data_owner();
        let validation = self.validate_data_dir(data_dir.as_ref());
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        Box::pin(async move {
            let _data_owner = data_owner?;
            validation?;
            if command.source_node_id != authenticated_source_node_id {
                return Ok(daemon_dfs_metadata_snapshot_rejection(
                    &command,
                    format!(
                        "authenticated metadata snapshot caller {authenticated_source_node_id} does not match claimed source {}",
                        command.source_node_id
                    ),
                ));
            }
            let Some(context) = context else {
                return Ok(daemon_dfs_metadata_snapshot_rejection(
                    &command,
                    "authoritative metadata replica shard context is unavailable",
                ));
            };
            let exact_authority = command.file_system_id.as_ref()
                == Some(&context.shard.file_system_id)
                && command.shard_id.0 == context.shard.shard_id
                && command.authority_epoch == Some(context.shard.epoch)
                && context.shard.primary_node_id == authenticated_source_node_id;
            if !exact_authority {
                return Ok(daemon_dfs_metadata_snapshot_rejection(
                    &command,
                    "metadata snapshot command does not match the authorized shard identity",
                ));
            }
            let descriptor = crate::DfsRpcAttachmentDescriptor {
                total_size_bytes: command.snapshot.total_size_bytes,
                content_hash: None,
            };
            command.image = Some(
                crate::runtime_message::ingest_native_snapshot_attachment(
                    data_dir.root(),
                    command.snapshot.clone(),
                    crate::DfsInboundAttachment { descriptor, body },
                    Arc::clone(&_data_owner),
                    None,
                )
                .await
                .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?,
            );
            let rejection_request_id = command.request_id.clone();
            let rejection_shard_id = command.shard_id.clone();
            let rejection_next_after_sequence = command.next_after_sequence;
            let admitted_context = context.clone();
            let operation = handle_daemon_dfs_resident_replica_operation(
                &paths,
                data_dir,
                admitted_context,
                &runtime,
                true,
                false,
                (&rejection_request_id, "metadata-snapshot-install"),
                move |shard, context| shard.install_replica_snapshot(context, command),
            )
            .await
            .map_err(daemon_dfs_data_plane_error)?;
            match operation {
                DaemonDfsResidentReplicaOperation::Completed { result, .. } => Ok(result),
                DaemonDfsResidentReplicaOperation::Rejected { reason } => {
                    Ok(daemon_dfs_metadata_snapshot_rejection_for(
                        rejection_request_id,
                        rejection_shard_id,
                        rejection_next_after_sequence,
                        reason,
                    ))
                }
            }
        })
    }

    fn handle_message<'a>(
        &'a self,
        command: DfsMessageCommand,
        attachment: Option<DfsInboundAttachment>,
        traceparent: Option<String>,
        encoded_value_bytes: Option<u64>,
    ) -> DfsDataPlaneHostFuture<'a, DfsDataPlaneResponse> {
        let data_owner = self.require_data_owner();
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let event_client = self.event_client.clone();
        let namespace_event = daemon_dfs_visible_mutation_event(&command);
        Box::pin(async move {
            let _data_owner = data_owner?;
            if attachment.is_none()
                && let DfsNetworkMessage::MetadataJournalBatch(batch) =
                    &command.routed_message.message
                && batch.authority_probe.is_some()
                && !batch.verify_prefix_only
                && batch.prefix_probe.is_none()
                && let Some(file_system_id) = batch.file_system_id.clone()
            {
                // The data-plane service has authenticated the routed source.
                // A fallback authority probe is the same replica-ingress
                // operation as its direct RPC, not a general filesystem
                // request. Share its physical owner, admission, validation
                // and materialization path. Promoted-primary prefix probes
                // keep their separate handoff-marker protocol below.
                let data_dir = Arc::new(SinomoDataDir::new(
                    daemon_data_dir(&paths).map_err(daemon_dfs_data_plane_error)?,
                ));
                let context = self
                    .retained_metadata_replica_shard_context(
                        Arc::clone(&data_dir),
                        file_system_id,
                        batch.batch.shard_id.clone(),
                    )
                    .expect("daemon host implements retained replica context")
                    .await?
                    .ok_or_else(|| {
                        DfsDataPlaneHostError::new(
                            "authoritative metadata replica shard context is unavailable",
                        )
                    })?;
                if command.routed_message.target_node_id != context.local_node_id {
                    return Err(DfsDataPlaneHostError::new(
                        "routed metadata authority probe targets another node",
                    ));
                }
                let local_node_id = context.local_node_id.clone();
                let DfsMessageCommand {
                    request_id,
                    routed_message,
                } = command;
                let DfsRoutedMessage {
                    source_node_id,
                    frame_id,
                    message_id,
                    message,
                    ..
                } = routed_message;
                let DfsNetworkMessage::MetadataJournalBatch(batch) = message else {
                    unreachable!("selected metadata journal command");
                };
                let response = self
                    .apply_metadata_journal_batch(
                        data_dir,
                        source_node_id.clone(),
                        batch,
                        Some(context),
                    )
                    .await?;
                return Ok(DfsDataPlaneResponse::value(DfsMessageResponse {
                    request_id,
                    outbound_messages: vec![DfsRoutedMessage {
                        source_node_id: local_node_id,
                        target_node_id: source_node_id,
                        frame_id,
                        message_id,
                        message: DfsNetworkMessage::MetadataJournalBatchResponse(response),
                    }],
                }));
            }
            let response = handle_daemon_dfs_message_with_encoded_size_async(
                &paths,
                command,
                attachment,
                traceparent,
                encoded_value_bytes,
                &runtime,
            )
            .await
            .map_err(daemon_dfs_data_plane_error)?;
            if daemon_dfs_response_committed_visible_mutation(&response)
                && let (Some(event_client), Some(namespace_event)) = (event_client, namespace_event)
            {
                publish_dfs_namespace_change(
                    &event_client,
                    namespace_event.origin_node_id,
                    namespace_event.operation,
                    namespace_event.paths,
                )
                .await;
            }
            Ok(response)
        })
    }
}

pub(super) fn daemon_dfs_data_plane_error(source: DaemonError) -> DfsDataPlaneHostError {
    DfsDataPlaneHostError::new(source.to_string())
}

pub(crate) fn with_daemon_dfs_control_plane_on_runtime<T: sinomo_dfs::DfsMessageTransport>(
    service: NetworkedDfsService<T>,
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> NetworkedDfsService<T> {
    with_daemon_dfs_control_plane_seeded_on_runtime(service, cluster_paths, runtime, None)
}

pub(super) fn with_daemon_dfs_control_plane_seeded_on_runtime<
    T: sinomo_dfs::DfsMessageTransport,
>(
    service: NetworkedDfsService<T>,
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
    authoritative_prefix: Option<sinomo_dfs::DfsAuthoritativePrefixProjection>,
) -> NetworkedDfsService<T> {
    let Some(runtime) = runtime else {
        return service;
    };
    if cluster_paths.state.exists() {
        let mut control_plane = DaemonDfsControlPlaneKvClient::new_seeded_on_runtime(
            cluster_paths,
            runtime,
            authoritative_prefix,
        );
        let service =
            with_daemon_dfs_placement_environment(service, cluster_paths, &mut control_plane);
        service.with_boxed_control_plane(
            Box::new(control_plane) as Box<dyn sinomo_dfs::DfsControlPlaneKvClient>
        )
    } else {
        service
    }
}

fn with_daemon_dfs_placement_environment<T: sinomo_dfs::DfsMessageTransport>(
    mut service: NetworkedDfsService<T>,
    cluster_paths: &ClusterPaths,
    _control_plane: &mut dyn sinomo_dfs::DfsControlPlaneKvClient,
) -> NetworkedDfsService<T> {
    let Some(data_root) = cluster_paths.root.parent() else {
        return service;
    };
    let data_dir = SinomoDataDir::new(data_root);
    let cache = cached_daemon_dfs_placement_snapshot_cache(&data_dir, service.node_id());
    service = service.with_placement_snapshot_cache(cache);
    let pins = sinomo_dfs::list_local_dfs_pins(sinomo_dfs::default_dfs_pin_root(&data_dir), None)
        .unwrap_or_default()
        .into_iter()
        .filter(|pin| pin.mode == sinomo_dfs::DfsPinMode::Content)
        .filter_map(|pin| {
            pin.object_id
                .map(|object_id| (object_id, pin.pinned_by_node_id))
        });
    service.set_content_pin_snapshot(pins);
    service
}

pub(crate) fn daemon_dfs_control_plane_client_on_runtime(
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> Option<Box<dyn sinomo_dfs::DfsControlPlaneKvClient>> {
    let runtime = runtime?;
    // Cluster membership requires an authority client even when the derived
    // materialized checkpoint is missing. Browser and other daemon-owned
    // clients seed from the process-wide retained DFS generation; only that
    // shared observer performs a bounded ReadIndex when cold or gapped.
    cluster_paths.state.exists().then(|| {
        Box::new(DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(
            cluster_paths,
            runtime,
        )) as Box<dyn sinomo_dfs::DfsControlPlaneKvClient>
    })
}

pub(crate) fn daemon_dfs_node_transport(
    local_node_id: sinomo_core::node::NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
) -> DfsNodeTransport {
    let peer_transport = Arc::new(DaemonDfsUnavailablePeerTransport);
    let sync_peer_transport: Arc<dyn DfsPeerTransport> = peer_transport.clone();
    let async_peer_transport: Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> = peer_transport;
    DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        sync_peer_transport,
        async_peer_transport,
    )
}

pub(crate) fn daemon_dfs_node_transport_on_runtime(
    local_node_id: sinomo_core::node::NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> DfsNodeTransport {
    let (peer_transport, async_peer_transport) =
        daemon_dfs_peer_transports_on_runtime(data_dir, runtime);
    DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        peer_transport,
        async_peer_transport,
    )
}

pub(crate) fn daemon_dfs_client_node_transport(
    local_node_id: NodeId,
    peer_node_ids: impl IntoIterator<Item = NodeId>,
    host: Arc<DfsRuntimeHost>,
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> DfsNodeTransport {
    // This is the already registered component host supplied by the normal
    // client opener. Core effect/genesis constructors do not bind these senders.
    let peer = Arc::new(DaemonDfsPeerTransport { host, native_prefix: Some(super::native_prefix_return::NativePrefixOwnerContext {data_dir:data_dir.clone(),runtime:runtime.clone()}) });
    let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
    let async_peer: Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> = peer;
    DfsNodeTransport::with_peer_transports_for_nodes(
        local_node_id,
        peer_node_ids,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        sync_peer,
        async_peer,
    )
    .with_registered_main_namespace_dispatch()
}

#[derive(Clone)]
pub(super) struct DaemonDfsPeerTransport {
    pub(super) host: Arc<DfsRuntimeHost>,
    pub(super) native_prefix: Option<super::native_prefix_return::NativePrefixOwnerContext>,
}

/// One explicit selected original. Selection changes representation/custody
/// routing only; the producer must independently hold real native admission.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug)]
pub(super) struct NativeRepairCustodyScope {
    material: crate::DfsOriginalRepairMaterial,
    selection_live: std::sync::atomic::AtomicBool,
    held: std::sync::atomic::AtomicBool,
    attempts: std::sync::atomic::AtomicUsize,
    captured: Mutex<Option<NativeRepairCustodyObservation>>,
    completion: Mutex<Option<Weak<OwnedOriginalRepairCompletion>>>,
    last_error: Mutex<Option<String>>,
    retained: Mutex<Option<crate::repair_queue_store::NativeOwnedRepairCompletionRef>>,
    #[cfg(test)]
    fail_namespace_once: std::sync::atomic::AtomicBool,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Debug)]
pub(crate) struct NativeRepairCustodyObservation {
    pub(crate) carrier: DfsRoutedMessage,
    pub(crate) returned_frame: DfsRoutedMessage,
    pub(crate) attempt_sequence: u64,
    pub(crate) rpc_request_id: String,
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) struct NativeRepairCustodySelection {
    owner: Weak<DfsDataOwnerLock>,
    scope: Arc<NativeRepairCustodyScope>,
}

#[cfg(any(test, feature = "test-support"))]
impl NativeRepairCustodySelection {
    pub(crate) fn allow_intake(&self) {
        self.scope.held.store(false, Ordering::Release);
    }
    pub(crate) fn captured(&self) -> Option<NativeRepairCustodyObservation> {
        self.scope.captured.lock().ok()?.clone()
    }
    pub(crate) fn retained_reference(
        &self,
    ) -> Option<crate::repair_queue_store::NativeOwnedRepairCompletionRef> {
        self.scope.retained.lock().ok()?.clone()
    }
    pub(crate) fn intake_attempts(&self) -> usize {
        self.scope.attempts.load(Ordering::Acquire)
    }
    pub(crate) fn captured_completion(&self) -> Option<Arc<OwnedOriginalRepairCompletion>> {
        self.scope.completion.lock().ok()?.as_ref()?.upgrade()
    }
    pub(crate) fn last_intake_error(&self) -> Option<String> {
        self.scope.last_error.lock().ok()?.clone()
    }
    #[cfg(test)]
    pub(crate) fn fail_next_namespace_once(&self) {
        self.scope
            .fail_namespace_once
            .store(true, Ordering::Release);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for NativeRepairCustodySelection {
    fn drop(&mut self) {
        self.scope.selection_live.store(false, Ordering::Release);
        let durably_deferred = self.scope.retained.lock().is_ok_and(|value| value.is_some());
        if let Some(owner) = self.owner.upgrade()
            && let Ok(mut selected) = owner.native_repair_custody.lock()
            && selected
                .as_ref()
                .is_some_and(|scope| Arc::ptr_eq(scope, &self.scope))
            && (Arc::strong_count(&self.scope) == 2 || durably_deferred)
        {
            selected.take();
        }
        // Already captured returns keep their typed custody route. Removing a
        // fixture scope never turns them into ordinary application responses.
        self.scope.held.store(false, Ordering::Release);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl DfsDataOwnerLock {
    pub(crate) fn native_repair_custody_return_pending(
        &self,
        carrier: &DfsRoutedMessage,
    ) -> Result<bool, crate::DfsError> {
        let selected =
            self.native_repair_custody
                .lock()
                .map_err(|_| crate::DfsError::Metadata {
                    message: "native Repair custody selection poisoned".into(),
                })?;
        let Some(scope) = selected
            .as_ref()
            .filter(|scope| scope.material.matches_carrier(carrier))
        else {
            return Ok(false);
        };
        Ok(scope
            .captured
            .lock()
            .map_err(|_| crate::DfsError::Metadata {
                message: "native custody observation poisoned".into(),
            })?
            .is_some())
    }
    pub(crate) fn reject_raw_selected_repair_response(
        &self,
        returned: &DfsRoutedMessage,
    ) -> Result<(), crate::DfsError> {
        let selected =
            self.native_repair_custody
                .lock()
                .map_err(|_| crate::DfsError::Metadata {
                    message: "native Repair custody selection poisoned".into(),
                })?;
        if selected.as_ref().is_some_and(|scope| {
            crate::node_transport::all_background_replica_response_identity_matches(
                &scope.material.original.source_node_id,
                &scope.material.original.routed(),
                returned,
            )
        }) {
            return Err(crate::DfsError::Metadata {
                message: "selected original Repair response lacks native owned-return provenance"
                    .into(),
            });
        }
        Ok(())
    }
    pub(crate) fn native_repair_custody_material_for_carrier(
        &self,
        carrier: &DfsRoutedMessage,
    ) -> Result<Option<crate::DfsOriginalRepairMaterial>, crate::DfsError> {
        let selected =
            self.native_repair_custody
                .lock()
                .map_err(|_| crate::DfsError::Metadata {
                    message: "native Repair custody selection poisoned".into(),
                })?;
        let Some(scope) = selected
            .as_ref()
            .filter(|scope| scope.material.matches_carrier(carrier))
        else {
            return Ok(None);
        };
        if scope
            .captured
            .lock()
            .map_err(|_| crate::DfsError::Metadata {
                message: "native custody observation poisoned".into(),
            })?
            .is_some()
        {
            return Err(crate::DfsError::Metadata {
                message: "native Repair scoped return awaits custody settlement".into(),
            });
        }
        Ok(Some(scope.material.clone()))
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn select_native_original_repair_custody(
    data_dir: &SinomoDataDir,
    material: &crate::DfsOriginalRepairMaterial,
) -> Result<NativeRepairCustodySelection, crate::DfsError> {
    material.validate()?;
    let error = |message: String| crate::DfsError::Metadata { message };
    let owner = require_daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir))
        .map_err(|e| error(e.to_string()))?;
    let layout = owner
        .selected_original_data_layout()
        .map_err(|e| error(e.to_string()))?
        .ok_or_else(|| {
            error("native Repair custody selection requires actual selected owner".into())
        })?;
    let queue = layout.queue()?;
    let birth = queue
        .original_repair(&material.original.routed())?
        .ok_or_else(|| error("native Repair custody selection lacks genuine original".into()))?;
    if birth.original_wire_material()? != *material
        || birth.closed_at_sequence.is_some()
        || !birth.observed.is_empty()
    {
        return Err(error(
            "native Repair custody selection changed open original".into(),
        ));
    }
    owner
        .repair_payload_inspection_context()
        .map_err(|e| error(e.to_string()))?
        .inspect(&birth.root)?;
    let mut selected = owner
        .native_repair_custody
        .lock()
        .map_err(|_| error("native Repair custody selection poisoned".into()))?;
    if selected.is_some() {
        return Err(error("native Repair custody selection is occupied".into()));
    }
    let scope = Arc::new(NativeRepairCustodyScope {
        material: material.clone(),
        selection_live: std::sync::atomic::AtomicBool::new(true),
        held: std::sync::atomic::AtomicBool::new(true),
        attempts: std::sync::atomic::AtomicUsize::new(0),
        captured: Mutex::new(None),
        completion: Mutex::new(None),
        last_error: Mutex::new(None),
        retained: Mutex::new(None),
        #[cfg(test)]
        fail_namespace_once: std::sync::atomic::AtomicBool::new(false),
    });
    *selected = Some(Arc::clone(&scope));
    Ok(NativeRepairCustodySelection {
        owner: Arc::downgrade(&owner),
        scope,
    })
}

/// Created only by the admitted normal producer. The actual body remains in
/// that producer's DfsRoutedSend; this carries no live mutation admission.
#[cfg(any(test, feature = "test-support"))]
pub(crate) struct PreparedNativeRepairCustody {
    attempt: crate::repair_queue_store::RepairOriginalAttemptWitness,
    data_dir: SinomoDataDir,
    owner: Arc<DfsDataOwnerLock>,
    host: Weak<DfsRuntimeHost>,
    scope: Arc<NativeRepairCustodyScope>,
    admission: crate::publication_guard::NativeRepairProducerAdmission,
}
#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for PreparedNativeRepairCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedNativeRepairCustody")
            .field("carrier", self.attempt.carrier())
            .finish_non_exhaustive()
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn prepare_native_repair_custody(
    data_dir: &SinomoDataDir,
    owner: Arc<DfsDataOwnerLock>,
    admission: crate::publication_guard::NativeRepairProducerAdmission,
    carrier: &DfsRoutedMessage,
    material: &crate::DfsOriginalRepairMaterial,
) -> Result<PreparedNativeRepairCustody, crate::DfsError> {
    let error = |message: String| crate::DfsError::Metadata { message };
    let current = require_daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir))
        .map_err(|e| error(e.to_string()))?;
    if !Arc::ptr_eq(&owner, &current) {
        return Err(error(
            "native Repair custody preparation changed owner".into(),
        ));
    }
    let host = registered_dfs_runtime_host(data_dir)
        .map_err(|e| error(e.to_string()))?
        .ok_or_else(|| error("native Repair custody preparation has no registered host".into()))?;
    let scope = owner
        .native_repair_custody
        .lock()
        .map_err(|_| error("native Repair custody selection poisoned".into()))?
        .as_ref()
        .filter(|scope| scope.material == *material)
        .cloned()
        .ok_or_else(|| {
            error("native Repair custody selection changed during preparation".into())
        })?;
    let layout = owner
        .selected_original_data_layout()
        .map_err(|e| error(e.to_string()))?
        .ok_or_else(|| error("native Repair custody preparation lost selected owner".into()))?;
    let attempt = layout
        .queue()?
        .capture_original_repair_attempt_witness(carrier, material)?;
    owner
        .repair_payload_inspection_context()
        .map_err(|e| error(e.to_string()))?
        .inspect(attempt.payload_root())?;
    admission.revalidate_before_dispatch()?;
    Ok(PreparedNativeRepairCustody {
        attempt,
        data_dir: data_dir.clone(),
        owner,
        host: Arc::downgrade(&host),
        scope,
        admission,
    })
}

/// Ephemeral DATA from one actual target-bound native RPC. Private fields and
/// no serialization or raw-frame constructor keep this distinct from a saved
/// unverified completion candidate. It grants no application or custody effect.
#[cfg(any(test, feature = "test-support"))]
pub(crate) struct OwnedOriginalRepairCompletion {
    attempt: crate::repair_queue_store::RepairOriginalAttemptWitness,
    returned: DfsRoutedMessage,
    rpc_request_id: String,
    owner: Arc<DfsDataOwnerLock>,
    scope: Option<Arc<NativeRepairCustodyScope>>,
    producer_admission: Option<crate::publication_guard::HistoricalNativeRepairProducerAdmission>,
}

#[cfg(any(test, feature = "test-support"))]
impl std::fmt::Debug for OwnedOriginalRepairCompletion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedOriginalRepairCompletion")
            .field("carrier", self.attempt.carrier())
            .field("attempt_sequence", &self.attempt.attempt_sequence())
            .field("returned", &self.returned)
            .field("rpc_request_id", &self.rpc_request_id)
            .finish()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl OwnedOriginalRepairCompletion {
    pub(crate) fn carrier(&self) -> &DfsRoutedMessage {
        self.attempt.carrier()
    }
    pub(crate) fn material(&self) -> &crate::DfsOriginalRepairMaterial {
        self.attempt.material()
    }
    pub(crate) fn returned_frame(&self) -> &DfsRoutedMessage {
        &self.returned
    }
    pub(crate) fn attempt_sequence(&self) -> u64 {
        self.attempt.attempt_sequence()
    }
    pub(crate) fn queue_incarnation(&self) -> &str {
        self.attempt.queue_incarnation()
    }
    pub(crate) fn rpc_request_id(&self) -> &str {
        &self.rpc_request_id
    }
    pub(crate) fn payload_root(&self) -> &crate::content_store::RepairPayloadRoot {
        self.attempt.payload_root()
    }
    pub(crate) fn revalidate_custody_owner(
        &self,
        queue: &crate::repair_queue_store::LocalDfsRepairQueueStore,
    ) -> Result<(), crate::DfsError> {
        if self.scope.is_none() || self.producer_admission.is_none() {
            return Err(crate::DfsError::Metadata {
                message: "native completion lacks admitted producer custody provenance".into(),
            });
        }
        // Past admission and actual RPC provenance remain DATA after retirement.
        // Do not read current serving authority or take a source dispatch gate.
        self.owner
            .selected_original_data_layout()
            .map_err(|e| crate::DfsError::Metadata {
                message: e.to_string(),
            })?
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "native completion lost physical selected owner".into(),
            })?;
        queue.check_original_repair_attempt_physical_owner(&self.attempt)?;
        self.owner
            .repair_payload_inspection_context()
            .map_err(|e| crate::DfsError::Metadata {
                message: e.to_string(),
            })?
            .inspect(self.attempt.payload_root())?;
        Ok(())
    }
    pub(super) fn custody_queue(
        &self,
    ) -> Result<crate::repair_queue_store::LocalDfsRepairQueueStore, crate::DfsError> {
        Ok(self.owner
            .selected_original_data_layout()
            .map_err(|e| crate::DfsError::Metadata {
                message: e.to_string(),
            })?
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "native completion lost selected queue".into(),
            })?
            .queue()?)
    }
    pub(super) fn before_custody_intake(self: &Arc<Self>) -> Result<(), crate::DfsError> {
        let scope = self
            .scope
            .as_ref()
            .ok_or_else(|| crate::DfsError::Metadata {
                message: "native completion has no selected custody route".into(),
            })?;
        let _ = scope
            .attempts
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_add(1))
            });
        let mut captured = scope
            .captured
            .lock()
            .map_err(|_| crate::DfsError::Metadata {
                message: "native custody observation poisoned".into(),
            })?;
        if captured.is_none() {
            *captured = Some(NativeRepairCustodyObservation {
                carrier: self.carrier().clone(),
                returned_frame: self.returned.clone(),
                attempt_sequence: self.attempt_sequence(),
                rpc_request_id: self.rpc_request_id.clone(),
            });
        }
        drop(captured);
        if let Ok(mut completion) = scope.completion.lock() {
            *completion = Some(Arc::downgrade(self));
        }
        if scope.held.load(Ordering::Acquire) {
            return Err(crate::DfsError::Metadata {
                message: "native original Repair custody intake held".into(),
            });
        }
        Ok(())
    }
    pub(super) fn record_custody_deferred(
        &self,
        deferred: &crate::repair_queue_store::DeferredNativeRepairCompletion,
    ) {
        if let Some(scope) = &self.scope
            && let Ok(mut retained) = scope.retained.lock()
        {
            *retained = Some(deferred.reference().clone());
        }
        if let Some(scope) = &self.scope
            && !scope.selection_live.load(Ordering::Acquire)
            && let Ok(mut selected) = self.owner.native_repair_custody.lock()
            && selected
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(scope, current))
        {
            selected.take();
        }
    }
    pub(super) fn record_custody_error(&self, error: &crate::DfsError) {
        if let Some(scope) = &self.scope
            && let Ok(mut last) = scope.last_error.lock()
        {
            *last = Some(error.to_string().chars().take(8192).collect());
        }
    }
    #[cfg(test)]
    pub(super) fn take_namespace_failure(&self) -> bool {
        self.scope
            .as_ref()
            .is_some_and(|scope| scope.fail_namespace_once.swap(false, Ordering::AcqRel))
    }
}

/// Send exactly one existing original/attempt through its registered native
/// owner, retaining the returned DATA without delivering or applying it locally.
/// The body is opened from the witness's actual retained root, never supplied
/// by a caller or reconstructed from current metadata. No ordinary send path
/// selects this dormant test-support primitive and no retry is performed here.
/// Historical attempt DATA is not source authority: this uses the existing
/// final source dispatch gate, and production activation still requires the
/// ordinary producer's native execution/admission ownership to be carried in.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn call_native_original_repair_owned(
    data_dir: &SinomoDataDir,
    attempt: crate::repair_queue_store::RepairOriginalAttemptWitness,
    options: DfsObjectRpcSendOptions,
) -> Result<OwnedOriginalRepairCompletion, crate::DfsError> {
    let invalid = |detail: &str| crate::DfsError::Metadata {
        message: format!("native original Repair completion {detail}"),
    };
    // The caller's existing whole-call budget includes owner/body preparation;
    // do not restart it after local reads or accept a return after its expiry.
    let deadline = options
        .overall_timeout
        .map(|budget| {
            Instant::now()
                .checked_add(budget)
                .ok_or_else(|| invalid("caller deadline is not representable"))
        })
        .transpose()?;
    let paths = DaemonPaths::new(data_dir);
    let owner =
        require_daemon_dfs_data_owner_lock(&paths).map_err(|error| invalid(&error.to_string()))?;
    let host = registered_dfs_runtime_host(data_dir)
        .map_err(|error| invalid(&error.to_string()))?
        .ok_or_else(|| invalid("has no registered native host"))?;
    if host.shutting_down.is_cancelled() {
        return Err(invalid("native host is shutting down"));
    }
    let identity =
        sinomo_core::cluster::read_cluster_state_without_kv(&ClusterPaths::new(data_dir))
            .map_err(crate::DfsError::from_network)?;
    let carrier = attempt.carrier();
    if carrier.source_node_id != identity.local_node_id
        || carrier.target_node_id == identity.local_node_id
        || !attempt.material().matches_carrier(carrier)
    {
        return Err(invalid(
            "original carrier does not belong to this native producer",
        ));
    }
    let layout = owner
        .selected_original_data_layout()
        .map_err(|error| invalid(&error.to_string()))?
        .ok_or_else(|| invalid("requires its actual selected queue owner"))?;
    let queue = layout.queue()?;
    queue.revalidate_original_repair_attempt_witness(&attempt)?;
    let inspection = owner
        .repair_payload_inspection_context()
        .map_err(|error| invalid(&error.to_string()))?;
    inspection.inspect(attempt.payload_root())?;
    let content = owner
        .open_native_content()
        .map_err(|error| invalid(&error.to_string()))?;
    let reader = content.open_retained_repair_payload(attempt.payload_root())?;
    let material = attempt.material();
    material.validate()?;
    let content_hash = material
        .attachment
        .content_hash
        .as_deref()
        .ok_or_else(|| invalid("original attachment has no exact hash"))?;
    let mut call = crate::DfsRoutedCall::with_attachment(
        carrier.clone(),
        crate::DfsOutboundAttachment::from_reader(
            reader,
            material.attachment.total_size_bytes,
            content_hash,
        ),
    );
    if owner
        .selects_original_repair_target(&carrier.target_node_id)
        .map_err(|error| invalid(&error.to_string()))?
    {
        call = call.with_original_repair(material.clone())?;
    }
    call_native_original_repair_owned_with_call(
        data_dir, owner, host, attempt, call, options, deadline, None, None,
    )
}

/// Only the normal admitted producer can create the private preparation. Its
/// already-owned body is consumed here without substitution or reopening.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn send_prepared_native_repair_custody(
    prepared: PreparedNativeRepairCustody,
    call: crate::DfsRoutedCall,
    options: DfsObjectRpcSendOptions,
) -> Result<OwnedOriginalRepairCompletion, crate::DfsError> {
    let invalid = || crate::DfsError::Metadata {
        message: "native custody send changed its admitted original/body".into(),
    };
    if call.message != *prepared.attempt.carrier()
        || call.attachment.as_ref().map(|body| &body.descriptor)
            != Some(&prepared.attempt.material().attachment)
        || call
            .original_repair
            .as_ref()
            .is_some_and(|material| material != prepared.attempt.material())
    {
        return Err(invalid());
    }
    let deadline = options
        .overall_timeout
        .map(|budget| Instant::now().checked_add(budget).ok_or_else(invalid))
        .transpose()?;
    prepared.admission.revalidate_before_dispatch()?;
    let host = prepared
        .host
        .upgrade()
        .ok_or_else(|| crate::DfsError::Metadata {
            message: "native custody prepared host is no longer registered".into(),
        })?;
    call_native_original_repair_owned_with_call(
        &prepared.data_dir,
        prepared.owner,
        host,
        prepared.attempt,
        call,
        options,
        deadline,
        Some(prepared.scope),
        Some(prepared.admission),
    )
}

#[cfg(any(test, feature = "test-support"))]
fn call_native_original_repair_owned_with_call(
    data_dir: &SinomoDataDir,
    owner: Arc<DfsDataOwnerLock>,
    host: Arc<DfsRuntimeHost>,
    attempt: crate::repair_queue_store::RepairOriginalAttemptWitness,
    call: crate::DfsRoutedCall,
    mut options: DfsObjectRpcSendOptions,
    deadline: Option<Instant>,
    scope: Option<Arc<NativeRepairCustodyScope>>,
    producer_admission: Option<crate::publication_guard::NativeRepairProducerAdmission>,
) -> Result<OwnedOriginalRepairCompletion, crate::DfsError> {
    let invalid = |detail: &str| crate::DfsError::Metadata {
        message: format!("native original Repair completion {detail}"),
    };
    let possibly_applied = |detail: String| crate::DfsError::NetworkDelivery {
        message: format!("native original Repair completion {detail}"),
        delivery_state: sinomo_core::network::NetworkDeliveryState::PossiblyApplied,
    };
    let paths = DaemonPaths::new(data_dir);
    let current_host = registered_dfs_runtime_host(data_dir)
        .map_err(|e| invalid(&e.to_string()))?
        .ok_or_else(|| invalid("has no registered native host"))?;
    let current_owner =
        require_daemon_dfs_data_owner_lock(&paths).map_err(|e| invalid(&e.to_string()))?;
    if !Arc::ptr_eq(&host, &current_host)
        || !Arc::ptr_eq(&owner, &current_owner)
        || host.shutting_down.is_cancelled()
    {
        return Err(invalid("prepared native owner changed before dispatch"));
    }
    let layout = owner
        .selected_original_data_layout()
        .map_err(|e| invalid(&e.to_string()))?
        .ok_or_else(|| invalid("requires its actual selected queue owner"))?;
    let queue = layout.queue()?;
    queue.revalidate_original_repair_attempt_witness(&attempt)?;
    let inspection = owner
        .repair_payload_inspection_context()
        .map_err(|e| invalid(&e.to_string()))?;
    inspection.inspect(attempt.payload_root())?;
    let target = attempt.carrier().target_node_id.clone();
    let (expected, call) = crate::node_transport::dfs_routed_call_rpc_call(call)
        .map_err(crate::DfsError::from_network)?;
    // Reuse the last gate of the existing detached sender: a closed/observed
    // birth or a frozen source cannot send merely because historical Attempt
    // DATA remains. This branch neither records a delivery nor renews admission.
    let dispatch = crate::runtime_message::checkpoint_detached_replica_delivery_with_layout(
        data_dir,
        crate::node_transport::DfsBackgroundReplicaDeliveryEvent::BeforeDispatch {
            message: expected.routed_message.clone(),
        },
        Some(&layout),
        Some(&inspection),
    )?;
    if matches!(
        dispatch,
        crate::node_transport::DfsBackgroundReplicaDispatch::Fenced
    ) {
        return Err(invalid("source dispatch is fenced"));
    }
    if let Some(deadline) = deadline {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| invalid("caller deadline expired before dispatch"))?;
        options.overall_timeout = Some(remaining);
    }
    // Concrete native owner only: neither DfsMessageSender, arbitrary peer
    // traits nor the generic decoded inbox can enter the constructor below.
    let peer = DaemonDfsPeerTransport {
        host: Arc::clone(&host),
        native_prefix: None, // Repair keeps its separate owned-return family.
    };
    #[cfg(any(test, feature = "test-support"))]
    let mut native_attempt = crate::native_shard_trace::begin_rpc_attempt(&target, &expected);
    let raw_return = peer.host.object_rpc.call(
        &target,
        call,
        options.cluster_rpc_options(),
        options.observe_connectivity,
    );
    #[cfg(any(test, feature = "test-support"))]
    if let (Some(observation), Err(error)) = (native_attempt.as_mut(), &raw_return) {
        observation.returned(&Err(DfsPeerTransportError::from_network(error.clone())));
    }
    let raw_return = raw_return.map_err(crate::DfsError::from_network)?;
    let decoded = crate::node_transport::dfs_message_response_from_rpc_result_sync(raw_return);
    #[cfg(any(test, feature = "test-support"))]
    if let Some(observation) = native_attempt.as_mut() {
        observation.returned(&decoded);
    }
    let decoded = decoded.map_err(|error| possibly_applied(error.to_string()))?;
    let returned =
        crate::node_transport::validate_original_repair_owned_rpc_return(&expected, decoded)
            .map_err(|error| possibly_applied(error.to_string()))?;
    let revalidate = || -> Result<(), crate::DfsError> {
        let current_host = registered_dfs_runtime_host(data_dir)
            .map_err(|error| invalid(&error.to_string()))?
            .ok_or_else(|| invalid("registered native host disappeared during RPC"))?;
        let current_owner = require_daemon_dfs_data_owner_lock(&paths)
            .map_err(|error| invalid(&error.to_string()))?;
        if !Arc::ptr_eq(&host, &current_host)
            || !Arc::ptr_eq(&owner, &current_owner)
            || current_host.shutting_down.is_cancelled()
        {
            return Err(invalid("native host or DATA owner changed during RPC"));
        }
        current_owner
            .selected_original_data_layout()
            .map_err(|error| invalid(&error.to_string()))?
            .ok_or_else(|| invalid("selected owner disappeared during RPC"))?;
        queue.revalidate_original_repair_attempt_witness(&attempt)?;
        inspection.inspect(attempt.payload_root())?;
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(invalid("caller deadline expired before completion capture"));
        }
        Ok(())
    };
    revalidate().map_err(|error| possibly_applied(error.to_string()))?;
    Ok(OwnedOriginalRepairCompletion {
        attempt,
        returned,
        rpc_request_id: expected.request_id,
        owner,
        scope,
        producer_admission: producer_admission.map(|proof| proof.into_historical()),
    })
}

#[derive(Clone, Debug)]
pub(super) struct DaemonDfsUnavailablePeerTransport;

pub(super) fn daemon_dfs_unavailable_peer_transport_error() -> DfsPeerTransportError {
    DfsPeerTransportError::new(
        "local-only DFS peer transport cannot send without the daemon-owned Tokio runtime",
    )
}

impl DfsPeerTransport for DaemonDfsUnavailablePeerTransport {
    fn call_object_rpc(
        &self,
        _target_node_id: &NodeId,
        _call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        Err(daemon_dfs_unavailable_peer_transport_error())
    }
}

impl sinomo_dfs::AsyncDfsPeerTransport for DaemonDfsUnavailablePeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        _target_node_id: &'a NodeId,
        _call: ObjectRpcCall,
        _options: DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>>
    {
        Box::pin(async { Err(daemon_dfs_unavailable_peer_transport_error()) })
    }
}

impl DfsPeerTransport for DaemonDfsPeerTransport {
    fn call_metadata_prefix_probe(
        &self, attempt: &crate::DfsMetadataPrefixProbeAttempt, options: DfsObjectRpcSendOptions,
    ) -> Result<crate::DfsMetadataPrefixProbeReturn, DfsPeerTransportError> {
        let context = self.native_prefix.as_ref().ok_or_else(|| DfsPeerTransportError::new("native prefix capture has no actual opener context"))?;
        super::native_prefix_return::call_native_prefix_probe(context,&self.host,attempt,options)
    }

    fn response_delivery_owner(&self) -> Option<Arc<crate::DfsRoutedResponseDelivery>> {
        Some(Arc::clone(&self.host.response_delivery))
    }

    fn call_object_rpc(
        &self,
        target_node_id: &NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> Result<ObjectRpcResult, DfsPeerTransportError> {
        self.host
            .object_rpc
            .call(
                target_node_id,
                call,
                options.cluster_rpc_options(),
                options.observe_connectivity,
            )
            .map_err(DfsPeerTransportError::from_network)
    }
}

impl sinomo_dfs::AsyncDfsPeerTransport for DaemonDfsPeerTransport {
    fn call_object_rpc_async<'a>(
        &'a self,
        target_node_id: &'a NodeId,
        call: ObjectRpcCall,
        options: DfsObjectRpcSendOptions,
    ) -> sinomo_dfs::DfsPeerTransportFuture<'a, Result<ObjectRpcResult, DfsPeerTransportError>>
    {
        Box::pin(async move {
            self.host
                .object_rpc
                .call_async(
                    target_node_id,
                    call,
                    options.cluster_rpc_options(),
                    options.observe_connectivity,
                )
                .await
                .map_err(DfsPeerTransportError::from_network)
        })
    }
}

pub(crate) fn daemon_dfs_peer_transports_on_runtime(
    data_dir: &SinomoDataDir,
    runtime: &tokio::runtime::Handle,
) -> (
    Arc<dyn DfsPeerTransport>,
    Arc<dyn sinomo_dfs::AsyncDfsPeerTransport>,
) {
    let Ok(host) = dfs_runtime_host(data_dir) else {
        let transport = Arc::new(DaemonDfsUnavailablePeerTransport);
        let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
        let async_peer_transport: Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> = transport;
        return (peer_transport, async_peer_transport);
    };
    let transport = Arc::new(DaemonDfsPeerTransport { host, native_prefix: Some(super::native_prefix_return::NativePrefixOwnerContext {data_dir:data_dir.clone(),runtime:runtime.clone()}) });
    let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
    let async_peer_transport: Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> = transport;
    (peer_transport, async_peer_transport)
}

#[cfg(test)]
pub(super) fn daemon_dfs_metadata_snapshot_install_sender() -> DfsMetadataSnapshotInstallSender {
    Arc::new(|_endpoint, _command, _timeout| {
        Err(DfsMetadataPeerTransportError::new(
            "test DFS metadata snapshot sender has no daemon runtime",
        ))
    })
}

// DFS inspection request/response types live in `sinomo-dfs`; this module only
// adapts them to daemon-local paths and metadata backends.

#[cfg(test)]
mod peer_delivery_tests {
    use super::*;
    use crate::{
        AsyncDfsMessageTransport, DfsMessageTransport, DfsNetworkMessage, DfsOutboundAttachment,
        DfsRoutedCall, DfsRoutedMessage, DfsStageBlobCommand,
    };
    use sinomo_core::network::{
        ClusterObjectRpcOptions, DestinationProcessAffinity, NetworkDeliveryState, NetworkError,
    };

    struct ClassifiedFailureCarrier {
        state: NetworkDeliveryState,
        calls: AtomicUsize,
    }

    impl ComponentObjectRpcTransport for ClassifiedFailureCarrier {
        fn call(
            &self,
            _target: &NodeId,
            call: ObjectRpcCall,
            options: ClusterObjectRpcOptions,
            _observe_connectivity: bool,
        ) -> Result<ObjectRpcResult, NetworkError> {
            assert!(!call.body.is_empty(), "exercise the DFS attachment adapter");
            assert_eq!(
                options.destination_process_affinity(),
                DestinationProcessAffinity::MainDaemon
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(NetworkError::delivery_failed(
                "classified host carrier failure",
                self.state,
                "injected request/response loss",
            ))
        }

        fn call_async<'a>(
            &'a self,
            target: &'a NodeId,
            call: ObjectRpcCall,
            options: ClusterObjectRpcOptions,
            observe_connectivity: bool,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<ObjectRpcResult, NetworkError>> + Send + 'a,
            >,
        > {
            Box::pin(async move { self.call(target, call, options, observe_connectivity) })
        }
    }

    fn attachment_call(local: &NodeId, remote: &NodeId, frame_id: u64) -> DfsRoutedCall {
        let bytes = b"host carrier delivery-state regression".to_vec();
        let hash = crate::content_hash(&bytes);
        DfsRoutedCall::with_attachment(
            DfsRoutedMessage {
                source_node_id: local.clone(),
                target_node_id: remote.clone(),
                frame_id,
                message_id: frame_id,
                message: DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                    request_id: format!("host-delivery-{frame_id}"),
                    intent_id: "host-delivery-intent".into(),
                    file_system_id: crate::sifs_file_system_id(),
                    object_id: crate::DfsObjectId::generate(),
                    expected_base_version: None,
                    version: crate::DfsVersion::generate(),
                    content_hash: hash.clone(),
                    bytes: Vec::new(),
                    object_metadata: None,
                    authority_shard: None,
                }),
            },
            DfsOutboundAttachment::from_bytes(bytes, hash),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_carrier_delivery_state_survives_sync_and_async_dfs_attachment_adapters() {
        for state in [
            NetworkDeliveryState::DefinitelyNotSent,
            NetworkDeliveryState::PossiblyApplied,
        ] {
            let root = sinomo_testdir::TestDir::new("dfs-host-carrier-delivery");
            let data_dir = SinomoDataDir::new(root.path());
            let baseline_host = test_dfs_runtime_host(&data_dir);
            let carrier = Arc::new(ClassifiedFailureCarrier {
                state,
                calls: AtomicUsize::new(0),
            });
            let host = Arc::new(DfsRuntimeHost::new(
                Arc::clone(&baseline_host.cluster),
                carrier.clone(),
                None,
            ));
            let peer = Arc::new(DaemonDfsPeerTransport { host, native_prefix: None });
            let local = NodeId::from_legacy_named("host-delivery-local").unwrap();
            let remote = NodeId::from_legacy_named("host-delivery-remote").unwrap();
            let sync_peer: Arc<dyn DfsPeerTransport> = peer.clone();
            let async_peer: Arc<dyn crate::AsyncDfsPeerTransport> = peer;
            let mut transport = DfsNodeTransport::with_peer_transports_for_nodes(
                local.clone(),
                [remote.clone()],
                Duration::from_secs(1),
                sync_peer,
                async_peer,
            );
            let error = transport
                .send_routed_call(&remote, attachment_call(&local, &remote, 1))
                .unwrap_err();
            assert_eq!(error.delivery_state(), Some(state), "sync: {error}");
            let error = transport
                .send_routed_call_async(&remote, attachment_call(&local, &remote, 2))
                .await
                .unwrap_err();
            assert_eq!(error.delivery_state(), Some(state), "async: {error}");
            assert_eq!(
                carrier.calls.load(Ordering::SeqCst),
                2,
                "ambiguous attachment delivery must not trigger a fresh resend"
            );
        }
    }
}
