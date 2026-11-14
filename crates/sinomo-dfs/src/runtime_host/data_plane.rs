use super::*;
use crate::{DfsMessageResponse, DfsNetworkMessage, DfsRoutedMessage};

#[derive(Clone, Debug)]
pub(crate) struct DaemonDfsDataPlaneHost {
    pub(super) paths: DaemonPaths,
    pub(super) runtime: tokio::runtime::Handle,
    pub(super) event_client: Option<ComponentEventClient>,
    live_owner: Option<Weak<DfsRuntimeHost>>,
}

impl DaemonDfsDataPlaneHost {
    #[cfg(test)]
    pub(crate) fn new_on_runtime(paths: DaemonPaths, runtime: &tokio::runtime::Handle) -> Self {
        let live_owner = Self::current_live_owner(&paths);
        Self {
            live_owner,
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
        let live_owner = Self::current_live_owner(&paths);
        Self {
            live_owner,
            paths,
            runtime: runtime.clone(),
            event_client: Some(event_client),
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_live_content_advertisement_for_test(
        &self, data_dir: &SinomoDataDir, deadline: tokio::time::Instant,
    ) -> Result<sinomo_dfs::DfsNodeAdvertisement, String> {
        self.validate_data_dir(data_dir).map_err(|error| error.to_string())?;
        let owner = self.live_owner.as_ref().and_then(Weak::upgrade)
            .ok_or_else(|| "missing actual live owner".to_string())?;
        super::placement_environment::wait_live_owner_observation_for_test(data_dir, &owner, deadline).await
    }

    fn current_live_owner(paths: &DaemonPaths) -> Option<Weak<DfsRuntimeHost>> {
        let root = daemon_data_dir(paths).ok()?;
        registered_dfs_runtime_host(&SinomoDataDir::new(root)).ok().flatten()
            .map(|owner| Arc::downgrade(&owner))
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
/// Whether `local`'s published record view of a write-set shard is at or past
/// (`epoch`, `generation`), or no longer names `local` as a member. A view
/// that is not ready yet keeps the caller waiting; a shard that disappeared
/// or cannot be read ends the wait so ordinary validation answers.
pub(super) fn daemon_dfs_write_set_view_reached(
    snapshot: &crate::DfsConfigurationSnapshot,
    local: &NodeId,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
    epoch: u64,
    generation: u64,
) -> bool {
    let Some(entries) =
        snapshot.entries(crate::metadata_authority_runtime::DFS_METADATA_AUTHORITY_PREFIX)
    else {
        return false;
    };
    match crate::metadata_replica_context_from_authority_entries(
        entries,
        local,
        file_system_id,
        shard_id,
    ) {
        Ok(Some(context)) => {
            context.shard.epoch > epoch
                || !context.local_node_holds_copy()
                || (context.shard.epoch == epoch
                    && context
                        .write_set
                        .as_ref()
                        .is_some_and(|write_set| write_set.generation >= generation))
        }
        Ok(None) | Err(_) => true,
    }
}

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

impl DfsDataPlaneHost for DaemonDfsDataPlaneHost {
    fn live_content_advertisement(
        &self,
        data_dir: &SinomoDataDir,
    ) -> Result<sinomo_dfs::DfsNodeAdvertisement, DfsDataPlaneHostError> {
        self.validate_data_dir(data_dir)?;
        let _data_owner = self.require_data_owner()?;
        let unavailable = || DfsDataPlaneHostError::new("live DFS content owner observation unavailable");
        let owner = self.live_owner.as_ref().and_then(Weak::upgrade).ok_or_else(unavailable)?;
        let current = registered_dfs_runtime_host(data_dir)
            .map_err(daemon_dfs_data_plane_error)?.ok_or_else(unavailable)?;
        if !Arc::ptr_eq(&owner, &current) || owner.shutting_down.is_cancelled() {
            return Err(unavailable());
        }
        super::placement_environment::live_daemon_dfs_content_advertisement(
            data_dir, &owner, time::OffsetDateTime::now_utc(),
        ).ok_or_else(unavailable)
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

    fn await_metadata_write_set_generation<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        file_system_id: DfsFileSystemId,
        shard_id: DfsMetadataShardId,
        epoch: u64,
        generation: u64,
    ) -> Option<DfsDataPlaneHostFuture<'a, ()>> {
        let data_owner = self.require_data_owner();
        let validation = self.validate_data_dir(data_dir.as_ref());
        let runtime = self.runtime.clone();
        Some(Box::pin(async move {
            let _data_owner = data_owner?;
            validation?;
            let cluster_paths = ClusterPaths::new(data_dir.as_ref());
            let state =
                sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths)
                    .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?;
            let authority = daemon_dfs_metadata_authority_generation_for_identity(
                &cluster_paths,
                &state.cluster_id,
                &state.local_node_id,
                Some(&runtime),
            )
            .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?;
            let mut published = authority.subscribe_configuration_snapshot();
            loop {
                let snapshot = Arc::clone(&published.borrow_and_update());
                if daemon_dfs_write_set_view_reached(
                    &snapshot,
                    &state.local_node_id,
                    &file_system_id,
                    &shard_id,
                    epoch,
                    generation,
                ) {
                    return Ok(());
                }
                if published.changed().await.is_err() {
                    // The host is shutting down; validation answers the probe.
                    return Ok(());
                }
            }
        }))
    }

    fn apply_metadata_journal_batch<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        authenticated_source_node_id: NodeId,
        command: sinomo_dfs::DfsMetadataJournalBatchCommand,
        context: Option<sinomo_dfs::DfsMetadataReplicaShardContext>,
    ) -> DfsDataPlaneHostFuture<'a, sinomo_dfs::DfsMetadataJournalBatchResponse> {
        let data_owner = self.require_data_owner();
        let validation = self.validate_data_dir(data_dir.as_ref());
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let event_client = self.event_client.clone();
        Box::pin(async move {
            let _data_owner = data_owner?;
            validation?;
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
            let guard_data_dir = Arc::clone(&data_dir);
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
                        let response =
                            sinomo_dfs::process_dfs_metadata_journal_batch(backend, Some(context), command);
                        #[cfg(any(test, feature = "test-support"))]
                        if let Some((root, probe, started)) = &diagnostic_apply {
                            crate::native_boundary_diagnostics::record(root, "replica-authority-applied", || serde_json::json!({
                                "probe": probe, "context": format!("{context:?}"), "response": response,
                                "ingress_elapsed_us": started.elapsed().as_micros(),
                            }), false);
                        }
                        let payloads = logical
                            .map_err(crate::DfsError::from)
                            .and_then(|batch| {
                                crate::metadata_replication::committed_payloads_for_batch(
                                    backend, &batch,
                                )
                            });
                        if response.applied_entries > 0
                            && response.result == sinomo_dfs::DfsMetadataJournalBatchResult::Applied
                            && let Ok(payloads) = &payloads
                        {
                            observe_replicated_namespace_job_guards(
                                guard_data_dir.as_ref(),
                                &context.shard,
                                payloads.iter().flat_map(|payload| &payload.metadata_effects),
                            );
                        }
                        let namespace_paths = payloads
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
            let guard_data_dir = Arc::clone(&data_dir);
            let operation = handle_daemon_dfs_resident_replica_operation(
                &paths,
                data_dir,
                admitted_context,
                &runtime,
                true,
                false,
                (&rejection_request_id, "metadata-snapshot-install"),
                move |shard, context| {
                    let response = shard.install_replica_snapshot(context, command)?;
                    if matches!(
                        response.result,
                        sinomo_dfs::DfsMetadataSnapshotInstallResult::Installed { .. }
                    ) {
                        replace_installed_namespace_job_guards(
                            guard_data_dir.as_ref(),
                            shard,
                            &context.shard,
                        );
                    }
                    Ok(response)
                },
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

/// A replicated batch carries the job records of the shard's cross-shard
/// namespace work. Apply them to the guards this node's served lanes share,
/// under the same shard admission as the batch, so a finished job releases its
/// paths here when its record arrives rather than at the next full hydration.
fn observe_replicated_namespace_job_guards<'a>(
    data_dir: &SinomoDataDir,
    shard: &sinomo_dfs::DfsMetadataShardConfig,
    effects: impl Iterator<Item = &'a sinomo_dfs::DfsPreparedMetadataEffect>,
) {
    let mut jobs = effects
        .filter_map(|effect| match effect {
            sinomo_dfs::DfsPreparedMetadataEffect::PutNamespaceJob { job } => Some(job),
            _ => None,
        })
        .peekable();
    if jobs.peek().is_none() {
        return;
    }
    match daemon_dfs_message_cache_cross_shard_namespace_locks(data_dir) {
        Ok(Some(guards)) => {
            let shard_id = Some(DfsMetadataShardId::new(shard.shard_id.clone()));
            for job in jobs {
                guards.observe_job(shard_id.clone(), job);
            }
        }
        // No served lane exists yet: its hydration reads these durable records.
        Ok(None) => {}
        Err(error) => tracing::warn!(target: "sinomo.dfs", %error,
            data_root = %data_dir.root().display(),
            "replicated namespace job guards could not be applied"),
    }
}

/// An installed snapshot replaces the shard's job records; replace that shard's
/// guards with the installed running jobs under the same admission. The
/// install itself stands even when this projection cannot be read: the next
/// full hydration rebuilds it from the same records.
fn replace_installed_namespace_job_guards(
    data_dir: &SinomoDataDir,
    handle: &sinomo_dfs::ResidentLocalMetadataShardHandle,
    shard: &sinomo_dfs::DfsMetadataShardConfig,
) {
    let guards = match daemon_dfs_message_cache_cross_shard_namespace_locks(data_dir) {
        Ok(Some(guards)) => guards,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(target: "sinomo.dfs", %error,
                data_root = %data_dir.root().display(),
                "installed namespace job guards could not be applied");
            return;
        }
    };
    let running = handle.with_backend(|backend| {
        let mut running = Vec::new();
        let mut cursor = None;
        loop {
            let page = sinomo_dfs::DfsMetadataBackend::list_namespace_jobs_page(
                backend,
                cursor.as_ref(),
                sinomo_dfs::DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE,
            )?;
            for job in page.jobs {
                if job.status == sinomo_dfs::DfsNamespaceJobStatus::Running
                    && let Some(paths) =
                        crate::service_state::cross_shard_namespace_job_guard_paths(&job)
                {
                    running.push((job.job_id, job.file_system_id, paths));
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(running),
            }
        }
    });
    match running {
        Ok(running) => guards
            .replace_shard_jobs(&DfsMetadataShardId::new(shard.shard_id.clone()), running),
        Err(error) => tracing::warn!(target: "sinomo.dfs", %error,
            data_root = %data_dir.root().display(),
            "installed namespace job guards could not be read"),
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
    let Some(runtime) = runtime else {
        return service;
    };
    if cluster_paths.state.exists() {
        let mut control_plane =
            DaemonDfsControlPlaneKvClient::new_daemon_client_on_runtime(cluster_paths, runtime);
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

#[cfg(any(test, feature = "test-support"))]
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
    let peer = Arc::new(DaemonDfsPeerTransport { host, native_prefix: Some(super::native_prefix_return::NativePrefixOwnerContext::new(data_dir, runtime)) });
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
    let transport = Arc::new(DaemonDfsPeerTransport { host, native_prefix: Some(super::native_prefix_return::NativePrefixOwnerContext::new(data_dir, runtime)) });
    let peer_transport: Arc<dyn DfsPeerTransport> = transport.clone();
    let async_peer_transport: Arc<dyn sinomo_dfs::AsyncDfsPeerTransport> = transport;
    (peer_transport, async_peer_transport)
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
                    receipt_generation: 0,
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
