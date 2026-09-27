use super::*;

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

pub(super) async fn handle_daemon_dfs_resident_replica_operation<R, F>(
    paths: &DaemonPaths,
    data_dir: Arc<SinomoDataDir>,
    expected_context: sinomo_dfs::DfsMetadataReplicaShardContext,
    runtime: &tokio::runtime::Handle,
    preempt_maintenance: bool,
    materialize_after: bool,
    operation: F,
) -> Result<DaemonDfsResidentReplicaOperation<R>, DaemonError>
where
    R: Send + 'static,
    F: FnOnce(&mut dyn DfsMetadataBackend) -> R + Send + 'static,
{
    let data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    let cluster_paths = ClusterPaths::new(data_dir.as_ref());
    let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)? else {
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
    let metadata_authority_preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
        &cluster_paths,
        &peer_snapshot,
        runtime,
    )
    .await?;
    let file_system_id = expected_context.shard.file_system_id.clone();
    let shard_id = DfsMetadataShardId::new(expected_context.shard.shard_id.clone());
    let exact_scope = DfsMessageExecutionScope::ExactShard {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let scheduler = daemon_dfs_mutation_scheduler(data_dir.as_ref())?;
    let mut admission =
        admit_daemon_dfs_message(&scheduler, &exact_scope, preempt_maintenance).await?;
    let mut shard = if admission.is_exact() {
        daemon_dfs_message_cache_exact_admission_handle_for_replica_context(
            data_dir.as_ref(),
            &cluster_paths,
            &peer_snapshot,
            &expected_context,
            runtime,
        )?
    } else {
        None
    };
    if shard.is_none() {
        if !admission.is_global() {
            drop(admission);
            admission = admit_daemon_dfs_message(
                &scheduler,
                &DfsMessageExecutionScope::GlobalExclusive,
                preempt_maintenance,
            )
            .await?;
        }
        // Generation creation and rollover require the global fence. Once the
        // resident generation is current, replica persistence checks out only
        // the exact shard and bypasses the routed service shell entirely.
        let resident = daemon_dfs_resident_metadata_backend_after_preflight(
            data_dir.as_ref(),
            &cluster_paths,
            &peer_snapshot,
            &metadata_authority_preflight,
            runtime,
        )?;
        if resident.is_none() {
            return Ok(DaemonDfsResidentReplicaOperation::Rejected {
                reason: format!(
                    "this node does not host filesystem {file_system_id} metadata shard {}",
                    shard_id.0
                ),
            });
        }
        shard = daemon_dfs_message_cache_exact_admission_handle_for_replica_context(
            data_dir.as_ref(),
            &cluster_paths,
            &peer_snapshot,
            &expected_context,
            runtime,
        )?;
    }
    let Some(shard) = shard else {
        return Ok(DaemonDfsResidentReplicaOperation::Rejected {
            reason: format!(
                "metadata replica authority context is stale for filesystem {file_system_id} shard {}",
                shard_id.0
            ),
        });
    };
    let process_shard = shard.clone();
    let process_data_dir = data_dir.as_ref().clone();
    let process_data_owner_lock = Arc::clone(&data_owner_lock);
    let process_runtime = runtime.clone();
    let result = tokio::task::spawn_blocking(move || {
        // Both capabilities live inside the blocking task: cancellation of the
        // object-RPC future cannot admit another mutation or invalidate daemon
        // data ownership while durable persistence is still running.
        let result = process_shard
            .with_backend(|backend| {
                Ok::<R, sinomo_dfs::MetadataBackendError>(operation(
                    backend as &mut dyn DfsMetadataBackend,
                ))
            })
            .map_err(DaemonError::from)?;
        if materialize_after {
            let generation_fence =
                admission
                    .into_generation_fence()
                    .ok_or_else(|| DaemonError::Parse {
                        detail: "durable DFS replica ingress lacks a metadata generation fence"
                            .to_string(),
                    })?;
            // Schedule before returning from the cancellation-independent
            // blocking task. The materializer owns the generation and daemon
            // data capabilities even if the object-RPC future is dropped.
            schedule_daemon_dfs_replica_materialization_on_runtime(
                process_data_dir,
                &process_runtime,
                process_shard,
                process_data_owner_lock,
                generation_fence,
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
    let Some(preflight_peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)?
    else {
        return Err(DaemonError::ClusterOrphan);
    };
    let metadata_authority_preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
        &cluster_paths,
        &preflight_peer_snapshot,
        runtime,
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
        let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)? else {
            return Err(DaemonError::ClusterOrphan);
        };
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
                let read_only = !matches!(
                    &request,
                    sinomo_dfs::DaemonDfsInspectionRequest::ResolveConflicts { .. }
                        | sinomo_dfs::DaemonDfsInspectionRequest::UseConflictVersion { .. }
                );
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
            let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)
                .map_err(|source| DfsDataPlaneHostError::new(source.to_string()))?
            else {
                return Ok(None);
            };
            let preflight = warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
                &cluster_paths,
                &peer_snapshot,
                &runtime,
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
        let data_owner = self.require_data_owner();
        let validation = self.validate_data_dir(data_dir.as_ref());
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let event_client = self.event_client.clone();
        Box::pin(async move {
            let _data_owner = data_owner?;
            validation?;
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
                    "metadata journal command does not match the authorized shard identity",
                ));
            }
            let namespace_changed = command
                .batch
                .entries
                .iter()
                .any(|entry| entry.mutation.changes_visible_object_placement());
            let mut namespace_paths = command
                .batch
                .entries
                .iter()
                .flat_map(|entry| entry.mutation.visible_namespace_paths())
                .map(str::to_string)
                .collect::<BTreeSet<_>>();
            if namespace_changed && namespace_paths.is_empty() {
                namespace_paths.insert("/".to_string());
            }
            let event_origin_node_id = context.local_node_id.clone();
            let may_have_deferred_materialization = command.durable_replication.is_some()
                || command
                    .authority_probe
                    .as_ref()
                    .is_some_and(|probe| probe.durable_journal_ack);
            // The retained publisher's caught-up probe is periodic replica
            // verification, not foreground mutation work. Let that exact
            // request identity queue behind an admitted routine pass so
            // synchronized probes cannot repeatedly cancel repair-queue
            // reconciliation. Handoff/sync probes, real journal application,
            // and malformed or mixed-purpose commands retain preemption.
            let preempt_maintenance = !daemon_dfs_metadata_prefix_probe_is_verification_only(
                &command,
                &authenticated_source_node_id,
                &context.local_node_id,
            );
            let rejection_request_id = command.request_id.clone();
            let rejection_shard_id = command.batch.shard_id.clone();
            let rejection_next_after_sequence = command.batch.next_after_sequence;
            let admitted_context = context.clone();
            let operation = handle_daemon_dfs_resident_replica_operation(
                &paths,
                data_dir.clone(),
                admitted_context,
                &runtime,
                preempt_maintenance,
                may_have_deferred_materialization,
                move |backend| {
                    let response = sinomo_dfs::process_dfs_metadata_journal_batch(
                        backend,
                        Some(&context),
                        command,
                    );
                    #[cfg(test)]
                    wait_at_daemon_dfs_message_committed_edge_test_hook(&response.request_id);
                    response
                },
            )
            .await
            .map_err(daemon_dfs_data_plane_error)?;
            let response = match operation {
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
            if namespace_changed
                && response.applied_entries > 0
                && response.result == sinomo_dfs::DfsMetadataJournalBatchResult::Applied
                && let Some(event_client) = event_client
            {
                if let Ok(data_dir) = paths.data_dir() {
                    crate::attachment_maintenance::notify(&data_dir);
                }
                publish_dfs_namespace_change(
                    &event_client,
                    event_origin_node_id,
                    "metadata-journal-applied",
                    namespace_paths.into_iter().collect(),
                )
                .await;
            }
            Ok(response)
        })
    }

    fn install_metadata_snapshot<'a>(
        &'a self,
        data_dir: Arc<SinomoDataDir>,
        authenticated_source_node_id: NodeId,
        command: sinomo_dfs::DfsMetadataSnapshotInstallCommand,
        context: Option<sinomo_dfs::DfsMetadataReplicaShardContext>,
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
                move |backend| {
                    sinomo_dfs::process_dfs_metadata_snapshot_install(
                        backend,
                        Some(&context),
                        command,
                    )
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
    ) -> DfsDataPlaneHostFuture<'a, DfsDataPlaneResponse> {
        let data_owner = self.require_data_owner();
        let paths = self.paths.clone();
        let runtime = self.runtime.clone();
        let event_client = self.event_client.clone();
        let namespace_event = daemon_dfs_visible_mutation_event(&command);
        Box::pin(async move {
            let _data_owner = data_owner?;
            let response =
                handle_daemon_dfs_message_async(&paths, command, attachment, traceparent, &runtime)
                    .await
                    .map_err(daemon_dfs_data_plane_error)?;
            if daemon_dfs_response_committed_visible_mutation(&response)
                && let (Some(event_client), Some(namespace_event)) = (event_client, namespace_event)
            {
                if let Ok(data_dir) = paths.data_dir() {
                    crate::attachment_maintenance::notify(&data_dir);
                }
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

/// Attach a control plane which retains the main daemon's ownership proof for
/// the complete service lifetime. Sandbox services can escape the blocking
/// handler which creates them; ordinary cached/message services must use the
/// weak-capability helper above.
pub(crate) fn with_daemon_owned_dfs_control_plane_on_runtime<T: sinomo_dfs::DfsMessageTransport>(
    service: NetworkedDfsService<T>,
    cluster_paths: &ClusterPaths,
    runtime: Option<&tokio::runtime::Handle>,
) -> NetworkedDfsService<T> {
    let Some(runtime) = runtime else {
        return service;
    };
    if !cluster_paths.state.exists() {
        return service;
    }
    let mut control_plane =
        DaemonDfsControlPlaneKvClient::new_owned_on_runtime(cluster_paths, runtime);
    let service = with_daemon_dfs_placement_environment(service, cluster_paths, &mut control_plane);
    service.with_boxed_control_plane(Box::new(control_plane))
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

#[derive(Clone)]
pub(super) struct DaemonDfsPeerTransport {
    pub(super) host: Arc<DfsRuntimeHost>,
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
            .map_err(|source| DfsPeerTransportError::new(source.to_string()))
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
                .map_err(|source| DfsPeerTransportError::new(source.to_string()))
        })
    }
}

pub(crate) fn daemon_dfs_peer_transports_on_runtime(
    data_dir: &SinomoDataDir,
    _runtime: &tokio::runtime::Handle,
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
    let transport = Arc::new(DaemonDfsPeerTransport { host });
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

pub(crate) fn open_daemon_dfs_metadata_backend(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Box<dyn DfsMetadataBackend>, DaemonError> {
    sinomo_dfs::open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths).map_err(Into::into)
}
