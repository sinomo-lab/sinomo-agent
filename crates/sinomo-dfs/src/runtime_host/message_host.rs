use super::*;

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DaemonDfsPublishPlanningAttemptOutcome {
    Complete,
    AuthorityStale,
    Failed,
    Deadline,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DaemonDfsPreDispatchTestPoint<'a> {
    ContentStoreCacheAccess,
    ExactLaneCacheAccess,
    BeforePreflightFence,
    AfterPreflightFence,
    PublishPlanningAuthorityCaptured {
        /// Whether this exact planner callback still owns its actual admission.
        admission_held: bool,
    },
    BeforeResidentPreflight,
    /// Negative-only local activation cut after observing completed authority.
    BeforeCompletedPrimaryRebind {
        shard_id: &'a crate::DfsMetadataShardId,
    },
    BeforeReplicaContextComparison,
    BeforeResidentBackendOpen,
    /// Whether the captured actual runtime admission owner has been dropped.
    ReplicaAdmissionReleased {
        released: bool,
    },
    /// Native prefix call made by this invocation's actual refresh owner.
    AuthorityNativePrefixRead {
        prefix: &'static str,
    },
    /// Entry into the original bounded outer publication attempt.
    PublishPlanningAttemptStarted {
        replan: usize,
    },
    /// Typed result of that actual attempt, before error propagation.
    PublishPlanningAttemptFinished {
        replan: usize,
        outcome: DaemonDfsPublishPlanningAttemptOutcome,
    },
    BeforeAuthorityRefresh {
        owner: &'static str,
        replan: usize,
    },
    /// Negative control before the common transfer claim; facts are copied
    /// from the actual command and current native owner, never test flags.
    BeforeReplicaTransferClaim {
        kind: sinomo_api::dfs::maintenance::DfsRepairTransferKind,
        background_repair: bool,
        intent_id_digest: [u8; 32],
        target_id_digest: [u8; 32],
        request_id_digest: [u8; 32],
        /// Exact prior producer row copied before this claim mutates any state.
        prior_transfer: Option<&'a crate::DfsRepairAttemptRecord>,
    },
    BeforeExecute {
        /// Presence of the original owned runtime admission at execution entry.
        admission_held: bool,
    },
    AfterExecute,
    /// Facts from a Running copy job reloaded at its durable step boundary.
    BeforeContentCopyStep {
        job_id_digest: [u8; 32],
        source_reference_selected: bool,
        source_reference_applied: bool,
    },
    /// Actual new retained-owner boundaries; hooks supply only negative faults.
    BeforeRetainedRepairQueueEnrollment {
        owner_id_digest: [u8; 32],
    },
    AfterRetainedRepairQueueEnrollment {
        owner_id_digest: [u8; 32],
    },
    /// Read-only facts from the actual Main report and its queue capture.
    AfterReport {
        committed_work: DfsMessageCommittedWork,
        repair_record_count: usize,
        new_repair_records_unattempted: bool,
        succeeded: bool,
    },
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) type DaemonDfsPreDispatchTestHook = dyn for<'a> Fn(
        DaemonDfsPreDispatchTestPoint<'a>,
        Option<&DfsMessageCommand>,
        Option<&DaemonDfsMetadataAuthorityPreflight>,
    ) -> Result<(), sinomo_dfs::DfsError>
    + Send
    + Sync;

#[cfg(any(test, feature = "test-support"))]
static DAEMON_DFS_PRE_DISPATCH_TEST_HOOKS: std::sync::OnceLock<
    Mutex<BTreeMap<PathBuf, Arc<DaemonDfsPreDispatchTestHook>>>,
> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-support"))]
pub(crate) struct DaemonDfsPreDispatchTestHookGuard {
    root: PathBuf,
}

#[cfg(any(test, feature = "test-support"))]
impl Drop for DaemonDfsPreDispatchTestHookGuard {
    fn drop(&mut self) {
        if let Some(hooks) = DAEMON_DFS_PRE_DISPATCH_TEST_HOOKS.get() {
            hooks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.root);
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn install_daemon_dfs_pre_dispatch_test_hook(
    data_dir: &SinomoDataDir,
    hook: Arc<DaemonDfsPreDispatchTestHook>,
) -> DaemonDfsPreDispatchTestHookGuard {
    let root = data_dir.root();
    let mut hooks = DAEMON_DFS_PRE_DISPATCH_TEST_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert!(
        !hooks.contains_key(&root),
        "pre-dispatch test hook already installed for root"
    );
    hooks.insert(root.clone(), hook);
    DaemonDfsPreDispatchTestHookGuard { root }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn daemon_dfs_pre_dispatch_test_hook(
    data_dir: &SinomoDataDir,
) -> Option<Arc<DaemonDfsPreDispatchTestHook>> {
    DAEMON_DFS_PRE_DISPATCH_TEST_HOOKS.get().and_then(|hooks| {
        hooks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&data_dir.root())
            .cloned()
    })
}

pub(crate) fn daemon_dfs_message_service_cache() -> &'static sinomo_dfs::DfsRuntimeMessageCache {
    sinomo_dfs::dfs_runtime_message_cache()
}

/// Borrow only an already-published resident metadata generation for status.
/// A busy cache, missing hydration, or observed authority rollover is an
/// explicit unavailable result: this read-only path must never wait for or
/// initiate the mutation lane's cold-open work.
pub(crate) fn daemon_dfs_bounded_status_resident(
    data_dir: &SinomoDataDir,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
) -> Result<ResidentLocalShardedMetadataBackend, sinomo_dfs::DfsError> {
    let slot = daemon_dfs_message_service_cache()
        .try_existing_root(&data_dir.root())?
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: "bounded DFS status unavailable because no resident metadata generation has been published".into(),
        })?;
    let cache = match slot.try_lock() {
        Ok(cache) => cache,
        Err(std::sync::TryLockError::WouldBlock) => {
            return Err(sinomo_dfs::DfsError::Network {
                message: "bounded DFS status unavailable while the resident generation cache is being updated"
                    .to_string(),
            });
        }
        Err(std::sync::TryLockError::Poisoned(_)) => {
            return Err(daemon_dfs_cache_lock_error());
        }
    };
    let entry = cache.as_ref().ok_or_else(|| {
        sinomo_dfs::DfsError::Network {
            message:
                "bounded DFS status unavailable because no resident metadata generation has been published"
                    .to_string(),
        }
    })?;
    entry.bounded_status_resident(peer_snapshot)
}

pub(crate) fn resident_dfs_observe_metrics(
    data_dir: &SinomoDataDir,
) -> Result<Vec<sinomo_api::observe::ObserveMetricRecord>, String> {
    let (resident, local_node_id) = {
        let slot = daemon_dfs_message_service_cache()
            .try_existing_root(&data_dir.root())
            .map_err(|error| format!("resident DFS metric exporter cache is busy: {error}"))?
            .ok_or_else(|| {
                "resident DFS metric exporter has no published metadata generation".to_string()
            })?;
        let cache = slot
            .try_lock()
            .map_err(|error| format!("resident DFS metric exporter cache is busy: {error}"))?;
        let entry = cache.as_ref().ok_or_else(|| {
            "resident DFS metric exporter has no published metadata generation".to_string()
        })?;
        let resident = entry
            .bounded_status_resident_current()
            .map_err(|error| format!("resident DFS metric exporter is unavailable: {error}"))?;
        (resident, entry.signature.local_node_id.clone())
    };
    let snapshot = resident
        .bounded_status_snapshot()
        .map_err(|error| format!("resident DFS metric snapshot is unavailable: {error}"))?;
    let status =
        sinomo_dfs::project_dfs_status_from_resident_snapshot(Some(&local_node_id), snapshot);
    Ok(crate::dfs_status_observe_metrics(status))
}

#[cfg(test)]
pub(crate) fn record_daemon_dfs_test_message_service_lane_open(data_dir: &SinomoDataDir) {
    let mut opens = DAEMON_DFS_TEST_MESSAGE_SERVICE_LANE_OPENS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test message service lane open counts");
    *opens.entry(data_dir.root()).or_default() += 1;
}

#[cfg(test)]
pub(crate) fn daemon_dfs_test_message_service_lane_open_count(data_dir: &SinomoDataDir) -> usize {
    DAEMON_DFS_TEST_MESSAGE_SERVICE_LANE_OPENS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test message service lane open counts")
        .get(&data_dir.root())
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn record_daemon_dfs_test_authoritative_exact_set_call(cluster_paths: &ClusterPaths) {
    let mut calls = DAEMON_DFS_TEST_AUTHORITATIVE_EXACT_SET_CALLS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test authoritative exact-set call counts");
    *calls.entry(cluster_paths.root.clone()).or_default() += 1;
}

#[cfg(test)]
pub(crate) fn take_daemon_dfs_test_authoritative_exact_set_call_count(
    cluster_paths: &ClusterPaths,
) -> usize {
    DAEMON_DFS_TEST_AUTHORITATIVE_EXACT_SET_CALLS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test authoritative exact-set call counts")
        .remove(&cluster_paths.root)
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn record_daemon_dfs_test_authoritative_prefix_call(cluster_paths: &ClusterPaths) {
    let mut calls = DAEMON_DFS_TEST_AUTHORITATIVE_PREFIX_CALLS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test authoritative prefix call counts");
    *calls.entry(cluster_paths.root.clone()).or_default() += 1;
}

#[cfg(test)]
pub(crate) fn take_daemon_dfs_test_authoritative_prefix_call_count(
    cluster_paths: &ClusterPaths,
) -> usize {
    DAEMON_DFS_TEST_AUTHORITATIVE_PREFIX_CALLS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("test authoritative prefix call counts")
        .remove(&cluster_paths.root)
        .unwrap_or_default()
}

pub(crate) fn evict_daemon_dfs_message_service_cache(
    data_dir: &SinomoDataDir,
) -> Result<bool, DaemonError> {
    let cache_key = data_dir.root();
    let removed = daemon_dfs_message_service_cache()
        .evict(&cache_key)
        .map_err(|_| DaemonError::Network {
            address: "DFS message service cache".to_string(),
            detail: "DFS message service cache is poisoned".to_string(),
        })?;
    if removed {
        tracing::info!(
            data_root = %cache_key.display(),
            "released daemon DFS message service before mandatory lifecycle work"
        );
    }
    Ok(removed)
}

#[cfg(test)]
pub(crate) fn take_daemon_dfs_message_mutation_service_for_maintenance(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    cluster_state: &sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
    metadata_authority_preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
) -> Result<DaemonDfsMaintenanceServiceLane, sinomo_dfs::DfsError> {
    try_take_daemon_dfs_message_mutation_service_for_maintenance(
        data_dir,
        cluster_paths,
        cluster_state,
        runtime,
        metadata_authority_preflight,
    )
    .map_err(DaemonDfsAuthorityPreExecutionError::into_dfs_error)
}

pub(super) fn try_take_daemon_dfs_message_mutation_service_for_maintenance(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    cluster_state: &sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
    metadata_authority_preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
) -> Result<DaemonDfsMaintenanceServiceLane, DaemonDfsAuthorityPreExecutionError> {
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight,
            None,
            metadata_authority_preflight,
        )?;
    }
    // Route discovery uses the same mounted owner's existing membership and
    // service projections as ordinary message admission. A fresh generic
    // configuration client would download the service catalog on every tick.
    // Publication still validates the retained authority generation below.
    let peer_snapshot = read_daemon_dfs_message_peer_snapshot(cluster_paths)?
        .filter(|snapshot| {
            snapshot.cluster_id == cluster_state.cluster_id
                && snapshot.local_node_id == cluster_state.local_node_id
        })
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: "DFS maintenance has no matching native peer projection".into(),
        })?;
    let peer_node_ids = peer_snapshot.peer_node_ids;
    let metadata_authority_generation = match metadata_authority_preflight {
        Some(preflight) => {
            daemon_dfs_require_retained_authority(&preflight.authority, preflight.generation)?;
            Arc::clone(&preflight.authority)
        }
        None => daemon_dfs_metadata_authority_generation_for_identity(
            cluster_paths,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
            Some(runtime),
        )?,
    };
    let (authority_generation, storage_generation) =
        metadata_authority_generation.lifecycle_generations()?;
    if let Some(preflight) = metadata_authority_preflight {
        daemon_dfs_require_retained_authority(
            &metadata_authority_generation,
            preflight.generation,
        )?;
    }
    let signature = DaemonDfsMessageServiceSignature {
        cluster_id: cluster_state.cluster_id.clone(),
        local_node_id: cluster_state.local_node_id.clone(),
        metadata_authority_generation: authority_generation,
        metadata_storage_generation: storage_generation,
    };
    let cached = {
        let slot = daemon_dfs_message_service_cache().root(&data_dir.root())?;
        let mut cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
        let stale = cache.as_ref().is_some_and(|entry| {
            !entry.signature.shares_metadata_storage(&signature)
                || !Arc::ptr_eq(
                    &entry.metadata_authority_generation,
                    &metadata_authority_generation,
                )
        });
        let (retained_content_store, retained_metadata_backend) = if stale {
            let entry = cache.take().expect("stale cache entry exists");
            let same_owner = entry.signature.cluster_id == signature.cluster_id
                && entry.signature.local_node_id == signature.local_node_id
                && Arc::ptr_eq(
                    &entry.metadata_authority_generation,
                    &metadata_authority_generation,
                );
            (
                Some(entry.content_store),
                same_owner
                    .then_some(entry.resident_metadata_backend)
                    .flatten(),
            )
        } else {
            (None, None)
        };
        if cache.is_none() {
            let content_store = match retained_content_store {
                Some(content_store) => content_store,
                None => sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?,
            };
            let resident_metadata_backend =
                try_open_daemon_dfs_resident_metadata_backend_for_authority_generation(
                    data_dir,
                    &signature.local_node_id,
                    &metadata_authority_generation,
                    signature.metadata_authority_generation,
                    retained_metadata_backend,
                )?;
            cache.replace(DaemonDfsMessageServiceCacheEntry {
                signature: signature.clone(),
                metadata_authority_generation: Arc::clone(&metadata_authority_generation),
                peer_node_ids: peer_node_ids.clone(),
                content_store,
                resident_metadata_backend,
                resident_metadata_initialized: true,
                write_intent_maintenance_scan: Arc::new(Default::default()),
                maintenance_progress: Arc::new(Default::default()),
                cross_shard_namespace_locks: Default::default(),
                primary_service: None,
                exact_shard_services: BTreeMap::new(),
                content_read_service: None,
            });
        }
        let entry = cache
            .as_mut()
            .expect("daemon DFS message cache entry inserted");
        // Exclusive maintenance admission has drained old exact leases. The
        // control-plane clients consume the new projection on their next read;
        // no physical metadata recovery is owned by a policy/proof delta.
        entry.signature = signature.clone();
        entry.peer_node_ids.clone_from(&peer_node_ids);
        if !entry.resident_metadata_initialized {
            entry.resident_metadata_backend =
                try_open_daemon_dfs_resident_metadata_backend_for_authority_generation(
                    data_dir,
                    &entry.signature.local_node_id,
                    &entry.metadata_authority_generation,
                    entry.signature.metadata_authority_generation,
                    entry.resident_metadata_backend.take(),
                )?;
            entry.resident_metadata_initialized = true;
        }
        // Extraction and rejected-take restoration share the cache lock. A
        // reader must never observe an empty slot while the old service remains
        // shared; only a successful exclusive take may expose that state.
        let cached = match entry.primary_service.take() {
            Some(handle) => match Arc::try_unwrap(handle) {
                Ok(handle) => Some(handle),
                Err(handle) => {
                    entry.primary_service = Some(handle);
                    return Err(sinomo_dfs::DfsError::Network {
                        message: "daemon DFS mutation lane remained referenced after exclusive maintenance admission".to_string(),
                    }.into());
                }
            },
            None => None,
        };
        (
            cached,
            entry.content_store.clone(),
            entry.resident_metadata_backend.clone(),
            entry.cross_shard_namespace_locks.clone(),
            Arc::clone(&entry.write_intent_maintenance_scan),
            Arc::clone(&entry.maintenance_progress),
        )
    };

    let (
        cached,
        content_store,
        resident_metadata_backend,
        cross_shard_namespace_locks,
        write_intent_maintenance_scan,
        maintenance_progress,
    ) = cached;
    let handle = match cached {
        Some(handle) => handle,
        None => Arc::try_unwrap(open_daemon_dfs_message_service_lane_with_content(
            data_dir,
            cluster_paths,
            &signature.local_node_id,
            peer_node_ids.clone(),
            runtime,
            content_store,
            Some(
                metadata_authority_generation
                    .load_complete_prefix_for_generation(signature.metadata_authority_generation)
                    .ok_or_else(|| {
                        daemon_dfs_stale_authority(
                            &metadata_authority_generation,
                            signature.metadata_authority_generation,
                        )
                    })?,
            ),
            resident_metadata_backend.map(DaemonDfsResidentMetadataScope::Global),
            cross_shard_namespace_locks,
        )?)
        .map_err(|_| sinomo_dfs::DfsError::Network {
            message: "newly constructed DFS mutation lane was unexpectedly shared".into(),
        })?,
    };
    let (lane, detached_send_runtime) = handle.into_parts();
    let mut lane = lane
        .into_inner()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    lane.refresh_peer_targets(peer_node_ids.clone());
    lane.service.write_intent_maintenance_scan = write_intent_maintenance_scan;
    lane.service.maintenance_progress = maintenance_progress;
    lane.service.maintenance_progress.bind_native_generation(
        signature.metadata_authority_generation,
        signature.metadata_storage_generation,
    )?;
    if let Some(scope) = lane.resident_metadata_scope.clone() {
        scope.attach(&mut lane.service)?;
    }
    daemon_dfs_require_retained_authority(
        &metadata_authority_generation,
        signature.metadata_authority_generation,
    )?;
    Ok(DaemonDfsMaintenanceServiceLane {
        signature,
        metadata_authority_generation,
        peer_node_ids,
        lane,
        detached_send_runtime,
    })
}

pub(crate) fn restore_daemon_dfs_message_mutation_service_after_maintenance(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    mut retained: DaemonDfsMaintenanceServiceLane,
) -> Result<bool, sinomo_dfs::DfsError> {
    let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(cluster_paths)? else {
        return Ok(false);
    };
    let metadata_authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        Some(runtime),
    )?;
    let (authority_generation, storage_generation) =
        metadata_authority_generation.lifecycle_generations()?;
    let current_signature = DaemonDfsMessageServiceSignature {
        cluster_id: peer_snapshot.cluster_id,
        local_node_id: peer_snapshot.local_node_id,
        metadata_authority_generation: authority_generation,
        metadata_storage_generation: storage_generation,
    };
    if !current_signature.shares_metadata_storage(&retained.signature)
        || !Arc::ptr_eq(
            &metadata_authority_generation,
            &retained.metadata_authority_generation,
        )
    {
        return Ok(false);
    }
    if retained
        .lane
        .service
        .authoritative_write_frozen_shard_count()?
        != 0
    {
        return Ok(false);
    }
    retained
        .lane
        .refresh_peer_targets(peer_snapshot.peer_node_ids.clone());
    retained
        .peer_node_ids
        .clone_from(&peer_snapshot.peer_node_ids);
    if retained.lane.resident_metadata_scope.is_some() {
        retained.lane.service.release_resident_metadata_backend()?;
    }

    let Some(slot) = daemon_dfs_message_service_cache().existing_root(&data_dir.root())? else {
        return Ok(false);
    };
    let mut cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    let Some(entry) = cache.as_mut() else {
        return Ok(false);
    };
    if !entry.signature.shares_metadata_storage(&retained.signature)
        || !Arc::ptr_eq(
            &entry.metadata_authority_generation,
            &retained.metadata_authority_generation,
        )
        || entry.primary_service.is_some()
    {
        return Ok(false);
    }
    entry.signature = current_signature;
    entry.peer_node_ids.clone_from(&retained.peer_node_ids);
    entry.primary_service = Some(Arc::new(DaemonDfsMessageServiceLaneHandle::new(
        retained.lane,
        retained.detached_send_runtime,
    )));
    Ok(true)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn release_daemon_dfs_idle_allocator_pages(_data_dir: &SinomoDataDir) -> bool {
    unsafe extern "C" {
        fn malloc_trim(pad: usize) -> std::ffi::c_int;
    }

    // Broad maintenance temporarily materializes all locally configured
    // metadata shards. glibc's per-thread arenas otherwise retain those freed
    // pages indefinitely, leaving an idle daemon with the peak RSS of the last
    // maintenance pass.
    let trimmed = unsafe { malloc_trim(0) != 0 };
    #[cfg(test)]
    record_daemon_dfs_allocator_trim_completion(_data_dir);
    trimmed
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn release_daemon_dfs_idle_allocator_pages(_data_dir: &SinomoDataDir) -> bool {
    #[cfg(test)]
    record_daemon_dfs_allocator_trim_completion(_data_dir);
    false
}

/// Release the process-global cached lane during main-daemon shutdown. Active
/// blocking handlers retain both their lane Arc and an explicit data-owner Arc,
/// so eviction drops only idle cache ownership and cannot shorten live work.
pub(crate) fn evict_daemon_dfs_message_service_cache_on_shutdown(
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    evict_daemon_dfs_message_service_cache(&SinomoDataDir::new(daemon_data_dir(paths)?))
}

pub(crate) fn daemon_dfs_mutation_scheduler(
    data_dir: &SinomoDataDir,
) -> Result<Arc<DaemonDfsMutationScheduler>, DaemonError> {
    DAEMON_DFS_MUTATION_SCHEDULERS
        .get_or_init(DfsMutationSchedulerRegistry::default)
        .scheduler(data_dir.root())
        .map_err(Into::into)
}

pub(crate) async fn admit_daemon_dfs_foreground_message(
    scheduler: &Arc<DaemonDfsMutationScheduler>,
    execution_scope: &DfsMessageExecutionScope,
) -> Result<DaemonDfsMutationAdmission, DaemonError> {
    admit_daemon_dfs_message(scheduler, execution_scope, true).await
}

pub(super) fn daemon_dfs_message_preempts_maintenance(
    message: &sinomo_dfs::DfsNetworkMessage,
) -> bool {
    // Placement polls and detached post-commit notifications must not cancel
    // the repairs they are waiting for. CommitVersion is sent only after the
    // write's commit boundary; no foreground durability barrier awaits it.
    // Scope, admission and all live authority/content checks remain unchanged.
    match message {
        sinomo_dfs::DfsNetworkMessage::CommitVersion(_) => false,
        sinomo_dfs::DfsNetworkMessage::RepairReplica(command) => !command.background_repair,
        sinomo_dfs::DfsNetworkMessage::NamespaceRequest(command)
            if matches!(
                command.operation,
                sinomo_dfs::DfsNamespaceOperation::ReconcilePlacement { .. }
            ) =>
        {
            false
        }
        _ => true,
    }
}

pub(crate) async fn admit_daemon_dfs_message(
    scheduler: &Arc<DaemonDfsMutationScheduler>,
    execution_scope: &DfsMessageExecutionScope,
    preempt_maintenance: bool,
) -> Result<DaemonDfsMutationAdmission, DaemonError> {
    if preempt_maintenance && !matches!(execution_scope, DfsMessageExecutionScope::ReadOnly) {
        scheduler.request_foreground_admission();
    }
    scheduler.admit(execution_scope).await.map_err(Into::into)
}

pub(crate) fn daemon_dfs_metadata_publish_lane_scheduler(
    data_dir: &SinomoDataDir,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Result<Arc<sinomo_dfs::DfsMetadataPublishScheduler>, DaemonError> {
    daemon_dfs_keyed_metadata_scheduler(
        &DAEMON_DFS_METADATA_PUBLISH_SCHEDULERS,
        "DFS metadata publish scheduler",
        data_dir,
        file_system_id,
        shard_id,
    )
}

pub(crate) fn daemon_dfs_metadata_materialization_lane_scheduler(
    data_dir: &SinomoDataDir,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Result<Arc<sinomo_dfs::DfsMetadataPublishScheduler>, DaemonError> {
    daemon_dfs_keyed_metadata_scheduler(
        &DAEMON_DFS_METADATA_MATERIALIZATION_SCHEDULERS,
        "DFS metadata materialization scheduler",
        data_dir,
        file_system_id,
        shard_id,
    )
}

pub(crate) fn daemon_dfs_allocator_cleanup_scheduler(
    data_dir: &SinomoDataDir,
) -> Result<Arc<sinomo_dfs::DfsMetadataPublishScheduler>, DaemonError> {
    DAEMON_DFS_ALLOCATOR_CLEANUP_SCHEDULERS
        .get_or_init(|| DfsMetadataSchedulerRegistry::new("DFS allocator cleanup scheduler"))
        .scheduler(data_dir.root())
        .map_err(Into::into)
}

pub(crate) fn daemon_dfs_keyed_metadata_scheduler(
    registry: &'static OnceLock<DfsMetadataSchedulerRegistry<DaemonDfsMetadataPublishLaneKey>>,
    label: &'static str,
    data_dir: &SinomoDataDir,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
) -> Result<Arc<sinomo_dfs::DfsMetadataPublishScheduler>, DaemonError> {
    registry
        .get_or_init(|| DfsMetadataSchedulerRegistry::new(label))
        .scheduler(DaemonDfsMetadataPublishLaneKey::new(
            data_dir.root(),
            file_system_id.clone(),
            shard_id.clone(),
        ))
        .map_err(Into::into)
}

pub(crate) fn daemon_dfs_metadata_publish_discovery_scheduler(
    data_dir: &SinomoDataDir,
) -> Result<Arc<sinomo_dfs::DfsMetadataPublishScheduler>, DaemonError> {
    DAEMON_DFS_METADATA_PUBLISH_DISCOVERY_SCHEDULERS
        .get_or_init(|| {
            DfsMetadataSchedulerRegistry::new("DFS metadata publish discovery scheduler")
        })
        .scheduler(data_dir.root())
        .map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn record_daemon_dfs_metadata_publish_discovery_attempt(
    data_root: &std::path::Path,
    success: bool,
) {
    if let Ok(mut attempts) = DAEMON_DFS_TEST_METADATA_PUBLISH_DISCOVERY_ATTEMPTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let counts = attempts.entry(data_root.to_path_buf()).or_default();
        counts.0 = counts.0.saturating_add(1);
        if success {
            counts.1 = counts.1.saturating_add(1);
        }
    }
}

#[cfg(test)]
pub(crate) fn daemon_dfs_metadata_publish_discovery_attempts(
    data_root: &std::path::Path,
) -> (usize, usize) {
    DAEMON_DFS_TEST_METADATA_PUBLISH_DISCOVERY_ATTEMPTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|attempts| attempts.get(data_root).copied())
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn daemon_dfs_metadata_publish_discovery_is_active(data_root: &std::path::Path) -> bool {
    DAEMON_DFS_METADATA_PUBLISH_DISCOVERY_SCHEDULERS
        .get()
        .is_some_and(|schedulers| schedulers.contains_live(&data_root.to_path_buf()))
}

#[cfg(test)]
pub(crate) fn record_daemon_dfs_metadata_publish_lane_request(
    data_dir: &SinomoDataDir,
    lane: &DaemonDfsMutationLaneKey,
) {
    if let Ok(mut requests) = DAEMON_DFS_TEST_METADATA_PUBLISH_LANE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let key = DaemonDfsMetadataPublishLaneKey {
            data_root: data_dir.root(),
            file_system_id: lane.file_system_id.clone(),
            shard_id: lane.shard_id.clone(),
        };
        let count = requests.entry(key).or_default();
        *count = count.saturating_add(1);
    }
}

#[cfg(test)]
pub(crate) fn daemon_dfs_metadata_publish_lane_request_count(
    data_dir: &SinomoDataDir,
    lane: &DaemonDfsMutationLaneKey,
) -> usize {
    let key = DaemonDfsMetadataPublishLaneKey {
        data_root: data_dir.root(),
        file_system_id: lane.file_system_id.clone(),
        shard_id: lane.shard_id.clone(),
    };
    DAEMON_DFS_TEST_METADATA_PUBLISH_LANE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|requests| requests.get(&key).copied())
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn record_daemon_dfs_repair_maintenance_edge_request(data_dir: &SinomoDataDir) {
    if let Ok(mut requests) = DAEMON_DFS_TEST_REPAIR_MAINTENANCE_EDGE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        let count = requests.entry(data_dir.root()).or_default();
        *count = count.saturating_add(1);
    }
}

#[cfg(test)]
pub(crate) fn daemon_dfs_repair_maintenance_edge_request_count(data_dir: &SinomoDataDir) -> usize {
    DAEMON_DFS_TEST_REPAIR_MAINTENANCE_EDGE_REQUESTS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|requests| requests.get(&data_dir.root()).copied())
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn install_daemon_dfs_message_committed_edge_test_hook(
    request_id: &str,
) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    DAEMON_DFS_TEST_MESSAGE_COMMITTED_EDGE_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .expect("message committed-edge test-hook lock")
        .insert(
            request_id.to_string(),
            Arc::new(DaemonDfsMessageCommittedEdgeTestHook {
                started_tx,
                release_rx: Mutex::new(release_rx),
            }),
        );
    (started_rx, release_tx)
}

#[cfg(test)]
pub(crate) fn wait_at_daemon_dfs_message_committed_edge_test_hook(request_id: &str) {
    let hook = DAEMON_DFS_TEST_MESSAGE_COMMITTED_EDGE_HOOKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()
        .and_then(|mut hooks| hooks.remove(request_id));
    let Some(hook) = hook else {
        return;
    };
    let _ = hook.started_tx.send(());
    let _ = hook
        .release_rx
        .lock()
        .ok()
        .and_then(|release_rx| release_rx.recv_timeout(Duration::from_secs(5)).ok());
}

pub(crate) fn daemon_default_dfs_lifecycle_gate(
    data_dir: &std::path::Path,
) -> Result<Arc<tokio::sync::Mutex<()>>, DaemonError> {
    let key = data_dir.to_path_buf();
    let mut gates = DAEMON_DEFAULT_DFS_LIFECYCLE_GATES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DaemonError::Network {
            address: "default DFS lifecycle".to_string(),
            detail: "default DFS lifecycle gate map is poisoned".to_string(),
        })?;
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let gate = Arc::new(tokio::sync::Mutex::new(()));
    gates.insert(key, Arc::downgrade(&gate));
    Ok(gate)
}

pub(crate) fn daemon_dfs_cache_lock_error() -> sinomo_dfs::DfsError {
    sinomo_dfs::DfsError::Network {
        message: "daemon DFS message service cache lock poisoned".to_string(),
    }
}

pub(crate) fn daemon_dfs_detached_send_runtime(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
) -> Result<Arc<DaemonDfsDetachedSendRuntime>, sinomo_dfs::DfsError> {
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(daemon_dfs_network_error)?;
    daemon_dfs_detached_send_runtime_for_identity(
        data_dir,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )
}

pub(crate) fn daemon_dfs_detached_send_runtime_for_identity(
    data_dir: &SinomoDataDir,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
) -> Result<Arc<DaemonDfsDetachedSendRuntime>, sinomo_dfs::DfsError> {
    sinomo_dfs::dfs_detached_send_runtime_for_identity(&data_dir.root(), cluster_id, local_node_id)
}

pub(crate) fn daemon_dfs_network_error(
    source: sinomo_core::network::NetworkError,
) -> sinomo_dfs::DfsError {
    sinomo_dfs::DfsError::Network {
        message: source.to_string(),
    }
}

fn try_open_daemon_dfs_resident_metadata_backend_for_authority_generation(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    authority_generation: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    expected_generation: u64,
    retained_backend: Option<ResidentLocalShardedMetadataBackend>,
) -> Result<Option<ResidentLocalShardedMetadataBackend>, DaemonDfsAuthorityPreExecutionError> {
    let projection = authority_generation
        .load_complete_prefix_for_generation(expected_generation)
        .ok_or_else(|| daemon_dfs_stale_authority(authority_generation, expected_generation))?;
    try_open_daemon_dfs_resident_metadata_backend_from_authority_projection(
        data_dir,
        local_node_id,
        authority_generation,
        expected_generation,
        &projection,
        retained_backend,
    )
}

fn try_open_daemon_dfs_resident_metadata_backend_from_authority_projection(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    authority_generation: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    expected_generation: u64,
    projection: &sinomo_dfs::DfsAuthoritativePrefixProjection,
    retained_backend: Option<ResidentLocalShardedMetadataBackend>,
) -> Result<Option<ResidentLocalShardedMetadataBackend>, DaemonDfsAuthorityPreExecutionError> {
    daemon_dfs_require_retained_authority(authority_generation, expected_generation)?;
    let shards = daemon_dfs_metadata_shards_from_authority_projection(
        projection,
        &sinomo_dfs::sifs_file_system_id(),
    )?;
    if shards.is_empty() {
        return Ok(None);
    }
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::BeforeResidentBackendOpen,
            None,
            None,
        )?;
    }
    let metadata_root = sinomo_dfs::default_dfs_metadata_shards_root(data_dir);
    let backend = match retained_backend {
        Some(previous) => {
            previous.into_reconfigured_for_replica_repair(metadata_root, shards, local_node_id)?
        }
        None => sinomo_dfs::open_local_sharded_metadata_backend_for_replica_repair(
            metadata_root,
            shards,
            local_node_id,
        )?
        .into_resident(),
    };
    backend.initialize_bounded_status_snapshots()?;
    daemon_dfs_require_retained_authority(authority_generation, expected_generation)?;
    Ok(Some(backend))
}

pub(crate) fn daemon_dfs_metadata_authority_generations()
-> &'static sinomo_dfs::DfsMetadataAuthorityRuntimeRegistry<DaemonDfsMetadataAuthorityGenerationKey>
{
    DAEMON_DFS_METADATA_AUTHORITY_GENERATIONS.get_or_init(|| {
        sinomo_dfs::DfsMetadataAuthorityRuntimeRegistry::new(
            "daemon DFS metadata-authority generation registry",
        )
    })
}

#[cfg(test)]
pub(crate) fn daemon_dfs_metadata_authority_generation(
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<Arc<DaemonDfsMetadataAuthorityGeneration>, sinomo_dfs::DfsError> {
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(daemon_dfs_network_error)?;
    let generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        Some(runtime),
    )?;
    let _ = generation.current_complete_prefix_revision();
    Ok(generation)
}

pub(crate) fn daemon_dfs_metadata_authority_generation_for_identity(
    cluster_paths: &ClusterPaths,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<Arc<DaemonDfsMetadataAuthorityGeneration>, sinomo_dfs::DfsError> {
    let key = DaemonDfsMetadataAuthorityGenerationKey {
        cluster_root: cluster_paths.root.clone(),
        cluster_id: cluster_id.to_string(),
        local_node_id: local_node_id.to_string(),
    };
    let (generation, created) = daemon_dfs_metadata_authority_generations().runtime(key)?;
    #[cfg(any(test, feature = "test-support"))]
    if let (Some(root), Some(owner)) = (
        cluster_paths.root.parent(),
        DaemonDfsControlPlaneKvClient::discover_data_owner_lock(cluster_paths),
    ) {
        // Bind the existing mounted owner before any caller can warm this
        // generation. Observation retains the same read; it schedules no read.
        generation.bind_native_observation_root(root.to_path_buf(), &owner);
    }
    if let Some(host) = daemon_dfs_metadata_authority_host(cluster_paths, runtime) {
        let _ = generation.attach_host(host);
    }
    if !created {
        return Ok(generation);
    }
    let observation_generation = Arc::downgrade(&generation);
    let observation_subscription =
        sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
            cluster_paths,
            move |event| {
                // Derived checkpoints cannot grant or retire a native proof.
                // Canonical effects and their uncertain outcomes carry the
                // actual native applied index from the owning Raft scope.
                if event.applied_index.is_none() {
                    return;
                }
                let Some(generation) = observation_generation.upgrade() else {
                    return;
                };
                match &event.value {
                    sinomo_core::kv::KvCommittedObservationValue::Delta(delta) => {
                        #[cfg(any(test, feature = "test-support"))]
                        if crate::test_runtime::withhold_native_configuration_delta(
                            generation.as_ref(),
                            delta,
                        ) {
                            return;
                        }
                        let observation = generation.observe_delta_observation(delta);
                        if !observation.accepted {
                            if observation.proof_withdrawn {
                                tracing::warn!(
                                    target: "sinomo.dfs.metadata_authority",
                                    revision = delta.revision.0,
                                    "DFS metadata-authority committed delta withdrew exact admission; scheduling one bounded authoritative refresh"
                                );
                            }
                            generation.request_authoritative_refresh();
                        }
                    }
                    sinomo_core::kv::KvCommittedObservationValue::Snapshot(snapshot) => {
                        if event.kind == sinomo_core::kv::KvCommittedObservationKind::Replacement {
                            let observation = generation.observe_discontinuity(snapshot.revision);
                            if observation.proof_withdrawn {
                                tracing::warn!(
                                    target: "sinomo.dfs.metadata_authority",
                                    revision = snapshot.revision.0,
                                    "DFS metadata-authority canonical replacement withdrew exact admission; scheduling one bounded authoritative refresh"
                                );
                            }
                        }
                        generation.observe_snapshot(snapshot);
                    }
                    sinomo_core::kv::KvCommittedObservationValue::Discontinuity(cause) => {
                        let observation = generation
                            .observe_discontinuity_hint(event.applied_index.and(event.revision));
                        if observation.proof_withdrawn {
                            tracing::warn!(
                                target: "sinomo.dfs.metadata_authority",
                                revision = ?event.revision,
                                applied_index = ?event.applied_index,
                                discontinuity = ?cause,
                                "DFS metadata-authority canonical observation became unavailable; withdrawing exact admission and scheduling one bounded authoritative refresh"
                            );
                        }
                    }
                }
            },
        );
    if generation
        .attach_observation_subscription(observation_subscription)
        .is_err()
    {
        return Err(sinomo_dfs::DfsError::Network {
            message:
                "new DFS metadata-authority generation already had a committed observation subscription"
                    .to_string(),
        });
    }
    Ok(generation)
}

pub(crate) fn daemon_dfs_retained_metadata_authority_generation(
    detached_send_runtime: &DaemonDfsDetachedSendRuntime,
    cluster_paths: &ClusterPaths,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
    runtime: Option<&tokio::runtime::Handle>,
) -> Result<Arc<DaemonDfsMetadataAuthorityGeneration>, sinomo_dfs::DfsError> {
    if let Some(generation) = detached_send_runtime.metadata_authority_generation() {
        if let Some(host) = daemon_dfs_metadata_authority_host(cluster_paths, runtime) {
            let _ = generation.attach_host(host);
        }
        let _ = generation.current_complete_prefix_revision();
        return Ok(generation);
    }
    let generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        cluster_id,
        local_node_id,
        runtime,
    )?;
    Ok(detached_send_runtime.retain_metadata_authority_generation(generation))
}

pub(crate) fn daemon_dfs_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "daemon dfs perf");
}

pub(crate) fn daemon_dfs_perf_trace_with_traceparent(
    traceparent: Option<&str>,
    args: std::fmt::Arguments<'_>,
) {
    tracing::trace!(
        target: "sinomo.dfs.perf",
        traceparent = %traceparent.unwrap_or_default(),
        detail = %args,
        "daemon dfs perf"
    );
}

/// Read peer routing from the registered native owner's already-published
/// local membership projection. This reader supplies configuration DATA only;
/// publication independently obtains and validates its actual authority guard.
/// sinomo:kv-configuration: Consume the current native owner's local membership projection for peer routing without acquiring publication authority.
pub(crate) fn read_daemon_dfs_message_peer_snapshot(
    cluster_paths: &ClusterPaths,
) -> Result<Option<DaemonDfsMessagePeerSnapshot>, sinomo_dfs::DfsError> {
    let state =
        match sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(cluster_paths) {
            Ok(state) => state,
            Err(sinomo_core::network::NetworkError::ClusterOrphan) => return Ok(None),
            Err(error) => return Err(sinomo_dfs::DfsError::from_network(error)),
        };
    let data_root = cluster_paths
        .root
        .parent()
        .ok_or_else(|| sinomo_dfs::DfsError::Metadata {
            message: "DFS peer projection has no owning data directory".into(),
        })?;
    let host = registered_dfs_runtime_host(&SinomoDataDir::new(data_root))
        .map_err(|error| sinomo_dfs::DfsError::Network {
            message: error.to_string(),
        })?
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: "DFS peer projection requires its registered native owner".into(),
        })?;
    if host.shutting_down.is_cancelled() {
        return Ok(None);
    }
    let (members, services) = read_native_dfs_peer_configuration(
        cluster_paths,
        &state,
        Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
    )?;
    if host.shutting_down.is_cancelled() {
        return Ok(None);
    }
    peer_snapshot_from_configuration_entries(&state, members, services).map(Some)
}

/// sinomo:kv-configuration: Borrow both routing scopes from the native owner's same applied projection; this grants no publication authority.
pub(super) fn read_native_dfs_peer_configuration(
    cluster_paths: &ClusterPaths,
    state: &sinomo_core::ClusterBootstrapState,
    deadline: Instant,
) -> Result<(Vec<sinomo_core::kv::KvEntry>, Vec<sinomo_core::kv::KvEntry>), sinomo_dfs::DfsError> {
    let wal_paths = cluster_paths
        .kv_paths()
        .raft_wal_paths(&state.local_node_id);
    let read = sinomo_core::kv::with_cluster_kv_resident_read_projection_before(
        &wal_paths,
        &state.local_node_id,
        deadline,
        |projection| {
            let members = projection.read_index_prefix_limited_by_payload_bytes(
                "/cluster/members/",
                sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
            )?;
            let services = projection.read_index_prefix_limited_by_payload_bytes(
                sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX,
                sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
            )?;
            Ok::<_, KvError>((members, services))
        },
    )
    .ok_or_else(|| KvError::Busy {
        path: wal_paths.root.display().to_string(),
    })??;
    read.value.map_err(Into::into)
}

fn peer_snapshot_from_configuration_entries(
    state: &sinomo_core::ClusterBootstrapState,
    members: Vec<sinomo_core::kv::KvEntry>,
    services: Vec<sinomo_core::kv::KvEntry>,
) -> Result<DaemonDfsMessagePeerSnapshot, sinomo_dfs::DfsError> {
    let members = members
        .into_iter()
        .map(|entry| entry.decode::<sinomo_core::ClusterMemberRecord>())
        .collect::<Result<Vec<_>, _>>()?;
    let mut peers = crate::cluster_service::dfs_provider_node_ids_from_current_entries(
        state, &members, &services,
    )?;
    if !peers.contains(&state.local_node_id) {
        peers.push(state.local_node_id.clone());
        peers.sort();
    }
    Ok(DaemonDfsMessagePeerSnapshot {
        cluster_id: state.cluster_id.clone(),
        local_node_id: state.local_node_id.clone(),
        peer_node_ids: peers,
    })
}

/// Join only the actual native owner's routing projection before DFS admission.
/// Both bounded prefixes come from one resident borrow. They grant no publication
/// authority; the same retained authority preflight follows before admission.
/// sinomo:kv-configuration: Read bounded peer membership and service routing from one canonical resident projection without opening, replaying, or falling back to a snapshot.
pub(crate) async fn read_daemon_dfs_message_peer_snapshot_before(
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    deadline: tokio::time::Instant,
) -> Result<Option<DaemonDfsMessagePeerSnapshot>, sinomo_dfs::DfsError> {
    let state =
        match sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(cluster_paths) {
            Ok(state) => state,
            Err(sinomo_core::network::NetworkError::ClusterOrphan) => return Ok(None),
            Err(error) => return Err(sinomo_dfs::DfsError::from_network(error)),
        };
    let data_root = cluster_paths
        .root
        .parent()
        .ok_or_else(|| sinomo_dfs::DfsError::Metadata {
            message: "DFS peer projection has no owning data directory".into(),
        })?;
    let data_dir = SinomoDataDir::new(data_root);
    let host = registered_dfs_runtime_host(&data_dir)
        .map_err(|error| sinomo_dfs::DfsError::Network {
            message: error.to_string(),
        })?
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: "DFS peer projection requires its registered native owner".into(),
        })?;
    if host.shutting_down.is_cancelled() {
        return Ok(None);
    }
    let owner =
        require_daemon_dfs_data_owner_lock(&DaemonPaths::new(&data_dir)).map_err(|error| {
            sinomo_dfs::DfsError::Network {
                message: error.to_string(),
            }
        })?;
    let peer_paths = cluster_paths.clone();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let task_owner = Arc::clone(&host);
    // Register the SAME blocking task before awaiting. A canceled caller must
    // not detach native/data-owner custody from the component's ordinary drain.
    let task = runtime.spawn_blocking(move || {
        let result = (|| -> Result<Option<DaemonDfsMessagePeerSnapshot>, sinomo_dfs::DfsError> {
            let (members, services) =
                read_native_dfs_peer_configuration(&peer_paths, &state, deadline.into_std())?;
            if host.shutting_down.is_cancelled()
                || !registered_dfs_runtime_host(&data_dir)
                    .map_err(|error| sinomo_dfs::DfsError::Network {
                        message: error.to_string(),
                    })?
                    .is_some_and(|current| Arc::ptr_eq(&current, &host))
            {
                return Ok(None);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(KvError::Busy {
                    path: peer_paths
                        .kv_paths()
                        .raft_wal_paths(&state.local_node_id)
                        .root
                        .display()
                        .to_string(),
                }
                .into());
            }
            peer_snapshot_from_configuration_entries(&state, members, services).map(Some)
        })();
        drop(owner);
        drop(host);
        let _ = result_tx.send(result);
    });
    task_owner.own_task(task);
    result_rx
        .await
        .map_err(|error| sinomo_dfs::DfsError::Network {
            message: format!("DFS native peer projection task did not return: {error}"),
        })?
}

pub(crate) fn schedule_daemon_dfs_message_deferred_work(
    tokio_runtime: &tokio::runtime::Handle,
    cluster_paths: &ClusterPaths,
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    mut worker: DfsNodeDetachedSendWorker,
) {
    #[cfg(test)]
    if let Some(data_root) = cluster_paths.root.parent() {
        crate::test_runtime::observe_checkpointed_background_repairs(
            &SinomoDataDir::new(data_root),
            &worker,
        );
    }
    if let Some(data_root) = cluster_paths.root.parent() {
        let data_dir = SinomoDataDir::new(data_root);
        let paths = DaemonPaths::new(&data_dir);
        worker.set_background_delivery_handler(move |event| {
            crate::runtime_message::checkpoint_detached_replica_delivery(&data_dir, event)
        });
        let response_runtime = tokio_runtime.clone();
        worker.set_background_response_handler(move |message, original| {
            let request_id = match &message.message {
                sinomo_dfs::DfsNetworkMessage::RepairReplicaResponse(response) => {
                    format!("dfs_background_repair_{}", response.request_id)
                }
                sinomo_dfs::DfsNetworkMessage::FetchBlobResponse(response) => {
                    format!("dfs_background_fetch_{}", response.request_id)
                }
                _ => {
                    return Err(sinomo_dfs::DfsError::Network {
                        message: "background dispatch received an unmatched response family".into(),
                    });
                }
            };
            let command = DfsMessageCommand {
                request_id,
                routed_message: message,
            };
            handle_daemon_dfs_message_on_runtime_with_background_response(
                &paths,
                command,
                &response_runtime,
                original,
            )
            .map(|_| ())
            .map_err(|error| sinomo_dfs::DfsError::Network {
                message: error.to_string(),
            })
        });
    }
    let deferred_target_node_ids =
        sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
            .map(|state| daemon_dfs_offline_node_ids(cluster_paths, &state))
            // Unknown/unreadable cluster state remains fail-open. Durable
            // detached work must never be discarded because a local status
            // projection is temporarily unavailable.
            .unwrap_or_default();
    schedule_daemon_dfs_message_deferred_work_with_policy_and_owner(
        tokio_runtime,
        runtime,
        worker,
        deferred_target_node_ids,
        dfs_runtime_host_for_cluster_paths(cluster_paths).ok(),
    );
}

#[cfg(test)]
pub(crate) fn schedule_daemon_dfs_message_deferred_work_with_policy(
    tokio_runtime: &tokio::runtime::Handle,
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    worker: DfsNodeDetachedSendWorker,
    deferred_target_node_ids: BTreeSet<NodeId>,
) {
    schedule_daemon_dfs_message_deferred_work_with_policy_and_owner(
        tokio_runtime,
        runtime,
        worker,
        deferred_target_node_ids,
        None,
    );
}

fn schedule_daemon_dfs_message_deferred_work_with_policy_and_owner(
    tokio_runtime: &tokio::runtime::Handle,
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    worker: DfsNodeDetachedSendWorker,
    deferred_target_node_ids: BTreeSet<NodeId>,
    task_owner: Option<Arc<DfsRuntimeHost>>,
) {
    runtime.schedule_deferred_work(
        worker,
        deferred_target_node_ids,
        DAEMON_DFS_PROMOTION_RETRY_DELAY,
        move |task| {
            let task = tokio_runtime.spawn(task);
            if let Some(owner) = task_owner.as_ref() {
                owner.own_task(task);
            }
        },
    );
}

#[cfg(test)]
pub(crate) fn open_daemon_dfs_message_service_lane(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &sinomo_core::node::NodeId,
    peer_node_ids: Vec<NodeId>,
    runtime: &tokio::runtime::Handle,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, sinomo_dfs::DfsError> {
    let content = sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
    open_daemon_dfs_message_service_lane_with_content(
        data_dir,
        cluster_paths,
        local_node_id,
        peer_node_ids,
        runtime,
        content,
        None,
        None,
        Default::default(),
    )
}

pub(crate) fn open_daemon_dfs_message_service_lane_with_content(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &sinomo_core::node::NodeId,
    peer_node_ids: Vec<NodeId>,
    runtime: &tokio::runtime::Handle,
    content: sinomo_dfs::LocalContentStore,
    authoritative_prefix: Option<sinomo_dfs::DfsAuthoritativePrefixProjection>,
    resident_metadata_scope: Option<DaemonDfsResidentMetadataScope>,
    cross_shard_namespace_locks: DfsCrossShardNamespaceLockRegistry,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, sinomo_dfs::DfsError> {
    let metadata_backend = if resident_metadata_scope.is_none() {
        Some(sinomo_dfs::open_cluster_aware_dfs_metadata_backend(
            data_dir,
            cluster_paths,
        )?)
    } else {
        None
    };
    #[cfg(test)]
    record_daemon_dfs_test_message_service_lane_open(data_dir);
    let detached_send_runtime = daemon_dfs_detached_send_runtime(data_dir, cluster_paths)?;
    let (peer_transport, async_peer_transport) =
        daemon_dfs_peer_transports_on_runtime(data_dir, runtime);
    let transport = DfsNodeTransport::new_with_peer_transports_for_nodes_and_detached_state(
        local_node_id.clone(),
        peer_node_ids.clone(),
        peer_transport,
        async_peer_transport,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        detached_send_runtime.detached_state(),
    );
    let service = with_daemon_dfs_control_plane_seeded_on_runtime(
        NetworkedDfsService::new(transport, content)
            .with_cross_shard_namespace_lock_registry(cross_shard_namespace_locks),
        cluster_paths,
        Some(runtime),
        authoritative_prefix,
    );
    let mut service = match resident_metadata_scope.as_ref() {
        Some(scope) => match scope {
            DaemonDfsResidentMetadataScope::ExactShard(shard) => {
                service.with_resident_metadata_shard_backend_lazy(shard.checkout()?)?
            }
            DaemonDfsResidentMetadataScope::Global(backend) => {
                service.with_resident_sharded_metadata_backend_lazy(backend.checkout_global()?)?
            }
        },
        None => {
            // Mutation lanes retain the filesystem catalog and durable namespace
            // guards without hydrating the complete metadata projection.
            service.with_boxed_metadata_backend_lazy(
                metadata_backend.expect("owned mutation lane opened a metadata backend"),
            )?
        }
    };
    // Synchronous quorum/handoff loops cannot observe Tokio task abortion.
    // Link the real host's stop signal once when constructing this lane so
    // their existing cooperative checks release admission during shutdown.
    if let Some(host) =
        registered_dfs_runtime_host(data_dir).map_err(|error| sinomo_dfs::DfsError::Network {
            message: error.to_string(),
        })?
    {
        service.add_cancellation(host.shutting_down.clone());
    }
    if resident_metadata_scope.is_some() {
        service.release_resident_metadata_backend()?;
    }
    Ok(Arc::new(DaemonDfsMessageServiceLaneHandle::new(
        DaemonDfsMessageServiceLane {
            service,
            peer_node_ids,
            resident_metadata_scope,
        },
        detached_send_runtime,
    )))
}

pub(crate) fn open_daemon_dfs_message_content_read_lane_with_content(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &sinomo_core::node::NodeId,
    peer_node_ids: Vec<NodeId>,
    runtime: &tokio::runtime::Handle,
    content: sinomo_dfs::LocalContentStore,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, sinomo_dfs::DfsError> {
    let detached_send_runtime = daemon_dfs_detached_send_runtime(data_dir, cluster_paths)?;
    let (peer_transport, async_peer_transport) =
        daemon_dfs_peer_transports_on_runtime(data_dir, runtime);
    let transport = DfsNodeTransport::new_with_peer_transports_for_nodes_and_detached_state(
        local_node_id.clone(),
        peer_node_ids.clone(),
        peer_transport,
        async_peer_transport,
        sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        detached_send_runtime.detached_state(),
    );
    Ok(Arc::new(DaemonDfsMessageServiceLaneHandle::new(
        DaemonDfsMessageServiceLane {
            service: NetworkedDfsService::new(transport, content),
            peer_node_ids,
            resident_metadata_scope: None,
        },
        detached_send_runtime,
    )))
}

pub(crate) struct DaemonDfsCommitPromotionRecovery {
    pub(super) detached_runtime: Arc<DaemonDfsDetachedSendRuntime>,
    pub(super) worker: DfsNodeDetachedSendWorker,
    pub(super) scan_result: Result<(), sinomo_dfs::DfsError>,
}

/// Rebuild the process-local promotion outbox after process startup or a
/// metadata storage/assignment transition, then prepare the detached worker.
/// Configuration policies and proof publication do not create another scan.
///
/// This uses a dedicated service that shares only detached identity state with
/// cached message lanes. The finite metadata scan runs under lifecycle
/// maintenance, but promotion RPCs are handed to the process-wide detached
/// worker instead of being drained while the cluster maintenance lease is held.
/// An offline replica can therefore delay only its own convergence; it cannot
/// fence another node out of unrelated cluster maintenance. An empty committed
/// shard topology is transient bootstrap state, so it leaves recovery
/// incomplete for the next maintenance tick.
pub(crate) fn prepare_daemon_dfs_commit_promotion_recovery(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsCommitPromotionRecovery, sinomo_dfs::DfsError> {
    let cluster_state = sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
        .map_err(daemon_dfs_network_error)?;
    let detached_send_runtime = daemon_dfs_detached_send_runtime_for_identity(
        data_dir,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
    )?;
    let file_system = sinomo_dfs::distributed_sifs(cluster_state.local_node_id.clone());
    let authority = daemon_dfs_retained_metadata_authority_generation(
        detached_send_runtime.as_ref(),
        cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        Some(runtime),
    )?;
    let (authority_generation, storage_generation) = authority.lifecycle_generations()?;
    let recovery_scan_required =
        detached_send_runtime.recovery_scan_generation() != storage_generation;
    let control_plane = daemon_dfs_control_plane_client_on_runtime(cluster_paths, Some(runtime))
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: "DFS promotion recovery has no registered configuration client".into(),
        })?;
    let peer_node_ids =
        registered_dfs_runtime_peer_node_ids(control_plane.as_ref(), &cluster_state).map_err(
            |error| sinomo_dfs::DfsError::Network {
                message: error.to_string(),
            },
        )?;
    let open_recovery_service = || {
        let content = sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
        let (peer_transport, async_peer_transport) =
            daemon_dfs_peer_transports_on_runtime(data_dir, runtime);
        let transport = DfsNodeTransport::new_with_peer_transports_for_nodes_and_detached_state(
            cluster_state.local_node_id.clone(),
            peer_node_ids.clone(),
            peer_transport,
            async_peer_transport,
            sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            detached_send_runtime.detached_state(),
        );
        Ok::<_, sinomo_dfs::DfsError>(NetworkedDfsService::new(transport, content))
    };

    // Create the lightweight worker before scanning. It shares the outbox, so
    // work reconstructed below is visible without retaining the metadata-heavy
    // recovery service or draining network calls inline.
    let mut recovery_service = open_recovery_service()?;
    let worker = recovery_service.prepare_message_deferred_work();

    let recovery_scan_started = Instant::now();
    let mut scan_deferred_for_empty_topology = false;
    let scan_result = if recovery_scan_required {
        (|| {
            if sinomo_dfs::cluster_dfs_metadata_shards(cluster_paths, &file_system.file_system_id)?
                .is_empty()
            {
                scan_deferred_for_empty_topology = true;
                return Ok(());
            }
            let metadata_backend =
                sinomo_dfs::open_cluster_aware_dfs_metadata_backend(data_dir, cluster_paths)?;
            with_daemon_dfs_control_plane_on_runtime(
                open_recovery_service()?,
                cluster_paths,
                Some(runtime),
            )
            .with_boxed_metadata_backend_for_commit_promotion_recovery(metadata_backend)
            .map(|_| {
                // The durable scan is complete even when one target remains
                // offline. Its exact send stays in the shared outbox.
                if authority.storage_generation() == storage_generation {
                    detached_send_runtime.record_recovery_scan_generation(storage_generation);
                }
            })
        })()
    } else {
        Ok(())
    };
    let recovery_scan_ms = recovery_scan_started.elapsed().as_millis();

    // Return the worker even when the scan failed: it may have queued valid
    // work before encountering a later corrupt/unavailable record, and retained
    // ready work must not depend on a successful fresh scan. The caller drops
    // its local lifecycle and execution guards before waking the scheduler.
    let pending = worker.has_pending()?;
    daemon_dfs_perf_trace(format_args!(
        "commit-promotion-recovery authority_generation={} storage_generation={} scanned={} scan_deferred_empty_topology={} scan_ms={} prepared_pending={}",
        authority_generation,
        storage_generation,
        recovery_scan_required && !scan_deferred_for_empty_topology,
        scan_deferred_for_empty_topology,
        recovery_scan_ms,
        pending
    ));
    Ok(DaemonDfsCommitPromotionRecovery {
        detached_runtime: detached_send_runtime,
        worker,
        scan_result,
    })
}

pub(crate) fn daemon_dfs_message_uses_content_read_lane(command: &DfsMessageCommand) -> bool {
    sinomo_dfs::dfs_runtime_message_uses_content_read_lane(command)
}

/// Admit work whose complete metadata state is owned by one shard runtime.
///
/// Replica application is backend-contained. A namespace request carrying one
/// exact authority stamp reads or owns its durable request ledger, prepare
/// state, and replay state in that same shard; cross-shard jobs retain the
/// global shell.
/// Distributed writes still retain coordinator and repair state across several
/// message families and therefore remain global until those responses carry a
/// complete shard identity too.
pub(crate) fn daemon_dfs_message_supports_exact_service_lane(command: &DfsMessageCommand) -> bool {
    sinomo_dfs::dfs_runtime_message_supports_exact_service_lane(command)
}

/// Prove that an exact admission can borrow the already-installed resident
/// generation. This check deliberately never opens a backend. A missing cache,
/// a topology edge, or a stale shard stamp falls back to global admission so
/// generation rollover and structured authority validation happen only after
/// all exact leases have drained.
pub(crate) fn daemon_dfs_message_cache_exact_admission_handle(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    key: &DaemonDfsMutationLaneKey,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<ResidentLocalMetadataShardHandle>, sinomo_dfs::DfsError> {
    let metadata_authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        Some(runtime),
    )?;
    let (observed_generation, contains_shard) =
        metadata_authority_generation.shard_projection_state(&key.file_system_id, &key.shard_id);
    if !contains_shard {
        return Ok(None);
    }

    let cache_wait_started =
        tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE).then(Instant::now);
    let Some(slot) = daemon_dfs_message_service_cache().existing_root(&data_dir.root())? else {
        return Ok(None);
    };
    let cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    if let Some(started) = cache_wait_started {
        daemon_dfs_perf_trace(format_args!(
            "dfs-admission-cache-lock eligibility=exact elapsed_us={} node={} data_root={}",
            started.elapsed().as_micros(),
            peer_snapshot.local_node_id,
            data_dir.root().display()
        ));
    }
    let Some(entry) = cache.as_ref() else {
        return Ok(None);
    };
    entry.exact_admission_handle(
        peer_snapshot,
        &metadata_authority_generation,
        observed_generation,
        key,
    )
}

pub(crate) fn daemon_dfs_message_cache_exact_admission_handle_for_replica_context(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    context: &sinomo_dfs::DfsMetadataReplicaShardContext,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<ResidentLocalMetadataShardHandle>, sinomo_dfs::DfsError> {
    if context.local_node_id != peer_snapshot.local_node_id {
        return Ok(None);
    }
    let metadata_authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        Some(runtime),
    )?;
    let (observed_generation, context_matches) =
        metadata_authority_generation.replica_context_state(context)?;
    if !context_matches {
        return Ok(None);
    }

    let cache_wait_started =
        tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE).then(Instant::now);
    let Some(slot) = daemon_dfs_message_service_cache().existing_root(&data_dir.root())? else {
        return Ok(None);
    };
    let cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    if let Some(started) = cache_wait_started {
        daemon_dfs_perf_trace(format_args!(
            "dfs-admission-cache-lock eligibility=replica elapsed_us={} node={} data_root={}",
            started.elapsed().as_micros(),
            peer_snapshot.local_node_id,
            data_dir.root().display()
        ));
    }
    let Some(entry) = cache.as_ref() else {
        return Ok(None);
    };
    entry.replica_admission_handle(
        peer_snapshot,
        &metadata_authority_generation,
        observed_generation,
        context,
    )
}

pub(crate) fn daemon_dfs_message_cache_exact_admission_is_current(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    key: &DaemonDfsMutationLaneKey,
    runtime: &tokio::runtime::Handle,
    #[cfg(test)] observed_command: Option<&DfsMessageCommand>,
) -> Result<bool, sinomo_dfs::DfsError> {
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::ExactLaneCacheAccess,
            observed_command,
            None,
        )?;
    }
    Ok(daemon_dfs_message_cache_exact_admission_handle(
        data_dir,
        cluster_paths,
        peer_snapshot,
        key,
        runtime,
    )?
    .is_some())
}

/// Admit a stamped namespace lookup to an exact lane only when both its full
/// authority stamp and every routed path match the retained committed
/// topology, and the exact shard backend is already resident.
///
/// The warm requirement keeps the first read after daemon start on the global
/// compatibility service. That cold path may initialize the one resident
/// generation, but it must not create and hydrate a new exact-shard service
/// merely because an unvalidated wire stamp names a shard id.
pub(crate) fn daemon_dfs_message_cache_exact_namespace_read_admission_is_current(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    command: &sinomo_dfs::DfsNamespaceCommand,
    runtime: &tokio::runtime::Handle,
    #[cfg(test)] observed_command: Option<&DfsMessageCommand>,
) -> Result<bool, sinomo_dfs::DfsError> {
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::ExactLaneCacheAccess,
            observed_command,
            None,
        )?;
    }
    let [stamped] = command.authority_shards.as_slice() else {
        return Ok(false);
    };
    if stamped.primary_node_id != peer_snapshot.local_node_id {
        return Ok(false);
    }
    let metadata_authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        Some(runtime),
    )?;
    let (observed_generation, stamp_matches) =
        metadata_authority_generation.namespace_read_stamp_state(command)?;
    if !stamp_matches {
        return Ok(false);
    }

    let cache_wait_started =
        tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE).then(Instant::now);
    let Some(slot) = daemon_dfs_message_service_cache().existing_root(&data_dir.root())? else {
        return Ok(false);
    };
    let cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    if let Some(started) = cache_wait_started {
        daemon_dfs_perf_trace(format_args!(
            "dfs-admission-cache-lock eligibility=namespace elapsed_us={} node={} data_root={}",
            started.elapsed().as_micros(),
            peer_snapshot.local_node_id,
            data_dir.root().display()
        ));
    }
    let Some(entry) = cache.as_ref() else {
        return Ok(false);
    };
    entry.exact_namespace_read_admission_is_current(
        peer_snapshot,
        &metadata_authority_generation,
        observed_generation,
        command,
    )
}

#[cfg(test)]
pub(crate) fn daemon_dfs_cached_resident_metadata_shard_handle(
    data_dir: &SinomoDataDir,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    file_system_id: &DfsFileSystemId,
    shard_id: &DfsMetadataShardId,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<ResidentLocalMetadataShardHandle, sinomo_dfs::DfsError> {
    let key = DaemonDfsMutationLaneKey {
        file_system_id: file_system_id.clone(),
        shard_id: shard_id.clone(),
    };
    let handle = daemon_dfs_message_cache_exact_admission_handle(
        data_dir,
        cluster_paths,
        peer_snapshot,
        &key,
        runtime,
    )?
    .ok_or_else(|| sinomo_dfs::DfsError::Metadata {
        message: format!(
            "resident DFS metadata generation is not current for filesystem {file_system_id} shard {}",
            shard_id.0
        ),
    })?;
    Ok(handle)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DaemonDfsMessageLaneKind {
    MutationGlobal,
    MutationExact(DaemonDfsMutationLaneKey),
    ContentRead,
}

fn try_daemon_dfs_global_message_lane(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    entry: &mut DaemonDfsMessageServiceCacheEntry,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, DaemonDfsAuthorityPreExecutionError> {
    if let Some(lane) = entry.primary_service.as_ref() {
        return Ok(Arc::clone(lane));
    }
    let resident_scope = entry
        .resident_metadata_backend
        .clone()
        .map(DaemonDfsResidentMetadataScope::Global);
    let lane = open_daemon_dfs_message_service_lane_with_content(
        data_dir,
        cluster_paths,
        &entry.signature.local_node_id,
        entry.peer_node_ids.clone(),
        runtime,
        entry.content_store.clone(),
        Some(
            entry
                .metadata_authority_generation
                .load_complete_prefix_for_generation(entry.signature.metadata_authority_generation)
                .ok_or_else(|| {
                    daemon_dfs_stale_authority(
                        &entry.metadata_authority_generation,
                        entry.signature.metadata_authority_generation,
                    )
                })?,
        ),
        resident_scope,
        entry.cross_shard_namespace_locks.clone(),
    )?;
    entry.primary_service = Some(Arc::clone(&lane));
    Ok(lane)
}

pub(crate) fn daemon_dfs_message_cache_entry<'a>(
    cache: &'a mut Option<DaemonDfsMessageServiceCacheEntry>,
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
    allow_generation_rollover: bool,
) -> Result<&'a mut DaemonDfsMessageServiceCacheEntry, sinomo_dfs::DfsError> {
    let cache_key = data_dir.root();
    let peer_node_ids = &peer_snapshot.peer_node_ids;
    let metadata_authority_generation = match cache.as_ref() {
        Some(entry)
            if entry.signature.cluster_id == peer_snapshot.cluster_id
                && entry.signature.local_node_id == peer_snapshot.local_node_id =>
        {
            Arc::clone(&entry.metadata_authority_generation)
        }
        _ => daemon_dfs_metadata_authority_generation_for_identity(
            cluster_paths,
            &peer_snapshot.cluster_id,
            &peer_snapshot.local_node_id,
            Some(runtime),
        )?,
    };
    let (authority_generation, storage_generation) =
        metadata_authority_generation.lifecycle_generations()?;
    let signature = DaemonDfsMessageServiceSignature {
        cluster_id: peer_snapshot.cluster_id.clone(),
        local_node_id: peer_snapshot.local_node_id.clone(),
        metadata_authority_generation: authority_generation,
        metadata_storage_generation: storage_generation,
    };
    let generation_changed = cache
        .as_ref()
        .is_some_and(|entry| entry.signature != signature);
    let storage_changed = cache
        .as_ref()
        .is_some_and(|entry| !entry.signature.shares_metadata_storage(&signature));
    if cache.as_ref().is_none() || (storage_changed && allow_generation_rollover) {
        // Release the superseded backend-bearing lanes before opening their
        // replacement, while retaining the one process-local content store.
        let (content_store, retained_metadata_backend) = match cache.take() {
            Some(entry) => {
                let same_owner = entry.signature.cluster_id == signature.cluster_id
                    && entry.signature.local_node_id == signature.local_node_id
                    && Arc::ptr_eq(
                        &entry.metadata_authority_generation,
                        &metadata_authority_generation,
                    );
                (
                    entry.content_store,
                    same_owner
                        .then_some(entry.resident_metadata_backend)
                        .flatten(),
                )
            }
            None => (
                sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?,
                None,
            ),
        };
        tracing::info!(
            data_root = %cache_key.display(),
            metadata_authority_generation = signature.metadata_authority_generation,
            peer_count = peer_node_ids.len(),
            "opened bounded daemon DFS message service cache generation"
        );
        cache.replace(DaemonDfsMessageServiceCacheEntry {
            signature,
            metadata_authority_generation,
            peer_node_ids: peer_node_ids.clone(),
            content_store,
            // This predecessor is unusable until the current retained cut
            // rebuilds its router and validates every unchanged shard slot.
            resident_metadata_backend: retained_metadata_backend,
            resident_metadata_initialized: false,
            write_intent_maintenance_scan: Arc::new(Default::default()),
            maintenance_progress: Arc::new(Default::default()),
            cross_shard_namespace_locks: Default::default(),
            primary_service: None,
            exact_shard_services: BTreeMap::new(),
            content_read_service: None,
        });
    } else if generation_changed && allow_generation_rollover {
        // Global admission fences the old authority generation. Keep the same
        // resident stores, repair ownership and service lanes for configuration
        // and proof updates; their control-plane clients consume exact deltas.
        cache.as_mut().expect("existing cache entry").signature = signature;
    } else if generation_changed {
        // An authority observer may advance while an exact-shard operation is
        // already admitted on the prior generation. Never open a replacement
        // beside that live capability. The next globally admitted command (or
        // maintenance pass) performs the rollover after every exact lease has
        // drained.
        tracing::debug!(
            data_root = %cache_key.display(),
            observed_generation = signature.metadata_authority_generation,
            retained_generation = cache.as_ref()
                .map(|entry| entry.signature.metadata_authority_generation)
                .unwrap_or_default(),
            "deferred daemon DFS resident generation rollover until global admission"
        );
    }

    let entry = cache
        .as_mut()
        .expect("daemon DFS message cache entry inserted");
    entry.peer_node_ids.clone_from(peer_node_ids);
    Ok(entry)
}

/// Complete or join the one bounded metadata-authority ReadIndex before an
/// async owner hands work to synchronous lane/cache code. The generation is
/// revalidated after the await, so callers never mistake a proof for a
/// topology generation which advanced while the request was in flight.
pub(crate) async fn warm_daemon_dfs_metadata_authority_for_identity_async(
    cluster_paths: &ClusterPaths,
    cluster_id: &sinomo_core::Id,
    local_node_id: &NodeId,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsMetadataAuthorityPreflight, sinomo_dfs::DfsError> {
    let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        cluster_id,
        local_node_id,
        Some(runtime),
    )?;
    warm_daemon_dfs_metadata_authority_before_deadline(
        authority_generation,
        deadline,
        #[cfg(test)]
        None,
    )
    .await
}

/// The caller owns one projection budget across native reads, generation
/// replans and reactive owner handoffs. Shared readiness remains deadline-free.
pub(crate) async fn warm_daemon_dfs_metadata_authority_before_deadline(
    authority_generation: Arc<DaemonDfsMetadataAuthorityGeneration>,
    deadline: tokio::time::Instant,
    #[cfg(test)] native_read_observer: Option<&(dyn Fn(&'static str) + Send + Sync)>,
) -> Result<DaemonDfsMetadataAuthorityPreflight, sinomo_dfs::DfsError> {
    let timeout_authority = Arc::clone(&authority_generation);
    let expected_generation = authority_generation.generation();
    tokio::time::timeout_at(deadline, async move {
    for replan in 0..=2 {
        let expected_generation = authority_generation.generation();
        let proof = authority_generation
            .ensure_complete_prefix_for_generation_async(expected_generation,
    #[cfg(test)] native_read_observer,
)
            .await;
        if proof.is_some() {
            // The shared refresh publishes its filesystem prefix before the
            // complete configuration bundle. Join that same owner before
            // handing a supposedly ready service to synchronous execution.
            // This await retains the caller's original deadline and replan cap.
            authority_generation.ensure_write_configuration_ready_async().await?;
        }
        if proof.is_some() && authority_generation.generation() == expected_generation {
            return Ok(DaemonDfsMetadataAuthorityPreflight {
                authority: authority_generation,
                generation: expected_generation,
            });
        }
        if authority_generation.generation() == expected_generation || replan == 2 {
            return Err(
                daemon_dfs_stale_authority(&authority_generation, expected_generation)
                    .into_dfs_error(),
            );
        }
        // Only a superseded expected generation replans here. An unavailable
        // current proof remains an error; the existing refresh owner owns I/O.
    }
    unreachable!("bounded authority preflight always returns")
    }).await.map_err(|_| {
        let current_generation = timeout_authority.generation();
        let current_ready = timeout_authority.retained_complete_prefix_revision_for_generation(current_generation).is_some();
        sinomo_dfs::DfsError::Network {
                message: format!("DFS metadata authority preflight projection deadline elapsed (expected_generation={expected_generation} current_generation={current_generation} current_ready={current_ready})"),
        }
    })?
}

pub(crate) async fn warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsMetadataAuthorityPreflight, sinomo_dfs::DfsError> {
    warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
        cluster_paths,
        peer_snapshot,
        runtime,
        tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
        #[cfg(test)]
        None,
    )
    .await
}

pub(crate) async fn warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
    deadline: tokio::time::Instant,
    #[cfg(test)] observed_command: Option<&DfsMessageCommand>,
) -> Result<DaemonDfsMetadataAuthorityPreflight, sinomo_dfs::DfsError> {
    let authority = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        Some(runtime),
    )?;
    #[cfg(test)]
    let read_hook = observed_command.and_then(|_| {
        cluster_paths
            .root
            .parent()
            .and_then(|root| daemon_dfs_pre_dispatch_test_hook(&SinomoDataDir::new(root)))
    });
    #[cfg(test)]
    let observe_read = |prefix| {
        if let Some(hook) = read_hook.as_ref() {
            hook(
                DaemonDfsPreDispatchTestPoint::AuthorityNativePrefixRead { prefix },
                observed_command,
                None,
            )
            .expect("actual invocation native read observer");
        }
    };
    warm_daemon_dfs_metadata_authority_before_deadline(
        authority,
        deadline,
        #[cfg(test)]
        read_hook
            .as_ref()
            .map(|_| &observe_read as &(dyn Fn(&'static str) + Send + Sync)),
    )
    .await
}

/// Open the default-DFS lifecycle backend from an already completed authority
/// preflight. This function is intentionally synchronous and retained-only: it
/// may run while global DFS admission is held, so it must never initiate
/// network I/O or enter an owner-runtime bridge.
pub(super) fn try_daemon_dfs_resident_metadata_backend_after_preflight(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    preflight: &DaemonDfsMetadataAuthorityPreflight,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<ResidentLocalShardedMetadataBackend>, DaemonDfsAuthorityPreExecutionError> {
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::BeforeResidentPreflight,
            None,
            Some(preflight),
        )?;
    }
    let expected_generation = preflight.generation;
    daemon_dfs_require_retained_authority(&preflight.authority, expected_generation)?;

    let operation = if crate::service_trace::dfs_perf_trace_enabled() {
        data_dir.root().display().to_string()
    } else {
        String::new()
    };
    let mut timing = crate::service_trace::DfsPhaseTimer::new(&operation);
    timing.mark("resident-cache-wait-started");
    let slot = daemon_dfs_message_service_cache().root(&data_dir.root())?;
    let mut cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    timing.mark("resident-cache-acquired");
    let result: Result<
        Option<ResidentLocalShardedMetadataBackend>,
        DaemonDfsAuthorityPreExecutionError,
    > = (|| {
        let entry = daemon_dfs_message_cache_entry(
            &mut cache,
            data_dir,
            cluster_paths,
            peer_snapshot,
            runtime,
            true,
        )?;
        timing.mark("resident-cache-entry-selected");
        if entry.signature.metadata_authority_generation != expected_generation
            || preflight.authority.generation() != expected_generation
            || !Arc::ptr_eq(&entry.metadata_authority_generation, &preflight.authority)
        {
            return Err(daemon_dfs_stale_authority(
                &preflight.authority,
                expected_generation,
            ));
        }
        if !entry.resident_metadata_initialized {
            timing.mark("resident-backend-open-started");
            // Materialize the bounded retained prefix only when this generation
            // actually needs a resident backend. Warm callers which merely prove
            // that the already-open generation is still current stay certificate-
            // only and do not clone the complete authority vector.
            let projection = preflight
                .authority
                .retained_complete_prefix_for_generation(expected_generation)
                .ok_or_else(|| {
                    daemon_dfs_stale_authority(&preflight.authority, expected_generation)
                })?;
            entry.resident_metadata_backend =
                try_open_daemon_dfs_resident_metadata_backend_from_authority_projection(
                    data_dir,
                    &entry.signature.local_node_id,
                    &entry.metadata_authority_generation,
                    expected_generation,
                    &projection,
                    entry.resident_metadata_backend.take(),
                )?;
            entry.resident_metadata_initialized = true;
            timing.mark("resident-backend-open-completed");
        }
        daemon_dfs_require_retained_authority(&preflight.authority, expected_generation)?;
        Ok(entry.resident_metadata_backend.clone())
    })();
    drop(cache);
    timing.mark("resident-cache-released");
    result
}

/// Only retained-proof rejection before service execution carries this type.
/// Backend, codec and storage failures remain terminal even if authority also
/// changes while the failing operation runs.
#[derive(Debug)]
pub(super) enum DaemonDfsAuthorityPreExecutionError {
    AuthorityPreflightStale(DaemonDfsAuthorityPreflightStale),
    Failed(sinomo_dfs::DfsError),
}

#[derive(Clone, Copy, Debug)]
pub(super) struct DaemonDfsAuthorityPreflightStale {
    pub(super) expected_generation: u64,
    pub(super) current_generation: u64,
    pub(super) current_ready: bool,
}

impl From<sinomo_dfs::DfsError> for DaemonDfsAuthorityPreExecutionError {
    fn from(error: sinomo_dfs::DfsError) -> Self {
        Self::Failed(error)
    }
}

impl From<sinomo_dfs::MetadataBackendError> for DaemonDfsAuthorityPreExecutionError {
    fn from(error: sinomo_dfs::MetadataBackendError) -> Self {
        Self::Failed(error.into())
    }
}

impl From<sinomo_dfs::ContentStoreError> for DaemonDfsAuthorityPreExecutionError {
    fn from(error: sinomo_dfs::ContentStoreError) -> Self {
        Self::Failed(error.into())
    }
}

impl DaemonDfsAuthorityPreExecutionError {
    pub(super) fn into_dfs_error(self) -> sinomo_dfs::DfsError {
        match self {
            Self::AuthorityPreflightStale(stale) => stale.into_dfs_error(),
            Self::Failed(error) => error,
        }
    }
}

impl DaemonDfsAuthorityPreflightStale {
    pub(super) fn into_dfs_error(self) -> sinomo_dfs::DfsError {
        sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS metadata authority advanced after async preflight of generation {} (current_generation={} current_ready={})",
                self.expected_generation, self.current_generation, self.current_ready,
            ),
        }
    }
}

pub(super) fn daemon_dfs_record_authority_replan(
    data_dir: &SinomoDataDir,
    owner: &'static str,
    replan: usize,
    stale: DaemonDfsAuthorityPreflightStale,
) -> Result<(), sinomo_dfs::DfsError> {
    tracing::trace!(target: "sinomo.dfs.perf", owner, replan, maximum = 2,
        data_root = %data_dir.root().display(),
        expected_generation = stale.expected_generation,
        current_generation = stale.current_generation, current_ready = stale.current_ready,
        "DFS authority preexecution replan");
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh { owner, replan },
            None,
            None,
        )?;
    }
    Ok(())
}

pub(super) fn daemon_dfs_stale_authority(
    authority: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    expected_generation: u64,
) -> DaemonDfsAuthorityPreExecutionError {
    let current_generation = authority.generation();
    DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(DaemonDfsAuthorityPreflightStale {
        expected_generation,
        current_generation,
        current_ready: authority
            .retained_complete_prefix_revision_for_generation(current_generation)
            .is_some()
            && matches!(
                authority.write_configuration_readiness(),
                Ok(crate::DfsWriteConfigurationReadiness::Ready(_))
            ),
    })
}

pub(super) fn daemon_dfs_require_retained_authority(
    authority: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    expected_generation: u64,
) -> Result<(), DaemonDfsAuthorityPreExecutionError> {
    if authority
        .retained_complete_prefix_revision_for_generation(expected_generation)
        .is_none()
    {
        return Err(daemon_dfs_stale_authority(authority, expected_generation));
    }
    // A shared refresh publishes the filesystem prefix before its complete
    // configuration. The last preeffect fence must reject that partial cut,
    // even when an ordinary revision catch-up leaves authority generation equal.
    match authority
        .write_configuration_readiness()
        .map_err(DaemonDfsAuthorityPreExecutionError::Failed)?
    {
        crate::DfsWriteConfigurationReadiness::Pending => {
            Err(daemon_dfs_stale_authority(authority, expected_generation))
        }
        crate::DfsWriteConfigurationReadiness::Ready(_) => authority
            .retained_complete_prefix_revision_for_generation(expected_generation)
            .map(|_| ())
            .ok_or_else(|| daemon_dfs_stale_authority(authority, expected_generation)),
    }
}

// Read-only observation of the actual fence, never an admission capability.
#[cfg(test)]
pub(crate) fn test_retained_authority_admission_is_ready(
    authority: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    generation: u64,
) -> bool {
    daemon_dfs_require_retained_authority(authority, generation).is_ok()
}

impl DaemonDfsMetadataAuthorityPreflight {
    pub(super) fn for_admission(&self, admission: &DaemonDfsMutationAdmission) -> Self {
        let generation = self.authority.generation();
        if self.admits_retained_generation(&self.authority, generation, admission.is_global()) {
            Self {
                authority: Arc::clone(&self.authority),
                generation,
            }
        } else {
            self.clone()
        }
    }

    pub(super) fn admits_retained_generation(
        &self,
        authority: &Arc<DaemonDfsMetadataAuthorityGeneration>,
        generation: u64,
        allow_generation_rollover: bool,
    ) -> bool {
        // Global admission drains the prior exact leases and may select a
        // successor that became ready while this request waited. Consume only
        // its retained proof here: cache/lane selection must not perform I/O.
        Arc::ptr_eq(authority, &self.authority)
            && authority.generation() == generation
            && (allow_generation_rollover || generation == self.generation)
            && daemon_dfs_require_retained_authority(authority, generation).is_ok()
    }
}

/// This outcome exists only before any command is handed to its service lane.
/// It must never classify arbitrary errors returned after command execution.
enum DaemonDfsMessageLaneSelection {
    Ready(Arc<DaemonDfsMessageServiceLaneHandle>, Duration),
    AuthorityPreflightStale(DaemonDfsAuthorityPreflightStale),
}

pub(crate) fn daemon_dfs_message_lane(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
    kind: DaemonDfsMessageLaneKind,
    allow_generation_rollover: bool,
    metadata_authority_preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
) -> Result<(Arc<DaemonDfsMessageServiceLaneHandle>, Duration), sinomo_dfs::DfsError> {
    match select_daemon_dfs_message_lane(
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        kind,
        allow_generation_rollover,
        metadata_authority_preflight,
        None,
        #[cfg(test)]
        None,
    )? {
        DaemonDfsMessageLaneSelection::Ready(lane, wait) => Ok((lane, wait)),
        DaemonDfsMessageLaneSelection::AuthorityPreflightStale(stale) => {
            Err(stale.into_dfs_error())
        }
    }
}

fn select_daemon_dfs_message_lane(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
    kind: DaemonDfsMessageLaneKind,
    allow_generation_rollover: bool,
    metadata_authority_preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
    response_command: Option<&DfsMessageCommand>,
    #[cfg(test)] observed_command: Option<&DfsMessageCommand>,
) -> Result<DaemonDfsMessageLaneSelection, sinomo_dfs::DfsError> {
    match try_select_daemon_dfs_message_lane(
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        kind,
        allow_generation_rollover,
        metadata_authority_preflight,
        response_command,
        #[cfg(test)]
        observed_command,
    ) {
        Ok(selection) => Ok(selection),
        Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale)) => Ok(
            DaemonDfsMessageLaneSelection::AuthorityPreflightStale(stale),
        ),
        Err(DaemonDfsAuthorityPreExecutionError::Failed(error)) => Err(error),
    }
}

fn try_select_daemon_dfs_message_lane(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
    kind: DaemonDfsMessageLaneKind,
    allow_generation_rollover: bool,
    metadata_authority_preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
    response_command: Option<&DfsMessageCommand>,
    #[cfg(test)] observed_command: Option<&DfsMessageCommand>,
) -> Result<DaemonDfsMessageLaneSelection, DaemonDfsAuthorityPreExecutionError> {
    let cache_lock_wait_started = Instant::now();
    let slot = daemon_dfs_message_service_cache().root(&data_dir.root())?;
    let mut cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    let cache_lock_wait = cache_lock_wait_started.elapsed();
    let entry = daemon_dfs_message_cache_entry(
        &mut cache,
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        allow_generation_rollover,
    )?;
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::BeforePreflightFence,
            observed_command,
            metadata_authority_preflight,
        )?;
    }
    if let Some(preflight) = metadata_authority_preflight
        && !preflight.admits_retained_generation(
            &entry.metadata_authority_generation,
            entry.signature.metadata_authority_generation,
            allow_generation_rollover,
        )
    {
        return Err(daemon_dfs_stale_authority(
            &preflight.authority,
            preflight.generation,
        ));
    }
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::AfterPreflightFence,
            observed_command,
            metadata_authority_preflight,
        )?;
    }
    if matches!(
        &kind,
        DaemonDfsMessageLaneKind::MutationExact(_) | DaemonDfsMessageLaneKind::MutationGlobal
    ) && !entry.resident_metadata_initialized
    {
        entry.resident_metadata_backend =
            try_open_daemon_dfs_resident_metadata_backend_for_authority_generation(
                data_dir,
                &entry.signature.local_node_id,
                &entry.metadata_authority_generation,
                entry.signature.metadata_authority_generation,
                entry.resident_metadata_backend.take(),
            )?;
        entry.resident_metadata_initialized = true;
    }
    if matches!(&kind, DaemonDfsMessageLaneKind::MutationGlobal)
        && let Some(command) = response_command
        && matches!(
            &command.routed_message.message,
            sinomo_dfs::DfsNetworkMessage::FetchBlobResponse(_)
        )
    {
        let mut candidates = Vec::new();
        if let Some(resident) = entry.resident_metadata_backend.as_ref() {
            for (key, lane) in &entry.exact_shard_services {
                let config = resident.shard_config(&key.shard_id)?;
                if config.file_system_id != key.file_system_id {
                    return Err(sinomo_dfs::DfsError::Metadata {
                        message: "cached fetch owner has a different filesystem scope".into(),
                    }
                    .into());
                }
                candidates.push((config, Arc::clone(lane)));
            }
        }
        let authority = Arc::clone(&entry.metadata_authority_generation);
        let generation = entry.signature.metadata_authority_generation;
        drop(cache);
        let owner =
            crate::runtime_message::existing_fetch_response_service_lane(&candidates, command)?;
        daemon_dfs_require_retained_authority(&authority, generation)?;
        if let Some(owner) = owner {
            return Ok(DaemonDfsMessageLaneSelection::Ready(owner, cache_lock_wait));
        }
        // Unmatched/restarted responses keep the existing conservative global
        // recovery path. No missing transfer state is turned into NotSent.
        return try_select_daemon_dfs_message_lane(
            data_dir,
            cluster_paths,
            peer_snapshot,
            runtime,
            kind,
            allow_generation_rollover,
            metadata_authority_preflight,
            None,
            #[cfg(test)]
            observed_command,
        );
    }
    let lane_kind = kind.clone();
    let lane = match kind {
        DaemonDfsMessageLaneKind::MutationExact(key)
            if entry.resident_metadata_backend.is_some() =>
        {
            if let Some(lane) = entry.exact_shard_services.get(&key) {
                Arc::clone(lane)
            } else {
                let resident = entry
                    .resident_metadata_backend
                    .as_ref()
                    .expect("exact resident lane checked above");
                let shard = match resident.scope_shard(&key.file_system_id, &key.shard_id) {
                    Ok(shard) => shard,
                    Err(error) if allow_generation_rollover => {
                        tracing::debug!(
                            file_system_id = %key.file_system_id,
                            shard_id = %key.shard_id.0,
                            reason = %error,
                            "falling back to globally admitted DFS service validation for a stale exact-shard stamp"
                        );
                        let lane = try_daemon_dfs_global_message_lane(
                            data_dir,
                            cluster_paths,
                            runtime,
                            entry,
                        )?;
                        daemon_dfs_require_retained_authority(
                            &entry.metadata_authority_generation,
                            entry.signature.metadata_authority_generation,
                        )?;
                        return Ok(DaemonDfsMessageLaneSelection::Ready(lane, cache_lock_wait));
                    }
                    Err(error) => return Err(error.into()),
                };
                let lane = open_daemon_dfs_message_service_lane_with_content(
                    data_dir,
                    cluster_paths,
                    &entry.signature.local_node_id,
                    entry.peer_node_ids.clone(),
                    runtime,
                    entry.content_store.clone(),
                    Some(
                        entry
                            .metadata_authority_generation
                            .load_complete_prefix_for_generation(
                                entry.signature.metadata_authority_generation,
                            )
                            .ok_or_else(|| {
                                daemon_dfs_stale_authority(
                                    &entry.metadata_authority_generation,
                                    entry.signature.metadata_authority_generation,
                                )
                            })?,
                    ),
                    Some(DaemonDfsResidentMetadataScope::ExactShard(shard)),
                    entry.cross_shard_namespace_locks.clone(),
                )?;
                entry.exact_shard_services.insert(key, Arc::clone(&lane));
                lane
            }
        }
        DaemonDfsMessageLaneKind::MutationExact(_) | DaemonDfsMessageLaneKind::MutationGlobal => {
            try_daemon_dfs_global_message_lane(data_dir, cluster_paths, runtime, entry)?
        }
        DaemonDfsMessageLaneKind::ContentRead => {
            if let Some(lane) = entry.content_read_service.as_ref() {
                Arc::clone(lane)
            } else {
                let lane = open_daemon_dfs_message_content_read_lane_with_content(
                    data_dir,
                    cluster_paths,
                    &entry.signature.local_node_id,
                    entry.peer_node_ids.clone(),
                    runtime,
                    entry.content_store.clone(),
                )?;
                entry.content_read_service = Some(Arc::clone(&lane));
                lane
            }
        }
    };
    if !matches!(lane_kind, DaemonDfsMessageLaneKind::ContentRead) {
        daemon_dfs_require_retained_authority(
            &entry.metadata_authority_generation,
            entry.signature.metadata_authority_generation,
        )?;
    }
    tracing::trace!(
        data_root = %data_dir.root().display(),
        lane = ?lane_kind,
        "selected bounded daemon DFS service lane"
    );
    Ok(DaemonDfsMessageLaneSelection::Ready(lane, cache_lock_wait))
}

pub(crate) fn daemon_dfs_message_content_store(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
    #[cfg(test)] observed_command: Option<&DfsMessageCommand>,
) -> Result<sinomo_dfs::LocalContentStore, sinomo_dfs::DfsError> {
    #[cfg(test)]
    if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(data_dir) {
        hook(
            DaemonDfsPreDispatchTestPoint::ContentStoreCacheAccess,
            observed_command,
            None,
        )?;
    }
    let slot = daemon_dfs_message_service_cache().root(&data_dir.root())?;
    let mut cache = slot.lock().map_err(|_| daemon_dfs_cache_lock_error())?;
    let entry = daemon_dfs_message_cache_entry(
        &mut cache,
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        false,
    )?;
    Ok(entry.content_store.clone())
}

/// Only this pre-execution variant returns an owned command for another
/// admission attempt. The exact shard/epoch/operation and quarantine capability
/// are preserved unchanged; service outcomes (including late errors) are final.
pub(super) enum DaemonDfsMessageDispatchAttempt {
    Complete(Option<DaemonDfsMessageCommandProcessResult>),
    RemoteNamespacePhase(
        DfsMessageCommand,
        Box<crate::source_adoption::RemoteNamespacePhase>,
    ),
    AuthorityPreflightStale(Box<DaemonDfsUnexecutedMessage>),
}

// Only the rare retry path allocates this owned payload. Healthy dispatch does
// not enlarge its return value to hold another complete wire command.
pub(super) struct DaemonDfsUnexecutedMessage {
    stale: DaemonDfsAuthorityPreflightStale,
    command: DfsMessageCommand,
    preingested_blob: Option<sinomo_dfs::ContentStoreQuarantinedBlob>,
}

pub(super) fn prepare_daemon_dfs_verified_blob_fetch(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer: &DaemonDfsMessagePeerSnapshot,
    command: &DfsMessageCommand,
    preflight: &DaemonDfsMetadataAuthorityPreflight,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<crate::service_fetch_transfer::VerifiedDfsBlobFetch>, sinomo_dfs::DfsError> {
    let sinomo_dfs::DfsNetworkMessage::FetchBlob(fetch) = &command.routed_message.message else {
        return Ok(None);
    };
    if command.routed_message.target_node_id != peer.local_node_id {
        return Ok(None);
    }
    let Some(stamped) = fetch.authority_shard.as_ref() else {
        return Ok(None);
    };
    let Some(topology) = preflight
        .authority
        .retained_complete_prefix_for_generation(preflight.generation)
    else {
        return Ok(None);
    };
    let shards =
        daemon_dfs_metadata_shards_from_authority_projection(&topology, &stamped.file_system_id)?;
    if !shards.contains(stamped)
        || (stamped.primary_node_id != peer.local_node_id
            && !stamped.replica_node_ids.contains(&peer.local_node_id))
    {
        return Ok(None);
    }
    let lane = match select_daemon_dfs_message_lane(
        data_dir,
        cluster_paths,
        peer,
        runtime,
        DaemonDfsMessageLaneKind::ContentRead,
        false,
        Some(preflight),
        None,
        #[cfg(test)]
        None,
    )? {
        DaemonDfsMessageLaneSelection::Ready(lane, _) => lane,
        DaemonDfsMessageLaneSelection::AuthorityPreflightStale(_) => return Ok(None),
    };
    let verified = match crate::local_state::with_current_owned_dfs_shard_inspection(
        data_dir,
        &sinomo_dfs::DfsMetadataShardId(stamped.shard_id.clone()),
        |backend| {
            let Ok(lane) = lane.try_lock() else {
                return Ok(None);
            };
            lane.service.verify_immutable_blob_fetch(
                backend,
                &command.routed_message.source_node_id,
                fetch,
                stamped,
                &shards,
            )
        },
    ) {
        Ok(verified) => verified.flatten(),
        Err(_) => return Ok(None), // Uncertified observational state stays global.
    };
    if preflight
        .authority
        .retained_complete_prefix_revision_for_generation(preflight.generation)
        .is_none()
    {
        return Ok(None);
    }
    Ok(verified)
}

pub(crate) fn process_daemon_dfs_message_command_on_runtime(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    command: DfsMessageCommand,
    preingested_blob: Option<sinomo_dfs::ContentStoreQuarantinedBlob>,
    execution_scope: &DfsMessageExecutionScope,
    allow_exact_service_lane: bool,
    allow_generation_rollover: bool,
    metadata_authority_preflight: Option<DaemonDfsMetadataAuthorityPreflight>,
    peer_snapshot_elapsed: Duration,
    mutation_admission: &mut Option<DaemonDfsMutationAdmission>,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> Result<Option<DaemonDfsMessageCommandProcessResult>, sinomo_dfs::DfsError> {
    match attempt_daemon_dfs_message_command_on_runtime(
        data_dir,
        cluster_paths,
        peer_snapshot,
        command,
        preingested_blob,
        None,
        execution_scope,
        allow_exact_service_lane,
        allow_generation_rollover,
        metadata_authority_preflight,
        peer_snapshot_elapsed,
        mutation_admission,
        runtime,
        traceparent,
    )? {
        DaemonDfsMessageDispatchAttempt::Complete(result) => Ok(result),
        DaemonDfsMessageDispatchAttempt::AuthorityPreflightStale(retry) => {
            Err(retry.stale.into_dfs_error())
        }
        DaemonDfsMessageDispatchAttempt::RemoteNamespacePhase(_, phase) => {
            phase.cancel_before_dispatch()?;
            Err(crate::DfsError::Metadata {
                message: "remote phase requires the normal asynchronous host continuation".into(),
            })
        }
    }
}

pub(super) fn attempt_daemon_dfs_message_command_on_runtime(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    command: DfsMessageCommand,
    preingested_blob: Option<sinomo_dfs::ContentStoreQuarantinedBlob>,
    verified_fetch: Option<crate::service_fetch_transfer::VerifiedDfsBlobFetch>,
    execution_scope: &DfsMessageExecutionScope,
    allow_exact_service_lane: bool,
    allow_generation_rollover: bool,
    metadata_authority_preflight: Option<DaemonDfsMetadataAuthorityPreflight>,
    peer_snapshot_elapsed: Duration,
    mutation_admission: &mut Option<DaemonDfsMutationAdmission>,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
) -> Result<DaemonDfsMessageDispatchAttempt, sinomo_dfs::DfsError> {
    attempt_daemon_dfs_message_command_with_remote_phase(
        data_dir,
        cluster_paths,
        peer_snapshot,
        command,
        preingested_blob,
        verified_fetch,
        execution_scope,
        allow_exact_service_lane,
        allow_generation_rollover,
        metadata_authority_preflight,
        peer_snapshot_elapsed,
        mutation_admission,
        runtime,
        traceparent,
        None,
    )
}

fn attempt_daemon_dfs_message_command_with_remote_phase(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    command: DfsMessageCommand,
    preingested_blob: Option<sinomo_dfs::ContentStoreQuarantinedBlob>,
    verified_fetch: Option<crate::service_fetch_transfer::VerifiedDfsBlobFetch>,
    execution_scope: &DfsMessageExecutionScope,
    allow_exact_service_lane: bool,
    allow_generation_rollover: bool,
    metadata_authority_preflight: Option<DaemonDfsMetadataAuthorityPreflight>,
    peer_snapshot_elapsed: Duration,
    mutation_admission: &mut Option<DaemonDfsMutationAdmission>,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    completed_phase: Option<crate::source_adoption::CompletedRemoteNamespacePhase>,
) -> Result<DaemonDfsMessageDispatchAttempt, sinomo_dfs::DfsError> {
    let trace_started = Instant::now();
    // Daemon startup owns data-directory initialization. Repeating its six
    // create_dir_all calls here turns every routed DFS message into layout I/O.
    let cache_key = data_dir.root();
    let is_read_only = verified_fetch.is_some() || dfs_message_is_read_only_request(&command);
    let use_content_read_lane =
        verified_fetch.is_some() || daemon_dfs_message_uses_content_read_lane(&command);
    let request_id = command.request_id.clone();
    let source_node_id = command.routed_message.source_node_id.clone();
    let target_node_id = command.routed_message.target_node_id.clone();
    let frame_id = command.routed_message.frame_id;
    let message_id = command.routed_message.message_id;
    let message_kind = daemon_dfs_message_kind(&command.routed_message.message);
    daemon_dfs_perf_trace(format_args!(
        "dfs-message-dispatch-start request={} kind={} source={} target={} frame_id={} message_id={} read_lane={} content_lane={}",
        request_id,
        message_kind,
        source_node_id,
        target_node_id,
        frame_id,
        message_id,
        is_read_only,
        use_content_read_lane
    ));
    let peer_snapshot_ms = peer_snapshot_elapsed.as_millis();
    if command.routed_message.target_node_id != peer_snapshot.local_node_id {
        return Err(sinomo_dfs::DfsError::Network {
            message: format!(
                "dfs message target {} does not match local node {}",
                command.routed_message.target_node_id, peer_snapshot.local_node_id
            ),
        });
    }
    let peer_node_ids = peer_snapshot.peer_node_ids.clone();
    let cache_select_started = Instant::now();
    let lane_kind = if use_content_read_lane {
        DaemonDfsMessageLaneKind::ContentRead
    } else if allow_exact_service_lane
        && let Some(key) = DaemonDfsMutationLaneKey::from_scope(execution_scope)
    {
        DaemonDfsMessageLaneKind::MutationExact(key)
    } else {
        DaemonDfsMessageLaneKind::MutationGlobal
    };
    let selection = select_daemon_dfs_message_lane(
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        lane_kind,
        allow_generation_rollover,
        metadata_authority_preflight.as_ref(),
        mutation_admission
            .as_ref()
            .filter(|admission| admission.is_global())
            .map(|_| &command),
        #[cfg(test)]
        Some(&command),
    )?;
    let (service_lane_handle, cache_lock_wait) = match selection {
        DaemonDfsMessageLaneSelection::Ready(lane, wait) => (lane, wait),
        DaemonDfsMessageLaneSelection::AuthorityPreflightStale(stale) => {
            return Ok(DaemonDfsMessageDispatchAttempt::AuthorityPreflightStale(
                Box::new(DaemonDfsUnexecutedMessage {
                    stale,
                    command,
                    preingested_blob,
                }),
            ));
        }
    };
    let cache_select = cache_select_started.elapsed();

    #[cfg(any(test, feature = "test-support"))]
    let execution_test_hook = daemon_dfs_pre_dispatch_test_hook(data_dir);
    #[cfg(any(test, feature = "test-support"))]
    let observed_command = execution_test_hook.as_ref().map(|_| command.clone());
    #[cfg(test)]
    struct CreatePrepareTestHookCleanup;
    #[cfg(test)]
    impl Drop for CreatePrepareTestHookCleanup {
        fn drop(&mut self) {
            crate::service_commit::BEFORE_CREATE_PREPARE.with(|hook| {
                hook.borrow_mut().take();
            });
            crate::publication_guard::AFTER_PREPARED_AUTHORIZATION.with(|hook| {
                hook.borrow_mut().take();
            });
        }
    }
    // A fixture may arm the existing one-shot on this actual Main worker.
    // Clear it on the SAME worker on every return and unwind, including errors.
    #[cfg(test)]
    let _create_prepare_cleanup = execution_test_hook
        .as_ref()
        .map(|_| CreatePrepareTestHookCleanup);
    #[cfg(any(test, feature = "test-support"))]
    if let Some(hook) = execution_test_hook.as_ref() {
        hook(
            DaemonDfsPreDispatchTestPoint::BeforeExecute {
                admission_held: mutation_admission.is_some(),
            },
            observed_command.as_ref(),
            metadata_authority_preflight.as_ref(),
        )?;
    }
    if verified_fetch.is_some() {
        let preflight = metadata_authority_preflight.as_ref().ok_or_else(|| {
            sinomo_dfs::DfsError::Metadata {
                message: "immutable fetch lost its retained authority preflight".to_string(),
            }
        })?;
        match daemon_dfs_require_retained_authority(&preflight.authority, preflight.generation) {
            Ok(()) => {}
            Err(DaemonDfsAuthorityPreExecutionError::AuthorityPreflightStale(stale)) => {
                return Ok(DaemonDfsMessageDispatchAttempt::AuthorityPreflightStale(
                    Box::new(DaemonDfsUnexecutedMessage {
                        stale,
                        command,
                        preingested_blob,
                    }),
                ));
            }
            Err(DaemonDfsAuthorityPreExecutionError::Failed(error)) => return Err(error),
        }
    }
    let mut publication_scope = match daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir))
        .map_err(|error| sinomo_dfs::DfsError::Metadata {
            message: error.to_string(),
        })? {
        Some(owner)
            if mutation_admission
                .as_ref()
                .is_some_and(|admission| admission.is_exact() || admission.is_global()) =>
        {
            Some(
                crate::publication_guard::DfsPublicationExecutionScope::enter(
                    data_dir.root(),
                    execution_scope.clone(),
                    mutation_admission,
                    owner,
                )?,
            )
        }
        _ => None,
    };
    let publication_provider = publication_scope
        .as_ref()
        .map(|scope| {
            let authority = metadata_authority_preflight
                .as_ref()
                .map(|preflight| Arc::clone(&preflight.authority))
                .or_else(|| {
                    service_lane_handle
                        .detached_send_runtime()
                        .metadata_authority_generation()
                })
                .ok_or_else(|| sinomo_dfs::DfsError::Metadata {
                    message: "admitted publication has no actual retained runtime authority".into(),
                })?;
            crate::DfsPublicationExecutionProvider::from_execution_owners(
                &scope.owners(),
                authority,
                runtime.clone(),
            )
            .map_err(sinomo_dfs::DfsError::from)
        })
        .transpose()?;
    let execution = crate::runtime_message::execute_runtime_message_command_with_verified_fetch(
        &service_lane_handle,
        peer_node_ids,
        command,
        preingested_blob,
        execution_scope,
        verified_fetch,
        publication_scope
            .as_ref()
            .map(crate::publication_guard::DfsPublicationExecutionScope::owners),
        publication_provider,
        completed_phase,
        |repair_queue_records| {
            persist_daemon_dfs_repair_queue_records(data_dir, repair_queue_records)
        },
    );
    if let Some(scope) = publication_scope.take() {
        scope.finish()?;
    }
    drop(publication_scope);
    let sinomo_dfs::DfsRuntimeMessageExecution {
        remote_namespace_phase,
        process_report,
        committed_work,
        replica_materialization_shard,
        deferred_work,
        #[cfg(test)]
        new_repair_records_unattempted,
        repair_queue_record_count,
        repair_queue_persistence,
        exact_projection_release_error,
        resident_backend_release_error,
        lane_lock_wait,
        backend_checkout,
        process,
        repair_queue_elapsed,
    } = execution?;
    #[cfg(test)]
    if let Some(hook) = execution_test_hook.as_ref() {
        hook(
            DaemonDfsPreDispatchTestPoint::AfterReport {
                committed_work: process_report.committed_work,
                repair_record_count: repair_queue_record_count,
                new_repair_records_unattempted,
                succeeded: process_report.outcome.is_ok(),
            },
            observed_command.as_ref(),
            metadata_authority_preflight.as_ref(),
        )?;
    }
    if let Some((command, phase)) = remote_namespace_phase {
        if let Some(error) = resident_backend_release_error {
            phase.cancel_before_dispatch()?;
            return Err(error);
        }
        return Ok(DaemonDfsMessageDispatchAttempt::RemoteNamespacePhase(
            command, phase,
        ));
    }
    match replica_materialization_shard {
        Ok(Some(shard)) => match daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir)) {
            Ok(Some(data_owner_lock)) => match mutation_admission
                .take()
                .filter(|admission| admission.is_exact() || admission.is_global())
            {
                Some(admission) => {
                    schedule_daemon_dfs_replica_materialization_on_runtime(
                        data_dir.clone(),
                        runtime,
                        shard,
                        data_owner_lock,
                        admission,
                    );
                }
                None => tracing::warn!(
                    "durable DFS replica ingest could not transfer its mutation admission to asynchronous materialization"
                ),
            },
            Ok(None) => tracing::warn!(
                "durable DFS replica ingest could not retain the daemon data-owner lock for asynchronous materialization"
            ),
            Err(error) => tracing::warn!(
                "durable DFS replica ingest could not schedule asynchronous materialization: {error}"
            ),
        },
        Ok(None) => {}
        Err(error) => tracing::warn!(
            "durable DFS replica ingest could not resolve its resident materialization shard: {error}"
        ),
    }
    let (outbound_message_count, event_count) = process_report
        .outcome
        .as_ref()
        .map(|result| (result.response.outbound_messages.len(), result.events.len()))
        .unwrap_or_default();
    daemon_dfs_perf_trace(format_args!(
        "dfs-message-dispatch request={} read_lane={} peer_snapshot_ms={} cache_select_ms={} cache_select_us={} cache_lock_wait_ms={} cache_lock_wait_us={} lane_lock_wait_ms={} lane_lock_wait_us={} backend_checkout_ms={} backend_checkout_us={} process_ms={} process_us={} repair_queue_records={} repair_queue_appended={} repair_queue_ms={} repair_queue_us={} process_ok={} outbound_messages={} events={} total_ms={}",
        request_id,
        is_read_only,
        peer_snapshot_ms,
        cache_select.as_millis(),
        cache_select.as_micros(),
        cache_lock_wait.as_millis(),
        cache_lock_wait.as_micros(),
        lane_lock_wait.as_millis(),
        lane_lock_wait.as_micros(),
        backend_checkout.as_millis(),
        backend_checkout.as_micros(),
        process.as_millis(),
        process.as_micros(),
        repair_queue_record_count,
        repair_queue_persistence
            .as_ref()
            .copied()
            .unwrap_or_default(),
        repair_queue_elapsed.as_millis(),
        repair_queue_elapsed.as_micros(),
        process_report.outcome.is_ok(),
        outbound_message_count,
        event_count,
        trace_started.elapsed().as_millis()
    ));
    if let Err(error) = repair_queue_persistence {
        tracing::error!(
            target: "sinomo.dfs.fault_healing",
            data_root = %cache_key.display(),
            request = %request_id,
            kind = message_kind,
            error = %error,
            "failed to persist committed DFS repair queue; internal maintenance reconciliation remains required"
        );
        if let Err(recovery_error) =
            request_daemon_dfs_maintenance_reconciliation(&DaemonPaths::new(data_dir))
        {
            tracing::error!(%recovery_error, "could not schedule metadata-owned DFS repair recovery");
        }
    }
    #[cfg(test)]
    wait_at_daemon_dfs_message_committed_edge_test_hook(&request_id);
    let retained_events = process_report
        .outcome
        .as_ref()
        .map(|result| result.events.as_slice())
        .unwrap_or_default();
    for event in retained_events {
        if let sinomo_dfs::NetworkedDfsServiceEvent::BlobFetchRejected {
            intent_id,
            object_id,
            source_node_id,
            reason,
        } = event
        {
            crate::catchup_diagnostics::blob_fetch_rejected(
                &peer_snapshot.local_node_id,
                source_node_id,
                intent_id,
                object_id,
                reason,
            );
        }
    }
    let committed_edges = emit_daemon_dfs_committed_edges(
        data_dir,
        cluster_paths,
        execution_scope,
        runtime,
        traceparent,
        committed_work,
        retained_events,
    );
    // Schedule detached convergence before returning across the cancellable
    // async JoinHandle boundary. A disconnected RPC drops its JoinHandle while
    // this blocking task keeps running, so deferring this wake to the caller
    // could leave restored promotion work idle until unrelated later traffic.
    let deferred_work_error = match deferred_work {
        Ok(Some((detached_runtime, worker))) => {
            schedule_daemon_dfs_message_deferred_work(
                runtime,
                cluster_paths,
                detached_runtime,
                worker,
            );
            None
        }
        Ok(None) => None,
        Err(error) => Some(error),
    };
    let DfsMessageProcessResult {
        response,
        attachment,
        events: _,
    } = process_report.outcome?;
    if let Some(error) = deferred_work_error {
        return Err(error);
    }
    if let Some(error) = exact_projection_release_error {
        return Err(error);
    }
    if let Some(error) = resident_backend_release_error {
        return Err(error);
    }
    #[cfg(test)]
    if let Some(hook) = execution_test_hook {
        hook(
            DaemonDfsPreDispatchTestPoint::AfterExecute,
            observed_command.as_ref(),
            metadata_authority_preflight.as_ref(),
        )?;
    }
    Ok(DaemonDfsMessageDispatchAttempt::Complete(Some(
        DaemonDfsMessageCommandProcessResult {
            response,
            attachment,
            committed_edges,
        },
    )))
}

/// Emit retained work produced by a durably processed DFS mutation scope.
///
/// Served-message and maintenance paths invoke this only after releasing their
/// backend-bearing generation, but before unwrapping the operation outcome. A
/// caller can abandon an outer `JoinHandle`, or later work can fail after a
/// durable mutation; transferring a caller-retained receipt here guarantees
/// committed work cannot depend on unrelated traffic.
pub(crate) struct DaemonDfsCommittedEffectsHost<'a> {
    pub(super) data_dir: &'a SinomoDataDir,
    pub(super) cluster_paths: &'a ClusterPaths,
    pub(super) execution_scope: &'a DfsMessageExecutionScope,
    pub(super) runtime: &'a tokio::runtime::Handle,
    pub(super) traceparent: Option<String>,
}

impl sinomo_dfs::DfsCommittedEffectsHost for DaemonDfsCommittedEffectsHost<'_> {
    fn record_replica_progress(&self) -> bool {
        let advanced =
            sinomo_dfs::invalidate_default_cluster_dfs_materialization_for_replica_progress(
                self.data_dir,
            );
        if advanced {
            notify_daemon_default_dfs_reconciliation_replica_progress(self.cluster_paths);
        }
        advanced
    }

    fn wake_maintenance(&self, _repair: bool, _namespace: bool) {
        #[cfg(test)]
        if _repair {
            record_daemon_dfs_repair_maintenance_edge_request(self.data_dir);
        }
        notify_daemon_dfs_maintenance_work(self.data_dir);
    }

    fn publish_metadata(&self) {
        schedule_daemon_dfs_metadata_publish_for_data_dir_on_runtime(
            self.data_dir.clone(),
            self.cluster_paths.clone(),
            self.runtime,
            self.traceparent.clone(),
            DaemonDfsMutationLaneKey::from_scope(self.execution_scope),
        );
    }

    fn semantic_changes(&self) {
        if let Ok(host) = dfs_runtime_host(self.data_dir)
            && let Some(events) = host.event_client.clone()
            && let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(
                self.cluster_paths,
            )
        {
            let origin = state.local_node_id.clone();
            let task = self.runtime.spawn(async move {
                publish_dfs_namespace_change(&events, origin, "metadata-committed", Vec::new())
                    .await;
            });
            host.own_task(task);
        }
    }

    fn storage_environment_changed(&self) {
        notify_daemon_dfs_storage_environment_dirty(self.data_dir);
    }
}

pub(crate) fn emit_daemon_dfs_committed_edges(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    execution_scope: &DfsMessageExecutionScope,
    runtime: &tokio::runtime::Handle,
    traceparent: Option<String>,
    committed_work: DfsMessageCommittedWork,
    events: &[sinomo_dfs::NetworkedDfsServiceEvent],
) -> DaemonDfsMessageCommittedEdges {
    sinomo_dfs::emit_runtime_committed_effects(
        &DaemonDfsCommittedEffectsHost {
            data_dir,
            cluster_paths,
            execution_scope,
            runtime,
            traceparent,
        },
        &committed_work,
        events,
    )
}

/// Borrowed diagnostics only; disabled tracing does not read clocks or allocate.
pub(super) struct DaemonDfsAdmissionTrace<'a> {
    pub(super) label: &'static str,
    pub(super) request: &'a str,
    pub(super) kind: &'a str,
    pub(super) attempt: usize,
    pub(super) scope: &'a DfsMessageExecutionScope,
    pub(super) traceparent: Option<&'a str>,
    pub(super) enabled: bool,
}

impl DaemonDfsAdmissionTrace<'_> {
    pub(super) fn start(&self) -> Option<Instant> {
        (self.enabled || tracing::enabled!(target: "sinomo.perf", tracing::Level::TRACE))
            .then(Instant::now)
    }

    fn scope_labels(&self) -> (&str, &str, &str) {
        match self.scope {
            DfsMessageExecutionScope::ReadOnly => ("read-only", "-", "-"),
            DfsMessageExecutionScope::ExactShard {
                file_system_id,
                shard_id,
            } => ("exact", file_system_id.as_str(), shard_id.0.as_str()),
            DfsMessageExecutionScope::NamespacePaths { file_system_id, .. } => {
                ("namespace-global", file_system_id.as_str(), "-")
            }
            DfsMessageExecutionScope::GlobalExclusive => ("global", "-", "-"),
        }
    }

    pub(super) fn span(&self, phase: &'static str) -> tracing::Span {
        if tracing::enabled!(target: "sinomo.perf", tracing::Level::TRACE) {
            let (command_scope, fs, shard) = self.scope_labels();
            return tracing::trace_span!(target: "sinomo.perf", "dfs.admission",
                stage = "dfs.admission", phase, request = self.request, kind = self.kind,
                attempt = self.attempt, traceparent = self.traceparent.unwrap_or_default(),
                command_scope, fs, shard);
        }
        self.log_span(phase)
    }

    fn log_span(&self, phase: &'static str) -> tracing::Span {
        if !self.enabled {
            return tracing::Span::none();
        }
        let (command_scope, fs, shard) = self.scope_labels();
        tracing::trace_span!(
            target: "sinomo.dfs.perf",
            "dfs-admission-request",
            request = self.request,
            kind = self.kind,
            attempt = self.attempt,
            phase,
            traceparent = self.traceparent.unwrap_or_default(),
            command_scope, fs, shard,
        )
    }

    pub(super) fn record(
        &self,
        phase: &'static str,
        outcome: &str,
        started: Option<Instant>,
        admission: Option<&DaemonDfsMutationAdmission>,
    ) {
        let Some(started) = started else { return };
        let elapsed_us = started.elapsed().as_micros();
        let admitted_scope = match admission {
            Some(admission) if admission.is_global() => "global",
            Some(admission) if admission.is_exact() => "exact",
            Some(_) => "read-only",
            None => "none",
        };
        let (command_scope, fs, shard) = self.scope_labels();
        self.log_span(phase).in_scope(|| {
            daemon_dfs_perf_trace_with_traceparent(self.traceparent, format_args!(
                "{} request={} kind={} attempt={} phase={} outcome={} command_scope={} fs={} shard={} admitted_scope={} elapsed_us={}",
                self.label, self.request, self.kind, self.attempt, phase, outcome,
                command_scope, fs, shard, admitted_scope, elapsed_us,
            ));
        });
    }

    pub(super) fn ready(&self, admission: &DaemonDfsMutationAdmission) {
        if !self.enabled {
            return;
        }
        let admitted_scope = if admission.is_global() {
            "global"
        } else if admission.is_exact() {
            "exact"
        } else {
            "read-only"
        };
        let (command_scope, fs, shard) = self.scope_labels();
        self.log_span("ready").in_scope(|| {
            daemon_dfs_perf_trace_with_traceparent(
                self.traceparent,
                format_args!(
                    "{}-ready request={} kind={} attempt={} command_scope={} fs={} shard={} admitted_scope={}",
                    self.label, self.request, self.kind, self.attempt, command_scope, fs, shard, admitted_scope,
                ),
            );
        });
    }
}

#[cfg(test)]
pub(crate) async fn handle_daemon_dfs_message_async(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
    traceparent: Option<String>,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsDataPlaneResponse, DaemonError> {
    // Direct test callers have no service envelope. Production obtains this
    // length from the already-encoded transport value and never re-encodes it.
    let encoded_value_bytes = crate::dfs_message_rpc_request(&command)?.body.len();
    handle_daemon_dfs_message_with_encoded_size_async(
        paths,
        command,
        attachment,
        traceparent,
        u64::try_from(encoded_value_bytes).ok(),
        runtime,
    )
    .await
}

/// Dispatch the existing owned phase after service/admission release. The
/// registered host joins its actual blocking child even when its caller leaves.
pub(super) async fn dispatch_daemon_dfs_remote_namespace_phase(
    data_dir: &SinomoDataDir,
    phase: Box<crate::source_adoption::RemoteNamespacePhase>,
    runtime: &tokio::runtime::Handle,
) -> Result<Box<crate::source_adoption::CompletedRemoteNamespacePhase>, DaemonError> {
    let task_owner = registered_dfs_runtime_host(data_dir)?.ok_or_else(|| DaemonError::Parse {
        detail: "native source phase has no registered runtime task owner".into(),
    })?;
    let data_owner = require_daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir))?;
    let (result_tx, result_rx) = tokio::sync::oneshot::channel();
    let task = runtime.spawn_blocking(move || {
        let result = phase.dispatch().map(Box::new);
        drop(data_owner);
        let _ = result_tx.send(result);
    });
    task_owner.own_task(task);
    result_rx
        .await
        .map_err(|error| DaemonError::Network {
            address: "native source phase".into(),
            detail: format!("owned source phase ended without a result: {error}"),
        })?
        .map_err(Into::into)
}

pub(crate) async fn handle_daemon_dfs_message_with_encoded_size_async(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
    traceparent: Option<String>,
    encoded_value_bytes: Option<u64>,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsDataPlaneResponse, DaemonError> {
    let data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    let trace_started = Instant::now();
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    // Only authenticated ingress calls this path. Delivery transfers opaque
    // response custody to the exact waiting transport; that service retains
    // source, object and current-authority validation. Do not acquire resident
    // mutation admission or invoke a browser callback while handing it off.
    if attachment.is_none()
        && let sinomo_dfs::DfsNetworkMessage::DistributedWriteResponse(response) =
            &command.routed_message.message
    {
        let local =
            sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths)?;
        if command.routed_message.target_node_id != local.local_node_id {
            return Err(DaemonError::Network {
                address: "DFS message".into(),
                detail: "DFS response target differs from the actual local identity".into(),
            });
        }
        let registered =
            registered_dfs_runtime_host(&data_dir)?.ok_or_else(|| DaemonError::Parse {
                detail: "DFS response delivery requires its actual registered runtime".into(),
            })?;
        let encoded_bytes = encoded_value_bytes
            .and_then(|bytes| usize::try_from(bytes).ok())
            .unwrap_or(usize::MAX);
        if registered
            .response_delivery
            .try_deliver_with_size(&command.routed_message, encoded_bytes)?
        {
            tracing::trace!(
                target: "sinomo.perf",
                stage = "dfs.response.delivery",
                request_id = %response.request_id,
                object_id = %response.object_id,
                rpc_request_id = %command.request_id,
                source_node_id = %command.routed_message.source_node_id,
                encoded_bytes,
                "DFS response custody transferred to registered requester"
            );
            return Ok(DfsDataPlaneResponse::value(crate::DfsMessageResponse {
                request_id: command.request_id,
                outbound_messages: Vec::new(),
            }));
        }
    }
    let peer_snapshot_started = Instant::now();
    let peer_deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
    let Some(peer_snapshot) =
        read_daemon_dfs_message_peer_snapshot_before(&cluster_paths, runtime, peer_deadline)
            .await?
    else {
        return Err(DaemonError::ClusterOrphan);
    };
    let peer_snapshot_elapsed = peer_snapshot_started.elapsed();
    if command.routed_message.target_node_id != peer_snapshot.local_node_id {
        return Err(DaemonError::Network {
            address: "DFS message".to_string(),
            detail: format!(
                "dfs message target {} does not match local node {}",
                command.routed_message.target_node_id, peer_snapshot.local_node_id
            ),
        });
    }
    let uses_content_read_lane = daemon_dfs_message_uses_content_read_lane(&command);
    let mut metadata_authority_preflight = if !uses_content_read_lane {
        // Complete or join authority I/O before even the shared content-store
        // lookup below can take the process-wide DFS cache mutex. Attachment
        // ingest may perform durable local I/O, but it never owns authority.
        Some(
            warm_daemon_dfs_metadata_authority_for_peer_snapshot_before(
                &cluster_paths,
                &peer_snapshot,
                runtime,
                peer_deadline,
                #[cfg(test)]
                Some(&command),
            )
            .await?,
        )
    } else {
        None
    };
    let content = if attachment.is_some() {
        let content_data_dir = data_dir.clone();
        let content_cluster_paths = cluster_paths.clone();
        let content_peer_snapshot = peer_snapshot.clone();
        let content_runtime = runtime.clone();
        #[cfg(test)]
        let content_observed_command =
            daemon_dfs_pre_dispatch_test_hook(&data_dir).map(|_| command.clone());
        let content_traceparent = traceparent.clone();
        let content_data_owner_lock = Arc::clone(&data_owner_lock);
        let content_span = tracing::trace_span!(target: "sinomo.perf",
            "dfs.message.content_store", stage = "dfs.message.content_store",
            traceparent = traceparent.as_deref().unwrap_or_default());
        // A cache miss can open durable stores, and a hit can wait behind a
        // generation opening them. Neither may block the RPC progress driver.
        // This task retains the data-root capability even if its caller leaves.
        Some(
            tokio::task::spawn_blocking(move || {
                let _entered = content_span.enter();
                let _data_owner_lock = content_data_owner_lock;
                sinomo_dfs::with_dfs_traceparent(content_traceparent.as_deref(), || {
                    daemon_dfs_message_content_store(
                        &content_data_dir,
                        &content_cluster_paths,
                        &content_peer_snapshot,
                        &content_runtime,
                        #[cfg(test)]
                        content_observed_command.as_ref(),
                    )
                })
            })
            .await
            .map_err(|source| DaemonError::Network {
                address: "DFS message".to_string(),
                detail: format!("DFS message content-store task failed: {source}"),
            })??,
        )
    } else {
        None
    };
    let (mut command, mut preingested_blob) =
        crate::runtime_message::ingest_dfs_message_attachment_with_owner(
            content,
            Some(data_dir.root()),
            command,
            attachment,
            Arc::clone(&data_owner_lock),
            traceparent.clone(),
        )
        .await?;
    // Keep the existing outer RPC future and cancellation/timeout scope. This
    // function receives no caller deadline and starts no new request deadline.
    // Only typed rejection before execution permits at most two async refreshes.
    let mut authority_refresh_retries = 0;
    let mut completed_remote_phase: Option<
        Box<crate::source_adoption::CompletedRemoteNamespacePhase>,
    > = None;
    loop {
        let verified_fetch = if matches!(
            &command.routed_message.message,
            sinomo_dfs::DfsNetworkMessage::FetchBlob(_)
        ) && let Some(preflight) = metadata_authority_preflight.as_ref()
        {
            let proof_data_dir = data_dir.clone();
            let proof_paths = cluster_paths.clone();
            let proof_peer = peer_snapshot.clone();
            let proof_command = command.clone();
            let proof_preflight = preflight.clone();
            let proof_runtime = runtime.clone();
            let proof_owner = Arc::clone(&data_owner_lock);
            tokio::task::spawn_blocking(move || {
                prepare_daemon_dfs_verified_blob_fetch(
                    &proof_data_dir,
                    &proof_paths,
                    &proof_peer,
                    &proof_command,
                    &proof_preflight,
                    &proof_runtime,
                )
                .map(|proof| proof.map(|proof| proof.with_response_owner(proof_owner)))
            })
            .await
            .map_err(|source| DaemonError::Network {
                address: "DFS message".to_string(),
                detail: format!("DFS immutable fetch proof task failed: {source}"),
            })??
        } else {
            None
        };
        let uses_content_read_lane = verified_fetch.is_some() || uses_content_read_lane;
        let request_id = command.request_id.clone();
        let source_node_id = command.routed_message.source_node_id.clone();
        let target_node_id = command.routed_message.target_node_id.clone();
        let frame_id = command.routed_message.frame_id;
        let message_id = command.routed_message.message_id;
        let message_kind = daemon_dfs_message_kind(&command.routed_message.message);
        let execution_scope = dfs_message_execution_scope(&command);
        let execution_scope_label = match &execution_scope {
            DfsMessageExecutionScope::ReadOnly => "read-only".to_string(),
            DfsMessageExecutionScope::ExactShard {
                file_system_id,
                shard_id,
            } => format!("shard:{file_system_id}:{}", shard_id.0),
            DfsMessageExecutionScope::NamespacePaths { .. } => "namespace-global".to_string(),
            DfsMessageExecutionScope::GlobalExclusive => "global".to_string(),
        };
        if authority_refresh_retries == 0 {
            daemon_dfs_perf_trace_with_traceparent(
                traceparent.as_deref(),
                format_args!(
                    "dfs-message-handler-start request={} kind={} source={} target={} frame_id={} message_id={}",
                    request_id, message_kind, source_node_id, target_node_id, frame_id, message_id
                ),
            );
        }
        let process_data_dir = data_dir.clone();
        let process_cluster_paths = cluster_paths.clone();
        let process_peer_snapshot = peer_snapshot.clone();
        let process_runtime = runtime.clone();
        let process_traceparent = traceparent.clone();
        let process_metadata_authority_preflight = metadata_authority_preflight.take();
        let process_completed_phase = completed_remote_phase.take();
        let process_preingested_blob = preingested_blob.take();
        let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
        let exact_service_lane_candidate = daemon_dfs_message_supports_exact_service_lane(&command)
            && DaemonDfsMutationLaneKey::from_scope(&execution_scope).is_some();
        let exact_namespace_read_command = match &command.routed_message.message {
            sinomo_dfs::DfsNetworkMessage::NamespaceRequest(namespace)
                if matches!(
                    namespace.operation,
                    sinomo_dfs::DfsNamespaceOperation::PollNamespaceChanges { .. }
                        | sinomo_dfs::DfsNamespaceOperation::LookupPath { .. }
                        | sinomo_dfs::DfsNamespaceOperation::LookupPaths { .. }
                ) =>
            {
                Some(namespace)
            }
            _ => None,
        };
        let namespace_change_poll = exact_namespace_read_command.is_some_and(|namespace| {
            matches!(
                namespace.operation,
                sinomo_dfs::DfsNamespaceOperation::PollNamespaceChanges { .. }
            )
        });
        let read_only_exact_service_lane_candidate =
            exact_service_lane_candidate && exact_namespace_read_command.is_some();
        if namespace_change_poll && !exact_service_lane_candidate {
            return Err(sinomo_dfs::DfsError::Metadata {
                message: "namespace change poll requires one complete exact-shard authority stamp"
                    .to_string(),
            }
            .into());
        }
        let admission_scope = if uses_content_read_lane {
            DfsMessageExecutionScope::ReadOnly
        } else if exact_service_lane_candidate {
            execution_scope.clone()
        } else {
            // Metadata-assisted reads, coordinator-bearing mutations, cache
            // initialization, and topology rollover all borrow the one global
            // resident generation. This prevents a second decoded backend and
            // ensures an obsolete exact stamp reaches service-level validation.
            DfsMessageExecutionScope::GlobalExclusive
        };
        let admission_trace = DaemonDfsAdmissionTrace {
            label: "dfs-message-admission",
            request: &request_id,
            kind: message_kind,
            attempt: authority_refresh_retries,
            scope: &execution_scope,
            traceparent: traceparent.as_deref(),
            enabled: tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE),
        };
        let admission_started = Instant::now();
        let scheduler_started = admission_trace.start();
        let preempt_maintenance =
            daemon_dfs_message_preempts_maintenance(&command.routed_message.message);
        let admitted = tracing::Instrument::instrument(
            admit_daemon_dfs_message(&scheduler, &admission_scope, preempt_maintenance),
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
        let mut admission = admitted?;
        let cache_started = (exact_service_lane_candidate && admission.is_exact())
            .then(|| admission_trace.start())
            .flatten();
        let exact = if exact_service_lane_candidate && admission.is_exact() {
            let exact_data_dir = data_dir.clone();
            let exact_cluster_paths = cluster_paths.clone();
            let exact_peer_snapshot = peer_snapshot.clone();
            let exact_runtime = runtime.clone();
            #[cfg(test)]
            let exact_observed_command =
                daemon_dfs_pre_dispatch_test_hook(&data_dir).map(|_| command.clone());
            let exact_namespace = exact_namespace_read_command.cloned();
            let exact_scope = execution_scope.clone();
            let exact_traceparent = traceparent.clone();
            let exact_data_owner_lock = Arc::clone(&data_owner_lock);
            let exact_span = admission_trace.span("exact-cache");
            // Eligibility can wait on the same generation cache as cold-open.
            // Keep its existing admission in the worker during cancellation;
            // it must not be released while the worker still reads the cache.
            let (exact, retained_admission) = tokio::task::spawn_blocking(move || {
                let _entered = exact_span.enter();
                let _data_owner_lock = exact_data_owner_lock;
                let exact = sinomo_dfs::with_dfs_traceparent(exact_traceparent.as_deref(), || {
                    if let Some(namespace) = exact_namespace.as_ref() {
                        daemon_dfs_message_cache_exact_namespace_read_admission_is_current(
                            &exact_data_dir,
                            &exact_cluster_paths,
                            &exact_peer_snapshot,
                            namespace,
                            &exact_runtime,
                            #[cfg(test)]
                            exact_observed_command.as_ref(),
                        )
                    } else {
                        match DaemonDfsMutationLaneKey::from_scope(&exact_scope) {
                            Some(key) => daemon_dfs_message_cache_exact_admission_is_current(
                                &exact_data_dir,
                                &exact_cluster_paths,
                                &exact_peer_snapshot,
                                &key,
                                &exact_runtime,
                                #[cfg(test)]
                                exact_observed_command.as_ref(),
                            ),
                            None => Ok(false),
                        }
                    }
                });
                (exact, admission)
            })
            .await
            .map_err(|source| DaemonError::Network {
                address: "DFS message".to_string(),
                detail: format!("DFS message exact-cache task failed: {source}"),
            })?;
            admission = retained_admission;
            exact
        } else {
            Ok(false)
        };
        admission_trace.record(
            "exact-cache",
            match &exact {
                Ok(true) => "eligible",
                Ok(false) => "ineligible",
                Err(_) => "error",
            },
            cache_started,
            Some(&admission),
        );
        let mut exact_service_lane = exact?;
        if exact_service_lane_candidate && !exact_service_lane {
            if namespace_change_poll {
                drop(admission);
                return Err(sinomo_dfs::DfsError::Metadata {
                message: "namespace change poll authority is stale, foreign, or not already resident; refusing to cold-open metadata"
                    .to_string(),
            }
            .into());
            }
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
                admission = upgraded?;
            }
            if read_only_exact_service_lane_candidate {
                // A cold or not-fully-validated lookup stays on the global service
                // which owns topology rollover and structured stamp rejection. It
                // must not create/hydrate an exact shard lane from wire identity.
                exact_service_lane = false;
            } else {
                // Mutations retain the existing behavior: global admission can
                // initialize or roll the resident generation, then the proven
                // exact replica/namespace command runs through its shard shell.
                exact_service_lane = true;
            }
        }
        admission_trace.ready(&admission);
        let allow_generation_rollover = admission.is_global();
        let admission_wait = admission_started.elapsed();
        let process_execution_scope = execution_scope.clone();
        let spawn_started = Instant::now();
        let blocking_queue_started = spawn_started;
        let process_data_owner_lock = Arc::clone(&data_owner_lock);
        #[cfg(test)]
        let admission_observation_hook = daemon_dfs_pre_dispatch_test_hook(&data_dir);
        #[cfg(test)]
        let admission_observed_command =
            admission_observation_hook.as_ref().map(|_| command.clone());
        let blocking_span = tracing::trace_span!(target: "sinomo.perf", "dfs.message.blocking",
            stage = "dfs.message.blocking", traceparent = traceparent.as_deref().unwrap_or_default());
        let queue_span = tracing::trace_span!(target: "sinomo.perf", parent: &blocking_span,
            "dfs.message.queue", stage = "dfs.message.queue");
        let (result, blocking_queue, blocking_work) = tokio::task::spawn_blocking(move || {
            let _entered = blocking_span.enter();
            drop(queue_span);
            let _work = tracing::trace_span!(target: "sinomo.perf", "dfs.message.work",
                stage = "dfs.message.work")
            .entered();
            // The async connection task and runtime shutdown may abandon this
            // JoinHandle while blocking work continues. Retain the OS-backed data
            // lock inside the blocking closure so no replacement daemon can prove
            // this placement-authority session dead prematurely.
            let _data_owner_lock = process_data_owner_lock;
            let mut mutation_admission = Some(admission);
            let blocking_started = Instant::now();
            let blocking_queue = blocking_queue_started.elapsed();
            let result = sinomo_dfs::with_dfs_traceparent(process_traceparent.as_deref(), || {
                attempt_daemon_dfs_message_command_with_remote_phase(
                    &process_data_dir,
                    &process_cluster_paths,
                    &process_peer_snapshot,
                    command,
                    process_preingested_blob,
                    verified_fetch,
                    &process_execution_scope,
                    exact_service_lane,
                    allow_generation_rollover,
                    process_metadata_authority_preflight,
                    peer_snapshot_elapsed,
                    &mut mutation_admission,
                    &process_runtime,
                    process_traceparent.clone(),
                    process_completed_phase.map(|completed| *completed),
                )
            });
            // A blocking task keeps running when its async JoinHandle is dropped.
            // Own admission here so a disconnected/cancelled RPC cannot release
            // the shard (or global) guard before the actual DFS mutation stops.
            #[cfg(test)]
            {
                // Observe the actual guard's final owning value through Weak.
                // An absent value is not proof of another owner's release.
                if let Some(hook) = admission_observation_hook {
                    let actual_admission = mutation_admission.take().map(Arc::new);
                    let previous_admission = actual_admission.as_ref().map(Arc::downgrade);
                    drop(actual_admission);
                    let released = previous_admission
                        .as_ref()
                        .is_some_and(|admission| admission.upgrade().is_none());
                    if let Err(error) = hook(
                        DaemonDfsPreDispatchTestPoint::ReplicaAdmissionReleased { released },
                        admission_observed_command.as_ref(),
                        None,
                    ) {
                        return (Err(error), blocking_queue, blocking_started.elapsed());
                    }
                }
            }
            drop(mutation_admission);
            (result, blocking_queue, blocking_started.elapsed())
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "DFS message".to_string(),
            detail: format!("DFS message task failed: {source}"),
        })?;
        let result = match result? {
            DaemonDfsMessageDispatchAttempt::Complete(result) => result,
            DaemonDfsMessageDispatchAttempt::RemoteNamespacePhase(original, phase) => {
                // The selected service mutex, checkout and mutation admission
                // have all ended. Only the actual source fences/owner remain.
                completed_remote_phase = Some(
                    dispatch_daemon_dfs_remote_namespace_phase(&data_dir, phase, runtime).await?,
                );
                command = original;
                metadata_authority_preflight = Some(
                    warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
                        &cluster_paths,
                        &peer_snapshot,
                        runtime,
                    )
                    .await?,
                );
                continue;
            }
            DaemonDfsMessageDispatchAttempt::AuthorityPreflightStale(retry) => {
                let DaemonDfsUnexecutedMessage {
                    stale,
                    command: retry_command,
                    preingested_blob: retry_blob,
                } = *retry;
                if authority_refresh_retries >= 2 {
                    return Err(stale.into_dfs_error().into());
                }
                // The blocking closure dropped mutation_admission before returning
                // this still-unexecuted command. Refresh performs no I/O under an
                // old exact/global admission, and the next loop acquires it afresh.
                command = retry_command;
                preingested_blob = retry_blob;
                authority_refresh_retries += 1;
                daemon_dfs_perf_trace_with_traceparent(
                    traceparent.as_deref(),
                    format_args!(
                        "dfs-message-authority-preflight-retry request={} expected_generation={} current_generation={} current_ready={} retry={} maximum=2",
                        request_id,
                        stale.expected_generation,
                        stale.current_generation,
                        stale.current_ready,
                        authority_refresh_retries,
                    ),
                );
                #[cfg(test)]
                if let Some(hook) = daemon_dfs_pre_dispatch_test_hook(&data_dir) {
                    hook(
                        DaemonDfsPreDispatchTestPoint::BeforeAuthorityRefresh {
                            owner: "message",
                            replan: authority_refresh_retries,
                        },
                        Some(&command),
                        None,
                    )?;
                }
                metadata_authority_preflight = Some(
                    warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
                        &cluster_paths,
                        &peer_snapshot,
                        runtime,
                    )
                    .await?,
                );
                continue;
            }
        };
        let spawn_blocking = spawn_started.elapsed();
        let Some(result) = result else {
            return Err(DaemonError::ClusterOrphan);
        };
        let DaemonDfsMessageCommandProcessResult {
            response,
            attachment,
            committed_edges,
        } = result;
        daemon_dfs_perf_trace_with_traceparent(
            traceparent.as_deref(),
            format_args!(
                "dfs-message-handler request={} scope={} admission_wait_ms={} admission_wait_us={} blocking_queue_ms={} blocking_queue_us={} blocking_work_ms={} blocking_work_us={} spawn_blocking_ms={} spawn_blocking_us={} publish_metadata={} repair_maintenance={} namespace_maintenance={} outbound_messages={} total_ms={}",
                request_id,
                execution_scope_label,
                admission_wait.as_millis(),
                admission_wait.as_micros(),
                blocking_queue.as_millis(),
                blocking_queue.as_micros(),
                blocking_work.as_millis(),
                blocking_work.as_micros(),
                spawn_blocking.as_millis(),
                spawn_blocking.as_micros(),
                committed_edges.publish_metadata,
                committed_edges.repair_maintenance,
                committed_edges.namespace_maintenance,
                response.outbound_messages.len(),
                trace_started.elapsed().as_millis()
            ),
        );
        return Ok(DfsDataPlaneResponse {
            response,
            attachment,
        });
    }
}

#[cfg(test)]
pub(crate) fn handle_daemon_dfs_message_on_runtime(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsMessageResponse, DaemonError> {
    handle_daemon_dfs_message_on_runtime_with_priority(paths, command, runtime, false, None)
}

/// A detached worker has matched this return to its authenticated peer and
/// original background request. Keep ordinary receipt/authority validation,
/// while re-admitting that producer's completion without foreground preemption.
fn handle_daemon_dfs_message_on_runtime_with_background_response(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    runtime: &tokio::runtime::Handle,
    original: Option<&sinomo_dfs::DfsRoutedMessage>,
) -> Result<sinomo_dfs::DfsMessageResponse, DaemonError> {
    let _data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    handle_daemon_dfs_message_on_runtime_with_priority(paths, command, runtime, true, original)
}

/// Route DATA from an actual locally owned original request. This supplies no
/// author capability: current intent/queue/policy and native kernel guards still
/// validate the reply and publication under the selected ordinary shard lease.
pub(super) fn owned_repair_completion_scope(
    original: &sinomo_dfs::DfsRoutedMessage,
    returned: &sinomo_dfs::DfsRoutedMessage,
    local: &NodeId,
    shards: &[sinomo_dfs::DfsMetadataShardConfig],
) -> Result<DfsMessageExecutionScope, sinomo_dfs::DfsError> {
    let invalid = || sinomo_dfs::DfsError::Metadata {
        message: "owned repair completion does not match its current original request/assignment"
            .into(),
    };
    let (
        sinomo_dfs::DfsNetworkMessage::RepairReplica(request),
        sinomo_dfs::DfsNetworkMessage::RepairReplicaResponse(response),
    ) = (&original.message, &returned.message)
    else {
        return Err(invalid());
    };
    let assignment = request.authority_shard.as_ref().ok_or_else(invalid)?;
    let object = request.object_metadata.as_ref().ok_or_else(invalid)?;
    if !request.background_repair
        || &original.source_node_id != local
        || &returned.target_node_id != local
        || returned.source_node_id != original.target_node_id
        || response.replica_node_id != original.target_node_id
        || response.request_id != request.request_id
        || response.intent_id != request.intent_id
        || response.file_system_id != request.file_system_id
        || response.object_id != request.object_id
        || response.version != request.version
        || assignment.file_system_id != request.file_system_id
        || &assignment.primary_node_id != local
        || assignment.shard_id.trim().is_empty()
        || object.file_system_id != request.file_system_id
        || object.object_id != request.object_id
    {
        return Err(invalid());
    }
    if matches!(&response.result,
        sinomo_dfs::DfsBlobTransferResult::Committed { content_hash }
            | sinomo_dfs::DfsBlobTransferResult::Durable { content_hash }
            if content_hash != &request.content_hash)
    {
        return Err(invalid());
    }
    let selected = sinomo_dfs::select_metadata_shard_config_for_path_strict_default_sifs_split(
        &request.file_system_id,
        shards,
        &object.path,
    )?
    .ok_or_else(invalid)?;
    if selected != *assignment {
        return Err(invalid());
    }
    Ok(DfsMessageExecutionScope::ExactShard {
        file_system_id: assignment.file_system_id.clone(),
        shard_id: sinomo_dfs::DfsMetadataShardId::new(&assignment.shard_id),
    })
}

fn handle_daemon_dfs_message_on_runtime_with_priority(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    runtime: &tokio::runtime::Handle,
    background_response: bool,
    original_repair: Option<&sinomo_dfs::DfsRoutedMessage>,
) -> Result<sinomo_dfs::DfsMessageResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let mut execution_scope = dfs_message_execution_scope(&command);
    let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)? else {
        return Err(DaemonError::ClusterOrphan);
    };
    let metadata_authority_preflight = if daemon_dfs_message_uses_content_read_lane(&command) {
        None
    } else {
        Some(
            runtime.block_on(warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
                &cluster_paths,
                &peer_snapshot,
                runtime,
            ))?,
        )
    };
    if let Some(original) = original_repair {
        let preflight = metadata_authority_preflight.as_ref().ok_or_else(|| {
            sinomo_dfs::DfsError::Metadata {
                message: "owned repair completion has no current retained configuration".into(),
            }
        })?;
        daemon_dfs_require_retained_authority(&preflight.authority, preflight.generation)
            .map_err(DaemonDfsAuthorityPreExecutionError::into_dfs_error)?;
        let projection_generation = preflight.authority.projection_generation();
        let shards = preflight
            .authority
            .cached_metadata_shards(
                &match &command.routed_message.message {
                    sinomo_dfs::DfsNetworkMessage::RepairReplicaResponse(response) => {
                        response.file_system_id.clone()
                    }
                    _ => {
                        return Err(sinomo_dfs::DfsError::Metadata {
                            message:
                                "owned repair completion provenance names another reply family"
                                    .into(),
                        }
                        .into());
                    }
                },
                projection_generation,
            )?
            .ok_or_else(|| sinomo_dfs::DfsError::Metadata {
                message: "owned repair completion current configuration projection is unavailable"
                    .into(),
            })?;
        execution_scope = owned_repair_completion_scope(
            original,
            &command.routed_message,
            &peer_snapshot.local_node_id,
            &shards,
        )?;
        if preflight.authority.projection_generation() != projection_generation {
            return Err(sinomo_dfs::DfsError::Metadata {
                message: "owned repair completion configuration changed before admission".into(),
            }
            .into());
        }
        daemon_dfs_require_retained_authority(&preflight.authority, preflight.generation)
            .map_err(DaemonDfsAuthorityPreExecutionError::into_dfs_error)?;
    }
    let mut mutation_admission = if background_response {
        let scheduler = daemon_dfs_mutation_scheduler(&data_dir)?;
        Some(runtime.block_on(admit_daemon_dfs_message(
            &scheduler,
            &execution_scope,
            false,
        ))?)
    } else {
        None
    };
    let result = process_daemon_dfs_message_command_on_runtime(
        &data_dir,
        &cluster_paths,
        &peer_snapshot,
        command,
        None,
        &execution_scope,
        original_repair.is_some(),
        original_repair.is_none(),
        metadata_authority_preflight,
        Duration::ZERO,
        &mut mutation_admission,
        runtime,
        None,
    )?;
    let Some(result) = result else {
        return Err(DaemonError::ClusterOrphan);
    };
    let DaemonDfsMessageCommandProcessResult {
        response: message_response,
        attachment: _,
        committed_edges: _,
    } = result;
    Ok(message_response)
}
