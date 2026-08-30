use super::*;

pub(crate) fn daemon_dfs_message_service_cache()
-> &'static Mutex<BTreeMap<PathBuf, DaemonDfsMessageServiceCacheEntry>> {
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
    let cache = match daemon_dfs_message_service_cache().try_lock() {
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
    let entry = cache.get(&data_dir.root()).ok_or_else(|| {
        sinomo_dfs::DfsError::Network {
            message:
                "bounded DFS status unavailable because no resident metadata generation has been published"
                    .to_string(),
        }
    })?;
    entry.bounded_status_resident(peer_snapshot)
}

pub(crate) fn register_daemon_dfs_observe_metric_exporter() {
    sinomo_observe::register_observe_metric_exporter_with_prefixes(
        "sinomo-dfs",
        ["sinomo.dfs."],
        |data_dir| {
            let (resident, local_node_id) = {
                let cache = daemon_dfs_message_service_cache().try_lock().map_err(|error| {
                    sinomo_observe::ObserveError::Runtime {
                        detail: format!(
                            "resident DFS observe exporter could not borrow its generation cache: {error}"
                        ),
                    }
                })?;
                let entry = cache.get(&data_dir.root()).ok_or_else(|| {
                    sinomo_observe::ObserveError::Runtime {
                        detail:
                            "resident DFS observe exporter has no published metadata generation"
                                .to_string(),
                    }
                })?;
                let resident = entry.bounded_status_resident_current().map_err(|error| {
                    sinomo_observe::ObserveError::Runtime {
                        detail: format!("resident DFS observe exporter is unavailable: {error}"),
                    }
                })?;
                (resident, entry.signature.local_node_id.clone())
            };
            let snapshot = resident.bounded_status_snapshot().map_err(|error| {
                sinomo_observe::ObserveError::Runtime {
                    detail: format!("resident DFS observe snapshot is unavailable: {error}"),
                }
            })?;
            let status = sinomo_dfs::project_dfs_status_from_resident_snapshot(
                Some(&local_node_id),
                snapshot,
            );
            Ok(sinomo_observe::dfs_status_observe_metrics(status))
        },
    );
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
    let mut cache =
        daemon_dfs_message_service_cache()
            .lock()
            .map_err(|_| DaemonError::Network {
                address: "DFS message service cache".to_string(),
                detail: "DFS message service cache is poisoned".to_string(),
            })?;
    let removed = cache.remove(&cache_key).is_some();
    if removed {
        tracing::info!(
            data_root = %cache_key.display(),
            "released daemon DFS message service before mandatory lifecycle work"
        );
    }
    Ok(removed)
}

pub(crate) fn take_daemon_dfs_message_mutation_service_for_maintenance(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    cluster_state: &sinomo_core::ClusterBootstrapState,
    runtime: &tokio::runtime::Handle,
    metadata_authority_preflight: Option<&DaemonDfsMetadataAuthorityPreflight>,
) -> Result<DaemonDfsMaintenanceServiceLane, sinomo_dfs::DfsError> {
    let peer_node_ids =
        sinomo_dfs::discovered_cluster_dfs_peer_node_ids(cluster_paths, cluster_state)?;
    let metadata_authority_generation = match metadata_authority_preflight {
        Some(preflight)
            if preflight.authority.generation() == preflight.generation
                && preflight
                    .authority
                    .retained_complete_prefix_revision_for_generation(preflight.generation)
                    .is_some() =>
        {
            Arc::clone(&preflight.authority)
        }
        Some(preflight) => {
            return Err(sinomo_dfs::DfsError::Network {
                message: format!(
                    "DFS metadata authority advanced after maintenance preflight of generation {}",
                    preflight.generation
                ),
            });
        }
        None => daemon_dfs_metadata_authority_generation_for_identity(
            cluster_paths,
            &cluster_state.cluster_id,
            &cluster_state.local_node_id,
            Some(runtime),
        )?,
    };
    let signature = DaemonDfsMessageServiceSignature {
        cluster_id: cluster_state.cluster_id.clone(),
        local_node_id: cluster_state.local_node_id.clone(),
        metadata_authority_generation: metadata_authority_generation.generation(),
    };
    let cache_key = data_dir.root();
    let cached = {
        let mut cache = daemon_dfs_message_service_cache()
            .lock()
            .map_err(|_| daemon_dfs_cache_lock_error())?;
        let stale = cache.get(&cache_key).is_some_and(|entry| {
            entry.signature != signature
                || !Arc::ptr_eq(
                    &entry.metadata_authority_generation,
                    &metadata_authority_generation,
                )
        });
        let retained_content_store = if stale {
            cache.remove(&cache_key).map(|entry| entry.content_store)
        } else {
            None
        };
        if !cache.contains_key(&cache_key) {
            let content_store = match retained_content_store {
                Some(content_store) => content_store,
                None => sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?,
            };
            let resident_metadata_backend =
                open_daemon_dfs_resident_metadata_backend_for_authority_generation(
                    data_dir,
                    &signature.local_node_id,
                    &metadata_authority_generation,
                    signature.metadata_authority_generation,
                )?;
            cache.insert(
                cache_key.clone(),
                DaemonDfsMessageServiceCacheEntry {
                    signature: signature.clone(),
                    metadata_authority_generation: Arc::clone(&metadata_authority_generation),
                    peer_node_ids: peer_node_ids.clone(),
                    content_store,
                    resident_metadata_backend,
                    resident_metadata_initialized: true,
                    cross_shard_namespace_locks: Default::default(),
                    primary_service: None,
                    exact_shard_services: BTreeMap::new(),
                    content_read_service: None,
                },
            );
        }
        let entry = cache
            .get_mut(&cache_key)
            .expect("daemon DFS message cache entry inserted");
        entry.peer_node_ids.clone_from(&peer_node_ids);
        if !entry.resident_metadata_initialized {
            entry.resident_metadata_backend =
                open_daemon_dfs_resident_metadata_backend_for_authority_generation(
                    data_dir,
                    &entry.signature.local_node_id,
                    &entry.metadata_authority_generation,
                    entry.signature.metadata_authority_generation,
                )?;
            entry.resident_metadata_initialized = true;
        }
        (
            entry.primary_service.take(),
            entry.content_store.clone(),
            entry.resident_metadata_backend.clone(),
            entry.cross_shard_namespace_locks.clone(),
        )
    };

    let (cached, content_store, resident_metadata_backend, cross_shard_namespace_locks) = cached;
    let handle = match cached {
        Some(handle) => handle,
        None => open_daemon_dfs_message_service_lane_with_content(
            data_dir,
            cluster_paths,
            &signature.local_node_id,
            peer_node_ids.clone(),
            runtime,
            content_store,
            Some(
                metadata_authority_generation
                    .load_complete_prefix_for_generation(
                        signature.metadata_authority_generation,
                    )
                    .ok_or_else(|| sinomo_dfs::DfsError::Network {
                        message: "daemon DFS maintenance lane awaits a complete retained authority prefix"
                            .to_string(),
                    })?,
            ),
            resident_metadata_backend.map(DaemonDfsResidentMetadataScope::Global),
            cross_shard_namespace_locks,
        )?,
    };
    let handle = Arc::try_unwrap(handle).map_err(|_| sinomo_dfs::DfsError::Network {
        message:
            "daemon DFS mutation lane remained referenced after exclusive maintenance admission"
                .to_string(),
    })?;
    let (lane, detached_send_runtime) = handle.into_parts();
    let mut lane = lane
        .into_inner()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    lane.refresh_peer_targets(peer_node_ids.clone());
    if let Some(scope) = lane.resident_metadata_scope.clone() {
        scope.attach(&mut lane.service)?;
    }
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
    let current_signature = DaemonDfsMessageServiceSignature {
        cluster_id: peer_snapshot.cluster_id,
        local_node_id: peer_snapshot.local_node_id,
        metadata_authority_generation: metadata_authority_generation.generation(),
    };
    if current_signature != retained.signature
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

    let cache_key = data_dir.root();
    let mut cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let Some(entry) = cache.get_mut(&cache_key) else {
        return Ok(false);
    };
    if entry.signature != retained.signature
        || !Arc::ptr_eq(
            &entry.metadata_authority_generation,
            &retained.metadata_authority_generation,
        )
        || entry.primary_service.is_some()
    {
        return Ok(false);
    }
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

pub(crate) fn open_daemon_dfs_resident_metadata_backend_for_authority_generation(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    authority_generation: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    expected_generation: u64,
) -> Result<Option<ResidentLocalShardedMetadataBackend>, sinomo_dfs::DfsError> {
    let projection = authority_generation
        .load_complete_prefix_for_generation(expected_generation)
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS resident metadata generation {expected_generation} lacks a complete retained authority projection"
            ),
        })?;
    open_daemon_dfs_resident_metadata_backend_from_authority_projection(
        data_dir,
        local_node_id,
        authority_generation,
        expected_generation,
        &projection,
    )
}

pub(crate) fn open_daemon_dfs_resident_metadata_backend_from_authority_projection(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    authority_generation: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    expected_generation: u64,
    projection: &sinomo_dfs::DfsAuthoritativePrefixProjection,
) -> Result<Option<ResidentLocalShardedMetadataBackend>, sinomo_dfs::DfsError> {
    let shards = daemon_dfs_metadata_shards_from_authority_projection(
        projection,
        &sinomo_dfs::sifs_file_system_id(),
    )?;
    if shards.is_empty() {
        return Ok(None);
    }
    let backend = sinomo_dfs::open_local_sharded_metadata_backend_for_replica_repair(
        sinomo_dfs::default_dfs_metadata_shards_root(data_dir),
        shards,
        local_node_id,
    )?
    .into_resident();
    backend.initialize_bounded_status_snapshots()?;
    if authority_generation.generation() != expected_generation {
        return Err(sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS metadata authority advanced while opening resident generation {expected_generation}"
            ),
        });
    }
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
    if let Some(host) = daemon_dfs_metadata_authority_host(cluster_paths, runtime) {
        let _ = generation.attach_host(host);
    }
    if !created {
        return Ok(generation);
    }
    let delta_generation = Arc::downgrade(&generation);
    let delta_subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_deltas(
        cluster_paths,
        move |delta| {
            let Some(generation) = delta_generation.upgrade() else {
                return;
            };
            if generation.observe_delta(delta) {
                return;
            }
            tracing::warn!(
                target: "sinomo.dfs.metadata_authority",
                revision = delta.revision.0,
                "DFS metadata-authority committed-delta gap invalidated exact admission; scheduling one bounded authoritative refresh"
            );
            generation.request_authoritative_refresh();
        },
    );
    if generation
        .attach_delta_subscription(delta_subscription)
        .is_err()
    {
        return Err(sinomo_dfs::DfsError::Network {
            message:
                "new DFS metadata-authority generation already had a committed-delta subscription"
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

pub(crate) fn read_daemon_dfs_message_peer_snapshot(
    cluster_paths: &ClusterPaths,
) -> Result<Option<DaemonDfsMessagePeerSnapshot>, sinomo_dfs::DfsError> {
    sinomo_dfs::read_dfs_runtime_message_peer_snapshot(
        cluster_paths,
        DAEMON_DFS_PEER_DISCOVERY_VALIDATION_LEASE,
    )
}

pub(crate) fn schedule_daemon_dfs_message_deferred_work(
    tokio_runtime: &tokio::runtime::Handle,
    cluster_paths: &ClusterPaths,
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    worker: DfsNodeDetachedSendWorker,
) {
    let deferred_target_node_ids =
        sinomo_core::cluster::read_cluster_state_without_kv(cluster_paths)
            .map(|state| daemon_dfs_offline_node_ids(cluster_paths, &state))
            // Unknown/unreadable cluster state remains fail-open. Durable
            // detached work must never be discarded because a local status
            // projection is temporarily unavailable.
            .unwrap_or_default();
    schedule_daemon_dfs_message_deferred_work_with_policy(
        tokio_runtime,
        runtime,
        worker,
        deferred_target_node_ids,
    );
}

pub(crate) fn schedule_daemon_dfs_message_deferred_work_with_policy(
    tokio_runtime: &tokio::runtime::Handle,
    runtime: Arc<DaemonDfsDetachedSendRuntime>,
    worker: DfsNodeDetachedSendWorker,
    deferred_target_node_ids: BTreeSet<NodeId>,
) {
    runtime.schedule_deferred_work(
        worker,
        deferred_target_node_ids,
        DAEMON_DFS_PROMOTION_RETRY_DELAY,
        |task| {
            let _deferred_work = tokio_runtime.spawn(task);
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
    let transport =
        DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes_and_detached_state(
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
    let transport =
        DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes_and_detached_state(
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

/// Rebuild the process-local promotion outbox once per observed authority
/// generation after restart, then prepare the detached retry worker.
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
    let authority_generation = daemon_dfs_retained_metadata_authority_generation(
        detached_send_runtime.as_ref(),
        cluster_paths,
        &cluster_state.cluster_id,
        &cluster_state.local_node_id,
        Some(runtime),
    )?
    .generation();
    let recovery_scan_required =
        detached_send_runtime.recovery_scan_generation() != authority_generation;
    let peer_node_ids =
        sinomo_dfs::discovered_cluster_dfs_peer_node_ids(cluster_paths, &cluster_state)?;
    let open_recovery_service = || {
        let content = sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?;
        let (peer_transport, async_peer_transport) =
            daemon_dfs_peer_transports_on_runtime(data_dir, runtime);
        let transport =
            DfsNodeServedMessageTransport::new_with_peer_transports_for_nodes_and_detached_state(
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
                detached_send_runtime.record_recovery_scan_generation(authority_generation);
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
        "commit-promotion-recovery generation={} scanned={} scan_deferred_empty_topology={} scan_ms={} prepared_pending={}",
        authority_generation,
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

    let cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let Some(entry) = cache.get(&data_dir.root()) else {
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

    let cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let Some(entry) = cache.get(&data_dir.root()) else {
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
) -> Result<bool, sinomo_dfs::DfsError> {
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
) -> Result<bool, sinomo_dfs::DfsError> {
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

    let cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let Some(entry) = cache.get(&data_dir.root()) else {
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

pub(crate) fn daemon_dfs_global_message_lane(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    runtime: &tokio::runtime::Handle,
    entry: &mut DaemonDfsMessageServiceCacheEntry,
) -> Result<Arc<DaemonDfsMessageServiceLaneHandle>, sinomo_dfs::DfsError> {
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
                .ok_or_else(|| sinomo_dfs::DfsError::Network {
                    message: "daemon DFS global lane awaits a complete retained authority prefix"
                        .to_string(),
                })?,
        ),
        resident_scope,
        entry.cross_shard_namespace_locks.clone(),
    )?;
    entry.primary_service = Some(Arc::clone(&lane));
    Ok(lane)
}

pub(crate) fn daemon_dfs_message_cache_entry<'a>(
    cache: &'a mut BTreeMap<PathBuf, DaemonDfsMessageServiceCacheEntry>,
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
    allow_generation_rollover: bool,
) -> Result<&'a mut DaemonDfsMessageServiceCacheEntry, sinomo_dfs::DfsError> {
    let cache_key = data_dir.root();
    let peer_node_ids = &peer_snapshot.peer_node_ids;
    let metadata_authority_generation = match cache.get(&cache_key) {
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
    let signature = DaemonDfsMessageServiceSignature {
        cluster_id: peer_snapshot.cluster_id.clone(),
        local_node_id: peer_snapshot.local_node_id.clone(),
        metadata_authority_generation: metadata_authority_generation.generation(),
    };
    let generation_changed = cache
        .get(&cache_key)
        .is_some_and(|entry| entry.signature != signature);
    if cache.get(&cache_key).is_none() || (generation_changed && allow_generation_rollover) {
        // Release the superseded backend-bearing lanes before opening their
        // replacement, while retaining the one process-local content store.
        let content_store = match cache.remove(&cache_key) {
            Some(entry) => entry.content_store,
            None => sinomo_dfs::LocalContentStore::new(data_dir.dfs_dir().join("content"))?,
        };
        tracing::info!(
            data_root = %cache_key.display(),
            metadata_authority_generation = signature.metadata_authority_generation,
            peer_count = peer_node_ids.len(),
            "opened bounded daemon DFS message service cache generation"
        );
        cache.insert(
            cache_key.clone(),
            DaemonDfsMessageServiceCacheEntry {
                signature,
                metadata_authority_generation,
                peer_node_ids: peer_node_ids.clone(),
                content_store,
                resident_metadata_backend: None,
                resident_metadata_initialized: false,
                cross_shard_namespace_locks: Default::default(),
                primary_service: None,
                exact_shard_services: BTreeMap::new(),
                content_read_service: None,
            },
        );
    } else if generation_changed {
        // An authority observer may advance while an exact-shard operation is
        // already admitted on the prior generation. Never open a replacement
        // beside that live capability. The next globally admitted command (or
        // maintenance pass) performs the rollover after every exact lease has
        // drained.
        tracing::debug!(
            data_root = %cache_key.display(),
            observed_generation = signature.metadata_authority_generation,
            retained_generation = cache
                .get(&cache_key)
                .map(|entry| entry.signature.metadata_authority_generation)
                .unwrap_or_default(),
            "deferred daemon DFS resident generation rollover until global admission"
        );
    }

    let entry = cache
        .get_mut(&cache_key)
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
    let authority_generation = daemon_dfs_metadata_authority_generation_for_identity(
        cluster_paths,
        cluster_id,
        local_node_id,
        Some(runtime),
    )?;
    let expected_generation = authority_generation.generation();
    authority_generation
        .ensure_complete_prefix_for_generation_async(expected_generation)
        .await
        .filter(|_| authority_generation.generation() == expected_generation)
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS metadata authority generation {expected_generation} lacks a complete retained projection after async preflight"
            ),
        })?;
    Ok(DaemonDfsMetadataAuthorityPreflight {
        authority: authority_generation,
        generation: expected_generation,
    })
}

pub(crate) async fn warm_daemon_dfs_metadata_authority_for_peer_snapshot_async(
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
) -> Result<DaemonDfsMetadataAuthorityPreflight, sinomo_dfs::DfsError> {
    warm_daemon_dfs_metadata_authority_for_identity_async(
        cluster_paths,
        &peer_snapshot.cluster_id,
        &peer_snapshot.local_node_id,
        runtime,
    )
    .await
}

/// Open the default-DFS lifecycle backend from an already completed authority
/// preflight. This function is intentionally synchronous and retained-only: it
/// may run while global DFS admission is held, so it must never initiate
/// network I/O or enter an owner-runtime bridge.
pub(crate) fn daemon_dfs_resident_metadata_backend_after_preflight(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    preflight: &DaemonDfsMetadataAuthorityPreflight,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<ResidentLocalShardedMetadataBackend>, sinomo_dfs::DfsError> {
    let expected_generation = preflight.generation;
    preflight
        .authority
        .retained_complete_prefix_revision_for_generation(expected_generation)
        .ok_or_else(|| sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS resident metadata generation {expected_generation} lacks a complete retained authority projection"
            ),
        })?;

    let mut cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let entry = daemon_dfs_message_cache_entry(
        &mut cache,
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        true,
    )?;
    if entry.signature.metadata_authority_generation != expected_generation
        || preflight.authority.generation() != expected_generation
        || !Arc::ptr_eq(&entry.metadata_authority_generation, &preflight.authority)
    {
        return Err(sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS metadata authority advanced while asynchronously opening resident generation {expected_generation}"
            ),
        });
    }
    if !entry.resident_metadata_initialized {
        // Materialize the bounded retained prefix only when this generation
        // actually needs a resident backend. Warm callers which merely prove
        // that the already-open generation is still current stay certificate-
        // only and do not clone the complete authority vector.
        let projection = preflight
            .authority
            .retained_complete_prefix_for_generation(expected_generation)
            .ok_or_else(|| sinomo_dfs::DfsError::Network {
                message: format!(
                    "DFS resident metadata generation {expected_generation} advanced before backend initialization"
                ),
            })?;
        entry.resident_metadata_backend =
            open_daemon_dfs_resident_metadata_backend_from_authority_projection(
                data_dir,
                &entry.signature.local_node_id,
                &entry.metadata_authority_generation,
                expected_generation,
                &projection,
            )?;
        entry.resident_metadata_initialized = true;
    }
    Ok(entry.resident_metadata_backend.clone())
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
    let cache_lock_wait_started = Instant::now();
    let mut cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
    let cache_lock_wait = cache_lock_wait_started.elapsed();
    let entry = daemon_dfs_message_cache_entry(
        &mut cache,
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        allow_generation_rollover,
    )?;
    if let Some(preflight) = metadata_authority_preflight
        && (preflight.authority.generation() != preflight.generation
            || entry.signature.metadata_authority_generation != preflight.generation
            || !Arc::ptr_eq(&entry.metadata_authority_generation, &preflight.authority))
    {
        return Err(sinomo_dfs::DfsError::Network {
            message: format!(
                "DFS metadata authority advanced after async preflight of generation {}",
                preflight.generation
            ),
        });
    }
    if matches!(
        &kind,
        DaemonDfsMessageLaneKind::MutationExact(_) | DaemonDfsMessageLaneKind::MutationGlobal
    ) && !entry.resident_metadata_initialized
    {
        entry.resident_metadata_backend =
            open_daemon_dfs_resident_metadata_backend_for_authority_generation(
                data_dir,
                &entry.signature.local_node_id,
                &entry.metadata_authority_generation,
                entry.signature.metadata_authority_generation,
            )?;
        entry.resident_metadata_initialized = true;
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
                        return daemon_dfs_global_message_lane(
                            data_dir,
                            cluster_paths,
                            runtime,
                            entry,
                        )
                        .map(|lane| (lane, cache_lock_wait));
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
                            .ok_or_else(|| sinomo_dfs::DfsError::Network {
                                message: "daemon DFS exact lane awaits a complete retained authority prefix"
                                    .to_string(),
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
            daemon_dfs_global_message_lane(data_dir, cluster_paths, runtime, entry)?
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
    tracing::trace!(
        data_root = %data_dir.root().display(),
        lane = ?lane_kind,
        "selected bounded daemon DFS service lane"
    );
    Ok((lane, cache_lock_wait))
}

pub(crate) fn daemon_dfs_message_content_store(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    peer_snapshot: &DaemonDfsMessagePeerSnapshot,
    runtime: &tokio::runtime::Handle,
) -> Result<sinomo_dfs::LocalContentStore, sinomo_dfs::DfsError> {
    let mut cache = daemon_dfs_message_service_cache()
        .lock()
        .map_err(|_| daemon_dfs_cache_lock_error())?;
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
    let trace_started = Instant::now();
    // Daemon startup owns data-directory initialization. Repeating its six
    // create_dir_all calls here turns every routed DFS message into layout I/O.
    let cache_key = data_dir.root();
    let is_read_only = dfs_message_is_read_only_request(&command);
    let use_content_read_lane = daemon_dfs_message_uses_content_read_lane(&command);
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
    let (service_lane_handle, cache_lock_wait) = daemon_dfs_message_lane(
        data_dir,
        cluster_paths,
        peer_snapshot,
        runtime,
        lane_kind,
        allow_generation_rollover,
        metadata_authority_preflight.as_ref(),
    )?;
    let cache_select = cache_select_started.elapsed();

    let sinomo_dfs::DfsRuntimeMessageExecution {
        process_report,
        committed_work,
        replica_materialization_shard,
        deferred_work,
        repair_queue_record_count,
        repair_queue_persistence,
        exact_projection_release_error,
        resident_backend_release_error,
        lane_lock_wait,
        backend_checkout,
        process,
        repair_queue_elapsed,
    } = sinomo_dfs::execute_runtime_message_command(
        &service_lane_handle,
        peer_node_ids,
        command,
        preingested_blob,
        execution_scope,
        |repair_queue_records| {
            persist_daemon_dfs_repair_queue_records(data_dir, repair_queue_records)
        },
    )?;
    match replica_materialization_shard {
        Ok(Some(shard)) => match daemon_dfs_data_owner_lock(&DaemonPaths::new(data_dir)) {
            Ok(Some(data_owner_lock)) => match mutation_admission
                .take()
                .and_then(DaemonDfsMutationAdmission::into_generation_fence)
            {
                Some(generation_fence) => {
                    schedule_daemon_dfs_replica_materialization_on_runtime(
                        data_dir.clone(),
                        runtime,
                        shard,
                        data_owner_lock,
                        generation_fence,
                    );
                }
                None => tracing::warn!(
                    "durable DFS replica ingest could not transfer its metadata generation fence to asynchronous materialization"
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
            "failed to persist committed DFS repair queue; restart recovery may require explicit disaster recovery"
        );
    }
    #[cfg(test)]
    wait_at_daemon_dfs_message_committed_edge_test_hook(&request_id);
    let committed_edges = emit_daemon_dfs_committed_edges(
        data_dir,
        cluster_paths,
        execution_scope,
        runtime,
        traceparent,
        committed_work,
        process_report
            .outcome
            .as_ref()
            .map(|result| result.events.as_slice())
            .unwrap_or_default(),
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
    Ok(Some(DaemonDfsMessageCommandProcessResult {
        response,
        attachment,
        committed_edges,
    }))
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
        notify_daemon_dfs_maintenance_work();
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

pub(crate) async fn handle_daemon_dfs_message_async(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    attachment: Option<DfsInboundAttachment>,
    traceparent: Option<String>,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsDataPlaneResponse, DaemonError> {
    let process_data_owner_lock = require_daemon_dfs_data_owner_lock(paths)?;
    let trace_started = Instant::now();
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let peer_snapshot_started = Instant::now();
    let Some(peer_snapshot) = read_daemon_dfs_message_peer_snapshot(&cluster_paths)? else {
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
    let metadata_authority_preflight = if !uses_content_read_lane {
        // Complete or join authority I/O before even the shared content-store
        // lookup below can take the process-wide DFS cache mutex. Attachment
        // ingest may perform durable local I/O, but it never owns authority.
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
    let content = if attachment.is_some() {
        Some(daemon_dfs_message_content_store(
            &data_dir,
            &cluster_paths,
            &peer_snapshot,
            runtime,
        )?)
    } else {
        None
    };
    let (command, preingested_blob) =
        ingest_dfs_message_attachment(content, command, attachment).await?;
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
    daemon_dfs_perf_trace_with_traceparent(
        traceparent.as_deref(),
        format_args!(
            "dfs-message-handler-start request={} kind={} source={} target={} frame_id={} message_id={}",
            request_id, message_kind, source_node_id, target_node_id, frame_id, message_id
        ),
    );
    let process_data_dir = data_dir.clone();
    let process_cluster_paths = cluster_paths.clone();
    let process_peer_snapshot = peer_snapshot.clone();
    let process_runtime = runtime.clone();
    let process_traceparent = traceparent.clone();
    let process_metadata_authority_preflight = metadata_authority_preflight;
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
    let admission_started = Instant::now();
    let mut admission = admit_daemon_dfs_foreground_message(&scheduler, &admission_scope).await?;
    let mut exact_service_lane = if exact_service_lane_candidate && admission.is_exact() {
        if let Some(namespace) = exact_namespace_read_command {
            daemon_dfs_message_cache_exact_namespace_read_admission_is_current(
                &data_dir,
                &cluster_paths,
                &peer_snapshot,
                namespace,
                runtime,
            )?
        } else {
            match DaemonDfsMutationLaneKey::from_scope(&execution_scope) {
                Some(key) => daemon_dfs_message_cache_exact_admission_is_current(
                    &data_dir,
                    &cluster_paths,
                    &peer_snapshot,
                    &key,
                    runtime,
                )?,
                None => false,
            }
        }
    } else {
        false
    };
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
            admission = admit_daemon_dfs_foreground_message(
                &scheduler,
                &DfsMessageExecutionScope::GlobalExclusive,
            )
            .await?;
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
    let allow_generation_rollover = admission.is_global();
    let admission_wait = admission_started.elapsed();
    let process_execution_scope = execution_scope.clone();
    let spawn_started = Instant::now();
    let blocking_queue_started = spawn_started;
    let (result, blocking_queue, blocking_work) = tokio::task::spawn_blocking(move || {
        // The async connection task and runtime shutdown may abandon this
        // JoinHandle while blocking work continues. Retain the OS-backed data
        // lock inside the blocking closure so no replacement daemon can prove
        // this placement-authority session dead prematurely.
        let _data_owner_lock = process_data_owner_lock;
        let mut mutation_admission = Some(admission);
        let blocking_started = Instant::now();
        let blocking_queue = blocking_queue_started.elapsed();
        let result = sinomo_dfs::with_dfs_traceparent(process_traceparent.as_deref(), || {
            process_daemon_dfs_message_command_on_runtime(
                &process_data_dir,
                &process_cluster_paths,
                &process_peer_snapshot,
                command,
                preingested_blob,
                &process_execution_scope,
                exact_service_lane,
                allow_generation_rollover,
                process_metadata_authority_preflight,
                peer_snapshot_elapsed,
                &mut mutation_admission,
                &process_runtime,
                process_traceparent.clone(),
            )
        });
        // A blocking task keeps running when its async JoinHandle is dropped.
        // Own admission here so a disconnected/cancelled RPC cannot release
        // the shard (or global) guard before the actual DFS mutation stops.
        drop(mutation_admission);
        (result, blocking_queue, blocking_started.elapsed())
    })
    .await
    .map_err(|source| DaemonError::Network {
        address: "DFS message".to_string(),
        detail: format!("DFS message task failed: {source}"),
    })?;
    let result = result?;
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
    Ok(DfsDataPlaneResponse {
        response,
        attachment,
    })
}

#[cfg(test)]
pub(crate) fn handle_daemon_dfs_message_on_runtime(
    paths: &DaemonPaths,
    command: DfsMessageCommand,
    runtime: &tokio::runtime::Handle,
) -> Result<DfsMessageResponse, DaemonError> {
    let data_dir = daemon_data_dir(paths)?;
    let data_dir = SinomoDataDir::new(data_dir);
    let cluster_paths = ClusterPaths::new(&data_dir);
    let execution_scope = dfs_message_execution_scope(&command);
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
    let mut mutation_admission = None;
    let result = process_daemon_dfs_message_command_on_runtime(
        &data_dir,
        &cluster_paths,
        &peer_snapshot,
        command,
        None,
        &execution_scope,
        false,
        true,
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
