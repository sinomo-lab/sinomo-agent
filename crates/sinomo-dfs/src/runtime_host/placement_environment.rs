use super::*;
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};

#[derive(Debug)]
struct DaemonDfsPlacementEnvironment {
    monitor: sinomo_dfs::DfsDeviceHealthMonitor,
    local_observation_owner: Option<Weak<DfsRuntimeHost>>,
    cache: sinomo_dfs::DfsPlacementSnapshotCache,
    metering: sinomo_dfs::DfsNetworkMetering,
    resource_pressure: sinomo_dfs::DfsResourcePressure,
    metering_sampled_at: Option<Instant>,
    last_published_local_advertisement: Option<sinomo_dfs::DfsNodeAdvertisement>,
    advertisements: BTreeMap<NodeId, sinomo_dfs::DfsNodeAdvertisement>,
    // Accepted only from the target's authenticated content-owner RPC, never
    // from node connectivity or a third-party snapshot. The local owner token
    // prevents reuse across this component's restart.
    live_advertisements: BTreeMap<NodeId, (Weak<DfsRuntimeHost>, sinomo_dfs::DfsNodeAdvertisement)>,
    remote_projection_revision: u64,
    remote_projection_pending: bool,
    remote_projection_generation: u64,
    remote_projection_initialized: bool,
}

static DAEMON_DFS_PLACEMENT_ENVIRONMENTS: OnceLock<
    Mutex<BTreeMap<PathBuf, DaemonDfsPlacementEnvironment>>,
> = OnceLock::new();
#[derive(Debug)]
struct DaemonDfsStorageEnvironmentSignal {
    #[cfg(test)]
    completed_owner: Mutex<Option<Weak<DfsRuntimeHost>>>,
    #[cfg(test)]
    completed: Arc<tokio::sync::Notify>,
    generation: AtomicU64,
    notify: Arc<tokio::sync::Notify>,
}

static DAEMON_DFS_STORAGE_ENVIRONMENT_SIGNALS: OnceLock<
    Mutex<BTreeMap<PathBuf, Arc<DaemonDfsStorageEnvironmentSignal>>>,
> = OnceLock::new();

const DAEMON_DFS_METERING_SAMPLE_INTERVAL: Duration = Duration::from_secs(5 * 60);
#[cfg(test)]
const DAEMON_DFS_ADVERTISEMENT_RENEWAL_MARGIN: time::Duration = time::Duration::minutes(5);
#[cfg(any(target_os = "linux", target_os = "windows"))]
const DAEMON_DFS_METERING_QUERY_TIMEOUT: Duration = Duration::from_secs(2);

fn daemon_dfs_storage_environment_signal(
    data_dir: &SinomoDataDir,
) -> Arc<DaemonDfsStorageEnvironmentSignal> {
    let mut signals = DAEMON_DFS_STORAGE_ENVIRONMENT_SIGNALS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Arc::clone(signals.entry(data_dir.root()).or_insert_with(|| {
        Arc::new(DaemonDfsStorageEnvironmentSignal {
            #[cfg(test)]
            completed_owner: Mutex::new(None),
            #[cfg(test)]
            completed: Arc::new(tokio::sync::Notify::new()),
            generation: AtomicU64::new(0),
            notify: Arc::new(tokio::sync::Notify::new()),
        })
    }))
}

pub(crate) fn daemon_dfs_storage_environment_notify(
    data_dir: &SinomoDataDir,
) -> Arc<tokio::sync::Notify> {
    Arc::clone(&daemon_dfs_storage_environment_signal(data_dir).notify)
}

pub(crate) fn daemon_dfs_storage_environment_dirty_generation(data_dir: &SinomoDataDir) -> u64 {
    daemon_dfs_storage_environment_signal(data_dir)
        .generation
        .load(Ordering::Acquire)
}

pub(crate) fn notify_daemon_dfs_storage_environment_dirty(data_dir: &SinomoDataDir) {
    let signal = daemon_dfs_storage_environment_signal(data_dir);
    signal.generation.fetch_add(1, Ordering::AcqRel);
    signal.notify.notify_one();
}

/// Retained observation only. In particular, do not call the environment
/// helper which would create a monitor/cache or sample missing host state.
#[cfg(feature = "storage-conversion")]
pub(super) fn retained_storage_import_placement_snapshot(
    data_dir: &SinomoDataDir,
) -> Option<Arc<sinomo_dfs::DfsPlacementSnapshot>> {
    DAEMON_DFS_PLACEMENT_ENVIRONMENTS
        .get()?
        .lock()
        .ok()?
        .get(&data_dir.root())?
        .cache
        .snapshot()
}

fn placement_environments() -> &'static Mutex<BTreeMap<PathBuf, DaemonDfsPlacementEnvironment>> {
    DAEMON_DFS_PLACEMENT_ENVIRONMENTS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn with_placement_environment<R>(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
    operation: impl FnOnce(&mut DaemonDfsPlacementEnvironment) -> R,
) -> R {
    let key = data_dir.root();
    let mut environments = placement_environments()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let environment = environments
        .entry(key)
        .or_insert_with(|| DaemonDfsPlacementEnvironment {
            monitor: sinomo_dfs::DfsDeviceHealthMonitor::new(
                node_id.clone(),
                vec![sinomo_dfs::DfsContentLocationConfig::default_for_root(
                    data_dir.dfs_dir().join("content"),
                )],
            ),
            local_observation_owner: None,
            cache: sinomo_dfs::DfsPlacementSnapshotCache::default(),
            metering: sinomo_dfs::DfsNetworkMetering::Unknown,
            resource_pressure: sinomo_dfs::DfsResourcePressure::Normal,
            metering_sampled_at: None,
            last_published_local_advertisement: None,
            advertisements: BTreeMap::new(),
            live_advertisements: BTreeMap::new(),
            remote_projection_revision: 0,
            remote_projection_pending: false,
            remote_projection_generation: 0,
            remote_projection_initialized: false,
        });
    operation(environment)
}

fn refresh_network_metering(
    environment: &mut DaemonDfsPlacementEnvironment,
    node_resources: Option<sinomo_device::NodeResourceSnapshot>,
) {
    if let Some(metering) = node_resources.and_then(dfs_network_metering_from_node_resources) {
        environment.metering = metering;
        environment.resource_pressure = node_resources
            .map(dfs_resource_pressure_from_node_resources)
            .unwrap_or_default();
        return;
    }
    environment.resource_pressure = sinomo_dfs::DfsResourcePressure::Normal;
    if environment
        .metering_sampled_at
        .is_some_and(|sampled| sampled.elapsed() < DAEMON_DFS_METERING_SAMPLE_INTERVAL)
    {
        return;
    }
    environment.metering = sample_os_network_metering();
    environment.metering_sampled_at = Some(Instant::now());
}

fn dfs_resource_pressure_from_node_resources(
    resources: sinomo_device::NodeResourceSnapshot,
) -> sinomo_dfs::DfsResourcePressure {
    match resources.mode {
        sinomo_device::NodeResourceMode::Normal => sinomo_dfs::DfsResourcePressure::Normal,
        sinomo_device::NodeResourceMode::Constrained => {
            sinomo_dfs::DfsResourcePressure::Constrained
        }
        sinomo_device::NodeResourceMode::Restricted => sinomo_dfs::DfsResourcePressure::Restricted,
    }
}

fn dfs_network_metering_from_node_resources(
    resources: sinomo_device::NodeResourceSnapshot,
) -> Option<sinomo_dfs::DfsNetworkMetering> {
    resources
        .observed
        .then_some(match resources.network_metering {
            sinomo_device::NodeNetworkMetering::Unmetered => {
                sinomo_dfs::DfsNetworkMetering::Unmetered
            }
            sinomo_device::NodeNetworkMetering::Metered => sinomo_dfs::DfsNetworkMetering::Metered,
            sinomo_device::NodeNetworkMetering::Unknown => sinomo_dfs::DfsNetworkMetering::Unknown,
        })
}

fn daemon_dfs_local_content_advertisement_with_resources(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
    zone: Option<String>,
    node_resources: Option<sinomo_device::NodeResourceSnapshot>,
    sample_storage: bool,
) -> sinomo_dfs::DfsNodeAdvertisement {
    with_placement_environment(data_dir, node_id, |environment| {
        refresh_network_metering(environment, node_resources);
        let now = time::OffsetDateTime::now_utc();
        if sample_storage {
            environment.monitor.sample_with_resources(
                now,
                environment.metering,
                environment.resource_pressure,
            );
            environment.local_observation_owner = registered_dfs_runtime_host(data_dir)
                .ok().flatten().map(|host| Arc::downgrade(&host));
        } else {
            environment
                .monitor
                .update_advisory_evidence(environment.metering, environment.resource_pressure);
        }
        environment.monitor.aggregate_advertisement(zone, now)
    })
}

#[cfg(test)]
pub(super) async fn wait_live_owner_observation_for_test(
    data_dir: &SinomoDataDir,
    owner: &Arc<DfsRuntimeHost>,
    deadline: tokio::time::Instant,
) -> Result<sinomo_dfs::DfsNodeAdvertisement, String> {
    let signal = daemon_dfs_storage_environment_signal(data_dir);
    loop {
        let notified = signal.completed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let completed = signal.completed_owner.lock().unwrap().as_ref().and_then(Weak::upgrade)
            .is_some_and(|completed| Arc::ptr_eq(&completed, owner));
        if completed && let Some(record) = live_daemon_dfs_content_advertisement(data_dir, owner, time::OffsetDateTime::now_utc()) {
            return Ok(record);
        }
        tokio::time::timeout_at(deadline, notified).await
            .map_err(|_| "real storage observation did not complete before the original fixture deadline".to_string())?;
    }
}

pub(super) fn live_daemon_dfs_content_advertisement(
    data_dir: &SinomoDataDir,
    owner: &Arc<DfsRuntimeHost>,
    now: time::OffsetDateTime,
) -> Option<sinomo_dfs::DfsNodeAdvertisement> {
    // Do not use with_placement_environment here: an RPC cannot manufacture
    // an observer or inherit a previous component incarnation's sample.
    let environments = placement_environments().lock().ok()?;
    let environment = environments.get(&data_dir.root())?;
    let sampled_owner = environment.local_observation_owner.as_ref()?.upgrade()?;
    if !Arc::ptr_eq(owner, &sampled_owner) || owner.shutting_down.is_cancelled() {
        return None;
    }
    environment.monitor.accepted_advertisement()
        .filter(|record| record.expires_at.is_some_and(|expiry| expiry > now))
}

pub(super) fn cached_daemon_dfs_local_content_advertisement(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
    zone: Option<String>,
) -> sinomo_dfs::DfsNodeAdvertisement {
    with_placement_environment(data_dir, node_id, |environment| {
        environment
            .monitor
            .aggregate_advertisement(zone, time::OffsetDateTime::now_utc())
    })
}

pub(crate) fn cached_daemon_dfs_placement_snapshot_cache(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
) -> sinomo_dfs::DfsPlacementSnapshotCache {
    with_placement_environment(data_dir, node_id, |environment| environment.cache.clone())
}

/// Demand-owned refresh for actual DFS operations. This creates no timer or
/// task owner and changes only the advisory placement snapshot. Canonical KV
/// entries and the caller's write authority stay unchanged.
pub(super) async fn refresh_live_content_owner_observations(
    data_dir: &SinomoDataDir,
    host: &Arc<DfsRuntimeHost>,
    configuration: &sinomo_dfs::DfsWriteConfigurationProjection,
    authority: &Arc<DaemonDfsMetadataAuthorityGeneration>,
    deadline: tokio::time::Instant,
    #[cfg(test)] transport_override: Option<&dyn sinomo_module::ComponentObjectRpcTransport>,
) {
    use futures_util::StreamExt;
    #[cfg(test)]
    let transport = transport_override.unwrap_or(host.object_rpc.as_ref());
    #[cfg(not(test))]
    let transport = host.object_rpc.as_ref();
    if host.shutting_down.is_cancelled() { return; }
    let cluster_paths = ClusterPaths::new(data_dir);
    let Ok(state) = sinomo_core::cluster::read_cluster_state_without_kv_cached_shared(&cluster_paths) else { return; };
    let local = state.local_node_id.clone();
    let now = time::OffsetDateTime::now_utc();
    let Ok(canonical) = configuration.content_advertisements() else { return; };
    let (generation, pending) = with_placement_environment(data_dir, &local, |environment| {
        (environment.remote_projection_generation, environment.remote_projection_pending)
    });
    if pending { return; }
    let needed = canonical.into_iter().filter(|record| {
        record.node_id != local && record.expires_at.is_some_and(|expiry| expiry <= now)
            && state.members.iter().any(|member| member.node_id == record.node_id && member.state.is_authorized())
            && with_placement_environment(data_dir, &local, |environment| {
                environment.advertisements.get(&record.node_id) == Some(record)
                    && !environment.live_advertisements.get(&record.node_id).is_some_and(|(owner, live)| {
                        owner.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, host))
                            && crate::live_advertisement::matches_confirmed_content_advertisement(&record.node_id, record, live, now)
                    })
            })
    }).collect::<Vec<_>>();
    let mut requests = futures_util::stream::iter(needed.into_iter().map(|confirmed| async move {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { return (confirmed, None); }
        let options = sinomo_dfs::DfsObjectRpcSendOptions::default()
            .with_overall_timeout(remaining).with_observe_connectivity(false).cluster_rpc_options();
        let response = tokio::time::timeout_at(deadline, crate::live_advertisement::fetch_live_content_advertisement(
            transport, &confirmed.node_id, &confirmed, options,
        )).await.ok().and_then(Result::ok);
        (confirmed, response)
    })).buffer_unordered(16);
    let mut changed = false;
    while let Some((confirmed, response)) = requests.next().await {
        let Some(live) = response else { continue; };
        let Ok(Some(current)) = authority.write_configuration_projection() else { continue; };
        let key = sinomo_dfs::content_service_key(&confirmed.node_id);
        let Some(expected_entry) = configuration.entries(&key)
            .and_then(|entries| entries.iter().find(|entry| entry.key == key)) else { continue; };
        if current.entries(&key).and_then(|entries| entries.iter().find(|entry| entry.key == key)) != Some(expected_entry)
            || current.entries(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
                != configuration.entries(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX) {
            continue;
        }
        changed |= with_placement_environment(data_dir, &local, |environment| {
            if host.shutting_down.is_cancelled()
                || environment.remote_projection_pending
                || environment.remote_projection_generation != generation
                || environment.advertisements.get(&confirmed.node_id) != Some(&confirmed)
                || !registered_dfs_runtime_host(data_dir).ok().flatten().is_some_and(|current| Arc::ptr_eq(&current, host)) {
                return false;
            }
            environment.live_advertisements.insert(confirmed.node_id.clone(), (Arc::downgrade(host), live));
            true
        });
    }
    if changed {
        refresh_daemon_dfs_placement_snapshot(data_dir, &cluster_paths, &local, Vec::new());
    }
}

pub(super) fn refresh_daemon_dfs_placement_snapshot(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    advertisements: Vec<sinomo_dfs::DfsNodeAdvertisement>,
) -> sinomo_dfs::DfsPlacementSnapshotCache {
    // Once the resident observer owns remote eligibility, a maintenance caller's
    // earlier service candidates cannot replace its newer committed projection.
    let (generation, advertisements) =
        with_placement_environment(data_dir, local_node_id, |environment| {
            let advertisements = if environment.remote_projection_initialized {
                environment.advertisements.values().cloned().collect()
            } else {
                advertisements
            };
            (environment.remote_projection_generation, advertisements)
        });
    refresh_daemon_dfs_placement_snapshot_at_generation(
        data_dir,
        cluster_paths,
        local_node_id,
        advertisements,
        generation,
    )
}

fn refresh_daemon_dfs_placement_snapshot_at_generation(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    mut advertisements: Vec<sinomo_dfs::DfsNodeAdvertisement>,
    remote_generation: u64,
) -> sinomo_dfs::DfsPlacementSnapshotCache {
    let live_owner = registered_dfs_runtime_host(data_dir).ok().flatten();
    with_placement_environment(data_dir, local_node_id, |environment| {
        if environment.remote_projection_generation != remote_generation || environment.remote_projection_pending {
            return;
        }
        for canonical in &mut advertisements {
            let Some((owner, live)) = environment.live_advertisements.get(&canonical.node_id) else { continue; };
            if live_owner.as_ref().is_some_and(|current| {
                !current.shutting_down.is_cancelled()
                    && owner.upgrade().is_some_and(|owner| Arc::ptr_eq(&owner, current))
            }) && crate::live_advertisement::matches_confirmed_content_advertisement(
                &canonical.node_id, canonical, live, time::OffsetDateTime::now_utc(),
            ) {
                *canonical = live.clone();
            }
        }
    });
    // Data-plane shells consume the daemon cache only. Sampling is owned by
    // the lifecycle lane above and never runs in a DFS request constructor.
    let local_advertisement = cached_daemon_dfs_local_content_advertisement(
        data_dir,
        local_node_id,
        daemon_dfs_local_zone(data_dir),
    );
    advertisements.retain(|advertisement| advertisement.node_id != *local_node_id);
    advertisements.push(local_advertisement);
    advertisements.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    advertisements.dedup_by(|left, right| left.node_id == right.node_id);

    let now = time::OffsetDateTime::now_utc();
    let metering_by_node = advertisements
        .iter()
        .map(|advertisement| {
            (
                advertisement.node_id.clone(),
                advertisement.network_metering,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut paths = BTreeMap::<(NodeId, NodeId), sinomo_dfs::DfsPlacementPathObservation>::new();
    if let Ok(records) = sinomo_core::network::read_cluster_connectivity_graph(cluster_paths) {
        for record in records {
            let state = match record.state {
                sinomo_core::network::ClusterConnectivityState::Healthy => {
                    sinomo_dfs::DfsPlacementPathState::Healthy
                }
                sinomo_core::network::ClusterConnectivityState::Degraded => {
                    sinomo_dfs::DfsPlacementPathState::Degraded
                }
                sinomo_core::network::ClusterConnectivityState::Unreachable => {
                    // This excludes only a directed endpoint. Node-addressed
                    // delivery can still reuse an authenticated duplex carrier
                    // or a relay route; networking owns that decision.
                    sinomo_dfs::DfsPlacementPathState::Unknown
                }
            };
            let observation = sinomo_dfs::DfsPlacementPathObservation {
                source_node_id: record.source_node_id.clone(),
                target_node_id: record.target_node_id.clone(),
                state,
                latency_micros: record
                    .latency_ms
                    .map(|latency| latency.saturating_mul(1_000)),
                // Passive DFS transfers fill this in through the shared cache.
                // Absence remains unknown rather than zero bandwidth.
                delivered_bytes_per_second: None,
                metering: aggregate_path_metering(
                    metering_by_node.get(&record.source_node_id).copied(),
                    metering_by_node.get(&record.target_node_id).copied(),
                ),
                observed_at: record.observed_at,
                expires_at: record.expires_at,
            };
            let key = (record.source_node_id, record.target_node_id);
            let replace = paths.get(&key).is_none_or(|current| {
                daemon_path_state_rank(observation.state) < daemon_path_state_rank(current.state)
                    || (observation.state == current.state
                        && observation.latency_micros < current.latency_micros)
            });
            if replace {
                paths.insert(key, observation);
            }
        }
    }
    let nodes = advertisements
        .iter()
        .map(sinomo_dfs::DfsPlacementNodeObservation::from_advertisement)
        .collect::<Vec<_>>();
    let snapshot = sinomo_dfs::DfsPlacementSnapshot {
        schema_version: sinomo_dfs::DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION,
        generation: 0,
        primary_node_id: local_node_id.clone(),
        observed_at: now,
        expires_at: now + time::Duration::minutes(5),
        nodes,
        paths: paths.into_values().collect(),
    };
    with_placement_environment(data_dir, local_node_id, |environment| {
        if !environment.remote_projection_pending
            && environment.remote_projection_generation == remote_generation
        {
            environment.cache.publish(snapshot);
        }
        environment.cache.clone()
    })
}

struct PreparedDaemonDfsStorageEnvironmentRefresh {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    local_node_id: NodeId,
    desired: sinomo_dfs::DfsNodeAdvertisement,
    publish_required: bool,
}

fn prepare_daemon_dfs_storage_environment_refresh(
    paths: &DaemonPaths,
    node_resources: Option<sinomo_device::NodeResourceSnapshot>,
    sample_storage: bool,
) -> Result<Option<PreparedDaemonDfsStorageEnvironmentRefresh>, DaemonError> {
    if paths.is_shadow() {
        return Ok(None);
    }
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    if sample_storage {
        let content_root = data_dir.dfs_dir().join("content");
        let content_exists = content_root
            .try_exists()
            .map_err(|source| DaemonError::Io {
                path: content_root.display().to_string(),
                detail: source.to_string(),
            })?;
        if !content_exists {
            // A clustered peer can become prepared before its first content
            // ingress. Initialize through its actual owner and converter before
            // sampling; Pending retains the ordinary refresh retry without
            // installing a missing-layout health baseline.
            let _actual_owner = require_daemon_dfs_data_owner_lock(paths)?;
            super::open_dfs_runtime_content_store(&data_dir)?;
        }
    }
    if with_placement_environment(&data_dir, &state.local_node_id, |environment| {
        environment.remote_projection_pending
    }) {
        rebuild_daemon_dfs_remote_placement_projection(
            &data_dir,
            &cluster_paths,
            &state.local_node_id,
        );
    }

    let desired = daemon_dfs_local_content_advertisement_with_resources(
        &data_dir,
        &state.local_node_id,
        daemon_dfs_local_zone(&data_dir),
        node_resources,
        sample_storage,
    );
    let publish_required =
        with_placement_environment(&data_dir, &state.local_node_id, |environment| {
            daemon_advertisement_publish_required(
                environment.last_published_local_advertisement.as_ref(),
                &desired,
                time::OffsetDateTime::now_utc(),
            )
        });
    Ok(Some(PreparedDaemonDfsStorageEnvironmentRefresh {
        data_dir,
        cluster_paths,
        local_node_id: state.local_node_id,
        desired,
        publish_required,
    }))
}

pub(crate) async fn refresh_daemon_dfs_storage_environment_with_resources_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    node_resources: sinomo_device::NodeResourceSnapshot,
    sample_storage: bool,
) -> Result<bool, DaemonError> {
    refresh_daemon_dfs_storage_environment_inner_on_runtime(
        paths,
        runtime,
        Some(node_resources),
        sample_storage,
    )
    .await
}

async fn refresh_daemon_dfs_storage_environment_inner_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
    node_resources: Option<sinomo_device::NodeResourceSnapshot>,
    sample_storage: bool,
) -> Result<bool, DaemonError> {
    let prepare_paths = paths.clone();
    let Some(prepared) = tokio::task::spawn_blocking(move || {
        prepare_daemon_dfs_storage_environment_refresh(
            &prepare_paths,
            node_resources,
            sample_storage,
        )
    })
    .await
    .map_err(|error| DaemonError::Network {
        address: "DFS storage environment preparation".to_string(),
        detail: error.to_string(),
    })??
    else {
        return Ok(false);
    };
    let PreparedDaemonDfsStorageEnvironmentRefresh {
        data_dir,
        cluster_paths,
        local_node_id,
        desired,
        publish_required,
    } = prepared;
    let mut control_plane = DaemonDfsControlPlaneKvClient::new_on_runtime(&cluster_paths, runtime);
    let published = if publish_required {
        sinomo_dfs::publish_dfs_content_advertisement_async(
            &mut control_plane,
            &local_node_id,
            desired.clone(),
        )
        .await?;
        with_placement_environment(&data_dir, &local_node_id, |environment| {
            environment.last_published_local_advertisement = Some(desired.clone());
        });
        true
    } else {
        false
    };
    let update_data_dir = data_dir.clone();
    let update_cluster_paths = cluster_paths.clone();
    let update_local_node_id = local_node_id.clone();
    tokio::task::spawn_blocking(move || {
        let (generation, advertisements) =
            with_placement_environment(&update_data_dir, &update_local_node_id, |environment| {
                environment
                    .advertisements
                    .insert(update_local_node_id.clone(), desired);
                (
                    environment.remote_projection_generation,
                    environment
                        .advertisements
                        .values()
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            });
        refresh_daemon_dfs_placement_snapshot_at_generation(
            &update_data_dir,
            &update_cluster_paths,
            &update_local_node_id,
            advertisements,
            generation,
        );
    })
    .await
    .map_err(|error| DaemonError::Network {
        address: "DFS placement snapshot cache".to_string(),
        detail: error.to_string(),
    })?;
    #[cfg(test)]
    {
        let signal = daemon_dfs_storage_environment_signal(&data_dir);
        let owner = with_placement_environment(&data_dir, &local_node_id, |environment| environment.local_observation_owner.clone());
        *signal.completed_owner.lock().unwrap() = owner;
        signal.completed.notify_waiters();
    }
    Ok(published)
}

fn prepare_daemon_dfs_remote_placement_subscription(
    paths: &DaemonPaths,
) -> Result<Option<(SinomoDataDir, ClusterPaths, NodeId)>, DaemonError> {
    if paths.is_shadow() {
        return Ok(None);
    }
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    Ok(Some((data_dir, cluster_paths, state.local_node_id)))
}

fn observe_daemon_dfs_remote_placement_snapshot(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    snapshot: &sinomo_core::kv::KvSnapshot,
    expected_generation: Option<u64>,
) {
    observe_daemon_dfs_remote_placement_entries(
        data_dir,
        cluster_paths,
        local_node_id,
        snapshot.revision,
        snapshot.entries.iter(),
        expected_generation,
    );
}

fn observe_daemon_dfs_remote_placement_entries<'a>(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    revision: sinomo_core::kv::KvRevision,
    entries: impl Iterator<Item = &'a sinomo_core::kv::KvEntry> + Clone,
    expected_generation: Option<u64>,
) -> bool {
    let revision = revision.0;
    let member_node_ids = entries
        .clone()
        .filter(|entry| entry.key.starts_with("/cluster/members/"))
        .filter_map(|entry| entry.decode::<sinomo_core::ClusterMemberRecord>().ok())
        .filter(|member| member.state.is_authorized())
        .map(|member| member.node_id)
        .collect::<BTreeSet<_>>();
    let remote_advertisements = entries
        .filter(|entry| entry.key.starts_with(sinomo_dfs::content_services_prefix()))
        .filter_map(|entry| entry.decode::<sinomo_dfs::DfsNodeAdvertisement>().ok())
        .filter(|advertisement| {
            advertisement.node_id != *local_node_id
                && member_node_ids.contains(&advertisement.node_id)
        })
        .map(|advertisement| (advertisement.node_id.clone(), advertisement))
        .collect::<BTreeMap<_, _>>();
    let advertisements = with_placement_environment(data_dir, local_node_id, |environment| {
        if revision < environment.remote_projection_revision
            || expected_generation
                .is_some_and(|generation| generation != environment.remote_projection_generation)
        {
            return (false, None);
        }
        environment.remote_projection_revision = revision;
        environment.remote_projection_initialized = true;
        let was_pending = std::mem::replace(&mut environment.remote_projection_pending, false);
        let current_remote = environment
            .advertisements
            .iter()
            .filter(|(node_id, _)| *node_id != local_node_id)
            .map(|(node_id, advertisement)| (node_id.clone(), advertisement.clone()))
            .collect::<BTreeMap<_, _>>();
        if !was_pending && current_remote == remote_advertisements {
            return (true, None);
        }
        environment
            .advertisements
            .retain(|node_id, _| node_id == local_node_id);
        environment.advertisements.extend(remote_advertisements);
        environment.live_advertisements.clear();
        environment.remote_projection_generation =
            environment.remote_projection_generation.wrapping_add(1);
        (
            true,
            Some((
                environment.remote_projection_generation,
                environment
                    .advertisements
                    .values()
                    .cloned()
                    .collect::<Vec<_>>(),
            )),
        )
    });
    let (accepted, advertisements) = advertisements;
    if let Some((generation, advertisements)) = advertisements {
        refresh_daemon_dfs_placement_snapshot_at_generation(
            data_dir,
            cluster_paths,
            local_node_id,
            advertisements,
            generation,
        );
    }
    accepted
}

fn withdraw_daemon_dfs_remote_placement_projection(
    data_dir: &SinomoDataDir,
    local_node_id: &NodeId,
    revision: Option<sinomo_core::kv::KvRevision>,
) {
    with_placement_environment(data_dir, local_node_id, |environment| {
        if let Some(revision) = revision {
            environment.remote_projection_revision =
                environment.remote_projection_revision.max(revision.0);
        }
        environment.remote_projection_pending = true;
        environment.remote_projection_initialized = true;
        environment.remote_projection_generation =
            environment.remote_projection_generation.wrapping_add(1);
        environment
            .advertisements
            .retain(|node_id, _| node_id == local_node_id);
        environment.cache.withdraw();
    });
    notify_daemon_dfs_storage_environment_dirty(data_dir);
}

/// Configuration projection recovery uses only the existing resident member
/// and content-provider prefixes, with one coherent committed revision. It
/// neither opens a canonical writer nor performs a network or snapshot read.
fn rebuild_daemon_dfs_remote_placement_projection(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
) -> bool {
    rebuild_daemon_dfs_remote_placement_from_reader(data_dir, cluster_paths, local_node_id, || {
        let kv_paths = cluster_paths.kv_paths();
        let read = sinomo_core::kv::with_cluster_kv_resident_read_projection_before(
            &kv_paths.raft_wal_paths(local_node_id),
            local_node_id,
            Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT,
            |projection| {
                let mut entries = Vec::new();
                for prefix in ["/cluster/members/", sinomo_dfs::content_services_prefix()] {
                    entries.extend(projection.read_index_prefix_limited_by_payload_bytes(
                        prefix,
                        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
                        sinomo_core::kv::CLUSTER_KV_RANGE_PREFIX_MAX_PAYLOAD_BYTES,
                    )?);
                }
                Ok::<_, sinomo_core::kv::KvError>(entries)
            },
        )
        .ok_or_else(|| sinomo_core::kv::KvError::Busy {
            path: cluster_paths.root.display().to_string(),
        })??;
        Ok::<_, sinomo_core::kv::KvError>((read.revision, read.value?))
    })
}

fn rebuild_daemon_dfs_remote_placement_from_reader(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    read: impl FnOnce() -> Result<
        (sinomo_core::kv::KvRevision, Vec<sinomo_core::kv::KvEntry>),
        sinomo_core::kv::KvError,
    >,
) -> bool {
    let generation = with_placement_environment(data_dir, local_node_id, |environment| {
        environment.remote_projection_generation
    });
    let Ok((revision, entries)) = read() else {
        return false;
    };
    observe_daemon_dfs_remote_placement_entries(
        data_dir,
        cluster_paths,
        local_node_id,
        revision,
        entries.iter(),
        Some(generation),
    )
}

pub(crate) fn subscribe_daemon_dfs_remote_placement_observation(
    paths: &DaemonPaths,
) -> Result<Option<sinomo_core::kv::KvCommittedObservationSubscription>, DaemonError> {
    let Some((data_dir, cluster_paths, local_node_id)) =
        prepare_daemon_dfs_remote_placement_subscription(paths)?
    else {
        return Ok(None);
    };
    let callback_data_dir = data_dir.clone();
    let callback_cluster_paths = cluster_paths.clone();
    let callback_local_node_id = local_node_id.clone();
    let subscription = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
        &cluster_paths,
        move |observation| {
            if observation.applied_index.is_none() {
                // Placement facts originate in resident native prefixes;
                // derived-file repair hints do not change their eligibility.
                return;
            }
            use sinomo_core::kv::{KvCommittedObservationKind, KvCommittedObservationValue};
            match &observation.value {
                KvCommittedObservationValue::Discontinuity(_) => {
                    withdraw_daemon_dfs_remote_placement_projection(
                        &callback_data_dir,
                        &callback_local_node_id,
                        observation.applied_index.and(observation.revision),
                    );
                    rebuild_daemon_dfs_remote_placement_projection(
                        &callback_data_dir,
                        &callback_cluster_paths,
                        &callback_local_node_id,
                    );
                }
                KvCommittedObservationValue::Snapshot(snapshot) => {
                    if observation.kind == KvCommittedObservationKind::Replacement {
                        withdraw_daemon_dfs_remote_placement_projection(
                            &callback_data_dir,
                            &callback_local_node_id,
                            observation.applied_index.and(observation.revision),
                        );
                    }
                    observe_daemon_dfs_remote_placement_snapshot(
                        &callback_data_dir,
                        &callback_cluster_paths,
                        &callback_local_node_id,
                        snapshot,
                        None,
                    );
                }
                KvCommittedObservationValue::Delta(delta) => {
                    let pending = with_placement_environment(
                        &callback_data_dir,
                        &callback_local_node_id,
                        |environment| environment.remote_projection_pending,
                    );
                    if pending
                        || delta.events.iter().any(|event| {
                            event.key.starts_with("/cluster/members/")
                                || event.key.starts_with("/cluster/endpoints/")
                                || event.key.starts_with(sinomo_core::service::CLUSTER_SERVICES_KV_PREFIX)
                                || event.key.starts_with(sinomo_dfs::content_services_prefix())
                        })
                    {
                        with_placement_environment(
                            &callback_data_dir,
                            &callback_local_node_id,
                            |environment| {
                                environment.remote_projection_revision =
                                    environment.remote_projection_revision.max(delta.revision.0);
                                environment.remote_projection_pending = true;
                                environment.remote_projection_generation =
                                    environment.remote_projection_generation.wrapping_add(1);
                                environment.live_advertisements.clear();
                                environment.cache.withdraw();
                            },
                        );
                        if !rebuild_daemon_dfs_remote_placement_projection(
                            &callback_data_dir,
                            &callback_cluster_paths,
                            &callback_local_node_id,
                        ) {
                            withdraw_daemon_dfs_remote_placement_projection(
                                &callback_data_dir,
                                &callback_local_node_id,
                                Some(delta.revision),
                            );
                        }
                    }
                }
            }
        },
    );
    // Register before seeding. The revision fence makes a callback that races
    // this read win over an older snapshot, while the latest observed read
    // closes the registration gap. The component runtime repeats this seed
    // only during its bounded process-replacement window, when the first
    // observed snapshot may still be behind the surviving daemon's view.
    if let Err(error) = refresh_daemon_dfs_remote_placement_observation(paths) {
        tracing::warn!(
            "DFS remote placement subscription started without an initial projection: {error}"
        );
    }
    Ok(Some(subscription))
}

pub(crate) fn refresh_daemon_dfs_remote_placement_observation(
    paths: &DaemonPaths,
) -> Result<bool, DaemonError> {
    let Some((data_dir, cluster_paths, local_node_id)) =
        prepare_daemon_dfs_remote_placement_subscription(paths)?
    else {
        return Ok(false);
    };
    Ok(rebuild_daemon_dfs_remote_placement_projection(
        &data_dir,
        &cluster_paths,
        &local_node_id,
    ))
}

fn daemon_advertisement_semantics_equal(
    left: &sinomo_dfs::DfsNodeAdvertisement,
    right: &sinomo_dfs::DfsNodeAdvertisement,
) -> bool {
    crate::placement_environment::advertisement_semantics_equal(left, right)
}

fn daemon_advertisement_publish_required(
    current: Option<&sinomo_dfs::DfsNodeAdvertisement>,
    desired: &sinomo_dfs::DfsNodeAdvertisement,
    _now: time::OffsetDateTime,
) -> bool {
    // An actual initial observation or semantic transition is durable work.
    // A storage sample's age never manufactures a publication or a fresh sample.
    current.is_none_or(|current| !daemon_advertisement_semantics_equal(current, desired))
}

fn aggregate_path_metering(
    source: Option<sinomo_dfs::DfsNetworkMetering>,
    target: Option<sinomo_dfs::DfsNetworkMetering>,
) -> sinomo_dfs::DfsNetworkMetering {
    match (source, target) {
        (Some(sinomo_dfs::DfsNetworkMetering::Metered), _)
        | (_, Some(sinomo_dfs::DfsNetworkMetering::Metered)) => {
            sinomo_dfs::DfsNetworkMetering::Metered
        }
        (
            Some(sinomo_dfs::DfsNetworkMetering::Unmetered),
            Some(sinomo_dfs::DfsNetworkMetering::Unmetered),
        ) => sinomo_dfs::DfsNetworkMetering::Unmetered,
        _ => sinomo_dfs::DfsNetworkMetering::Unknown,
    }
}

fn daemon_path_state_rank(state: sinomo_dfs::DfsPlacementPathState) -> u8 {
    match state {
        sinomo_dfs::DfsPlacementPathState::Healthy => 0,
        sinomo_dfs::DfsPlacementPathState::Degraded => 1,
        sinomo_dfs::DfsPlacementPathState::Unknown => 2,
        sinomo_dfs::DfsPlacementPathState::Unreachable => 3,
    }
}

fn sample_os_network_metering() -> sinomo_dfs::DfsNetworkMetering {
    #[cfg(target_os = "linux")]
    {
        let mut command = Command::new("nmcli");
        command.args([
            "--wait",
            "2",
            "-t",
            "-f",
            "GENERAL.STATE,GENERAL.METERED",
            "device",
            "show",
        ]);
        command_stdout_bounded(command, DAEMON_DFS_METERING_QUERY_TIMEOUT)
            .map(|output| parse_network_manager_metering(&output))
            .unwrap_or(sinomo_dfs::DfsNetworkMetering::Unknown)
    }
    #[cfg(target_os = "windows")]
    {
        sample_windows_network_metering()
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        sinomo_dfs::DfsNetworkMetering::Unknown
    }
}

#[cfg(target_os = "windows")]
struct WindowsRuntimeApartment;

#[cfg(target_os = "windows")]
struct WindowsNetworkMeteringWorker {
    requests:
        std::sync::mpsc::SyncSender<std::sync::mpsc::SyncSender<sinomo_dfs::DfsNetworkMetering>>,
}

#[cfg(target_os = "windows")]
impl WindowsNetworkMeteringWorker {
    fn start() -> Option<Self> {
        let (request_tx, request_rx) = std::sync::mpsc::sync_channel(1);
        // The worker owns WinRT's thread-scoped apartment and keeps a bounded
        // synchronous OS query off Tokio workers. It deliberately uses Rust's
        // standard thread stack; stack growth is not an error-handling policy.
        std::thread::Builder::new()
            .name("sinomo-winrt-metering".to_string())
            .spawn(move || run_windows_network_metering_worker(request_rx))
            .ok()?;
        Some(Self {
            requests: request_tx,
        })
    }

    fn sample(&self) -> sinomo_dfs::DfsNetworkMetering {
        let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
        if self.requests.try_send(reply_tx).is_err() {
            return sinomo_dfs::DfsNetworkMetering::Unknown;
        }
        reply_rx
            .recv_timeout(DAEMON_DFS_METERING_QUERY_TIMEOUT)
            .unwrap_or(sinomo_dfs::DfsNetworkMetering::Unknown)
    }
}

#[cfg(target_os = "windows")]
impl WindowsRuntimeApartment {
    fn initialize() -> windows::core::Result<Self> {
        use windows::Win32::System::WinRT::{RO_INIT_MULTITHREADED, RoInitialize};

        // SAFETY: this call initializes WinRT for only the current sampling
        // thread and the guard balances every successful initialization.
        unsafe { RoInitialize(RO_INIT_MULTITHREADED)? };
        Ok(Self)
    }
}

#[cfg(target_os = "windows")]
impl Drop for WindowsRuntimeApartment {
    fn drop(&mut self) {
        use windows::Win32::System::WinRT::RoUninitialize;

        // SAFETY: this guard exists only after RoInitialize succeeded on this
        // thread and is dropped before the sampling call returns.
        unsafe { RoUninitialize() };
    }
}

#[cfg(target_os = "windows")]
fn sample_windows_network_metering() -> sinomo_dfs::DfsNetworkMetering {
    static WORKER: OnceLock<Option<WindowsNetworkMeteringWorker>> = OnceLock::new();
    WORKER
        .get_or_init(WindowsNetworkMeteringWorker::start)
        .as_ref()
        .map(WindowsNetworkMeteringWorker::sample)
        .unwrap_or(sinomo_dfs::DfsNetworkMetering::Unknown)
}

#[cfg(target_os = "windows")]
fn run_windows_network_metering_worker(
    requests: std::sync::mpsc::Receiver<
        std::sync::mpsc::SyncSender<sinomo_dfs::DfsNetworkMetering>,
    >,
) {
    let Ok(_apartment) = WindowsRuntimeApartment::initialize() else {
        return;
    };
    while let Ok(reply) = requests.recv() {
        let observation =
            query_windows_network_metering().unwrap_or(sinomo_dfs::DfsNetworkMetering::Unknown);
        let _ = reply.send(observation);
    }
}

#[cfg(target_os = "windows")]
fn query_windows_network_metering() -> windows::core::Result<sinomo_dfs::DfsNetworkMetering> {
    use windows::Networking::Connectivity::{
        NetworkConnectivityLevel, NetworkCostType, NetworkInformation,
    };

    let profiles = NetworkInformation::GetConnectionProfiles()?;
    let mut observations = Vec::new();
    for index in 0..profiles.Size()? {
        let profile = profiles.GetAt(index)?;
        if profile.GetNetworkConnectivityLevel()? == NetworkConnectivityLevel::None {
            continue;
        }
        let cost = profile.GetConnectionCost()?.NetworkCostType()?;
        observations.push(if cost == NetworkCostType::Unrestricted {
            sinomo_dfs::DfsNetworkMetering::Unmetered
        } else if cost == NetworkCostType::Fixed || cost == NetworkCostType::Variable {
            sinomo_dfs::DfsNetworkMetering::Metered
        } else {
            sinomo_dfs::DfsNetworkMetering::Unknown
        });
    }
    Ok(aggregate_active_windows_network_metering(observations))
}

#[cfg(any(test, target_os = "windows"))]
fn aggregate_active_windows_network_metering(
    observations: impl IntoIterator<Item = sinomo_dfs::DfsNetworkMetering>,
) -> sinomo_dfs::DfsNetworkMetering {
    let mut saw_active = false;
    let mut saw_unknown = false;
    for observation in observations {
        saw_active = true;
        match observation {
            sinomo_dfs::DfsNetworkMetering::Metered => return observation,
            sinomo_dfs::DfsNetworkMetering::Unknown => saw_unknown = true,
            sinomo_dfs::DfsNetworkMetering::Unmetered => {}
        }
    }
    if saw_active && !saw_unknown {
        sinomo_dfs::DfsNetworkMetering::Unmetered
    } else {
        sinomo_dfs::DfsNetworkMetering::Unknown
    }
}

#[cfg(target_os = "linux")]
fn command_stdout_bounded(mut command: Command, timeout: Duration) -> Option<String> {
    command.stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = command.spawn().ok()?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => {
                let output = child.wait_with_output().ok()?;
                return String::from_utf8(output.stdout).ok();
            }
            Ok(Some(_)) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) if started.elapsed() < timeout => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn parse_network_manager_metering(output: &str) -> sinomo_dfs::DfsNetworkMetering {
    let mut saw_connected = false;
    let mut saw_unknown = false;
    for block in output.split("\n\n") {
        let connected = block.lines().any(|line| {
            line.strip_prefix("GENERAL.STATE:")
                .is_some_and(|state| state.trim_start().starts_with("100"))
        });
        if !connected {
            continue;
        }
        saw_connected = true;
        let metered = block
            .lines()
            .find_map(|line| line.strip_prefix("GENERAL.METERED:"));
        match metered.map(str::trim) {
            Some("yes") | Some("guess-yes") => return sinomo_dfs::DfsNetworkMetering::Metered,
            Some("no") | Some("guess-no") => {}
            _ => saw_unknown = true,
        }
    }
    if saw_connected && !saw_unknown {
        sinomo_dfs::DfsNetworkMetering::Unmetered
    } else {
        sinomo_dfs::DfsNetworkMetering::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct PlacementResidentReadMetrics(Arc<std::sync::atomic::AtomicUsize>);

    impl metrics::HistogramFn for PlacementResidentReadMetrics {
        fn record(&self, _value: f64) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl metrics::Recorder for PlacementResidentReadMetrics {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            _: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            metrics::Counter::noop()
        }
        fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }
        fn register_histogram(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            if key.name() == "sinomo.kv.raft.resident.read_projection_wait.us" {
                metrics::Histogram::from_arc(Arc::new(self.clone()))
            } else {
                metrics::Histogram::noop()
            }
        }
    }

    fn derived_future_remote_event_cannot_raise_native_floor(failing_import: bool) {
        use sinomo_core::kv::{InProcessKvStore, KvCommand, KvOp, KvRevision};
        let root = sinomo_testdir::TestDir::new("dfs-placement-derived-future-event");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named("placement-future-local").unwrap();
        let remote = NodeId::from_legacy_named("placement-future-remote").unwrap();
        let cluster =
            sinomo_core::node::ClusterId::from_legacy_named("placement-future-cluster").unwrap();
        let now = time::OffsetDateTime::now_utc();
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster.clone(),
            "placement future",
            local.clone(),
            "local",
            "local-key",
            now,
        );
        sinomo_core::cluster::write_cluster_state(&paths, &state).unwrap();
        let kv_paths = paths.kv_paths();
        let native = kv_paths.raft_store();
        native.set_leader_compact(&local, 1).unwrap();
        let baseline = native
            .append_command_and_publish(
                &local,
                KvCommand::Op(KvOp::Put {
                    key: "/native/anchor".into(),
                    value: b"one".to_vec(),
                    lease_id: None,
                }),
                |_| false,
            )
            .unwrap();
        assert_eq!(baseline.revision, KvRevision(1));
        let mut image = InProcessKvStore::restore(baseline).unwrap();
        let member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster,
            remote.clone(),
            "forged remote",
            "remote-key",
            now,
        );
        image
            .put_serde(
                sinomo_core::node::cluster_member_key(&remote),
                &member,
                None,
            )
            .unwrap();
        let mut advertisement = sinomo_dfs::DfsNodeAdvertisement::content(remote.clone());
        advertisement.storage_health = sinomo_dfs::DfsHealthState::Healthy;
        advertisement.storage_freshness = sinomo_dfs::DfsObservationFreshness::Fresh;
        image
            .put_serde(
                sinomo_dfs::content_service_key(&remote),
                &advertisement,
                None,
            )
            .unwrap();
        let future = image.try_snapshot().unwrap();
        assert!(future.revision > KvRevision(1));
        if failing_import {
            sinomo_core::kv::write_cluster_kv_materialized_snapshot_if_changed(&kv_paths, &future)
                .unwrap();
        }
        let daemon_paths = DaemonPaths::new(&data_dir);
        let _adapter = subscribe_daemon_dfs_remote_placement_observation(&daemon_paths)
            .unwrap()
            .unwrap();
        // The normal lifecycle builds the local placement cache from the strict
        // startup native projection, whose remote prefixes are complete/empty.
        let cache = refresh_daemon_dfs_placement_snapshot(&data_dir, &paths, &local, Vec::new());
        let canonical = cache.snapshot().expect("native R1 placement cache");
        assert!(canonical.nodes.iter().any(|node| node.node_id == local));
        assert!(canonical.nodes.iter().all(|node| node.node_id != remote));
        let projection_before = with_placement_environment(&data_dir, &local, |environment| {
            (
                environment.remote_projection_revision,
                environment.remote_projection_generation,
                environment.remote_projection_pending,
                environment.advertisements.clone(),
            )
        });
        assert_eq!(projection_before.0, 1);
        assert!(!projection_before.2);
        let dirty_before = daemon_dfs_storage_environment_dirty_generation(&data_dir);
        let held = native.open_node(&local).unwrap();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&delivered);
        let _witness = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
            &paths,
            move |event| {
                observed.lock().unwrap().push((
                    event.revision,
                    event.previous_revision,
                    event.applied_index,
                    matches!(
                        event.value,
                        sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_)
                    ),
                ));
            },
        );
        let native_reads = PlacementResidentReadMetrics::default();
        let writer_reads = native_reads.clone();
        let writer_paths = kv_paths.clone();
        let writer_future = future.clone();
        let (finished, completed) = std::sync::mpsc::channel();
        let publication = std::thread::spawn(move || {
            metrics::with_local_recorder(&writer_reads, || {
                if failing_import {
                    image.put("/derived/next", b"next".to_vec(), None).unwrap();
                    let desired = image.try_snapshot().unwrap();
                    let mut classifier_calls = 0;
                    let result = sinomo_core::kv::write_imported_cluster_kv_snapshot_if_changed(
                        &writer_paths,
                        &desired,
                        |_| {
                            classifier_calls += 1;
                            assert_eq!(classifier_calls, 1);
                            // Test-only IO fault after capture of known future
                            // prestate; production classifiers remain pure.
                            std::fs::remove_file(&writer_paths.snapshot).unwrap();
                            std::fs::create_dir(&writer_paths.snapshot).unwrap();
                            false
                        },
                    );
                    assert_eq!(classifier_calls, 1);
                    assert!(result.is_err());
                } else {
                    sinomo_core::kv::write_cluster_kv_materialized_snapshot_if_changed(
                        &writer_paths,
                        &writer_future,
                    )
                    .unwrap();
                }
            });
            finished.send(()).unwrap();
        });
        completed
            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .expect("derived writer completes while the native owner remains held");
        publication.join().unwrap();
        assert_eq!(
            native_reads.0.load(Ordering::SeqCst),
            0,
            "derived delivery must not attempt the bounded native read boundary"
        );
        assert!(
            Arc::ptr_eq(&canonical, &cache.snapshot().unwrap()),
            "derived uncertainty cannot retire the existing canonical cache"
        );
        assert_eq!(
            with_placement_environment(&data_dir, &local, |environment| (
                environment.remote_projection_revision,
                environment.remote_projection_generation,
                environment.remote_projection_pending,
                environment.advertisements.clone(),
            )),
            projection_before
        );
        assert_eq!(
            daemon_dfs_storage_environment_dirty_generation(&data_dir),
            dirty_before,
            "derived delivery must not schedule new placement work"
        );
        let events = delivered.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, Some(future.revision));
        assert_eq!(events[0].2, None);
        assert_eq!(events[0].3, failing_import);
        if failing_import {
            assert_eq!(events[0].1, Some(future.revision));
        }
        drop(events);
        drop(held);
        assert!(Arc::ptr_eq(&canonical, &cache.snapshot().unwrap()));
    }

    struct LiveReplyGate {
        inner: Arc<dyn sinomo_module::ComponentObjectRpcTransport>,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl sinomo_module::ComponentObjectRpcTransport for LiveReplyGate {
        fn call(&self, target: &NodeId, call: ObjectRpcCall, options: sinomo_core::network::ClusterObjectRpcOptions, observe: bool) -> Result<ObjectRpcResult, sinomo_core::network::NetworkError> {
            self.inner.call(target, call, options, observe)
        }
        fn call_async<'a>(&'a self, target: &'a NodeId, call: ObjectRpcCall, options: sinomo_core::network::ClusterObjectRpcOptions, observe: bool)
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ObjectRpcResult, sinomo_core::network::NetworkError>> + Send + 'a>> {
            Box::pin(async move {
                let result = self.inner.call_async(target, call, options, observe).await?;
                self.entered.notify_one();
                self.release.notified().await;
                Ok(result)
            })
        }
    }

    #[test]
    fn live_owner_read_rejects_expiry_and_other_incarnations_without_sampling() {
        let node = NodeId::from_legacy_named("live_owner_expiry_control").unwrap();
        let mut fixture = crate::test_support::DfsTestRuntime::new("live-owner-expiry-control", node.clone()).unwrap();
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        fixture.start_nodes().unwrap();
        let data = fixture.data_dir(&node).clone();
        let owner = registered_dfs_runtime_host(&data).unwrap().unwrap();
        let live = fixture.runtime_handle().block_on(wait_live_owner_observation_for_test(&data, &owner, deadline)).unwrap();
        let sequence = with_placement_environment(&data, &node, |environment| environment.monitor.cached_snapshot().sequence);
        assert!(live_daemon_dfs_content_advertisement(&data, &owner, live.expires_at.unwrap()).is_none());
        let other = Arc::new(DfsRuntimeHost::new(owner.cluster.clone(), owner.object_rpc.clone(), None));
        assert!(live_daemon_dfs_content_advertisement(&data, &other, time::OffsetDateTime::now_utc()).is_none());
        assert_eq!(with_placement_environment(&data, &node, |environment| environment.monitor.cached_snapshot().sequence), sequence);
        assert_eq!(live_daemon_dfs_content_advertisement(&data, &owner, time::OffsetDateTime::now_utc()).unwrap(), live);
        drop(other); drop(owner); fixture.close().unwrap();
    }

    #[test]
    fn live_owner_consumer_keeps_kv_expired_and_invalidates_on_endpoint_publication() {
        let deadline = tokio::time::Instant::now() + sinomo_core::network::DEFAULT_NETWORK_TIMEOUT;
        let (mut fixture, nodes) = crate::test_support::DfsTestRuntime::start_cluster("live-owner-consumer-fence", 2).unwrap();
        let local = nodes[0].clone(); let remote = nodes[1].clone();
        let data = fixture.data_dir(&local).clone();
        let paths = ClusterPaths::new(&data);
        let runtime = fixture.runtime_handle();
        let state = sinomo_core::cluster::read_cluster_state_without_kv(&paths).unwrap();
        let owner = registered_dfs_runtime_host(&data).unwrap().unwrap();
        let target = super::super::data_plane::DaemonDfsDataPlaneHost::new_on_runtime(
            DfsRuntimePaths::for_host_role(fixture.data_dir(&remote), sinomo_module::ComponentHostRole::Embedded), &runtime,
        );
        let live = fixture.runtime_handle().block_on(target.wait_live_content_advertisement_for_test(fixture.data_dir(&remote), deadline)).unwrap();
        let mut expired = live.clone(); expired.updated_at -= time::Duration::hours(1);
        expired.expires_at = Some(time::OffsetDateTime::now_utc() - time::Duration::seconds(1));
        let mut client = DaemonDfsControlPlaneKvClient::new_on_runtime(&paths, &runtime);
        fixture.runtime_handle().block_on(crate::publish_dfs_content_advertisement_async(&mut client, &remote, expired.clone())).unwrap();
        refresh_daemon_dfs_remote_placement_observation(&DfsRuntimePaths::for_host_role(&data, sinomo_module::ComponentHostRole::Embedded)).unwrap();
        let authority = super::super::message_host::daemon_dfs_metadata_authority_generation_for_identity(
            &paths, &state.cluster_id, &local, Some(&runtime),
        ).unwrap();
        let configuration = fixture.runtime_handle().block_on(authority.ready_write_configuration_projection_async()).unwrap().unwrap();
        fixture.runtime_handle().block_on(refresh_live_content_owner_observations(
            &data, &owner, &configuration, &authority, tokio::time::Instant::now() + Duration::from_secs(3), None,
        ));
        assert!(with_placement_environment(&data, &local, |environment| environment.live_advertisements.contains_key(&remote)),
            "the actual target's accepted memory sample must supplement its expired canonical baseline");
        let key = crate::content_service_key(&remote);
        let stored = fixture.runtime_handle().block_on(crate::AsyncDfsControlPlaneKvClient::get_async(&client, &key)).unwrap().unwrap();
        assert_eq!(stored.decode::<crate::DfsNodeAdvertisement>().unwrap(), expired, "freshness must not rewrite canonical KV");
        let endpoint = sinomo_core::network::daemon_lan_endpoint(&remote, "127.0.0.1:39999".into(), 10);
        let key = sinomo_core::cluster::cluster_endpoint_key(&remote, &endpoint);
        fixture.runtime_handle().block_on(crate::AsyncDfsControlPlaneKvClient::put_async(
            &mut client, key, sinomo_core::kv::encode_kv_value(&endpoint).unwrap(),
        )).unwrap();
        assert!(!with_placement_environment(&data, &local, |environment| environment.live_advertisements.contains_key(&remote)),
            "a new endpoint generation must invalidate the prior owner observation");
        // Hold an actual authenticated owner response while a new semantic
        // configuration commits. The returned old body cannot populate the
        // new placement generation even though it matched the original cut.
        refresh_daemon_dfs_remote_placement_observation(&DfsRuntimePaths::for_host_role(&data, sinomo_module::ComponentHostRole::Embedded)).unwrap();
        let configuration = fixture.runtime_handle().block_on(authority.ready_write_configuration_projection_async()).unwrap().unwrap();
        let gate = LiveReplyGate { inner: owner.object_rpc.clone(), entered: Arc::new(tokio::sync::Notify::new()), release: Arc::new(tokio::sync::Notify::new()) };
        fixture.runtime_handle().block_on(async {
            let refresh = refresh_live_content_owner_observations(&data, &owner, &configuration, &authority,
                tokio::time::Instant::now() + Duration::from_secs(3), Some(&gate));
            tokio::pin!(refresh);
            tokio::select! {
                _ = gate.entered.notified() => {},
                _ = &mut refresh => panic!("the expired canonical record must request its real owner"),
            }
            let mut changed = expired.clone(); changed.zone = Some("new-confirmed-zone".into());
            crate::publish_dfs_content_advertisement_async(&mut client, &remote, changed).await.unwrap();
            gate.release.notify_one();
            refresh.await;
        });
        assert!(!with_placement_environment(&data, &local, |environment| environment.live_advertisements.contains_key(&remote)),
            "a real response from an earlier confirmed cut must be discarded");
        drop(configuration); drop(authority); drop(client); drop(owner); drop(target); fixture.close().unwrap();
    }

    #[test]
    fn future_derived_snapshot_subscription_cannot_grant_remote_placement() {
        derived_future_remote_event_cannot_raise_native_floor(false);
    }

    #[test]
    fn future_known_prestate_gap_subscription_cannot_strand_remote_placement() {
        derived_future_remote_event_cannot_raise_native_floor(true);
    }

    #[test]
    fn native_gap_subscription_withdraws_until_resident_release_and_recovers() {
        use sinomo_core::kv::{KvCommand, KvOp, KvRevision};
        let root = sinomo_testdir::TestDir::new("dfs-placement-real-native-gap");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named("placement-native-gap-local").unwrap();
        let cluster =
            sinomo_core::node::ClusterId::from_legacy_named("placement-native-gap-cluster")
                .unwrap();
        let state = sinomo_core::ClusterBootstrapState::initialized_locally(
            cluster,
            "native placement gap",
            local.clone(),
            "local",
            "local-key",
            time::OffsetDateTime::now_utc(),
        );
        sinomo_core::cluster::write_cluster_state(&paths, &state).unwrap();
        let kv_paths = paths.kv_paths();
        let native = kv_paths.raft_store();
        native.set_leader_compact(&local, 1).unwrap();
        let baseline = native
            .append_command_and_publish(
                &local,
                KvCommand::Op(KvOp::Put {
                    key: "/native/anchor".into(),
                    value: b"one".to_vec(),
                    lease_id: None,
                }),
                |_| false,
            )
            .unwrap();
        assert_eq!(baseline.revision, KvRevision(1));
        let _adapter =
            subscribe_daemon_dfs_remote_placement_observation(&DaemonPaths::new(&data_dir))
                .unwrap()
                .unwrap();
        let cache = refresh_daemon_dfs_placement_snapshot(&data_dir, &paths, &local, Vec::new());
        let canonical = cache.snapshot().unwrap();
        let result = native
            .append_command_with_response(
                &local,
                KvCommand::Op(KvOp::Delete {
                    key: "/native/missing".into(),
                }),
            )
            .unwrap();
        assert_eq!(result.snapshot.revision, KvRevision(1));
        assert!(result.events.is_empty());
        assert!(result.observation_receipt.is_some());
        let held = native.open_node(&local).unwrap();
        let actual_index = held.applied_index();
        assert_eq!(
            actual_index, 2,
            "real no-state-change commit advanced native prefix"
        );
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&delivered);
        let _witness = sinomo_core::cluster::subscribe_cluster_kv_committed_observations(
            &paths,
            move |event| {
                observed.lock().unwrap().push((
                    event.revision,
                    event.applied_index,
                    matches!(
                        event.value,
                        sinomo_core::kv::KvCommittedObservationValue::Discontinuity(_)
                    ),
                ));
            },
        );
        let dirty_before = daemon_dfs_storage_environment_dirty_generation(&data_dir);
        let notify = daemon_dfs_storage_environment_notify(&data_dir);
        let mut withdrawn = Box::pin(notify.notified());
        // Consume one completed startup permit, then register the exact wake
        // before publication. No polling or another source edge is involved.
        if withdrawn.as_mut().enable() {
            withdrawn = Box::pin(notify.notified());
            assert!(!withdrawn.as_mut().enable());
        }
        let (finished, completed) = std::sync::mpsc::channel();
        let publication = std::thread::spawn(move || {
            let receipt = result.observation_receipt.as_ref().unwrap();
            receipt.abandon_publication().unwrap();
            receipt.wait_delivered().unwrap();
            finished.send(()).unwrap();
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime
            .block_on(async {
                tokio::time::timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT, withdrawn).await
            })
            .expect("genuine native withdrawal precedes the busy resident wait");
        assert!(cache.snapshot().is_none());
        assert_eq!(
            daemon_dfs_storage_environment_dirty_generation(&data_dir),
            dirty_before + 1
        );
        assert!(
            completed.try_recv().is_err(),
            "delivery waits for the existing native owner"
        );
        assert!(
            delivered.lock().unwrap().is_empty(),
            "observer order remains blocked on the native recovery callback"
        );
        drop(held);
        completed
            .recv_timeout(sinomo_core::network::DEFAULT_NETWORK_TIMEOUT)
            .unwrap();
        publication.join().unwrap();
        let recovered = cache
            .snapshot()
            .expect("native release restores placement without another source edge");
        assert!(!Arc::ptr_eq(&canonical, &recovered));
        assert_eq!(
            recovered
                .nodes
                .iter()
                .map(|node| &node.node_id)
                .collect::<Vec<_>>(),
            canonical
                .nodes
                .iter()
                .map(|node| &node.node_id)
                .collect::<Vec<_>>(),
            "native recovery preserves the canonical placement targets",
        );
        assert_eq!(
            with_placement_environment(&data_dir, &local, |environment| environment
                .remote_projection_revision),
            1
        );
        assert_eq!(
            delivered.lock().unwrap().as_slice(),
            &[(Some(KvRevision(1)), Some(actual_index), true)]
        );
        assert_eq!(
            daemon_dfs_storage_environment_dirty_generation(&data_dir),
            dirty_before + 1
        );
    }

    #[test]
    fn old_placement_advertisement_bundle_cannot_publish_after_gap_and_fresh_recovery() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-delayed-old-bundle");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named("placement-delayed-local").unwrap();
        let remote = NodeId::from_legacy_named("placement-delayed-old-remote").unwrap();
        let old_generation = with_placement_environment(&data_dir, &local, |environment| {
            environment.remote_projection_generation
        });
        let old_ads = vec![sinomo_dfs::DfsNodeAdvertisement::content(remote.clone())];
        withdraw_daemon_dfs_remote_placement_projection(
            &data_dir,
            &local,
            Some(sinomo_core::kv::KvRevision(7)),
        );
        assert!(rebuild_daemon_dfs_remote_placement_from_reader(
            &data_dir,
            &paths,
            &local,
            || Ok((sinomo_core::kv::KvRevision(7), Vec::new()))
        ));
        let cache = cached_daemon_dfs_placement_snapshot_cache(&data_dir, &local);
        let fresh = cache.snapshot().unwrap();
        refresh_daemon_dfs_placement_snapshot_at_generation(
            &data_dir,
            &paths,
            &local,
            old_ads.clone(),
            old_generation,
        );
        assert!(Arc::ptr_eq(&fresh, &cache.snapshot().unwrap()));
        refresh_daemon_dfs_placement_snapshot(&data_dir, &paths, &local, old_ads);
        assert!(
            !cache
                .snapshot()
                .unwrap()
                .nodes
                .iter()
                .any(|node| node.node_id == remote),
            "maintenance cannot replace the initialized resident projection with old candidates"
        );
    }

    #[test]
    fn resident_placement_read_crossing_equal_revision_gap_cannot_republish() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-read-crossing-gap");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named("placement-read-gap-local").unwrap();
        let cache = cached_daemon_dfs_placement_snapshot_cache(&data_dir, &local);
        let rebuilt =
            rebuild_daemon_dfs_remote_placement_from_reader(&data_dir, &paths, &local, || {
                // The coherent resident bundle was captured before this gap. Its
                // equal revision cannot establish post-gap coverage.
                withdraw_daemon_dfs_remote_placement_projection(
                    &data_dir,
                    &local,
                    Some(sinomo_core::kv::KvRevision(7)),
                );
                Ok((sinomo_core::kv::KvRevision(7), Vec::new()))
            });
        assert!(!rebuilt);
        assert!(cache.snapshot().is_none());
        assert!(with_placement_environment(
            &data_dir,
            &local,
            |environment| environment.remote_projection_pending
        ));
        assert!(rebuild_daemon_dfs_remote_placement_from_reader(
            &data_dir,
            &paths,
            &local,
            || Ok((sinomo_core::kv::KvRevision(7), Vec::new()))
        ));
        assert!(cache.snapshot().is_some());
    }

    #[test]
    fn remote_placement_gap_withdraws_shared_cache_and_failed_rebuild_stays_unknown() {
        let root = sinomo_testdir::TestDir::new("dfs-placement-observation-gap");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().unwrap();
        let local = NodeId::from_legacy_named("placement-gap-local").unwrap();
        let remote = NodeId::from_legacy_named("placement-gap-remote").unwrap();
        let mut advertisement =
            cached_daemon_dfs_local_content_advertisement(&data_dir, &local, None);
        advertisement.node_id = remote.clone();
        let cache = with_placement_environment(&data_dir, &local, |environment| {
            environment
                .advertisements
                .insert(remote.clone(), advertisement);
            environment.remote_projection_revision = 7;
            environment.cache.clone()
        });
        refresh_daemon_dfs_placement_snapshot(
            &data_dir,
            &paths,
            &local,
            with_placement_environment(&data_dir, &local, |environment| {
                environment.advertisements.values().cloned().collect()
            }),
        );
        assert!(cache.snapshot().is_some());
        withdraw_daemon_dfs_remote_placement_projection(
            &data_dir,
            &local,
            Some(sinomo_core::kv::KvRevision(7)),
        );
        assert!(
            cache.snapshot().is_none(),
            "equal-revision gap must withdraw shared eligibility"
        );
        assert!(!with_placement_environment(
            &data_dir,
            &local,
            |environment| environment.advertisements.contains_key(&remote)
        ));
        assert!(!rebuild_daemon_dfs_remote_placement_projection(
            &data_dir, &paths, &local
        ));
        refresh_daemon_dfs_placement_snapshot(&data_dir, &paths, &local, Vec::new());
        assert!(
            cache.snapshot().is_none(),
            "local refresh cannot publish while remote projection is unknown"
        );
        observe_daemon_dfs_remote_placement_entries(
            &data_dir,
            &paths,
            &local,
            sinomo_core::kv::KvRevision(6),
            std::iter::empty(),
            None,
        );
        assert!(
            cache.snapshot().is_none(),
            "older projection cannot cross the gap floor"
        );
        observe_daemon_dfs_remote_placement_entries(
            &data_dir,
            &paths,
            &local,
            sinomo_core::kv::KvRevision(7),
            std::iter::empty(),
            None,
        );
        assert!(
            cache.snapshot().is_some(),
            "real equal-revision resident coverage recovers"
        );
    }

    #[test]
    fn direct_endpoint_failures_do_not_exclude_routed_placement_candidates() {
        use sinomo_core::ClusterTransportKind;
        use sinomo_core::network::{
            ClusterConnectivityRecord, ClusterConnectivityState, ClusterEndpointId,
            write_local_cluster_connectivity_records,
        };
        use sinomo_core::node::ClusterId;

        let root = sinomo_testdir::TestDir::new("dfs-placement-asymmetric-connectivity");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let local = NodeId::from_legacy_named("placement-public-primary").expect("primary");
        let remote = NodeId::from_legacy_named("placement-private-replica").expect("replica");
        let cluster =
            ClusterId::from_legacy_named("placement-asymmetric-cluster").expect("cluster");
        let now = time::OffsetDateTime::now_utc();
        let observation = |source: &NodeId, target: &NodeId, state| ClusterConnectivityRecord {
            cluster_id: cluster.clone(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint_id: ClusterEndpointId::from_legacy_named(target.as_str()).expect("endpoint"),
            transport: ClusterTransportKind::LanTcp,
            address: format!("{target}.example.test:21353"),
            latency_ms: (state == ClusterConnectivityState::Healthy).then_some(1),
            consecutive_failures: u16::from(state == ClusterConnectivityState::Unreachable),
            last_success_at: None,
            last_failure_at: None,
            failure_reason: None,
            state,
            observed_at: now,
            expires_at: now + time::Duration::minutes(5),
        };
        // A private replica can establish a duplex carrier to the public
        // primary even when the primary cannot dial the replica's LAN address.
        // Direct observations stay directed; routing owns usable carriers.
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            &[observation(
                &local,
                &remote,
                ClusterConnectivityState::Unreachable,
            )],
        )
        .expect("failed direct endpoint");
        write_local_cluster_connectivity_records(
            &paths,
            &remote,
            &[observation(
                &remote,
                &local,
                ClusterConnectivityState::Healthy,
            )],
        )
        .expect("working reverse direction");

        let snapshot = refresh_daemon_dfs_placement_snapshot(
            &data_dir,
            &paths,
            &local,
            vec![sinomo_dfs::DfsNodeAdvertisement::content(remote.clone())],
        )
        .snapshot()
        .expect("placement snapshot");
        let request = sinomo_dfs::DfsReplicaPlacementRequest {
            file_system_id: sinomo_dfs::DfsFileSystemId::from_legacy_named("placement-fs")
                .expect("filesystem"),
            object_id: sinomo_dfs::DfsObjectId::from_legacy_named("placement-object")
                .expect("object"),
            target_version: sinomo_dfs::DfsVersion::from_legacy_named("placement-version")
                .expect("version"),
            coordinator_node_id: local.clone(),
            policy: sinomo_dfs::DfsReplicationPolicy::new(2, 2).expect("W=2, N=2"),
            candidates: vec![
                sinomo_dfs::DfsReplicaCandidate::new(local.clone()),
                sinomo_dfs::DfsReplicaCandidate::new(remote.clone()),
            ],
        };
        let placement =
            sinomo_dfs::explain_replicas_with_placement_snapshot(&request, &snapshot, &[], &[])
                .expect("endpoint failure must leave routed admission to networking");
        assert_eq!(placement.plan.desired_replica_nodes.len(), 2);
        assert_eq!(placement.plan.preferred_commit_nodes.len(), 2);
        assert_eq!(
            snapshot
                .path(&local, &remote)
                .expect("outbound observation")
                .state,
            sinomo_dfs::DfsPlacementPathState::Unknown,
            "a reverse observation must not synthesize healthy outbound connectivity"
        );
        assert_eq!(
            snapshot
                .path(&remote, &local)
                .expect("inbound observation")
                .state,
            sinomo_dfs::DfsPlacementPathState::Healthy,
        );

        let mut critical = (*snapshot).clone();
        let replica = critical
            .nodes
            .iter_mut()
            .find(|node| node.node_id == remote)
            .expect("remote storage observation");
        replica.storage_health = sinomo_dfs::DfsHealthState::Critical;
        replica.storage_freshness = sinomo_dfs::DfsObservationFreshness::Fresh;
        replica.accepts_new_content = false;
        assert!(
            matches!(
                sinomo_dfs::explain_replicas_with_placement_snapshot(&request, &critical, &[], &[]),
                Err(sinomo_dfs::DfsDistributedError::InvalidReplicaSet { .. })
            ),
            "storage admission and N must remain enforced"
        );
    }

    #[test]
    fn committed_snapshot_updates_remote_placement_without_polling() {
        let root = sinomo_testdir::TestDir::new("daemon-dfs-remote-placement-subscription");
        let data_dir = SinomoDataDir::new(&root);
        let cluster_paths = ClusterPaths::new(&data_dir);
        cluster_paths.ensure().expect("cluster paths");
        let now = time::OffsetDateTime::now_utc();
        let cluster_id = sinomo_core::node::ClusterId::from_legacy_named(
            "cluster_dfs_remote_placement_subscription",
        )
        .expect("cluster");
        let local = NodeId::from_legacy_named("node_dfs_remote_placement_local").expect("local");
        let remote = NodeId::from_legacy_named("node_dfs_remote_placement_remote").expect("remote");
        let local_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            local.clone(),
            "local",
            "local-public-key",
            now,
        );
        let remote_member = sinomo_core::ClusterMemberRecord::local_voter(
            cluster_id,
            remote.clone(),
            "remote",
            "remote-public-key",
            now,
        );
        let mut store = sinomo_core::kv::InProcessKvStore::new();
        store
            .put_serde(
                sinomo_core::node::cluster_member_key(&local),
                &local_member,
                None,
            )
            .expect("local member");
        store
            .put_serde(
                sinomo_core::node::cluster_member_key(&remote),
                &remote_member,
                None,
            )
            .expect("remote member");
        let remote_key = sinomo_dfs::content_service_key(&remote);
        store
            .put_serde(
                remote_key.clone(),
                &sinomo_dfs::DfsNodeAdvertisement::content(remote.clone()),
                None,
            )
            .expect("remote advertisement");

        observe_daemon_dfs_remote_placement_snapshot(
            &data_dir,
            &cluster_paths,
            &local,
            &store.snapshot().expect("advertised snapshot"),
            None,
        );
        let cache = cached_daemon_dfs_placement_snapshot_cache(&data_dir, &local);
        assert!(
            cache
                .snapshot()
                .expect("placement snapshot")
                .nodes
                .iter()
                .any(|node| node.node_id == remote)
        );

        store.delete(remote_key).expect("remove advertisement");
        observe_daemon_dfs_remote_placement_snapshot(
            &data_dir,
            &cluster_paths,
            &local,
            &store.snapshot().expect("removed snapshot"),
            None,
        );
        assert!(
            cache
                .snapshot()
                .expect("updated placement snapshot")
                .nodes
                .iter()
                .all(|node| node.node_id != remote)
        );
    }

    #[test]
    fn advertisement_semantics_ignore_sample_only_measurement_changes() {
        let node_id = NodeId::from_legacy_named("placement-advertisement-node").expect("node");
        let mut left = sinomo_dfs::DfsNodeAdvertisement::content(node_id);
        left.storage_health = sinomo_dfs::DfsHealthState::Healthy;
        left.storage_freshness = sinomo_dfs::DfsObservationFreshness::Fresh;
        let mut right = left.clone();
        right.usable_capacity_bytes = Some(42);
        right.advertisement_generation = 9;
        assert!(daemon_advertisement_semantics_equal(&left, &right));
        right.storage_health = sinomo_dfs::DfsHealthState::Degraded;
        assert!(!daemon_advertisement_semantics_equal(&left, &right));
    }

    #[test]
    fn host_resource_metering_requires_an_observed_snapshot() {
        assert_eq!(
            dfs_network_metering_from_node_resources(sinomo_device::NodeResourceSnapshot::default()),
            None,
            "startup uncertainty must retain the daemon's native OS sampler"
        );
        assert_eq!(
            dfs_network_metering_from_node_resources(
                sinomo_device::NodeResourceSnapshot::observed(
                    sinomo_device::NodeResourceMode::Constrained,
                    sinomo_device::NodeNetworkMetering::Metered,
                )
            ),
            Some(sinomo_dfs::DfsNetworkMetering::Metered)
        );
        assert_eq!(
            dfs_resource_pressure_from_node_resources(
                sinomo_device::NodeResourceSnapshot::observed(
                    sinomo_device::NodeResourceMode::Restricted,
                    sinomo_device::NodeNetworkMetering::Unmetered,
                )
            ),
            sinomo_dfs::DfsResourcePressure::Restricted
        );
    }

    #[test]
    fn unchanged_advertisement_never_publishes_from_time_alone() {
        let node_id = NodeId::from_legacy_named("placement-advertisement-lease").unwrap();
        let now = time::OffsetDateTime::now_utc();
        let mut current = sinomo_dfs::DfsNodeAdvertisement::content(node_id);
        let ttl = time::Duration::try_from(sinomo_dfs::DEFAULT_DFS_STORAGE_OBSERVATION_TTL).unwrap();
        current.expires_at = Some(now + ttl);
        let desired = current.clone();
        for at in [now, now + ttl - DAEMON_DFS_ADVERTISEMENT_RENEWAL_MARGIN, now + ttl, now + ttl + time::Duration::seconds(1)] {
            assert!(!daemon_advertisement_publish_required(Some(&current), &desired, at), "elapsed time alone cannot publish");
        }
        let mut changed = desired.clone(); changed.accepts_content = false;
        assert!(daemon_advertisement_publish_required(Some(&current), &changed, now));
        assert!(daemon_advertisement_publish_required(None, &desired, now), "initial activity still publishes");
    }

    #[test]
    fn path_metering_requires_both_ends_to_be_known_unmetered() {
        use sinomo_dfs::DfsNetworkMetering::{Metered, Unknown, Unmetered};

        assert_eq!(aggregate_path_metering(Some(Unmetered), None), Unknown);
        assert_eq!(
            aggregate_path_metering(Some(Unmetered), Some(Unmetered)),
            Unmetered
        );
        assert_eq!(aggregate_path_metering(Some(Metered), None), Metered);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn network_manager_unknown_is_not_unmetered() {
        assert_eq!(
            parse_network_manager_metering(
                "GENERAL.STATE:100 (connected)\nGENERAL.METERED:unknown\n"
            ),
            sinomo_dfs::DfsNetworkMetering::Unknown
        );
        assert_eq!(
            parse_network_manager_metering("GENERAL.STATE:100 (connected)\nGENERAL.METERED:yes\n"),
            sinomo_dfs::DfsNetworkMetering::Metered
        );
    }

    #[test]
    fn windows_metering_aggregation_preserves_unknowns() {
        use sinomo_dfs::DfsNetworkMetering::{Metered, Unknown, Unmetered};

        assert_eq!(aggregate_active_windows_network_metering([]), Unknown);
        assert_eq!(
            aggregate_active_windows_network_metering([Unmetered, Unmetered]),
            Unmetered
        );
        assert_eq!(
            aggregate_active_windows_network_metering([Unmetered, Unknown]),
            Unknown
        );
        assert_eq!(
            aggregate_active_windows_network_metering([Unknown, Metered]),
            Metered
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_native_metering_probe_is_bounded() {
        let started = Instant::now();
        let observation = sample_windows_network_metering();
        assert!(matches!(
            observation,
            sinomo_dfs::DfsNetworkMetering::Unknown
                | sinomo_dfs::DfsNetworkMetering::Unmetered
                | sinomo_dfs::DfsNetworkMetering::Metered
        ));
        assert!(
            started.elapsed() <= DAEMON_DFS_METERING_QUERY_TIMEOUT + Duration::from_secs(1),
            "native Windows metering must not block daemon lease renewal"
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_native_metering_query_uses_standard_thread_stack() {
        let query = std::thread::Builder::new()
            .name("sinomo-winrt-metering-stack-test".to_string())
            .spawn(|| {
                let _apartment = WindowsRuntimeApartment::initialize()?;
                query_windows_network_metering()
            })
            .expect("spawn standard-stack WinRT query")
            .join()
            .expect("standard-stack WinRT query thread must not overflow")
            .expect("standard-stack WinRT query must complete");
        assert!(matches!(
            query,
            sinomo_dfs::DfsNetworkMetering::Unknown
                | sinomo_dfs::DfsNetworkMetering::Unmetered
                | sinomo_dfs::DfsNetworkMetering::Metered
        ));
    }
}
