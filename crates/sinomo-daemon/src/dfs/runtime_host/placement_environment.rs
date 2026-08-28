use super::*;
use sinomo_core::cluster::ClusterKvPathsExt;
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};

#[derive(Debug)]
struct DaemonDfsPlacementEnvironment {
    monitor: sinomo_dfs::DfsDeviceHealthMonitor,
    cache: sinomo_dfs::DfsPlacementSnapshotCache,
    metering: sinomo_dfs::DfsNetworkMetering,
    metering_sampled_at: Option<Instant>,
    last_published_local_advertisement: Option<sinomo_dfs::DfsNodeAdvertisement>,
    advertisements: BTreeMap<NodeId, sinomo_dfs::DfsNodeAdvertisement>,
    next_remote_observation_index: usize,
}

static DAEMON_DFS_PLACEMENT_ENVIRONMENTS: OnceLock<
    Mutex<BTreeMap<PathBuf, DaemonDfsPlacementEnvironment>>,
> = OnceLock::new();

const DAEMON_DFS_METERING_SAMPLE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const DAEMON_DFS_ADVERTISEMENT_RENEWAL_MARGIN: time::Duration = time::Duration::seconds(30);
#[cfg(any(target_os = "linux", target_os = "windows"))]
const DAEMON_DFS_METERING_QUERY_TIMEOUT: Duration = Duration::from_secs(2);

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
                vec![sinomo_dfs::DfsContentLocationConfig::legacy(
                    data_dir.dfs_dir().join("content"),
                )],
            ),
            cache: sinomo_dfs::DfsPlacementSnapshotCache::default(),
            metering: sinomo_dfs::DfsNetworkMetering::Unknown,
            metering_sampled_at: None,
            last_published_local_advertisement: None,
            advertisements: BTreeMap::new(),
            next_remote_observation_index: 0,
        });
    operation(environment)
}

fn refresh_os_metering(environment: &mut DaemonDfsPlacementEnvironment) {
    if environment
        .metering_sampled_at
        .is_some_and(|sampled| sampled.elapsed() < DAEMON_DFS_METERING_SAMPLE_INTERVAL)
    {
        return;
    }
    environment.metering = sample_os_network_metering();
    environment.metering_sampled_at = Some(Instant::now());
}

pub(super) fn daemon_dfs_local_content_advertisement(
    data_dir: &SinomoDataDir,
    node_id: &NodeId,
    zone: Option<String>,
) -> sinomo_dfs::DfsNodeAdvertisement {
    with_placement_environment(data_dir, node_id, |environment| {
        refresh_os_metering(environment);
        let now = time::OffsetDateTime::now_utc();
        environment.monitor.sample_if_due(now, environment.metering);
        environment.monitor.aggregate_advertisement(zone, now)
    })
}

fn cached_daemon_dfs_local_content_advertisement(
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

pub(super) fn refresh_daemon_dfs_placement_snapshot(
    data_dir: &SinomoDataDir,
    cluster_paths: &ClusterPaths,
    local_node_id: &NodeId,
    mut advertisements: Vec<sinomo_dfs::DfsNodeAdvertisement>,
) -> sinomo_dfs::DfsPlacementSnapshotCache {
    // Data-plane shells consume the daemon cache only. Sampling is owned by
    // the lifecycle lane above and never runs in a DFS request constructor.
    let local_advertisement = cached_daemon_dfs_local_content_advertisement(
        data_dir,
        local_node_id,
        daemon_dfs_local_zone(),
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
                    sinomo_dfs::DfsPlacementPathState::Unreachable
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
        environment.cache.publish(snapshot);
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
) -> Result<Option<PreparedDaemonDfsStorageEnvironmentRefresh>, DaemonError> {
    if crate::daemon_paths_are_shadow(paths) {
        return Ok(None);
    }
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let desired = daemon_dfs_local_content_advertisement(
        &data_dir,
        &state.local_node_id,
        daemon_dfs_local_zone(),
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

pub(crate) async fn refresh_daemon_dfs_storage_environment_on_runtime(
    paths: &DaemonPaths,
    runtime: &tokio::runtime::Handle,
) -> Result<bool, DaemonError> {
    let prepare_paths = paths.clone();
    let Some(prepared) = tokio::task::spawn_blocking(move || {
        prepare_daemon_dfs_storage_environment_refresh(&prepare_paths)
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
        let advertisements =
            with_placement_environment(&update_data_dir, &update_local_node_id, |environment| {
                environment
                    .advertisements
                    .insert(update_local_node_id.clone(), desired);
                environment
                    .advertisements
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            });
        refresh_daemon_dfs_placement_snapshot(
            &update_data_dir,
            &update_cluster_paths,
            &update_local_node_id,
            advertisements,
        );
    })
    .await
    .map_err(|error| DaemonError::Network {
        address: "DFS placement snapshot cache".to_string(),
        detail: error.to_string(),
    })?;
    Ok(published)
}

struct PreparedDaemonDfsRemotePlacementObservation {
    data_dir: SinomoDataDir,
    cluster_paths: ClusterPaths,
    local_node_id: NodeId,
    remote_node_id: Option<NodeId>,
}

fn prepare_daemon_dfs_remote_placement_observation(
    paths: &DaemonPaths,
) -> Result<Option<PreparedDaemonDfsRemotePlacementObservation>, DaemonError> {
    if crate::daemon_paths_are_shadow(paths) {
        return Ok(None);
    }
    let data_dir = SinomoDataDir::new(daemon_data_dir(paths)?);
    let cluster_paths = ClusterPaths::new(&data_dir);
    if !cluster_paths.state.exists() {
        return Ok(None);
    }
    let state = sinomo_core::cluster::read_cluster_state_without_kv(&cluster_paths)?;
    let mut remote_node_ids = state
        .members
        .iter()
        .map(|member| member.node_id.clone())
        .filter(|node_id| node_id != &state.local_node_id)
        .collect::<Vec<_>>();
    remote_node_ids.sort();
    remote_node_ids.dedup();
    let remote_node_id_set = remote_node_ids.iter().cloned().collect::<BTreeSet<_>>();
    let remote_node_id =
        with_placement_environment(&data_dir, &state.local_node_id, |environment| {
            environment.advertisements.retain(|node_id, _| {
                node_id == &state.local_node_id || remote_node_id_set.contains(node_id)
            });
            if remote_node_ids.is_empty() {
                environment.next_remote_observation_index = 0;
                None
            } else {
                let index = environment.next_remote_observation_index % remote_node_ids.len();
                environment.next_remote_observation_index = index.saturating_add(1);
                Some(remote_node_ids[index].clone())
            }
        });
    Ok(Some(PreparedDaemonDfsRemotePlacementObservation {
        data_dir,
        cluster_paths,
        local_node_id: state.local_node_id,
        remote_node_id,
    }))
}

pub(crate) async fn refresh_daemon_dfs_remote_placement_observation_on_runtime(
    paths: &DaemonPaths,
    _runtime: &tokio::runtime::Handle,
) -> Result<(), DaemonError> {
    let prepare_paths = paths.clone();
    let Some(prepared) = tokio::task::spawn_blocking(move || {
        prepare_daemon_dfs_remote_placement_observation(&prepare_paths)
    })
    .await
    .map_err(|error| DaemonError::Network {
        address: "DFS remote placement observation preparation".to_string(),
        detail: error.to_string(),
    })??
    else {
        return Ok(());
    };
    let PreparedDaemonDfsRemotePlacementObservation {
        data_dir,
        cluster_paths,
        local_node_id,
        remote_node_id,
    } = prepared;
    tokio::task::spawn_blocking(move || -> Result<(), DaemonError> {
        // Placement advertisements are leased observations, not authority.
        // Read the replicated local projection so telemetry cannot consume or
        // queue behind Raft ReadIndex. Expired records remain present only as
        // stale/unknown evidence and are never treated as healthy, fast, or
        // unmetered by the snapshot planner.
        let remote_advertisement = remote_node_id
            .as_ref()
            .map(|remote_node_id| {
                sinomo_core::kv::read_cluster_kv_entry(
                    &cluster_paths.kv_paths(),
                    &sinomo_dfs::content_service_key(remote_node_id),
                )
                .and_then(|entry| {
                    entry
                        .map(|entry| entry.decode::<sinomo_dfs::DfsNodeAdvertisement>())
                        .transpose()
                })
            })
            .transpose()?
            .flatten();
        let advertisements = with_placement_environment(&data_dir, &local_node_id, |environment| {
            if let Some(remote_node_id) = remote_node_id {
                if let Some(advertisement) = remote_advertisement {
                    environment
                        .advertisements
                        .insert(remote_node_id, advertisement);
                } else {
                    environment.advertisements.remove(&remote_node_id);
                }
            }
            environment
                .advertisements
                .values()
                .cloned()
                .collect::<Vec<_>>()
        });
        refresh_daemon_dfs_placement_snapshot(
            &data_dir,
            &cluster_paths,
            &local_node_id,
            advertisements,
        );
        Ok(())
    })
    .await
    .map_err(|error| DaemonError::Network {
        address: "DFS remote placement snapshot cache".to_string(),
        detail: error.to_string(),
    })??;
    Ok(())
}

fn daemon_advertisement_semantics_equal(
    left: &sinomo_dfs::DfsNodeAdvertisement,
    right: &sinomo_dfs::DfsNodeAdvertisement,
) -> bool {
    left.node_id == right.node_id
        && left.capacity_weight == right.capacity_weight
        && left.accepts_content == right.accepts_content
        && left.zone == right.zone
        && left.storage_health == right.storage_health
        && left.storage_freshness == right.storage_freshness
        && left.content_location_count == right.content_location_count
        && left.network_metering == right.network_metering
}

fn daemon_advertisement_publish_required(
    current: Option<&sinomo_dfs::DfsNodeAdvertisement>,
    desired: &sinomo_dfs::DfsNodeAdvertisement,
    now: time::OffsetDateTime,
) -> bool {
    current.is_none_or(|current| {
        !daemon_advertisement_semantics_equal(current, desired)
            || current.expires_at.is_none_or(|expires_at| {
                expires_at <= now + DAEMON_DFS_ADVERTISEMENT_RENEWAL_MARGIN
            })
    })
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
        return sample_windows_network_metering();
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
    fn unchanged_advertisement_renews_only_in_final_lease_third() {
        let node_id = NodeId::from_legacy_named("placement-advertisement-lease").expect("node");
        let now = time::OffsetDateTime::now_utc();
        let mut current = sinomo_dfs::DfsNodeAdvertisement::content(node_id);
        current.expires_at = Some(now + time::Duration::seconds(90));
        let desired = current.clone();

        assert!(!daemon_advertisement_publish_required(
            Some(&current),
            &desired,
            now + time::Duration::seconds(59),
        ));
        assert!(daemon_advertisement_publish_required(
            Some(&current),
            &desired,
            now + time::Duration::seconds(60),
        ));
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
