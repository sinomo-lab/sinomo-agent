use crate::{
    DaemonError, DaemonPaths, IdleStatsSnapshot, send_daemon_rpc_async, send_daemon_rpc_on_runtime,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterMemberRole, ClusterMemberState,
    ClusterNodeAvailability, ClusterNodeRolloutStatus, ClusterNodeStatusRecord,
};
use sinomo_local::SinomoDataDir;
use sinomo_network::{ClusterId, DaemonRpcTimeouts};
use sinomo_node::{
    DAEMON_MAIN_CLUSTER_ROLE, DAEMON_SHADOW_CLUSTER_ROLE, NodeId, cluster_node_is_local_process,
};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const SINOMO_DAEMON_DEFAULT_HOST: &str = "127.0.0.1";
pub const SINOMO_DAEMON_DEFAULT_PORT: u16 = 21_353;
pub const SINOMO_SHADOW_DAEMON_DEFAULT_PORT: u16 = 21_354;
pub const SINOMO_DAEMON_ADVERTISE_HOST_ENV: &str = "SINOMO_DAEMON_ADVERTISE_HOST";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonRunOptions {
    pub host: String,
    pub port: u16,
}

impl Default for DaemonRunOptions {
    fn default() -> Self {
        Self {
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
        }
    }
}

impl DaemonRunOptions {
    pub fn shadow_default() -> Self {
        Self {
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonClusterProcess {
    Main,
    Shadow,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonClusterRegistration {
    pub process: DaemonClusterProcess,
    pub main_options: DaemonRunOptions,
    pub shadow_options: DaemonRunOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonProbeStatus {
    Running(DaemonStatus),
    Locked { status: Option<DaemonStatus> },
    NotRunning,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonClusterStatus {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub process: String,
    pub member_role: ClusterMemberRole,
    pub member_state: ClusterMemberState,
}

pub fn daemon_cluster_registration(
    process: DaemonClusterProcess,
    main_options: DaemonRunOptions,
    shadow_options: DaemonRunOptions,
) -> DaemonClusterRegistration {
    DaemonClusterRegistration {
        process,
        main_options,
        shadow_options,
    }
}

pub fn daemon_cluster_status(
    state: &ClusterBootstrapState,
    process: DaemonClusterProcess,
) -> Option<DaemonClusterStatus> {
    let node_id = state.local_node_id.clone();
    let member = state
        .members
        .iter()
        .find(|member| member.node_id == node_id)?;
    Some(DaemonClusterStatus {
        cluster_id: state.cluster_id.clone(),
        node_id,
        process: match process {
            DaemonClusterProcess::Main => DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            DaemonClusterProcess::Shadow => DAEMON_SHADOW_CLUSTER_ROLE.to_string(),
        },
        member_role: member.role.clone(),
        member_state: member.state.clone(),
    })
}

#[cfg(test)]
pub(crate) fn ensure_daemon_cluster_members(
    state: &mut ClusterBootstrapState,
    main_options: &DaemonRunOptions,
    shadow_options: &DaemonRunOptions,
    timestamp: OffsetDateTime,
) -> bool {
    let main_node_id = state.local_node_id.clone();
    let shadow_node_id = sinomo_node::shadow_daemon_node_id(&state.local_node_id);
    let lan_ips = sinomo_network::discover_default_lan_ip_candidates();
    let route_ips = sinomo_network::discover_peer_route_ip_candidates(state);
    let main_endpoints = daemon_advertised_endpoints_for_state(
        state,
        &main_node_id,
        main_options,
        0,
        &lan_ips,
        &route_ips,
    );
    let shadow_endpoints = daemon_advertised_endpoints_for_state(
        state,
        &shadow_node_id,
        shadow_options,
        10,
        &lan_ips,
        &route_ips,
    );
    sinomo_network::ensure_daemon_cluster_members(
        state,
        main_endpoints,
        shadow_endpoints,
        timestamp,
    )
}

pub fn daemon_advertised_endpoints_for_state(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    let allow_public_ip_discovery = cluster_node_is_local_process(state, node_id);
    daemon_advertised_endpoints_from_ips(
        node_id,
        options,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
}

pub async fn daemon_advertised_endpoints_for_state_async(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
) -> Vec<ClusterEndpointRecord> {
    let allow_public_ip_discovery = cluster_node_is_local_process(state, node_id);
    daemon_advertised_endpoints_from_ips_async(
        node_id,
        options,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
    .await
}

fn daemon_advertised_endpoints_from_ips(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_endpoints: bool,
) -> Vec<ClusterEndpointRecord> {
    let advertise_host = std::env::var(SINOMO_DAEMON_ADVERTISE_HOST_ENV).ok();
    sinomo_network::daemon_advertised_endpoints_from_static_addresses(
        node_id,
        daemon_connect_address(&options.host, options.port),
        &options.host,
        options.port,
        priority,
        lan_ips,
        route_ips,
        allow_public_endpoints,
        advertise_host.as_deref(),
    )
}

async fn daemon_advertised_endpoints_from_ips_async(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
) -> Vec<ClusterEndpointRecord> {
    let advertise_host = std::env::var(SINOMO_DAEMON_ADVERTISE_HOST_ENV).ok();
    sinomo_network::daemon_advertised_endpoints_from_addresses_with_public_discovery_and_advertise_host_async(
        node_id,
        daemon_connect_address(&options.host, options.port),
        &options.host,
        options.port,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
        advertise_host.as_deref(),
    )
    .await
}

pub fn current_shadow_daemon_options(data_dir: &Path, timeout: Duration) -> DaemonRunOptions {
    match probe_daemon_status(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        timeout,
    ) {
        DaemonProbeStatus::Running(status) => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status } => status
            .as_ref()
            .map(daemon_options_from_status)
            .unwrap_or_else(DaemonRunOptions::shadow_default),
        DaemonProbeStatus::NotRunning => DaemonRunOptions::shadow_default(),
    }
}

pub(crate) fn current_shadow_daemon_options_on_runtime(
    data_dir: &Path,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> DaemonRunOptions {
    match probe_daemon_status_on_runtime(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        timeout,
        runtime,
    ) {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => daemon_options_from_status(&status),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => {
            DaemonRunOptions::shadow_default()
        }
    }
}

pub fn current_daemon_cluster_options(
    data_dir: &Path,
) -> Option<(DaemonRunOptions, DaemonRunOptions)> {
    let main_options = read_daemon_status(&DaemonPaths::with_root(data_dir.join("daemon")))
        .map(|status| daemon_options_from_status(&status))
        .ok();
    let shadow_options =
        read_daemon_status(&DaemonPaths::with_root(data_dir.join("shadow-daemon")))
            .map(|status| daemon_options_from_status(&status))
            .ok();
    if main_options.is_none() && shadow_options.is_none() {
        return None;
    }
    Some((
        main_options.unwrap_or_else(DaemonRunOptions::default),
        shadow_options.unwrap_or_else(DaemonRunOptions::shadow_default),
    ))
}

pub fn daemon_cluster_status_matches(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    status: &DaemonStatus,
) -> bool {
    status.cluster.as_ref().is_some_and(|cluster| {
        if cluster.node_id != endpoint.node_id {
            return false;
        }
        cluster.cluster_id == state.cluster_id
            || state
                .members
                .iter()
                .find(|member| member.node_id == endpoint.node_id)
                .is_some_and(|member| member.cluster_id == cluster.cluster_id)
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub pid: u32,
    pub host: String,
    pub port: u16,
    pub address: String,
    #[serde(default = "daemon_package_version")]
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_os: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_arch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollout: Option<ClusterNodeRolloutStatus>,
    pub started_at: String,
    pub data_dir: PathBuf,
    pub lock_file: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<DaemonClusterStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_stats: Option<IdleStatsSnapshot>,
}

impl DaemonStatus {
    pub fn new(
        options: &DaemonRunOptions,
        paths: &DaemonPaths,
        cluster: Option<DaemonClusterStatus>,
    ) -> Self {
        Self {
            pid: std::process::id(),
            host: options.host.clone(),
            port: options.port,
            address: daemon_connect_address(&options.host, options.port),
            version: daemon_package_version(),
            commit: daemon_git_commit(),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            executable: std::env::current_exe().ok(),
            rollout: None,
            started_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            data_dir: paths.root.parent().unwrap_or(&paths.root).to_path_buf(),
            lock_file: paths.lock.clone(),
            cluster,
            idle_stats: None,
        }
    }
}

pub fn daemon_package_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Short git commit the daemon binary was built from, embedded by build.rs.
/// Returns None when the build had no git context (commit recorded as "unknown").
pub fn daemon_git_commit() -> Option<String> {
    match option_env!("SINOMO_GIT_COMMIT") {
        Some(commit) if !commit.is_empty() && commit != "unknown" => Some(commit.to_string()),
        _ => None,
    }
}

pub fn daemon_connect_address(host: &str, port: u16) -> String {
    let connect_host = match host {
        "0.0.0.0" | "::" => SINOMO_DAEMON_DEFAULT_HOST,
        value => value,
    };
    format!("{connect_host}:{port}")
}

pub fn daemon_bind_address(status: &DaemonStatus) -> String {
    format!("{}:{}", status.host, status.port)
}

pub fn daemon_options_from_status(status: &DaemonStatus) -> DaemonRunOptions {
    DaemonRunOptions {
        host: status.host.clone(),
        port: status.port,
    }
}

pub fn daemon_status_matches_options(status: &DaemonStatus, host: &str, port: u16) -> bool {
    status.host == host && status.port == port
}

pub fn daemon_cluster_process_from_paths(
    data_dir: &Path,
    paths: &DaemonPaths,
) -> DaemonClusterProcess {
    if paths.root == data_dir.join("shadow-daemon") {
        DaemonClusterProcess::Shadow
    } else {
        DaemonClusterProcess::Main
    }
}

pub fn daemon_cluster_options_from_status(
    data_dir: &Path,
    paths: &DaemonPaths,
    status: &DaemonStatus,
    process: DaemonClusterProcess,
) -> (DaemonRunOptions, DaemonRunOptions) {
    let main_paths = DaemonPaths::with_root(data_dir.join("daemon"));
    let shadow_paths = DaemonPaths::with_root(data_dir.join("shadow-daemon"));
    let main_options = if process == DaemonClusterProcess::Main {
        daemon_options_from_status(status)
    } else {
        read_daemon_status(&main_paths)
            .map(|status| daemon_options_from_status(&status))
            .unwrap_or_else(|_| DaemonRunOptions::default())
    };
    let shadow_options = if paths.root == shadow_paths.root {
        daemon_options_from_status(status)
    } else {
        read_daemon_status(&shadow_paths)
            .map(|status| daemon_options_from_status(&status))
            .unwrap_or_else(|_| DaemonRunOptions::shadow_default())
    };
    (main_options, shadow_options)
}

pub fn cluster_node_status_from_daemon_status(
    status: &DaemonStatus,
    cluster: &DaemonClusterStatus,
    rollout: Option<ClusterNodeRolloutStatus>,
    observed_at: OffsetDateTime,
) -> ClusterNodeStatusRecord {
    ClusterNodeStatusRecord {
        cluster_id: cluster.cluster_id.clone(),
        node_id: cluster.node_id.clone(),
        process: cluster.process.clone(),
        availability: ClusterNodeAvailability::Online,
        daemon_version: status.version.clone(),
        // Use the subject daemon's own commit (carried in its DaemonStatus), not
        // daemon_git_commit() of whatever binary is building this record — otherwise
        // a CLI of a different build that refreshes KV would stamp the node with its
        // own commit instead of the daemon's.
        commit: status.commit.clone(),
        target_os: status.target_os.clone(),
        target_arch: status.target_arch.clone(),
        pid: Some(status.pid),
        address: Some(status.address.clone()),
        bind_address: Some(daemon_bind_address(status)),
        executable: status
            .executable
            .as_ref()
            .map(|path| path.display().to_string()),
        rollout: rollout.or_else(|| status.rollout.clone()),
        daemon_started_at: Some(status.started_at.clone()),
        observed_at,
    }
}

pub fn invite_introducers_from_daemon_status(
    node_id: &NodeId,
    status: &DaemonStatus,
) -> Vec<sinomo_core::ClusterEndpointRecord> {
    if matches!(status.host.as_str(), "0.0.0.0" | "::") {
        let mut endpoints = Vec::new();
        for ip in sinomo_network::discover_default_lan_ip_candidates() {
            endpoints.push(sinomo_network::invite_introducer_endpoint(
                node_id,
                sinomo_network::format_ip_port(ip, status.port),
            ));
        }
        endpoints.push(sinomo_network::invite_introducer_endpoint(
            node_id,
            status.address.clone(),
        ));
        endpoints
    } else {
        vec![sinomo_network::invite_introducer_endpoint(
            node_id,
            status.address.clone(),
        )]
    }
}

pub fn daemon_probe_status_needs_restart_for_options(
    status: DaemonProbeStatus,
    host: &str,
    port: u16,
) -> bool {
    match status {
        DaemonProbeStatus::Running(status)
        | DaemonProbeStatus::Locked {
            status: Some(status),
        } => !daemon_status_matches_options(&status, host, port),
        DaemonProbeStatus::Locked { status: None } | DaemonProbeStatus::NotRunning => false,
    }
}

#[derive(Debug)]
pub struct DaemonLock {
    file: File,
    paths: DaemonPaths,
}

impl DaemonLock {
    pub fn acquire(
        paths: &DaemonPaths,
        options: &DaemonRunOptions,
        cluster: Option<DaemonClusterStatus>,
    ) -> Result<Self, DaemonError> {
        paths.ensure()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&paths.lock)
            .map_err(|source| DaemonError::Io {
                path: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;
        file.try_lock_exclusive()
            .map_err(|source| DaemonError::AlreadyRunning {
                lock_file: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;

        let status = DaemonStatus::new(options, paths, cluster);
        write_daemon_status(paths, &status)?;
        file.set_len(0).map_err(|source| DaemonError::Io {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })?;
        (&file)
            .seek(SeekFrom::Start(0))
            .map_err(|source| DaemonError::Io {
                path: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;
        let status_bytes = sinomo_network::SinomoObjectCodec::default().encode(&status)?;
        (&file)
            .write_all(&status_bytes)
            .map_err(|source| DaemonError::Io {
                path: paths.lock.display().to_string(),
                detail: source.to_string(),
            })?;
        (&file).flush().map_err(|source| DaemonError::Io {
            path: paths.lock.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(Self {
            file,
            paths: paths.clone(),
        })
    }
}

#[derive(Debug)]
pub struct DaemonDataOwnerLock {
    file: File,
    path: PathBuf,
}

impl DaemonDataOwnerLock {
    pub fn acquire(paths: &DaemonPaths) -> Result<Self, DaemonError> {
        paths.ensure()?;
        let path = paths.data_owner_lock.clone();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        file.try_lock_exclusive()
            .map_err(|source| DaemonError::AlreadyRunning {
                lock_file: path.display().to_string(),
                detail: format!(
                    "main daemon data directory is already owned by another process: {source}"
                ),
            })?;
        file.set_len(0).map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        (&file)
            .seek(SeekFrom::Start(0))
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        (&file)
            .write_all(format!("pid={}\n", std::process::id()).as_bytes())
            .map_err(|source| DaemonError::Io {
                path: path.display().to_string(),
                detail: source.to_string(),
            })?;
        (&file).flush().map_err(|source| DaemonError::Io {
            path: path.display().to_string(),
            detail: source.to_string(),
        })?;
        Ok(Self { file, path })
    }
}

impl Drop for DaemonDataOwnerLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
        let _ = fs::remove_file(&self.path);
    }
}

impl Drop for DaemonLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.paths.status);
        let _ = self.file.unlock();
        let _ = fs::remove_file(&self.paths.lock);
    }
}

pub fn probe_daemon_status(paths: &DaemonPaths, timeout: Duration) -> DaemonProbeStatus {
    let status = read_daemon_status(paths).ok();
    if let Some(status) = &status {
        // Prefer the live status returned over the wire: it carries fields the
        // on-disk snapshot never persists (e.g. idle_stats). Fall back to the
        // on-disk status only for the liveness comparison.
        if let Some(remote_status) = probe_daemon_status_at_address(&status.address, timeout)
            && daemon_status_matches_probe(status, &remote_status)
        {
            return DaemonProbeStatus::Running(remote_status);
        }
    }

    stale_daemon_lock_probe(paths, status)
}

pub(crate) fn probe_daemon_status_on_runtime(
    paths: &DaemonPaths,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> DaemonProbeStatus {
    let status = read_daemon_status(paths).ok();
    if let Some(status) = &status {
        // Prefer the live status returned over the wire: it carries fields the
        // on-disk snapshot never persists (e.g. idle_stats). Fall back to the
        // on-disk status only for the liveness comparison.
        if let Some(remote_status) =
            probe_daemon_status_at_address_on_runtime(&status.address, timeout, runtime)
            && daemon_status_matches_probe(status, &remote_status)
        {
            return DaemonProbeStatus::Running(remote_status);
        }
    }

    stale_daemon_lock_probe(paths, status)
}

pub(crate) async fn probe_daemon_status_async(
    paths: &DaemonPaths,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> DaemonProbeStatus {
    probe_daemon_status_with_timeouts_async(
        paths,
        DaemonRpcTimeouts::connect_and_idle(timeout),
        runtime,
    )
    .await
}

pub(crate) async fn probe_daemon_status_with_timeouts_async(
    paths: &DaemonPaths,
    timeouts: DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> DaemonProbeStatus {
    let read_paths = paths.clone();
    let status = runtime
        .spawn_blocking(move || read_daemon_status(&read_paths).ok())
        .await
        .unwrap_or(None);
    if let Some(status) = &status {
        if let Some(remote_status) =
            probe_daemon_status_at_address_with_timeouts_async(&status.address, timeouts, runtime)
                .await
            && daemon_status_matches_probe(status, &remote_status)
        {
            return DaemonProbeStatus::Running(remote_status);
        }
    }

    let lock_paths = paths.clone();
    let fallback_status = status.clone();
    runtime
        .spawn_blocking(move || stale_daemon_lock_probe(&lock_paths, status))
        .await
        .unwrap_or(DaemonProbeStatus::Locked {
            status: fallback_status,
        })
}

pub fn probe_daemon_status_at_address(address: &str, timeout: Duration) -> Option<DaemonStatus> {
    let request = sinomo_network::ObjectRpcRequest::encode_body(
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
    )
    .ok()?;
    crate::send_daemon_rpc(address, &request, timeout)
        .ok()?
        .decode_body::<DaemonStatus>()
        .ok()
}

pub(crate) fn probe_daemon_status_at_address_on_runtime(
    address: &str,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Option<DaemonStatus> {
    let request = sinomo_network::ObjectRpcRequest::encode_body(
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
    )
    .ok()?;
    send_daemon_rpc_on_runtime(address, &request, timeout, runtime)
        .ok()?
        .decode_body::<DaemonStatus>()
        .ok()
}

pub(crate) async fn probe_daemon_status_at_address_with_timeouts_async(
    address: &str,
    timeouts: DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Option<DaemonStatus> {
    let request = sinomo_network::ObjectRpcRequest::encode_body(
        crate::DAEMON_STATUS_ROUTE,
        crate::DAEMON_RPC_CATEGORY,
        &(),
    )
    .ok()?;
    send_daemon_rpc_async(address, &request, timeouts, runtime)
        .await
        .ok()?
        .decode_body::<DaemonStatus>()
        .ok()
}

pub fn default_main_daemon_status(timeout: Duration) -> Result<Option<DaemonStatus>, DaemonError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = DaemonPaths::new(&data_dir);
    Ok(match probe_daemon_status(&paths, timeout) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => None,
    })
}

pub(crate) async fn default_main_daemon_status_with_timeouts_async(
    timeouts: DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Result<Option<DaemonStatus>, DaemonError> {
    let paths = runtime
        .spawn_blocking(|| {
            let data_dir = SinomoDataDir::from_env_or_home()?;
            data_dir.ensure_root_layout()?;
            Ok::<_, DaemonError>(DaemonPaths::new(&data_dir))
        })
        .await
        .map_err(|source| DaemonError::Network {
            address: "daemon status probe".to_string(),
            detail: format!("daemon status path task failed: {source}"),
        })??;
    Ok(
        match probe_daemon_status_with_timeouts_async(&paths, timeouts, runtime).await {
            DaemonProbeStatus::Running(status) => Some(status),
            DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => None,
        },
    )
}

fn stale_daemon_lock_probe(paths: &DaemonPaths, status: Option<DaemonStatus>) -> DaemonProbeStatus {
    if !paths.lock.exists() {
        let _ = fs::remove_file(&paths.status);
        return DaemonProbeStatus::NotRunning;
    }

    let Ok(file) = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&paths.lock)
    else {
        return DaemonProbeStatus::Locked { status };
    };

    match file.try_lock_exclusive() {
        Ok(()) => {
            let _ = file.unlock();
            let _ = fs::remove_file(&paths.lock);
            let _ = fs::remove_file(&paths.status);
            DaemonProbeStatus::NotRunning
        }
        Err(_) => DaemonProbeStatus::Locked { status },
    }
}

fn daemon_status_matches_probe(expected: &DaemonStatus, actual: &DaemonStatus) -> bool {
    expected.pid == actual.pid
        && expected.data_dir == actual.data_dir
        && expected.lock_file == actual.lock_file
        && expected.port == actual.port
}

pub fn read_daemon_status(paths: &DaemonPaths) -> Result<DaemonStatus, DaemonError> {
    let bytes = fs::read(&paths.status).map_err(|source| DaemonError::Io {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })?;
    sinomo_network::SinomoObjectCodec::default()
        .decode(&bytes)
        .map_err(DaemonError::from)
}

pub fn write_daemon_status(paths: &DaemonPaths, status: &DaemonStatus) -> Result<(), DaemonError> {
    let status_bytes = sinomo_network::SinomoObjectCodec::default().encode(status)?;
    fs::write(&paths.status, status_bytes).map_err(|source| DaemonError::Io {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_core::{
        ClusterEndpointPrivacy, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
        ClusterTransportKind,
    };
    use sinomo_node::shadow_daemon_node_id;

    fn cluster_status(cluster_id: ClusterId, node_id: NodeId) -> DaemonClusterStatus {
        DaemonClusterStatus {
            cluster_id,
            node_id,
            process: DAEMON_MAIN_CLUSTER_ROLE.to_string(),
            member_role: ClusterMemberRole::NonVoting,
            member_state: ClusterMemberState::NonVoting,
        }
    }

    fn daemon_status_for_cluster(cluster: DaemonClusterStatus) -> DaemonStatus {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-status-cluster-match");
        DaemonStatus::new(
            &DaemonRunOptions::default(),
            &DaemonPaths::with_root(root.join("daemon")),
            Some(cluster),
        )
    }

    #[test]
    fn daemon_data_owner_lock_excludes_second_owner() {
        let root = sinomo_testdir::TestDir::new("sinomo-daemon-data-owner-lock");
        let paths = DaemonPaths::with_root(root.join("daemon"));
        let owner = DaemonDataOwnerLock::acquire(&paths).expect("owner lock");

        assert!(DaemonDataOwnerLock::acquire(&paths).is_err());

        drop(owner);
        DaemonDataOwnerLock::acquire(&paths).expect("lock released");
    }

    #[test]
    fn local_shadow_advertisement_includes_public_route_ip() {
        let now = OffsetDateTime::now_utc();
        let state = ClusterBootstrapState::initialized_locally(
            ClusterId::from_legacy_named("cluster_shadow_public_route").expect("cluster"),
            "shadow public route".to_string(),
            NodeId::from_legacy_named("node_shadow_public_route").expect("node"),
            "shadow public route".to_string(),
            "main-public-key".to_string(),
            now,
        );
        let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
        let endpoints = daemon_advertised_endpoints_for_state(
            &state,
            &shadow_node_id,
            &DaemonRunOptions {
                host: "0.0.0.0".to_string(),
                port: 21_354,
            },
            10,
            &[],
            &["198.18.0.76".parse().expect("benchmark-network ip")],
        );

        assert!(endpoints.iter().any(|endpoint| {
            endpoint.node_id == shadow_node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && endpoint.address == "198.18.0.76:21354"
                && endpoint.privacy == ClusterEndpointPrivacy::InternetDirect
        }));
    }

    #[test]
    fn advertised_host_adds_stable_hostname_endpoint_for_wildcard_bind() {
        let node_id = NodeId::from_legacy_named("node_advertise_hostname").expect("node");
        let endpoints = sinomo_network::daemon_advertised_endpoints_from_static_addresses(
            &node_id,
            "127.0.0.1:21353".to_string(),
            "0.0.0.0",
            21_353,
            0,
            &["172.24.0.4".parse().expect("lan ip")],
            &[],
            true,
            Some("node4"),
        );

        assert!(endpoints.iter().any(|endpoint| {
            endpoint.node_id == node_id
                && endpoint.transport == ClusterTransportKind::LanTcp
                && endpoint.address == "node4:21353"
                && endpoint.privacy == ClusterEndpointPrivacy::Lan
                && endpoint.priority == 5
        }));
    }

    #[test]
    fn cluster_status_match_accepts_member_cluster_id_when_state_cluster_is_stale() {
        let now = OffsetDateTime::now_utc();
        let stale_cluster =
            ClusterId::from_legacy_named("cluster_status_match_stale").expect("cluster");
        let member_cluster =
            ClusterId::from_legacy_named("cluster_status_match_member").expect("cluster");
        let local = NodeId::from_legacy_named("node_status_match_local").expect("node");
        let remote = NodeId::from_legacy_named("node_status_match_remote").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            stale_cluster,
            "stale cluster",
            local,
            "local",
            "pk-local",
            now,
        );
        let mut member = ClusterMemberRecord::local_non_voting(
            member_cluster.clone(),
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote, "172.23.0.2:21353".to_string(), 5);
        member.endpoints = vec![endpoint.clone()];
        state.members.push(member);

        let status = daemon_status_for_cluster(cluster_status(member_cluster, remote));

        assert!(daemon_cluster_status_matches(&state, &endpoint, &status));
    }

    #[test]
    fn cluster_status_match_rejects_unrelated_cluster_id() {
        let now = OffsetDateTime::now_utc();
        let state_cluster =
            ClusterId::from_legacy_named("cluster_status_match_state").expect("cluster");
        let member_cluster =
            ClusterId::from_legacy_named("cluster_status_match_member_two").expect("cluster");
        let other_cluster =
            ClusterId::from_legacy_named("cluster_status_match_other").expect("cluster");
        let local = NodeId::from_legacy_named("node_status_match_local_two").expect("node");
        let remote = NodeId::from_legacy_named("node_status_match_remote_two").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            state_cluster,
            "state cluster",
            local,
            "local",
            "pk-local",
            now,
        );
        let mut member = ClusterMemberRecord::local_non_voting(
            member_cluster,
            remote.clone(),
            "remote",
            "pk-remote",
            now,
        );
        let endpoint =
            sinomo_network::daemon_lan_endpoint(&remote, "172.23.0.3:21353".to_string(), 5);
        member.endpoints = vec![endpoint.clone()];
        state.members.push(member);

        let status = daemon_status_for_cluster(cluster_status(other_cluster, remote));

        assert!(!daemon_cluster_status_matches(&state, &endpoint, &status));
    }
}
