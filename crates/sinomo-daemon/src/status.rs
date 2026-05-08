use crate::{
    AuthenticatedDaemonCommand, DaemonError, DaemonPaths, DaemonSignerAuthority, send_daemon_rpc,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterId,
    ClusterMemberRecord, ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability,
    ClusterNodeRolloutStatus, ClusterNodeStatusRecord, NodeId,
};
use sinomo_local::SinomoDataDir;
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
pub const DAEMON_MAIN_CLUSTER_ROLE: &str = "daemon-main";
pub const DAEMON_SHADOW_CLUSTER_ROLE: &str = "daemon-shadow";
pub const CLUSTER_NODE_STATUS_STALE_AFTER_SECONDS: i64 = 120;

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
    let node_id = match process {
        DaemonClusterProcess::Main => state.local_node_id.clone(),
        DaemonClusterProcess::Shadow => sinomo_node::shadow_daemon_node_id(&state.local_node_id),
    };
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

pub fn ensure_daemon_cluster_members(
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
    let allow_public_ip_discovery = *node_id == state.local_node_id;
    daemon_advertised_endpoints_from_ips(
        node_id,
        options,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
}

pub fn daemon_advertised_endpoints_from_ips(
    node_id: &NodeId,
    options: &DaemonRunOptions,
    priority: u16,
    lan_ips: &[IpAddr],
    route_ips: &[IpAddr],
    allow_public_ip_discovery: bool,
) -> Vec<ClusterEndpointRecord> {
    sinomo_network::daemon_advertised_endpoints_from_addresses_with_public_discovery(
        node_id,
        daemon_connect_address(&options.host, options.port),
        &options.host,
        options.port,
        priority,
        lan_ips,
        route_ips,
        allow_public_ip_discovery,
    )
}

pub fn current_shadow_daemon_options(data_dir: &Path, timeout: Duration) -> DaemonRunOptions {
    match probe_daemon_status(
        &DaemonPaths::with_root(data_dir.join("shadow-daemon")),
        timeout,
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

pub fn daemon_cluster_status_matches(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    status: &DaemonStatus,
) -> bool {
    status.cluster.as_ref().is_some_and(|cluster| {
        cluster.cluster_id == state.cluster_id && cluster.node_id == endpoint.node_id
    })
}

pub fn authenticated_signer_matches_cluster_node(
    state: &ClusterBootstrapState,
    authenticated: &AuthenticatedDaemonCommand,
    node_id: &NodeId,
) -> bool {
    if !matches!(
        authenticated.signer,
        DaemonSignerAuthority::Local | DaemonSignerAuthority::Voter
    ) {
        return false;
    }
    state.members.iter().any(|member| {
        member.node_id == *node_id
            && member.cluster_id == state.cluster_id
            && member.state.is_authorized()
            && member.node_id.as_str() == authenticated.signer_node_id
            && member.node_public_key == authenticated.signer_public_key
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
        }
    }
}

pub fn daemon_package_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
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

pub fn unknown_cluster_node_status(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    observed_at: OffsetDateTime,
) -> ClusterNodeStatusRecord {
    ClusterNodeStatusRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: member.node_id.clone(),
        process: member
            .capabilities
            .iter()
            .find_map(|capability| match capability.as_str() {
                "daemon.main.run" => Some(DAEMON_MAIN_CLUSTER_ROLE.to_string()),
                "daemon.shadow.supervise" => Some(DAEMON_SHADOW_CLUSTER_ROLE.to_string()),
                _ => None,
            })
            .unwrap_or_else(|| "unknown".to_string()),
        availability: ClusterNodeAvailability::Unknown,
        daemon_version: "unknown".to_string(),
        target_os: None,
        target_arch: None,
        pid: None,
        address: member
            .endpoints
            .iter()
            .find(|endpoint| !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly))
            .or_else(|| member.endpoints.first())
            .map(|endpoint| endpoint.address.clone()),
        bind_address: None,
        executable: None,
        rollout: None,
        daemon_started_at: None,
        observed_at,
    }
}

pub fn cluster_node_status_is_stale(status: &ClusterNodeStatusRecord) -> bool {
    (OffsetDateTime::now_utc() - status.observed_at).whole_seconds()
        > CLUSTER_NODE_STATUS_STALE_AFTER_SECONDS
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
        if probe_daemon_status_at_address(&status.address, timeout)
            .is_some_and(|remote_status| daemon_status_matches_probe(status, &remote_status))
        {
            return DaemonProbeStatus::Running(status.clone());
        }
    }

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

pub fn probe_daemon_status_at_address(address: &str, timeout: Duration) -> Option<DaemonStatus> {
    let request = sinomo_network::ObjectRpcRequest::encode_body(
        sinomo_network::DAEMON_STATUS_ROUTE,
        sinomo_network::DAEMON_RPC_CATEGORY,
        &(),
    )
    .ok()?;
    send_daemon_rpc(address, &request, timeout)
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

pub fn daemon_status_matches_probe(expected: &DaemonStatus, actual: &DaemonStatus) -> bool {
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
