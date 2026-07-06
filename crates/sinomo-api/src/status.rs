use crate::{
    DAEMON_RPC_CATEGORY, DAEMON_STATUS_ROUTE, DaemonApiError, DaemonPaths, IdleStatsSnapshot,
    send_daemon_rpc_async, send_daemon_rpc_on_runtime,
};
use fs2::FileExt;
use sinomo_core::local::SinomoDataDir;
use sinomo_core::network::{DaemonRpcTimeouts, ObjectRpcRequest};
use sinomo_core::node::{ClusterId, NodeId};
use sinomo_core::{
    ClusterMemberRole, ClusterMemberState, ClusterNodeAvailability, ClusterNodeRolloutStatus,
    ClusterNodeStatusRecord,
};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const SINOMO_DAEMON_DEFAULT_HOST: &str = "127.0.0.1";
pub const SINOMO_DAEMON_DEFAULT_PORT: u16 = 21_353;
pub const SINOMO_SHADOW_DAEMON_DEFAULT_PORT: u16 = 21_354;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DaemonProbeStatus {
    Running(DaemonStatus),
    Locked { status: Option<DaemonStatus> },
    NotRunning,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DaemonClusterStatus {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub process: String,
    pub member_role: ClusterMemberRole,
    pub member_state: ClusterMemberState,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
    pub hostname: Option<String>,
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
            hostname: daemon_hostname(),
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

pub fn daemon_hostname() -> Option<String> {
    for key in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(value) = std::env::var(key)
            && let Some(hostname) = clean_daemon_hostname(&value)
        {
            return Some(hostname);
        }
    }
    std::process::Command::new("hostname")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|hostname| clean_daemon_hostname(&hostname))
        .or_else(|| {
            ["/proc/sys/kernel/hostname", "/etc/hostname"]
                .into_iter()
                .find_map(|path| {
                    std::fs::read_to_string(path)
                        .ok()
                        .and_then(|hostname| clean_daemon_hostname(&hostname))
                })
        })
}

fn clean_daemon_hostname(hostname: &str) -> Option<String> {
    let hostname = hostname.trim().trim_end_matches('.');
    if hostname.is_empty()
        || hostname.eq_ignore_ascii_case("localhost")
        || hostname.eq_ignore_ascii_case("localhost.localdomain")
        || hostname.eq_ignore_ascii_case("(none)")
        || hostname.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some(hostname.to_string())
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
        commit: status.commit.clone(),
        target_os: status.target_os.clone(),
        target_arch: status.target_arch.clone(),
        hostname: status.hostname.clone(),
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

pub fn probe_daemon_status(paths: &DaemonPaths, timeout: Duration) -> DaemonProbeStatus {
    let status = read_daemon_status(paths).ok();
    if let Some(status) = &status
        && let Some(remote_status) = probe_daemon_status_at_address(&status.address, timeout)
        && daemon_status_matches_probe(status, &remote_status)
    {
        return DaemonProbeStatus::Running(remote_status);
    }

    stale_daemon_lock_probe(paths, status)
}

pub fn probe_daemon_status_on_runtime(
    paths: &DaemonPaths,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> DaemonProbeStatus {
    let status = read_daemon_status(paths).ok();
    if let Some(status) = &status
        && let Some(remote_status) =
            probe_daemon_status_at_address_on_runtime(&status.address, timeout, runtime)
        && daemon_status_matches_probe(status, &remote_status)
    {
        return DaemonProbeStatus::Running(remote_status);
    }

    stale_daemon_lock_probe(paths, status)
}

pub async fn probe_daemon_status_async(
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

pub async fn probe_daemon_status_with_timeouts_async(
    paths: &DaemonPaths,
    timeouts: DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> DaemonProbeStatus {
    let read_paths = paths.clone();
    let status = runtime
        .spawn_blocking(move || read_daemon_status(&read_paths).ok())
        .await
        .unwrap_or(None);
    if let Some(status) = &status
        && let Some(remote_status) =
            probe_daemon_status_at_address_with_timeouts_async(&status.address, timeouts, runtime)
                .await
        && daemon_status_matches_probe(status, &remote_status)
    {
        return DaemonProbeStatus::Running(remote_status);
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
    let request =
        ObjectRpcRequest::encode_body(DAEMON_STATUS_ROUTE, DAEMON_RPC_CATEGORY, &()).ok()?;
    crate::send_daemon_rpc(address, &request, timeout)
        .ok()?
        .decode_body::<DaemonStatus>()
        .ok()
}

pub fn probe_daemon_status_at_address_on_runtime(
    address: &str,
    timeout: Duration,
    runtime: &tokio::runtime::Handle,
) -> Option<DaemonStatus> {
    let request =
        ObjectRpcRequest::encode_body(DAEMON_STATUS_ROUTE, DAEMON_RPC_CATEGORY, &()).ok()?;
    send_daemon_rpc_on_runtime(address, &request, timeout, runtime)
        .ok()?
        .decode_body::<DaemonStatus>()
        .ok()
}

pub async fn probe_daemon_status_at_address_with_timeouts_async(
    address: &str,
    timeouts: DaemonRpcTimeouts,
    runtime: &tokio::runtime::Handle,
) -> Option<DaemonStatus> {
    let request =
        ObjectRpcRequest::encode_body(DAEMON_STATUS_ROUTE, DAEMON_RPC_CATEGORY, &()).ok()?;
    send_daemon_rpc_async(address, &request, timeouts, runtime)
        .await
        .ok()?
        .decode_body::<DaemonStatus>()
        .ok()
}

pub fn default_main_daemon_status(
    timeout: Duration,
) -> Result<Option<DaemonStatus>, DaemonApiError> {
    let data_dir = SinomoDataDir::from_env_or_home()?;
    data_dir.ensure_root_layout()?;
    let paths = DaemonPaths::new(&data_dir);
    Ok(match probe_daemon_status(&paths, timeout) {
        DaemonProbeStatus::Running(status) => Some(status),
        DaemonProbeStatus::Locked { .. } | DaemonProbeStatus::NotRunning => None,
    })
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

pub fn read_daemon_status(paths: &DaemonPaths) -> Result<DaemonStatus, DaemonApiError> {
    let bytes = fs::read(&paths.status).map_err(|source| DaemonApiError::Io {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })?;
    sinomo_core::network::SinomoObjectCodec::default()
        .decode(&bytes)
        .map_err(DaemonApiError::from)
}

pub fn write_daemon_status(
    paths: &DaemonPaths,
    status: &DaemonStatus,
) -> Result<(), DaemonApiError> {
    let status_bytes = sinomo_core::network::SinomoObjectCodec::default().encode(status)?;
    fs::write(&paths.status, status_bytes).map_err(|source| DaemonApiError::Io {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn daemon_data_dir_from_paths(paths: &DaemonPaths) -> Result<&Path, DaemonApiError> {
    paths.root.parent().ok_or_else(|| DaemonApiError::Io {
        path: paths.root.display().to_string(),
        detail: "daemon root has no parent data directory".to_string(),
    })
}
