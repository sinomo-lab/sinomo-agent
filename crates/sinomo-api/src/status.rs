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
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const SINOMO_DAEMON_DEFAULT_HOST: &str = "127.0.0.1";
pub const SINOMO_DAEMON_DEFAULT_PORT: u16 = 21_353;
pub const SINOMO_SHADOW_DAEMON_DEFAULT_PORT: u16 = 21_354;
pub const SINOMO_DAEMON_PORT_ENV: &str = "SINOMO_DAEMON_PORT";
pub const SINOMO_SHADOW_DAEMON_PORT_ENV: &str = "SINOMO_SHADOW_DAEMON_PORT";
pub const SINOMO_DAEMON_PORT_FALLBACK_ENV: &str = "SINOMO_DAEMON_PORT_FALLBACK";
pub const SINOMO_SHADOW_DAEMON_PORT_FALLBACK_ENV: &str = "SINOMO_SHADOW_DAEMON_PORT_FALLBACK";

const fn default_daemon_port_fallback() -> bool {
    true
}
/// Current revision of the local daemon control protocol understood by first-party clients.
///
/// This is deliberately independent from the package version: compatible app and daemon builds
/// may have different release versions, while an incompatible wire change must advance this
/// value explicitly.
pub const SINOMO_DAEMON_API_PROTOCOL_VERSION: u32 = 1;
/// Environment variable set only on an app-owned daemon child. The value is observational
/// process-instance identity, not an authority token.
pub const SINOMO_APP_DAEMON_LAUNCH_ID_ENV: &str = "SINOMO_APP_DAEMON_LAUNCH_ID";
/// Optional development/test override for the sibling `sinomo` CLI executable used by the app.
pub const SINOMO_APP_DAEMON_BINARY_ENV: &str = "SINOMO_APP_DAEMON_BINARY";

static DAEMON_BUILD_INFO: OnceLock<DaemonBuildInfo> = OnceLock::new();

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonBuildInfo {
    pub version: String,
    pub commit: Option<String>,
}

impl DaemonBuildInfo {
    pub fn new(version: impl Into<String>, commit: Option<String>) -> Self {
        let version = version.into();
        let version = match version.trim() {
            "" => env!("CARGO_PKG_VERSION").to_string(),
            version => version.to_string(),
        };
        let commit = commit.and_then(|commit| {
            let commit = commit.trim();
            if commit.is_empty() || commit == "unknown" {
                None
            } else {
                Some(commit.to_string())
            }
        });
        Self { version, commit }
    }
}

impl Default for DaemonBuildInfo {
    fn default() -> Self {
        Self::new(env!("CARGO_PKG_VERSION"), None)
    }
}

pub fn install_daemon_build_info(info: DaemonBuildInfo) -> Result<(), &'static str> {
    if let Some(existing) = DAEMON_BUILD_INFO.get() {
        return if existing == &info {
            Ok(())
        } else {
            Err("daemon build info was already installed")
        };
    }
    DAEMON_BUILD_INFO
        .set(info)
        .map_err(|_| "daemon build info was already installed")
}

pub fn daemon_build_info() -> DaemonBuildInfo {
    DAEMON_BUILD_INFO.get().cloned().unwrap_or_default()
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DaemonRunOptions {
    pub host: String,
    pub port: u16,
    #[serde(default = "default_daemon_port_fallback")]
    pub port_fallback: bool,
}

impl Default for DaemonRunOptions {
    fn default() -> Self {
        Self {
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_DAEMON_DEFAULT_PORT,
            port_fallback: true,
        }
    }
}

impl DaemonRunOptions {
    pub fn shadow_default() -> Self {
        Self {
            host: SINOMO_DAEMON_DEFAULT_HOST.to_string(),
            port: SINOMO_SHADOW_DAEMON_DEFAULT_PORT,
            port_fallback: true,
        }
    }

    pub fn strict_port(mut self) -> Self {
        self.port_fallback = false;
        self
    }

    pub fn main_from_environment() -> Result<Self, String> {
        Self::from_environment(
            Self::default(),
            SINOMO_DAEMON_PORT_ENV,
            SINOMO_DAEMON_PORT_FALLBACK_ENV,
        )
    }

    pub fn shadow_from_environment() -> Result<Self, String> {
        Self::from_environment(
            Self::shadow_default(),
            SINOMO_SHADOW_DAEMON_PORT_ENV,
            SINOMO_SHADOW_DAEMON_PORT_FALLBACK_ENV,
        )
    }

    fn from_environment(
        mut options: Self,
        port_env: &str,
        fallback_env: &str,
    ) -> Result<Self, String> {
        if let Ok(value) = std::env::var(fallback_env) {
            options.port_fallback = parse_daemon_fallback_bool(fallback_env, &value)?;
        }
        if let Ok(value) = std::env::var(port_env) {
            options.port = value
                .parse::<u16>()
                .map_err(|source| format!("invalid {port_env} `{value}`: {source}"))?;
            options.port_fallback = false;
        }
        Ok(options)
    }
}

fn parse_daemon_fallback_bool(name: &str, value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid {name} `{value}`: expected true or false")),
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
    #[serde(default = "initial_daemon_api_protocol_version")]
    pub api_protocol_version: u32,
    /// Present only for a daemon whose lifecycle is owned by one graphical app process.
    /// Callers must also match the observed PID before treating the daemon as owned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_launch_id: Option<String>,
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub component_status: BTreeMap<String, serde_json::Value>,
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
            api_protocol_version: SINOMO_DAEMON_API_PROTOCOL_VERSION,
            app_launch_id: std::env::var(SINOMO_APP_DAEMON_LAUNCH_ID_ENV)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty()),
            version: daemon_package_version(),
            commit: daemon_git_commit(),
            target_os: Some(std::env::consts::OS.to_string()),
            target_arch: Some(std::env::consts::ARCH.to_string()),
            hostname: daemon_hostname(),
            executable: std::env::current_exe().ok(),
            rollout: None,
            component_status: BTreeMap::new(),
            started_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "unknown".to_string()),
            data_dir: paths.root.parent().unwrap_or(&paths.root).to_path_buf(),
            lock_file: paths.lock.clone(),
            cluster,
            idle_stats: None,
        }
    }

    pub fn apply_component_status_projection(
        &mut self,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<(), String> {
        let key = key.into();
        if key == "sinomo.rollout" {
            self.rollout = Some(
                serde_json::from_value(value.clone())
                    .map_err(|error| format!("decode rollout status projection: {error}"))?,
            );
        }
        self.component_status.insert(key, value);
        Ok(())
    }
}

pub fn daemon_package_version() -> String {
    daemon_build_info().version
}

/// Git revision the daemon process was built from, installed by the application binary.
/// Current ordinary builds embed the full commit; older builds may report an abbreviation.
/// Returns None when the process had no git context (commit recorded as "unknown").
pub fn daemon_git_commit() -> Option<String> {
    daemon_build_info().commit
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
        // A status describes an already selected concrete listener. Rollout
        // and supervision preserve that exact reachable endpoint.
        port_fallback: false,
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

    match FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            let _ = FileExt::unlock(&file);
            let _ = fs::remove_file(&paths.lock);
            let _ = fs::remove_file(&paths.status);
            DaemonProbeStatus::NotRunning
        }
        Err(_) => DaemonProbeStatus::Locked { status },
    }
}

fn daemon_status_matches_probe(expected: &DaemonStatus, actual: &DaemonStatus) -> bool {
    expected.pid == actual.pid
        && expected.app_launch_id == actual.app_launch_id
        && expected.api_protocol_version == actual.api_protocol_version
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

fn initial_daemon_api_protocol_version() -> u32 {
    // Status records written before the explicit field was introduced speak the protocol now
    // named revision 1. Future incompatible revisions must write their number explicitly.
    SINOMO_DAEMON_API_PROTOCOL_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_status_without_protocol_or_launch_identity_decodes_as_unowned() {
        let paths = DaemonPaths::with_root(std::env::temp_dir().join("sinomo-api-status-legacy"));
        let status = DaemonStatus::new(&DaemonRunOptions::default(), &paths, None);
        let mut value = serde_json::to_value(status).expect("status JSON");
        let object = value.as_object_mut().expect("status object");
        object.remove("api_protocol_version");
        object.remove("app_launch_id");

        let decoded: DaemonStatus = serde_json::from_value(value).expect("legacy status");

        assert_eq!(
            decoded.api_protocol_version,
            SINOMO_DAEMON_API_PROTOCOL_VERSION
        );
        assert_eq!(decoded.app_launch_id, None);
    }
}
