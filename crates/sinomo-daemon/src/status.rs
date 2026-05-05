use crate::{DaemonError, DaemonPaths, send_daemon_command};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sinomo_core::{
    ClusterId, ClusterMemberRole, ClusterMemberState, ClusterNodeRolloutStatus, NodeId,
};
use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const SINOMO_DAEMON_DEFAULT_HOST: &str = "127.0.0.1";
pub const SINOMO_DAEMON_DEFAULT_PORT: u16 = 21_353;
pub const SINOMO_SHADOW_DAEMON_DEFAULT_PORT: u16 = 21_354;

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
        serde_json::to_writer_pretty(&file, &status).map_err(|source| DaemonError::Json {
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
        if send_daemon_command(&status.address, "STATUS", timeout)
            .ok()
            .and_then(|response| serde_json::from_str::<DaemonStatus>(&response).ok())
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

pub fn daemon_status_matches_probe(expected: &DaemonStatus, actual: &DaemonStatus) -> bool {
    expected.pid == actual.pid
        && expected.data_dir == actual.data_dir
        && expected.lock_file == actual.lock_file
        && expected.port == actual.port
}

pub fn read_daemon_status(paths: &DaemonPaths) -> Result<DaemonStatus, DaemonError> {
    let content = fs::read_to_string(&paths.status).map_err(|source| DaemonError::Io {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })?;
    serde_json::from_str(&content).map_err(|source| DaemonError::Json {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })
}

pub fn write_daemon_status(paths: &DaemonPaths, status: &DaemonStatus) -> Result<(), DaemonError> {
    let status_json = serde_json::to_vec_pretty(status).map_err(|source| DaemonError::Json {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })?;
    fs::write(&paths.status, status_json).map_err(|source| DaemonError::Io {
        path: paths.status.display().to_string(),
        detail: source.to_string(),
    })
}
