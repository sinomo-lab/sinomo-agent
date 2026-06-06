use crate::{DaemonError, DaemonPaths, read_daemon_status};
use sinomo_core::Id;
use std::path::{Path, PathBuf};

pub use sinomo_observe::{
    OPERATIONAL_EVENTS_LOCK_RELATIVE_PATH, OPERATIONAL_EVENTS_RELATIVE_PATH,
    OPERATIONAL_JSONL_MAX_BYTES, OPERATIONAL_JSONL_RETAINED_FILES,
    OPERATIONAL_LOG_LOCK_RELATIVE_PATH, OPERATIONAL_LOG_RELATIVE_PATH, OperationalEventQuery,
    OperationalEventRecord, OperationalLogQuery, OperationalLogRecord, OperationalSeverity,
    operational_events_path, operational_logs_path,
};

#[allow(clippy::too_many_arguments)]
pub fn append_operational_event(
    paths: &DaemonPaths,
    severity: OperationalSeverity,
    component: &str,
    kind: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) -> Result<OperationalEventRecord, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    sinomo_observe::append_operational_event(
        &data_dir,
        Some(operational_origin_node_id(paths, &data_dir)),
        severity,
        component,
        kind,
        message,
        operation_id,
        fields,
    )
    .map_err(DaemonError::from)
}

#[allow(clippy::too_many_arguments)]
pub fn append_operational_event_best_effort(
    paths: &DaemonPaths,
    severity: OperationalSeverity,
    component: &str,
    kind: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) {
    let result = (|| {
        let data_dir = daemon_data_dir_path(paths)?;
        sinomo_observe::append_operational_event_if_enabled(
            &data_dir,
            Some(operational_origin_node_id(paths, &data_dir)),
            severity,
            component,
            kind,
            message,
            operation_id,
            fields,
        )
        .map(|_| ())
        .map_err(DaemonError::from)
    })();
    if let Err(error) = result {
        eprintln!("sinomo daemon operational event append failed: {error}");
    }
}

pub fn append_operational_log(
    paths: &DaemonPaths,
    severity: OperationalSeverity,
    component: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) -> Result<OperationalLogRecord, DaemonError> {
    let data_dir = daemon_data_dir_path(paths)?;
    sinomo_observe::append_operational_log(
        &data_dir,
        severity,
        component,
        message,
        operation_id,
        fields,
    )
    .map_err(DaemonError::from)
}

pub fn append_operational_log_best_effort(
    paths: &DaemonPaths,
    severity: OperationalSeverity,
    component: &str,
    message: impl Into<String>,
    operation_id: Option<&str>,
    fields: serde_json::Value,
) {
    let result = (|| {
        let data_dir = daemon_data_dir_path(paths)?;
        sinomo_observe::append_operational_log_if_enabled(
            &data_dir,
            severity,
            component,
            message,
            operation_id,
            fields,
        )
        .map(|_| ())
        .map_err(DaemonError::from)
    })();
    if let Err(error) = result {
        eprintln!("sinomo daemon operational log append failed: {error}");
    }
}

pub fn read_operational_events(
    data_dir: impl AsRef<Path>,
    query: &OperationalEventQuery,
) -> Result<Vec<OperationalEventRecord>, DaemonError> {
    sinomo_observe::read_operational_events(data_dir, query).map_err(DaemonError::from)
}

pub fn read_operational_logs(
    data_dir: impl AsRef<Path>,
    query: &OperationalLogQuery,
) -> Result<Vec<OperationalLogRecord>, DaemonError> {
    sinomo_observe::read_operational_logs(data_dir, query).map_err(DaemonError::from)
}

fn daemon_data_dir_path(paths: &DaemonPaths) -> Result<PathBuf, DaemonError> {
    paths
        .root
        .parent()
        .map(PathBuf::from)
        .ok_or_else(|| DaemonError::Io {
            path: paths.root.display().to_string(),
            detail: "daemon root has no data directory parent".to_string(),
        })
}

fn operational_origin_node_id(paths: &DaemonPaths, data_dir: &Path) -> Id {
    if let Ok(status) = read_daemon_status(paths)
        && let Some(cluster) = status.cluster
    {
        return cluster.node_id;
    }
    Id::derived(
        "sinomo.daemon.operational.local-node",
        data_dir.display().to_string().as_bytes(),
    )
}
