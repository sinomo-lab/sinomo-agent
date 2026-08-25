//! Daemon composition hooks for the observation owner crate.

use std::path::{Path, PathBuf};

use sinomo_core::Id;

use crate::{DaemonError, DaemonPaths, read_daemon_status};

pub use sinomo_observe::OperationalSeverity;

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
        tracing::warn!("sinomo daemon operational event append failed: {error}");
    }
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
        tracing::warn!("sinomo daemon operational log append failed: {error}");
    }
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
