use crate::{DaemonError, DaemonPaths, read_daemon_status};
use sinomo_core::Id;
use sinomo_local::SinomoDataDir;
use sinomo_network::ClusterPaths;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub use sinomo_observe::OperationalSeverity;

pub(crate) fn register_observe_metric_exporters(
    service_hooks: &[crate::DaemonServiceRegistryHook],
) {
    sinomo_observe::register_observe_metric_exporter("sinomo-dfs", dfs_observe_metrics);
    sinomo_observe::register_observe_metric_exporter("sinomo-kv", kv_observe_metrics);
    for hook in service_hooks {
        hook.register_observe_metric_exporters();
    }
}

fn dfs_observe_metrics(
    data_dir: &SinomoDataDir,
) -> Result<Vec<sinomo_observe::ObserveMetricRecord>, sinomo_observe::ObserveError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let status = sinomo_dfs::inspect_cluster_aware_dfs_status(data_dir, &cluster_paths).map_err(
        |source| sinomo_observe::ObserveError::Runtime {
            detail: format!("dfs snapshot metric export failed: {source}"),
        },
    )?;
    let mut metrics = vec![
        gauge("sinomo.dfs.filesystems", status.file_system_count),
        gauge("sinomo.dfs.objects", status.object_count),
        gauge("sinomo.dfs.versions", status.version_count),
        gauge("sinomo.dfs.receipts", status.receipt_count),
        gauge("sinomo.dfs.intents", status.intent_count),
        gauge("sinomo.dfs.pending_writes", status.pending_write_count),
        gauge("sinomo.dfs.open_conflicts", status.open_conflict_count),
        gauge("sinomo.dfs.pending_repairs", status.pending_repair_count),
        gauge(
            "sinomo.dfs.pending_namespace_jobs",
            status.pending_namespace_job_count,
        ),
        gauge(
            "sinomo.dfs.failed_namespace_jobs",
            status.failed_namespace_job_count,
        ),
    ];
    for shard in status.metadata_shards {
        let shard_labels = labels([
            ("shard_id", shard.shard_id.0.clone()),
            (
                "file_system_id",
                shard
                    .file_system_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            ),
            (
                "primary_node_id",
                shard
                    .primary_node_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            ),
        ]);
        metrics.push(sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.dfs.metadata_shard.journal_next_sequence",
            shard_labels.clone(),
            shard.journal_next_sequence as f64,
        ));
        metrics.push(sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.dfs.metadata_shard.journal_last_sequence",
            shard_labels.clone(),
            shard.journal_last_sequence as f64,
        ));
        metrics.push(sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.dfs.metadata_shard.replicas",
            shard_labels.clone(),
            shard.replica_node_ids.len() as f64,
        ));
        for lag in shard.replica_lag {
            let mut lag_labels = shard_labels.clone();
            lag_labels.insert("target_node_id".to_string(), lag.target_node_id.to_string());
            metrics.push(sinomo_observe::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replica_lag_entries",
                lag_labels.clone(),
                lag.lag_entries as f64,
            ));
            metrics.push(sinomo_observe::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replica_replicated_through_sequence",
                lag_labels,
                lag.replicated_through_sequence as f64,
            ));
        }
        for cursor in shard.replay_cursors {
            let mut cursor_labels = shard_labels.clone();
            cursor_labels.insert("source_shard_id".to_string(), cursor.source_shard_id.0);
            metrics.push(sinomo_observe::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replay_replayed_through_sequence",
                cursor_labels,
                cursor.replayed_through_sequence as f64,
            ));
        }
    }
    Ok(metrics)
}

fn kv_observe_metrics(
    data_dir: &SinomoDataDir,
) -> Result<Vec<sinomo_observe::ObserveMetricRecord>, sinomo_observe::ObserveError> {
    let cluster_paths = ClusterPaths::new(data_dir);
    let state = match sinomo_network::read_cluster_state_without_kv(&cluster_paths) {
        Ok(state) => state,
        Err(_) if !cluster_paths.state.exists() => return Ok(Vec::new()),
        Err(source) => {
            return Err(sinomo_observe::ObserveError::Runtime {
                detail: format!("kv snapshot metric export failed to read cluster state: {source}"),
            });
        }
    };
    let kv_paths = cluster_paths.kv_paths();
    let snapshot = sinomo_kv::read_cluster_kv_current_snapshot_for_node(
        &kv_paths,
        state.local_node_id.clone(),
    )
    .map_err(|source| sinomo_observe::ObserveError::Runtime {
        detail: format!("kv snapshot metric export failed: {source}"),
    })?;
    let node_labels = labels([("node_id", state.local_node_id.to_string())]);
    Ok(vec![
        sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.kv.revision",
            node_labels.clone(),
            snapshot.revision.0 as f64,
        ),
        sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.kv.compacted_revision",
            node_labels.clone(),
            snapshot.compacted_revision.0 as f64,
        ),
        sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.kv.entries",
            node_labels.clone(),
            snapshot.entries.len() as f64,
        ),
        sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.kv.leases",
            node_labels.clone(),
            snapshot.leases.len() as f64,
        ),
        sinomo_observe::ObserveMetricRecord::gauge(
            "sinomo.kv.retained_events",
            node_labels,
            snapshot.events.len() as f64,
        ),
    ])
}

fn gauge(name: impl Into<String>, value: usize) -> sinomo_observe::ObserveMetricRecord {
    sinomo_observe::ObserveMetricRecord::gauge(name, BTreeMap::new(), value as f64)
}

fn labels<const N: usize>(labels: [(&str, String); N]) -> BTreeMap<String, String> {
    labels
        .into_iter()
        .filter(|(_, value)| !value.is_empty())
        .map(|(key, value)| (key.to_string(), value))
        .collect()
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
