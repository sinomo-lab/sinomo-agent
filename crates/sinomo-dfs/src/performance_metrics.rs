use std::collections::BTreeMap;
use std::time::Instant;

type StatusGauge = (&'static str, fn(&crate::DfsOperatorStatus) -> usize);

const STATUS_GAUGES: &[StatusGauge] = &[
    ("sinomo.dfs.filesystems", |status| status.file_system_count),
    ("sinomo.dfs.objects", |status| status.object_count),
    ("sinomo.dfs.versions", |status| status.version_count),
    ("sinomo.dfs.receipts", |status| status.receipt_count),
    ("sinomo.dfs.intents", |status| status.intent_count),
    ("sinomo.dfs.pending_writes", |status| {
        status.pending_write_count
    }),
    ("sinomo.dfs.pending_repairs", |status| {
        status.pending_repair_count
    }),
    ("sinomo.dfs.pending_namespace_jobs", |status| {
        status.pending_namespace_job_count
    }),
    ("sinomo.dfs.failed_namespace_jobs", |status| {
        status.failed_namespace_job_count
    }),
];

pub(crate) fn dfs_status_metric_prefix_matches(prefix: Option<&str>) -> bool {
    let Some(prefix) = prefix else { return true };
    STATUS_GAUGES
        .iter()
        .any(|(name, _)| name.starts_with(prefix))
        || "sinomo.dfs.metadata_shard.".starts_with(prefix)
        || prefix.starts_with("sinomo.dfs.metadata_shard.")
}

/// Convert a bounded DFS status image into the external metric-export
/// contract without depending on the observation runtime implementation.
pub fn dfs_status_observe_metrics(
    status: crate::DfsOperatorStatus,
) -> Vec<sinomo_api::observe::ObserveMetricRecord> {
    let mut metrics = STATUS_GAUGES
        .iter()
        .map(|(name, value)| {
            sinomo_api::observe::ObserveMetricRecord::gauge(
                *name,
                BTreeMap::new(),
                value(&status) as f64,
            )
        })
        .collect::<Vec<_>>();
    for shard in status.metadata_shards {
        let mut shard_labels = BTreeMap::new();
        shard_labels.insert("shard_id".to_string(), shard.shard_id.0.clone());
        if let Some(file_system_id) = shard.file_system_id.as_ref() {
            shard_labels.insert("file_system_id".to_string(), file_system_id.to_string());
        }
        if let Some(primary_node_id) = shard.primary_node_id.as_ref() {
            shard_labels.insert("primary_node_id".to_string(), primary_node_id.to_string());
        }
        for (name, value) in [
            (
                "sinomo.dfs.metadata_shard.journal_next_sequence",
                shard.journal_next_sequence as f64,
            ),
            (
                "sinomo.dfs.metadata_shard.journal_last_sequence",
                shard.journal_last_sequence as f64,
            ),
            (
                "sinomo.dfs.metadata_shard.replicas",
                shard.replica_node_ids.len() as f64,
            ),
        ] {
            metrics.push(sinomo_api::observe::ObserveMetricRecord::gauge(
                name,
                shard_labels.clone(),
                value,
            ));
        }
        for lag in shard.replica_lag {
            let mut labels = shard_labels.clone();
            labels.insert("target_node_id".to_string(), lag.target_node_id.to_string());
            metrics.push(sinomo_api::observe::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replica_lag_entries",
                labels.clone(),
                lag.lag_entries as f64,
            ));
            metrics.push(sinomo_api::observe::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replica_replicated_through_sequence",
                labels,
                lag.replicated_through_sequence as f64,
            ));
        }
        for cursor in shard.replay_cursors {
            let mut labels = shard_labels.clone();
            labels.insert("source_shard_id".to_string(), cursor.source_shard_id.0);
            metrics.push(sinomo_api::observe::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replay_replayed_through_sequence",
                labels,
                cursor.replayed_through_sequence as f64,
            ));
        }
    }
    metrics
}

pub(crate) struct DfsOperationMetricGuard {
    operation: &'static str,
    plane: &'static str,
    outcome: &'static str,
    started: Instant,
}

impl DfsOperationMetricGuard {
    pub(crate) fn start(route: &str, plane: &'static str) -> Self {
        Self {
            operation: dfs_operation_for_route(route),
            plane,
            outcome: "error",
            started: Instant::now(),
        }
    }

    pub(crate) fn succeed(&mut self) {
        self.outcome = "ok";
    }
}

impl Drop for DfsOperationMetricGuard {
    fn drop(&mut self) {
        metrics::counter!(
            "sinomo.dfs.operations",
            "operation" => self.operation,
            "plane" => self.plane,
            "outcome" => self.outcome
        )
        .increment(1);
        metrics::histogram!(
            "sinomo.dfs.operation.total.us",
            "operation" => self.operation,
            "plane" => self.plane,
            "outcome" => self.outcome
        )
        .record(self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as f64);
    }
}

fn dfs_operation_for_route(route: &str) -> &'static str {
    match route {
        crate::DFS_STATUS_ROUTE => "status",
        crate::DFS_INSPECTION_ROUTE => "inspection",
        crate::DFS_SMOKE_ROUTE => "smoke",
        crate::DFS_DIRECT_READ_ROUTE | crate::DFS_DATA_READ_ROUTE => "read",
        crate::DFS_DATA_READ_RESPONSE_ROUTE => "read_response",
        crate::DFS_DATA_BULK_READ_ROUTE => "bulk_read",
        crate::DFS_DATA_BULK_READ_RESPONSE_ROUTE => "bulk_read_response",
        crate::DFS_DATA_WRITE_ROUTE | crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE => "write",
        crate::DFS_DATA_WRITE_RESPONSE_ROUTE | crate::DFS_DATA_DISTRIBUTED_WRITE_RESPONSE_ROUTE => {
            "write_response"
        }
        crate::DFS_DATA_NAMESPACE_ROUTE => "namespace",
        crate::DFS_DATA_NAMESPACE_RESPONSE_ROUTE => "namespace_response",
        crate::DFS_DATA_STAGE_BLOB_ROUTE | crate::DFS_DATA_STAGE_PATCH_ROUTE => "stage",
        crate::DFS_DATA_STAGE_BLOB_RESPONSE_ROUTE => "stage_response",
        crate::DFS_DATA_COMMIT_VERSION_ROUTE => "commit",
        crate::DFS_DATA_COMMIT_VERSION_RESPONSE_ROUTE => "commit_response",
        crate::DFS_DATA_REPAIR_REPLICA_ROUTE => "repair",
        crate::DFS_DATA_REPAIR_REPLICA_RESPONSE_ROUTE => "repair_response",
        crate::DFS_DATA_FETCH_BLOB_ROUTE => "fetch",
        crate::DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE => "fetch_response",
        crate::DFS_METADATA_JOURNAL_BATCH_ROUTE | crate::DFS_DATA_METADATA_JOURNAL_BATCH_ROUTE => {
            "metadata_journal"
        }
        crate::DFS_DATA_METADATA_JOURNAL_BATCH_RESPONSE_ROUTE => "metadata_journal_response",
        crate::DFS_METADATA_SNAPSHOT_INSTALL_ROUTE
        | crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_ROUTE => "metadata_snapshot",
        crate::DFS_DATA_METADATA_SNAPSHOT_INSTALL_RESPONSE_ROUTE => "metadata_snapshot_response",
        crate::DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_ROUTE => "content_root_catalog",
        crate::DFS_DATA_CONTENT_ROOT_CATALOG_PAGE_RESPONSE_ROUTE => "content_root_catalog_response",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrelated_counter_exports_do_not_request_a_metadata_snapshot() {
        assert!(!dfs_status_metric_prefix_matches(Some(
            "sinomo.dfs.background"
        )));
        assert!(!dfs_status_metric_prefix_matches(Some(
            "sinomo.dfs.operation"
        )));
        assert!(dfs_status_metric_prefix_matches(None));
        assert!(dfs_status_metric_prefix_matches(Some("sinomo.dfs.")));
        assert!(dfs_status_metric_prefix_matches(Some("sinomo.dfs.objects")));
        assert!(dfs_status_metric_prefix_matches(Some(
            "sinomo.dfs.metadata_shard.replica_lag"
        )));
    }

    #[test]
    fn dfs_routes_map_to_bounded_operation_families() {
        assert_eq!(
            dfs_operation_for_route(crate::DFS_DIRECT_READ_ROUTE),
            "read"
        );
        assert_eq!(
            dfs_operation_for_route(crate::DFS_DATA_DISTRIBUTED_WRITE_ROUTE),
            "write"
        );
        assert_eq!(
            dfs_operation_for_route(crate::DFS_DATA_REPAIR_REPLICA_ROUTE),
            "repair"
        );
        assert_eq!(dfs_operation_for_route("/future/route"), "unknown");
    }
}
