use std::collections::BTreeMap;
use std::time::Instant;

/// Convert a bounded DFS status image into the external metric-export
/// contract without depending on the observation runtime implementation.
pub fn dfs_status_observe_metrics(
    status: crate::DfsOperatorStatus,
) -> Vec<sinomo_observe_contract::ObserveMetricRecord> {
    let gauge = |name, value| {
        sinomo_observe_contract::ObserveMetricRecord::gauge(name, BTreeMap::new(), value as f64)
    };
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
            metrics.push(sinomo_observe_contract::ObserveMetricRecord::gauge(
                name,
                shard_labels.clone(),
                value,
            ));
        }
        for lag in shard.replica_lag {
            let mut labels = shard_labels.clone();
            labels.insert("target_node_id".to_string(), lag.target_node_id.to_string());
            metrics.push(sinomo_observe_contract::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replica_lag_entries",
                labels.clone(),
                lag.lag_entries as f64,
            ));
            metrics.push(sinomo_observe_contract::ObserveMetricRecord::gauge(
                "sinomo.dfs.metadata_shard.replica_replicated_through_sequence",
                labels,
                lag.replicated_through_sequence as f64,
            ));
        }
        for cursor in shard.replay_cursors {
            let mut labels = shard_labels.clone();
            labels.insert("source_shard_id".to_string(), cursor.source_shard_id.0);
            metrics.push(sinomo_observe_contract::ObserveMetricRecord::gauge(
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
