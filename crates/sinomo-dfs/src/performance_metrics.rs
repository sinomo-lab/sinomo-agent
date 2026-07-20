use std::time::Instant;

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
        crate::DFS_EXPOSE_CONFIG_ROUTE => "expose_config",
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
