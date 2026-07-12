//! DFS service trace and diagnostic label helpers.

use crate::{
    DfsDistributedWriteResult, DfsNamespaceOperation, DfsNetworkMessage, NetworkedDfsWriteOutcome,
};

thread_local! {
    static DFS_TRACEPARENT: std::cell::RefCell<Option<String>> = const {
        std::cell::RefCell::new(None)
    };
}

struct DfsTraceparentGuard(Option<String>);

impl Drop for DfsTraceparentGuard {
    fn drop(&mut self) {
        let previous = self.0.take();
        DFS_TRACEPARENT.with(|traceparent| {
            traceparent.replace(previous);
        });
    }
}

/// Run synchronous DFS work with the originating object-RPC trace context.
///
/// DFS crosses async and blocking boundaries in the daemon. Callers carry the
/// traceparent explicitly to the blocking boundary and install it here so
/// internal routed RPCs and performance events retain one end-to-end identity.
pub fn with_dfs_traceparent<T>(traceparent: Option<&str>, task: impl FnOnce() -> T) -> T {
    let previous = DFS_TRACEPARENT.with(|current| {
        current.replace(
            traceparent
                .map(str::trim)
                .filter(|traceparent| !traceparent.is_empty())
                .map(ToString::to_string),
        )
    });
    let _guard = DfsTraceparentGuard(previous);
    task()
}

pub(crate) fn current_dfs_traceparent() -> Option<String> {
    DFS_TRACEPARENT.with(|traceparent| traceparent.borrow().clone())
}

pub(crate) fn dfs_perf_trace(args: std::fmt::Arguments<'_>) {
    if !dfs_perf_trace_enabled() {
        return;
    }
    if let Some(traceparent) = current_dfs_traceparent() {
        tracing::trace!(
            target: "sinomo.dfs.perf",
            traceparent = %traceparent,
            detail = %args,
            "dfs perf"
        );
    } else {
        tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs perf");
    }
}

pub(crate) fn dfs_perf_trace_enabled() -> bool {
    tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE)
}

pub(crate) fn dfs_network_message_trace_label(message: &DfsNetworkMessage) -> &'static str {
    match message {
        DfsNetworkMessage::ReadRequest(_) => "read-request",
        DfsNetworkMessage::ReadResponse(_) => "read-response",
        DfsNetworkMessage::BulkReadRequest(_) => "bulk-read-request",
        DfsNetworkMessage::BulkReadResponse(_) => "bulk-read-response",
        DfsNetworkMessage::WriteRequest(_) => "write-request",
        DfsNetworkMessage::WriteResponse(_) => "write-response",
        DfsNetworkMessage::DistributedWriteRequest(_) => "distributed-write-request",
        DfsNetworkMessage::DistributedWriteResponse(_) => "distributed-write-response",
        DfsNetworkMessage::NamespaceRequest(_) => "namespace-request",
        DfsNetworkMessage::NamespaceResponse(_) => "namespace-response",
        DfsNetworkMessage::StageBlob(_) => "stage-blob",
        DfsNetworkMessage::StagePatch(_) => "stage-patch",
        DfsNetworkMessage::StageBlobResponse(_) => "stage-blob-response",
        DfsNetworkMessage::CommitVersion(_) => "commit-version",
        DfsNetworkMessage::CommitVersionResponse(_) => "commit-version-response",
        DfsNetworkMessage::RepairReplica(_) => "repair-replica",
        DfsNetworkMessage::RepairReplicaResponse(_) => "repair-replica-response",
        DfsNetworkMessage::FetchBlob(_) => "fetch-blob",
        DfsNetworkMessage::FetchBlobResponse(_) => "fetch-blob-response",
        DfsNetworkMessage::MetadataJournalBatch(_) => "metadata-journal-batch",
        DfsNetworkMessage::MetadataJournalBatchResponse(_) => "metadata-journal-batch-response",
        DfsNetworkMessage::MetadataSnapshotInstall(_) => "metadata-snapshot-install",
        DfsNetworkMessage::MetadataSnapshotInstallResponse(_) => {
            "metadata-snapshot-install-response"
        }
    }
}

pub(crate) fn dfs_namespace_operation_trace_label(
    operation: &DfsNamespaceOperation,
) -> &'static str {
    match operation {
        DfsNamespaceOperation::Mkdir { .. } => "mkdir",
        DfsNamespaceOperation::MkdirAll { .. } => "mkdir-all",
        DfsNamespaceOperation::Rmdir { .. } => "rmdir",
        DfsNamespaceOperation::Unlink { .. } => "unlink",
        DfsNamespaceOperation::Rename { .. } => "rename",
        DfsNamespaceOperation::Hardlink { .. } => "hardlink",
        DfsNamespaceOperation::SetMode { .. } => "set-mode",
        DfsNamespaceOperation::SetOwner { .. } => "set-owner",
        DfsNamespaceOperation::SetTimes { .. } => "set-times",
        DfsNamespaceOperation::TouchDirectoryModified { .. } => "touch-directory-modified",
        DfsNamespaceOperation::ClearDirRecursive { .. } => "clear-dir-recursive",
        DfsNamespaceOperation::SyncPath { .. } => "sync-path",
        DfsNamespaceOperation::LookupPath { .. } => "lookup-path",
        DfsNamespaceOperation::LookupPaths { .. } => "lookup-paths",
        DfsNamespaceOperation::ApplyTransaction { .. } => "apply-transaction",
        DfsNamespaceOperation::AdvisoryLock { .. } => "advisory-lock",
        DfsNamespaceOperation::FileSync { .. } => "file-sync",
        DfsNamespaceOperation::DirectorySync { .. } => "directory-sync",
        DfsNamespaceOperation::AdvisoryLockMigrationPrepare { .. } => {
            "advisory-lock-migration-prepare"
        }
        DfsNamespaceOperation::AdoptObjectAtPath { .. } => "adopt-object-at-path",
        DfsNamespaceOperation::AdoptHardlinkAlias { .. } => "adopt-hardlink-alias",
    }
}

pub(crate) fn dfs_distributed_write_result_trace_label(
    result: &DfsDistributedWriteResult,
) -> &'static str {
    match result {
        DfsDistributedWriteResult::Rejected { .. } => "rejected",
        DfsDistributedWriteResult::Outcome { outcome } => match outcome {
            NetworkedDfsWriteOutcome::Pending { .. } => "pending",
            NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } => {
                if repair_jobs.is_empty() {
                    "committed"
                } else {
                    "committed-with-repair"
                }
            }
            NetworkedDfsWriteOutcome::Conflict { .. } => "conflict",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_traceparent_is_nested_and_restored() {
        assert_eq!(current_dfs_traceparent(), None);
        with_dfs_traceparent(Some("outer"), || {
            assert_eq!(current_dfs_traceparent().as_deref(), Some("outer"));
            with_dfs_traceparent(Some("inner"), || {
                assert_eq!(current_dfs_traceparent().as_deref(), Some("inner"));
            });
            assert_eq!(current_dfs_traceparent().as_deref(), Some("outer"));
            with_dfs_traceparent(None, || {
                assert_eq!(current_dfs_traceparent(), None);
            });
            assert_eq!(current_dfs_traceparent().as_deref(), Some("outer"));
        });
        assert_eq!(current_dfs_traceparent(), None);
    }
}
