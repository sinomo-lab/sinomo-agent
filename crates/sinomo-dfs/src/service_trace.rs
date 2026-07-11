//! DFS service trace and diagnostic label helpers.

use crate::{
    DfsDistributedWriteResult, DfsNamespaceOperation, DfsNetworkMessage, NetworkedDfsWriteOutcome,
};

pub(crate) fn dfs_perf_trace(args: std::fmt::Arguments<'_>) {
    tracing::trace!(target: "sinomo.dfs.perf", detail = %args, "dfs perf");
}

pub(crate) fn dfs_perf_trace_enabled() -> bool {
    tracing::enabled!(target: "sinomo.dfs.perf", tracing::Level::TRACE)
}

pub(crate) fn dfs_network_message_trace_label(message: &DfsNetworkMessage) -> &'static str {
    match message {
        DfsNetworkMessage::ReadRequest(_) => "read-request",
        DfsNetworkMessage::ReadResponse(_) => "read-response",
        DfsNetworkMessage::ReadResponseChunk(_) => "read-response-chunk",
        DfsNetworkMessage::BulkReadRequest(_) => "bulk-read-request",
        DfsNetworkMessage::BulkReadResponse(_) => "bulk-read-response",
        DfsNetworkMessage::WriteRequest(_) => "write-request",
        DfsNetworkMessage::WriteResponse(_) => "write-response",
        DfsNetworkMessage::DistributedWriteRequest(_) => "distributed-write-request",
        DfsNetworkMessage::DistributedWriteResponse(_) => "distributed-write-response",
        DfsNetworkMessage::NamespaceRequest(_) => "namespace-request",
        DfsNetworkMessage::NamespaceResponse(_) => "namespace-response",
        DfsNetworkMessage::StageBlob(_) => "stage-blob",
        DfsNetworkMessage::StageBlobChunk(_) => "stage-blob-chunk",
        DfsNetworkMessage::StagePatch(_) => "stage-patch",
        DfsNetworkMessage::StagePatchChunk(_) => "stage-patch-chunk",
        DfsNetworkMessage::StageBlobResponse(_) => "stage-blob-response",
        DfsNetworkMessage::CommitVersion(_) => "commit-version",
        DfsNetworkMessage::CommitVersionResponse(_) => "commit-version-response",
        DfsNetworkMessage::RepairReplica(_) => "repair-replica",
        DfsNetworkMessage::RepairReplicaChunk(_) => "repair-replica-chunk",
        DfsNetworkMessage::RepairReplicaResponse(_) => "repair-replica-response",
        DfsNetworkMessage::FetchBlob(_) => "fetch-blob",
        DfsNetworkMessage::FetchBlobResponse(_) => "fetch-blob-response",
        DfsNetworkMessage::FetchBlobResponseChunk(_) => "fetch-blob-response-chunk",
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
