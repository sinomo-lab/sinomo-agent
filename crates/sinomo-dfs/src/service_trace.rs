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
    let span = tracing::trace_span!(target: "sinomo.perf", "dfs.request",
        stage = "dfs.request", traceparent = traceparent.unwrap_or_default());
    span.in_scope(task)
}

pub fn current_dfs_traceparent() -> Option<String> {
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
        DfsNetworkMessage::ExclusiveCreateRequest(_) => "exclusive-create-request",
        DfsNetworkMessage::RetainedWriteRequest(_) => "retained-write-request",
        DfsNetworkMessage::DistributedWriteResponse(_) => "distributed-write-response",
        DfsNetworkMessage::StableWriteReconciliationRequest(_) => {
            "stable-write-reconciliation-request"
        }
        DfsNetworkMessage::StableWriteReconciliationResponse(_) => {
            "stable-write-reconciliation-response"
        }
        DfsNetworkMessage::NamespaceRequest(_) => "namespace-request",
        DfsNetworkMessage::NamespaceResponse(_) => "namespace-response",
        DfsNetworkMessage::StableNamespaceReconciliationRequest(_) => {
            "stable-namespace-reconciliation-request"
        }
        DfsNetworkMessage::StableNamespaceReconciliationResponse(_) => {
            "stable-namespace-reconciliation-response"
        }
        DfsNetworkMessage::StageBlob(_) => "stage-blob",
        DfsNetworkMessage::StageRetainedBlob(_) => "stage-retained-blob",
        DfsNetworkMessage::RepairRetainedBlob(_) => "repair-retained-blob",
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
        DfsNetworkMessage::ContentRootCatalogPageRequest(_) => "content-root-catalog-page-request",
        DfsNetworkMessage::ContentRootCatalogPageResponse(_) => {
            "content-root-catalog-page-response"
        }
    }
}

pub(crate) fn dfs_namespace_operation_trace_label(
    operation: &DfsNamespaceOperation,
) -> &'static str {
    match operation {
        DfsNamespaceOperation::BeginContentCopy { .. } => "file-history-restore-begin",
        DfsNamespaceOperation::ContentReference { .. } => "file-history-pin",
        DfsNamespaceOperation::MetadataQuery { .. } => "file-history-query",
        DfsNamespaceOperation::PollNamespaceChanges { .. } => "poll-namespace-changes",
        DfsNamespaceOperation::Mkdir { .. } => "mkdir",
        DfsNamespaceOperation::MkdirWithAttrs { .. } => "mkdir-with-attrs",
        DfsNamespaceOperation::MkdirAtNoLinks { .. } => "mkdir-at-no-links",
        DfsNamespaceOperation::MkdirAtNoLinksWithAttrs(_) => "mkdir-at-no-links-with-attrs",
        DfsNamespaceOperation::MkdirAll { .. } => "mkdir-all",
        DfsNamespaceOperation::Rmdir { .. } => "rmdir",
        DfsNamespaceOperation::ConditionalRmdir { .. } => "conditional-rmdir",
        DfsNamespaceOperation::Unlink { .. } => "unlink",
        DfsNamespaceOperation::ConditionalUnlink { .. } => "conditional-unlink",
        DfsNamespaceOperation::Rename { .. } => "rename",
        DfsNamespaceOperation::ConditionalRename { .. } => "conditional-rename",
        DfsNamespaceOperation::RenameSingleNamespaceAtomic { .. } => {
            "rename-single-namespace-atomic"
        }
        DfsNamespaceOperation::RenameDirectoryBounded { .. } => "rename-directory-bounded",
        DfsNamespaceOperation::ResumeNamespaceJobBounded { .. } => "resume-namespace-job-bounded",
        DfsNamespaceOperation::Hardlink { .. } => "hardlink",
        DfsNamespaceOperation::InstallHardlinkTargetFence { .. } => "install-hardlink-target-fence",
        DfsNamespaceOperation::CommitHardlinkTargetFence { .. } => "commit-hardlink-target-fence",
        DfsNamespaceOperation::SetMode { .. } => "set-mode",
        DfsNamespaceOperation::SetOwner { .. } => "set-owner",
        DfsNamespaceOperation::SetSymlinkOwner { .. } => "set-symlink-owner",
        DfsNamespaceOperation::SetTimes { .. } => "set-times",
        DfsNamespaceOperation::RecordAccess { .. } => "record-access",
        DfsNamespaceOperation::TouchDirectoryModified { .. } => "touch-directory-modified",
        DfsNamespaceOperation::ClearDirRecursive { .. } => "clear-dir-recursive",
        DfsNamespaceOperation::LookupPath { .. } => "lookup-path",
        DfsNamespaceOperation::LookupPaths { .. } => "lookup-paths",
        DfsNamespaceOperation::CommitLocalEventualBatch { .. } => "commit-local-eventual-batch",
        DfsNamespaceOperation::ReconcilePlacement { .. } => "reconcile-placement",
        DfsNamespaceOperation::AbortPendingWrite { .. } => "abort-pending-write",
        DfsNamespaceOperation::AdvisoryLock { .. } => "advisory-lock",
        DfsNamespaceOperation::FileSync { .. } => "file-sync",
        DfsNamespaceOperation::DirectorySync { .. } => "directory-sync",

        DfsNamespaceOperation::AdoptObjectAtPath { .. } => "adopt-object-at-path",
        DfsNamespaceOperation::FinalizeObjectFamilyAdoption { .. } => {
            "finalize-object-family-adoption"
        }
        DfsNamespaceOperation::VerifyObjectFamilyAdoption { .. } => "verify-object-family-adoption",
        DfsNamespaceOperation::AdoptHardlinkAlias { .. } => "adopt-hardlink-alias",
    }
}

pub(crate) fn dfs_distributed_write_result_trace_label(
    result: &DfsDistributedWriteResult,
) -> &'static str {
    match result {
        DfsDistributedWriteResult::Rejected { .. } => "rejected",
        DfsDistributedWriteResult::PreconditionFailed { .. } => "precondition-failed",
        DfsDistributedWriteResult::UnsupportedPrecondition { .. } => "unsupported-precondition",
        DfsDistributedWriteResult::ReconciliationRequired { .. } => "reconciliation-required",
        DfsDistributedWriteResult::CommitOutcomeUnknown { .. } => "commit-outcome-unknown",
        DfsDistributedWriteResult::Outcome { outcome } => match outcome {
            NetworkedDfsWriteOutcome::Pending { .. } => "pending",
            NetworkedDfsWriteOutcome::Committed { repair_jobs, .. } => {
                if repair_jobs.is_empty() {
                    "committed"
                } else {
                    "committed-with-repair"
                }
            }
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

/// Optional wall-clock phase timings; disabled tracing avoids allocating an ID.
pub struct DfsPhaseTimer {
    operation: String,
    last: Option<std::time::Instant>,
}
impl DfsPhaseTimer {
    pub fn new(operation: &str) -> Self {
        let enabled = dfs_perf_trace_enabled();
        Self {
            operation: if enabled {
                operation.into()
            } else {
                String::new()
            },
            last: enabled.then(std::time::Instant::now),
        }
    }
    pub fn mark(&mut self, phase: &'static str) {
        if let Some(last) = self.last.as_mut() {
            let now = std::time::Instant::now();
            dfs_perf_trace(format_args!(
                "attachment-phase operation={} phase={} elapsed_us={}",
                self.operation,
                phase,
                now.duration_since(*last).as_micros()
            ));
            *last = now;
        }
    }
}
