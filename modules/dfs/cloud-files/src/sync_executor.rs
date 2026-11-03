//! Shared durable execution with Cloud Files-compatible names.
pub use sinomo_file_sync::sync_executor::{
    WritebackDfsMutationAdapter as CloudFilesDfsMutationAdapter,
    WritebackDfsMutationCommand as CloudFilesDfsMutationCommand,
    WritebackDfsMutationContent as CloudFilesDfsMutationContent,
    WritebackDfsMutationOutcome as CloudFilesDfsMutationOutcome,
    WritebackDfsMutationReconciliation as CloudFilesDfsMutationReconciliation,
    WritebackLocalCompletionEffect as CloudFilesLocalCompletionEffect,
    WritebackLocalCompletionOutcome as CloudFilesLocalCompletionOutcome,
    WritebackSyncExecutionProgress as CloudFilesSyncExecutionProgress,
    WritebackSyncExecutorError as CloudFilesSyncExecutorError,
    execute_ready_writeback_operations as execute_ready_cloud_files_operations,
    execute_ready_writeback_operations_bounded as execute_ready_cloud_files_operations_bounded,
    execute_writeback_operation as execute_cloud_files_operation,
};
