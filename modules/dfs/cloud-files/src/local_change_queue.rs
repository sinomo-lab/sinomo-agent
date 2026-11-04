//! Cloud Files storage ownership adapter for the shared writeback queue.
use crate::protocol::SINOMO_DFS_CLOUD_FILES_MODULE_ID;
pub use shared::{
    WritebackConflictEvidence as CloudFilesConflictEvidence,
    WritebackContentStager as CloudFilesContentStager,
    WritebackCursorOnlyQueueInspection as CloudFilesCursorOnlyQueueInspection,
    WritebackCursorOnlyQueueInspectionGuard as CloudFilesCursorOnlyQueueInspectionGuard,
    WritebackDestinationPrecondition as CloudFilesDestinationPrecondition,
    WritebackExpectedObjectState as CloudFilesExpectedObjectState,
    WritebackLocalChangeQueueError as CloudFilesLocalChangeQueueError,
    WritebackLocalOperation as CloudFilesLocalOperation,
    WritebackLocalOperationKind as CloudFilesLocalOperationKind,
    WritebackLocalOperationPreconditions as CloudFilesLocalOperationPreconditions,
    WritebackLocalOperationRequest as CloudFilesLocalOperationRequest,
    WritebackLocalOperationRetirement as CloudFilesLocalOperationRetirement,
    WritebackLocalOperationStatus as CloudFilesLocalOperationStatus,
    WritebackLocalQueueMaintenance as CloudFilesLocalQueueMaintenance,
    WritebackLocalQueueSummary as CloudFilesLocalQueueSummary,
    WritebackRemoteMutationEvidence as CloudFilesRemoteMutationEvidence,
    WritebackStagedContent as CloudFilesStagedContent, stable_capture_label,
};
use sinomo_file_sync::local_change_queue as shared;
use sinomo_runtime::local::SinomoDataDir;

pub struct CloudFilesLocalChangeQueue(shared::WritebackLocalChangeQueue);
impl std::ops::Deref for CloudFilesLocalChangeQueue {
    type Target = shared::WritebackLocalChangeQueue;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for CloudFilesLocalChangeQueue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl CloudFilesLocalChangeQueue {
    pub fn open(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, CloudFilesLocalChangeQueueError> {
        shared::WritebackLocalChangeQueue::open(
            SINOMO_DFS_CLOUD_FILES_MODULE_ID,
            data_dir,
            mount,
            root,
        )
        .map(Self)
    }
    pub(crate) fn open_authorized_cursor_recovery(
        data_dir: &SinomoDataDir,
        mount: impl Into<String>,
        root: impl Into<String>,
    ) -> Result<Self, CloudFilesLocalChangeQueueError> {
        shared::WritebackLocalChangeQueue::open_authorized_cursor_recovery(
            SINOMO_DFS_CLOUD_FILES_MODULE_ID,
            data_dir,
            mount,
            root,
        )
        .map(Self)
    }
}
pub(crate) fn fresh_local_change_scope_is_absent(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<bool, CloudFilesLocalChangeQueueError> {
    shared::fresh_local_change_scope_is_absent(
        SINOMO_DFS_CLOUD_FILES_MODULE_ID,
        data_dir,
        mount,
        root,
    )
}
pub(crate) fn inspect_cursor_only_local_change_queue(
    data_dir: &SinomoDataDir,
    mount: &str,
    root: &str,
) -> Result<CloudFilesCursorOnlyQueueInspectionGuard, CloudFilesLocalChangeQueueError> {
    shared::inspect_cursor_only_local_change_queue(
        SINOMO_DFS_CLOUD_FILES_MODULE_ID,
        data_dir,
        mount,
        root,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cloud_files_adapter_keeps_existing_owner_and_shared_lock() {
        let path = std::env::temp_dir().join(format!(
            "cf-shared-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let data = SinomoDataDir::new(&path);
        let cloud = CloudFilesLocalChangeQueue::open(&data, "mount-a", "/services").unwrap();
        let location = cloud.queue_dir().to_path_buf();
        assert!(
            location.starts_with(
                data.module_runtime_dir()
                    .join(SINOMO_DFS_CLOUD_FILES_MODULE_ID)
                    .join("local-changes")
            )
        );
        assert!(
            shared::WritebackLocalChangeQueue::open(
                SINOMO_DFS_CLOUD_FILES_MODULE_ID,
                &data,
                "mount-a",
                "/services"
            )
            .is_err()
        );
        drop(cloud);
        let reopened = shared::WritebackLocalChangeQueue::open(
            SINOMO_DFS_CLOUD_FILES_MODULE_ID,
            &data,
            "mount-a",
            "/services",
        )
        .unwrap();
        assert_eq!(reopened.queue_dir(), location);
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}
