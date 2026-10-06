//! Explicit file history layered on the distributed semantic filesystem core.
//!
//! The module owns history discovery, retention settings, capture commands and
//! restore orchestration. The filesystem owner continues to authorize and
//! durably publish every revision, content grant and recovery job.

pub use sinomo_api::dfs::file_history::*;
use sinomo_core::node::NodeId;
use sinomo_dfs::*;

mod metadata;
mod policy;
mod query;
mod restore;
mod validation;
pub use metadata::{
    capture_from_version as decode_history_capture, core_policy as encode_history_policy,
    history_policy as decode_history_policy, install_history_capture, install_history_policy,
};

pub mod cli;
pub mod service;
pub use service::{component_factory, module_enabled, module_registration, register_storage_paths};

pub const HISTORY_MODULE_ID: &str = "module.dfs.history";

/// A scoped module view; authority and durable effects stay with the core.
pub struct DfsHistoryService<'a, T: DfsMessageTransport> {
    core: &'a mut NetworkedDfsService<T>,
}

impl<'a, T: DfsMessageTransport> DfsHistoryService<'a, T> {
    pub fn new(core: &'a mut NetworkedDfsService<T>) -> Self {
        Self { core }
    }
}

pub trait DfsHistoryExt<T: DfsMessageTransport> {
    fn history(&mut self) -> DfsHistoryService<'_, T>;

    fn file_history_policy(
        &mut self,
        file_system_id: &DfsFileSystemId,
    ) -> Result<DfsFileHistoryPolicy, DfsError> {
        self.history().file_history_policy(file_system_id)
    }

    fn update_file_history_policy(
        &mut self,
        request: DfsFileHistoryPolicyUpdateRequest,
    ) -> Result<DfsFileHistoryPolicy, DfsError> {
        self.history().update_file_history_policy(request)
    }

    fn preserve_local_file_version<R: std::io::Read>(
        &mut self,
        request: DfsFileHistoryPreserveRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> Result<DfsFileHistoryPreserveOutcome, DfsError> {
        self.history()
            .preserve_local_file_version(request, reader, options)
    }

    fn file_history_pin(
        &mut self,
        request: DfsFileHistoryPinRequest,
    ) -> Result<DfsFileHistoryPinResponse, DfsError> {
        self.history().file_history_pin(request)
    }

    fn list_file_history(
        &mut self,
        request: DfsFileHistoryListRequest,
    ) -> Result<DfsFileHistoryListResponse, DfsError> {
        self.history().list_file_history(request)
    }

    fn read_file_history(
        &mut self,
        request: DfsFileHistoryReadRequest,
    ) -> Result<DfsFileHistoryReadOutcome, DfsError> {
        self.history().read_file_history(request)
    }

    fn read_file_version_content(
        &mut self,
        identity: &DfsFileVersionIdentity,
        content_hash: &str,
        expected_size_bytes: Option<u64>,
    ) -> Result<Vec<u8>, DfsError> {
        self.history()
            .read_file_version_content(identity, content_hash, expected_size_bytes)
    }

    fn restore_file_history(
        &mut self,
        request: DfsFileHistoryRestoreRequest,
        options: DfsWriteOptions,
    ) -> Result<DfsFileHistoryRestoreResponse, DfsError> {
        self.history().restore_file_history(request, options)
    }
}

impl<T: DfsMessageTransport> DfsHistoryExt<T> for NetworkedDfsService<T> {
    fn history(&mut self) -> DfsHistoryService<'_, T> {
        DfsHistoryService::new(self)
    }
}

pub use DfsHistoryExt as NetworkedDfsHistoryExt;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod core_integration_tests;
