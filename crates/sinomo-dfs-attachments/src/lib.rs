//! Optional file-associated annotations and derived artifacts.
//!
//! The attachment component owns catalogs, producer admission, upload leases
//! and reclamation. All bytes and conditional publications use the distributed
//! filesystem's public authority and storage operations.

pub use sinomo_api::dfs::attachments::*;
use sinomo_dfs::*;
use std::io::{Read, Seek};
use std::path::Path;

pub mod attachment_admission;
pub mod attachment_catalog;
pub mod attachment_maintenance;
pub mod attachment_upload;
pub mod cli;
pub mod service;
pub use service::{
    FEATURE_ROUTES, component_factory, module_enabled, module_id, module_registration,
    register_storage_paths,
};
mod attachments;
mod background_observation;
mod checkpoint;
mod service_trace;

pub use attachment_maintenance::DfsAttachmentMaintenanceReport;

pub type DfsAttachmentResult<T> = Result<T, DfsPosixError>;

/// A module-owned attachment operation borrowing the canonical DFS service.
/// It has no metadata backend, transport, or authority of its own.
pub struct DfsAttachmentService<'a, T> {
    service: &'a mut NetworkedDfsService<T>,
}

impl<'a, T> DfsAttachmentService<'a, T> {
    pub fn new(service: &'a mut NetworkedDfsService<T>) -> Self {
        Self { service }
    }
}

impl<T> std::ops::Deref for DfsAttachmentService<'_, T> {
    type Target = NetworkedDfsService<T>;

    fn deref(&self) -> &Self::Target {
        self.service
    }
}

impl<T> std::ops::DerefMut for DfsAttachmentService<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.service
    }
}

/// Optional attachment operations, imported only by attachment consumers.
pub trait DfsAttachmentsExt<T: DfsMessageTransport> {
    fn file_attachment(
        &mut self,
        request: DfsAttachmentRequest,
        options: Option<DfsWriteOptions>,
    ) -> DfsAttachmentResult<DfsAttachmentResponse>;

    fn file_attachment_from_reader<R: Read + Seek>(
        &mut self,
        request: DfsAttachmentUploadRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> DfsAttachmentResult<DfsAttachmentResponse>;

    fn maintain_attachment_source(
        &mut self,
        source: &DfsAttachmentSource,
        after: Option<&DfsDirectoryPageCursor>,
        limit: usize,
    ) -> DfsAttachmentResult<(Option<DfsDirectoryPageCursor>, Option<u64>, usize)>;

    fn maintain_file_attachments_bounded(
        &mut self,
        checkpoint: &Path,
        shards: &[DfsMetadataShardConfig],
        max_sources: usize,
    ) -> Result<DfsAttachmentMaintenanceReport, DfsError>;
}

impl<T: DfsMessageTransport> DfsAttachmentsExt<T> for NetworkedDfsService<T> {
    fn file_attachment(
        &mut self,
        request: DfsAttachmentRequest,
        options: Option<DfsWriteOptions>,
    ) -> DfsAttachmentResult<DfsAttachmentResponse> {
        DfsAttachmentService::new(self).file_attachment(request, options)
    }

    fn file_attachment_from_reader<R: Read + Seek>(
        &mut self,
        request: DfsAttachmentUploadRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> DfsAttachmentResult<DfsAttachmentResponse> {
        DfsAttachmentService::new(self).file_attachment_from_reader(request, reader, options)
    }

    fn maintain_attachment_source(
        &mut self,
        source: &DfsAttachmentSource,
        after: Option<&DfsDirectoryPageCursor>,
        limit: usize,
    ) -> DfsAttachmentResult<(Option<DfsDirectoryPageCursor>, Option<u64>, usize)> {
        DfsAttachmentService::new(self).maintain_attachment_source(source, after, limit)
    }

    fn maintain_file_attachments_bounded(
        &mut self,
        checkpoint: &Path,
        shards: &[DfsMetadataShardConfig],
        max_sources: usize,
    ) -> Result<DfsAttachmentMaintenanceReport, DfsError> {
        DfsAttachmentService::new(self).maintain_file_attachments_bounded(
            checkpoint,
            shards,
            max_sources,
        )
    }
}

/// Trusted route adapters derive this identity from checked service context.
/// Wire producer labels never select an authenticated catalog owner.
pub(crate) trait DfsAttachmentHostExt<T: DfsMessageTransport> {
    fn file_attachment_as(
        &mut self,
        actor: &str,
        request: DfsAttachmentRequest,
        options: Option<DfsWriteOptions>,
    ) -> DfsAttachmentResult<DfsAttachmentResponse>;

    fn file_attachment_from_reader_as<R: Read + Seek>(
        &mut self,
        actor: &str,
        request: DfsAttachmentUploadRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> DfsAttachmentResult<DfsAttachmentResponse>;
    #[cfg(test)]
    fn maintain_file_attachments_bounded_at(
        &mut self,
        checkpoint: &Path,
        shards: &[DfsMetadataShardConfig],
        max_sources: usize,
        now: u64,
    ) -> Result<DfsAttachmentMaintenanceReport, DfsError>;
}

impl<T: DfsMessageTransport> DfsAttachmentHostExt<T> for NetworkedDfsService<T> {
    fn file_attachment_as(
        &mut self,
        actor: &str,
        request: DfsAttachmentRequest,
        options: Option<DfsWriteOptions>,
    ) -> DfsAttachmentResult<DfsAttachmentResponse> {
        DfsAttachmentService::new(self).file_attachment_as(actor, request, options)
    }

    fn file_attachment_from_reader_as<R: Read + Seek>(
        &mut self,
        actor: &str,
        request: DfsAttachmentUploadRequest,
        reader: R,
        options: DfsWriteOptions,
    ) -> DfsAttachmentResult<DfsAttachmentResponse> {
        DfsAttachmentService::new(self)
            .file_attachment_from_reader_as(actor, request, reader, options)
    }
    #[cfg(test)]
    fn maintain_file_attachments_bounded_at(
        &mut self,
        checkpoint: &Path,
        shards: &[DfsMetadataShardConfig],
        max_sources: usize,
        now: u64,
    ) -> Result<DfsAttachmentMaintenanceReport, DfsError> {
        DfsAttachmentService::new(self).maintain_file_attachments_bounded_at(
            checkpoint,
            shards,
            max_sources,
            now,
        )
    }
}

pub(crate) fn dfs_now_unix_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
        .unwrap_or(1_000_000_000)
}

#[cfg(test)]
mod tests;
