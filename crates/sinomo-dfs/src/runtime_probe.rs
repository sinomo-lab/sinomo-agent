//! Isolated DFS runtime correctness and performance probes.
//!
//! These probes exercise production storage code against a caller-owned empty
//! directory. They are intentionally ordinary runtime APIs rather than Rust
//! test-harness workloads so `sinomo test run` can collect phase metrics and
//! apply platform-appropriate health thresholds from an installed binary.

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::{DfsError, LocalContentStore, LocalSobjMetadataBackend};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfsContentVersionGcRuntimeProbeOptions {
    pub version_count: usize,
    pub write_batch_size: usize,
}

impl Default for DfsContentVersionGcRuntimeProbeOptions {
    fn default() -> Self {
        Self {
            version_count: 10_000,
            write_batch_size: 4_096,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfsContentVersionGcRuntimeProbeReport {
    pub version_count: u64,
    pub write_elapsed_ms: u64,
    pub reservation_release_elapsed_ms: u64,
    pub projection_elapsed_ms: u64,
    pub gc_elapsed_ms: u64,
    pub restart_elapsed_ms: u64,
    pub total_elapsed_ms: u64,
    pub deleted_version_count: u64,
    pub durable_delete_batch_count: u64,
    pub durable_directory_sync_count: u64,
    pub retained_manifest_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfsMetadataHistoryGcRuntimeProbeOptions {
    pub overwrite_count: usize,
    pub namespace_request_count: usize,
    pub content_batch_size: usize,
    pub gc_batch_size: u64,
}

impl Default for DfsMetadataHistoryGcRuntimeProbeOptions {
    fn default() -> Self {
        Self {
            overwrite_count: 100_000,
            namespace_request_count: 1_000,
            content_batch_size: 4_096,
            gc_batch_size: 256,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfsMetadataHistoryGcRuntimeProbeReport {
    pub overwrite_count: u64,
    pub namespace_request_count: u64,
    pub unique_content_hash_count: u64,
    pub metadata_gc_passes: u64,
    pub orphan_gc_passes: u64,
    pub content_write_elapsed_ms: u64,
    pub reservation_release_elapsed_ms: u64,
    pub metadata_write_elapsed_ms: u64,
    pub metadata_gc_elapsed_ms: u64,
    pub content_gc_elapsed_ms: u64,
    pub restart_elapsed_ms: u64,
    pub total_elapsed_ms: u64,
    pub deleted_orphan_count: u64,
    pub reclaimed_packed_segment_bytes: u64,
    pub retained_manifest_count: u64,
    pub retained_metadata_record_count: u64,
    pub retained_metadata_bytes: u64,
    pub journal_bytes_before: u64,
    pub journal_bytes_after: u64,
    pub materialized_table_bytes: u64,
    pub snapshot_bytes_before: u64,
    pub snapshot_bytes_after: u64,
    pub content_bytes_before: u64,
    pub content_bytes_after: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfsMetadataHydrationRuntimeProbeOptions {
    pub object_count: usize,
}

impl Default for DfsMetadataHydrationRuntimeProbeOptions {
    fn default() -> Self {
        Self {
            object_count: crate::DFS_METADATA_HYDRATE_OBJECT_PAGE_SIZE.saturating_add(1),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DfsMetadataHydrationRuntimeProbeReport {
    pub object_count: u64,
    pub page_size: u64,
    pub crossed_page_boundary: bool,
    pub write_elapsed_ms: u64,
    pub hydration_elapsed_ms: u64,
    pub total_elapsed_ms: u64,
    pub tail_object_restored: bool,
    pub tombstone_restored: bool,
}

pub fn run_content_version_gc_runtime_probe(
    root: &Path,
    options: DfsContentVersionGcRuntimeProbeOptions,
) -> Result<DfsContentVersionGcRuntimeProbeReport, DfsError> {
    LocalContentStore::run_version_gc_runtime_probe(root, options)
}

pub fn run_metadata_history_gc_runtime_probe(
    root: &Path,
    options: DfsMetadataHistoryGcRuntimeProbeOptions,
) -> Result<DfsMetadataHistoryGcRuntimeProbeReport, DfsError> {
    LocalSobjMetadataBackend::run_history_gc_runtime_probe(root, options)
}

pub fn run_metadata_hydration_runtime_probe(
    root: &Path,
    options: DfsMetadataHydrationRuntimeProbeOptions,
) -> Result<DfsMetadataHydrationRuntimeProbeReport, DfsError> {
    crate::service_hydration::run_metadata_hydration_runtime_probe(root, options)
}
