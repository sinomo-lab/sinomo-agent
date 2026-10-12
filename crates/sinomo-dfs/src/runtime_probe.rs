//! Isolated DFS runtime correctness and performance probes.
//!
//! These probes exercise production storage code against a caller-owned empty
//! directory. They are intentionally ordinary runtime APIs rather than Rust
//! test-harness workloads so `sinomo test run` can collect phase metrics and
//! apply platform-appropriate health thresholds from an installed binary.

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::{DfsError, LocalContentStore};

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

pub fn run_content_version_gc_runtime_probe(
    root: &Path,
    options: DfsContentVersionGcRuntimeProbeOptions,
) -> Result<DfsContentVersionGcRuntimeProbeReport, DfsError> {
    LocalContentStore::run_version_gc_runtime_probe(root, options)
}
