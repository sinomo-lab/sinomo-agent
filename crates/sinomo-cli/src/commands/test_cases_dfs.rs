//! DFS cluster runtime test cases.
//!
//! These run on a live cluster node (`sinomo test run <id> --require-cluster`)
//! against the node's own SIFS service, so an external orchestrator (see
//! docker/test-cluster.sh) can drive cross-node consistency scenarios by
//! invoking parameterized probes on different nodes:
//!
//! - `cluster.dfs.read-your-write` — self-contained write/read/rename/delete
//!   roundtrip; read-your-write must hold immediately on the writing node.
//! - `cluster.dfs.routed-status` — route a signed DFS status RPC to every
//!   remote main daemon; this catches DFS route auth/routing regressions that a
//!   generic network ping can miss. Route/RPC failures are hard-gated by
//!   default; dirty DFS status may still poll for convergence.
//! - `cluster.dfs.remote-shard-write` — choose a metadata shard whose primary is
//!   not this node, then write through that routed mutation path.
//! - `cluster.dfs.shard-layout` — resolve representative paths for the default
//!   root, services, and users lanes through live cluster KV and report their
//!   current shard primaries and namespace-prepare codecs for external
//!   performance orchestration.
//! - `cluster.dfs.service-advertisements` — verify DFS service discovery
//!   records advertise node identities instead of concrete network routes.
//! - `cluster.dfs.placement-management` — exercise Files-compatible placement
//!   preview/update/progress APIs, constraints, inheritance, and reconciliation
//!   of files written before the policy change.
//! - `cluster.dfs.invariants` — durable operational-state sweep: no open
//!   conflicts, leaked cross-shard locks, or reachable metadata-replica lag.
//! - `cluster.dfs.lightweight-latency` — warmed SIFS stat/list/read-prefix
//!   latency probe for file-browser-style lightweight operations.
//! - `cluster.dfs.raw-lightweight-latency` — the matching node-local raw
//!   filesystem operations under `SINOMO_DATA_DIR`, used to price the DFS
//!   algorithm and network overhead against the actual backing filesystem.
//! - `cluster.dfs.protocol-codec-baseline` — in-memory DFS body-aware object-RPC
//!   head codec throughput for representative large-object messages, independent
//!   of attachment I/O, disk, and live network conditions.
//! - `cluster.dfs.content-version-gc` / `cluster.dfs.metadata-history-gc` —
//!   isolated runtime storage-maintenance acceptance workloads with phase,
//!   reclamation, durability-amplification, and restart metrics.
//! - `cluster.dfs.metadata-hydration` — isolated durable metadata hydration
//!   correctness and latency across the production object-page boundary.
//! - `cluster.dfs.write` / `cluster.dfs.expect-file` / `cluster.dfs.expect-absent`
//!   / `cluster.dfs.rename` / `cluster.dfs.remove` — parameterized probes
//!   (`--param path=... --param payload=...`); the expect-* probes poll with a
//!   bounded deadline to absorb cross-node replication lag.
//! - `cluster.dfs.expect-replicated` / `cluster.dfs.damage-local-loose-blob`
//!   — fault-injection probes used by the Docker gate to verify reads survive
//!   one damaged durable replica.
//! - `cluster.dfs.small-file-baseline` — node-local raw tiny-file write baseline
//!   used to compare DFS bulk-write syscall amplification against ordinary file
//!   creation on the same container storage path.
//! - `cluster.dfs.bulk-write` / `cluster.dfs.bulk-expect` — deterministic bulk
//!   write/read probes for cluster-level throughput, convergence evidence, and
//!   steady-state process I/O accounting.
//! - `cluster.dfs.disk-baseline` — node-local raw file write/read baseline used
//!   to compare DFS large-object throughput against the container's current
//!   storage path.
//! - `cluster.dfs.large-write` / `cluster.dfs.large-expect` — deterministic
//!   large-object throughput probes that exercise streaming body transfer and
//!   read-side no-write accounting.

use sinomo_runtime::dfs::DaemonDfsApiClientExt;

use crate::app_api::SifsBrowser;
use crate::file_manager::FileManagerBackend;
use crate::sifs_file_manager::{
    SifsFileManagerBackend, SifsFileManagerObservation, SifsOneShotBulkWriteRequester,
};
use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::cluster::ClusterKvPathsExt;
use sinomo_runtime::core::DfsObjectPolicy;
use sinomo_runtime::dfs::{
    self, DaemonDfsInspectionRequest, DaemonDfsInspectionResponse, DfsAttachmentMessageCommand,
    DfsFetchBlobResponse, DfsFetchBlobResult, DfsFileSystemId, DfsMessageCommand,
    DfsNetworkMessage, DfsObjectId, DfsOperatorRepairView, DfsOperatorStatus, DfsPlacementPolicy,
    DfsPlacementReconciliationState, DfsPlacementScope, DfsReadData, DfsReadResponse,
    DfsReadResult, DfsRoutedMessage, DfsRpcAttachmentDescriptor,
    DfsStableNamespaceOperationReconciliation, DfsStageBlobCommand, DfsVersion,
    SIFS_FILE_SYSTEM_ID,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths, ObjectRpcRequest};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestMetricUnit, SinomoTestOutcome,
    SinomoTestRegistryBuilder, SinomoTestRequirement, SinomoTestResult,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_EXPECT_TIMEOUT_SECONDS: u64 = 30;
const RENAME_ATTEMPT_MAX: Duration = Duration::from_secs(60);
const DEFAULT_PLACEMENT_EXPECT_TIMEOUT_SECONDS: u64 = 180;
const DEFAULT_ROUTED_STATUS_TIMEOUT_MS: u64 = 5_000;
const MAX_ROUTED_STATUS_CONCURRENCY: usize = 16;
const DEFAULT_BULK_FILE_COUNT: usize = 128;
const DEFAULT_BULK_FILE_BYTES: usize = 4 * 1024;
const DEFAULT_LIGHTWEIGHT_LATENCY_ITERATIONS: usize = 128;
const DEFAULT_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS: usize = 16;
const DEFAULT_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES: usize = 64;
const DEFAULT_LIGHTWEIGHT_LATENCY_MAX_BACKEND_OPEN_MS: usize = 2_000;
const MAX_LIGHTWEIGHT_LATENCY_ITERATIONS: usize = 10_000;
const MAX_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS: usize = 1_000;
const MAX_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES: usize = 64 * 1024;
const DEFAULT_LARGE_FILE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PROTOCOL_CODEC_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_PROTOCOL_CODEC_ITERATIONS: usize = 8;
const DEFAULT_CONTENT_VERSION_GC_VERSIONS: usize = 10_000;
const DEFAULT_CONTENT_VERSION_GC_WRITE_BATCH: usize = 4_096;
const DEFAULT_CONTENT_VERSION_GC_MAX_TOTAL_MS: u64 = 120_000;
const DEFAULT_CONTENT_VERSION_GC_MAX_RESTART_MS: u64 = 10_000;

const MAX_DFS_STORAGE_RUNTIME_PROBE_ITEMS: usize = 1_000_000;
const EXPECT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_ROUNDTRIP_BASE: &str = "/services/cluster-tests";
const MAX_RECORDED_BULK_RETRY_REASONS: usize = 8;

const DFS_TEST_NAMESPACE_ROOTS: [&str; 3] = [
    "/cluster-tests/runs",
    "/services/cluster-tests/runs",
    "/users/cluster-tests/runs",
];

struct ScopedSifsTestNamespace {
    browser: SifsBrowser,
    roots: Vec<String>,
}

mod attachments;
mod semantic_lost_reply;

struct ScopedDfsRuntimeProbeRoot {
    root: PathBuf,
    run_root: PathBuf,
}

impl ScopedDfsRuntimeProbeRoot {
    fn create(ctx: &SinomoTestContext, case: &str) -> Result<Self, SinomoTestError> {
        if case.is_empty()
            || !case
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(SinomoTestError::failed(format!(
                "invalid DFS runtime probe case name `{case}`"
            )));
        }
        let run_root = ctx
            .data_dir()
            .join("runtime-tests")
            .join("dfs")
            .join(ctx.run_id());
        let root = run_root.join(case);
        if root.exists() {
            return Err(SinomoTestError::failed(format!(
                "DFS runtime probe root already exists: {}",
                root.display()
            )));
        }
        fs::create_dir_all(&root).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot create DFS runtime probe root {}: {source}",
                root.display()
            ))
        })?;
        let metadata = fs::symlink_metadata(&root).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot inspect DFS runtime probe root {}: {source}",
                root.display()
            ))
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(SinomoTestError::failed(format!(
                "DFS runtime probe root is not a real directory: {}",
                root.display()
            )));
        }
        Ok(Self { root, run_root })
    }

    fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for ScopedDfsRuntimeProbeRoot {
    fn drop(&mut self) {
        if self.root.parent() != Some(self.run_root.as_path()) || self.root == self.run_root {
            return;
        }
        let Ok(metadata) = fs::symlink_metadata(&self.root) else {
            return;
        };
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}

impl ScopedSifsTestNamespace {
    fn open(
        ctx: &SinomoTestContext,
        roots: impl IntoIterator<Item = String>,
    ) -> Result<Self, SinomoTestError> {
        let roots = roots
            .into_iter()
            .map(|root| scoped_sifs_test_run_root(&root))
            .collect::<Result<BTreeSet<_>, _>>()?
            .into_iter()
            .collect();
        Ok(Self {
            browser: open_local_daemon_sifs_browser(ctx)?,
            roots,
        })
    }
}

impl Drop for ScopedSifsTestNamespace {
    fn drop(&mut self) {
        for root in self.roots.iter().rev() {
            let _ = remove_scoped_sifs_test_tree(&self.browser, root);
        }
    }
}

fn scoped_dfs_test_base(ctx: &SinomoTestContext, case: &str) -> Result<String, SinomoTestError> {
    let base = ctx
        .parameter("base")
        .unwrap_or(DEFAULT_ROUNDTRIP_BASE)
        .trim_end_matches('/');
    if !matches!(
        base,
        "/cluster-tests" | "/services/cluster-tests" | "/users/cluster-tests"
    ) {
        return Err(SinomoTestError::InvalidParameter {
            name: "base".to_string(),
            detail: "must be one of /cluster-tests, /services/cluster-tests, or /users/cluster-tests; live tests create a temporary subtree below its runs directory"
                .to_string(),
        });
    }
    let path = format!("{base}/runs/{}/{case}", ctx.run_id());
    validate_scoped_sifs_test_namespace(&path)?;
    Ok(path)
}

fn validate_scoped_sifs_test_namespace(path: &str) -> Result<(), SinomoTestError> {
    let normalized = path.trim_end_matches('/');
    let path_safe = normalized.starts_with('/')
        && !normalized.bytes().any(|byte| byte == 0)
        && !normalized.split('/').any(|part| matches!(part, "." | ".."));
    let scoped = DFS_TEST_NAMESPACE_ROOTS.iter().any(|root| {
        normalized
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/') && suffix.len() > 1)
    });
    if path_safe && scoped {
        Ok(())
    } else {
        Err(SinomoTestError::failed(format!(
            "refusing to use unscoped SIFS test namespace `{path}`"
        )))
    }
}

fn scoped_sifs_test_run_root(path: &str) -> Result<String, SinomoTestError> {
    validate_scoped_sifs_test_namespace(path)?;
    let normalized = path.trim_end_matches('/');
    for root in DFS_TEST_NAMESPACE_ROOTS {
        if let Some(suffix) = normalized.strip_prefix(root) {
            let run_id = suffix
                .trim_start_matches('/')
                .split('/')
                .next()
                .filter(|run_id| !run_id.is_empty())
                .ok_or_else(|| {
                    SinomoTestError::failed(format!(
                        "SIFS test namespace `{path}` has no run identity"
                    ))
                })?;
            return Ok(format!("{root}/{run_id}"));
        }
    }
    Err(SinomoTestError::failed(format!(
        "SIFS test namespace `{path}` has no recognized runs root"
    )))
}

fn remove_scoped_sifs_test_tree(browser: &SifsBrowser, path: &str) -> Result<(), String> {
    validate_scoped_sifs_test_namespace(path).map_err(|error| error.to_string())?;
    for entry in browser.list_if_exists(path)? {
        if entry.is_dir && !entry.is_link {
            remove_scoped_sifs_test_tree(browser, &entry.path)?;
        } else {
            browser.delete(&entry.path, false)?;
        }
    }
    match browser.delete(path, true) {
        Ok(()) => Ok(()),
        Err(error) if sifs_cleanup_error_is_not_found(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

fn sifs_cleanup_error_is_not_found(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("not found") || error.contains("does not exist") || error.contains("enoent")
}

pub fn register_dfs_cluster_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(attachments::dfs_file_attachments())
        .register(semantic_lost_reply::dfs_semantic_lost_reply())
        .register(dfs_read_your_write())
        .register(dfs_routed_status())
        .register(dfs_remote_shard_write())
        .register(dfs_shard_layout())
        .register(dfs_service_advertisements())
        .register(dfs_placement_management())
        .register(dfs_placement_activity())
        .register(dfs_concurrent_file_race())
        .register(dfs_concurrent_dir_race())
        .register(dfs_failover_current_read())
        .register(dfs_invariants())
        .register(dfs_lightweight_latency())
        .register(dfs_raw_lightweight_latency())
        .register(dfs_protocol_codec_baseline())
        .register(dfs_content_version_gc())
        .register(dfs_write())
        .register(dfs_write_latency())
        .register(dfs_expect_file())
        .register(dfs_expect_absent())
        .register(dfs_expect_replicated())
        .register(dfs_damage_local_loose_blob())
        .register(dfs_rename())
        .register(dfs_remove())
        .register(dfs_small_file_baseline())
        .register(dfs_bulk_write())
        .register(dfs_bulk_expect())
        .register(dfs_disk_baseline())
        .register(dfs_large_write())
        .register(dfs_large_expect())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_read_your_write(ctx) {
        id: "cluster.dfs.read-your-write",
        summary: "SIFS write/read/rename/delete roundtrip with immediate read-your-write on this node",
        tags: ["cluster-dfs", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_read_your_write(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_content_version_gc(ctx) {
        id: "cluster.dfs.content-version-gc",
        summary: "Run isolated 10k content-version GC and report phase, durability-amplification, and restart metrics",
        tags: ["cluster-dfs", "probe", "performance", "storage-maintenance", "dfs-storage-runtime-acceptance"],
        requirements: [],
        run: {
            run_content_version_gc(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_routed_status(ctx) {
        id: "cluster.dfs.routed-status",
        summary: "Route signed DFS status RPCs to every remote main daemon",
        tags: ["cluster-dfs", "probe", "routing", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_routed_status(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_remote_shard_write(ctx) {
        id: "cluster.dfs.remote-shard-write",
        summary: "Write a SIFS file through a metadata shard whose primary is remote from this node",
        tags: ["cluster-dfs", "probe", "routing", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_remote_shard_write(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_shard_layout(ctx) {
        id: "cluster.dfs.shard-layout",
        summary: "Resolve the live root, services, and users metadata-shard primaries and prepare codecs",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime, SinomoTestRequirement::ClusterKvSnapshot],
        run: {
            run_shard_layout(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_service_advertisements(ctx) {
        id: "cluster.dfs.service-advertisements",
        summary: "Verify DFS service advertisements use network-routed node targets",
        tags: ["cluster-dfs", "probe", "routing", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_service_advertisements(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_placement_activity(ctx) {
        id: "cluster.dfs.placement-activity",
        summary: "Read a bounded page of placement activity and exact active-job progress without mutations",
        tags: ["cluster-dfs", "probe", "diagnostic", "placement"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_placement_activity(ctx)
        }
    }
}

fn run_placement_activity(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let limit = optional_usize_parameter(ctx, "limit", 16)?;
    if limit == 0 || limit > dfs::MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE {
        return Err(SinomoTestError::InvalidParameter {
            name: "limit".into(),
            detail: format!(
                "must be between 1 and {}",
                dfs::MAX_DFS_PLACEMENT_RECONCILIATION_ACTIVITY_PAGE_SIZE
            ),
        });
    }
    let browser = open_local_daemon_sifs_browser(ctx)?;
    let page = browser
        .placement_activity(crate::app_api::SifsPlacementActivityRequest {
            statuses: vec![
                dfs::DfsPlacementReconciliationActivityStatus::Running,
                dfs::DfsPlacementReconciliationActivityStatus::Retrying,
            ],
            path_prefix: ctx.parameter("path-prefix").map(str::to_string),
            limit,
            ..Default::default()
        })
        .map_err(SinomoTestError::failed)?;
    let mut jobs = Vec::with_capacity(page.activities.len());
    for activity in &page.activities {
        let job = browser
            .placement_progress(&activity.job_id)
            .map_err(SinomoTestError::failed)?;
        jobs.push(serde_json::json!({
            "job_id": job.job_id,
            "path": job.path,
            "state": job.state,
            "revision": job.revision,
            "cursor": job.cursor,
            "scanned_object_count": job.scanned_object_count,
            "matched_object_count": job.matched_object_count,
            "completed_repair_count": job.completed_repair_count,
            "pending_repair_count": job.pending_repair_count,
            "updated_at_unix_nanos": job.updated_at_unix_nanos,
            "last_error": job.last_error,
        }));
    }
    ctx.record("activity", &page);
    ctx.record("jobs", jobs);
    ctx.record("limit", limit);
    Ok(SinomoTestOutcome::passed(
        "bounded placement activity collected",
    ))
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_placement_management(ctx) {
        id: "cluster.dfs.placement-management",
        summary: "Exercise managed DFS placement preview, updates, constraints, inheritance, and durable reconciliation through daemon APIs",
        tags: ["cluster-dfs", "probe", "placement"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_placement_management(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_concurrent_file_race(ctx) {
        id: "cluster.dfs.concurrent-file-race",
        summary: "Race two SIFS file writers and verify the final current read is one complete payload",
        tags: ["cluster-dfs", "probe", "correctness", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_concurrent_file_race(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_concurrent_dir_race(ctx) {
        id: "cluster.dfs.concurrent-dir-race",
        summary: "Race child creation with parent rmdir and verify no orphan child remains",
        tags: ["cluster-dfs", "probe", "correctness", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_concurrent_dir_race(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_failover_current_read(ctx) {
        id: "cluster.dfs.failover-current-read",
        summary: "Verify a current SIFS read refreshes after another writer advances the visible version",
        tags: ["cluster-dfs", "probe", "correctness", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_failover_current_read(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_invariants(ctx) {
        id: "cluster.dfs.invariants",
        summary: "SIFS durable invariants: no leaked cross-shard locks or reachable replica lag",
        tags: ["cluster-dfs", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_invariants(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_lightweight_latency(ctx) {
        id: "cluster.dfs.lightweight-latency",
        summary: "Measure cold SIFS hydration and warmed stat/list/read-prefix latency for lightweight file-browser operations",
        tags: ["cluster-dfs", "probe", "performance", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_lightweight_latency(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_raw_lightweight_latency(ctx) {
        id: "cluster.dfs.raw-lightweight-latency",
        summary: "Measure warmed node-local raw stat/list/read-prefix latency under SINOMO_DATA_DIR",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [],
        run: {
            run_raw_lightweight_latency(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_protocol_codec_baseline(ctx) {
        id: "cluster.dfs.protocol-codec-baseline",
        summary: "Measure in-memory DFS data-plane object-RPC encode/decode throughput for body-aware heads",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [],
        run: {
            run_protocol_codec_baseline(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_write(ctx) {
        id: "cluster.dfs.write",
        summary: "Write a SIFS file on this node (--param path=... --param payload=...)",
        tags: ["cluster-dfs", "probe"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_write(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_write_latency(ctx) {
        id: "cluster.dfs.write-latency",
        summary: "Measure ordinary committed daemon API create/overwrite latency with per-operation p95 gates",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_write_latency(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_expect_file(ctx) {
        id: "cluster.dfs.expect-file",
        summary: "Poll until a SIFS file readable on this node matches the payload (--param path=... --param payload=...)",
        tags: ["cluster-dfs", "probe"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_expect_file(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_expect_absent(ctx) {
        id: "cluster.dfs.expect-absent",
        summary: "Poll until a SIFS path is absent on this node (--param path=...)",
        tags: ["cluster-dfs", "probe"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_expect_absent(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_expect_replicated(ctx) {
        id: "cluster.dfs.expect-replicated",
        summary: "Poll until a SIFS file has the requested number of current locations",
        tags: ["cluster-dfs", "probe", "fault-injection"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_expect_replicated(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_damage_local_loose_blob(ctx) {
        id: "cluster.dfs.damage-local-loose-blob",
        summary: "Delete this node's local loose blob for a replicated SIFS file",
        tags: ["cluster-dfs", "probe", "fault-injection"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_damage_local_loose_blob(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_rename(ctx) {
        id: "cluster.dfs.rename",
        summary: "Rename a SIFS path from this node, routed/coordinated as needed (--param from=... --param to=...)",
        tags: ["cluster-dfs", "probe"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_rename(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_remove(ctx) {
        id: "cluster.dfs.remove",
        summary: "Remove a SIFS file or empty directory from this node (--param path=...)",
        tags: ["cluster-dfs", "probe"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_remove(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_small_file_baseline(ctx) {
        id: "cluster.dfs.small-file-baseline",
        summary: "Measure node-local raw tiny-file create/write throughput and syscall count under SINOMO_DATA_DIR",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [],
        run: {
            run_small_file_baseline(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_bulk_write(ctx) {
        id: "cluster.dfs.bulk-write",
        summary: "Write deterministic SIFS files and report throughput (--param base=... [--param files=N] [--param bytes=N] [--param overwrite-existing=BOOL])",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_bulk_write(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_bulk_expect(ctx) {
        id: "cluster.dfs.bulk-expect",
        summary: "Poll deterministic SIFS files from this node and report convergence/read throughput and read-side process I/O (--param base=... [--param files=N] [--param bytes=N])",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_bulk_expect(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_disk_baseline(ctx) {
        id: "cluster.dfs.disk-baseline",
        summary: "Measure node-local raw file write/read throughput under SINOMO_DATA_DIR (--param bytes=N)",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [],
        run: {
            run_disk_baseline(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_large_write(ctx) {
        id: "cluster.dfs.large-write",
        summary: "Write one deterministic large SIFS file and report throughput, publish timing, and process I/O (--param path=... [--param bytes=N])",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_large_write(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_large_expect(ctx) {
        id: "cluster.dfs.large-expect",
        summary: "Poll one deterministic large SIFS file from this node and report convergence/read throughput and read-side process I/O (--param path=... [--param bytes=N])",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_large_expect(ctx)
        }
    }
}

fn run_read_your_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = scoped_dfs_test_base(ctx, "read-your-write")?;
    let _namespace = ScopedSifsTestNamespace::open(ctx, [base.clone()])?;
    let timeout = expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let nonce = probe_nonce();
    let path = format!("{base}/ryw-{nonce}.txt");
    let renamed = format!("{base}/ryw-{nonce}-renamed.txt");
    let payload = format!("read-your-write {node} {nonce}");
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("renamed", &renamed);

    let stage_started = Instant::now();
    retry_bulk_mutation(
        deadline,
        || backend.write_file_wait_for_commit_with_deadline(&path, payload.as_bytes(), deadline),
        || format!("cannot write SIFS file {path}"),
    )?;
    ctx.record("write_ms", stage_started.elapsed().as_millis());
    let stage_started = Instant::now();
    expect_payload_now(&backend, &path, payload.as_bytes(), "after write")?;
    ctx.record("read_after_write_ms", stage_started.elapsed().as_millis());

    let stage_started = Instant::now();
    backend
        .rename_with_deadline(&path, &renamed, deadline)
        .map_err(SinomoTestError::failed)?;
    ctx.record("rename_ms", stage_started.elapsed().as_millis());
    let stage_started = Instant::now();
    expect_payload_now(&backend, &renamed, payload.as_bytes(), "after rename")?;
    ctx.record("read_after_rename_ms", stage_started.elapsed().as_millis());
    let stage_started = Instant::now();
    expect_absent_now(&backend, &path, "after rename")?;
    ctx.record(
        "absent_after_rename_ms",
        stage_started.elapsed().as_millis(),
    );

    let stage_started = Instant::now();
    backend
        .delete_file_with_deadline(&renamed, deadline)
        .map_err(SinomoTestError::failed)?;
    ctx.record("delete_ms", stage_started.elapsed().as_millis());
    let stage_started = Instant::now();
    expect_absent_now(&backend, &renamed, "after delete")?;
    ctx.record(
        "absent_after_delete_ms",
        stage_started.elapsed().as_millis(),
    );

    Ok(SinomoTestOutcome::passed(format!(
        "write/read/rename/delete roundtrip held read-your-write on node {node}"
    )))
}

fn run_routed_status(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let timeout_ms =
        optional_u64_parameter(ctx, "timeout-ms")?.unwrap_or(DEFAULT_ROUTED_STATUS_TIMEOUT_MS);
    let gate = DfsRoutedStatusGate::from_context(ctx, timeout_ms)?;
    let timeout = expect_timeout(ctx)?;
    let expected_targets = optional_usize_parameter(ctx, "expected-targets", 1)?;
    let max_remote_pending_writes =
        optional_usize_parameter(ctx, "max-remote-pending-writes", usize::MAX)?;
    let max_remote_pending_repairs =
        optional_usize_parameter(ctx, "max-remote-pending-repairs", usize::MAX)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon_for_data_dir(&data_dir, &cluster_paths).map_err(
        |error| {
            SinomoTestError::failed(format!(
                "cannot refresh cluster snapshot before DFS routed status: {error}"
            ))
        },
    )?;
    let (state, _) = super::cluster_health_rows_for_data_dir(&data_dir).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot read live cluster membership before DFS routed status: {error}"
        ))
    })?;
    let provider_node_ids = dfs::discovered_cluster_dfs_provider_node_ids(&cluster_paths, &state)
        .map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot discover DFS providers before routed status: {error}"
            ))
        })?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let targets = filter_dfs_routed_status_targets(
        ctx,
        &state,
        super::test::live_online_remote_main_test_targets(&data_dir, &state)?
            .into_iter()
            .filter(|node_id| provider_node_ids.contains(node_id))
            .collect(),
    )?;
    let unavailable_repair_targets =
        dfs_unavailable_repair_targets_from_state(&cluster_paths, &state);
    ctx.record("target_count", targets.len());
    ctx.record(
        "targets",
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
    );
    ctx.record(
        "probe_concurrency",
        targets.len().min(MAX_ROUTED_STATUS_CONCURRENCY),
    );
    if !super::test::cluster_test_has_expected_target_count(targets.len(), expected_targets) {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS routed status saw {} remote main targets, expected at least {expected_targets}",
            targets.len()
        )));
    }

    let daemon_status = match sinomo_runtime::daemon::probe_daemon_status(
        &sinomo_runtime::daemon::DaemonPaths::new(&data_dir),
        sinomo_runtime::daemon::DAEMON_API_PROBE_TIMEOUT,
    ) {
        sinomo_runtime::daemon::DaemonProbeStatus::Running(status) => status,
        sinomo_runtime::daemon::DaemonProbeStatus::Locked { .. } => {
            return Err(SinomoTestError::failed(
                "the daemon owns the routed-status data directory but its RPC service is unavailable",
            ));
        }
        sinomo_runtime::daemon::DaemonProbeStatus::NotRunning => {
            return Err(SinomoTestError::failed(
                "the routed-status data directory has no running main daemon",
            ));
        }
    };
    let Some(daemon_cluster) = daemon_status.cluster.as_ref() else {
        return Err(SinomoTestError::failed(
            "the routed-status daemon did not advertise a cluster identity",
        ));
    };
    if daemon_cluster.cluster_id != state.cluster_id
        || daemon_cluster.node_id != state.local_node_id
        || daemon_cluster.process != sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE
    {
        return Err(SinomoTestError::failed(format!(
            "the routed-status daemon advertised cluster/node/process {}/{}/{}, expected {}/{}/{}",
            daemon_cluster.cluster_id,
            daemon_cluster.node_id,
            daemon_cluster.process,
            state.cluster_id,
            state.local_node_id,
            sinomo_runtime::node::DAEMON_MAIN_CLUSTER_ROLE,
        )));
    }
    let daemon_client = DaemonApiClient::at_address(daemon_status.address)
        .with_timeout(Duration::from_millis(timeout_ms));
    let request = dfs::dfs_status_rpc_request()
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;

    let started = Instant::now();
    let deadline = started + timeout;
    let wall_gate = gate.for_bounded_target_count(targets.len());
    let mut attempts = 0usize;
    let mut failure_history = Vec::new();
    let mut dirty_status_history = Vec::new();
    ctx.record("timeout_ms", timeout_ms);
    ctx.record("max_attempts", gate.max_attempts);
    ctx.record("max_elapsed_ms", gate.max_elapsed.as_millis() as u64);
    ctx.record(
        "wall_max_elapsed_ms",
        wall_gate.max_elapsed.as_millis() as u64,
    );
    loop {
        attempts = attempts.saturating_add(1);
        let attempt = match probe_dfs_routed_status_targets(
            &daemon_client,
            &request,
            &targets,
            Duration::from_millis(timeout_ms),
            &unavailable_repair_targets,
        ) {
            Ok(probes) => aggregate_dfs_routed_status_probes(probes),
            Err(error) => DfsRoutedStatusAttempt {
                failures: vec![format!("probe worker failure: {error}")],
                ..DfsRoutedStatusAttempt::default()
            },
        };
        let DfsRoutedStatusAttempt {
            statuses,
            repair_details,
            bounded_omissions,
            blocking_incomplete_reasons,
            target_elapsed_ms,
            failures,
        } = attempt;
        ctx.record("target_elapsed_ms", &target_elapsed_ms);
        if !bounded_omissions.is_empty() {
            ctx.record("bounded_omissions", &bounded_omissions);
        }
        if !blocking_incomplete_reasons.is_empty() {
            ctx.record("blocking_incomplete_reasons", &blocking_incomplete_reasons);
        }
        let dirty_statuses = routed_status_dirty_summaries(
            &statuses,
            max_remote_pending_writes,
            max_remote_pending_repairs,
        );
        let elapsed = started.elapsed();
        if !failures.is_empty() {
            failure_history.extend(
                failures
                    .iter()
                    .map(|failure| format!("attempt {attempts}: {failure}")),
            );
        }
        if !dirty_statuses.is_empty() {
            dirty_status_history.extend(
                dirty_statuses
                    .iter()
                    .map(|dirty| format!("attempt {attempts}: {dirty}")),
            );
        }
        if failures.is_empty() && dirty_statuses.is_empty() {
            ctx.record("statuses", &statuses);
            ctx.record("attempts", attempts);
            ctx.record("elapsed_ms", elapsed.as_millis() as u64);
            if !failure_history.is_empty() {
                ctx.record("failure_history", &failure_history);
            }
            if !dirty_status_history.is_empty() {
                ctx.record("dirty_status_history", &dirty_status_history);
            }
            if failure_history.is_empty()
                && dirty_status_history.is_empty()
                && wall_gate.success_exceeded(attempts, elapsed)
            {
                return Ok(SinomoTestOutcome::failed(format!(
                    "DFS routed status succeeded in {}ms, exceeding the {}ms route/RPC gate",
                    elapsed.as_millis(),
                    wall_gate.max_elapsed.as_millis()
                )));
            }
            if !failure_history.is_empty() && wall_gate.success_exceeded(attempts, elapsed) {
                return Ok(SinomoTestOutcome::failed(format!(
                    "DFS routed status recovered from route/RPC failures after {attempts} attempt(s) in {}ms, exceeding the gate of {} attempt(s) in {}ms",
                    elapsed.as_millis(),
                    wall_gate.max_attempts,
                    wall_gate.max_elapsed.as_millis()
                )));
            }
            return Ok(SinomoTestOutcome::passed(format!(
                "DFS routed status succeeded for {} remote main daemon(s); {} target(s) reported bounded observation omissions",
                targets.len(),
                bounded_omissions.len()
            )));
        }
        if (!failures.is_empty() && wall_gate.exhausted(attempts, elapsed))
            || Instant::now() >= deadline
        {
            ctx.record("statuses", &statuses);
            ctx.record("attempts", attempts);
            ctx.record("elapsed_ms", elapsed.as_millis() as u64);
            if !repair_details.is_empty() {
                ctx.record("repair_details", &repair_details);
            }
            if !failures.is_empty() {
                ctx.record("failures", failures.clone());
                ctx.record("failure_history", &failure_history);
                return Ok(SinomoTestOutcome::failed(format!(
                    "DFS routed status failed for {}/{} targets",
                    failures.len(),
                    targets.len()
                )));
            }
            ctx.record("dirty_statuses", dirty_statuses.clone());
            ctx.record("dirty_status_history", &dirty_status_history);
            return Ok(SinomoTestOutcome::failed(format!(
                "DFS routed status found dirty status on {}/{} target(s)",
                dirty_statuses.len(),
                targets.len()
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

#[derive(Debug)]
struct DfsRoutedStatusTargetProbe {
    target: NodeId,
    elapsed_ms: u64,
    status: Option<BTreeMap<&'static str, u64>>,
    repair_details: Vec<BTreeMap<&'static str, String>>,
    bounded_omissions: Vec<String>,
    blocking_incomplete_reasons: Vec<String>,
    error: Option<String>,
}

impl DfsRoutedStatusTargetProbe {
    fn failed(target: NodeId, started: Instant, error: impl Into<String>) -> Self {
        Self {
            target,
            elapsed_ms: started.elapsed().as_millis() as u64,
            status: None,
            repair_details: Vec::new(),
            bounded_omissions: Vec::new(),
            blocking_incomplete_reasons: Vec::new(),
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Default)]
struct DfsRoutedStatusAttempt {
    statuses: BTreeMap<String, BTreeMap<&'static str, u64>>,
    repair_details: BTreeMap<String, Vec<BTreeMap<&'static str, String>>>,
    bounded_omissions: BTreeMap<String, Vec<String>>,
    blocking_incomplete_reasons: BTreeMap<String, Vec<String>>,
    target_elapsed_ms: BTreeMap<String, u64>,
    failures: Vec<String>,
}

fn probe_dfs_routed_status_targets(
    daemon_client: &DaemonApiClient,
    request: &ObjectRpcRequest,
    targets: &[NodeId],
    timeout: Duration,
    unavailable_repair_targets: &BTreeSet<NodeId>,
) -> Result<Vec<DfsRoutedStatusTargetProbe>, String> {
    run_bounded_dfs_routed_status_jobs(targets, MAX_ROUTED_STATUS_CONCURRENCY, &|target| {
        let panic_target = target.clone();
        let daemon_client = daemon_client.clone();
        let request = request.clone();
        let started = Instant::now();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            probe_dfs_routed_status_target(
                &daemon_client,
                &request,
                target,
                timeout,
                unavailable_repair_targets,
            )
        }))
        .unwrap_or_else(|_| {
            DfsRoutedStatusTargetProbe::failed(panic_target, started, "probe worker panicked")
        })
    })
}

fn probe_dfs_routed_status_target(
    daemon_client: &DaemonApiClient,
    request: &ObjectRpcRequest,
    target: NodeId,
    timeout: Duration,
    unavailable_repair_targets: &BTreeSet<NodeId>,
) -> DfsRoutedStatusTargetProbe {
    let started = Instant::now();
    let result = (|| -> Result<_, String> {
        let response = daemon_client
            .dfs_object_rpc_to_target_with_options(
                &target,
                request,
                dfs::DfsObjectRpcSendOptions::new(timeout)
                    .with_overall_timeout(timeout)
                    .with_observe_connectivity(true),
            )
            .map_err(|error| error.to_string())?;
        let status = dfs::decode_dfs_status_rpc_response(&response)
            .map_err(|error| format!("decode failed: {error}"))?;
        let repairs = if routed_status_needs_repair_views(&status, unavailable_repair_targets) {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err("repair inspection exceeded the target probe budget".to_string());
            }
            fetch_dfs_repair_views_for_status(
                daemon_client,
                &target,
                remaining,
                status.pending_repair_count,
            )
            .map_err(|error| format!("repair inspection failed: {error}"))?
        } else {
            Vec::new()
        };
        let summary = dfs_status_probe_summary(&status, &repairs, unavailable_repair_targets);
        let bounded_omissions = status
            .bounded_omissions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let mut blocking_incomplete_reasons = status
            .blocking_incomplete_reasons()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        if status.cross_shard_namespace_lock_count.is_none() {
            blocking_incomplete_reasons.push(DFS_CROSS_SHARD_LOCK_STATUS_UNOBSERVED.to_string());
        }
        let repair_details = repairs
            .iter()
            .map(dfs_repair_probe_summary)
            .collect::<Vec<_>>();
        Ok((
            summary,
            repair_details,
            bounded_omissions,
            blocking_incomplete_reasons,
        ))
    })();
    match result {
        Ok((status, repair_details, bounded_omissions, blocking_incomplete_reasons)) => {
            DfsRoutedStatusTargetProbe {
                target,
                elapsed_ms: started.elapsed().as_millis() as u64,
                status: Some(status),
                repair_details,
                bounded_omissions,
                blocking_incomplete_reasons,
                error: None,
            }
        }
        Err(error) => DfsRoutedStatusTargetProbe::failed(target, started, error),
    }
}

fn aggregate_dfs_routed_status_probes(
    probes: Vec<DfsRoutedStatusTargetProbe>,
) -> DfsRoutedStatusAttempt {
    let mut attempt = DfsRoutedStatusAttempt::default();
    for probe in probes {
        let target = probe.target.to_string();
        attempt
            .target_elapsed_ms
            .insert(target.clone(), probe.elapsed_ms);
        if let Some(error) = probe.error {
            attempt.failures.push(format!("{target}: {error}"));
            continue;
        }
        let Some(status) = probe.status else {
            attempt
                .failures
                .push(format!("{target}: probe returned no status"));
            continue;
        };
        attempt.statuses.insert(target.clone(), status);
        if !probe.repair_details.is_empty() {
            attempt
                .repair_details
                .insert(target.clone(), probe.repair_details);
        }
        if !probe.bounded_omissions.is_empty() {
            attempt
                .bounded_omissions
                .insert(target.clone(), probe.bounded_omissions);
        }
        if !probe.blocking_incomplete_reasons.is_empty() {
            attempt
                .blocking_incomplete_reasons
                .insert(target, probe.blocking_incomplete_reasons);
        }
    }
    attempt
}

fn run_bounded_dfs_routed_status_jobs<T, R, F>(
    items: &[T],
    max_concurrency: usize,
    probe: &F,
) -> Result<Vec<R>, String>
where
    T: Clone + Send + Sync,
    R: Send,
    F: Fn(T) -> R + Sync,
{
    if max_concurrency == 0 {
        return Err("probe concurrency must be greater than zero".to_string());
    }
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let next = AtomicUsize::new(0);
    let worker_count = items.len().min(max_concurrency);
    let mut indexed = std::thread::scope(|scope| -> Result<Vec<(usize, R)>, String> {
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let next = &next;
            workers.push(scope.spawn(move || {
                let mut results = Vec::new();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(item) = items.get(index) else {
                        break;
                    };
                    results.push((index, probe(item.clone())));
                }
                results
            }));
        }
        let mut indexed = Vec::with_capacity(items.len());
        for worker in workers {
            indexed.extend(
                worker
                    .join()
                    .map_err(|_| "bounded probe worker panicked".to_string())?,
            );
        }
        Ok(indexed)
    })?;
    if indexed.len() != items.len() {
        return Err(format!(
            "bounded probe completed {} of {} jobs",
            indexed.len(),
            items.len()
        ));
    }
    indexed.sort_by_key(|(index, _)| *index);
    Ok(indexed.into_iter().map(|(_, result)| result).collect())
}

fn run_remote_shard_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let timeout = expect_timeout(ctx)?;
    let split_diagnostics = optional_bool_parameter(ctx, "split-diagnostics", false)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let open_started = Instant::now();
    let backend = open_backend(ctx)?;
    let open_ms = open_started.elapsed().as_millis() as u64;
    let node = backend.node_label();
    let local_node_id = NodeId::new(node.clone()).map_err(|source| {
        SinomoTestError::failed(format!("local SIFS node id `{node}` is invalid: {source}"))
    })?;
    let status_started = Instant::now();
    let status = backend
        .operator_status()
        .map_err(SinomoTestError::failed)?
        .ok_or_else(|| {
            SinomoTestError::failed(
                "cluster-aware SIFS operator status is unavailable for remote-shard probe",
            )
        })?;
    let status_ms = status_started.elapsed().as_millis() as u64;
    let file_system_id =
        DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).map_err(|source| {
            SinomoTestError::failed(format!("default SIFS filesystem id is invalid: {source}"))
        })?;
    let shard_summaries = status
        .metadata_shards
        .iter()
        .map(|shard| {
            format!(
                "{} fs={} primary={} replicas={}",
                shard.shard_id.0,
                shard
                    .file_system_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<none>".to_string()),
                shard
                    .primary_node_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "<none>".to_string()),
                shard
                    .replica_node_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>();
    ctx.record("node", &node);
    ctx.record("metadata_shards", &shard_summaries);

    let Some(target) = status.metadata_shards.iter().find_map(|shard| {
        if shard.file_system_id.as_ref() != Some(&file_system_id) {
            return None;
        }
        let primary_node_id = shard.primary_node_id.as_ref()?;
        if primary_node_id == &local_node_id {
            return None;
        }
        let path_prefix = remote_shard_write_probe_prefix(&shard.shard_id.0, ctx.run_id())?;
        Some((
            shard.shard_id.0.clone(),
            primary_node_id.clone(),
            path_prefix,
            shard.epoch,
        ))
    }) else {
        return Ok(SinomoTestOutcome::skipped(format!(
            "node {node} owns every known SIFS metadata shard primary; remote-primary routing is not applicable on this node"
        )));
    };

    let nonce = probe_nonce();
    let _namespace = ScopedSifsTestNamespace::open(ctx, [target.2.clone()])?;
    let path = format!("{}/remote-shard-{nonce}.txt", target.2);
    let deferred_path = format!("{}/remote-shard-deferred-{nonce}.txt", target.2);
    let app_path = format!("{}/remote-shard-app-{nonce}.txt", target.2);
    let payload = ctx
        .parameter("payload")
        .map(|payload| payload.as_bytes().to_vec())
        .unwrap_or_else(|| {
            format!(
                "remote-shard-write node={node} shard={} primary={} nonce={nonce}",
                target.0, target.1
            )
            .into_bytes()
        });
    let write_started = Instant::now();
    // A cold remote authority may spend the full routed namespace-operation
    // budget hydrating its lane before it can commit the mutation. Keep each
    // diagnostic phase bounded, but do not undercut that production budget.
    let phase_timeout = remote_shard_write_phase_timeout(timeout);
    let next_phase_deadline = || std::cmp::min(Instant::now() + phase_timeout, deadline);
    let mut split_failures = Vec::new();
    if split_diagnostics {
        let create_parent_started = Instant::now();
        let create_parent_deadline = next_phase_deadline();
        retry_bulk_mutation(
            create_parent_deadline,
            || backend.create_dir_all_deferred_publish_until(&target.2, create_parent_deadline),
            || {
                format!(
                    "cannot create SIFS directory {} through remote metadata shard",
                    target.2
                )
            },
        )?;
        let create_parent_ms = create_parent_started.elapsed().as_millis() as u64;
        ctx.record("split_diagnostics", true);
        ctx.record("create_parent_ms", create_parent_ms);

        let app_write_started = Instant::now();
        let app_write_deadline = next_phase_deadline();
        let app_write_result = retry_bulk_mutation(
            app_write_deadline,
            || {
                backend.write_file_wait_for_commit_with_deadline(
                    &app_path,
                    &payload,
                    app_write_deadline,
                )
            },
            || format!("cannot app-write SIFS file {app_path} through remote metadata shard"),
        );
        let app_write_ms = app_write_started.elapsed().as_millis() as u64;
        ctx.record("app_path", &app_path);
        ctx.record("app_write_ms", app_write_ms);
        match app_write_result {
            Ok(()) => {
                let app_read_started = Instant::now();
                let app_read = backend
                    .read_file(&app_path)
                    .map_err(SinomoTestError::failed)?;
                let app_read_ms = app_read_started.elapsed().as_millis() as u64;
                ctx.record("app_read_ms", app_read_ms);
                if app_read != payload {
                    ctx.record("app_read_bytes", app_read.len());
                    split_failures.push(format!(
                        "remote-shard app write on node {node} read back {} bytes, expected {}",
                        app_read.len(),
                        payload.len()
                    ));
                }
            }
            Err(error) => {
                let error = error.to_string();
                ctx.record("app_write_error", &error);
                split_failures.push(error);
            }
        }

        let known_parent_write_started = Instant::now();
        let known_parent_write_deadline = next_phase_deadline();
        let known_parent_write_result = retry_bulk_mutation(
            known_parent_write_deadline,
            || {
                backend.write_file_with_known_parent_no_publish_until(
                    &path,
                    &payload,
                    known_parent_write_deadline,
                )
            },
            || format!("cannot write SIFS file {path} through remote metadata shard"),
        );
        let known_parent_write_ms = known_parent_write_started.elapsed().as_millis() as u64;
        ctx.record("known_parent_path", &path);
        ctx.record("known_parent_write_ms", known_parent_write_ms);
        if let Err(error) = known_parent_write_result {
            let error = error.to_string();
            ctx.record("known_parent_write_error", &error);
            split_failures.push(error);
        }

        let deferred_write_started = Instant::now();
        let deferred_write_deadline = next_phase_deadline();
        let deferred_write_result = retry_bulk_mutation(
            deferred_write_deadline,
            || {
                backend.write_file_with_known_parent_deferred_publish_until(
                    &deferred_path,
                    &payload,
                    deferred_write_deadline,
                )
            },
            || format!("cannot write SIFS file {deferred_path} through remote metadata shard"),
        );
        let deferred_write_ms = deferred_write_started.elapsed().as_millis() as u64;
        ctx.record("deferred_path", &deferred_path);
        ctx.record("deferred_write_ms", deferred_write_ms);
        if let Err(error) = deferred_write_result {
            let error = error.to_string();
            ctx.record("deferred_write_error", &error);
            split_failures.push(error);
        }

        let dispatch_started = Instant::now();
        let dispatch = backend.finish_pending_mutation_dispatch(Some(deadline));
        ctx.record(
            "pending_dispatch_ms",
            dispatch_started.elapsed().as_millis() as u64,
        );
        if let Err(error) = dispatch {
            ctx.record("pending_dispatch_error", error);
        }
    } else {
        let write_deadline = next_phase_deadline();
        retry_bulk_mutation(
            write_deadline,
            || backend.write_file_wait_for_commit_with_deadline(&path, &payload, write_deadline),
            || format!("cannot write SIFS file {path} through remote metadata shard"),
        )?;
    }
    let write_ms = write_started.elapsed().as_millis() as u64;
    let read_path = if split_diagnostics {
        app_path.as_str()
    } else {
        path.as_str()
    };
    if !split_failures.is_empty() {
        ctx.record("shard_id", &target.0);
        ctx.record("remote_primary_node_id", target.1.to_string());
        ctx.record("shard_epoch", target.3);
        ctx.record("path", read_path);
        ctx.record("bytes", payload.len());
        ctx.record("open_backend_ms", open_ms);
        ctx.record("operator_status_ms", status_ms);
        ctx.record("write_ms", write_ms);
        ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
        return Ok(SinomoTestOutcome::failed(format!(
            "remote-shard split diagnostics failed on node {node}: {}",
            split_failures.join("; ")
        )));
    }
    let read_started = Instant::now();
    let read = backend
        .read_file(read_path)
        .map_err(SinomoTestError::failed)?;
    let read_ms = read_started.elapsed().as_millis() as u64;
    if read != payload {
        ctx.record("read_bytes", read.len());
        return Ok(SinomoTestOutcome::failed(format!(
            "remote-shard write on node {node} read back {} bytes, expected {}",
            read.len(),
            payload.len()
        )));
    }

    ctx.record("shard_id", &target.0);
    ctx.record("remote_primary_node_id", target.1.to_string());
    ctx.record("shard_epoch", target.3);
    ctx.record("path", read_path);
    ctx.record("bytes", payload.len());
    ctx.record("open_backend_ms", open_ms);
    ctx.record("operator_status_ms", status_ms);
    ctx.record("write_ms", write_ms);
    ctx.record("read_ms", read_ms);
    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
    Ok(SinomoTestOutcome::passed(format!(
        "wrote {} bytes from node {node} through remote SIFS metadata shard {} on {}",
        payload.len(),
        target.0,
        target.1
    )))
}

#[derive(serde::Serialize)]
struct DfsShardLayoutProbe {
    lane: &'static str,
    base_prefix: &'static str,
    shard_id: String,
    file_system_id: String,
    primary_node_id: String,
    replica_node_ids: Vec<String>,
    epoch: u64,
    metadata_protocol_version: u8,
}

fn run_shard_layout(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    const PROBES: [(&str, &str, &str); 3] = [
        (
            "root",
            "/cluster-tests/dfs-perf",
            "/cluster-tests/dfs-perf/layout-probe",
        ),
        (
            "services",
            "/services/cluster-tests/dfs-perf",
            "/services/cluster-tests/dfs-perf/layout-probe",
        ),
        (
            "users",
            "/users/cluster-tests/dfs-perf",
            "/users/cluster-tests/dfs-perf/layout-probe",
        ),
    ];

    let expected_replica_count = optional_usize_parameter(ctx, "expected-replica-count", 0)?;
    let expected_distinct_primary_count =
        optional_usize_parameter(ctx, "expected-distinct-primary-count", 0)?;
    let authority = ctx.authoritative_cluster_snapshot().ok_or_else(|| {
        SinomoTestError::failed(
            "live SIFS shard layout requires one invocation-scoped authoritative cluster snapshot",
        )
    })?;
    let backend = open_observation(ctx)?;
    let node = backend.node_label();
    let mut layouts = Vec::with_capacity(PROBES.len());
    for (lane, base_prefix, candidate_path) in PROBES {
        let shard = backend
            .metadata_shard_for_path_at_authority(candidate_path, &authority)
            .map_err(SinomoTestError::failed)?
            .ok_or_else(|| {
                SinomoTestError::failed(format!(
                    "no live SIFS metadata shard controls {candidate_path} for the {lane} performance lane"
                ))
            })?;
        for member_id in std::iter::once(&shard.primary_node_id).chain(&shard.replica_node_ids) {
            let member = authority
                .snapshot()
                .get(&sinomo_runtime::node::cluster_member_key(member_id))
                .ok_or_else(|| {
                    SinomoTestError::failed(format!("metadata member {member_id} is absent"))
                })?
                .decode::<sinomo_runtime::core::ClusterMemberRecord>()
                .map_err(|error| SinomoTestError::failed(error.to_string()))?;
            if !member.capabilities.iter().any(|capability| {
                capability.as_str()
                    == sinomo_runtime::node::DFS_METADATA_PREPARED_OPERATION_V7_CAPABILITY
            }) {
                return Err(SinomoTestError::failed(format!(
                    "metadata member {member_id} must upgrade before the current protocol can be measured"
                )));
            }
        }
        layouts.push(DfsShardLayoutProbe {
            lane,
            base_prefix,
            shard_id: shard.shard_id.to_string(),
            file_system_id: shard.file_system_id.to_string(),
            primary_node_id: shard.primary_node_id.to_string(),
            replica_node_ids: shard
                .replica_node_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
            epoch: shard.epoch,
            metadata_protocol_version: dfs::DFS_PREPARED_OPERATION_PROTOCOL_VERSION,
        });
    }

    // Use the same admitted snapshot as the layout. The materialized KV file
    // can retain a completed handoff after the resident authority has advanced.
    let handoff_prefix = format!(
        "/dfs/filesystems/{}/metadata-membership-handoffs/",
        dfs::sifs_file_system_id(),
    );
    let snapshot = authority.snapshot();
    let pending_handoff_keys = snapshot
        .entries
        .iter()
        .filter(|entry| entry.key.starts_with(&handoff_prefix))
        .map(|entry| entry.key.as_str())
        .collect::<Vec<_>>();
    ctx.record("pending_handoff_keys", &pending_handoff_keys);
    ctx.record("authority_revision", snapshot.revision.0);

    let distinct_shard_ids = layouts
        .iter()
        .map(|layout| layout.shard_id.as_str())
        .collect::<BTreeSet<_>>();
    let distinct_primary_node_ids = layouts
        .iter()
        .map(|layout| layout.primary_node_id.as_str())
        .collect::<BTreeSet<_>>();
    ctx.record("node", &node);
    ctx.record("shards", &layouts);
    ctx.record("distinct_shard_count", distinct_shard_ids.len());
    ctx.record(
        "distinct_primary_node_count",
        distinct_primary_node_ids.len(),
    );
    if distinct_shard_ids.len() != layouts.len() {
        return Ok(SinomoTestOutcome::failed(format!(
            "live SIFS routing resolved {} performance lanes to only {} distinct metadata shards",
            layouts.len(),
            distinct_shard_ids.len()
        )));
    }
    if expected_replica_count > 0
        && layouts
            .iter()
            .any(|layout| layout.replica_node_ids.len() != expected_replica_count)
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "live SIFS metadata-shard layout has not converged to {expected_replica_count} replicas per shard: {}",
            layouts
                .iter()
                .map(|layout| format!("{}={}", layout.shard_id, layout.replica_node_ids.len()))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    if expected_distinct_primary_count > 0
        && distinct_primary_node_ids.len() != expected_distinct_primary_count
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "live SIFS metadata-shard layout has {} distinct primaries; expected {expected_distinct_primary_count}",
            distinct_primary_node_ids.len()
        )));
    }
    Ok(SinomoTestOutcome::passed(format!(
        "resolved {} live SIFS metadata-shard performance lanes from node {node}",
        layouts.len()
    )))
}

fn remote_shard_write_probe_prefix(shard_id: &str, run_id: &str) -> Option<String> {
    let root = match shard_id {
        "root" => "/cluster-tests/runs",
        "services" => "/services/cluster-tests/runs",
        "users" => "/users/cluster-tests/runs",
        _ => return None,
    };
    Some(format!("{root}/{run_id}/remote-shard-write"))
}

fn remote_shard_write_phase_timeout(timeout: Duration) -> Duration {
    timeout
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DfsRoutedStatusGate {
    max_attempts: usize,
    max_elapsed: Duration,
}

impl DfsRoutedStatusGate {
    fn from_context(ctx: &SinomoTestContext, timeout_ms: u64) -> Result<Self, SinomoTestError> {
        let max_attempts = optional_usize_parameter(ctx, "max-attempts", 1)?;
        require_positive("max-attempts", max_attempts)?;
        let max_elapsed_ms = optional_u64_parameter(ctx, "max-elapsed-ms")?.unwrap_or(timeout_ms);
        if max_elapsed_ms == 0 {
            return Err(SinomoTestError::InvalidParameter {
                name: "max-elapsed-ms".to_string(),
                detail: "must be greater than zero".to_string(),
            });
        }
        Ok(Self {
            max_attempts,
            max_elapsed: Duration::from_millis(max_elapsed_ms),
        })
    }

    fn exhausted(&self, attempts: usize, elapsed: Duration) -> bool {
        attempts >= self.max_attempts || elapsed >= self.max_elapsed
    }

    fn success_exceeded(&self, attempts: usize, elapsed: Duration) -> bool {
        attempts > self.max_attempts || elapsed > self.max_elapsed
    }

    fn for_bounded_target_count(self, target_count: usize) -> Self {
        let wave_count = target_count.max(1).div_ceil(MAX_ROUTED_STATUS_CONCURRENCY);
        let wave_count = u32::try_from(wave_count).unwrap_or(u32::MAX);
        Self {
            max_attempts: self.max_attempts,
            max_elapsed: self.max_elapsed.saturating_mul(wave_count),
        }
    }
}

fn dfs_member_available_for_route(
    cluster_paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    member: &sinomo_runtime::core::ClusterMemberRecord,
) -> bool {
    match network::cluster_member_main_daemon_connectivity(cluster_paths, state, member) {
        network::ClusterMemberMainDaemonConnectivity::Healthy
        | network::ClusterMemberMainDaemonConnectivity::Unknown => true,
        network::ClusterMemberMainDaemonConnectivity::Degraded
        | network::ClusterMemberMainDaemonConnectivity::Unreachable => {
            dfs_target_has_healthy_route(cluster_paths, state, &member.node_id)
        }
    }
}

fn dfs_target_has_healthy_route(
    cluster_paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    target: &NodeId,
) -> bool {
    let graph = network::ClusterConnectivityGraph::read_for_local(cluster_paths, state);
    network::cluster_healthy_route_next_hop(&graph, &state.local_node_id, target).is_some()
}

fn dfs_unavailable_repair_targets_from_state(
    cluster_paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
) -> BTreeSet<NodeId> {
    state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized()
                && !dfs_member_available_for_route(cluster_paths, state, member)
        })
        .map(|member| member.node_id.clone())
        .collect()
}

fn filter_dfs_routed_status_targets(
    ctx: &SinomoTestContext,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    targets: Vec<NodeId>,
) -> Result<Vec<NodeId>, SinomoTestError> {
    if ctx.parameter("target-node-id").is_some() && ctx.parameter("target-name").is_some() {
        return Err(SinomoTestError::InvalidParameter {
            name: "target-node-id".to_string(),
            detail: "cannot be combined with target-name".to_string(),
        });
    }

    let Some(target_node_id) = dfs_target_filter_node_id(ctx, state)? else {
        return Ok(targets);
    };
    let filtered = targets
        .into_iter()
        .filter(|target| target == &target_node_id)
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "target-node-id".to_string(),
            detail: format!("no routed DFS status target matched {target_node_id}"),
        });
    }
    Ok(filtered)
}

fn dfs_target_filter_node_id(
    ctx: &SinomoTestContext,
    state: &sinomo_runtime::core::ClusterBootstrapState,
) -> Result<Option<NodeId>, SinomoTestError> {
    if let Some(target_node_id) = ctx.parameter("target-node-id") {
        let target_node_id = target_node_id.trim();
        if target_node_id.is_empty() {
            return Err(SinomoTestError::InvalidParameter {
                name: "target-node-id".to_string(),
                detail: "must not be empty".to_string(),
            });
        }
        return target_node_id
            .parse::<NodeId>()
            .map(Some)
            .map_err(|source| SinomoTestError::InvalidParameter {
                name: "target-node-id".to_string(),
                detail: source.to_string(),
            });
    }

    let Some(target_name) = ctx.parameter("target-name") else {
        return Ok(None);
    };
    let target_name = target_name.trim();
    if target_name.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "target-name".to_string(),
            detail: "must not be empty".to_string(),
        });
    }
    let matches = state
        .members
        .iter()
        .filter(|member| member.display_name == target_name)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [member] => Ok(Some(member.node_id.clone())),
        [] => Err(SinomoTestError::InvalidParameter {
            name: "target-name".to_string(),
            detail: format!("no cluster member is named {target_name}"),
        }),
        _ => Err(SinomoTestError::InvalidParameter {
            name: "target-name".to_string(),
            detail: format!("multiple cluster members are named {target_name}"),
        }),
    }
}

fn fetch_dfs_repair_views_for_status(
    daemon_client: &DaemonApiClient,
    target: &NodeId,
    timeout: Duration,
    pending_repair_count: usize,
) -> Result<Vec<DfsOperatorRepairView>, String> {
    let limit = pending_repair_count.max(1);
    let request = dfs::dfs_inspection_rpc_request(&DaemonDfsInspectionRequest::Repair {
        file_system_id: None,
        object_id: None,
        limit: Some(limit),
        cursor: None,
    })
    .map_err(|error| error.to_string())?;
    let response = daemon_client
        .dfs_object_rpc_to_target_with_options(
            target,
            &request,
            dfs::DfsObjectRpcSendOptions::new(timeout)
                .with_overall_timeout(timeout)
                .with_observe_connectivity(true),
        )
        .map_err(|error| error.to_string())?;
    match dfs::decode_dfs_inspection_rpc_response(&response).map_err(|error| error.to_string())? {
        DaemonDfsInspectionResponse::Repair { repairs, .. } => Ok(repairs),
        other => Err(format!("unexpected DFS inspection response: {other:?}")),
    }
}

fn dfs_repair_probe_summary(repair: &DfsOperatorRepairView) -> BTreeMap<&'static str, String> {
    BTreeMap::from([
        ("intent_id", repair.intent_id.clone()),
        ("file_system_id", repair.file_system_id.to_string()),
        ("object_id", repair.object_id.to_string()),
        ("version", repair.version.to_string()),
        (
            "content_hash",
            repair.content_hash.clone().unwrap_or_default(),
        ),
        (
            "missing_replica_node_id",
            repair.missing_replica_node_id.to_string(),
        ),
    ])
}

const DFS_CROSS_SHARD_LOCK_STATUS_UNOBSERVED: &str =
    "durable cross-shard namespace lock status was not observed";

fn dfs_status_probe_summary(
    status: &DfsOperatorStatus,
    repairs: &[DfsOperatorRepairView],
    unavailable_repair_targets: &BTreeSet<NodeId>,
) -> BTreeMap<&'static str, u64> {
    let unavailable_pending_repairs =
        dfs_unavailable_pending_repair_count(status, repairs, unavailable_repair_targets);
    let active_pending_repairs = status
        .pending_repair_count
        .saturating_sub(unavailable_pending_repairs);
    BTreeMap::from([
        (
            "incomplete_reasons",
            status
                .blocking_incomplete_reasons()
                .len()
                .saturating_add(usize::from(
                    status.cross_shard_namespace_lock_count.is_none(),
                )) as u64,
        ),
        ("bounded_omissions", status.bounded_omissions.len() as u64),
        ("file_systems", status.file_system_count as u64),
        ("objects", status.object_count as u64),
        ("pending_writes", status.pending_write_count as u64),
        ("pending_repairs", active_pending_repairs as u64),
        ("total_pending_repairs", status.pending_repair_count as u64),
        (
            "unavailable_pending_repairs",
            unavailable_pending_repairs as u64,
        ),
    ])
}

fn dfs_unavailable_pending_repair_count(
    status: &DfsOperatorStatus,
    repairs: &[DfsOperatorRepairView],
    unavailable_repair_targets: &BTreeSet<NodeId>,
) -> usize {
    if status.pending_repair_count == 0 || unavailable_repair_targets.is_empty() {
        return 0;
    }
    let mut unavailable_repairs = status
        .repair_attempts
        .iter()
        .filter(|attempt| unavailable_repair_targets.contains(&attempt.missing_replica_node_id))
        .map(|attempt| {
            (
                attempt.intent_id.clone(),
                attempt.missing_replica_node_id.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    unavailable_repairs.extend(
        repairs
            .iter()
            .filter(|repair| unavailable_repair_targets.contains(&repair.missing_replica_node_id))
            .map(|repair| {
                (
                    repair.intent_id.clone(),
                    repair.missing_replica_node_id.clone(),
                )
            }),
    );
    unavailable_repairs.len().min(status.pending_repair_count)
}

fn routed_status_needs_repair_views(
    status: &DfsOperatorStatus,
    unavailable_repair_targets: &BTreeSet<NodeId>,
) -> bool {
    status.pending_repair_count > 0 && !unavailable_repair_targets.is_empty()
}

fn run_service_advertisements(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let expected_nodes = optional_usize_parameter(ctx, "expected-nodes", 1)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before DFS service advertisement check: {error}"
        ))
    })?;

    let metadata = read_dfs_service_advertisements(&cluster_paths, dfs::metadata_services_prefix())
        .map_err(SinomoTestError::failed)?;
    let content = read_dfs_service_advertisements(&cluster_paths, dfs::content_services_prefix())
        .map_err(SinomoTestError::failed)?;
    let bad = metadata
        .iter()
        .chain(content.iter())
        .filter(|&advertisement| advertisement.node_id.as_str().is_empty())
        .map(|advertisement| format!("{} has empty node id", advertisement.node_id))
        .collect::<Vec<_>>();

    ctx.record("metadata_count", metadata.len());
    ctx.record("content_count", content.len());
    ctx.record(
        "metadata_nodes",
        metadata
            .iter()
            .map(dfs_service_advertisement_summary)
            .collect::<Vec<_>>(),
    );
    ctx.record(
        "content_nodes",
        content
            .iter()
            .map(dfs_service_advertisement_summary)
            .collect::<Vec<_>>(),
    );

    if !bad.is_empty() {
        ctx.record("bad_advertisements", &bad);
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS service advertisements contain {} invalid node advertisement(s)",
            bad.len()
        )));
    }
    if metadata.len() < expected_nodes || content.len() < expected_nodes {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS service advertisements saw metadata={} content={}, expected at least {expected_nodes} each",
            metadata.len(),
            content.len()
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "DFS service advertisements are node-routed for {} metadata and {} content node(s)",
        metadata.len(),
        content.len()
    )))
}

fn run_placement_management(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let timeout = if ctx.parameter("timeout-seconds").is_some() {
        expect_timeout(ctx)?
    } else {
        Duration::from_secs(DEFAULT_PLACEMENT_EXPECT_TIMEOUT_SECONDS)
    };
    let requested_min_zones = optional_usize_parameter(ctx, "min-zones", 0)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before DFS placement check: {error}"
        ))
    })?;
    let mut content =
        read_dfs_service_advertisements(&cluster_paths, dfs::content_services_prefix())
            .map_err(SinomoTestError::failed)?;
    content.retain(|advertisement| advertisement.accepts_content);
    content.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    content.dedup_by(|left, right| left.node_id == right.node_id);
    if content.len() < 4 {
        return Ok(SinomoTestOutcome::failed(format!(
            "placement constraints require at least four content nodes, found {}",
            content.len()
        )));
    }
    let available_zones = content
        .iter()
        .filter_map(|advertisement| advertisement.zone.clone())
        .collect::<BTreeSet<_>>();
    let min_zones = if requested_min_zones == 0 {
        available_zones.len().min(3)
    } else {
        requested_min_zones
    };
    if available_zones.len() < min_zones {
        return Ok(SinomoTestOutcome::failed(format!(
            "placement check requires {min_zones} zone(s), but advertisements expose only {}: {}",
            available_zones.len(),
            available_zones
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let required_node_id = content[0].node_id.clone();
    let forbidden_node_id = content
        .iter()
        .rev()
        .find(|advertisement| advertisement.node_id != required_node_id)
        .map(|advertisement| advertisement.node_id.clone())
        .ok_or_else(|| SinomoTestError::failed("cannot choose distinct placement constraints"))?;
    let nonce = probe_nonce();
    let namespace = scoped_dfs_test_base(ctx, "placement-management")?;
    let _namespace = ScopedSifsTestNamespace::open(ctx, [namespace.clone()])?;
    let base = format!("{namespace}/placement-{nonce}");
    let inherited_path = format!("{base}/inherited.txt");
    let override_path = format!("{base}/override.txt");
    let browser = open_local_daemon_sifs_browser(ctx)?;
    ctx.record("node", browser.node_id());
    ctx.record("path", &base);
    ctx.record("content_nodes", content.len());
    ctx.record("available_zones", &available_zones);
    ctx.record("required_node_id", required_node_id.to_string());
    ctx.record("forbidden_node_id", forbidden_node_id.to_string());
    ctx.record("min_zones", min_zones);

    browser
        .create_dir_all(&base)
        .map_err(|error| placement_test_error("create test directory", error))?;
    let inherited_body = format!("placement inheritance probe {nonce}");
    let override_body = format!("placement self override probe {nonce}");
    browser
        .write_file(&inherited_path, inherited_body.as_bytes())
        .map_err(|error| placement_test_error("write inherited test file", error))?;
    browser
        .write_file(&override_path, override_body.as_bytes())
        .map_err(|error| placement_test_error("write override test file", error))?;

    let initial_inherited = browser
        .stat(&inherited_path)
        .map_err(|error| placement_test_error("stat existing test file", error))?;
    ctx.record(
        "initial_replica_count",
        initial_inherited.current_locations.len(),
    );

    let mut eventual = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, 1, 3)
        .map_err(|error| placement_test_error("construct eventual placement policy", error))?;
    eventual.required_node_ids = vec![required_node_id.clone()];
    eventual.forbidden_node_ids = vec![forbidden_node_id.clone()];
    eventual.min_distinct_zones = min_zones;
    eventual
        .validate()
        .map_err(|error| placement_test_error("validate eventual placement policy", error))?;

    let preview = browser
        .preview_placement(&base, Some(eventual.clone()), DfsPlacementScope::Subtree)
        .map_err(|error| placement_test_error("preview subtree placement", error))?;
    ctx.record("subtree_preview", &preview);
    let desired = &preview.placement.plan.desired_replica_nodes;
    if desired.len() != 3
        || !desired.contains(&required_node_id)
        || desired.contains(&forbidden_node_id)
        || preview.placement.represented_zone_count < min_zones
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "subtree preview violated N/required/forbidden/zone constraints: desired={desired:?}, zones={}",
            preview.placement.represented_zone_count
        )));
    }
    if preview.warnings.is_empty() {
        return Ok(SinomoTestOutcome::failed(
            "replicated-eventual W=1/N=3 preview did not explain asynchronous repair",
        ));
    }

    let subtree_update = browser
        .update_placement(
            &base,
            Some(eventual.clone()),
            DfsPlacementScope::Subtree,
            preview.preview_token.clone(),
        )
        .map_err(|error| placement_test_error("apply subtree placement", error))?;
    ctx.record("subtree_job_id", &subtree_update.reconciliation.job_id);
    // Reopen the Files-facing daemon client before observing the job. The job
    // must be recoverable by id from cluster KV, not retained in client memory.
    let progress_browser = open_local_daemon_sifs_browser(ctx)?;
    let (subtree_job, subtree_states) = wait_for_placement_job(
        &progress_browser,
        &subtree_update.reconciliation.job_id,
        timeout,
    )
    .map_err(|error| placement_test_error("reconcile subtree placement", error))?;
    ctx.record("subtree_progress_states", &subtree_states);
    ctx.record("subtree_final_job", &subtree_job);
    if subtree_job.matched_object_count < 3 || subtree_job.updated_object_count < 2 {
        return Ok(SinomoTestOutcome::failed(format!(
            "subtree reconciliation did not account for existing objects: matched={}, updated={}",
            subtree_job.matched_object_count, subtree_job.updated_object_count
        )));
    }

    let inherited_details = browser
        .stat(&inherited_path)
        .map_err(|error| placement_test_error("stat inherited placement", error))?;
    let inherited_placement = inherited_details
        .placement
        .as_ref()
        .ok_or_else(|| SinomoTestError::failed("inherited file stat omitted resolved placement"))?;
    if inherited_placement.policy != eventual
        || inherited_placement.source_path.as_deref() != Some(base.as_str())
        || inherited_placement.source_scope != Some(DfsPlacementScope::Subtree)
        || !inherited_placement.inherited
        || inherited_details.current_locations.len() != 3
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "existing file did not converge to inherited subtree placement: {inherited_details:?}"
        )));
    }

    let confirmed = DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedConfirmed, 2, 2)
        .map_err(|error| placement_test_error("construct confirmed placement policy", error))?;
    let confirmed_preview = browser
        .preview_placement(
            &override_path,
            Some(confirmed.clone()),
            DfsPlacementScope::SelfOnly,
        )
        .map_err(|error| placement_test_error("preview self placement", error))?;
    ctx.record("self_preview", &confirmed_preview);
    if confirmed_preview.resolved.policy != confirmed
        || confirmed_preview.resolved.source_path.as_deref() != Some(override_path.as_str())
        || confirmed_preview.resolved.source_scope != Some(DfsPlacementScope::SelfOnly)
        || confirmed_preview.resolved.inherited
    {
        return Ok(SinomoTestOutcome::failed(
            "self preview did not override the inherited subtree rule",
        ));
    }
    let confirmed_update = browser
        .update_placement(
            &override_path,
            Some(confirmed.clone()),
            DfsPlacementScope::SelfOnly,
            confirmed_preview.preview_token.clone(),
        )
        .map_err(|error| placement_test_error("apply self placement", error))?;
    let (confirmed_job, confirmed_states) = wait_for_placement_job(
        &progress_browser,
        &confirmed_update.reconciliation.job_id,
        timeout,
    )
    .map_err(|error| placement_test_error("reconcile self placement", error))?;
    ctx.record("self_progress_states", &confirmed_states);
    ctx.record("self_final_job", &confirmed_job);

    let override_details = browser
        .stat(&override_path)
        .map_err(|error| placement_test_error("stat self placement", error))?;
    let override_placement = override_details
        .placement
        .as_ref()
        .ok_or_else(|| SinomoTestError::failed("self-overridden file omitted placement"))?;
    let sibling_details = browser
        .stat(&inherited_path)
        .map_err(|error| placement_test_error("stat inherited sibling", error))?;
    if override_placement.policy != confirmed
        || override_details.current_locations.len() != 2
        || sibling_details
            .placement
            .as_ref()
            .map(|placement| &placement.policy)
            != Some(&eventual)
    {
        return Ok(SinomoTestOutcome::failed(
            "self-only confirmed placement leaked to its inherited sibling or did not converge",
        ));
    }

    let removal_preview = browser
        .preview_placement(&override_path, None, DfsPlacementScope::SelfOnly)
        .map_err(|error| placement_test_error("preview self-rule removal", error))?;
    ctx.record("self_removal_preview", &removal_preview);
    if removal_preview.resolved.policy != eventual
        || removal_preview.resolved.source_path.as_deref() != Some(base.as_str())
        || !removal_preview.resolved.inherited
    {
        return Ok(SinomoTestOutcome::failed(
            "removing the self-only rule did not preview restoration of subtree inheritance",
        ));
    }
    let removal_update = browser
        .update_placement(
            &override_path,
            None,
            DfsPlacementScope::SelfOnly,
            removal_preview.preview_token.clone(),
        )
        .map_err(|error| placement_test_error("remove self placement", error))?;
    let (removal_job, removal_states) = wait_for_placement_job(
        &progress_browser,
        &removal_update.reconciliation.job_id,
        timeout,
    )
    .map_err(|error| placement_test_error("reconcile self-rule removal", error))?;
    ctx.record("self_removal_progress_states", &removal_states);
    ctx.record("self_removal_final_job", &removal_job);
    let restored_details = browser
        .stat(&override_path)
        .map_err(|error| placement_test_error("stat restored inheritance", error))?;
    if restored_details
        .placement
        .as_ref()
        .is_none_or(|placement| placement.policy != eventual || !placement.inherited)
        || restored_details.current_locations.len() != 3
    {
        return Ok(SinomoTestOutcome::failed(
            "self-rule removal did not durably restore the inherited N=3 placement",
        ));
    }

    let subtree_removal_preview = browser
        .preview_placement(&base, None, DfsPlacementScope::Subtree)
        .map_err(|error| placement_test_error("preview subtree placement removal", error))?;
    let subtree_removal = browser
        .update_placement(
            &base,
            None,
            DfsPlacementScope::Subtree,
            subtree_removal_preview.preview_token,
        )
        .map_err(|error| placement_test_error("remove subtree placement", error))?;
    let (_, cleanup_states) = wait_for_placement_job(
        &progress_browser,
        &subtree_removal.reconciliation.job_id,
        timeout,
    )
    .map_err(|error| placement_test_error("reconcile subtree-rule removal", error))?;
    ctx.record("cleanup_progress_states", &cleanup_states);
    browser
        .delete(&inherited_path, false)
        .map_err(|error| placement_test_error("delete inherited test file", error))?;
    browser
        .delete(&override_path, false)
        .map_err(|error| placement_test_error("delete override test file", error))?;
    browser
        .delete(&base, true)
        .map_err(|error| placement_test_error("delete placement test directory", error))?;

    Ok(SinomoTestOutcome::passed(format!(
        "placement preview and durable reconciliation enforced eventual W=1/N=3 across {min_zones} zone(s), required/forbidden nodes, confirmed W=N=2, and self/subtree inheritance"
    )))
}

fn open_local_daemon_sifs_browser(ctx: &SinomoTestContext) -> Result<SifsBrowser, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let paths = sinomo_runtime::daemon::DaemonPaths::new(&data_dir);
    let status = sinomo_runtime::daemon::read_daemon_status(&paths).map_err(|error| {
        SinomoTestError::failed(format!("cannot read local daemon status: {error}"))
    })?;
    SifsBrowser::open_daemon(status.address).map_err(|error| {
        SinomoTestError::failed(format!("cannot open daemon SIFS browser: {error}"))
    })
}

fn wait_for_placement_job(
    browser: &SifsBrowser,
    job_id: &str,
    timeout: Duration,
) -> Result<(dfs::DfsPlacementReconciliationJob, Vec<String>), String> {
    let deadline = Instant::now() + timeout;
    let mut states = Vec::new();
    loop {
        let job = browser.placement_progress(job_id)?;
        let state = format!("{:?}", job.state);
        if states.last() != Some(&state) {
            states.push(state);
        }
        if job.state == DfsPlacementReconciliationState::Completed {
            return Ok((job, states));
        }
        if job.state == DfsPlacementReconciliationState::Failed {
            return Err(format!(
                "placement job {job_id} failed: {}",
                job.last_error.as_deref().unwrap_or("no detail")
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "placement job {job_id} did not complete within {}s; state={:?}, scanned={}, repairs={}/{}, last_error={}",
                timeout.as_secs(),
                job.state,
                job.scanned_object_count,
                job.completed_repair_count,
                job.scheduled_repair_count,
                job.last_error.as_deref().unwrap_or("none")
            ));
        }
        let _ = browser.request_maintenance_refresh();
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn placement_test_error(operation: &str, error: impl std::fmt::Display) -> SinomoTestError {
    SinomoTestError::failed(format!("cannot {operation}: {error}"))
}

fn read_dfs_service_advertisements(
    cluster_paths: &ClusterPaths,
    prefix: &str,
) -> Result<Vec<dfs::DfsNodeAdvertisement>, String> {
    let mut advertisements: Vec<dfs::DfsNodeAdvertisement> = Vec::new();
    for entry in
        sinomo_runtime::kv::read_cluster_kv_prefix_entries(&cluster_paths.kv_paths(), prefix)
            .map_err(|error| error.to_string())?
    {
        advertisements.push(entry.decode().map_err(|error| {
            format!(
                "cannot decode DFS service advertisement {}: {error}",
                entry.key
            )
        })?);
    }
    advertisements.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(advertisements)
}

fn dfs_service_advertisement_summary(
    advertisement: &dfs::DfsNodeAdvertisement,
) -> BTreeMap<&'static str, String> {
    BTreeMap::from([
        ("node_id", advertisement.node_id.to_string()),
        (
            "accepts_metadata",
            advertisement.accepts_metadata.to_string(),
        ),
        ("accepts_content", advertisement.accepts_content.to_string()),
    ])
}

fn routed_status_dirty_summaries(
    statuses: &BTreeMap<String, BTreeMap<&'static str, u64>>,
    max_remote_pending_writes: usize,
    max_remote_pending_repairs: usize,
) -> Vec<String> {
    statuses
        .iter()
        .filter_map(|(target, status)| {
            let incomplete_reasons = *status.get("incomplete_reasons").unwrap_or(&0);
            let bounded_omissions = *status.get("bounded_omissions").unwrap_or(&0);
            let pending_writes = *status.get("pending_writes").unwrap_or(&0);
            let pending_repairs = *status.get("pending_repairs").unwrap_or(&0);
            if incomplete_reasons > 0
                || pending_writes > max_remote_pending_writes as u64
                || pending_repairs > max_remote_pending_repairs as u64
            {
                let total_pending_repairs = status
                    .get("total_pending_repairs")
                    .copied()
                    .unwrap_or(pending_repairs);
                let unavailable_pending_repairs = status
                    .get("unavailable_pending_repairs")
                    .copied()
                    .unwrap_or(0);
                Some(format!(
                    "{target}: incomplete_reasons={incomplete_reasons}, bounded_omissions={bounded_omissions}, pending_writes={pending_writes}, pending_repairs={pending_repairs}, total_pending_repairs={total_pending_repairs}, unavailable_pending_repairs={unavailable_pending_repairs}"
                ))
            } else {
                None
            }
        })
        .collect()
}

fn run_concurrent_file_race(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = scoped_dfs_test_base(ctx, "concurrent-file-race")?;
    let _namespace = ScopedSifsTestNamespace::open(ctx, [base.clone()])?;
    let timeout = expect_timeout(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let nonce = probe_nonce();
    let path = format!("{base}/file-race-{nonce}.bin");
    let seed = vec![b'A'; 4096];
    let writer_a = vec![b'B'; 4096];
    let writer_b = vec![b'C'; 4096];
    backend
        .create_dir_all(&base)
        .map_err(SinomoTestError::failed)?;
    backend
        .write_file_wait_for_commit_with_deadline(&path, &seed, Instant::now() + timeout)
        .map_err(SinomoTestError::failed)?;
    ctx.record("node", &node);
    ctx.record("path", &path);

    let started = Instant::now();
    let deadline = started + timeout;
    let run_writer = |label: &'static str,
                      data_dir: SinomoDataDir,
                      path: String,
                      bytes: Vec<u8>,
                      deadline: Instant| {
        std::thread::spawn(move || {
            let started = Instant::now();
            let result = SifsFileManagerBackend::open(&data_dir).and_then(|backend| {
                backend.write_file_wait_for_commit_with_deadline(&path, &bytes, deadline)
            });
            (
                label,
                result.map(|_| ()).map_err(|error| error.to_string()),
                started.elapsed().as_millis() as u64,
            )
        })
    };
    let thread_a = run_writer(
        "writer-a",
        data_dir.clone(),
        path.clone(),
        writer_a.clone(),
        deadline,
    );
    let thread_b = run_writer(
        "writer-b",
        data_dir.clone(),
        path.clone(),
        writer_b.clone(),
        deadline,
    );
    let results = [
        thread_a
            .join()
            .map_err(|_| SinomoTestError::failed("writer-a thread panicked"))?,
        thread_b
            .join()
            .map_err(|_| SinomoTestError::failed("writer-b thread panicked"))?,
    ];
    ctx.record(
        "operation_results",
        results
            .iter()
            .map(|(label, result, elapsed_ms)| {
                format!("{label}:{result:?}:elapsed_ms={elapsed_ms}")
            })
            .collect::<Vec<_>>(),
    );
    let successful_writers = results
        .iter()
        .filter(|(_, result, _)| result.is_ok())
        .count();
    let writer_errors = results
        .iter()
        .filter_map(|(label, result, _)| {
            result
                .as_ref()
                .err()
                .map(|error| format!("{label}: {error}"))
        })
        .collect::<Vec<_>>();
    ctx.record("successful_writers", successful_writers);
    if !writer_errors.is_empty() {
        ctx.record("writer_errors", writer_errors.clone());
        return Ok(SinomoTestOutcome::failed(format!(
            "concurrent file race on {node} hit writer errors at {path}"
        )));
    }
    if successful_writers != results.len() {
        return Ok(SinomoTestOutcome::failed(format!(
            "concurrent file race on {node} committed {successful_writers}/{} writers at {path}",
            results.len()
        )));
    }

    let deadline = started + timeout;
    let valid = [&writer_a, &writer_b];
    loop {
        let read = backend.read_file(&path).map_err(SinomoTestError::failed)?;
        let read_class = classify_file_race_payload(&read, &seed, &writer_a, &writer_b);
        if valid
            .iter()
            .any(|candidate| candidate.as_slice() == read.as_slice())
        {
            ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
            ctx.record("final_bytes", read.len());
            ctx.record("final_payload", read_class);
            return Ok(SinomoTestOutcome::passed(format!(
                "concurrent file race on {node} ended with one complete payload at {path}"
            )));
        }
        if Instant::now() >= deadline {
            ctx.record("last_read_bytes", read.len());
            ctx.record("last_read_payload", read_class);
            ctx.record("last_read_profile", file_race_payload_profile(&read));
            if let Ok(fresh_backend) = SifsFileManagerBackend::open(&data_dir)
                && let Ok(fresh_read) = fresh_backend.read_file(&path)
            {
                ctx.record("fresh_read_bytes", fresh_read.len());
                ctx.record(
                    "fresh_read_payload",
                    classify_file_race_payload(&fresh_read, &seed, &writer_a, &writer_b),
                );
                ctx.record("fresh_read_profile", file_race_payload_profile(&fresh_read));
            }
            return Ok(SinomoTestOutcome::failed(format!(
                "concurrent file race on {node} produced a non-serialized payload at {path}"
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn classify_file_race_payload(
    read: &[u8],
    seed: &[u8],
    writer_a: &[u8],
    writer_b: &[u8],
) -> &'static str {
    if read == seed {
        "seed"
    } else if read == writer_a {
        "writer-a"
    } else if read == writer_b {
        "writer-b"
    } else {
        "mixed"
    }
}

fn file_race_payload_profile(read: &[u8]) -> String {
    let mut seed_bytes = 0usize;
    let mut writer_a_bytes = 0usize;
    let mut writer_b_bytes = 0usize;
    let mut other_bytes = 0usize;
    for byte in read {
        match *byte {
            b'A' => seed_bytes += 1,
            b'B' => writer_a_bytes += 1,
            b'C' => writer_b_bytes += 1,
            _ => other_bytes += 1,
        }
    }
    format!(
        "A={seed_bytes},B={writer_a_bytes},C={writer_b_bytes},other={other_bytes},len={}",
        read.len()
    )
}

fn run_concurrent_dir_race(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = scoped_dfs_test_base(ctx, "concurrent-dir-race")?;
    let _namespace = ScopedSifsTestNamespace::open(ctx, [base.clone()])?;
    let timeout = expect_timeout(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let nonce = probe_nonce();
    let dir = format!("{base}/dir-race-{nonce}");
    let child = format!("{dir}/child.txt");
    backend
        .create_dir_all(&base)
        .map_err(SinomoTestError::failed)?;
    backend.create_dir(&dir).map_err(SinomoTestError::failed)?;
    ctx.record("node", &node);
    ctx.record("dir", &dir);
    ctx.record("child", &child);

    let create_dir = data_dir.clone();
    let create_child = child.clone();
    let create_deadline = Instant::now() + timeout;
    let create_thread = std::thread::spawn(move || {
        let started = Instant::now();
        let result = SifsFileManagerBackend::open(&create_dir).and_then(|backend| {
            backend.write_file_with_existing_parent_wait_for_commit_with_deadline(
                &create_child,
                b"child",
                create_deadline,
            )
        });
        (
            "child-create",
            result.map(|_| ()).map_err(|error| error.to_string()),
            started.elapsed().as_millis() as u64,
        )
    });
    let remove_dir = data_dir.clone();
    let remove_path = dir.clone();
    let remove_thread = std::thread::spawn(move || {
        let started = Instant::now();
        let result = SifsFileManagerBackend::open(&remove_dir)
            .and_then(|backend| backend.delete_dir(&remove_path));
        (
            "rmdir",
            result.map(|_| ()).map_err(|error| error.to_string()),
            started.elapsed().as_millis() as u64,
        )
    });
    let results = [
        create_thread
            .join()
            .map_err(|_| SinomoTestError::failed("child-create thread panicked"))?,
        remove_thread
            .join()
            .map_err(|_| SinomoTestError::failed("rmdir thread panicked"))?,
    ];
    ctx.record(
        "operation_results",
        results
            .iter()
            .map(|(label, result, elapsed_ms)| {
                format!("{label}:{result:?}:elapsed_ms={elapsed_ms}")
            })
            .collect::<Vec<_>>(),
    );

    let dir_present =
        dfs_stat_reports_path_present(&dir, backend.stat(&dir)).map_err(SinomoTestError::failed)?;
    let child_present = dfs_stat_reports_path_present(&child, backend.stat(&child))
        .map_err(SinomoTestError::failed)?;
    ctx.record("dir_present", dir_present);
    ctx.record("child_present", child_present);
    if (dir_present && child_present) || (!dir_present && !child_present) {
        Ok(SinomoTestOutcome::passed(format!(
            "concurrent dir race on {node} left no orphan child under {dir}"
        )))
    } else {
        Ok(SinomoTestOutcome::failed(format!(
            "concurrent dir race on {node} left inconsistent state: dir_present={dir_present}, child_present={child_present}"
        )))
    }
}

fn run_failover_current_read(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = scoped_dfs_test_base(ctx, "failover-current-read")?;
    let _namespace = ScopedSifsTestNamespace::open(ctx, [base.clone()])?;
    let timeout = expect_timeout(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let nonce = probe_nonce();
    let path = format!("{base}/current-read-{nonce}.txt");
    let old = format!("old current-read payload {nonce}");
    let new = format!("new current-read payload {nonce} refreshed");
    backend
        .write_file_wait_for_commit_with_deadline(&path, old.as_bytes(), Instant::now() + timeout)
        .map_err(SinomoTestError::failed)?;
    let cached_reader = SifsFileManagerBackend::open_read_only(&data_dir).map_err(|error| {
        SinomoTestError::failed(format!("cannot open cached reader backend: {error}"))
    })?;
    let first = cached_reader
        .read_file(&path)
        .map_err(SinomoTestError::failed)?;
    if first != old.as_bytes() {
        return Ok(SinomoTestOutcome::failed(format!(
            "initial current read on {node} returned unexpected bytes at {path}"
        )));
    }
    backend
        .write_file_wait_for_commit_with_deadline(&path, new.as_bytes(), Instant::now() + timeout)
        .map_err(SinomoTestError::failed)?;

    let started = Instant::now();
    let deadline = started + timeout;
    let mut observations = Vec::new();
    loop {
        let read = cached_reader
            .read_file(&path)
            .map_err(SinomoTestError::failed)?;
        let payload = if read == old.as_bytes() {
            "old"
        } else if read == new.as_bytes() {
            "new"
        } else {
            "other"
        };
        observations.push(format!(
            "elapsed_ms={}:bytes={}:payload={}",
            started.elapsed().as_millis(),
            read.len(),
            payload
        ));
        if read == new.as_bytes() {
            ctx.record("node", &node);
            ctx.record("path", &path);
            ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
            ctx.record("observations", observations);
            return Ok(SinomoTestOutcome::passed(format!(
                "current read on {node} refreshed to the latest visible version at {path}"
            )));
        }
        if Instant::now() >= deadline {
            ctx.record("node", &node);
            ctx.record("path", &path);
            ctx.record("observations", observations);
            return Ok(SinomoTestOutcome::failed(format!(
                "current read on {node} did not refresh to the latest visible version at {path} within {}s",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn run_protocol_codec_baseline(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_PROTOCOL_CODEC_BYTES)?;
    let iterations =
        optional_usize_parameter(ctx, "iterations", DEFAULT_PROTOCOL_CODEC_ITERATIONS)?;
    let min_encode_attachment_bytes_described_per_second =
        optional_f64_parameter(ctx, "min-encode-bytes-per-second", 0.0)?;
    let min_decode_attachment_bytes_described_per_second =
        optional_f64_parameter(ctx, "min-decode-bytes-per-second", 0.0)?;
    require_positive("bytes", bytes)?;
    require_positive("iterations", iterations)?;

    let payload = bulk_payload("dfs-protocol-codec", 0, bytes);
    let payload_hash = format!("blake3:{}", blake3::hash(&payload).to_hex());
    let samples = dfs_protocol_codec_samples(&payload);
    let encoded_samples = samples
        .iter()
        .map(|sample| {
            let request = encode_dfs_protocol_codec_head(sample)?;
            Ok(EncodedDfsProtocolCodecSample {
                label: sample.label,
                encoded_head_bytes: request.body.len(),
                request,
            })
        })
        .collect::<Result<Vec<_>, SinomoTestError>>()?;
    let messages_per_iteration = encoded_samples.len();
    let total_messages = messages_per_iteration.saturating_mul(iterations);
    let total_logical_attachment_bytes = bytes.saturating_mul(total_messages);
    let encoded_head_bytes_per_iteration = encoded_samples
        .iter()
        .map(|sample| sample.encoded_head_bytes)
        .sum::<usize>();

    let encode_started = Instant::now();
    let mut encoded_head_bytes = 0_usize;
    for _ in 0..iterations {
        for sample in &samples {
            let request = encode_dfs_protocol_codec_head(sample)?;
            encoded_head_bytes = encoded_head_bytes.saturating_add(request.body.len());
            std::hint::black_box(&request);
        }
    }
    let encode_elapsed = encode_started.elapsed();

    let decode_started = Instant::now();
    let mut decoded_head_bytes = 0_usize;
    for _ in 0..iterations {
        for sample in &encoded_samples {
            let decoded = sample
                .request
                .decode_body::<DfsAttachmentMessageCommand>()
                .map_err(|error| {
                    SinomoTestError::failed(format!(
                        "cannot decode {} DFS attachment request head: {error}",
                        sample.label
                    ))
                })?;
            validate_decoded_dfs_protocol_codec_head(&decoded, bytes, payload_hash.as_str())?;
            decoded_head_bytes = decoded_head_bytes.saturating_add(sample.encoded_head_bytes);
            std::hint::black_box(decoded);
        }
    }
    let decode_elapsed = decode_started.elapsed();

    let head_encode_bytes_per_second = rate_per_second(encoded_head_bytes as f64, encode_elapsed);
    let head_decode_bytes_per_second = rate_per_second(decoded_head_bytes as f64, decode_elapsed);
    let head_encode_messages_per_second = rate_per_second(total_messages as f64, encode_elapsed);
    let head_decode_messages_per_second = rate_per_second(total_messages as f64, decode_elapsed);
    // Preserve the existing configurable gate as a logical-capacity metric: it
    // measures how many attachment bytes the encoded heads describe, not bytes
    // copied through the SOBJ codec. Actual encoded-head throughput is recorded
    // separately above.
    let encode_attachment_bytes_described_per_second =
        rate_per_second(total_logical_attachment_bytes as f64, encode_elapsed);
    let decode_attachment_bytes_described_per_second =
        rate_per_second(total_logical_attachment_bytes as f64, decode_elapsed);

    ctx.record("logical_attachment_bytes_per_message", bytes as u64);
    ctx.record("logical_attachment_content_hash", &payload_hash);
    ctx.record("iterations", iterations);
    ctx.record("messages_per_iteration", messages_per_iteration);
    ctx.record("total_messages", total_messages);
    ctx.record(
        "total_logical_attachment_bytes_described",
        total_logical_attachment_bytes as u64,
    );
    ctx.record(
        "encoded_head_bytes_per_iteration",
        encoded_head_bytes_per_iteration as u64,
    );
    ctx.record("encode_elapsed_ms", encode_elapsed.as_millis() as u64);
    ctx.record("decode_elapsed_ms", decode_elapsed.as_millis() as u64);
    ctx.record("head_encode_bytes_per_second", head_encode_bytes_per_second);
    ctx.record("head_decode_bytes_per_second", head_decode_bytes_per_second);
    ctx.record(
        "head_encode_messages_per_second",
        head_encode_messages_per_second,
    );
    ctx.record(
        "head_decode_messages_per_second",
        head_decode_messages_per_second,
    );
    ctx.record(
        "encode_attachment_bytes_described_per_second",
        encode_attachment_bytes_described_per_second,
    );
    ctx.record(
        "decode_attachment_bytes_described_per_second",
        decode_attachment_bytes_described_per_second,
    );
    ctx.record(
        "min_encode_attachment_bytes_described_per_second",
        min_encode_attachment_bytes_described_per_second,
    );
    ctx.record(
        "min_decode_attachment_bytes_described_per_second",
        min_decode_attachment_bytes_described_per_second,
    );
    ctx.record(
        "sample_labels",
        samples
            .iter()
            .map(|sample| sample.label)
            .collect::<Vec<_>>(),
    );

    if min_encode_attachment_bytes_described_per_second > 0.0
        && encode_attachment_bytes_described_per_second
            < min_encode_attachment_bytes_described_per_second
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS body-head encode capacity described {encode_attachment_bytes_described_per_second:.2} attachment bytes/s, below required {min_encode_attachment_bytes_described_per_second:.2} bytes/s"
        )));
    }
    if min_decode_attachment_bytes_described_per_second > 0.0
        && decode_attachment_bytes_described_per_second
            < min_decode_attachment_bytes_described_per_second
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS body-head decode capacity described {decode_attachment_bytes_described_per_second:.2} attachment bytes/s, below required {min_decode_attachment_bytes_described_per_second:.2} bytes/s"
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "DFS protocol codec processed {total_messages} body-aware heads describing {bytes}-byte attachments (encode {head_encode_messages_per_second:.2} heads/s, decode {head_decode_messages_per_second:.2} heads/s)"
    )))
}

fn run_content_version_gc(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let version_count =
        optional_usize_parameter(ctx, "versions", DEFAULT_CONTENT_VERSION_GC_VERSIONS)?;
    let write_batch_size =
        optional_usize_parameter(ctx, "write-batch", DEFAULT_CONTENT_VERSION_GC_WRITE_BATCH)?;
    let max_total_ms = optional_u64_parameter(ctx, "max-total-ms")?
        .unwrap_or(DEFAULT_CONTENT_VERSION_GC_MAX_TOTAL_MS);
    let max_restart_ms = optional_u64_parameter(ctx, "max-restart-ms")?
        .unwrap_or(DEFAULT_CONTENT_VERSION_GC_MAX_RESTART_MS);
    require_runtime_probe_count("versions", version_count, 2)?;
    require_runtime_probe_count("write-batch", write_batch_size, 1)?;

    let root = ScopedDfsRuntimeProbeRoot::create(ctx, "content-version-gc")?;
    let report = dfs::run_content_version_gc_runtime_probe(
        root.path(),
        dfs::DfsContentVersionGcRuntimeProbeOptions {
            version_count,
            write_batch_size,
        },
    )
    .map_err(|error| {
        SinomoTestError::failed(format!("content-version GC runtime probe failed: {error}"))
    })?;

    ctx.record("root", root.path().display().to_string());
    ctx.record("performance", &report);
    ctx.record("max_total_ms", max_total_ms);
    ctx.record("max_restart_ms", max_restart_ms);
    record_u64_metric(
        ctx,
        "versions",
        report.version_count,
        SinomoTestMetricUnit::Count,
    );
    record_u64_metric(
        ctx,
        "write_elapsed_ms",
        report.write_elapsed_ms,
        SinomoTestMetricUnit::Milliseconds,
    );
    record_u64_metric(
        ctx,
        "reservation_release_elapsed_ms",
        report.reservation_release_elapsed_ms,
        SinomoTestMetricUnit::Milliseconds,
    );
    record_u64_metric(
        ctx,
        "projection_elapsed_ms",
        report.projection_elapsed_ms,
        SinomoTestMetricUnit::Milliseconds,
    );
    record_u64_metric(
        ctx,
        "gc_elapsed_ms",
        report.gc_elapsed_ms,
        SinomoTestMetricUnit::Milliseconds,
    );
    record_u64_metric(
        ctx,
        "restart_elapsed_ms",
        report.restart_elapsed_ms,
        SinomoTestMetricUnit::Milliseconds,
    );
    record_u64_metric(
        ctx,
        "total_elapsed_ms",
        report.total_elapsed_ms,
        SinomoTestMetricUnit::Milliseconds,
    );
    let write_rate = rate_per_second(
        report.version_count as f64,
        Duration::from_millis(report.write_elapsed_ms.max(1)),
    );
    ctx.record_metric(
        "write_versions_per_second",
        write_rate,
        SinomoTestMetricUnit::OperationsPerSecond,
    );

    if max_total_ms > 0 && report.total_elapsed_ms > max_total_ms {
        return Ok(SinomoTestOutcome::failed(format!(
            "content-version GC runtime probe took {}ms, above allowed {max_total_ms}ms",
            report.total_elapsed_ms
        )));
    }
    if max_restart_ms > 0 && report.restart_elapsed_ms > max_restart_ms {
        return Ok(SinomoTestOutcome::failed(format!(
            "content-version GC restart took {}ms, above allowed {max_restart_ms}ms",
            report.restart_elapsed_ms
        )));
    }
    Ok(SinomoTestOutcome::passed(format!(
        "content-version GC deleted {} of {} versions in {}ms and reopened in {}ms",
        report.deleted_version_count,
        report.version_count,
        report.gc_elapsed_ms,
        report.restart_elapsed_ms
    )))
}

fn require_runtime_probe_count(
    name: &str,
    value: usize,
    minimum: usize,
) -> Result<(), SinomoTestError> {
    if value < minimum || value > MAX_DFS_STORAGE_RUNTIME_PROBE_ITEMS {
        return Err(SinomoTestError::InvalidParameter {
            name: name.to_string(),
            detail: format!(
                "must be between {minimum} and {MAX_DFS_STORAGE_RUNTIME_PROBE_ITEMS}, got {value}"
            ),
        });
    }
    Ok(())
}

fn record_u64_metric(
    ctx: &mut SinomoTestContext,
    name: &'static str,
    value: u64,
    unit: SinomoTestMetricUnit,
) {
    ctx.record_metric(name, value as f64, unit);
}

fn run_invariants(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let timeout = expect_timeout(ctx)?;
    let deadline = Instant::now() + timeout;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    // The acceptance runner is a separate process from the resident daemon.
    // Reuse the daemon-owned live-health projection instead of trying to open
    // the daemon's local Raft store merely to select observation targets.
    let (cluster_state, health_rows) =
        super::cluster_health_rows_for_data_dir(&data_dir).map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot read live cluster membership before checking DFS invariants: {error}"
            ))
        })?;
    let live_online_remote_node_ids = super::test::remote_main_test_targets_from_health_snapshot(
        &cluster_state,
        &cluster_state,
        &health_rows,
        false,
    )?
    .into_iter()
    .collect::<BTreeSet<_>>();
    let explicitly_unreachable_replica_node_ids = dfs_unavailable_targets_from_live_health(
        &cluster_paths,
        &cluster_state,
        &live_online_remote_node_ids,
    );
    let backend = open_observation_with_deadline(ctx, deadline)?;
    let node = backend.node_label();
    ctx.record("node", &node);
    loop {
        let snapshot = backend
            .operational_snapshot()
            .map_err(SinomoTestError::failed)?;
        let metrics = snapshot.metrics;
        let mut violations = Vec::new();

        if metrics.cross_shard_namespace_lock_count != 0 {
            violations.push(format!(
                "cross_shard_namespace_lock_count={}",
                metrics.cross_shard_namespace_lock_count
            ));
        }
        let operator_status = snapshot.operator_status;
        let status_completeness = assess_dfs_invariant_status_completeness(&operator_status);
        violations.extend(
            status_completeness
                .blocking_incomplete_reasons
                .iter()
                .map(|reason| format!("operator status incomplete: {reason}")),
        );
        ctx.record(
            "bounded_status_omissions",
            &status_completeness.bounded_omissions,
        );
        ctx.record(
            "blocking_status_incomplete_reasons",
            &status_completeness.blocking_incomplete_reasons,
        );
        let replica_assessment = assess_dfs_invariant_replica_health(
            &node,
            &operator_status.metadata_shards,
            &explicitly_unreachable_replica_node_ids,
        );
        violations.extend(replica_assessment.violations());
        record_dfs_invariant_replica_assessment(ctx, &replica_assessment);
        if violations.is_empty() {
            ctx.record("object_count", metrics.object_count);
            ctx.record("repair_job_count", metrics.repair_job_count);
            ctx.record("repair_attempt_count", metrics.repair_attempt_count);
            ctx.record("pending_write_count", metrics.pending_write_count);
            ctx.record(
                "pending_metadata_replication_count",
                metrics.pending_metadata_replication_count,
            );
            return Ok(SinomoTestOutcome::passed(
                if status_completeness.bounded_omissions.is_empty() {
                    format!("DFS operational invariants hold on node {node}")
                } else {
                    format!(
                        "DFS observed operational invariants are clean on node {node}; {} bounded metric observation(s) remain explicitly omitted",
                        status_completeness.bounded_omissions.len()
                    )
                },
            ));
        }
        if Instant::now() >= deadline {
            ctx.record("violations", &violations);
            return Ok(SinomoTestOutcome::failed(format!(
                "DFS invariants violated on node {node} after {}s: {}",
                timeout.as_secs(),
                violations.join(", ")
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DfsInvariantStatusCompletenessAssessment {
    bounded_omissions: Vec<String>,
    blocking_incomplete_reasons: Vec<String>,
}

fn assess_dfs_invariant_status_completeness(
    status: &DfsOperatorStatus,
) -> DfsInvariantStatusCompletenessAssessment {
    DfsInvariantStatusCompletenessAssessment {
        bounded_omissions: status
            .bounded_omissions
            .iter()
            .map(ToString::to_string)
            .collect(),
        blocking_incomplete_reasons: status
            .blocking_incomplete_reasons()
            .into_iter()
            .map(str::to_string)
            .collect(),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DfsInvariantReplicaAssessment {
    local_primary_shard_count: usize,
    reported_replica_count: usize,
    configured_replica_count: usize,
    explicitly_unreachable_configured_replica_count: usize,
    reachable_reported_replica_count: usize,
    reachable_configured_replica_count: usize,
    lagging_reachable_replica_count: usize,
    max_reachable_replica_lag_entries: u64,
    lagging_unreachable_replica_count: usize,
    max_unreachable_replica_lag_entries: u64,
    explicitly_unreachable_replica_node_ids: BTreeSet<NodeId>,
    reachable_replica_status_mismatches: Vec<String>,
    metadata_quorum_losses: Vec<String>,
}

impl DfsInvariantReplicaAssessment {
    fn violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        if !self.reachable_replica_status_mismatches.is_empty() {
            violations.push(format!(
                "reachable_local_primary_replica_status_mismatches={}",
                self.reachable_replica_status_mismatches.join(";")
            ));
        }
        if self.lagging_reachable_replica_count != 0 {
            violations.push(format!(
                "lagging_reachable_local_primary_replica_count={}, max_lag_entries={}",
                self.lagging_reachable_replica_count, self.max_reachable_replica_lag_entries
            ));
        }
        if !self.metadata_quorum_losses.is_empty() {
            violations.push(format!(
                "local_primary_metadata_quorum_lost={}",
                self.metadata_quorum_losses.join(";")
            ));
        }
        violations
    }
}

/// Returns remote main daemons that the current live-health sweep did not
/// observe online and for which no healthy route remains. A stale or startup
/// `Unknown`/`Degraded` connectivity record must not make an offline replica
/// look available, while a healthy route keeps a transient probe miss visible
/// to the invariant.
fn dfs_unavailable_targets_from_live_health(
    cluster_paths: &ClusterPaths,
    state: &sinomo_runtime::core::ClusterBootstrapState,
    live_online_remote_node_ids: &BTreeSet<NodeId>,
) -> BTreeSet<NodeId> {
    state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && member.capabilities.iter().any(|capability| {
                    capability.as_str() == sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY
                })
                && !live_online_remote_node_ids.contains(&member.node_id)
                && !dfs_target_has_healthy_route(cluster_paths, state, &member.node_id)
        })
        .map(|member| member.node_id.clone())
        .collect()
}

fn assess_dfs_invariant_replica_health(
    local_node_id: &str,
    metadata_shards: &[dfs::DfsMetadataShardStatus],
    explicitly_unreachable_node_ids: &BTreeSet<NodeId>,
) -> DfsInvariantReplicaAssessment {
    let mut assessment = DfsInvariantReplicaAssessment::default();
    for shard in metadata_shards.iter().filter(|shard| {
        shard
            .primary_node_id
            .as_ref()
            .is_some_and(|primary| primary.as_str() == local_node_id)
    }) {
        assessment.local_primary_shard_count += 1;
        assessment.reported_replica_count += shard.replica_lag.len();
        assessment.configured_replica_count += shard.replica_node_ids.len();

        let configured_node_ids = shard
            .replica_node_ids
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let reported_node_ids = shard
            .replica_lag
            .iter()
            .map(|lag| lag.target_node_id.clone())
            .collect::<BTreeSet<_>>();
        let unreachable_configured_node_ids = configured_node_ids
            .intersection(explicitly_unreachable_node_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
        let reachable_configured_node_ids = configured_node_ids
            .difference(&unreachable_configured_node_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
        let reachable_reported_node_ids = reported_node_ids
            .intersection(&reachable_configured_node_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
        let missing_reachable_node_ids = reachable_configured_node_ids
            .difference(&reported_node_ids)
            .cloned()
            .collect::<BTreeSet<_>>();
        let unexpected_reported_node_ids = reported_node_ids
            .difference(&configured_node_ids)
            .cloned()
            .collect::<BTreeSet<_>>();

        assessment.explicitly_unreachable_configured_replica_count +=
            unreachable_configured_node_ids.len();
        assessment.reachable_configured_replica_count += reachable_configured_node_ids.len();
        assessment.reachable_reported_replica_count += reachable_reported_node_ids.len();
        assessment
            .explicitly_unreachable_replica_node_ids
            .extend(unreachable_configured_node_ids.iter().cloned());

        if !missing_reachable_node_ids.is_empty() || !unexpected_reported_node_ids.is_empty() {
            assessment.reachable_replica_status_mismatches.push(format!(
                "{} missing_reachable={:?} unexpected={:?}",
                shard.shard_id.0,
                node_id_strings(&missing_reachable_node_ids),
                node_id_strings(&unexpected_reported_node_ids)
            ));
        }

        for lag in &shard.replica_lag {
            if lag.lag_entries == 0 {
                continue;
            }
            if unreachable_configured_node_ids.contains(&lag.target_node_id) {
                assessment.lagging_unreachable_replica_count += 1;
                assessment.max_unreachable_replica_lag_entries = assessment
                    .max_unreachable_replica_lag_entries
                    .max(lag.lag_entries);
            } else if reachable_configured_node_ids.contains(&lag.target_node_id) {
                assessment.lagging_reachable_replica_count += 1;
                assessment.max_reachable_replica_lag_entries = assessment
                    .max_reachable_replica_lag_entries
                    .max(lag.lag_entries);
            }
        }

        let quorum_size = dfs::dfs_metadata_replica_quorum_size(shard.replica_node_ids.len());
        let available_participant_count = reachable_configured_node_ids.len().saturating_add(1);
        if available_participant_count < quorum_size {
            assessment.metadata_quorum_losses.push(format!(
                "{} available={available_participant_count} quorum={quorum_size} configured={} unreachable={:?}",
                shard.shard_id.0,
                shard.replica_node_ids.len().saturating_add(1),
                node_id_strings(&unreachable_configured_node_ids)
            ));
        }
    }
    assessment
}

fn node_id_strings(node_ids: &BTreeSet<NodeId>) -> Vec<String> {
    node_ids.iter().map(ToString::to_string).collect()
}

fn record_dfs_invariant_replica_assessment(
    ctx: &mut SinomoTestContext,
    assessment: &DfsInvariantReplicaAssessment,
) {
    ctx.record(
        "local_primary_shard_count",
        assessment.local_primary_shard_count,
    );
    ctx.record(
        "local_primary_replica_count",
        assessment.reported_replica_count,
    );
    ctx.record(
        "configured_local_primary_replica_count",
        assessment.configured_replica_count,
    );
    ctx.record(
        "explicitly_unreachable_configured_local_primary_replica_count",
        assessment.explicitly_unreachable_configured_replica_count,
    );
    ctx.record(
        "explicitly_unreachable_local_primary_replica_node_ids",
        node_id_strings(&assessment.explicitly_unreachable_replica_node_ids),
    );
    ctx.record(
        "reachable_local_primary_replica_count",
        assessment.reachable_reported_replica_count,
    );
    ctx.record(
        "configured_reachable_local_primary_replica_count",
        assessment.reachable_configured_replica_count,
    );
    ctx.record(
        "lagging_reachable_local_primary_replica_count",
        assessment.lagging_reachable_replica_count,
    );
    ctx.record(
        "max_reachable_local_primary_replica_lag_entries",
        assessment.max_reachable_replica_lag_entries,
    );
    ctx.record(
        "lagging_unreachable_local_primary_replica_count",
        assessment.lagging_unreachable_replica_count,
    );
    ctx.record(
        "max_unreachable_local_primary_replica_lag_entries",
        assessment.max_unreachable_replica_lag_entries,
    );
    ctx.record(
        "reachable_local_primary_replica_status_mismatches",
        &assessment.reachable_replica_status_mismatches,
    );
    ctx.record(
        "local_primary_metadata_quorum_losses",
        &assessment.metadata_quorum_losses,
    );
}

fn run_lightweight_latency(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = scoped_dfs_test_base(ctx, "lightweight-latency")?;
    let iterations =
        optional_usize_parameter(ctx, "iterations", DEFAULT_LIGHTWEIGHT_LATENCY_ITERATIONS)?;
    let warmup_iterations = optional_usize_parameter(
        ctx,
        "warmup-iterations",
        DEFAULT_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS,
    )?;
    let requested_payload_bytes = optional_usize_parameter(
        ctx,
        "payload-bytes",
        DEFAULT_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES,
    )?;
    let existing_file = ctx
        .parameter("existing-file")
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_string);
    let _namespace = existing_file
        .is_none()
        .then(|| ScopedSifsTestNamespace::open(ctx, [base.clone()]))
        .transpose()?;
    let max_p50_us = optional_usize_parameter(ctx, "max-p50-us", 0)? as u64;
    let max_p95_us = optional_usize_parameter(ctx, "max-p95-us", 0)? as u64;
    let max_max_us = optional_usize_parameter(ctx, "max-max-us", 0)? as u64;
    let max_backend_open_ms = optional_usize_parameter(
        ctx,
        "max-backend-open-ms",
        DEFAULT_LIGHTWEIGHT_LATENCY_MAX_BACKEND_OPEN_MS,
    )? as u64;
    require_positive("iterations", iterations)?;
    require_positive("payload-bytes", requested_payload_bytes)?;
    if iterations > MAX_LIGHTWEIGHT_LATENCY_ITERATIONS {
        return Err(SinomoTestError::failed(format!(
            "iterations must not exceed {MAX_LIGHTWEIGHT_LATENCY_ITERATIONS}"
        )));
    }
    if warmup_iterations > MAX_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS {
        return Err(SinomoTestError::failed(format!(
            "warmup-iterations must not exceed {MAX_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS}"
        )));
    }
    if requested_payload_bytes > MAX_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES {
        return Err(SinomoTestError::failed(format!(
            "payload-bytes must not exceed {MAX_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES}"
        )));
    }

    let backend_open_started = Instant::now();
    let backend = open_backend(ctx)?;
    let backend_open_elapsed = backend_open_started.elapsed();
    let node = backend.node_label();
    let setup_started = Instant::now();
    let (
        fixture_dir,
        file_name,
        file_path,
        payload,
        file_bytes,
        read_prefix_truncated,
        reused_fixture,
    ) = if let Some(file_path) = existing_file {
        let normalized = file_path.trim_end_matches('/');
        let (parent, file_name) = normalized.rsplit_once('/').ok_or_else(|| {
            SinomoTestError::failed(format!(
                "existing-file `{file_path}` must include an absolute parent directory"
            ))
        })?;
        if file_name.is_empty() {
            return Err(SinomoTestError::failed(format!(
                "existing-file `{file_path}` has no file name"
            )));
        }
        let fixture_dir = if parent.is_empty() { "/" } else { parent };
        let metadata = backend.stat(normalized).map_err(SinomoTestError::failed)?;
        if metadata.kind != crate::file_manager::FileManagerEntryKind::File {
            return Err(SinomoTestError::failed(format!(
                "existing-file `{file_path}` is not a file"
            )));
        }
        let file_bytes = metadata.size.ok_or_else(|| {
            SinomoTestError::failed(format!(
                "existing-file `{file_path}` did not report its size"
            ))
        })?;
        let preview = backend
            .read_file_prefix(normalized, requested_payload_bytes as u64)
            .map_err(SinomoTestError::failed)?;
        let expected_truncated = file_bytes > requested_payload_bytes as u64;
        if preview.truncated != expected_truncated {
            return Err(SinomoTestError::failed(format!(
                "existing-file `{file_path}` prefix truncation was {}, expected {expected_truncated}",
                preview.truncated
            )));
        }
        let payload = preview.bytes;
        if payload.is_empty() {
            return Err(SinomoTestError::failed(format!(
                "existing-file `{file_path}` must not be empty"
            )));
        }
        (
            fixture_dir.to_string(),
            file_name.to_string(),
            normalized.to_string(),
            payload,
            file_bytes,
            expected_truncated,
            true,
        )
    } else {
        let nonce = probe_nonce();
        let fixture_dir = format!("{base}/latency-{nonce}");
        let file_name = "tiny.txt".to_string();
        let file_path = format!("{fixture_dir}/{file_name}");
        let payload = pattern_payload(
            &format!("dfs-lightweight-latency-{node}-{nonce}"),
            requested_payload_bytes,
        );
        backend
            .create_dir_all(&fixture_dir)
            .map_err(SinomoTestError::failed)?;
        backend
            .write_file(&file_path, &payload)
            .map_err(SinomoTestError::failed)?;
        let file_bytes = payload.len() as u64;
        (
            fixture_dir,
            file_name,
            file_path,
            payload,
            file_bytes,
            false,
            false,
        )
    };
    let setup_elapsed = setup_started.elapsed();
    let read_prefix_bytes = payload.len();

    for _ in 0..warmup_iterations {
        run_lightweight_latency_operations(
            &backend,
            &fixture_dir,
            &file_path,
            &file_name,
            &payload,
            file_bytes,
            read_prefix_truncated,
        )?;
    }

    let mut latency_us = BTreeMap::<&'static str, Vec<u64>>::new();
    let measurement_started = Instant::now();
    for _ in 0..iterations {
        record_lightweight_operation_latency(&mut latency_us, "stat_file", || {
            validate_lightweight_stat_file(&backend, &file_path, file_bytes)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "stat_dir", || {
            validate_lightweight_stat_dir(&backend, &fixture_dir)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "list_dir", || {
            validate_lightweight_list_dir(&backend, &fixture_dir, &file_name)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "read_prefix", || {
            validate_lightweight_read_prefix(&backend, &file_path, &payload, read_prefix_truncated)
        })?;
    }
    let measurement_elapsed = measurement_started.elapsed();

    ctx.record("node", &node);
    ctx.record("base", &base);
    ctx.record("fixture_dir", &fixture_dir);
    ctx.record("file_path", &file_path);
    ctx.record("reused_fixture", reused_fixture);
    ctx.record("iterations", iterations);
    ctx.record("warmup_iterations", warmup_iterations);
    ctx.record("payload_bytes", file_bytes);
    ctx.record("read_prefix_bytes", read_prefix_bytes);
    ctx.record("read_prefix_truncated", read_prefix_truncated);
    ctx.record("max_p50_us", max_p50_us);
    ctx.record("max_p95_us", max_p95_us);
    ctx.record("max_max_us", max_max_us);
    ctx.record("max_backend_open_ms", max_backend_open_ms);
    ctx.record(
        "backend_open_elapsed_ms",
        backend_open_elapsed.as_millis() as u64,
    );
    ctx.record("setup_elapsed_ms", setup_elapsed.as_millis() as u64);
    ctx.record(
        "measurement_elapsed_ms",
        measurement_elapsed.as_millis() as u64,
    );

    let mut operation_summaries = BTreeMap::new();
    let mut worst_p50 = (String::new(), 0_u64);
    let mut worst_p95 = (String::new(), 0_u64);
    let mut worst_max = (String::new(), 0_u64);
    for (operation, mut samples) in latency_us {
        samples.sort_unstable();
        let count = samples.len();
        let min = samples.iter().copied().min().unwrap_or(0);
        let p50 = percentile_value(&samples, 50);
        let p95 = percentile_value(&samples, 95);
        let max = samples.iter().copied().max().unwrap_or(0);
        let mean = mean_value(&samples);
        let operation_key = operation.to_string();
        ctx.record(format!("{operation}_count"), count);
        ctx.record(format!("{operation}_latency_min_us"), min);
        ctx.record(format!("{operation}_latency_p50_us"), p50);
        ctx.record(format!("{operation}_latency_p95_us"), p95);
        ctx.record(format!("{operation}_latency_max_us"), max);
        ctx.record(format!("{operation}_latency_mean_us"), mean);
        operation_summaries.insert(
            operation_key.clone(),
            format!("count={count} min={min} mean={mean:.2} p50={p50} p95={p95} max={max}"),
        );
        if p50 > worst_p50.1 {
            worst_p50 = (operation_key.clone(), p50);
        }
        if p95 > worst_p95.1 {
            worst_p95 = (operation_key.clone(), p95);
        }
        if max > worst_max.1 {
            worst_max = (operation_key, max);
        }
    }
    ctx.record("operation_latency_us", operation_summaries);
    ctx.record("worst_p50_operation", &worst_p50.0);
    ctx.record("worst_p50_us", worst_p50.1);
    ctx.record("worst_p95_operation", &worst_p95.0);
    ctx.record("worst_p95_us", worst_p95.1);
    ctx.record("worst_max_operation", &worst_max.0);
    ctx.record("worst_max_us", worst_max.1);

    if max_backend_open_ms > 0 && backend_open_elapsed.as_millis() as u64 > max_backend_open_ms {
        return Ok(SinomoTestOutcome::failed(format!(
            "cold SIFS backend hydration took {}ms, above allowed {max_backend_open_ms}ms",
            backend_open_elapsed.as_millis()
        )));
    }
    if latency_reaches_exclusive_limit(worst_p50.1, max_p50_us) {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS lightweight latency worst p50 {}={}us is not below required {max_p50_us}us",
            worst_p50.0, worst_p50.1
        )));
    }
    if latency_reaches_exclusive_limit(worst_p95.1, max_p95_us) {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS lightweight latency worst p95 {}={}us is not below required {max_p95_us}us",
            worst_p95.0, worst_p95.1
        )));
    }
    if latency_reaches_exclusive_limit(worst_max.1, max_max_us) {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS lightweight latency worst max {}={}us is not below required {max_max_us}us",
            worst_max.0, worst_max.1
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "DFS lightweight latency on node {node}: worst p50 {}={}us, worst p95 {}={}us over {iterations} warmed iterations",
        worst_p50.0, worst_p50.1, worst_p95.0, worst_p95.1
    )))
}

struct RawLightweightLatencyFixture {
    root: std::path::PathBuf,
}

impl Drop for RawLightweightLatencyFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn run_raw_lightweight_latency(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let iterations =
        optional_usize_parameter(ctx, "iterations", DEFAULT_LIGHTWEIGHT_LATENCY_ITERATIONS)?;
    let warmup_iterations = optional_usize_parameter(
        ctx,
        "warmup-iterations",
        DEFAULT_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS,
    )?;
    let payload_bytes = optional_usize_parameter(
        ctx,
        "payload-bytes",
        DEFAULT_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES,
    )?;
    let max_p50_us = optional_usize_parameter(ctx, "max-p50-us", 0)? as u64;
    let max_p95_us = optional_usize_parameter(ctx, "max-p95-us", 0)? as u64;
    let max_max_us = optional_usize_parameter(ctx, "max-max-us", 0)? as u64;
    require_positive("iterations", iterations)?;
    require_positive("payload-bytes", payload_bytes)?;
    if iterations > MAX_LIGHTWEIGHT_LATENCY_ITERATIONS {
        return Err(SinomoTestError::failed(format!(
            "iterations must not exceed {MAX_LIGHTWEIGHT_LATENCY_ITERATIONS}"
        )));
    }
    if warmup_iterations > MAX_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS {
        return Err(SinomoTestError::failed(format!(
            "warmup-iterations must not exceed {MAX_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS}"
        )));
    }
    if payload_bytes > MAX_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES {
        return Err(SinomoTestError::failed(format!(
            "payload-bytes must not exceed {MAX_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES}"
        )));
    }

    let setup_started = Instant::now();
    let root = ctx
        .data_dir()
        .join("test-artifacts")
        .join("dfs-raw-lightweight-latency")
        .join(probe_nonce());
    fs::create_dir_all(&root).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot create raw lightweight latency directory {}: {source}",
            root.display()
        ))
    })?;
    let fixture = RawLightweightLatencyFixture { root };
    let file_name = "tiny.txt";
    let file_path = fixture.root.join(file_name);
    let payload = pattern_payload("dfs-raw-lightweight-latency", payload_bytes);
    fs::write(&file_path, &payload).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot write raw lightweight latency file {}: {source}",
            file_path.display()
        ))
    })?;
    let setup_elapsed = setup_started.elapsed();

    for _ in 0..warmup_iterations {
        run_raw_lightweight_latency_operations(&fixture.root, &file_path, file_name, &payload)?;
    }

    let mut latency_us = BTreeMap::<&'static str, Vec<u64>>::new();
    let measurement_started = Instant::now();
    for _ in 0..iterations {
        record_lightweight_operation_latency(&mut latency_us, "stat_file", || {
            validate_raw_lightweight_stat_file(&file_path, payload.len() as u64)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "stat_dir", || {
            validate_raw_lightweight_stat_dir(&fixture.root)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "list_dir", || {
            validate_raw_lightweight_list_dir(&fixture.root, file_name)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "read_prefix", || {
            validate_raw_lightweight_read_prefix(&file_path, &payload)
        })?;
    }
    let measurement_elapsed = measurement_started.elapsed();

    ctx.record("root", fixture.root.display().to_string());
    ctx.record("file_path", file_path.display().to_string());
    ctx.record("iterations", iterations);
    ctx.record("warmup_iterations", warmup_iterations);
    ctx.record("payload_bytes", payload_bytes);
    ctx.record("max_p50_us", max_p50_us);
    ctx.record("max_p95_us", max_p95_us);
    ctx.record("max_max_us", max_max_us);
    ctx.record("setup_elapsed_ms", setup_elapsed.as_millis() as u64);
    ctx.record(
        "measurement_elapsed_ms",
        measurement_elapsed.as_millis() as u64,
    );

    let mut operation_summaries = BTreeMap::new();
    let mut worst_p50 = (String::new(), 0_u64);
    let mut worst_p95 = (String::new(), 0_u64);
    let mut worst_max = (String::new(), 0_u64);
    for (operation, mut samples) in latency_us {
        samples.sort_unstable();
        let count = samples.len();
        let min = samples.iter().copied().min().unwrap_or(0);
        let p50 = percentile_value(&samples, 50);
        let p95 = percentile_value(&samples, 95);
        let max = samples.iter().copied().max().unwrap_or(0);
        let mean = mean_value(&samples);
        let operation_key = operation.to_string();
        ctx.record(format!("{operation}_count"), count);
        ctx.record(format!("{operation}_latency_min_us"), min);
        ctx.record(format!("{operation}_latency_p50_us"), p50);
        ctx.record(format!("{operation}_latency_p95_us"), p95);
        ctx.record(format!("{operation}_latency_max_us"), max);
        ctx.record(format!("{operation}_latency_mean_us"), mean);
        operation_summaries.insert(
            operation_key.clone(),
            format!("count={count} min={min} mean={mean:.2} p50={p50} p95={p95} max={max}"),
        );
        if p50 > worst_p50.1 {
            worst_p50 = (operation_key.clone(), p50);
        }
        if p95 > worst_p95.1 {
            worst_p95 = (operation_key.clone(), p95);
        }
        if max > worst_max.1 {
            worst_max = (operation_key, max);
        }
    }
    ctx.record("operation_latency_us", operation_summaries);
    ctx.record("worst_p50_operation", &worst_p50.0);
    ctx.record("worst_p50_us", worst_p50.1);
    ctx.record("worst_p95_operation", &worst_p95.0);
    ctx.record("worst_p95_us", worst_p95.1);
    ctx.record("worst_max_operation", &worst_max.0);
    ctx.record("worst_max_us", worst_max.1);

    if latency_reaches_exclusive_limit(worst_p50.1, max_p50_us) {
        return Ok(SinomoTestOutcome::failed(format!(
            "raw lightweight latency worst p50 {}={}us is not below required {max_p50_us}us",
            worst_p50.0, worst_p50.1
        )));
    }
    if latency_reaches_exclusive_limit(worst_p95.1, max_p95_us) {
        return Ok(SinomoTestOutcome::failed(format!(
            "raw lightweight latency worst p95 {}={}us is not below required {max_p95_us}us",
            worst_p95.0, worst_p95.1
        )));
    }
    if latency_reaches_exclusive_limit(worst_max.1, max_max_us) {
        return Ok(SinomoTestOutcome::failed(format!(
            "raw lightweight latency worst max {}={}us is not below required {max_max_us}us",
            worst_max.0, worst_max.1
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "raw lightweight latency: worst p50 {}={}us, worst p95 {}={}us over {iterations} warmed iterations",
        worst_p50.0, worst_p50.1, worst_p95.0, worst_p95.1
    )))
}

fn run_raw_lightweight_latency_operations(
    fixture_dir: &Path,
    file_path: &Path,
    file_name: &str,
    payload: &[u8],
) -> Result<(), SinomoTestError> {
    validate_raw_lightweight_stat_file(file_path, payload.len() as u64)?;
    validate_raw_lightweight_stat_dir(fixture_dir)?;
    validate_raw_lightweight_list_dir(fixture_dir, file_name)?;
    validate_raw_lightweight_read_prefix(file_path, payload)?;
    Ok(())
}

fn validate_raw_lightweight_stat_file(
    path: &Path,
    expected_bytes: u64,
) -> Result<(), SinomoTestError> {
    let metadata = fs::metadata(path).map_err(|source| {
        SinomoTestError::failed(format!("cannot stat raw file {}: {source}", path.display()))
    })?;
    if !metadata.is_file() || metadata.len() != expected_bytes {
        return Err(SinomoTestError::failed(format!(
            "raw file {} metadata was file={} size={}, expected file=true size={expected_bytes}",
            path.display(),
            metadata.is_file(),
            metadata.len()
        )));
    }
    Ok(())
}

fn validate_raw_lightweight_stat_dir(path: &Path) -> Result<(), SinomoTestError> {
    let metadata = fs::metadata(path).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot stat raw directory {}: {source}",
            path.display()
        ))
    })?;
    if !metadata.is_dir() {
        return Err(SinomoTestError::failed(format!(
            "raw path {} is not a directory",
            path.display()
        )));
    }
    Ok(())
}

fn validate_raw_lightweight_list_dir(
    path: &Path,
    expected_name: &str,
) -> Result<(), SinomoTestError> {
    let entries = fs::read_dir(path).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot list raw directory {}: {source}",
            path.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot read raw directory entry under {}: {source}",
                path.display()
            ))
        })?;
        if entry.file_name().to_string_lossy() == expected_name {
            return Ok(());
        }
    }
    Err(SinomoTestError::failed(format!(
        "raw directory {} did not contain {expected_name}",
        path.display()
    )))
}

fn validate_raw_lightweight_read_prefix(
    path: &Path,
    expected: &[u8],
) -> Result<(), SinomoTestError> {
    let file = fs::File::open(path).map_err(|source| {
        SinomoTestError::failed(format!("cannot open raw file {}: {source}", path.display()))
    })?;
    let mut actual = Vec::with_capacity(expected.len());
    file.take(expected.len() as u64)
        .read_to_end(&mut actual)
        .map_err(|source| {
            SinomoTestError::failed(format!("cannot read raw file {}: {source}", path.display()))
        })?;
    if actual != expected {
        return Err(SinomoTestError::failed(format!(
            "raw prefix read {} returned {} bytes, expected {}",
            path.display(),
            actual.len(),
            expected.len()
        )));
    }
    Ok(())
}

fn run_lightweight_latency_operations(
    backend: &SifsFileManagerBackend,
    fixture_dir: &str,
    file_path: &str,
    file_name: &str,
    payload: &[u8],
    file_bytes: u64,
    read_prefix_truncated: bool,
) -> Result<(), SinomoTestError> {
    validate_lightweight_stat_file(backend, file_path, file_bytes)?;
    validate_lightweight_stat_dir(backend, fixture_dir)?;
    validate_lightweight_list_dir(backend, fixture_dir, file_name)?;
    validate_lightweight_read_prefix(backend, file_path, payload, read_prefix_truncated)?;
    Ok(())
}

fn record_lightweight_operation_latency<F>(
    latency_us: &mut BTreeMap<&'static str, Vec<u64>>,
    operation: &'static str,
    run: F,
) -> Result<(), SinomoTestError>
where
    F: FnOnce() -> Result<(), SinomoTestError>,
{
    let started = Instant::now();
    run()?;
    latency_us
        .entry(operation)
        .or_default()
        .push(started.elapsed().as_micros() as u64);
    Ok(())
}

fn validate_lightweight_stat_file(
    backend: &SifsFileManagerBackend,
    path: &str,
    expected_bytes: u64,
) -> Result<(), SinomoTestError> {
    let metadata = backend.stat(path).map_err(SinomoTestError::failed)?;
    if metadata.kind != crate::file_manager::FileManagerEntryKind::File {
        return Err(SinomoTestError::failed(format!(
            "{path} stat kind {:?}, expected file",
            metadata.kind
        )));
    }
    if metadata.size != Some(expected_bytes) {
        return Err(SinomoTestError::failed(format!(
            "{path} stat size {:?}, expected {expected_bytes}",
            metadata.size
        )));
    }
    Ok(())
}

fn validate_lightweight_stat_dir(
    backend: &SifsFileManagerBackend,
    path: &str,
) -> Result<(), SinomoTestError> {
    let metadata = backend.stat(path).map_err(SinomoTestError::failed)?;
    if metadata.kind != crate::file_manager::FileManagerEntryKind::Directory {
        return Err(SinomoTestError::failed(format!(
            "{path} stat kind {:?}, expected directory",
            metadata.kind
        )));
    }
    Ok(())
}

fn validate_lightweight_list_dir(
    backend: &SifsFileManagerBackend,
    path: &str,
    expected_name: &str,
) -> Result<(), SinomoTestError> {
    let entries = backend.list_dir(path).map_err(SinomoTestError::failed)?;
    if !entries.iter().any(|entry| {
        entry.name == expected_name && entry.kind == crate::file_manager::FileManagerEntryKind::File
    }) {
        return Err(SinomoTestError::failed(format!(
            "{path} listing did not include file {expected_name}"
        )));
    }
    Ok(())
}

fn validate_lightweight_read_prefix(
    backend: &SifsFileManagerBackend,
    path: &str,
    expected: &[u8],
    expected_truncated: bool,
) -> Result<(), SinomoTestError> {
    let read = backend
        .read_file_prefix(path, expected.len() as u64)
        .map_err(SinomoTestError::failed)?;
    if read.truncated != expected_truncated {
        return Err(SinomoTestError::failed(format!(
            "{path} read-prefix truncation was {}, expected {expected_truncated}",
            read.truncated
        )));
    }
    if read.bytes != expected {
        return Err(SinomoTestError::failed(format!(
            "{path} read-prefix returned {} bytes, expected {}",
            read.bytes.len(),
            expected.len()
        )));
    }
    Ok(())
}

fn write_latency_placement_policy(scenario: &str) -> Result<DfsPlacementPolicy, SinomoTestError> {
    let (w, n) = match scenario {
        "w1-n2" => (1, 2),
        "w2-n3" => (2, 3),
        _ => {
            return Err(SinomoTestError::failed(
                "replication must be w1-n2 or w2-n3",
            ));
        }
    };
    // Both allow asynchronous repair up to N after W durable acknowledgements.
    // ReplicatedConfirmed means W == N and would change the requested scenario.
    DfsPlacementPolicy::new(DfsObjectPolicy::ReplicatedEventual, w, n)
        .map_err(|error| SinomoTestError::failed(error.to_string()))
}

fn verify_write_latency_placement(
    placement: &dfs::DfsResolvedPlacementPolicy,
    base: &str,
    expected: &DfsPlacementPolicy,
) -> Result<(), String> {
    if placement.policy != *expected
        || placement.source_path.as_deref() != Some(base)
        || placement.source_scope != Some(DfsPlacementScope::Subtree)
    {
        return Err(format!(
            "write fixture placement differs from requested policy: {placement:?}"
        ));
    }
    Ok(())
}

fn write_latency_preview_can_refresh(error: &str) -> bool {
    !error.contains("PossiblyApplied")
        && (error.contains("SIFS placement preview is stale")
            || error.contains(dfs::DFS_PLACEMENT_APPLY_REJECTED_PREFIX))
}

fn write_latency_payload_seed(operation: &str, index: usize, path: &str) -> String {
    format!("dfs-write-latency-{operation}-{index}|{path}")
}

fn run_write_latency(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let prepared_base = ctx.parameter("prepared-base").map(str::to_owned);
    let base = if let Some(base) = prepared_base.as_ref() {
        validate_scoped_sifs_test_namespace(base)?;
        base.clone()
    } else {
        scoped_dfs_test_base(ctx, "write-latency")?
    };
    let replication = ctx.parameter("replication").unwrap_or("w1-n2").to_owned();
    let policy = write_latency_placement_policy(&replication)?;
    let iterations = optional_usize_parameter(ctx, "iterations", 100)?;
    let warmup = optional_usize_parameter(ctx, "warmup-iterations", 12)?;
    let bytes = optional_usize_parameter(ctx, "payload-bytes", 4096)?;
    let max_p95_us = optional_usize_parameter(ctx, "max-p95-us", 100_000)? as u64;
    let retain_fixture = optional_bool_parameter(ctx, "retain-fixture", false)?;
    if prepared_base.is_some() && !retain_fixture {
        return Err(SinomoTestError::failed(
            "prepared-base requires retain-fixture=true",
        ));
    }
    require_positive("iterations", iterations)?;
    require_positive("payload-bytes", bytes)?;
    if iterations > 10_000 || warmup > 1_000 || bytes > 1024 * 1024 {
        return Err(SinomoTestError::failed(
            "write-latency limits: iterations <= 10000, warmup-iterations <= 1000, payload-bytes <= 1048576",
        ));
    }
    let _namespace = (!retain_fixture)
        .then(|| ScopedSifsTestNamespace::open(ctx, [base.clone()]))
        .transpose()?;
    let setup_started = Instant::now();
    // Keep the same public daemon client throughout the loop. The timed call
    // includes encoding, RPC and the daemon's committed response, without CLI
    // startup, Docker exec, payload construction, reads or fixture cleanup.
    let browser = open_local_daemon_sifs_browser(ctx)?;
    if prepared_base.is_none() {
        browser
            .create_dir_all(&base)
            .map_err(SinomoTestError::failed)?;
    }
    ctx.record("base", &base);
    ctx.record("prepared_fixture", prepared_base.is_some());
    ctx.record("replication_scenario", &replication);
    ctx.record("requested_placement_policy", &policy);
    let apply_policy = |policy: Option<DfsPlacementPolicy>| -> Result<_, String> {
        for attempt in 1..=5 {
            let preview =
                browser.preview_placement(&base, policy.clone(), DfsPlacementScope::Subtree)?;
            let update = match browser.update_placement(
                &base,
                policy.clone(),
                DfsPlacementScope::Subtree,
                preview.preview_token,
            ) {
                Ok(update) => update,
                Err(error) if attempt < 5 && write_latency_preview_can_refresh(&error) => {
                    // Refresh only an explicitly rejected Apply or preview,
                    // keeping the same run-owned path and requested policy.
                    // Setup is not timed; never retry writes or ambiguous
                    // policy updates.
                    std::thread::sleep(Duration::from_millis(250));
                    continue;
                }
                Err(error) => return Err(error),
            };
            // This run-owned directory is empty at setup, so there is no old
            // content to reconcile. The update durably installs the rule; stat
            // below proves its effective value before any timed write. Waiting
            // on unrelated background repair scheduling would not strengthen
            // that proof. Cleanup removes the rule and the entire fixture.
            return Ok((update.reconciliation, attempt));
        }
        unreachable!("final placement attempt returns its result")
    };
    if prepared_base.is_none() {
        let (job, preview_attempts) =
            apply_policy(Some(policy.clone())).map_err(SinomoTestError::failed)?;
        ctx.record("placement_setup_job", &job);
        ctx.record("placement_setup_preview_attempts", preview_attempts);
    }
    let resolved = browser
        .stat(&base)
        .map_err(SinomoTestError::failed)?
        .placement
        .ok_or_else(|| SinomoTestError::failed("write fixture omitted resolved placement"))?;
    ctx.record("placement_before", &resolved);
    verify_write_latency_placement(&resolved, &base, &policy).map_err(SinomoTestError::failed)?;
    ctx.record("node", browser.node_id());
    ctx.record("iterations", iterations);
    ctx.record("warmup_iterations", warmup);
    ctx.record("payload_bytes", bytes);
    ctx.record("max_p95_us", max_p95_us);
    ctx.record("wait_for_commit", true);
    ctx.record("benchmark_retries", 0);
    ctx.record("retain_fixture", retain_fixture);
    ctx.record("setup_elapsed_ms", setup_started.elapsed().as_millis());
    ctx.record("timing_scope", "public daemon DFS write_file API through committed response; client RPC included; setup, reads and cleanup excluded");
    ctx.record(
        "percentiles",
        "nearest rank, separate create and overwrite groups",
    );
    let mut samples = Vec::new();
    let mut latency_us = BTreeMap::<&str, Vec<u64>>::new();
    let mut failure = None;
    'workload: for index in 0..warmup + iterations {
        let path = if prepared_base.is_some() {
            // Fresh names retain true create/overwrite pairs in a reused,
            // policy-verified fixture without capturing policy setup work.
            format!("{base}/profile-{}-{index:06}.bin", ctx.run_id())
        } else {
            format!("{base}/file-{index:06}.bin")
        };
        for operation in ["create", "overwrite"] {
            let seed = write_latency_payload_seed(operation, index, &path);
            let payload = pattern_payload(&seed, bytes);
            let started = Instant::now();
            // Do not wrap this in the probe retry helper: a failed operation
            // is evidence, and production retries remain inside the timer.
            let result = browser.write_file(&path, &payload);
            let elapsed_us = started.elapsed().as_micros() as u64;
            samples.push(serde_json::json!({
                "index": index, "operation": operation, "path": path,
                "payload_seed": seed, "elapsed_us": elapsed_us,
                "measured": index >= warmup, "succeeded": result.is_ok(),
                "error": result.as_ref().err(),
                "placement_verified": false,
            }));
            ctx.record("samples", &samples);
            if let Err(error) = result {
                failure = Some(format!("{operation} {path} failed: {error}"));
                break 'workload;
            }
            if index >= warmup {
                latency_us.entry(operation).or_default().push(elapsed_us);
            }
            // Check the file's effective policy outside the write timer. Do not
            // infer W/N from the caller, shard, or a desired replica count.
            let placement_check = (|| -> Result<(), String> {
                let resolved = browser
                    .stat(&path)?
                    .placement
                    .ok_or_else(|| "written file omitted resolved placement".to_owned())?;
                let sample = samples.last_mut().expect("just recorded write sample");
                sample["placement_policy"] =
                    serde_json::to_value(&resolved.policy).map_err(|error| error.to_string())?;
                sample["placement_source_path"] = serde_json::json!(resolved.source_path);
                verify_write_latency_placement(&resolved, &base, &policy)?;
                sample["placement_verified"] = serde_json::json!(true);
                Ok(())
            })();
            ctx.record("samples", &samples);
            if let Err(error) = placement_check {
                failure = Some(format!(
                    "{operation} {path} policy verification failed: {error}"
                ));
                break 'workload;
            }
            match browser.read_file(&path) {
                Ok(actual) if actual == payload => {}
                Ok(_) => {
                    failure = Some(format!("{operation} {path} read-your-write bytes mismatch"));
                    break 'workload;
                }
                Err(error) => {
                    failure = Some(format!(
                        "{operation} {path} read-your-write failed: {error}"
                    ));
                    break 'workload;
                }
            }
        }
    }
    ctx.record("correctness_passed", failure.is_none());
    let mut summaries = BTreeMap::new();
    for operation in ["create", "overwrite"] {
        let mut values = latency_us.remove(operation).unwrap_or_default();
        values.sort_unstable();
        let p95 = percentile_value(&values, 95);
        summaries.insert(
            operation,
            serde_json::json!({
                "count": values.len(), "p50_us": percentile_value(&values, 50),
                "p95_us": p95, "max_us": values.last().copied().unwrap_or(0),
                "mean_us": mean_value(&values),
            }),
        );
        if failure.is_none() && values.len() != iterations {
            failure = Some(format!(
                "{operation} measured {} samples, expected {iterations}",
                values.len()
            ));
        }
        if failure.is_none() && latency_reaches_exclusive_limit(p95, max_p95_us) {
            failure = Some(format!(
                "{operation} p95={p95}us is not below {max_p95_us}us"
            ));
        }
    }
    ctx.record("operation_latency_us", summaries);
    if !retain_fixture {
        let cleanup_started = Instant::now();
        let cleanup =
            apply_policy(None).and_then(|_| remove_scoped_sifs_test_tree(&browser, &base));
        ctx.record("cleanup_elapsed_ms", cleanup_started.elapsed().as_millis());
        ctx.record("cleanup_passed", cleanup.is_ok());
        if let Err(error) = cleanup {
            failure.get_or_insert_with(|| format!("fixture cleanup failed: {error}"));
        }
    }
    if let Some(error) = failure {
        return Ok(SinomoTestOutcome::failed(error));
    }
    Ok(SinomoTestOutcome::passed(format!(
        "ordinary committed writes ({replication}): {iterations} create and overwrite samples, {bytes} bytes each"
    )))
}

fn run_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let payload = required_payload_bytes(ctx)?;
    let foreground_repair = optional_bool_parameter(ctx, "foreground-repair", false)?;
    let max_retries = optional_usize_parameter(ctx, "max-retries", usize::MAX)?;
    let timeout = expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let mut retry_metrics = DfsMutationRetryMetrics::default();
    retry_bulk_mutation_with_metrics(
        deadline,
        || {
            if foreground_repair {
                backend
                    .write_file_with_foreground_replica_repair_deadline(&path, &payload, deadline)
            } else {
                backend.write_file_with_deadline(&path, &payload, deadline)
            }
        },
        || format!("cannot write SIFS file {path}"),
        &mut retry_metrics,
    )?;
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("bytes", payload.len());
    ctx.record("foreground_repair", foreground_repair);
    ctx.record("wait_for_commit", true);
    ctx.record("retry_count", retry_metrics.retry_count);
    ctx.record("retry_reasons", &retry_metrics.retry_reasons);
    ctx.record("max_retries", max_retries);
    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
    if retry_metrics.retry_count > max_retries {
        return Ok(SinomoTestOutcome::failed(format!(
            "wrote {} bytes to {path} on node {node}, but required at most {max_retries} retries and observed {}",
            payload.len(),
            retry_metrics.retry_count
        )));
    }
    Ok(SinomoTestOutcome::passed(format!(
        "wrote {} bytes to {path} on node {node}",
        payload.len()
    )))
}

fn run_expect_file(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let payload = required_payload_bytes(ctx)?;
    let timeout = expect_timeout(ctx)?;
    let max_proc_write_bytes = optional_u64_parameter(ctx, "max-proc-write-bytes")?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_read_only_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("bytes", payload.len());
    if let Some(max_proc_write_bytes) = max_proc_write_bytes {
        ctx.record("max_proc_write_bytes", max_proc_write_bytes);
    }
    let proc_io_before = current_proc_io();
    let mut last_state;
    loop {
        match backend.read_file(&path) {
            Ok(bytes) if bytes == payload => {
                ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                if let Some(proc_io_delta) = proc_io_before
                    .zip(current_proc_io())
                    .map(|(before, after)| after.saturating_delta(before))
                {
                    record_proc_io_delta_flat(ctx, proc_io_delta);
                    if let Some(max_proc_write_bytes) = max_proc_write_bytes
                        && proc_io_delta.write_bytes > max_proc_write_bytes
                    {
                        return Ok(SinomoTestOutcome::failed(format!(
                            "{path} matched on node {node}, but read proc write_bytes delta {} was above allowed {max_proc_write_bytes}",
                            proc_io_delta.write_bytes
                        )));
                    }
                }
                return Ok(SinomoTestOutcome::passed(format!(
                    "{path} matched the expected payload on node {node} after {}ms",
                    started.elapsed().as_millis()
                )));
            }
            Ok(bytes) => {
                last_state = format!(
                    "content mismatch ({} bytes, expected {}): {:?}",
                    bytes.len(),
                    payload.len(),
                    String::from_utf8_lossy(&bytes[..bytes.len().min(128)])
                );
            }
            Err(error) => last_state = error,
        }
        if Instant::now() >= deadline {
            ctx.record("last_state", &last_state);
            return Ok(SinomoTestOutcome::failed(format!(
                "{path} did not match the expected payload on node {node} within {}s: {last_state}",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn run_expect_replicated(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let min_locations = optional_usize_parameter(ctx, "min-locations", 2)?;
    let timeout = expect_timeout(ctx)?;
    require_positive("min-locations", min_locations)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_observation_until(ctx, deadline)?;
    let node = backend.node_label();
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("min_locations", min_locations);
    loop {
        let last_state = match backend.object_by_path(&path) {
            Ok(Some(view)) => {
                let object = &view.object;
                let mut current_locations = object.current_locations.clone();
                if let Some(content_hash) = object.content_hash.as_deref() {
                    for receipt in &view.receipts {
                        if receipt.receipt.committed_version == object.version
                            && receipt.content_hash.as_deref() == Some(content_hash)
                            && !current_locations.contains(&receipt.replica_node_id)
                        {
                            current_locations.push(receipt.replica_node_id.clone());
                        }
                    }
                }
                let size_bytes = view
                    .versions
                    .iter()
                    .find(|version| version.version == object.version)
                    .and_then(|version| version.size_bytes);
                let desired_replica_nodes = if matches!(
                    &object.policy,
                    sinomo_runtime::core::DfsObjectPolicy::ReplicatedConfirmed
                        | sinomo_runtime::core::DfsObjectPolicy::ReplicatedEventual
                ) {
                    match current_version_desired_replica_nodes(&view) {
                        Ok(desired_replica_nodes) if !desired_replica_nodes.is_empty() => {
                            desired_replica_nodes
                        }
                        Ok(_) => {
                            if Instant::now() >= deadline {
                                ctx.record(
                                    "last_state",
                                    "current replicated intent has no desired replica nodes",
                                );
                                return Ok(SinomoTestOutcome::failed(format!(
                                    "{path} did not reach {min_locations} current locations on node {node} within {}s: current replicated intent has no desired replica nodes",
                                    timeout.as_secs()
                                )));
                            }
                            std::thread::sleep(EXPECT_POLL_INTERVAL);
                            continue;
                        }
                        Err(error) => {
                            if Instant::now() >= deadline {
                                ctx.record("last_state", &error);
                                return Ok(SinomoTestOutcome::failed(format!(
                                    "{path} did not reach {min_locations} current locations on node {node} within {}s: {error}",
                                    timeout.as_secs()
                                )));
                            }
                            std::thread::sleep(EXPECT_POLL_INTERVAL);
                            continue;
                        }
                    }
                } else {
                    Vec::new()
                };
                let missing_desired_replica_nodes =
                    missing_desired_replica_nodes(&desired_replica_nodes, &current_locations);
                if object.content_hash.is_some()
                    && current_locations.len() >= min_locations
                    && missing_desired_replica_nodes.is_empty()
                {
                    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                    ctx.record("object_id", object.object_id.to_string());
                    ctx.record("version", object.version.to_string());
                    ctx.record(
                        "content_hash",
                        object.content_hash.clone().unwrap_or_default(),
                    );
                    ctx.record("size_bytes", size_bytes.unwrap_or_default());
                    ctx.record(
                        "current_locations",
                        stat_location_labels(&current_locations),
                    );
                    ctx.record(
                        "desired_replica_nodes",
                        stat_location_labels(&desired_replica_nodes),
                    );
                    return Ok(SinomoTestOutcome::passed(format!(
                        "{path} has all {} desired replicas among {} current locations on node {node} after {}ms",
                        desired_replica_nodes.len(),
                        current_locations.len(),
                        started.elapsed().as_millis()
                    )));
                }
                format!(
                    "locations={} current={:?} desired={:?} missing_desired={:?} content_hash={:?} size={:?}",
                    current_locations.len(),
                    stat_location_labels(&current_locations),
                    stat_location_labels(&desired_replica_nodes),
                    stat_location_labels(&missing_desired_replica_nodes),
                    object.content_hash,
                    size_bytes
                )
            }
            Ok(None) => format!("cannot inspect {path}: path not found"),
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            ctx.record("last_state", &last_state);
            return Ok(SinomoTestOutcome::failed(format!(
                "{path} did not reach {min_locations} current locations on node {node} within {}s: {last_state}",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn current_version_desired_replica_nodes(
    object: &dfs::DfsOperatorObjectView,
) -> Result<Vec<NodeId>, String> {
    object
        .intents
        .iter()
        .find(|intent| intent.target_version == object.object.version)
        .map(|intent| intent.desired_replica_node_ids.clone())
        .ok_or_else(|| {
            format!(
                "DFS inspection did not return the write intent for object {} version {}",
                object.object.object_id, object.object.version
            )
        })
}

fn missing_desired_replica_nodes(
    desired_replica_nodes: &[NodeId],
    current_locations: &[NodeId],
) -> Vec<NodeId> {
    desired_replica_nodes
        .iter()
        .filter(|node_id| !current_locations.contains(node_id))
        .cloned()
        .collect()
}

fn run_damage_local_loose_blob(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let backend = open_read_only_backend(ctx)?;
    let node = backend.node_label();
    let stat = backend
        .debug_remove_local_loose_blob_for_path(&path)
        .map_err(SinomoTestError::failed)?;
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("object_id", stat.object_id.to_string());
    ctx.record("version", stat.version.to_string());
    ctx.record("content_hash", stat.content_hash.unwrap_or_default());
    ctx.record("size_bytes", stat.size_bytes.unwrap_or_default());
    ctx.record(
        "current_locations",
        stat_location_labels(&stat.current_locations),
    );
    Ok(SinomoTestOutcome::passed(format!(
        "removed local loose blob for {path} on node {node}"
    )))
}

fn run_expect_absent(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let timeout = expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_read_only_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    ctx.record("node", &node);
    ctx.record("path", &path);
    let mut last_state;
    loop {
        match backend.stat(&path) {
            Err(error) if is_not_found(&error) => {
                ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                return Ok(SinomoTestOutcome::passed(format!(
                    "{path} is absent on node {node} after {}ms",
                    started.elapsed().as_millis()
                )));
            }
            Err(error) => last_state = error,
            Ok(metadata) => last_state = format!("still present ({:?})", metadata.kind),
        }
        if Instant::now() >= deadline {
            ctx.record("last_state", &last_state);
            return Ok(SinomoTestOutcome::failed(format!(
                "{path} is still present (or unreadable) on node {node} after {}s: {last_state}",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn run_rename(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let from = ctx.required_parameter("from")?.to_string();
    let to = ctx.required_parameter("to")?.to_string();
    let timeout = expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    let operation_id = format!(
        "cluster.dfs.rename:{}",
        blake3::hash(format!("{from}\0{to}").as_bytes()).to_hex()
    );
    // Cold fallback setup belongs to the whole operation budget. Reserving
    // retry time starts after setup, as it did before resident lazy clients.
    backend
        .prepare_stable_namespace_mutation_until(deadline)
        .map_err(SinomoTestError::failed)?;
    let mut reconcile_before_attempt = false;
    let mut retry_metrics = DfsMutationRetryMetrics::default();
    loop {
        let now = Instant::now();
        let attempt_budget = deadline
            .saturating_duration_since(now)
            .div_f64(2.0)
            .min(RENAME_ATTEMPT_MAX);
        let attempt_deadline = (now + attempt_budget).min(deadline);
        if reconcile_before_attempt {
            match backend.reconcile_rename_with_operation_id_and_deadline(
                &from,
                &to,
                &operation_id,
                attempt_deadline,
            ) {
                Ok(DfsStableNamespaceOperationReconciliation::Applied { .. }) => {
                    ctx.record("node", &node);
                    ctx.record("from", &from);
                    ctx.record("to", &to);
                    ctx.record("operation_id", &operation_id);
                    ctx.record("retry_count", retry_metrics.retry_count);
                    ctx.record("retry_reasons", &retry_metrics.retry_reasons);
                    ctx.record("reconciled", true);
                    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                    return Ok(SinomoTestOutcome::passed(format!(
                        "reconciled durable rename {from} to {to} on node {node} after {}ms",
                        started.elapsed().as_millis()
                    )));
                }
                Ok(DfsStableNamespaceOperationReconciliation::NotApplied)
                | Ok(DfsStableNamespaceOperationReconciliation::AmbiguousOrExpired { .. }) => {}
                Ok(DfsStableNamespaceOperationReconciliation::Conflict { reason }) => {
                    return Err(SinomoTestError::failed(format!(
                        "rename operation {operation_id} conflicts with durable history: {reason}"
                    )));
                }
                Err(error) if is_retryable_rename_error(&error) => {
                    retry_metrics.record_retry(&error);
                    if Instant::now() >= deadline {
                        ctx.record("last_state", &error);
                        return Ok(SinomoTestOutcome::failed(format!(
                            "rename {from} to {to} on node {node} could not reconcile within {}s: {error}",
                            timeout.as_secs()
                        )));
                    }
                    std::thread::sleep(EXPECT_POLL_INTERVAL);
                    continue;
                }
                Err(error) => return Err(SinomoTestError::failed(error)),
            }
        }
        match backend.rename_with_operation_id_and_deadline(
            &from,
            &to,
            &operation_id,
            attempt_deadline,
        ) {
            Ok(()) => {
                ctx.record("node", &node);
                ctx.record("from", &from);
                ctx.record("to", &to);
                ctx.record("operation_id", &operation_id);
                ctx.record("retry_count", retry_metrics.retry_count);
                ctx.record("retry_reasons", &retry_metrics.retry_reasons);
                ctx.record("reconciled", false);
                ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                return Ok(SinomoTestOutcome::passed(format!(
                    "renamed {from} to {to} on node {node} after {}ms",
                    started.elapsed().as_millis()
                )));
            }
            Err(error) if is_retryable_rename_error(&error) => {
                retry_metrics.record_retry(&error);
                reconcile_before_attempt = true;
                if Instant::now() >= deadline {
                    ctx.record("node", &node);
                    ctx.record("from", &from);
                    ctx.record("to", &to);
                    ctx.record("operation_id", &operation_id);
                    ctx.record("retry_count", retry_metrics.retry_count);
                    ctx.record("retry_reasons", &retry_metrics.retry_reasons);
                    ctx.record("last_state", &error);
                    return Ok(SinomoTestOutcome::failed(format!(
                        "rename {from} to {to} on node {node} did not complete within {}s: {error}",
                        timeout.as_secs()
                    )));
                }
                std::thread::sleep(EXPECT_POLL_INTERVAL);
            }
            Err(error) => return Err(SinomoTestError::failed(error)),
        }
    }
}

fn run_remove(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let timeout = expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    backend
        .remove_with_deadline(&path, deadline)
        .map_err(SinomoTestError::failed)?;
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
    Ok(SinomoTestOutcome::passed(format!(
        "removed {path} on node {node} after {}ms",
        started.elapsed().as_millis()
    )))
}

fn run_small_file_baseline(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let files = optional_usize_parameter(ctx, "files", DEFAULT_BULK_FILE_COUNT)?;
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_BULK_FILE_BYTES)?;
    let sync_writes = optional_bool_parameter(ctx, "sync-writes", false)?;
    let min_files_per_second = optional_f64_parameter(ctx, "min-files-per-second", 0.0)?;
    let max_proc_syscw_per_file = optional_f64_parameter(ctx, "max-proc-syscw-per-file", 0.0)?;
    require_positive("files", files)?;
    require_positive("bytes", bytes)?;

    let benchmark = raw_small_file_baseline(ctx, files, bytes, sync_writes)?;
    record_raw_small_file_baseline(ctx, &benchmark, "raw_small");
    ctx.record("min_files_per_second", min_files_per_second);
    ctx.record("max_proc_syscw_per_file", max_proc_syscw_per_file);

    if min_files_per_second > 0.0 && benchmark.files_per_second < min_files_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "raw small-file baseline wrote {} files in {}ms ({:.2} files/s), below required {:.2} files/s",
            benchmark.files,
            benchmark.elapsed.as_millis(),
            benchmark.files_per_second,
            min_files_per_second
        )));
    }
    if max_proc_syscw_per_file > 0.0
        && benchmark.proc_syscw_per_file.unwrap_or(0.0) > max_proc_syscw_per_file
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "raw small-file baseline wrote {} files with proc syscw/file {:.2}, above allowed {:.2}",
            benchmark.files,
            benchmark.proc_syscw_per_file.unwrap_or(0.0),
            max_proc_syscw_per_file
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "raw small-file baseline wrote {} files in {}ms ({:.2} files/s, {:.2} syscw/file)",
        benchmark.files,
        benchmark.elapsed.as_millis(),
        benchmark.files_per_second,
        benchmark.proc_syscw_per_file.unwrap_or(0.0)
    )))
}

struct DfsBulkWriteRequester {
    requester: SifsOneShotBulkWriteRequester,
    overwrite_existing: bool,
}

impl DfsBulkWriteRequester {
    fn open(ctx: &SinomoTestContext, overwrite_existing: bool) -> Result<Self, SinomoTestError> {
        let data_dir = SinomoDataDir::new(ctx.data_dir());
        SifsOneShotBulkWriteRequester::open(&data_dir, ctx.authoritative_cluster_snapshot())
            .map(|requester| Self {
                requester,
                overwrite_existing,
            })
            .map_err(|error| {
                SinomoTestError::failed(format!(
                    "cannot open one-shot SIFS bulk write requester: {error}"
                ))
            })
    }

    fn node_label(&self) -> String {
        self.requester.node_label()
    }

    fn create_dir_all_until(&self, path: &str, deadline: Instant) -> Result<(), String> {
        self.requester
            .create_dir_all_deferred_local_replay_until(path, deadline)
    }

    fn write_files_until(
        &self,
        files: &[(String, Vec<u8>)],
        deadline: Instant,
    ) -> Result<(), String> {
        self.requester
            .write_files_with_known_parents_deferred_publish_mode_until(
                files,
                self.overwrite_existing,
                deadline,
            )
    }

    fn publish_metadata_after_mutation(&self) -> Result<(), String> {
        self.requester.publish_metadata_after_mutation()
    }
}

fn run_bulk_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = ctx
        .required_parameter("base")?
        .trim_end_matches('/')
        .to_string();
    let files = optional_usize_parameter(ctx, "files", DEFAULT_BULK_FILE_COUNT)?;
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_BULK_FILE_BYTES)?;
    let overwrite_existing = optional_bool_parameter(ctx, "overwrite-existing", false)?;
    let timeout = expect_timeout(ctx)?;
    let min_files_per_second = optional_f64_parameter(ctx, "min-files-per-second", 0.0)?;
    let max_proc_wchar_amplification =
        optional_f64_parameter(ctx, "max-proc-wchar-amplification", 0.0)?;
    let max_proc_write_bytes_amplification =
        optional_f64_parameter(ctx, "max-proc-write-bytes-amplification", 0.0)?;
    let max_proc_syscw_per_file = optional_f64_parameter(ctx, "max-proc-syscw-per-file", 0.0)?;
    let max_proc_syscw_fixed = optional_u64_parameter(ctx, "max-proc-syscw-fixed")?.unwrap_or(0);
    require_positive("files", files)?;
    require_positive("bytes", bytes)?;

    let open_started = Instant::now();
    let backend = DfsBulkWriteRequester::open(ctx, overwrite_existing)?;
    let open_elapsed = open_started.elapsed();
    let node = backend.node_label();
    let proc_io_before = current_proc_io();
    let mut mkdir_retries = DfsMutationRetryMetrics::default();
    let mut write_retries = DfsMutationRetryMetrics::default();

    let started = Instant::now();
    let deadline = started + timeout;
    let mkdir_result = retry_bulk_mutation_with_metrics(
        deadline,
        || backend.create_dir_all_until(&base, deadline),
        || format!("cannot create SIFS directory {base}"),
        &mut mkdir_retries,
    );
    record_bulk_retry_metrics(ctx, &mkdir_retries, &write_retries);
    mkdir_result?;
    let mkdir_elapsed = started.elapsed();
    let proc_io_after_mkdir = current_proc_io();
    let batch_started = Instant::now();
    let batch = (0..files)
        .map(|index| (bulk_path(&base, index), bulk_payload(&base, index, bytes)))
        .collect::<Vec<_>>();
    let batch_build_elapsed = batch_started.elapsed();
    let write_started = Instant::now();
    let write_result = retry_bulk_mutation_with_metrics(
        deadline,
        || backend.write_files_until(&batch, deadline),
        || format!("cannot write SIFS bulk batch under {base}"),
        &mut write_retries,
    );
    record_bulk_retry_metrics(ctx, &mkdir_retries, &write_retries);
    write_result?;
    let write_elapsed = write_started.elapsed();
    let proc_io_after_write = current_proc_io();
    let publish_started = Instant::now();
    backend
        .publish_metadata_after_mutation()
        .map_err(SinomoTestError::failed)?;
    let publish_elapsed = publish_started.elapsed();
    let proc_io_after_publish = current_proc_io();
    let elapsed = started.elapsed();
    let proc_io_delta = proc_io_before
        .zip(current_proc_io())
        .map(|(before, after)| after.saturating_delta(before));
    let proc_io_mkdir_delta = proc_io_before
        .zip(proc_io_after_mkdir)
        .map(|(before, after)| after.saturating_delta(before));
    let proc_io_write_delta = proc_io_after_mkdir
        .zip(proc_io_after_write)
        .map(|(before, after)| after.saturating_delta(before));
    let proc_io_publish_delta = proc_io_after_write
        .zip(proc_io_after_publish)
        .map(|(before, after)| after.saturating_delta(before));
    let elapsed_ms = elapsed.as_millis() as u64;
    let total_bytes = (files as u64).saturating_mul(bytes as u64);
    let files_per_second = rate_per_second(files as f64, elapsed);
    let bytes_per_second = rate_per_second(total_bytes as f64, elapsed);

    ctx.record("node", &node);
    ctx.record("base", &base);
    ctx.record("files", files);
    ctx.record("bytes_per_file", bytes);
    ctx.record("overwrite_existing", overwrite_existing);
    ctx.record("total_bytes", total_bytes);
    ctx.record("open_elapsed_ms", open_elapsed.as_millis() as u64);
    ctx.record("elapsed_ms", elapsed_ms);
    ctx.record("mkdir_elapsed_ms", mkdir_elapsed.as_millis() as u64);
    ctx.record(
        "batch_build_elapsed_ms",
        batch_build_elapsed.as_millis() as u64,
    );
    ctx.record("write_elapsed_ms", write_elapsed.as_millis() as u64);
    ctx.record("publish_elapsed_ms", publish_elapsed.as_millis() as u64);
    ctx.record("files_per_second", files_per_second);
    ctx.record("bytes_per_second", bytes_per_second);
    ctx.record(
        "data_write_bytes_per_second",
        rate_per_second(total_bytes as f64, write_elapsed),
    );
    ctx.record("min_files_per_second", min_files_per_second);
    ctx.record("max_proc_wchar_amplification", max_proc_wchar_amplification);
    ctx.record(
        "max_proc_write_bytes_amplification",
        max_proc_write_bytes_amplification,
    );
    ctx.record("max_proc_syscw_per_file", max_proc_syscw_per_file);
    ctx.record("max_proc_syscw_fixed", max_proc_syscw_fixed);
    ctx.record(
        "proc_syscw_bulk_write_budget",
        bulk_write_syscw_budget(files, max_proc_syscw_per_file, max_proc_syscw_fixed),
    );
    if let Some(proc_io_delta) = proc_io_delta {
        let proc_wchar_amplification =
            amplification_ratio(proc_io_delta.wchar, total_bytes).unwrap_or(0.0);
        let proc_write_bytes_amplification =
            amplification_ratio(proc_io_delta.write_bytes, total_bytes).unwrap_or(0.0);
        let proc_syscw_per_file = per_file(proc_io_delta.syscw, files).unwrap_or(0.0);
        record_proc_io_delta_flat(ctx, proc_io_delta);
        ctx.record("proc_wchar_amplification", proc_wchar_amplification);
        ctx.record(
            "proc_write_bytes_amplification",
            proc_write_bytes_amplification,
        );
        ctx.record("proc_syscw_per_file", proc_syscw_per_file);
        if max_proc_wchar_amplification > 0.0
            && proc_wchar_amplification > max_proc_wchar_amplification
        {
            return Ok(SinomoTestOutcome::failed(format!(
                "bulk write on node {node} wrote {files} files with proc wchar amplification {proc_wchar_amplification:.2}x, above allowed {max_proc_wchar_amplification:.2}x"
            )));
        }
        if max_proc_write_bytes_amplification > 0.0
            && proc_write_bytes_amplification > max_proc_write_bytes_amplification
        {
            return Ok(SinomoTestOutcome::failed(format!(
                "bulk write on node {node} wrote {files} files with proc write_bytes amplification {proc_write_bytes_amplification:.2}x, above allowed {max_proc_write_bytes_amplification:.2}x"
            )));
        }
    }
    if let Some(delta) = proc_io_mkdir_delta {
        ctx.record("proc_write_bytes_after_mkdir_delta", delta.write_bytes);
        ctx.record("proc_wchar_after_mkdir_delta", delta.wchar);
        ctx.record("proc_syscw_after_mkdir_delta", delta.syscw);
    }
    if let Some(delta) = proc_io_write_delta {
        ctx.record("proc_write_bytes_bulk_write_delta", delta.write_bytes);
        ctx.record("proc_wchar_bulk_write_delta", delta.wchar);
        ctx.record("proc_syscw_bulk_write_delta", delta.syscw);
        ctx.record(
            "proc_syscw_bulk_write_per_file",
            per_file(delta.syscw, files).unwrap_or(0.0),
        );
        if max_proc_syscw_per_file > 0.0
            && !bulk_write_syscw_within_budget(
                delta.syscw,
                files,
                max_proc_syscw_per_file,
                max_proc_syscw_fixed,
            )
        {
            let budget =
                bulk_write_syscw_budget(files, max_proc_syscw_per_file, max_proc_syscw_fixed);
            return Ok(SinomoTestOutcome::failed(format!(
                "bulk data write on node {node} wrote {files} files with {} proc syscw, above affine budget {budget:.2} ({max_proc_syscw_per_file:.2}/file * {files} + {max_proc_syscw_fixed} fixed)",
                delta.syscw
            )));
        }
    }
    if let Some(delta) = proc_io_publish_delta {
        ctx.record("proc_write_bytes_publish_delta", delta.write_bytes);
        ctx.record("proc_wchar_publish_delta", delta.wchar);
        ctx.record("proc_syscw_publish_delta", delta.syscw);
    }

    if min_files_per_second > 0.0 && files_per_second < min_files_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "bulk write on node {node} wrote {files} files in {elapsed_ms}ms ({files_per_second:.2} files/s), below required {min_files_per_second:.2} files/s"
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "bulk write on node {node} wrote {files} files ({total_bytes} bytes, overwrite-existing={overwrite_existing}) in {elapsed_ms}ms ({files_per_second:.2} files/s)"
    )))
}

fn run_bulk_expect(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = ctx
        .required_parameter("base")?
        .trim_end_matches('/')
        .to_string();
    let files = optional_usize_parameter(ctx, "files", DEFAULT_BULK_FILE_COUNT)?;
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_BULK_FILE_BYTES)?;
    let timeout = expect_timeout(ctx)?;
    let min_files_per_second = optional_f64_parameter(ctx, "min-files-per-second", 0.0)?;
    let max_proc_write_bytes = optional_u64_parameter(ctx, "max-proc-write-bytes")?;
    require_positive("files", files)?;
    require_positive("bytes", bytes)?;

    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_read_only_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    let read_pool = backend
        .open_parallel_read_pool(files)
        .map_err(SinomoTestError::failed)?;
    let expected_started = Instant::now();
    let expected_payloads = (0..files)
        .map(|index| bulk_payload(&base, index, bytes))
        .collect::<Vec<_>>();
    let expected_payload_build_elapsed = expected_started.elapsed();
    let measurement_started = Instant::now();
    let proc_io_before = current_proc_io();
    let mut matched = vec![false; files];
    let mut matched_count = 0usize;
    let mut last_state = String::new();

    loop {
        let pending = (0..files)
            .filter(|index| !matched[*index])
            .map(|index| (index, bulk_path(&base, index)))
            .collect::<Vec<_>>();
        let pending_paths = pending
            .iter()
            .map(|(_, path)| path.clone())
            .collect::<Vec<_>>();
        let read_results = read_pool.read_files(&pending_paths);
        for ((index, path), result) in pending.into_iter().zip(read_results) {
            if Instant::now() >= deadline {
                break;
            }
            let expected = &expected_payloads[index];
            match result {
                Ok(actual) if actual.as_slice() == expected.as_slice() => {
                    matched[index] = true;
                    matched_count += 1;
                }
                Ok(actual) => {
                    last_state = format!(
                        "{path} content mismatch ({} bytes, expected {})",
                        actual.len(),
                        expected.len()
                    );
                }
                Err(error) => {
                    last_state = format!("{path}: {error}");
                }
            }
        }

        if matched_count == files {
            let elapsed = measurement_started.elapsed();
            let elapsed_ms = elapsed.as_millis() as u64;
            let total_bytes = (files as u64).saturating_mul(bytes as u64);
            let files_per_second = rate_per_second(files as f64, elapsed);
            let bytes_per_second = rate_per_second(total_bytes as f64, elapsed);
            let proc_io_delta = proc_io_before
                .zip(current_proc_io())
                .map(|(before, after)| after.saturating_delta(before));
            ctx.record("node", &node);
            ctx.record("base", &base);
            ctx.record("files", files);
            ctx.record("read_workers", read_pool.worker_count());
            ctx.record("bytes_per_file", bytes);
            ctx.record("total_bytes", total_bytes);
            ctx.record(
                "expected_payload_build_elapsed_ms",
                expected_payload_build_elapsed.as_millis() as u64,
            );
            ctx.record("elapsed_ms", elapsed_ms);
            ctx.record("files_per_second", files_per_second);
            ctx.record("bytes_per_second", bytes_per_second);
            ctx.record("min_files_per_second", min_files_per_second);
            if let Some(max_proc_write_bytes) = max_proc_write_bytes {
                ctx.record("max_proc_write_bytes", max_proc_write_bytes);
            }
            if let Some(proc_io_delta) = proc_io_delta {
                record_proc_io_delta_flat(ctx, proc_io_delta);
                if let Some(max_proc_write_bytes) = max_proc_write_bytes
                    && proc_io_delta.write_bytes > max_proc_write_bytes
                {
                    return Ok(SinomoTestOutcome::failed(format!(
                        "bulk expect on node {node} read {files} files with proc write_bytes delta {}, above allowed {max_proc_write_bytes}",
                        proc_io_delta.write_bytes
                    )));
                }
            }

            if min_files_per_second > 0.0 && files_per_second < min_files_per_second {
                return Ok(SinomoTestOutcome::failed(format!(
                    "bulk expect on node {node} read {files} files in {elapsed_ms}ms ({files_per_second:.2} files/s), below required {min_files_per_second:.2} files/s"
                )));
            }
            return Ok(SinomoTestOutcome::passed(format!(
                "bulk expect on node {node} read {files} files ({total_bytes} bytes) in {elapsed_ms}ms ({files_per_second:.2} files/s)"
            )));
        }

        if Instant::now() >= deadline {
            ctx.record("node", &node);
            ctx.record("base", &base);
            ctx.record("files", files);
            ctx.record("read_workers", read_pool.worker_count());
            ctx.record("matched_files", matched_count);
            ctx.record("last_state", &last_state);
            return Ok(SinomoTestOutcome::failed(format!(
                "bulk expect on node {node} matched {matched_count}/{files} files under {base} within {}s: {last_state}",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn run_disk_baseline(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_LARGE_FILE_BYTES)?;
    let sync_write = optional_bool_parameter(ctx, "sync-write", true)?;
    let min_write_bytes_per_second =
        optional_f64_parameter(ctx, "min-write-bytes-per-second", 0.0)?;
    let min_read_bytes_per_second = optional_f64_parameter(ctx, "min-read-bytes-per-second", 0.0)?;
    require_positive("bytes", bytes)?;

    let benchmark = raw_disk_baseline(ctx, bytes, "disk-baseline", sync_write)?;
    record_raw_disk_baseline(ctx, &benchmark, "raw_disk");
    ctx.record("min_write_bytes_per_second", min_write_bytes_per_second);
    ctx.record("min_read_bytes_per_second", min_read_bytes_per_second);

    if min_write_bytes_per_second > 0.0
        && benchmark.write_bytes_per_second < min_write_bytes_per_second
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "raw disk baseline wrote {} bytes in {}ms ({:.2} bytes/s), below required {:.2} bytes/s",
            benchmark.bytes,
            benchmark.write_elapsed.as_millis(),
            benchmark.write_bytes_per_second,
            min_write_bytes_per_second
        )));
    }
    if min_read_bytes_per_second > 0.0
        && benchmark.read_bytes_per_second < min_read_bytes_per_second
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "raw disk baseline read {} bytes in {}ms ({:.2} bytes/s), below required {:.2} bytes/s",
            benchmark.bytes,
            benchmark.read_elapsed.as_millis(),
            benchmark.read_bytes_per_second,
            min_read_bytes_per_second
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "raw disk baseline wrote {:.2} bytes/s and read {:.2} bytes/s for {} bytes",
        benchmark.write_bytes_per_second, benchmark.read_bytes_per_second, benchmark.bytes
    )))
}

fn run_large_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_LARGE_FILE_BYTES)?;
    let timeout = expect_timeout(ctx)?;
    let min_bytes_per_second = optional_f64_parameter(ctx, "min-bytes-per-second", 0.0)?;
    let measure_raw_write_baseline =
        optional_bool_parameter(ctx, "measure-raw-write-baseline", false)?;
    let min_raw_write_ratio = optional_f64_parameter(ctx, "min-raw-write-ratio", 0.0)?;
    let raw_sync_write = optional_bool_parameter(ctx, "raw-sync-write", true)?;
    let max_proc_wchar_amplification =
        optional_f64_parameter(ctx, "max-proc-wchar-amplification", 0.0)?;
    let max_proc_write_bytes_amplification =
        optional_f64_parameter(ctx, "max-proc-write-bytes-amplification", 0.0)?;
    require_positive("bytes", bytes)?;

    let payload_started = Instant::now();
    let payload = large_payload(&path, bytes);
    let payload_build_elapsed = payload_started.elapsed();
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let started = Instant::now();
    let deadline = started + timeout;
    let parent = parent_probe_path(&path);
    let mkdir_started = Instant::now();
    if let Some(parent) = parent.as_deref() {
        retry_bulk_mutation(
            deadline,
            || backend.create_dir_all_deferred_local_replay_until(parent, deadline),
            || format!("cannot create SIFS parent directory {parent}"),
        )?;
    }
    let mkdir_elapsed = mkdir_started.elapsed();
    let proc_io_before = current_proc_io();
    let write_started = Instant::now();
    retry_bulk_mutation(
        deadline,
        || backend.write_file_with_known_parent_deferred_publish_until(&path, &payload, deadline),
        || format!("cannot write large SIFS file {path}"),
    )?;
    let write_elapsed = write_started.elapsed();
    let proc_io_after_write = current_proc_io();
    let publish_started = Instant::now();
    backend
        .publish_metadata_after_mutation()
        .map_err(SinomoTestError::failed)?;
    let publish_elapsed = publish_started.elapsed();
    let proc_io_after_publish = current_proc_io();
    let elapsed = started.elapsed();
    let total_bytes = bytes as u64;
    let bytes_per_second = rate_per_second(total_bytes as f64, elapsed);
    let data_write_bytes_per_second = rate_per_second(total_bytes as f64, write_elapsed);
    let proc_io_delta = proc_io_before
        .zip(proc_io_after_publish)
        .map(|(before, after)| after.saturating_delta(before));
    let proc_io_write_delta = proc_io_before
        .zip(proc_io_after_write)
        .map(|(before, after)| after.saturating_delta(before));
    let proc_io_publish_delta = proc_io_after_write
        .zip(proc_io_after_publish)
        .map(|(before, after)| after.saturating_delta(before));
    // Publication and all measured DFS work are complete. The raw baseline
    // owns its own full payload and readback; retain only the saved scalar
    // diagnostics while it runs, rather than three large buffers plus a DFS
    // metadata owner in this same bounded process/container.
    drop(payload);
    drop(backend);
    let raw_baseline = if measure_raw_write_baseline || min_raw_write_ratio > 0.0 {
        Some(raw_disk_baseline(
            ctx,
            bytes,
            &format!("large-write-{path}"),
            raw_sync_write,
        )?)
    } else {
        None
    };

    ctx.record("node", &node);
    ctx.record("path", &path);
    if let Some(parent) = parent {
        ctx.record("parent", &parent);
    }
    ctx.record("bytes", total_bytes);
    ctx.record(
        "payload_build_elapsed_ms",
        payload_build_elapsed.as_millis() as u64,
    );
    ctx.record("elapsed_ms", elapsed.as_millis() as u64);
    ctx.record("mkdir_elapsed_ms", mkdir_elapsed.as_millis() as u64);
    ctx.record("write_elapsed_ms", write_elapsed.as_millis() as u64);
    ctx.record("publish_elapsed_ms", publish_elapsed.as_millis() as u64);
    ctx.record("bytes_per_second", bytes_per_second);
    ctx.record("data_write_bytes_per_second", data_write_bytes_per_second);
    ctx.record("min_bytes_per_second", min_bytes_per_second);
    ctx.record("measure_raw_write_baseline", measure_raw_write_baseline);
    ctx.record("min_raw_write_ratio", min_raw_write_ratio);
    if let Some(raw_baseline) = raw_baseline.as_ref() {
        record_raw_disk_baseline(ctx, raw_baseline, "raw_disk");
        ctx.record(
            "raw_write_ratio",
            ratio(
                data_write_bytes_per_second,
                raw_baseline.write_bytes_per_second,
            )
            .unwrap_or(0.0),
        );
    }
    ctx.record("max_proc_wchar_amplification", max_proc_wchar_amplification);
    ctx.record(
        "max_proc_write_bytes_amplification",
        max_proc_write_bytes_amplification,
    );
    if let Some(proc_io_delta) = proc_io_delta {
        let proc_wchar_amplification =
            amplification_ratio(proc_io_delta.wchar, total_bytes).unwrap_or(0.0);
        let proc_write_bytes_amplification =
            amplification_ratio(proc_io_delta.write_bytes, total_bytes).unwrap_or(0.0);
        record_proc_io_delta_flat(ctx, proc_io_delta);
        ctx.record("proc_wchar_amplification", proc_wchar_amplification);
        ctx.record(
            "proc_write_bytes_amplification",
            proc_write_bytes_amplification,
        );
        if let Some(delta) = proc_io_write_delta {
            ctx.record("proc_write_bytes_data_write_delta", delta.write_bytes);
            ctx.record("proc_wchar_data_write_delta", delta.wchar);
            ctx.record("proc_syscw_data_write_delta", delta.syscw);
        }
        if let Some(delta) = proc_io_publish_delta {
            ctx.record("proc_write_bytes_publish_delta", delta.write_bytes);
            ctx.record("proc_wchar_publish_delta", delta.wchar);
            ctx.record("proc_syscw_publish_delta", delta.syscw);
        }
        if max_proc_wchar_amplification > 0.0
            && proc_wchar_amplification > max_proc_wchar_amplification
        {
            return Ok(SinomoTestOutcome::failed(format!(
                "large write on node {node} wrote {total_bytes} bytes with proc wchar amplification {proc_wchar_amplification:.2}x, above allowed {max_proc_wchar_amplification:.2}x"
            )));
        }
        if max_proc_write_bytes_amplification > 0.0
            && proc_write_bytes_amplification > max_proc_write_bytes_amplification
        {
            return Ok(SinomoTestOutcome::failed(format!(
                "large write on node {node} wrote {total_bytes} bytes with proc write_bytes amplification {proc_write_bytes_amplification:.2}x, above allowed {max_proc_write_bytes_amplification:.2}x"
            )));
        }
    }

    if min_bytes_per_second > 0.0 && data_write_bytes_per_second < min_bytes_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "large write on node {node} wrote {total_bytes} bytes in {}ms ({data_write_bytes_per_second:.2} bytes/s), below required {min_bytes_per_second:.2} bytes/s",
            write_elapsed.as_millis()
        )));
    }
    if let Some(raw_baseline) = raw_baseline.as_ref()
        && min_raw_write_ratio > 0.0
    {
        let raw_ratio = ratio(
            data_write_bytes_per_second,
            raw_baseline.write_bytes_per_second,
        )
        .unwrap_or(0.0);
        if raw_ratio < min_raw_write_ratio {
            return Ok(SinomoTestOutcome::failed(format!(
                "large write on node {node} wrote {total_bytes} bytes at {data_write_bytes_per_second:.2} bytes/s, {raw_ratio:.2}x raw disk write baseline, below required {min_raw_write_ratio:.2}x"
            )));
        }
    }

    Ok(SinomoTestOutcome::passed(format!(
        "large write on node {node} wrote {total_bytes} bytes in {}ms ({data_write_bytes_per_second:.2} bytes/s)",
        write_elapsed.as_millis()
    )))
}

fn run_large_expect(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_LARGE_FILE_BYTES)?;
    let timeout = expect_timeout(ctx)?;
    let min_bytes_per_second = optional_f64_parameter(ctx, "min-bytes-per-second", 0.0)?;
    let min_raw_read_ratio = optional_f64_parameter(ctx, "min-raw-read-ratio", 0.0)?;
    let raw_sync_write = optional_bool_parameter(ctx, "raw-sync-write", true)?;
    let max_proc_write_bytes = optional_u64_parameter(ctx, "max-proc-write-bytes")?;
    require_positive("bytes", bytes)?;

    let payload_started = Instant::now();
    let expected = large_payload(&path, bytes);
    let payload_build_elapsed = payload_started.elapsed();
    let raw_baseline = if min_raw_read_ratio > 0.0 {
        Some(raw_disk_baseline(
            ctx,
            bytes,
            &format!("large-expect-{path}"),
            raw_sync_write,
        )?)
    } else {
        None
    };
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_read_only_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    let measurement_started = Instant::now();
    let proc_io_before = current_proc_io();
    let mut last_state = "not attempted".to_string();
    let mut last_state_elapsed_ms = 0_u64;
    let mut attempts = 0_u64;
    let mut mismatch_attempts = 0_u64;
    let mut error_attempts = 0_u64;
    let mut first_error = None::<String>;
    let mut unique_errors = BTreeSet::<String>::new();

    loop {
        attempts = attempts.saturating_add(1);
        let read_started = Instant::now();
        match backend.read_file(&path) {
            Ok(actual) => {
                let read_elapsed = read_started.elapsed();
                if actual.as_slice() != expected.as_slice() {
                    mismatch_attempts = mismatch_attempts.saturating_add(1);
                    last_state = format!(
                        "{path} content mismatch ({} bytes, expected {})",
                        actual.len(),
                        expected.len()
                    );
                    last_state_elapsed_ms = measurement_started.elapsed().as_millis() as u64;
                    if Instant::now() >= deadline {
                        ctx.record("node", &node);
                        ctx.record("path", &path);
                        ctx.record("bytes", bytes as u64);
                        ctx.record("attempts", attempts);
                        ctx.record("mismatch_attempts", mismatch_attempts);
                        ctx.record("error_attempts", error_attempts);
                        ctx.record("last_state_elapsed_ms", last_state_elapsed_ms);
                        if let Some(first_error) = first_error.as_ref() {
                            ctx.record("first_error", first_error);
                        }
                        ctx.record("unique_errors", &unique_errors);
                        ctx.record("last_state", &last_state);
                        return Ok(SinomoTestOutcome::failed(format!(
                            "large expect on node {node} did not read matching {bytes} byte file at {path} within {}s: {last_state}",
                            timeout.as_secs()
                        )));
                    }
                    std::thread::sleep(EXPECT_POLL_INTERVAL);
                    continue;
                }
                let elapsed = measurement_started.elapsed();
                let elapsed_ms = elapsed.as_millis() as u64;
                let read_elapsed_ms = read_elapsed.as_millis() as u64;
                let total_bytes = bytes as u64;
                let bytes_per_second = rate_per_second(total_bytes as f64, elapsed);
                let data_read_bytes_per_second = rate_per_second(total_bytes as f64, read_elapsed);
                let convergence_elapsed = elapsed.saturating_sub(read_elapsed);
                let proc_io_delta = proc_io_before
                    .zip(current_proc_io())
                    .map(|(before, after)| after.saturating_delta(before));
                ctx.record("node", &node);
                ctx.record("path", &path);
                ctx.record("bytes", total_bytes);
                ctx.record(
                    "expected_payload_build_elapsed_ms",
                    payload_build_elapsed.as_millis() as u64,
                );
                ctx.record("elapsed_ms", elapsed_ms);
                ctx.record(
                    "convergence_elapsed_ms",
                    convergence_elapsed.as_millis() as u64,
                );
                ctx.record("read_elapsed_ms", read_elapsed_ms);
                ctx.record("attempts", attempts);
                ctx.record("mismatch_attempts", mismatch_attempts);
                ctx.record("error_attempts", error_attempts);
                ctx.record("last_state_elapsed_ms", last_state_elapsed_ms);
                if let Some(first_error) = first_error.as_ref() {
                    ctx.record("first_error", first_error);
                }
                ctx.record("unique_errors", &unique_errors);
                if attempts > 1 {
                    ctx.record("last_state_before_success", &last_state);
                }
                ctx.record("bytes_per_second", bytes_per_second);
                ctx.record("data_read_bytes_per_second", data_read_bytes_per_second);
                ctx.record("min_bytes_per_second", min_bytes_per_second);
                ctx.record("min_raw_read_ratio", min_raw_read_ratio);
                if let Some(raw_baseline) = raw_baseline.as_ref() {
                    record_raw_disk_baseline(ctx, raw_baseline, "raw_disk");
                    ctx.record(
                        "raw_read_ratio",
                        ratio(
                            data_read_bytes_per_second,
                            raw_baseline.read_bytes_per_second,
                        )
                        .unwrap_or(0.0),
                    );
                }
                if let Some(max_proc_write_bytes) = max_proc_write_bytes {
                    ctx.record("max_proc_write_bytes", max_proc_write_bytes);
                }
                if let Some(proc_io_delta) = proc_io_delta {
                    record_proc_io_delta_flat(ctx, proc_io_delta);
                    if let Some(max_proc_write_bytes) = max_proc_write_bytes
                        && proc_io_delta.write_bytes > max_proc_write_bytes
                    {
                        return Ok(SinomoTestOutcome::failed(format!(
                            "large expect on node {node} read {total_bytes} bytes with proc write_bytes delta {}, above allowed {max_proc_write_bytes}",
                            proc_io_delta.write_bytes
                        )));
                    }
                }

                if min_bytes_per_second > 0.0 && data_read_bytes_per_second < min_bytes_per_second {
                    return Ok(SinomoTestOutcome::failed(format!(
                        "large expect on node {node} read {total_bytes} bytes in {read_elapsed_ms}ms ({data_read_bytes_per_second:.2} bytes/s), below required {min_bytes_per_second:.2} bytes/s"
                    )));
                }
                if let Some(raw_baseline) = raw_baseline.as_ref()
                    && min_raw_read_ratio > 0.0
                {
                    let raw_ratio = ratio(
                        data_read_bytes_per_second,
                        raw_baseline.read_bytes_per_second,
                    )
                    .unwrap_or(0.0);
                    if raw_ratio < min_raw_read_ratio {
                        return Ok(SinomoTestOutcome::failed(format!(
                            "large expect on node {node} read {total_bytes} bytes at {data_read_bytes_per_second:.2} bytes/s, {raw_ratio:.2}x raw disk read baseline, below required {min_raw_read_ratio:.2}x"
                        )));
                    }
                }
                return Ok(SinomoTestOutcome::passed(format!(
                    "large expect on node {node} matched after {elapsed_ms}ms; final read {total_bytes} bytes in {read_elapsed_ms}ms ({data_read_bytes_per_second:.2} bytes/s)"
                )));
            }
            Err(error) => {
                error_attempts = error_attempts.saturating_add(1);
                last_state = format!("{path}: {error}");
                first_error.get_or_insert_with(|| last_state.clone());
                if unique_errors.len() < MAX_RECORDED_BULK_RETRY_REASONS {
                    unique_errors.insert(last_state.clone());
                }
                last_state_elapsed_ms = measurement_started.elapsed().as_millis() as u64;
            }
        }

        if Instant::now() >= deadline {
            ctx.record("node", &node);
            ctx.record("path", &path);
            ctx.record("bytes", bytes as u64);
            ctx.record("attempts", attempts);
            ctx.record("mismatch_attempts", mismatch_attempts);
            ctx.record("error_attempts", error_attempts);
            ctx.record("last_state_elapsed_ms", last_state_elapsed_ms);
            if let Some(first_error) = first_error.as_ref() {
                ctx.record("first_error", first_error);
            }
            ctx.record("unique_errors", &unique_errors);
            ctx.record("last_state", &last_state);
            return Ok(SinomoTestOutcome::failed(format!(
                "large expect on node {node} did not read matching {bytes} byte file at {path} within {}s: {last_state}",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn open_backend(ctx: &SinomoTestContext) -> Result<SifsFileManagerBackend, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    SifsFileManagerBackend::open(&data_dir)
        .map_err(|error| SinomoTestError::failed(format!("cannot open SIFS backend: {error}")))
}

fn open_backend_with_deadline(
    ctx: &SinomoTestContext,
    deadline: Instant,
) -> Result<SifsFileManagerBackend, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    SifsFileManagerBackend::open_with_deadline(&data_dir, deadline)
        .map_err(|error| SinomoTestError::failed(format!("cannot open SIFS backend: {error}")))
}

fn open_observation(
    ctx: &SinomoTestContext,
) -> Result<SifsFileManagerObservation, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    SifsFileManagerObservation::open(&data_dir)
        .map_err(|error| SinomoTestError::failed(format!("cannot open SIFS observation: {error}")))
}

fn open_observation_with_deadline(
    ctx: &SinomoTestContext,
    deadline: Instant,
) -> Result<SifsFileManagerObservation, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    SifsFileManagerObservation::open_with_deadline(&data_dir, deadline)
        .map_err(|error| SinomoTestError::failed(format!("cannot open SIFS observation: {error}")))
}

fn open_read_only_backend(
    ctx: &SinomoTestContext,
) -> Result<SifsFileManagerBackend, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    SifsFileManagerBackend::open_read_only(&data_dir).map_err(|error| {
        SinomoTestError::failed(format!("cannot open read-only SIFS backend: {error}"))
    })
}

fn open_read_only_backend_with_deadline(
    ctx: &SinomoTestContext,
    deadline: Instant,
) -> Result<SifsFileManagerBackend, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    SifsFileManagerBackend::open_read_only_with_deadline(&data_dir, deadline).map_err(|error| {
        SinomoTestError::failed(format!("cannot open read-only SIFS backend: {error}"))
    })
}

// The expect-* probes are run on nodes that may still be converging (right
// after formation or a restart), where opening the backend itself can fail
// transiently — keep retrying within the probe's own deadline.
fn open_backend_until(
    ctx: &SinomoTestContext,
    deadline: Instant,
) -> Result<SifsFileManagerBackend, SinomoTestError> {
    retry_backend_open_until(deadline, "SIFS backend", || {
        open_backend_with_deadline(ctx, deadline)
    })
}

fn open_observation_until(
    ctx: &SinomoTestContext,
    deadline: Instant,
) -> Result<SifsFileManagerObservation, SinomoTestError> {
    retry_backend_open_until(deadline, "SIFS observation", || {
        open_observation_with_deadline(ctx, deadline)
    })
}

fn open_read_only_backend_until(
    ctx: &SinomoTestContext,
    deadline: Instant,
) -> Result<SifsFileManagerBackend, SinomoTestError> {
    retry_backend_open_until(deadline, "read-only SIFS backend", || {
        open_read_only_backend_with_deadline(ctx, deadline)
    })
}

fn retry_backend_open_until<T>(
    deadline: Instant,
    label: &str,
    mut open: impl FnMut() -> Result<T, SinomoTestError>,
) -> Result<T, SinomoTestError> {
    loop {
        match open() {
            Ok(_) if Instant::now() >= deadline => {
                return Err(SinomoTestError::failed(format!(
                    "{label} opened after the operation deadline"
                )));
            }
            Ok(backend) => return Ok(backend),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                std::thread::sleep(EXPECT_POLL_INTERVAL.min(remaining));
            }
        }
    }
}

#[derive(Default)]
struct DfsMutationRetryMetrics {
    retry_count: usize,
    retry_reasons: Vec<String>,
}

impl DfsMutationRetryMetrics {
    fn record_retry(&mut self, reason: &str) {
        self.retry_count = self.retry_count.saturating_add(1);
        if self.retry_reasons.len() < MAX_RECORDED_BULK_RETRY_REASONS {
            self.retry_reasons.push(reason.to_string());
        }
    }

    fn truncated_reason_count(&self) -> usize {
        self.retry_count.saturating_sub(self.retry_reasons.len())
    }
}

fn record_bulk_retry_metrics(
    ctx: &mut SinomoTestContext,
    mkdir: &DfsMutationRetryMetrics,
    write: &DfsMutationRetryMetrics,
) {
    let retry_count = mkdir.retry_count.saturating_add(write.retry_count);
    let retry_reasons = mkdir
        .retry_reasons
        .iter()
        .map(|reason| format!("mkdir: {reason}"))
        .chain(
            write
                .retry_reasons
                .iter()
                .map(|reason| format!("write: {reason}")),
        )
        .take(MAX_RECORDED_BULK_RETRY_REASONS)
        .collect::<Vec<_>>();
    ctx.record("retry_count", retry_count);
    ctx.record(
        "retry_reasons_truncated",
        retry_count.saturating_sub(retry_reasons.len()),
    );
    ctx.record("retry_reasons", retry_reasons);
    ctx.record("mkdir_retry_count", mkdir.retry_count);
    ctx.record("mkdir_retry_reasons", &mkdir.retry_reasons);
    ctx.record(
        "mkdir_retry_reasons_truncated",
        mkdir.truncated_reason_count(),
    );
    ctx.record("write_retry_count", write.retry_count);
    ctx.record("write_retry_reasons", &write.retry_reasons);
    ctx.record(
        "write_retry_reasons_truncated",
        write.truncated_reason_count(),
    );
}

fn retry_bulk_mutation<T, E, F, A>(
    deadline: Instant,
    operation: F,
    action: A,
) -> Result<T, SinomoTestError>
where
    F: FnMut() -> Result<T, E>,
    E: ToString,
    A: Fn() -> String,
{
    retry_bulk_mutation_observed(deadline, operation, action, |_| {})
}

fn retry_bulk_mutation_with_metrics<T, E, F, A>(
    deadline: Instant,
    operation: F,
    action: A,
    metrics: &mut DfsMutationRetryMetrics,
) -> Result<T, SinomoTestError>
where
    F: FnMut() -> Result<T, E>,
    E: ToString,
    A: Fn() -> String,
{
    retry_bulk_mutation_observed(deadline, operation, action, |reason| {
        metrics.record_retry(reason);
    })
}

fn retry_bulk_mutation_observed<T, E, F, A, O>(
    deadline: Instant,
    mut operation: F,
    action: A,
    mut on_retry: O,
) -> Result<T, SinomoTestError>
where
    F: FnMut() -> Result<T, E>,
    E: ToString,
    A: Fn() -> String,
    O: FnMut(&str),
{
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                let error = error.to_string();
                if Instant::now() >= deadline || !is_retryable_dfs_probe_error(&error) {
                    return Err(SinomoTestError::failed(format!("{}: {error}", action())));
                }
                on_retry(&error);
                std::thread::sleep(EXPECT_POLL_INTERVAL);
            }
        }
    }
}

struct DfsProtocolCodecSample {
    label: &'static str,
    route: &'static str,
    head: DfsAttachmentMessageCommand,
}

struct EncodedDfsProtocolCodecSample {
    label: &'static str,
    request: sinomo_runtime::network::ObjectRpcRequest,
    encoded_head_bytes: usize,
}

fn dfs_protocol_codec_samples(payload: &[u8]) -> Vec<DfsProtocolCodecSample> {
    let source_node_id = NodeId::from_legacy_named("dfs_codec_source").expect("source node id");
    let target_node_id = NodeId::from_legacy_named("dfs_codec_target").expect("target node id");
    let file_system_id =
        DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("filesystem id");
    let object_id = DfsObjectId::from_legacy_named("dfs_codec_object").expect("object id");
    let version = DfsVersion::from_legacy_named("dfs_codec_version").expect("version id");
    let attachment = DfsRpcAttachmentDescriptor {
        total_size_bytes: u64::try_from(payload.len()).expect("DFS codec payload fits u64"),
        content_hash: Some(format!("blake3:{}", blake3::hash(payload).to_hex())),
    };

    vec![
        dfs_protocol_codec_sample(
            "stage-blob-attachment-head",
            dfs::DFS_DATA_STAGE_BLOB_ROUTE,
            1,
            1,
            &source_node_id,
            &target_node_id,
            DfsNetworkMessage::StageBlob(DfsStageBlobCommand {
                request_id: "dfs-codec-stage".to_string(),
                intent_id: "dfs-codec-intent".to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: None,
                version: version.clone(),
                content_hash: attachment.content_hash.clone().expect("content hash"),
                bytes: Vec::new(),
                object_metadata: None,
                authority_shard: None,
            }),
            attachment.clone(),
        ),
        dfs_protocol_codec_sample(
            "read-response-attachment-head",
            dfs::DFS_DATA_READ_RESPONSE_ROUTE,
            2,
            2,
            &target_node_id,
            &source_node_id,
            DfsNetworkMessage::ReadResponse(DfsReadResponse {
                request_id: "dfs-codec-read".to_string(),
                object_id: object_id.clone(),
                result: DfsReadResult::Found(DfsReadData {
                    object_id: object_id.clone(),
                    version: version.clone(),
                    content_hash: attachment.content_hash.clone(),
                    bytes: Vec::new(),
                }),
            }),
            attachment.clone(),
        ),
        dfs_protocol_codec_sample(
            "fetch-blob-response-attachment-head",
            dfs::DFS_DATA_FETCH_BLOB_RESPONSE_ROUTE,
            3,
            3,
            &target_node_id,
            &source_node_id,
            DfsNetworkMessage::FetchBlobResponse(DfsFetchBlobResponse {
                request_id: "dfs-codec-fetch".to_string(),
                intent_id: "dfs-codec-intent".to_string(),
                object_id,
                version,
                source_node_id: target_node_id.clone(),
                result: DfsFetchBlobResult::Found {
                    content_hash: attachment.content_hash.clone().expect("content hash"),
                    bytes: Vec::new(),
                },
            }),
            attachment,
        ),
    ]
}

fn dfs_protocol_codec_sample(
    label: &'static str,
    route: &'static str,
    frame_id: u64,
    message_id: u64,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    message: DfsNetworkMessage,
    attachment: DfsRpcAttachmentDescriptor,
) -> DfsProtocolCodecSample {
    DfsProtocolCodecSample {
        label,
        route,
        head: DfsAttachmentMessageCommand {
            command: DfsMessageCommand {
                request_id: format!("dfs-codec-{label}-{frame_id}"),
                routed_message: DfsRoutedMessage {
                    source_node_id: source_node_id.clone(),
                    target_node_id: target_node_id.clone(),
                    frame_id,
                    message_id,
                    message,
                },
            },
            attachment,
        },
    }
}

fn encode_dfs_protocol_codec_head(
    sample: &DfsProtocolCodecSample,
) -> Result<sinomo_runtime::network::ObjectRpcRequest, SinomoTestError> {
    sinomo_runtime::network::ObjectRpcRequest::encode_body(
        sample.route,
        dfs::DFS_RPC_CATEGORY,
        &sample.head,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot encode {} DFS attachment request head: {error}",
            sample.label
        ))
    })
}

fn validate_decoded_dfs_protocol_codec_head(
    head: &DfsAttachmentMessageCommand,
    expected_attachment_bytes: usize,
    expected_attachment_hash: &str,
) -> Result<(), SinomoTestError> {
    let expected_attachment_bytes = u64::try_from(expected_attachment_bytes)
        .map_err(|_| SinomoTestError::failed("DFS codec attachment length does not fit u64"))?;
    if head.attachment.total_size_bytes != expected_attachment_bytes {
        return Err(SinomoTestError::failed(format!(
            "decoded DFS attachment descriptor declared {} bytes, expected {expected_attachment_bytes}",
            head.attachment.total_size_bytes
        )));
    }
    if head.attachment.content_hash.as_deref() != Some(expected_attachment_hash) {
        return Err(SinomoTestError::failed(format!(
            "decoded DFS attachment descriptor hash {:?} did not match attachment hash {expected_attachment_hash}",
            head.attachment.content_hash
        )));
    }
    if head.command.routed_message.source_node_id == head.command.routed_message.target_node_id {
        return Err(SinomoTestError::failed(
            "decoded DFS attachment head did not preserve distinct source and target nodes",
        ));
    }

    let (message_hash, embedded_bytes) = match &head.command.routed_message.message {
        DfsNetworkMessage::StageBlob(command) => (
            Some(command.content_hash.as_str()),
            command.bytes.as_slice(),
        ),
        DfsNetworkMessage::ReadResponse(response) => match &response.result {
            DfsReadResult::Found(data) => (data.content_hash.as_deref(), data.bytes.as_slice()),
            other => {
                return Err(SinomoTestError::failed(format!(
                    "decoded DFS read response attachment head was not found data: {other:?}"
                )));
            }
        },
        DfsNetworkMessage::FetchBlobResponse(response) => match &response.result {
            DfsFetchBlobResult::Found {
                content_hash,
                bytes,
            } => (Some(content_hash.as_str()), bytes.as_slice()),
            other => {
                return Err(SinomoTestError::failed(format!(
                    "decoded DFS fetch response attachment head was not found data: {other:?}"
                )));
            }
        },
        other => {
            return Err(SinomoTestError::failed(format!(
                "unexpected DFS attachment protocol message variant: {other:?}"
            )));
        }
    };
    if message_hash != Some(expected_attachment_hash) {
        return Err(SinomoTestError::failed(format!(
            "decoded DFS message head hash {message_hash:?} did not match attachment hash {expected_attachment_hash}"
        )));
    }
    if !embedded_bytes.is_empty() {
        return Err(SinomoTestError::failed(format!(
            "decoded DFS attachment head embedded {} attachment bytes in SOBJ",
            embedded_bytes.len()
        )));
    }
    Ok(())
}

fn expect_timeout(ctx: &SinomoTestContext) -> Result<Duration, SinomoTestError> {
    let seconds = match ctx.parameter("timeout-seconds") {
        Some(raw) => raw.trim().parse::<u64>().map_err(|_| {
            SinomoTestError::failed(format!(
                "timeout-seconds must be a non-negative integer, got `{raw}`"
            ))
        })?,
        None => DEFAULT_EXPECT_TIMEOUT_SECONDS,
    };
    Ok(Duration::from_secs(seconds))
}

fn optional_usize_parameter(
    ctx: &SinomoTestContext,
    key: &str,
    default: usize,
) -> Result<usize, SinomoTestError> {
    match ctx.parameter(key) {
        Some(raw) => {
            raw.trim()
                .parse::<usize>()
                .map_err(|source| SinomoTestError::InvalidParameter {
                    name: key.to_string(),
                    detail: source.to_string(),
                })
        }
        None => Ok(default),
    }
}

fn optional_f64_parameter(
    ctx: &SinomoTestContext,
    key: &str,
    default: f64,
) -> Result<f64, SinomoTestError> {
    match ctx.parameter(key) {
        Some(raw) => {
            raw.trim()
                .parse::<f64>()
                .map_err(|source| SinomoTestError::InvalidParameter {
                    name: key.to_string(),
                    detail: source.to_string(),
                })
        }
        None => Ok(default),
    }
}

fn optional_bool_parameter(
    ctx: &SinomoTestContext,
    key: &str,
    default: bool,
) -> Result<bool, SinomoTestError> {
    match ctx.parameter(key) {
        Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => Err(SinomoTestError::InvalidParameter {
                name: key.to_string(),
                detail: format!("expected boolean, got {other}"),
            }),
        },
        None => Ok(default),
    }
}

fn optional_u64_parameter(
    ctx: &SinomoTestContext,
    key: &str,
) -> Result<Option<u64>, SinomoTestError> {
    match ctx.parameter(key) {
        Some(raw) => raw.trim().parse::<u64>().map(Some).map_err(|source| {
            SinomoTestError::InvalidParameter {
                name: key.to_string(),
                detail: source.to_string(),
            }
        }),
        None => Ok(None),
    }
}

fn require_positive(name: &str, value: usize) -> Result<(), SinomoTestError> {
    if value == 0 {
        return Err(SinomoTestError::InvalidParameter {
            name: name.to_string(),
            detail: "must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn required_payload_bytes(ctx: &SinomoTestContext) -> Result<Vec<u8>, SinomoTestError> {
    if let Some(payload) = ctx.parameter("payload") {
        return Ok(payload.as_bytes().to_vec());
    }
    let bytes = optional_usize_parameter(ctx, "payload-bytes", 0)?;
    if bytes == 0 {
        return Err(SinomoTestError::failed(
            "missing payload: pass --param payload=... or --param payload-bytes=N".to_string(),
        ));
    }
    let seed = ctx
        .parameter("payload-seed")
        .unwrap_or("sinomo-dfs-payload");
    Ok(pattern_payload(seed, bytes))
}

fn pattern_payload(seed: &str, bytes: usize) -> Vec<u8> {
    let seed = format!("{seed}|sinomo-dfs-payload|");
    let mut payload = Vec::with_capacity(bytes);
    while payload.len() < bytes {
        payload.extend_from_slice(seed.as_bytes());
    }
    payload.truncate(bytes);
    payload
}

fn stat_location_labels(locations: &[sinomo_runtime::node::NodeId]) -> Vec<String> {
    locations.iter().map(ToString::to_string).collect()
}

fn bulk_path(base: &str, index: usize) -> String {
    format!("{}/bulk-{index:06}.bin", base.trim_end_matches('/'))
}

fn bulk_payload(base: &str, index: usize, bytes: usize) -> Vec<u8> {
    let seed = format!("{index:08}|{base}|sinomo-dfs-bulk|");
    let mut payload = Vec::with_capacity(bytes);
    while payload.len() < bytes {
        payload.extend_from_slice(seed.as_bytes());
    }
    payload.truncate(bytes);
    payload
}

fn large_payload(path: &str, bytes: usize) -> Vec<u8> {
    pattern_payload(&format!("{path}|sinomo-dfs-large"), bytes)
}

#[derive(Clone, Debug)]
struct RawSmallFileBaseline {
    root: String,
    files: usize,
    bytes_per_file: usize,
    total_bytes: u64,
    sync_writes: bool,
    batch_build_elapsed: Duration,
    elapsed: Duration,
    files_per_second: f64,
    bytes_per_second: f64,
    proc_io_delta: Option<ProcIoSample>,
    proc_syscw_per_file: Option<f64>,
}

fn raw_small_file_baseline(
    ctx: &SinomoTestContext,
    files: usize,
    bytes: usize,
    sync_writes: bool,
) -> Result<RawSmallFileBaseline, SinomoTestError> {
    require_positive("files", files)?;
    require_positive("bytes", bytes)?;
    let root = ctx
        .data_dir()
        .join("test-artifacts")
        .join("dfs-small-file-baseline")
        .join(probe_nonce());
    fs::create_dir_all(&root).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot create raw small-file baseline directory {}: {source}",
            root.display()
        ))
    })?;
    let batch_started = Instant::now();
    let batch = (0..files)
        .map(|index| {
            (
                root.join(format!("raw-{index:06}.bin")),
                bulk_payload("raw-small-file-baseline", index, bytes),
            )
        })
        .collect::<Vec<_>>();
    let batch_build_elapsed = batch_started.elapsed();
    let proc_io_before = current_proc_io();
    let started = Instant::now();
    for (path, payload) in &batch {
        let mut file = fs::File::create(path).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot create raw small-file baseline file {}: {source}",
                path.display()
            ))
        })?;
        file.write_all(payload).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot write raw small-file baseline file {}: {source}",
                path.display()
            ))
        })?;
        if sync_writes {
            file.sync_all().map_err(|source| {
                SinomoTestError::failed(format!(
                    "cannot sync raw small-file baseline file {}: {source}",
                    path.display()
                ))
            })?;
        }
    }
    let elapsed = started.elapsed();
    let proc_io_delta = proc_io_before
        .zip(current_proc_io())
        .map(|(before, after)| after.saturating_delta(before));
    fs::remove_dir_all(&root).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot remove raw small-file baseline directory {}: {source}",
            root.display()
        ))
    })?;

    let total_bytes = (files as u64).saturating_mul(bytes as u64);
    Ok(RawSmallFileBaseline {
        root: root.display().to_string(),
        files,
        bytes_per_file: bytes,
        total_bytes,
        sync_writes,
        batch_build_elapsed,
        elapsed,
        files_per_second: rate_per_second(files as f64, elapsed),
        bytes_per_second: rate_per_second(total_bytes as f64, elapsed),
        proc_io_delta,
        proc_syscw_per_file: proc_io_delta.and_then(|delta| per_file(delta.syscw, files)),
    })
}

fn record_raw_small_file_baseline(
    ctx: &mut SinomoTestContext,
    baseline: &RawSmallFileBaseline,
    prefix: &str,
) {
    ctx.record(format!("{prefix}_root"), &baseline.root);
    ctx.record(format!("{prefix}_files"), baseline.files);
    ctx.record(format!("{prefix}_bytes_per_file"), baseline.bytes_per_file);
    ctx.record(format!("{prefix}_total_bytes"), baseline.total_bytes);
    ctx.record(format!("{prefix}_sync_writes"), baseline.sync_writes);
    ctx.record(
        format!("{prefix}_batch_build_elapsed_ms"),
        baseline.batch_build_elapsed.as_millis() as u64,
    );
    ctx.record(
        format!("{prefix}_elapsed_ms"),
        baseline.elapsed.as_millis() as u64,
    );
    ctx.record(
        format!("{prefix}_files_per_second"),
        baseline.files_per_second,
    );
    ctx.record(
        format!("{prefix}_bytes_per_second"),
        baseline.bytes_per_second,
    );
    if let Some(delta) = baseline.proc_io_delta {
        record_proc_io_delta(ctx, prefix, "write", delta);
    }
    if let Some(syscw_per_file) = baseline.proc_syscw_per_file {
        ctx.record(format!("{prefix}_proc_syscw_per_file"), syscw_per_file);
    }
}

#[derive(Clone, Debug)]
struct RawDiskBaseline {
    path: String,
    bytes: u64,
    sync_write: bool,
    payload_build_elapsed: Duration,
    write_elapsed: Duration,
    write_dir_sync_elapsed: Duration,
    read_elapsed: Duration,
    write_bytes_per_second: f64,
    read_bytes_per_second: f64,
    write_proc_io_delta: Option<ProcIoSample>,
    read_proc_io_delta: Option<ProcIoSample>,
}

fn raw_disk_baseline(
    ctx: &SinomoTestContext,
    bytes: usize,
    seed: &str,
    sync_write: bool,
) -> Result<RawDiskBaseline, SinomoTestError> {
    require_positive("bytes", bytes)?;
    let root = ctx
        .data_dir()
        .join("test-artifacts")
        .join("dfs-disk-baseline");
    fs::create_dir_all(&root).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot create raw disk baseline directory {}: {source}",
            root.display()
        ))
    })?;
    let path = root.join(format!("baseline-{}.bin", probe_nonce()));
    let payload_started = Instant::now();
    let payload = pattern_payload(seed, bytes);
    let payload_build_elapsed = payload_started.elapsed();

    let write_proc_before = current_proc_io();
    let write_started = Instant::now();
    {
        let mut file = fs::File::create(&path).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot create raw disk baseline file {}: {source}",
                path.display()
            ))
        })?;
        file.write_all(&payload).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot write raw disk baseline file {}: {source}",
                path.display()
            ))
        })?;
        if sync_write {
            file.sync_all().map_err(|source| {
                SinomoTestError::failed(format!(
                    "cannot sync raw disk baseline file {}: {source}",
                    path.display()
                ))
            })?;
        }
    }
    let write_dir_sync_started = Instant::now();
    if sync_write && let Some(parent) = path.parent() {
        sync_raw_disk_baseline_dir(parent)?;
    }
    let write_dir_sync_elapsed = write_dir_sync_started.elapsed();
    let write_elapsed = write_started.elapsed();
    let write_proc_after = current_proc_io();

    let read_proc_before = current_proc_io();
    let read_started = Instant::now();
    let mut actual = Vec::with_capacity(bytes);
    {
        let mut file = fs::File::open(&path).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot open raw disk baseline file {}: {source}",
                path.display()
            ))
        })?;
        file.read_to_end(&mut actual).map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot read raw disk baseline file {}: {source}",
                path.display()
            ))
        })?;
    }
    let read_elapsed = read_started.elapsed();
    let read_proc_after = current_proc_io();
    if actual != payload {
        let _ = fs::remove_file(&path);
        return Err(SinomoTestError::failed(format!(
            "raw disk baseline file {} read back {} bytes, expected {} bytes",
            path.display(),
            actual.len(),
            payload.len()
        )));
    }
    let cleanup_result = fs::remove_file(&path);
    if let Err(source) = cleanup_result {
        return Err(SinomoTestError::failed(format!(
            "cannot remove raw disk baseline file {}: {source}",
            path.display()
        )));
    }

    let total_bytes = bytes as u64;
    Ok(RawDiskBaseline {
        path: path.display().to_string(),
        bytes: total_bytes,
        sync_write,
        payload_build_elapsed,
        write_elapsed,
        write_dir_sync_elapsed,
        read_elapsed,
        write_bytes_per_second: rate_per_second(total_bytes as f64, write_elapsed),
        read_bytes_per_second: rate_per_second(total_bytes as f64, read_elapsed),
        write_proc_io_delta: write_proc_before
            .zip(write_proc_after)
            .map(|(before, after)| after.saturating_delta(before)),
        read_proc_io_delta: read_proc_before
            .zip(read_proc_after)
            .map(|(before, after)| after.saturating_delta(before)),
    })
}

fn sync_raw_disk_baseline_dir(path: &Path) -> Result<(), SinomoTestError> {
    let dir = fs::File::open(path).map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot open raw disk baseline directory {} for sync: {source}",
            path.display()
        ))
    })?;
    dir.sync_all().map_err(|source| {
        SinomoTestError::failed(format!(
            "cannot sync raw disk baseline directory {}: {source}",
            path.display()
        ))
    })
}

fn record_raw_disk_baseline(ctx: &mut SinomoTestContext, baseline: &RawDiskBaseline, prefix: &str) {
    ctx.record(format!("{prefix}_path"), &baseline.path);
    ctx.record(format!("{prefix}_bytes"), baseline.bytes);
    ctx.record(format!("{prefix}_sync_write"), baseline.sync_write);
    ctx.record(
        format!("{prefix}_payload_build_elapsed_ms"),
        baseline.payload_build_elapsed.as_millis() as u64,
    );
    ctx.record(
        format!("{prefix}_write_elapsed_ms"),
        baseline.write_elapsed.as_millis() as u64,
    );
    ctx.record(
        format!("{prefix}_write_dir_sync_elapsed_ms"),
        baseline.write_dir_sync_elapsed.as_millis() as u64,
    );
    ctx.record(
        format!("{prefix}_read_elapsed_ms"),
        baseline.read_elapsed.as_millis() as u64,
    );
    ctx.record(
        format!("{prefix}_write_bytes_per_second"),
        baseline.write_bytes_per_second,
    );
    ctx.record(
        format!("{prefix}_read_bytes_per_second"),
        baseline.read_bytes_per_second,
    );
    if let Some(delta) = baseline.write_proc_io_delta {
        record_proc_io_delta(ctx, prefix, "write", delta);
    }
    if let Some(delta) = baseline.read_proc_io_delta {
        record_proc_io_delta(ctx, prefix, "read", delta);
    }
}

fn record_proc_io_delta(
    ctx: &mut SinomoTestContext,
    prefix: &str,
    phase: &str,
    delta: ProcIoSample,
) {
    ctx.record(format!("{prefix}_{phase}_proc_rchar_delta"), delta.rchar);
    ctx.record(format!("{prefix}_{phase}_proc_wchar_delta"), delta.wchar);
    ctx.record(format!("{prefix}_{phase}_proc_syscr_delta"), delta.syscr);
    ctx.record(format!("{prefix}_{phase}_proc_syscw_delta"), delta.syscw);
    ctx.record(
        format!("{prefix}_{phase}_proc_read_bytes_delta"),
        delta.read_bytes,
    );
    ctx.record(
        format!("{prefix}_{phase}_proc_write_bytes_delta"),
        delta.write_bytes,
    );
    ctx.record(
        format!("{prefix}_{phase}_proc_cancelled_write_bytes_delta"),
        delta.cancelled_write_bytes,
    );
}

fn record_proc_io_delta_flat(ctx: &mut SinomoTestContext, delta: ProcIoSample) {
    ctx.record("proc_rchar_delta", delta.rchar);
    ctx.record("proc_wchar_delta", delta.wchar);
    ctx.record("proc_syscr_delta", delta.syscr);
    ctx.record("proc_syscw_delta", delta.syscw);
    ctx.record("proc_read_bytes_delta", delta.read_bytes);
    ctx.record("proc_write_bytes_delta", delta.write_bytes);
    ctx.record(
        "proc_cancelled_write_bytes_delta",
        delta.cancelled_write_bytes,
    );
}

fn parent_probe_path(path: &str) -> Option<String> {
    let path = path.trim_end_matches('/');
    let slash = path.rfind('/')?;
    if slash == 0 {
        None
    } else {
        Some(path[..slash].to_string())
    }
}

fn rate_per_second(count: f64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds <= f64::EPSILON {
        return count;
    }
    count / seconds
}

fn percentile_value(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

fn latency_reaches_exclusive_limit(observed_us: u64, limit_us: u64) -> bool {
    limit_us > 0 && observed_us >= limit_us
}

fn mean_value(values: &[u64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().copied().sum::<u64>() as f64 / values.len() as f64
}

#[derive(Clone, Copy, Debug, Default)]
struct ProcIoSample {
    rchar: u64,
    wchar: u64,
    syscr: u64,
    syscw: u64,
    read_bytes: u64,
    write_bytes: u64,
    cancelled_write_bytes: u64,
}

impl ProcIoSample {
    fn saturating_delta(self, before: Self) -> Self {
        Self {
            rchar: self.rchar.saturating_sub(before.rchar),
            wchar: self.wchar.saturating_sub(before.wchar),
            syscr: self.syscr.saturating_sub(before.syscr),
            syscw: self.syscw.saturating_sub(before.syscw),
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
            cancelled_write_bytes: self
                .cancelled_write_bytes
                .saturating_sub(before.cancelled_write_bytes),
        }
    }
}

fn current_proc_io() -> Option<ProcIoSample> {
    let raw = fs::read_to_string("/proc/self/io").ok()?;
    let mut sample = ProcIoSample::default();
    for line in raw.lines() {
        let (key, value) = line.split_once(':')?;
        let value = value.trim().parse::<u64>().ok()?;
        match key {
            "rchar" => sample.rchar = value,
            "wchar" => sample.wchar = value,
            "syscr" => sample.syscr = value,
            "syscw" => sample.syscw = value,
            "read_bytes" => sample.read_bytes = value,
            "write_bytes" => sample.write_bytes = value,
            "cancelled_write_bytes" => sample.cancelled_write_bytes = value,
            _ => {}
        }
    }
    Some(sample)
}

fn amplification_ratio(written_bytes: u64, logical_bytes: u64) -> Option<f64> {
    if logical_bytes == 0 {
        return None;
    }
    Some(written_bytes as f64 / logical_bytes as f64)
}

fn ratio(numerator: f64, denominator: f64) -> Option<f64> {
    if denominator <= f64::EPSILON {
        return None;
    }
    Some(numerator / denominator)
}

fn per_file(count: u64, files: usize) -> Option<f64> {
    if files == 0 {
        return None;
    }
    Some(count as f64 / files as f64)
}

fn bulk_write_syscw_budget(files: usize, max_per_file: f64, fixed: u64) -> f64 {
    max_per_file * files as f64 + fixed as f64
}

fn bulk_write_syscw_within_budget(
    observed: u64,
    files: usize,
    max_per_file: f64,
    fixed: u64,
) -> bool {
    observed as f64 <= bulk_write_syscw_budget(files, max_per_file, fixed)
}

fn expect_payload_now(
    backend: &SifsFileManagerBackend,
    path: &str,
    payload: &[u8],
    stage: &str,
) -> Result<(), SinomoTestError> {
    let bytes = backend.read_file(path).map_err(|error| {
        SinomoTestError::failed(format!("read-your-write broken {stage}: {error}"))
    })?;
    if bytes != payload {
        return Err(SinomoTestError::failed(format!(
            "read-your-write broken {stage}: {path} returned {} bytes, expected {}",
            bytes.len(),
            payload.len()
        )));
    }
    Ok(())
}

fn expect_absent_now(
    backend: &SifsFileManagerBackend,
    path: &str,
    stage: &str,
) -> Result<(), SinomoTestError> {
    match backend.stat(path) {
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(SinomoTestError::failed(format!(
            "cannot verify {path} is absent {stage}: {error}"
        ))),
        Ok(_) => Err(SinomoTestError::failed(format!(
            "{path} still exists {stage}"
        ))),
    }
}

fn is_not_found(error: &str) -> bool {
    error.contains("not found")
}

fn dfs_stat_reports_path_present<T>(path: &str, stat: Result<T, String>) -> Result<bool, String> {
    match stat {
        Ok(_) => Ok(true),
        Err(error) if is_not_found(&error) => Ok(false),
        Err(error) => Err(format!(
            "cannot determine whether {path} is present after concurrent directory race: {error}"
        )),
    }
}

fn is_retryable_rename_error(error: &str) -> bool {
    error.contains("moving between metadata shards")
        || error.contains("retry the operation")
        || error.contains("SIFS operation deadline expired during SIFS rename")
        || error.contains("metadata shard config incomplete")
        || is_retryable_dfs_probe_error(error)
}

fn is_retryable_dfs_probe_error(error: &str) -> bool {
    error.contains("metadata shard config incomplete")
        || error.contains("authority redirects")
        || error.contains("DFS metadata authority advanced after async preflight")
        || error.contains("write did not commit yet")
        || (error.contains("DFS error: network error") && error.contains("timed out"))
        || (error.contains("DFS error: network delivery PossiblyApplied")
            && (error.contains("timed out")
                || error.contains("network timeout")
                || error.contains("network retries exhausted")))
        || (error.contains("metadata serialization error")
            && (error.contains("invalid magic") || error.contains("unexpected end of input")))
}

fn probe_nonce() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    format!("{nanos:x}-{:x}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        CapabilityName, ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
        ClusterEndpointSource, ClusterMemberRecord, ClusterTransportKind,
    };
    use sinomo_runtime::node::ClusterId;

    #[test]
    fn self_contained_dfs_tests_use_unique_run_namespaces() {
        let context = SinomoTestContext::new("unused");
        let base =
            scoped_dfs_test_base(&context, "read-your-write").expect("scoped test namespace");

        assert_eq!(
            base,
            format!(
                "/services/cluster-tests/runs/{}/read-your-write",
                context.run_id()
            )
        );
        assert_eq!(
            scoped_sifs_test_run_root(&base).expect("run root"),
            format!("/services/cluster-tests/runs/{}", context.run_id())
        );
        assert!(
            scoped_dfs_test_base(
                &context
                    .clone()
                    .with_parameter("base", "/services/sinomo-agent/channels"),
                "read-your-write"
            )
            .is_err()
        );
    }

    #[test]
    fn remote_shard_probe_paths_are_scoped_by_lane_and_run() {
        assert_eq!(
            remote_shard_write_probe_prefix("root", "run-1").as_deref(),
            Some("/cluster-tests/runs/run-1/remote-shard-write")
        );
        assert_eq!(
            remote_shard_write_probe_prefix("services", "run-1").as_deref(),
            Some("/services/cluster-tests/runs/run-1/remote-shard-write")
        );
        assert_eq!(
            remote_shard_write_probe_prefix("users", "run-1").as_deref(),
            Some("/users/cluster-tests/runs/run-1/remote-shard-write")
        );
        assert!(remote_shard_write_probe_prefix("unknown", "run-1").is_none());
    }

    #[test]
    fn rename_retries_ambiguous_delivery_and_sync_timeout_failures() {
        let error = "DFS error: network delivery PossiblyApplied: network retries exhausted \
            during reconcile DFS message response after 2 attempts (PossiblyApplied): \
            initial response was ambiguous: network timeout";

        assert!(is_retryable_rename_error(error));
        assert!(is_retryable_rename_error(
            "DFS error: network error: timed out after 45000ms before metadata shard services \
             reached a POSIX sync certificate"
        ));
        assert!(is_retryable_rename_error(
            "DFS error: network error: DFS metadata authority advanced after async preflight of \
             generation 2"
        ));
        assert!(is_retryable_rename_error(
            "SIFS operation deadline expired during SIFS rename"
        ));
        assert!(is_retryable_rename_error(
            "DFS error: network delivery PossiblyApplied: daemon network request failed \
             (PossiblyApplied): network timeout during local object RPC in Overall"
        ));
    }

    #[test]
    fn scoped_sifs_test_cleanup_removes_only_the_run_tree() {
        let root = sinomo_testdir::TestDir::new("sinomo-cli-dfs-scoped-cleanup");
        let data_dir = SinomoDataDir::new(&root);
        let browser = SifsBrowser::open(&data_dir).expect("open local SIFS browser");
        let run_root = "/services/cluster-tests/runs/unit-run";
        let fixture = format!("{run_root}/case/fixture.txt");
        browser
            .create_dir_all(&format!("{run_root}/case"))
            .expect("create test namespace");
        browser
            .write_file(&fixture, b"temporary")
            .expect("write temporary fixture");

        remove_scoped_sifs_test_tree(&browser, run_root).expect("remove test namespace");

        assert!(browser.stat(run_root).is_err());
        assert!(validate_scoped_sifs_test_namespace("/services/sinomo-agent/channels").is_err());
    }

    #[test]
    fn write_latency_policies_require_explicit_subtree_evidence() {
        for (scenario, w, n) in [("w1-n2", 1, 2), ("w2-n3", 2, 3)] {
            let policy = write_latency_placement_policy(scenario).expect("policy");
            assert_eq!(
                (policy.min_commit_replicas, policy.desired_replicas),
                (w, n)
            );
            assert_eq!(policy.consistency, DfsObjectPolicy::ReplicatedEventual);
            let mut resolved: dfs::DfsResolvedPlacementPolicy =
                serde_json::from_value(serde_json::json!({
                    "path": "/fixture/file", "policy": policy, "inherited": true,
                    "source_path": "/fixture", "source_scope": "subtree", "explanation": "test",
                }))
                .expect("resolved policy");
            verify_write_latency_placement(&resolved, "/fixture", &policy).expect("exact rule");
            resolved.source_path = Some("/".into());
            assert!(verify_write_latency_placement(&resolved, "/fixture", &policy).is_err());
            resolved.source_path = Some("/fixture".into());
            resolved.policy.desired_replicas += 1;
            assert!(verify_write_latency_placement(&resolved, "/fixture", &policy).is_err());
        }
        assert!(write_latency_placement_policy("w1-n3").is_err());
    }

    #[test]
    fn write_latency_setup_retries_only_definite_rejections() {
        assert!(write_latency_preview_can_refresh(
            "SIFS placement preview is stale; refresh the preview"
        ));
        assert!(!write_latency_preview_can_refresh(
            "PossiblyApplied: SIFS placement preview is stale"
        ));
        assert!(!write_latency_preview_can_refresh("network timeout"));
        for reason in [
            "the placement control-plane snapshot changed",
            "an active placement metadata mutation excludes Apply",
        ] {
            assert!(write_latency_preview_can_refresh(&format!(
                "{} {reason}",
                dfs::DFS_PLACEMENT_APPLY_REJECTED_PREFIX
            )));
            assert!(!write_latency_preview_can_refresh(&format!(
                "PossiblyApplied: {} {reason}",
                dfs::DFS_PLACEMENT_APPLY_REJECTED_PREFIX
            )));
        }
        assert!(!write_latency_preview_can_refresh(
            "raft node is busy at raft storage"
        ));
        assert!(!write_latency_preview_can_refresh(
            "placement authority permit release failed"
        ));
    }

    #[test]
    fn write_latency_prepared_fixtures_do_not_reuse_content_across_runs() {
        let first = write_latency_payload_seed("create", 0, "/fixture/profile-run-a-000000.bin");
        let second = write_latency_payload_seed("create", 0, "/fixture/profile-run-b-000000.bin");
        let overwrite =
            write_latency_payload_seed("overwrite", 0, "/fixture/profile-run-b-000000.bin");
        assert_ne!(
            pattern_payload(&first, 4096),
            pattern_payload(&second, 4096)
        );
        assert_ne!(
            pattern_payload(&second, 4096),
            pattern_payload(&overwrite, 4096)
        );
    }

    #[test]
    fn lightweight_latency_limit_is_strictly_exclusive() {
        assert!(!latency_reaches_exclusive_limit(99_999, 100_000));
        assert!(latency_reaches_exclusive_limit(100_000, 100_000));
        assert!(latency_reaches_exclusive_limit(100_001, 100_000));
        assert!(!latency_reaches_exclusive_limit(u64::MAX, 0));
    }

    #[test]
    fn root_level_probe_paths_do_not_repeat_root_creation() {
        assert_eq!(parent_probe_path("/large.bin"), None);
        assert_eq!(parent_probe_path("/large.bin/"), None);
        assert_eq!(
            parent_probe_path("/services/large.bin"),
            Some("/services".to_string())
        );
    }

    #[test]
    fn bulk_write_syscw_affine_budget_accepts_boundary_and_rejects_excess() {
        let files = 64;
        let max_per_file = 4.0;
        let fixed = 16;

        assert_eq!(bulk_write_syscw_budget(files, max_per_file, fixed), 272.0);
        assert!(bulk_write_syscw_within_budget(
            272,
            files,
            max_per_file,
            fixed
        ));
        assert!(!bulk_write_syscw_within_budget(
            273,
            files,
            max_per_file,
            fixed
        ));
    }

    #[test]
    fn bulk_write_overwrite_existing_defaults_false_and_rejects_invalid_boolean() {
        let default = SinomoTestContext::new("unused");
        assert!(
            !optional_bool_parameter(&default, "overwrite-existing", false)
                .expect("default overwrite-existing mode")
        );

        let enabled = SinomoTestContext::new("unused").with_parameter("overwrite-existing", "true");
        assert!(
            optional_bool_parameter(&enabled, "overwrite-existing", false)
                .expect("explicit overwrite-existing mode")
        );

        let invalid =
            SinomoTestContext::new("unused").with_parameter("overwrite-existing", "sometimes");
        assert!(matches!(
            optional_bool_parameter(&invalid, "overwrite-existing", false),
            Err(SinomoTestError::InvalidParameter { ref name, .. })
                if name == "overwrite-existing"
        ));
    }

    #[test]
    fn bulk_retry_metrics_count_retries_and_keep_bounded_reasons() {
        let mut metrics = DfsMutationRetryMetrics::default();
        for index in 0..(MAX_RECORDED_BULK_RETRY_REASONS + 3) {
            metrics.record_retry(&format!("retry-{index}"));
        }

        assert_eq!(metrics.retry_count, MAX_RECORDED_BULK_RETRY_REASONS + 3);
        assert_eq!(metrics.retry_reasons.len(), MAX_RECORDED_BULK_RETRY_REASONS);
        assert_eq!(metrics.truncated_reason_count(), 3);
        assert_eq!(
            metrics.retry_reasons.first().map(String::as_str),
            Some("retry-0")
        );
    }

    #[test]
    fn observed_bulk_retry_records_the_retry_that_precedes_success() {
        let mut attempts = 0usize;
        let mut metrics = DfsMutationRetryMetrics::default();
        let result = retry_bulk_mutation_with_metrics(
            Instant::now() + Duration::from_secs(2),
            || {
                attempts += 1;
                if attempts == 1 {
                    Err("DFS error: network error: routed write timed out")
                } else {
                    Ok("committed")
                }
            },
            || "write test batch".to_string(),
            &mut metrics,
        )
        .expect("second attempt succeeds");

        assert_eq!(result, "committed");
        assert_eq!(attempts, 2);
        assert_eq!(metrics.retry_count, 1);
        assert_eq!(
            metrics.retry_reasons,
            vec!["DFS error: network error: routed write timed out"]
        );
        assert_eq!(metrics.truncated_reason_count(), 0);
    }

    #[test]
    fn dfs_protocol_codec_uses_attachment_heads_without_embedded_attachment_bytes() {
        let payload = bulk_payload("dfs-protocol-codec-test", 0, 16 * 1024);
        let payload_hash = format!("blake3:{}", blake3::hash(&payload).to_hex());
        let samples = dfs_protocol_codec_samples(&payload);

        assert_eq!(samples.len(), 3);
        for sample in &samples {
            let request = encode_dfs_protocol_codec_head(sample).expect("encode attachment head");
            let decoded = request
                .decode_body::<DfsAttachmentMessageCommand>()
                .expect("decode attachment head");
            validate_decoded_dfs_protocol_codec_head(
                &decoded,
                payload.len(),
                payload_hash.as_str(),
            )
            .expect("validate attachment head");
        }
    }

    #[test]
    fn backend_open_completed_after_deadline_is_rejected_before_mutation() {
        let deadline = Instant::now() + Duration::from_millis(5);
        let error = retry_backend_open_until(deadline, "test backend", || {
            std::thread::sleep(Duration::from_millis(15));
            Ok(())
        })
        .expect_err("late backend open must not authorize a mutation");

        assert!(
            error
                .to_string()
                .contains("opened after the operation deadline")
        );
    }

    #[test]
    fn concurrent_dir_presence_distinguishes_absence_from_probe_failure() {
        assert_eq!(
            dfs_stat_reports_path_present("/present", Ok::<_, String>(())),
            Ok(true)
        );
        assert_eq!(
            dfs_stat_reports_path_present::<()>(
                "/absent",
                Err("cannot inspect /absent: path not found".to_string()),
            ),
            Ok(false)
        );

        let error = dfs_stat_reports_path_present::<()>(
            "/unknown",
            Err("network read timed out".to_string()),
        )
        .expect_err("transport failure must not look like absence");
        assert!(error.contains("cannot determine whether /unknown is present"));
        assert!(error.contains("network read timed out"));
    }

    #[test]
    fn desired_replica_check_rejects_an_equal_sized_wrong_location_set() {
        let desired = [
            NodeId::from_legacy_named("node_desired_a").expect("desired a"),
            NodeId::from_legacy_named("node_desired_b").expect("desired b"),
        ];
        let current = [
            desired[0].clone(),
            NodeId::from_legacy_named("node_wrong").expect("wrong node"),
        ];

        assert_eq!(
            missing_desired_replica_nodes(&desired, &current),
            vec![desired[1].clone()]
        );
        assert_eq!(desired.len(), current.len());
    }

    #[test]
    fn remote_shard_write_phase_budget_allows_a_cold_routed_namespace_operation() {
        assert_eq!(
            remote_shard_write_phase_timeout(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        assert_eq!(
            remote_shard_write_phase_timeout(Duration::from_secs(3)),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn dfs_invariants_tolerate_lag_on_one_explicitly_unreachable_replica_with_quorum() {
        let local = NodeId::from_legacy_named("node_dfs_invariant_local").expect("local");
        let offline = NodeId::from_legacy_named("node_dfs_invariant_offline").expect("offline");
        let reachable_a =
            NodeId::from_legacy_named("node_dfs_invariant_reachable_a").expect("reachable a");
        let reachable_b =
            NodeId::from_legacy_named("node_dfs_invariant_reachable_b").expect("reachable b");
        let shard = invariant_metadata_shard_status(
            &local,
            &[offline.clone(), reachable_a.clone(), reachable_b.clone()],
            &[
                (offline.clone(), 5),
                (reachable_a.clone(), 0),
                (reachable_b.clone(), 0),
            ],
        );
        let explicitly_unreachable = BTreeSet::from([offline.clone()]);

        let assessment =
            assess_dfs_invariant_replica_health(local.as_str(), &[shard], &explicitly_unreachable);

        assert!(assessment.violations().is_empty(), "{assessment:?}");
        assert_eq!(assessment.configured_replica_count, 3);
        assert_eq!(assessment.reported_replica_count, 3);
        assert_eq!(
            assessment.explicitly_unreachable_configured_replica_count,
            1
        );
        assert_eq!(assessment.lagging_unreachable_replica_count, 1);
        assert_eq!(assessment.max_unreachable_replica_lag_entries, 5);
        assert_eq!(assessment.lagging_reachable_replica_count, 0);
        assert_eq!(
            assessment.explicitly_unreachable_replica_node_ids,
            BTreeSet::from([offline])
        );
    }

    #[test]
    fn dfs_invariants_keep_reachable_replica_lag_as_a_failure() {
        let local = NodeId::from_legacy_named("node_dfs_invariant_lag_local").expect("local");
        let offline = NodeId::from_legacy_named("node_dfs_invariant_lag_offline").expect("offline");
        let reachable_a =
            NodeId::from_legacy_named("node_dfs_invariant_lag_reachable_a").expect("reachable a");
        let reachable_b =
            NodeId::from_legacy_named("node_dfs_invariant_lag_reachable_b").expect("reachable b");
        let shard = invariant_metadata_shard_status(
            &local,
            &[offline.clone(), reachable_a.clone(), reachable_b.clone()],
            &[(offline.clone(), 7), (reachable_a, 2), (reachable_b, 0)],
        );

        let assessment = assess_dfs_invariant_replica_health(
            local.as_str(),
            &[shard],
            &BTreeSet::from([offline]),
        );
        let violations = assessment.violations();

        assert_eq!(assessment.lagging_reachable_replica_count, 1);
        assert_eq!(assessment.max_reachable_replica_lag_entries, 2);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("lagging_reachable")),
            "{violations:?}"
        );
    }

    #[test]
    fn dfs_invariants_retain_counts_but_allow_missing_status_for_unreachable_replica() {
        let local = NodeId::from_legacy_named("node_dfs_invariant_count_local").expect("local");
        let offline =
            NodeId::from_legacy_named("node_dfs_invariant_count_offline").expect("offline");
        let reachable_a =
            NodeId::from_legacy_named("node_dfs_invariant_count_reachable_a").expect("reachable a");
        let reachable_b =
            NodeId::from_legacy_named("node_dfs_invariant_count_reachable_b").expect("reachable b");
        let shard = invariant_metadata_shard_status(
            &local,
            &[offline.clone(), reachable_a.clone(), reachable_b.clone()],
            &[(reachable_a, 0), (reachable_b, 0)],
        );

        let assessment = assess_dfs_invariant_replica_health(
            local.as_str(),
            &[shard],
            &BTreeSet::from([offline]),
        );

        assert!(assessment.violations().is_empty(), "{assessment:?}");
        assert_eq!(assessment.configured_replica_count, 3);
        assert_eq!(assessment.reported_replica_count, 2);
        assert_eq!(assessment.reachable_configured_replica_count, 2);
        assert_eq!(assessment.reachable_reported_replica_count, 2);
        assert!(assessment.reachable_replica_status_mismatches.is_empty());
    }

    #[test]
    fn dfs_invariants_keep_metadata_quorum_loss_as_a_failure() {
        let local = NodeId::from_legacy_named("node_dfs_invariant_quorum_local").expect("local");
        let offline_a =
            NodeId::from_legacy_named("node_dfs_invariant_quorum_offline_a").expect("offline a");
        let offline_b =
            NodeId::from_legacy_named("node_dfs_invariant_quorum_offline_b").expect("offline b");
        let reachable =
            NodeId::from_legacy_named("node_dfs_invariant_quorum_reachable").expect("reachable");
        let shard = invariant_metadata_shard_status(
            &local,
            &[offline_a.clone(), offline_b.clone(), reachable.clone()],
            &[
                (offline_a.clone(), 4),
                (offline_b.clone(), 3),
                (reachable, 0),
            ],
        );

        let assessment = assess_dfs_invariant_replica_health(
            local.as_str(),
            &[shard],
            &BTreeSet::from([offline_a, offline_b]),
        );
        let violations = assessment.violations();

        assert_eq!(assessment.lagging_reachable_replica_count, 0);
        assert_eq!(assessment.metadata_quorum_losses.len(), 1);
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains("metadata_quorum_lost")),
            "{violations:?}"
        );
    }

    #[test]
    fn dfs_invariant_unavailable_targets_require_failed_live_probe_and_no_healthy_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-invariant-unreachable-target");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let paths = ClusterPaths::new(&data_dir);
        let mut state = test_state();
        let target = state.members[1].node_id.clone();
        let endpoint = test_endpoint(
            &target,
            "endpoint_dfs_invariant_unreachable_target",
            "unreachable.test:21353",
        );
        state.members[1].endpoints = vec![endpoint.clone()];
        assert!(
            dfs_unavailable_targets_from_live_health(&paths, &state, &BTreeSet::new())
                .contains(&target),
            "an unknown node missed by the current live-health sweep is unavailable"
        );
        let degraded = connectivity_record(
            &state,
            &endpoint,
            network::ClusterConnectivityState::Degraded,
        );
        network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[degraded],
        )
        .expect("write degraded connectivity");
        let live_online = BTreeSet::from([target.clone()]);
        assert!(
            !dfs_unavailable_targets_from_live_health(&paths, &state, &live_online)
                .contains(&target),
            "a node observed online remains in the invariant even with degraded connectivity"
        );
        assert!(
            dfs_unavailable_targets_from_live_health(&paths, &state, &BTreeSet::new())
                .contains(&target),
            "a failed current live probe without a healthy route is tolerated as unavailable"
        );

        let healthy = connectivity_record(
            &state,
            &endpoint,
            network::ClusterConnectivityState::Healthy,
        );
        network::write_local_cluster_connectivity_records(&paths, &state.local_node_id, &[healthy])
            .expect("write healthy connectivity");

        assert!(
            !dfs_unavailable_targets_from_live_health(&paths, &state, &BTreeSet::new())
                .contains(&target),
            "a healthy route keeps a transient live-probe miss visible to invariant checks"
        );
    }

    fn invariant_metadata_shard_status(
        primary_node_id: &NodeId,
        replica_node_ids: &[NodeId],
        replica_lag: &[(NodeId, u64)],
    ) -> dfs::DfsMetadataShardStatus {
        dfs::DfsMetadataShardStatus {
            shard_id: dfs::DfsMetadataShardId::new("services"),
            file_system_id: None,
            primary_node_id: Some(primary_node_id.clone()),
            replica_node_ids: replica_node_ids.to_vec(),
            epoch: Some(1),
            journal_next_sequence: 11,
            journal_last_sequence: 10,
            replica_lag: replica_lag
                .iter()
                .map(
                    |(target_node_id, lag_entries)| dfs::DfsMetadataReplicaLagStatus {
                        target_node_id: target_node_id.clone(),
                        replicated_through_sequence: 10_u64.saturating_sub(*lag_entries),
                        lag_entries: *lag_entries,
                    },
                )
                .collect(),
            replay_cursors: Vec::new(),
        }
    }

    fn test_state() -> ClusterBootstrapState {
        let now = time::OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_dfs_status_targets").expect("cluster id");
        let local_node =
            NodeId::from_legacy_named("node_dfs_status_targets_local").expect("local node");
        let remote_node =
            NodeId::from_legacy_named("node_dfs_status_targets_remote").expect("remote node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "dfs status targets",
            local_node,
            "local",
            "local-public-key",
            now,
        );
        state.members[0].capabilities.push(main_daemon_capability());
        let mut remote = ClusterMemberRecord::local_voter(
            cluster_id,
            remote_node,
            "remote",
            "remote-public-key",
            now,
        );
        remote.capabilities.push(main_daemon_capability());
        state.members.push(remote);
        state
    }

    fn add_remote_main(state: &mut ClusterBootstrapState, name: &str) -> NodeId {
        let now = time::OffsetDateTime::now_utc();
        let node = NodeId::from_legacy_named(name).expect("node");
        let mut member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            node.clone(),
            name,
            format!("{name}-public-key"),
            now,
        );
        member.capabilities.push(main_daemon_capability());
        state.members.push(member);
        node
    }

    fn main_daemon_capability() -> CapabilityName {
        CapabilityName::new(sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY)
            .expect("main daemon capability")
    }

    fn test_endpoint(node_id: &NodeId, name: &str, address: &str) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: network::ClusterEndpointId::from_legacy_named(name).expect("endpoint"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: address.to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        }
    }

    fn repair_attempt(
        intent_id: &str,
        missing_replica_node_id: NodeId,
    ) -> dfs::DfsRepairAttemptRecord {
        dfs::DfsRepairAttemptRecord {
            transfer_delivery_state: None,
            completed_transfer_request_id: None,
            transfer_kind: None,
            intent_id: intent_id.to_string(),
            object_id: DfsObjectId::from_legacy_named(format!("object_{intent_id}"))
                .expect("object id"),
            version: DfsVersion::from_legacy_named(format!("version_{intent_id}"))
                .expect("version"),
            content_hash: format!("blake3:{intent_id}"),
            missing_replica_node_id,
            attempts: 1,
            in_flight: false,
            last_attempt_unix_nanos: 1,
            next_retry_unix_nanos: 2,
            transfer_request_id: None,
            last_error: Some("network error at cluster target rpc: no usable route".to_string()),
        }
    }

    fn repair_view(intent_id: &str, missing_replica_node_id: NodeId) -> DfsOperatorRepairView {
        DfsOperatorRepairView {
            intent_id: intent_id.to_string(),
            file_system_id: DfsFileSystemId::from_legacy_named("fs_dfs_repair_view")
                .expect("file system id"),
            object_id: DfsObjectId::from_legacy_named(format!("object_{intent_id}"))
                .expect("object id"),
            version: DfsVersion::from_legacy_named(format!("version_{intent_id}"))
                .expect("version"),
            content_hash: Some(format!("blake3:{intent_id}")),
            missing_replica_node_id,
        }
    }

    fn connectivity_record(
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
        connectivity_state: network::ClusterConnectivityState,
    ) -> network::ClusterConnectivityRecord {
        let now = time::OffsetDateTime::now_utc();
        let consecutive_failures = match connectivity_state {
            network::ClusterConnectivityState::Healthy => 0,
            network::ClusterConnectivityState::Degraded => 2,
            network::ClusterConnectivityState::Unreachable => 3,
        };
        network::cluster_connectivity_record(
            state,
            endpoint,
            connectivity_state,
            Some(1),
            consecutive_failures,
            Some(now.to_string()),
            None,
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        )
    }

    #[test]
    fn bounded_routed_status_probes_start_targets_concurrently() {
        let item_count = 3usize;
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let runner_release = std::sync::Arc::clone(&release);
        let runner = std::thread::spawn(move || {
            let items = (0..item_count).collect::<Vec<_>>();
            run_bounded_dfs_routed_status_jobs(&items, item_count, &|item| {
                started_tx.send(item).expect("record started probe");
                let (released, wake) = &*runner_release;
                let mut released = released.lock().expect("release lock");
                while !*released {
                    released = wake.wait(released).expect("release wait");
                }
                item
            })
        });

        let mut started = Vec::new();
        for _ in 0..item_count {
            match started_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(item) => started.push(item),
                Err(_) => break,
            }
        }
        let (released, wake) = &*release;
        *released.lock().expect("release lock") = true;
        wake.notify_all();
        let results = runner
            .join()
            .expect("bounded probe runner")
            .expect("bounded probe results");

        started.sort_unstable();
        assert_eq!(started, vec![0, 1, 2]);
        assert_eq!(results, vec![0, 1, 2]);
    }

    #[test]
    fn bounded_routed_status_probes_never_exceed_limit() {
        let active = std::sync::Arc::new(AtomicUsize::new(0));
        let max_active = std::sync::Arc::new(AtomicUsize::new(0));
        let items = (0..8usize).collect::<Vec<_>>();
        let results = run_bounded_dfs_routed_status_jobs(&items, 2, &|item| {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(current, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(20));
            active.fetch_sub(1, Ordering::SeqCst);
            item * 2
        })
        .expect("bounded probe results");

        assert_eq!(results, vec![0, 2, 4, 6, 8, 10, 12, 14]);
        assert_eq!(max_active.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn routed_status_attempt_preserves_target_errors_and_elapsed_diagnostics() {
        let successful_target =
            NodeId::from_legacy_named("node_routed_status_success").expect("successful target");
        let failed_target =
            NodeId::from_legacy_named("node_routed_status_failure").expect("failed target");
        let status = BTreeMap::from([("objects", 7_u64)]);
        let repair_details = vec![BTreeMap::from([("intent_id", "repair-a".to_string())])];
        let bounded_omissions = vec!["historical routing visibility omitted".to_string()];
        let attempt = aggregate_dfs_routed_status_probes(vec![
            DfsRoutedStatusTargetProbe {
                target: successful_target.clone(),
                elapsed_ms: 41,
                status: Some(status.clone()),
                repair_details: repair_details.clone(),
                bounded_omissions: bounded_omissions.clone(),
                blocking_incomplete_reasons: Vec::new(),
                error: None,
            },
            DfsRoutedStatusTargetProbe {
                target: failed_target.clone(),
                elapsed_ms: 73,
                status: None,
                repair_details: Vec::new(),
                bounded_omissions: Vec::new(),
                blocking_incomplete_reasons: Vec::new(),
                error: Some("decode failed: fixture".to_string()),
            },
        ]);

        assert_eq!(
            attempt.statuses.get(&successful_target.to_string()),
            Some(&status)
        );
        assert_eq!(
            attempt.repair_details.get(&successful_target.to_string()),
            Some(&repair_details)
        );
        assert_eq!(
            attempt
                .bounded_omissions
                .get(&successful_target.to_string()),
            Some(&bounded_omissions)
        );
        assert_eq!(
            attempt
                .target_elapsed_ms
                .get(&successful_target.to_string()),
            Some(&41)
        );
        assert_eq!(
            attempt.target_elapsed_ms.get(&failed_target.to_string()),
            Some(&73)
        );
        assert_eq!(
            attempt.failures,
            vec![format!("{failed_target}: decode failed: fixture")]
        );
    }

    #[test]
    fn dfs_routed_status_gate_defaults_to_single_five_second_attempt() {
        let ctx = SinomoTestContext::new("unused");

        let gate =
            DfsRoutedStatusGate::from_context(&ctx, DEFAULT_ROUTED_STATUS_TIMEOUT_MS).unwrap();

        assert_eq!(
            gate,
            DfsRoutedStatusGate {
                max_attempts: 1,
                max_elapsed: Duration::from_millis(DEFAULT_ROUTED_STATUS_TIMEOUT_MS),
            }
        );
        assert!(!gate.exhausted(0, Duration::from_millis(4_999)));
        assert!(gate.exhausted(1, Duration::from_millis(1)));
        assert!(gate.exhausted(0, Duration::from_millis(5_000)));
        assert!(!gate.success_exceeded(1, Duration::from_millis(5_000)));
        assert!(gate.success_exceeded(2, Duration::from_millis(1)));
        assert!(gate.success_exceeded(1, Duration::from_millis(5_001)));
    }

    #[test]
    fn dfs_routed_status_gate_accepts_explicit_polling_budget() {
        let ctx = SinomoTestContext::new("unused")
            .with_parameter("max-attempts", "3")
            .with_parameter("max-elapsed-ms", "7000");

        let gate = DfsRoutedStatusGate::from_context(&ctx, 5_000).unwrap();

        assert_eq!(
            gate,
            DfsRoutedStatusGate {
                max_attempts: 3,
                max_elapsed: Duration::from_millis(7_000),
            }
        );
        assert!(!gate.exhausted(2, Duration::from_millis(6_999)));
        assert!(gate.exhausted(3, Duration::from_millis(1)));
        assert!(gate.exhausted(1, Duration::from_millis(7_000)));
    }

    #[test]
    fn dfs_routed_status_gate_scales_wall_budget_for_bounded_probe_waves() {
        let gate = DfsRoutedStatusGate {
            max_attempts: 1,
            max_elapsed: Duration::from_secs(5),
        };

        assert_eq!(
            gate.for_bounded_target_count(0).max_elapsed,
            Duration::from_secs(5)
        );
        assert_eq!(
            gate.for_bounded_target_count(MAX_ROUTED_STATUS_CONCURRENCY)
                .max_elapsed,
            Duration::from_secs(5)
        );
        assert_eq!(
            gate.for_bounded_target_count(MAX_ROUTED_STATUS_CONCURRENCY + 1)
                .max_elapsed,
            Duration::from_secs(10)
        );
        assert_eq!(
            gate.for_bounded_target_count(MAX_ROUTED_STATUS_CONCURRENCY * 2 + 1)
                .max_elapsed,
            Duration::from_secs(15)
        );
    }

    #[test]
    fn dfs_routed_status_gate_rejects_zero_limits() {
        let attempts_ctx = SinomoTestContext::new("unused").with_parameter("max-attempts", "0");
        let attempts_error = DfsRoutedStatusGate::from_context(&attempts_ctx, 5_000).unwrap_err();
        assert!(matches!(
            attempts_error,
            SinomoTestError::InvalidParameter { ref name, .. } if name == "max-attempts"
        ));

        let elapsed_ctx = SinomoTestContext::new("unused").with_parameter("max-elapsed-ms", "0");
        let elapsed_error = DfsRoutedStatusGate::from_context(&elapsed_ctx, 5_000).unwrap_err();
        assert!(matches!(
            elapsed_error,
            SinomoTestError::InvalidParameter { ref name, .. } if name == "max-elapsed-ms"
        ));
    }

    #[test]
    fn dfs_unavailable_repair_targets_include_degraded_main_without_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-degraded-repair-target");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let paths = ClusterPaths::new(&data_dir);
        let mut state = test_state();
        let unavailable = state.members[1].node_id.clone();
        let endpoint = test_endpoint(
            &unavailable,
            "endpoint_dfs_degraded_repair_target",
            "unavailable.test:21353",
        );
        state.members[1].endpoints = vec![endpoint.clone()];
        let degraded = connectivity_record(
            &state,
            &endpoint,
            network::ClusterConnectivityState::Degraded,
        );
        network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[degraded],
        )
        .expect("write connectivity");

        let targets = dfs_unavailable_repair_targets_from_state(&paths, &state);

        assert!(
            targets.contains(&unavailable),
            "a degraded main daemon with no healthy route must be treated as unavailable repair debt: {targets:?}"
        );
    }

    #[test]
    fn dfs_unavailable_repair_targets_keep_routed_degraded_main_available() {
        let root = sinomo_testdir::TestDir::new("sinomo-dfs-routed-repair-target");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let paths = ClusterPaths::new(&data_dir);
        let mut state = test_state();
        let bridge = state.members[1].node_id.clone();
        let target = add_remote_main(&mut state, "node_dfs_routed_repair_target");
        let bridge_endpoint = test_endpoint(
            &bridge,
            "endpoint_dfs_routed_repair_bridge",
            "bridge.test:21353",
        );
        let target_endpoint = test_endpoint(
            &target,
            "endpoint_dfs_routed_repair_target",
            "target.test:21353",
        );
        state.members[1].endpoints = vec![bridge_endpoint.clone()];
        state.members[2].endpoints = vec![target_endpoint.clone()];
        let local_to_target = connectivity_record(
            &state,
            &target_endpoint,
            network::ClusterConnectivityState::Degraded,
        );
        let local_to_bridge = connectivity_record(
            &state,
            &bridge_endpoint,
            network::ClusterConnectivityState::Healthy,
        );
        let mut bridge_state = state.clone();
        bridge_state.local_node_id = bridge.clone();
        let bridge_to_target = connectivity_record(
            &bridge_state,
            &target_endpoint,
            network::ClusterConnectivityState::Healthy,
        );
        network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[local_to_target, local_to_bridge],
        )
        .expect("write local connectivity");
        network::write_local_cluster_connectivity_record_set(&paths, &bridge, &[bridge_to_target])
            .expect("write bridge connectivity");

        let targets = dfs_unavailable_repair_targets_from_state(&paths, &state);

        assert!(
            !targets.contains(&target),
            "a degraded direct repair target with a healthy route must remain available: {targets:?}"
        );
    }

    #[test]
    fn dfs_status_summary_excludes_unavailable_repair_targets_from_active_debt() {
        let unavailable =
            NodeId::from_legacy_named("node_dfs_unavailable_repair_target").expect("node");
        let reachable =
            NodeId::from_legacy_named("node_dfs_reachable_repair_target").expect("node");
        let mut unavailable_repair_targets = BTreeSet::new();
        unavailable_repair_targets.insert(unavailable.clone());
        let status = DfsOperatorStatus {
            storage_migrations: Vec::new(),
            incomplete_reasons: Vec::new(),
            bounded_omissions: Vec::new(),
            file_system_count: 1,
            object_count: 2,
            version_count: 3,
            receipt_count: 4,
            intent_count: 5,
            pending_write_count: 0,

            pending_repair_count: 2,
            repair_attempt_count: 2,
            pending_namespace_job_count: 0,
            failed_namespace_job_count: 0,
            cross_shard_namespace_lock_count: Some(0),
            metadata_shards: Vec::new(),
            repair_attempts: vec![
                repair_attempt("offline_replica", unavailable),
                repair_attempt("reachable_replica", reachable),
            ],
        };
        let repairs = Vec::new();

        assert!(!routed_status_needs_repair_views(&status, &BTreeSet::new()));
        assert!(routed_status_needs_repair_views(
            &status,
            &unavailable_repair_targets
        ));

        let summary = dfs_status_probe_summary(&status, &repairs, &unavailable_repair_targets);

        assert_eq!(summary.get("pending_repairs"), Some(&1));
        assert_eq!(summary.get("total_pending_repairs"), Some(&2));
        assert_eq!(summary.get("unavailable_pending_repairs"), Some(&1));
    }

    #[test]
    fn dfs_status_summary_uses_repair_views_when_attempt_records_are_absent() {
        let unavailable =
            NodeId::from_legacy_named("node_dfs_unavailable_repair_view_target").expect("node");
        let mut unavailable_repair_targets = BTreeSet::new();
        unavailable_repair_targets.insert(unavailable.clone());
        let status = DfsOperatorStatus {
            storage_migrations: Vec::new(),
            incomplete_reasons: Vec::new(),
            bounded_omissions: Vec::new(),
            file_system_count: 1,
            object_count: 2,
            version_count: 3,
            receipt_count: 4,
            intent_count: 5,
            pending_write_count: 0,

            pending_repair_count: 1,
            repair_attempt_count: 0,
            pending_namespace_job_count: 0,
            failed_namespace_job_count: 0,
            cross_shard_namespace_lock_count: Some(0),
            metadata_shards: Vec::new(),
            repair_attempts: Vec::new(),
        };
        let repairs = vec![repair_view("offline_replica_without_attempt", unavailable)];

        let summary = dfs_status_probe_summary(&status, &repairs, &unavailable_repair_targets);

        assert_eq!(summary.get("pending_repairs"), Some(&0));
        assert_eq!(summary.get("total_pending_repairs"), Some(&1));
        assert_eq!(summary.get("unavailable_pending_repairs"), Some(&1));
    }

    #[test]
    fn dfs_routed_status_marks_incomplete_remote_status_dirty() {
        let mut status = DfsOperatorStatus {
            storage_migrations: Vec::new(),
            incomplete_reasons: vec![
                "bounded resident metadata status unavailable: generation is cold".to_string(),
            ],
            bounded_omissions: Vec::new(),
            file_system_count: 1,
            object_count: 0,
            version_count: 0,
            receipt_count: 0,
            intent_count: 0,
            pending_write_count: 0,

            pending_repair_count: 0,
            repair_attempt_count: 0,
            pending_namespace_job_count: 0,
            failed_namespace_job_count: 0,
            cross_shard_namespace_lock_count: Some(0),
            metadata_shards: Vec::new(),
            repair_attempts: Vec::new(),
        };
        let unavailable_repair_targets = BTreeSet::new();
        let summary = dfs_status_probe_summary(&status, &[], &unavailable_repair_targets);

        assert_eq!(summary.get("incomplete_reasons"), Some(&1));
        assert_eq!(summary.get("bounded_omissions"), Some(&0));
        let statuses = BTreeMap::from([("remote-node".to_string(), summary)]);
        let dirty = routed_status_dirty_summaries(&statuses, usize::MAX, usize::MAX);
        assert_eq!(dirty.len(), 1);
        assert!(dirty[0].contains("remote-node: incomplete_reasons=1"));

        status.incomplete_reasons.clear();
        status.cross_shard_namespace_lock_count = None;
        let unobserved_summary =
            dfs_status_probe_summary(&status, &[], &unavailable_repair_targets);
        assert_eq!(unobserved_summary.get("incomplete_reasons"), Some(&1));
        let unobserved_statuses = BTreeMap::from([("remote-node".to_string(), unobserved_summary)]);
        assert_eq!(
            routed_status_dirty_summaries(&unobserved_statuses, usize::MAX, usize::MAX,).len(),
            1,
            "old or bounded remote status must not report a false clean lock count"
        );

        status.cross_shard_namespace_lock_count = Some(0);
        let complete_summary = dfs_status_probe_summary(&status, &[], &unavailable_repair_targets);
        assert_eq!(complete_summary.get("incomplete_reasons"), Some(&0));
        let complete_statuses = BTreeMap::from([("remote-node".to_string(), complete_summary)]);
        assert!(
            routed_status_dirty_summaries(&complete_statuses, usize::MAX, usize::MAX,).is_empty()
        );
    }

    #[test]
    fn dfs_acceptance_keeps_bounded_omissions_visible_without_hiding_real_debt() {
        let legacy_reason = "repair queue observation unavailable: bounded DFS repair queue status unavailable because the resident queue is not hydrated";
        let mut status = DfsOperatorStatus {
            storage_migrations: Vec::new(),
            incomplete_reasons: Vec::new(),
            bounded_omissions: Vec::new(),
            file_system_count: 1,
            object_count: 0,
            version_count: 0,
            receipt_count: 0,
            intent_count: 0,
            pending_write_count: 0,

            pending_repair_count: 0,
            repair_attempt_count: 0,
            pending_namespace_job_count: 0,
            failed_namespace_job_count: 0,
            cross_shard_namespace_lock_count: Some(0),
            metadata_shards: Vec::new(),
            repair_attempts: Vec::new(),
        };
        status.push_bounded_omission(
            dfs::DfsOperatorStatusOmission::resident_repair_queue_not_hydrated(),
        );
        let unavailable_repair_targets = BTreeSet::new();

        let summary = dfs_status_probe_summary(&status, &[], &unavailable_repair_targets);
        assert_eq!(summary.get("incomplete_reasons"), Some(&0));
        assert_eq!(summary.get("bounded_omissions"), Some(&1));
        let statuses = BTreeMap::from([("remote-node".to_string(), summary)]);
        assert!(
            routed_status_dirty_summaries(&statuses, 0, 0).is_empty(),
            "a typed bounded omission is not itself observed debt"
        );

        let completeness = assess_dfs_invariant_status_completeness(&status);
        assert!(completeness.blocking_incomplete_reasons.is_empty());
        assert_eq!(completeness.bounded_omissions, vec![legacy_reason]);

        status.pending_write_count = 1;
        let debt_summary = dfs_status_probe_summary(&status, &[], &unavailable_repair_targets);
        let debt_statuses = BTreeMap::from([("remote-node".to_string(), debt_summary)]);
        assert_eq!(
            routed_status_dirty_summaries(&debt_statuses, 0, 0).len(),
            1,
            "observed pending writes must still fail beside an omission"
        );

        status
            .incomplete_reasons
            .push("resident metadata cursor projection is corrupt".to_string());
        let incomplete = assess_dfs_invariant_status_completeness(&status);
        assert_eq!(
            incomplete.blocking_incomplete_reasons,
            vec!["resident metadata cursor projection is corrupt"]
        );
    }
}
