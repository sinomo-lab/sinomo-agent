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
//!   generic network ping can miss.
//! - `cluster.dfs.invariants` — operational-metric sweep: no open conflicts,
//!   no stuck routed namespace ops, no leaked cross-shard locks.
//! - `cluster.dfs.lightweight-latency` — warmed SIFS stat/list/read-prefix
//!   latency probe for file-browser-style lightweight operations.
//! - `cluster.dfs.protocol-codec-baseline` — in-memory DFS typed message
//!   object-RPC codec throughput for representative chunk messages, independent
//!   of disk and live network conditions.
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
//!   large-object throughput probes that exercise chunked transfer and read-side
//!   no-write accounting.

use crate::file_manager::FileManagerBackend;
use crate::sifs_file_manager::SifsFileManagerBackend;
use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::cluster;
use sinomo_runtime::dfs::{
    self, DfsFetchBlobResponseChunk, DfsFileSystemId, DfsMessageCommand, DfsNetworkMessage,
    DfsObjectId, DfsOperatorStatus, DfsReadResponseChunk, DfsRoutedMessage,
    DfsStageBlobChunkCommand, DfsVersion, SIFS_FILE_SYSTEM_ID,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::ClusterPaths;
use sinomo_runtime::node::NodeId;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_EXPECT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_BULK_FILE_COUNT: usize = 128;
const DEFAULT_BULK_FILE_BYTES: usize = 4 * 1024;
const DEFAULT_LIGHTWEIGHT_LATENCY_ITERATIONS: usize = 128;
const DEFAULT_LIGHTWEIGHT_LATENCY_WARMUP_ITERATIONS: usize = 16;
const DEFAULT_LIGHTWEIGHT_LATENCY_PAYLOAD_BYTES: usize = 64;
const DEFAULT_LARGE_FILE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PROTOCOL_CODEC_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_PROTOCOL_CODEC_ITERATIONS: usize = 8;
const EXPECT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_ROUNDTRIP_BASE: &str = "/services/cluster-tests";

pub fn register_dfs_cluster_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(dfs_read_your_write())
        .register(dfs_routed_status())
        .register(dfs_concurrent_file_race())
        .register(dfs_concurrent_dir_race())
        .register(dfs_failover_current_read())
        .register(dfs_invariants())
        .register(dfs_lightweight_latency())
        .register(dfs_protocol_codec_baseline())
        .register(dfs_write())
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
        tags: ["cluster-dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_read_your_write(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_routed_status(ctx) {
        id: "cluster.dfs.routed-status",
        summary: "Route signed DFS status RPCs to every remote main daemon",
        tags: ["cluster-dfs", "probe", "routing"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_routed_status(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_concurrent_file_race(ctx) {
        id: "cluster.dfs.concurrent-file-race",
        summary: "Race two SIFS file writers and verify the final current read is one complete payload",
        tags: ["cluster-dfs", "probe", "correctness"],
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
        tags: ["cluster-dfs", "probe", "correctness"],
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
        tags: ["cluster-dfs", "probe", "correctness"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_failover_current_read(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_invariants(ctx) {
        id: "cluster.dfs.invariants",
        summary: "SIFS operational invariants: no open conflicts, stuck routed ops, or leaked cross-shard locks",
        tags: ["cluster-dfs"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_invariants(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_lightweight_latency(ctx) {
        id: "cluster.dfs.lightweight-latency",
        summary: "Measure warmed SIFS stat/list/read-prefix latency for lightweight file-browser operations",
        tags: ["cluster-dfs", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_lightweight_latency(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn dfs_protocol_codec_baseline(ctx) {
        id: "cluster.dfs.protocol-codec-baseline",
        summary: "Measure in-memory DFS data-plane object-RPC encode/decode throughput for chunk payloads",
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
        summary: "Write deterministic SIFS files and report throughput (--param base=... [--param files=N] [--param bytes=N])",
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
    let base = ctx
        .parameter("base")
        .unwrap_or(DEFAULT_ROUNDTRIP_BASE)
        .trim_end_matches('/')
        .to_string();
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

    retry_bulk_mutation(
        deadline,
        || backend.write_file(&path, payload.as_bytes()),
        || format!("cannot write SIFS file {path}"),
    )?;
    expect_payload_now(&backend, &path, payload.as_bytes(), "after write")?;

    backend
        .rename(&path, &renamed)
        .map_err(SinomoTestError::failed)?;
    expect_payload_now(&backend, &renamed, payload.as_bytes(), "after rename")?;
    expect_absent_now(&backend, &path, "after rename")?;

    backend
        .delete_file(&renamed)
        .map_err(SinomoTestError::failed)?;
    expect_absent_now(&backend, &renamed, "after delete")?;

    Ok(SinomoTestOutcome::passed(format!(
        "write/read/rename/delete roundtrip held read-your-write on node {node}"
    )))
}

fn run_routed_status(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let timeout_ms = optional_u64_parameter(ctx, "timeout-ms")?.unwrap_or(5_000);
    let expected_targets = optional_usize_parameter(ctx, "expected-targets", 1)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before DFS routed status: {error}"
        ))
    })?;
    let state = cluster::read_cluster_state(&cluster_paths)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let targets = dfs_routed_status_targets_from_state(&state);
    ctx.record("target_count", targets.len());
    ctx.record(
        "targets",
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
    );
    if targets.len() < expected_targets {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS routed status saw {} remote main targets, expected at least {expected_targets}",
            targets.len()
        )));
    }

    let daemon_client = DaemonApiClient::connect()
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot connect to local daemon for DFS routed status: {source}"
            ))
        })?
        .with_timeout(Duration::from_millis(timeout_ms).saturating_add(Duration::from_secs(1)));
    let request = dfs::dfs_status_rpc_request()
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    let mut statuses = BTreeMap::new();
    let mut failures = Vec::new();
    for target in &targets {
        match daemon_client.object_rpc_to_target(
            target,
            &request,
            Duration::from_millis(timeout_ms),
            true,
        ) {
            Ok(response) => match dfs::decode_dfs_status_rpc_response(&response) {
                Ok(status) => {
                    statuses.insert(target.to_string(), dfs_status_probe_summary(&status));
                }
                Err(error) => failures.push(format!("{target}: decode failed: {error}")),
            },
            Err(error) => failures.push(format!("{target}: {error}")),
        }
    }
    ctx.record("statuses", statuses);
    if !failures.is_empty() {
        ctx.record("failures", failures.clone());
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS routed status failed for {}/{} targets",
            failures.len(),
            targets.len()
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "DFS routed status succeeded for {} remote main daemon(s)",
        targets.len()
    )))
}

fn dfs_routed_status_targets_from_state(
    state: &sinomo_runtime::core::ClusterBootstrapState,
) -> Vec<NodeId> {
    let mut targets = state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && member.capabilities.iter().any(|capability| {
                    capability.as_str() == sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY
                })
        })
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    targets.sort();
    targets
}

fn dfs_status_probe_summary(status: &DfsOperatorStatus) -> BTreeMap<&'static str, u64> {
    BTreeMap::from([
        ("file_systems", status.file_system_count as u64),
        ("objects", status.object_count as u64),
        ("pending_writes", status.pending_write_count as u64),
        ("open_conflicts", status.open_conflict_count as u64),
        ("pending_repairs", status.pending_repair_count as u64),
    ])
}

fn run_concurrent_file_race(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = ctx
        .parameter("base")
        .unwrap_or(DEFAULT_ROUNDTRIP_BASE)
        .trim_end_matches('/')
        .to_string();
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
        .write_file(&path, &seed)
        .map_err(SinomoTestError::failed)?;
    ctx.record("node", &node);
    ctx.record("path", &path);

    let started = Instant::now();
    let run_writer =
        |label: &'static str, data_dir: SinomoDataDir, path: String, bytes: Vec<u8>| {
            std::thread::spawn(move || {
                let started = Instant::now();
                let result = SifsFileManagerBackend::open(&data_dir)
                    .and_then(|backend| backend.write_file(&path, &bytes));
                (
                    label,
                    result.map(|_| ()).map_err(|error| error.to_string()),
                    started.elapsed().as_millis() as u64,
                )
            })
        };
    let thread_a = run_writer("writer-a", data_dir.clone(), path.clone(), writer_a.clone());
    let thread_b = run_writer("writer-b", data_dir.clone(), path.clone(), writer_b.clone());
    let results = vec![
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

    let deadline = started + timeout;
    let valid = [&seed, &writer_a, &writer_b];
    loop {
        let read = backend.read_file(&path).map_err(SinomoTestError::failed)?;
        if valid
            .iter()
            .any(|candidate| candidate.as_slice() == read.as_slice())
        {
            ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
            ctx.record("final_bytes", read.len());
            ctx.record(
                "successful_writers",
                results
                    .iter()
                    .filter(|(_, result, _)| result.is_ok())
                    .count(),
            );
            return Ok(SinomoTestOutcome::passed(format!(
                "concurrent file race on {node} ended with one complete payload at {path}"
            )));
        }
        if Instant::now() >= deadline {
            ctx.record("last_read_bytes", read.len());
            return Ok(SinomoTestOutcome::failed(format!(
                "concurrent file race on {node} produced a non-serialized payload at {path}"
            )));
        }
        std::thread::sleep(EXPECT_POLL_INTERVAL);
    }
}

fn run_concurrent_dir_race(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = ctx
        .parameter("base")
        .unwrap_or(DEFAULT_ROUNDTRIP_BASE)
        .trim_end_matches('/')
        .to_string();
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let nonce = probe_nonce();
    let dir = format!("{base}/dir-race-{nonce}");
    let child = format!("{dir}/child.txt");
    backend.create_dir(&dir).map_err(SinomoTestError::failed)?;
    ctx.record("node", &node);
    ctx.record("dir", &dir);
    ctx.record("child", &child);

    let create_dir = data_dir.clone();
    let create_child = child.clone();
    let create_thread = std::thread::spawn(move || {
        let started = Instant::now();
        let result = SifsFileManagerBackend::open(&create_dir)
            .and_then(|backend| backend.write_file(&create_child, b"child"));
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
    let results = vec![
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

    let dir_present = backend.stat(&dir).is_ok();
    let child_present = backend.stat(&child).is_ok();
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
    let base = ctx
        .parameter("base")
        .unwrap_or(DEFAULT_ROUNDTRIP_BASE)
        .trim_end_matches('/')
        .to_string();
    let timeout = expect_timeout(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let nonce = probe_nonce();
    let path = format!("{base}/current-read-{nonce}.txt");
    let old = format!("old current-read payload {nonce}");
    let new = format!("new current-read payload {nonce}");
    backend
        .write_file(&path, old.as_bytes())
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
        .write_file(&path, new.as_bytes())
        .map_err(SinomoTestError::failed)?;

    let started = Instant::now();
    let deadline = started + timeout;
    let mut observations = Vec::new();
    loop {
        let read = cached_reader
            .read_file(&path)
            .map_err(SinomoTestError::failed)?;
        observations.push(format!(
            "elapsed_ms={}:bytes={}",
            started.elapsed().as_millis(),
            read.len()
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
    let min_encode_bytes_per_second =
        optional_f64_parameter(ctx, "min-encode-bytes-per-second", 0.0)?;
    let min_decode_bytes_per_second =
        optional_f64_parameter(ctx, "min-decode-bytes-per-second", 0.0)?;
    require_positive("bytes", bytes)?;
    require_positive("iterations", iterations)?;

    let payload = bulk_payload("dfs-protocol-codec", 0, bytes);
    let samples = dfs_protocol_codec_samples(&payload)?;
    let encoded_samples = samples
        .iter()
        .map(|sample| {
            let request = dfs::dfs_routed_message_rpc_request(&sample.command.routed_message)
                .map_err(|error| {
                    SinomoTestError::failed(format!(
                        "cannot encode {} DFS message request: {error}",
                        sample.label
                    ))
                })?;
            Ok(EncodedDfsProtocolCodecSample {
                label: sample.label,
                request_body_bytes: request.body.len(),
                protocol_payload_bytes: sample.protocol_payload_bytes,
                request,
            })
        })
        .collect::<Result<Vec<_>, SinomoTestError>>()?;
    let messages_per_iteration = encoded_samples.len();
    let total_messages = messages_per_iteration.saturating_mul(iterations);
    let total_payload_bytes = bytes.saturating_mul(total_messages);
    let request_body_bytes_per_iteration = encoded_samples
        .iter()
        .map(|sample| sample.request_body_bytes)
        .sum::<usize>();
    let protocol_payload_bytes_per_iteration = encoded_samples
        .iter()
        .map(|sample| sample.protocol_payload_bytes)
        .sum::<usize>();

    let encode_started = Instant::now();
    let mut encoded_request_body_bytes = 0_usize;
    for _ in 0..iterations {
        for sample in &samples {
            let request = dfs::dfs_routed_message_rpc_request(&sample.command.routed_message)
                .map_err(|error| {
                    SinomoTestError::failed(format!(
                        "cannot encode {} DFS message request: {error}",
                        sample.label
                    ))
                })?;
            encoded_request_body_bytes =
                encoded_request_body_bytes.saturating_add(request.body.len());
            std::hint::black_box(&request);
        }
    }
    let encode_elapsed = encode_started.elapsed();

    let decode_started = Instant::now();
    let mut decoded_request_body_bytes = 0_usize;
    for _ in 0..iterations {
        for sample in &encoded_samples {
            let decoded = sample
                .request
                .decode_body::<DfsMessageCommand>()
                .map_err(|error| {
                    SinomoTestError::failed(format!(
                        "cannot decode {} DFS message request body: {error}",
                        sample.label
                    ))
                })?;
            let decoded_payload_bytes = decoded_dfs_message_payload_bytes(&decoded)?;
            if decoded_payload_bytes != bytes {
                return Ok(SinomoTestOutcome::failed(format!(
                    "decoded {} DFS protocol payload had {decoded_payload_bytes} bytes, expected {bytes}",
                    sample.label
                )));
            }
            decoded_request_body_bytes =
                decoded_request_body_bytes.saturating_add(sample.request_body_bytes);
            std::hint::black_box(decoded);
        }
    }
    let decode_elapsed = decode_started.elapsed();

    let encode_bytes_per_second = rate_per_second(total_payload_bytes as f64, encode_elapsed);
    let decode_bytes_per_second = rate_per_second(total_payload_bytes as f64, decode_elapsed);
    let request_body_encode_bytes_per_second =
        rate_per_second(encoded_request_body_bytes as f64, encode_elapsed);
    let request_body_decode_bytes_per_second =
        rate_per_second(decoded_request_body_bytes as f64, decode_elapsed);

    ctx.record("payload_bytes_per_message", bytes as u64);
    ctx.record("iterations", iterations);
    ctx.record("messages_per_iteration", messages_per_iteration);
    ctx.record("total_messages", total_messages);
    ctx.record("total_payload_bytes", total_payload_bytes as u64);
    ctx.record(
        "request_body_bytes_per_iteration",
        request_body_bytes_per_iteration as u64,
    );
    ctx.record(
        "protocol_payload_bytes_per_iteration",
        protocol_payload_bytes_per_iteration as u64,
    );
    ctx.record("encode_elapsed_ms", encode_elapsed.as_millis() as u64);
    ctx.record("decode_elapsed_ms", decode_elapsed.as_millis() as u64);
    ctx.record("encode_bytes_per_second", encode_bytes_per_second);
    ctx.record("decode_bytes_per_second", decode_bytes_per_second);
    ctx.record(
        "request_body_encode_bytes_per_second",
        request_body_encode_bytes_per_second,
    );
    ctx.record(
        "request_body_decode_bytes_per_second",
        request_body_decode_bytes_per_second,
    );
    ctx.record("min_encode_bytes_per_second", min_encode_bytes_per_second);
    ctx.record("min_decode_bytes_per_second", min_decode_bytes_per_second);
    ctx.record(
        "sample_labels",
        samples
            .iter()
            .map(|sample| sample.label)
            .collect::<Vec<_>>(),
    );

    if min_encode_bytes_per_second > 0.0 && encode_bytes_per_second < min_encode_bytes_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS data-plane object-RPC encode throughput {encode_bytes_per_second:.2} bytes/s is below required {min_encode_bytes_per_second:.2} bytes/s"
        )));
    }
    if min_decode_bytes_per_second > 0.0 && decode_bytes_per_second < min_decode_bytes_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS data-plane object-RPC decode throughput {decode_bytes_per_second:.2} bytes/s is below required {min_decode_bytes_per_second:.2} bytes/s"
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "DFS protocol codec processed {total_messages} messages of {bytes} bytes (encode {encode_bytes_per_second:.2} bytes/s, decode {decode_bytes_per_second:.2} bytes/s)"
    )))
}

fn run_invariants(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let timeout = expect_timeout(ctx)?;
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    ctx.record("node", &node);
    let deadline = Instant::now() + timeout;
    loop {
        let metrics = backend
            .operational_metrics()
            .map_err(SinomoTestError::failed)?;
        let mut violations = Vec::new();
        if metrics.open_conflict_count != 0 {
            violations.push(format!(
                "open_conflict_count={}",
                metrics.open_conflict_count
            ));
        }
        if metrics.cross_shard_namespace_lock_count != 0 {
            violations.push(format!(
                "cross_shard_namespace_lock_count={}",
                metrics.cross_shard_namespace_lock_count
            ));
        }
        if metrics.pending_routed_namespace_op_count != 0 {
            violations.push(format!(
                "pending_routed_namespace_op_count={}",
                metrics.pending_routed_namespace_op_count
            ));
        }
        if metrics.pending_routed_write_count != 0 {
            violations.push(format!(
                "pending_routed_write_count={}",
                metrics.pending_routed_write_count
            ));
        }
        if violations.is_empty() {
            ctx.record("object_count", metrics.object_count);
            ctx.record("repair_job_count", metrics.repair_job_count);
            ctx.record("repair_attempt_count", metrics.repair_attempt_count);
            ctx.record("pending_write_count", metrics.pending_write_count);
            ctx.record(
                "pending_metadata_replication_count",
                metrics.pending_metadata_replication_count,
            );
            return Ok(SinomoTestOutcome::passed(format!(
                "DFS operational invariants hold on node {node}"
            )));
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

fn run_lightweight_latency(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = ctx
        .parameter("base")
        .unwrap_or(DEFAULT_ROUNDTRIP_BASE)
        .trim_end_matches('/')
        .to_string();
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

    let backend_open_started = Instant::now();
    let backend = open_backend(ctx)?;
    let backend_open_elapsed = backend_open_started.elapsed();
    let node = backend.node_label();
    let nonce = probe_nonce();
    let fixture_dir = format!("{base}/latency-{nonce}");
    let file_name = "tiny.txt";
    let file_path = format!("{fixture_dir}/{file_name}");
    let payload = pattern_payload(
        &format!("dfs-lightweight-latency-{node}-{nonce}"),
        payload_bytes,
    );

    let setup_started = Instant::now();
    backend
        .create_dir_all(&fixture_dir)
        .map_err(SinomoTestError::failed)?;
    backend
        .write_file(&file_path, &payload)
        .map_err(SinomoTestError::failed)?;
    let setup_elapsed = setup_started.elapsed();

    for _ in 0..warmup_iterations {
        run_lightweight_latency_operations(
            &backend,
            &fixture_dir,
            &file_path,
            file_name,
            &payload,
        )?;
    }

    let mut latency_us = BTreeMap::<&'static str, Vec<u64>>::new();
    let measurement_started = Instant::now();
    for _ in 0..iterations {
        record_lightweight_operation_latency(&mut latency_us, "stat_file", || {
            validate_lightweight_stat_file(&backend, &file_path, payload_bytes)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "stat_dir", || {
            validate_lightweight_stat_dir(&backend, &fixture_dir)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "list_dir", || {
            validate_lightweight_list_dir(&backend, &fixture_dir, file_name)
        })?;
        record_lightweight_operation_latency(&mut latency_us, "read_prefix", || {
            validate_lightweight_read_prefix(&backend, &file_path, &payload)
        })?;
    }
    let measurement_elapsed = measurement_started.elapsed();

    ctx.record("node", &node);
    ctx.record("base", &base);
    ctx.record("fixture_dir", &fixture_dir);
    ctx.record("file_path", &file_path);
    ctx.record("iterations", iterations);
    ctx.record("warmup_iterations", warmup_iterations);
    ctx.record("payload_bytes", payload_bytes);
    ctx.record("max_p50_us", max_p50_us);
    ctx.record("max_p95_us", max_p95_us);
    ctx.record("max_max_us", max_max_us);
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

    if max_p50_us > 0 && worst_p50.1 > max_p50_us {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS lightweight latency worst p50 {}={}us is above allowed {max_p50_us}us",
            worst_p50.0, worst_p50.1
        )));
    }
    if max_p95_us > 0 && worst_p95.1 > max_p95_us {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS lightweight latency worst p95 {}={}us is above allowed {max_p95_us}us",
            worst_p95.0, worst_p95.1
        )));
    }
    if max_max_us > 0 && worst_max.1 > max_max_us {
        return Ok(SinomoTestOutcome::failed(format!(
            "DFS lightweight latency worst max {}={}us is above allowed {max_max_us}us",
            worst_max.0, worst_max.1
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "DFS lightweight latency on node {node}: worst p50 {}={}us, worst p95 {}={}us over {iterations} warmed iterations",
        worst_p50.0, worst_p50.1, worst_p95.0, worst_p95.1
    )))
}

fn run_lightweight_latency_operations(
    backend: &SifsFileManagerBackend,
    fixture_dir: &str,
    file_path: &str,
    file_name: &str,
    payload: &[u8],
) -> Result<(), SinomoTestError> {
    validate_lightweight_stat_file(backend, file_path, payload.len())?;
    validate_lightweight_stat_dir(backend, fixture_dir)?;
    validate_lightweight_list_dir(backend, fixture_dir, file_name)?;
    validate_lightweight_read_prefix(backend, file_path, payload)?;
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
    expected_bytes: usize,
) -> Result<(), SinomoTestError> {
    let metadata = backend.stat(path).map_err(SinomoTestError::failed)?;
    if metadata.kind != crate::file_manager::FileManagerEntryKind::File {
        return Err(SinomoTestError::failed(format!(
            "{path} stat kind {:?}, expected file",
            metadata.kind
        )));
    }
    if metadata.size != Some(expected_bytes as u64) {
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
) -> Result<(), SinomoTestError> {
    let read = backend
        .read_file_prefix(path, expected.len() as u64)
        .map_err(SinomoTestError::failed)?;
    if read.truncated {
        return Err(SinomoTestError::failed(format!(
            "{path} read-prefix unexpectedly truncated"
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

fn run_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let payload = required_payload_bytes(ctx)?;
    let foreground_repair = optional_bool_parameter(ctx, "foreground-repair", false)?;
    let timeout = expect_timeout(ctx)?;
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    retry_bulk_mutation(
        deadline,
        || {
            if foreground_repair {
                backend.write_file_with_foreground_replica_repair(&path, &payload)
            } else {
                backend.write_file(&path, &payload)
            }
        },
        || format!("cannot write SIFS file {path}"),
    )?;
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("bytes", payload.len());
    ctx.record("foreground_repair", foreground_repair);
    ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
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
    let backend = open_read_only_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("min_locations", min_locations);
    loop {
        let last_state = match backend.dfs_stat(&path) {
            Ok(stat)
                if stat.content_hash.is_some() && stat.current_locations.len() >= min_locations =>
            {
                ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                ctx.record("object_id", stat.object_id.to_string());
                ctx.record("version", stat.version.to_string());
                ctx.record("content_hash", stat.content_hash.unwrap_or_default());
                ctx.record("size_bytes", stat.size_bytes.unwrap_or_default());
                ctx.record(
                    "current_locations",
                    stat_location_labels(&stat.current_locations),
                );
                return Ok(SinomoTestOutcome::passed(format!(
                    "{path} has {} current locations on node {node} after {}ms",
                    stat.current_locations.len(),
                    started.elapsed().as_millis()
                )));
            }
            Ok(stat) => {
                format!(
                    "locations={} content_hash={:?} size={:?}",
                    stat.current_locations.len(),
                    stat.content_hash,
                    stat.size_bytes
                )
            }
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
    loop {
        match backend.rename(&from, &to) {
            Ok(()) => {
                ctx.record("node", &node);
                ctx.record("from", &from);
                ctx.record("to", &to);
                ctx.record("elapsed_ms", started.elapsed().as_millis() as u64);
                return Ok(SinomoTestOutcome::passed(format!(
                    "renamed {from} to {to} on node {node} after {}ms",
                    started.elapsed().as_millis()
                )));
            }
            Err(error) if is_retryable_rename_error(&error) => {
                if Instant::now() >= deadline {
                    ctx.record("node", &node);
                    ctx.record("from", &from);
                    ctx.record("to", &to);
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
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let metadata = backend.stat(&path).map_err(SinomoTestError::failed)?;
    match metadata.kind {
        crate::file_manager::FileManagerEntryKind::Directory => {
            backend.delete_dir(&path).map_err(SinomoTestError::failed)?
        }
        _ => backend
            .delete_file(&path)
            .map_err(SinomoTestError::failed)?,
    }
    ctx.record("node", &node);
    ctx.record("path", &path);
    Ok(SinomoTestOutcome::passed(format!(
        "removed {path} on node {node}"
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

fn run_bulk_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let base = ctx
        .required_parameter("base")?
        .trim_end_matches('/')
        .to_string();
    let files = optional_usize_parameter(ctx, "files", DEFAULT_BULK_FILE_COUNT)?;
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_BULK_FILE_BYTES)?;
    let timeout = expect_timeout(ctx)?;
    let min_files_per_second = optional_f64_parameter(ctx, "min-files-per-second", 0.0)?;
    let max_proc_wchar_amplification =
        optional_f64_parameter(ctx, "max-proc-wchar-amplification", 0.0)?;
    let max_proc_write_bytes_amplification =
        optional_f64_parameter(ctx, "max-proc-write-bytes-amplification", 0.0)?;
    let max_proc_syscw_per_file = optional_f64_parameter(ctx, "max-proc-syscw-per-file", 0.0)?;
    require_positive("files", files)?;
    require_positive("bytes", bytes)?;

    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let proc_io_before = current_proc_io();

    let started = Instant::now();
    let deadline = started + timeout;
    retry_bulk_mutation(
        deadline,
        || backend.create_dir_all_deferred_local_replay(&base),
        || format!("cannot create SIFS directory {base}"),
    )?;
    let mkdir_elapsed = started.elapsed();
    let proc_io_after_mkdir = current_proc_io();
    let batch_started = Instant::now();
    let batch = (0..files)
        .map(|index| (bulk_path(&base, index), bulk_payload(&base, index, bytes)))
        .collect::<Vec<_>>();
    let batch_build_elapsed = batch_started.elapsed();
    let write_started = Instant::now();
    retry_bulk_mutation(
        deadline,
        || backend.write_files_with_known_parents_deferred_publish(&batch),
        || format!("cannot write SIFS bulk batch under {base}"),
    )?;
    let write_elapsed = write_started.elapsed();
    let proc_io_after_write = current_proc_io();
    let publish_started = Instant::now();
    backend.publish_metadata_after_mutation();
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
    ctx.record("total_bytes", total_bytes);
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
        if max_proc_syscw_per_file > 0.0 && proc_syscw_per_file > max_proc_syscw_per_file {
            return Ok(SinomoTestOutcome::failed(format!(
                "bulk write on node {node} wrote {files} files with proc syscw/file {proc_syscw_per_file:.2}, above allowed {max_proc_syscw_per_file:.2}"
            )));
        }
    }

    if min_files_per_second > 0.0 && files_per_second < min_files_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "bulk write on node {node} wrote {files} files in {elapsed_ms}ms ({files_per_second:.2} files/s), below required {min_files_per_second:.2} files/s"
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "bulk write on node {node} wrote {files} files ({total_bytes} bytes) in {elapsed_ms}ms ({files_per_second:.2} files/s)"
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
            || backend.create_dir_all_deferred_local_replay(parent),
            || format!("cannot create SIFS parent directory {parent}"),
        )?;
    }
    let mkdir_elapsed = mkdir_started.elapsed();
    let proc_io_before = current_proc_io();
    let write_started = Instant::now();
    retry_bulk_mutation(
        deadline,
        || backend.write_file_with_known_parent_deferred_publish(&path, &payload),
        || format!("cannot write large SIFS file {path}"),
    )?;
    let write_elapsed = write_started.elapsed();
    let proc_io_after_write = current_proc_io();
    let publish_started = Instant::now();
    backend.publish_metadata_after_mutation();
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
    let raw_baseline = if min_raw_write_ratio > 0.0 {
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

fn open_read_only_backend(
    ctx: &SinomoTestContext,
) -> Result<SifsFileManagerBackend, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    SifsFileManagerBackend::open_read_only(&data_dir).map_err(|error| {
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
    loop {
        match open_backend(ctx) {
            Ok(backend) => return Ok(backend),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => std::thread::sleep(EXPECT_POLL_INTERVAL),
        }
    }
}

fn open_read_only_backend_until(
    ctx: &SinomoTestContext,
    deadline: Instant,
) -> Result<SifsFileManagerBackend, SinomoTestError> {
    loop {
        match open_read_only_backend(ctx) {
            Ok(backend) => return Ok(backend),
            Err(error) if Instant::now() >= deadline => return Err(error),
            Err(_) => std::thread::sleep(EXPECT_POLL_INTERVAL),
        }
    }
}

fn retry_bulk_mutation<T, E, F, A>(
    deadline: Instant,
    mut operation: F,
    action: A,
) -> Result<T, SinomoTestError>
where
    F: FnMut() -> Result<T, E>,
    E: ToString,
    A: Fn() -> String,
{
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) => {
                let error = error.to_string();
                if Instant::now() >= deadline || !is_retryable_dfs_probe_error(&error) {
                    return Err(SinomoTestError::failed(format!("{}: {error}", action())));
                }
                std::thread::sleep(EXPECT_POLL_INTERVAL);
            }
        }
    }
}

struct DfsProtocolCodecSample {
    label: &'static str,
    command: DfsMessageCommand,
    protocol_payload_bytes: usize,
}

struct EncodedDfsProtocolCodecSample {
    label: &'static str,
    request: sinomo_runtime::network::ObjectRpcRequest,
    request_body_bytes: usize,
    protocol_payload_bytes: usize,
}

fn dfs_protocol_codec_samples(
    payload: &[u8],
) -> Result<Vec<DfsProtocolCodecSample>, SinomoTestError> {
    let source_node_id = NodeId::from_legacy_named("dfs_codec_source").expect("source node id");
    let target_node_id = NodeId::from_legacy_named("dfs_codec_target").expect("target node id");
    let file_system_id =
        DfsFileSystemId::from_legacy_named(SIFS_FILE_SYSTEM_ID).expect("filesystem id");
    let object_id = DfsObjectId::from_legacy_named("dfs_codec_object").expect("object id");
    let version = DfsVersion::from_legacy_named("dfs_codec_version").expect("version id");
    let total_size_bytes = payload.len() as u64;

    Ok(vec![
        dfs_protocol_codec_sample(
            "stage-blob-chunk",
            1,
            1,
            &source_node_id,
            &target_node_id,
            DfsNetworkMessage::StageBlobChunk(DfsStageBlobChunkCommand {
                request_id: "dfs-codec-stage".to_string(),
                intent_id: "dfs-codec-intent".to_string(),
                file_system_id: file_system_id.clone(),
                object_id: object_id.clone(),
                expected_base_version: None,
                version: version.clone(),
                content_hash: "dfs-codec-hash".to_string(),
                total_size_bytes,
                chunk_index: 0,
                total_chunks: 1,
                chunk_hash: None,
                bytes: payload.to_vec(),
                object_metadata: None,
                authority_shard: None,
            }),
        )?,
        dfs_protocol_codec_sample(
            "read-response-chunk",
            2,
            2,
            &target_node_id,
            &source_node_id,
            DfsNetworkMessage::ReadResponseChunk(DfsReadResponseChunk {
                request_id: "dfs-codec-read".to_string(),
                object_id: object_id.clone(),
                version: version.clone(),
                content_hash: Some("dfs-codec-hash".to_string()),
                range_offset: None,
                object_total_size_bytes: None,
                total_size_bytes,
                chunk_index: 0,
                total_chunks: 1,
                chunk_hash: None,
                bytes: payload.to_vec(),
            }),
        )?,
        dfs_protocol_codec_sample(
            "fetch-blob-response-chunk",
            3,
            3,
            &target_node_id,
            &source_node_id,
            DfsNetworkMessage::FetchBlobResponseChunk(DfsFetchBlobResponseChunk {
                request_id: "dfs-codec-fetch".to_string(),
                intent_id: "dfs-codec-intent".to_string(),
                object_id,
                version,
                source_node_id: target_node_id.clone(),
                content_hash: "dfs-codec-hash".to_string(),
                total_size_bytes,
                chunk_index: 0,
                total_chunks: 1,
                chunk_hash: None,
                bytes: payload.to_vec(),
            }),
        )?,
    ])
}

fn dfs_protocol_codec_sample(
    label: &'static str,
    frame_id: u64,
    message_id: u64,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    message: DfsNetworkMessage,
) -> Result<DfsProtocolCodecSample, SinomoTestError> {
    let protocol_payload_bytes = dfs_message_payload_bytes(&message)?;
    Ok(DfsProtocolCodecSample {
        label,
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
        protocol_payload_bytes,
    })
}

fn dfs_message_payload_bytes(message: &DfsNetworkMessage) -> Result<usize, SinomoTestError> {
    match message {
        DfsNetworkMessage::StageBlobChunk(command) => Ok(command.bytes.len()),
        DfsNetworkMessage::ReadResponseChunk(command) => Ok(command.bytes.len()),
        DfsNetworkMessage::FetchBlobResponseChunk(command) => Ok(command.bytes.len()),
        other => Err(SinomoTestError::failed(format!(
            "unexpected DFS protocol message variant: {other:?}"
        ))),
    }
}

fn decoded_dfs_message_payload_bytes(
    command: &DfsMessageCommand,
) -> Result<usize, SinomoTestError> {
    if command.routed_message.source_node_id == command.routed_message.target_node_id {
        return Err(SinomoTestError::failed(
            "decoded DFS message did not preserve distinct source and target nodes",
        ));
    }
    dfs_message_payload_bytes(&command.routed_message.message)
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
        Some("/".to_string())
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

fn is_retryable_rename_error(error: &str) -> bool {
    error.contains("moving between metadata shards")
        || error.contains("retry the operation")
        || error.contains("metadata shard config incomplete")
        || is_retryable_dfs_probe_error(error)
}

fn is_retryable_dfs_probe_error(error: &str) -> bool {
    error.contains("metadata shard config incomplete")
        || error.contains("authority redirects")
        || error.contains("write did not commit yet")
        || (error.contains("DFS error: network error") && error.contains("timed out"))
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
