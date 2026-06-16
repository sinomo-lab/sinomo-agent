//! DFS cluster runtime test cases.
//!
//! These run on a live cluster node (`sinomo test run <id> --require-cluster`)
//! against the node's own SIFS service, so an external orchestrator (see
//! docker/test-cluster.sh) can drive cross-node consistency scenarios by
//! invoking parameterized probes on different nodes:
//!
//! - `cluster.dfs.read-your-write` — self-contained write/read/rename/delete
//!   roundtrip; read-your-write must hold immediately on the writing node.
//! - `cluster.dfs.invariants` — operational-metric sweep: no open conflicts,
//!   no stuck routed namespace ops, no leaked cross-shard locks.
//! - `cluster.dfs.write` / `cluster.dfs.expect-file` / `cluster.dfs.expect-absent`
//!   / `cluster.dfs.rename` / `cluster.dfs.remove` — parameterized probes
//!   (`--param path=... --param payload=...`); the expect-* probes poll with a
//!   bounded deadline to absorb cross-node replication lag.
//! - `cluster.dfs.expect-replicated` / `cluster.dfs.damage-local-loose-blob`
//!   — fault-injection probes used by the Docker gate to verify reads survive
//!   one damaged durable replica.
//! - `cluster.dfs.bulk-write` / `cluster.dfs.bulk-expect` — deterministic bulk
//!   write/read probes for cluster-level throughput, convergence evidence, and
//!   steady-state process I/O accounting.
//! - `cluster.dfs.large-write` / `cluster.dfs.large-expect` — deterministic
//!   large-object throughput probes that exercise chunked transfer and read-side
//!   no-write accounting.

use crate::file_manager::FileManagerBackend;
use crate::sifs_file_manager::SifsFileManagerBackend;
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_EXPECT_TIMEOUT_SECONDS: u64 = 30;
const DEFAULT_BULK_FILE_COUNT: usize = 128;
const DEFAULT_BULK_FILE_BYTES: usize = 4 * 1024;
const DEFAULT_LARGE_FILE_BYTES: usize = 16 * 1024 * 1024;
const EXPECT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_ROUNDTRIP_BASE: &str = "/services/cluster-tests";

pub fn register_dfs_cluster_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(dfs_read_your_write())
        .register(dfs_invariants())
        .register(dfs_write())
        .register(dfs_expect_file())
        .register(dfs_expect_absent())
        .register(dfs_expect_replicated())
        .register(dfs_damage_local_loose_blob())
        .register(dfs_rename())
        .register(dfs_remove())
        .register(dfs_bulk_write())
        .register(dfs_bulk_expect())
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
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    let nonce = probe_nonce();
    let path = format!("{base}/ryw-{nonce}.txt");
    let renamed = format!("{base}/ryw-{nonce}-renamed.txt");
    let payload = format!("read-your-write {node} {nonce}");
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("renamed", &renamed);

    backend
        .write_file(&path, payload.as_bytes())
        .map_err(SinomoTestError::failed)?;
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

fn run_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let payload = required_payload_bytes(ctx)?;
    let backend = open_backend(ctx)?;
    let node = backend.node_label();
    backend
        .write_file(&path, &payload)
        .map_err(SinomoTestError::failed)?;
    ctx.record("node", &node);
    ctx.record("path", &path);
    ctx.record("bytes", payload.len());
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
                    ctx.record("proc_rchar_delta", proc_io_delta.rchar);
                    ctx.record("proc_wchar_delta", proc_io_delta.wchar);
                    ctx.record("proc_read_bytes_delta", proc_io_delta.read_bytes);
                    ctx.record("proc_write_bytes_delta", proc_io_delta.write_bytes);
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
    if let Some(proc_io_delta) = proc_io_delta {
        let proc_wchar_amplification =
            amplification_ratio(proc_io_delta.wchar, total_bytes).unwrap_or(0.0);
        let proc_write_bytes_amplification =
            amplification_ratio(proc_io_delta.write_bytes, total_bytes).unwrap_or(0.0);
        ctx.record("proc_rchar_delta", proc_io_delta.rchar);
        ctx.record("proc_wchar_delta", proc_io_delta.wchar);
        ctx.record("proc_read_bytes_delta", proc_io_delta.read_bytes);
        ctx.record("proc_write_bytes_delta", proc_io_delta.write_bytes);
        ctx.record("proc_wchar_amplification", proc_wchar_amplification);
        ctx.record(
            "proc_write_bytes_amplification",
            proc_write_bytes_amplification,
        );
        if let Some(delta) = proc_io_mkdir_delta {
            ctx.record("proc_write_bytes_after_mkdir_delta", delta.write_bytes);
            ctx.record("proc_wchar_after_mkdir_delta", delta.wchar);
        }
        if let Some(delta) = proc_io_write_delta {
            ctx.record("proc_write_bytes_bulk_write_delta", delta.write_bytes);
            ctx.record("proc_wchar_bulk_write_delta", delta.wchar);
        }
        if let Some(delta) = proc_io_publish_delta {
            ctx.record("proc_write_bytes_publish_delta", delta.write_bytes);
            ctx.record("proc_wchar_publish_delta", delta.wchar);
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
                ctx.record("proc_rchar_delta", proc_io_delta.rchar);
                ctx.record("proc_wchar_delta", proc_io_delta.wchar);
                ctx.record("proc_read_bytes_delta", proc_io_delta.read_bytes);
                ctx.record("proc_write_bytes_delta", proc_io_delta.write_bytes);
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

fn run_large_write(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let path = ctx.required_parameter("path")?.to_string();
    let bytes = optional_usize_parameter(ctx, "bytes", DEFAULT_LARGE_FILE_BYTES)?;
    let timeout = expect_timeout(ctx)?;
    let min_bytes_per_second = optional_f64_parameter(ctx, "min-bytes-per-second", 0.0)?;
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
        .zip(current_proc_io())
        .map(|(before, after)| after.saturating_delta(before));
    let proc_io_write_delta = proc_io_before
        .zip(proc_io_after_write)
        .map(|(before, after)| after.saturating_delta(before));
    let proc_io_publish_delta = proc_io_after_write
        .zip(proc_io_after_publish)
        .map(|(before, after)| after.saturating_delta(before));

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
        ctx.record("proc_rchar_delta", proc_io_delta.rchar);
        ctx.record("proc_wchar_delta", proc_io_delta.wchar);
        ctx.record("proc_read_bytes_delta", proc_io_delta.read_bytes);
        ctx.record("proc_write_bytes_delta", proc_io_delta.write_bytes);
        ctx.record("proc_wchar_amplification", proc_wchar_amplification);
        ctx.record(
            "proc_write_bytes_amplification",
            proc_write_bytes_amplification,
        );
        if let Some(delta) = proc_io_write_delta {
            ctx.record("proc_write_bytes_data_write_delta", delta.write_bytes);
            ctx.record("proc_wchar_data_write_delta", delta.wchar);
        }
        if let Some(delta) = proc_io_publish_delta {
            ctx.record("proc_write_bytes_publish_delta", delta.write_bytes);
            ctx.record("proc_wchar_publish_delta", delta.wchar);
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
    let max_proc_write_bytes = optional_u64_parameter(ctx, "max-proc-write-bytes")?;
    require_positive("bytes", bytes)?;

    let payload_started = Instant::now();
    let expected = large_payload(&path, bytes);
    let payload_build_elapsed = payload_started.elapsed();
    let started = Instant::now();
    let deadline = started + timeout;
    let backend = open_read_only_backend_until(ctx, deadline)?;
    let node = backend.node_label();
    let measurement_started = Instant::now();
    let proc_io_before = current_proc_io();
    let mut last_state: String;

    loop {
        match backend.read_file(&path) {
            Ok(actual) if actual.as_slice() == expected.as_slice() => {
                let elapsed = measurement_started.elapsed();
                let elapsed_ms = elapsed.as_millis() as u64;
                let total_bytes = bytes as u64;
                let bytes_per_second = rate_per_second(total_bytes as f64, elapsed);
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
                ctx.record("bytes_per_second", bytes_per_second);
                ctx.record("min_bytes_per_second", min_bytes_per_second);
                if let Some(max_proc_write_bytes) = max_proc_write_bytes {
                    ctx.record("max_proc_write_bytes", max_proc_write_bytes);
                }
                if let Some(proc_io_delta) = proc_io_delta {
                    ctx.record("proc_rchar_delta", proc_io_delta.rchar);
                    ctx.record("proc_wchar_delta", proc_io_delta.wchar);
                    ctx.record("proc_read_bytes_delta", proc_io_delta.read_bytes);
                    ctx.record("proc_write_bytes_delta", proc_io_delta.write_bytes);
                    if let Some(max_proc_write_bytes) = max_proc_write_bytes
                        && proc_io_delta.write_bytes > max_proc_write_bytes
                    {
                        return Ok(SinomoTestOutcome::failed(format!(
                            "large expect on node {node} read {total_bytes} bytes with proc write_bytes delta {}, above allowed {max_proc_write_bytes}",
                            proc_io_delta.write_bytes
                        )));
                    }
                }

                if min_bytes_per_second > 0.0 && bytes_per_second < min_bytes_per_second {
                    return Ok(SinomoTestOutcome::failed(format!(
                        "large expect on node {node} read {total_bytes} bytes in {elapsed_ms}ms ({bytes_per_second:.2} bytes/s), below required {min_bytes_per_second:.2} bytes/s"
                    )));
                }
                return Ok(SinomoTestOutcome::passed(format!(
                    "large expect on node {node} read {total_bytes} bytes in {elapsed_ms}ms ({bytes_per_second:.2} bytes/s)"
                )));
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

        if Instant::now() >= deadline {
            ctx.record("node", &node);
            ctx.record("path", &path);
            ctx.record("bytes", bytes as u64);
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

#[derive(Clone, Copy, Debug, Default)]
struct ProcIoSample {
    rchar: u64,
    wchar: u64,
    read_bytes: u64,
    write_bytes: u64,
}

impl ProcIoSample {
    fn saturating_delta(self, before: Self) -> Self {
        Self {
            rchar: self.rchar.saturating_sub(before.rchar),
            wchar: self.wchar.saturating_sub(before.wchar),
            read_bytes: self.read_bytes.saturating_sub(before.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(before.write_bytes),
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
            "read_bytes" => sample.read_bytes = value,
            "write_bytes" => sample.write_bytes = value,
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
    (error.contains("DFS error: network error") && error.contains("timed out"))
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
