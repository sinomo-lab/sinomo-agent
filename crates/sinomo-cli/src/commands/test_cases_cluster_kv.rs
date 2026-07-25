//! Live cluster-KV performance probes.
//!
//! These cases deliberately send transactions through the running local
//! daemon. A successful sample therefore represents a quorum-backed cluster-KV
//! commit (including authority forwarding when the local daemon is not the
//! leader), rather than a write to the CLI process's local snapshot.

use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::cluster;
use sinomo_runtime::kv::{KvEntry, KvOp, KvTxn, KvTxnRequest, KvTxnResponse};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_ITERATIONS: usize = 12;
const MAX_ITERATIONS: usize = 128;
const DEFAULT_WARMUP_ITERATIONS: usize = 4;
const MAX_WARMUP_ITERATIONS: usize = 64;
const DEFAULT_OPS_PER_TRANSACTION: usize = 1;
const MAX_OPS_PER_TRANSACTION: usize = 256;
const DEFAULT_VALUE_BYTES: usize = 256;
const MAX_VALUE_BYTES: usize = 16 * 1024;
const MAX_TRANSACTION_VALUE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOTAL_VALUE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_MAX_FAILURES: usize = 0;
const DEFAULT_MAX_P50_MS: u64 = 1_000;
const DEFAULT_MAX_P95_MS: u64 = 2_000;
const DEFAULT_MIN_COMMITS_PER_SECOND: f64 = 1.0;
const DEFAULT_RANGE_ITERATIONS: usize = 64;
const MAX_RANGE_ITERATIONS: usize = 512;
const DEFAULT_RANGE_ENTRIES: usize = 64;
const MAX_RANGE_ENTRIES: usize = 1_024;
const MAX_RANGE_VALUE_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MIN_READS_PER_SECOND: f64 = 0.0;

pub fn register_cluster_kv_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(cluster_kv_quorum_commit_benchmark())
        .register(cluster_kv_range_read_benchmark())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn cluster_kv_quorum_commit_benchmark(ctx) {
        id: "cluster.kv.quorum-commit-benchmark",
        summary: "Sequential daemon-mediated cluster-KV quorum commit latency and throughput",
        tags: ["cluster-kv", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_cluster_kv_quorum_commit_benchmark(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn cluster_kv_range_read_benchmark(ctx) {
        id: "cluster.kv.range-read-benchmark",
        summary: "Daemon-mediated cluster-KV local or authoritative prefix-read latency and throughput",
        tags: ["cluster-kv", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_cluster_kv_range_read_benchmark(ctx)
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ClusterKvBenchmarkOptions {
    iterations: usize,
    warmup_iterations: usize,
    ops_per_transaction: usize,
    value_bytes: usize,
    timeout_ms: u64,
    max_failures: usize,
    max_p50_ms: u64,
    max_p95_ms: u64,
    min_commits_per_second: f64,
}

impl ClusterKvBenchmarkOptions {
    fn from_context(ctx: &SinomoTestContext) -> Result<Self, SinomoTestError> {
        let options = Self {
            iterations: usize_parameter(ctx, "iterations", DEFAULT_ITERATIONS)?,
            warmup_iterations: usize_parameter(
                ctx,
                "warmup-iterations",
                DEFAULT_WARMUP_ITERATIONS,
            )?,
            ops_per_transaction: usize_parameter(
                ctx,
                "ops-per-transaction",
                DEFAULT_OPS_PER_TRANSACTION,
            )?,
            value_bytes: usize_parameter(ctx, "value-bytes", DEFAULT_VALUE_BYTES)?,
            timeout_ms: u64_parameter(ctx, "timeout-ms", DEFAULT_TIMEOUT_MS)?,
            max_failures: usize_parameter(ctx, "max-failures", DEFAULT_MAX_FAILURES)?,
            max_p50_ms: u64_parameter(ctx, "max-p50-ms", DEFAULT_MAX_P50_MS)?,
            max_p95_ms: u64_parameter(ctx, "max-p95-ms", DEFAULT_MAX_P95_MS)?,
            min_commits_per_second: f64_parameter(
                ctx,
                "min-commits-per-second",
                DEFAULT_MIN_COMMITS_PER_SECOND,
            )?,
        };
        options.validate()?;
        Ok(options)
    }

    fn validate(&self) -> Result<(), SinomoTestError> {
        if self.iterations == 0 || self.iterations > MAX_ITERATIONS {
            return Err(invalid_parameter(
                "iterations",
                format!("must be between 1 and {MAX_ITERATIONS}"),
            ));
        }
        if self.warmup_iterations > MAX_WARMUP_ITERATIONS {
            return Err(invalid_parameter(
                "warmup-iterations",
                format!("must not exceed {MAX_WARMUP_ITERATIONS}"),
            ));
        }
        if self.ops_per_transaction == 0 || self.ops_per_transaction > MAX_OPS_PER_TRANSACTION {
            return Err(invalid_parameter(
                "ops-per-transaction",
                format!("must be between 1 and {MAX_OPS_PER_TRANSACTION}"),
            ));
        }
        if self.value_bytes > MAX_VALUE_BYTES {
            return Err(invalid_parameter(
                "value-bytes",
                format!("must not exceed {MAX_VALUE_BYTES}"),
            ));
        }
        let transaction_value_bytes = self.ops_per_transaction.saturating_mul(self.value_bytes);
        if transaction_value_bytes > MAX_TRANSACTION_VALUE_BYTES {
            return Err(invalid_parameter(
                "ops-per-transaction",
                format!(
                    "ops-per-transaction * value-bytes must not exceed {MAX_TRANSACTION_VALUE_BYTES} bytes"
                ),
            ));
        }
        let total_value_bytes = self
            .iterations
            .saturating_add(self.warmup_iterations)
            .saturating_mul(transaction_value_bytes);
        if total_value_bytes > MAX_TOTAL_VALUE_BYTES {
            return Err(invalid_parameter(
                "value-bytes",
                format!(
                    "(iterations + warmup-iterations) * ops-per-transaction * value-bytes must not exceed {MAX_TOTAL_VALUE_BYTES} bytes"
                ),
            ));
        }
        if self.timeout_ms == 0 {
            return Err(invalid_parameter("timeout-ms", "must be greater than zero"));
        }
        if self.max_failures > self.iterations {
            return Err(invalid_parameter(
                "max-failures",
                "must not exceed iterations",
            ));
        }
        if self.max_p50_ms > 0 && self.max_p95_ms > 0 && self.max_p50_ms > self.max_p95_ms {
            return Err(invalid_parameter(
                "max-p50-ms",
                "must not exceed max-p95-ms when both thresholds are enabled",
            ));
        }
        if !self.min_commits_per_second.is_finite() || self.min_commits_per_second < 0.0 {
            return Err(invalid_parameter(
                "min-commits-per-second",
                "must be a finite, non-negative number",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterKvReadConsistency {
    Local,
    Authoritative,
}

impl ClusterKvReadConsistency {
    fn from_context(ctx: &SinomoTestContext) -> Result<Self, SinomoTestError> {
        match ctx.parameter("consistency").unwrap_or("local") {
            "local" => Ok(Self::Local),
            "authoritative" => Ok(Self::Authoritative),
            other => Err(invalid_parameter(
                "consistency",
                format!("must be `local` or `authoritative`, got `{other}`"),
            )),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Authoritative => "authoritative",
        }
    }

    const fn storage_path(self) -> &'static str {
        match self {
            Self::Local => "daemon-object-rpc/local-materialized-snapshot",
            Self::Authoritative => "daemon-object-rpc/current-term-quorum-barrier",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ClusterKvRangeBenchmarkOptions {
    consistency: ClusterKvReadConsistency,
    iterations: usize,
    warmup_iterations: usize,
    entries: usize,
    value_bytes: usize,
    timeout_ms: u64,
    max_failures: usize,
    max_p50_ms: u64,
    max_p95_ms: u64,
    min_reads_per_second: f64,
}

impl ClusterKvRangeBenchmarkOptions {
    fn from_context(ctx: &SinomoTestContext) -> Result<Self, SinomoTestError> {
        let options = Self {
            consistency: ClusterKvReadConsistency::from_context(ctx)?,
            iterations: usize_parameter(ctx, "iterations", DEFAULT_RANGE_ITERATIONS)?,
            warmup_iterations: usize_parameter(
                ctx,
                "warmup-iterations",
                DEFAULT_WARMUP_ITERATIONS,
            )?,
            entries: usize_parameter(ctx, "entries", DEFAULT_RANGE_ENTRIES)?,
            value_bytes: usize_parameter(ctx, "value-bytes", DEFAULT_VALUE_BYTES)?,
            timeout_ms: u64_parameter(ctx, "timeout-ms", DEFAULT_TIMEOUT_MS)?,
            max_failures: usize_parameter(ctx, "max-failures", DEFAULT_MAX_FAILURES)?,
            max_p50_ms: u64_parameter(ctx, "max-p50-ms", 0)?,
            max_p95_ms: u64_parameter(ctx, "max-p95-ms", 0)?,
            min_reads_per_second: f64_parameter(
                ctx,
                "min-reads-per-second",
                DEFAULT_MIN_READS_PER_SECOND,
            )?,
        };
        options.validate()?;
        Ok(options)
    }

    fn validate(&self) -> Result<(), SinomoTestError> {
        if self.iterations == 0 || self.iterations > MAX_RANGE_ITERATIONS {
            return Err(invalid_parameter(
                "iterations",
                format!("must be between 1 and {MAX_RANGE_ITERATIONS}"),
            ));
        }
        if self.warmup_iterations > MAX_WARMUP_ITERATIONS {
            return Err(invalid_parameter(
                "warmup-iterations",
                format!("must not exceed {MAX_WARMUP_ITERATIONS}"),
            ));
        }
        if self.entries == 0 || self.entries > MAX_RANGE_ENTRIES {
            return Err(invalid_parameter(
                "entries",
                format!("must be between 1 and {MAX_RANGE_ENTRIES}"),
            ));
        }
        if self.value_bytes > MAX_VALUE_BYTES {
            return Err(invalid_parameter(
                "value-bytes",
                format!("must not exceed {MAX_VALUE_BYTES}"),
            ));
        }
        if self.entries.saturating_mul(self.value_bytes) > MAX_RANGE_VALUE_BYTES {
            return Err(invalid_parameter(
                "entries",
                format!("entries * value-bytes must not exceed {MAX_RANGE_VALUE_BYTES} bytes"),
            ));
        }
        if self.timeout_ms == 0 {
            return Err(invalid_parameter("timeout-ms", "must be greater than zero"));
        }
        if self.max_failures > self.iterations {
            return Err(invalid_parameter(
                "max-failures",
                "must not exceed iterations",
            ));
        }
        if self.max_p50_ms > 0 && self.max_p95_ms > 0 && self.max_p50_ms > self.max_p95_ms {
            return Err(invalid_parameter(
                "max-p50-ms",
                "must not exceed max-p95-ms when both thresholds are enabled",
            ));
        }
        if !self.min_reads_per_second.is_finite() || self.min_reads_per_second < 0.0 {
            return Err(invalid_parameter(
                "min-reads-per-second",
                "must be a finite, non-negative number",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ClusterKvBenchmarkMetrics {
    success_count: usize,
    failure_count: usize,
    elapsed_us: u64,
    commits_per_second: f64,
    operations_per_second: f64,
    value_bytes_per_second: f64,
    latency_min_us: u64,
    latency_p50_us: u64,
    latency_p95_us: u64,
    latency_max_us: u64,
    revision_min: Option<u64>,
    revision_max: Option<u64>,
    revisions_strictly_increasing: bool,
}

impl ClusterKvBenchmarkMetrics {
    fn from_samples(
        latencies_us: &[u64],
        revisions: &[u64],
        failure_count: usize,
        elapsed: Duration,
        value_bytes: usize,
        ops_per_transaction: usize,
    ) -> Self {
        let mut sorted_latencies = latencies_us.to_vec();
        sorted_latencies.sort_unstable();
        let success_count = sorted_latencies.len();
        let elapsed_seconds = elapsed.as_secs_f64();
        let commits_per_second = rate_per_second(success_count, elapsed_seconds);
        let successful_operations = success_count.saturating_mul(ops_per_transaction);
        let successful_value_bytes = successful_operations.saturating_mul(value_bytes);
        Self {
            success_count,
            failure_count,
            elapsed_us: duration_micros_u64(elapsed),
            commits_per_second,
            operations_per_second: rate_per_second(successful_operations, elapsed_seconds),
            value_bytes_per_second: if elapsed_seconds > 0.0 {
                successful_value_bytes as f64 / elapsed_seconds
            } else {
                0.0
            },
            latency_min_us: sorted_latencies.first().copied().unwrap_or(0),
            latency_p50_us: percentile_value(&sorted_latencies, 50),
            latency_p95_us: percentile_value(&sorted_latencies, 95),
            latency_max_us: sorted_latencies.last().copied().unwrap_or(0),
            revision_min: revisions.iter().copied().min(),
            revision_max: revisions.iter().copied().max(),
            revisions_strictly_increasing: revisions.windows(2).all(|pair| pair[0] < pair[1]),
        }
    }
}

fn run_cluster_kv_quorum_commit_benchmark(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let options = ClusterKvBenchmarkOptions::from_context(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = cluster::read_cluster_state_without_kv(&cluster_paths).map_err(|source| {
        SinomoTestError::failed(format!("cannot read local cluster state: {source}"))
    })?;
    let client = DaemonApiClient::connect()
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot connect to local daemon for cluster-KV benchmark: {source}"
            ))
        })?
        .with_timeout(Duration::from_millis(options.timeout_ms));

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let key_prefix = format!(
        "/tests/performance/cluster-kv/{}/{}-{nonce}/",
        state.local_node_id,
        std::process::id()
    );
    let value = benchmark_value(options.value_bytes);
    let mut latencies_us = Vec::with_capacity(options.iterations);
    let mut revisions = Vec::with_capacity(options.iterations);
    let mut failures = Vec::new();

    ctx.record("node_id", state.local_node_id.to_string());
    ctx.record("transport_path", "daemon-object-rpc/cluster-kv-txn");
    ctx.record("storage_path", "raft-quorum-commit");
    record_benchmark_profile(ctx);
    ctx.record("key_prefix", &key_prefix);
    ctx.record("iterations", options.iterations);
    ctx.record("warmup_iterations", options.warmup_iterations);
    ctx.record("ops_per_transaction", options.ops_per_transaction);
    ctx.record("value_bytes", options.value_bytes);
    ctx.record("timeout_ms", options.timeout_ms);
    ctx.record("max_failures", options.max_failures);
    ctx.record("max_p50_ms", options.max_p50_ms);
    ctx.record("max_p95_ms", options.max_p95_ms);
    ctx.record("min_commits_per_second", options.min_commits_per_second);

    for iteration in 0..options.warmup_iterations {
        let txn = benchmark_put_txn(
            &key_prefix,
            "warmup",
            iteration,
            options.ops_per_transaction,
            &value,
        );
        let warmup = commit_daemon_cluster_kv_txn(&client, txn).and_then(|response| {
            validate_benchmark_commit_response(&response, options.ops_per_transaction)
        });
        if let Err(detail) = warmup {
            let cleanup = cleanup_benchmark_prefix(&client, &key_prefix);
            record_cleanup(ctx, &cleanup);
            return Ok(SinomoTestOutcome::failed(format!(
                "cluster-KV benchmark warmup iteration {iteration} failed: {detail}"
            )));
        }
    }

    let measured_started = Instant::now();
    for iteration in 0..options.iterations {
        let txn = benchmark_put_txn(
            &key_prefix,
            "measured",
            iteration,
            options.ops_per_transaction,
            &value,
        );
        let started = Instant::now();
        match commit_daemon_cluster_kv_txn(&client, txn).and_then(|response| {
            validate_benchmark_commit_response(&response, options.ops_per_transaction)?;
            Ok(response)
        }) {
            Ok(response) => {
                latencies_us.push(duration_micros_u64(started.elapsed()));
                revisions.push(response.revision.0);
            }
            Err(detail) => failures.push(format!("iteration {iteration}: {detail}")),
        }
    }
    let measured_elapsed = measured_started.elapsed();

    // Delete by the unique run prefix so an RPC with an ambiguous outcome is
    // covered too. Cleanup is deliberately best-effort and is not part of the
    // measured latency distribution.
    let cleanup = cleanup_benchmark_prefix(&client, &key_prefix);
    record_cleanup(ctx, &cleanup);

    let metrics = ClusterKvBenchmarkMetrics::from_samples(
        &latencies_us,
        &revisions,
        failures.len(),
        measured_elapsed,
        options.value_bytes,
        options.ops_per_transaction,
    );
    record_metrics(ctx, &metrics, &revisions, &failures);

    if metrics.failure_count > options.max_failures {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV benchmark had {}/{} failed quorum commits, above allowed {}",
            metrics.failure_count, options.iterations, options.max_failures
        )));
    }
    if metrics.success_count == 0 {
        return Ok(SinomoTestOutcome::failed(
            "cluster-KV benchmark had no successful quorum commits",
        ));
    }
    if !metrics.revisions_strictly_increasing {
        return Ok(SinomoTestOutcome::failed(format!(
            "successful cluster-KV commit revisions were not strictly increasing: {revisions:?}"
        )));
    }
    if threshold_exceeded(metrics.latency_p50_us, options.max_p50_ms) {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV quorum commit p50 latency {:.3}ms is above allowed {}ms",
            micros_to_millis(metrics.latency_p50_us),
            options.max_p50_ms
        )));
    }
    if threshold_exceeded(metrics.latency_p95_us, options.max_p95_ms) {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV quorum commit p95 latency {:.3}ms is above allowed {}ms",
            micros_to_millis(metrics.latency_p95_us),
            options.max_p95_ms
        )));
    }
    if options.min_commits_per_second > 0.0
        && metrics.commits_per_second < options.min_commits_per_second
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV quorum commit throughput {:.2} commits/s is below required {:.2} commits/s",
            metrics.commits_per_second, options.min_commits_per_second
        )));
    }

    let cleanup_detail = cleanup_detail(&cleanup);
    Ok(SinomoTestOutcome::passed(format!(
        "cluster-KV committed {}/{} transactions ({} ops/txn) in {:.3}ms ({:.2} commits/s, {:.2} ops/s, p50={:.3}ms p95={:.3}ms, revisions={}..{}, {cleanup_detail})",
        metrics.success_count,
        options.iterations,
        options.ops_per_transaction,
        micros_to_millis(metrics.elapsed_us),
        metrics.commits_per_second,
        metrics.operations_per_second,
        micros_to_millis(metrics.latency_p50_us),
        micros_to_millis(metrics.latency_p95_us),
        metrics.revision_min.unwrap_or(0),
        metrics.revision_max.unwrap_or(0),
    )))
}

#[derive(Clone, Debug)]
struct ClusterKvRangeRead {
    entries: Vec<KvEntry>,
    revision: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct ClusterKvRangeBenchmarkMetrics {
    success_count: usize,
    failure_count: usize,
    elapsed_us: u64,
    reads_per_second: f64,
    entries_per_second: f64,
    value_bytes_per_second: f64,
    latency_min_us: u64,
    latency_p50_us: u64,
    latency_p95_us: u64,
    latency_max_us: u64,
    revision_min: Option<u64>,
    revision_max: Option<u64>,
    revisions_non_decreasing: bool,
}

impl ClusterKvRangeBenchmarkMetrics {
    fn from_samples(
        latencies_us: &[u64],
        revisions: &[u64],
        failure_count: usize,
        elapsed: Duration,
        entries_per_read: usize,
        value_bytes: usize,
    ) -> Self {
        let mut sorted_latencies = latencies_us.to_vec();
        sorted_latencies.sort_unstable();
        let success_count = sorted_latencies.len();
        let elapsed_seconds = elapsed.as_secs_f64();
        let successful_entries = success_count.saturating_mul(entries_per_read);
        let successful_value_bytes = successful_entries.saturating_mul(value_bytes);
        Self {
            success_count,
            failure_count,
            elapsed_us: duration_micros_u64(elapsed),
            reads_per_second: rate_per_second(success_count, elapsed_seconds),
            entries_per_second: rate_per_second(successful_entries, elapsed_seconds),
            value_bytes_per_second: rate_per_second(successful_value_bytes, elapsed_seconds),
            latency_min_us: sorted_latencies.first().copied().unwrap_or(0),
            latency_p50_us: percentile_value(&sorted_latencies, 50),
            latency_p95_us: percentile_value(&sorted_latencies, 95),
            latency_max_us: sorted_latencies.last().copied().unwrap_or(0),
            revision_min: revisions.iter().copied().min(),
            revision_max: revisions.iter().copied().max(),
            revisions_non_decreasing: revisions.windows(2).all(|pair| pair[0] <= pair[1]),
        }
    }
}

fn run_cluster_kv_range_read_benchmark(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let options = ClusterKvRangeBenchmarkOptions::from_context(ctx)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    let state = cluster::read_cluster_state_without_kv(&cluster_paths).map_err(|source| {
        SinomoTestError::failed(format!("cannot read local cluster state: {source}"))
    })?;
    let client = DaemonApiClient::connect()
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot connect to local daemon for cluster-KV range benchmark: {source}"
            ))
        })?
        .with_timeout(Duration::from_millis(options.timeout_ms));

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let key_prefix = format!(
        "/tests/performance/cluster-kv-range/{}/{}-{nonce}/",
        state.local_node_id,
        std::process::id()
    );
    let value = benchmark_value(options.value_bytes);
    let seed_txn = benchmark_put_txn(&key_prefix, "seed", 0, options.entries, &value);
    let seed_response = match commit_daemon_cluster_kv_txn(&client, seed_txn).and_then(|response| {
        validate_benchmark_commit_response(&response, options.entries)?;
        Ok(response)
    }) {
        Ok(response) => response,
        Err(detail) => {
            let cleanup = cleanup_benchmark_prefix(&client, &key_prefix);
            record_cleanup(ctx, &cleanup);
            return Ok(SinomoTestOutcome::failed(format!(
                "cannot seed cluster-KV range benchmark through quorum: {detail}"
            )));
        }
    };
    let expected_prefix = format!("{key_prefix}seed/0000/");
    let seed_revision = seed_response.revision.0;

    ctx.record("node_id", state.local_node_id.to_string());
    ctx.record("transport_path", "daemon-object-rpc/cluster-kv-range");
    ctx.record("storage_path", options.consistency.storage_path());
    ctx.record("consistency", options.consistency.as_str());
    record_benchmark_profile(ctx);
    ctx.record("key_prefix", &expected_prefix);
    ctx.record("seed_revision", seed_revision);
    ctx.record("iterations", options.iterations);
    ctx.record("warmup_iterations", options.warmup_iterations);
    ctx.record("entries", options.entries);
    ctx.record("value_bytes", options.value_bytes);
    ctx.record("timeout_ms", options.timeout_ms);
    ctx.record("max_failures", options.max_failures);
    ctx.record("max_p50_ms", options.max_p50_ms);
    ctx.record("max_p95_ms", options.max_p95_ms);
    ctx.record("min_reads_per_second", options.min_reads_per_second);

    for iteration in 0..options.warmup_iterations {
        let warmup = read_daemon_cluster_kv_range(&client, &expected_prefix, options.consistency)
            .and_then(|read| {
                validate_benchmark_range_read(
                    &read,
                    &expected_prefix,
                    options.entries,
                    &value,
                    seed_revision,
                )?;
                Ok(read)
            });
        if let Err(detail) = warmup {
            let cleanup = cleanup_benchmark_prefix(&client, &key_prefix);
            record_cleanup(ctx, &cleanup);
            return Ok(SinomoTestOutcome::failed(format!(
                "cluster-KV range benchmark warmup iteration {iteration} failed: {detail}"
            )));
        }
    }

    let mut latencies_us = Vec::with_capacity(options.iterations);
    let mut revisions = Vec::with_capacity(options.iterations);
    let mut failures = Vec::new();
    let measured_started = Instant::now();
    for iteration in 0..options.iterations {
        let started = Instant::now();
        match read_daemon_cluster_kv_range(&client, &expected_prefix, options.consistency).and_then(
            |read| {
                validate_benchmark_range_read(
                    &read,
                    &expected_prefix,
                    options.entries,
                    &value,
                    seed_revision,
                )?;
                Ok(read)
            },
        ) {
            Ok(read) => {
                latencies_us.push(duration_micros_u64(started.elapsed()));
                revisions.push(read.revision);
            }
            Err(detail) => failures.push(format!("iteration {iteration}: {detail}")),
        }
    }
    let measured_elapsed = measured_started.elapsed();

    let cleanup = cleanup_benchmark_prefix(&client, &key_prefix);
    record_cleanup(ctx, &cleanup);

    let metrics = ClusterKvRangeBenchmarkMetrics::from_samples(
        &latencies_us,
        &revisions,
        failures.len(),
        measured_elapsed,
        options.entries,
        options.value_bytes,
    );
    record_range_metrics(ctx, &metrics, &revisions, &failures);

    if metrics.failure_count > options.max_failures {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV {} range benchmark had {}/{} failed reads, above allowed {}",
            options.consistency.as_str(),
            metrics.failure_count,
            options.iterations,
            options.max_failures
        )));
    }
    if metrics.success_count == 0 {
        return Ok(SinomoTestOutcome::failed(
            "cluster-KV range benchmark had no successful reads",
        ));
    }
    if !metrics.revisions_non_decreasing {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV range-read revisions moved backward: {revisions:?}"
        )));
    }
    if threshold_exceeded(metrics.latency_p50_us, options.max_p50_ms) {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV {} range-read p50 latency {:.3}ms is above allowed {}ms",
            options.consistency.as_str(),
            micros_to_millis(metrics.latency_p50_us),
            options.max_p50_ms
        )));
    }
    if threshold_exceeded(metrics.latency_p95_us, options.max_p95_ms) {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV {} range-read p95 latency {:.3}ms is above allowed {}ms",
            options.consistency.as_str(),
            micros_to_millis(metrics.latency_p95_us),
            options.max_p95_ms
        )));
    }
    if options.min_reads_per_second > 0.0 && metrics.reads_per_second < options.min_reads_per_second
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "cluster-KV {} range throughput {:.2} reads/s is below required {:.2} reads/s",
            options.consistency.as_str(),
            metrics.reads_per_second,
            options.min_reads_per_second
        )));
    }

    let cleanup_detail = cleanup_detail(&cleanup);
    Ok(SinomoTestOutcome::passed(format!(
        "cluster-KV {} range read {}/{} samples of {} entries in {:.3}ms ({:.2} reads/s, {:.2} entries/s, p50={:.3}ms p95={:.3}ms, revisions={}..{}, {cleanup_detail})",
        options.consistency.as_str(),
        metrics.success_count,
        options.iterations,
        options.entries,
        micros_to_millis(metrics.elapsed_us),
        metrics.reads_per_second,
        metrics.entries_per_second,
        micros_to_millis(metrics.latency_p50_us),
        micros_to_millis(metrics.latency_p95_us),
        metrics.revision_min.unwrap_or(0),
        metrics.revision_max.unwrap_or(0),
    )))
}

fn read_daemon_cluster_kv_range(
    client: &DaemonApiClient,
    prefix: &str,
    consistency: ClusterKvReadConsistency,
) -> Result<ClusterKvRangeRead, String> {
    let payload = match consistency {
        ClusterKvReadConsistency::Local => {
            sinomo_runtime::kv::cluster_kv_local_range_prefix_rpc_payload(prefix)
        }
        ClusterKvReadConsistency::Authoritative => {
            sinomo_runtime::kv::cluster_kv_authoritative_range_prefix_rpc_payload(prefix)
        }
    }
    .map_err(|source| format!("cannot encode cluster-KV range request: {source}"))?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = client
        .call_request(&request)
        .map_err(|source| format!("daemon-mediated cluster-KV range read failed: {source}"))?;
    match consistency {
        ClusterKvReadConsistency::Local => {
            let read =
                sinomo_runtime::kv::decode_cluster_kv_local_range_prefix_rpc_body(&response.body)
                    .map_err(|source| {
                    format!("cannot decode local cluster-KV range response: {source}")
                })?;
            Ok(ClusterKvRangeRead {
                entries: read.value,
                revision: read.revision.0,
            })
        }
        ClusterKvReadConsistency::Authoritative => {
            let read = sinomo_runtime::kv::decode_cluster_kv_authoritative_range_prefix_rpc_body(
                &response.body,
            )
            .map_err(|source| {
                format!("cannot decode authoritative cluster-KV range response: {source}")
            })?;
            Ok(ClusterKvRangeRead {
                entries: read.value,
                revision: read.authority.revision.0,
            })
        }
    }
}

fn validate_benchmark_range_read(
    read: &ClusterKvRangeRead,
    prefix: &str,
    expected_entries: usize,
    expected_value: &[u8],
    minimum_revision: u64,
) -> Result<(), String> {
    if read.revision < minimum_revision {
        return Err(format!(
            "range revision {} predates seed revision {minimum_revision}",
            read.revision
        ));
    }
    if read.entries.len() != expected_entries {
        return Err(format!(
            "range returned {} entries, expected {expected_entries}",
            read.entries.len()
        ));
    }
    for (index, entry) in read.entries.iter().enumerate() {
        let expected_key = format!("{prefix}{index:04}");
        if entry.key != expected_key {
            return Err(format!(
                "range entry {index} had key {}, expected {expected_key}",
                entry.key
            ));
        }
        if entry.value != expected_value {
            return Err(format!(
                "range entry {index} returned {} value bytes that did not match the fixture",
                entry.value.len()
            ));
        }
    }
    Ok(())
}

fn benchmark_put_txn(
    key_prefix: &str,
    phase: &str,
    iteration: usize,
    ops_per_transaction: usize,
    value: &[u8],
) -> KvTxn {
    let success = (0..ops_per_transaction)
        .map(|operation| KvOp::Put {
            key: format!("{key_prefix}{phase}/{iteration:04}/{operation:04}"),
            value: value.to_vec(),
            lease_id: None,
        })
        .collect();
    KvTxn {
        compares: Vec::new(),
        success,
        failure: Vec::new(),
    }
}

fn validate_benchmark_commit_response(
    response: &KvTxnResponse,
    expected_operations: usize,
) -> Result<(), String> {
    if !response.succeeded {
        return Err(format!(
            "transaction was not committed (revision={})",
            response.revision.0
        ));
    }
    if response.responses.len() != expected_operations {
        return Err(format!(
            "transaction returned {} operation responses, expected {expected_operations}",
            response.responses.len()
        ));
    }
    if response
        .responses
        .iter()
        .any(|response| !matches!(response, sinomo_runtime::kv::KvOpResponse::Put { .. }))
    {
        return Err("transaction returned a non-put operation response".to_string());
    }
    Ok(())
}

fn commit_daemon_cluster_kv_txn(
    client: &DaemonApiClient,
    txn: KvTxn,
) -> Result<KvTxnResponse, String> {
    let request = KvTxnRequest::generated(txn);
    let payload = sinomo_runtime::kv::cluster_kv_txn_rpc_payload(&request)
        .map_err(|source| format!("cannot encode cluster-KV transaction: {source}"))?;
    let request =
        network::ObjectRpcRequest::from_encoded_body(payload.route, payload.category, payload.body);
    let response = client
        .call_request(&request)
        .map_err(|source| format!("daemon-mediated cluster-KV transaction failed: {source}"))?;
    let commit = sinomo_runtime::kv::decode_cluster_kv_txn_rpc_body(&response.body)
        .map_err(|source| format!("cannot decode cluster-KV transaction response: {source}"))?;
    commit.txn_response.ok_or_else(|| {
        "daemon returned a barrier result for a cluster-KV transaction request".to_string()
    })
}

fn cleanup_benchmark_prefix(
    client: &DaemonApiClient,
    key_prefix: &str,
) -> Result<KvTxnResponse, String> {
    commit_daemon_cluster_kv_txn(
        client,
        KvTxn {
            compares: Vec::new(),
            success: vec![KvOp::DeletePrefix {
                prefix: key_prefix.to_string(),
            }],
            failure: Vec::new(),
        },
    )
}

fn record_cleanup(ctx: &mut SinomoTestContext, cleanup: &Result<KvTxnResponse, String>) {
    match cleanup {
        Ok(response) => {
            ctx.record("cleanup_succeeded", response.succeeded);
            ctx.record("cleanup_revision", response.revision.0);
        }
        Err(detail) => {
            ctx.record("cleanup_succeeded", false);
            ctx.record("cleanup_error", detail);
        }
    }
}

fn cleanup_detail(cleanup: &Result<KvTxnResponse, String>) -> String {
    match cleanup {
        Ok(response) if response.succeeded => "cleanup=ok".to_string(),
        Ok(response) => format!("cleanup=not-committed@{}", response.revision.0),
        Err(detail) => format!("cleanup=warning:{detail}"),
    }
}

fn record_metrics(
    ctx: &mut SinomoTestContext,
    metrics: &ClusterKvBenchmarkMetrics,
    revisions: &[u64],
    failures: &[String],
) {
    ctx.record("success_count", metrics.success_count);
    ctx.record("failure_count", metrics.failure_count);
    ctx.record("elapsed_us", metrics.elapsed_us);
    ctx.record("elapsed_ms", micros_to_millis(metrics.elapsed_us));
    ctx.record("commits_per_second", metrics.commits_per_second);
    ctx.record("operations_per_second", metrics.operations_per_second);
    ctx.record("value_bytes_per_second", metrics.value_bytes_per_second);
    ctx.record("latency_min_us", metrics.latency_min_us);
    ctx.record("latency_p50_us", metrics.latency_p50_us);
    ctx.record("latency_p95_us", metrics.latency_p95_us);
    ctx.record("latency_max_us", metrics.latency_max_us);
    ctx.record("latency_min_ms", micros_to_millis(metrics.latency_min_us));
    ctx.record("latency_p50_ms", micros_to_millis(metrics.latency_p50_us));
    ctx.record("latency_p95_ms", micros_to_millis(metrics.latency_p95_us));
    ctx.record("latency_max_ms", micros_to_millis(metrics.latency_max_us));
    ctx.record("revision_min", metrics.revision_min);
    ctx.record("revision_max", metrics.revision_max);
    ctx.record(
        "revisions_strictly_increasing",
        metrics.revisions_strictly_increasing,
    );
    ctx.record("revisions", revisions);
    if !failures.is_empty() {
        ctx.record(
            "failure_samples",
            failures.iter().take(12).cloned().collect::<Vec<_>>(),
        );
    }
}

fn record_range_metrics(
    ctx: &mut SinomoTestContext,
    metrics: &ClusterKvRangeBenchmarkMetrics,
    revisions: &[u64],
    failures: &[String],
) {
    ctx.record("success_count", metrics.success_count);
    ctx.record("failure_count", metrics.failure_count);
    ctx.record("elapsed_us", metrics.elapsed_us);
    ctx.record("elapsed_ms", micros_to_millis(metrics.elapsed_us));
    ctx.record("reads_per_second", metrics.reads_per_second);
    ctx.record("entries_per_second", metrics.entries_per_second);
    ctx.record("value_bytes_per_second", metrics.value_bytes_per_second);
    ctx.record("latency_min_us", metrics.latency_min_us);
    ctx.record("latency_p50_us", metrics.latency_p50_us);
    ctx.record("latency_p95_us", metrics.latency_p95_us);
    ctx.record("latency_max_us", metrics.latency_max_us);
    ctx.record("latency_min_ms", micros_to_millis(metrics.latency_min_us));
    ctx.record("latency_p50_ms", micros_to_millis(metrics.latency_p50_us));
    ctx.record("latency_p95_ms", micros_to_millis(metrics.latency_p95_us));
    ctx.record("latency_max_ms", micros_to_millis(metrics.latency_max_us));
    ctx.record("revision_min", metrics.revision_min);
    ctx.record("revision_max", metrics.revision_max);
    ctx.record("revisions_non_decreasing", metrics.revisions_non_decreasing);
    ctx.record("revisions", revisions);
    if !failures.is_empty() {
        ctx.record(
            "failure_samples",
            failures.iter().take(12).cloned().collect::<Vec<_>>(),
        );
    }
}

fn benchmark_value(bytes: usize) -> Vec<u8> {
    (0..bytes).map(|index| b'a' + (index % 26) as u8).collect()
}

fn record_benchmark_profile(ctx: &mut SinomoTestContext) {
    if let Some(profile) = ctx.parameter("profile").map(str::to_string) {
        ctx.record("profile", profile);
    }
}

fn usize_parameter(
    ctx: &SinomoTestContext,
    name: &str,
    default: usize,
) -> Result<usize, SinomoTestError> {
    let Some(value) = ctx.parameter(name) else {
        return Ok(default);
    };
    value
        .parse::<usize>()
        .map_err(|source| invalid_parameter(name, source.to_string()))
}

fn u64_parameter(
    ctx: &SinomoTestContext,
    name: &str,
    default: u64,
) -> Result<u64, SinomoTestError> {
    let Some(value) = ctx.parameter(name) else {
        return Ok(default);
    };
    value
        .parse::<u64>()
        .map_err(|source| invalid_parameter(name, source.to_string()))
}

fn f64_parameter(
    ctx: &SinomoTestContext,
    name: &str,
    default: f64,
) -> Result<f64, SinomoTestError> {
    let Some(value) = ctx.parameter(name) else {
        return Ok(default);
    };
    value
        .parse::<f64>()
        .map_err(|source| invalid_parameter(name, source.to_string()))
}

fn invalid_parameter(name: &str, detail: impl Into<String>) -> SinomoTestError {
    SinomoTestError::InvalidParameter {
        name: name.to_string(),
        detail: detail.into(),
    }
}

fn percentile_value(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn duration_micros_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn micros_to_millis(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

fn rate_per_second(count: usize, elapsed_seconds: f64) -> f64 {
    if elapsed_seconds > 0.0 {
        count as f64 / elapsed_seconds
    } else {
        0.0
    }
}

fn threshold_exceeded(observed_us: u64, max_ms: u64) -> bool {
    max_ms > 0 && observed_us > max_ms.saturating_mul(1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_options_accept_useful_overrides() {
        let ctx = SinomoTestContext::new("unused")
            .with_parameter("iterations", "20")
            .with_parameter("warmup-iterations", "3")
            .with_parameter("ops-per-transaction", "8")
            .with_parameter("value-bytes", "4096")
            .with_parameter("timeout-ms", "5000")
            .with_parameter("max-failures", "2")
            .with_parameter("max-p50-ms", "400")
            .with_parameter("max-p95-ms", "900")
            .with_parameter("min-commits-per-second", "2.5");

        let options = ClusterKvBenchmarkOptions::from_context(&ctx).expect("options");

        assert_eq!(options.iterations, 20);
        assert_eq!(options.warmup_iterations, 3);
        assert_eq!(options.ops_per_transaction, 8);
        assert_eq!(options.value_bytes, 4096);
        assert_eq!(options.timeout_ms, 5_000);
        assert_eq!(options.max_failures, 2);
        assert_eq!(options.max_p50_ms, 400);
        assert_eq!(options.max_p95_ms, 900);
        assert_eq!(options.min_commits_per_second, 2.5);
    }

    #[test]
    fn benchmark_options_enforce_resource_bounds() {
        for (name, value) in [
            ("iterations", "0"),
            ("iterations", "129"),
            ("warmup-iterations", "65"),
            ("ops-per-transaction", "0"),
            ("ops-per-transaction", "257"),
            ("value-bytes", "16385"),
            ("timeout-ms", "0"),
            ("min-commits-per-second", "NaN"),
        ] {
            let ctx = SinomoTestContext::new("unused").with_parameter(name, value);
            assert!(
                ClusterKvBenchmarkOptions::from_context(&ctx).is_err(),
                "{name}={value} should be rejected"
            );
        }

        let failures = SinomoTestContext::new("unused")
            .with_parameter("iterations", "2")
            .with_parameter("max-failures", "3");
        assert!(ClusterKvBenchmarkOptions::from_context(&failures).is_err());

        let thresholds = SinomoTestContext::new("unused")
            .with_parameter("max-p50-ms", "901")
            .with_parameter("max-p95-ms", "900");
        assert!(ClusterKvBenchmarkOptions::from_context(&thresholds).is_err());
    }

    #[test]
    fn metrics_capture_distribution_throughput_failures_and_revisions() {
        let metrics = ClusterKvBenchmarkMetrics::from_samples(
            &[4_000, 1_000, 3_000, 2_000],
            &[10, 12, 13, 20],
            1,
            Duration::from_secs(2),
            100,
            4,
        );

        assert_eq!(metrics.success_count, 4);
        assert_eq!(metrics.failure_count, 1);
        assert_eq!(metrics.commits_per_second, 2.0);
        assert_eq!(metrics.operations_per_second, 8.0);
        assert_eq!(metrics.value_bytes_per_second, 800.0);
        assert_eq!(metrics.latency_min_us, 1_000);
        assert_eq!(metrics.latency_p50_us, 2_000);
        assert_eq!(metrics.latency_p95_us, 4_000);
        assert_eq!(metrics.latency_max_us, 4_000);
        assert_eq!(metrics.revision_min, Some(10));
        assert_eq!(metrics.revision_max, Some(20));
        assert!(metrics.revisions_strictly_increasing);

        let non_monotonic = ClusterKvBenchmarkMetrics::from_samples(
            &[1, 2],
            &[8, 8],
            0,
            Duration::from_secs(1),
            1,
            1,
        );
        assert!(!non_monotonic.revisions_strictly_increasing);
    }

    #[test]
    fn range_benchmark_options_distinguish_local_and_authoritative_reads() {
        let local = ClusterKvRangeBenchmarkOptions::from_context(&SinomoTestContext::new("unused"))
            .expect("local options");
        assert_eq!(local.consistency, ClusterKvReadConsistency::Local);

        let authoritative = ClusterKvRangeBenchmarkOptions::from_context(
            &SinomoTestContext::new("unused")
                .with_parameter("consistency", "authoritative")
                .with_parameter("entries", "128")
                .with_parameter("iterations", "40")
                .with_parameter("warmup-iterations", "5")
                .with_parameter("value-bytes", "1024"),
        )
        .expect("authoritative options");
        assert_eq!(
            authoritative.consistency,
            ClusterKvReadConsistency::Authoritative
        );
        assert_eq!(authoritative.entries, 128);
        assert_eq!(authoritative.iterations, 40);
        assert_eq!(authoritative.warmup_iterations, 5);
        assert_eq!(authoritative.value_bytes, 1024);

        assert!(
            ClusterKvRangeBenchmarkOptions::from_context(
                &SinomoTestContext::new("unused").with_parameter("consistency", "eventual")
            )
            .is_err()
        );
    }

    #[test]
    fn range_metrics_capture_logical_throughput_and_monotonic_revisions() {
        let metrics = ClusterKvRangeBenchmarkMetrics::from_samples(
            &[3_000, 1_000, 2_000],
            &[10, 10, 12],
            1,
            Duration::from_secs(2),
            4,
            100,
        );

        assert_eq!(metrics.success_count, 3);
        assert_eq!(metrics.failure_count, 1);
        assert_eq!(metrics.reads_per_second, 1.5);
        assert_eq!(metrics.entries_per_second, 6.0);
        assert_eq!(metrics.value_bytes_per_second, 600.0);
        assert_eq!(metrics.latency_p50_us, 2_000);
        assert!(metrics.revisions_non_decreasing);

        let reversed = ClusterKvRangeBenchmarkMetrics::from_samples(
            &[1, 2],
            &[12, 11],
            0,
            Duration::from_secs(1),
            1,
            1,
        );
        assert!(!reversed.revisions_non_decreasing);
    }

    #[test]
    fn benchmark_payload_has_exact_requested_size() {
        assert_eq!(benchmark_value(0), Vec::<u8>::new());
        assert_eq!(benchmark_value(27).len(), 27);
        assert_eq!(&benchmark_value(27)[..3], b"abc");
    }
}
