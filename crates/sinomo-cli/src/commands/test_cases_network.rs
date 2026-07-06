//! Cluster network runtime test cases.
//!
//! These probes exercise the signed daemon object-RPC path without touching DFS.
//! The Docker cluster gate uses them before DFS scenarios so routing, relay,
//! authentication, and concurrent request handling must be healthy first.

use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::cluster;
use sinomo_runtime::core::{ClusterBootstrapState, ClusterMemberRecord};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_REQUESTS_PER_TARGET: usize = 20;
const DEFAULT_CONCURRENCY: usize = 32;
const DEFAULT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_EXPECTED_TARGETS: usize = 5;
const DEFAULT_MAX_FAILURES: usize = 0;
const DEFAULT_MAX_P50_MS: u64 = 50;
const DEFAULT_MAX_P95_MS: u64 = 250;
const DEFAULT_MAX_P50_US: u64 = 0;
const DEFAULT_MAX_P95_US: u64 = 0;
const DEFAULT_MIN_REQUESTS_PER_SECOND: f64 = 50.0;
const DEFAULT_MIN_PAYLOAD_BYTES_PER_SECOND: f64 = 0.0;

pub fn register_network_cluster_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder.register(network_ping_benchmark())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_ping_benchmark(ctx) {
        id: "cluster.network.ping-benchmark",
        summary: "Concurrent signed cluster network ping RPCs to every remote main daemon",
        tags: ["cluster-network", "probe", "performance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_ping_benchmark(ctx)
        }
    }
}

fn run_network_ping_benchmark(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let requests_per_target =
        optional_usize_parameter(ctx, "requests-per-target", DEFAULT_REQUESTS_PER_TARGET)?;
    let warmup_requests_per_target =
        optional_usize_parameter(ctx, "warmup-requests-per-target", requests_per_target)?;
    let concurrency = optional_usize_parameter(ctx, "concurrency", DEFAULT_CONCURRENCY)?;
    let timeout_ms = optional_u64_parameter(ctx, "timeout-ms", DEFAULT_TIMEOUT_MS)?;
    let expected_targets =
        optional_usize_parameter(ctx, "expected-targets", DEFAULT_EXPECTED_TARGETS)?;
    let max_failures = optional_usize_parameter(ctx, "max-failures", DEFAULT_MAX_FAILURES)?;
    let max_p50_ms = optional_u64_parameter(ctx, "max-p50-ms", DEFAULT_MAX_P50_MS)?;
    let max_p95_ms = optional_u64_parameter(ctx, "max-p95-ms", DEFAULT_MAX_P95_MS)?;
    let max_p50_us = optional_u64_parameter(ctx, "max-p50-us", DEFAULT_MAX_P50_US)?;
    let max_p95_us = optional_u64_parameter(ctx, "max-p95-us", DEFAULT_MAX_P95_US)?;
    let min_requests_per_second = optional_f64_parameter(
        ctx,
        "min-requests-per-second",
        DEFAULT_MIN_REQUESTS_PER_SECOND,
    )?;
    let payload_bytes = optional_usize_parameter(ctx, "payload-bytes", 0)?;
    let response_payload_bytes = optional_usize_parameter(ctx, "response-payload-bytes", 0)?;
    let min_payload_bytes_per_second = optional_f64_parameter(
        ctx,
        "min-payload-bytes-per-second",
        DEFAULT_MIN_PAYLOAD_BYTES_PER_SECOND,
    )?;
    require_positive("requests-per-target", requests_per_target)?;
    require_positive("concurrency", concurrency)?;
    require_positive("timeout-ms", timeout_ms as usize)?;

    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before network ping: {error}"
        ))
    })?;
    let cluster_state = Arc::new(
        cluster::read_cluster_state(&cluster_paths)
            .map_err(|error| SinomoTestError::failed(error.to_string()))?,
    );
    let targets = network_ping_targets_from_state(&cluster_state)?;
    let target_count = targets.len();
    ctx.record("target_count", target_count);
    ctx.record(
        "targets",
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
    );
    if target_count < expected_targets {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping saw {target_count} remote main targets, expected at least {expected_targets}"
        )));
    }

    let benchmark_started = Instant::now();
    let prepare_started = Instant::now();
    let nonce_prefix = format!("{}-{}", current_millis(), std::process::id());
    let total_requests = target_count.saturating_mul(requests_per_target);
    let warmup_total_requests = target_count.saturating_mul(warmup_requests_per_target);
    let concurrency_limit = concurrency
        .min(total_requests.max(warmup_total_requests))
        .max(1);
    let runtime_worker_threads = network_ping_runtime_worker_threads(concurrency_limit);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_worker_threads)
        .enable_all()
        .build()
        .map_err(|source| {
            SinomoTestError::failed(format!("cannot create network ping runtime: {source}"))
        })?;
    let daemon_client = DaemonApiClient::connect()
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot connect to local daemon for network ping forwarding: {source}"
            ))
        })?
        .with_timeout(Duration::from_millis(timeout_ms).saturating_add(Duration::from_secs(1)));
    let build_jobs =
        |phase: &str, requests_per_target: usize| -> Result<Vec<NetworkPingJob>, SinomoTestError> {
            let mut jobs = Vec::with_capacity(target_count.saturating_mul(requests_per_target));
            for attempt in 0..requests_per_target {
                for target_node_id in &targets {
                    let nonce = format!("ping-{phase}-{nonce_prefix}-{target_node_id}-{attempt}");
                    let request = cluster::cluster_network_ping_rpc_request_with_response_payload(
                        &nonce,
                        payload_bytes,
                        response_payload_bytes,
                    )
                    .map_err(|source| {
                        SinomoTestError::failed(format!(
                            "cannot prepare network ping request for {}: {source}",
                            target_node_id
                        ))
                    })?;
                    jobs.push(NetworkPingJob {
                        target_node_id: target_node_id.clone(),
                        timeout: Duration::from_millis(timeout_ms),
                        request_payload_bytes: payload_bytes,
                        response_payload_bytes,
                        nonce,
                        request,
                        daemon_client: daemon_client.clone(),
                    });
                }
            }
            Ok(jobs)
        };
    let warmup_jobs = build_jobs("warmup", warmup_requests_per_target)?;
    let jobs = build_jobs("measure", requests_per_target)?;
    let prepare_elapsed = prepare_started.elapsed();
    let total_requests = jobs.len();
    ctx.record("requests_per_target", requests_per_target);
    ctx.record("warmup_requests_per_target", warmup_requests_per_target);
    ctx.record("warmup_total_requests", warmup_jobs.len());
    ctx.record("total_requests", total_requests);
    ctx.record("concurrency", concurrency_limit);
    ctx.record("runtime_worker_threads", runtime_worker_threads);
    ctx.record("timeout_ms", timeout_ms);
    ctx.record("request_payload_bytes", payload_bytes);
    ctx.record("response_payload_bytes", response_payload_bytes);
    ctx.record("max_failures", max_failures);
    ctx.record("max_p50_ms", max_p50_ms);
    ctx.record("max_p95_ms", max_p95_ms);
    ctx.record("max_p50_us", max_p50_us);
    ctx.record("max_p95_us", max_p95_us);
    ctx.record("min_requests_per_second", min_requests_per_second);
    ctx.record("min_payload_bytes_per_second", min_payload_bytes_per_second);
    ctx.record(
        "request_prepare_elapsed_ms",
        prepare_elapsed.as_millis() as u64,
    );
    ctx.record(
        "request_prepare_requests_per_second",
        rate_per_second(total_requests as f64, prepare_elapsed),
    );

    let warmup_started = Instant::now();
    let warmup_results = if warmup_jobs.is_empty() {
        Vec::new()
    } else {
        runtime.block_on(run_network_ping_jobs_async(warmup_jobs, concurrency_limit))?
    };
    let warmup_elapsed = warmup_started.elapsed();
    let warmup_success_count = warmup_results
        .iter()
        .filter(|result| matches!(result.outcome, NetworkPingOutcome::Success))
        .count();
    let warmup_failures = warmup_results
        .iter()
        .filter_map(|result| match &result.outcome {
            NetworkPingOutcome::Success => None,
            NetworkPingOutcome::Failure(detail) => {
                Some(format!("{}: {}", result.target_node_id, detail))
            }
        })
        .collect::<Vec<_>>();
    ctx.record("warmup_success_count", warmup_success_count);
    ctx.record("warmup_failure_count", warmup_failures.len());
    ctx.record("warmup_elapsed_ms", warmup_elapsed.as_millis() as u64);
    if !warmup_failures.is_empty() {
        ctx.record(
            "warmup_failure_samples",
            warmup_failures.iter().take(12).cloned().collect::<Vec<_>>(),
        );
    }
    if warmup_failures.len() > max_failures {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping warmup had {}/{} failed RPCs, above allowed {max_failures}",
            warmup_failures.len(),
            warmup_results.len()
        )));
    }

    let started = Instant::now();
    let mut results = runtime.block_on(run_network_ping_jobs_async(jobs, concurrency_limit))?;
    let elapsed = started.elapsed();
    let total_elapsed = benchmark_started.elapsed();
    results.sort_by_key(|result| (result.target_node_id.clone(), result.latency_ms));

    let mut latencies_ms = Vec::new();
    let mut latencies_us = Vec::new();
    let mut failures = Vec::new();
    let mut successes_by_target = BTreeMap::<String, usize>::new();
    let mut failures_by_target = BTreeMap::<String, usize>::new();
    let mut latencies_by_target = BTreeMap::<String, Vec<u64>>::new();
    let mut latencies_by_target_us = BTreeMap::<String, Vec<u64>>::new();
    for result in &results {
        match &result.outcome {
            NetworkPingOutcome::Success => {
                latencies_ms.push(result.latency_ms);
                latencies_us.push(result.latency_us);
                latencies_by_target
                    .entry(result.target_node_id.to_string())
                    .or_default()
                    .push(result.latency_ms);
                latencies_by_target_us
                    .entry(result.target_node_id.to_string())
                    .or_default()
                    .push(result.latency_us);
                *successes_by_target
                    .entry(result.target_node_id.to_string())
                    .or_default() += 1;
            }
            NetworkPingOutcome::Failure(detail) => {
                failures.push(format!("{}: {}", result.target_node_id, detail));
                *failures_by_target
                    .entry(result.target_node_id.to_string())
                    .or_default() += 1;
            }
        }
    }
    latencies_ms.sort_unstable();
    latencies_us.sort_unstable();
    let success_count = latencies_ms.len();
    let failure_count = failures.len();
    let p50_ms = percentile_value(&latencies_ms, 50);
    let p95_ms = percentile_value(&latencies_ms, 95);
    let max_ms = latencies_ms.iter().copied().max().unwrap_or(0);
    let min_ms = latencies_ms.iter().copied().min().unwrap_or(0);
    let p50_us = percentile_value(&latencies_us, 50);
    let p95_us = percentile_value(&latencies_us, 95);
    let max_us = latencies_us.iter().copied().max().unwrap_or(0);
    let min_us = latencies_us.iter().copied().min().unwrap_or(0);
    let slowest_targets_ms = network_ping_slowest_target_summaries(&latencies_by_target, "ms");
    let slowest_targets_us = network_ping_slowest_target_summaries(&latencies_by_target_us, "us");
    let requests_per_second = rate_per_second(success_count as f64, elapsed);
    let payload_bytes_per_success = payload_bytes.saturating_add(response_payload_bytes);
    let total_success_payload_bytes = payload_bytes_per_success.saturating_mul(success_count);
    let payload_bytes_per_second = rate_per_second(total_success_payload_bytes as f64, elapsed);
    let latency_by_target_ms = latencies_by_target
        .into_iter()
        .map(|(target, mut target_latencies)| {
            target_latencies.sort_unstable();
            let min = target_latencies.iter().copied().min().unwrap_or(0);
            let p50 = percentile_value(&target_latencies, 50);
            let p95 = percentile_value(&target_latencies, 95);
            let max = target_latencies.iter().copied().max().unwrap_or(0);
            (
                target,
                format!(
                    "count={} min={min} p50={p50} p95={p95} max={max}",
                    target_latencies.len()
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let latency_by_target_us = latencies_by_target_us
        .into_iter()
        .map(|(target, mut target_latencies)| {
            target_latencies.sort_unstable();
            let min = target_latencies.iter().copied().min().unwrap_or(0);
            let p50 = percentile_value(&target_latencies, 50);
            let p95 = percentile_value(&target_latencies, 95);
            let max = target_latencies.iter().copied().max().unwrap_or(0);
            (
                target,
                format!(
                    "count={} min={min} p50={p50} p95={p95} max={max}",
                    target_latencies.len()
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    ctx.record("success_count", success_count);
    ctx.record("failure_count", failure_count);
    ctx.record("elapsed_ms", elapsed.as_millis() as u64);
    ctx.record(
        "total_elapsed_including_prepare_ms",
        total_elapsed.as_millis() as u64,
    );
    ctx.record("requests_per_second", requests_per_second);
    ctx.record("success_payload_bytes", total_success_payload_bytes as u64);
    ctx.record("payload_bytes_per_second", payload_bytes_per_second);
    ctx.record("latency_min_ms", min_ms);
    ctx.record("latency_p50_ms", p50_ms);
    ctx.record("latency_p95_ms", p95_ms);
    ctx.record("latency_max_ms", max_ms);
    ctx.record("latency_min_us", min_us);
    ctx.record("latency_p50_us", p50_us);
    ctx.record("latency_p95_us", p95_us);
    ctx.record("latency_max_us", max_us);
    ctx.record("successes_by_target", successes_by_target);
    ctx.record("failures_by_target", failures_by_target);
    ctx.record("latency_by_target_ms", latency_by_target_ms);
    ctx.record("latency_by_target_us", latency_by_target_us);
    if !failures.is_empty() {
        ctx.record(
            "failure_samples",
            failures.iter().take(12).cloned().collect::<Vec<_>>(),
        );
    }

    if failure_count > max_failures {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping had {failure_count}/{total_requests} failed RPCs, above allowed {max_failures}"
        )));
    }
    if success_count == 0 {
        return Ok(SinomoTestOutcome::failed(
            "network ping had no successful RPCs".to_string(),
        ));
    }
    if max_p50_ms > 0 && p50_ms > max_p50_ms {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping p50 latency {p50_ms}ms is above allowed {max_p50_ms}ms{}",
            slowest_target_detail(&slowest_targets_ms)
        )));
    }
    if max_p95_ms > 0 && p95_ms > max_p95_ms {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping p95 latency {p95_ms}ms is above allowed {max_p95_ms}ms{}",
            slowest_target_detail(&slowest_targets_ms)
        )));
    }
    if max_p50_us > 0 && p50_us > max_p50_us {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping p50 latency {p50_us}us is above allowed {max_p50_us}us{}",
            slowest_target_detail(&slowest_targets_us)
        )));
    }
    if max_p95_us > 0 && p95_us > max_p95_us {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping p95 latency {p95_us}us is above allowed {max_p95_us}us{}",
            slowest_target_detail(&slowest_targets_us)
        )));
    }
    if min_requests_per_second > 0.0 && requests_per_second < min_requests_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping throughput {requests_per_second:.2} req/s is below required {min_requests_per_second:.2} req/s"
        )));
    }
    if min_payload_bytes_per_second > 0.0 && payload_bytes_per_second < min_payload_bytes_per_second
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping payload throughput {payload_bytes_per_second:.2} bytes/s is below required {min_payload_bytes_per_second:.2} bytes/s"
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "network ping completed {success_count}/{total_requests} RPCs in {}ms ({requests_per_second:.2} req/s, {payload_bytes_per_second:.2} payload bytes/s, p50={p50_ms}ms/{p50_us}us p95={p95_ms}ms/{p95_us}us)",
        elapsed.as_millis()
    )))
}

fn network_ping_targets_from_state(
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, SinomoTestError> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id
            || !member.state.is_authorized()
            || !member_has_main_daemon(member)
        {
            continue;
        }
        targets.push(member.node_id.clone());
    }
    targets.sort();
    Ok(targets)
}

fn member_has_main_daemon(member: &ClusterMemberRecord) -> bool {
    member
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY)
}

#[derive(Clone, Debug)]
struct NetworkPingJob {
    target_node_id: NodeId,
    nonce: String,
    timeout: Duration,
    request_payload_bytes: usize,
    response_payload_bytes: usize,
    request: network::ObjectRpcRequest,
    daemon_client: DaemonApiClient,
}

#[derive(Debug)]
struct NetworkPingResult {
    target_node_id: NodeId,
    latency_ms: u64,
    latency_us: u64,
    outcome: NetworkPingOutcome,
}

#[derive(Debug)]
enum NetworkPingOutcome {
    Success,
    Failure(String),
}

async fn run_network_ping_job(job: NetworkPingJob) -> NetworkPingResult {
    let started = Instant::now();
    let outcome = run_network_ping_job_inner(&job).await;
    let elapsed = started.elapsed();
    NetworkPingResult {
        target_node_id: job.target_node_id,
        latency_ms: elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        latency_us: elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
        outcome,
    }
}

async fn run_network_ping_job_inner(job: &NetworkPingJob) -> NetworkPingOutcome {
    let daemon_client = job.daemon_client.clone();
    let target_node_id = job.target_node_id.clone();
    let request = job.request.clone();
    let timeout = job.timeout;
    let response = match tokio::task::spawn_blocking(move || {
        daemon_client.dfs_object_rpc_to_target(&target_node_id, &request, timeout, true)
    })
    .await
    .map_err(|source| source.to_string())
    .and_then(|response| response.map_err(|source| source.to_string()))
    {
        Ok(response) => response,
        Err(error) => return NetworkPingOutcome::Failure(error),
    };
    let response = match cluster::decode_cluster_network_ping_rpc_response(&response) {
        Ok(response) => response,
        Err(error) => return NetworkPingOutcome::Failure(error.to_string()),
    };
    if response.nonce != job.nonce {
        return NetworkPingOutcome::Failure(format!(
            "nonce mismatch: got {}, expected {}",
            response.nonce, job.nonce
        ));
    }
    if response.responder_node_id != job.target_node_id {
        return NetworkPingOutcome::Failure(format!(
            "responder mismatch: got {}, expected {}",
            response.responder_node_id, job.target_node_id
        ));
    }
    if response.payload_bytes != job.request_payload_bytes {
        return NetworkPingOutcome::Failure(format!(
            "payload length mismatch: got {}, expected {}",
            response.payload_bytes, job.request_payload_bytes
        ));
    }
    if response.response_payload.len() != job.response_payload_bytes {
        return NetworkPingOutcome::Failure(format!(
            "response payload length mismatch: got {}, expected {}",
            response.response_payload.len(),
            job.response_payload_bytes
        ));
    }
    NetworkPingOutcome::Success
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

fn optional_u64_parameter(
    ctx: &SinomoTestContext,
    key: &str,
    default: u64,
) -> Result<u64, SinomoTestError> {
    match ctx.parameter(key) {
        Some(raw) => {
            raw.trim()
                .parse::<u64>()
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

fn require_positive(name: &str, value: usize) -> Result<(), SinomoTestError> {
    if value == 0 {
        return Err(SinomoTestError::InvalidParameter {
            name: name.to_string(),
            detail: "must be greater than zero".to_string(),
        });
    }
    Ok(())
}

async fn run_network_ping_jobs_async(
    jobs: Vec<NetworkPingJob>,
    concurrency: usize,
) -> Result<Vec<NetworkPingResult>, SinomoTestError> {
    let mut jobs = jobs.into_iter();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..concurrency {
        let Some(job) = jobs.next() else {
            break;
        };
        tasks.spawn(async move { run_network_ping_job(job).await });
    }

    let mut results = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let result = joined.map_err(|source| {
            SinomoTestError::failed(format!("network ping task failed: {source}"))
        })?;
        results.push(result);
        if let Some(job) = jobs.next() {
            tasks.spawn(async move { run_network_ping_job(job).await });
        }
    }
    Ok(results)
}

fn network_ping_runtime_worker_threads(concurrency: usize) -> usize {
    let available = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);
    available.clamp(2, concurrency.max(2).min(16))
}

fn percentile_value(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

fn network_ping_slowest_target_summaries(
    latencies_by_target: &BTreeMap<String, Vec<u64>>,
    unit: &str,
) -> Vec<String> {
    let mut summaries = latencies_by_target
        .iter()
        .filter_map(|(target, latencies)| {
            if latencies.is_empty() {
                return None;
            }
            let mut sorted = latencies.clone();
            sorted.sort_unstable();
            let p50 = percentile_value(&sorted, 50);
            let p95 = percentile_value(&sorted, 95);
            let max = sorted.iter().copied().max().unwrap_or(0);
            Some((
                p50,
                p95,
                max,
                format!("{target}: p50={p50}{unit} p95={p95}{unit} max={max}{unit}"),
            ))
        })
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| right.2.cmp(&left.2))
            .then_with(|| left.3.cmp(&right.3))
    });
    summaries
        .into_iter()
        .take(3)
        .map(|(_, _, _, summary)| summary)
        .collect()
}

fn slowest_target_detail(slowest_targets: &[String]) -> String {
    if slowest_targets.is_empty() {
        String::new()
    } else {
        format!("; slowest targets: {}", slowest_targets.join(", "))
    }
}

fn rate_per_second(count: f64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds <= f64::EPSILON {
        return count;
    }
    count / seconds
}

fn current_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
