//! Cluster network runtime test cases.
//!
//! These probes exercise the signed daemon object-RPC path without touching DFS.
//! The Docker cluster gate uses them before DFS scenarios so routing, relay,
//! authentication, and concurrent request handling must be healthy first.

use sinomo_runtime::core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord,
};
use sinomo_runtime::daemon::sign_daemon_rpc_request_for_local_node;
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
    let cluster_state = Arc::new(
        network::read_cluster_state(&cluster_paths)
            .map_err(|error| SinomoTestError::failed(error.to_string()))?,
    );
    let targets = network_ping_targets_from_state(&cluster_paths, &cluster_state)?;
    let target_count = targets.len();
    ctx.record("target_count", target_count);
    ctx.record(
        "targets",
        targets
            .iter()
            .map(|target| {
                format!(
                    "{} {} {:?}",
                    target.node_id, target.address, target.transport
                )
            })
            .collect::<Vec<_>>(),
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
    let runtime_handle = runtime.handle().clone();
    let build_jobs = |phase: &str,
                      requests_per_target: usize|
     -> Result<Vec<NetworkPingJob>, SinomoTestError> {
        let mut jobs = Vec::with_capacity(target_count.saturating_mul(requests_per_target));
        for endpoint in &targets {
            for attempt in 0..requests_per_target {
                let nonce = format!("ping-{phase}-{nonce_prefix}-{}-{attempt}", endpoint.node_id);
                let request = network::cluster_network_ping_rpc_request_with_response_payload(
                    &nonce,
                    payload_bytes,
                    response_payload_bytes,
                )
                .map_err(|source| {
                    SinomoTestError::failed(format!(
                        "cannot prepare network ping request for {}: {source}",
                        endpoint.node_id
                    ))
                })?;
                jobs.push(NetworkPingJob {
                    target_node_id: endpoint.node_id.clone(),
                    endpoint: endpoint.clone(),
                    timeout: Duration::from_millis(timeout_ms),
                    request_payload_bytes: payload_bytes,
                    response_payload_bytes,
                    nonce,
                    request,
                    runtime: runtime_handle.clone(),
                    paths: cluster_paths.clone(),
                    cluster_state: Arc::clone(&cluster_state),
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
            NetworkPingOutcome::Failure(detail) => Some(format!(
                "{} via {}: {}",
                result.target_node_id, result.endpoint_address, detail
            )),
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
                failures.push(format!(
                    "{} via {}: {}",
                    result.target_node_id, result.endpoint_address, detail
                ));
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
            "network ping p50 latency {p50_ms}ms is above allowed {max_p50_ms}ms"
        )));
    }
    if max_p95_ms > 0 && p95_ms > max_p95_ms {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping p95 latency {p95_ms}ms is above allowed {max_p95_ms}ms"
        )));
    }
    if max_p50_us > 0 && p50_us > max_p50_us {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping p50 latency {p50_us}us is above allowed {max_p50_us}us"
        )));
    }
    if max_p95_us > 0 && p95_us > max_p95_us {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping p95 latency {p95_us}us is above allowed {max_p95_us}us"
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
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, SinomoTestError> {
    let mut targets = BTreeMap::<NodeId, ClusterEndpointRecord>::new();
    for member in &state.members {
        if member.node_id == state.local_node_id
            || !member.state.is_authorized()
            || !member_has_main_daemon(member)
        {
            continue;
        }
        let Some(endpoint) = network::ClusterEndpointSelector::new(&paths, &state)
            .select_member(
                member,
                network::ClusterEndpointSelectionPurpose::MemberDaemonStatus,
            )
            .into_iter()
            .find(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
        else {
            continue;
        };
        targets.insert(member.node_id.clone(), endpoint);
    }
    Ok(targets.into_values().collect())
}

fn member_has_main_daemon(member: &ClusterMemberRecord) -> bool {
    member
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == network::DAEMON_MAIN_RUN_CAPABILITY)
}

#[derive(Clone, Debug)]
struct NetworkPingJob {
    target_node_id: NodeId,
    endpoint: ClusterEndpointRecord,
    nonce: String,
    timeout: Duration,
    request_payload_bytes: usize,
    response_payload_bytes: usize,
    request: network::ObjectRpcRequest,
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
    cluster_state: Arc<ClusterBootstrapState>,
}

#[derive(Debug)]
struct NetworkPingResult {
    target_node_id: NodeId,
    endpoint_address: String,
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
        endpoint_address: job.endpoint.address,
        latency_ms: elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        latency_us: elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
        outcome,
    }
}

async fn run_network_ping_job_inner(job: &NetworkPingJob) -> NetworkPingOutcome {
    let mut host = NetworkPingEndpointRpcHost {
        cluster_state: Arc::clone(&job.cluster_state),
    };
    let mut client = network::ClusterObjectRpcClient::routed(
        &mut host,
        network::ClusterObjectRpcRouteContext::new(&job.runtime, &job.paths),
    );
    let response = match client
        .call_endpoint(
            &job.endpoint,
            &job.request,
            network::ClusterObjectRpcOptions::new(network::DaemonRpcTimeouts::connect_and_idle(
                job.timeout,
            )),
        )
        .await
    {
        Ok(response) => response,
        Err(error) => return NetworkPingOutcome::Failure(error.to_string()),
    };
    let response = match network::decode_cluster_network_ping_rpc_response(&response) {
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

struct NetworkPingEndpointRpcHost {
    cluster_state: Arc<ClusterBootstrapState>,
}

impl network::ClusterSignedEndpointRpcAsyncHost for NetworkPingEndpointRpcHost {
    fn sign_endpoint_rpc_request_async<'a>(
        &'a mut self,
        request: &'a network::ObjectRpcRequest,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Vec<u8>> {
        let request = request.clone();
        Box::pin(async move {
            sign_daemon_rpc_request_for_local_node(&request)
                .map_err(|error| network_ping_network_error("cluster endpoint signing", error))
        })
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Option<network::ClusterId>> {
        let cluster_id = self.cluster_state.cluster_id.clone();
        Box::pin(async move { Ok(Some(cluster_id)) })
    }
}

fn network_ping_network_error(operation: &str, error: impl ToString) -> network::NetworkError {
    network::NetworkError::Io {
        operation: operation.to_string(),
        detail: error.to_string(),
    }
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
