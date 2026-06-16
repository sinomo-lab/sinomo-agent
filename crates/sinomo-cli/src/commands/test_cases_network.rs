//! Cluster network runtime test cases.
//!
//! These probes exercise the signed daemon object-RPC path without touching DFS.
//! The Docker cluster gate uses them before DFS scenarios so routing, relay,
//! authentication, and concurrent request handling must be healthy first.

use sinomo_runtime::core::{ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord};
use sinomo_runtime::daemon::{daemon_rpc_command, sign_daemon_rpc_request_for_local_node};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_REQUESTS_PER_TARGET: usize = 20;
const DEFAULT_CONCURRENCY: usize = 32;
const DEFAULT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_EXPECTED_TARGETS: usize = 5;
const DEFAULT_MAX_FAILURES: usize = 0;
const DEFAULT_MAX_P50_MS: u64 = 50;
const DEFAULT_MAX_P95_MS: u64 = 250;
const DEFAULT_MIN_REQUESTS_PER_SECOND: f64 = 50.0;

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
    let concurrency = optional_usize_parameter(ctx, "concurrency", DEFAULT_CONCURRENCY)?;
    let timeout_ms = optional_u64_parameter(ctx, "timeout-ms", DEFAULT_TIMEOUT_MS)?;
    let expected_targets =
        optional_usize_parameter(ctx, "expected-targets", DEFAULT_EXPECTED_TARGETS)?;
    let max_failures = optional_usize_parameter(ctx, "max-failures", DEFAULT_MAX_FAILURES)?;
    let max_p50_ms = optional_u64_parameter(ctx, "max-p50-ms", DEFAULT_MAX_P50_MS)?;
    let max_p95_ms = optional_u64_parameter(ctx, "max-p95-ms", DEFAULT_MAX_P95_MS)?;
    let min_requests_per_second = optional_f64_parameter(
        ctx,
        "min-requests-per-second",
        DEFAULT_MIN_REQUESTS_PER_SECOND,
    )?;
    let payload_bytes = optional_usize_parameter(ctx, "payload-bytes", 0)?;
    let require_direct = optional_bool_parameter(ctx, "require-direct", false)?;
    require_positive("requests-per-target", requests_per_target)?;
    require_positive("concurrency", concurrency)?;
    require_positive("timeout-ms", timeout_ms as usize)?;

    let targets = network_ping_targets(ctx)?;
    let target_count = targets.len();
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
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
    let (target_routes, route_by_target) =
        network_ping_target_route_descriptions(&cluster_paths, &targets);
    ctx.record("target_routes", target_routes.clone());
    ctx.record(
        "target_route_modes",
        route_by_target
            .iter()
            .map(|(node_id, route)| (node_id.to_string(), route.clone()))
            .collect::<BTreeMap<_, _>>(),
    );
    ctx.record(
        "target_route_counts",
        route_counts(route_by_target.values().map(String::as_str)),
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
    let concurrency_limit = concurrency.min(total_requests).max(1);
    let runtime_worker_threads = network_ping_runtime_worker_threads(concurrency_limit);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_worker_threads)
        .enable_all()
        .build()
        .map_err(|source| {
            SinomoTestError::failed(format!("cannot create network ping runtime: {source}"))
        })?;
    let runtime_handle = runtime.handle().clone();
    let mut jobs = Vec::with_capacity(total_requests);
    for endpoint in targets {
        let route_mode = route_by_target
            .get(&endpoint.node_id)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        for attempt in 0..requests_per_target {
            let nonce = format!("ping-{nonce_prefix}-{}-{attempt}", endpoint.node_id);
            let request = network::cluster_network_ping_rpc_request(&nonce, payload_bytes)
                .map_err(|source| {
                    SinomoTestError::failed(format!(
                        "cannot prepare network ping request for {}: {source}",
                        endpoint.node_id
                    ))
                })?;
            let request_auth =
                sign_daemon_rpc_request_for_local_node(&request).map_err(|source| {
                    SinomoTestError::failed(format!(
                        "cannot sign network ping request for {}: {source}",
                        endpoint.node_id
                    ))
                })?;
            let request_command = daemon_rpc_command(&request.route);
            jobs.push(NetworkPingJob {
                target_node_id: endpoint.node_id.clone(),
                endpoint: endpoint.clone(),
                timeout: Duration::from_millis(timeout_ms),
                payload_bytes,
                nonce,
                request,
                request_auth,
                request_command,
                route_mode: route_mode.clone(),
                runtime: runtime_handle.clone(),
                paths: cluster_paths.clone(),
            });
        }
    }
    let prepare_elapsed = prepare_started.elapsed();
    let total_requests = jobs.len();
    ctx.record("requests_per_target", requests_per_target);
    ctx.record("total_requests", total_requests);
    ctx.record("concurrency", concurrency_limit);
    ctx.record("runtime_worker_threads", runtime_worker_threads);
    ctx.record("timeout_ms", timeout_ms);
    ctx.record("payload_bytes", payload_bytes);
    ctx.record("max_failures", max_failures);
    ctx.record("max_p50_ms", max_p50_ms);
    ctx.record("max_p95_ms", max_p95_ms);
    ctx.record("min_requests_per_second", min_requests_per_second);
    ctx.record("require_direct", require_direct);
    ctx.record(
        "request_prepare_elapsed_ms",
        prepare_elapsed.as_millis() as u64,
    );
    ctx.record(
        "request_prepare_requests_per_second",
        rate_per_second(total_requests as f64, prepare_elapsed),
    );

    let started = Instant::now();
    let mut results = runtime.block_on(run_network_ping_jobs_async(jobs, concurrency_limit))?;
    let elapsed = started.elapsed();
    let total_elapsed = benchmark_started.elapsed();
    results.sort_by_key(|result| (result.target_node_id.clone(), result.latency_ms));

    let mut latencies = Vec::new();
    let mut failures = Vec::new();
    let mut successes_by_target = BTreeMap::<String, usize>::new();
    let mut failures_by_target = BTreeMap::<String, usize>::new();
    let mut latencies_by_target = BTreeMap::<String, Vec<u64>>::new();
    let mut latencies_by_route = BTreeMap::<String, Vec<u64>>::new();
    for result in &results {
        match &result.outcome {
            NetworkPingOutcome::Success => {
                latencies.push(result.latency_ms);
                latencies_by_route
                    .entry(result.route_mode.clone())
                    .or_default()
                    .push(result.latency_ms);
                latencies_by_target
                    .entry(result.target_node_id.to_string())
                    .or_default()
                    .push(result.latency_ms);
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
    latencies.sort_unstable();
    let success_count = latencies.len();
    let failure_count = failures.len();
    let p50_ms = percentile_ms(&latencies, 50);
    let p95_ms = percentile_ms(&latencies, 95);
    let max_ms = latencies.iter().copied().max().unwrap_or(0);
    let min_ms = latencies.iter().copied().min().unwrap_or(0);
    let requests_per_second = rate_per_second(success_count as f64, elapsed);
    let latency_by_target_ms = latencies_by_target
        .into_iter()
        .map(|(target, mut target_latencies)| {
            target_latencies.sort_unstable();
            let min = target_latencies.iter().copied().min().unwrap_or(0);
            let p50 = percentile_ms(&target_latencies, 50);
            let p95 = percentile_ms(&target_latencies, 95);
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
    let latency_by_route_ms = latencies_by_route
        .into_iter()
        .map(|(route, mut route_latencies)| {
            route_latencies.sort_unstable();
            let min = route_latencies.iter().copied().min().unwrap_or(0);
            let p50 = percentile_ms(&route_latencies, 50);
            let p95 = percentile_ms(&route_latencies, 95);
            let max = route_latencies.iter().copied().max().unwrap_or(0);
            (
                route,
                format!(
                    "count={} min={min} p50={p50} p95={p95} max={max}",
                    route_latencies.len()
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
    ctx.record("latency_min_ms", min_ms);
    ctx.record("latency_p50_ms", p50_ms);
    ctx.record("latency_p95_ms", p95_ms);
    ctx.record("latency_max_ms", max_ms);
    ctx.record("successes_by_target", successes_by_target);
    ctx.record("failures_by_target", failures_by_target);
    ctx.record("latency_by_target_ms", latency_by_target_ms);
    ctx.record("latency_by_route_ms", latency_by_route_ms);
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
    if require_direct {
        let relayed_targets = route_by_target
            .iter()
            .filter_map(|(target, route)| (route == "relay").then(|| target.to_string()))
            .collect::<Vec<_>>();
        if !relayed_targets.is_empty() {
            return Ok(SinomoTestOutcome::failed(format!(
                "network ping used relay routes for direct-required targets: {}",
                relayed_targets.join(", ")
            )));
        }
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
    if min_requests_per_second > 0.0 && requests_per_second < min_requests_per_second {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping throughput {requests_per_second:.2} req/s is below required {min_requests_per_second:.2} req/s"
        )));
    }

    Ok(SinomoTestOutcome::passed(format!(
        "network ping completed {success_count}/{total_requests} RPCs in {}ms ({requests_per_second:.2} req/s, p50={p50_ms}ms p95={p95_ms}ms)",
        elapsed.as_millis()
    )))
}

fn network_ping_targets(
    ctx: &SinomoTestContext,
) -> Result<Vec<ClusterEndpointRecord>, SinomoTestError> {
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let paths = ClusterPaths::new(&data_dir);
    let state = network::read_cluster_state(&paths)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let mut targets = BTreeMap::<NodeId, ClusterEndpointRecord>::new();
    for member in &state.members {
        if member.node_id == state.local_node_id
            || !member.state.is_authorized()
            || !member_has_main_daemon(member)
        {
            continue;
        }
        let Some(endpoint) =
            network::cluster_member_daemon_status_targets_with_connectivity(&paths, &state, member)
                .into_iter()
                .find(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
        else {
            continue;
        };
        targets.insert(member.node_id.clone(), endpoint);
    }
    Ok(targets.into_values().collect())
}

fn network_ping_target_route_descriptions(
    paths: &ClusterPaths,
    targets: &[ClusterEndpointRecord],
) -> (Vec<String>, BTreeMap<NodeId, String>) {
    let Ok(state) = network::read_cluster_state(paths) else {
        let routes = targets
            .iter()
            .map(|target| {
                format!(
                    "{} {} {:?} route=unknown",
                    target.node_id, target.address, target.transport
                )
            })
            .collect();
        let route_by_target = targets
            .iter()
            .map(|target| (target.node_id.clone(), "unknown".to_string()))
            .collect();
        return (routes, route_by_target);
    };
    let graph = network::ClusterConnectivityGraph::read_for_local(paths, &state);
    let mut route_by_target = BTreeMap::new();
    let routes = targets
        .iter()
        .map(|target| {
            let route =
                if network::cluster_endpoint_prefers_relay_with_graph(&graph, &state, target) {
                    "relay"
                } else {
                    "direct"
                };
            route_by_target.insert(target.node_id.clone(), route.to_string());
            format!(
                "{} {} {:?} route={route}",
                target.node_id, target.address, target.transport
            )
        })
        .collect();
    (routes, route_by_target)
}

fn route_counts<'a>(routes: impl IntoIterator<Item = &'a str>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for route in routes {
        *counts.entry(route.to_string()).or_default() += 1;
    }
    counts
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
    payload_bytes: usize,
    request: network::ObjectRpcRequest,
    request_auth: Vec<u8>,
    request_command: String,
    route_mode: String,
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
}

#[derive(Debug)]
struct NetworkPingResult {
    target_node_id: NodeId,
    endpoint_address: String,
    route_mode: String,
    latency_ms: u64,
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
    NetworkPingResult {
        target_node_id: job.target_node_id,
        endpoint_address: job.endpoint.address,
        route_mode: job.route_mode,
        latency_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        outcome,
    }
}

async fn run_network_ping_job_inner(job: &NetworkPingJob) -> NetworkPingOutcome {
    let mut host = NetworkPingEndpointRpcHost {
        runtime: job.runtime.clone(),
        paths: job.paths.clone(),
    };
    let response = match network::send_signed_rpc_to_endpoint_async_with_host(
        &job.endpoint,
        &job.request,
        &job.request_auth,
        &job.request_command,
        &daemon_rpc_command(network::CLUSTER_RELAY_ROUTE),
        network::ClusterSignedEndpointRpcOptions {
            timeouts: network::DaemonRpcTimeouts::connect_and_idle(job.timeout),
        },
        &mut host,
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
    if response.payload_bytes != job.payload_bytes {
        return NetworkPingOutcome::Failure(format!(
            "payload length mismatch: got {}, expected {}",
            response.payload_bytes, job.payload_bytes
        ));
    }
    NetworkPingOutcome::Success
}

struct NetworkPingEndpointRpcHost {
    runtime: tokio::runtime::Handle,
    paths: ClusterPaths,
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

    fn try_gateway_signed_rpc_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
        _request: &'a network::ObjectRpcRequest,
        _request_auth: &'a [u8],
        _request_command: &'a str,
        _timeouts: network::DaemonRpcTimeouts,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Option<network::ObjectRpcResponse>> {
        Box::pin(async { Ok(None) })
    }

    fn endpoint_prefers_relay_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, bool> {
        let paths = self.paths.clone();
        let endpoint = endpoint.clone();
        Box::pin(async move { Ok(network::cluster_endpoint_prefers_relay(&paths, &endpoint)) })
    }

    fn endpoint_rpc_cluster_id_async<'a>(
        &'a mut self,
        _endpoint: &'a ClusterEndpointRecord,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Option<network::ClusterId>> {
        let paths = self.paths.clone();
        Box::pin(async move {
            Ok(network::read_cluster_state(&paths)
                .ok()
                .map(|state| state.cluster_id))
        })
    }

    fn send_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        command: &'a str,
        auth: &'a [u8],
        request: &'a network::ObjectRpcRequest,
        timeouts: network::DaemonRpcTimeouts,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, network::ObjectRpcResponse> {
        Box::pin(async move {
            network::send_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                command,
                Some(auth),
                request,
                timeouts,
            )
            .await
        })
    }

    fn relay_signed_endpoint_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        request: &'a network::ObjectRpcRequest,
        request_auth: &'a [u8],
        request_command: &'a str,
        relay_command: &'a str,
        timeouts: network::DaemonRpcTimeouts,
    ) -> network::ClusterSignedEndpointRpcFuture<'a, Option<network::ObjectRpcResponse>> {
        let runtime = self.runtime.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            network::relay_rpc_to_endpoint_from_paths_async_with_host(
                &runtime,
                &paths,
                endpoint,
                request,
                request_auth.to_vec(),
                request_command.to_string(),
                relay_command,
                network::ClusterSignedRelayRpcOptions { timeouts },
                self,
            )
            .await
        })
    }
}

impl network::ClusterSignedRelayRpcAsyncHost for NetworkPingEndpointRpcHost {
    fn sign_relay_rpc_request_async<'a>(
        &'a mut self,
        request: &'a network::ObjectRpcRequest,
    ) -> network::ClusterSignedRelayRpcFuture<'a, Vec<u8>> {
        let request = request.clone();
        Box::pin(async move {
            sign_daemon_rpc_request_for_local_node(&request)
                .map_err(|error| network_ping_network_error("cluster relay signing", error))
        })
    }

    fn send_signed_relay_rpc_async<'a>(
        &'a mut self,
        endpoint: &'a ClusterEndpointRecord,
        command: &'a str,
        auth: &'a [u8],
        request: &'a network::ObjectRpcRequest,
        timeouts: network::DaemonRpcTimeouts,
    ) -> network::ClusterSignedRelayRpcFuture<'a, network::ObjectRpcResponse> {
        Box::pin(async move {
            network::send_daemon_rpc_to_cluster_endpoint_async(
                endpoint,
                command,
                Some(auth),
                request,
                timeouts,
            )
            .await
        })
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

fn percentile_ms(sorted: &[u64], percentile: usize) -> u64 {
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
