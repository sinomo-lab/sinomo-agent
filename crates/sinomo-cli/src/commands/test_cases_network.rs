//! Cluster network runtime test cases.
//!
//! These probes exercise the signed daemon object-RPC path without touching DFS.
//! The Docker cluster gate uses them before DFS scenarios so routing, relay,
//! authentication, and concurrent request handling must be healthy first.

use sinomo_runtime::api::DaemonApiClient;
use sinomo_runtime::cluster;
use sinomo_runtime::core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord,
    ClusterTransportKind,
};
use sinomo_runtime::local::SinomoDataDir;
use sinomo_runtime::network::{self, ClusterPaths};
use sinomo_runtime::node::NodeId;
use sinomo_runtime::observe::observe_network_timeline_response_from_snapshot;
use sinomo_runtime::test::{
    SinomoTestContext, SinomoTestError, SinomoTestOutcome, SinomoTestRegistryBuilder,
    SinomoTestRequirement, SinomoTestResult,
};
use std::collections::{BTreeMap, BTreeSet};
#[cfg(unix)]
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

// Measure long enough that scheduler and timer-boundary noise cannot decide the
// 50 req/s acceptance gate. With two targets this yields 200 measured RPCs,
// while still completing comfortably inside the cluster acceptance budget.
const DEFAULT_REQUESTS_PER_TARGET: usize = 100;
// Warm the authenticated route, directory, and daemon execution paths before
// starting the timed window, but keep that untimed work explicitly bounded
// instead of coupling it to the larger measurement sample. Each call uses one
// independent logical stream.
const DEFAULT_WARMUP_REQUESTS_PER_TARGET: usize = 20;
// Keep the acceptance default below the daemon's saturation point so the
// latency gates measure healthy interactive RPC service rather than queueing
// behind a burst of control connections. Higher-concurrency throughput runs
// remain available through `--param concurrency=<n>`.
const DEFAULT_CONCURRENCY: usize = 8;
const DEFAULT_TIMEOUT_MS: u64 = 2_000;
const DEFAULT_EXPECTED_TARGETS: usize = 1;
const DEFAULT_MAX_FAILURES: usize = 0;
// The real cluster includes a WAN aarch64 node whose healthy steady-state
// median is commonly 50-70ms. Keep the 2s failure deadline and 250ms p95 gate
// strict while leaving enough p50 headroom for that supported topology.
const DEFAULT_MAX_P50_MS: u64 = 100;
const DEFAULT_MAX_P95_MS: u64 = 250;
const DEFAULT_MAX_P50_US: u64 = 0;
const DEFAULT_MAX_P95_US: u64 = 0;
const DEFAULT_MIN_REQUESTS_PER_SECOND: f64 = 50.0;
const DEFAULT_MIN_PAYLOAD_BYTES_PER_SECOND: f64 = 0.0;
// Keep diagnostic sampling small enough that the ordinary fixed-memory
// timeline captures on the caller and target cannot be flooded by one test.
const MAX_TIMELINE_SAMPLES: usize = 128;
const MAX_CLIENT_TIMELINE_EVENTS: usize = 4 * 1024;
// A sampled bodyless call currently emits 13 client-side events. Leave room
// for lifecycle/error events as the recorder evolves.
const CLIENT_TIMELINE_BASE_EVENTS_PER_SAMPLE: usize = 32;
// Exercise the ordinary RPC interface with a substantial request and response.
// This is a test fixture size, not a protocol boundary.
const ARBITRARY_MESSAGE_PAYLOAD_BYTES: usize = 1024 * 1024;
// Exercise a large attachment in each direction without embedding the bytes in
// either SOBJ head.
const SEMANTIC_ATTACHMENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_BENCHMARK_REQUESTS_PER_TARGET: usize = 10_000;
const MAX_BENCHMARK_CONCURRENCY: usize = 1_024;
const MAX_BENCHMARK_TIMEOUT_MS: u64 = 300_000;
const MAX_PREPARED_NETWORK_PING_JOBS: usize = 100_000;
const MAX_PREPARED_EMBEDDED_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
// Debug builds of the stream-native signed routing path exceed Tokio's 2 MiB
// default worker stack even for one bodyless request. Keep the benchmark's
// private runtime bounded and self-contained instead of requiring operators to
// set RUST_MIN_STACK for the cluster acceptance suite.
const NETWORK_PING_RUNTIME_THREAD_STACK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NetworkPingPayloadMode {
    EmbeddedSobj,
    SemanticAttachment,
}

impl NetworkPingPayloadMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::EmbeddedSobj => "embedded-sobj",
            Self::SemanticAttachment => "semantic-attachment",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum NetworkPingRouteClass {
    Direct,
    Relayed,
    Unresolved,
}

impl NetworkPingRouteClass {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Relayed => "relayed",
            Self::Unresolved => "unresolved",
        }
    }
}

fn network_ping_route_topology(
    route_path: Option<&[NodeId]>,
) -> (NetworkPingRouteClass, Option<usize>) {
    let Some(route_path) = route_path else {
        return (NetworkPingRouteClass::Unresolved, None);
    };
    let relay_hops = route_path.len().saturating_sub(2);
    (
        if relay_hops == 0 {
            NetworkPingRouteClass::Direct
        } else {
            NetworkPingRouteClass::Relayed
        },
        Some(relay_hops),
    )
}

#[cfg(unix)]
fn process_peak_resident_bytes() -> Option<u64> {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `usage` points to writable storage for one `rusage`; a zero
    // return initializes it according to `getrusage(2)`.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized the complete structure.
    let resident = u64::try_from(unsafe { usage.assume_init() }.ru_maxrss).ok()?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // Linux and Android expose ru_maxrss in KiB.
        resident.checked_mul(1024)
    }
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        // Darwin exposes ru_maxrss in bytes.
        Some(resident)
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios"
    )))]
    {
        let _ = resident;
        None
    }
}

#[cfg(not(unix))]
fn process_peak_resident_bytes() -> Option<u64> {
    None
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct NetworkPingRunDefaults {
    payload_mode: NetworkPingPayloadMode,
    parameter_prefix: Option<&'static str>,
    requests_per_target: usize,
    warmup_requests_per_target: Option<usize>,
    concurrency: usize,
    timeout_ms: u64,
    expected_targets: usize,
    max_failures: usize,
    max_p50_ms: u64,
    max_p95_ms: u64,
    max_p50_us: u64,
    max_p95_us: u64,
    min_requests_per_second: f64,
    payload_bytes: usize,
    response_payload_bytes: usize,
    min_payload_bytes_per_second: f64,
}

impl NetworkPingRunDefaults {
    const fn benchmark() -> Self {
        Self {
            payload_mode: NetworkPingPayloadMode::EmbeddedSobj,
            parameter_prefix: None,
            requests_per_target: DEFAULT_REQUESTS_PER_TARGET,
            warmup_requests_per_target: Some(DEFAULT_WARMUP_REQUESTS_PER_TARGET),
            concurrency: DEFAULT_CONCURRENCY,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            expected_targets: DEFAULT_EXPECTED_TARGETS,
            max_failures: DEFAULT_MAX_FAILURES,
            max_p50_ms: DEFAULT_MAX_P50_MS,
            max_p95_ms: DEFAULT_MAX_P95_MS,
            max_p50_us: DEFAULT_MAX_P50_US,
            max_p95_us: DEFAULT_MAX_P95_US,
            min_requests_per_second: DEFAULT_MIN_REQUESTS_PER_SECOND,
            payload_bytes: 0,
            response_payload_bytes: 0,
            min_payload_bytes_per_second: DEFAULT_MIN_PAYLOAD_BYTES_PER_SECOND,
        }
    }

    const fn arbitrary_message() -> Self {
        Self {
            payload_mode: NetworkPingPayloadMode::EmbeddedSobj,
            parameter_prefix: Some("arbitrary-message"),
            requests_per_target: 1,
            warmup_requests_per_target: Some(0),
            concurrency: DEFAULT_CONCURRENCY,
            timeout_ms: 10_000,
            expected_targets: DEFAULT_EXPECTED_TARGETS,
            max_failures: 0,
            max_p50_ms: 0,
            max_p95_ms: 0,
            max_p50_us: 0,
            max_p95_us: 0,
            min_requests_per_second: 0.0,
            payload_bytes: ARBITRARY_MESSAGE_PAYLOAD_BYTES,
            response_payload_bytes: ARBITRARY_MESSAGE_PAYLOAD_BYTES,
            min_payload_bytes_per_second: 0.0,
        }
    }

    const fn semantic_attachment() -> Self {
        Self {
            payload_mode: NetworkPingPayloadMode::SemanticAttachment,
            parameter_prefix: Some("attachment"),
            requests_per_target: 4,
            warmup_requests_per_target: Some(1),
            concurrency: 4,
            timeout_ms: 30_000,
            expected_targets: DEFAULT_EXPECTED_TARGETS,
            max_failures: 0,
            max_p50_ms: 0,
            max_p95_ms: 0,
            max_p50_us: 0,
            max_p95_us: 0,
            min_requests_per_second: 0.0,
            payload_bytes: SEMANTIC_ATTACHMENT_BYTES,
            response_payload_bytes: SEMANTIC_ATTACHMENT_BYTES,
            min_payload_bytes_per_second: 0.0,
        }
    }

    fn parameter_name(self, name: &str) -> String {
        self.parameter_prefix
            .map(|prefix| format!("{prefix}-{name}"))
            .unwrap_or_else(|| name.to_string())
    }
}

pub fn register_network_cluster_test_cases(
    builder: SinomoTestRegistryBuilder,
) -> SinomoTestRegistryBuilder {
    builder
        .register(network_ping_benchmark())
        .register(network_semantic_attachment_benchmark())
        .register(network_arbitrary_message())
        .register(network_state_hygiene())
        .register(network_state_hygiene_dirty_fixture())
        .register(network_endpoint_error_fallback())
        .register(network_stale_direct_timeout_demotion())
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_semantic_attachment_benchmark(ctx) {
        id: "cluster.network.semantic-attachment-benchmark",
        summary: "Stream small SOBJ heads with large semantic request and response attachments",
        tags: ["cluster-network", "probe", "performance", "semantic-attachment"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_semantic_attachment_benchmark(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_arbitrary_message(ctx) {
        id: "cluster.network.arbitrary-message",
        summary: "Route an arbitrary-sized request and response through the ordinary daemon RPC interface",
        tags: ["cluster-network", "probe", "arbitrary-message", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_arbitrary_message(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_ping_benchmark(ctx) {
        id: "cluster.network.ping-benchmark",
        summary: "Concurrent transport-bound cluster network ping RPCs to every remote main daemon",
        tags: ["cluster-network", "probe", "performance", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_ping_benchmark(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_state_hygiene(ctx) {
        id: "cluster.network.hygiene",
        summary: "Advertised endpoints and active connectivity records are internally consistent",
        tags: ["cluster-network", "probe", "routing", "hygiene", "cluster-network-dfs-acceptance"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_state_hygiene(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_state_hygiene_dirty_fixture(ctx) {
        id: "cluster.network.hygiene-dirty-fixture",
        summary: "Network hygiene detects and restores an injected stale local connectivity record",
        tags: ["cluster-network", "probe", "routing", "hygiene", "failure-mode"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_state_hygiene_dirty_fixture(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_endpoint_error_fallback(ctx) {
        id: "cluster.network.endpoint-error-fallback",
        summary: "Routed daemon RPC retries a reachable endpoint that returns an auth error",
        tags: ["cluster-network", "probe", "routing", "failure-mode"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_endpoint_error_fallback(ctx)
        }
    }
}

sinomo_runtime::test::sinomo_test_case! {
    pub fn network_stale_direct_timeout_demotion(ctx) {
        id: "cluster.network.stale-direct-timeout-demotion",
        summary: "A routed timeout demotes stale healthy direct endpoint evidence before follow-up RPCs",
        tags: ["cluster-network", "probe", "routing", "failure-mode"],
        requirements: [SinomoTestRequirement::ClusterRuntime],
        run: {
            run_network_stale_direct_timeout_demotion(ctx)
        }
    }
}

fn run_network_state_hygiene(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let expected_targets = optional_usize_parameter(ctx, "expected-targets", 1)?;
    let max_findings = optional_usize_parameter(ctx, "max-findings", 0)?;
    let require_active_routes = optional_bool_parameter(ctx, "require-active-routes", true)?;

    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before network hygiene: {error}"
        ))
    })?;
    let cluster_state = cluster::read_cluster_state(&cluster_paths)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let targets = filter_network_test_targets(
        ctx,
        &cluster_state,
        super::test::live_online_remote_main_test_targets(&data_dir, &cluster_state)?,
    )?;
    ctx.record("target_count", targets.len());
    ctx.record(
        "targets",
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
    );
    if !super::test::cluster_test_has_expected_target_count(targets.len(), expected_targets) {
        return Ok(SinomoTestOutcome::failed(format!(
            "network hygiene saw {} remote main targets, expected at least {expected_targets}",
            targets.len()
        )));
    }
    let pruned_local_records =
        network::prune_local_cluster_connectivity_records_for_state(&cluster_paths, &cluster_state)
            .map_err(|error| {
                SinomoTestError::failed(format!(
                    "cannot prune stale local connectivity records before network hygiene: {error}"
                ))
            })?;
    ctx.record("pruned_local_connectivity_records", pruned_local_records);

    let local_records =
        network::read_local_cluster_connectivity_records(&cluster_paths).map_err(|error| {
            SinomoTestError::failed(format!("cannot read local connectivity records: {error}"))
        })?;
    let kv_records =
        cluster::read_cluster_connectivity_graph_from_kv(&cluster_paths).map_err(|error| {
            SinomoTestError::failed(format!("cannot read KV connectivity records: {error}"))
        })?;
    let active_records =
        network::read_cluster_connectivity_graph(&cluster_paths).map_err(|error| {
            SinomoTestError::failed(format!("cannot read active connectivity graph: {error}"))
        })?;
    let findings = network_hygiene_findings(
        &cluster_state,
        &local_records,
        &kv_records,
        &active_records,
        NetworkHygieneOptions::for_targets(require_active_routes, targets.iter().cloned()),
    );
    ctx.record("local_connectivity_records", local_records.len());
    ctx.record("kv_connectivity_records", kv_records.len());
    ctx.record("active_connectivity_records", active_records.len());
    ctx.record("max_findings", max_findings);
    ctx.record("findings", findings.clone());
    if findings.len() > max_findings {
        return Ok(SinomoTestOutcome::failed(format!(
            "network hygiene found {} issue(s): {}",
            findings.len(),
            findings
                .iter()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join("; ")
        )));
    }
    Ok(SinomoTestOutcome::passed(
        "advertised endpoints and connectivity records are consistent",
    ))
}

fn run_network_state_hygiene_dirty_fixture(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let expected_targets = optional_usize_parameter(ctx, "expected-targets", 1)?;
    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before network hygiene dirty fixture: {error}"
        ))
    })?;
    let cluster_state = cluster::read_cluster_state(&cluster_paths)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let targets = filter_network_test_targets(
        ctx,
        &cluster_state,
        super::test::live_online_remote_main_test_targets(&data_dir, &cluster_state)?,
    )?;
    ctx.record("target_count", targets.len());
    ctx.record(
        "targets",
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
    );
    if !super::test::cluster_test_has_expected_target_count(targets.len(), expected_targets) {
        return Ok(SinomoTestOutcome::failed(format!(
            "network hygiene dirty fixture saw {} remote main targets, expected at least {expected_targets}",
            targets.len()
        )));
    }
    let target = targets
        .first()
        .cloned()
        .ok_or_else(|| SinomoTestError::failed("no remote main target available".to_string()))?;

    let original_source_records = network::read_local_cluster_connectivity_records_for_source(
        &cluster_paths,
        &cluster_state.local_node_id,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot snapshot local-source connectivity records: {error}"
        ))
    })?;
    let dirty = network_hygiene_dirty_fixture_record(&cluster_state, &target);
    let mut seeded_records = original_source_records.clone();
    seeded_records.retain(|record| record.endpoint_id != dirty.endpoint_id);
    seeded_records.push(dirty.clone());
    network::write_local_cluster_connectivity_record_set(
        &cluster_paths,
        &cluster_state.local_node_id,
        &seeded_records,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot seed dirty local connectivity fixture: {error}"
        ))
    })?;

    let probe_result =
        run_network_state_hygiene_dirty_fixture_probe(&cluster_paths, &cluster_state, &dirty);
    let restore_result = (|| -> Result<(), network::NetworkError> {
        let mut current_source_records =
            network::read_local_cluster_connectivity_records_for_source(
                &cluster_paths,
                &cluster_state.local_node_id,
            )?;
        current_source_records.retain(|record| record.endpoint_id != dirty.endpoint_id);
        let restored_records = target_scoped_connectivity_fixture_records(
            &cluster_paths,
            &cluster_state.local_node_id,
            &target,
            current_source_records,
        )?;
        network::write_local_cluster_connectivity_record_set(
            &cluster_paths,
            &cluster_state.local_node_id,
            &restored_records,
        )
    })();
    if let Err(error) = restore_result {
        return Err(SinomoTestError::failed(format!(
            "cannot restore local connectivity after hygiene dirty fixture: {error}"
        )));
    }
    let findings = probe_result?;
    ctx.record("dirty_target", target.to_string());
    ctx.record("dirty_endpoint_id", dirty.endpoint_id.to_string());
    ctx.record("dirty_address", dirty.address.clone());
    ctx.record("findings", findings.clone());
    if !findings.iter().any(|finding| {
        finding.contains("references unadvertised endpoint")
            && finding.contains(dirty.endpoint_id.as_str())
    }) {
        return Ok(SinomoTestOutcome::failed(format!(
            "network hygiene did not flag injected stale endpoint {}; findings={findings:?}",
            dirty.endpoint_id
        )));
    }

    let restored_source_records = network::read_local_cluster_connectivity_records_for_source(
        &cluster_paths,
        &cluster_state.local_node_id,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot verify local connectivity restore after dirty fixture: {error}"
        ))
    })?;
    if restored_source_records
        .iter()
        .any(|record| record.endpoint_id == dirty.endpoint_id)
    {
        return Ok(SinomoTestOutcome::failed(format!(
            "dirty endpoint {} remained after restore",
            dirty.endpoint_id
        )));
    }

    Ok(SinomoTestOutcome::passed(
        "network hygiene detected an injected stale local connectivity record and restored state",
    ))
}

fn run_network_state_hygiene_dirty_fixture_probe(
    cluster_paths: &ClusterPaths,
    cluster_state: &ClusterBootstrapState,
    dirty: &network::ClusterConnectivityRecord,
) -> Result<Vec<String>, SinomoTestError> {
    let local_records =
        network::read_local_cluster_connectivity_records(cluster_paths).map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot read local connectivity records after dirty fixture: {error}"
            ))
        })?;
    if !local_records
        .iter()
        .any(|record| record.endpoint_id == dirty.endpoint_id)
    {
        return Err(SinomoTestError::failed(format!(
            "dirty endpoint {} was not persisted before hygiene probe",
            dirty.endpoint_id
        )));
    }
    let kv_records =
        cluster::read_cluster_connectivity_graph_from_kv(cluster_paths).map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot read KV connectivity records after dirty fixture: {error}"
            ))
        })?;
    let active_records =
        network::read_cluster_connectivity_graph(cluster_paths).map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot read active connectivity graph after dirty fixture: {error}"
            ))
        })?;
    Ok(network_hygiene_findings(
        cluster_state,
        &local_records,
        &kv_records,
        &active_records,
        NetworkHygieneOptions::unscoped(false),
    ))
}

fn network_hygiene_dirty_fixture_record(
    state: &ClusterBootstrapState,
    target: &NodeId,
) -> network::ClusterConnectivityRecord {
    let now = OffsetDateTime::now_utc();
    network::ClusterConnectivityRecord {
        cluster_id: state.cluster_id.clone(),
        source_node_id: state.local_node_id.clone(),
        target_node_id: target.clone(),
        endpoint_id: network::ClusterEndpointId::derived(
            "sinomo.test.network.hygiene.dirty-endpoint",
            format!("{}\0{}", state.local_node_id.as_str(), target.as_str()),
        ),
        transport: ClusterTransportKind::LanTcp,
        address: "stale.example.test:21353".to_string(),
        state: network::ClusterConnectivityState::Healthy,
        latency_ms: Some(1),
        consecutive_failures: 0,
        last_success_at: Some(now.to_string()),
        last_failure_at: None,
        failure_reason: None,
        observed_at: now,
        expires_at: now + time::Duration::seconds(network::CLUSTER_CONNECTIVITY_TTL_SECONDS),
    }
}

fn run_network_ping_benchmark(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    run_network_ping_with_defaults(ctx, NetworkPingRunDefaults::benchmark())
}

fn run_network_semantic_attachment_benchmark(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    run_network_ping_with_defaults(ctx, NetworkPingRunDefaults::semantic_attachment())
}

fn should_sample_network_timeline(
    job_index: usize,
    total_jobs: usize,
    requested_samples: usize,
) -> bool {
    let sample_count = requested_samples.min(total_jobs);
    if sample_count == 0 || job_index >= total_jobs {
        return false;
    }
    if job_index == 0 {
        return true;
    }
    job_index.saturating_mul(sample_count) / total_jobs
        != job_index.saturating_sub(1).saturating_mul(sample_count) / total_jobs
}

fn benchmark_client_timeline_capacity(timeline_sample_count: usize) -> usize {
    timeline_sample_count
        .saturating_mul(CLIENT_TIMELINE_BASE_EVENTS_PER_SAMPLE)
        .clamp(1, MAX_CLIENT_TIMELINE_EVENTS)
}

fn run_network_arbitrary_message(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    run_network_ping_with_defaults(ctx, NetworkPingRunDefaults::arbitrary_message())
}

fn run_network_ping_with_defaults(
    ctx: &mut SinomoTestContext,
    defaults: NetworkPingRunDefaults,
) -> SinomoTestResult {
    let requests_per_target_name = defaults.parameter_name("requests-per-target");
    let warmup_requests_per_target_name = defaults.parameter_name("warmup-requests-per-target");
    let concurrency_name = defaults.parameter_name("concurrency");
    let timeout_name = defaults.parameter_name("timeout-ms");
    let max_failures_name = defaults.parameter_name("max-failures");
    let max_p50_ms_name = defaults.parameter_name("max-p50-ms");
    let max_p95_ms_name = defaults.parameter_name("max-p95-ms");
    let max_p50_us_name = defaults.parameter_name("max-p50-us");
    let max_p95_us_name = defaults.parameter_name("max-p95-us");
    let min_requests_per_second_name = defaults.parameter_name("min-requests-per-second");
    let payload_bytes_name = defaults.parameter_name("payload-bytes");
    let response_payload_bytes_name = defaults.parameter_name("response-payload-bytes");
    let min_payload_bytes_per_second_name = defaults.parameter_name("min-payload-bytes-per-second");
    let timeline_samples_name = defaults.parameter_name("timeline-samples");
    let requests_per_target =
        optional_usize_parameter(ctx, &requests_per_target_name, defaults.requests_per_target)?;
    let warmup_requests_per_target = optional_usize_parameter(
        ctx,
        &warmup_requests_per_target_name,
        defaults
            .warmup_requests_per_target
            .unwrap_or(requests_per_target),
    )?;
    let concurrency = optional_usize_parameter(ctx, &concurrency_name, defaults.concurrency)?;
    let timeout_ms = optional_u64_parameter(ctx, &timeout_name, defaults.timeout_ms)?;
    let expected_targets =
        optional_usize_parameter(ctx, "expected-targets", defaults.expected_targets)?;
    let include_unobserved_targets =
        optional_bool_parameter(ctx, "include-unobserved-targets", false)?;
    let max_failures = optional_usize_parameter(ctx, &max_failures_name, defaults.max_failures)?;
    let max_p50_ms = optional_u64_parameter(ctx, &max_p50_ms_name, defaults.max_p50_ms)?;
    let max_p95_ms = optional_u64_parameter(ctx, &max_p95_ms_name, defaults.max_p95_ms)?;
    let max_p50_us = optional_u64_parameter(ctx, &max_p50_us_name, defaults.max_p50_us)?;
    let max_p95_us = optional_u64_parameter(ctx, &max_p95_us_name, defaults.max_p95_us)?;
    let min_requests_per_second = optional_f64_parameter(
        ctx,
        &min_requests_per_second_name,
        defaults.min_requests_per_second,
    )?;
    let payload_bytes = optional_usize_parameter(ctx, &payload_bytes_name, defaults.payload_bytes)?;
    let response_payload_bytes = optional_usize_parameter(
        ctx,
        &response_payload_bytes_name,
        defaults.response_payload_bytes,
    )?;
    let (attachment_request_bytes, attachment_response_bytes) =
        if defaults.payload_mode == NetworkPingPayloadMode::SemanticAttachment {
            let request_bytes =
                u64::try_from(payload_bytes).map_err(|_| SinomoTestError::InvalidParameter {
                    name: payload_bytes_name.clone(),
                    detail: "does not fit the attachment byte-count field".to_string(),
                })?;
            let response_bytes = u64::try_from(response_payload_bytes).map_err(|_| {
                SinomoTestError::InvalidParameter {
                    name: response_payload_bytes_name.clone(),
                    detail: "does not fit the attachment byte-count field".to_string(),
                }
            })?;
            for (name, bytes) in [
                (&payload_bytes_name, request_bytes),
                (&response_payload_bytes_name, response_bytes),
            ] {
                if bytes > cluster::CLUSTER_NETWORK_PING_ATTACHMENT_MAX_BYTES {
                    return Err(SinomoTestError::InvalidParameter {
                        name: name.clone(),
                        detail: format!(
                            "must be at most {} bytes",
                            cluster::CLUSTER_NETWORK_PING_ATTACHMENT_MAX_BYTES
                        ),
                    });
                }
            }
            (request_bytes, response_bytes)
        } else {
            (0, 0)
        };
    let min_payload_bytes_per_second = optional_f64_parameter(
        ctx,
        &min_payload_bytes_per_second_name,
        defaults.min_payload_bytes_per_second,
    )?;
    let timeline_samples = optional_usize_parameter(ctx, &timeline_samples_name, 0)?;
    if timeline_samples > MAX_TIMELINE_SAMPLES {
        return Err(SinomoTestError::InvalidParameter {
            name: timeline_samples_name,
            detail: format!("must be at most {MAX_TIMELINE_SAMPLES}"),
        });
    }
    require_positive(&requests_per_target_name, requests_per_target)?;
    require_positive(&concurrency_name, concurrency)?;
    require_positive(&timeout_name, timeout_ms as usize)?;
    require_at_most(
        &requests_per_target_name,
        requests_per_target,
        MAX_BENCHMARK_REQUESTS_PER_TARGET,
    )?;
    require_at_most(
        &warmup_requests_per_target_name,
        warmup_requests_per_target,
        MAX_BENCHMARK_REQUESTS_PER_TARGET,
    )?;
    require_at_most(&concurrency_name, concurrency, MAX_BENCHMARK_CONCURRENCY)?;
    if timeout_ms > MAX_BENCHMARK_TIMEOUT_MS {
        return Err(SinomoTestError::InvalidParameter {
            name: timeout_name,
            detail: format!("must be at most {MAX_BENCHMARK_TIMEOUT_MS}"),
        });
    }
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
    let discovered_targets = if include_unobserved_targets {
        super::test::authorized_remote_main_test_targets(&data_dir, &cluster_state)?
    } else {
        super::test::live_online_remote_main_test_targets(&data_dir, &cluster_state)?
    };
    let targets = filter_network_test_targets(ctx, &cluster_state, discovered_targets)?;
    let target_count = targets.len();
    let route_graph =
        network::ClusterConnectivityGraph::read_for_local(&cluster_paths, cluster_state.as_ref());
    let expected_route_by_target = targets
        .iter()
        .map(|target| {
            let route =
                network::cluster_route_next_hop(&route_graph, &cluster_state.local_node_id, target);
            let (route_class, relay_hops) =
                network_ping_route_topology(route.as_ref().map(|route| route.path.as_slice()));
            (
                target.clone(),
                (route_class, relay_hops, route.map(|route| route.path)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    ctx.record(
        "target_selection",
        if include_unobserved_targets {
            "authorized-membership"
        } else {
            "live-online"
        },
    );
    ctx.record("target_count", target_count);
    ctx.record(
        "targets",
        targets.iter().map(ToString::to_string).collect::<Vec<_>>(),
    );
    ctx.record(
        "expected_route_by_target",
        expected_route_by_target
            .iter()
            .map(|(target, (route_class, relay_hops, path))| {
                (
                    target.to_string(),
                    format!(
                        "class={} relay_hops={} path={}",
                        route_class.as_str(),
                        relay_hops
                            .map(|hops| hops.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        path.as_ref()
                            .map(|nodes| nodes
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                                .join(" -> "))
                            .unwrap_or_else(|| "unresolved".to_string())
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    );
    if !super::test::cluster_test_has_expected_target_count(target_count, expected_targets) {
        return Ok(SinomoTestOutcome::failed(format!(
            "network ping saw {target_count} remote main targets, expected at least {expected_targets}"
        )));
    }

    let measured_job_count = target_count
        .checked_mul(requests_per_target)
        .ok_or_else(|| SinomoTestError::InvalidParameter {
            name: requests_per_target_name.clone(),
            detail: "target count times requests per target overflows".to_string(),
        })?;
    let warmup_job_count = target_count
        .checked_mul(warmup_requests_per_target)
        .ok_or_else(|| SinomoTestError::InvalidParameter {
            name: warmup_requests_per_target_name.clone(),
            detail: "target count times warmup requests per target overflows".to_string(),
        })?;
    for (name, jobs) in [
        (&requests_per_target_name, measured_job_count),
        (&warmup_requests_per_target_name, warmup_job_count),
    ] {
        if jobs > MAX_PREPARED_NETWORK_PING_JOBS {
            return Err(SinomoTestError::InvalidParameter {
                name: name.clone(),
                detail: format!(
                    "would prepare {jobs} jobs, above the bounded maximum {MAX_PREPARED_NETWORK_PING_JOBS}"
                ),
            });
        }
    }
    if defaults.payload_mode == NetworkPingPayloadMode::EmbeddedSobj {
        let prepared_payload_bytes = measured_job_count
            .checked_add(warmup_job_count)
            .and_then(|jobs| jobs.checked_mul(payload_bytes))
            .unwrap_or(usize::MAX);
        let maximum_live_payload_bytes = concurrency
            .checked_mul(payload_bytes.max(response_payload_bytes))
            .unwrap_or(usize::MAX);
        if prepared_payload_bytes > MAX_PREPARED_EMBEDDED_PAYLOAD_BYTES
            || maximum_live_payload_bytes > MAX_PREPARED_EMBEDDED_PAYLOAD_BYTES
        {
            return Err(SinomoTestError::InvalidParameter {
                name: payload_bytes_name,
                detail: format!(
                    "embedded benchmark payloads exceed the bounded {} byte preparation/live budget",
                    MAX_PREPARED_EMBEDDED_PAYLOAD_BYTES
                ),
            });
        }
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
        .thread_stack_size(NETWORK_PING_RUNTIME_THREAD_STACK_BYTES)
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
    let daemon_address = daemon_client.address().to_string();
    let control_timeout = Duration::from_millis(timeout_ms).saturating_add(Duration::from_secs(1));
    let build_jobs =
        |phase: &str, requests_per_target: usize| -> Result<Vec<NetworkPingJob>, SinomoTestError> {
            let total_jobs = target_count.saturating_mul(requests_per_target);
            let mut jobs = Vec::with_capacity(total_jobs);
            for attempt in 0..requests_per_target {
                for target_node_id in &targets {
                    let (route_class, relay_hops, _) = expected_route_by_target
                        .get(target_node_id)
                        .expect("every selected target has a route classification");
                    let nonce = format!("ping-{phase}-{nonce_prefix}-{target_node_id}-{attempt}");
                    let mut call = match defaults.payload_mode {
                        NetworkPingPayloadMode::EmbeddedSobj => {
                            cluster::cluster_network_ping_rpc_call_with_response_payload(
                                &nonce,
                                payload_bytes,
                                response_payload_bytes,
                            )
                        }
                        NetworkPingPayloadMode::SemanticAttachment => {
                            cluster::cluster_network_ping_attachment_rpc_request(
                                &nonce,
                                attachment_request_bytes,
                                attachment_response_bytes,
                            )
                            .map(|request| {
                                network::ObjectRpcCall::new(
                                    request,
                                    network::ObjectRpcBodySource::zeroes(attachment_request_bytes),
                                )
                            })
                        }
                    }
                    .map_err(|source| {
                        SinomoTestError::failed(format!(
                            "cannot prepare network ping request for {}: {source}",
                            target_node_id
                        ))
                    })?;
                    if phase == "measure"
                        && should_sample_network_timeline(jobs.len(), total_jobs, timeline_samples)
                    {
                        call.request = call
                            .request
                            .with_trace_context_if_missing()
                            .with_observe_sampled(true);
                    }
                    jobs.push(NetworkPingJob {
                        payload_mode: defaults.payload_mode,
                        target_node_id: target_node_id.clone(),
                        route_class: *route_class,
                        relay_hops: *relay_hops,
                        timeout: Duration::from_millis(timeout_ms),
                        control_timeout,
                        request_payload_bytes: payload_bytes,
                        response_payload_bytes,
                        nonce,
                        call,
                        daemon_address: daemon_address.clone(),
                    });
                }
            }
            Ok(jobs)
        };
    let warmup_jobs = build_jobs("warmup", warmup_requests_per_target)?;
    let jobs = build_jobs("measure", requests_per_target)?;
    let timeline_sample_count = jobs
        .iter()
        .filter(|job| job.call.request.observe_sampled())
        .count();
    let timeline_first_sample = jobs.iter().find(|job| job.call.request.observe_sampled());
    let prepare_elapsed = prepare_started.elapsed();
    let total_requests = jobs.len();
    let maximum_request_sobj_head_bytes = jobs
        .iter()
        .map(|job| job.call.request.body.len())
        .max()
        .unwrap_or(0);
    ctx.record("payload_mode", defaults.payload_mode.as_str());
    ctx.record("requests_per_target", requests_per_target);
    ctx.record("warmup_requests_per_target", warmup_requests_per_target);
    ctx.record("warmup_total_requests", warmup_jobs.len());
    ctx.record("total_requests", total_requests);
    ctx.record("concurrency", concurrency_limit);
    ctx.record("runtime_worker_threads", runtime_worker_threads);
    ctx.record("timeout_ms", timeout_ms);
    ctx.record("request_payload_bytes", payload_bytes);
    ctx.record("response_payload_bytes", response_payload_bytes);
    ctx.record(
        "maximum_request_sobj_head_bytes",
        maximum_request_sobj_head_bytes,
    );
    ctx.record("max_failures", max_failures);
    ctx.record("max_p50_ms", max_p50_ms);
    ctx.record("max_p95_ms", max_p95_ms);
    ctx.record("max_p50_us", max_p50_us);
    ctx.record("max_p95_us", max_p95_us);
    ctx.record("timeline_sample_count", timeline_sample_count);
    if let Some(job) = timeline_first_sample {
        ctx.record(
            "timeline_first_traceparent",
            job.call.request.traceparent().unwrap_or_default(),
        );
        ctx.record("timeline_first_target", job.target_node_id.to_string());
    }
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
        .filter(|result| matches!(result.outcome, NetworkPingOutcome::Success { .. }))
        .count();
    let warmup_failures = warmup_results
        .iter()
        .filter_map(|result| match &result.outcome {
            NetworkPingOutcome::Success { .. } => None,
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

    let source_daemon_chunk_metrics_before = runtime
        .block_on(daemon_client.network_chunk_metrics_async())
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot sample source-daemon network chunk metrics before benchmark: {source}"
            ))
        })?
        .chunk_metrics;
    let client_timeline_capacity = benchmark_client_timeline_capacity(timeline_sample_count);
    if timeline_sample_count > 0 {
        network::start_network_timeline_capture(client_timeline_capacity).map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot start benchmark client network timeline: {error}"
            ))
        })?;
    }
    // Snapshot after warmup so allocation/copy counters describe only the
    // measured calls. This makes ownership regressions visible alongside
    // latency and throughput without changing either hard gate.
    let chunk_metrics_before = network::network_chunk_metrics();
    let started = Instant::now();
    let measured_result = runtime.block_on(run_network_ping_jobs_async(jobs, concurrency_limit));
    let elapsed = started.elapsed();
    let chunk_metrics_after = network::network_chunk_metrics();
    let client_timeline = if timeline_sample_count > 0 {
        Some(network::stop_network_timeline_capture().map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot stop benchmark client network timeline: {error}"
            ))
        })?)
    } else {
        None
    };
    let source_daemon_chunk_metrics_after = runtime
        .block_on(daemon_client.network_chunk_metrics_async())
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot sample source-daemon network chunk metrics after benchmark: {source}"
            ))
        })?
        .chunk_metrics;
    let mut results = measured_result?;
    let total_elapsed = benchmark_started.elapsed();
    if let Some(snapshot) = client_timeline {
        ctx.record(
            "timeline_client_capture",
            observe_network_timeline_response_from_snapshot(
                Some("network-ping-client".to_string()),
                "test-client",
                Some(cluster_state.local_node_id.to_string()),
                snapshot,
            ),
        );
    }
    results.sort_by_key(|result| (result.target_node_id.clone(), result.latency_ms));

    let mut latencies_ms = Vec::new();
    let mut latencies_us = Vec::new();
    let mut proxy_round_trip_latencies_us = Vec::new();
    let mut failures = Vec::new();
    let mut successes_by_target = BTreeMap::<String, usize>::new();
    let mut failures_by_target = BTreeMap::<String, usize>::new();
    let mut successes_by_route_class = BTreeMap::<String, usize>::new();
    let mut successes_by_relay_hops = BTreeMap::<String, usize>::new();
    let mut latencies_by_target = BTreeMap::<String, Vec<u64>>::new();
    let mut latencies_by_target_us = BTreeMap::<String, Vec<u64>>::new();
    for result in &results {
        match &result.outcome {
            NetworkPingOutcome::Success { .. } => {
                latencies_ms.push(result.latency_ms);
                latencies_us.push(result.latency_us);
                proxy_round_trip_latencies_us.push(result.proxy_round_trip_latency_us);
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
                *successes_by_route_class
                    .entry(result.route_class.as_str().to_string())
                    .or_default() += 1;
                *successes_by_relay_hops
                    .entry(
                        result
                            .relay_hops
                            .map(|hops| hops.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    )
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
    proxy_round_trip_latencies_us.sort_unstable();
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
    let proxy_round_trip_p50_us = percentile_value(&proxy_round_trip_latencies_us, 50);
    let proxy_round_trip_p95_us = percentile_value(&proxy_round_trip_latencies_us, 95);
    let slowest_targets_ms = network_ping_slowest_target_summaries(&latencies_by_target, "ms");
    let slowest_targets_us = network_ping_slowest_target_summaries(&latencies_by_target_us, "us");
    let requests_per_second = rate_per_second(success_count as f64, elapsed);
    let payload_bytes_per_success = payload_bytes.saturating_add(response_payload_bytes);
    let total_success_request_bytes = payload_bytes.saturating_mul(success_count);
    let total_success_response_bytes = response_payload_bytes.saturating_mul(success_count);
    let total_success_payload_bytes = payload_bytes_per_success.saturating_mul(success_count);
    let request_bytes_per_second = rate_per_second(total_success_request_bytes as f64, elapsed);
    let response_bytes_per_second = rate_per_second(total_success_response_bytes as f64, elapsed);
    let payload_bytes_per_second = rate_per_second(total_success_payload_bytes as f64, elapsed);
    let successful_payload_bytes_by_route_class = successes_by_route_class
        .iter()
        .map(|(route_class, successes)| {
            (
                route_class.clone(),
                payload_bytes_per_success.saturating_mul(*successes) as u64,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let payload_bytes_per_second_by_route_class = successful_payload_bytes_by_route_class
        .iter()
        .map(|(route_class, bytes)| (route_class.clone(), rate_per_second(*bytes as f64, elapsed)))
        .collect::<BTreeMap<_, _>>();
    let client_chunk_allocations = chunk_metrics_after
        .allocations
        .saturating_sub(chunk_metrics_before.allocations);
    let client_chunk_reuses = chunk_metrics_after
        .reuses
        .saturating_sub(chunk_metrics_before.reuses);
    let client_chunk_deallocations = chunk_metrics_after
        .deallocations
        .saturating_sub(chunk_metrics_before.deallocations);
    let client_payload_copy_bytes = chunk_metrics_after
        .payload_copy_bytes
        .saturating_sub(chunk_metrics_before.payload_copy_bytes);
    let client_shared_source_normalization_copy_bytes = chunk_metrics_after
        .shared_source_normalization_copy_bytes
        .saturating_sub(chunk_metrics_before.shared_source_normalization_copy_bytes);
    let client_byte_stream_write_adapter_copy_bytes = chunk_metrics_after
        .byte_stream_write_adapter_copy_bytes
        .saturating_sub(chunk_metrics_before.byte_stream_write_adapter_copy_bytes);
    let client_byte_stream_read_adapter_copy_bytes = chunk_metrics_after
        .byte_stream_read_adapter_copy_bytes
        .saturating_sub(chunk_metrics_before.byte_stream_read_adapter_copy_bytes);
    let client_contiguous_body_materialization_copy_bytes = chunk_metrics_after
        .contiguous_body_materialization_copy_bytes
        .saturating_sub(chunk_metrics_before.contiguous_body_materialization_copy_bytes);
    let source_daemon_chunk_allocations = source_daemon_chunk_metrics_after
        .allocations
        .saturating_sub(source_daemon_chunk_metrics_before.allocations);
    let source_daemon_chunk_reuses = source_daemon_chunk_metrics_after
        .reuses
        .saturating_sub(source_daemon_chunk_metrics_before.reuses);
    let source_daemon_chunk_deallocations = source_daemon_chunk_metrics_after
        .deallocations
        .saturating_sub(source_daemon_chunk_metrics_before.deallocations);
    let source_daemon_payload_copy_bytes = source_daemon_chunk_metrics_after
        .payload_copy_bytes
        .saturating_sub(source_daemon_chunk_metrics_before.payload_copy_bytes);
    let source_daemon_shared_source_normalization_copy_bytes = source_daemon_chunk_metrics_after
        .shared_source_normalization_copy_bytes
        .saturating_sub(source_daemon_chunk_metrics_before.shared_source_normalization_copy_bytes);
    let source_daemon_byte_stream_write_adapter_copy_bytes = source_daemon_chunk_metrics_after
        .byte_stream_write_adapter_copy_bytes
        .saturating_sub(source_daemon_chunk_metrics_before.byte_stream_write_adapter_copy_bytes);
    let source_daemon_byte_stream_read_adapter_copy_bytes = source_daemon_chunk_metrics_after
        .byte_stream_read_adapter_copy_bytes
        .saturating_sub(source_daemon_chunk_metrics_before.byte_stream_read_adapter_copy_bytes);
    let source_daemon_contiguous_body_materialization_copy_bytes =
        source_daemon_chunk_metrics_after
            .contiguous_body_materialization_copy_bytes
            .saturating_sub(
                source_daemon_chunk_metrics_before.contiguous_body_materialization_copy_bytes,
            );
    let chunk_allocations =
        client_chunk_allocations.saturating_add(source_daemon_chunk_allocations);
    let chunk_reuses = client_chunk_reuses.saturating_add(source_daemon_chunk_reuses);
    let chunk_deallocations =
        client_chunk_deallocations.saturating_add(source_daemon_chunk_deallocations);
    let payload_copy_bytes =
        client_payload_copy_bytes.saturating_add(source_daemon_payload_copy_bytes);
    let shared_source_normalization_copy_bytes = client_shared_source_normalization_copy_bytes
        .saturating_add(source_daemon_shared_source_normalization_copy_bytes);
    let byte_stream_write_adapter_copy_bytes = client_byte_stream_write_adapter_copy_bytes
        .saturating_add(source_daemon_byte_stream_write_adapter_copy_bytes);
    let byte_stream_read_adapter_copy_bytes = client_byte_stream_read_adapter_copy_bytes
        .saturating_add(source_daemon_byte_stream_read_adapter_copy_bytes);
    let contiguous_body_materialization_copy_bytes =
        client_contiguous_body_materialization_copy_bytes
            .saturating_add(source_daemon_contiguous_body_materialization_copy_bytes);
    let chunk_allocations_per_gib = if total_success_payload_bytes == 0 {
        0.0
    } else {
        chunk_allocations as f64 * (1024_f64 * 1024_f64 * 1024_f64)
            / total_success_payload_bytes as f64
    };
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
    ctx.record("chunk_allocations", chunk_allocations);
    ctx.record("chunk_reuses", chunk_reuses);
    ctx.record("chunk_deallocations", chunk_deallocations);
    ctx.record("payload_copy_bytes", payload_copy_bytes);
    ctx.record(
        "shared_source_normalization_copy_bytes",
        shared_source_normalization_copy_bytes,
    );
    ctx.record(
        "byte_stream_write_adapter_copy_bytes",
        byte_stream_write_adapter_copy_bytes,
    );
    ctx.record(
        "byte_stream_read_adapter_copy_bytes",
        byte_stream_read_adapter_copy_bytes,
    );
    ctx.record(
        "contiguous_body_materialization_copy_bytes",
        contiguous_body_materialization_copy_bytes,
    );
    ctx.record("client_chunk_allocations", client_chunk_allocations);
    ctx.record("client_chunk_reuses", client_chunk_reuses);
    ctx.record("client_chunk_deallocations", client_chunk_deallocations);
    ctx.record("client_payload_copy_bytes", client_payload_copy_bytes);
    ctx.record(
        "client_shared_source_normalization_copy_bytes",
        client_shared_source_normalization_copy_bytes,
    );
    ctx.record(
        "client_byte_stream_write_adapter_copy_bytes",
        client_byte_stream_write_adapter_copy_bytes,
    );
    ctx.record(
        "client_byte_stream_read_adapter_copy_bytes",
        client_byte_stream_read_adapter_copy_bytes,
    );
    ctx.record(
        "client_contiguous_body_materialization_copy_bytes",
        client_contiguous_body_materialization_copy_bytes,
    );
    ctx.record(
        "source_daemon_chunk_allocations",
        source_daemon_chunk_allocations,
    );
    ctx.record("source_daemon_chunk_reuses", source_daemon_chunk_reuses);
    ctx.record(
        "source_daemon_chunk_deallocations",
        source_daemon_chunk_deallocations,
    );
    ctx.record(
        "source_daemon_payload_copy_bytes",
        source_daemon_payload_copy_bytes,
    );
    ctx.record(
        "source_daemon_shared_source_normalization_copy_bytes",
        source_daemon_shared_source_normalization_copy_bytes,
    );
    ctx.record(
        "source_daemon_byte_stream_write_adapter_copy_bytes",
        source_daemon_byte_stream_write_adapter_copy_bytes,
    );
    ctx.record(
        "source_daemon_byte_stream_read_adapter_copy_bytes",
        source_daemon_byte_stream_read_adapter_copy_bytes,
    );
    ctx.record(
        "source_daemon_contiguous_body_materialization_copy_bytes",
        source_daemon_contiguous_body_materialization_copy_bytes,
    );
    ctx.record("chunk_allocations_per_gib", chunk_allocations_per_gib);
    ctx.record(
        "pooled_chunk_allocations_per_gib",
        chunk_allocations_per_gib,
    );
    ctx.record(
        "chunk_peak_active_bytes",
        chunk_metrics_after
            .peak_active_bytes
            .max(source_daemon_chunk_metrics_after.peak_active_bytes) as u64,
    );
    ctx.record(
        "chunk_peak_allocated_bytes",
        chunk_metrics_after
            .peak_allocated_bytes
            .max(source_daemon_chunk_metrics_after.peak_allocated_bytes) as u64,
    );
    ctx.record(
        "chunk_retained_bytes",
        chunk_metrics_after
            .retained_bytes
            .saturating_add(source_daemon_chunk_metrics_after.retained_bytes) as u64,
    );
    ctx.record(
        "client_chunk_peak_active_bytes",
        chunk_metrics_after.peak_active_bytes as u64,
    );
    ctx.record(
        "client_chunk_peak_allocated_bytes",
        chunk_metrics_after.peak_allocated_bytes as u64,
    );
    ctx.record(
        "source_daemon_chunk_peak_active_bytes",
        source_daemon_chunk_metrics_after.peak_active_bytes as u64,
    );
    ctx.record(
        "source_daemon_chunk_peak_allocated_bytes",
        source_daemon_chunk_metrics_after.peak_allocated_bytes as u64,
    );
    ctx.record(
        "logical_transfer_bytes_per_success",
        payload_bytes_per_success as u64,
    );
    match defaults.payload_mode {
        NetworkPingPayloadMode::EmbeddedSobj => {
            ctx.record(
                "embedded_sobj_logical_bytes",
                total_success_payload_bytes as u64,
            );
            ctx.record(
                "embedded_sobj_logical_bytes_per_second",
                payload_bytes_per_second,
            );
        }
        NetworkPingPayloadMode::SemanticAttachment => {
            ctx.record(
                "successful_request_attachment_bytes",
                total_success_request_bytes as u64,
            );
            ctx.record(
                "request_attachment_bytes_per_second",
                request_bytes_per_second,
            );
            ctx.record(
                "successful_response_attachment_bytes",
                total_success_response_bytes as u64,
            );
            ctx.record(
                "response_attachment_bytes_per_second",
                response_bytes_per_second,
            );
            ctx.record(
                "semantic_attachment_duplex_logical_bytes",
                total_success_payload_bytes as u64,
            );
            ctx.record(
                "semantic_attachment_duplex_logical_bytes_per_second",
                payload_bytes_per_second,
            );
        }
    }
    ctx.record("latency_min_ms", min_ms);
    ctx.record("latency_p50_ms", p50_ms);
    ctx.record("latency_p95_ms", p95_ms);
    ctx.record("latency_max_ms", max_ms);
    ctx.record("latency_min_us", min_us);
    ctx.record("latency_p50_us", p50_us);
    ctx.record("latency_p95_us", p95_us);
    ctx.record("latency_max_us", max_us);
    ctx.record("proxy_round_trip_latency_p50_us", proxy_round_trip_p50_us);
    ctx.record("proxy_round_trip_latency_p95_us", proxy_round_trip_p95_us);
    ctx.record("successes_by_target", successes_by_target);
    ctx.record("successes_by_route_class", successes_by_route_class);
    ctx.record("successes_by_relay_hops", successes_by_relay_hops);
    ctx.record(
        "successful_payload_bytes_by_route_class",
        successful_payload_bytes_by_route_class,
    );
    ctx.record(
        "payload_bytes_per_second_by_route_class",
        payload_bytes_per_second_by_route_class,
    );
    if let Some(process_peak_resident_bytes) = process_peak_resident_bytes() {
        ctx.record("process_peak_resident_bytes", process_peak_resident_bytes);
    }
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
            "network ping {} aggregate duplex logical throughput {payload_bytes_per_second:.2} bytes/s is below required {min_payload_bytes_per_second:.2} bytes/s",
            defaults.payload_mode.as_str()
        )));
    }

    let throughput_summary = match defaults.payload_mode {
        NetworkPingPayloadMode::EmbeddedSobj => {
            format!("{payload_bytes_per_second:.2} embedded-SOBJ logical bytes/s")
        }
        NetworkPingPayloadMode::SemanticAttachment => format!(
            "{payload_bytes_per_second:.2} aggregate duplex logical bytes/s (request {request_bytes_per_second:.2}, response {response_bytes_per_second:.2})"
        ),
    };
    Ok(SinomoTestOutcome::passed(format!(
        "network ping completed {success_count}/{total_requests} {} RPCs in {}ms ({requests_per_second:.2} req/s, {throughput_summary}, payload-copy-bytes={payload_copy_bytes}, chunk-allocations/GiB={chunk_allocations_per_gib:.2}, p50={p50_ms}ms/{p50_us}us p95={p95_ms}ms/{p95_us}us)",
        defaults.payload_mode.as_str(),
        elapsed.as_millis()
    )))
}

fn run_network_endpoint_error_fallback(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let target_name = ctx.required_parameter("target-name")?.trim().to_string();
    let bad_endpoint_address = ctx
        .required_parameter("bad-endpoint-address")?
        .trim()
        .to_string();
    if target_name.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "target-name".to_string(),
            detail: "must not be empty".to_string(),
        });
    }
    if bad_endpoint_address.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "bad-endpoint-address".to_string(),
            detail: "must not be empty".to_string(),
        });
    }
    let timeout_ms = optional_u64_parameter(ctx, "timeout-ms", DEFAULT_TIMEOUT_MS)?;

    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before endpoint fallback probe: {error}"
        ))
    })?;
    let cluster_state = cluster::read_cluster_state(&cluster_paths)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let target = cluster_state
        .members
        .iter()
        .find(|member| {
            member.display_name == target_name
                && member.state.is_authorized()
                && member_has_main_daemon(member)
        })
        .ok_or_else(|| {
            SinomoTestError::failed(format!(
                "cannot find authorized main daemon target named {target_name}"
            ))
        })?;
    let bad_endpoint = target
        .endpoints
        .iter()
        .find(|endpoint| endpoint.address == bad_endpoint_address)
        .cloned()
        .ok_or_else(|| {
            let addresses = target
                .endpoints
                .iter()
                .map(|endpoint| endpoint.address.clone())
                .collect::<Vec<_>>();
            SinomoTestError::failed(format!(
                "target {target_name} does not advertise bad endpoint {bad_endpoint_address}; addresses={addresses:?}"
            ))
        })?;

    let original_connectivity = cluster_connectivity_records_for_source_with_bootstrap_fallback(
        &cluster_paths,
        &cluster_state.local_node_id,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot snapshot KV connectivity before endpoint fallback probe: {error}"
        ))
    })?;
    let seed_records = endpoint_error_fallback_seed_records(
        &cluster_state,
        target,
        &bad_endpoint,
        original_connectivity.clone(),
    );
    sync_connectivity_fixture_to_quorum(
        &cluster_paths,
        &cluster_state.local_node_id,
        &target.node_id,
        seed_records,
        "seed deterministic bad endpoint preference",
    )?;

    let probe_result =
        run_network_endpoint_error_fallback_probe(target, &bad_endpoint_address, timeout_ms);

    let restore_result = sync_connectivity_fixture_to_quorum(
        &cluster_paths,
        &cluster_state.local_node_id,
        &target.node_id,
        original_connectivity.clone(),
        "restore connectivity after endpoint fallback probe",
    );
    if let Err(error) = restore_result {
        return Err(error);
    }
    let probe_outcome = probe_result?;
    if !matches!(
        probe_outcome.status,
        sinomo_runtime::test::SinomoTestStatus::Passed
    ) {
        return Ok(probe_outcome);
    }

    let now = OffsetDateTime::now_utc();
    let mut identity_mismatch = network::cluster_connectivity_record(
        &cluster_state,
        &bad_endpoint,
        network::ClusterConnectivityState::Unreachable,
        None,
        network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
        None,
        Some(now.to_string()),
        now,
        network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
    );
    identity_mismatch.failure_reason =
        Some(network::ClusterConnectivityFailureReason::IdentityMismatch);
    let mut final_connectivity = original_connectivity;
    final_connectivity.retain(|record| record.endpoint_id != bad_endpoint.endpoint_id);
    final_connectivity.push(identity_mismatch);
    sync_connectivity_fixture_to_quorum(
        &cluster_paths,
        &cluster_state.local_node_id,
        &target.node_id,
        final_connectivity,
        "mark bad endpoint as identity-mismatch after fallback probe",
    )?;

    ctx.record("target_name", target_name);
    ctx.record("target_node_id", target.node_id.to_string());
    ctx.record("bad_endpoint_address", bad_endpoint_address);
    ctx.record(
        "seeded_bad_endpoint_id",
        bad_endpoint.endpoint_id.to_string(),
    );
    Ok(SinomoTestOutcome::passed(
        "routed RPC retried the auth-error endpoint and reached the requested target",
    ))
}

fn cluster_connectivity_records_for_source_with_bootstrap_fallback(
    cluster_paths: &ClusterPaths,
    source_node_id: &NodeId,
) -> Result<Vec<network::ClusterConnectivityRecord>, network::NetworkError> {
    let records = cluster::read_cluster_connectivity_graph_from_kv(cluster_paths)?
        .into_iter()
        .filter(|record| record.source_node_id == *source_node_id)
        .collect::<Vec<_>>();
    if records.is_empty() {
        // When the source has not yet published into KV, use the local
        // bootstrap cache to construct the complete source-owned set that this
        // fixture will commit to quorum.
        return network::read_local_cluster_connectivity_records_for_source(
            cluster_paths,
            source_node_id,
        );
    }
    Ok(records)
}

fn sync_connectivity_fixture_to_quorum(
    cluster_paths: &ClusterPaths,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    records: Vec<network::ClusterConnectivityRecord>,
    action: &str,
) -> Result<(), SinomoTestError> {
    // Fault-injection probes snapshot an existing source set, publish a newer
    // synthetic observation, and then restore the snapshot. Production
    // connectivity merging intentionally rejects observations older than the
    // current record, so an unmodified snapshot cannot undo the fixture. Make
    // the requested fixture set a fresh observation without changing its
    // reachability semantics.
    let records = target_scoped_connectivity_fixture_records(
        cluster_paths,
        source_node_id,
        target_node_id,
        records,
    )
    .map_err(|error| SinomoTestError::failed(format!("cannot prepare to {action}: {error}")))?;
    let mut driver = super::cli_cluster_control_plane_driver(cluster_paths);
    let snapshot = cluster::sync_cluster_connectivity_records_to_quorum_with_host(
        cluster_paths,
        source_node_id,
        records,
        &mut driver,
    )
    .map_err(|error| SinomoTestError::failed(format!("cannot {action}: {error}")))?;
    if let Some(snapshot) = snapshot {
        super::write_imported_cluster_kv_snapshot(cluster_paths, &snapshot)
            .map_err(|error| SinomoTestError::failed(format!("cannot cache {action}: {error}")))?;
    }
    Ok(())
}

fn refresh_connectivity_fixture_records(
    mut records: Vec<network::ClusterConnectivityRecord>,
    target_node_id: &NodeId,
) -> Vec<network::ClusterConnectivityRecord> {
    let now = OffsetDateTime::now_utc();
    for record in records
        .iter_mut()
        .filter(|record| record.target_node_id == *target_node_id)
    {
        let ttl_seconds = if matches!(record.state, network::ClusterConnectivityState::Unreachable)
        {
            network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS
        } else {
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS
        };
        record.observed_at = now;
        record.expires_at = now + time::Duration::seconds(ttl_seconds);
    }
    records
}

fn target_scoped_connectivity_fixture_records(
    cluster_paths: &ClusterPaths,
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    desired_source_records: Vec<network::ClusterConnectivityRecord>,
) -> Result<Vec<network::ClusterConnectivityRecord>, network::NetworkError> {
    let mut records =
        network::read_local_cluster_connectivity_records_for_source(cluster_paths, source_node_id)?;
    records.retain(|record| record.target_node_id != *target_node_id);
    records.extend(desired_source_records.into_iter().filter(|record| {
        record.source_node_id == *source_node_id && record.target_node_id == *target_node_id
    }));
    Ok(refresh_connectivity_fixture_records(
        records,
        target_node_id,
    ))
}

fn reload_local_daemon_connectivity_for_fixture(action: &str) -> Result<(), SinomoTestError> {
    let daemon_client = DaemonApiClient::connect().map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot connect to local daemon to {action}: {error}"
        ))
    })?;
    daemon_client.reload_local_connectivity().map_err(|error| {
        SinomoTestError::failed(format!("cannot ask local daemon to {action}: {error}"))
    })?;
    Ok(())
}

fn run_network_stale_direct_timeout_demotion(ctx: &mut SinomoTestContext) -> SinomoTestResult {
    let target_name = ctx.required_parameter("target-name")?.trim().to_string();
    let stall_endpoint_address = ctx
        .required_parameter("stall-endpoint-address")?
        .trim()
        .to_string();
    if target_name.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "target-name".to_string(),
            detail: "must not be empty".to_string(),
        });
    }
    if stall_endpoint_address.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "stall-endpoint-address".to_string(),
            detail: "must not be empty".to_string(),
        });
    }
    let timeout_ms = optional_u64_parameter(ctx, "timeout-ms", DEFAULT_TIMEOUT_MS)?;
    let followup_max_ms = optional_u64_parameter(ctx, "followup-max-ms", 1_000)?;

    let data_dir = SinomoDataDir::new(ctx.data_dir());
    let cluster_paths = ClusterPaths::new(&data_dir);
    super::refresh_cluster_kv_from_local_daemon(&cluster_paths).map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot refresh cluster snapshot before stale direct demotion probe: {error}"
        ))
    })?;
    let cluster_state = cluster::read_cluster_state(&cluster_paths)
        .map_err(|error| SinomoTestError::failed(error.to_string()))?;
    let target = cluster_state
        .members
        .iter()
        .find(|member| {
            member.display_name == target_name
                && member.state.is_authorized()
                && member_has_main_daemon(member)
        })
        .or_else(|| {
            cluster_state.members.iter().find(|member| {
                member.state.is_authorized()
                    && member_has_main_daemon(member)
                    && member
                        .endpoints
                        .iter()
                        .any(|endpoint| endpoint.address == stall_endpoint_address)
            })
        })
        .ok_or_else(|| {
            SinomoTestError::failed(format!(
                "cannot find authorized main daemon target named {target_name} or advertising stale direct endpoint {stall_endpoint_address}"
            ))
        })?;
    let stall_endpoint = target
        .endpoints
        .iter()
        .find(|endpoint| endpoint.address == stall_endpoint_address)
        .cloned()
        .ok_or_else(|| {
            let addresses = target
                .endpoints
                .iter()
                .map(|endpoint| endpoint.address.clone())
                .collect::<Vec<_>>();
            SinomoTestError::failed(format!(
                "target {target_name} does not advertise stale direct endpoint {stall_endpoint_address}; addresses={addresses:?}"
            ))
        })?;

    let original_connectivity = cluster_connectivity_records_for_source_with_bootstrap_fallback(
        &cluster_paths,
        &cluster_state.local_node_id,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot snapshot KV connectivity before stale direct demotion probe: {error}"
        ))
    })?;
    let seed_records = stale_direct_timeout_seed_records(
        &cluster_state,
        target,
        &stall_endpoint,
        original_connectivity.clone(),
    );
    let seed_records = target_scoped_connectivity_fixture_records(
        &cluster_paths,
        &cluster_state.local_node_id,
        &target.node_id,
        seed_records,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot prepare stale direct endpoint preference: {error}"
        ))
    })?;
    let probe_result = (|| -> Result<(SinomoTestOutcome, Duration, Duration), SinomoTestError> {
        // This fixture changes the route used to reach the quorum authority.
        // Keep it out of quorum: publishing it first would make that transaction
        // route through the intentionally stalled endpoint and become
        // outcome-unknown before the actual demotion probe starts. Explicitly
        // reload the daemon's process cache after the shared file update.
        network::write_local_cluster_connectivity_records(
            &cluster_paths,
            &cluster_state.local_node_id,
            &seed_records,
        )
        .map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot seed stale direct endpoint preference: {error}"
            ))
        })?;
        reload_local_daemon_connectivity_for_fixture("reload stale direct endpoint preference")?;

        let (first_outcome, first_elapsed) =
            run_network_target_ping_timed(target, timeout_ms, "stale-direct-timeout-demotion")?;
        if !matches!(
            first_outcome.status,
            sinomo_runtime::test::SinomoTestStatus::Passed
        ) {
            return Ok((first_outcome, first_elapsed, Duration::ZERO));
        }
        let (followup_outcome, followup_elapsed) =
            run_network_target_ping_timed(target, timeout_ms, "stale-direct-followup")?;
        if !matches!(
            followup_outcome.status,
            sinomo_runtime::test::SinomoTestStatus::Passed
        ) {
            return Ok((followup_outcome, first_elapsed, followup_elapsed));
        }
        Ok((followup_outcome, first_elapsed, followup_elapsed))
    })();

    let restored_connectivity = target_scoped_connectivity_fixture_records(
        &cluster_paths,
        &cluster_state.local_node_id,
        &target.node_id,
        original_connectivity,
    )
    .map_err(|error| {
        SinomoTestError::failed(format!(
            "cannot prepare connectivity restore after stale direct demotion probe: {error}"
        ))
    })?;
    let restore_result = (|| -> Result<(), SinomoTestError> {
        network::write_local_cluster_connectivity_record_set(
            &cluster_paths,
            &cluster_state.local_node_id,
            &restored_connectivity,
        )
        .map_err(|error| {
            SinomoTestError::failed(format!(
                "cannot restore connectivity after stale direct demotion probe: {error}"
            ))
        })?;
        reload_local_daemon_connectivity_for_fixture(
            "reload connectivity restored after stale direct demotion probe",
        )
    })();
    restore_result?;

    let (probe_outcome, first_elapsed, followup_elapsed) = probe_result?;
    if !matches!(
        probe_outcome.status,
        sinomo_runtime::test::SinomoTestStatus::Passed
    ) {
        return Ok(probe_outcome);
    }
    if followup_elapsed > Duration::from_millis(followup_max_ms) {
        return Ok(SinomoTestOutcome::failed(format!(
            "follow-up routed RPC took {}ms after stale endpoint demotion, above allowed {followup_max_ms}ms",
            followup_elapsed.as_millis()
        )));
    }

    ctx.record("target_name", target_name);
    ctx.record("target_node_id", target.node_id.to_string());
    ctx.record("stall_endpoint_address", stall_endpoint_address);
    ctx.record(
        "seeded_stall_endpoint_id",
        stall_endpoint.endpoint_id.to_string(),
    );
    ctx.record("first_rpc_ms", first_elapsed.as_millis().to_string());
    ctx.record("followup_rpc_ms", followup_elapsed.as_millis().to_string());
    Ok(SinomoTestOutcome::passed(format!(
        "stale direct endpoint timed out once in {}ms and follow-up routed RPC completed in {}ms",
        first_elapsed.as_millis(),
        followup_elapsed.as_millis()
    )))
}

fn endpoint_error_fallback_seed_records(
    cluster_state: &ClusterBootstrapState,
    target: &ClusterMemberRecord,
    bad_endpoint: &sinomo_runtime::core::ClusterEndpointRecord,
    original_connectivity: Vec<network::ClusterConnectivityRecord>,
) -> Vec<network::ClusterConnectivityRecord> {
    let now = OffsetDateTime::now_utc();
    let mut records = original_connectivity;
    let mut covered_endpoint_ids = records
        .iter()
        .filter(|record| {
            record.source_node_id == cluster_state.local_node_id
                && record.target_node_id == target.node_id
        })
        .map(|record| record.endpoint_id.clone())
        .collect::<BTreeSet<_>>();
    for record in &mut records {
        if record.source_node_id == cluster_state.local_node_id
            && record.target_node_id == target.node_id
            && record.endpoint_id != bad_endpoint.endpoint_id
        {
            record.state = network::ClusterConnectivityState::Degraded;
            record.latency_ms = None;
            record.consecutive_failures =
                network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD.saturating_sub(1);
            record.last_failure_at = Some(now.to_string());
            record.failure_reason = None;
            record.observed_at = now;
            record.expires_at =
                now + time::Duration::seconds(network::CLUSTER_CONNECTIVITY_TTL_SECONDS as i64);
        }
    }
    for endpoint in &target.endpoints {
        if endpoint.endpoint_id == bad_endpoint.endpoint_id
            || covered_endpoint_ids.contains(&endpoint.endpoint_id)
        {
            continue;
        }
        let degraded = network::cluster_connectivity_record(
            cluster_state,
            endpoint,
            network::ClusterConnectivityState::Degraded,
            None,
            network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD.saturating_sub(1),
            None,
            Some(now.to_string()),
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        records.push(degraded);
        covered_endpoint_ids.insert(endpoint.endpoint_id.clone());
    }
    let bad_preference = network::cluster_connectivity_record(
        cluster_state,
        bad_endpoint,
        network::ClusterConnectivityState::Healthy,
        Some(0),
        0,
        Some(now.to_string()),
        None,
        now,
        network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
    );
    records.retain(|record| {
        !(record.source_node_id == cluster_state.local_node_id
            && record.target_node_id == target.node_id
            && record.endpoint_id == bad_endpoint.endpoint_id)
    });
    records.push(bad_preference);
    records
}

fn stale_direct_timeout_seed_records(
    cluster_state: &ClusterBootstrapState,
    target: &ClusterMemberRecord,
    stall_endpoint: &sinomo_runtime::core::ClusterEndpointRecord,
    original_connectivity: Vec<network::ClusterConnectivityRecord>,
) -> Vec<network::ClusterConnectivityRecord> {
    let mut records = endpoint_error_fallback_seed_records(
        cluster_state,
        target,
        stall_endpoint,
        original_connectivity,
    );
    let now = OffsetDateTime::now_utc();
    for record in records.iter_mut().filter(|record| {
        record.source_node_id == cluster_state.local_node_id
            && record.target_node_id == target.node_id
            && record.endpoint_id != stall_endpoint.endpoint_id
    }) {
        // Keep real fallbacks Healthy so the stall's first command timeout
        // moves it behind them. A deliberately high observed latency makes the
        // zero-latency stall deterministic for the first attempt without
        // poisoning the alternatives as Degraded fixture residue.
        record.state = network::ClusterConnectivityState::Healthy;
        record.latency_ms = Some(60_000);
        record.consecutive_failures = 0;
        record.last_success_at = Some(now.to_string());
        record.last_failure_at = None;
        record.failure_reason = None;
        record.observed_at = now;
        record.expires_at =
            now + time::Duration::seconds(network::CLUSTER_CONNECTIVITY_TTL_SECONDS);
    }
    records
}

fn run_network_target_ping_timed(
    target: &ClusterMemberRecord,
    timeout_ms: u64,
    nonce_prefix: &str,
) -> Result<(SinomoTestOutcome, Duration), SinomoTestError> {
    let nonce = format!("{nonce_prefix}-{}-{}", current_millis(), std::process::id());
    let request = cluster::cluster_network_ping_rpc_request_with_response_payload(&nonce, 0, 0)
        .map_err(|source| {
            SinomoTestError::failed(format!("cannot prepare routed ping request: {source}"))
        })?;
    let daemon_client = DaemonApiClient::connect()
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot connect to local daemon for routed ping probe: {source}"
            ))
        })?
        .with_timeout(Duration::from_millis(timeout_ms).saturating_add(Duration::from_secs(1)));
    let started = Instant::now();
    let response = daemon_client
        .object_rpc_to_target(
            &target.node_id,
            &request,
            Duration::from_millis(timeout_ms),
            true,
        )
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "routed ping RPC to {} failed: {source}",
                target.display_name
            ))
        })?;
    let elapsed = started.elapsed();
    let response = cluster::decode_cluster_network_ping_rpc_response(&response)
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    if response.nonce != nonce {
        return Ok((
            SinomoTestOutcome::failed(format!(
                "nonce mismatch: got {}, expected {}",
                response.nonce, nonce
            )),
            elapsed,
        ));
    }
    if response.responder_node_id != target.node_id {
        return Ok((
            SinomoTestOutcome::failed(format!(
                "responder mismatch: got {}, expected {}",
                response.responder_node_id, target.node_id
            )),
            elapsed,
        ));
    }
    Ok((SinomoTestOutcome::passed(""), elapsed))
}

fn run_network_endpoint_error_fallback_probe(
    target: &ClusterMemberRecord,
    bad_endpoint_address: &str,
    timeout_ms: u64,
) -> SinomoTestResult {
    let nonce = format!(
        "endpoint-error-fallback-{}-{}",
        current_millis(),
        std::process::id()
    );
    let request = cluster::cluster_network_ping_rpc_request_with_response_payload(&nonce, 0, 0)
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot prepare endpoint fallback ping request: {source}"
            ))
        })?;
    let daemon_client = DaemonApiClient::connect()
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "cannot connect to local daemon for endpoint fallback probe: {source}"
            ))
        })?
        .with_timeout(Duration::from_millis(timeout_ms).saturating_add(Duration::from_secs(1)));
    let response = daemon_client
        .object_rpc_to_target(
            &target.node_id,
            &request,
            Duration::from_millis(timeout_ms),
            true,
        )
        .map_err(|source| {
            SinomoTestError::failed(format!(
                "endpoint fallback routed RPC failed after seeding {bad_endpoint_address}: {source}"
            ))
        })?;
    let response = cluster::decode_cluster_network_ping_rpc_response(&response)
        .map_err(|source| SinomoTestError::failed(source.to_string()))?;
    if response.nonce != nonce {
        return Ok(SinomoTestOutcome::failed(format!(
            "nonce mismatch: got {}, expected {}",
            response.nonce, nonce
        )));
    }
    if response.responder_node_id != target.node_id {
        return Ok(SinomoTestOutcome::failed(format!(
            "responder mismatch: got {}, expected {}",
            response.responder_node_id, target.node_id
        )));
    }
    Ok(SinomoTestOutcome::passed(""))
}

#[cfg(test)]
fn network_ping_targets_from_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<NodeId>, SinomoTestError> {
    let targets = network_test_member_targets_from_state(state)?;
    Ok(targets
        .into_iter()
        .filter(|target| network_ping_target_is_available_for_probe(paths, state, target))
        .collect())
}

#[cfg(test)]
fn network_ping_target_is_available_for_probe(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target: &NodeId,
) -> bool {
    state
        .members
        .iter()
        .find(|member| member.node_id == *target)
        .is_none_or(|member| {
            match network::cluster_member_main_daemon_connectivity(paths, state, member) {
                network::ClusterMemberMainDaemonConnectivity::Healthy
                | network::ClusterMemberMainDaemonConnectivity::Unknown => true,
                network::ClusterMemberMainDaemonConnectivity::Degraded
                | network::ClusterMemberMainDaemonConnectivity::Unreachable => {
                    network_target_has_healthy_route(paths, state, target)
                }
            }
        })
}

#[cfg(test)]
fn network_target_has_healthy_route(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    target: &NodeId,
) -> bool {
    let graph = network::ClusterConnectivityGraph::read_for_local(paths, state);
    network::cluster_healthy_route_next_hop(&graph, &state.local_node_id, target).is_some()
}

fn network_test_member_targets_from_state(
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

fn filter_network_test_targets(
    ctx: &SinomoTestContext,
    state: &ClusterBootstrapState,
    targets: Vec<NodeId>,
) -> Result<Vec<NodeId>, SinomoTestError> {
    if ctx.parameter("target-node-id").is_some() && ctx.parameter("target-name").is_some() {
        return Err(SinomoTestError::InvalidParameter {
            name: "target-node-id".to_string(),
            detail: "cannot be combined with target-name".to_string(),
        });
    }

    let Some(target_node_id) = network_target_filter_node_id(ctx, state)? else {
        return Ok(targets);
    };
    let filtered = targets
        .into_iter()
        .filter(|target| target == &target_node_id)
        .collect::<Vec<_>>();
    if filtered.is_empty() {
        return Err(SinomoTestError::InvalidParameter {
            name: "target-node-id".to_string(),
            detail: format!("no network test target matched {target_node_id}"),
        });
    }
    Ok(filtered)
}

fn network_target_filter_node_id(
    ctx: &SinomoTestContext,
    state: &ClusterBootstrapState,
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

fn member_has_main_daemon(member: &ClusterMemberRecord) -> bool {
    member
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == sinomo_runtime::node::DAEMON_MAIN_RUN_CAPABILITY)
}

#[derive(Clone, Debug)]
struct NetworkHygieneOptions {
    require_active_routes: bool,
    active_route_targets: Option<BTreeSet<NodeId>>,
}

impl NetworkHygieneOptions {
    fn unscoped(require_active_routes: bool) -> Self {
        Self {
            require_active_routes,
            active_route_targets: None,
        }
    }

    fn for_targets(require_active_routes: bool, targets: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            require_active_routes,
            active_route_targets: Some(targets.into_iter().collect()),
        }
    }
}

#[derive(Clone, Debug)]
struct NetworkHygieneEndpointInfo {
    node_id: NodeId,
    transport: ClusterTransportKind,
    address: String,
    privacy: ClusterEndpointPrivacy,
}

fn network_hygiene_findings(
    state: &ClusterBootstrapState,
    local_records: &[network::ClusterConnectivityRecord],
    kv_records: &[network::ClusterConnectivityRecord],
    active_records: &[network::ClusterConnectivityRecord],
    options: NetworkHygieneOptions,
) -> Vec<String> {
    let mut findings = Vec::new();
    let mut endpoints = BTreeMap::<network::ClusterEndpointId, NetworkHygieneEndpointInfo>::new();
    let mut members = BTreeMap::<NodeId, &ClusterMemberRecord>::new();
    for member in &state.members {
        members.insert(member.node_id.clone(), member);
        collect_member_endpoint_hygiene_findings(state, member, &mut endpoints, &mut findings);
    }
    collect_connectivity_record_hygiene_findings(
        "local",
        state,
        &members,
        &endpoints,
        local_records,
        &mut findings,
    );
    collect_connectivity_record_hygiene_findings(
        "kv",
        state,
        &members,
        &endpoints,
        kv_records,
        &mut findings,
    );
    if options.require_active_routes {
        collect_active_route_hygiene_findings(
            state,
            active_records,
            options.active_route_targets.as_ref(),
            &mut findings,
        );
    }
    findings.sort();
    findings.dedup();
    findings
}

fn collect_member_endpoint_hygiene_findings(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    endpoints: &mut BTreeMap<network::ClusterEndpointId, NetworkHygieneEndpointInfo>,
    findings: &mut Vec<String>,
) {
    if !member.state.is_authorized() {
        return;
    }
    let remote = member.node_id != state.local_node_id;
    let mut member_endpoint_ids = BTreeSet::new();
    for endpoint in &member.endpoints {
        if endpoint.node_id != member.node_id {
            findings.push(format!(
                "member {} contains endpoint {} for different node {}",
                member.node_id, endpoint.endpoint_id, endpoint.node_id
            ));
        }
        if !member_endpoint_ids.insert(endpoint.endpoint_id.clone()) {
            findings.push(format!(
                "member {} advertises duplicate endpoint id {}",
                member.node_id, endpoint.endpoint_id
            ));
        }
        if endpoint.address.trim().is_empty() {
            findings.push(format!(
                "member {} endpoint {} has empty address",
                member.node_id, endpoint.endpoint_id
            ));
        }
        if remote && endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
            findings.push(format!(
                "remote member {} advertises local-only endpoint {} at {}",
                member.node_id, endpoint.endpoint_id, endpoint.address
            ));
        }
        if remote
            && matches!(
                endpoint.transport,
                ClusterTransportKind::InProcess | ClusterTransportKind::Loopback
            )
        {
            findings.push(format!(
                "remote member {} advertises non-peer transport {:?} endpoint {} at {}",
                member.node_id, endpoint.transport, endpoint.endpoint_id, endpoint.address
            ));
        }
        if endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
            && endpoint_address_is_peer_undialable(&endpoint.address)
        {
            findings.push(format!(
                "member {} advertises peer-visible endpoint {} at undialable address {}",
                member.node_id, endpoint.endpoint_id, endpoint.address
            ));
        }
        if let Some(existing) = endpoints.insert(
            endpoint.endpoint_id.clone(),
            NetworkHygieneEndpointInfo::from_endpoint(endpoint),
        ) && existing.node_id != endpoint.node_id
        {
            findings.push(format!(
                "endpoint id {} is advertised by both {} and {}",
                endpoint.endpoint_id, existing.node_id, endpoint.node_id
            ));
        }
    }
}

fn collect_connectivity_record_hygiene_findings(
    scope: &str,
    state: &ClusterBootstrapState,
    members: &BTreeMap<NodeId, &ClusterMemberRecord>,
    endpoints: &BTreeMap<network::ClusterEndpointId, NetworkHygieneEndpointInfo>,
    records: &[network::ClusterConnectivityRecord],
    findings: &mut Vec<String>,
) {
    for record in records {
        if record.cluster_id != state.cluster_id {
            findings.push(format!(
                "{scope} connectivity record {} -> {} uses foreign cluster {}",
                record.source_node_id, record.target_node_id, record.cluster_id
            ));
        }
        if !members
            .get(&record.source_node_id)
            .is_some_and(|member| member.state.is_authorized())
        {
            findings.push(format!(
                "{scope} connectivity record source {} is not an authorized member",
                record.source_node_id
            ));
        }
        if !members
            .get(&record.target_node_id)
            .is_some_and(|member| member.state.is_authorized())
        {
            findings.push(format!(
                "{scope} connectivity record target {} is not an authorized member",
                record.target_node_id
            ));
        }
        let Some(endpoint) = endpoints.get(&record.endpoint_id) else {
            findings.push(format!(
                "{scope} connectivity record {} -> {} references unadvertised endpoint {} at {}",
                record.source_node_id, record.target_node_id, record.endpoint_id, record.address
            ));
            continue;
        };
        if endpoint.node_id != record.target_node_id {
            findings.push(format!(
                "{scope} connectivity record {} -> {} references endpoint {} owned by {}",
                record.source_node_id, record.target_node_id, record.endpoint_id, endpoint.node_id
            ));
        }
        if endpoint.transport != record.transport {
            findings.push(format!(
                "{scope} connectivity record {} -> {} endpoint {} transport {:?} != advertised {:?}",
                record.source_node_id,
                record.target_node_id,
                record.endpoint_id,
                record.transport,
                endpoint.transport
            ));
        }
        if endpoint.address != record.address {
            findings.push(format!(
                "{scope} connectivity record {} -> {} endpoint {} address {} != advertised {}",
                record.source_node_id,
                record.target_node_id,
                record.endpoint_id,
                record.address,
                endpoint.address
            ));
        }
        if record.source_node_id != record.target_node_id
            && record.state != network::ClusterConnectivityState::Unreachable
            && endpoint.privacy == ClusterEndpointPrivacy::LocalOnly
        {
            findings.push(format!(
                "{scope} connectivity record {} -> {} marks local-only endpoint {} reachable",
                record.source_node_id, record.target_node_id, record.endpoint_id
            ));
        }
    }
}

fn collect_active_route_hygiene_findings(
    state: &ClusterBootstrapState,
    active_records: &[network::ClusterConnectivityRecord],
    active_route_targets: Option<&BTreeSet<NodeId>>,
    findings: &mut Vec<String>,
) {
    let graph = network::ClusterConnectivityGraph::from_records(
        state.local_node_id.clone(),
        active_records.to_vec(),
    );
    let targets = match active_route_targets {
        Some(targets) => targets.iter().cloned().collect::<Vec<_>>(),
        None => network_test_member_targets_from_state(state).unwrap_or_default(),
    };
    for target in targets {
        if network::cluster_route_next_hop(&graph, &state.local_node_id, &target).is_none() {
            findings.push(format!(
                "active connectivity graph has no route from {} to remote main {}",
                state.local_node_id, target
            ));
        }
    }
}

impl NetworkHygieneEndpointInfo {
    fn from_endpoint(endpoint: &ClusterEndpointRecord) -> Self {
        Self {
            node_id: endpoint.node_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            privacy: endpoint.privacy.clone(),
        }
    }
}

fn endpoint_address_is_peer_undialable(address: &str) -> bool {
    if endpoint_address_uses_localhost_name(address) {
        return true;
    }
    endpoint_address_socket(address).is_some_and(|socket| {
        socket.ip().is_loopback() || socket.ip().is_unspecified() || socket.ip().is_multicast()
    })
}

fn endpoint_address_socket(address: &str) -> Option<SocketAddr> {
    let value = normalized_tcp_endpoint_address(address);
    value.parse::<SocketAddr>().ok()
}

fn endpoint_address_uses_localhost_name(address: &str) -> bool {
    let value = normalized_tcp_endpoint_address(address)
        .trim()
        .to_ascii_lowercase();
    value == "localhost" || value.starts_with("localhost:") || value.starts_with("localhost/")
}

fn normalized_tcp_endpoint_address(address: &str) -> &str {
    let value = address.trim();
    if let Some(rest) = value
        .strip_prefix("tcp://")
        .or_else(|| value.strip_prefix("lan-tcp://"))
    {
        return rest;
    }
    value
}

#[derive(Debug)]
struct NetworkPingJob {
    payload_mode: NetworkPingPayloadMode,
    target_node_id: NodeId,
    route_class: NetworkPingRouteClass,
    relay_hops: Option<usize>,
    nonce: String,
    timeout: Duration,
    control_timeout: Duration,
    request_payload_bytes: usize,
    response_payload_bytes: usize,
    call: network::ObjectRpcCall,
    daemon_address: String,
}

#[derive(Debug)]
struct NetworkPingResult {
    target_node_id: NodeId,
    route_class: NetworkPingRouteClass,
    relay_hops: Option<usize>,
    latency_ms: u64,
    latency_us: u64,
    proxy_round_trip_latency_us: u64,
    outcome: NetworkPingOutcome,
}

#[derive(Debug)]
enum NetworkPingOutcome {
    Success { forward_elapsed: Option<Duration> },
    Failure(String),
}

async fn run_network_ping_job(job: NetworkPingJob) -> NetworkPingResult {
    let started = Instant::now();
    let timeout = job.timeout;
    let target_node_id = job.target_node_id.clone();
    let route_class = job.route_class;
    let relay_hops = job.relay_hops;
    let outcome =
        run_network_ping_outcome_with_deadline(timeout, run_network_ping_job_inner(job)).await;
    let wall_elapsed = started.elapsed();
    // The direct and one-relay hardgates constrain the daemon's routed RPC,
    // not the additional test-client-to-daemon proxy RPC. The full outer call
    // still executes and its response is validated below; only the latency
    // sample uses the daemon-measured inner interval.
    let elapsed = match &outcome {
        NetworkPingOutcome::Success {
            forward_elapsed: Some(elapsed),
        } => *elapsed,
        _ => wall_elapsed,
    };
    NetworkPingResult {
        target_node_id,
        route_class,
        relay_hops,
        latency_ms: elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        latency_us: elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
        proxy_round_trip_latency_us: wall_elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
        outcome,
    }
}

async fn run_network_ping_outcome_with_deadline(
    deadline: Duration,
    outcome: impl std::future::Future<Output = NetworkPingOutcome>,
) -> NetworkPingOutcome {
    match tokio::time::timeout(deadline, outcome).await {
        Ok(outcome) => outcome,
        Err(_) => NetworkPingOutcome::Failure(format!(
            "network ping job exceeded {}ms deadline",
            deadline.as_millis()
        )),
    }
}

async fn run_network_ping_job_inner(job: NetworkPingJob) -> NetworkPingOutcome {
    let NetworkPingJob {
        payload_mode,
        target_node_id,
        route_class: _,
        relay_hops: _,
        nonce,
        timeout,
        control_timeout,
        request_payload_bytes,
        response_payload_bytes,
        call,
        daemon_address,
    } = job;
    let client = DaemonApiClient::at_address(daemon_address).with_timeout(control_timeout);
    let (response, response_body, forward_elapsed) = match payload_mode {
        NetworkPingPayloadMode::EmbeddedSobj => {
            let (result, forward_elapsed) = match client
                .object_rpc_call_to_target_with_forward_elapsed_async(
                    &target_node_id,
                    call,
                    timeout,
                    Some(timeout),
                    true,
                )
                .await
            {
                Ok(response) => response,
                Err(error) => return NetworkPingOutcome::Failure(error.to_string()),
            };
            let response = match result
                .decode_body::<cluster::ClusterNetworkPingResponse>()
                .await
            {
                Ok(response) => response,
                Err(error) => return NetworkPingOutcome::Failure(error.to_string()),
            };
            (response, None, Some(forward_elapsed))
        }
        NetworkPingPayloadMode::SemanticAttachment => {
            let (result, forward_elapsed) = match client
                .object_rpc_call_to_target_with_forward_elapsed_async(
                    &target_node_id,
                    call,
                    timeout,
                    Some(timeout),
                    true,
                )
                .await
            {
                Ok(result) => result,
                Err(error) => return NetworkPingOutcome::Failure(error.to_string()),
            };
            if let Err(error) = result.response.error_detail() {
                return NetworkPingOutcome::Failure(error.to_string());
            }
            let response = match cluster::decode_cluster_network_ping_rpc_response(&result.response)
            {
                Ok(response) => response,
                Err(error) => return NetworkPingOutcome::Failure(error.to_string()),
            };
            (response, Some(result.body), Some(forward_elapsed))
        }
    };
    if response.nonce != nonce {
        return NetworkPingOutcome::Failure(format!(
            "nonce mismatch: got {}, expected {}",
            response.nonce, nonce
        ));
    }
    if response.responder_node_id != target_node_id {
        return NetworkPingOutcome::Failure(format!(
            "responder mismatch: got {}, expected {}",
            response.responder_node_id, target_node_id
        ));
    }
    match payload_mode {
        NetworkPingPayloadMode::EmbeddedSobj => {
            if response.payload_mode != cluster::ClusterNetworkPingPayloadMode::EmbeddedSobj {
                return NetworkPingOutcome::Failure(format!(
                    "response payload mode mismatch: got {:?}, expected embedded SOBJ",
                    response.payload_mode
                ));
            }
            if response.payload_bytes != request_payload_bytes {
                return NetworkPingOutcome::Failure(format!(
                    "payload length mismatch: got {}, expected {}",
                    response.payload_bytes, request_payload_bytes
                ));
            }
            if response.response_payload.len() != response_payload_bytes {
                return NetworkPingOutcome::Failure(format!(
                    "response payload length mismatch: got {}, expected {}",
                    response.response_payload.len(),
                    response_payload_bytes
                ));
            }
        }
        NetworkPingPayloadMode::SemanticAttachment => {
            if response.payload_mode != cluster::ClusterNetworkPingPayloadMode::SemanticAttachment {
                return NetworkPingOutcome::Failure(format!(
                    "response payload mode mismatch: got {:?}, expected semantic attachment",
                    response.payload_mode
                ));
            }
            if response.payload_bytes != 0 || !response.response_payload.is_empty() {
                return NetworkPingOutcome::Failure(
                    "semantic attachment response mixed bytes into the SOBJ head".to_string(),
                );
            }
            let expected_request_bytes = match u64::try_from(request_payload_bytes) {
                Ok(bytes) => bytes,
                Err(_) => {
                    return NetworkPingOutcome::Failure(
                        "request attachment size does not fit u64".to_string(),
                    );
                }
            };
            if response.request_attachment_bytes != expected_request_bytes {
                return NetworkPingOutcome::Failure(format!(
                    "request attachment length mismatch: got {}, expected {}",
                    response.request_attachment_bytes, request_payload_bytes
                ));
            }
            let expected_response_bytes = match u64::try_from(response_payload_bytes) {
                Ok(bytes) => bytes,
                Err(_) => {
                    return NetworkPingOutcome::Failure(
                        "response attachment size does not fit u64".to_string(),
                    );
                }
            };
            if response.response_attachment_bytes != expected_response_bytes {
                return NetworkPingOutcome::Failure(format!(
                    "response attachment head length mismatch: got {}, expected {}",
                    response.response_attachment_bytes, expected_response_bytes
                ));
            }
            let Some(response_body) = response_body else {
                return NetworkPingOutcome::Failure(
                    "semantic attachment response omitted its body stream".to_string(),
                );
            };
            if response_body.total_bytes() != Some(expected_response_bytes) {
                return NetworkPingOutcome::Failure(format!(
                    "response attachment descriptor declares {:?} bytes, expected {}",
                    response_body.total_bytes(),
                    expected_response_bytes
                ));
            }
            let actual_response_bytes = match response_body.discard_owned_chunks().await {
                Ok(bytes) => bytes,
                Err(error) => return NetworkPingOutcome::Failure(error.to_string()),
            };
            if actual_response_bytes != expected_response_bytes {
                return NetworkPingOutcome::Failure(format!(
                    "response attachment delivered {actual_response_bytes} bytes, expected {expected_response_bytes}"
                ));
            }
        }
    }
    NetworkPingOutcome::Success { forward_elapsed }
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
            _ => Err(SinomoTestError::InvalidParameter {
                name: key.to_string(),
                detail: "must be one of true,false,1,0,yes,no,on,off".to_string(),
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

fn require_at_most(name: &str, value: usize, maximum: usize) -> Result<(), SinomoTestError> {
    if value > maximum {
        return Err(SinomoTestError::InvalidParameter {
            name: name.to_string(),
            detail: format!("must be at most {maximum}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::core::{
        CapabilityName, ClusterEndpointSource, ClusterMemberRecord, ClusterMemberState,
        ClusterTransportKind,
    };
    use sinomo_runtime::node::ClusterId;

    #[test]
    fn ping_benchmark_defaults_use_stable_bounded_measurement_window() {
        let defaults = NetworkPingRunDefaults::benchmark();

        assert_eq!(defaults.requests_per_target, 100);
        assert_eq!(defaults.warmup_requests_per_target, Some(20));
        assert_eq!(defaults.concurrency, 8);
        assert_eq!(defaults.min_requests_per_second, 50.0);
    }

    #[test]
    fn route_topology_separates_direct_and_relayed_throughput_samples() {
        let local = NodeId::from_legacy_named("node_network_topology_local").expect("local");
        let relay = NodeId::from_legacy_named("node_network_topology_relay").expect("relay");
        let target = NodeId::from_legacy_named("node_network_topology_target").expect("target");
        let direct = vec![local.clone(), target.clone()];
        let relayed = vec![local, relay, target];

        assert_eq!(
            network_ping_route_topology(Some(&direct)),
            (NetworkPingRouteClass::Direct, Some(0))
        );
        assert_eq!(
            network_ping_route_topology(Some(&relayed)),
            (NetworkPingRouteClass::Relayed, Some(1))
        );
        assert_eq!(
            network_ping_route_topology(None),
            (NetworkPingRouteClass::Unresolved, None)
        );
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn process_peak_resident_memory_is_available_for_cluster_benchmarks() {
        assert!(process_peak_resident_bytes().is_some_and(|bytes| bytes > 0));
    }

    #[test]
    fn timeline_samples_are_bounded_and_spread_across_the_measurement() {
        let selected = (0..5_000)
            .filter(|index| should_sample_network_timeline(*index, 5_000, 128))
            .collect::<Vec<_>>();

        assert_eq!(selected.len(), 128);
        assert_eq!(selected.first(), Some(&0));
        assert!(selected.last().is_some_and(|index| *index > 4_900));
        assert_eq!(
            (0..8)
                .filter(|index| should_sample_network_timeline(*index, 8, 128))
                .count(),
            8
        );
        assert!(!should_sample_network_timeline(0, 5_000, 0));
    }

    #[test]
    fn client_timeline_capacity_conservatively_covers_small_payloads() {
        assert_eq!(benchmark_client_timeline_capacity(1), 32);
        assert_eq!(benchmark_client_timeline_capacity(128), 4096);
    }

    #[test]
    fn arbitrary_message_defaults_use_ordinary_size_neutral_parameters() {
        let defaults = NetworkPingRunDefaults::arbitrary_message();
        let request_parameter = defaults.parameter_name("payload-bytes");
        let response_parameter = defaults.parameter_name("response-payload-bytes");

        assert_eq!(defaults.payload_bytes, ARBITRARY_MESSAGE_PAYLOAD_BYTES);
        assert_eq!(
            defaults.response_payload_bytes,
            ARBITRARY_MESSAGE_PAYLOAD_BYTES
        );
        assert_eq!(defaults.requests_per_target, 1);
        assert_eq!(defaults.warmup_requests_per_target, Some(0));
        assert_eq!(request_parameter, "arbitrary-message-payload-bytes");
        assert_eq!(
            response_parameter,
            "arbitrary-message-response-payload-bytes"
        );
    }

    #[test]
    fn semantic_attachment_defaults_exercise_one_window_per_direction_with_bounded_concurrency() {
        let defaults = NetworkPingRunDefaults::semantic_attachment();

        assert_eq!(
            defaults.payload_mode,
            NetworkPingPayloadMode::SemanticAttachment
        );
        assert_eq!(defaults.payload_bytes, 8 * 1024 * 1024);
        assert_eq!(defaults.response_payload_bytes, 8 * 1024 * 1024);
        assert_eq!(defaults.requests_per_target, 4);
        assert_eq!(defaults.warmup_requests_per_target, Some(1));
        assert_eq!(defaults.concurrency, 4);
        assert_eq!(
            defaults.parameter_name("payload-bytes"),
            "attachment-payload-bytes"
        );
    }

    #[test]
    fn network_ping_job_parameters_have_explicit_allocation_bounds() {
        assert!(
            require_at_most(
                "requests-per-target",
                MAX_BENCHMARK_REQUESTS_PER_TARGET,
                MAX_BENCHMARK_REQUESTS_PER_TARGET,
            )
            .is_ok()
        );
        assert!(
            require_at_most(
                "requests-per-target",
                MAX_BENCHMARK_REQUESTS_PER_TARGET + 1,
                MAX_BENCHMARK_REQUESTS_PER_TARGET,
            )
            .is_err()
        );
        assert!(MAX_PREPARED_NETWORK_PING_JOBS >= 6 * 128);
        assert!(MAX_PREPARED_EMBEDDED_PAYLOAD_BYTES >= 8 * 1024 * 1024);
    }

    #[tokio::test]
    async fn network_ping_deadline_covers_a_stalled_response_body_phase() {
        let (head_complete_tx, head_complete_rx) = tokio::sync::oneshot::channel();
        let outcome =
            run_network_ping_outcome_with_deadline(Duration::from_millis(10), async move {
                head_complete_tx
                    .send(())
                    .expect("mark response head complete");
                std::future::pending::<NetworkPingOutcome>().await
            })
            .await;

        head_complete_rx
            .await
            .expect("deadline future reached response body phase");
        assert!(matches!(
            outcome,
            NetworkPingOutcome::Failure(detail)
                if detail == "network ping job exceeded 10ms deadline"
        ));
    }

    fn test_state() -> ClusterBootstrapState {
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_network_hygiene").expect("cluster");
        let local_node = NodeId::from_legacy_named("node_network_hygiene_local").expect("node");
        let remote_node = NodeId::from_legacy_named("node_network_hygiene_remote").expect("node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "network hygiene",
            local_node.clone(),
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
        let now = OffsetDateTime::now_utc();
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

    fn test_connectivity_record(
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> network::ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        network::cluster_connectivity_record(
            state,
            endpoint,
            network::ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        )
    }

    #[test]
    fn network_hygiene_flags_remote_local_only_endpoint() {
        let mut state = test_state();
        let remote_index = 1;
        let mut endpoint = test_endpoint(
            &state.members[remote_index].node_id,
            "endpoint_network_hygiene_remote_local_only",
            "peer.example.test:21353",
        );
        endpoint.privacy = ClusterEndpointPrivacy::LocalOnly;
        state.members[remote_index].endpoints.push(endpoint);

        let findings = network_hygiene_findings(
            &state,
            &[],
            &[],
            &[],
            NetworkHygieneOptions::unscoped(false),
        );

        assert!(
            findings
                .iter()
                .any(|finding| finding.contains("advertises local-only endpoint")),
            "{findings:?}"
        );
    }

    #[test]
    fn network_hygiene_flags_connectivity_record_for_removed_endpoint() {
        let state = test_state();
        let remote_index = 1;
        let endpoint = test_endpoint(
            &state.members[remote_index].node_id,
            "endpoint_network_hygiene_removed",
            "peer.example.test:21353",
        );
        let record = test_connectivity_record(&state, &endpoint);

        let findings = network_hygiene_findings(
            &state,
            &[record],
            &[],
            &[],
            NetworkHygieneOptions::unscoped(false),
        );

        assert!(
            findings
                .iter()
                .any(|finding| finding.contains("references unadvertised endpoint")),
            "{findings:?}"
        );
    }

    #[test]
    fn network_hygiene_flags_missing_active_route_to_remote_main() {
        let state = test_state();

        let findings =
            network_hygiene_findings(&state, &[], &[], &[], NetworkHygieneOptions::unscoped(true));

        assert!(
            findings
                .iter()
                .any(|finding| finding.contains("has no route")),
            "{findings:?}"
        );
    }

    #[test]
    fn network_hygiene_accepts_current_advertised_connectivity() {
        let mut state = test_state();
        let remote_index = 1;
        let endpoint = test_endpoint(
            &state.members[remote_index].node_id,
            "endpoint_network_hygiene_current",
            "peer.example.test:21353",
        );
        let record = test_connectivity_record(&state, &endpoint);
        state.members[remote_index].endpoints.push(endpoint);

        let findings = network_hygiene_findings(
            &state,
            &[record.clone()],
            &[],
            &[record],
            NetworkHygieneOptions::unscoped(true),
        );

        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn network_hygiene_flags_peer_visible_loopback_address() {
        let mut state = test_state();
        let remote_index = 1;
        let endpoint = test_endpoint(
            &state.members[remote_index].node_id,
            "endpoint_network_hygiene_loopback",
            "127.0.0.1:21353",
        );
        state.members[remote_index].endpoints.push(endpoint);

        let findings = network_hygiene_findings(
            &state,
            &[],
            &[],
            &[],
            NetworkHygieneOptions::unscoped(false),
        );

        assert!(
            findings
                .iter()
                .any(|finding| finding.contains("undialable address")),
            "{findings:?}"
        );
    }

    #[test]
    fn network_hygiene_ignores_removed_member_endpoint() {
        let mut state = test_state();
        let remote_index = 1;
        state.members[remote_index].state = ClusterMemberState::Removed;
        let mut endpoint = test_endpoint(
            &state.members[remote_index].node_id,
            "endpoint_network_hygiene_removed_member",
            "127.0.0.1:21353",
        );
        endpoint.privacy = ClusterEndpointPrivacy::LocalOnly;
        state.members[remote_index].endpoints.push(endpoint);

        let findings = network_hygiene_findings(
            &state,
            &[],
            &[],
            &[],
            NetworkHygieneOptions::unscoped(false),
        );

        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn network_target_filter_selects_requested_remote_main() {
        let mut state = test_state();
        let selected = state.members[1].node_id.clone();
        let other = add_remote_main(&mut state, "node_network_hygiene_roaming_remote");
        let targets = network_test_member_targets_from_state(&state).expect("targets");
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&selected), "{targets:?}");
        assert!(targets.contains(&other), "{targets:?}");

        let ctx =
            SinomoTestContext::new("/tmp").with_parameter("target-node-id", selected.to_string());
        let filtered = filter_network_test_targets(&ctx, &state, targets).expect("filter");

        assert_eq!(filtered, vec![selected]);
    }

    #[test]
    fn network_target_filter_selects_requested_remote_main_by_name() {
        let mut state = test_state();
        let selected = state.members[1].node_id.clone();
        let other = add_remote_main(&mut state, "node_network_hygiene_roaming_remote");
        let targets = network_test_member_targets_from_state(&state).expect("targets");
        assert_eq!(targets.len(), 2);
        assert!(targets.contains(&selected), "{targets:?}");
        assert!(targets.contains(&other), "{targets:?}");

        let ctx = SinomoTestContext::new("/tmp").with_parameter("target-name", "remote");
        let filtered = filter_network_test_targets(&ctx, &state, targets).expect("filter");

        assert_eq!(filtered, vec![selected]);
    }

    #[test]
    fn network_target_filter_rejects_unavailable_target() {
        let state = test_state();
        let missing = NodeId::from_legacy_named("node_network_hygiene_missing").expect("node");
        let targets = network_test_member_targets_from_state(&state).expect("targets");
        let ctx =
            SinomoTestContext::new("/tmp").with_parameter("target-node-id", missing.to_string());

        let error = filter_network_test_targets(&ctx, &state, targets).expect_err("missing target");

        assert!(
            matches!(
                error,
                SinomoTestError::InvalidParameter { ref name, .. } if name == "target-node-id"
            ),
            "{error:?}"
        );
    }

    #[test]
    fn connectivity_fixture_refresh_restores_an_older_snapshot() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-fixture-refresh");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let paths = ClusterPaths::new(&data_dir);
        let mut state = test_state();
        let remote = state.members[1].node_id.clone();
        let unrelated_target =
            add_remote_main(&mut state, "node_network_fixture_refresh_unrelated");
        let endpoint = test_endpoint(
            &remote,
            "endpoint_network_fixture_refresh",
            "remote.test:21353",
        );
        let fixture_only_endpoint = test_endpoint(
            &remote,
            "endpoint_network_fixture_only",
            "fixture-only.test:21353",
        );
        let unrelated_endpoint = test_endpoint(
            &unrelated_target,
            "endpoint_network_fixture_unrelated",
            "unrelated.test:21353",
        );
        state.members[1].endpoints = vec![endpoint.clone(), fixture_only_endpoint.clone()];
        state.members[2].endpoints = vec![unrelated_endpoint.clone()];
        let now = OffsetDateTime::now_utc();
        let original_at = now - time::Duration::seconds(2);
        let seeded_at = now - time::Duration::seconds(1);
        let original = network::cluster_connectivity_record(
            &state,
            &endpoint,
            network::ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some(original_at.to_string()),
            None,
            original_at,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let seeded = network::cluster_connectivity_record(
            &state,
            &endpoint,
            network::ClusterConnectivityState::Degraded,
            None,
            network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD.saturating_sub(1),
            None,
            Some(seeded_at.to_string()),
            seeded_at,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let fixture_only = network::cluster_connectivity_record(
            &state,
            &fixture_only_endpoint,
            network::ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(seeded_at.to_string()),
            None,
            seeded_at,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let unrelated = network::cluster_connectivity_record(
            &state,
            &unrelated_endpoint,
            network::ClusterConnectivityState::Unreachable,
            None,
            network::CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
            None,
            Some(original_at.to_string()),
            original_at,
            network::CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        );
        network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[seeded, fixture_only, unrelated.clone()],
        )
        .expect("seed newer fixture observation");

        let restored =
            refresh_connectivity_fixture_records(vec![original, unrelated.clone()], &remote);
        assert!(restored[0].observed_at > seeded_at);
        assert_eq!(
            restored[1], unrelated,
            "unrelated target TTLs stay untouched"
        );
        network::write_local_cluster_connectivity_record_set(
            &paths,
            &state.local_node_id,
            &restored,
        )
        .expect("restore refreshed fixture snapshot");

        let records = network::read_local_cluster_connectivity_records_for_source(
            &paths,
            &state.local_node_id,
        )
        .expect("read restored connectivity");
        assert_eq!(records.len(), 2);
        let restored_target = records
            .iter()
            .find(|record| record.target_node_id == remote)
            .expect("restored fixture target");
        assert_eq!(
            restored_target.state,
            network::ClusterConnectivityState::Healthy
        );
        assert_eq!(
            restored_target.expires_at - restored_target.observed_at,
            time::Duration::seconds(network::CLUSTER_CONNECTIVITY_TTL_SECONDS)
        );
    }

    #[test]
    fn stale_direct_timeout_fixture_keeps_real_fallbacks_healthy() {
        let mut state = test_state();
        let target_node = state.members[1].node_id.clone();
        let stall = test_endpoint(
            &target_node,
            "endpoint_network_stale_direct_stall",
            "stall.test:21353",
        );
        let fallback = test_endpoint(
            &target_node,
            "endpoint_network_stale_direct_fallback",
            "fallback.test:21353",
        );
        state.members[1].endpoints = vec![stall.clone(), fallback.clone()];

        let records =
            stale_direct_timeout_seed_records(&state, &state.members[1], &stall, Vec::new());
        let stall_record = records
            .iter()
            .find(|record| record.endpoint_id == stall.endpoint_id)
            .expect("stall record");
        let fallback_record = records
            .iter()
            .find(|record| record.endpoint_id == fallback.endpoint_id)
            .expect("fallback record");

        assert_eq!(
            stall_record.state,
            network::ClusterConnectivityState::Healthy
        );
        assert_eq!(stall_record.latency_ms, Some(0));
        assert_eq!(
            fallback_record.state,
            network::ClusterConnectivityState::Healthy
        );
        assert_eq!(fallback_record.latency_ms, Some(60_000));
        assert_eq!(fallback_record.consecutive_failures, 0);
    }

    #[test]
    fn network_ping_targets_exclude_shadow_only_degraded_main() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-ping-target-shadow-only");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let paths = ClusterPaths::new(&data_dir);
        let mut state = test_state();
        let remote = state.members[1].node_id.clone();
        let main_endpoint = test_endpoint(
            &remote,
            "endpoint_network_ping_shadow_main",
            "remote.test:21353",
        );
        let shadow_endpoint = test_endpoint(
            &remote,
            "endpoint_network_ping_shadow_shadow",
            "remote.test:21354",
        );
        state.members[1].endpoints = vec![main_endpoint.clone(), shadow_endpoint.clone()];
        let now = OffsetDateTime::now_utc();
        let main_connectivity = network::cluster_connectivity_record(
            &state,
            &main_endpoint,
            network::ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some(now.to_string()),
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let shadow_connectivity = network::cluster_connectivity_record(
            &state,
            &shadow_endpoint,
            network::ClusterConnectivityState::Degraded,
            Some(50),
            1,
            Some(now.to_string()),
            None,
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[main_connectivity, shadow_connectivity],
        )
        .expect("write connectivity");

        let targets = network_ping_targets_from_state(&paths, &state).expect("targets");

        assert!(
            targets.is_empty(),
            "shadow-only degraded connectivity must not make a main daemon benchmark targetable: {targets:?}"
        );
    }

    #[test]
    fn network_ping_targets_exclude_degraded_main() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-ping-target-degraded-main");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let paths = ClusterPaths::new(&data_dir);
        let mut state = test_state();
        let remote = state.members[1].node_id.clone();
        let main_endpoint = test_endpoint(
            &remote,
            "endpoint_network_ping_degraded_main",
            "remote.test:21353",
        );
        state.members[1].endpoints = vec![main_endpoint.clone()];
        let now = OffsetDateTime::now_utc();
        let main_connectivity = network::cluster_connectivity_record(
            &state,
            &main_endpoint,
            network::ClusterConnectivityState::Degraded,
            Some(80),
            2,
            Some(now.to_string()),
            None,
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[main_connectivity],
        )
        .expect("write connectivity");

        let targets = network_ping_targets_from_state(&paths, &state).expect("targets");

        assert!(
            targets.is_empty(),
            "degraded main daemon connectivity must not make a benchmark targetable: {targets:?}"
        );
    }

    #[test]
    fn network_ping_targets_include_degraded_main_with_healthy_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-ping-target-routed-main");
        let data_dir = SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("root layout");
        let paths = ClusterPaths::new(&data_dir);
        let mut state = test_state();
        let bridge = state.members[1].node_id.clone();
        let target = add_remote_main(&mut state, "node_network_ping_routed_target");
        let bridge_endpoint = test_endpoint(
            &bridge,
            "endpoint_network_ping_routed_bridge",
            "bridge.test:21353",
        );
        let target_endpoint = test_endpoint(
            &target,
            "endpoint_network_ping_routed_target",
            "target.test:21353",
        );
        state.members[1].endpoints = vec![bridge_endpoint.clone()];
        state.members[2].endpoints = vec![target_endpoint.clone()];
        let now = OffsetDateTime::now_utc();
        let local_to_target = network::cluster_connectivity_record(
            &state,
            &target_endpoint,
            network::ClusterConnectivityState::Degraded,
            Some(80),
            2,
            Some(now.to_string()),
            None,
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let local_to_bridge = network::cluster_connectivity_record(
            &state,
            &bridge_endpoint,
            network::ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mut bridge_state = state.clone();
        bridge_state.local_node_id = bridge.clone();
        let bridge_to_target = network::cluster_connectivity_record(
            &bridge_state,
            &target_endpoint,
            network::ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some(now.to_string()),
            None,
            now,
            network::CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        network::write_local_cluster_connectivity_records(
            &paths,
            &state.local_node_id,
            &[local_to_target, local_to_bridge],
        )
        .expect("write local connectivity");
        network::write_local_cluster_connectivity_record_set(&paths, &bridge, &[bridge_to_target])
            .expect("write bridge connectivity");

        let targets = network_ping_targets_from_state(&paths, &state).expect("targets");

        assert!(
            targets.contains(&target),
            "degraded direct connectivity must stay targetable when the graph has a healthy route: {targets:?}"
        );
    }

    #[test]
    fn network_hygiene_scoped_active_route_ignores_unselected_roaming_node() {
        let mut state = test_state();
        let selected = state.members[1].node_id.clone();
        let roaming = add_remote_main(&mut state, "node_network_hygiene_roaming_remote");
        let endpoint = test_endpoint(
            &selected,
            "endpoint_network_hygiene_selected",
            "selected.example.test:21353",
        );
        let record = test_connectivity_record(&state, &endpoint);
        state.members[1].endpoints.push(endpoint);

        let unscoped_findings = network_hygiene_findings(
            &state,
            &[record.clone()],
            &[],
            &[record.clone()],
            NetworkHygieneOptions::unscoped(true),
        );
        assert!(
            unscoped_findings
                .iter()
                .any(|finding| finding.contains(roaming.as_str())),
            "{unscoped_findings:?}"
        );

        let scoped_findings = network_hygiene_findings(
            &state,
            &[record.clone()],
            &[],
            &[record],
            NetworkHygieneOptions::for_targets(true, [selected]),
        );

        assert!(scoped_findings.is_empty(), "{scoped_findings:?}");
    }
}
