#[cfg(test)]
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_read_model::read_cluster_node_statuses_from_kv;
#[cfg(any(test, feature = "test-support"))]
use crate::local_state_store::read_cluster_state;
use crate::local_state_store::read_cluster_state_async;
use crate::prelude::*;
use sinomo_network::CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY;
use sinomo_network::ClusterMembershipRpcFuture;
use sinomo_network::{
    CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, CLUSTER_HEALTH_SCOPE_LOCAL,
    CLUSTER_HEALTH_SCOPE_METADATA_KEY, CLUSTER_NETWORK_PING_ROUTE, CLUSTER_NETWORK_RPC_CATEGORY,
    ObjectRpcRequest, ObjectRpcResponse,
};
use sinomo_network::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityState,
    ClusterEndpointSelector, ClusterPaths, NetworkError,
};
use sinomo_network::{
    cluster_endpoint_can_carry_main_daemon_rpc, cluster_endpoint_module_url_identity,
    cluster_route_next_hop,
};
use sinomo_node::{
    DAEMON_MAIN_RUN_CAPABILITY, cluster_member_expected_daemon_process,
    cluster_member_is_legacy_shadow_daemon, cluster_member_is_local_process,
};

const CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS: usize = 8;
// Peer expansion is a semantic aggregation budget, not a transport no-progress
// timeout. Keep it below the outer 5s daemon RPC idle window so an unresolved
// cluster can still return a deliberate partial view instead of making the
// caller fall back to a less capable local-only sweep.
const CLUSTER_HEALTH_PEER_ROWS_TOTAL_BUDGET: std::time::Duration =
    std::time::Duration::from_secs(2);
// Start a peer-view hedge once directly reachable members have had a short
// opportunity to answer. The full request budget is still available for a
// second direct pass after peer expansion, so this improves relayed topologies
// without turning this latency hedge into a production liveness deadline.
const CLUSTER_HEALTH_DIRECT_PROBE_HEDGE_DELAY: std::time::Duration =
    std::time::Duration::from_millis(100);
// Live status fanout and any peer expansion share this semantic operation
// budget. The daemon RPC transport has a 5s no-progress timeout, so retaining
// one second for dispatch, storage reads, and response encoding keeps a
// deliberately partial health response preferable to a transport timeout.
const CLUSTER_HEALTH_LIVE_TOTAL_BUDGET: std::time::Duration = std::time::Duration::from_secs(4);
const CLUSTER_HEALTH_LIVE_PROBES_METADATA_KEY: &str = "sinomo-health-live-probes";
const CLUSTER_HEALTH_LIVE_PROBES_METADATA_VALUE: &str = "true";

#[derive(Clone, Copy, Debug, Default)]
struct ClusterHealthTraceStats {
    reportable_member_count: usize,
    live_status_count: usize,
    row_count: usize,
    online_count: usize,
    unresolved_count: usize,
    timeout_us: u64,
}

impl ClusterHealthTraceStats {
    fn from_rows(rows: &[ClusterHealthRow]) -> Self {
        let online_count = rows
            .iter()
            .filter(|row| row.availability == ClusterNodeAvailability::Online)
            .count();
        Self {
            reportable_member_count: rows.len(),
            row_count: rows.len(),
            online_count,
            unresolved_count: rows.len().saturating_sub(online_count),
            ..Self::default()
        }
    }
}

/// A diagnostic-only lifecycle record for one `/cluster/health` request.
///
/// The Docker formation gate may cancel the client at its absolute deadline.
/// A completion-only trace would make that indistinguishable from a handler
/// that never started, so `Drop` records cancellation when the handler future
/// is actually retired before a normal result is produced.
struct ClusterHealthRequestTrace {
    request_id: String,
    traceparent: String,
    started: std::time::Instant,
    local_scope: bool,
    include_peer_views: bool,
    live_probes: bool,
    local_node_id: String,
    kv_revision: u64,
    member_count: usize,
    state_loaded: bool,
    result_stats: ClusterHealthTraceStats,
    finished: bool,
}

impl ClusterHealthRequestTrace {
    fn new(request: &ObjectRpcRequest) -> Option<Self> {
        if request.route != CLUSTER_HEALTH_ROUTE
            || !tracing::enabled!(target: "sinomo.cluster.health", tracing::Level::TRACE)
        {
            return None;
        }
        let trace = Self {
            request_id: sinomo_base::Id::generate().to_string(),
            traceparent: request.traceparent().unwrap_or_default().to_string(),
            started: std::time::Instant::now(),
            local_scope: cluster_health_rpc_request_is_local_scope(request),
            include_peer_views: cluster_health_rpc_request_includes_peer_views(request),
            live_probes: cluster_health_rpc_request_uses_live_probes(request),
            local_node_id: String::new(),
            kv_revision: 0,
            member_count: 0,
            state_loaded: false,
            result_stats: ClusterHealthTraceStats::default(),
            finished: false,
        };
        trace.emit_instant(
            "cluster_health_request",
            "started",
            "",
            ClusterHealthTraceStats::default(),
        );
        Some(trace)
    }

    fn observe_state(&mut self, state: &ClusterBootstrapState, outcome: &'static str) {
        self.local_node_id = state.local_node_id.to_string();
        self.kv_revision = state.kv_revision;
        self.member_count = state.members.len();
        self.state_loaded = true;
        self.emit_instant(
            "cluster_health_state_snapshot",
            outcome,
            "",
            ClusterHealthTraceStats {
                reportable_member_count: state
                    .members
                    .iter()
                    .filter(|member| {
                        !matches!(member.state, ClusterMemberState::Removed)
                            && cluster_health_member_is_reportable(member)
                    })
                    .count(),
                ..ClusterHealthTraceStats::default()
            },
        );
    }

    fn observe_rows(&mut self, rows: &[ClusterHealthRow]) {
        self.result_stats = ClusterHealthTraceStats::from_rows(rows);
    }

    fn emit_instant(
        &self,
        stage: &'static str,
        outcome: &'static str,
        target_node_id: &str,
        stats: ClusterHealthTraceStats,
    ) {
        tracing::trace!(
            target: "sinomo.cluster.health",
            stage,
            outcome,
            request_id = %self.request_id,
            traceparent = %self.traceparent,
            local_node_id = %self.local_node_id,
            target_node_id,
            local_scope = self.local_scope,
            include_peer_views = self.include_peer_views,
            live_probes = self.live_probes,
            state_loaded = self.state_loaded,
            kv_revision = self.kv_revision,
            member_count = self.member_count,
            reportable_member_count = stats.reportable_member_count,
            live_status_count = stats.live_status_count,
            row_count = stats.row_count,
            online_count = stats.online_count,
            unresolved_count = stats.unresolved_count,
            timeout_us = stats.timeout_us,
            "cluster health observation"
        );
    }

    fn emit_duration(
        &self,
        stage: &'static str,
        started: std::time::Instant,
        outcome: &'static str,
        target_node_id: &str,
        stats: ClusterHealthTraceStats,
    ) {
        let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        tracing::trace!(
            target: "sinomo.cluster.health",
            stage,
            outcome,
            request_id = %self.request_id,
            traceparent = %self.traceparent,
            local_node_id = %self.local_node_id,
            target_node_id,
            local_scope = self.local_scope,
            include_peer_views = self.include_peer_views,
            live_probes = self.live_probes,
            state_loaded = self.state_loaded,
            kv_revision = self.kv_revision,
            member_count = self.member_count,
            reportable_member_count = stats.reportable_member_count,
            live_status_count = stats.live_status_count,
            row_count = stats.row_count,
            online_count = stats.online_count,
            unresolved_count = stats.unresolved_count,
            timeout_us = stats.timeout_us,
            elapsed_us,
            "cluster health observation"
        );
    }

    fn finish(mut self, outcome: &'static str) {
        self.emit_duration(
            "cluster_health_request",
            self.started,
            outcome,
            "",
            self.result_stats,
        );
        self.finished = true;
    }
}

impl Drop for ClusterHealthRequestTrace {
    fn drop(&mut self) {
        if !self.finished {
            self.emit_duration(
                "cluster_health_request",
                self.started,
                "cancelled",
                "",
                self.result_stats,
            );
        }
    }
}

struct ClusterHealthPhaseTrace<'a> {
    request: &'a ClusterHealthRequestTrace,
    stage: &'static str,
    target_node_id: String,
    started: std::time::Instant,
    initial_stats: ClusterHealthTraceStats,
    finished: bool,
}

impl<'a> ClusterHealthPhaseTrace<'a> {
    fn new(
        request: &'a ClusterHealthRequestTrace,
        stage: &'static str,
        target_node_id: impl Into<String>,
        stats: ClusterHealthTraceStats,
    ) -> Self {
        let target_node_id = target_node_id.into();
        request.emit_instant(stage, "started", &target_node_id, stats);
        Self {
            request,
            stage,
            target_node_id,
            started: std::time::Instant::now(),
            initial_stats: stats,
            finished: false,
        }
    }

    fn finish(mut self, outcome: &'static str, stats: ClusterHealthTraceStats) {
        self.request.emit_duration(
            self.stage,
            self.started,
            outcome,
            &self.target_node_id,
            stats,
        );
        self.finished = true;
    }
}

impl Drop for ClusterHealthPhaseTrace<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.request.emit_duration(
                self.stage,
                self.started,
                "cancelled",
                &self.target_node_id,
                self.initial_stats,
            );
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterHealthRow {
    pub member: ClusterMemberRecord,
    pub status: ClusterNodeStatusRecord,
    pub availability: ClusterNodeAvailability,
    /// True when this row's liveness was established by a direct status probe
    /// in the current health request, either locally or by a queried peer.
    /// Older peers omit the field and cannot turn cached status into liveness.
    #[serde(default)]
    pub live_probe_observed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterNetworkPingRequest {
    pub nonce: String,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "sinomo_network::serialize_bytes",
        deserialize_with = "sinomo_network::deserialize_bytes"
    )]
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub response_payload_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterNetworkPingResponse {
    pub nonce: String,
    pub responder_node_id: NodeId,
    pub payload_bytes: usize,
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "sinomo_network::serialize_bytes",
        deserialize_with = "sinomo_network::deserialize_bytes"
    )]
    pub response_payload: Vec<u8>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

pub fn cluster_health_rpc_request() -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, &())
}

pub fn cluster_health_rpc_request_without_peer_views() -> Result<ObjectRpcRequest, NetworkError> {
    let mut request = cluster_health_rpc_request()?;
    request.metadata.insert(
        CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY.to_string(),
        "false".to_string(),
    );
    Ok(request)
}

/// Requests a live cluster-wide view from one peer without asking that peer to
/// recursively expand through additional peers.
///
/// This is distinct from [`cluster_health_rpc_request_without_peer_views`],
/// which is the cheap KV-backed snapshot used by routine foreground readers.
pub fn cluster_health_rpc_request_without_recursive_peer_views()
-> Result<ObjectRpcRequest, NetworkError> {
    let mut request = cluster_health_rpc_request_without_peer_views()?;
    request.metadata.insert(
        CLUSTER_HEALTH_LIVE_PROBES_METADATA_KEY.to_string(),
        CLUSTER_HEALTH_LIVE_PROBES_METADATA_VALUE.to_string(),
    );
    Ok(request)
}

pub fn cluster_network_ping_rpc_request(
    nonce: impl Into<String>,
    payload_bytes: usize,
) -> Result<ObjectRpcRequest, NetworkError> {
    cluster_network_ping_rpc_request_with_response_payload(nonce, payload_bytes, 0)
}

pub fn cluster_network_ping_rpc_request_with_response_payload(
    nonce: impl Into<String>,
    payload_bytes: usize,
    response_payload_bytes: usize,
) -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(
        CLUSTER_NETWORK_PING_ROUTE,
        CLUSTER_NETWORK_RPC_CATEGORY,
        &ClusterNetworkPingRequest {
            nonce: nonce.into(),
            payload: sinomo_network::try_zeroed_vec(payload_bytes, "cluster network ping payload")?,
            response_payload_bytes,
        },
    )
    .map(ObjectRpcRequest::with_response_retry_safe)
}

pub fn cluster_local_health_rpc_request() -> Result<ObjectRpcRequest, NetworkError> {
    let mut request = cluster_health_rpc_request()?;
    request.metadata.insert(
        CLUSTER_HEALTH_SCOPE_METADATA_KEY.to_string(),
        CLUSTER_HEALTH_SCOPE_LOCAL.to_string(),
    );
    Ok(request)
}

pub fn cluster_health_rpc_request_is_local_scope(request: &ObjectRpcRequest) -> bool {
    request
        .metadata
        .get(CLUSTER_HEALTH_SCOPE_METADATA_KEY)
        .is_some_and(|scope| scope == CLUSTER_HEALTH_SCOPE_LOCAL)
}

fn cluster_health_rpc_request_includes_peer_views(request: &ObjectRpcRequest) -> bool {
    !request
        .metadata
        .get(CLUSTER_HEALTH_INCLUDE_PEER_VIEWS_METADATA_KEY)
        .is_some_and(|include| include == "false" || include == "0")
}

fn cluster_health_rpc_request_uses_live_probes(request: &ObjectRpcRequest) -> bool {
    request
        .metadata
        .get(CLUSTER_HEALTH_LIVE_PROBES_METADATA_KEY)
        .is_some_and(|value| value == CLUSTER_HEALTH_LIVE_PROBES_METADATA_VALUE)
}

pub fn decode_cluster_health_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    response.decode_body()
}

pub fn decode_cluster_network_ping_rpc_response(
    response: &ObjectRpcResponse,
) -> Result<ClusterNetworkPingResponse, NetworkError> {
    response.decode_body()
}

#[cfg(any(test, feature = "test-support"))]
pub fn handle_cluster_health_rpc_request<P: ClusterStatusProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
) -> Result<ObjectRpcResponse, NetworkError> {
    match request.route.as_str() {
        CLUSTER_HEALTH_ROUTE => {
            let _: () = request.decode_body()?;
            let local_scope = cluster_health_rpc_request_is_local_scope(&request);
            let include_peer_views = cluster_health_rpc_request_includes_peer_views(&request);
            let live_probes = cluster_health_rpc_request_uses_live_probes(&request);
            let rows = if local_scope {
                cluster_health_rows_with_probe_for_scope(
                    paths,
                    state,
                    probe,
                    false,
                    true,
                    observed_at,
                )?
            } else if live_probes {
                cluster_health_rows_with_probe(paths, state, probe, false, observed_at)?
            } else if !include_peer_views {
                cluster_health_rows_from_stored_statuses(paths, state, false, observed_at)?
            } else {
                cluster_health_rows_with_probe(
                    paths,
                    state,
                    probe,
                    include_peer_views,
                    observed_at,
                )?
            };
            ObjectRpcResponse::encode_body(CLUSTER_HEALTH_ROUTE, &rows)
        }
        CLUSTER_NETWORK_PING_ROUTE => {
            handle_cluster_network_ping_rpc_request(state, request, observed_at)
        }
        other => Err(NetworkError::UnknownRoute {
            route: other.to_string(),
        }),
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn handle_current_cluster_health_rpc_request<P: ClusterStatusProbe + ?Sized>(
    paths: &ClusterPaths,
    probe: &P,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
) -> Result<ObjectRpcResponse, NetworkError> {
    let state = read_cluster_state(paths)?;
    handle_cluster_health_rpc_request(paths, &state, probe, request, observed_at)
}

pub async fn handle_cluster_health_rpc_request_async<P: ClusterStatusAsyncProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut trace = ClusterHealthRequestTrace::new(&request);
    if let Some(trace) = trace.as_mut() {
        trace.observe_state(state, "provided");
    }
    let result = handle_cluster_health_rpc_request_async_observed(
        paths,
        state,
        probe,
        request,
        observed_at,
        &mut trace,
    )
    .await;
    if let Some(trace) = trace.take() {
        trace.finish(if result.is_ok() { "completed" } else { "error" });
    }
    result
}

async fn handle_cluster_health_rpc_request_async_observed<P: ClusterStatusAsyncProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
    trace: &mut Option<ClusterHealthRequestTrace>,
) -> Result<ObjectRpcResponse, NetworkError> {
    match request.route.as_str() {
        CLUSTER_HEALTH_ROUTE => {
            let _: () = request.decode_body()?;
            let local_scope = cluster_health_rpc_request_is_local_scope(&request);
            let include_peer_views = cluster_health_rpc_request_includes_peer_views(&request);
            let live_probes = cluster_health_rpc_request_uses_live_probes(&request);
            let rows = if local_scope {
                if trace.is_some() {
                    cluster_health_rows_with_async_probe_for_scope_with_budget_observed(
                        paths,
                        state,
                        probe,
                        false,
                        true,
                        observed_at,
                        CLUSTER_HEALTH_LIVE_TOTAL_BUDGET,
                        trace.as_ref(),
                    )
                    .await?
                } else {
                    cluster_health_rows_with_async_probe_for_scope(
                        paths,
                        state,
                        probe,
                        false,
                        true,
                        observed_at,
                    )
                    .await?
                }
            } else if live_probes {
                if trace.is_some() {
                    cluster_health_rows_with_async_probe_for_scope_with_budget_observed(
                        paths,
                        state,
                        probe,
                        false,
                        false,
                        observed_at,
                        CLUSTER_HEALTH_LIVE_TOTAL_BUDGET,
                        trace.as_ref(),
                    )
                    .await?
                } else {
                    cluster_health_rows_with_async_probe(paths, state, probe, false, observed_at)
                        .await?
                }
            } else if !include_peer_views {
                let phase = trace.as_ref().map(|trace| {
                    ClusterHealthPhaseTrace::new(
                        trace,
                        "cluster_health_stored_status_read",
                        "",
                        ClusterHealthTraceStats::default(),
                    )
                });
                match cluster_health_rows_from_stored_statuses_async(
                    paths,
                    state,
                    false,
                    observed_at,
                )
                .await
                {
                    Ok(rows) => {
                        if let Some(phase) = phase {
                            phase.finish("completed", ClusterHealthTraceStats::from_rows(&rows));
                        }
                        rows
                    }
                    Err(error) => {
                        if let Some(phase) = phase {
                            phase.finish("error", ClusterHealthTraceStats::default());
                        }
                        return Err(error);
                    }
                }
            } else {
                if trace.is_some() {
                    cluster_health_rows_with_async_probe_for_scope_with_budget_observed(
                        paths,
                        state,
                        probe,
                        include_peer_views,
                        false,
                        observed_at,
                        CLUSTER_HEALTH_LIVE_TOTAL_BUDGET,
                        trace.as_ref(),
                    )
                    .await?
                } else {
                    cluster_health_rows_with_async_probe(
                        paths,
                        state,
                        probe,
                        include_peer_views,
                        observed_at,
                    )
                    .await?
                }
            };
            if let Some(trace) = trace.as_mut() {
                trace.observe_rows(&rows);
            }
            ObjectRpcResponse::encode_body(CLUSTER_HEALTH_ROUTE, &rows)
        }
        CLUSTER_NETWORK_PING_ROUTE => {
            handle_cluster_network_ping_rpc_request(state, request, observed_at)
        }
        other => Err(NetworkError::UnknownRoute {
            route: other.to_string(),
        }),
    }
}

pub async fn handle_current_cluster_health_rpc_request_async<
    P: ClusterStatusAsyncProbe + ?Sized,
>(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    probe: &P,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
) -> Result<ObjectRpcResponse, NetworkError> {
    let mut trace = ClusterHealthRequestTrace::new(&request);
    let state_read_started = std::time::Instant::now();
    if let Some(trace) = trace.as_ref() {
        trace.emit_instant(
            "cluster_health_state_snapshot_read",
            "started",
            "",
            ClusterHealthTraceStats::default(),
        );
    }
    let state = match read_cluster_state_async(runtime, paths).await {
        Ok(state) => {
            if let Some(trace) = trace.as_mut() {
                trace.observe_state(&state, "loaded");
                trace.emit_duration(
                    "cluster_health_state_snapshot_read",
                    state_read_started,
                    "completed",
                    "",
                    ClusterHealthTraceStats {
                        reportable_member_count: state.members.len(),
                        ..ClusterHealthTraceStats::default()
                    },
                );
            }
            state
        }
        Err(error) => {
            if let Some(trace) = trace.as_ref() {
                trace.emit_duration(
                    "cluster_health_state_snapshot_read",
                    state_read_started,
                    "error",
                    "",
                    ClusterHealthTraceStats::default(),
                );
            }
            if let Some(trace) = trace.take() {
                trace.finish("error");
            }
            return Err(error);
        }
    };
    if trace.is_none() {
        return handle_cluster_health_rpc_request_async(paths, &state, probe, request, observed_at)
            .await;
    }
    let result = handle_cluster_health_rpc_request_async_observed(
        paths,
        &state,
        probe,
        request,
        observed_at,
        &mut trace,
    )
    .await;
    if let Some(trace) = trace.take() {
        trace.finish(if result.is_ok() { "completed" } else { "error" });
    }
    result
}

pub fn handle_cluster_network_ping_rpc_request(
    state: &ClusterBootstrapState,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
) -> Result<ObjectRpcResponse, NetworkError> {
    handle_cluster_network_ping_rpc_request_for_node(&state.local_node_id, request, observed_at)
}

pub fn handle_cluster_network_ping_rpc_request_for_node(
    responder_node_id: &NodeId,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
) -> Result<ObjectRpcResponse, NetworkError> {
    match request.route.as_str() {
        CLUSTER_NETWORK_PING_ROUTE => {
            let request: ClusterNetworkPingRequest = request.decode_body()?;
            let response = ClusterNetworkPingResponse {
                nonce: request.nonce,
                responder_node_id: responder_node_id.clone(),
                payload_bytes: request.payload.len(),
                response_payload: sinomo_network::try_zeroed_vec(
                    request.response_payload_bytes,
                    "cluster network ping response payload",
                )?,
                observed_at,
            };
            ObjectRpcResponse::encode_body(CLUSTER_NETWORK_PING_ROUTE, &response)
        }
        other => Err(NetworkError::UnknownRoute {
            route: other.to_string(),
        }),
    }
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterDoctorFinding {
    pub severity: String,
    pub code: String,
    pub detail: String,
}

impl ClusterDoctorFinding {
    fn info(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: "info".to_string(),
            code: code.into(),
            detail: detail.into(),
        }
    }

    fn warning(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: "warning".to_string(),
            code: code.into(),
            detail: detail.into(),
        }
    }

    pub fn is_warning(&self) -> bool {
        self.severity == "warning"
    }
}

/// Availability of the configured Raft state authorities represented by a
/// cluster health snapshot.
///
/// Individual members may be offline without degrading the control plane. The
/// load-bearing threshold is the same configured voter quorum used by the KV
/// implementation, not a fixed number of tolerated host failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterStateAuthorityHealth {
    pub authority_count: usize,
    pub online_authority_count: usize,
    pub quorum_size: usize,
}

impl ClusterStateAuthorityHealth {
    pub fn has_quorum(self) -> bool {
        self.online_authority_count >= self.quorum_size
    }
}

pub fn cluster_state_authority_health(
    state: &ClusterBootstrapState,
    health: &[ClusterHealthRow],
) -> ClusterStateAuthorityHealth {
    let online_nodes = health
        .iter()
        .filter(|row| row.availability == ClusterNodeAvailability::Online)
        .map(|row| &row.member.node_id)
        .collect::<BTreeSet<_>>();
    let authority_count = state
        .members
        .iter()
        .filter(|member| state.node_is_raft_voter(&member.node_id))
        .count();
    let online_authority_count = state
        .members
        .iter()
        .filter(|member| {
            state.node_is_raft_voter(&member.node_id) && online_nodes.contains(&member.node_id)
        })
        .count();
    ClusterStateAuthorityHealth {
        authority_count,
        online_authority_count,
        quorum_size: sinomo_kv::cluster_kv_raft_quorum_size_for_state(state),
    }
}

pub trait ClusterStatusProbe {
    fn probe_member_status(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
    ) -> Option<ClusterNodeStatusRecord>;

    /// Fetches the peer's live full-cluster view without recursive peer
    /// expansion. `timeout` is this attempt's slice of the shared aggregation
    /// deadline.
    fn peer_health_rows(
        &self,
        _state: &ClusterBootstrapState,
        _member: &ClusterMemberRecord,
        _timeout: std::time::Duration,
    ) -> Option<Vec<ClusterHealthRow>> {
        None
    }
}

pub trait ClusterStatusAsyncProbe: Sync {
    fn probe_member_status_async<'a>(
        &'a self,
        state: &'a ClusterBootstrapState,
        member: &'a ClusterMemberRecord,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>>;

    fn probe_member_statuses_async<'a>(
        &'a self,
        state: &'a ClusterBootstrapState,
        members: &'a [ClusterMemberRecord],
        statuses: &'a mut BTreeMap<NodeId, Option<ClusterNodeStatusRecord>>,
    ) -> ClusterMembershipRpcFuture<'a, ()> {
        Box::pin(async move {
            for member in members {
                statuses.insert(
                    member.node_id.clone(),
                    self.probe_member_status_async(state, member).await?,
                );
            }
            Ok(())
        })
    }

    /// Async counterpart of [`ClusterStatusProbe::peer_health_rows`].
    fn peer_health_rows_async<'a>(
        &'a self,
        _state: &'a ClusterBootstrapState,
        _member: &'a ClusterMemberRecord,
        _timeout: std::time::Duration,
    ) -> ClusterMembershipRpcFuture<'a, Option<Vec<ClusterHealthRow>>> {
        Box::pin(async { Ok(None) })
    }
}

pub fn cluster_health_rows_with_probe<P: ClusterStatusProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    include_peer_views: bool,
    observed_at: OffsetDateTime,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    cluster_health_rows_with_probe_for_scope(
        paths,
        state,
        probe,
        include_peer_views,
        false,
        observed_at,
    )
}

fn cluster_health_rows_from_stored_statuses(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_scope: bool,
    observed_at: OffsetDateTime,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    let stored_statuses = read_cluster_node_statuses_from_kv(paths)?;
    let mut rows = Vec::new();
    for member in state
        .members
        .iter()
        .filter(|member| cluster_health_member_is_in_scope(state, member, local_scope))
    {
        let expected_process = cluster_member_expected_daemon_process(member);
        let stored =
            stored_cluster_node_status_for_member(&stored_statuses, member, expected_process);
        let availability = stored
            .as_ref()
            .map(|status| status.availability.clone())
            .unwrap_or(ClusterNodeAvailability::Unknown);
        let mut status =
            stored.unwrap_or_else(|| unknown_cluster_node_status(state, member, observed_at));
        status.availability = availability.clone();
        rows.push(ClusterHealthRow {
            member: member.clone(),
            status,
            availability,
            live_probe_observed: false,
        });
    }
    rows.sort_by(|left, right| left.member.node_id.cmp(&right.member.node_id));
    Ok(rows)
}

fn cluster_health_rows_with_probe_for_scope<P: ClusterStatusProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    include_peer_views: bool,
    local_scope: bool,
    observed_at: OffsetDateTime,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    let stored_statuses = read_cluster_node_statuses_from_kv(paths)?;
    let connectivity = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut rows = Vec::new();
    for member in state
        .members
        .iter()
        .filter(|member| cluster_health_member_is_in_scope(state, member, local_scope))
    {
        let expected_process = cluster_member_expected_daemon_process(member);
        let live = probe
            .probe_member_status(state, member)
            .filter(|status| cluster_node_status_matches_member(status, member, expected_process));
        let stored =
            stored_cluster_node_status_for_member(&stored_statuses, member, expected_process);
        let live_online = live.is_some();
        let availability = cluster_health_member_availability(
            state,
            member,
            &connectivity,
            live_online,
            stored.as_ref(),
        );
        let mut status = if let Some(live) = live {
            let mut live = live;
            if live.rollout.is_none() {
                live.rollout = stored.as_ref().and_then(|status| status.rollout.clone());
            }
            live
        } else if let Some(stored) = stored {
            stored
        } else {
            unknown_cluster_node_status(state, member, observed_at)
        };
        status.availability = availability.clone();
        rows.push(ClusterHealthRow {
            member: member.clone(),
            status,
            availability,
            live_probe_observed: live_online,
        });
    }
    if include_peer_views {
        merge_reachable_peer_health_rows(state, &mut rows, probe);
    }
    rows.sort_by(|left, right| left.member.node_id.cmp(&right.member.node_id));
    Ok(rows)
}

pub async fn cluster_health_rows_with_async_probe<P: ClusterStatusAsyncProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    include_peer_views: bool,
    observed_at: OffsetDateTime,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    cluster_health_rows_with_async_probe_for_scope(
        paths,
        state,
        probe,
        include_peer_views,
        false,
        observed_at,
    )
    .await
}

async fn cluster_health_rows_from_stored_statuses_async(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    local_scope: bool,
    observed_at: OffsetDateTime,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    tokio::task::spawn_blocking(move || {
        cluster_health_rows_from_stored_statuses(&paths, &state, local_scope, observed_at)
    })
    .await
    .map_err(|source| NetworkError::Io {
        operation: "cluster health stored status rows".to_string(),
        detail: format!("cluster health stored status task failed: {source}"),
    })?
}

async fn cluster_health_rows_with_async_probe_for_scope<P: ClusterStatusAsyncProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    include_peer_views: bool,
    local_scope: bool,
    observed_at: OffsetDateTime,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    cluster_health_rows_with_async_probe_for_scope_with_budget(
        paths,
        state,
        probe,
        include_peer_views,
        local_scope,
        observed_at,
        CLUSTER_HEALTH_LIVE_TOTAL_BUDGET,
    )
    .await
}

async fn cluster_health_rows_with_async_probe_for_scope_with_budget<
    P: ClusterStatusAsyncProbe + ?Sized,
>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    include_peer_views: bool,
    local_scope: bool,
    observed_at: OffsetDateTime,
    live_budget: std::time::Duration,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    cluster_health_rows_with_async_probe_for_scope_with_budget_observed(
        paths,
        state,
        probe,
        include_peer_views,
        local_scope,
        observed_at,
        live_budget,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn cluster_health_rows_with_async_probe_for_scope_with_budget_observed<
    P: ClusterStatusAsyncProbe + ?Sized,
>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    include_peer_views: bool,
    local_scope: bool,
    observed_at: OffsetDateTime,
    live_budget: std::time::Duration,
    trace: Option<&ClusterHealthRequestTrace>,
) -> Result<Vec<ClusterHealthRow>, NetworkError> {
    let deadline = tokio::time::Instant::now() + live_budget;
    // A stalled direct member must not consume the only opportunity to learn
    // about it through a healthy bridge. Recursive health reserves the peer
    // aggregation slice up front; nonrecursive peer requests keep the whole
    // live budget because they cannot expand again.
    let peer_budget_reserve = if include_peer_views {
        CLUSTER_HEALTH_PEER_ROWS_TOTAL_BUDGET.min(live_budget / 2)
    } else {
        std::time::Duration::ZERO
    };
    let direct_probe_deadline = deadline
        .checked_sub(peer_budget_reserve)
        .unwrap_or(deadline);
    let probe_deadline = if include_peer_views {
        direct_probe_deadline.min(
            tokio::time::Instant::now()
                .checked_add(CLUSTER_HEALTH_DIRECT_PROBE_HEDGE_DELAY)
                .unwrap_or(direct_probe_deadline),
        )
    } else {
        direct_probe_deadline
    };
    let paths_for_statuses = paths.clone();
    let stored_status_read = trace.map(|trace| {
        ClusterHealthPhaseTrace::new(
            trace,
            "cluster_health_stored_status_read",
            "",
            ClusterHealthTraceStats::default(),
        )
    });
    let stored_statuses = tokio::task::spawn_blocking(move || {
        read_cluster_node_statuses_from_kv(&paths_for_statuses)
    })
    .await;
    let stored_statuses = match stored_statuses {
        Ok(Ok(statuses)) => {
            if let Some(stored_status_read) = stored_status_read {
                stored_status_read.finish("completed", ClusterHealthTraceStats::default());
            }
            statuses
        }
        Ok(Err(error)) => {
            if let Some(stored_status_read) = stored_status_read {
                stored_status_read.finish("error", ClusterHealthTraceStats::default());
            }
            return Err(error);
        }
        Err(source) => {
            if let Some(stored_status_read) = stored_status_read {
                stored_status_read.finish("error", ClusterHealthTraceStats::default());
            }
            return Err(NetworkError::Io {
                operation: "cluster health status read".to_string(),
                detail: format!("cluster health status read task failed: {source}"),
            });
        }
    };
    let connectivity = ClusterConnectivityGraph::read_for_local(paths, state);
    let reportable_members = state
        .members
        .iter()
        .filter(|member| cluster_health_member_is_in_scope(state, member, local_scope))
        .cloned()
        .collect::<Vec<_>>();
    let mut live_statuses = BTreeMap::new();
    if tokio::time::Instant::now() < probe_deadline {
        let timeout_us = u64::try_from(
            probe_deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_micros(),
        )
        .unwrap_or(u64::MAX);
        let live_probe = trace.map(|trace| {
            ClusterHealthPhaseTrace::new(
                trace,
                "cluster_health_live_probe_batch",
                "",
                ClusterHealthTraceStats {
                    reportable_member_count: reportable_members.len(),
                    timeout_us,
                    ..ClusterHealthTraceStats::default()
                },
            )
        });
        match tokio::time::timeout_at(
            probe_deadline,
            probe.probe_member_statuses_async(state, &reportable_members, &mut live_statuses),
        )
        .await
        {
            Ok(Ok(())) => {
                if let Some(live_probe) = live_probe {
                    live_probe.finish(
                        "completed",
                        ClusterHealthTraceStats {
                            reportable_member_count: reportable_members.len(),
                            live_status_count: live_statuses
                                .values()
                                .filter(|status| status.is_some())
                                .count(),
                            timeout_us,
                            ..ClusterHealthTraceStats::default()
                        },
                    );
                }
            }
            Ok(Err(error)) => {
                if let Some(live_probe) = live_probe {
                    live_probe.finish(
                        "error",
                        ClusterHealthTraceStats {
                            reportable_member_count: reportable_members.len(),
                            live_status_count: live_statuses
                                .values()
                                .filter(|status| status.is_some())
                                .count(),
                            timeout_us,
                            ..ClusterHealthTraceStats::default()
                        },
                    );
                }
                return Err(error);
            }
            Err(_) => {
                if let Some(live_probe) = live_probe {
                    live_probe.finish(
                        "timed_out",
                        ClusterHealthTraceStats {
                            reportable_member_count: reportable_members.len(),
                            live_status_count: live_statuses
                                .values()
                                .filter(|status| status.is_some())
                                .count(),
                            timeout_us,
                            ..ClusterHealthTraceStats::default()
                        },
                    );
                }
                // The probe writes each completed status into caller-owned
                // state, so cancellation retains useful rows while the
                // remaining members fall back to stored/unknown status below.
            }
        }
    } else if let Some(trace) = trace {
        trace.emit_instant(
            "cluster_health_live_probe_batch",
            "budget_exhausted",
            "",
            ClusterHealthTraceStats {
                reportable_member_count: reportable_members.len(),
                ..ClusterHealthTraceStats::default()
            },
        );
    }
    if let Some(trace) = trace {
        for member in &reportable_members {
            let live = live_statuses
                .get(&member.node_id)
                .is_some_and(|status| status.is_some());
            trace.emit_instant(
                "cluster_health_live_probe_result",
                if live { "observed" } else { "unresolved" },
                member.node_id.as_str(),
                ClusterHealthTraceStats {
                    reportable_member_count: reportable_members.len(),
                    live_status_count: usize::from(live),
                    ..ClusterHealthTraceStats::default()
                },
            );
        }
    }
    let mut rows = Vec::new();
    for member in &reportable_members {
        let expected_process = cluster_member_expected_daemon_process(member);
        let live = live_statuses
            .get(&member.node_id)
            .and_then(|status| status.clone())
            .filter(|status| cluster_node_status_matches_member(status, member, expected_process));
        let stored =
            stored_cluster_node_status_for_member(&stored_statuses, member, expected_process);
        let live_online = live.is_some();
        let availability = cluster_health_member_availability(
            state,
            member,
            &connectivity,
            live_online,
            stored.as_ref(),
        );
        let mut status = if let Some(live) = live {
            let mut live = live;
            if live.rollout.is_none() {
                live.rollout = stored.as_ref().and_then(|status| status.rollout.clone());
            }
            live
        } else if let Some(stored) = stored {
            stored
        } else {
            unknown_cluster_node_status(state, member, observed_at)
        };
        status.availability = availability.clone();
        rows.push(ClusterHealthRow {
            member: member.clone(),
            status,
            availability,
            live_probe_observed: live_online,
        });
    }
    if include_peer_views {
        merge_reachable_peer_health_rows_async_observed(state, &mut rows, probe, deadline, trace)
            .await?;
        if cluster_health_rows_have_unresolved_members(&rows)
            && tokio::time::Instant::now() < deadline
        {
            let retry_members = rows
                .iter()
                .filter(|row| row.availability != ClusterNodeAvailability::Online)
                .map(|row| row.member.clone())
                .collect::<Vec<_>>();
            let timeout_us = u64::try_from(
                deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_micros(),
            )
            .unwrap_or(u64::MAX);
            let retry_trace = trace.map(|trace| {
                ClusterHealthPhaseTrace::new(
                    trace,
                    "cluster_health_live_probe_retry",
                    "",
                    ClusterHealthTraceStats {
                        reportable_member_count: retry_members.len(),
                        timeout_us,
                        ..ClusterHealthTraceStats::from_rows(&rows)
                    },
                )
            });
            let mut retry_statuses = BTreeMap::new();
            let retry_result = tokio::time::timeout_at(
                deadline,
                probe.probe_member_statuses_async(state, &retry_members, &mut retry_statuses),
            )
            .await;
            let retry_live_count = retry_statuses
                .values()
                .filter(|status| status.is_some())
                .count();
            match retry_result {
                Ok(Ok(())) => {
                    if let Some(retry_trace) = retry_trace {
                        retry_trace.finish(
                            "completed",
                            ClusterHealthTraceStats {
                                reportable_member_count: retry_members.len(),
                                live_status_count: retry_live_count,
                                timeout_us,
                                ..ClusterHealthTraceStats::from_rows(&rows)
                            },
                        );
                    }
                }
                Ok(Err(error)) => {
                    if let Some(retry_trace) = retry_trace {
                        retry_trace.finish(
                            "error",
                            ClusterHealthTraceStats {
                                reportable_member_count: retry_members.len(),
                                live_status_count: retry_live_count,
                                timeout_us,
                                ..ClusterHealthTraceStats::from_rows(&rows)
                            },
                        );
                    }
                    return Err(error);
                }
                Err(_) => {
                    if let Some(retry_trace) = retry_trace {
                        retry_trace.finish(
                            "timed_out",
                            ClusterHealthTraceStats {
                                reportable_member_count: retry_members.len(),
                                live_status_count: retry_live_count,
                                timeout_us,
                                ..ClusterHealthTraceStats::from_rows(&rows)
                            },
                        );
                    }
                }
            }
            for row in &mut rows {
                if row.availability == ClusterNodeAvailability::Online {
                    continue;
                }
                let expected_process = cluster_member_expected_daemon_process(&row.member);
                let Some(mut live) = retry_statuses
                    .get(&row.member.node_id)
                    .and_then(|status| status.clone())
                    .filter(|status| {
                        cluster_node_status_matches_member(status, &row.member, expected_process)
                    })
                else {
                    continue;
                };
                if live.rollout.is_none() {
                    live.rollout = row.status.rollout.clone();
                }
                live.availability = ClusterNodeAvailability::Online;
                row.status = live;
                row.availability = ClusterNodeAvailability::Online;
                row.live_probe_observed = true;
            }
        }
    }
    rows.sort_by(|left, right| left.member.node_id.cmp(&right.member.node_id));
    Ok(rows)
}

pub fn cluster_doctor_findings(
    state: &ClusterBootstrapState,
    connectivity: &[ClusterConnectivityRecord],
    health: &[ClusterHealthRow],
) -> Vec<ClusterDoctorFinding> {
    let mut findings = Vec::new();
    let authority_health = cluster_state_authority_health(state, health);
    let unavailable_nodes = health
        .iter()
        .filter(|row| {
            matches!(
                row.availability,
                ClusterNodeAvailability::Offline | ClusterNodeAvailability::Unknown
            ) && !cluster_health_node_is_nonreportable_member(state, &row.member.node_id)
        })
        .map(|row| row.member.node_id.clone())
        .collect::<BTreeSet<_>>();
    for row in health {
        if cluster_health_node_is_nonreportable_member(state, &row.member.node_id) {
            continue;
        }
        if row.availability == ClusterNodeAvailability::Offline {
            let detail = format!(
                "{} is offline from {} using advertised endpoints",
                row.member.node_id, state.local_node_id
            );
            findings.push(if authority_health.has_quorum() {
                ClusterDoctorFinding::info(
                    "node-offline",
                    format!("{detail}; state authority quorum remains available"),
                )
            } else {
                ClusterDoctorFinding::warning("node-offline", detail)
            });
        } else if row.availability == ClusterNodeAvailability::Unknown {
            let detail = format!("{} has no recent reachable status", row.member.node_id);
            findings.push(if authority_health.has_quorum() {
                ClusterDoctorFinding::info(
                    "node-unknown",
                    format!("{detail}; state authority quorum remains available"),
                )
            } else {
                ClusterDoctorFinding::warning("node-unknown", detail)
            });
        }
    }

    let connectivity_graph = ClusterConnectivityGraph::from_records(
        state.local_node_id.clone(),
        connectivity.iter().cloned(),
    );
    let authority_members = state
        .members
        .iter()
        .filter(|member| {
            cluster_health_member_is_reportable(member)
                && !matches!(member.state, ClusterMemberState::Removed)
                && member.has_raft_vote()
        })
        .collect::<Vec<_>>();
    for source in &authority_members {
        for target in &authority_members {
            if source.node_id == target.node_id {
                continue;
            }
            // A retained member that is currently unavailable is expected to
            // have no live route. Once the remaining state authorities still
            // form quorum, the member observation above is sufficient and
            // route/endpoint duplicates must not degrade the cluster.
            if authority_health.has_quorum()
                && (unavailable_nodes.contains(&source.node_id)
                    || unavailable_nodes.contains(&target.node_id))
            {
                continue;
            }
            if cluster_route_next_hop(&connectivity_graph, &source.node_id, &target.node_id)
                .is_none()
            {
                findings.push(ClusterDoctorFinding::warning(
                    "authority-route-missing",
                    format!(
                        "{} has no reachable route to state authority {}",
                        source.node_id, target.node_id
                    ),
                ));
            }
        }
    }
    let online_nodes = health
        .iter()
        .filter(|row| {
            row.availability == ClusterNodeAvailability::Online
                && !cluster_health_node_is_nonreportable_member(state, &row.member.node_id)
        })
        .map(|row| row.member.node_id.clone())
        .collect::<BTreeSet<_>>();
    for record in connectivity {
        if cluster_health_node_is_nonreportable_member(state, &record.source_node_id)
            || cluster_health_node_is_nonreportable_member(state, &record.target_node_id)
        {
            continue;
        }
        if authority_health.has_quorum()
            && (unavailable_nodes.contains(&record.source_node_id)
                || unavailable_nodes.contains(&record.target_node_id))
        {
            continue;
        }
        if record.state == ClusterConnectivityState::Unreachable
            && !online_nodes.contains(&record.target_node_id)
            && cluster_route_next_hop(
                &connectivity_graph,
                &record.source_node_id,
                &record.target_node_id,
            )
            .is_none()
        {
            findings.push(ClusterDoctorFinding::warning(
                "endpoint-unreachable",
                format!(
                    "{} cannot reach {} at {} via {:?} ({} consecutive failures)",
                    record.source_node_id,
                    record.target_node_id,
                    record.address,
                    record.transport,
                    record.consecutive_failures
                ),
            ));
        }
    }

    for member in &state.members {
        if !cluster_health_member_is_reportable(member) {
            continue;
        }
        let mut by_identity = BTreeMap::<String, usize>::new();
        for endpoint in &member.endpoints {
            let identity = cluster_endpoint_module_url_identity(endpoint)
                .unwrap_or_else(|| endpoint.address.clone());
            *by_identity.entry(identity).or_default() += 1;
        }
        for (identity, count) in by_identity {
            if count > 1 {
                findings.push(ClusterDoctorFinding::warning(
                    "duplicate-endpoint",
                    format!(
                        "{} has {count} endpoint records for endpoint identity {identity}",
                        member.node_id
                    ),
                ));
            }
        }
    }

    let route_targets = ClusterEndpointSelector::advertised_peer_route_targets_for_state(state);
    if !route_targets.is_empty() {
        let route_ips = ClusterEndpointSelector::discover_peer_route_ip_candidates_for_state(state);
        if route_ips.is_empty() {
            findings.push(ClusterDoctorFinding::warning(
                "peer-route-unadvertised",
                format!(
                    "{} peer route target(s) exist, but no routable local source address could be discovered",
                    route_targets.len()
                ),
            ));
        }
    }

    findings.sort_by(|left, right| (&left.code, &left.detail).cmp(&(&right.code, &right.detail)));
    findings
}

fn cluster_health_node_is_nonreportable_member(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> bool {
    if *node_id == sinomo_node::shadow_daemon_node_id(&state.local_node_id) {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .is_some_and(|member| {
            matches!(member.state, ClusterMemberState::Removed)
                || !cluster_health_member_is_reportable(member)
        })
}

fn cluster_health_member_is_reportable(member: &ClusterMemberRecord) -> bool {
    !cluster_member_is_legacy_shadow_daemon(member)
}

fn cluster_health_member_is_in_scope(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    local_scope: bool,
) -> bool {
    !matches!(member.state, ClusterMemberState::Removed)
        && cluster_health_member_is_reportable(member)
        && (!local_scope || cluster_member_is_local_process(state, member))
}

pub fn unknown_cluster_node_status(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    observed_at: OffsetDateTime,
) -> ClusterNodeStatusRecord {
    ClusterNodeStatusRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: member.node_id.clone(),
        process: cluster_member_expected_daemon_process(member)
            .map(str::to_string)
            .unwrap_or_else(|| "unknown".to_string()),
        availability: ClusterNodeAvailability::Unknown,
        daemon_version: "unknown".to_string(),
        commit: None,
        target_os: None,
        target_arch: None,
        hostname: None,
        pid: None,
        address: unknown_status_fallback_address(member),
        bind_address: None,
        executable: None,
        rollout: None,
        daemon_started_at: None,
        observed_at,
    }
}

fn unknown_status_fallback_address(member: &ClusterMemberRecord) -> Option<String> {
    member
        .endpoints
        .iter()
        .find(|endpoint| {
            !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
                && cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
        })
        .or_else(|| {
            member
                .endpoints
                .iter()
                .find(|endpoint| !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly))
        })
        .or_else(|| member.endpoints.first())
        .map(|endpoint| endpoint.address.clone())
}

fn cluster_health_member_availability(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    connectivity: &ClusterConnectivityGraph,
    live_online: bool,
    stored: Option<&ClusterNodeStatusRecord>,
) -> ClusterNodeAvailability {
    if live_online {
        return ClusterNodeAvailability::Online;
    }
    if stored.is_some_and(|status| status.availability == ClusterNodeAvailability::Offline) {
        return ClusterNodeAvailability::Offline;
    }
    if cluster_health_member_unreachable_via_connectivity(state, member, connectivity) {
        return ClusterNodeAvailability::Offline;
    }
    ClusterNodeAvailability::Unknown
}

fn cluster_health_member_unreachable_via_connectivity(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    connectivity: &ClusterConnectivityGraph,
) -> bool {
    if cluster_member_is_local_process(state, member) {
        return false;
    }
    let mut has_probeable_endpoint = false;
    for endpoint in &member.endpoints {
        let probeable = endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
            && cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
            && matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::Loopback
                    | ClusterTransportKind::Iroh
            );
        if !probeable {
            continue;
        }
        has_probeable_endpoint = true;
        if connectivity.unavailability_rank(endpoint) == 0 {
            return false;
        }
    }
    has_probeable_endpoint
}

fn cluster_node_status_matches_member(
    status: &ClusterNodeStatusRecord,
    member: &ClusterMemberRecord,
    expected_process: Option<&str>,
) -> bool {
    status.node_id == member.node_id
        && expected_process
            .map(|process| status.process == process)
            .unwrap_or(true)
}

fn stored_cluster_node_status_for_member(
    statuses: &[ClusterNodeStatusRecord],
    member: &ClusterMemberRecord,
    expected_process: Option<&str>,
) -> Option<ClusterNodeStatusRecord> {
    statuses
        .iter()
        .find(|status| cluster_node_status_matches_member(status, member, expected_process))
        .cloned()
}

fn merge_reachable_peer_health_rows<P: ClusterStatusProbe + ?Sized>(
    state: &ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    probe: &P,
) {
    // Iterative frontier merge: peers that become known-online through another
    // peer's view are queried too. In a segmented topology liveness is only
    // observable transitively (node1 learns of the far segment from the
    // bridge's view), so a single pass over the initially-online peers misses
    // every node more than one bridge away.
    let mut queried = std::collections::BTreeSet::new();
    queried.insert(state.local_node_id.clone());
    let deadline = std::time::Instant::now() + CLUSTER_HEALTH_PEER_ROWS_TOTAL_BUDGET;
    while queried.len() <= CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS {
        if !cluster_health_rows_have_unresolved_members(rows) {
            return;
        }
        let Some(member) = next_online_peer_to_query(state, rows, &queried) else {
            return;
        };
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        // Peer views are queried sequentially, so give the preferred bridge the
        // entire remaining shared budget. Dividing by every possible peer can
        // make each slice shorter than the bounded nonrecursive health view,
        // guaranteeing that every useful response is cancelled. A fast
        // failure leaves the unspent portion for the next candidate.
        let timeout = remaining;
        queried.insert(member.node_id.clone());
        let Some(peer_rows) = probe.peer_health_rows(state, &member, timeout) else {
            continue;
        };
        merge_online_peer_health_rows(rows, peer_rows);
    }
}

/// The best not-yet-queried online peer: most non-local endpoints first
/// (bridges are dual-homed, so this prefers them), node id as tie-breaker.
fn next_online_peer_to_query(
    _state: &ClusterBootstrapState,
    rows: &[ClusterHealthRow],
    queried: &std::collections::BTreeSet<NodeId>,
) -> Option<ClusterMemberRecord> {
    rows.iter()
        .filter(|row| {
            row.availability == ClusterNodeAvailability::Online
                && row.live_probe_observed
                && !queried.contains(&row.member.node_id)
                && row
                    .member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        })
        .max_by_key(|row| {
            (
                row.member
                    .endpoints
                    .iter()
                    .filter(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
                    .count(),
                std::cmp::Reverse(row.member.node_id.clone()),
            )
        })
        .map(|row| row.member.clone())
}

#[cfg(test)]
async fn merge_reachable_peer_health_rows_async<P: ClusterStatusAsyncProbe + ?Sized>(
    state: &ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    probe: &P,
    live_deadline: tokio::time::Instant,
) -> Result<(), NetworkError> {
    merge_reachable_peer_health_rows_async_observed(state, rows, probe, live_deadline, None).await
}

async fn merge_reachable_peer_health_rows_async_observed<P: ClusterStatusAsyncProbe + ?Sized>(
    state: &ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    probe: &P,
    live_deadline: tokio::time::Instant,
    trace: Option<&ClusterHealthRequestTrace>,
) -> Result<(), NetworkError> {
    let mut queried = std::collections::BTreeSet::new();
    queried.insert(state.local_node_id.clone());
    let deadline =
        live_deadline.min(tokio::time::Instant::now() + CLUSTER_HEALTH_PEER_ROWS_TOTAL_BUDGET);
    while queried.len() <= CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS {
        if !cluster_health_rows_have_unresolved_members(rows) {
            if let Some(trace) = trace {
                trace.emit_instant(
                    "cluster_health_peer_frontier",
                    "all_resolved",
                    "",
                    ClusterHealthTraceStats::from_rows(rows),
                );
            }
            return Ok(());
        }
        let Some(member) = next_online_peer_to_query(state, rows, &queried) else {
            if let Some(trace) = trace {
                trace.emit_instant(
                    "cluster_health_peer_frontier",
                    "no_reachable_peer",
                    "",
                    ClusterHealthTraceStats::from_rows(rows),
                );
            }
            return Ok(());
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            if let Some(trace) = trace {
                trace.emit_instant(
                    "cluster_health_peer_frontier",
                    "budget_exhausted",
                    member.node_id.as_str(),
                    ClusterHealthTraceStats::from_rows(rows),
                );
            }
            return Ok(());
        }
        // Preserve the absolute deadline while letting the preferred bridge
        // finish one bounded nonrecursive view. Later peers receive whatever
        // remains after this attempt returns.
        let timeout = remaining;
        let timeout_us = u64::try_from(timeout.as_micros()).unwrap_or(u64::MAX);
        queried.insert(member.node_id.clone());
        let peer_query = trace.map(|trace| {
            ClusterHealthPhaseTrace::new(
                trace,
                "cluster_health_peer_query",
                member.node_id.to_string(),
                ClusterHealthTraceStats {
                    reportable_member_count: rows.len(),
                    timeout_us,
                    ..ClusterHealthTraceStats::from_rows(rows)
                },
            )
        });
        let peer_rows = match tokio::time::timeout(
            timeout,
            probe.peer_health_rows_async(state, &member, timeout),
        )
        .await
        {
            Ok(Ok(rows)) => rows,
            Ok(Err(error)) => {
                if let Some(peer_query) = peer_query {
                    peer_query.finish(
                        "error",
                        ClusterHealthTraceStats {
                            reportable_member_count: rows.len(),
                            timeout_us,
                            ..ClusterHealthTraceStats::from_rows(rows)
                        },
                    );
                }
                return Err(error);
            }
            Err(_) => {
                if let Some(peer_query) = peer_query {
                    peer_query.finish(
                        "timed_out",
                        ClusterHealthTraceStats {
                            reportable_member_count: rows.len(),
                            timeout_us,
                            ..ClusterHealthTraceStats::from_rows(rows)
                        },
                    );
                }
                continue;
            }
        };
        let Some(peer_rows) = peer_rows else {
            if let Some(peer_query) = peer_query {
                peer_query.finish(
                    "unavailable",
                    ClusterHealthTraceStats {
                        reportable_member_count: rows.len(),
                        timeout_us,
                        ..ClusterHealthTraceStats::from_rows(rows)
                    },
                );
            }
            continue;
        };
        let peer_row_count = peer_rows.len();
        let peer_live_count = peer_rows
            .iter()
            .filter(|row| {
                row.availability == ClusterNodeAvailability::Online && row.live_probe_observed
            })
            .count();
        let changed = merge_online_peer_health_rows(rows, peer_rows);
        if let Some(peer_query) = peer_query {
            let aggregate = ClusterHealthTraceStats::from_rows(rows);
            peer_query.finish(
                if changed { "merged" } else { "unchanged" },
                ClusterHealthTraceStats {
                    reportable_member_count: rows.len(),
                    live_status_count: peer_live_count,
                    row_count: peer_row_count,
                    online_count: aggregate.online_count,
                    unresolved_count: aggregate.unresolved_count,
                    timeout_us,
                },
            );
        }
    }
    if let Some(trace) = trace {
        trace.emit_instant(
            "cluster_health_peer_frontier",
            "peer_limit_reached",
            "",
            ClusterHealthTraceStats::from_rows(rows),
        );
    }
    Ok(())
}

fn cluster_health_rows_have_unresolved_members(rows: &[ClusterHealthRow]) -> bool {
    rows.iter()
        .any(|row| row.availability != ClusterNodeAvailability::Online)
}

fn merge_online_peer_health_rows(
    rows: &mut [ClusterHealthRow],
    peer_rows: Vec<ClusterHealthRow>,
) -> bool {
    let mut changed = false;
    let mut by_node = peer_rows
        .into_iter()
        .filter(|row| {
            row.availability == ClusterNodeAvailability::Online && row.live_probe_observed
        })
        .map(|row| (row.member.node_id.clone(), row))
        .collect::<BTreeMap<_, _>>();
    for row in rows {
        let Some(peer_row) = by_node.remove(&row.member.node_id) else {
            continue;
        };
        // Once both views already agree that the node is online, preserve the
        // newest status details. For unavailable rows, the explicit live-probe
        // provenance is stronger than timestamps from independently skewed
        // host clocks.
        if row.availability == ClusterNodeAvailability::Online
            && peer_row.status.observed_at <= row.status.observed_at
        {
            continue;
        }
        row.status = peer_row.status;
        row.status.availability = ClusterNodeAvailability::Online;
        row.availability = ClusterNodeAvailability::Online;
        row.live_probe_observed = true;
        changed = true;
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_object::Value;

    fn object_field<'a>(value: &'a Value, key: &str) -> &'a Value {
        let Value::Object(object) = value else {
            panic!("expected object value for field {key}, got {value:?}");
        };
        object
            .fields
            .iter()
            .find(|field| field.key == key)
            .map(|field| &field.value)
            .unwrap_or_else(|| panic!("missing object field {key} in {object:?}"))
    }

    fn assert_native_bytes_field(value: &impl serde::Serialize, key: &str) {
        let value = sinomo_object::serde_ext::to_value(value).expect("serialize value");
        let field = object_field(&value, key);
        assert!(
            matches!(field, Value::Bytes(_)),
            "expected {key} to serialize as native bytes, got {field:?}"
        );
    }

    #[test]
    fn cluster_network_ping_payloads_use_native_bytes_codec() {
        let payload = vec![0, 1, 2, 3, 253, 254, 255];
        assert_native_bytes_field(
            &ClusterNetworkPingRequest {
                nonce: "nonce".to_string(),
                payload: payload.clone(),
                response_payload_bytes: payload.len(),
            },
            "payload",
        );
        assert_native_bytes_field(
            &ClusterNetworkPingResponse {
                nonce: "nonce".to_string(),
                responder_node_id: NodeId::from_legacy_named("node").expect("node id"),
                payload_bytes: payload.len(),
                response_payload: payload,
                observed_at: OffsetDateTime::UNIX_EPOCH,
            },
            "response_payload",
        );
    }

    #[test]
    fn cluster_network_ping_requests_are_response_retry_safe() {
        let request = cluster_network_ping_rpc_request("retry-safe", 0).expect("ping request");

        assert!(
            request.is_response_retry_safe(),
            "a read-only nonce ping can safely retry after an ambiguous response timeout"
        );
    }

    fn health_test_member(
        cluster_id: &ClusterId,
        node_id: &NodeId,
        name: &str,
        now: OffsetDateTime,
    ) -> ClusterMemberRecord {
        let mut member =
            ClusterMemberRecord::local_voter(cluster_id.clone(), node_id.clone(), name, "pk", now);
        member
            .capabilities
            .push(CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("capability"));
        member
    }

    fn health_test_status(
        cluster_id: &ClusterId,
        node_id: &NodeId,
        commit: &str,
        observed_at: OffsetDateTime,
    ) -> ClusterNodeStatusRecord {
        ClusterNodeStatusRecord {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            process: "daemon-main".to_string(),
            availability: ClusterNodeAvailability::Online,
            daemon_version: "0.1.0".to_string(),
            commit: Some(commit.to_string()),
            target_os: Some("linux".to_string()),
            target_arch: Some("x86_64".to_string()),
            hostname: None,
            pid: None,
            address: Some("127.0.0.1:21353".to_string()),
            bind_address: None,
            executable: None,
            rollout: None,
            daemon_started_at: None,
            observed_at,
        }
    }

    fn health_test_row(
        member: &ClusterMemberRecord,
        status: ClusterNodeStatusRecord,
    ) -> ClusterHealthRow {
        ClusterHealthRow {
            member: member.clone(),
            status,
            availability: ClusterNodeAvailability::Online,
            live_probe_observed: true,
        }
    }

    #[test]
    fn cluster_health_row_defaults_missing_live_probe_provenance_to_false() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_health_legacy_row").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_health_legacy_row").expect("node");
        let now = OffsetDateTime::now_utc();
        let member = health_test_member(&cluster_id, &node_id, "node", now);
        let row = health_test_row(
            &member,
            health_test_status(&cluster_id, &node_id, "legacy", now),
        );
        let mut encoded = serde_json::to_value(row).expect("encode health row");
        encoded
            .as_object_mut()
            .expect("health row object")
            .remove("live_probe_observed");

        let decoded: ClusterHealthRow =
            serde_json::from_value(encoded).expect("decode legacy health row");

        assert!(!decoded.live_probe_observed);
    }

    #[test]
    fn peer_health_merge_refreshes_online_row_when_peer_status_is_newer() {
        let cluster_id = ClusterId::from_legacy_named("cluster_health_refresh").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_health_refresh").expect("node");
        let now = OffsetDateTime::now_utc();
        let member = health_test_member(&cluster_id, &node_id, "node", now);
        let mut rows = vec![health_test_row(
            &member,
            health_test_status(
                &cluster_id,
                &node_id,
                "old",
                now - time::Duration::seconds(60),
            ),
        )];
        let peer_rows = vec![health_test_row(
            &member,
            health_test_status(&cluster_id, &node_id, "new", now),
        )];

        assert!(merge_online_peer_health_rows(&mut rows, peer_rows));
        assert_eq!(rows[0].status.commit.as_deref(), Some("new"));
        assert_eq!(rows[0].availability, ClusterNodeAvailability::Online);

        let older_peer_rows = vec![health_test_row(
            &member,
            health_test_status(
                &cluster_id,
                &node_id,
                "older",
                now - time::Duration::seconds(120),
            ),
        )];
        assert!(!merge_online_peer_health_rows(&mut rows, older_peer_rows));
        assert_eq!(rows[0].status.commit.as_deref(), Some("new"));
    }

    #[test]
    fn peer_health_merge_requires_live_probe_evidence_to_restore_online() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_health_restore_online").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_health_restore_online").expect("node");
        let now = OffsetDateTime::now_utc();
        let member = health_test_member(&cluster_id, &node_id, "node", now);

        for availability in [
            ClusterNodeAvailability::Offline,
            ClusterNodeAvailability::Unknown,
        ] {
            let mut unavailable_status = health_test_status(&cluster_id, &node_id, "cached", now);
            unavailable_status.availability = availability.clone();
            let unavailable_row = ClusterHealthRow {
                member: member.clone(),
                status: unavailable_status,
                availability: availability.clone(),
                live_probe_observed: false,
            };

            let mut repeated_cached_online = health_test_row(
                &member,
                health_test_status(&cluster_id, &node_id, "cached", now),
            );
            repeated_cached_online.live_probe_observed = false;
            let mut rows = vec![unavailable_row.clone()];
            assert!(!merge_online_peer_health_rows(
                &mut rows,
                vec![repeated_cached_online]
            ));
            assert_eq!(rows[0].availability, availability);

            for live_observed_at in [now, now - time::Duration::seconds(1)] {
                let mut rows = vec![unavailable_row.clone()];
                let live_online = vec![health_test_row(
                    &member,
                    health_test_status(&cluster_id, &node_id, "live", live_observed_at),
                )];
                assert!(merge_online_peer_health_rows(&mut rows, live_online));
                assert_eq!(rows[0].availability, ClusterNodeAvailability::Online);
                assert!(rows[0].live_probe_observed);
                assert_eq!(rows[0].status.commit.as_deref(), Some("live"));
            }
        }
    }

    struct StaticPeerHealthProbe {
        rows_by_peer: BTreeMap<NodeId, Vec<ClusterHealthRow>>,
    }

    impl ClusterStatusProbe for StaticPeerHealthProbe {
        fn probe_member_status(
            &self,
            _state: &ClusterBootstrapState,
            _member: &ClusterMemberRecord,
        ) -> Option<ClusterNodeStatusRecord> {
            None
        }

        fn peer_health_rows(
            &self,
            _state: &ClusterBootstrapState,
            member: &ClusterMemberRecord,
            _timeout: std::time::Duration,
        ) -> Option<Vec<ClusterHealthRow>> {
            self.rows_by_peer.get(&member.node_id).cloned()
        }
    }

    fn add_health_test_lan_endpoint(member: &mut ClusterMemberRecord, address: impl Into<String>) {
        member.endpoints.push(ClusterEndpointRecord {
            endpoint_id: sinomo_base::Id::generate(),
            node_id: member.node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: address.into(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        });
    }

    fn peer_budget_test_fixture(
        name: &str,
    ) -> (
        ClusterBootstrapState,
        Vec<ClusterHealthRow>,
        NodeId,
        NodeId,
        ClusterHealthRow,
    ) {
        let cluster_id = ClusterId::from_legacy_named(format!("cluster_health_peer_budget_{name}"))
            .expect("cluster");
        let local_id = NodeId::from_legacy_named(format!("node_peer_budget_{name}_local"))
            .expect("local node");
        let bridge_id = NodeId::from_legacy_named(format!("node_peer_budget_{name}_bridge"))
            .expect("bridge node");
        let decoy_a_id =
            NodeId::from_legacy_named(format!("node_peer_budget_{name}_decoy_a")).expect("decoy a");
        let decoy_b_id =
            NodeId::from_legacy_named(format!("node_peer_budget_{name}_decoy_b")).expect("decoy b");
        let deep_id =
            NodeId::from_legacy_named(format!("node_peer_budget_{name}_deep")).expect("deep node");
        let now = OffsetDateTime::now_utc();
        let local = health_test_member(&cluster_id, &local_id, "local", now);
        let mut bridge = health_test_member(&cluster_id, &bridge_id, "bridge", now);
        add_health_test_lan_endpoint(&mut bridge, "bridge-a:21353");
        add_health_test_lan_endpoint(&mut bridge, "bridge-b:21353");
        let decoy_a = health_test_member(&cluster_id, &decoy_a_id, "decoy-a", now);
        let decoy_b = health_test_member(&cluster_id, &decoy_b_id, "decoy-b", now);
        let deep = health_test_member(&cluster_id, &deep_id, "deep", now);
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: format!("peer budget {name}"),
            local_node_id: local_id.clone(),
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![
                local.clone(),
                bridge.clone(),
                decoy_a.clone(),
                decoy_b.clone(),
                deep.clone(),
            ],
            initialized_at: now,
            updated_at: now,
        };
        let mut deep_unknown = health_test_status(&cluster_id, &deep_id, "unknown", now);
        deep_unknown.availability = ClusterNodeAvailability::Unknown;
        let rows = vec![
            health_test_row(
                &local,
                health_test_status(&cluster_id, &local_id, "local", now),
            ),
            health_test_row(
                &bridge,
                health_test_status(&cluster_id, &bridge_id, "bridge", now),
            ),
            health_test_row(
                &decoy_a,
                health_test_status(&cluster_id, &decoy_a_id, "decoy-a", now),
            ),
            health_test_row(
                &decoy_b,
                health_test_status(&cluster_id, &decoy_b_id, "decoy-b", now),
            ),
            ClusterHealthRow {
                member: deep.clone(),
                status: deep_unknown,
                availability: ClusterNodeAvailability::Unknown,
                live_probe_observed: false,
            },
        ];
        let deep_row = health_test_row(
            &deep,
            health_test_status(&cluster_id, &deep_id, "via-preferred-bridge", now),
        );
        (state, rows, bridge_id, deep_id, deep_row)
    }

    #[test]
    fn preferred_bridge_receives_full_remaining_sync_peer_budget() {
        struct BudgetSensitivePeerView {
            bridge_node_id: NodeId,
            deep_row: ClusterHealthRow,
            attempts: std::sync::Mutex<Vec<(NodeId, std::time::Duration)>>,
        }

        impl ClusterStatusProbe for BudgetSensitivePeerView {
            fn probe_member_status(
                &self,
                _state: &ClusterBootstrapState,
                _member: &ClusterMemberRecord,
            ) -> Option<ClusterNodeStatusRecord> {
                None
            }

            fn peer_health_rows(
                &self,
                _state: &ClusterBootstrapState,
                member: &ClusterMemberRecord,
                timeout: std::time::Duration,
            ) -> Option<Vec<ClusterHealthRow>> {
                self.attempts
                    .lock()
                    .expect("peer attempts")
                    .push((member.node_id.clone(), timeout));
                (member.node_id == self.bridge_node_id
                    && timeout >= std::time::Duration::from_millis(1_500))
                .then(|| vec![self.deep_row.clone()])
            }
        }

        let (state, mut rows, bridge_id, deep_id, deep_row) = peer_budget_test_fixture("sync");
        let probe = BudgetSensitivePeerView {
            bridge_node_id: bridge_id.clone(),
            deep_row,
            attempts: std::sync::Mutex::new(Vec::new()),
        };

        merge_reachable_peer_health_rows(&state, &mut rows, &probe);

        assert_eq!(
            rows.iter()
                .find(|row| row.member.node_id == deep_id)
                .expect("deep row")
                .availability,
            ClusterNodeAvailability::Online
        );
        let attempts = probe.attempts.lock().expect("peer attempts");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].0, bridge_id);
        assert!(attempts[0].1 >= std::time::Duration::from_millis(1_500));
    }

    #[tokio::test]
    async fn preferred_bridge_receives_full_remaining_async_peer_budget() {
        struct BudgetSensitiveAsyncPeerView {
            bridge_node_id: NodeId,
            deep_row: ClusterHealthRow,
            attempts: std::sync::Mutex<Vec<(NodeId, std::time::Duration)>>,
        }

        impl ClusterStatusAsyncProbe for BudgetSensitiveAsyncPeerView {
            fn probe_member_status_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                _member: &'a ClusterMemberRecord,
            ) -> ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>> {
                Box::pin(async { Ok(None) })
            }

            fn peer_health_rows_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                member: &'a ClusterMemberRecord,
                timeout: std::time::Duration,
            ) -> ClusterMembershipRpcFuture<'a, Option<Vec<ClusterHealthRow>>> {
                self.attempts
                    .lock()
                    .expect("peer attempts")
                    .push((member.node_id.clone(), timeout));
                let can_finish = member.node_id == self.bridge_node_id
                    && timeout >= std::time::Duration::from_millis(1_500);
                let deep_row = self.deep_row.clone();
                Box::pin(async move { Ok(can_finish.then(|| vec![deep_row])) })
            }
        }

        let (state, mut rows, bridge_id, deep_id, deep_row) = peer_budget_test_fixture("async");
        let probe = BudgetSensitiveAsyncPeerView {
            bridge_node_id: bridge_id.clone(),
            deep_row,
            attempts: std::sync::Mutex::new(Vec::new()),
        };
        let started = tokio::time::Instant::now();

        merge_reachable_peer_health_rows_async(
            &state,
            &mut rows,
            &probe,
            tokio::time::Instant::now() + CLUSTER_HEALTH_PEER_ROWS_TOTAL_BUDGET,
        )
        .await
        .expect("peer expansion");

        assert!(started.elapsed() < CLUSTER_HEALTH_PEER_ROWS_TOTAL_BUDGET);
        assert_eq!(
            rows.iter()
                .find(|row| row.member.node_id == deep_id)
                .expect("deep row")
                .availability,
            ClusterNodeAvailability::Online
        );
        let attempts = probe.attempts.lock().expect("peer attempts");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].0, bridge_id);
        assert!(attempts[0].1 >= std::time::Duration::from_millis(1_500));
    }

    #[test]
    fn reachable_peer_health_refreshes_stale_online_rows() {
        let cluster_id =
            ClusterId::from_legacy_named("cluster_health_all_online_refresh").expect("cluster");
        let local_id =
            NodeId::from_legacy_named("node_health_all_online_local").expect("local node");
        let remote_id =
            NodeId::from_legacy_named("node_health_all_online_remote").expect("remote node");
        let unresolved_id = NodeId::from_legacy_named("node_health_unresolved").expect("node");
        let now = OffsetDateTime::now_utc();
        let local_member = health_test_member(&cluster_id, &local_id, "local", now);
        let remote_member = health_test_member(&cluster_id, &remote_id, "remote", now);
        let unresolved_member = health_test_member(&cluster_id, &unresolved_id, "unresolved", now);
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: "health refresh".to_string(),
            local_node_id: local_id.clone(),
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![
                local_member.clone(),
                remote_member.clone(),
                unresolved_member.clone(),
            ],
            initialized_at: now,
            updated_at: now,
        };
        let mut rows = vec![
            health_test_row(
                &local_member,
                health_test_status(&cluster_id, &local_id, "local", now),
            ),
            health_test_row(
                &remote_member,
                health_test_status(
                    &cluster_id,
                    &remote_id,
                    "stale",
                    now - time::Duration::seconds(60),
                ),
            ),
            ClusterHealthRow {
                member: unresolved_member.clone(),
                status: {
                    let mut status =
                        health_test_status(&cluster_id, &unresolved_id, "unknown", now);
                    status.availability = ClusterNodeAvailability::Unknown;
                    status
                },
                availability: ClusterNodeAvailability::Unknown,
                live_probe_observed: false,
            },
        ];
        let probe = StaticPeerHealthProbe {
            rows_by_peer: BTreeMap::from([(
                remote_id.clone(),
                vec![
                    health_test_row(
                        &remote_member,
                        health_test_status(&cluster_id, &remote_id, "fresh", now),
                    ),
                    health_test_row(
                        &unresolved_member,
                        health_test_status(&cluster_id, &unresolved_id, "resolved", now),
                    ),
                ],
            )]),
        };

        merge_reachable_peer_health_rows(&state, &mut rows, &probe);

        let remote = rows
            .iter()
            .find(|row| row.member.node_id == remote_id)
            .expect("remote row");
        assert_eq!(remote.status.commit.as_deref(), Some("fresh"));
        assert_eq!(
            rows.iter()
                .find(|row| row.member.node_id == unresolved_id)
                .expect("resolved row")
                .availability,
            ClusterNodeAvailability::Online
        );
    }

    #[test]
    fn peer_health_expansion_skips_fully_resolved_rows() {
        struct NoPeerCalls;

        impl ClusterStatusProbe for NoPeerCalls {
            fn probe_member_status(
                &self,
                _state: &ClusterBootstrapState,
                _member: &ClusterMemberRecord,
            ) -> Option<ClusterNodeStatusRecord> {
                None
            }

            fn peer_health_rows(
                &self,
                _state: &ClusterBootstrapState,
                _member: &ClusterMemberRecord,
                _timeout: std::time::Duration,
            ) -> Option<Vec<ClusterHealthRow>> {
                panic!("resolved health must not query peer views")
            }
        }

        let cluster_id = ClusterId::from_legacy_named("cluster_health_resolved").expect("cluster");
        let local_id = NodeId::from_legacy_named("node_health_resolved_local").expect("local");
        let remote_id = NodeId::from_legacy_named("node_health_resolved_remote").expect("remote");
        let now = OffsetDateTime::now_utc();
        let local = health_test_member(&cluster_id, &local_id, "local", now);
        let remote = health_test_member(&cluster_id, &remote_id, "remote", now);
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: "resolved health".to_string(),
            local_node_id: local_id.clone(),
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![local.clone(), remote.clone()],
            initialized_at: now,
            updated_at: now,
        };
        let mut rows = vec![
            health_test_row(
                &local,
                health_test_status(&cluster_id, &local_id, "local", now),
            ),
            health_test_row(
                &remote,
                health_test_status(&cluster_id, &remote_id, "remote", now),
            ),
        ];

        merge_reachable_peer_health_rows(&state, &mut rows, &NoPeerCalls);
    }

    #[tokio::test]
    async fn delayed_two_hop_peer_health_expansion_reaches_deep_node() {
        struct DelayedPeerViews {
            rows_by_peer: BTreeMap<NodeId, Vec<ClusterHealthRow>>,
            attempted_timeouts: std::sync::Mutex<Vec<std::time::Duration>>,
        }

        impl ClusterStatusAsyncProbe for DelayedPeerViews {
            fn probe_member_status_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                _member: &'a ClusterMemberRecord,
            ) -> ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>> {
                Box::pin(async { Ok(None) })
            }

            fn peer_health_rows_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                member: &'a ClusterMemberRecord,
                timeout: std::time::Duration,
            ) -> ClusterMembershipRpcFuture<'a, Option<Vec<ClusterHealthRow>>> {
                self.attempted_timeouts
                    .lock()
                    .expect("timeout observations")
                    .push(timeout);
                let rows = self.rows_by_peer.get(&member.node_id).cloned();
                Box::pin(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    Ok(rows)
                })
            }
        }

        let cluster_id = ClusterId::from_legacy_named("cluster_health_two_hop").expect("cluster");
        let local_id = NodeId::from_legacy_named("node_health_two_hop_local").expect("local");
        let bridge_a_id =
            NodeId::from_legacy_named("node_health_two_hop_bridge_a").expect("bridge a");
        let bridge_b_id =
            NodeId::from_legacy_named("node_health_two_hop_bridge_b").expect("bridge b");
        let deep_id = NodeId::from_legacy_named("node_health_two_hop_deep").expect("deep");
        let now = OffsetDateTime::now_utc();
        let local = health_test_member(&cluster_id, &local_id, "local", now);
        let bridge_a = health_test_member(&cluster_id, &bridge_a_id, "bridge-a", now);
        let bridge_b = health_test_member(&cluster_id, &bridge_b_id, "bridge-b", now);
        let deep = health_test_member(&cluster_id, &deep_id, "deep", now);
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: "two hop health".to_string(),
            local_node_id: local_id.clone(),
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![
                local.clone(),
                bridge_a.clone(),
                bridge_b.clone(),
                deep.clone(),
            ],
            initialized_at: now,
            updated_at: now,
        };
        let unresolved_row = |member: &ClusterMemberRecord| {
            let mut status = health_test_status(&cluster_id, &member.node_id, "unknown", now);
            status.availability = ClusterNodeAvailability::Unknown;
            ClusterHealthRow {
                member: member.clone(),
                status,
                availability: ClusterNodeAvailability::Unknown,
                live_probe_observed: false,
            }
        };
        let mut rows = vec![
            health_test_row(
                &local,
                health_test_status(&cluster_id, &local_id, "local", now),
            ),
            health_test_row(
                &bridge_a,
                health_test_status(&cluster_id, &bridge_a_id, "bridge-a", now),
            ),
            unresolved_row(&bridge_b),
            unresolved_row(&deep),
        ];
        let probe = DelayedPeerViews {
            rows_by_peer: BTreeMap::from([
                (
                    bridge_a_id,
                    vec![health_test_row(
                        &bridge_b,
                        health_test_status(&cluster_id, &bridge_b_id, "bridge-b", now),
                    )],
                ),
                (
                    bridge_b_id,
                    vec![health_test_row(
                        &deep,
                        health_test_status(&cluster_id, &deep_id, "deep", now),
                    )],
                ),
            ]),
            attempted_timeouts: std::sync::Mutex::new(Vec::new()),
        };

        merge_reachable_peer_health_rows_async(
            &state,
            &mut rows,
            &probe,
            tokio::time::Instant::now() + CLUSTER_HEALTH_LIVE_TOTAL_BUDGET,
        )
        .await
        .expect("two-hop peer expansion");

        assert!(
            rows.iter()
                .all(|row| row.availability == ClusterNodeAvailability::Online),
            "both bridge views should resolve the deep member"
        );
        let attempted_timeouts = probe
            .attempted_timeouts
            .lock()
            .expect("timeout observations");
        assert_eq!(attempted_timeouts.len(), 2);
        assert!(
            attempted_timeouts[0] > CLUSTER_HEALTH_PEER_ROWS_TOTAL_BUDGET / 2,
            "the preferred bridge should receive enough of the shared deadline"
        );
        assert!(
            attempted_timeouts[1] <= attempted_timeouts[0],
            "the next bridge should receive only the remaining shared budget"
        );
    }

    #[tokio::test]
    async fn stalled_live_probe_returns_completed_statuses_within_shared_budget() {
        struct StalledAfterLocalProbe {
            local_node_id: NodeId,
            cluster_id: ClusterId,
            observed_at: OffsetDateTime,
        }

        impl ClusterStatusAsyncProbe for StalledAfterLocalProbe {
            fn probe_member_status_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                member: &'a ClusterMemberRecord,
            ) -> ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>> {
                let status = (member.node_id == self.local_node_id).then(|| {
                    health_test_status(
                        &self.cluster_id,
                        &self.local_node_id,
                        "completed-before-timeout",
                        self.observed_at,
                    )
                });
                Box::pin(async move {
                    if status.is_none() {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    Ok(status)
                })
            }
        }

        let root = sinomo_testdir::TestDir::new("sinomo-cluster-health-stalled-live-probe");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_health_stalled_live_probe").expect("cluster");
        let local_id =
            NodeId::from_legacy_named("node_health_stalled_live_probe_local").expect("local");
        let remote_id =
            NodeId::from_legacy_named("node_health_stalled_live_probe_remote").expect("remote");
        let local = health_test_member(&cluster_id, &local_id, "local", now);
        let remote = health_test_member(&cluster_id, &remote_id, "remote", now);
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: "stalled live probe".to_string(),
            local_node_id: local_id.clone(),
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![local, remote],
            initialized_at: now,
            updated_at: now,
        };
        let probe = StalledAfterLocalProbe {
            local_node_id: local_id.clone(),
            cluster_id,
            observed_at: now,
        };

        let started = tokio::time::Instant::now();
        let rows = cluster_health_rows_with_async_probe_for_scope_with_budget(
            &paths,
            &state,
            &probe,
            false,
            false,
            now,
            std::time::Duration::from_millis(75),
        )
        .await
        .expect("partial health rows");

        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "stalled member must not overrun the shared health budget"
        );
        let local_row = rows
            .iter()
            .find(|row| row.member.node_id == local_id)
            .expect("local row");
        assert_eq!(local_row.availability, ClusterNodeAvailability::Online);
        assert_eq!(
            local_row.status.commit.as_deref(),
            Some("completed-before-timeout"),
            "a status completed before cancellation must be retained"
        );
        let remote_row = rows
            .iter()
            .find(|row| row.member.node_id == remote_id)
            .expect("remote row");
        assert_eq!(remote_row.availability, ClusterNodeAvailability::Unknown);
    }

    #[tokio::test]
    async fn stalled_deep_probe_preserves_budget_for_useful_bridge_peer_view() {
        struct StalledDeepWithBridgeView {
            cluster_id: ClusterId,
            local_node_id: NodeId,
            bridge_node_id: NodeId,
            deep_node_id: NodeId,
            deep_member: ClusterMemberRecord,
            observed_at: OffsetDateTime,
            peer_calls: std::sync::Mutex<usize>,
        }

        impl ClusterStatusAsyncProbe for StalledDeepWithBridgeView {
            fn probe_member_status_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                member: &'a ClusterMemberRecord,
            ) -> ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>> {
                let stalled = member.node_id == self.deep_node_id;
                let status = (!stalled).then(|| {
                    let commit = if member.node_id == self.local_node_id {
                        "local-direct"
                    } else {
                        "bridge-direct"
                    };
                    health_test_status(&self.cluster_id, &member.node_id, commit, self.observed_at)
                });
                Box::pin(async move {
                    if stalled {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    Ok(status)
                })
            }

            fn peer_health_rows_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                member: &'a ClusterMemberRecord,
                _timeout: std::time::Duration,
            ) -> ClusterMembershipRpcFuture<'a, Option<Vec<ClusterHealthRow>>> {
                assert_eq!(member.node_id, self.bridge_node_id);
                *self.peer_calls.lock().expect("peer calls") += 1;
                let row = health_test_row(
                    &self.deep_member,
                    health_test_status(
                        &self.cluster_id,
                        &self.deep_node_id,
                        "via-bridge",
                        self.observed_at,
                    ),
                );
                Box::pin(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    Ok(Some(vec![row]))
                })
            }
        }

        let root = sinomo_testdir::TestDir::new("sinomo-cluster-health-stalled-deep-bridge");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_health_stalled_deep_bridge").expect("cluster");
        let local_id =
            NodeId::from_legacy_named("node_health_stalled_deep_bridge_local").expect("local");
        let bridge_id =
            NodeId::from_legacy_named("node_health_stalled_deep_bridge_bridge").expect("bridge");
        let deep_id =
            NodeId::from_legacy_named("node_health_stalled_deep_bridge_deep").expect("deep");
        let local = health_test_member(&cluster_id, &local_id, "local", now);
        let bridge = health_test_member(&cluster_id, &bridge_id, "bridge", now);
        let deep = health_test_member(&cluster_id, &deep_id, "deep", now);
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: "stalled deep bridge".to_string(),
            local_node_id: local_id.clone(),
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![local, bridge, deep.clone()],
            initialized_at: now,
            updated_at: now,
        };
        let probe = StalledDeepWithBridgeView {
            cluster_id,
            local_node_id: local_id,
            bridge_node_id: bridge_id,
            deep_node_id: deep_id.clone(),
            deep_member: deep,
            observed_at: now,
            peer_calls: std::sync::Mutex::new(0),
        };

        let started = tokio::time::Instant::now();
        let rows = cluster_health_rows_with_async_probe_for_scope_with_budget(
            &paths,
            &state,
            &probe,
            true,
            false,
            now,
            std::time::Duration::from_millis(200),
        )
        .await
        .expect("bridge-expanded health rows");

        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "recursive health must remain within its shared budget"
        );
        assert_eq!(*probe.peer_calls.lock().expect("peer calls"), 1);
        let deep_row = rows
            .iter()
            .find(|row| row.member.node_id == deep_id)
            .expect("deep row");
        assert_eq!(deep_row.availability, ClusterNodeAvailability::Online);
        assert_eq!(deep_row.status.commit.as_deref(), Some("via-bridge"));
    }

    #[tokio::test]
    async fn peer_hedge_retries_slow_direct_member_when_no_peer_view_is_available() {
        struct SlowThenResponsiveProbe {
            cluster_id: ClusterId,
            remote_node_id: NodeId,
            observed_at: OffsetDateTime,
            remote_attempts: std::sync::atomic::AtomicUsize,
        }

        impl ClusterStatusAsyncProbe for SlowThenResponsiveProbe {
            fn probe_member_status_async<'a>(
                &'a self,
                _state: &'a ClusterBootstrapState,
                member: &'a ClusterMemberRecord,
            ) -> ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>> {
                let is_remote = member.node_id == self.remote_node_id;
                let attempt = is_remote.then(|| {
                    self.remote_attempts
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                });
                let status = health_test_status(
                    &self.cluster_id,
                    &member.node_id,
                    if is_remote {
                        "remote-direct-retry"
                    } else {
                        "local-direct"
                    },
                    self.observed_at,
                );
                Box::pin(async move {
                    if attempt == Some(0) {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    } else if attempt.is_some() {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    Ok(Some(status))
                })
            }
        }

        let root = sinomo_testdir::TestDir::new("sinomo-cluster-health-peer-hedge-direct-retry");
        let data_dir = SinomoDataDir::new(&root);
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_health_peer_hedge_direct_retry")
            .expect("cluster");
        let local_id =
            NodeId::from_legacy_named("node_health_peer_hedge_direct_retry_local").expect("local");
        let remote_id = NodeId::from_legacy_named("node_health_peer_hedge_direct_retry_remote")
            .expect("remote");
        let state = ClusterBootstrapState {
            cluster_id: cluster_id.clone(),
            cluster_name: "peer hedge direct retry".to_string(),
            local_node_id: local_id.clone(),
            local_node_display_name: "local".to_string(),
            term: 1,
            kv_revision: 1,
            members: vec![
                health_test_member(&cluster_id, &local_id, "local", now),
                health_test_member(&cluster_id, &remote_id, "remote", now),
            ],
            initialized_at: now,
            updated_at: now,
        };
        let probe = SlowThenResponsiveProbe {
            cluster_id,
            remote_node_id: remote_id.clone(),
            observed_at: now,
            remote_attempts: std::sync::atomic::AtomicUsize::new(0),
        };

        let started = tokio::time::Instant::now();
        let rows = cluster_health_rows_with_async_probe_for_scope_with_budget(
            &paths,
            &state,
            &probe,
            true,
            false,
            now,
            std::time::Duration::from_millis(350),
        )
        .await
        .expect("hedged direct retry rows");

        assert!(started.elapsed() < std::time::Duration::from_millis(500));
        assert_eq!(
            probe
                .remote_attempts
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a peer hedge must fall back to the remaining direct-probe budget"
        );
        let remote_row = rows
            .iter()
            .find(|row| row.member.node_id == remote_id)
            .expect("remote row");
        assert_eq!(remote_row.availability, ClusterNodeAvailability::Online);
        assert_eq!(
            remote_row.status.commit.as_deref(),
            Some("remote-direct-retry")
        );
    }

    #[test]
    fn non_recursive_peer_view_request_keeps_live_probes_enabled() {
        let request =
            cluster_health_rpc_request_without_recursive_peer_views().expect("peer view request");
        assert!(!cluster_health_rpc_request_includes_peer_views(&request));
        assert!(cluster_health_rpc_request_uses_live_probes(&request));
    }
}
