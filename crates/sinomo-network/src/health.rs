use crate::prelude::*;
use crate::route::cluster_route_next_hop;
use crate::rpc::{
    CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, CLUSTER_HEALTH_SCOPE_LOCAL,
    CLUSTER_HEALTH_SCOPE_METADATA_KEY, CLUSTER_NETWORK_PING_ROUTE, CLUSTER_NETWORK_RPC_CATEGORY,
    ObjectRpcRequest, ObjectRpcResponse,
};
#[cfg(test)]
use crate::storage::read_cluster_state;
use crate::storage::{
    ClusterMembershipRpcFuture, DAEMON_MAIN_RUN_CAPABILITY, read_cluster_node_statuses_from_kv,
    read_cluster_state_async,
};
use crate::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityState, ClusterPaths,
    NetworkError, cluster_member_expected_daemon_process, cluster_member_is_legacy_shadow_daemon,
    iroh_endpoint_public_key, live_cluster_member_probe_possible,
};
use crate::{advertised_peer_route_targets, discover_peer_route_ip_candidates};

const CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterHealthRow {
    pub member: ClusterMemberRecord,
    pub status: ClusterNodeStatusRecord,
    pub availability: ClusterNodeAvailability,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterNetworkPingRequest {
    pub nonce: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "serde_bytes")]
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub response_payload_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterNetworkPingResponse {
    pub nonce: String,
    pub responder_node_id: NodeId,
    pub payload_bytes: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "serde_bytes")]
    pub response_payload: Vec<u8>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

pub fn cluster_health_rpc_request() -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, &())
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
            payload: vec![0_u8; payload_bytes],
            response_payload_bytes,
        },
    )
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

#[cfg(test)]
pub(crate) fn handle_cluster_health_rpc_request<P: ClusterStatusProbe + ?Sized>(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    probe: &P,
    request: ObjectRpcRequest,
    observed_at: OffsetDateTime,
) -> Result<ObjectRpcResponse, NetworkError> {
    match request.route.as_str() {
        CLUSTER_HEALTH_ROUTE => {
            let _: () = request.decode_body()?;
            let rows = cluster_health_rows_with_probe(
                paths,
                state,
                probe,
                !cluster_health_rpc_request_is_local_scope(&request),
                observed_at,
            )?;
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

#[cfg(test)]
pub(crate) fn handle_current_cluster_health_rpc_request<P: ClusterStatusProbe + ?Sized>(
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
    match request.route.as_str() {
        CLUSTER_HEALTH_ROUTE => {
            let _: () = request.decode_body()?;
            let rows = cluster_health_rows_with_async_probe(
                paths,
                state,
                probe,
                !cluster_health_rpc_request_is_local_scope(&request),
                observed_at,
            )
            .await?;
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
    let state = read_cluster_state_async(runtime, paths).await?;
    handle_cluster_health_rpc_request_async(paths, &state, probe, request, observed_at).await
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
                response_payload: vec![0_u8; request.response_payload_bytes],
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
    fn warning(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            severity: "warning".to_string(),
            code: code.into(),
            detail: detail.into(),
        }
    }
}

pub trait ClusterStatusProbe {
    fn probe_member_status(
        &self,
        state: &ClusterBootstrapState,
        member: &ClusterMemberRecord,
    ) -> Option<ClusterNodeStatusRecord>;

    fn peer_local_health_rows(
        &self,
        _state: &ClusterBootstrapState,
        _member: &ClusterMemberRecord,
    ) -> Option<Vec<ClusterHealthRow>> {
        None
    }
}

pub trait ClusterStatusAsyncProbe {
    fn probe_member_status_async<'a>(
        &'a self,
        state: &'a ClusterBootstrapState,
        member: &'a ClusterMemberRecord,
    ) -> ClusterMembershipRpcFuture<'a, Option<ClusterNodeStatusRecord>>;

    fn peer_local_health_rows_async<'a>(
        &'a self,
        _state: &'a ClusterBootstrapState,
        _member: &'a ClusterMemberRecord,
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
    let stored_statuses = read_cluster_node_statuses_from_kv(paths)?;
    let mut rows = Vec::new();
    for member in &state.members {
        if matches!(member.state, ClusterMemberState::Removed)
            || !cluster_health_member_is_reportable(member)
        {
            continue;
        }
        let expected_process = cluster_member_expected_daemon_process(member);
        let live = probe
            .probe_member_status(state, member)
            .filter(|status| cluster_node_status_matches_member(status, member, expected_process));
        let stored =
            stored_cluster_node_status_for_member(&stored_statuses, member, expected_process);
        let live_online = live.is_some();
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
        // Availability is driven by the active probe, not by stored status freshness.
        let availability = if live_online {
            ClusterNodeAvailability::Online
        } else if live_cluster_member_probe_possible(state, member) {
            ClusterNodeAvailability::Offline
        } else {
            ClusterNodeAvailability::Unknown
        };
        status.availability = availability.clone();
        rows.push(ClusterHealthRow {
            member: member.clone(),
            status,
            availability,
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
    let paths_for_statuses = paths.clone();
    let stored_statuses = tokio::task::spawn_blocking(move || {
        read_cluster_node_statuses_from_kv(&paths_for_statuses)
    })
    .await
    .map_err(|source| NetworkError::Io {
        operation: "cluster health status read".to_string(),
        detail: format!("cluster health status read task failed: {source}"),
    })??;
    let mut rows = Vec::new();
    for member in &state.members {
        if matches!(member.state, ClusterMemberState::Removed)
            || !cluster_health_member_is_reportable(member)
        {
            continue;
        }
        let expected_process = cluster_member_expected_daemon_process(member);
        let live = probe
            .probe_member_status_async(state, member)
            .await?
            .filter(|status| cluster_node_status_matches_member(status, member, expected_process));
        let stored =
            stored_cluster_node_status_for_member(&stored_statuses, member, expected_process);
        let live_online = live.is_some();
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
        let availability = if live_online {
            ClusterNodeAvailability::Online
        } else if live_cluster_member_probe_possible(state, member) {
            ClusterNodeAvailability::Offline
        } else {
            ClusterNodeAvailability::Unknown
        };
        status.availability = availability.clone();
        rows.push(ClusterHealthRow {
            member: member.clone(),
            status,
            availability,
        });
    }
    if include_peer_views {
        merge_reachable_peer_health_rows_async(state, &mut rows, probe).await?;
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
    for row in health {
        if cluster_health_node_is_nonreportable_member(state, &row.member.node_id) {
            continue;
        }
        if row.availability == ClusterNodeAvailability::Offline {
            findings.push(ClusterDoctorFinding::warning(
                "node-offline",
                format!(
                    "{} is offline from {} using advertised endpoints",
                    row.member.node_id, state.local_node_id
                ),
            ));
        } else if row.availability == ClusterNodeAvailability::Unknown {
            findings.push(ClusterDoctorFinding::warning(
                "node-unknown",
                format!("{} has no recent reachable status", row.member.node_id),
            ));
        }
    }

    let connectivity_graph = ClusterConnectivityGraph::from_records(
        state.local_node_id.clone(),
        connectivity.iter().cloned(),
    );
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
        let mut by_public_key = BTreeMap::<String, usize>::new();
        for endpoint in &member.endpoints {
            if let Some(public_key) = iroh_endpoint_public_key(endpoint) {
                *by_public_key.entry(public_key).or_default() += 1;
            }
        }
        for (public_key, count) in by_public_key {
            if count > 1 {
                findings.push(ClusterDoctorFinding::warning(
                    "duplicate-iroh-endpoint",
                    format!(
                        "{} has {count} Iroh endpoint records for public key {public_key}",
                        member.node_id
                    ),
                ));
            }
        }
    }

    let route_targets = advertised_peer_route_targets(state);
    if !route_targets.is_empty() {
        let route_ips = discover_peer_route_ip_candidates(state);
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
        pid: None,
        address: member
            .endpoints
            .iter()
            .find(|endpoint| !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly))
            .or_else(|| member.endpoints.first())
            .map(|endpoint| endpoint.address.clone()),
        bind_address: None,
        executable: None,
        rollout: None,
        daemon_started_at: None,
        observed_at,
    }
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
    while queried.len() <= CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS {
        if !rows
            .iter()
            .any(|row| row.availability != ClusterNodeAvailability::Online)
        {
            return;
        }
        let Some(member) = next_online_peer_to_query(state, rows, &queried) else {
            return;
        };
        queried.insert(member.node_id.clone());
        let Some(peer_rows) = probe.peer_local_health_rows(state, &member) else {
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

async fn merge_reachable_peer_health_rows_async<P: ClusterStatusAsyncProbe + ?Sized>(
    state: &ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    probe: &P,
) -> Result<(), NetworkError> {
    let mut queried = std::collections::BTreeSet::new();
    queried.insert(state.local_node_id.clone());
    while queried.len() <= CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS {
        if !rows
            .iter()
            .any(|row| row.availability != ClusterNodeAvailability::Online)
        {
            return Ok(());
        }
        let Some(member) = next_online_peer_to_query(state, rows, &queried) else {
            return Ok(());
        };
        queried.insert(member.node_id.clone());
        let Some(peer_rows) = probe.peer_local_health_rows_async(state, &member).await? else {
            continue;
        };
        merge_online_peer_health_rows(rows, peer_rows);
    }
    Ok(())
}

fn merge_online_peer_health_rows(rows: &mut [ClusterHealthRow], peer_rows: Vec<ClusterHealthRow>) {
    let mut by_node = peer_rows
        .into_iter()
        .filter(|row| row.availability == ClusterNodeAvailability::Online)
        .map(|row| (row.member.node_id.clone(), row))
        .collect::<BTreeMap<_, _>>();
    for row in rows {
        if row.availability == ClusterNodeAvailability::Online {
            continue;
        }
        let Some(peer_row) = by_node.remove(&row.member.node_id) else {
            continue;
        };
        row.status = peer_row.status;
        row.status.availability = ClusterNodeAvailability::Online;
        row.availability = ClusterNodeAvailability::Online;
    }
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
}
