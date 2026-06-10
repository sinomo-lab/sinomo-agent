use crate::prelude::*;
use crate::rpc::{
    CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, CLUSTER_HEALTH_SCOPE_LOCAL,
    CLUSTER_HEALTH_SCOPE_METADATA_KEY, ObjectRpcRequest, ObjectRpcResponse,
};
use crate::storage::{
    ClusterMembershipRpcFuture, DAEMON_MAIN_CLUSTER_ROLE, DAEMON_MAIN_RUN_CAPABILITY,
    DAEMON_SHADOW_CLUSTER_ROLE, DAEMON_SHADOW_SUPERVISE_CAPABILITY, advertised_peer_route_targets,
    discover_peer_route_ip_candidates, iroh_endpoint_public_key,
    live_cluster_member_probe_possible, read_cluster_node_statuses_from_kv, read_cluster_state,
    read_cluster_state_async,
};
use crate::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityState, ClusterPaths,
    NetworkError, cluster_route_next_hop,
};

const CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterHealthRow {
    pub member: ClusterMemberRecord,
    pub status: ClusterNodeStatusRecord,
    pub availability: ClusterNodeAvailability,
}

pub fn cluster_health_rpc_request() -> Result<ObjectRpcRequest, NetworkError> {
    ObjectRpcRequest::encode_body(CLUSTER_HEALTH_ROUTE, CLUSTER_HEALTH_RPC_CATEGORY, &())
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
            let rows = cluster_health_rows_with_probe(
                paths,
                state,
                probe,
                !cluster_health_rpc_request_is_local_scope(&request),
                observed_at,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_HEALTH_ROUTE, &rows)
        }
        other => Err(NetworkError::UnknownRoute {
            route: other.to_string(),
        }),
    }
}

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
        if matches!(member.state, ClusterMemberState::Removed) {
            continue;
        }
        let live = probe.probe_member_status(state, member);
        let stored = stored_statuses
            .iter()
            .find(|status| status.node_id == member.node_id)
            .cloned();
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
        if matches!(member.state, ClusterMemberState::Removed) {
            continue;
        }
        let live = probe.probe_member_status_async(state, member).await?;
        let stored = stored_statuses
            .iter()
            .find(|status| status.node_id == member.node_id)
            .cloned();
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
        .filter(|row| row.availability == ClusterNodeAvailability::Online)
        .map(|row| row.member.node_id.clone())
        .collect::<BTreeSet<_>>();
    for record in connectivity {
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

pub fn unknown_cluster_node_status(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    observed_at: OffsetDateTime,
) -> ClusterNodeStatusRecord {
    ClusterNodeStatusRecord {
        cluster_id: state.cluster_id.clone(),
        node_id: member.node_id.clone(),
        process: member
            .capabilities
            .iter()
            .find_map(|capability| match capability.as_str() {
                DAEMON_MAIN_RUN_CAPABILITY => Some(DAEMON_MAIN_CLUSTER_ROLE.to_string()),
                DAEMON_SHADOW_SUPERVISE_CAPABILITY => Some(DAEMON_SHADOW_CLUSTER_ROLE.to_string()),
                _ => None,
            })
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

fn merge_reachable_peer_health_rows<P: ClusterStatusProbe + ?Sized>(
    state: &ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    probe: &P,
) {
    if !rows
        .iter()
        .any(|row| row.availability != ClusterNodeAvailability::Online)
    {
        return;
    }

    let mut peer_members = rows
        .iter()
        .filter(|row| {
            row.availability == ClusterNodeAvailability::Online
                && row.member.node_id != state.local_node_id
                && row
                    .member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        })
        .map(|row| row.member.clone())
        .collect::<Vec<_>>();
    peer_members.sort_by_key(|member| {
        (
            std::cmp::Reverse(
                member
                    .endpoints
                    .iter()
                    .filter(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
                    .count(),
            ),
            member.node_id.clone(),
        )
    });

    for member in peer_members
        .into_iter()
        .take(CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS)
    {
        let Some(peer_rows) = probe.peer_local_health_rows(state, &member) else {
            continue;
        };
        merge_online_peer_health_rows(rows, peer_rows);
        if rows
            .iter()
            .all(|row| row.availability == ClusterNodeAvailability::Online)
        {
            break;
        }
    }
}

async fn merge_reachable_peer_health_rows_async<P: ClusterStatusAsyncProbe + ?Sized>(
    state: &ClusterBootstrapState,
    rows: &mut [ClusterHealthRow],
    probe: &P,
) -> Result<(), NetworkError> {
    if !rows
        .iter()
        .any(|row| row.availability != ClusterNodeAvailability::Online)
    {
        return Ok(());
    }

    let mut peer_members = rows
        .iter()
        .filter(|row| {
            row.availability == ClusterNodeAvailability::Online
                && row.member.node_id != state.local_node_id
                && row
                    .member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        })
        .map(|row| row.member.clone())
        .collect::<Vec<_>>();
    peer_members.sort_by_key(|member| {
        (
            std::cmp::Reverse(
                member
                    .endpoints
                    .iter()
                    .filter(|endpoint| endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
                    .count(),
            ),
            member.node_id.clone(),
        )
    });

    for member in peer_members
        .into_iter()
        .take(CLUSTER_HEALTH_PEER_ROWS_MAX_PEERS)
    {
        let Some(peer_rows) = probe.peer_local_health_rows_async(state, &member).await? else {
            continue;
        };
        merge_online_peer_health_rows(rows, peer_rows);
        if rows
            .iter()
            .all(|row| row.availability == ClusterNodeAvailability::Online)
        {
            break;
        }
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
