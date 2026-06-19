use crate::NetworkError;
use crate::is_loopback_connect_address;
use crate::kv_read_model::{cluster_node_status_map_from_kv, read_cluster_endpoints_from_kv};
use crate::lan::{
    discover_local_ip_candidates_for_targets, discover_local_ip_candidates_for_targets_async,
    is_invitable_ip,
};
use crate::membership::locally_learned_bridge_route_endpoint;
use crate::{ClusterConnectivityGraph, ClusterConnectivityState};
use crate::{ClusterPaths, iroh_endpoint_public_key};
use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord,
    ClusterNodeAvailability, ClusterNodeStatusRecord, ClusterTransportKind,
};
use sinomo_node::{
    DAEMON_MAIN_RUN_CAPABILITY, NodeId, cluster_member_is_local_process, daemon_lan_endpoint_id,
    daemon_loopback_endpoint_id, shadow_daemon_node_id,
};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};

const DEFAULT_DAEMON_SHADOW_PORT: u16 = 21_354;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterEndpointSelectionPurpose {
    DaemonCommand,
    DfsData,
    InvitePublish,
    LiveVoterCommand,
    MainDaemonCommand,
    MainDaemonCommandCandidates,
    MemberDaemonStatus,
    MemberPublish,
    MemberPublishVoterFirst,
    NetworkProbe,
    RaftCommand,
    RaftMemberReplication,
    RelayHop,
    Rollout,
    VoterCommand,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterEndpointTransferProfile {
    Conservative,
    Lan,
    Local,
}

pub struct ClusterEndpointSelector<'a> {
    paths: &'a ClusterPaths,
    state: &'a ClusterBootstrapState,
}

impl<'a> ClusterEndpointSelector<'a> {
    pub fn new(paths: &'a ClusterPaths, state: &'a ClusterBootstrapState) -> Self {
        Self { paths, state }
    }

    pub fn advertised_peer_route_targets_for_state(
        state: &ClusterBootstrapState,
    ) -> Vec<SocketAddr> {
        advertised_peer_route_targets(state)
    }

    pub fn discover_peer_route_ip_candidates_for_state(
        state: &ClusterBootstrapState,
    ) -> Vec<IpAddr> {
        discover_peer_route_ip_candidates(state)
    }

    pub async fn discover_peer_route_ip_candidates_for_state_async(
        state: &ClusterBootstrapState,
    ) -> Vec<IpAddr> {
        discover_peer_route_ip_candidates_async(state).await
    }

    pub fn transfer_profile_for_endpoint(
        endpoint: &ClusterEndpointRecord,
    ) -> ClusterEndpointTransferProfile {
        cluster_endpoint_transfer_profile(endpoint)
    }

    pub fn transfer_profile_for_endpoints<'e>(
        endpoints: impl IntoIterator<Item = &'e ClusterEndpointRecord>,
    ) -> ClusterEndpointTransferProfile {
        cluster_endpoint_transfer_profile_for_targets(endpoints)
    }

    pub fn select(&self, purpose: ClusterEndpointSelectionPurpose) -> Vec<ClusterEndpointRecord> {
        match purpose {
            ClusterEndpointSelectionPurpose::DaemonCommand => {
                cluster_daemon_command_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::DfsData => {
                cluster_dfs_data_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::InvitePublish => {
                cluster_invite_publish_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::LiveVoterCommand => {
                cluster_raft_live_voter_targets(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MainDaemonCommand => {
                cluster_main_daemon_command_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates => {
                cluster_main_daemon_command_endpoint_candidates_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MemberDaemonStatus => Vec::new(),
            ClusterEndpointSelectionPurpose::MemberPublish => {
                cluster_member_publish_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::MemberPublishVoterFirst => {
                cluster_member_publish_targets_voter_first_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::NetworkProbe => {
                cluster_network_probe_targets_impl(self.state)
            }
            ClusterEndpointSelectionPurpose::RaftCommand => {
                cluster_raft_command_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::RaftMemberReplication => {
                cluster_raft_member_replication_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::RelayHop => {
                cluster_main_daemon_command_endpoint_candidates_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::Rollout => {
                cluster_rollout_endpoint_targets_impl(self.paths, self.state)
            }
            ClusterEndpointSelectionPurpose::VoterCommand => {
                cluster_voter_command_targets_impl(self.paths, self.state)
            }
        }
    }

    pub fn select_node(
        &self,
        purpose: ClusterEndpointSelectionPurpose,
        node_id: &NodeId,
    ) -> Vec<ClusterEndpointRecord> {
        let mut seen = BTreeSet::new();
        self.select(purpose)
            .into_iter()
            .filter(|endpoint| endpoint.node_id == *node_id)
            .filter(|endpoint| {
                seen.insert((
                    format!("{:?}", endpoint.transport),
                    endpoint.address.clone(),
                ))
            })
            .collect()
    }

    pub(crate) fn select_relay_hop_with_graph(
        &self,
        graph: &ClusterConnectivityGraph,
        source_node_id: &NodeId,
        node_id: &NodeId,
        allow_unknown_locally_learned_endpoint: bool,
    ) -> Vec<ClusterEndpointRecord> {
        let mut endpoints = Vec::new();
        let mut seen = BTreeSet::new();
        let has_healthy_next_hop_endpoint = if let Some(endpoint) = graph
            .best_healthy_endpoint_matching(
                source_node_id,
                node_id,
                cluster_endpoint_can_carry_main_daemon_rpc,
            ) {
            push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
            true
        } else {
            false
        };
        let has_reachable_next_hop_endpoint = if has_healthy_next_hop_endpoint {
            true
        } else if let Some(endpoint) = graph.best_reachable_endpoint_matching(
            source_node_id,
            node_id,
            cluster_endpoint_can_carry_main_daemon_rpc,
        ) {
            push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
            true
        } else {
            false
        };
        let local_graph_has_reachable_targets =
            cluster_graph_has_reachable_main_daemon_route_targets(graph, source_node_id);
        if let Some(member) = self
            .state
            .members
            .iter()
            .find(|member| member.node_id == *node_id && member.state.is_authorized())
        {
            for endpoint in member
                .endpoints
                .iter()
                .filter(|endpoint| locally_learned_bridge_route_endpoint(endpoint))
                .filter(|endpoint| cluster_endpoint_can_carry_main_daemon_rpc(endpoint))
                .filter(|endpoint| {
                    relay_hop_endpoint_is_usable(
                        graph,
                        source_node_id,
                        endpoint,
                        has_healthy_next_hop_endpoint,
                        has_reachable_next_hop_endpoint,
                        local_graph_has_reachable_targets,
                        allow_unknown_locally_learned_endpoint,
                    )
                })
            {
                push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint.clone());
            }
        }
        for endpoint in self
            .select_node(ClusterEndpointSelectionPurpose::RelayHop, node_id)
            .into_iter()
            .filter(|endpoint| {
                relay_hop_endpoint_is_usable(
                    graph,
                    source_node_id,
                    endpoint,
                    has_healthy_next_hop_endpoint,
                    has_reachable_next_hop_endpoint,
                    local_graph_has_reachable_targets,
                    allow_unknown_locally_learned_endpoint,
                )
            })
        {
            push_unique_cluster_endpoint(&mut endpoints, &mut seen, endpoint);
        }
        endpoints
    }

    pub fn select_member(
        &self,
        member: &ClusterMemberRecord,
        purpose: ClusterEndpointSelectionPurpose,
    ) -> Vec<ClusterEndpointRecord> {
        match purpose {
            ClusterEndpointSelectionPurpose::MemberDaemonStatus => {
                cluster_member_daemon_status_targets_with_connectivity(
                    self.paths, self.state, member,
                )
            }
            _ => Vec::new(),
        }
    }

    pub fn rollout_targets_for_main_with_command_endpoint(
        &self,
        main_node_id: &NodeId,
        command_endpoint: Option<&ClusterEndpointRecord>,
        candidates: &[ClusterEndpointRecord],
    ) -> Vec<ClusterEndpointRecord> {
        cluster_rollout_target_endpoints_for_main_with_command_endpoint_impl(
            self.paths,
            self.state,
            main_node_id,
            command_endpoint,
            candidates,
        )
    }
}

fn cluster_endpoint_transfer_profile(
    endpoint: &ClusterEndpointRecord,
) -> ClusterEndpointTransferProfile {
    match endpoint.transport {
        ClusterTransportKind::InProcess | ClusterTransportKind::Loopback => {
            ClusterEndpointTransferProfile::Local
        }
        ClusterTransportKind::LanTcp | ClusterTransportKind::LanQuic => {
            ClusterEndpointTransferProfile::Lan
        }
        ClusterTransportKind::Iroh
        | ClusterTransportKind::WebRtc
        | ClusterTransportKind::TurnRelay
        | ClusterTransportKind::HttpsRelay => ClusterEndpointTransferProfile::Conservative,
    }
}

fn cluster_endpoint_transfer_profile_for_targets<'e>(
    endpoints: impl IntoIterator<Item = &'e ClusterEndpointRecord>,
) -> ClusterEndpointTransferProfile {
    let mut profile = ClusterEndpointTransferProfile::Conservative;
    for endpoint in endpoints {
        match cluster_endpoint_transfer_profile(endpoint) {
            ClusterEndpointTransferProfile::Local => return ClusterEndpointTransferProfile::Local,
            ClusterEndpointTransferProfile::Lan => profile = ClusterEndpointTransferProfile::Lan,
            ClusterEndpointTransferProfile::Conservative => {}
        }
    }
    profile
}

pub(crate) fn cluster_sync_targets_with_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let node_statuses = cluster_node_status_map_from_kv(paths);
    cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses)
}

fn cluster_sync_targets_with_connectivity_context(
    state: &ClusterBootstrapState,
    graph: &ClusterConnectivityGraph,
    node_statuses: &BTreeMap<NodeId, ClusterNodeStatusRecord>,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = cluster_sync_targets(state);
    endpoints.sort_by_key(|endpoint| {
        let node_status_rank = cluster_node_status_route_rank(node_statuses.get(&endpoint.node_id));
        (
            node_status_rank,
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            cluster_endpoint_connectivity_attempt_rank(graph, endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    endpoints
}

pub(crate) fn cluster_raft_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let node_statuses = cluster_node_status_map_from_kv(paths);
    let mut endpoints_by_node = BTreeMap::<NodeId, Vec<ClusterEndpointRecord>>::new();
    for endpoint in cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses) {
        endpoints_by_node
            .entry(endpoint.node_id.clone())
            .or_default()
            .push(endpoint);
    }
    for endpoints in endpoints_by_node.values_mut() {
        endpoints.sort_by_key(|endpoint| cluster_raft_route_order_key(&graph, endpoint));
        let mut seen = BTreeSet::new();
        endpoints.retain(|endpoint| {
            seen.insert((
                format!("{:?}", endpoint.transport),
                endpoint.address.clone(),
            ))
        });
    }
    let mut node_order = endpoints_by_node
        .iter()
        .filter_map(|(node_id, endpoints)| {
            endpoints.first().map(|endpoint| {
                (
                    cluster_node_status_route_rank(node_statuses.get(node_id)),
                    cluster_raft_route_order_key(&graph, endpoint),
                    node_id.clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    node_order.sort_by_key(|(status_rank, route_rank, node_id)| {
        (*status_rank, route_rank.clone(), node_id.clone())
    });
    let ordered_node_ids = node_order
        .into_iter()
        .map(|(_, _, node_id)| node_id)
        .collect::<Vec<_>>();
    let max_endpoint_count = endpoints_by_node.values().map(Vec::len).max().unwrap_or(0);
    let mut targets = Vec::new();
    for endpoint_index in 0..max_endpoint_count {
        for node_id in &ordered_node_ids {
            if let Some(endpoints) = endpoints_by_node.get(node_id)
                && let Some(endpoint) = endpoints.get(endpoint_index)
            {
                targets.push(endpoint.clone());
            }
        }
    }
    targets
}

pub(crate) fn cluster_raft_live_voter_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let node_statuses = cluster_node_status_map_from_kv(paths);
    cluster_raft_command_targets_impl(paths, state)
        .into_iter()
        .filter(|endpoint| sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id))
        .filter(|endpoint| {
            !node_statuses
                .get(&endpoint.node_id)
                .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
        })
        .collect()
}

pub(crate) async fn cluster_raft_live_voter_targets_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || {
            Ok(ClusterEndpointSelector::new(&paths, &state)
                .select(ClusterEndpointSelectionPurpose::LiveVoterCommand))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft voter target selection".to_string(),
            detail: format!("cluster raft voter target selection task failed: {source}"),
        })?
}

pub(crate) fn cluster_raft_member_replication_targets(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    ClusterEndpointSelector::new(paths, state)
        .select(ClusterEndpointSelectionPurpose::RaftMemberReplication)
}

pub(crate) async fn cluster_raft_member_replication_targets_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    runtime
        .spawn_blocking(move || cluster_raft_member_replication_targets(&paths, &state))
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft member replication target selection".to_string(),
            detail: format!("member replication target selection task failed: {source}"),
        })
}

pub(crate) fn cluster_raft_member_replication_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let node_statuses = cluster_node_status_map_from_kv(paths);
    cluster_raft_command_targets_impl(paths, state)
        .into_iter()
        .filter(|endpoint| endpoint.node_id != state.local_node_id)
        .filter(|endpoint| {
            state.members.iter().any(|member| {
                member.node_id == endpoint.node_id
                    && member.state.is_authorized()
                    && cluster_member_has_main_daemon_capability(member)
            })
        })
        .filter(|endpoint| {
            !node_statuses
                .get(&endpoint.node_id)
                .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
        })
        .collect()
}

pub(crate) async fn cluster_node_blocks_leader_preference_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Result<bool, NetworkError> {
    let paths = paths.clone();
    let state = state.clone();
    let node_id = node_id.clone();
    runtime
        .spawn_blocking(move || {
            Ok(cluster_node_blocks_leader_preference(
                &paths, &state, &node_id,
            ))
        })
        .await
        .map_err(|source| NetworkError::Io {
            operation: "cluster raft leader preference".to_string(),
            detail: format!("leader preference task failed: {source}"),
        })?
}

pub(crate) fn cluster_node_blocks_leader_preference(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> bool {
    // A node blocks leader preference if it has gracefully reported itself
    // offline, or if active connectivity probes can no longer reach it. The
    // self-reported `observed_at` is not consulted: it is no longer refreshed on
    // a timer, so an idle-but-healthy node must not be deprioritized.
    let node_statuses = cluster_node_status_map_from_kv(paths);
    if node_statuses
        .get(node_id)
        .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
    {
        return true;
    }
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .is_some_and(|member| cluster_member_unreachable_via_connectivity(paths, state, member))
}

/// True when `member` has probeable endpoints but the local node's active
/// connectivity probes currently mark every one of them unreachable.
///
/// This is the probe-based liveness signal: unlike the self-reported node-status
/// `observed_at` (which is no longer periodically refreshed), it reflects whether
/// we can actually reach the peer right now. A member we have never probed (no
/// connectivity record yet) is not reported unreachable.
pub(crate) fn cluster_member_unreachable_via_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    if cluster_member_is_local_process(state, member) {
        return false;
    }
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut has_probeable_endpoint = false;
    for endpoint in &member.endpoints {
        let probeable = endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
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
        if graph.unavailability_rank(endpoint) == 0 {
            // At least one probeable endpoint is not marked unreachable.
            return false;
        }
    }
    has_probeable_endpoint
}

pub(crate) fn cluster_daemon_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    prefer_bounded_daemon_command_targets(
        cluster_sync_targets_with_connectivity(paths, state)
            .into_iter()
            .filter(|endpoint| cluster_daemon_command_endpoint_is_usable(&graph, endpoint)),
    )
}

fn cluster_daemon_command_endpoint_is_usable(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    match graph.state_for_endpoint(endpoint) {
        Some(ClusterConnectivityState::Healthy) | None => true,
        Some(ClusterConnectivityState::Degraded) | Some(ClusterConnectivityState::Unreachable) => {
            false
        }
    }
}

pub(crate) fn cluster_dfs_data_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let node_statuses = cluster_node_status_map_from_kv(paths);
    prefer_bounded_daemon_command_targets(cluster_sync_targets_with_connectivity_context(
        state,
        &graph,
        &node_statuses,
    ))
}

pub(crate) fn cluster_main_daemon_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut selected_remote_nodes = BTreeSet::new();
    cluster_main_daemon_command_endpoint_candidates_impl(paths, state)
        .into_iter()
        .filter(|endpoint| selected_remote_nodes.insert(endpoint.node_id.clone()))
        .collect()
}

pub(crate) fn cluster_main_daemon_command_endpoint_candidates_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let main_daemon_node_ids = state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && member
                    .capabilities
                    .iter()
                    .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>();
    cluster_daemon_command_targets_impl(paths, state)
        .into_iter()
        .filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && main_daemon_node_ids.contains(&endpoint.node_id)
                && !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
        })
        .collect()
}

pub(crate) fn cluster_endpoint_identifies_shadow_daemon_for_node(
    endpoint: &ClusterEndpointRecord,
) -> bool {
    let shadow_node_id = shadow_daemon_node_id(&endpoint.node_id);
    let stable_shadow_endpoint = match endpoint.transport {
        ClusterTransportKind::LanTcp => {
            endpoint.endpoint_id == daemon_lan_endpoint_id(&shadow_node_id, &endpoint.address)
        }
        ClusterTransportKind::Loopback => {
            endpoint.endpoint_id == daemon_loopback_endpoint_id(&shadow_node_id)
        }
        ClusterTransportKind::Iroh => {
            iroh_endpoint_public_key(endpoint).is_some_and(|public_key| {
                endpoint.endpoint_id == crate::stable_iroh_endpoint_id(&shadow_node_id, &public_key)
            })
        }
        _ => false,
    };
    stable_shadow_endpoint || cluster_endpoint_uses_default_shadow_daemon_port(endpoint)
}

pub(crate) fn cluster_endpoint_can_carry_main_daemon_rpc(endpoint: &ClusterEndpointRecord) -> bool {
    !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
}

pub(crate) fn cluster_graph_has_reachable_main_daemon_route_targets(
    graph: &ClusterConnectivityGraph,
    source_node_id: &NodeId,
) -> bool {
    graph
        .reachable_targets(source_node_id)
        .iter()
        .any(|target| {
            graph
                .best_reachable_endpoint_matching(
                    source_node_id,
                    target,
                    cluster_endpoint_can_carry_main_daemon_rpc,
                )
                .is_some()
        })
}

pub(crate) fn relay_hop_endpoint_is_usable(
    graph: &ClusterConnectivityGraph,
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
    has_healthy_next_hop_endpoint: bool,
    has_reachable_next_hop_endpoint: bool,
    local_graph_has_reachable_targets: bool,
    allow_unknown_locally_learned_endpoint: bool,
) -> bool {
    match graph.state_for_endpoint(endpoint) {
        Some(ClusterConnectivityState::Healthy) => true,
        Some(ClusterConnectivityState::Degraded) => !has_healthy_next_hop_endpoint,
        Some(ClusterConnectivityState::Unreachable) => {
            !has_reachable_next_hop_endpoint
                && graph
                    .reachable_targets(source_node_id)
                    .contains(&endpoint.node_id)
        }
        None => {
            (!has_healthy_next_hop_endpoint && !local_graph_has_reachable_targets)
                || (allow_unknown_locally_learned_endpoint
                    && !has_reachable_next_hop_endpoint
                    && locally_learned_bridge_route_endpoint(endpoint))
        }
    }
}

fn cluster_endpoint_uses_default_shadow_daemon_port(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && cluster_endpoint_address_port(&endpoint.address) == Some(DEFAULT_DAEMON_SHADOW_PORT)
}

fn cluster_endpoint_address_port(address: &str) -> Option<u16> {
    if let Ok(socket) = address.parse::<SocketAddr>() {
        return Some(socket.port());
    }
    address.rsplit_once(':')?.1.parse::<u16>().ok()
}

pub(crate) fn cluster_member_publish_targets_voter_first_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut targets = cluster_member_publish_targets_impl(paths, state);
    targets.sort_by_key(|endpoint| {
        !sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id)
    });
    targets
}

pub(crate) fn cluster_member_publish_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let main_daemon_node_ids = state
        .members
        .iter()
        .filter(|member| {
            member.node_id != state.local_node_id
                && member.state.is_authorized()
                && cluster_member_has_main_daemon_capability(member)
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut targets = Vec::new();

    for member in state.members.iter().filter(|member| {
        member.node_id != state.local_node_id
            && member.state.is_authorized()
            && cluster_member_has_main_daemon_capability(member)
    }) {
        for endpoint in member
            .endpoints
            .iter()
            .filter(|endpoint| locally_learned_bridge_route_endpoint(endpoint))
            .filter(|endpoint| !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint))
        {
            push_unique_cluster_endpoint(&mut targets, &mut seen, endpoint.clone());
        }
    }

    for node_id in graph.healthy_targets(&state.local_node_id) {
        if !main_daemon_node_ids.contains(&node_id) {
            continue;
        }
        let Some(endpoint) =
            graph.best_healthy_endpoint_matching(&state.local_node_id, &node_id, |endpoint| {
                !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
            })
        else {
            continue;
        };
        push_unique_cluster_endpoint(&mut targets, &mut seen, endpoint);
    }

    for endpoint in cluster_main_daemon_command_targets_impl(paths, state)
        .into_iter()
        .chain(cluster_voter_command_targets_impl(paths, state))
    {
        if endpoint.node_id == state.local_node_id {
            continue;
        }
        push_unique_cluster_endpoint(&mut targets, &mut seen, endpoint);
    }

    targets
}

fn cluster_member_has_main_daemon_capability(member: &ClusterMemberRecord) -> bool {
    member
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
}

fn push_unique_cluster_endpoint(
    endpoints: &mut Vec<ClusterEndpointRecord>,
    seen: &mut BTreeSet<(NodeId, String, String)>,
    endpoint: ClusterEndpointRecord,
) {
    if seen.insert((
        endpoint.node_id.clone(),
        format!("{:?}", endpoint.transport),
        endpoint.address.clone(),
    )) {
        endpoints.push(endpoint);
    }
}

pub(crate) fn cluster_voter_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    cluster_raft_command_targets_impl(paths, state)
        .into_iter()
        .filter(|endpoint| sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id))
        .collect()
}

pub(crate) fn cluster_invite_publish_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut seen_nodes = BTreeSet::new();
    let mut targets = Vec::new();
    for endpoint in cluster_voter_command_targets_impl(paths, state) {
        if !seen_nodes.insert(endpoint.node_id.clone()) {
            continue;
        }
        targets.push(endpoint);
    }
    targets
}

pub(crate) fn cluster_member_daemon_status_targets_with_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    cluster_member_daemon_status_targets_with_graph(state, member, &graph)
}

fn cluster_member_daemon_status_targets_with_graph(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    graph: &ClusterConnectivityGraph,
) -> Vec<ClusterEndpointRecord> {
    let is_local_process = cluster_member_is_local_process(state, member);
    let member_runs_main_daemon = cluster_member_has_main_daemon_capability(member);
    let mut endpoints = prefer_bounded_daemon_command_targets(member.endpoints.clone());
    endpoints.sort_by_key(|endpoint| {
        (
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    endpoints
        .into_iter()
        .filter(|endpoint| {
            is_local_process || !matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly)
        })
        .filter(|endpoint| {
            !member_runs_main_daemon
                || !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
        })
        .collect()
}

pub(crate) fn cluster_rollout_endpoint_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoints = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        endpoints.extend(
            member
                .endpoints
                .iter()
                .filter(|endpoint| {
                    endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                        && matches!(
                            endpoint.transport,
                            ClusterTransportKind::LanTcp
                                | ClusterTransportKind::Loopback
                                | ClusterTransportKind::Iroh
                        )
                })
                .cloned(),
        );
    }
    if let Ok(kv_endpoints) = read_cluster_endpoints_from_kv(paths) {
        for endpoint in kv_endpoints {
            if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly
                || !matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp
                        | ClusterTransportKind::Loopback
                        | ClusterTransportKind::Iroh
                )
                || endpoints
                    .iter()
                    .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
            {
                continue;
            }
            endpoints.push(endpoint);
        }
    }
    sort_cluster_rollout_endpoints(paths, state, &mut endpoints);
    endpoints
}

pub(crate) fn cluster_rollout_target_endpoints_for_main_with_command_endpoint_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    main_node_id: &NodeId,
    command_endpoint: Option<&ClusterEndpointRecord>,
    candidates: &[ClusterEndpointRecord],
) -> Vec<ClusterEndpointRecord> {
    let shadow_node_id = shadow_daemon_node_id(main_node_id);
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut candidates = candidates
        .iter()
        .filter(|endpoint| {
            (endpoint.node_id == shadow_node_id || endpoint.node_id == *main_node_id)
                && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
        })
        .cloned()
        .collect::<Vec<_>>();
    if let Some(command_endpoint) = command_endpoint
        && command_endpoint.node_id == *main_node_id
        && command_endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
        && !candidates
            .iter()
            .any(|candidate| candidate.endpoint_id == command_endpoint.endpoint_id)
    {
        candidates.push(command_endpoint.clone());
    }
    candidates.sort_by_key(|endpoint| {
        (
            cluster_rollout_route_process_rank(&graph, endpoint, main_node_id, &shadow_node_id),
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    candidates
}

fn cluster_rollout_route_process_rank(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
    main_node_id: &NodeId,
    shadow_node_id: &NodeId,
) -> u8 {
    let unavailable = graph.unavailability_rank(endpoint) != 0;
    if endpoint.node_id == *main_node_id {
        if unavailable { 2 } else { 0 }
    } else if endpoint.node_id == *shadow_node_id {
        if unavailable { 3 } else { 1 }
    } else {
        4
    }
}

fn sort_cluster_rollout_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    endpoints: &mut [ClusterEndpointRecord],
) {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    endpoints.sort_by_key(|endpoint| {
        (
            endpoint.node_id.clone(),
            graph.unavailability_rank(endpoint),
            cluster_endpoint_static_fallback_rank(endpoint),
            graph.quality_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
}

fn cluster_node_status_route_rank(status: Option<&ClusterNodeStatusRecord>) -> u8 {
    match status.map(|status| &status.availability) {
        Some(ClusterNodeAvailability::Online) => 0,
        None | Some(ClusterNodeAvailability::Unknown) => 1,
        Some(ClusterNodeAvailability::Offline) => 3,
    }
}

fn cluster_endpoint_connectivity_attempt_rank(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
) -> u8 {
    match graph.state_for_endpoint(endpoint) {
        Some(ClusterConnectivityState::Healthy) => 0,
        None => 1,
        Some(ClusterConnectivityState::Degraded) => 2,
        Some(ClusterConnectivityState::Unreachable) => 3,
    }
}

fn cluster_raft_route_availability_rank(
    endpoint: &ClusterEndpointRecord,
    connectivity: Option<&ClusterConnectivityState>,
) -> u8 {
    match (endpoint.transport.clone(), connectivity) {
        (_, Some(ClusterConnectivityState::Unreachable)) => 4,
        (ClusterTransportKind::Iroh, Some(ClusterConnectivityState::Healthy)) => 2,
        (ClusterTransportKind::Iroh, _) => 3,
        (_, Some(ClusterConnectivityState::Healthy)) => 0,
        _ => 1,
    }
}

fn cluster_endpoint_static_fallback_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    match endpoint.transport {
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback => {
            cluster_direct_endpoint_static_fallback_rank(endpoint)
        }
        ClusterTransportKind::Iroh => 2,
        _ => 8,
    }
}

fn cluster_direct_endpoint_static_fallback_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    let Ok(address) = endpoint.address.parse::<SocketAddr>() else {
        return if !is_loopback_connect_address(&endpoint.address) && endpoint.address.contains(':')
        {
            0
        } else {
            3
        };
    };
    match address.ip() {
        IpAddr::V4(ip) if ip.is_private() || ip.is_link_local() => 0,
        IpAddr::V6(ip) if ip.is_unique_local() || ip.is_unicast_link_local() => 0,
        ip if ip.is_loopback() || ip.is_unspecified() => 2,
        _ => 1,
    }
}

fn cluster_raft_route_order_key(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
) -> (u8, u8, u8, u16, String) {
    (
        cluster_raft_route_availability_rank(endpoint, graph.state_for_endpoint(endpoint)),
        cluster_endpoint_static_fallback_rank(endpoint),
        graph.quality_rank(endpoint),
        endpoint.priority,
        endpoint.address.clone(),
    )
}

pub(crate) fn cluster_network_probe_targets_impl(
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoint_ids = BTreeSet::new();
    let mut endpoints = Vec::new();
    for member in &state.members {
        if cluster_member_is_local_process(state, member) || !member.state.is_authorized() {
            continue;
        }
        endpoints.extend(
            member
                .endpoints
                .iter()
                .filter(|endpoint| {
                    endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                        && matches!(
                            endpoint.transport,
                            ClusterTransportKind::LanTcp
                                | ClusterTransportKind::Loopback
                                | ClusterTransportKind::Iroh
                        )
                        && endpoint_ids.insert(endpoint.endpoint_id.clone())
                })
                .cloned(),
        );
    }
    endpoints.sort_by_key(|endpoint| {
        (
            endpoint.node_id.clone(),
            cluster_endpoint_static_fallback_rank(endpoint),
            endpoint.priority,
            endpoint.address.clone(),
        )
    });
    endpoints
}

pub(crate) fn prefer_bounded_daemon_command_targets(
    endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
) -> Vec<ClusterEndpointRecord> {
    let mut endpoint_route_keys = BTreeSet::new();
    let endpoints = endpoints
        .into_iter()
        .filter(|endpoint| {
            endpoint_route_keys.insert((
                endpoint.node_id.clone(),
                format!("{:?}", endpoint.transport),
                endpoint.address.clone(),
            ))
        })
        .collect::<Vec<_>>();
    let mut selected = Vec::new();
    let mut selected_endpoint_ids = BTreeSet::new();
    let mut nodes_with_primary = BTreeSet::new();
    for endpoint in &endpoints {
        if nodes_with_primary.insert(endpoint.node_id.clone()) {
            selected_endpoint_ids.insert(endpoint.endpoint_id.clone());
            selected.push(endpoint.clone());
        }
    }
    for endpoint in endpoints {
        if selected_endpoint_ids.insert(endpoint.endpoint_id.clone()) {
            selected.push(endpoint);
        }
    }
    selected
}

pub(crate) fn advertised_peer_route_targets(state: &ClusterBootstrapState) -> Vec<SocketAddr> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        for endpoint in &member.endpoints {
            if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
                continue;
            }
            if !matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
            ) {
                continue;
            }
            let Ok(address) = endpoint.address.parse::<SocketAddr>() else {
                continue;
            };
            if is_invitable_ip(address.ip()) && !targets.contains(&address) {
                targets.push(address);
            }
        }
    }
    targets.sort();
    targets
}

pub(crate) fn discover_peer_route_ip_candidates(state: &ClusterBootstrapState) -> Vec<IpAddr> {
    discover_local_ip_candidates_for_targets(advertised_peer_route_targets(state))
}

pub(crate) async fn discover_peer_route_ip_candidates_async(
    state: &ClusterBootstrapState,
) -> Vec<IpAddr> {
    discover_local_ip_candidates_for_targets_async(advertised_peer_route_targets(state)).await
}

pub(crate) fn cluster_sync_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    let mut targets = Vec::new();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        if !member
            .capabilities
            .iter()
            .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
        {
            continue;
        }
        let mut endpoints = member.endpoints.clone();
        endpoints.sort_by_key(|endpoint| {
            (
                cluster_endpoint_static_fallback_rank(endpoint),
                endpoint.priority,
                endpoint.address.clone(),
            )
        });
        targets.extend(endpoints.into_iter().filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
                && matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp
                        | ClusterTransportKind::Loopback
                        | ClusterTransportKind::Iroh
                )
        }));
    }
    targets
}
