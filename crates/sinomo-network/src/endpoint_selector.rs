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
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr};

const DEFAULT_DAEMON_SHADOW_PORT: u16 = 21_354;

fn try_clone_selector_id(
    id: &sinomo_core::Id,
    operation: &str,
) -> Result<sinomo_core::Id, NetworkError> {
    crate::object::try_clone_id(id, operation)
}

fn try_clone_selector_string(value: &str, operation: &str) -> Result<String, NetworkError> {
    crate::object::try_clone_string(value, operation)
}

fn try_clone_selector_endpoint(
    endpoint: &ClusterEndpointRecord,
    operation: &str,
) -> Result<ClusterEndpointRecord, NetworkError> {
    Ok(ClusterEndpointRecord {
        endpoint_id: try_clone_selector_id(&endpoint.endpoint_id, operation)?,
        node_id: try_clone_selector_id(&endpoint.node_id, operation)?,
        transport: endpoint.transport.clone(),
        address: try_clone_selector_string(&endpoint.address, operation)?,
        source: endpoint.source.clone(),
        privacy: endpoint.privacy.clone(),
        priority: endpoint.priority,
        last_observed_latency_ms: endpoint.last_observed_latency_ms,
    })
}

#[derive(Clone, Debug, Default)]
struct EndpointNodeSet {
    nodes: Vec<NodeId>,
}

impl EndpointNodeSet {
    fn contains(&self, node_id: &NodeId) -> bool {
        self.position(node_id).is_ok()
    }

    fn try_insert_clone(
        &mut self,
        node_id: &NodeId,
        operation: &str,
    ) -> Result<bool, NetworkError> {
        match self.position(node_id) {
            Ok(_) => Ok(false),
            Err(index) => {
                let node_id = try_clone_selector_id(node_id, operation)?;
                crate::object::try_reserve_vec(&mut self.nodes, 1, operation)?;
                self.nodes.insert(index, node_id);
                Ok(true)
            }
        }
    }

    fn position(&self, node_id: &NodeId) -> Result<usize, usize> {
        self.nodes.binary_search(node_id)
    }
}

#[derive(Clone, Debug, Default)]
struct EndpointIdSet {
    ids: Vec<sinomo_core::Id>,
}

impl EndpointIdSet {
    fn try_insert_clone(
        &mut self,
        endpoint_id: &sinomo_core::Id,
        operation: &str,
    ) -> Result<bool, NetworkError> {
        match self.position(endpoint_id) {
            Ok(_) => Ok(false),
            Err(index) => {
                let endpoint_id = try_clone_selector_id(endpoint_id, operation)?;
                crate::object::try_reserve_vec(&mut self.ids, 1, operation)?;
                self.ids.insert(index, endpoint_id);
                Ok(true)
            }
        }
    }

    fn position(&self, endpoint_id: &sinomo_core::Id) -> Result<usize, usize> {
        self.ids.binary_search(endpoint_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EndpointRouteKey {
    node_id: NodeId,
    transport_rank: u8,
    address: String,
}

impl Ord for EndpointRouteKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.node_id, self.transport_rank, &self.address).cmp(&(
            &other.node_id,
            other.transport_rank,
            &other.address,
        ))
    }
}

impl PartialOrd for EndpointRouteKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Default)]
struct EndpointRouteKeySet {
    keys: Vec<EndpointRouteKey>,
}

impl EndpointRouteKeySet {
    fn try_insert_endpoint(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        operation: &str,
    ) -> Result<bool, NetworkError> {
        let transport_rank = cluster_endpoint_transport_rank(&endpoint.transport);
        match self.keys.binary_search_by(|key| {
            (&key.node_id, key.transport_rank, key.address.as_str()).cmp(&(
                &endpoint.node_id,
                transport_rank,
                endpoint.address.as_str(),
            ))
        }) {
            Ok(_) => Ok(false),
            Err(index) => {
                let key = EndpointRouteKey {
                    node_id: try_clone_selector_id(&endpoint.node_id, operation)?,
                    transport_rank,
                    address: try_clone_selector_string(&endpoint.address, operation)?,
                };
                crate::object::try_reserve_vec(&mut self.keys, 1, operation)?;
                self.keys.insert(index, key);
                Ok(true)
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
struct EndpointByNodeIndex {
    entries: Vec<(NodeId, Vec<ClusterEndpointRecord>)>,
}

impl EndpointByNodeIndex {
    fn try_push(
        &mut self,
        endpoint: ClusterEndpointRecord,
        operation: &str,
    ) -> Result<(), NetworkError> {
        match self.position(&endpoint.node_id) {
            Ok(index) => {
                crate::object::try_reserve_vec(&mut self.entries[index].1, 1, operation)?;
                self.entries[index].1.push(endpoint);
            }
            Err(index) => {
                let node_id = try_clone_selector_id(&endpoint.node_id, operation)?;
                let mut endpoints = Vec::new();
                crate::object::try_reserve_vec(&mut endpoints, 1, operation)?;
                endpoints.push(endpoint);
                crate::object::try_reserve_vec(&mut self.entries, 1, operation)?;
                self.entries.insert(index, (node_id, endpoints));
            }
        }
        Ok(())
    }

    fn get(&self, node_id: &NodeId) -> Option<&[ClusterEndpointRecord]> {
        self.position(node_id).ok().and_then(|index| {
            self.entries
                .get(index)
                .map(|(_, endpoints)| endpoints.as_slice())
        })
    }

    fn values_mut(&mut self) -> impl Iterator<Item = &mut Vec<ClusterEndpointRecord>> {
        self.entries.iter_mut().map(|(_, endpoints)| endpoints)
    }

    fn iter(&self) -> impl Iterator<Item = (&NodeId, &[ClusterEndpointRecord])> {
        self.entries
            .iter()
            .map(|(node_id, endpoints)| (node_id, endpoints.as_slice()))
    }

    fn max_endpoint_count(&self) -> usize {
        self.entries
            .iter()
            .map(|(_, endpoints)| endpoints.len())
            .max()
            .unwrap_or(0)
    }

    fn position(&self, node_id: &NodeId) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|(stored, _)| stored.cmp(node_id))
    }
}

fn cluster_endpoint_transport_rank(transport: &ClusterTransportKind) -> u8 {
    match transport {
        ClusterTransportKind::InProcess => 0,
        ClusterTransportKind::Loopback => 1,
        ClusterTransportKind::LanTcp => 2,
        ClusterTransportKind::LanQuic => 3,
        ClusterTransportKind::Iroh => 4,
        ClusterTransportKind::WebRtc => 5,
        ClusterTransportKind::TurnRelay => 6,
        ClusterTransportKind::HttpsRelay => 7,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterEndpointSelectionPurpose {
    AvailableClusterStateAuthority,
    ClusterStateAuthority,
    ClusterStateCommand,
    DaemonCommand,
    DfsData,
    InvitePublish,
    MainDaemonCommand,
    MainDaemonCommandCandidates,
    MemberDaemonStatus,
    MemberPublish,
    NetworkProbe,
    RelayHop,
    Rollout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClusterEndpointInternalSelectionPurpose {
    Public(ClusterEndpointSelectionPurpose),
    LiveClusterStateAuthorityCommand,
    MemberPublishClusterStateAuthorityFirst,
    RaftMemberReplication,
    VoterCommand,
}

impl From<ClusterEndpointSelectionPurpose> for ClusterEndpointInternalSelectionPurpose {
    fn from(purpose: ClusterEndpointSelectionPurpose) -> Self {
        Self::Public(purpose)
    }
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
        self.try_select(purpose).unwrap_or_default()
    }

    pub fn try_select(
        &self,
        purpose: ClusterEndpointSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        self.try_select_internal(purpose.into())
    }

    pub(crate) fn select_internal(
        &self,
        purpose: ClusterEndpointInternalSelectionPurpose,
    ) -> Vec<ClusterEndpointRecord> {
        self.try_select_internal(purpose).unwrap_or_default()
    }

    pub(crate) fn try_select_internal(
        &self,
        purpose: ClusterEndpointInternalSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        match purpose {
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::AvailableClusterStateAuthority,
            ) => try_cluster_live_voter_command_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::ClusterStateAuthority,
            ) => try_cluster_voter_command_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::ClusterStateCommand,
            ) => try_cluster_raft_command_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::DaemonCommand,
            ) => try_cluster_daemon_command_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::DfsData,
            ) => try_cluster_dfs_data_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::InvitePublish,
            ) => try_cluster_invite_publish_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::LiveClusterStateAuthorityCommand => {
                try_cluster_live_voter_command_targets_impl(self.paths, self.state)
            }
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::MainDaemonCommand,
            ) => try_cluster_main_daemon_command_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::MainDaemonCommandCandidates,
            ) => try_cluster_main_daemon_command_endpoint_candidates_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::MemberDaemonStatus,
            ) => try_cluster_member_daemon_status_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::MemberPublish,
            ) => try_cluster_member_publish_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::MemberPublishClusterStateAuthorityFirst => {
                try_cluster_member_publish_targets_voter_first_impl(self.paths, self.state)
            }
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::NetworkProbe,
            ) => try_cluster_network_probe_targets_impl(self.state),
            ClusterEndpointInternalSelectionPurpose::RaftMemberReplication => {
                try_cluster_raft_member_replication_targets_impl(self.paths, self.state)
            }
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::RelayHop,
            ) => try_cluster_main_daemon_command_endpoint_candidates_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::Public(
                ClusterEndpointSelectionPurpose::Rollout,
            ) => try_cluster_rollout_endpoint_targets_impl(self.paths, self.state),
            ClusterEndpointInternalSelectionPurpose::VoterCommand => {
                try_cluster_voter_command_targets_impl(self.paths, self.state)
            }
        }
    }

    pub fn unique_node_targets(
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
    ) -> Vec<ClusterEndpointRecord> {
        Self::try_unique_node_targets(endpoints).unwrap_or_default()
    }

    pub fn try_unique_node_targets(
        endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let mut seen = EndpointNodeSet::default();
        let mut unique = Vec::new();
        for endpoint in endpoints {
            if !seen.try_insert_clone(&endpoint.node_id, "select unique endpoint nodes")? {
                continue;
            }
            crate::object::try_reserve_vec(&mut unique, 1, "select unique endpoint nodes")?;
            unique.push(endpoint);
        }
        Ok(unique)
    }

    pub fn select_unique_node_targets(
        &self,
        purpose: ClusterEndpointSelectionPurpose,
    ) -> Vec<ClusterEndpointRecord> {
        self.try_select_unique_node_targets(purpose)
            .unwrap_or_default()
    }

    pub fn try_select_unique_node_targets(
        &self,
        purpose: ClusterEndpointSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        Self::try_unique_node_targets(self.try_select(purpose)?)
    }

    pub(crate) fn select_internal_unique_node_targets(
        &self,
        purpose: ClusterEndpointInternalSelectionPurpose,
    ) -> Vec<ClusterEndpointRecord> {
        self.try_select_internal_unique_node_targets(purpose)
            .unwrap_or_default()
    }

    pub(crate) fn try_select_internal_unique_node_targets(
        &self,
        purpose: ClusterEndpointInternalSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        Self::try_unique_node_targets(self.try_select_internal(purpose)?)
    }

    pub async fn select_async(
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        purpose: ClusterEndpointSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let _ = runtime;
        ClusterEndpointSelector::new(paths, state).try_select(purpose)
    }

    pub(crate) async fn select_internal_async(
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        purpose: ClusterEndpointInternalSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let _ = runtime;
        ClusterEndpointSelector::new(paths, state).try_select_internal(purpose)
    }

    pub async fn select_unique_node_targets_async(
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        purpose: ClusterEndpointSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let _ = runtime;
        ClusterEndpointSelector::new(paths, state).try_select_unique_node_targets(purpose)
    }

    pub(crate) async fn select_internal_unique_node_targets_async(
        runtime: &tokio::runtime::Handle,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        purpose: ClusterEndpointInternalSelectionPurpose,
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        let _ = runtime;
        ClusterEndpointSelector::new(paths, state).try_select_internal_unique_node_targets(purpose)
    }

    pub fn rollout_targets_for_main_with_command_endpoint(
        &self,
        main_node_id: &NodeId,
        command_endpoint: Option<&ClusterEndpointRecord>,
        candidates: &[ClusterEndpointRecord],
    ) -> Vec<ClusterEndpointRecord> {
        self.try_rollout_targets_for_main_with_command_endpoint(
            main_node_id,
            command_endpoint,
            candidates,
        )
        .unwrap_or_default()
    }

    pub fn try_rollout_targets_for_main_with_command_endpoint(
        &self,
        main_node_id: &NodeId,
        command_endpoint: Option<&ClusterEndpointRecord>,
        candidates: &[ClusterEndpointRecord],
    ) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        cluster_rollout_target_endpoints_for_main_with_command_endpoint_impl(
            self.paths,
            self.state,
            main_node_id,
            command_endpoint,
            candidates,
        )
    }
}

fn try_cluster_member_daemon_status_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let kv_endpoints = match read_cluster_endpoints_from_kv(paths) {
        Ok(endpoints) => endpoints,
        Err(error @ NetworkError::ResourceExhausted { .. }) => return Err(error),
        Err(_) => Vec::new(),
    };
    let mut targets = Vec::new();
    for member in &state.members {
        let mut member_targets = try_cluster_member_daemon_status_targets_with_graph(
            state,
            member,
            &graph,
            &kv_endpoints,
        )?;
        crate::object::try_reserve_vec(
            &mut targets,
            member_targets.len(),
            "collect member daemon status endpoints",
        )?;
        targets.append(&mut member_targets);
    }
    Ok(targets)
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

fn try_cluster_sync_targets_with_connectivity_context(
    state: &ClusterBootstrapState,
    graph: &ClusterConnectivityGraph,
    node_statuses: &BTreeMap<NodeId, ClusterNodeStatusRecord>,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut endpoints = try_cluster_sync_targets(state)?;
    endpoints.sort_by(|left, right| {
        cluster_sync_targets_with_connectivity_cmp(graph, node_statuses, left, right)
    });
    Ok(endpoints)
}

fn cluster_sync_targets_with_connectivity_cmp(
    graph: &ClusterConnectivityGraph,
    node_statuses: &BTreeMap<NodeId, ClusterNodeStatusRecord>,
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> Ordering {
    (
        cluster_node_status_route_rank(node_statuses.get(&left.node_id)),
        graph.unavailability_rank(left),
        cluster_endpoint_static_fallback_rank(left),
        cluster_endpoint_connectivity_attempt_rank(graph, left),
        graph.quality_rank(left),
        left.priority,
        left.address.as_str(),
    )
        .cmp(&(
            cluster_node_status_route_rank(node_statuses.get(&right.node_id)),
            graph.unavailability_rank(right),
            cluster_endpoint_static_fallback_rank(right),
            cluster_endpoint_connectivity_attempt_rank(graph, right),
            graph.quality_rank(right),
            right.priority,
            right.address.as_str(),
        ))
}

#[cfg(test)]
pub(crate) fn cluster_sync_targets_with_connectivity(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Vec<ClusterEndpointRecord> {
    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let node_statuses = cluster_node_status_map_from_kv(paths);
    try_cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses)
        .unwrap_or_default()
}

pub(crate) fn try_cluster_raft_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let node_statuses = cluster_node_status_map_from_kv(paths);
    let mut endpoints_by_node = EndpointByNodeIndex::default();
    let mut command_targets =
        try_cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses)?;
    append_cluster_kv_main_daemon_endpoints(paths, state, &mut command_targets)?;
    for endpoint in command_targets {
        if !cluster_endpoint_can_carry_main_daemon_rpc(&endpoint) {
            continue;
        }
        endpoints_by_node.try_push(endpoint, "group raft endpoints by node")?;
    }
    for endpoints in endpoints_by_node.values_mut() {
        endpoints.sort_by(|left, right| cluster_raft_route_order_cmp(&graph, left, right));
        let mut seen = EndpointRouteKeySet::default();
        let mut deduped = Vec::new();
        for endpoint in endpoints.drain(..) {
            if !seen.try_insert_endpoint(&endpoint, "dedupe raft endpoint routes")? {
                continue;
            }
            crate::object::try_reserve_vec(&mut deduped, 1, "dedupe raft endpoint routes")?;
            deduped.push(endpoint);
        }
        *endpoints = deduped;
    }
    let mut node_order = Vec::new();
    for (node_id, endpoints) in endpoints_by_node.iter() {
        let Some(endpoint) = endpoints.first() else {
            continue;
        };
        crate::object::try_reserve_vec(&mut node_order, 1, "order raft endpoint nodes")?;
        node_order.push((
            cluster_node_status_route_rank(node_statuses.get(node_id)),
            cluster_raft_route_order_key(&graph, endpoint, "order raft endpoint nodes")?,
            try_clone_selector_id(node_id, "order raft endpoint nodes")?,
        ));
    }
    node_order
        .sort_by(|left, right| (left.0, &left.1, &left.2).cmp(&(right.0, &right.1, &right.2)));
    let mut ordered_node_ids = Vec::new();
    crate::object::try_reserve_vec(
        &mut ordered_node_ids,
        node_order.len(),
        "order raft endpoint nodes",
    )?;
    for (_, _, node_id) in node_order {
        ordered_node_ids.push(node_id);
    }
    let max_endpoint_count = endpoints_by_node.max_endpoint_count();
    let mut targets = Vec::new();
    for endpoint_index in 0..max_endpoint_count {
        for node_id in &ordered_node_ids {
            if let Some(endpoints) = endpoints_by_node.get(node_id)
                && let Some(endpoint) = endpoints.get(endpoint_index)
            {
                crate::object::try_reserve_vec(&mut targets, 1, "collect raft endpoint targets")?;
                targets.push(try_clone_selector_endpoint(
                    endpoint,
                    "collect raft endpoint targets",
                )?);
            }
        }
    }
    Ok(targets)
}

fn try_cluster_live_voter_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let node_statuses = cluster_node_status_map_from_kv(paths);
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let mut targets = Vec::new();
    for endpoint in try_cluster_raft_command_targets_impl(paths, state)? {
        if !sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id) {
            continue;
        }
        if node_statuses
            .get(&endpoint.node_id)
            .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
            && graph.unavailability_rank(&endpoint) > 0
        {
            continue;
        }
        crate::object::try_reserve_vec(&mut targets, 1, "collect live voter endpoints")?;
        targets.push(endpoint);
    }
    Ok(targets)
}

pub(crate) fn try_cluster_raft_member_replication_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let node_statuses = cluster_node_status_map_from_kv(paths);
    let mut targets = Vec::new();
    for endpoint in try_cluster_raft_command_targets_impl(paths, state)? {
        if endpoint.node_id == state.local_node_id {
            continue;
        }
        if !state.members.iter().any(|member| {
            member.node_id == endpoint.node_id
                && member.state.is_authorized()
                && cluster_member_has_main_daemon_capability(member)
        }) {
            continue;
        }
        if node_statuses
            .get(&endpoint.node_id)
            .is_some_and(|status| status.availability == ClusterNodeAvailability::Offline)
        {
            continue;
        }
        crate::object::try_reserve_vec(
            &mut targets,
            1,
            "collect raft member replication endpoints",
        )?;
        targets.push(endpoint);
    }
    Ok(targets)
}

pub(crate) async fn cluster_node_blocks_leader_preference_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Result<bool, NetworkError> {
    let _ = runtime;
    Ok(cluster_node_blocks_leader_preference(paths, state, node_id))
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

pub(crate) fn try_cluster_daemon_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let node_statuses = cluster_node_status_map_from_kv(paths);
    let mut sync_targets =
        try_cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses)?;
    append_cluster_kv_main_daemon_endpoints(paths, state, &mut sync_targets)?;
    let trace = cluster_endpoint_selector_trace_enabled();
    let mut usable_targets = Vec::new();
    for endpoint in sync_targets
        .iter()
        .filter(|endpoint| cluster_daemon_command_endpoint_is_usable(&graph, endpoint))
    {
        crate::object::try_reserve_vec(&mut usable_targets, 1, "collect daemon command targets")?;
        usable_targets.push(try_clone_selector_endpoint(
            endpoint,
            "collect daemon command targets",
        )?);
    }
    let selected = try_prefer_bounded_daemon_command_targets(usable_targets)?;
    if trace {
        cluster_endpoint_selector_trace(
            trace,
            format_args!(
                "daemon-command-targets sync={} usable={} selected={} sync_targets={}",
                sync_targets.len(),
                selected.len(),
                cluster_endpoint_selector_endpoint_list(&selected),
                cluster_endpoint_selector_endpoint_list_with_graph(&sync_targets, &graph)
            ),
        );
    }
    Ok(selected)
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

pub(crate) fn try_cluster_dfs_data_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let node_statuses = cluster_node_status_map_from_kv(paths);
    let mut targets =
        try_cluster_sync_targets_with_connectivity_context(state, &graph, &node_statuses)?;
    let main_daemon_node_ids = collect_remote_main_daemon_node_ids(state)?;
    append_cluster_kv_main_daemon_endpoints(paths, state, &mut targets)?;
    targets.retain(|endpoint| {
        dfs_data_endpoint_is_usable_for_main_daemon(&main_daemon_node_ids, endpoint)
    });
    targets.sort_by(|left, right| {
        cluster_sync_targets_with_connectivity_cmp(&graph, &node_statuses, left, right)
    });
    try_prefer_bounded_daemon_command_targets(targets)
}

fn collect_remote_main_daemon_node_ids(
    state: &ClusterBootstrapState,
) -> Result<EndpointNodeSet, NetworkError> {
    let mut main_daemon_node_ids = EndpointNodeSet::default();
    for member in state.members.iter().filter(|member| {
        member.node_id != state.local_node_id
            && member.state.is_authorized()
            && cluster_member_has_main_daemon_capability(member)
    }) {
        main_daemon_node_ids.try_insert_clone(&member.node_id, "collect main daemon nodes")?;
    }
    Ok(main_daemon_node_ids)
}

fn dfs_data_endpoint_is_usable_for_main_daemon(
    main_daemon_node_ids: &EndpointNodeSet,
    endpoint: &ClusterEndpointRecord,
) -> bool {
    endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
        && main_daemon_node_ids.contains(&endpoint.node_id)
        && cluster_endpoint_can_carry_main_daemon_rpc(endpoint)
        && matches!(
            endpoint.transport,
            ClusterTransportKind::LanTcp
                | ClusterTransportKind::Loopback
                | ClusterTransportKind::Iroh
        )
}

fn append_cluster_kv_main_daemon_endpoints(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    targets: &mut Vec<ClusterEndpointRecord>,
) -> Result<(), NetworkError> {
    let main_daemon_node_ids = collect_remote_main_daemon_node_ids(state)?;
    match read_cluster_endpoints_from_kv(paths) {
        Ok(kv_endpoints) => {
            for endpoint in kv_endpoints {
                if !dfs_data_endpoint_is_usable_for_main_daemon(&main_daemon_node_ids, &endpoint) {
                    continue;
                }
                crate::object::try_reserve_vec(targets, 1, "collect DFS data KV endpoints")?;
                targets.push(endpoint);
            }
        }
        Err(error @ NetworkError::ResourceExhausted { .. }) => return Err(error),
        Err(_) => {}
    }
    Ok(())
}

pub(crate) fn try_cluster_main_daemon_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut selected_remote_nodes = EndpointNodeSet::default();
    let mut targets = Vec::new();
    for endpoint in try_cluster_main_daemon_command_endpoint_candidates_impl(paths, state)? {
        if !selected_remote_nodes
            .try_insert_clone(&endpoint.node_id, "select main daemon endpoint nodes")?
        {
            continue;
        }
        crate::object::try_reserve_vec(&mut targets, 1, "select main daemon endpoint nodes")?;
        targets.push(endpoint);
    }
    Ok(targets)
}

pub(crate) fn try_cluster_main_daemon_command_endpoint_candidates_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let trace = cluster_endpoint_selector_trace_enabled();
    let mut main_daemon_node_ids = EndpointNodeSet::default();
    for member in state.members.iter().filter(|member| {
        member.node_id != state.local_node_id
            && member.state.is_authorized()
            && member
                .capabilities
                .iter()
                .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
    }) {
        main_daemon_node_ids.try_insert_clone(&member.node_id, "collect main daemon nodes")?;
    }
    let command_targets = try_cluster_daemon_command_targets_impl(paths, state)?;
    let mut selected = Vec::new();
    for endpoint in command_targets {
        if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly
            || !main_daemon_node_ids.contains(&endpoint.node_id)
            || cluster_endpoint_identifies_shadow_daemon_for_node(&endpoint)
        {
            continue;
        }
        crate::object::try_reserve_vec(&mut selected, 1, "select main daemon candidates")?;
        selected.push(endpoint);
    }
    if trace {
        cluster_endpoint_selector_trace(
            trace,
            format_args!(
                "main-daemon-command-candidates main_nodes={:?} selected={}",
                main_daemon_node_ids.nodes,
                cluster_endpoint_selector_endpoint_list(&selected)
            ),
        );
    }
    Ok(selected)
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

pub fn cluster_endpoint_can_carry_main_daemon_rpc(endpoint: &ClusterEndpointRecord) -> bool {
    !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
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

fn cluster_endpoint_selector_trace_enabled() -> bool {
    tracing::enabled!(target: "sinomo.network.endpoint_selector", tracing::Level::TRACE)
}

fn cluster_endpoint_selector_trace(enabled: bool, args: std::fmt::Arguments<'_>) {
    if enabled {
        tracing::trace!(
            target: "sinomo.network.endpoint_selector",
            detail = %args,
            "cluster endpoint selector"
        );
    }
}

struct ClusterEndpointListDisplay<'a> {
    endpoints: &'a [ClusterEndpointRecord],
}

impl fmt::Display for ClusterEndpointListDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            write!(
                formatter,
                "{}:{}:{:?}",
                endpoint.node_id, endpoint.address, endpoint.transport
            )?;
        }
        Ok(())
    }
}

struct ClusterEndpointListWithGraphDisplay<'a> {
    endpoints: &'a [ClusterEndpointRecord],
    graph: &'a ClusterConnectivityGraph,
}

impl fmt::Display for ClusterEndpointListWithGraphDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, endpoint) in self.endpoints.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            write!(
                formatter,
                "{}:{}:{:?}:state={:?}:privacy={:?}",
                endpoint.node_id,
                endpoint.address,
                endpoint.transport,
                self.graph.state_for_endpoint(endpoint),
                endpoint.privacy
            )?;
        }
        Ok(())
    }
}

fn cluster_endpoint_selector_endpoint_list(
    endpoints: &[ClusterEndpointRecord],
) -> ClusterEndpointListDisplay<'_> {
    ClusterEndpointListDisplay { endpoints }
}

fn cluster_endpoint_selector_endpoint_list_with_graph<'a>(
    endpoints: &'a [ClusterEndpointRecord],
    graph: &'a ClusterConnectivityGraph,
) -> ClusterEndpointListWithGraphDisplay<'a> {
    ClusterEndpointListWithGraphDisplay { endpoints, graph }
}

pub(crate) fn try_cluster_member_publish_targets_voter_first_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut targets = try_cluster_member_publish_targets_impl(paths, state)?;
    targets.sort_by_key(|endpoint| {
        !sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id)
    });
    Ok(targets)
}

pub(crate) fn try_cluster_member_publish_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let mut main_daemon_node_ids = EndpointNodeSet::default();
    for member in state.members.iter().filter(|member| {
        member.node_id != state.local_node_id
            && member.state.is_authorized()
            && cluster_member_has_main_daemon_capability(member)
    }) {
        main_daemon_node_ids.try_insert_clone(&member.node_id, "collect member publish nodes")?;
    }
    let mut seen = EndpointRouteKeySet::default();
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
            push_unique_cluster_endpoint(&mut targets, &mut seen, endpoint)?;
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
        push_unique_cluster_endpoint(&mut targets, &mut seen, &endpoint)?;
    }

    for endpoint in try_cluster_main_daemon_command_targets_impl(paths, state)?
        .into_iter()
        .chain(try_cluster_voter_command_targets_impl(paths, state)?)
    {
        if endpoint.node_id == state.local_node_id {
            continue;
        }
        push_unique_cluster_endpoint(&mut targets, &mut seen, &endpoint)?;
    }

    Ok(targets)
}

fn cluster_member_has_main_daemon_capability(member: &ClusterMemberRecord) -> bool {
    member
        .capabilities
        .iter()
        .any(|capability| capability.as_str() == DAEMON_MAIN_RUN_CAPABILITY)
}

fn push_unique_cluster_endpoint(
    endpoints: &mut Vec<ClusterEndpointRecord>,
    seen: &mut EndpointRouteKeySet,
    endpoint: &ClusterEndpointRecord,
) -> Result<(), NetworkError> {
    if seen.try_insert_endpoint(endpoint, "dedupe cluster endpoint routes")? {
        crate::object::try_reserve_vec(endpoints, 1, "dedupe cluster endpoint routes")?;
        endpoints.push(try_clone_selector_endpoint(
            endpoint,
            "dedupe cluster endpoint routes",
        )?);
    }
    Ok(())
}

pub(crate) fn try_cluster_voter_command_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut targets = Vec::new();
    for endpoint in try_cluster_raft_command_targets_impl(paths, state)? {
        if !sinomo_kv::cluster_kv_raft_node_is_voter(state, &endpoint.node_id) {
            continue;
        }
        crate::object::try_reserve_vec(&mut targets, 1, "collect voter command endpoints")?;
        targets.push(endpoint);
    }
    Ok(targets)
}

pub(crate) fn try_cluster_invite_publish_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut seen_nodes = EndpointNodeSet::default();
    let mut targets = Vec::new();
    for endpoint in try_cluster_voter_command_targets_impl(paths, state)? {
        if !seen_nodes.try_insert_clone(&endpoint.node_id, "select invite publish nodes")? {
            continue;
        }
        crate::object::try_reserve_vec(&mut targets, 1, "select invite publish nodes")?;
        targets.push(endpoint);
    }
    Ok(targets)
}

fn try_cluster_member_daemon_status_targets_with_graph(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
    graph: &ClusterConnectivityGraph,
    kv_endpoints: &[ClusterEndpointRecord],
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let is_local_process = cluster_member_is_local_process(state, member);
    let member_runs_main_daemon = cluster_member_has_main_daemon_capability(member);
    let mut member_endpoints = Vec::new();
    for endpoint in &member.endpoints {
        if !is_local_process && matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly) {
            continue;
        }
        if member_runs_main_daemon && cluster_endpoint_identifies_shadow_daemon_for_node(endpoint) {
            continue;
        }
        crate::object::try_reserve_vec(
            &mut member_endpoints,
            1,
            "collect member daemon status endpoints",
        )?;
        member_endpoints.push(try_clone_selector_endpoint(
            endpoint,
            "collect member daemon status endpoints",
        )?);
    }
    for endpoint in kv_endpoints
        .iter()
        .filter(|endpoint| endpoint.node_id == member.node_id)
    {
        if !is_local_process && matches!(endpoint.privacy, ClusterEndpointPrivacy::LocalOnly) {
            continue;
        }
        if member_runs_main_daemon && cluster_endpoint_identifies_shadow_daemon_for_node(endpoint) {
            continue;
        }
        if !matches!(
            endpoint.transport,
            ClusterTransportKind::LanTcp
                | ClusterTransportKind::Loopback
                | ClusterTransportKind::Iroh
        ) {
            continue;
        }
        crate::object::try_reserve_vec(
            &mut member_endpoints,
            1,
            "collect member daemon status kv endpoints",
        )?;
        member_endpoints.push(try_clone_selector_endpoint(
            endpoint,
            "collect member daemon status kv endpoints",
        )?);
    }
    let mut endpoints = try_prefer_bounded_daemon_command_targets(member_endpoints)?;
    endpoints.sort_by(|left, right| cluster_member_daemon_status_endpoint_cmp(graph, left, right));
    Ok(endpoints)
}

fn cluster_member_daemon_status_endpoint_cmp(
    graph: &ClusterConnectivityGraph,
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> Ordering {
    (
        graph.unavailability_rank(left),
        cluster_endpoint_static_fallback_rank(left),
        graph.quality_rank(left),
        left.priority,
        left.address.as_str(),
    )
        .cmp(&(
            graph.unavailability_rank(right),
            cluster_endpoint_static_fallback_rank(right),
            graph.quality_rank(right),
            right.priority,
            right.address.as_str(),
        ))
}

pub(crate) fn try_cluster_rollout_endpoint_targets_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut endpoints = Vec::new();
    let mut endpoint_ids = EndpointIdSet::default();
    for member in &state.members {
        if member.node_id == state.local_node_id || !member.state.is_authorized() {
            continue;
        }
        for endpoint in member.endpoints.iter().filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
                )
        }) {
            if !endpoint_ids.try_insert_clone(&endpoint.endpoint_id, "collect rollout endpoints")? {
                continue;
            }
            crate::object::try_reserve_vec(&mut endpoints, 1, "collect rollout endpoints")?;
            endpoints.push(try_clone_selector_endpoint(
                endpoint,
                "collect rollout endpoints",
            )?);
        }
    }
    match read_cluster_endpoints_from_kv(paths) {
        Ok(kv_endpoints) => {
            for endpoint in kv_endpoints {
                if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly
                    || !matches!(
                        endpoint.transport,
                        ClusterTransportKind::LanTcp
                            | ClusterTransportKind::Loopback
                            | ClusterTransportKind::Iroh
                    )
                    || !endpoint_ids
                        .try_insert_clone(&endpoint.endpoint_id, "collect rollout kv endpoints")?
                {
                    continue;
                }
                crate::object::try_reserve_vec(&mut endpoints, 1, "collect rollout kv endpoints")?;
                endpoints.push(endpoint);
            }
        }
        Err(error @ NetworkError::ResourceExhausted { .. }) => return Err(error),
        Err(_) => {}
    }
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    sort_cluster_rollout_endpoints(&graph, &mut endpoints);
    Ok(endpoints)
}

pub(crate) fn cluster_rollout_target_endpoints_for_main_with_command_endpoint_impl(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    main_node_id: &NodeId,
    command_endpoint: Option<&ClusterEndpointRecord>,
    candidates: &[ClusterEndpointRecord],
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let shadow_node_id = shadow_daemon_node_id(main_node_id);
    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let mut selected_candidates = Vec::new();
    for endpoint in candidates.iter().filter(|endpoint| {
        (endpoint.node_id == shadow_node_id || endpoint.node_id == *main_node_id)
            && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
    }) {
        crate::object::try_reserve_vec(
            &mut selected_candidates,
            1,
            "collect rollout command endpoint candidates",
        )?;
        selected_candidates.push(try_clone_selector_endpoint(
            endpoint,
            "collect rollout command endpoint candidates",
        )?);
    }
    if let Some(command_endpoint) = command_endpoint
        && command_endpoint.node_id == *main_node_id
        && command_endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
        && !selected_candidates
            .iter()
            .any(|candidate| candidate.endpoint_id == command_endpoint.endpoint_id)
    {
        crate::object::try_reserve_vec(
            &mut selected_candidates,
            1,
            "collect rollout command endpoint candidates",
        )?;
        selected_candidates.push(try_clone_selector_endpoint(
            command_endpoint,
            "collect rollout command endpoint candidates",
        )?);
    }
    selected_candidates.sort_by(|left, right| {
        cluster_rollout_target_endpoint_cmp(&graph, left, right, main_node_id, &shadow_node_id)
    });
    Ok(selected_candidates)
}

fn cluster_rollout_target_endpoint_cmp(
    graph: &ClusterConnectivityGraph,
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
    main_node_id: &NodeId,
    shadow_node_id: &NodeId,
) -> Ordering {
    (
        cluster_rollout_route_process_rank(graph, left, main_node_id, shadow_node_id),
        graph.unavailability_rank(left),
        cluster_endpoint_static_fallback_rank(left),
        graph.quality_rank(left),
        left.priority,
        left.address.as_str(),
    )
        .cmp(&(
            cluster_rollout_route_process_rank(graph, right, main_node_id, shadow_node_id),
            graph.unavailability_rank(right),
            cluster_endpoint_static_fallback_rank(right),
            graph.quality_rank(right),
            right.priority,
            right.address.as_str(),
        ))
}

fn cluster_rollout_route_process_rank(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
    main_node_id: &NodeId,
    shadow_node_id: &NodeId,
) -> u8 {
    let unavailable = graph.unavailability_rank(endpoint) != 0;
    let identifies_shadow = cluster_endpoint_identifies_shadow_daemon_for_node(endpoint);
    if endpoint.node_id == *main_node_id && !identifies_shadow {
        if unavailable { 2 } else { 0 }
    } else if endpoint.node_id == *shadow_node_id
        || (endpoint.node_id == *main_node_id && identifies_shadow)
    {
        if unavailable { 3 } else { 1 }
    } else {
        4
    }
}

fn sort_cluster_rollout_endpoints(
    graph: &ClusterConnectivityGraph,
    endpoints: &mut [ClusterEndpointRecord],
) {
    endpoints.sort_by(|left, right| cluster_rollout_endpoint_cmp(graph, left, right));
}

fn cluster_rollout_endpoint_cmp(
    graph: &ClusterConnectivityGraph,
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> Ordering {
    (
        &left.node_id,
        graph.unavailability_rank(left),
        cluster_endpoint_static_fallback_rank(left),
        graph.quality_rank(left),
        left.priority,
        left.address.as_str(),
    )
        .cmp(&(
            &right.node_id,
            graph.unavailability_rank(right),
            cluster_endpoint_static_fallback_rank(right),
            graph.quality_rank(right),
            right.priority,
            right.address.as_str(),
        ))
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
    match (&endpoint.transport, connectivity) {
        (_, Some(ClusterConnectivityState::Unreachable)) => 4,
        (&ClusterTransportKind::Iroh, Some(ClusterConnectivityState::Healthy)) => 2,
        (&ClusterTransportKind::Iroh, _) => 3,
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

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ClusterRaftRouteOrderKey {
    availability_rank: u8,
    static_fallback_rank: u8,
    quality_rank: u8,
    priority: u16,
    address: String,
}

fn cluster_raft_route_order_cmp(
    graph: &ClusterConnectivityGraph,
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> Ordering {
    (
        cluster_raft_route_availability_rank(left, graph.state_for_endpoint(left)),
        cluster_endpoint_static_fallback_rank(left),
        graph.quality_rank(left),
        left.priority,
        left.address.as_str(),
    )
        .cmp(&(
            cluster_raft_route_availability_rank(right, graph.state_for_endpoint(right)),
            cluster_endpoint_static_fallback_rank(right),
            graph.quality_rank(right),
            right.priority,
            right.address.as_str(),
        ))
}

fn cluster_raft_route_order_key(
    graph: &ClusterConnectivityGraph,
    endpoint: &ClusterEndpointRecord,
    operation: &str,
) -> Result<ClusterRaftRouteOrderKey, NetworkError> {
    Ok(ClusterRaftRouteOrderKey {
        availability_rank: cluster_raft_route_availability_rank(
            endpoint,
            graph.state_for_endpoint(endpoint),
        ),
        static_fallback_rank: cluster_endpoint_static_fallback_rank(endpoint),
        quality_rank: graph.quality_rank(endpoint),
        priority: endpoint.priority,
        address: try_clone_selector_string(&endpoint.address, operation)?,
    })
}

pub(crate) fn try_cluster_network_probe_targets_impl(
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut endpoint_ids = EndpointIdSet::default();
    let mut endpoints = Vec::new();
    for member in &state.members {
        if cluster_member_is_local_process(state, member) || !member.state.is_authorized() {
            continue;
        }
        for endpoint in member.endpoints.iter().filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
                )
        }) {
            if !endpoint_ids
                .try_insert_clone(&endpoint.endpoint_id, "select network probe endpoints")?
            {
                continue;
            }
            crate::object::try_reserve_vec(&mut endpoints, 1, "select network probe endpoints")?;
            endpoints.push(try_clone_selector_endpoint(
                endpoint,
                "select network probe endpoints",
            )?);
        }
    }
    endpoints.sort_by(|left, right| {
        (
            &left.node_id,
            cluster_endpoint_static_fallback_rank(left),
            left.priority,
            left.address.as_str(),
        )
            .cmp(&(
                &right.node_id,
                cluster_endpoint_static_fallback_rank(right),
                right.priority,
                right.address.as_str(),
            ))
    });
    Ok(endpoints)
}

#[cfg(test)]
pub(crate) fn prefer_bounded_daemon_command_targets(
    endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
) -> Vec<ClusterEndpointRecord> {
    try_prefer_bounded_daemon_command_targets(endpoints).unwrap_or_default()
}

pub(crate) fn try_prefer_bounded_daemon_command_targets(
    endpoints: impl IntoIterator<Item = ClusterEndpointRecord>,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let mut endpoint_route_keys = EndpointRouteKeySet::default();
    let mut endpoints_deduped = Vec::new();
    for endpoint in endpoints {
        if !endpoint_route_keys.try_insert_endpoint(&endpoint, "dedupe daemon command endpoints")? {
            continue;
        }
        crate::object::try_reserve_vec(
            &mut endpoints_deduped,
            1,
            "dedupe daemon command endpoints",
        )?;
        endpoints_deduped.push(endpoint);
    }
    let mut selected = Vec::new();
    let mut selected_endpoint_ids = EndpointIdSet::default();
    let mut nodes_with_primary = EndpointNodeSet::default();
    for endpoint in &endpoints_deduped {
        if nodes_with_primary
            .try_insert_clone(&endpoint.node_id, "select primary daemon endpoints")?
        {
            selected_endpoint_ids
                .try_insert_clone(&endpoint.endpoint_id, "select primary daemon endpoints")?;
            crate::object::try_reserve_vec(&mut selected, 1, "select primary daemon endpoints")?;
            selected.push(try_clone_selector_endpoint(
                endpoint,
                "select primary daemon endpoints",
            )?);
        }
    }
    for endpoint in endpoints_deduped {
        if selected_endpoint_ids
            .try_insert_clone(&endpoint.endpoint_id, "select secondary daemon endpoints")?
        {
            crate::object::try_reserve_vec(&mut selected, 1, "select secondary daemon endpoints")?;
            selected.push(endpoint);
        }
    }
    Ok(selected)
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

#[cfg(test)]
pub(crate) fn cluster_sync_targets(state: &ClusterBootstrapState) -> Vec<ClusterEndpointRecord> {
    try_cluster_sync_targets(state).unwrap_or_default()
}

pub(crate) fn try_cluster_sync_targets(
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
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
        let mut endpoints = Vec::new();
        for endpoint in member.endpoints.iter().filter(|endpoint| {
            endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
                && !cluster_endpoint_identifies_shadow_daemon_for_node(endpoint)
                && matches!(
                    endpoint.transport,
                    ClusterTransportKind::LanTcp
                        | ClusterTransportKind::Loopback
                        | ClusterTransportKind::Iroh
                )
        }) {
            crate::object::try_reserve_vec(&mut endpoints, 1, "collect cluster sync endpoints")?;
            endpoints.push(try_clone_selector_endpoint(
                endpoint,
                "collect cluster sync endpoints",
            )?);
        }
        endpoints.sort_by(cluster_sync_endpoint_cmp);
        crate::object::try_reserve_vec(
            &mut targets,
            endpoints.len(),
            "collect cluster sync endpoints",
        )?;
        targets.append(&mut endpoints);
    }
    Ok(targets)
}

fn cluster_sync_endpoint_cmp(
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> Ordering {
    (
        cluster_endpoint_static_fallback_rank(left),
        left.priority,
        left.address.as_str(),
    )
        .cmp(&(
            cluster_endpoint_static_fallback_rank(right),
            right.priority,
            right.address.as_str(),
        ))
}
