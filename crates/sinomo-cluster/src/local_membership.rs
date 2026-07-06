#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::prelude::*;
use sinomo_network::ClusterConnectivityProbeState;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{CLUSTER_NETWORK_INFO_TTL_SECONDS, build_cluster_network_info_record};
use sinomo_network::{
    cleanup_cluster_endpoint_duplicates, cluster_state_needs_join_registration,
    upsert_member_endpoint,
};
use sinomo_network::{
    write_local_cluster_connectivity_record_set, write_local_cluster_connectivity_record_set_async,
};
use sinomo_node::{cluster_node_is_local_process, main_daemon_capabilities, shadow_daemon_node_id};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use time::OffsetDateTime;

pub fn refresh_cluster_connectivity_environment(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    environment_id: &mut Option<String>,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    observed_at: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let local_environment_id = build_cluster_network_info_record(
        state,
        &state.local_node_id,
        observed_at,
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    )
    .environment_id;
    let changed = environment_id
        .as_ref()
        .is_some_and(|known| known != &local_environment_id);
    if changed {
        connectivity.clear();
        write_local_cluster_connectivity_record_set(paths, &state.local_node_id, &[])?;
    }
    *environment_id = Some(local_environment_id);
    Ok(changed)
}

pub async fn refresh_cluster_connectivity_environment_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    environment_id: &mut Option<String>,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    observed_at: OffsetDateTime,
) -> Result<bool, NetworkError> {
    let local_environment_id = build_cluster_network_info_record(
        state,
        &state.local_node_id,
        observed_at,
        CLUSTER_NETWORK_INFO_TTL_SECONDS,
    )
    .environment_id;
    let changed = environment_id
        .as_ref()
        .is_some_and(|known| known != &local_environment_id);
    if changed {
        connectivity.clear();
        write_local_cluster_connectivity_record_set_async(
            runtime,
            paths,
            &state.local_node_id,
            &[],
        )
        .await?;
    }
    *environment_id = Some(local_environment_id);
    Ok(changed)
}

pub fn apply_cluster_node_role(
    member: &mut ClusterMemberRecord,
    role: ClusterNodeRole,
    timestamp: OffsetDateTime,
) {
    member.node_role = role;
    member.updated_at = timestamp;
}

pub fn prepare_pending_cluster_join_state(
    cluster_id: ClusterId,
    cluster_name: String,
    local_node_id: NodeId,
    local_node_display_name: String,
    main_public_key: String,
    shadow_public_key: String,
    timestamp: OffsetDateTime,
) -> ClusterBootstrapState {
    let mut state = ClusterBootstrapState::pending_join(
        cluster_id,
        cluster_name,
        local_node_id,
        local_node_display_name,
        main_public_key.clone(),
        timestamp,
    );
    ensure_daemon_cluster_members(&mut state, Vec::new(), Vec::new(), timestamp);
    assign_local_daemon_member_public_keys(&mut state, main_public_key, shadow_public_key);
    state
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterLocalDaemonMembershipObservation {
    pub main_endpoints: Vec<ClusterEndpointRecord>,
    pub shadow_endpoints: Vec<ClusterEndpointRecord>,
    pub main_public_key: String,
    pub shadow_public_key: String,
    pub timestamp: OffsetDateTime,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClusterLocalDaemonMembershipRefresh {
    pub members_changed: bool,
    pub public_keys_changed: bool,
    pub iroh_endpoints_changed: bool,
}

impl ClusterLocalDaemonMembershipRefresh {
    pub fn changed(&self) -> bool {
        self.members_changed || self.public_keys_changed || self.iroh_endpoints_changed
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn prepare_cluster_join_registration_state_with_local_daemon_members(
    mut state: ClusterBootstrapState,
    main_endpoints: Vec<ClusterEndpointRecord>,
    shadow_endpoints: Vec<ClusterEndpointRecord>,
    main_public_key: String,
    shadow_public_key: String,
    timestamp: OffsetDateTime,
) -> ClusterBootstrapState {
    apply_local_daemon_membership_observation(
        &mut state,
        ClusterLocalDaemonMembershipObservation {
            main_endpoints,
            shadow_endpoints,
            main_public_key,
            shadow_public_key,
            timestamp,
        },
    );
    state
}

pub fn apply_local_daemon_membership_observation(
    state: &mut ClusterBootstrapState,
    observation: ClusterLocalDaemonMembershipObservation,
) -> ClusterLocalDaemonMembershipRefresh {
    let members_changed = ensure_daemon_cluster_members(
        state,
        observation.main_endpoints,
        observation.shadow_endpoints,
        observation.timestamp,
    );
    let public_keys_changed = assign_local_daemon_member_public_keys(
        state,
        observation.main_public_key,
        observation.shadow_public_key,
    );
    ClusterLocalDaemonMembershipRefresh {
        members_changed,
        public_keys_changed,
        iroh_endpoints_changed: false,
    }
}

pub fn refresh_local_daemon_membership_from_disk(
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    observation: ClusterLocalDaemonMembershipObservation,
) -> Result<ClusterLocalDaemonMembershipRefresh, NetworkError> {
    let timestamp = observation.timestamp;
    let endpoint_cleanup_changed = cleanup_cluster_endpoint_duplicates(state);
    let mut refresh = apply_local_daemon_membership_observation(state, observation);
    let _ = paths;
    refresh.iroh_endpoints_changed = endpoint_cleanup_changed;
    if endpoint_cleanup_changed {
        state.updated_at = timestamp;
    }
    Ok(refresh)
}

pub async fn refresh_local_daemon_membership_from_disk_async(
    runtime: &tokio::runtime::Handle,
    paths: &ClusterPaths,
    state: &mut ClusterBootstrapState,
    observation: ClusterLocalDaemonMembershipObservation,
) -> Result<ClusterLocalDaemonMembershipRefresh, NetworkError> {
    let timestamp = observation.timestamp;
    let endpoint_cleanup_changed = cleanup_cluster_endpoint_duplicates(state);
    let mut refresh = apply_local_daemon_membership_observation(state, observation);
    let _ = (runtime, paths);
    refresh.iroh_endpoints_changed = endpoint_cleanup_changed;
    if endpoint_cleanup_changed {
        state.updated_at = timestamp;
    }
    Ok(refresh)
}

pub fn assign_local_daemon_member_public_keys(
    state: &mut ClusterBootstrapState,
    main_public_key: String,
    _shadow_public_key: String,
) -> bool {
    let mut changed = false;
    for member in &mut state.members {
        if member.node_id == state.local_node_id && member.node_public_key != main_public_key {
            member.node_public_key = main_public_key.clone();
            changed = true;
        }
    }
    changed
}

pub fn ensure_daemon_cluster_members(
    state: &mut ClusterBootstrapState,
    main_endpoints: Vec<ClusterEndpointRecord>,
    shadow_endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    let endpoints = main_and_shadow_supervision_endpoints(state, main_endpoints, shadow_endpoints);
    let changed = remove_local_shadow_daemon_cluster_member(state)
        | ensure_main_daemon_cluster_member(state, endpoints, timestamp);
    if changed {
        state.updated_at = timestamp;
    }
    changed
}

fn remove_local_shadow_daemon_cluster_member(state: &mut ClusterBootstrapState) -> bool {
    let shadow_node_id = shadow_daemon_node_id(&state.local_node_id);
    let before = state.members.len();
    state
        .members
        .retain(|member| member.node_id != shadow_node_id);
    state.members.len() != before
}

fn main_and_shadow_supervision_endpoints(
    state: &ClusterBootstrapState,
    mut main_endpoints: Vec<ClusterEndpointRecord>,
    shadow_endpoints: Vec<ClusterEndpointRecord>,
) -> Vec<ClusterEndpointRecord> {
    let main_node_id = state.local_node_id.clone();
    main_endpoints.extend(shadow_endpoints.into_iter().map(|mut endpoint| {
        endpoint.node_id = main_node_id.clone();
        endpoint
    }));
    main_endpoints
}

pub fn ensure_main_daemon_cluster_member(
    state: &mut ClusterBootstrapState,
    endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    let local_node_id = state.local_node_id.clone();
    let existing = state
        .members
        .iter()
        .find(|member| member.node_id == local_node_id);
    let role = existing
        .map(|member| member.role.clone())
        .unwrap_or(ClusterMemberRole::Learner);
    let node_role = existing
        .map(|member| member.node_role.clone())
        .unwrap_or_default();
    let member_state = existing
        .map(|member| member.state.clone())
        .unwrap_or(ClusterMemberState::Pending);
    let node_public_key = existing_member_public_key(state, &local_node_id).unwrap_or_default();
    let mut member = ClusterMemberRecord::local_voter(
        state.cluster_id.clone(),
        local_node_id,
        state.local_node_display_name.clone(),
        node_public_key,
        timestamp,
    );
    let capabilities = main_daemon_capabilities(&role);
    member.node_role = node_role;
    member.role = role;
    member.state = member_state;
    upsert_daemon_member(state, member, capabilities, endpoints, timestamp)
}

pub fn existing_member_public_key(
    state: &ClusterBootstrapState,
    node_id: &NodeId,
) -> Option<String> {
    state
        .members
        .iter()
        .find(|member| member.node_id == *node_id)
        .map(|member| member.node_public_key.clone())
        .filter(|key| !key.trim().is_empty())
}

pub fn upsert_daemon_member(
    state: &mut ClusterBootstrapState,
    replacement: ClusterMemberRecord,
    capabilities: Vec<CapabilityName>,
    endpoints: Vec<ClusterEndpointRecord>,
    timestamp: OffsetDateTime,
) -> bool {
    if let Some(member) = state
        .members
        .iter_mut()
        .find(|member| member.node_id == replacement.node_id)
    {
        let mut next_endpoints = member.endpoints.clone();
        if endpoints
            .iter()
            .any(daemon_tcp_endpoint_is_listener_advertisement)
        {
            next_endpoints
                .retain(|endpoint| !daemon_tcp_endpoint_is_listener_advertisement(endpoint));
        }
        for endpoint in endpoints {
            next_endpoints.retain(|candidate| candidate.endpoint_id != endpoint.endpoint_id);
            next_endpoints.push(endpoint);
        }
        next_endpoints.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        let changed = member.display_name != replacement.display_name
            || member.node_role != replacement.node_role
            || member.role != replacement.role
            || member.state != replacement.state
            || member.node_public_key != replacement.node_public_key
            || member.capabilities != capabilities
            || member.allowed_capabilities != capabilities
            || member.endpoints != next_endpoints;
        if changed {
            member.display_name = replacement.display_name;
            member.node_role = replacement.node_role;
            member.role = replacement.role;
            member.state = replacement.state;
            member.node_public_key = replacement.node_public_key;
            member.capabilities = capabilities.clone();
            member.allowed_capabilities = capabilities;
            member.endpoints = next_endpoints;
            member.updated_at = timestamp;
        }
        changed
    } else {
        let mut member = replacement;
        member.capabilities = capabilities.clone();
        member.allowed_capabilities = capabilities;
        member.endpoints = endpoints;
        member
            .endpoints
            .sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        state.members.push(member);
        true
    }
}

fn daemon_tcp_endpoint_is_listener_advertisement(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::Loopback | ClusterTransportKind::LanTcp
    ) && matches!(
        endpoint.source,
        ClusterEndpointSource::Static | ClusterEndpointSource::Observed
    )
}

pub fn merge_cluster_join_registration(
    local: &mut ClusterBootstrapState,
    accepted: ClusterBootstrapState,
) {
    let local_node_id = local.local_node_id.clone();
    let local_node_display_name = local.local_node_display_name.clone();
    let initialized_at = local.initialized_at;
    let local_remote_route_endpoints = locally_learned_remote_route_endpoints(local);
    *local = accepted;
    local.local_node_id = local_node_id;
    local.local_node_display_name = local_node_display_name;
    local.initialized_at = initialized_at;
    preserve_locally_learned_remote_route_endpoints(local, local_remote_route_endpoints);
}

pub fn cluster_status_summary_label(state: &ClusterBootstrapState) -> &'static str {
    if cluster_state_needs_join_registration(state) {
        "pending-join"
    } else if sinomo_kv::cluster_kv_raft_local_is_voter(state) {
        "voter"
    } else {
        "read-only"
    }
}

pub(crate) fn locally_learned_remote_route_endpoints(
    state: &ClusterBootstrapState,
) -> BTreeMap<NodeId, Vec<ClusterEndpointRecord>> {
    let local_process_node_ids = state
        .members
        .iter()
        .filter(|member| {
            cluster_node_is_local_process(state, &member.node_id) && member.state.is_authorized()
        })
        .map(|member| member.node_id.clone())
        .collect::<BTreeSet<_>>();
    state
        .members
        .iter()
        .filter(|member| {
            member.state.is_authorized() && !local_process_node_ids.contains(&member.node_id)
        })
        .filter_map(|member| {
            let endpoints = member
                .endpoints
                .iter()
                .filter(|endpoint| locally_learned_route_endpoint(endpoint))
                .cloned()
                .collect::<Vec<_>>();
            (!endpoints.is_empty()).then_some((member.node_id.clone(), endpoints))
        })
        .collect()
}

fn locally_learned_route_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.address.parse::<SocketAddr>().is_err()
        && endpoint.address.contains(':')
        && matches!(endpoint.source, ClusterEndpointSource::Observed)
        && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
}

pub(crate) fn preserve_locally_learned_remote_route_endpoints(
    state: &mut ClusterBootstrapState,
    endpoints_by_node: BTreeMap<NodeId, Vec<ClusterEndpointRecord>>,
) {
    for (node_id, endpoints) in endpoints_by_node {
        let Some(member) = state
            .members
            .iter_mut()
            .find(|member| member.node_id == node_id && member.state.is_authorized())
        else {
            continue;
        };
        for endpoint in endpoints {
            upsert_member_endpoint(member, endpoint);
        }
    }
}
