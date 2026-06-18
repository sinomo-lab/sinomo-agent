use sinomo_core::{
    CapabilityName, ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
    ClusterEndpointSource, ClusterMemberRecord, ClusterMemberState, ClusterTransportKind,
};
use sinomo_node::{DAEMON_MAIN_RUN_CAPABILITY, NodeId, shadow_daemon_node_id};
use std::net::SocketAddr;

use crate::local_membership::cluster_state_needs_join_registration;

pub const DAEMON_SHADOW_SUPERVISE_CAPABILITY: &str = "daemon.shadow.supervise";
pub const DAEMON_MAIN_CLUSTER_ROLE: &str = "daemon-main";
pub const DAEMON_SHADOW_CLUSTER_ROLE: &str = "daemon-shadow";

pub(crate) fn locally_learned_bridge_route_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.address.parse::<SocketAddr>().is_err()
        && endpoint.address.contains(':')
        && matches!(endpoint.source, ClusterEndpointSource::Observed)
        && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
}

pub fn cluster_member_is_legacy_shadow_daemon(member: &ClusterMemberRecord) -> bool {
    cluster_capabilities_are_legacy_shadow_only(&member.capabilities)
}

pub(crate) fn cluster_capabilities_are_legacy_shadow_only(capabilities: &[CapabilityName]) -> bool {
    let mut has_main_capability = false;
    let mut has_shadow_capability = false;
    for capability in capabilities {
        match capability.as_str() {
            DAEMON_MAIN_RUN_CAPABILITY => has_main_capability = true,
            DAEMON_SHADOW_SUPERVISE_CAPABILITY => has_shadow_capability = true,
            _ => {}
        }
    }
    has_shadow_capability && !has_main_capability
}

pub fn cluster_member_expected_daemon_process(
    member: &ClusterMemberRecord,
) -> Option<&'static str> {
    member
        .capabilities
        .iter()
        .find_map(|capability| match capability.as_str() {
            DAEMON_MAIN_RUN_CAPABILITY => Some(DAEMON_MAIN_CLUSTER_ROLE),
            DAEMON_SHADOW_SUPERVISE_CAPABILITY => Some(DAEMON_SHADOW_CLUSTER_ROLE),
            _ => None,
        })
}

pub fn cluster_member_is_local_process(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    cluster_node_is_local_process(state, &member.node_id)
}

/// True when the cluster has at least one member that is not this node's own
/// main/shadow process and has not been removed.
pub fn cluster_has_remote_peers(state: &ClusterBootstrapState) -> bool {
    state.members.iter().any(|member| {
        !matches!(member.state, ClusterMemberState::Removed)
            && !cluster_member_is_local_process(state, member)
    })
}

pub fn cluster_is_settled_solo(state: &ClusterBootstrapState) -> bool {
    !cluster_has_remote_peers(state) && !cluster_state_needs_join_registration(state)
}

pub fn cluster_local_process_members(state: &ClusterBootstrapState) -> Vec<ClusterMemberRecord> {
    state
        .members
        .iter()
        .filter(|member| cluster_member_is_local_process(state, member))
        .cloned()
        .collect()
}

pub fn live_cluster_member_probe_possible(
    state: &ClusterBootstrapState,
    member: &ClusterMemberRecord,
) -> bool {
    let is_local_process = cluster_member_is_local_process(state, member);
    member.endpoints.iter().any(|endpoint| {
        (is_local_process || endpoint.privacy != ClusterEndpointPrivacy::LocalOnly)
            && matches!(
                endpoint.transport,
                ClusterTransportKind::LanTcp
                    | ClusterTransportKind::Loopback
                    | ClusterTransportKind::Iroh
            )
    })
}

pub fn cluster_node_is_local_process(state: &ClusterBootstrapState, node_id: &NodeId) -> bool {
    *node_id == state.local_node_id || *node_id == shadow_daemon_node_id(&state.local_node_id)
}
