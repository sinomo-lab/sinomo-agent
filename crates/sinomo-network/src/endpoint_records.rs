use crate::prelude::*;
use sinomo_node::{cluster_endpoint_stable_eq, cluster_has_remote_peers};

/// True for an endpoint the local node learned over the LAN bridge rather
/// than from a member advertisement: a TCP-ish observed endpoint whose
/// address is a host:port string that does not parse as a socket address.
pub(crate) fn locally_learned_bridge_route_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.address.parse::<SocketAddr>().is_err()
        && endpoint.address.contains(':')
        && matches!(endpoint.source, ClusterEndpointSource::Observed)
        && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
}

/// Whether the local node still needs to run join registration: it either has
/// no member record yet, or its record is an unauthorized learner.
pub fn cluster_state_needs_join_registration(state: &ClusterBootstrapState) -> bool {
    state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .is_none_or(|member| {
            matches!(member.role, ClusterMemberRole::Learner) && !member.state.is_authorized()
        })
}

/// A settled solo cluster: no remote peers and no pending join registration.
pub fn cluster_is_settled_solo(state: &ClusterBootstrapState) -> bool {
    !cluster_has_remote_peers(state) && !cluster_state_needs_join_registration(state)
}

pub fn apply_cluster_endpoint_advertisement(
    state: &mut ClusterBootstrapState,
    endpoint: ClusterEndpointRecord,
    persisted_endpoints: Option<&[ClusterEndpointRecord]>,
    timestamp: OffsetDateTime,
) -> Option<ClusterMemberRecord> {
    let member = state
        .members
        .iter_mut()
        .find(|member| member.node_id == endpoint.node_id)?;
    upsert_member_endpoint(member, endpoint);
    if persisted_endpoints == Some(member.endpoints.as_slice()) {
        return None;
    }
    state.updated_at = timestamp;
    Some(member.clone())
}

pub fn cleanup_cluster_endpoint_duplicates(state: &mut ClusterBootstrapState) -> bool {
    state.members.iter_mut().fold(false, |changed, member| {
        cleanup_member_endpoint_duplicates(member) || changed
    })
}

pub fn cleanup_member_endpoint_duplicates(member: &mut ClusterMemberRecord) -> bool {
    let original = member.endpoints.clone();
    let mut endpoints: Vec<ClusterEndpointRecord> = Vec::new();
    for endpoint in original {
        if endpoints.iter().any(|candidate| {
            candidate.endpoint_id == endpoint.endpoint_id
                || (candidate.transport == endpoint.transport
                    && candidate.address == endpoint.address)
        }) {
            continue;
        }
        endpoints.push(endpoint);
    }
    if member.endpoints == endpoints {
        false
    } else {
        member.endpoints = endpoints;
        true
    }
}

pub fn upsert_member_endpoint(
    member: &mut ClusterMemberRecord,
    endpoint: ClusterEndpointRecord,
) -> bool {
    let changed = false;
    if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
    {
        if *existing == endpoint {
            changed
        } else if cluster_endpoint_stable_eq(existing, &endpoint) {
            changed
        } else {
            *existing = endpoint;
            true
        }
    } else if member.endpoints.iter().any(|candidate| {
        candidate.transport == endpoint.transport && candidate.address == endpoint.address
    }) {
        changed
    } else {
        member.endpoints.push(endpoint);
        true
    }
}
