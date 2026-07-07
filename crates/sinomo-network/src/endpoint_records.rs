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
        if let Some(index) = endpoints
            .iter()
            .position(|candidate| cluster_endpoint_duplicate_identity_eq(candidate, &endpoint))
        {
            if cluster_endpoint_duplicate_replacement_is_preferred(&endpoints[index], &endpoint) {
                endpoints[index] = endpoint;
            }
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
    } else if let Some(existing) = member
        .endpoints
        .iter_mut()
        .find(|candidate| cluster_endpoint_duplicate_identity_eq(candidate, &endpoint))
    {
        if cluster_endpoint_duplicate_replacement_is_preferred(existing, &endpoint) {
            *existing = endpoint;
            true
        } else {
            changed
        }
    } else {
        member.endpoints.push(endpoint);
        true
    }
}

pub fn cluster_endpoint_duplicate_identity_eq(
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> bool {
    if left.node_id != right.node_id {
        return false;
    }
    left.endpoint_id == right.endpoint_id
        || (left.transport == right.transport && left.address == right.address)
        || cluster_endpoint_module_url_identity(left)
            .zip(cluster_endpoint_module_url_identity(right))
            .is_some_and(|(left, right)| left == right)
}

fn cluster_endpoint_duplicate_replacement_is_preferred(
    existing: &ClusterEndpointRecord,
    incoming: &ClusterEndpointRecord,
) -> bool {
    match (
        cluster_endpoint_module_url_identity(existing),
        cluster_endpoint_module_url_identity(incoming),
    ) {
        (Some(existing_identity), Some(incoming_identity))
            if existing_identity == incoming_identity =>
        {
            cluster_endpoint_module_url_rank(incoming) < cluster_endpoint_module_url_rank(existing)
        }
        _ => false,
    }
}

pub fn cluster_endpoint_module_url_identity(endpoint: &ClusterEndpointRecord) -> Option<String> {
    let (scheme, rest) = endpoint.address.split_once("://")?;
    let authority = rest
        .split(['?', '#'])
        .next()
        .filter(|authority| !authority.is_empty())?;
    Some(format!(
        "{}://{}",
        scheme.trim().to_ascii_lowercase(),
        authority
    ))
}

fn cluster_endpoint_module_url_rank(endpoint: &ClusterEndpointRecord) -> u8 {
    if endpoint.address.contains('?') || endpoint.address.contains('#') {
        1
    } else {
        0
    }
}
