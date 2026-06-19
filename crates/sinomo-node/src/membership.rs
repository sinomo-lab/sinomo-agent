use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointRecord, ClusterEndpointSource, ClusterMemberRecord,
};

pub fn cluster_endpoint_stable_eq(
    left: &ClusterEndpointRecord,
    right: &ClusterEndpointRecord,
) -> bool {
    left.endpoint_id == right.endpoint_id
        && left.node_id == right.node_id
        && left.transport == right.transport
        && left.address == right.address
        && cluster_endpoint_source_stable_eq(&left.source, &right.source)
        && left.privacy == right.privacy
        && left.priority == right.priority
        && left.last_observed_latency_ms == right.last_observed_latency_ms
}

fn cluster_endpoint_source_stable_eq(
    left: &ClusterEndpointSource,
    right: &ClusterEndpointSource,
) -> bool {
    left == right
        || matches!(
            (left, right),
            (ClusterEndpointSource::Mdns, ClusterEndpointSource::Observed)
                | (ClusterEndpointSource::Observed, ClusterEndpointSource::Mdns)
        )
}

pub fn cluster_endpoint_list_stable_eq(
    left: &[ClusterEndpointRecord],
    right: &[ClusterEndpointRecord],
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut left = left.iter().collect::<Vec<_>>();
    let mut right = right.iter().collect::<Vec<_>>();
    left.sort_by(cluster_endpoint_record_order);
    right.sort_by(cluster_endpoint_record_order);
    left.into_iter()
        .zip(right)
        .all(|(left, right)| cluster_endpoint_stable_eq(left, right))
}

fn cluster_endpoint_record_order(
    left: &&ClusterEndpointRecord,
    right: &&ClusterEndpointRecord,
) -> std::cmp::Ordering {
    left.endpoint_id
        .cmp(&right.endpoint_id)
        .then_with(|| left.node_id.cmp(&right.node_id))
        .then_with(|| left.address.cmp(&right.address))
}

pub fn cluster_member_stable_eq(left: &ClusterMemberRecord, right: &ClusterMemberRecord) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.display_name == right.display_name
        && left.node_role == right.node_role
        && left.role == right.role
        && left.state == right.state
        && left.trust_domain == right.trust_domain
        && left.node_public_key == right.node_public_key
        && left.certificate_id == right.certificate_id
        && left.capabilities == right.capabilities
        && left.allowed_capabilities == right.allowed_capabilities
        && cluster_endpoint_list_stable_eq(&left.endpoints, &right.endpoints)
        && left.joined_at == right.joined_at
}

pub fn cluster_bootstrap_state_stable_eq(
    left: &ClusterBootstrapState,
    right: &ClusterBootstrapState,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.cluster_name == right.cluster_name
        && left.local_node_id == right.local_node_id
        && left.local_node_display_name == right.local_node_display_name
        && left.term == right.term
        && left.initialized_at == right.initialized_at
        && left.members.len() == right.members.len()
        && left
            .members
            .iter()
            .zip(right.members.iter())
            .all(|(left, right)| cluster_member_stable_eq(left, right))
}

pub fn upsert_member_record(state: &mut ClusterBootstrapState, member: ClusterMemberRecord) {
    if let Some(existing) = state
        .members
        .iter_mut()
        .find(|existing| existing.node_id == member.node_id)
    {
        *existing = member;
    } else {
        state.members.push(member);
    }
}
