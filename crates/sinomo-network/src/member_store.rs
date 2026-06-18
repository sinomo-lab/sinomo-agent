use crate::endpoint_records::{
    cleanup_member_iroh_endpoints, cluster_endpoint_list_stable_eq, upsert_member_endpoint,
};
use sinomo_core::{
    ClusterBootstrapState, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
};
use time::OffsetDateTime;

pub(crate) fn apply_cluster_raft_role(
    member: &mut ClusterMemberRecord,
    role: ClusterMemberRole,
    timestamp: OffsetDateTime,
) {
    sinomo_node::apply_cluster_raft_role(member, role, timestamp);
}

pub(crate) fn cluster_raft_state_for_role(role: &ClusterMemberRole) -> ClusterMemberState {
    match role {
        ClusterMemberRole::Voter => ClusterMemberState::Voter,
        ClusterMemberRole::Learner => ClusterMemberState::Learner,
        ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
    }
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

pub fn merge_cluster_state_from_peer(
    local: &mut ClusterBootstrapState,
    peer: ClusterBootstrapState,
) -> bool {
    if peer.cluster_id != local.cluster_id {
        return false;
    }
    let mut changed = false;
    for member in peer.members {
        changed |= merge_member_record(local, member);
    }
    if peer.kv_revision > local.kv_revision {
        local.kv_revision = peer.kv_revision;
    }
    if peer.updated_at > local.updated_at {
        local.updated_at = peer.updated_at;
    }
    changed
}

pub fn merge_member_record(
    local: &mut ClusterBootstrapState,
    incoming: ClusterMemberRecord,
) -> bool {
    if incoming.cluster_id != local.cluster_id {
        return false;
    }
    if let Some(existing) = local
        .members
        .iter_mut()
        .find(|member| member.node_id == incoming.node_id)
    {
        let mut changed = false;
        if incoming.updated_at > existing.updated_at {
            *existing = incoming;
            cleanup_member_iroh_endpoints(existing);
            return true;
        }
        for endpoint in incoming.endpoints {
            changed |= upsert_member_endpoint(existing, endpoint);
        }
        changed |= cleanup_member_iroh_endpoints(existing);
        changed
    } else {
        let mut incoming = incoming;
        cleanup_member_iroh_endpoints(&mut incoming);
        local.members.push(incoming);
        true
    }
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
