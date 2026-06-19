use serde::{Deserialize, Serialize};
use sinomo_core::{
    CapabilityName, ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord,
    ClusterEndpointSource, ClusterMemberRecord, ClusterMemberState, ClusterTransportKind, Id,
};
use time::OffsetDateTime;

use crate::{
    DAEMON_MAIN_CLUSTER_ROLE, DAEMON_MAIN_RUN_CAPABILITY, DAEMON_SHADOW_CLUSTER_ROLE,
    DAEMON_SHADOW_SUPERVISE_CAPABILITY, NodeId, shadow_daemon_node_id,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterIdentityRecord {
    pub cluster_id: Id,
    pub cluster_name: String,
    pub local_node_id: NodeId,
    pub local_node_display_name: String,
    pub term: u64,
    pub kv_revision: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub initialized_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl From<&ClusterBootstrapState> for ClusterIdentityRecord {
    fn from(state: &ClusterBootstrapState) -> Self {
        Self {
            cluster_id: state.cluster_id.clone(),
            cluster_name: state.cluster_name.clone(),
            local_node_id: state.local_node_id.clone(),
            local_node_display_name: state.local_node_display_name.clone(),
            term: state.term,
            kv_revision: state.kv_revision,
            initialized_at: state.initialized_at,
            updated_at: state.updated_at,
        }
    }
}

/// Compares two cluster identity records ignoring volatile bookkeeping fields
/// (`kv_revision`, `updated_at`) that change on every publish without reflecting
/// a real configuration change.
pub fn cluster_identity_record_stable_eq(
    left: &ClusterIdentityRecord,
    right: &ClusterIdentityRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.cluster_name == right.cluster_name
        && left.local_node_id == right.local_node_id
        && left.local_node_display_name == right.local_node_display_name
        && left.term == right.term
        && left.initialized_at == right.initialized_at
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterCapabilityPolicyRecord {
    pub cluster_id: Id,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<CapabilityName>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

impl From<&ClusterMemberRecord> for ClusterCapabilityPolicyRecord {
    fn from(member: &ClusterMemberRecord) -> Self {
        Self {
            cluster_id: member.cluster_id.clone(),
            node_id: member.node_id.clone(),
            capabilities: member.capabilities.clone(),
            allowed_capabilities: member.allowed_capabilities.clone(),
            updated_at: member.updated_at,
        }
    }
}

pub fn cluster_capability_policy_stable_eq(
    left: &ClusterCapabilityPolicyRecord,
    right: &ClusterCapabilityPolicyRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.capabilities == right.capabilities
        && left.allowed_capabilities == right.allowed_capabilities
}

pub fn cluster_member_is_legacy_shadow_daemon(member: &ClusterMemberRecord) -> bool {
    cluster_capabilities_are_legacy_shadow_only(&member.capabilities)
}

pub fn cluster_capabilities_are_legacy_shadow_only(capabilities: &[CapabilityName]) -> bool {
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
