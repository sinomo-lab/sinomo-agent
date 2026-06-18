use crate::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterIdentityRecord {
    pub cluster_id: ClusterId,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ClusterCapabilityPolicyRecord {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<CapabilityName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_capabilities: Vec<CapabilityName>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInvitationStateRecord {
    pub cluster_id: ClusterId,
    pub invite_id: ClusterInviteId,
    pub state: String,
    pub issuer_node_id: NodeId,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
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

/// Compares two cluster identity records ignoring volatile bookkeeping fields
/// (`kv_revision`, `updated_at`) that change on every publish without reflecting
/// a real configuration change.
pub(crate) fn cluster_identity_record_stable_eq(
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

pub(crate) fn cluster_capability_policy_stable_eq(
    left: &ClusterCapabilityPolicyRecord,
    right: &ClusterCapabilityPolicyRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.node_id == right.node_id
        && left.capabilities == right.capabilities
        && left.allowed_capabilities == right.allowed_capabilities
}

pub(crate) fn cluster_invitation_state_stable_eq(
    left: &ClusterInvitationStateRecord,
    right: &ClusterInvitationStateRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.invite_id == right.invite_id
        && left.state == right.state
        && left.issuer_node_id == right.issuer_node_id
        && left.expires_at == right.expires_at
}
