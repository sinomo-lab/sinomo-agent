use crate::prelude::*;

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
