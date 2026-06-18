use crate::prelude::*;
use sinomo_kv::KvRevision;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationRequest {
    pub invite: ClusterJoinInvite,
    pub invitation_secret: String,
    pub members: Vec<ClusterMemberRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInvitePublishRequest {
    pub invite: ClusterJoinInvite,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterInvitePublishResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterMemberPublishRequest {
    pub members: Vec<ClusterMemberRecord>,
    /// The publisher's local cluster KV snapshot revision. When present, the
    /// voter includes its current KV snapshot in the response only if the
    /// publisher is behind. This lets learners converge their local KV snapshot
    /// through the bridge-relayable member-publish route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_kv_revision: Option<KvRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterMemberPublishResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    pub kv_snapshot: Option<KvSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterAuthenticatedPeer {
    pub node_id: String,
    pub public_key: String,
}
