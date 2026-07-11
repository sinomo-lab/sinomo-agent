use crate::invite_publish::{
    ClusterInvitePublishHost, handle_cluster_invite_publish_request_with_host,
};
use crate::join_registration::{
    ClusterJoinRegistrationHost, handle_cluster_join_registration_request_with_host,
};
use crate::member_publish::{
    ClusterMemberPublishHost, handle_cluster_member_publish_request_with_host,
};
use crate::prelude::*;
use sinomo_kv::KvRevision;
#[allow(unused_imports)]
use sinomo_kv::KvSnapshot;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{
    CLUSTER_INVITE_PUBLISH_ROUTE, CLUSTER_JOIN_REGISTER_ROUTE, CLUSTER_MEMBER_PUBLISH_ROUTE,
    ObjectRpcRequest, ObjectRpcResponse,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationRequest {
    pub invite: ClusterJoinInvite,
    pub invitation_secret: String,
    #[serde(deserialize_with = "sinomo_network::deserialize_vec")]
    pub members: Vec<ClusterMemberRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterJoinRegistrationResponse {
    pub accepted: bool,
    pub detail: String,
    pub state: ClusterBootstrapState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_snapshot: Option<KvSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responder_node_id: Option<NodeId>,
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
    #[serde(deserialize_with = "sinomo_network::deserialize_vec")]
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

pub fn handle_cluster_membership_rpc_request_with_hosts<E, J, M, I>(
    paths: &ClusterPaths,
    request: ObjectRpcRequest,
    signer_node_id: &str,
    signer_public_key: &str,
    now: OffsetDateTime,
    join_host: &mut J,
    member_host: &mut M,
    invite_host: &mut I,
) -> Result<ObjectRpcResponse, E>
where
    E: From<NetworkError> + ToString,
    J: ClusterJoinRegistrationHost,
    M: ClusterMemberPublishHost<Error = E>,
    I: ClusterInvitePublishHost<Error = E>,
{
    match request.route.as_str() {
        CLUSTER_JOIN_REGISTER_ROUTE => {
            let payload: ClusterJoinRegistrationRequest = request.decode_body().map_err(E::from)?;
            let response =
                handle_cluster_join_registration_request_with_host(paths, payload, now, join_host)
                    .map_err(E::from)?;
            ObjectRpcResponse::encode_body(CLUSTER_JOIN_REGISTER_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_INVITE_PUBLISH_ROUTE => {
            let payload: ClusterInvitePublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_invite_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                signer_public_key,
                now,
                invite_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_INVITE_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        CLUSTER_MEMBER_PUBLISH_ROUTE => {
            let payload: ClusterMemberPublishRequest = request.decode_body().map_err(E::from)?;
            let response = handle_cluster_member_publish_request_with_host(
                paths,
                payload,
                signer_node_id,
                member_host,
            )?;
            ObjectRpcResponse::encode_body(CLUSTER_MEMBER_PUBLISH_ROUTE, &response).map_err(E::from)
        }
        other => Err(E::from(NetworkError::UnknownRoute {
            route: other.to_string(),
        })),
    }
}
