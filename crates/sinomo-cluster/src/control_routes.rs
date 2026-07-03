//! Daemon-mediated cluster control: object-RPC route constants, request and
//! response payload types, the route auth-policy table, and SOBJ encode/decode
//! helpers.
//!
//! The daemon registers these routes (`/cluster/control/*`) and executes each
//! operation in-process with its own transports and hosts; the CLI and app call
//! them through the daemon API instead of driving cluster mutations directly.
//! The payload codec mirrors `sinomo-kv`'s RPC helpers: name-keyed SOBJ
//! documents so fields can be added without breaking older peers.

use crate::prelude::*;
use serde::de::DeserializeOwned;
use sinomo_kv::ClusterKvRouteAuth;
use sinomo_network::NetworkError;
use sinomo_object::{LengthMode, serde_ext as sinomo_object_serde};

/// SOBJ document version for cluster-control RPC payloads.
pub const CLUSTER_CONTROL_OBJECT_VERSION_ID: u16 = 1;
/// Wire category shared by every cluster-control route.
pub const CLUSTER_CONTROL_RPC_CATEGORY: &str = "cluster.control";
/// Initialize a new single-node cluster on the daemon's node.
pub const CLUSTER_CONTROL_INIT_ROUTE: &str = "/cluster/control/init";
/// Join an existing cluster using an invite.
pub const CLUSTER_CONTROL_JOIN_ROUTE: &str = "/cluster/control/join";
/// Create and publish a cluster join invite.
pub const CLUSTER_CONTROL_INVITE_ROUTE: &str = "/cluster/control/invite";
/// Rename a cluster member (defaults to the daemon's local node).
pub const CLUSTER_CONTROL_RENAME_ROUTE: &str = "/cluster/control/rename";
/// Change a cluster member's node role.
pub const CLUSTER_CONTROL_ROLE_ROUTE: &str = "/cluster/control/role";
/// Remove cluster members.
pub const CLUSTER_CONTROL_REMOVE_ROUTE: &str = "/cluster/control/remove";
/// Leave the current cluster and re-initialize as a single-node cluster.
pub const CLUSTER_CONTROL_LEAVE_ROUTE: &str = "/cluster/control/leave";
/// Capability tag advertised for cluster-control mutations.
pub const CLUSTER_CONTROL_ADMIN_CAPABILITY: &str = "cluster.control.admin";

/// Every cluster-control object-RPC route.
pub const CLUSTER_CONTROL_RPC_ROUTES: &[&str] = &[
    CLUSTER_CONTROL_INIT_ROUTE,
    CLUSTER_CONTROL_JOIN_ROUTE,
    CLUSTER_CONTROL_INVITE_ROUTE,
    CLUSTER_CONTROL_RENAME_ROUTE,
    CLUSTER_CONTROL_ROLE_ROUTE,
    CLUSTER_CONTROL_REMOVE_ROUTE,
    CLUSTER_CONTROL_LEAVE_ROUTE,
];

/// Auth policy for one cluster-control route; same shape as
/// [`sinomo_kv::ClusterKvRpcRoutePolicy`] so daemon authorization can consult
/// both tables uniformly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterControlRoutePolicy {
    pub route: &'static str,
    pub category: &'static str,
    pub auth: ClusterKvRouteAuth,
    pub required_capabilities: &'static [&'static str],
}

const CLUSTER_CONTROL_MUTATION_AUTH: ClusterKvRouteAuth = ClusterKvRouteAuth::LocalOrRaftVoter;

const fn cluster_control_mutation_policy(route: &'static str) -> ClusterControlRoutePolicy {
    ClusterControlRoutePolicy {
        route,
        category: CLUSTER_CONTROL_RPC_CATEGORY,
        auth: CLUSTER_CONTROL_MUTATION_AUTH,
        required_capabilities: &[CLUSTER_CONTROL_ADMIN_CAPABILITY],
    }
}

/// Auth policy table: every cluster-control route is a mutation and requires an
/// admin signer (the local daemon identity or a cluster raft voter).
pub const CLUSTER_CONTROL_RPC_ROUTE_POLICIES: &[ClusterControlRoutePolicy] = &[
    cluster_control_mutation_policy(CLUSTER_CONTROL_INIT_ROUTE),
    cluster_control_mutation_policy(CLUSTER_CONTROL_JOIN_ROUTE),
    cluster_control_mutation_policy(CLUSTER_CONTROL_INVITE_ROUTE),
    cluster_control_mutation_policy(CLUSTER_CONTROL_RENAME_ROUTE),
    cluster_control_mutation_policy(CLUSTER_CONTROL_ROLE_ROUTE),
    cluster_control_mutation_policy(CLUSTER_CONTROL_REMOVE_ROUTE),
    cluster_control_mutation_policy(CLUSTER_CONTROL_LEAVE_ROUTE),
];

/// Looks up the auth policy for a cluster-control route.
pub fn cluster_control_rpc_route_policy(route: &str) -> Option<&'static ClusterControlRoutePolicy> {
    CLUSTER_CONTROL_RPC_ROUTE_POLICIES
        .iter()
        .find(|policy| policy.route == route)
}

/// Initialize a single-node cluster on the daemon's node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlInitRequest {
    /// Human-readable cluster name.
    pub cluster_name: String,
    /// Display name for the local node; defaults to the cluster name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_display_name: Option<String>,
}

/// Result of a cluster init.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlInitResponse {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
}

/// Join an existing cluster using an encoded invite URI (or join command).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlJoinRequest {
    /// `sinomo-join://...` invite URI or full `sinomo cluster join ...` command.
    pub invite: String,
}

/// Result of a cluster join.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlJoinResponse {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    /// Introducer endpoint address that accepted the join, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_endpoint: Option<String>,
    /// Present when the join ran as a single-node migration (the node was
    /// already settled in another cluster and its source storage was
    /// quarantined).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<ClusterControlJoinMigrationSummary>,
}

/// Summary of a single-node migration performed by a daemon-mediated join.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlJoinMigrationSummary {
    pub migration_id: sinomo_base::Id,
    pub source_cluster_id: ClusterId,
    /// Where the frozen source cluster storage (and the migration/import
    /// records) live on the node.
    pub quarantine_root: String,
    /// Node-local config keys imported into the target cluster's KV.
    pub imported_node_local_config_keys: usize,
    /// Node-local config keys deferred (target KV not yet writable).
    pub deferred_node_local_config_keys: usize,
}

/// Create and publish a cluster join invite.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlInviteRequest {
    /// Optional TTL override in seconds. When omitted, the daemon default is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<i64>,
    /// Optional introducer address overrides. When empty, daemon-default
    /// advertised introducers are used.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub introducers: Vec<String>,
}

/// A freshly published invite.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlInviteResponse {
    /// Encoded `sinomo-join://` invite URI.
    pub invite: String,
}

/// Rename a cluster member. Without `node_id` the daemon renames its local
/// node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlRenameRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
    pub display_name: String,
}

/// Result of a member rename.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlRenameResponse {
    pub node_id: NodeId,
    pub previous_name: String,
    pub name: String,
    pub changed: bool,
}

/// Change a cluster member's node role.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlRoleRequest {
    pub node_id: NodeId,
    pub role: ClusterNodeRole,
}

/// Result of a member role change.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlRoleResponse {
    pub node_id: NodeId,
    pub previous_role: ClusterNodeRole,
    pub role: ClusterNodeRole,
    pub changed: bool,
}

/// Remove cluster members by node id.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlRemoveRequest {
    pub node_ids: Vec<NodeId>,
}

/// One removed cluster member.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlRemovedMember {
    pub node_id: NodeId,
    pub node_role: ClusterNodeRole,
    pub state: ClusterMemberState,
}

/// Result of a member removal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlRemoveResponse {
    pub removed: Vec<ClusterControlRemovedMember>,
}

/// Leave the current cluster (local state is quarantined, not deleted).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlLeaveRequest {}

/// Result of leaving a cluster: the node re-initializes as a fresh
/// single-node cluster.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterControlLeaveResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_cluster_id: Option<ClusterId>,
    pub new_cluster_id: ClusterId,
    pub node_id: NodeId,
}

/// An encoded cluster-control RPC body plus its route/category, mirroring
/// [`sinomo_kv::ClusterKvRpcPayload`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterControlRpcPayload {
    pub route: &'static str,
    pub category: &'static str,
    pub body: Vec<u8>,
}

fn encode_cluster_control_rpc_body<T: Serialize + ?Sized>(
    value: &T,
) -> Result<Vec<u8>, NetworkError> {
    sinomo_object_serde::to_bytes(value, CLUSTER_CONTROL_OBJECT_VERSION_ID, LengthMode::U32)
        .map_err(|source| NetworkError::Serialization {
            detail: format!("cluster control rpc body encode failed: {source}"),
        })
}

fn decode_cluster_control_rpc_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, NetworkError> {
    sinomo_object_serde::from_bytes(body).map_err(|source| NetworkError::Serialization {
        detail: format!("cluster control rpc body decode failed: {source}"),
    })
}

fn cluster_control_rpc_payload<T: Serialize + ?Sized>(
    route: &'static str,
    value: &T,
) -> Result<ClusterControlRpcPayload, NetworkError> {
    Ok(ClusterControlRpcPayload {
        route,
        category: CLUSTER_CONTROL_RPC_CATEGORY,
        body: encode_cluster_control_rpc_body(value)?,
    })
}

macro_rules! cluster_control_rpc_codec {
    (
        $route:ident,
        $request:ty,
        $response:ty,
        $request_payload:ident,
        $decode_request:ident,
        $response_payload:ident,
        $decode_response:ident $(,)?
    ) => {
        /// Encodes the request payload for this cluster-control route.
        pub fn $request_payload(
            request: &$request,
        ) -> Result<ClusterControlRpcPayload, NetworkError> {
            cluster_control_rpc_payload($route, request)
        }

        /// Decodes the request body received on this cluster-control route.
        pub fn $decode_request(body: &[u8]) -> Result<$request, NetworkError> {
            decode_cluster_control_rpc_body(body)
        }

        /// Encodes the response payload for this cluster-control route.
        pub fn $response_payload(
            response: &$response,
        ) -> Result<ClusterControlRpcPayload, NetworkError> {
            cluster_control_rpc_payload($route, response)
        }

        /// Decodes the response body returned by this cluster-control route.
        pub fn $decode_response(body: &[u8]) -> Result<$response, NetworkError> {
            decode_cluster_control_rpc_body(body)
        }
    };
}

cluster_control_rpc_codec!(
    CLUSTER_CONTROL_INIT_ROUTE,
    ClusterControlInitRequest,
    ClusterControlInitResponse,
    cluster_control_init_rpc_payload,
    decode_cluster_control_init_rpc_request_body,
    cluster_control_init_response_rpc_payload,
    decode_cluster_control_init_rpc_response_body,
);

cluster_control_rpc_codec!(
    CLUSTER_CONTROL_JOIN_ROUTE,
    ClusterControlJoinRequest,
    ClusterControlJoinResponse,
    cluster_control_join_rpc_payload,
    decode_cluster_control_join_rpc_request_body,
    cluster_control_join_response_rpc_payload,
    decode_cluster_control_join_rpc_response_body,
);

cluster_control_rpc_codec!(
    CLUSTER_CONTROL_INVITE_ROUTE,
    ClusterControlInviteRequest,
    ClusterControlInviteResponse,
    cluster_control_invite_rpc_payload,
    decode_cluster_control_invite_rpc_request_body,
    cluster_control_invite_response_rpc_payload,
    decode_cluster_control_invite_rpc_response_body,
);

cluster_control_rpc_codec!(
    CLUSTER_CONTROL_RENAME_ROUTE,
    ClusterControlRenameRequest,
    ClusterControlRenameResponse,
    cluster_control_rename_rpc_payload,
    decode_cluster_control_rename_rpc_request_body,
    cluster_control_rename_response_rpc_payload,
    decode_cluster_control_rename_rpc_response_body,
);

cluster_control_rpc_codec!(
    CLUSTER_CONTROL_ROLE_ROUTE,
    ClusterControlRoleRequest,
    ClusterControlRoleResponse,
    cluster_control_role_rpc_payload,
    decode_cluster_control_role_rpc_request_body,
    cluster_control_role_response_rpc_payload,
    decode_cluster_control_role_rpc_response_body,
);

cluster_control_rpc_codec!(
    CLUSTER_CONTROL_REMOVE_ROUTE,
    ClusterControlRemoveRequest,
    ClusterControlRemoveResponse,
    cluster_control_remove_rpc_payload,
    decode_cluster_control_remove_rpc_request_body,
    cluster_control_remove_response_rpc_payload,
    decode_cluster_control_remove_rpc_response_body,
);

cluster_control_rpc_codec!(
    CLUSTER_CONTROL_LEAVE_ROUTE,
    ClusterControlLeaveRequest,
    ClusterControlLeaveResponse,
    cluster_control_leave_rpc_payload,
    decode_cluster_control_leave_rpc_request_body,
    cluster_control_leave_response_rpc_payload,
    decode_cluster_control_leave_rpc_response_body,
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_control_policy_table_covers_every_route() {
        assert_eq!(
            CLUSTER_CONTROL_RPC_ROUTES.len(),
            CLUSTER_CONTROL_RPC_ROUTE_POLICIES.len()
        );
        for route in CLUSTER_CONTROL_RPC_ROUTES {
            let policy = cluster_control_rpc_route_policy(route)
                .unwrap_or_else(|| panic!("cluster control route {route} has no auth policy"));
            assert_eq!(policy.route, *route);
            assert_eq!(policy.category, CLUSTER_CONTROL_RPC_CATEGORY);
            assert!(route.starts_with("/cluster/control/"));
        }
    }

    #[test]
    fn cluster_control_routes_are_unique() {
        let mut routes: Vec<&str> = CLUSTER_CONTROL_RPC_ROUTES.to_vec();
        routes.sort_unstable();
        routes.dedup();
        assert_eq!(routes.len(), CLUSTER_CONTROL_RPC_ROUTES.len());
    }

    #[test]
    fn cluster_control_mutations_require_local_or_voter_admin() {
        for policy in CLUSTER_CONTROL_RPC_ROUTE_POLICIES {
            assert_eq!(
                policy.auth,
                ClusterKvRouteAuth::LocalOrRaftVoter,
                "cluster control route {} must require a local or voter signer",
                policy.route
            );
            assert_eq!(policy.auth.as_scope(), "local-or-cluster-voter");
            assert_eq!(
                policy.required_capabilities,
                &[CLUSTER_CONTROL_ADMIN_CAPABILITY],
                "cluster control route {} must advertise the admin capability",
                policy.route
            );
        }
    }

    #[test]
    fn cluster_control_rpc_route_policy_rejects_unknown_routes() {
        assert!(cluster_control_rpc_route_policy("/cluster/control/unknown").is_none());
        assert!(cluster_control_rpc_route_policy("/cluster/kv/txn").is_none());
    }

    #[test]
    fn cluster_control_payloads_round_trip() {
        let init = ClusterControlInitRequest {
            cluster_name: "test-cluster".to_string(),
            node_display_name: Some("test-node".to_string()),
        };
        let payload = cluster_control_init_rpc_payload(&init).expect("encode init");
        assert_eq!(payload.route, CLUSTER_CONTROL_INIT_ROUTE);
        assert_eq!(payload.category, CLUSTER_CONTROL_RPC_CATEGORY);
        assert_eq!(
            decode_cluster_control_init_rpc_request_body(&payload.body).expect("decode init"),
            init
        );

        let rename = ClusterControlRenameRequest {
            node_id: None,
            display_name: "renamed".to_string(),
        };
        let payload = cluster_control_rename_rpc_payload(&rename).expect("encode rename");
        assert_eq!(
            decode_cluster_control_rename_rpc_request_body(&payload.body).expect("decode rename"),
            rename
        );

        let node_id = NodeId::generate();
        let role = ClusterControlRoleResponse {
            node_id: node_id.clone(),
            previous_role: ClusterNodeRole::Workstation,
            role: ClusterNodeRole::Server,
            changed: true,
        };
        let payload = cluster_control_role_response_rpc_payload(&role).expect("encode role");
        assert_eq!(
            decode_cluster_control_role_rpc_response_body(&payload.body).expect("decode role"),
            role
        );

        let leave = ClusterControlLeaveResponse {
            previous_cluster_id: None,
            new_cluster_id: ClusterId::generate(),
            node_id,
        };
        let payload = cluster_control_leave_response_rpc_payload(&leave).expect("encode leave");
        assert_eq!(
            decode_cluster_control_leave_rpc_response_body(&payload.body).expect("decode leave"),
            leave
        );
    }
}
