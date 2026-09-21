use crate::NodeId;
use sinomo_base::{
    CapabilityName, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterTransportKind, Id,
};

pub use sinomo_base::DAEMON_MAIN_RUN_CAPABILITY;
pub const DAEMON_SHADOW_SUPERVISE_CAPABILITY: &str = "daemon.shadow.supervise";
/// Allows a current Raft voter to administer replicated cluster membership and
/// identity through the daemon-owned cluster-control routes.
pub const CLUSTER_CONTROL_ADMIN_CAPABILITY: &str = "cluster.control.admin";
/// Allows a cluster member to inspect built-in native Rust module state.
pub const MODULE_NATIVE_RUST_READ_CAPABILITY: &str = "module.native-rust.read";
/// Allows a cluster member, subject to node-local remote-management consent,
/// to change built-in native Rust module state on the target node.
pub const MODULE_NATIVE_RUST_ADMIN_CAPABILITY: &str = "module.native-rust.admin";
/// Allows a cluster member, subject to node-local remote-management consent,
/// to withdraw a module generation and drain its in-flight work on the target.
pub const MODULE_HOT_UNLOAD_ADMIN_CAPABILITY: &str = "module.hot-unload.admin";
/// Advertised only by main daemons that understand snapshot-bound placement
/// reconciliation jobs and their v1 control-plane namespace.
pub const DFS_PLACEMENT_RECONCILIATION_V1_CAPABILITY: &str = "dfs.placement.reconciliation.v1";
/// Advertised by main daemons that can replay packed-v1 namespace prepare
/// journal bodies. A shard may select that persisted format only when every
/// assigned metadata member advertises this capability.
pub const DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY: &str =
    "dfs.metadata.namespace-prepare.packed.v1";
/// Advertised by main daemons that can replay packed-v2 namespace prepare
/// journal bodies as well as every earlier namespace-prepare format. Selection
/// remains capability-fenced per metadata shard so an older assigned member
/// can never receive a format it cannot decode.
pub const DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY: &str =
    "dfs.metadata.namespace-prepare.packed.v2";
/// Packed-v3 supports the complete file-history contract: captures, pins,
/// retained repair owners, and durability plans across metadata generations.
/// Earlier development builds advertised partial history capabilities instead;
/// they must not satisfy admission for this consolidated format.
pub const DFS_METADATA_PACKED_NAMESPACE_PREPARE_V3_CAPABILITY: &str =
    "dfs.metadata.namespace-prepare.packed.v3";
pub const DAEMON_MAIN_CLUSTER_ROLE: &str = "daemon-main";
pub const DAEMON_SHADOW_CLUSTER_ROLE: &str = "daemon-shadow";

pub fn shadow_daemon_node_id(main_node_id: &NodeId) -> NodeId {
    NodeId::derived("sinomo.node.shadow", main_node_id.as_str())
}

pub fn main_daemon_capabilities(role: &ClusterMemberRole) -> Vec<CapabilityName> {
    let mut capabilities = vec![
        CapabilityName::new("cluster.message.receive").expect("static capability"),
        CapabilityName::new("cluster.message.send").expect("static capability"),
        CapabilityName::new("cluster.kv.read").expect("static capability"),
        CapabilityName::new("cluster.kv.write").expect("static capability"),
        CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("static capability"),
        CapabilityName::new(MODULE_NATIVE_RUST_READ_CAPABILITY).expect("static capability"),
        CapabilityName::new(MODULE_NATIVE_RUST_ADMIN_CAPABILITY).expect("static capability"),
        CapabilityName::new(MODULE_HOT_UNLOAD_ADMIN_CAPABILITY).expect("static capability"),
        CapabilityName::new(DFS_PLACEMENT_RECONCILIATION_V1_CAPABILITY).expect("static capability"),
        CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V1_CAPABILITY)
            .expect("static capability"),
        CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V2_CAPABILITY)
            .expect("static capability"),
        CapabilityName::new(DFS_METADATA_PACKED_NAMESPACE_PREPARE_V3_CAPABILITY)
            .expect("static capability"),
    ];
    if matches!(role, ClusterMemberRole::Voter) {
        capabilities.push(CapabilityName::new("cluster.kv.vote").expect("static capability"));
        capabilities.push(
            CapabilityName::new(CLUSTER_CONTROL_ADMIN_CAPABILITY).expect("static capability"),
        );
    }
    capabilities
}

pub fn apply_cluster_raft_role(
    member: &mut ClusterMemberRecord,
    role: ClusterMemberRole,
    timestamp: time::OffsetDateTime,
) {
    member.role = role.clone();
    member.state = match role {
        ClusterMemberRole::Voter => ClusterMemberState::Voter,
        ClusterMemberRole::Learner => ClusterMemberState::Learner,
        ClusterMemberRole::NonVoting => ClusterMemberState::NonVoting,
    };
    member.capabilities = main_daemon_capabilities(&role);
    member.allowed_capabilities = member.capabilities.clone();
    member.updated_at = timestamp;
}

pub fn shadow_daemon_capabilities() -> Vec<CapabilityName> {
    vec![
        CapabilityName::new("cluster.message.receive").expect("static capability"),
        CapabilityName::new("cluster.message.send").expect("static capability"),
        CapabilityName::new(DAEMON_SHADOW_SUPERVISE_CAPABILITY).expect("static capability"),
    ]
}

pub fn daemon_loopback_endpoint_id(node_id: &NodeId) -> Id {
    Id::derived("sinomo.daemon.endpoint.loopback", node_id.as_str())
}

pub fn daemon_lan_endpoint_id(node_id: &NodeId, address: &str) -> Id {
    Id::derived(
        "sinomo.daemon.endpoint.lan",
        format!("{}\0{address}", node_id.as_str()),
    )
}

pub fn sanitize_endpoint_id_body(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

pub fn cluster_transport_capability_label(transport: &ClusterTransportKind) -> &'static str {
    match transport {
        ClusterTransportKind::InProcess => "cluster.transport.in-process",
        ClusterTransportKind::Loopback => "cluster.transport.loopback",
        ClusterTransportKind::LanTcp => "cluster.transport.lan-tcp",
        ClusterTransportKind::LanQuic => "cluster.transport.lan-quic",
        ClusterTransportKind::Iroh => "cluster.transport.iroh",
        ClusterTransportKind::WebRtc => "cluster.transport.webrtc",
        ClusterTransportKind::TurnRelay => "cluster.transport.turn-relay",
        ClusterTransportKind::HttpsRelay => "cluster.transport.https-relay",
    }
}
