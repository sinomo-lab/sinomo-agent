use crate::NodeId;
use sinomo_core::{
    CapabilityName, ClusterMemberRecord, ClusterMemberRole, ClusterMemberState,
    ClusterTransportKind, Id,
};

pub const DAEMON_MAIN_RUN_CAPABILITY: &str = "daemon.main.run";
pub const DAEMON_SHADOW_SUPERVISE_CAPABILITY: &str = "daemon.shadow.supervise";

pub fn shadow_daemon_node_id(main_node_id: &NodeId) -> NodeId {
    NodeId::derived("sinomo.node.shadow", main_node_id.as_str())
}

pub fn main_daemon_capabilities(role: &ClusterMemberRole) -> Vec<CapabilityName> {
    let mut capabilities = vec![
        CapabilityName::new("cluster.message.receive").expect("static capability"),
        CapabilityName::new("cluster.message.send").expect("static capability"),
        CapabilityName::new(DAEMON_MAIN_RUN_CAPABILITY).expect("static capability"),
    ];
    if matches!(role, ClusterMemberRole::Voter) {
        capabilities.push(CapabilityName::new("cluster.kv.vote").expect("static capability"));
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
