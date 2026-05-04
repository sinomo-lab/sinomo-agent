use sinomo_core::{
    CapabilityName, ClusterEndpointId, ClusterMemberRole, ClusterTransportKind, NodeId,
};

pub fn shadow_daemon_node_id(main_node_id: &NodeId) -> NodeId {
    NodeId::new(format!("{}-shadow", main_node_id.as_str())).expect("derived shadow daemon node id")
}

pub fn main_daemon_capabilities(role: &ClusterMemberRole) -> Vec<CapabilityName> {
    let mut capabilities = vec![
        CapabilityName::new("cluster.message.receive").expect("static capability"),
        CapabilityName::new("cluster.message.send").expect("static capability"),
        CapabilityName::new("daemon.main.run").expect("static capability"),
    ];
    if matches!(role, ClusterMemberRole::Voter) {
        capabilities.push(CapabilityName::new("cluster.kv.vote").expect("static capability"));
    }
    capabilities
}

pub fn shadow_daemon_capabilities() -> Vec<CapabilityName> {
    vec![
        CapabilityName::new("cluster.message.receive").expect("static capability"),
        CapabilityName::new("cluster.message.send").expect("static capability"),
        CapabilityName::new("daemon.shadow.supervise").expect("static capability"),
    ]
}

pub fn daemon_loopback_endpoint_id(node_id: &NodeId) -> ClusterEndpointId {
    let body = node_id.as_str().trim_start_matches(NodeId::PREFIX);
    ClusterEndpointId::new(format!("endpoint_{body}_daemon_loopback"))
        .expect("derived daemon endpoint id")
}

pub fn daemon_lan_endpoint_id(node_id: &NodeId, address: &str) -> ClusterEndpointId {
    let node = node_id.as_str().trim_start_matches(NodeId::PREFIX);
    let address = sanitize_endpoint_id_body(address);
    ClusterEndpointId::new(format!("endpoint_{node}_daemon_lan_{address}"))
        .expect("derived daemon lan endpoint id")
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
