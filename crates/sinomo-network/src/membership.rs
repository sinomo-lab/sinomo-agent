use sinomo_core::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource,
    ClusterTransportKind,
};
use sinomo_node::cluster_has_remote_peers;
use std::net::SocketAddr;

use crate::local_membership::cluster_state_needs_join_registration;

pub(crate) fn locally_learned_bridge_route_endpoint(endpoint: &ClusterEndpointRecord) -> bool {
    matches!(
        endpoint.transport,
        ClusterTransportKind::LanTcp | ClusterTransportKind::Loopback
    ) && endpoint.address.parse::<SocketAddr>().is_err()
        && endpoint.address.contains(':')
        && matches!(endpoint.source, ClusterEndpointSource::Observed)
        && endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
}

pub fn cluster_is_settled_solo(state: &ClusterBootstrapState) -> bool {
    !cluster_has_remote_peers(state) && !cluster_state_needs_join_registration(state)
}
