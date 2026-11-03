//! Operator-supplied routes to an existing peer.
//!
//! A node whose peers all moved while it was offline may know no reachable
//! address for any of them. An operator can give it one. The hint is route data
//! in the local bootstrap state, which consensus routing reads; the transport
//! still authenticates the peer on every call, and the hint never changes
//! membership, keys or roles. Once contact lets the cluster catch this node up,
//! committed KV supplies the peer's published endpoints and a hint for an
//! address the peer publishes is dropped as redundant.

use crate::local_state_store::{
    acquire_local_cluster_state_mutation_lock, write_operator_route_hint_state_unlocked,
};
use serde::{Deserialize, Serialize};
use sinomo_base::{ClusterBootstrapState, ClusterEndpointRecord};
pub(crate) use sinomo_network::is_cluster_route_hint_endpoint;
use sinomo_network::{
    ClusterPaths, NetworkError, cluster_route_hint_endpoint_id as route_hint_endpoint_id,
    read_object_file,
};
use sinomo_node::{NodeId, cluster_node_is_local_process};

pub const CLUSTER_CONTROL_ROUTE_HINT_ROUTE: &str = "/cluster/control/route-hint";

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterRouteHintRequest {
    pub node_id: NodeId,
    /// `host:port` of the peer's daemon listener.
    pub address: String,
    #[serde(default)]
    pub remove: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterRouteHintResponse {
    pub endpoint: ClusterEndpointRecord,
    /// Whether the local bootstrap state changed. Recording an existing hint,
    /// an address the peer already publishes, or removing an absent hint is a
    /// no-op.
    pub changed: bool,
}

fn cluster_route_hint_endpoint(
    node_id: &NodeId,
    address: &str,
) -> Result<ClusterEndpointRecord, NetworkError> {
    let invalid = |detail: &str| NetworkError::directory(format!("route hint: {detail}"));
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| invalid("address must be host:port"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() || port.parse::<u16>().ok().filter(|port| *port != 0).is_none() {
        return Err(invalid("address must be host:port with a non-zero port"));
    }
    if let Ok(ip) = host.parse::<std::net::IpAddr>()
        && (ip.is_unspecified() || ip.is_multicast())
    {
        return Err(invalid("address must name a reachable host"));
    }
    let mut endpoint = sinomo_network::daemon_lan_endpoint(node_id, address.to_string(), 0);
    endpoint.endpoint_id = route_hint_endpoint_id(node_id, address);
    Ok(endpoint)
}

/// Records or removes one route hint for an authorized remote member.
pub fn apply_cluster_route_hint(
    paths: &ClusterPaths,
    request: &ClusterRouteHintRequest,
) -> Result<ClusterRouteHintResponse, NetworkError> {
    let endpoint = cluster_route_hint_endpoint(&request.node_id, &request.address)?;
    let _lock = acquire_local_cluster_state_mutation_lock(paths)?;
    let mut state: ClusterBootstrapState = read_object_file(&paths.state)?;
    if cluster_node_is_local_process(&state, &request.node_id) {
        return Err(NetworkError::directory(
            "route hint: the target is this node; hints name a remote peer",
        ));
    }
    let member = state
        .members
        .iter_mut()
        .find(|member| member.node_id == request.node_id && member.state.is_authorized())
        .ok_or(NetworkError::UnauthorizedNode {
            node_id: request.node_id.clone(),
        })?;
    let changed = if request.remove {
        let before = member.endpoints.len();
        member
            .endpoints
            .retain(|existing| existing.endpoint_id != endpoint.endpoint_id);
        member.endpoints.len() != before
    } else if member
        .endpoints
        .iter()
        .any(|existing| existing.address == endpoint.address)
    {
        false
    } else {
        member.endpoints.push(endpoint.clone());
        true
    };
    if changed {
        write_operator_route_hint_state_unlocked(paths, &state)?;
        crate::kv_read_model::invalidate_cluster_directory_projection(paths);
        sinomo_network::invalidate_cluster_route_graph_cache(paths);
    }
    Ok(ClusterRouteHintResponse { endpoint, changed })
}

#[cfg(test)]
mod tests;
