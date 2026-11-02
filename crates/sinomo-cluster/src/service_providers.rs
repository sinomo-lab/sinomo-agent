//! Cluster-wide provider selection over the committed service catalog.

use crate::kv_local_store::read_cluster_kv_current_or_cached_snapshot_arc_if_present;
use crate::kv_read_model::{KvClusterDirectory, read_cluster_members_from_snapshot};
use sinomo_base::ClusterBootstrapState;
use sinomo_network::{
    ClusterConnectivityGraph, ClusterPaths, NetworkError, cluster_route_next_hop,
};
use sinomo_node::NodeId;
use sinomo_service::{
    CLUSTER_SERVICES_KV_PREFIX, ServiceDiscoveryHealth, ServiceInstanceRecord,
    ServiceTargetSelector, TargetSelectionPolicy,
};
use std::collections::BTreeSet;

/// Nodes that can serve `capability`, in the order a caller should try them:
/// this node first when it provides the capability, then by route cost.
///
/// `local_offer` is this node's live registry, which is current before the
/// catalog publishes it. A remote provider qualifies when its committed record
/// is ready, it is an authorized member and the routing graph has a usable
/// path to it. Reachability is routing evidence; the selected destination still
/// authorizes the call.
pub fn resolve_cluster_service_providers(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    capability: &str,
    local_offer: bool,
) -> Result<Vec<NodeId>, NetworkError> {
    let local = &state.local_node_id;
    let mut candidates = Vec::new();
    if local_offer {
        candidates.push(ServiceInstanceRecord::new(capability, local.clone()));
    }
    if let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? {
        let authorized = read_cluster_members_from_snapshot(&snapshot)?
            .into_iter()
            .filter(|member| member.state.is_authorized())
            .map(|member| member.node_id)
            .collect::<BTreeSet<_>>();
        let graph = ClusterConnectivityGraph::read_for_local(paths, state);
        for entry in snapshot.prefix_entries(CLUSTER_SERVICES_KV_PREFIX) {
            // A record this node cannot decode is not an offer it can use.
            let Ok(record) = entry.decode::<ServiceInstanceRecord>() else {
                continue;
            };
            if &record.node_id == local
                || record.health != ServiceDiscoveryHealth::Ready
                || !record
                    .provider_capabilities
                    .iter()
                    .any(|provided| provided == capability)
                || !authorized.contains(&record.node_id)
                || cluster_route_next_hop(&graph, local, &record.node_id).is_none()
            {
                continue;
            }
            candidates.push(record);
        }
    }
    ServiceTargetSelector::new(TargetSelectionPolicy::LocalFirst).select_targets(
        &candidates,
        &KvClusterDirectory::new(paths.clone()),
        local,
    )
}
