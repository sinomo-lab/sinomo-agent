use crate::prelude::*;
use sinomo_kv::KvSnapshot;
#[cfg(any(test, feature = "test-support"))]
pub(crate) use sinomo_node::cluster_node_status_key_matches_record;
pub(crate) use sinomo_node::{
    cluster_capability_policy_key, cluster_identity_key, cluster_invitation_key,
    cluster_invitation_state_key, cluster_member_key, cluster_node_status_key,
    cluster_node_status_key_for_process, cluster_node_status_key_for_record,
};

pub(crate) fn cluster_node_status_snapshot_record(
    snapshot: &KvSnapshot,
    status: &ClusterNodeStatusRecord,
) -> Option<ClusterNodeStatusRecord> {
    let key = cluster_node_status_key_for_record(status);
    snapshot
        .get(&key)
        .and_then(|entry| entry.decode::<ClusterNodeStatusRecord>().ok())
}

pub fn cluster_network_info_key(node_id: &NodeId) -> String {
    format!("/cluster/network/info/{node_id}")
}

pub fn cluster_connectivity_source_prefix(source_node_id: &NodeId) -> String {
    format!("/cluster/network/connectivity/{source_node_id}/")
}

pub fn cluster_connectivity_key(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    endpoint_id: &ClusterEndpointId,
) -> String {
    format!("/cluster/network/connectivity/{source_node_id}/{target_node_id}/{endpoint_id}")
}

pub fn cluster_endpoint_key(node_id: &NodeId, endpoint: &ClusterEndpointRecord) -> String {
    format!(
        "{}{}",
        cluster_endpoint_prefix(node_id),
        endpoint.endpoint_id
    )
}

pub fn cluster_endpoint_prefix(node_id: &NodeId) -> String {
    format!("/cluster/endpoints/{node_id}/")
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_certificate_key(certificate: &ClusterCertificate) -> String {
    format!("/cluster/certificates/{}", certificate.certificate_id)
}
