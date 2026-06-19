use crate::prelude::*;
use sinomo_node::DAEMON_MAIN_CLUSTER_ROLE;
pub(crate) use sinomo_node::{cluster_capability_policy_key, cluster_identity_key};

pub(crate) fn cluster_node_status_key(node_id: &NodeId) -> String {
    format!("/cluster/node-status/{node_id}")
}

pub(crate) fn cluster_node_status_key_for_record(status: &ClusterNodeStatusRecord) -> String {
    if status.process == DAEMON_MAIN_CLUSTER_ROLE {
        cluster_node_status_key(&status.node_id)
    } else {
        cluster_node_status_key_for_process(&status.node_id, &status.process)
    }
}

pub(crate) fn cluster_node_status_key_for_process(node_id: &NodeId, process: &str) -> String {
    if process == DAEMON_MAIN_CLUSTER_ROLE {
        cluster_node_status_key(node_id)
    } else {
        format!("/cluster/node-status/{node_id}/{process}")
    }
}

pub(crate) fn cluster_node_status_key_matches_record(
    key: &str,
    status: &ClusterNodeStatusRecord,
) -> bool {
    key == cluster_node_status_key_for_record(status)
        || (status.process == DAEMON_MAIN_CLUSTER_ROLE
            && key == cluster_node_status_key(&status.node_id))
}

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

pub fn cluster_member_key(node_id: &NodeId) -> String {
    format!("/cluster/members/{node_id}")
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

#[cfg(test)]
pub(crate) fn cluster_certificate_key(certificate: &ClusterCertificate) -> String {
    format!("/cluster/certificates/{}", certificate.certificate_id)
}

pub fn cluster_invitation_key(invite_id: &ClusterInviteId) -> String {
    format!("/cluster/invitations/{invite_id}")
}

pub fn cluster_invitation_state_key(invite_id: &ClusterInviteId) -> String {
    format!("/cluster/invitations/{invite_id}/state")
}
