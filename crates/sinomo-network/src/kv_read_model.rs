use crate::ClusterPaths;
use crate::connectivity::{ClusterConnectivityRecord, ClusterConnectivityRecordQuery};
use crate::connectivity_publish::cluster_network_record_live;
use crate::endpoint_records::cleanup_member_iroh_endpoints;
use crate::error::NetworkError;
use crate::keyspace::cluster_endpoint_prefix;
use crate::kv_local_store::{
    read_cluster_kv_cached_snapshot_if_present,
    read_cluster_kv_latest_observed_snapshot_arc_if_present,
};
#[cfg(test)]
use crate::network_info::ClusterNetworkInfoRecord;
use sinomo_core::{ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeStatusRecord};
use sinomo_kv::{CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, KvSnapshot};
use sinomo_node::{ClusterCapabilityPolicyRecord, NodeId};
use std::collections::BTreeMap;
use time::OffsetDateTime;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct KvBackedClusterConnectivityRecordQuery;

impl ClusterConnectivityRecordQuery for KvBackedClusterConnectivityRecordQuery {
    fn read_kv_connectivity_records(
        &self,
        paths: &ClusterPaths,
    ) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
        read_cluster_connectivity_graph_from_kv(paths)
    }
}

pub fn read_cluster_members_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut members: Vec<ClusterMemberRecord> = Vec::new();
    for entry in
        snapshot.range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        members.push(entry.decode()?);
    }
    let policies = read_cluster_capability_policies_from_snapshot(&snapshot)?;
    for member in &mut members {
        if let Some(policy) = policies.get(&member.node_id) {
            member.capabilities = policy.capabilities.clone();
            member.allowed_capabilities = policy.allowed_capabilities.clone();
        }
        for endpoint in read_cluster_member_endpoints_from_snapshot(&snapshot, &member.node_id)? {
            if !member
                .endpoints
                .iter()
                .any(|candidate| candidate.endpoint_id == endpoint.endpoint_id)
            {
                member.endpoints.push(endpoint);
            }
        }
        cleanup_member_iroh_endpoints(member);
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(members)
}

pub(crate) fn read_cluster_endpoints_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_cached_snapshot_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut endpoints = read_cluster_endpoints_from_snapshot(&snapshot)?
        .into_values()
        .flatten()
        .collect::<Vec<_>>();
    endpoints.sort_by(|left, right| {
        (
            left.node_id.as_str(),
            left.priority,
            left.endpoint_id.as_str(),
        )
            .cmp(&(
                right.node_id.as_str(),
                right.priority,
                right.endpoint_id.as_str(),
            ))
    });
    Ok(endpoints)
}

fn read_cluster_member_endpoints_from_snapshot(
    snapshot: &KvSnapshot,
    node_id: &NodeId,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let endpoint_prefix = cluster_endpoint_prefix(node_id);
    let mut endpoints = Vec::new();
    for entry in
        snapshot.range_prefix_limited(&endpoint_prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        endpoints.push(entry.decode()?);
    }
    Ok(endpoints)
}

fn read_cluster_endpoints_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<BTreeMap<NodeId, Vec<ClusterEndpointRecord>>, NetworkError> {
    let mut endpoints: BTreeMap<NodeId, Vec<ClusterEndpointRecord>> = BTreeMap::new();
    for entry in
        snapshot.range_prefix_limited("/cluster/endpoints/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        let endpoint: ClusterEndpointRecord = entry.decode()?;
        endpoints
            .entry(endpoint.node_id.clone())
            .or_default()
            .push(endpoint);
    }
    Ok(endpoints)
}

pub(crate) fn read_cluster_capability_policies_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<BTreeMap<NodeId, ClusterCapabilityPolicyRecord>, NetworkError> {
    let mut policies = BTreeMap::new();
    for entry in snapshot.range_prefix_limited(
        "/cluster/policy/capabilities/",
        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let policy: ClusterCapabilityPolicyRecord = entry.decode()?;
        policies.insert(policy.node_id.clone(), policy);
    }
    Ok(policies)
}

pub fn read_cluster_node_statuses_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNodeStatusRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let mut statuses: Vec<ClusterNodeStatusRecord> = Vec::new();
    for entry in snapshot
        .range_prefix_limited("/cluster/node-status/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)?
    {
        statuses.push(entry.decode()?);
    }
    statuses.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.process.cmp(&right.process))
    });
    Ok(statuses)
}

#[cfg(test)]
pub(crate) fn read_cluster_network_info_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNetworkInfoRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let now = OffsetDateTime::now_utc();
    let mut records: Vec<ClusterNetworkInfoRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        "/cluster/network/info/",
        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let record: ClusterNetworkInfoRecord = entry.decode()?;
        if cluster_network_record_live(record.expires_at, now) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(records)
}

pub fn read_cluster_connectivity_graph_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let now = OffsetDateTime::now_utc();
    let mut records: Vec<ClusterConnectivityRecord> = Vec::new();
    for entry in snapshot.range_prefix_limited(
        "/cluster/network/connectivity/",
        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
    )? {
        let record: ClusterConnectivityRecord = entry.decode()?;
        if cluster_network_record_live(record.expires_at, now) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| {
        (
            left.source_node_id.as_str(),
            left.target_node_id.as_str(),
            left.endpoint_id.as_str(),
        )
            .cmp(&(
                right.source_node_id.as_str(),
                right.target_node_id.as_str(),
                right.endpoint_id.as_str(),
            ))
    });
    Ok(records)
}

pub(crate) fn cluster_node_status_map_from_kv(
    paths: &ClusterPaths,
) -> BTreeMap<NodeId, ClusterNodeStatusRecord> {
    read_cluster_node_statuses_from_kv(paths)
        .unwrap_or_default()
        .into_iter()
        .map(|status| (status.node_id.clone(), status))
        .collect()
}
