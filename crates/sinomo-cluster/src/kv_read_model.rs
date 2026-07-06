use crate::keyspace::cluster_endpoint_prefix;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::{
    read_cluster_kv_cached_snapshot_if_present, read_cluster_kv_current_snapshot_arc,
    read_cluster_kv_latest_observed_snapshot_arc_if_present,
};
use sinomo_base::{ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeStatusRecord};
use sinomo_kv::{CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, KvSnapshot};
#[cfg(any(test, feature = "test-support"))]
use sinomo_network::ClusterNetworkInfoRecord;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::cleanup_member_endpoint_duplicates;
use sinomo_network::{ClusterConnectivityRecord, cluster_network_record_live};
use sinomo_node::{ClusterCapabilityPolicyRecord, NodeId};
use std::collections::BTreeMap;
use time::OffsetDateTime;

pub fn read_cluster_members_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    read_cluster_members_from_snapshot(snapshot.as_ref())
}

pub fn read_cluster_members_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
    let mut members: Vec<ClusterMemberRecord> = Vec::new();
    for entry in snapshot
        .range_prefix_limited("/cluster/members/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map_err(kv_err)?
    {
        members.push(entry.decode().map_err(kv_err)?);
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
        cleanup_member_endpoint_duplicates(member);
    }
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(members)
}

pub fn read_cluster_endpoints_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_current_or_cached_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let endpoint_map = read_cluster_endpoints_from_snapshot(&snapshot)?;
    let mut endpoints = Vec::new();
    for endpoint in endpoint_map.into_values().flatten() {
        sinomo_network::try_reserve_vec(&mut endpoints, 1, "collect cluster endpoints from kv")?;
        endpoints.push(endpoint);
    }
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

fn read_cluster_kv_current_or_cached_snapshot_arc_if_present(
    paths: &ClusterPaths,
) -> Result<Option<std::sync::Arc<KvSnapshot>>, NetworkError> {
    match read_cluster_kv_current_snapshot_arc(paths) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(current_error) => match read_cluster_kv_cached_snapshot_if_present(paths)? {
            Some(snapshot) => Ok(Some(snapshot)),
            None => Err(current_error),
        },
    }
}

fn read_cluster_member_endpoints_from_snapshot(
    snapshot: &KvSnapshot,
    node_id: &NodeId,
) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
    let endpoint_prefix = cluster_endpoint_prefix(node_id);
    let mut endpoints = Vec::new();
    for entry in snapshot
        .range_prefix_limited(&endpoint_prefix, CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map_err(kv_err)?
    {
        endpoints.push(entry.decode().map_err(kv_err)?);
    }
    Ok(endpoints)
}

fn read_cluster_endpoints_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<BTreeMap<NodeId, Vec<ClusterEndpointRecord>>, NetworkError> {
    let mut endpoints: BTreeMap<NodeId, Vec<ClusterEndpointRecord>> = BTreeMap::new();
    for entry in snapshot
        .range_prefix_limited("/cluster/endpoints/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map_err(kv_err)?
    {
        let endpoint: ClusterEndpointRecord = entry.decode().map_err(kv_err)?;
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
    for entry in snapshot
        .range_prefix_limited(
            "/cluster/policy/capabilities/",
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )
        .map_err(kv_err)?
    {
        let policy: ClusterCapabilityPolicyRecord = entry.decode().map_err(kv_err)?;
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
        .range_prefix_limited("/cluster/node-status/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map_err(kv_err)?
    {
        statuses.push(entry.decode().map_err(kv_err)?);
    }
    statuses.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.process.cmp(&right.process))
    });
    Ok(statuses)
}

#[cfg(any(test, feature = "test-support"))]
pub fn read_cluster_network_info_from_kv(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterNetworkInfoRecord>, NetworkError> {
    let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)? else {
        return Ok(Vec::new());
    };
    let now = OffsetDateTime::now_utc();
    let mut records: Vec<ClusterNetworkInfoRecord> = Vec::new();
    for entry in snapshot
        .range_prefix_limited(
            "/cluster/network/info/",
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )
        .map_err(kv_err)?
    {
        let record: ClusterNetworkInfoRecord = entry.decode().map_err(kv_err)?;
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
    for entry in snapshot
        .range_prefix_limited(
            "/cluster/network/connectivity/",
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )
        .map_err(kv_err)?
    {
        let record: ClusterConnectivityRecord = entry.decode().map_err(kv_err)?;
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

/// KV-backed [`ClusterDirectory`]: serves the routing layer from the local
/// cluster KV snapshot. Moves to `sinomo-cluster` with the rest of this file
/// in stage B-ii; the daemon then registers it via
/// [`sinomo_network::set_cluster_directory_provider`].
pub struct KvClusterDirectory {
    paths: ClusterPaths,
}

impl KvClusterDirectory {
    pub fn new(paths: ClusterPaths) -> Self {
        Self { paths }
    }
}

impl sinomo_network::ClusterDirectory for KvClusterDirectory {
    fn members(&self) -> Result<Vec<ClusterMemberRecord>, NetworkError> {
        read_cluster_members_from_kv(&self.paths)
    }

    fn endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
        read_cluster_endpoints_from_kv(&self.paths)
    }

    fn node_statuses(
        &self,
    ) -> Result<std::collections::BTreeMap<NodeId, ClusterNodeStatusRecord>, NetworkError> {
        Ok(cluster_node_status_map_from_kv(&self.paths))
    }

    fn connectivity_records(&self) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
        // Active connectivity is network-plane state, not a stable cluster
        // fact. KV keeps durable membership/endpoints/status; route choice uses
        // local probe records plus peer records fetched directly over the
        // network plane.
        Ok(Vec::new())
    }

    fn version(&self) -> Result<sinomo_network::DirectoryVersion, NetworkError> {
        // Same source as the read methods above (max of the local raft view
        // and the imported snapshot cache): a version computed from a
        // narrower source than the data would let a node with a lagging raft
        // WAL keep validating route caches built before an imported snapshot
        // arrived. `None` (no snapshot at all) maps to 0, matching the empty
        // reads; errors propagate so callers do not cache on a bogus value.
        Ok(sinomo_network::DirectoryVersion(
            crate::kv_local_store::latest_observed_cluster_kv_snapshot_revision(&self.paths)?
                .map(|revision| revision.0)
                .unwrap_or(0),
        ))
    }
}
