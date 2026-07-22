use crate::keyspace::cluster_endpoint_prefix;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::read_cluster_kv_latest_observed_snapshot_arc_if_present;
use sinomo_base::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeStatusRecord,
};
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
        member
            .endpoints
            .retain(cluster_endpoint_record_is_cluster_visible);
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
    // Directory and discovery readers must consume the same max-revision
    // projection advertised by `KvClusterDirectory::version`. A follower's
    // local Raft state can briefly trail an accepted/imported snapshot after a
    // join; preferring any readable local snapshot hid the newly admitted
    // member while reporting the newer directory generation.
    read_cluster_kv_latest_observed_snapshot_arc_if_present(paths)
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
        let endpoint: ClusterEndpointRecord = entry.decode().map_err(kv_err)?;
        if cluster_endpoint_record_is_cluster_visible(&endpoint) {
            endpoints.push(endpoint);
        }
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
        if !cluster_endpoint_record_is_cluster_visible(&endpoint) {
            continue;
        }
        endpoints
            .entry(endpoint.node_id.clone())
            .or_default()
            .push(endpoint);
    }
    Ok(endpoints)
}

fn cluster_endpoint_record_is_cluster_visible(endpoint: &ClusterEndpointRecord) -> bool {
    endpoint.privacy != ClusterEndpointPrivacy::LocalOnly
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
    read_cluster_connectivity_graph_from_snapshot(snapshot.as_ref())
}

fn read_cluster_connectivity_graph_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
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
        read_cluster_connectivity_graph_from_kv(&self.paths)
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

    fn records_snapshot(&self) -> Result<sinomo_network::DirectoryRecordsSnapshot, NetworkError> {
        let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(&self.paths)?
        else {
            return Ok(sinomo_network::DirectoryRecordsSnapshot::new(
                sinomo_network::DirectoryVersion(0),
                Vec::new(),
                Vec::new(),
            ));
        };
        let members = read_cluster_members_from_snapshot(snapshot.as_ref())?;
        let endpoints = read_cluster_endpoints_from_snapshot(snapshot.as_ref())?
            .into_values()
            .flatten()
            .collect();
        Ok(sinomo_network::DirectoryRecordsSnapshot::new(
            sinomo_network::DirectoryVersion(snapshot.revision.0),
            members,
            endpoints,
        ))
    }

    fn routing_records_snapshot(
        &self,
    ) -> Result<sinomo_network::DirectoryRoutingRecordsSnapshot, NetworkError> {
        let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(&self.paths)?
        else {
            return Ok(sinomo_network::DirectoryRoutingRecordsSnapshot::new(
                sinomo_network::DirectoryVersion(0),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ));
        };
        let members = read_cluster_members_from_snapshot(snapshot.as_ref())?;
        let endpoints = read_cluster_endpoints_from_snapshot(snapshot.as_ref())?
            .into_values()
            .flatten()
            .collect();
        let connectivity_records =
            read_cluster_connectivity_graph_from_snapshot(snapshot.as_ref())?;
        Ok(sinomo_network::DirectoryRoutingRecordsSnapshot::new(
            sinomo_network::DirectoryVersion(snapshot.revision.0),
            members,
            endpoints,
            connectivity_records,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspace::{cluster_endpoint_key, cluster_member_key};
    use sinomo_kv::{InProcessKvStore, KvCommand, KvOp};
    use sinomo_network::ClusterDirectory as _;
    use sinomo_node::ClusterId;

    #[test]
    fn kv_read_model_filters_local_only_endpoint_records() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_kv_read_model_endpoint_filter").expect("cluster");
        let node_id =
            NodeId::from_legacy_named("node_kv_read_model_endpoint_filter").expect("node");
        let mut member =
            ClusterMemberRecord::local_voter(cluster_id, node_id.clone(), "node", "pk-node", now);
        let loopback = ClusterEndpointRecord::loopback(node_id.clone(), "127.0.0.1:21353");
        let public = sinomo_network::daemon_lan_endpoint(
            &node_id,
            "peer.example.test:21353".to_string(),
            10,
        );
        member.endpoints = vec![loopback.clone(), public.clone()];

        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_member_key(&node_id), &member, None)
            .expect("put member");
        store
            .put_serde(cluster_endpoint_key(&node_id, &loopback), &loopback, None)
            .expect("put loopback endpoint");
        store
            .put_serde(cluster_endpoint_key(&node_id, &public), &public, None)
            .expect("put public endpoint");
        let snapshot = store.try_snapshot().expect("snapshot");

        let members = read_cluster_members_from_snapshot(&snapshot).expect("read members");
        let member = members
            .iter()
            .find(|member| member.node_id == node_id)
            .expect("member");
        assert_eq!(member.endpoints, vec![public.clone()]);

        let endpoints = read_cluster_endpoints_from_snapshot(&snapshot).expect("read endpoints");
        assert_eq!(endpoints.get(&node_id), Some(&vec![public]));
    }

    #[test]
    fn kv_read_model_prefers_newer_imported_membership_over_stale_local_raft_view() {
        let root = sinomo_testdir::TestDir::new("kv-read-model-latest-observed-membership");
        let data_dir = sinomo_local::SinomoDataDir::new(root.path());
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_kv_read_model_latest").expect("cluster");
        let local = NodeId::from_legacy_named("node_kv_read_model_latest_local").expect("local");
        let joined = NodeId::from_legacy_named("node_kv_read_model_latest_joined").expect("joined");
        let state = sinomo_base::ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "latest observed directory",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        crate::write_cluster_state(&paths, &state).expect("cluster state");
        let local_member = state.members[0].clone();
        let stale_current = crate::append_cluster_raft_command(
            &paths,
            &local,
            KvCommand::Op(
                KvOp::put_serde(cluster_member_key(&local), &local_member, None)
                    .expect("local member op"),
            ),
        )
        .expect("stale local raft snapshot");
        let mut joined_member = ClusterMemberRecord::pending_learner(
            cluster_id,
            joined.clone(),
            "joined",
            "pk-joined",
            now,
        );
        joined_member.state = sinomo_base::ClusterMemberState::Learner;
        let mut accepted = InProcessKvStore::restore(stale_current).expect("accepted store");
        accepted
            .put_serde(cluster_member_key(&joined), &joined_member, None)
            .expect("joined member");
        let accepted = accepted.try_snapshot().expect("accepted snapshot");
        crate::write_imported_cluster_kv_snapshot_if_changed(&paths, &accepted)
            .expect("import accepted snapshot");

        let local_current = crate::read_cluster_kv_current_snapshot(&paths)
            .expect("local raft snapshot remains readable");
        assert!(
            local_current.get(&cluster_member_key(&joined)).is_none(),
            "the regression requires local Raft materialization to trail the imported acceptance"
        );
        let members = read_cluster_members_from_kv(&paths).expect("latest observed members");
        assert!(
            members.iter().any(|member| member.node_id == joined),
            "the directory read model must expose the newer accepted member"
        );
        let records = KvClusterDirectory::new(paths.clone())
            .records_snapshot()
            .expect("transactional directory records");
        let (version, members, _) = records.into_parts();
        assert_eq!(
            version,
            sinomo_network::DirectoryVersion(accepted.revision.0)
        );
        assert!(
            members.iter().any(|member| member.node_id == joined),
            "the transactional routing snapshot must use that same accepted revision"
        );
    }
}
