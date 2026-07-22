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
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use time::OffsetDateTime;

const MAX_CLUSTER_DIRECTORY_PROJECTION_CACHE_ENTRIES: usize = 128;

static CLUSTER_DIRECTORY_PROJECTION_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_DIRECTORY_PROJECTIONS: LazyLock<
    Mutex<HashMap<PathBuf, ClusterDirectoryProjectionCacheEntry>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ClusterDirectoryProjection {
    routing_version: sinomo_network::DirectoryVersion,
    members: Vec<ClusterMemberRecord>,
    endpoints: Vec<ClusterEndpointRecord>,
    connectivity_records: Vec<ClusterConnectivityRecord>,
}

impl ClusterDirectoryProjection {
    fn live_connectivity_records(&self, now: OffsetDateTime) -> Vec<ClusterConnectivityRecord> {
        self.connectivity_records
            .iter()
            .filter(|record| cluster_network_record_live(record.expires_at, now))
            .cloned()
            .collect()
    }
}

struct ClusterDirectoryProjectionCacheEntry {
    snapshot: Arc<KvSnapshot>,
    mark: blake3::Hash,
    projection: Arc<ClusterDirectoryProjection>,
    last_used: u64,
}

fn cluster_directory_projection_cache_tick() -> u64 {
    CLUSTER_DIRECTORY_PROJECTION_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn cluster_directory_projection_mark(snapshot: &KvSnapshot) -> Result<blake3::Hash, NetworkError> {
    const ROUTE_PREFIXES: [&str; 4] = [
        "/cluster/members/",
        "/cluster/endpoints/",
        "/cluster/policy/capabilities/",
        "/cluster/network/connectivity/",
    ];
    let mut hasher = blake3::Hasher::new();
    for prefix in ROUTE_PREFIXES {
        hasher.update(&(prefix.len() as u64).to_le_bytes());
        hasher.update(prefix.as_bytes());
        for (index, entry) in snapshot.prefix_entries(prefix).enumerate() {
            if index >= CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES {
                return Err(NetworkError::Directory {
                    detail: format!(
                        "cluster directory projection prefix {prefix} exceeds the {} entry limit",
                        CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES
                    ),
                });
            }
            hasher.update(&(entry.key.len() as u64).to_le_bytes());
            hasher.update(entry.key.as_bytes());
            if prefix == "/cluster/network/connectivity/" {
                let record: ClusterConnectivityRecord = entry.decode().map_err(kv_err)?;
                for value in [
                    record.cluster_id.as_str(),
                    record.source_node_id.as_str(),
                    record.target_node_id.as_str(),
                    record.endpoint_id.as_str(),
                    record.address.as_str(),
                ] {
                    hasher.update(&(value.len() as u64).to_le_bytes());
                    hasher.update(value.as_bytes());
                }
                hasher.update(&[match record.transport {
                    sinomo_base::ClusterTransportKind::InProcess => 1,
                    sinomo_base::ClusterTransportKind::Loopback => 2,
                    sinomo_base::ClusterTransportKind::LanTcp => 3,
                    sinomo_base::ClusterTransportKind::LanQuic => 4,
                    sinomo_base::ClusterTransportKind::Iroh => 5,
                    sinomo_base::ClusterTransportKind::WebRtc => 6,
                    sinomo_base::ClusterTransportKind::TurnRelay => 7,
                    sinomo_base::ClusterTransportKind::HttpsRelay => 8,
                }]);
                hasher.update(&[match record.state {
                    sinomo_network::ClusterConnectivityState::Healthy => 1,
                    sinomo_network::ClusterConnectivityState::Degraded => 2,
                    sinomo_network::ClusterConnectivityState::Unreachable => 3,
                }]);
            } else {
                hasher.update(&(entry.value.len() as u64).to_le_bytes());
                hasher.update(&entry.value);
            }
        }
    }
    Ok(hasher.finalize())
}

fn read_cluster_directory_projection(
    paths: &ClusterPaths,
    snapshot: Arc<KvSnapshot>,
) -> Result<Arc<ClusterDirectoryProjection>, NetworkError> {
    if let Ok(mut cache) = CLUSTER_DIRECTORY_PROJECTIONS.lock()
        && let Some(entry) = cache.get_mut(&paths.root)
        && Arc::ptr_eq(&entry.snapshot, &snapshot)
    {
        entry.last_used = cluster_directory_projection_cache_tick();
        return Ok(Arc::clone(&entry.projection));
    }

    // Most cluster-KV revisions touch application or control-plane keys that do
    // not affect routing. Hash the exact serialized route inputs so those
    // revisions can retain the decoded projection while still invalidating on
    // any membership, endpoint, capability, or connectivity change.
    let mark = cluster_directory_projection_mark(snapshot.as_ref())?;
    if let Ok(mut cache) = CLUSTER_DIRECTORY_PROJECTIONS.lock()
        && let Some(entry) = cache.get_mut(&paths.root)
        && entry.mark == mark
    {
        entry.snapshot = snapshot;
        entry.last_used = cluster_directory_projection_cache_tick();
        return Ok(Arc::clone(&entry.projection));
    }

    let members = read_cluster_members_from_snapshot(snapshot.as_ref())?;
    let endpoints = read_cluster_endpoints_from_snapshot(snapshot.as_ref())?
        .into_values()
        .flatten()
        .collect();
    let connectivity_records =
        // Keep the decoded records, not their time-dependent live view. Route
        // capture filters lease expiry on every call below.
        read_all_cluster_connectivity_records_from_snapshot(snapshot.as_ref())?;

    let Ok(mut cache) = CLUSTER_DIRECTORY_PROJECTIONS.lock() else {
        return Ok(Arc::new(ClusterDirectoryProjection {
            routing_version: sinomo_network::DirectoryVersion(snapshot.revision.0),
            members,
            endpoints,
            connectivity_records,
        }));
    };
    // Another reader may have populated this exact generation while the
    // projection was decoded outside the cache lock. Reuse its allocation.
    if let Some(entry) = cache.get_mut(&paths.root)
        && entry.mark == mark
    {
        entry.snapshot = snapshot;
        entry.last_used = cluster_directory_projection_cache_tick();
        return Ok(Arc::clone(&entry.projection));
    }
    let routing_version = cache
        .get(&paths.root)
        .map(|entry| {
            sinomo_network::DirectoryVersion(
                entry
                    .projection
                    .routing_version
                    .0
                    .saturating_add(1)
                    .max(snapshot.revision.0),
            )
        })
        .unwrap_or(sinomo_network::DirectoryVersion(snapshot.revision.0));
    let projection = Arc::new(ClusterDirectoryProjection {
        routing_version,
        members,
        endpoints,
        connectivity_records,
    });
    if !cache.contains_key(&paths.root)
        && cache.len() >= MAX_CLUSTER_DIRECTORY_PROJECTION_CACHE_ENTRIES
        && let Some(eviction_path) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(path, _)| path.clone())
    {
        cache.remove(&eviction_path);
    }
    cache.insert(
        paths.root.clone(),
        ClusterDirectoryProjectionCacheEntry {
            snapshot,
            mark,
            projection: Arc::clone(&projection),
            last_used: cluster_directory_projection_cache_tick(),
        },
    );
    Ok(projection)
}

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
    Ok(
        read_all_cluster_connectivity_records_from_snapshot(snapshot)?
            .into_iter()
            .filter(|record| cluster_network_record_live(record.expires_at, now))
            .collect(),
    )
}

fn read_all_cluster_connectivity_records_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let mut records: Vec<ClusterConnectivityRecord> = Vec::new();
    for entry in snapshot
        .range_prefix_limited(
            "/cluster/network/connectivity/",
            CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES,
        )
        .map_err(kv_err)?
    {
        let record: ClusterConnectivityRecord = entry.decode().map_err(kv_err)?;
        records.push(record);
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
        let version = sinomo_network::DirectoryVersion(snapshot.revision.0);
        let projection = read_cluster_directory_projection(&self.paths, snapshot)?;
        Ok(sinomo_network::DirectoryRecordsSnapshot::new(
            version,
            projection.members.clone(),
            projection.endpoints.clone(),
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
        let projection = read_cluster_directory_projection(&self.paths, snapshot)?;
        Ok(sinomo_network::DirectoryRoutingRecordsSnapshot::new(
            projection.routing_version,
            projection.members.clone(),
            projection.endpoints.clone(),
            projection.live_connectivity_records(OffsetDateTime::now_utc()),
        ))
    }

    fn routing_version(&self) -> Result<sinomo_network::DirectoryVersion, NetworkError> {
        let Some(snapshot) = read_cluster_kv_latest_observed_snapshot_arc_if_present(&self.paths)?
        else {
            return Ok(sinomo_network::DirectoryVersion(0));
        };
        Ok(read_cluster_directory_projection(&self.paths, snapshot)?.routing_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyspace::{cluster_endpoint_key, cluster_member_key};
    use sinomo_base::ClusterTransportKind;
    use sinomo_kv::{InProcessKvStore, KvCommand, KvOp};
    use sinomo_network::{ClusterConnectivityState, ClusterDirectory as _, ClusterEndpointId};
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
    fn directory_projection_cache_reuses_only_equal_route_inputs() {
        let root = sinomo_testdir::TestDir::new("kv-read-model-directory-projection-cache");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(root.path()));
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_kv_read_model_cache").expect("cluster");
        let node_id = NodeId::from_legacy_named("node_kv_read_model_cache").expect("node");
        let member =
            ClusterMemberRecord::local_voter(cluster_id, node_id.clone(), "node", "pk-node", now);
        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_member_key(&node_id), &member, None)
            .expect("put member");
        let snapshot = Arc::new(store.try_snapshot().expect("snapshot"));

        let first = read_cluster_directory_projection(&paths, Arc::clone(&snapshot))
            .expect("first projection");
        let second = read_cluster_directory_projection(&paths, Arc::clone(&snapshot))
            .expect("cached projection");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.routing_version, second.routing_version);

        let replacement = Arc::new((*snapshot).clone());
        let same_route =
            read_cluster_directory_projection(&paths, replacement).expect("replacement projection");
        assert!(
            Arc::ptr_eq(&first, &same_route),
            "a new snapshot allocation with identical route inputs should reuse the decoded projection"
        );

        store
            .put("/tests/unrelated", b"application state".to_vec(), None)
            .expect("put unrelated state");
        let unrelated_snapshot = Arc::new(store.try_snapshot().expect("unrelated snapshot"));
        let unrelated_revision = read_cluster_directory_projection(&paths, unrelated_snapshot)
            .expect("unrelated revision projection");
        assert!(
            Arc::ptr_eq(&first, &unrelated_revision),
            "an unrelated KV revision should reuse the decoded route projection"
        );
        assert_eq!(first.routing_version, unrelated_revision.routing_version);

        let another_node =
            NodeId::from_legacy_named("node_kv_read_model_cache_changed").expect("another node");
        let another_member = ClusterMemberRecord::local_non_voting(
            member.cluster_id.clone(),
            another_node.clone(),
            "another node",
            "pk-another-node",
            now,
        );
        store
            .put_serde(cluster_member_key(&another_node), &another_member, None)
            .expect("put changed route member");
        let changed_snapshot = Arc::new(store.try_snapshot().expect("changed snapshot"));
        let rebuilt = read_cluster_directory_projection(&paths, changed_snapshot)
            .expect("changed route projection");
        assert!(
            !Arc::ptr_eq(&first, &rebuilt),
            "a route-relevant KV change must invalidate the decoded projection"
        );
        assert!(rebuilt.routing_version > first.routing_version);
    }

    #[test]
    fn cached_directory_projection_rechecks_connectivity_expiry() {
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_kv_read_model_expiry").expect("cluster");
        let source = NodeId::from_legacy_named("node_kv_read_model_expiry_source").expect("source");
        let target = NodeId::from_legacy_named("node_kv_read_model_expiry_target").expect("target");
        let record = ClusterConnectivityRecord {
            cluster_id,
            source_node_id: source,
            target_node_id: target,
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_kv_read_model_expiry")
                .expect("endpoint"),
            transport: ClusterTransportKind::LanTcp,
            address: "10.0.0.2:21353".to_string(),
            state: ClusterConnectivityState::Healthy,
            latency_ms: Some(1),
            consecutive_failures: 0,
            last_success_at: Some("ok".to_string()),
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::seconds(1),
        };
        let projection = ClusterDirectoryProjection {
            routing_version: sinomo_network::DirectoryVersion(1),
            members: Vec::new(),
            endpoints: Vec::new(),
            connectivity_records: vec![record.clone()],
        };

        assert_eq!(projection.live_connectivity_records(now), vec![record]);
        assert!(
            projection
                .live_connectivity_records(now + time::Duration::seconds(2))
                .is_empty(),
            "a cache hit must not extend a connectivity lease"
        );
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
