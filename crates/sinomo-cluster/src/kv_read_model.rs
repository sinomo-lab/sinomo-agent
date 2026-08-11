use crate::keyspace::cluster_endpoint_prefix;
#[allow(unused_imports)]
use crate::kv_local_store::kv_err;
use crate::kv_local_store::read_cluster_kv_latest_observed_snapshot_arc_if_present;
use sinomo_base::{
    ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterMemberRecord, ClusterNodeStatusRecord,
    ClusterTransportKind,
};
use sinomo_kv::{CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES, KvSnapshot};
#[cfg(any(test, feature = "test-support"))]
use sinomo_network::ClusterNetworkInfoRecord;
use sinomo_network::ClusterPaths;
use sinomo_network::NetworkError;
use sinomo_network::{ClusterConnectivityRecord, cluster_network_record_live};
use sinomo_network::{cleanup_member_endpoint_duplicates, upsert_member_endpoint};
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
    endpoint_freshness: BTreeMap<(NodeId, sinomo_network::ClusterEndpointId), EndpointFreshness>,
    connectivity_records: Vec<ClusterConnectivityRecord>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct EndpointFreshness {
    modify_revision: u64,
    create_revision: u64,
    version: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum IrohProcessRole {
    MainDaemon,
    ShadowDaemon,
}

impl ClusterDirectoryProjection {
    fn current_routing_records(&self) -> (Vec<ClusterMemberRecord>, Vec<ClusterEndpointRecord>) {
        let Some(registry) = sinomo_network::network_module_registry() else {
            return (self.members.clone(), self.endpoints.clone());
        };
        let endpoint_role = |endpoint: &ClusterEndpointRecord| {
            if endpoint.transport != ClusterTransportKind::Iroh {
                return None;
            }
            match registry.endpoint_role(endpoint) {
                sinomo_network::NetworkModuleEndpointRole::MainDaemon => {
                    Some(IrohProcessRole::MainDaemon)
                }
                sinomo_network::NetworkModuleEndpointRole::ShadowDaemon => {
                    Some(IrohProcessRole::ShadowDaemon)
                }
                sinomo_network::NetworkModuleEndpointRole::Unknown => None,
            }
        };

        // Iroh owns one persisted secret/public endpoint per daemon process;
        // another endpoint for the same node and positive process role is a
        // later process generation, not a simultaneous route. Keep this rule
        // protocol-specific: other module transports may legitimately publish
        // multiple same-role endpoints.
        let mut newest = BTreeMap::<(NodeId, IrohProcessRole), EndpointFreshness>::new();
        for endpoint in &self.endpoints {
            let Some(role) = endpoint_role(endpoint) else {
                continue;
            };
            let Some(freshness) = self
                .endpoint_freshness
                .get(&(endpoint.node_id.clone(), endpoint.endpoint_id.clone()))
            else {
                continue;
            };
            newest
                .entry((endpoint.node_id.clone(), role))
                .and_modify(|current| *current = (*current).max(*freshness))
                .or_insert(*freshness);
        }

        let retain_endpoint = |endpoint: &ClusterEndpointRecord| {
            let Some(role) = endpoint_role(endpoint) else {
                return true;
            };
            let Some(newest_freshness) = newest.get(&(endpoint.node_id.clone(), role)) else {
                // Embedded-only records have no KV entry revision. Preserve
                // them because there is no durable generation order to apply.
                return true;
            };
            self.endpoint_freshness
                .get(&(endpoint.node_id.clone(), endpoint.endpoint_id.clone()))
                .is_some_and(|freshness| freshness == newest_freshness)
        };

        let mut endpoints = self.endpoints.clone();
        endpoints.retain(&retain_endpoint);
        let mut members = self.members.clone();
        for member in &mut members {
            member.endpoints.retain(&retain_endpoint);
        }
        (members, endpoints)
    }

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
    module_routing_generation: u64,
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
            if prefix == "/cluster/endpoints/" {
                // Endpoint generation selection depends on KV recency even
                // when a publisher writes byte-identical endpoint content.
                hasher.update(&entry.modify_revision.0.to_le_bytes());
                hasher.update(&entry.create_revision.0.to_le_bytes());
                hasher.update(&entry.version.0.to_le_bytes());
            }
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
    let module_routing_generation = sinomo_network::network_module_routing_generation();
    read_cluster_directory_projection_at_module_generation(
        paths,
        snapshot,
        module_routing_generation,
    )
}

fn read_cluster_directory_projection_at_module_generation(
    paths: &ClusterPaths,
    snapshot: Arc<KvSnapshot>,
    module_routing_generation: u64,
) -> Result<Arc<ClusterDirectoryProjection>, NetworkError> {
    if let Ok(mut cache) = CLUSTER_DIRECTORY_PROJECTIONS.lock()
        && let Some(entry) = cache.get_mut(&paths.root)
        && Arc::ptr_eq(&entry.snapshot, &snapshot)
        && entry.module_routing_generation == module_routing_generation
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
        && entry.module_routing_generation == module_routing_generation
    {
        entry.snapshot = snapshot;
        entry.last_used = cluster_directory_projection_cache_tick();
        return Ok(Arc::clone(&entry.projection));
    }

    let members = read_cluster_members_from_snapshot(snapshot.as_ref())?;
    let (endpoint_map, endpoint_freshness) =
        read_cluster_endpoints_with_freshness_from_snapshot(snapshot.as_ref())?;
    let endpoints = endpoint_map.into_values().flatten().collect();
    let connectivity_records =
        // Keep the decoded records, not their time-dependent live view. Route
        // capture filters lease expiry on every call below.
        read_all_cluster_connectivity_records_from_snapshot(snapshot.as_ref())?;

    let Ok(mut cache) = CLUSTER_DIRECTORY_PROJECTIONS.lock() else {
        return Ok(Arc::new(ClusterDirectoryProjection {
            routing_version: sinomo_network::DirectoryVersion(snapshot.revision.0),
            members,
            endpoints,
            endpoint_freshness,
            connectivity_records,
        }));
    };
    // Another reader may have populated this exact generation while the
    // projection was decoded outside the cache lock. Reuse its allocation.
    if let Some(entry) = cache.get_mut(&paths.root)
        && entry.mark == mark
        && entry.module_routing_generation == module_routing_generation
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
        endpoint_freshness,
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
            module_routing_generation,
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
            upsert_member_endpoint(member, endpoint);
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
    read_cluster_endpoints_with_freshness_from_snapshot(snapshot).map(|(endpoints, _)| endpoints)
}

fn read_cluster_endpoints_with_freshness_from_snapshot(
    snapshot: &KvSnapshot,
) -> Result<
    (
        BTreeMap<NodeId, Vec<ClusterEndpointRecord>>,
        BTreeMap<(NodeId, sinomo_network::ClusterEndpointId), EndpointFreshness>,
    ),
    NetworkError,
> {
    let mut endpoints: BTreeMap<NodeId, Vec<ClusterEndpointRecord>> = BTreeMap::new();
    let mut freshness = BTreeMap::new();
    for entry in snapshot
        .range_prefix_limited("/cluster/endpoints/", CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES)
        .map_err(kv_err)?
    {
        let endpoint: ClusterEndpointRecord = entry.decode().map_err(kv_err)?;
        if !cluster_endpoint_record_is_cluster_visible(&endpoint) {
            continue;
        }
        freshness.insert(
            (endpoint.node_id.clone(), endpoint.endpoint_id.clone()),
            EndpointFreshness {
                modify_revision: entry.modify_revision.0,
                create_revision: entry.create_revision.0,
                version: entry.version.0,
            },
        );
        endpoints
            .entry(endpoint.node_id.clone())
            .or_default()
            .push(endpoint);
    }
    Ok((endpoints, freshness))
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
        let (members, endpoints) = projection.current_routing_records();
        Ok(sinomo_network::DirectoryRecordsSnapshot::new(
            version, members, endpoints,
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
        let (members, endpoints) = projection.current_routing_records();
        Ok(sinomo_network::DirectoryRoutingRecordsSnapshot::new(
            projection.routing_version,
            members,
            endpoints,
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
    use sinomo_base::{ClusterEndpointSource, ClusterTransportKind, Id};
    use sinomo_kv::{InProcessKvStore, KvCommand, KvOp};
    use sinomo_network::{
        AuthenticatedLink, ClusterConnectivityState, ClusterDirectory as _, ClusterEndpointId,
        NetworkHealthPolicy, NetworkModule, NetworkModuleContext, NetworkModuleDescriptor,
        NetworkModuleEndpointRole, NetworkModuleRegistry, RouteConstraints, RoutedSessionIdentity,
        set_network_module_registry,
    };
    use sinomo_node::ClusterId;

    struct TestIrohRoleModule;

    static TEST_NETWORK_MODULE_REGISTRY_LOCK: Mutex<()> = Mutex::new(());
    static TEST_IROH_DIALED_ENDPOINTS: Mutex<Vec<ClusterEndpointId>> = Mutex::new(Vec::new());

    struct TestRouteIdentity(NodeId);

    impl RoutedSessionIdentity for TestRouteIdentity {
        fn local_node_id(&self) -> &NodeId {
            &self.0
        }

        fn sign(&self, _transcript: &[u8]) -> Result<Vec<u8>, NetworkError> {
            Ok(Vec::new())
        }

        fn verify(
            &self,
            _signer_node_id: &NodeId,
            _authorized_public_key: &str,
            _transcript: &[u8],
            _signature: &[u8],
        ) -> Result<(), NetworkError> {
            Ok(())
        }
    }

    impl NetworkModule for TestIrohRoleModule {
        fn descriptor(&self) -> NetworkModuleDescriptor {
            NetworkModuleDescriptor {
                id: "kv-read-model-test-iroh".to_string(),
                transports: vec![ClusterTransportKind::Iroh],
                address_schemes: vec!["iroh".to_string()],
                dialer: true,
                listener: false,
                supports_runtime_toggle: true,
            }
        }

        fn start(&self, _ctx: &NetworkModuleContext) -> Result<(), NetworkError> {
            Ok(())
        }

        fn stop(&self) -> Result<(), NetworkError> {
            Ok(())
        }

        fn connect_link(
            &self,
            endpoint: &ClusterEndpointRecord,
            _health: NetworkHealthPolicy,
        ) -> sinomo_network::NetworkFuture<AuthenticatedLink> {
            let endpoint_id = endpoint.endpoint_id.clone();
            Box::pin(async move {
                TEST_IROH_DIALED_ENDPOINTS
                    .lock()
                    .expect("dialed endpoints lock")
                    .push(endpoint_id);
                Err(NetworkError::Io {
                    operation: "test Iroh dial".to_string(),
                    detail: "expected test dial rejection".to_string(),
                })
            })
        }

        fn local_endpoints(&self) -> Result<Vec<ClusterEndpointRecord>, NetworkError> {
            Ok(Vec::new())
        }

        fn endpoint_role(&self, endpoint: &ClusterEndpointRecord) -> NetworkModuleEndpointRole {
            let Some(public_key) = endpoint
                .address
                .strip_prefix("iroh://")
                .and_then(|rest| rest.split(['?', '#']).next())
                .filter(|public_key| !public_key.is_empty())
            else {
                return NetworkModuleEndpointRole::Unknown;
            };
            let shadow_node_id = sinomo_node::shadow_daemon_node_id(&endpoint.node_id);
            if endpoint.endpoint_id
                == Id::derived(
                    "sinomo.iroh.endpoint",
                    format!("{}\0{public_key}", shadow_node_id.as_str()),
                )
            {
                return NetworkModuleEndpointRole::ShadowDaemon;
            }
            if endpoint.endpoint_id
                == Id::derived(
                    "sinomo.iroh.endpoint",
                    format!("{}\0{public_key}", endpoint.node_id.as_str()),
                )
            {
                return NetworkModuleEndpointRole::MainDaemon;
            }
            NetworkModuleEndpointRole::Unknown
        }
    }

    fn install_test_iroh_role_module() {
        let registry = NetworkModuleRegistry::new();
        registry.register(Arc::new(TestIrohRoleModule), None, true, false);
        set_network_module_registry(Arc::new(registry));
    }

    fn test_iroh_main_endpoint(
        node_id: &NodeId,
        endpoint_public_key: &str,
    ) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: Id::derived(
                "sinomo.iroh.endpoint",
                format!("{}\0{endpoint_public_key}", node_id.as_str()),
            ),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::Iroh,
            address: format!(
                "iroh://{endpoint_public_key}?relay=https%3A%2F%2Fuse1-1.relay.n0.iroh.link"
            ),
            source: ClusterEndpointSource::Rendezvous,
            privacy: ClusterEndpointPrivacy::Relayed,
            priority: 90,
            last_observed_latency_ms: None,
        }
    }

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
    fn keyed_iroh_relay_upgrades_same_id_embedded_key_only_record() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let cluster_id = ClusterId::from_legacy_named("cluster_kv_read_model_iroh_relay_upgrade")
            .expect("cluster");
        let node_id =
            NodeId::from_legacy_named("node_kv_read_model_iroh_relay_upgrade").expect("node");
        let relay = test_iroh_main_endpoint(&node_id, "windows-public-key");
        let mut key_only = relay.clone();
        key_only.address = "iroh://windows-public-key".to_string();
        key_only.privacy = ClusterEndpointPrivacy::InternetDirect;
        let mut member = ClusterMemberRecord::local_voter(
            cluster_id,
            node_id.clone(),
            "windows",
            "pk-windows",
            now,
        );
        member.endpoints = vec![key_only];

        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_member_key(&node_id), &member, None)
            .expect("put member");
        store
            .put_serde(cluster_endpoint_key(&node_id, &relay), &relay, None)
            .expect("put relay endpoint");
        let snapshot = store.try_snapshot().expect("snapshot");

        let members = read_cluster_members_from_snapshot(&snapshot).expect("read members");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].endpoints, vec![relay.clone()]);

        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(
            "kv-read-model-iroh-relay-upgrade",
        ));
        let projection = read_cluster_directory_projection(&paths, Arc::new(snapshot))
            .expect("directory projection");
        let (members, endpoints) = projection.current_routing_records();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].endpoints, vec![relay.clone()]);
        assert_eq!(endpoints, vec![relay]);
    }

    #[test]
    fn routing_projection_keeps_only_newest_iroh_process_generation() {
        let _registry_guard = TEST_NETWORK_MODULE_REGISTRY_LOCK
            .lock()
            .expect("network module registry lock");
        install_test_iroh_role_module();
        let root = sinomo_testdir::TestDir::new("kv-read-model-iroh-generation");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(root.path()));
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_kv_read_model_iroh_generation").expect("cluster");
        let source =
            NodeId::from_legacy_named("node_kv_read_model_iroh_generation_source").expect("source");
        let target =
            NodeId::from_legacy_named("node_kv_read_model_iroh_generation_target").expect("target");

        // Make the stale endpoint sort first by endpoint id. Before revision
        // supersession, that obsolete generation reached routing first because
        // both live records otherwise have identical priority/cost metadata.
        let mut candidates = (0..32)
            .map(|index| test_iroh_main_endpoint(&target, &format!("public-key-{index:02}")))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.endpoint_id.cmp(&right.endpoint_id));
        let stale = candidates.first().expect("stale candidate").clone();
        let current = candidates.last().expect("current candidate").clone();
        assert!(stale.endpoint_id < current.endpoint_id);

        let source_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            source.clone(),
            "source",
            "pk-source",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.endpoints = vec![stale.clone(), current.clone()];

        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_member_key(&source), &source_member, None)
            .expect("put source member");
        store
            .put_serde(cluster_member_key(&target), &target_member, None)
            .expect("put target member");
        store
            .put_serde(cluster_endpoint_key(&target, &stale), &stale, None)
            .expect("put stale endpoint");
        store
            .put_serde(cluster_endpoint_key(&target, &current), &current, None)
            .expect("put current endpoint");
        let snapshot = store.try_snapshot().expect("snapshot");
        let stale_revision = snapshot
            .get(&cluster_endpoint_key(&target, &stale))
            .expect("stale entry")
            .modify_revision;
        let current_revision = snapshot
            .get(&cluster_endpoint_key(&target, &current))
            .expect("current entry")
            .modify_revision;
        assert!(current_revision > stale_revision);

        let projection = read_cluster_directory_projection(&paths, Arc::new(snapshot))
            .expect("directory projection");
        let (members, endpoints) = projection.current_routing_records();
        let projected_target = members
            .iter()
            .find(|member| member.node_id == target)
            .expect("projected target");
        assert_eq!(
            projected_target
                .endpoints
                .iter()
                .filter(|endpoint| endpoint.transport == ClusterTransportKind::Iroh)
                .map(|endpoint| endpoint.endpoint_id.clone())
                .collect::<Vec<_>>(),
            vec![current.endpoint_id.clone()],
            "embedded stale endpoint generations must be removed too"
        );
        assert_eq!(
            endpoints
                .iter()
                .filter(|endpoint| {
                    endpoint.node_id == target && endpoint.transport == ClusterTransportKind::Iroh
                })
                .map(|endpoint| endpoint.endpoint_id.clone())
                .collect::<Vec<_>>(),
            vec![current.endpoint_id.clone()]
        );

        let directory = sinomo_network::DirectorySnapshot::try_from_records(
            projection.routing_version,
            members,
            endpoints,
        )
        .expect("directory snapshot");
        let connectivity = sinomo_network::connectivity_snapshot_from_directory_records(
            &directory,
            Vec::<ClusterConnectivityRecord>::new(),
            now,
        )
        .expect("empty connectivity snapshot");
        let routes = sinomo_network::plan_routes(
            &directory,
            &connectivity,
            &source,
            &target,
            RouteConstraints::default(),
        );
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops.len(), 1);
        assert_eq!(routes[0].hops[0].endpoint.endpoint_id, current.endpoint_id);
    }

    #[tokio::test]
    async fn module_registry_generation_invalidates_a_prepared_directory_cache() {
        let _registry_guard = TEST_NETWORK_MODULE_REGISTRY_LOCK
            .lock()
            .expect("network module registry lock");
        set_network_module_registry(Arc::new(NetworkModuleRegistry::new()));
        let generation_without_roles = sinomo_network::network_module_routing_generation();
        let root = sinomo_testdir::TestDir::new("kv-read-model-module-cache-generation");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(root.path()));
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_kv_read_model_module_cache").expect("cluster");
        let source =
            NodeId::from_legacy_named("node_kv_read_model_module_cache_source").expect("source");
        let target =
            NodeId::from_legacy_named("node_kv_read_model_module_cache_target").expect("target");
        let stale = test_iroh_main_endpoint(&target, "cache-stale-public-key");
        let current = test_iroh_main_endpoint(&target, "cache-current-public-key");
        let source_member = ClusterMemberRecord::local_voter(
            cluster_id.clone(),
            source.clone(),
            "source",
            "pk-source",
            now,
        );
        let mut target_member = ClusterMemberRecord::local_voter(
            cluster_id,
            target.clone(),
            "target",
            "pk-target",
            now,
        );
        target_member.endpoints = vec![stale.clone(), current.clone()];
        let mut store = InProcessKvStore::new();
        store
            .put_serde(cluster_member_key(&source), &source_member, None)
            .expect("put source member");
        store
            .put_serde(cluster_member_key(&target), &target_member, None)
            .expect("put target member");
        store
            .put_serde(cluster_endpoint_key(&target, &stale), &stale, None)
            .expect("put stale endpoint");
        store
            .put_serde(cluster_endpoint_key(&target, &current), &current, None)
            .expect("put current endpoint");
        let snapshot = Arc::new(store.try_snapshot().expect("snapshot"));

        let before = read_cluster_directory_projection(&paths, Arc::clone(&snapshot))
            .expect("projection before module roles");
        let prepared_route_cache_generation = before.routing_version;
        let (unclassified_members, unclassified_endpoints) = before.current_routing_records();
        assert_eq!(
            unclassified_endpoints
                .iter()
                .filter(|endpoint| endpoint.node_id == target)
                .count(),
            2,
            "the regression starts with a cached raw stale+current directory snapshot"
        );
        let unclassified_directory = sinomo_network::DirectorySnapshot::try_from_records(
            before.routing_version,
            unclassified_members,
            unclassified_endpoints,
        )
        .expect("pre-install directory snapshot");
        let unclassified_connectivity =
            sinomo_network::connectivity_snapshot_from_directory_records(
                &unclassified_directory,
                Vec::<ClusterConnectivityRecord>::new(),
                now,
            )
            .expect("pre-install connectivity snapshot");
        let unclassified_tracker = Arc::new(sinomo_network::ConnectivityTracker::new(
            unclassified_connectivity,
        ));
        let unclassified_connectivity_view: Arc<dyn sinomo_network::ConnectivityView> =
            unclassified_tracker.clone();
        let unclassified_failure_sink: Arc<dyn sinomo_network::ConnectivityFailureSink> =
            unclassified_tracker;
        let prepared_connector = Arc::new(
            sinomo_network::MultiHopRoutedConnector::from_snapshot(
                source.clone(),
                unclassified_directory,
                unclassified_connectivity_view,
                unclassified_failure_sink,
                Arc::new(sinomo_network::RegisteredNetworkModuleLinkProvider::new(
                    NetworkHealthPolicy::default(),
                )),
                Arc::new(TestRouteIdentity(source.clone())),
            )
            .expect("pre-install prepared connector"),
        );
        sinomo_network::prepared_dynamic_route_cache_insert_for_test(
            paths.root.clone(),
            source.clone(),
            prepared_route_cache_generation.0,
            generation_without_roles,
            prepared_connector,
        );
        assert!(
            sinomo_network::prepared_dynamic_route_cache_contains_for_test(
                paths.root.clone(),
                source.clone(),
                prepared_route_cache_generation.0,
                generation_without_roles,
            ),
            "the old raw connector must actually occupy the prepared-route cache"
        );

        install_test_iroh_role_module();
        let generation_with_roles = sinomo_network::network_module_routing_generation();
        assert!(
            generation_with_roles > generation_without_roles,
            "installing the role-aware dialer must advance a monotonic routing generation"
        );
        // Reproduce projection-cache eviction: the rebuilt projection has no
        // prior synthetic routing version to increment and therefore reuses
        // the unchanged KV snapshot revision. The network-local module fence,
        // not projection-cache residency, must still reject the old connector.
        CLUSTER_DIRECTORY_PROJECTIONS
            .lock()
            .expect("directory projection cache lock")
            .remove(&paths.root);
        let after = read_cluster_directory_projection(&paths, snapshot)
            .expect("projection after module roles");
        assert_eq!(
            after.routing_version, prepared_route_cache_generation,
            "eviction intentionally reproduces the colliding distributed directory generation"
        );
        assert!(
            !sinomo_network::prepared_dynamic_route_cache_contains_for_test(
                paths.root.clone(),
                source.clone(),
                after.routing_version.0,
                generation_with_roles,
            ),
            "the post-install call must miss the old connector even when projection eviction reuses the directory generation"
        );
        assert!(
            !Arc::ptr_eq(&before, &after),
            "the raw pre-registry projection must not survive its cache fast path"
        );
        let (members, endpoints) = after.current_routing_records();
        assert_eq!(
            endpoints
                .iter()
                .filter(|endpoint| endpoint.node_id == target)
                .map(|endpoint| endpoint.endpoint_id.clone())
                .collect::<Vec<_>>(),
            vec![current.endpoint_id.clone()]
        );
        let directory = sinomo_network::DirectorySnapshot::try_from_records(
            after.routing_version,
            members,
            endpoints,
        )
        .expect("post-install directory snapshot");
        let connectivity = sinomo_network::connectivity_snapshot_from_directory_records(
            &directory,
            Vec::<ClusterConnectivityRecord>::new(),
            now,
        )
        .expect("empty connectivity snapshot");
        let routes = sinomo_network::plan_routes(
            &directory,
            &connectivity,
            &source,
            &target,
            RouteConstraints::default(),
        );
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].hops[0].endpoint.endpoint_id, current.endpoint_id);

        TEST_IROH_DIALED_ENDPOINTS
            .lock()
            .expect("dialed endpoints lock")
            .clear();
        let tracker = Arc::new(sinomo_network::ConnectivityTracker::new(connectivity));
        let connectivity_view: Arc<dyn sinomo_network::ConnectivityView> = tracker.clone();
        let failure_sink: Arc<dyn sinomo_network::ConnectivityFailureSink> = tracker;
        let connector = sinomo_network::MultiHopRoutedConnector::from_snapshot(
            source.clone(),
            directory,
            connectivity_view,
            failure_sink,
            Arc::new(sinomo_network::RegisteredNetworkModuleLinkProvider::new(
                NetworkHealthPolicy::default(),
            )),
            Arc::new(TestRouteIdentity(source.clone())),
        )
        .expect("post-install connector");
        let result = sinomo_network::RoutedConnector::connect(
            &connector,
            target,
            sinomo_network::ConnectOptions::default(),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("test module unexpectedly accepted the selected dial"),
            Err(error) => error,
        };
        assert!(matches!(error, NetworkError::RetryExhausted { .. }));
        assert_eq!(
            TEST_IROH_DIALED_ENDPOINTS
                .lock()
                .expect("dialed endpoints lock")
                .as_slice(),
            &[current.endpoint_id],
            "after the cache miss, only the newest endpoint may be attempted"
        );
    }

    #[test]
    fn routing_projection_preserves_unknown_and_non_iroh_multi_endpoints() {
        let _registry_guard = TEST_NETWORK_MODULE_REGISTRY_LOCK
            .lock()
            .expect("network module registry lock");
        install_test_iroh_role_module();
        let node_id = NodeId::from_legacy_named("node_kv_read_model_multi_endpoint")
            .expect("multi-endpoint node");
        let mut unknown_first = test_iroh_main_endpoint(&node_id, "unknown-first");
        unknown_first.endpoint_id = Id::derived("test.unknown.iroh.endpoint", "first");
        let mut unknown_second = test_iroh_main_endpoint(&node_id, "unknown-second");
        unknown_second.endpoint_id = Id::derived("test.unknown.iroh.endpoint", "second");
        let mut webrtc_first = unknown_first.clone();
        webrtc_first.endpoint_id = Id::derived("test.webrtc.endpoint", "first");
        webrtc_first.transport = ClusterTransportKind::WebRtc;
        webrtc_first.address = "webrtc://first".to_string();
        let mut webrtc_second = webrtc_first.clone();
        webrtc_second.endpoint_id = Id::derived("test.webrtc.endpoint", "second");
        webrtc_second.address = "webrtc://second".to_string();
        let endpoints = vec![unknown_first, unknown_second, webrtc_first, webrtc_second];
        let endpoint_freshness = endpoints
            .iter()
            .enumerate()
            .map(|(index, endpoint)| {
                (
                    (endpoint.node_id.clone(), endpoint.endpoint_id.clone()),
                    EndpointFreshness {
                        modify_revision: (index + 1) as u64,
                        create_revision: (index + 1) as u64,
                        version: 1,
                    },
                )
            })
            .collect();
        let projection = ClusterDirectoryProjection {
            routing_version: sinomo_network::DirectoryVersion(1),
            members: Vec::new(),
            endpoints: endpoints.clone(),
            endpoint_freshness,
            connectivity_records: Vec::new(),
        };

        let (_, projected_endpoints) = projection.current_routing_records();
        assert_eq!(projected_endpoints, endpoints);
    }

    #[test]
    fn directory_projection_cache_reuses_only_equal_route_inputs() {
        let root = sinomo_testdir::TestDir::new("kv-read-model-directory-projection-cache");
        let paths = ClusterPaths::new(&sinomo_local::SinomoDataDir::new(root.path()));
        // This test exercises only the route-input cache key. Pin the module
        // generation explicitly so another parallel test installing a network
        // module cannot turn the second lookup into a different cache input.
        let module_routing_generation = sinomo_network::network_module_routing_generation();
        let read_projection = |snapshot| {
            read_cluster_directory_projection_at_module_generation(
                &paths,
                snapshot,
                module_routing_generation,
            )
        };
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

        let first = read_projection(Arc::clone(&snapshot)).expect("first projection");
        let second = read_projection(Arc::clone(&snapshot)).expect("cached projection");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.routing_version, second.routing_version);

        let replacement = Arc::new((*snapshot).clone());
        let same_route = read_projection(replacement).expect("replacement projection");
        assert!(
            Arc::ptr_eq(&first, &same_route),
            "a new snapshot allocation with identical route inputs should reuse the decoded projection"
        );

        store
            .put("/tests/unrelated", b"application state".to_vec(), None)
            .expect("put unrelated state");
        let unrelated_snapshot = Arc::new(store.try_snapshot().expect("unrelated snapshot"));
        let unrelated_revision =
            read_projection(unrelated_snapshot).expect("unrelated revision projection");
        assert!(
            Arc::ptr_eq(&first, &unrelated_revision),
            "an unrelated KV revision should reuse the decoded route projection"
        );
        assert_eq!(first.routing_version, unrelated_revision.routing_version);

        let endpoint = sinomo_network::daemon_lan_endpoint(
            &node_id,
            "peer-cache.example.test:21353".to_string(),
            10,
        );
        store
            .put_serde(cluster_endpoint_key(&node_id, &endpoint), &endpoint, None)
            .expect("put route endpoint");
        let endpoint_snapshot = Arc::new(store.try_snapshot().expect("endpoint snapshot"));
        let endpoint_projection =
            read_projection(endpoint_snapshot.clone()).expect("endpoint projection");
        let first_endpoint_revision = endpoint_snapshot
            .get(&cluster_endpoint_key(&node_id, &endpoint))
            .expect("endpoint entry")
            .modify_revision;
        store
            .put_serde(cluster_endpoint_key(&node_id, &endpoint), &endpoint, None)
            .expect("republish byte-identical route endpoint");
        let republished_snapshot = Arc::new(store.try_snapshot().expect("republished snapshot"));
        assert!(
            republished_snapshot
                .get(&cluster_endpoint_key(&node_id, &endpoint))
                .expect("republished endpoint entry")
                .modify_revision
                > first_endpoint_revision
        );
        let republished_projection =
            read_projection(republished_snapshot).expect("republished projection");
        assert!(
            !Arc::ptr_eq(&endpoint_projection, &republished_projection),
            "endpoint revision recency participates in generation selection even when bytes match"
        );
        assert!(republished_projection.routing_version > endpoint_projection.routing_version);

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
        let rebuilt = read_projection(changed_snapshot).expect("changed route projection");
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
            endpoint_freshness: BTreeMap::new(),
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
