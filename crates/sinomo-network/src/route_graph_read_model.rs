use crate::connectivity::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityRecordQuery,
    ClusterConnectivityState, cluster_network_record_live,
};
use crate::connectivity_store::{
    read_local_cluster_connectivity_records, sort_cluster_connectivity_records,
};
use crate::error::NetworkError;
use crate::file_mark::{ClusterFileMark, cluster_file_mark};
use crate::{ClusterEndpointId, ClusterPaths};
use sinomo_base::{ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use time::OffsetDateTime;

const MAX_CLUSTER_ROUTE_GRAPH_CACHE_ENTRIES: usize = 128;

static CLUSTER_ROUTE_GRAPH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_ROUTE_GRAPHS: LazyLock<Mutex<HashMap<PathBuf, ClusterRouteGraphCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ClusterRouteGraphCacheEntry {
    mark: ClusterRouteGraphCacheMark,
    graph: ClusterConnectivityGraph,
    earliest_expires_at: Option<OffsetDateTime>,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRouteGraphCacheMark {
    state: ClusterFileMark,
    pub directory_version: Option<crate::directory::DirectoryVersion>,
    local_connectivity: ClusterFileMark,
}

#[derive(Clone, Debug, Default)]
struct ClusterRouteGraphLiveEndpointIndex {
    known_nodes: BTreeSet<NodeId>,
    endpoint_ids_by_node: BTreeMap<NodeId, BTreeSet<ClusterEndpointId>>,
}

impl ClusterRouteGraphLiveEndpointIndex {
    fn try_insert_known_node(
        &mut self,
        node_id: &NodeId,
        operation: &str,
    ) -> Result<(), NetworkError> {
        self.known_nodes
            .insert(crate::object::try_clone_id(node_id, operation)?);
        Ok(())
    }

    fn try_insert_endpoint(
        &mut self,
        endpoint: &ClusterEndpointRecord,
        operation: &str,
    ) -> Result<(), NetworkError> {
        let node_id = crate::object::try_clone_id(&endpoint.node_id, operation)?;
        self.known_nodes.insert(node_id.clone());
        if endpoint.privacy == ClusterEndpointPrivacy::LocalOnly {
            return Ok(());
        }
        self.endpoint_ids_by_node
            .entry(node_id)
            .or_default()
            .insert(crate::object::try_clone_id(
                &endpoint.endpoint_id,
                operation,
            )?);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.known_nodes.is_empty() && self.endpoint_ids_by_node.is_empty()
    }

    fn record_targets_live_endpoint(&self, record: &ClusterConnectivityRecord) -> bool {
        if let Some(endpoint_ids) = self.endpoint_ids_by_node.get(&record.target_node_id) {
            return endpoint_ids.contains(&record.endpoint_id);
        }
        true
    }
}

#[derive(Clone, Debug, Default)]
struct ClusterRouteGraphRecordIndex {
    records: BTreeMap<(NodeId, NodeId, ClusterEndpointId), ClusterConnectivityRecord>,
}

impl ClusterRouteGraphRecordIndex {
    fn record_key(
        record: &ClusterConnectivityRecord,
        operation: &str,
    ) -> Result<(NodeId, NodeId, ClusterEndpointId), NetworkError> {
        Ok((
            crate::object::try_clone_id(&record.source_node_id, operation)?,
            crate::object::try_clone_id(&record.target_node_id, operation)?,
            crate::object::try_clone_id(&record.endpoint_id, operation)?,
        ))
    }

    fn try_insert(
        &mut self,
        record: ClusterConnectivityRecord,
        operation: &str,
    ) -> Result<(), NetworkError> {
        let key = Self::record_key(&record, operation)?;
        self.records.insert(key, record);
        Ok(())
    }

    fn into_records(self) -> Vec<ClusterConnectivityRecord> {
        self.records.into_values().collect()
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn contains_source(&self, source_node_id: &NodeId) -> bool {
        self.records
            .keys()
            .any(|(record_source_node_id, _, _)| record_source_node_id == source_node_id)
    }
}

impl ClusterConnectivityGraph {
    pub fn try_read_for_local(
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Self, NetworkError> {
        let now = OffsetDateTime::now_utc();
        let before_mark = cluster_route_graph_cache_mark(paths);
        if let Some(graph) = read_cluster_route_graph_from_cache(paths, state, before_mark, now) {
            return Ok(graph);
        }
        let graph = Self::try_read_for_local_with_query(
            paths,
            state,
            &crate::connectivity::DirectoryConnectivityRecordQuery,
        )?;
        let after_mark = cluster_route_graph_cache_mark(paths);
        if before_mark == after_mark {
            store_cluster_route_graph_cache_entry(paths, after_mark, graph.clone(), now);
        }
        Ok(graph)
    }

    pub fn read_for_local(paths: &ClusterPaths, state: &ClusterBootstrapState) -> Self {
        let now = OffsetDateTime::now_utc();
        let before_mark = cluster_route_graph_cache_mark(paths);
        if let Some(graph) = read_cluster_route_graph_from_cache(paths, state, before_mark, now) {
            return graph;
        }
        let graph = Self::read_for_local_with_query(
            paths,
            state,
            &crate::connectivity::DirectoryConnectivityRecordQuery,
        );
        let after_mark = cluster_route_graph_cache_mark(paths);
        if before_mark == after_mark {
            store_cluster_route_graph_cache_entry(paths, after_mark, graph.clone(), now);
        }
        graph
    }

    pub fn read_for_local_with_query<Q>(
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        query: &Q,
    ) -> Self
    where
        Q: ClusterConnectivityRecordQuery + ?Sized,
    {
        Self::from_records(
            state.local_node_id.clone(),
            read_cluster_connectivity_graph_with_bootstrap_and_query(
                paths,
                Some(&state.cluster_id),
                Some(&state.local_node_id),
                Some(state),
                query,
            )
            .unwrap_or_default(),
        )
    }

    pub fn try_read_for_local_with_query<Q>(
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        query: &Q,
    ) -> Result<Self, NetworkError>
    where
        Q: ClusterConnectivityRecordQuery + ?Sized,
    {
        let source_node_id =
            crate::object::try_clone_id(&state.local_node_id, "build local connectivity graph")?;
        Self::try_from_records(
            source_node_id,
            read_cluster_connectivity_graph_with_bootstrap_and_query(
                paths,
                Some(&state.cluster_id),
                Some(&state.local_node_id),
                Some(state),
                query,
            )?,
        )
    }
}

fn cluster_route_graph_cache_tick() -> u64 {
    CLUSTER_ROUTE_GRAPH_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

pub fn cluster_route_graph_cache_mark(paths: &ClusterPaths) -> Option<ClusterRouteGraphCacheMark> {
    Some(ClusterRouteGraphCacheMark {
        state: cluster_file_mark(&paths.state)?,
        // A directory version error means "unknown": return no mark at all
        // (skip caching) rather than a `None` field that would compare equal
        // to itself and keep validating a stale graph.
        directory_version: Some(
            crate::directory::cluster_directory_for(paths)
                .version()
                .ok()?,
        ),
        local_connectivity: cluster_file_mark(&paths.connectivity_local)?,
    })
}

fn read_cluster_route_graph_from_cache(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    mark: Option<ClusterRouteGraphCacheMark>,
    now: OffsetDateTime,
) -> Option<ClusterConnectivityGraph> {
    let mark = mark?;
    let mut cache = CLUSTER_ROUTE_GRAPHS.lock().ok()?;
    let stale = {
        let entry = cache.get(&paths.root)?;
        entry.mark != mark
            || entry.graph.source_node_id() != &state.local_node_id
            || entry
                .earliest_expires_at
                .is_some_and(|expires_at| expires_at <= now)
    };
    if stale {
        cache.remove(&paths.root);
        return None;
    }
    let entry = cache.get_mut(&paths.root)?;
    entry.last_used = cluster_route_graph_cache_tick();
    Some(entry.graph.clone())
}

fn store_cluster_route_graph_cache_entry(
    paths: &ClusterPaths,
    mark: Option<ClusterRouteGraphCacheMark>,
    graph: ClusterConnectivityGraph,
    _now: OffsetDateTime,
) {
    let Some(mark) = mark else {
        return;
    };
    let Ok(mut cache) = CLUSTER_ROUTE_GRAPHS.lock() else {
        return;
    };
    evict_cluster_route_graph_cache_entry_if_needed(&mut cache, &paths.root);
    if !cache.contains_key(&paths.root) && cache.try_reserve(1).is_err() {
        return;
    }
    cache.insert(
        paths.root.clone(),
        ClusterRouteGraphCacheEntry {
            mark,
            earliest_expires_at: graph.earliest_expires_at(),
            graph,
            last_used: cluster_route_graph_cache_tick(),
        },
    );
}

pub fn invalidate_cluster_route_graph_cache(paths: &ClusterPaths) {
    if let Ok(mut cache) = CLUSTER_ROUTE_GRAPHS.lock() {
        cache.remove(&paths.root);
    }
}

fn evict_cluster_route_graph_cache_entry_if_needed(
    cache: &mut HashMap<PathBuf, ClusterRouteGraphCacheEntry>,
    protected: &PathBuf,
) {
    if cache.contains_key(protected) || cache.len() < MAX_CLUSTER_ROUTE_GRAPH_CACHE_ENTRIES {
        return;
    }
    let eviction_candidate = cache
        .iter()
        .filter(|(path, _)| *path != protected)
        .min_by_key(|(_, entry)| entry.last_used)
        .map(|(path, _)| path.clone());
    if let Some(path) = eviction_candidate {
        cache.remove(&path);
    }
}

pub fn read_cluster_connectivity_graph(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    read_cluster_connectivity_graph_with_query(
        paths,
        &crate::connectivity::DirectoryConnectivityRecordQuery,
    )
}

pub fn read_cluster_connectivity_graph_with_query<Q>(
    paths: &ClusterPaths,
    query: &Q,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>
where
    Q: ClusterConnectivityRecordQuery + ?Sized,
{
    let state =
        crate::object_file_store::read_optional_object_file::<ClusterBootstrapState>(&paths.state)
            .ok()
            .flatten();
    let cluster_id = state.as_ref().map(|state| &state.cluster_id);
    let local_source_node_id = state.as_ref().map(|state| &state.local_node_id);
    read_cluster_connectivity_graph_with_bootstrap_and_query(
        paths,
        cluster_id,
        local_source_node_id,
        state.as_ref(),
        query,
    )
}

pub(crate) fn read_cluster_connectivity_graph_for_state(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    read_cluster_connectivity_graph_with_bootstrap_and_query(
        paths,
        Some(&state.cluster_id),
        Some(&state.local_node_id),
        Some(state),
        &crate::connectivity::DirectoryConnectivityRecordQuery,
    )
}

fn read_cluster_connectivity_graph_with_bootstrap_and_query<Q>(
    paths: &ClusterPaths,
    cluster_id: Option<&sinomo_node::ClusterId>,
    _local_source_node_id: Option<&NodeId>,
    state: Option<&ClusterBootstrapState>,
    query: &Q,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>
where
    Q: ClusterConnectivityRecordQuery + ?Sized,
{
    let now = OffsetDateTime::now_utc();
    let live_endpoint_index = cluster_route_graph_live_endpoint_index(paths, state)?;
    let mut records_by_key = ClusterRouteGraphRecordIndex::default();
    let directory_read_error = match query.read_kv_connectivity_records(paths) {
        Ok(records) => {
            for record in records.into_iter().filter(|record| {
                route_graph_record_is_usable(record, cluster_id, live_endpoint_index.as_ref(), now)
            }) {
                records_by_key.try_insert(record, "index directory route graph records")?;
            }
            if !records_by_key.is_empty() {
                if let Some(local_source_node_id) = _local_source_node_id
                    && !records_by_key.contains_source(local_source_node_id)
                    && let Ok(local_records) = read_local_cluster_connectivity_records(paths)
                {
                    // During cold join, a node may know how to reach the
                    // cluster before its own source-owned probe records have
                    // replicated into cluster KV. Only those local-source
                    // records may supplement the KV graph; local records
                    // from other sources never extend the authoritative view.
                    for record in local_records.into_iter().filter(|record| {
                        record.source_node_id == *local_source_node_id
                            && route_graph_record_is_usable(
                                record,
                                cluster_id,
                                live_endpoint_index.as_ref(),
                                now,
                            )
                    }) {
                        records_by_key.try_insert(
                            record,
                            "index local source bootstrap route graph records",
                        )?;
                    }
                }
                let mut records = records_by_key.into_records();
                sort_cluster_connectivity_records(&mut records);
                return Ok(records);
            }
            None
        }
        Err(err) => Some(err),
    };

    let local_records = match read_local_cluster_connectivity_records(paths) {
        Ok(records) => records,
        Err(err) => {
            if let Some(directory_read_error) = directory_read_error {
                return Err(directory_read_error);
            }
            return Err(err);
        }
    };

    for record in local_records.into_iter().filter(|record| {
        route_graph_record_is_usable(record, cluster_id, live_endpoint_index.as_ref(), now)
    }) {
        records_by_key.try_insert(record, "index bootstrap route graph records")?;
    }

    if records_by_key.is_empty()
        && let Some(directory_read_error) = directory_read_error
    {
        return Err(directory_read_error);
    }

    let mut records = records_by_key.into_records();
    sort_cluster_connectivity_records(&mut records);
    Ok(records)
}

fn route_graph_record_is_usable(
    record: &ClusterConnectivityRecord,
    cluster_id: Option<&sinomo_node::ClusterId>,
    live_endpoint_index: Option<&ClusterRouteGraphLiveEndpointIndex>,
    now: OffsetDateTime,
) -> bool {
    cluster_id.is_none_or(|cluster_id| record.cluster_id == *cluster_id)
        && cluster_network_record_live(record.expires_at, now)
        && (record.state != ClusterConnectivityState::Unreachable
            || live_endpoint_index
                .as_ref()
                .is_none_or(|index| index.record_targets_live_endpoint(record)))
}

fn cluster_route_graph_live_endpoint_index(
    paths: &ClusterPaths,
    state: Option<&ClusterBootstrapState>,
) -> Result<Option<ClusterRouteGraphLiveEndpointIndex>, NetworkError> {
    let mut index = ClusterRouteGraphLiveEndpointIndex::default();
    if let Some(state) = state {
        for member in &state.members {
            index.try_insert_known_node(&member.node_id, "index route graph live endpoints")?;
            for endpoint in &member.endpoints {
                index.try_insert_endpoint(endpoint, "index route graph live endpoints")?;
            }
        }
    }
    let directory = crate::directory::cluster_directory_for(paths);
    match directory.members() {
        Ok(members) => {
            for member in members {
                index.try_insert_known_node(&member.node_id, "index route graph live endpoints")?;
                for endpoint in &member.endpoints {
                    index.try_insert_endpoint(endpoint, "index route graph live endpoints")?;
                }
            }
        }
        Err(error @ NetworkError::ResourceExhausted { .. }) => return Err(error),
        Err(_) => {}
    }
    match directory.endpoints() {
        Ok(endpoints) => {
            for endpoint in endpoints {
                index.try_insert_endpoint(&endpoint, "index route graph live endpoints")?;
            }
        }
        Err(error @ NetworkError::ResourceExhausted { .. }) => return Err(error),
        Err(_) => {}
    }
    Ok((!index.is_empty()).then_some(index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectivity::{CLUSTER_CONNECTIVITY_TTL_SECONDS, cluster_connectivity_record};
    use crate::connectivity_store::write_local_cluster_connectivity_records;
    use sinomo_base::{ClusterEndpointSource, ClusterTransportKind};
    use sinomo_node::ClusterId;

    #[derive(Clone)]
    struct StaticConnectivityQuery {
        records: Vec<ClusterConnectivityRecord>,
    }

    impl ClusterConnectivityRecordQuery for StaticConnectivityQuery {
        fn read_kv_connectivity_records(
            &self,
            _paths: &ClusterPaths,
        ) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
            Ok(self.records.clone())
        }
    }

    fn test_endpoint(name: &str, node_id: &NodeId, address: &str) -> ClusterEndpointRecord {
        ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(name).expect("endpoint id"),
            node_id: node_id.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: address.to_string(),
            source: ClusterEndpointSource::Static,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 10,
            last_observed_latency_ms: None,
        }
    }

    #[test]
    fn route_graph_uses_kv_graph_as_source_of_truth_when_available() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-bootstrap-missing");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_bootstrap").expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_route_graph_bootstrap_local").expect("local node");
        let kv_target =
            NodeId::from_legacy_named("node_route_graph_bootstrap_kv").expect("kv target");
        let bootstrap_target = NodeId::from_legacy_named("node_route_graph_bootstrap_local_edge")
            .expect("bootstrap target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph bootstrap",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let kv_endpoint = test_endpoint(
            "endpoint_route_graph_bootstrap_kv",
            &kv_target,
            "node-kv:21353",
        );
        let bootstrap_endpoint = test_endpoint(
            "endpoint_route_graph_bootstrap_local",
            &bootstrap_target,
            "node-bootstrap:21353",
        );
        let kv_record = cluster_connectivity_record(
            &state,
            &kv_endpoint,
            ClusterConnectivityState::Healthy,
            Some(11),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mut duplicate_local_record = kv_record.clone();
        duplicate_local_record.latency_ms = Some(99);
        let bootstrap_record = cluster_connectivity_record(
            &state,
            &bootstrap_endpoint,
            ClusterConnectivityState::Healthy,
            Some(22),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            &[duplicate_local_record, bootstrap_record.clone()],
        )
        .expect("write local connectivity");

        let records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![kv_record.clone()],
            },
        )
        .expect("read route graph records");

        assert_eq!(records.len(), 1);
        let kv = records
            .iter()
            .find(|record| record.target_node_id == kv_target)
            .expect("kv target record");
        assert_eq!(kv.latency_ms, Some(11));
        assert!(
            records
                .iter()
                .all(|record| record.target_node_id != bootstrap_target),
            "local bootstrap records must not extend a non-empty KV graph"
        );
    }

    #[test]
    fn route_graph_allows_local_source_bootstrap_until_source_reaches_kv() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-bootstrap-self");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_bootstrap").expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_route_graph_bootstrap_local").expect("local node");
        let authority =
            NodeId::from_legacy_named("node_route_graph_bootstrap_authority").expect("authority");
        let upstream_source =
            NodeId::from_legacy_named("node_route_graph_bootstrap_upstream").expect("upstream");
        let upstream_target =
            NodeId::from_legacy_named("node_route_graph_bootstrap_peer").expect("peer");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route graph bootstrap",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let upstream_state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph bootstrap upstream",
            upstream_source.clone(),
            "upstream",
            "pk-upstream",
            now,
        );
        let upstream_endpoint = test_endpoint(
            "endpoint_route_graph_bootstrap_upstream",
            &upstream_target,
            "node-peer:21353",
        );
        let authority_endpoint = test_endpoint(
            "endpoint_route_graph_bootstrap_authority",
            &authority,
            "node-authority:21353",
        );
        let upstream_kv_record = cluster_connectivity_record(
            &upstream_state,
            &upstream_endpoint,
            ClusterConnectivityState::Healthy,
            Some(13),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let local_bootstrap_record = cluster_connectivity_record(
            &state,
            &authority_endpoint,
            ClusterConnectivityState::Healthy,
            Some(5),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&local_bootstrap_record),
        )
        .expect("write local connectivity");

        let records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![upstream_kv_record],
            },
        )
        .expect("read route graph records");

        assert_eq!(records.len(), 2);
        let local_bootstrap = records
            .iter()
            .find(|record| record.source_node_id == local && record.target_node_id == authority)
            .expect("local source bootstrap record");
        assert_eq!(local_bootstrap.latency_ms, Some(5));
    }

    #[test]
    fn route_graph_composes_local_source_bootstrap_with_kv_upstream_route() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-bootstrap-route");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_bootstrap_route").expect("cluster");
        let local =
            NodeId::from_legacy_named("node_route_graph_bootstrap_route_local").expect("local");
        let bridge =
            NodeId::from_legacy_named("node_route_graph_bootstrap_route_bridge").expect("bridge");
        let authority = NodeId::from_legacy_named("node_route_graph_bootstrap_route_authority")
            .expect("authority");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route graph bootstrap route",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph bootstrap bridge",
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        let bridge_endpoint = test_endpoint(
            "endpoint_route_graph_bootstrap_route_bridge",
            &bridge,
            "bridge:21353",
        );
        let authority_endpoint = test_endpoint(
            "endpoint_route_graph_bootstrap_route_authority",
            &authority,
            "authority:21353",
        );
        let local_to_bridge = cluster_connectivity_record(
            &state,
            &bridge_endpoint,
            ClusterConnectivityState::Healthy,
            Some(2),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let bridge_to_authority = cluster_connectivity_record(
            &bridge_state,
            &authority_endpoint,
            ClusterConnectivityState::Healthy,
            Some(3),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&local_to_bridge),
        )
        .expect("write local connectivity");

        let records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![bridge_to_authority],
            },
        )
        .expect("read route graph records");
        let graph = ClusterConnectivityGraph::from_records(local.clone(), records);

        let route =
            crate::route::cluster_route_next_hop(&graph, &local, &authority).expect("route");

        assert_eq!(route.next_hop, bridge);
        assert_eq!(route.path, vec![local, bridge, authority]);
    }

    #[test]
    fn route_graph_uses_local_bootstrap_when_directory_graph_empty() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-bootstrap-empty");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_bootstrap").expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_route_graph_bootstrap_local").expect("local node");
        let bootstrap_target = NodeId::from_legacy_named("node_route_graph_bootstrap_local_edge")
            .expect("bootstrap target");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph bootstrap",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bootstrap_endpoint = test_endpoint(
            "endpoint_route_graph_bootstrap_local",
            &bootstrap_target,
            "node-bootstrap:21353",
        );
        let bootstrap_record = cluster_connectivity_record(
            &state,
            &bootstrap_endpoint,
            ClusterConnectivityState::Healthy,
            Some(22),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&bootstrap_record),
        )
        .expect("write local connectivity");

        let records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: Vec::new(),
            },
        )
        .expect("read route graph records");

        assert_eq!(records.len(), 1);
        let bootstrap = records
            .iter()
            .find(|record| record.target_node_id == bootstrap_target)
            .expect("bootstrap target record");
        assert_eq!(bootstrap.latency_ms, Some(22));
        assert_eq!(bootstrap.address, bootstrap_record.address);
    }
}
