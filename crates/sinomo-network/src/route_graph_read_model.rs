use crate::connectivity::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityRecordQuery,
};
use crate::connectivity_publish::cluster_network_record_live;
use crate::connectivity_store::{
    local_cluster_connectivity_content_generation, read_local_cluster_connectivity_records,
    sort_cluster_connectivity_records,
};
use crate::error::NetworkError;
use crate::file_mark::{ClusterFileMark, cluster_file_mark};
use crate::kv_read_model::KvBackedClusterConnectivityRecordQuery;
use crate::{ClusterEndpointId, ClusterPaths};
use sinomo_core::ClusterBootstrapState;
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
struct ClusterRouteGraphCacheMark {
    state: ClusterFileMark,
    local_connectivity: ClusterFileMark,
    local_connectivity_generation: u64,
}

#[derive(Clone, Debug, Default)]
struct ClusterRouteGraphSourceSet {
    sources: BTreeSet<NodeId>,
}

impl ClusterRouteGraphSourceSet {
    fn try_insert_clone(
        &mut self,
        source_node_id: &NodeId,
        operation: &str,
    ) -> Result<bool, NetworkError> {
        Ok(self
            .sources
            .insert(crate::object::try_clone_id(source_node_id, operation)?))
    }

    fn contains(&self, source_node_id: &NodeId) -> bool {
        self.sources.contains(source_node_id)
    }
}

#[derive(Clone, Debug, Default)]
struct ClusterRouteGraphRecordIndex {
    records: BTreeMap<(NodeId, NodeId, ClusterEndpointId), ClusterConnectivityRecord>,
}

impl ClusterRouteGraphRecordIndex {
    fn try_insert(
        &mut self,
        record: ClusterConnectivityRecord,
        operation: &str,
    ) -> Result<(), NetworkError> {
        let key = (
            crate::object::try_clone_id(&record.source_node_id, operation)?,
            crate::object::try_clone_id(&record.target_node_id, operation)?,
            crate::object::try_clone_id(&record.endpoint_id, operation)?,
        );
        self.records.insert(key, record);
        Ok(())
    }

    fn into_records(self) -> Vec<ClusterConnectivityRecord> {
        self.records.into_values().collect()
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
            &KvBackedClusterConnectivityRecordQuery,
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
        let graph =
            Self::read_for_local_with_query(paths, state, &KvBackedClusterConnectivityRecordQuery);
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
            read_cluster_connectivity_graph_with_suppressed_local_source_and_query(
                paths,
                Some(&state.cluster_id),
                Some(&state.local_node_id),
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
            read_cluster_connectivity_graph_with_suppressed_local_source_and_query(
                paths,
                Some(&state.cluster_id),
                Some(&state.local_node_id),
                query,
            )?,
        )
    }
}

fn cluster_route_graph_cache_tick() -> u64 {
    CLUSTER_ROUTE_GRAPH_CACHE_CLOCK.fetch_add(1, Ordering::Relaxed)
}

fn cluster_route_graph_cache_mark(paths: &ClusterPaths) -> Option<ClusterRouteGraphCacheMark> {
    Some(ClusterRouteGraphCacheMark {
        state: cluster_file_mark(&paths.state)?,
        local_connectivity: cluster_file_mark(&paths.connectivity_local)?,
        local_connectivity_generation: local_cluster_connectivity_content_generation(paths),
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

pub(crate) fn read_cluster_connectivity_graph(
    paths: &ClusterPaths,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    read_cluster_connectivity_graph_with_query(paths, &KvBackedClusterConnectivityRecordQuery)
}

pub(crate) fn read_cluster_connectivity_graph_with_query<Q>(
    paths: &ClusterPaths,
    query: &Q,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>
where
    Q: ClusterConnectivityRecordQuery + ?Sized,
{
    read_cluster_connectivity_graph_with_suppressed_local_source_and_query(paths, None, None, query)
}

fn read_cluster_connectivity_graph_with_suppressed_local_source_and_query<Q>(
    paths: &ClusterPaths,
    cluster_id: Option<&sinomo_node::ClusterId>,
    local_source_node_id: Option<&NodeId>,
    query: &Q,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>
where
    Q: ClusterConnectivityRecordQuery + ?Sized,
{
    let now = OffsetDateTime::now_utc();
    let mut local_records = Vec::new();
    for record in read_local_cluster_connectivity_records(paths)?
        .into_iter()
        .filter(|record| {
            cluster_id.is_none_or(|cluster_id| record.cluster_id == *cluster_id)
                && cluster_network_record_live(record.expires_at, now)
        })
    {
        crate::object::try_reserve_vec(&mut local_records, 1, "collect local route graph records")?;
        local_records.push(record);
    }
    // A node's own live probe results are authoritative for its outgoing edges. Drop any
    // KV-published edge whose source we hold local records for; when constructing the local
    // route graph, also drop KV-published edges for this node even if the in-process cache is
    // empty after restart/network change. Self-published KV edges can be stale (e.g. healthy
    // before roaming, or unreachable before reconnecting), while other sources' edges are the
    // remote topology we still need for relay planning.
    let mut local_sources = ClusterRouteGraphSourceSet::default();
    for record in &local_records {
        local_sources
            .try_insert_clone(&record.source_node_id, "collect local route graph sources")?;
    }
    if let Some(local_source_node_id) = local_source_node_id {
        local_sources
            .try_insert_clone(local_source_node_id, "collect local route graph sources")?;
    }
    let mut records_by_key = ClusterRouteGraphRecordIndex::default();
    for record in query.read_kv_connectivity_records(paths)? {
        if cluster_id.is_some_and(|cluster_id| record.cluster_id != *cluster_id) {
            continue;
        }
        if local_sources.contains(&record.source_node_id) {
            continue;
        }
        records_by_key.try_insert(record, "index kv route graph records")?;
    }
    for record in local_records {
        records_by_key.try_insert(record, "index local route graph records")?;
    }
    let mut records = records_by_key.into_records();
    sort_cluster_connectivity_records(&mut records);
    Ok(records)
}
