use crate::connectivity::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityRecordQuery,
};
use crate::connectivity_publish::cluster_network_record_live;
use crate::connectivity_store::{
    local_cluster_connectivity_content_generation, read_local_cluster_connectivity_records,
    sort_cluster_connectivity_records,
};
use crate::error::NetworkError;
use crate::kv_read_model::KvBackedClusterConnectivityRecordQuery;
use crate::storage::{ClusterFileMark, cluster_file_mark};
use crate::{ClusterEndpointId, ClusterPaths};
use sinomo_core::ClusterBootstrapState;
use sinomo_node::NodeId;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

const MAX_CLUSTER_ROUTE_GRAPH_CACHE_ENTRIES: usize = 128;
const CLUSTER_ROUTE_GRAPH_CACHE_MAX_AGE: Duration = Duration::from_secs(1);

static CLUSTER_ROUTE_GRAPH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_ROUTE_GRAPHS: LazyLock<Mutex<HashMap<PathBuf, ClusterRouteGraphCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ClusterRouteGraphCacheEntry {
    mark: ClusterRouteGraphCacheMark,
    graph: ClusterConnectivityGraph,
    cached_at: Instant,
    earliest_expires_at: Option<OffsetDateTime>,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClusterRouteGraphCacheMark {
    state: ClusterFileMark,
    local_connectivity: ClusterFileMark,
    local_connectivity_generation: u64,
}

impl ClusterConnectivityGraph {
    pub fn read_for_local(paths: &ClusterPaths, state: &ClusterBootstrapState) -> Self {
        let now = OffsetDateTime::now_utc();
        let checked_at = Instant::now();
        let before_mark = cluster_route_graph_cache_mark(paths);
        if let Some(graph) =
            read_cluster_route_graph_from_cache(paths, state, before_mark, now, checked_at)
        {
            return graph;
        }
        let graph =
            Self::read_for_local_with_query(paths, state, &KvBackedClusterConnectivityRecordQuery);
        let after_mark = cluster_route_graph_cache_mark(paths);
        if before_mark == after_mark {
            store_cluster_route_graph_cache_entry(
                paths,
                after_mark,
                graph.clone(),
                now,
                checked_at,
            );
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
                Some(&state.local_node_id),
                query,
            )
            .unwrap_or_default(),
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
        local_connectivity_generation: local_cluster_connectivity_content_generation(),
    })
}

fn read_cluster_route_graph_from_cache(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    mark: Option<ClusterRouteGraphCacheMark>,
    now: OffsetDateTime,
    checked_at: Instant,
) -> Option<ClusterConnectivityGraph> {
    let mark = mark?;
    let mut cache = CLUSTER_ROUTE_GRAPHS.lock().ok()?;
    let stale = {
        let entry = cache.get(&paths.root)?;
        entry.mark != mark
            || entry.graph.source_node_id() != &state.local_node_id
            || checked_at.duration_since(entry.cached_at) >= CLUSTER_ROUTE_GRAPH_CACHE_MAX_AGE
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
    cached_at: Instant,
) {
    let Some(mark) = mark else {
        return;
    };
    let Ok(mut cache) = CLUSTER_ROUTE_GRAPHS.lock() else {
        return;
    };
    evict_cluster_route_graph_cache_entry_if_needed(&mut cache, &paths.root);
    cache.insert(
        paths.root.clone(),
        ClusterRouteGraphCacheEntry {
            mark,
            earliest_expires_at: graph.earliest_expires_at(),
            graph,
            cached_at,
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
    read_cluster_connectivity_graph_with_suppressed_local_source_and_query(paths, None, query)
}

fn read_cluster_connectivity_graph_with_suppressed_local_source_and_query<Q>(
    paths: &ClusterPaths,
    local_source_node_id: Option<&NodeId>,
    query: &Q,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>
where
    Q: ClusterConnectivityRecordQuery + ?Sized,
{
    let now = OffsetDateTime::now_utc();
    let local_records = read_local_cluster_connectivity_records(paths)?
        .into_iter()
        .filter(|record| cluster_network_record_live(record.expires_at, now))
        .collect::<Vec<_>>();
    // A node's own live probe results are authoritative for its outgoing edges. Drop any
    // KV-published edge whose source we hold local records for; when constructing the local
    // route graph, also drop KV-published edges for this node even if the in-process cache is
    // empty after restart/network change. Self-published KV edges can be stale (e.g. healthy
    // before roaming, or unreachable before reconnecting), while other sources' edges are the
    // remote topology we still need for relay planning.
    let local_sources = local_records
        .iter()
        .map(|record| record.source_node_id.clone())
        .chain(local_source_node_id.cloned())
        .collect::<std::collections::BTreeSet<_>>();
    let mut records_by_key =
        BTreeMap::<(NodeId, NodeId, ClusterEndpointId), ClusterConnectivityRecord>::new();
    for record in query.read_kv_connectivity_records(paths)? {
        if local_sources.contains(&record.source_node_id) {
            continue;
        }
        records_by_key.insert(
            (
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            ),
            record,
        );
    }
    for record in local_records {
        records_by_key.insert(
            (
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            ),
            record,
        );
    }
    let mut records = records_by_key.into_values().collect::<Vec<_>>();
    sort_cluster_connectivity_records(&mut records);
    Ok(records)
}
