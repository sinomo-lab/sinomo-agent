use crate::connectivity::{
    ClusterConnectivityGraph, ClusterConnectivityRecord, ClusterConnectivityRecordQuery,
    ClusterConnectivityState, cluster_network_record_live,
};
use crate::connectivity_store::{
    local_cluster_connectivity_content_generation, read_local_cluster_connectivity_records,
    sort_cluster_connectivity_records,
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
use std::time::{Duration, Instant};
use time::OffsetDateTime;

const MAX_CLUSTER_ROUTE_GRAPH_CACHE_ENTRIES: usize = 128;
const CLUSTER_ROUTE_GRAPH_VALIDATION_LEASE: Duration = Duration::from_millis(250);

static CLUSTER_ROUTE_GRAPH_CACHE_CLOCK: AtomicU64 = AtomicU64::new(1);
static CLUSTER_ROUTE_GRAPHS: LazyLock<Mutex<HashMap<PathBuf, ClusterRouteGraphCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ClusterRouteGraphCacheEntry {
    mark: ClusterRouteGraphCacheMark,
    graph: ClusterConnectivityGraph,
    earliest_expires_at: Option<OffsetDateTime>,
    validated_at: Instant,
    last_used: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterRouteGraphCacheMark {
    state: ClusterFileMark,
    pub directory_version: Option<crate::directory::DirectoryVersion>,
    local_connectivity: ClusterFileMark,
    local_connectivity_generation: u64,
}

#[derive(Clone, Debug, Default)]
struct ClusterRouteGraphLiveEndpointIndex {
    known_nodes: BTreeSet<NodeId>,
    endpoint_ids_by_node: BTreeMap<NodeId, BTreeSet<ClusterEndpointId>>,
    endpoint_routes_by_node: BTreeMap<NodeId, Vec<(sinomo_base::ClusterTransportKind, String)>>,
}

impl ClusterRouteGraphLiveEndpointIndex {
    fn try_from_directory_snapshot(
        directory: &crate::DirectorySnapshot,
    ) -> Result<Self, NetworkError> {
        let mut index = Self::default();
        for node in directory.nodes().values() {
            index.try_insert_known_node(&node.node_id, "index routing snapshot nodes")?;
            for endpoint in &node.endpoints {
                index.try_insert_endpoint(endpoint, "index routing snapshot endpoints")?;
            }
        }
        Ok(index)
    }

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
        self.endpoint_routes_by_node
            .entry(crate::object::try_clone_id(&endpoint.node_id, operation)?)
            .or_default()
            .push((
                endpoint.transport.clone(),
                crate::object::try_clone_string(&endpoint.address, operation)?,
            ));
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.known_nodes.is_empty()
            && self.endpoint_ids_by_node.is_empty()
            && self.endpoint_routes_by_node.is_empty()
    }

    fn record_targets_live_endpoint(&self, record: &ClusterConnectivityRecord) -> bool {
        if let Some(endpoint_ids) = self.endpoint_ids_by_node.get(&record.target_node_id)
            && endpoint_ids.contains(&record.endpoint_id)
        {
            return true;
        }
        if let Some(endpoint_routes) = self.endpoint_routes_by_node.get(&record.target_node_id) {
            return endpoint_routes.iter().any(|(transport, address)| {
                transport == &record.transport && address == &record.address
            });
        }
        !self
            .endpoint_ids_by_node
            .contains_key(&record.target_node_id)
    }

    fn record_nodes_are_known(&self, record: &ClusterConnectivityRecord) -> bool {
        self.known_nodes.contains(&record.source_node_id)
            && self.known_nodes.contains(&record.target_node_id)
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

    fn try_insert_if_newer(
        &mut self,
        record: ClusterConnectivityRecord,
        operation: &str,
    ) -> Result<(), NetworkError> {
        let key = Self::record_key(&record, operation)?;
        let should_insert = self.records.get(&key).is_none_or(|existing| {
            if existing.state == ClusterConnectivityState::Healthy
                && crate::connectivity::cluster_connectivity_failure_reason_is_identity_mismatch(
                    &record,
                )
            {
                return false;
            }
            record.observed_at > existing.observed_at
        });
        if should_insert {
            self.records.insert(key, record);
        }
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
            .any(|(source, _, _)| source == source_node_id)
    }
}

impl ClusterConnectivityGraph {
    pub fn try_read_for_local(
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
    ) -> Result<Self, NetworkError> {
        let now = OffsetDateTime::now_utc();
        if let Some(graph) = read_cluster_route_graph_from_fresh_cache(paths, state, now) {
            return Ok(graph);
        }
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
        if let Some(graph) = read_cluster_route_graph_from_fresh_cache(paths, state, now) {
            return graph;
        }
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
    entry.validated_at = Instant::now();
    entry.last_used = cluster_route_graph_cache_tick();
    Some(entry.graph.clone())
}

fn read_cluster_route_graph_from_fresh_cache(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    now: OffsetDateTime,
) -> Option<ClusterConnectivityGraph> {
    let checked_at = Instant::now();
    let local_generation = local_cluster_connectivity_content_generation(paths);
    let mut cache = CLUSTER_ROUTE_GRAPHS.lock().ok()?;
    let stale = {
        let entry = cache.get(&paths.root)?;
        entry.mark.local_connectivity_generation != local_generation
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
    if checked_at.saturating_duration_since(entry.validated_at)
        >= CLUSTER_ROUTE_GRAPH_VALIDATION_LEASE
    {
        return None;
    }
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
            validated_at: Instant::now(),
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

/// Builds the connectivity graph against membership, endpoints, and KV
/// evidence captured from one directory generation.
///
/// Local source-owned observations remain an allowed bootstrap overlay, but
/// the directory/KV half of the graph cannot cross a materialization boundary
/// and accidentally filter every first hop from a still-connected node.
pub(crate) fn read_cluster_connectivity_graph_for_routing_snapshot(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    directory: &crate::DirectorySnapshot,
    directory_records: Vec<ClusterConnectivityRecord>,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    let now = OffsetDateTime::now_utc();
    let live_endpoint_index =
        ClusterRouteGraphLiveEndpointIndex::try_from_directory_snapshot(directory)?;
    let mut records_by_key = ClusterRouteGraphRecordIndex::default();
    for record in directory_records.into_iter().filter(|record| {
        route_graph_record_is_usable(
            record,
            Some(&state.cluster_id),
            Some(&live_endpoint_index),
            now,
        )
    }) {
        records_by_key.try_insert(record, "index routing snapshot connectivity records")?;
    }
    if !records_by_key.is_empty() {
        overlay_local_bootstrap_records(
            paths,
            Some(&state.cluster_id),
            Some(&live_endpoint_index),
            now,
            Some(&state.local_node_id),
            &mut records_by_key,
        )?;
        let mut records = records_by_key.into_records();
        sort_cluster_connectivity_records(&mut records);
        return Ok(records);
    }

    for record in read_local_cluster_connectivity_records(paths)?
        .into_iter()
        .filter(|record| {
            route_graph_record_is_usable(
                record,
                Some(&state.cluster_id),
                Some(&live_endpoint_index),
                now,
            )
        })
    {
        records_by_key.try_insert(record, "index routing snapshot bootstrap records")?;
    }
    let mut records = records_by_key.into_records();
    sort_cluster_connectivity_records(&mut records);
    Ok(records)
}

fn read_cluster_connectivity_graph_with_bootstrap_and_query<Q>(
    paths: &ClusterPaths,
    cluster_id: Option<&sinomo_node::ClusterId>,
    local_source_node_id: Option<&NodeId>,
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
                overlay_local_bootstrap_records(
                    paths,
                    cluster_id,
                    live_endpoint_index.as_ref(),
                    now,
                    local_source_node_id,
                    &mut records_by_key,
                )?;
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

fn overlay_local_bootstrap_records(
    paths: &ClusterPaths,
    cluster_id: Option<&sinomo_node::ClusterId>,
    live_endpoint_index: Option<&ClusterRouteGraphLiveEndpointIndex>,
    now: OffsetDateTime,
    local_source_node_id: Option<&NodeId>,
    records_by_key: &mut ClusterRouteGraphRecordIndex,
) -> Result<(), NetworkError> {
    let Some(local_source_node_id) = local_source_node_id else {
        return Ok(());
    };
    let local_source_reached_directory = records_by_key.contains_source(local_source_node_id);
    let local_records = match read_local_cluster_connectivity_records(paths) {
        Ok(records) => records,
        Err(_) => return Ok(()),
    };
    for record in local_records.into_iter().filter(|record| {
        (!local_source_reached_directory || record.source_node_id == *local_source_node_id)
            && route_graph_record_is_usable(record, cluster_id, live_endpoint_index, now)
    }) {
        // Before this node has published any observation into KV, its accepted bootstrap
        // graph is the only complete view it has of a relay path. Keep that whole view so
        // a sparse directory cannot erase an imported upstream edge during cold start.
        // Once this source reaches KV, only overlay its own newer observations: KV then
        // remains authoritative for records owned by every other source.
        records_by_key
            .try_insert_if_newer(record, "overlay local bootstrap route graph records")?;
    }
    Ok(())
}

fn route_graph_record_is_usable(
    record: &ClusterConnectivityRecord,
    cluster_id: Option<&sinomo_node::ClusterId>,
    live_endpoint_index: Option<&ClusterRouteGraphLiveEndpointIndex>,
    now: OffsetDateTime,
) -> bool {
    cluster_id.is_none_or(|cluster_id| record.cluster_id == *cluster_id)
        && cluster_network_record_live(record.expires_at, now)
        && live_endpoint_index
            .as_ref()
            .is_none_or(|index| index.record_nodes_are_known(record))
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
            if !member.state.is_authorized() {
                continue;
            }
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
                if !member.state.is_authorized() {
                    continue;
                }
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
    use sinomo_base::{ClusterEndpointSource, ClusterMemberRecord, ClusterTransportKind};
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

    fn add_test_member(
        state: &mut ClusterBootstrapState,
        node_id: &NodeId,
        endpoints: Vec<ClusterEndpointRecord>,
        now: OffsetDateTime,
    ) {
        let mut member = ClusterMemberRecord::local_voter(
            state.cluster_id.clone(),
            node_id.clone(),
            node_id.as_str(),
            "pk-test",
            now,
        );
        member.endpoints = endpoints;
        state.members.push(member);
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
        let mut state = ClusterBootstrapState::initialized_locally(
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
        add_test_member(&mut state, &kv_target, vec![kv_endpoint.clone()], now);
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
        let mut state = ClusterBootstrapState::initialized_locally(
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
        add_test_member(
            &mut state,
            &authority,
            vec![authority_endpoint.clone()],
            now,
        );
        add_test_member(&mut state, &upstream_source, Vec::new(), now);
        add_test_member(
            &mut state,
            &upstream_target,
            vec![upstream_endpoint.clone()],
            now,
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
    fn route_graph_prefers_newer_local_source_record_over_stale_kv_failure() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-local-newer");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_local_newer").expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_route_graph_local_newer_local").expect("local node");
        let target =
            NodeId::from_legacy_named("node_route_graph_local_newer_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph local newer",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = test_endpoint(
            "endpoint_route_graph_local_newer_target",
            &target,
            "198.51.100.76:21353",
        );
        add_test_member(&mut state, &target, vec![endpoint.clone()], now);

        let mut stale_kv_failure = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Unreachable,
            None,
            3,
            None,
            Some(now.to_string()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        stale_kv_failure.failure_reason =
            Some(crate::ClusterConnectivityFailureReason::IdentityMismatch);
        let refreshed_at = now + time::Duration::seconds(1);
        let local_healthy = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(42),
            0,
            Some(refreshed_at.to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&local_healthy),
        )
        .expect("write local connectivity");

        let records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![stale_kv_failure],
            },
        )
        .expect("read route graph records");
        let graph = ClusterConnectivityGraph::from_records(local.clone(), records);
        let endpoint = graph
            .best_healthy_endpoint(&local, &target)
            .expect("newer local healthy endpoint should override stale KV failure");

        assert_eq!(endpoint.address, "198.51.100.76:21353");
        assert_eq!(endpoint.last_observed_latency_ms, Some(42));
    }

    #[test]
    fn route_graph_keeps_kv_healthy_over_newer_local_identity_mismatch() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-kv-healthy");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_kv_healthy").expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_route_graph_kv_healthy_local").expect("local node");
        let target =
            NodeId::from_legacy_named("node_route_graph_kv_healthy_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph kv healthy",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = test_endpoint(
            "endpoint_route_graph_kv_healthy_target",
            &target,
            "198.51.100.88:21353",
        );
        add_test_member(&mut state, &target, vec![endpoint.clone()], now);

        let kv_healthy = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(31),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let mismatch_at = now + time::Duration::seconds(1);
        let local_mismatch = crate::connectivity::cluster_connectivity_record_for_identity_mismatch(
            &state,
            &endpoint,
            &mismatch_at.to_string(),
            mismatch_at,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&local_mismatch),
        )
        .expect("write local connectivity");

        let records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![kv_healthy],
            },
        )
        .expect("read route graph records");
        let graph = ClusterConnectivityGraph::from_records(local.clone(), records);
        let endpoint = graph
            .best_healthy_endpoint(&local, &target)
            .expect("KV healthy endpoint should survive local identity mismatch");

        assert_eq!(endpoint.address, "198.51.100.88:21353");
        assert_eq!(endpoint.last_observed_latency_ms, Some(31));
    }

    #[test]
    fn route_graph_prefers_newer_local_command_auth_denied_over_kv_healthy() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-command-auth-denied");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_route_graph_command_auth_denied")
            .expect("cluster id");
        let local = NodeId::from_legacy_named("node_route_graph_command_auth_denied_local")
            .expect("local node");
        let target = NodeId::from_legacy_named("node_route_graph_command_auth_denied_target")
            .expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph command auth denied",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = test_endpoint(
            "endpoint_route_graph_command_auth_denied_target",
            &target,
            "198.51.100.89:21353",
        );
        add_test_member(&mut state, &target, vec![endpoint.clone()], now);

        let kv_healthy = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(29),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let denied_at = now + time::Duration::seconds(1);
        let local_auth_denied =
            crate::connectivity::cluster_connectivity_record_for_command_auth_denied(
                &state,
                &endpoint,
                &denied_at.to_string(),
                denied_at,
            );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&local_auth_denied),
        )
        .expect("write local connectivity");

        let records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![kv_healthy],
            },
        )
        .expect("read route graph records");
        let graph = ClusterConnectivityGraph::from_records(local.clone(), records);

        assert!(
            graph.best_healthy_endpoint(&local, &target).is_none(),
            "newer command-auth-denied observations must block stale healthy route evidence"
        );
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
        let mut state = ClusterBootstrapState::initialized_locally(
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
        add_test_member(&mut state, &bridge, vec![bridge_endpoint.clone()], now);
        add_test_member(
            &mut state,
            &authority,
            vec![authority_endpoint.clone()],
            now,
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
    fn route_graph_preserves_imported_bootstrap_path_until_local_source_reaches_kv() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-imported-bootstrap");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id = ClusterId::from_legacy_named("cluster_route_graph_imported_bootstrap")
            .expect("cluster");
        let local =
            NodeId::from_legacy_named("node_route_graph_imported_bootstrap_local").expect("local");
        let bridge = NodeId::from_legacy_named("node_route_graph_imported_bootstrap_bridge")
            .expect("bridge");
        let authority = NodeId::from_legacy_named("node_route_graph_imported_bootstrap_authority")
            .expect("authority");
        let directory_source =
            NodeId::from_legacy_named("node_route_graph_imported_bootstrap_directory_source")
                .expect("directory source");
        let directory_target =
            NodeId::from_legacy_named("node_route_graph_imported_bootstrap_directory_target")
                .expect("directory target");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route graph imported bootstrap",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let bridge_state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route graph imported bootstrap bridge",
            bridge.clone(),
            "bridge",
            "pk-bridge",
            now,
        );
        let directory_state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph imported bootstrap directory",
            directory_source.clone(),
            "directory source",
            "pk-directory",
            now,
        );
        let bridge_endpoint = test_endpoint(
            "endpoint_route_graph_imported_bootstrap_bridge",
            &bridge,
            "bridge:21353",
        );
        let authority_endpoint = test_endpoint(
            "endpoint_route_graph_imported_bootstrap_authority",
            &authority,
            "authority:21353",
        );
        let directory_endpoint = test_endpoint(
            "endpoint_route_graph_imported_bootstrap_directory",
            &directory_target,
            "directory-target:21353",
        );
        add_test_member(&mut state, &bridge, vec![bridge_endpoint.clone()], now);
        add_test_member(
            &mut state,
            &authority,
            vec![authority_endpoint.clone()],
            now,
        );
        add_test_member(&mut state, &directory_source, Vec::new(), now);
        add_test_member(
            &mut state,
            &directory_target,
            vec![directory_endpoint.clone()],
            now,
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
        let unrelated_directory_record = cluster_connectivity_record(
            &directory_state,
            &directory_endpoint,
            ClusterConnectivityState::Healthy,
            Some(4),
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
        .expect("write local bootstrap edge");
        write_local_cluster_connectivity_records(
            &paths,
            &bridge,
            std::slice::from_ref(&bridge_to_authority),
        )
        .expect("write imported bootstrap edge");

        let cold_start_records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![unrelated_directory_record.clone()],
            },
        )
        .expect("read cold-start route graph records");
        let cold_start_graph =
            ClusterConnectivityGraph::from_records(local.clone(), cold_start_records);
        let route = crate::route::cluster_route_next_hop(&cold_start_graph, &local, &authority)
            .expect("imported bootstrap path must survive a sparse directory");

        assert_eq!(route.next_hop, bridge);
        assert_eq!(route.path, vec![local.clone(), bridge, authority.clone()]);

        let published_records = read_cluster_connectivity_graph_with_bootstrap_and_query(
            &paths,
            Some(&state.cluster_id),
            Some(&state.local_node_id),
            Some(&state),
            &StaticConnectivityQuery {
                records: vec![unrelated_directory_record, local_to_bridge],
            },
        )
        .expect("read published route graph records");
        let published_graph =
            ClusterConnectivityGraph::from_records(local.clone(), published_records);

        assert!(
            crate::route::cluster_route_next_hop(&published_graph, &local, &authority).is_none(),
            "after the local source reaches KV, non-local cached edges must stop extending it"
        );
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
        let mut state = ClusterBootstrapState::initialized_locally(
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
        add_test_member(
            &mut state,
            &bootstrap_target,
            vec![bootstrap_endpoint.clone()],
            now,
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

    #[test]
    fn route_graph_excludes_local_connectivity_for_removed_members() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-removed-members");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_removed").expect("cluster id");
        let local = NodeId::from_legacy_named("node_route_graph_removed_local").expect("local");
        let removed_target =
            NodeId::from_legacy_named("node_route_graph_removed_target").expect("removed target");
        let removed_source =
            NodeId::from_legacy_named("node_route_graph_removed_source").expect("removed source");
        let state = ClusterBootstrapState::initialized_locally(
            cluster_id.clone(),
            "route graph removed members",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let removed_target_endpoint = test_endpoint(
            "endpoint_route_graph_removed_target",
            &removed_target,
            "removed-target:21353",
        );
        let local_endpoint = test_endpoint(
            "endpoint_route_graph_removed_local",
            &local,
            "local-node:21353",
        );
        let removed_target_record = cluster_connectivity_record(
            &state,
            &removed_target_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        let removed_source_state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph removed source",
            removed_source.clone(),
            "removed-source",
            "pk-removed",
            now,
        );
        let removed_source_record = cluster_connectivity_record(
            &removed_source_state,
            &local_endpoint,
            ClusterConnectivityState::Healthy,
            Some(1),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(
            &paths,
            &local,
            std::slice::from_ref(&removed_target_record),
        )
        .expect("write removed target record");
        write_local_cluster_connectivity_records(
            &paths,
            &removed_source,
            std::slice::from_ref(&removed_source_record),
        )
        .expect("write removed source record");

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

        assert!(
            records.is_empty(),
            "state-scoped route graph must ignore local records for removed members"
        );
    }

    #[test]
    fn route_graph_cache_ignores_latency_only_local_connectivity_refresh() {
        let root = sinomo_testdir::TestDir::new("sinomo-network-route-graph-cache-refresh");
        let data_dir = sinomo_local::SinomoDataDir::new(&root);
        data_dir.ensure_root_layout().expect("layout");
        let paths = ClusterPaths::new(&data_dir);
        paths.ensure().expect("cluster paths");
        let now = OffsetDateTime::now_utc();
        let cluster_id =
            ClusterId::from_legacy_named("cluster_route_graph_cache").expect("cluster id");
        let local = NodeId::from_legacy_named("node_route_graph_cache_local").expect("local node");
        let target =
            NodeId::from_legacy_named("node_route_graph_cache_target").expect("target node");
        let mut state = ClusterBootstrapState::initialized_locally(
            cluster_id,
            "route graph cache",
            local.clone(),
            "local",
            "pk-local",
            now,
        );
        let endpoint = test_endpoint("endpoint_route_graph_cache", &target, "target:21353");
        add_test_member(&mut state, &target, vec![endpoint.clone()], now);
        let initial_record = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(7),
            0,
            Some(now.to_string()),
            None,
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[initial_record])
            .expect("write initial local connectivity");

        let initial_graph =
            ClusterConnectivityGraph::try_read_for_local(&paths, &state).expect("initial graph");
        let initial_endpoint = initial_graph
            .best_healthy_endpoint(&local, &target)
            .expect("initial healthy endpoint");
        assert_eq!(initial_endpoint.last_observed_latency_ms, Some(7));
        let cached_mark = cluster_route_graph_cache_mark(&paths).expect("cached mark");

        let refreshed_at = now + time::Duration::seconds(1);
        let refreshed_record = cluster_connectivity_record(
            &state,
            &endpoint,
            ClusterConnectivityState::Healthy,
            Some(19),
            0,
            Some(refreshed_at.to_string()),
            None,
            refreshed_at,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
        write_local_cluster_connectivity_records(&paths, &local, &[refreshed_record])
            .expect("refresh local connectivity");
        let refreshed_mark = cluster_route_graph_cache_mark(&paths).expect("refreshed mark");
        assert_eq!(
            cached_mark.local_connectivity,
            refreshed_mark.local_connectivity
        );
        assert_eq!(
            cached_mark.local_connectivity_generation, refreshed_mark.local_connectivity_generation,
            "latency-only evidence must not rebuild the route graph"
        );

        let refreshed_graph =
            ClusterConnectivityGraph::try_read_for_local(&paths, &state).expect("refreshed graph");
        let refreshed_endpoint = refreshed_graph
            .best_healthy_endpoint(&local, &target)
            .expect("refreshed healthy endpoint");
        assert_eq!(
            refreshed_endpoint.last_observed_latency_ms,
            Some(7),
            "the durable graph retains its published evidence while live path metrics own fresh samples"
        );
    }
}
