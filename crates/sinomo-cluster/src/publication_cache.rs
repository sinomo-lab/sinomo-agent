use crate::{
    cluster_node_status_publish_record_matches,
    connectivity_publish::cluster_connectivity_publish_record_matches,
};
use sinomo_base::ClusterNodeStatusRecord;
use sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
use sinomo_network::{
    ClusterConnectivityRecord, ClusterEndpointId, ClusterNetworkInfoRecord, ClusterPaths,
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS, cluster_network_info_publish_record_matches,
};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex;
use time::OffsetDateTime;

pub const CLUSTER_PUBLICATION_CACHE_MAX_NODE_RECORDS: usize = CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
pub const CLUSTER_PUBLICATION_CACHE_MAX_CONNECTIVITY_SOURCES: usize =
    CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;
pub const CLUSTER_PUBLICATION_CACHE_MAX_RECORDS_PER_SOURCE: usize =
    MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS;
pub const CLUSTER_PUBLICATION_CACHE_MAX_CONNECTIVITY_RECORDS: usize =
    CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES;

/// Bounds for the process-local projection of records already published to
/// cluster KV. Keeping the bounds explicit lets alternate runtime hosts use
/// the same cluster-owned policy without copying daemon globals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterPublicationCacheLimits {
    pub max_network_info_records: usize,
    pub max_node_status_records: usize,
    pub max_connectivity_sources: usize,
    pub max_connectivity_records_per_source: usize,
    pub max_connectivity_records: usize,
}

impl ClusterPublicationCacheLimits {
    pub const fn production() -> Self {
        Self {
            max_network_info_records: CLUSTER_PUBLICATION_CACHE_MAX_NODE_RECORDS,
            max_node_status_records: CLUSTER_PUBLICATION_CACHE_MAX_NODE_RECORDS,
            max_connectivity_sources: CLUSTER_PUBLICATION_CACHE_MAX_CONNECTIVITY_SOURCES,
            max_connectivity_records_per_source: CLUSTER_PUBLICATION_CACHE_MAX_RECORDS_PER_SOURCE,
            max_connectivity_records: CLUSTER_PUBLICATION_CACHE_MAX_CONNECTIVITY_RECORDS,
        }
    }
}

impl Default for ClusterPublicationCacheLimits {
    fn default() -> Self {
        Self::production()
    }
}

/// Cluster-owned, process-local cache of records known to have crossed the
/// quorum publication boundary.
///
/// Runtime hosts own the concrete KV transport. They call the `remember_*`
/// methods only after a successful or authoritative no-op publication and use
/// the matching methods to suppress redundant work. Local observations must
/// never warm this cache merely because they were persisted locally.
#[derive(Debug)]
pub struct ClusterPublicationCache {
    limits: ClusterPublicationCacheLimits,
    state: Mutex<ClusterPublicationCacheState>,
}

#[derive(Debug, Default)]
struct ClusterPublicationCacheState {
    network_info: BTreeMap<PublishedNodeKey, ClusterNetworkInfoRecord>,
    node_status: BTreeMap<PublishedNodeKey, ClusterNodeStatusRecord>,
    connectivity: BTreeMap<PublishedConnectivitySourceKey, PublishedConnectivitySourceCache>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PublishedNodeKey {
    root: PathBuf,
    node_id: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PublishedConnectivitySourceKey {
    root: PathBuf,
    source_node_id: NodeId,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PublishedConnectivityRecordKey {
    target_node_id: NodeId,
    endpoint_id: ClusterEndpointId,
}

#[derive(Debug, Default)]
struct PublishedConnectivitySourceCache {
    records: BTreeMap<PublishedConnectivityRecordKey, ClusterConnectivityRecord>,
}

impl Default for ClusterPublicationCache {
    fn default() -> Self {
        Self::new(ClusterPublicationCacheLimits::production())
    }
}

impl ClusterPublicationCache {
    pub fn new(limits: ClusterPublicationCacheLimits) -> Self {
        assert!(
            limits.max_network_info_records > 0,
            "network-info publication cache capacity must be positive"
        );
        assert!(
            limits.max_node_status_records > 0,
            "node-status publication cache capacity must be positive"
        );
        assert!(
            limits.max_connectivity_sources > 0,
            "connectivity publication source capacity must be positive"
        );
        assert!(
            limits.max_connectivity_records_per_source > 0,
            "per-source connectivity publication capacity must be positive"
        );
        assert!(
            limits.max_connectivity_records > 0,
            "connectivity publication record capacity must be positive"
        );
        Self {
            limits,
            state: Mutex::new(ClusterPublicationCacheState::default()),
        }
    }

    pub fn network_info_matches(
        &self,
        paths: &ClusterPaths,
        info: &ClusterNetworkInfoRecord,
        now: OffsetDateTime,
    ) -> bool {
        let key = published_node_key(paths, &info.node_id);
        self.state.lock().is_ok_and(|state| {
            state
                .network_info
                .get(&key)
                .is_some_and(|known| cluster_network_info_publish_record_matches(known, info, now))
        })
    }

    pub fn remember_network_info(&self, paths: &ClusterPaths, info: &ClusterNetworkInfoRecord) {
        let key = published_node_key(paths, &info.node_id);
        if let Ok(mut state) = self.state.lock() {
            insert_bounded(
                &mut state.network_info,
                key,
                info.clone(),
                self.limits.max_network_info_records,
            );
        }
    }

    pub fn node_status_matches(
        &self,
        paths: &ClusterPaths,
        status: &ClusterNodeStatusRecord,
    ) -> bool {
        let key = published_node_key(paths, &status.node_id);
        self.state.lock().is_ok_and(|state| {
            state
                .node_status
                .get(&key)
                .is_some_and(|known| cluster_node_status_publish_record_matches(known, status))
        })
    }

    pub fn remember_node_status(&self, paths: &ClusterPaths, status: &ClusterNodeStatusRecord) {
        let key = published_node_key(paths, &status.node_id);
        if let Ok(mut state) = self.state.lock() {
            insert_bounded(
                &mut state.node_status,
                key,
                status.clone(),
                self.limits.max_node_status_records,
            );
        }
    }

    pub fn connectivity_record_set_matches(
        &self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
        now: OffsetDateTime,
    ) -> bool {
        let incoming_keys = records
            .iter()
            .map(connectivity_record_key)
            .collect::<BTreeSet<_>>();
        let source_key = connectivity_source_key(paths, source_node_id);
        self.state.lock().is_ok_and(|state| {
            let Some(source) = state.connectivity.get(&source_key) else {
                return false;
            };
            if source
                .records
                .keys()
                .any(|key| !incoming_keys.contains(key))
            {
                return false;
            }
            records.iter().all(|record| {
                source
                    .records
                    .get(&connectivity_record_key(record))
                    .is_some_and(|known| {
                        cluster_connectivity_publish_record_matches(known, record, now)
                    })
            })
        })
    }

    pub fn remember_connectivity_record_set(
        &self,
        paths: &ClusterPaths,
        source_node_id: &NodeId,
        records: &[ClusterConnectivityRecord],
    ) {
        if let Ok(mut state) = self.state.lock() {
            let source_key = connectivity_source_key(paths, source_node_id);
            ensure_connectivity_source(
                &mut state.connectivity,
                source_key.clone(),
                self.limits.max_connectivity_sources,
            );
            let source = state
                .connectivity
                .get_mut(&source_key)
                .expect("published connectivity source was inserted");
            source.records.clear();
            if records.len() <= self.limits.max_connectivity_records_per_source {
                for record in records {
                    source
                        .records
                        .insert(connectivity_record_key(record), record.clone());
                }
            }
            trim_connectivity_records(
                &mut state.connectivity,
                &source_key,
                self.limits.max_connectivity_records,
            );
        }
    }
}

fn published_node_key(paths: &ClusterPaths, node_id: &NodeId) -> PublishedNodeKey {
    PublishedNodeKey {
        root: paths.root.clone(),
        node_id: node_id.clone(),
    }
}

fn connectivity_source_key(
    paths: &ClusterPaths,
    source_node_id: &NodeId,
) -> PublishedConnectivitySourceKey {
    PublishedConnectivitySourceKey {
        root: paths.root.clone(),
        source_node_id: source_node_id.clone(),
    }
}

fn connectivity_record_key(record: &ClusterConnectivityRecord) -> PublishedConnectivityRecordKey {
    PublishedConnectivityRecordKey {
        target_node_id: record.target_node_id.clone(),
        endpoint_id: record.endpoint_id.clone(),
    }
}

fn insert_bounded<K, V>(cache: &mut BTreeMap<K, V>, key: K, value: V, max_entries: usize)
where
    K: Clone + Ord,
{
    if !cache.contains_key(&key) {
        while cache.len() >= max_entries {
            let Some(first) = cache.keys().next().cloned() else {
                break;
            };
            cache.remove(&first);
        }
    }
    cache.insert(key, value);
}

fn ensure_connectivity_source(
    cache: &mut BTreeMap<PublishedConnectivitySourceKey, PublishedConnectivitySourceCache>,
    source_key: PublishedConnectivitySourceKey,
    max_sources: usize,
) {
    if cache.contains_key(&source_key) {
        return;
    }
    while cache.len() >= max_sources {
        let Some(first) = cache.keys().next().cloned() else {
            break;
        };
        cache.remove(&first);
    }
    cache.insert(source_key, PublishedConnectivitySourceCache::default());
}

fn connectivity_record_count(
    cache: &BTreeMap<PublishedConnectivitySourceKey, PublishedConnectivitySourceCache>,
) -> usize {
    cache.values().map(|source| source.records.len()).sum()
}

fn trim_connectivity_records(
    cache: &mut BTreeMap<PublishedConnectivitySourceKey, PublishedConnectivitySourceCache>,
    protected_source_key: &PublishedConnectivitySourceKey,
    max_records: usize,
) {
    while connectivity_record_count(cache) > max_records {
        let evict_source = cache
            .iter()
            .find(|(source_key, source)| {
                *source_key != protected_source_key && !source.records.is_empty()
            })
            .map(|(source_key, _)| source_key.clone())
            .or_else(|| {
                cache
                    .keys()
                    .find(|source_key| *source_key != protected_source_key)
                    .cloned()
            });
        if let Some(source_key) = evict_source {
            cache.remove(&source_key);
            continue;
        }

        let Some(source) = cache.get_mut(protected_source_key) else {
            break;
        };
        while source.records.len() > max_records {
            let Some(first) = source.records.keys().next().cloned() else {
                break;
            };
            source.records.remove(&first);
        }
        break;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::ClusterTransportKind;
    use sinomo_local::SinomoDataDir;
    use sinomo_node::ClusterId;

    fn cache_with_small_limits() -> ClusterPublicationCache {
        ClusterPublicationCache::new(ClusterPublicationCacheLimits {
            max_network_info_records: 2,
            max_node_status_records: 2,
            max_connectivity_sources: 3,
            max_connectivity_records_per_source: 3,
            max_connectivity_records: 2,
        })
    }

    fn connectivity_record(
        source: &str,
        target: &str,
        endpoint: &str,
    ) -> ClusterConnectivityRecord {
        let now = OffsetDateTime::now_utc();
        ClusterConnectivityRecord {
            cluster_id: ClusterId::from_legacy_named("cluster_publication_cache").expect("cluster"),
            source_node_id: NodeId::from_legacy_named(source).expect("source"),
            target_node_id: NodeId::from_legacy_named(target).expect("target"),
            endpoint_id: ClusterEndpointId::from_legacy_named(endpoint).expect("endpoint"),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.10:21353".to_string(),
            state: sinomo_network::ClusterConnectivityState::Healthy,
            latency_ms: Some(4),
            consecutive_failures: 0,
            last_success_at: Some("ok".to_string()),
            last_failure_at: None,
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::minutes(5),
        }
    }

    #[test]
    fn bounded_insert_evicts_the_first_non_matching_key() {
        let mut values = BTreeMap::new();
        insert_bounded(&mut values, "a", 1, 2);
        insert_bounded(&mut values, "b", 2, 2);
        insert_bounded(&mut values, "c", 3, 2);

        assert_eq!(values, BTreeMap::from([("b", 2), ("c", 3)]));
    }

    #[test]
    fn connectivity_cache_is_scoped_and_matches_committed_semantics() {
        let cache = cache_with_small_limits();
        let paths_a = ClusterPaths::new(&SinomoDataDir::new("/tmp/publication-cache-a"));
        let paths_b = ClusterPaths::new(&SinomoDataDir::new("/tmp/publication-cache-b"));
        let record =
            connectivity_record("node_cache_source", "node_cache_target", "endpoint_cache");
        let now = record.observed_at;

        cache.remember_connectivity_record_set(
            &paths_a,
            &record.source_node_id,
            std::slice::from_ref(&record),
        );
        assert!(cache.connectivity_record_set_matches(
            &paths_a,
            &record.source_node_id,
            std::slice::from_ref(&record),
            now,
        ));
        assert!(!cache.connectivity_record_set_matches(
            &paths_b,
            &record.source_node_id,
            std::slice::from_ref(&record),
            now,
        ));

        // Publication never renews an unchanged row, so its age cannot make
        // it unpublished; only a semantic change can.
        let mut aged = record;
        aged.expires_at = now - time::Duration::seconds(30);
        cache.remember_connectivity_record_set(
            &paths_b,
            &aged.source_node_id,
            std::slice::from_ref(&aged),
        );
        assert!(cache.connectivity_record_set_matches(
            &paths_b,
            &aged.source_node_id,
            std::slice::from_ref(&aged),
            now,
        ));
        let mut changed = aged;
        changed.state = sinomo_network::ClusterConnectivityState::Unreachable;
        assert!(!cache.connectivity_record_set_matches(
            &paths_b,
            &changed.source_node_id,
            std::slice::from_ref(&changed),
            now,
        ));
    }

    #[test]
    fn connectivity_cache_distinguishes_unknown_and_known_empty_sources() {
        let cache = cache_with_small_limits();
        let paths = ClusterPaths::new(&SinomoDataDir::new("/tmp/publication-cache-empty"));
        let source = NodeId::from_legacy_named("node_cache_empty_source").expect("source");
        let now = OffsetDateTime::now_utc();

        assert!(!cache.connectivity_record_set_matches(&paths, &source, &[], now));
        cache.remember_connectivity_record_set(&paths, &source, &[]);
        assert!(cache.connectivity_record_set_matches(&paths, &source, &[], now));
    }

    #[test]
    fn connectivity_cache_enforces_the_process_wide_record_bound() {
        let cache = cache_with_small_limits();
        let paths = ClusterPaths::new(&SinomoDataDir::new("/tmp/publication-cache-bounded"));
        let first = connectivity_record(
            "node_cache_source_a",
            "node_cache_target_a",
            "endpoint_cache_a",
        );
        let second = connectivity_record(
            "node_cache_source_b",
            "node_cache_target_b",
            "endpoint_cache_b",
        );
        let third = connectivity_record(
            "node_cache_source_c",
            "node_cache_target_c",
            "endpoint_cache_c",
        );
        let now = first.observed_at;

        for record in [&first, &second, &third] {
            cache.remember_connectivity_record_set(
                &paths,
                &record.source_node_id,
                std::slice::from_ref(record),
            );
        }

        assert!(!cache.connectivity_record_set_matches(
            &paths,
            &first.source_node_id,
            std::slice::from_ref(&first),
            now,
        ));
        assert!(cache.connectivity_record_set_matches(
            &paths,
            &second.source_node_id,
            std::slice::from_ref(&second),
            now,
        ));
        assert!(cache.connectivity_record_set_matches(
            &paths,
            &third.source_node_id,
            std::slice::from_ref(&third),
            now,
        ));
        assert_eq!(
            connectivity_record_count(&cache.state.lock().expect("publication cache").connectivity),
            2
        );
    }
}
