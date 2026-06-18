use crate::prelude::*;
use crate::{ClusterPaths, NetworkError};

const CLUSTER_CONNECTIVITY_UNAVAILABLE_AFTER_FAILURES: u16 = 3;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterConnectivityState {
    Healthy,
    Degraded,
    Unreachable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClusterConnectivityRecord {
    pub cluster_id: ClusterId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub endpoint_id: ClusterEndpointId,
    pub transport: ClusterTransportKind,
    pub address: String,
    pub state: ClusterConnectivityState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    pub consecutive_failures: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct ClusterConnectivityGraph {
    source_node_id: NodeId,
    records: BTreeMap<ClusterConnectivityRecordKey, ClusterConnectivityRecord>,
    records_by_address: BTreeMap<ClusterConnectivityAddressKey, ClusterConnectivityRecordKey>,
    healthy_records_by_pair: BTreeMap<(NodeId, NodeId), BTreeSet<(u64, ClusterEndpointId)>>,
    reachable_records_by_pair: BTreeMap<(NodeId, NodeId), BTreeSet<(u8, u64, ClusterEndpointId)>>,
    reachable_targets_by_source: BTreeMap<NodeId, BTreeSet<NodeId>>,
    healthy_targets_by_source: BTreeMap<NodeId, BTreeSet<NodeId>>,
}

type ClusterConnectivityRecordKey = (NodeId, NodeId, ClusterEndpointId);
type ClusterConnectivityAddressKey = (NodeId, NodeId, String);

pub trait ClusterConnectivityRecordQuery {
    fn read_kv_connectivity_records(
        &self,
        paths: &ClusterPaths,
    ) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>;
}

impl ClusterConnectivityGraph {
    pub fn from_records(
        source_node_id: NodeId,
        records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    ) -> Self {
        let records: BTreeMap<_, _> = records
            .into_iter()
            .map(|record| {
                (
                    (
                        record.source_node_id.clone(),
                        record.target_node_id.clone(),
                        record.endpoint_id.clone(),
                    ),
                    record,
                )
            })
            .collect();
        let mut reachable_targets_by_source = BTreeMap::<NodeId, BTreeSet<NodeId>>::new();
        let mut healthy_targets_by_source = BTreeMap::<NodeId, BTreeSet<NodeId>>::new();
        let mut records_by_address =
            BTreeMap::<ClusterConnectivityAddressKey, ClusterConnectivityRecordKey>::new();
        let mut healthy_records_by_pair =
            BTreeMap::<(NodeId, NodeId), BTreeSet<(u64, ClusterEndpointId)>>::new();
        let mut reachable_records_by_pair =
            BTreeMap::<(NodeId, NodeId), BTreeSet<(u8, u64, ClusterEndpointId)>>::new();
        for record in records.values() {
            let record_key = (
                record.source_node_id.clone(),
                record.target_node_id.clone(),
                record.endpoint_id.clone(),
            );
            for address_key in cluster_endpoint_address_lookup_keys(&record.address) {
                records_by_address
                    .entry((
                        record.source_node_id.clone(),
                        record.target_node_id.clone(),
                        address_key,
                    ))
                    .or_insert_with(|| record_key.clone());
            }
            if record.target_node_id == record.source_node_id {
                continue;
            }
            if record.state != ClusterConnectivityState::Unreachable {
                reachable_targets_by_source
                    .entry(record.source_node_id.clone())
                    .or_default()
                    .insert(record.target_node_id.clone());
                reachable_records_by_pair
                    .entry((record.source_node_id.clone(), record.target_node_id.clone()))
                    .or_default()
                    .insert((
                        cluster_route_connectivity_quality_rank(Some(&record.state)),
                        record.latency_ms.unwrap_or(u64::MAX),
                        record.endpoint_id.clone(),
                    ));
            }
            if record.state == ClusterConnectivityState::Healthy {
                healthy_targets_by_source
                    .entry(record.source_node_id.clone())
                    .or_default()
                    .insert(record.target_node_id.clone());
                healthy_records_by_pair
                    .entry((record.source_node_id.clone(), record.target_node_id.clone()))
                    .or_default()
                    .insert((
                        record.latency_ms.unwrap_or(u64::MAX),
                        record.endpoint_id.clone(),
                    ));
            }
        }
        Self {
            source_node_id,
            records,
            records_by_address,
            healthy_records_by_pair,
            reachable_records_by_pair,
            reachable_targets_by_source,
            healthy_targets_by_source,
        }
    }

    pub(crate) fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    pub fn record_for_endpoint(
        &self,
        endpoint: &ClusterEndpointRecord,
    ) -> Option<&ClusterConnectivityRecord> {
        self.records
            .get(&(
                self.source_node_id.clone(),
                endpoint.node_id.clone(),
                endpoint.endpoint_id.clone(),
            ))
            .or_else(|| {
                // Fall back to matching on (source, target, address): the network address is
                // the stable identity of an endpoint. The same address can be derived with a
                // different `endpoint_id` or transport classification across code paths (the
                // probe uses member endpoints; routing queries with command-target endpoints),
                // which previously caused a silent miss -- so this no longer requires the
                // transport to match and normalizes the address.
                cluster_endpoint_address_lookup_keys(&endpoint.address)
                    .into_iter()
                    .find_map(|address_key| {
                        self.records_by_address
                            .get(&(
                                self.source_node_id.clone(),
                                endpoint.node_id.clone(),
                                address_key,
                            ))
                            .and_then(|record_key| self.records.get(record_key))
                    })
            })
    }

    pub fn state_for_endpoint(
        &self,
        endpoint: &ClusterEndpointRecord,
    ) -> Option<&ClusterConnectivityState> {
        self.record_for_endpoint(endpoint)
            .map(|record| &record.state)
    }

    pub fn unavailability_rank(&self, endpoint: &ClusterEndpointRecord) -> u8 {
        cluster_route_unavailability_rank(self.record_for_endpoint(endpoint))
    }

    pub fn quality_rank(&self, endpoint: &ClusterEndpointRecord) -> u8 {
        cluster_route_connectivity_quality_rank(self.state_for_endpoint(endpoint))
    }

    /// Node ids directly reachable from `source` per a non-`Unreachable` connectivity
    /// record (over any endpoint), excluding `source` itself. This is the adjacency the
    /// multi-hop route discovery in [`crate::route`] walks over the cluster-wide graph.
    pub fn reachable_targets(&self, source: &NodeId) -> BTreeSet<NodeId> {
        self.reachable_targets_by_source
            .get(source)
            .cloned()
            .unwrap_or_default()
    }

    /// Node ids `source` has a CURRENTLY-HEALTHY connectivity record to (over any endpoint),
    /// excluding `source`. Unlike [`reachable_targets`], `Degraded` does NOT count: a routing
    /// first hop must be a link we can dial reliably *now*, so a never-reachable peer that is
    /// merely mid-convergence (transiently `Degraded`) is not treated as a direct hop. This is
    /// the adjacency multi-hop route discovery in [`crate::route`] walks.
    pub fn healthy_targets(&self, source: &NodeId) -> BTreeSet<NodeId> {
        self.healthy_targets_by_source
            .get(source)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn healthy_target_refs<'a>(
        &'a self,
        source: &NodeId,
    ) -> impl Iterator<Item = &'a NodeId> + 'a {
        self.healthy_targets_by_source
            .get(source)
            .into_iter()
            .flat_map(|targets| targets.iter())
    }

    /// A dialable endpoint to reach `target` from `source` over a currently-healthy link, or
    /// `None` if no healthy record exists. Built straight from the connectivity record (which
    /// holds the exact address+transport last probed healthy), so it is immune to the
    /// endpoint-object matching in [`Self::record_for_endpoint`]; among several healthy
    /// endpoints it prefers the lowest observed latency. Used to pick the reachable link of a
    /// multi-homed bridge for relaying.
    pub fn best_healthy_endpoint(
        &self,
        source: &NodeId,
        target: &NodeId,
    ) -> Option<ClusterEndpointRecord> {
        self.best_healthy_endpoint_matching(source, target, |_| true)
    }

    pub(crate) fn best_healthy_endpoint_matching(
        &self,
        source: &NodeId,
        target: &NodeId,
        mut predicate: impl FnMut(&ClusterEndpointRecord) -> bool,
    ) -> Option<ClusterEndpointRecord> {
        self.healthy_records_by_pair
            .get(&(source.clone(), target.clone()))
            .into_iter()
            .flat_map(|records| records.iter())
            .filter_map(|(_, endpoint_id)| {
                self.records
                    .get(&(source.clone(), target.clone(), endpoint_id.clone()))
            })
            .map(cluster_endpoint_record_for_connectivity_record)
            .find(|endpoint| predicate(endpoint))
    }

    /// A dialable endpoint to reach `target` from `source` over a non-`Unreachable`
    /// link. Healthy links are preferred; degraded links are returned only when
    /// no healthy endpoint is available. This is used by relay forwarding during
    /// convergence, where the next hop may be known reachable but not yet healthy.
    pub fn best_reachable_endpoint(
        &self,
        source: &NodeId,
        target: &NodeId,
    ) -> Option<ClusterEndpointRecord> {
        self.best_reachable_endpoint_matching(source, target, |_| true)
    }

    pub(crate) fn best_reachable_endpoint_matching(
        &self,
        source: &NodeId,
        target: &NodeId,
        mut predicate: impl FnMut(&ClusterEndpointRecord) -> bool,
    ) -> Option<ClusterEndpointRecord> {
        self.reachable_records_by_pair
            .get(&(source.clone(), target.clone()))
            .into_iter()
            .flat_map(|records| records.iter())
            .filter_map(|(_, _, endpoint_id)| {
                self.records
                    .get(&(source.clone(), target.clone(), endpoint_id.clone()))
            })
            .map(cluster_endpoint_record_for_connectivity_record)
            .find(|endpoint| predicate(endpoint))
    }

    pub(crate) fn earliest_expires_at(&self) -> Option<OffsetDateTime> {
        self.records.values().map(|record| record.expires_at).min()
    }
}

fn cluster_endpoint_record_for_connectivity_record(
    record: &ClusterConnectivityRecord,
) -> ClusterEndpointRecord {
    ClusterEndpointRecord {
        endpoint_id: record.endpoint_id.clone(),
        node_id: record.target_node_id.clone(),
        transport: record.transport.clone(),
        address: record.address.clone(),
        source: ClusterEndpointSource::Observed,
        privacy: ClusterEndpointPrivacy::Lan,
        priority: 0,
        last_observed_latency_ms: record
            .latency_ms
            .map(|ms| ms.min(u64::from(u32::MAX)) as u32),
    }
}

/// Whether two endpoint address strings denote the same network address. Compares the raw
/// strings, then a normalized `SocketAddr` parse so equivalent spellings still match. Used by
/// connectivity-record lookup, where the same address may be derived slightly differently
/// across code paths (probe targets vs command targets).
fn cluster_endpoint_address_lookup_keys(address: &str) -> Vec<String> {
    let mut keys = vec![format!("raw:{address}")];
    if let Ok(socket_addr) = address.parse::<SocketAddr>() {
        keys.push(format!("socket:{socket_addr}"));
    }
    keys
}

fn cluster_route_unavailability_rank(connectivity: Option<&ClusterConnectivityRecord>) -> u8 {
    match connectivity {
        Some(record)
            if record.state == ClusterConnectivityState::Unreachable
                && record.consecutive_failures
                    >= CLUSTER_CONNECTIVITY_UNAVAILABLE_AFTER_FAILURES =>
        {
            1
        }
        _ => 0,
    }
}

fn cluster_route_connectivity_quality_rank(connectivity: Option<&ClusterConnectivityState>) -> u8 {
    match connectivity {
        Some(ClusterConnectivityState::Healthy) => 0,
        Some(ClusterConnectivityState::Degraded) => 1,
        None => 2,
        Some(ClusterConnectivityState::Unreachable) => 3,
    }
}
