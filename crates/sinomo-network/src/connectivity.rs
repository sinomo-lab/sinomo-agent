use crate::endpoint_selector::{ClusterEndpointSelectionPurpose, ClusterEndpointSelector};
use crate::maintenance::{
    CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS,
    cluster_connectivity_probe_max_endpoints_per_tick_for_role,
    cluster_network_probe_interval_for_role,
};
use crate::prelude::*;
use crate::route_graph_read_model::read_cluster_connectivity_graph;
use crate::{ClusterPaths, NetworkError};

pub const CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD: u16 = 3;
pub const CLUSTER_CONNECTIVITY_TTL_SECONDS: i64 = 900;
pub const CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS: i64 = 7200;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityProbeState {
    pub record: ClusterConnectivityRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityProbeBatchEntry {
    pub key: String,
    pub endpoint: ClusterEndpointRecord,
    pub previous: Option<ClusterConnectivityRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityProbeBatch {
    pub active_keys: BTreeSet<String>,
    pub entries: Vec<ClusterConnectivityProbeBatchEntry>,
    pub next_cursor: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterConnectivityProbeObservation {
    Healthy,
    IdentityMismatch,
    Failed,
}

pub fn select_cluster_connectivity_probe_batch(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    connectivity_cursor: usize,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
    now: OffsetDateTime,
) -> ClusterConnectivityProbeBatch {
    let refresh_deadline =
        now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
    let targets = ClusterEndpointSelector::new(paths, state)
        .select(ClusterEndpointSelectionPurpose::NetworkProbe);
    let active_keys = targets
        .iter()
        .map(|endpoint| cluster_connectivity_probe_key(&state.local_node_id, endpoint))
        .collect::<BTreeSet<_>>();

    // Seed the in-memory probe cache from the merged graph (KV + the local probe store,
    // local-authoritative for our own edges). A learner cannot commit its connectivity
    // edges to KV, so seeding from KV alone would discard its last-known reachability.
    if connectivity.is_empty() {
        for record in read_cluster_connectivity_graph(paths)
            .unwrap_or_default()
            .into_iter()
            .filter(|record| record.source_node_id == state.local_node_id)
        {
            // Must match `cluster_connectivity_probe_key` ("src:tgt:endpoint"),
            // not the KV key, or the seed never lands in the probe cache.
            let key = format!(
                "{}:{}:{}",
                record.source_node_id, record.target_node_id, record.endpoint_id
            );
            if active_keys.contains(&key) {
                connectivity.insert(key, ClusterConnectivityProbeState { record });
            }
        }
    }

    let graph = ClusterConnectivityGraph::read_for_local(paths, state);
    let mut probe_entries = targets
        .iter()
        .filter_map(|endpoint| {
            let key = cluster_connectivity_probe_key(&state.local_node_id, endpoint);
            let previous = connectivity.get(&key).map(|probe| probe.record.clone());
            let due = previous.as_ref().is_none_or(|record| {
                cluster_connectivity_probe_due_with_graph(
                    record,
                    endpoint,
                    now,
                    refresh_deadline,
                    local_node_role,
                    &graph,
                    &state.local_node_id,
                    startup_settle_active,
                )
            });
            due.then(|| ClusterConnectivityProbeBatchEntry {
                key,
                endpoint: endpoint.clone(),
                previous,
            })
        })
        .collect::<Vec<_>>();
    probe_entries.sort_by_key(|entry| {
        (
            cluster_connectivity_probe_expiry_rank(entry.previous.as_ref()),
            cluster_connectivity_probe_state_rank(entry.previous.as_ref()),
            entry.endpoint.node_id.clone(),
            entry.endpoint.priority,
            entry.endpoint.address.clone(),
        )
    });
    let probe_count =
        probe_entries
            .len()
            .min(cluster_connectivity_probe_max_endpoints_per_tick_for_role(
                local_node_role,
            ));
    let _ = connectivity_cursor;
    let next_cursor = 0;
    let entries = probe_entries.into_iter().take(probe_count).collect();

    ClusterConnectivityProbeBatch {
        active_keys,
        entries,
        next_cursor,
    }
}

fn cluster_connectivity_probe_expiry_rank(
    record: Option<&ClusterConnectivityRecord>,
) -> OffsetDateTime {
    record
        .map(|record| record.expires_at)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn cluster_connectivity_probe_state_rank(record: Option<&ClusterConnectivityRecord>) -> u8 {
    match record.map(|record| &record.state) {
        None => 0,
        Some(ClusterConnectivityState::Healthy) => 1,
        Some(ClusterConnectivityState::Degraded) => 2,
        Some(ClusterConnectivityState::Unreachable) => 3,
    }
}

pub fn cluster_connectivity_record_for_probe_observation(
    state: &ClusterBootstrapState,
    entry: &ClusterConnectivityProbeBatchEntry,
    observation: ClusterConnectivityProbeObservation,
    latency_ms: u64,
    timestamp: &str,
    now: OffsetDateTime,
    startup_settle_active: bool,
) -> ClusterConnectivityRecord {
    match observation {
        ClusterConnectivityProbeObservation::Healthy => {
            cluster_connectivity_record_for_probe_outcome(
                state,
                &entry.endpoint,
                entry.previous.as_ref(),
                true,
                latency_ms,
                timestamp,
                now,
            )
        }
        ClusterConnectivityProbeObservation::IdentityMismatch => {
            cluster_connectivity_record_for_identity_mismatch(
                state,
                &entry.endpoint,
                timestamp,
                now,
            )
        }
        ClusterConnectivityProbeObservation::Failed if startup_settle_active => {
            cluster_connectivity_record_for_startup_probe_failure(
                state,
                &entry.endpoint,
                entry.previous.as_ref(),
                latency_ms,
                timestamp,
                now,
            )
        }
        ClusterConnectivityProbeObservation::Failed => {
            cluster_connectivity_record_for_probe_outcome(
                state,
                &entry.endpoint,
                entry.previous.as_ref(),
                false,
                latency_ms,
                timestamp,
                now,
            )
        }
    }
}

pub(crate) fn cluster_connectivity_record_for_probe_outcome(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    previous: Option<&ClusterConnectivityRecord>,
    healthy: bool,
    latency_ms: u64,
    timestamp: &str,
    now: OffsetDateTime,
) -> ClusterConnectivityRecord {
    if healthy {
        return cluster_connectivity_record(
            state,
            endpoint,
            ClusterConnectivityState::Healthy,
            Some(latency_ms),
            0,
            Some(timestamp.to_string()),
            previous.and_then(|record| record.last_failure_at.clone()),
            now,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        );
    }

    let observed_failures = previous
        .map(|record| record.consecutive_failures.saturating_add(1))
        .unwrap_or(1);
    let retained_success = previous.and_then(|record| {
        if observed_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
            record.last_success_at.clone()
        } else {
            None
        }
    });
    let previously_healthy =
        previous.is_some_and(|record| record.state == ClusterConnectivityState::Healthy);
    let state_label = if previously_healthy && observed_failures == 1 {
        // Single-failure grace: a confirmed-healthy link survives one transient
        // probe failure as Healthy. A persistent outage still degrades on the next
        // probe, while a never-confirmed link gets no grace.
        ClusterConnectivityState::Healthy
    } else if observed_failures >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
        ClusterConnectivityState::Unreachable
    } else {
        ClusterConnectivityState::Degraded
    };
    let failures = observed_failures.min(CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD);
    let ttl_seconds = if observed_failures >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
        CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS
    } else {
        CLUSTER_CONNECTIVITY_TTL_SECONDS
    };
    cluster_connectivity_record(
        state,
        endpoint,
        state_label,
        previous.and_then(|record| record.latency_ms),
        failures,
        retained_success,
        Some(timestamp.to_string()),
        now,
        ttl_seconds,
    )
}

pub(crate) fn cluster_connectivity_record_for_startup_probe_failure(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    previous: Option<&ClusterConnectivityRecord>,
    latency_ms: u64,
    timestamp: &str,
    now: OffsetDateTime,
) -> ClusterConnectivityRecord {
    let mut record = cluster_connectivity_record_for_probe_outcome(
        state, endpoint, previous, false, latency_ms, timestamp, now,
    );
    if record.state == ClusterConnectivityState::Unreachable {
        record.state = ClusterConnectivityState::Degraded;
        record.consecutive_failures = CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
            .saturating_sub(1)
            .max(1);
        record.expires_at = now + time::Duration::seconds(CLUSTER_CONNECTIVITY_TTL_SECONDS);
    }
    record
}

pub(crate) fn cluster_connectivity_record_for_identity_mismatch(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    timestamp: &str,
    now: OffsetDateTime,
) -> ClusterConnectivityRecord {
    cluster_connectivity_record(
        state,
        endpoint,
        ClusterConnectivityState::Unreachable,
        None,
        CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
        None,
        Some(timestamp.to_string()),
        now,
        CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
    )
}

pub(crate) fn cluster_connectivity_record_for_hard_failure(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    previous: Option<&ClusterConnectivityRecord>,
    timestamp: &str,
    now: OffsetDateTime,
) -> ClusterConnectivityRecord {
    let observed_failures = previous
        .map(|record| record.consecutive_failures.saturating_add(1))
        .unwrap_or(1);
    let failures = observed_failures.min(CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD);
    let retained_success = previous.and_then(|record| {
        if observed_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD {
            record.last_success_at.clone()
        } else {
            None
        }
    });
    let (state_label, ttl_seconds) = if observed_failures >= CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
    {
        (
            ClusterConnectivityState::Unreachable,
            CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
        )
    } else {
        (
            ClusterConnectivityState::Degraded,
            CLUSTER_CONNECTIVITY_TTL_SECONDS,
        )
    };
    cluster_connectivity_record(
        state,
        endpoint,
        state_label,
        previous.and_then(|record| record.latency_ms),
        failures,
        retained_success,
        Some(timestamp.to_string()),
        now,
        ttl_seconds,
    )
}

pub fn apply_cluster_connectivity_probe_results(
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    active_keys: &BTreeSet<String>,
    probed: Vec<(String, ClusterConnectivityRecord)>,
) -> Vec<ClusterConnectivityRecord> {
    for (key, record) in probed {
        connectivity.insert(key, ClusterConnectivityProbeState { record });
    }

    connectivity.retain(|key, _state| active_keys.contains(key));
    connectivity
        .values()
        .map(|state| state.record.clone())
        .collect()
}

pub fn cluster_connectivity_probe_due_with_startup_settle(
    record: &ClusterConnectivityRecord,
    now: OffsetDateTime,
    refresh_deadline: OffsetDateTime,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
) -> bool {
    if startup_settle_active && record.state != ClusterConnectivityState::Healthy {
        return true;
    }
    if record.expires_at <= refresh_deadline {
        return true;
    }
    let retry_interval = cluster_network_probe_interval_for_role(local_node_role);
    let retry_seconds = retry_interval.as_secs().min(i64::MAX as u64) as i64;
    let retry_due = record.observed_at + time::Duration::seconds(retry_seconds) <= now;
    if record.state == ClusterConnectivityState::Unreachable {
        return retry_due;
    }
    if record.state != ClusterConnectivityState::Healthy
        && record.consecutive_failures < CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD
    {
        return retry_due;
    }
    if record.state != ClusterConnectivityState::Healthy {
        // The failure budget is exhausted, but the endpoint must not freeze
        // as degraded until record expiry: a peer that becomes reachable
        // later has to be rediscovered. Retry at a slower cadence than
        // healthy refreshes.
        let degraded_retry_seconds = retry_seconds.saturating_mul(2);
        return record.observed_at + time::Duration::seconds(degraded_retry_seconds) <= now;
    }
    record.consecutive_failures > 0 && retry_due
}

pub fn cluster_connectivity_probe_due_with_graph(
    record: &ClusterConnectivityRecord,
    endpoint: &ClusterEndpointRecord,
    now: OffsetDateTime,
    refresh_deadline: OffsetDateTime,
    local_node_role: &ClusterNodeRole,
    graph: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    startup_settle_active: bool,
) -> bool {
    if !cluster_connectivity_probe_due_with_startup_settle(
        record,
        now,
        refresh_deadline,
        local_node_role,
        startup_settle_active,
    ) {
        return false;
    }
    if startup_settle_active && record.state != ClusterConnectivityState::Healthy {
        return true;
    }
    if record.expires_at <= refresh_deadline {
        return true;
    }
    if record.state == ClusterConnectivityState::Healthy {
        return true;
    }
    // A due non-healthy record re-probes even when a relay route exists:
    // a healed DIRECT endpoint always beats relaying, and the route may
    // itself traverse stale edges. The slow degraded cadence in the base
    // check keeps the probe volume negligible.
    let _ = (graph, local_node_id, endpoint);
    true
}

pub fn cluster_connectivity_probe_key(
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> String {
    format!(
        "{}:{}:{}",
        source_node_id, endpoint.node_id, endpoint.endpoint_id
    )
}

pub fn cluster_connectivity_record(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    connectivity_state: ClusterConnectivityState,
    latency_ms: Option<u64>,
    consecutive_failures: u16,
    last_success_at: Option<String>,
    last_failure_at: Option<String>,
    observed_at: OffsetDateTime,
    ttl_seconds: i64,
) -> ClusterConnectivityRecord {
    // The caller passes an explicit per-state TTL (e.g. CLUSTER_CONNECTIVITY_TTL_SECONDS for
    // healthy/degraded, CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS for unreachable); honor it
    // directly rather than silently halving non-healthy lifetimes.
    ClusterConnectivityRecord {
        cluster_id: state.cluster_id.clone(),
        source_node_id: state.local_node_id.clone(),
        target_node_id: endpoint.node_id.clone(),
        endpoint_id: endpoint.endpoint_id.clone(),
        transport: endpoint.transport.clone(),
        address: endpoint.address.clone(),
        state: connectivity_state,
        latency_ms,
        consecutive_failures,
        last_success_at,
        last_failure_at,
        observed_at,
        expires_at: observed_at + time::Duration::seconds(ttl_seconds),
    }
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
