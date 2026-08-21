use crate::{
    ClusterBootstrapState, ClusterEndpointPrivacy, ClusterEndpointRecord, ClusterEndpointSource,
    ClusterMemberRecord, ClusterTransportKind,
};
use sinomo_kv::{ClusterKvCommittedDelta, KvEvent, KvEventKind, KvSnapshot, decode_kv_value};
use sinomo_network::__connectivity_store::{
    ClusterConnectivityRoutingProjection, cluster_connectivity_routing_projection,
};
use sinomo_network::{
    ClusterEndpointSelectionPurpose, ClusterEndpointSelector, ClusterPaths, NetworkError,
};
use sinomo_node::NodeId;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityEndpointProjection {
    pub endpoint_id: sinomo_base::Id,
    pub node_id: NodeId,
    pub transport: ClusterTransportKind,
    pub address: String,
    pub source: ClusterEndpointSource,
    pub privacy: ClusterEndpointPrivacy,
    pub priority: u16,
}

impl From<&ClusterEndpointRecord> for ClusterConnectivityEndpointProjection {
    fn from(endpoint: &ClusterEndpointRecord) -> Self {
        Self {
            endpoint_id: endpoint.endpoint_id.clone(),
            node_id: endpoint.node_id.clone(),
            transport: endpoint.transport.clone(),
            address: endpoint.address.clone(),
            source: endpoint.source.clone(),
            privacy: endpoint.privacy.clone(),
            priority: endpoint.priority,
        }
    }
}

/// Projects only the endpoint attributes that can change probe input. This is
/// deliberately narrower than full bootstrap-state equality.
pub fn cluster_connectivity_probe_input_projection(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
) -> Result<Vec<ClusterConnectivityEndpointProjection>, NetworkError> {
    ClusterEndpointSelector::new(paths, state)
        .try_select(ClusterEndpointSelectionPurpose::NetworkProbe)
        .map(|endpoints| {
            endpoints
                .iter()
                .map(ClusterConnectivityEndpointProjection::from)
                .collect()
        })
}

/// Returns whether a retained probe generation still represents the current
/// cluster identity, local source, and selected endpoint inputs.
pub fn cluster_connectivity_probe_topology_is_current(
    paths: &ClusterPaths,
    expected: &ClusterBootstrapState,
    current: &ClusterBootstrapState,
) -> bool {
    expected.cluster_id == current.cluster_id
        && expected.local_node_id == current.local_node_id
        && cluster_connectivity_probe_input_projection(paths, expected).is_ok_and(|expected| {
            cluster_connectivity_probe_input_projection(paths, current)
                .is_ok_and(|current| current == expected)
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterConnectivityMemberProjection {
    pub node_id: NodeId,
    pub authorized: bool,
    pub endpoints: Vec<ClusterConnectivityEndpointProjection>,
}

impl From<&ClusterMemberRecord> for ClusterConnectivityMemberProjection {
    fn from(member: &ClusterMemberRecord) -> Self {
        let mut endpoints = member
            .endpoints
            .iter()
            .map(ClusterConnectivityEndpointProjection::from)
            .collect::<Vec<_>>();
        endpoints.sort_by(|left, right| {
            (&left.node_id, &left.endpoint_id, &left.address).cmp(&(
                &right.node_id,
                &right.endpoint_id,
                &right.address,
            ))
        });
        Self {
            node_id: member.node_id.clone(),
            authorized: member.state.is_authorized(),
            endpoints,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterConnectivityTopologyValue {
    Member(ClusterConnectivityMemberProjection),
    Endpoint(ClusterConnectivityEndpointProjection),
    Connectivity(ClusterConnectivityRoutingProjection),
    Malformed(Vec<u8>),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClusterConnectivityProjectionChange {
    pub probe_inputs_changed: bool,
    pub route_generation_changed: bool,
}

impl ClusterConnectivityProjectionChange {
    fn record_key(&mut self, key: &str) {
        if connectivity_probe_input_key(key) {
            self.probe_inputs_changed = true;
            self.route_generation_changed = true;
        } else if connectivity_record_key(key) {
            self.route_generation_changed = true;
        }
    }
}

#[derive(Debug)]
pub struct ClusterConnectivityTopologyProjection {
    revision: Option<u64>,
    pub values: BTreeMap<String, ClusterConnectivityTopologyValue>,
    include_connectivity_records: bool,
}

impl Default for ClusterConnectivityTopologyProjection {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ClusterConnectivityTopologyProjection {
    pub fn new(include_connectivity_records: bool) -> Self {
        Self {
            revision: None,
            values: BTreeMap::new(),
            include_connectivity_records,
        }
    }

    pub fn revision(&self) -> Option<u64> {
        self.revision
    }

    pub fn seed_if_newer(&mut self, snapshot: &KvSnapshot) {
        if self
            .revision
            .is_some_and(|revision| snapshot.revision.0 <= revision)
        {
            return;
        }
        self.values =
            topology_projection_from_snapshot(snapshot, self.include_connectivity_records);
        self.revision = Some(snapshot.revision.0);
    }

    pub fn observe(&mut self, snapshot: &KvSnapshot) -> ClusterConnectivityProjectionChange {
        let Some(revision) = self.revision else {
            let next =
                topology_projection_from_snapshot(snapshot, self.include_connectivity_records);
            let changed = connectivity_projection_change(&self.values, &next);
            self.values = next;
            self.revision = Some(snapshot.revision.0);
            return changed;
        };
        if snapshot.revision.0 <= revision {
            return ClusterConnectivityProjectionChange::default();
        }
        if revision < snapshot.compacted_revision.0 {
            let next =
                topology_projection_from_snapshot(snapshot, self.include_connectivity_records);
            let changed = connectivity_projection_change(&self.values, &next);
            self.values = next;
            self.revision = Some(snapshot.revision.0);
            return changed;
        }

        let first_new = snapshot
            .events
            .partition_point(|event| event.revision.0 <= revision);
        let new_events = &snapshot.events[first_new..];
        if !cluster_connectivity_events_cover_revision_range(
            revision,
            snapshot.revision.0,
            new_events,
        ) {
            let next =
                topology_projection_from_snapshot(snapshot, self.include_connectivity_records);
            let changed = connectivity_projection_change(&self.values, &next);
            self.values = next;
            self.revision = Some(snapshot.revision.0);
            return changed;
        }
        let mut before = BTreeMap::new();
        for event in new_events {
            if !connectivity_topology_key(&event.key, self.include_connectivity_records) {
                continue;
            }
            before
                .entry(event.key.clone())
                .or_insert_with(|| self.values.get(&event.key).cloned());
            match &event.kind {
                KvEventKind::Put { value } => {
                    self.values
                        .insert(event.key.clone(), topology_value(&event.key, value));
                }
                KvEventKind::Delete => {
                    self.values.remove(&event.key);
                }
            }
        }
        self.revision = Some(snapshot.revision.0);
        let mut changed = ClusterConnectivityProjectionChange::default();
        for (key, previous) in before {
            if previous != self.values.get(&key).cloned() {
                changed.record_key(&key);
            }
        }
        changed
    }

    /// Apply one complete process-local commit delta, or request a full
    /// resident rebuild when the delta cannot prove continuity.
    pub fn observe_delta(
        &mut self,
        delta: &ClusterKvCommittedDelta,
    ) -> Option<ClusterConnectivityProjectionChange> {
        let revision = self.revision?;
        if delta.revision.0 <= revision {
            return Some(ClusterConnectivityProjectionChange::default());
        }
        if revision < delta.compacted_revision.0 || delta.events.is_empty() {
            return None;
        }
        if !cluster_connectivity_events_cover_revision_range(
            revision,
            delta.revision.0,
            &delta.events,
        ) {
            return None;
        }

        let relevant_event_keys = delta
            .events
            .iter()
            .filter(|event| {
                connectivity_topology_key(&event.key, self.include_connectivity_records)
            })
            .map(|event| event.key.as_str())
            .collect::<BTreeSet<_>>();
        let mut final_keys = BTreeSet::new();
        let mut relevant_final_keys = BTreeSet::new();
        for state in &delta.final_key_states {
            if !final_keys.insert(state.key.as_str())
                || state
                    .entry
                    .as_ref()
                    .is_some_and(|entry| entry.key != state.key)
            {
                return None;
            }
            if connectivity_topology_key(&state.key, self.include_connectivity_records) {
                relevant_final_keys.insert(state.key.as_str());
            }
        }
        if relevant_event_keys != relevant_final_keys {
            return None;
        }

        let mut changed = ClusterConnectivityProjectionChange::default();
        for state in &delta.final_key_states {
            if !connectivity_topology_key(&state.key, self.include_connectivity_records) {
                continue;
            }
            let previous = self.values.get(&state.key).cloned();
            match state.entry.as_ref() {
                Some(entry) => {
                    self.values
                        .insert(state.key.clone(), topology_value(&state.key, &entry.value));
                }
                None => {
                    self.values.remove(&state.key);
                }
            }
            if previous != self.values.get(&state.key).cloned() {
                changed.record_key(&state.key);
            }
        }
        self.revision = Some(delta.revision.0);
        Some(changed)
    }
}

pub fn cluster_connectivity_events_cover_revision_range(
    previous_revision: u64,
    current_revision: u64,
    events: &[KvEvent],
) -> bool {
    current_revision <= previous_revision
        || (events.first().map(|event| event.revision.0)
            == Some(previous_revision.saturating_add(1))
            && events.last().map(|event| event.revision.0) == Some(current_revision)
            && events.windows(2).all(|events| {
                let previous = events[0].revision.0;
                let current = events[1].revision.0;
                current == previous || current == previous.saturating_add(1)
            }))
}

fn connectivity_projection_change(
    before: &BTreeMap<String, ClusterConnectivityTopologyValue>,
    after: &BTreeMap<String, ClusterConnectivityTopologyValue>,
) -> ClusterConnectivityProjectionChange {
    let mut changed = ClusterConnectivityProjectionChange::default();
    let mut keys = before.keys().map(String::as_str).collect::<BTreeSet<_>>();
    keys.extend(after.keys().map(String::as_str));
    for key in keys {
        if before.get(key) != after.get(key) {
            changed.record_key(key);
        }
    }
    changed
}

fn topology_projection_from_snapshot(
    snapshot: &KvSnapshot,
    include_connectivity_records: bool,
) -> BTreeMap<String, ClusterConnectivityTopologyValue> {
    snapshot
        .entries
        .iter()
        .filter(|entry| connectivity_topology_key(&entry.key, include_connectivity_records))
        .map(|entry| (entry.key.clone(), topology_value(&entry.key, &entry.value)))
        .collect()
}

fn topology_value(key: &str, value: &[u8]) -> ClusterConnectivityTopologyValue {
    if key.starts_with("/cluster/members/") {
        return decode_kv_value::<ClusterMemberRecord>(value)
            .map(|member| {
                ClusterConnectivityTopologyValue::Member(ClusterConnectivityMemberProjection::from(
                    &member,
                ))
            })
            .unwrap_or_else(|_| ClusterConnectivityTopologyValue::Malformed(value.to_vec()));
    }
    if key.starts_with("/cluster/endpoints/") {
        return decode_kv_value::<ClusterEndpointRecord>(value)
            .map(|endpoint| {
                ClusterConnectivityTopologyValue::Endpoint(
                    ClusterConnectivityEndpointProjection::from(&endpoint),
                )
            })
            .unwrap_or_else(|_| ClusterConnectivityTopologyValue::Malformed(value.to_vec()));
    }
    decode_kv_value::<sinomo_network::ClusterConnectivityRecord>(value)
        .map(|record| {
            ClusterConnectivityTopologyValue::Connectivity(cluster_connectivity_routing_projection(
                &record,
            ))
        })
        .unwrap_or_else(|_| ClusterConnectivityTopologyValue::Malformed(value.to_vec()))
}

fn connectivity_topology_key(key: &str, include_connectivity_records: bool) -> bool {
    connectivity_probe_input_key(key)
        || (include_connectivity_records && connectivity_record_key(key))
}

fn connectivity_probe_input_key(key: &str) -> bool {
    key.starts_with("/cluster/members/") || key.starts_with("/cluster/endpoints/")
}

fn connectivity_record_key(key: &str) -> bool {
    key.starts_with("/cluster/network/connectivity/")
}
