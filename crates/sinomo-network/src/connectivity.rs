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
/// Maximum connectivity records a single source node may publish. Must stay
/// equal to `sinomo_kv::CLUSTER_KV_RANGE_PREFIX_MAX_ENTRIES` (the KV-sync code
/// asserts this at compile time) so a full source record set survives a
/// prefix read.
pub const MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS: usize = 4096;
/// Maximum records carried by one private connectivity snapshot RPC or
/// accepted by one aggregate local-store merge.
///
/// This remains equal to the cluster KV whole-prefix range limit without
/// introducing an R0 network -> KV dependency. A snapshot over this limit is
/// rejected rather than truncated because an incomplete graph can look valid
/// while omitting the only route to the state authority.
pub const MAX_CLUSTER_CONNECTIVITY_SNAPSHOT_RECORDS: usize = 4096;

/// Whether a network-domain record (connectivity/network-info) is still live
/// at `now` per its `expires_at` stamp.
pub fn cluster_network_record_live(expires_at: OffsetDateTime, now: OffsetDateTime) -> bool {
    expires_at > now
}

/// Whether durable connectivity evidence is close enough to expiry that an
/// otherwise stable observation must renew it.
///
/// This deliberately matches the probe selection margin. A probe that renews
/// the local source set must be publishable in the same generation; otherwise
/// low-frequency mobile/sensor schedules could accept a fresh local expiry
/// while letting the older cluster-KV evidence lapse before their next tick.
pub fn cluster_connectivity_record_should_refresh(
    expires_at: OffsetDateTime,
    now: OffsetDateTime,
) -> bool {
    expires_at <= now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS)
}

pub fn cluster_connectivity_stable_eq(
    left: &ClusterConnectivityRecord,
    right: &ClusterConnectivityRecord,
) -> bool {
    left.cluster_id == right.cluster_id
        && left.source_node_id == right.source_node_id
        && left.target_node_id == right.target_node_id
        && left.endpoint_id == right.endpoint_id
        && left.transport == right.transport
        && left.address == right.address
        && left.state == right.state
        && left.failure_reason == right.failure_reason
}

pub fn validate_cluster_connectivity_records_for_source(
    source_node_id: &NodeId,
    records: &[ClusterConnectivityRecord],
) -> Result<(), NetworkError> {
    if records.len() > MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS {
        return Err(NetworkError::ClusterConnectivityRecordLimit {
            source_node_id: source_node_id.clone(),
            observed_records: records.len(),
            max_records: MAX_CLUSTER_CONNECTIVITY_SOURCE_RECORDS,
        });
    }
    for record in records {
        if record.source_node_id != *source_node_id {
            return Err(NetworkError::ClusterConnectivitySourceMismatch {
                expected_source_node_id: source_node_id.clone(),
                record_source_node_id: record.source_node_id.clone(),
            });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterConnectivityState {
    Healthy,
    Degraded,
    Unreachable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterConnectivityFailureReason {
    IdentityMismatch,
    CommandAuthDenied,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<ClusterConnectivityFailureReason>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct ClusterConnectivityGraph {
    source_node_id: NodeId,
    records: ClusterConnectivityRecordIndex,
    records_by_address: ClusterConnectivityAddressIndex,
    healthy_records_by_pair: ClusterConnectivityPairIndex<(u64, ClusterEndpointId)>,
    reachable_records_by_pair: ClusterConnectivityPairIndex<(u8, u64, ClusterEndpointId)>,
    reachable_targets_by_source: ClusterConnectivityTargetIndex,
    healthy_targets_by_source: ClusterConnectivityTargetIndex,
}

#[derive(Clone, Debug, Default)]
struct ClusterConnectivityRecordIndex {
    records: BTreeMap<(NodeId, NodeId, ClusterEndpointId), ClusterConnectivityRecord>,
}

impl ClusterConnectivityRecordIndex {
    fn try_insert(
        &mut self,
        record: ClusterConnectivityRecord,
        operation: &str,
    ) -> Result<(), NetworkError> {
        let key = (
            try_clone_connectivity_id(&record.source_node_id, operation)?,
            try_clone_connectivity_id(&record.target_node_id, operation)?,
            try_clone_connectivity_id(&record.endpoint_id, operation)?,
        );
        self.records.insert(key, record);
        Ok(())
    }

    fn get(
        &self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        endpoint_id: &ClusterEndpointId,
    ) -> Option<&ClusterConnectivityRecord> {
        self.records.get(&(
            source_node_id.clone(),
            target_node_id.clone(),
            endpoint_id.clone(),
        ))
    }

    fn iter(&self) -> impl Iterator<Item = &ClusterConnectivityRecord> {
        self.records.values()
    }

    fn clear(&mut self) {
        self.records.clear();
    }
}

#[derive(Clone, Debug, Default)]
struct ClusterConnectivityAddressIndex {
    entries: BTreeMap<(NodeId, NodeId, String), ClusterEndpointId>,
}

impl ClusterConnectivityAddressIndex {
    fn try_insert(
        &mut self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        address_key: &str,
        endpoint_id: &ClusterEndpointId,
        operation: &str,
    ) -> Result<(), NetworkError> {
        self.entries
            .entry((
                try_clone_connectivity_id(source_node_id, operation)?,
                try_clone_connectivity_id(target_node_id, operation)?,
                try_clone_connectivity_string(address_key, operation)?,
            ))
            .or_insert(try_clone_connectivity_id(endpoint_id, operation)?);
        Ok(())
    }

    fn endpoint_id(
        &self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        address_key: &str,
    ) -> Option<&ClusterEndpointId> {
        self.entries.get(&(
            source_node_id.clone(),
            target_node_id.clone(),
            address_key.to_string(),
        ))
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Clone, Debug)]
struct ClusterConnectivityPairIndex<T> {
    entries: BTreeMap<(NodeId, NodeId), BTreeSet<T>>,
}

impl<T> Default for ClusterConnectivityPairIndex<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<T> ClusterConnectivityPairIndex<T>
where
    T: Ord,
{
    fn try_insert(
        &mut self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        value: T,
        operation: &str,
    ) -> Result<(), NetworkError> {
        self.entries
            .entry((
                try_clone_connectivity_id(source_node_id, operation)?,
                try_clone_connectivity_id(target_node_id, operation)?,
            ))
            .or_default()
            .insert(value);
        Ok(())
    }

    fn get(&self, source_node_id: &NodeId, target_node_id: &NodeId) -> Option<&BTreeSet<T>> {
        self.entries
            .get(&(source_node_id.clone(), target_node_id.clone()))
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

#[derive(Clone, Debug, Default)]
struct ClusterConnectivityTargetIndex {
    entries: BTreeMap<NodeId, BTreeSet<NodeId>>,
}

impl ClusterConnectivityTargetIndex {
    fn try_insert(
        &mut self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        operation: &str,
    ) -> Result<(), NetworkError> {
        self.entries
            .entry(try_clone_connectivity_id(source_node_id, operation)?)
            .or_default()
            .insert(try_clone_connectivity_id(target_node_id, operation)?);
        Ok(())
    }

    fn get(&self, source_node_id: &NodeId) -> Option<&BTreeSet<NodeId>> {
        self.entries.get(source_node_id)
    }

    fn try_cloned_targets(
        &self,
        source_node_id: &NodeId,
        operation: &str,
    ) -> Result<Vec<NodeId>, NetworkError> {
        let Some(targets) = self.get(source_node_id) else {
            return Ok(Vec::new());
        };
        let mut cloned = Vec::new();
        crate::object::try_reserve_vec(&mut cloned, targets.len(), operation)?;
        for target in targets {
            cloned.push(try_clone_connectivity_id(target, operation)?);
        }
        Ok(cloned)
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

pub trait ClusterConnectivityRecordQuery {
    fn read_kv_connectivity_records(
        &self,
        paths: &ClusterPaths,
    ) -> Result<Vec<ClusterConnectivityRecord>, NetworkError>;
}

/// Default [`ClusterConnectivityRecordQuery`]: asks the registered cluster
/// directory for supplemental records. In normal production wiring this is not
/// the live connectivity source; active records come from the network-plane
/// local store.
#[derive(Clone, Copy, Debug, Default)]
pub struct DirectoryConnectivityRecordQuery;

impl ClusterConnectivityRecordQuery for DirectoryConnectivityRecordQuery {
    fn read_kv_connectivity_records(
        &self,
        paths: &ClusterPaths,
    ) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
        crate::directory::cluster_directory_for(paths).connectivity_records()
    }
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
    pub active_keys: Vec<String>,
    pub entries: Vec<ClusterConnectivityProbeBatchEntry>,
    /// Number of currently-due entries omitted from this bounded batch.
    ///
    /// Callers should keep convergence scheduling active while this is non-zero
    /// instead of waiting for the normal role-specific probe interval.
    pub remaining_due: usize,
    pub next_cursor: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterConnectivityProbeObservation {
    Healthy,
    IdentityMismatch,
    Failed,
}

fn try_clone_connectivity_id(
    id: &sinomo_base::Id,
    operation: &str,
) -> Result<sinomo_base::Id, NetworkError> {
    crate::object::try_clone_id(id, operation)
}

fn try_clone_connectivity_string(value: &str, operation: &str) -> Result<String, NetworkError> {
    crate::object::try_clone_string(value, operation)
}

fn try_clone_connectivity_string_option(
    value: Option<&String>,
    operation: &str,
) -> Result<Option<String>, NetworkError> {
    value
        .map(|value| try_clone_connectivity_string(value, operation))
        .transpose()
}

fn try_clone_connectivity_endpoint(
    endpoint: &ClusterEndpointRecord,
    operation: &str,
) -> Result<ClusterEndpointRecord, NetworkError> {
    Ok(ClusterEndpointRecord {
        endpoint_id: try_clone_connectivity_id(&endpoint.endpoint_id, operation)?,
        node_id: try_clone_connectivity_id(&endpoint.node_id, operation)?,
        transport: endpoint.transport.clone(),
        address: try_clone_connectivity_string(&endpoint.address, operation)?,
        source: endpoint.source.clone(),
        privacy: endpoint.privacy.clone(),
        priority: endpoint.priority,
        last_observed_latency_ms: endpoint.last_observed_latency_ms,
    })
}

fn try_clone_connectivity_record(
    record: &ClusterConnectivityRecord,
    operation: &str,
) -> Result<ClusterConnectivityRecord, NetworkError> {
    Ok(ClusterConnectivityRecord {
        cluster_id: try_clone_connectivity_id(&record.cluster_id, operation)?,
        source_node_id: try_clone_connectivity_id(&record.source_node_id, operation)?,
        target_node_id: try_clone_connectivity_id(&record.target_node_id, operation)?,
        endpoint_id: try_clone_connectivity_id(&record.endpoint_id, operation)?,
        transport: record.transport.clone(),
        address: try_clone_connectivity_string(&record.address, operation)?,
        state: record.state.clone(),
        latency_ms: record.latency_ms,
        consecutive_failures: record.consecutive_failures,
        last_success_at: try_clone_connectivity_string_option(
            record.last_success_at.as_ref(),
            operation,
        )?,
        last_failure_at: try_clone_connectivity_string_option(
            record.last_failure_at.as_ref(),
            operation,
        )?,
        failure_reason: record.failure_reason.clone(),
        observed_at: record.observed_at,
        expires_at: record.expires_at,
    })
}

fn try_cluster_connectivity_probe_key_parts(
    source_node_id: &NodeId,
    target_node_id: &NodeId,
    endpoint_id: &ClusterEndpointId,
) -> Result<String, NetworkError> {
    let requested_bytes = source_node_id
        .as_str()
        .len()
        .saturating_add(target_node_id.as_str().len())
        .saturating_add(endpoint_id.as_str().len())
        .saturating_add(2);
    let mut key = String::new();
    key.try_reserve(requested_bytes)
        .map_err(|_| NetworkError::ResourceExhausted {
            operation: "build connectivity probe key".to_string(),
            requested_bytes,
        })?;
    key.push_str(source_node_id.as_str());
    key.push(':');
    key.push_str(target_node_id.as_str());
    key.push(':');
    key.push_str(endpoint_id.as_str());
    Ok(key)
}

fn try_cluster_connectivity_probe_key(
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> Result<String, NetworkError> {
    try_cluster_connectivity_probe_key_parts(
        source_node_id,
        &endpoint.node_id,
        &endpoint.endpoint_id,
    )
}

fn sorted_string_slice_contains(values: &[String], value: &str) -> bool {
    values
        .binary_search_by(|stored| stored.as_str().cmp(value))
        .is_ok()
}

fn try_insert_sorted_string(
    values: &mut Vec<String>,
    value: String,
    operation: &str,
) -> Result<bool, NetworkError> {
    match values.binary_search_by(|stored| stored.as_str().cmp(value.as_str())) {
        Ok(_) => Ok(false),
        Err(index) => {
            crate::object::try_reserve_vec(values, 1, operation)?;
            values.insert(index, value);
            Ok(true)
        }
    }
}

pub fn select_cluster_connectivity_probe_batch(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    connectivity_cursor: usize,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
    convergence_active: bool,
    force_probe: bool,
    now: OffsetDateTime,
) -> ClusterConnectivityProbeBatch {
    try_select_cluster_connectivity_probe_batch(
        paths,
        state,
        connectivity,
        connectivity_cursor,
        local_node_role,
        startup_settle_active,
        convergence_active,
        force_probe,
        now,
    )
    .unwrap_or_else(|_| ClusterConnectivityProbeBatch {
        active_keys: Vec::new(),
        entries: Vec::new(),
        remaining_due: 0,
        next_cursor: 0,
    })
}

pub fn try_select_cluster_connectivity_probe_batch(
    paths: &ClusterPaths,
    state: &ClusterBootstrapState,
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    connectivity_cursor: usize,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
    convergence_active: bool,
    force_probe: bool,
    now: OffsetDateTime,
) -> Result<ClusterConnectivityProbeBatch, NetworkError> {
    let refresh_deadline =
        now + time::Duration::seconds(CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
    let targets = ClusterEndpointSelector::new(paths, state)
        .try_select(ClusterEndpointSelectionPurpose::NetworkProbe)?;
    let mut active_keys = Vec::new();
    for endpoint in &targets {
        let key = try_cluster_connectivity_probe_key(&state.local_node_id, endpoint)?;
        try_insert_sorted_string(
            &mut active_keys,
            key,
            "collect active connectivity probe keys",
        )?;
    }

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
            let key = try_cluster_connectivity_probe_key_parts(
                &record.source_node_id,
                &record.target_node_id,
                &record.endpoint_id,
            );
            let key = key?;
            if sorted_string_slice_contains(&active_keys, &key) {
                connectivity.insert(key, ClusterConnectivityProbeState { record });
            }
        }
    }

    let graph = ClusterConnectivityGraph::try_read_for_local(paths, state)?;
    let mut probe_entries = Vec::new();
    for endpoint in &targets {
        let key = try_cluster_connectivity_probe_key(&state.local_node_id, endpoint)?;
        let previous = connectivity
            .get(&key)
            .map(|probe| {
                try_clone_connectivity_record(&probe.record, "clone previous connectivity record")
            })
            .transpose()?;
        let due = force_probe
            || previous.as_ref().is_none_or(|record| {
                cluster_connectivity_probe_due_with_graph_and_policy(
                    record,
                    endpoint,
                    now,
                    refresh_deadline,
                    local_node_role,
                    &graph,
                    &state.local_node_id,
                    startup_settle_active,
                    convergence_active,
                )
            });
        if !due {
            continue;
        }
        crate::object::try_reserve_vec(
            &mut probe_entries,
            1,
            "collect connectivity probe entries",
        )?;
        probe_entries.push(ClusterConnectivityProbeBatchEntry {
            key,
            endpoint: try_clone_connectivity_endpoint(
                endpoint,
                "clone connectivity probe endpoint",
            )?,
            previous,
        });
    }
    probe_entries.sort_by(|left, right| cluster_connectivity_probe_entry_cmp(left, right));
    let due_count = probe_entries.len();
    let probe_count = due_count.min(cluster_connectivity_probe_max_endpoints_per_tick_for_role(
        local_node_role,
    ));
    let (probe_entries, next_cursor) = select_rotating_cluster_connectivity_probe_entries(
        probe_entries,
        probe_count,
        connectivity_cursor,
    );
    let remaining_due = due_count.saturating_sub(probe_entries.len());

    Ok(ClusterConnectivityProbeBatch {
        active_keys,
        entries: probe_entries,
        remaining_due,
        next_cursor,
    })
}

fn select_rotating_cluster_connectivity_probe_entries(
    mut entries: Vec<ClusterConnectivityProbeBatchEntry>,
    count: usize,
    cursor: usize,
) -> (Vec<ClusterConnectivityProbeBatchEntry>, usize) {
    if entries.len() <= count {
        return (entries, 0);
    }
    if count == 0 {
        return (Vec::new(), cursor.min(entries.len()));
    }

    // Always keep the top-ranked entry so the nearest-expiring or brand-new
    // endpoint is not pushed out by fairness rotation. Rotate the remaining
    // bounded slots so endpoints behind stale/failing peers still get probed.
    let first = entries.remove(0);
    let rotating_len = entries.len();
    let start = cursor % rotating_len;
    let rotating_count = count.saturating_sub(1).min(rotating_len);
    let mut selected = Vec::with_capacity(count);
    selected.push(first);
    for offset in 0..rotating_count {
        let index = (start + offset) % rotating_len;
        selected.push(entries[index].clone());
    }
    let next_cursor = if rotating_len == 0 {
        0
    } else {
        (start + rotating_count) % rotating_len
    };
    (selected, next_cursor)
}

fn cluster_connectivity_probe_entry_cmp(
    left: &ClusterConnectivityProbeBatchEntry,
    right: &ClusterConnectivityProbeBatchEntry,
) -> std::cmp::Ordering {
    (
        cluster_connectivity_probe_expiry_rank(left.previous.as_ref()),
        cluster_connectivity_probe_state_rank(left.previous.as_ref()),
        &left.endpoint.node_id,
        left.endpoint.priority,
        left.endpoint.address.as_str(),
    )
        .cmp(&(
            cluster_connectivity_probe_expiry_rank(right.previous.as_ref()),
            cluster_connectivity_probe_state_rank(right.previous.as_ref()),
            &right.endpoint.node_id,
            right.endpoint.priority,
            right.endpoint.address.as_str(),
        ))
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

pub fn cluster_connectivity_record_for_probe_outcome(
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

pub fn cluster_connectivity_record_for_startup_probe_failure(
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

pub fn cluster_connectivity_record_for_identity_mismatch(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    timestamp: &str,
    now: OffsetDateTime,
) -> ClusterConnectivityRecord {
    let mut record = cluster_connectivity_record(
        state,
        endpoint,
        ClusterConnectivityState::Unreachable,
        None,
        CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
        None,
        Some(timestamp.to_string()),
        now,
        CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
    );
    record.failure_reason = Some(ClusterConnectivityFailureReason::IdentityMismatch);
    record
}

pub fn cluster_connectivity_record_for_command_auth_denied(
    state: &ClusterBootstrapState,
    endpoint: &ClusterEndpointRecord,
    timestamp: &str,
    now: OffsetDateTime,
) -> ClusterConnectivityRecord {
    let mut record = cluster_connectivity_record(
        state,
        endpoint,
        ClusterConnectivityState::Unreachable,
        None,
        CLUSTER_CONNECTIVITY_FAILURE_THRESHOLD,
        None,
        Some(timestamp.to_string()),
        now,
        CLUSTER_CONNECTIVITY_UNREACHABLE_TTL_SECONDS,
    );
    record.failure_reason = Some(ClusterConnectivityFailureReason::CommandAuthDenied);
    record
}

pub fn cluster_connectivity_record_for_hard_failure(
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
    active_keys: &[String],
    probed: Vec<(String, ClusterConnectivityRecord)>,
) -> Vec<ClusterConnectivityRecord> {
    try_apply_cluster_connectivity_probe_results(connectivity, active_keys, probed)
        .unwrap_or_default()
}

pub fn try_apply_cluster_connectivity_probe_results(
    connectivity: &mut BTreeMap<String, ClusterConnectivityProbeState>,
    active_keys: &[String],
    probed: Vec<(String, ClusterConnectivityRecord)>,
) -> Result<Vec<ClusterConnectivityRecord>, NetworkError> {
    for (key, record) in probed {
        connectivity.insert(key, ClusterConnectivityProbeState { record });
    }

    connectivity.retain(|key, _state| sorted_string_slice_contains(active_keys, key));
    let mut records = Vec::new();
    for state in connectivity.values() {
        crate::object::try_reserve_vec(
            &mut records,
            1,
            "collect connectivity probe result records",
        )?;
        records.push(try_clone_connectivity_record(
            &state.record,
            "collect connectivity probe result records",
        )?);
    }
    Ok(records)
}

pub fn cluster_connectivity_probe_due_with_startup_settle(
    record: &ClusterConnectivityRecord,
    now: OffsetDateTime,
    refresh_deadline: OffsetDateTime,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
) -> bool {
    cluster_connectivity_probe_due_with_policy(
        record,
        now,
        refresh_deadline,
        local_node_role,
        startup_settle_active,
        false,
    )
}

pub fn cluster_connectivity_probe_due_with_policy(
    record: &ClusterConnectivityRecord,
    now: OffsetDateTime,
    refresh_deadline: OffsetDateTime,
    local_node_role: &ClusterNodeRole,
    startup_settle_active: bool,
    convergence_active: bool,
) -> bool {
    let retry_interval = cluster_network_probe_interval_for_role(local_node_role);
    let retry_seconds = retry_interval.as_secs().min(i64::MAX as u64) as i64;
    let retry_due = record.observed_at + time::Duration::seconds(retry_seconds) <= now;
    if cluster_connectivity_failure_reason_is_identity_mismatch(record) {
        if startup_settle_active {
            return record.expires_at <= refresh_deadline;
        }
        return record.expires_at <= refresh_deadline || retry_due;
    }
    if startup_settle_active && record.state != ClusterConnectivityState::Healthy {
        return true;
    }
    if startup_settle_active && retry_due {
        return true;
    }
    if convergence_active && record.state == ClusterConnectivityState::Degraded {
        return true;
    }
    if record.expires_at <= refresh_deadline {
        return true;
    }
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
    cluster_connectivity_probe_due_with_graph_and_policy(
        record,
        endpoint,
        now,
        refresh_deadline,
        local_node_role,
        graph,
        local_node_id,
        startup_settle_active,
        false,
    )
}

pub fn cluster_connectivity_probe_due_with_graph_and_policy(
    record: &ClusterConnectivityRecord,
    endpoint: &ClusterEndpointRecord,
    now: OffsetDateTime,
    refresh_deadline: OffsetDateTime,
    local_node_role: &ClusterNodeRole,
    graph: &ClusterConnectivityGraph,
    local_node_id: &NodeId,
    startup_settle_active: bool,
    convergence_active: bool,
) -> bool {
    if record.transport != endpoint.transport || record.address != endpoint.address {
        return true;
    }
    if !cluster_connectivity_probe_due_with_policy(
        record,
        now,
        refresh_deadline,
        local_node_role,
        startup_settle_active,
        convergence_active,
    ) {
        return false;
    }
    if startup_settle_active && record.state != ClusterConnectivityState::Healthy {
        return true;
    }
    if convergence_active && record.state == ClusterConnectivityState::Degraded {
        return true;
    }
    if record.expires_at <= refresh_deadline {
        return true;
    }
    if record.state == ClusterConnectivityState::Healthy {
        return true;
    }
    if cluster_connectivity_failure_reason_is_identity_mismatch(record) {
        return true;
    }
    // When the target is already reachable through a currently healthy route graph,
    // a failed direct endpoint is not load-bearing. Let the record refresh near expiry
    // instead of burning a probe every role interval and churning local connectivity state.
    if crate::route::cluster_healthy_route_next_hop(graph, local_node_id, &endpoint.node_id)
        .is_some()
    {
        return false;
    }
    true
}

pub fn cluster_connectivity_failure_reason_is_identity_mismatch(
    record: &ClusterConnectivityRecord,
) -> bool {
    matches!(
        record.failure_reason,
        Some(ClusterConnectivityFailureReason::IdentityMismatch)
    )
}

#[cfg(any(test, feature = "test-support"))]
pub fn cluster_connectivity_probe_key(
    source_node_id: &NodeId,
    endpoint: &ClusterEndpointRecord,
) -> String {
    try_cluster_connectivity_probe_key(source_node_id, endpoint).unwrap_or_default()
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
        failure_reason: None,
        observed_at,
        expires_at: observed_at + time::Duration::seconds(ttl_seconds),
    }
}

impl ClusterConnectivityGraph {
    pub fn from_records(
        source_node_id: NodeId,
        records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    ) -> Self {
        let mut graph = Self::empty(source_node_id);
        if graph.try_replace_records(records).is_err() {
            graph.clear_indexes();
        }
        graph
    }

    pub fn try_from_records(
        source_node_id: NodeId,
        records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    ) -> Result<Self, NetworkError> {
        let mut graph = Self::empty(source_node_id);
        graph.try_replace_records(records)?;
        Ok(graph)
    }

    fn empty(source_node_id: NodeId) -> Self {
        Self {
            source_node_id,
            records: ClusterConnectivityRecordIndex::default(),
            records_by_address: ClusterConnectivityAddressIndex::default(),
            healthy_records_by_pair: ClusterConnectivityPairIndex::default(),
            reachable_records_by_pair: ClusterConnectivityPairIndex::default(),
            reachable_targets_by_source: ClusterConnectivityTargetIndex::default(),
            healthy_targets_by_source: ClusterConnectivityTargetIndex::default(),
        }
    }

    fn clear_indexes(&mut self) {
        self.records.clear();
        self.records_by_address.clear();
        self.healthy_records_by_pair.clear();
        self.reachable_records_by_pair.clear();
        self.reachable_targets_by_source.clear();
        self.healthy_targets_by_source.clear();
    }

    fn try_replace_records(
        &mut self,
        records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    ) -> Result<(), NetworkError> {
        let mut record_index = ClusterConnectivityRecordIndex::default();
        for record in records {
            record_index.try_insert(record, "index connectivity records")?;
        }

        let mut records_by_address = ClusterConnectivityAddressIndex::default();
        let mut healthy_records_by_pair = ClusterConnectivityPairIndex::default();
        let mut reachable_records_by_pair = ClusterConnectivityPairIndex::default();
        let mut reachable_targets_by_source = ClusterConnectivityTargetIndex::default();
        let mut healthy_targets_by_source = ClusterConnectivityTargetIndex::default();

        for record in record_index.iter() {
            for address_key in try_cluster_endpoint_address_lookup_keys(&record.address)? {
                records_by_address.try_insert(
                    &record.source_node_id,
                    &record.target_node_id,
                    &address_key,
                    &record.endpoint_id,
                    "index connectivity records by address",
                )?;
            }
            if record.target_node_id == record.source_node_id {
                continue;
            }
            if record.state != ClusterConnectivityState::Unreachable {
                reachable_targets_by_source.try_insert(
                    &record.source_node_id,
                    &record.target_node_id,
                    "index reachable connectivity targets",
                )?;
                reachable_records_by_pair.try_insert(
                    &record.source_node_id,
                    &record.target_node_id,
                    (
                        cluster_route_connectivity_quality_rank(Some(&record.state)),
                        record.latency_ms.unwrap_or(u64::MAX),
                        try_clone_connectivity_id(
                            &record.endpoint_id,
                            "index reachable connectivity records",
                        )?,
                    ),
                    "index reachable connectivity records",
                )?;
            }
            if record.state == ClusterConnectivityState::Healthy {
                healthy_targets_by_source.try_insert(
                    &record.source_node_id,
                    &record.target_node_id,
                    "index healthy connectivity targets",
                )?;
                healthy_records_by_pair.try_insert(
                    &record.source_node_id,
                    &record.target_node_id,
                    (
                        record.latency_ms.unwrap_or(u64::MAX),
                        try_clone_connectivity_id(
                            &record.endpoint_id,
                            "index healthy connectivity records",
                        )?,
                    ),
                    "index healthy connectivity records",
                )?;
            }
        }

        self.records = record_index;
        self.records_by_address = records_by_address;
        self.healthy_records_by_pair = healthy_records_by_pair;
        self.reachable_records_by_pair = reachable_records_by_pair;
        self.reachable_targets_by_source = reachable_targets_by_source;
        self.healthy_targets_by_source = healthy_targets_by_source;
        Ok(())
    }

    pub(crate) fn source_node_id(&self) -> &NodeId {
        &self.source_node_id
    }

    pub fn record_for_endpoint(
        &self,
        endpoint: &ClusterEndpointRecord,
    ) -> Option<&ClusterConnectivityRecord> {
        self.records
            .get(
                &self.source_node_id,
                &endpoint.node_id,
                &endpoint.endpoint_id,
            )
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
                            .endpoint_id(&self.source_node_id, &endpoint.node_id, &address_key)
                            .and_then(|endpoint_id| {
                                self.records.get(
                                    &self.source_node_id,
                                    &endpoint.node_id,
                                    endpoint_id,
                                )
                            })
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
    pub fn reachable_targets(&self, source: &NodeId) -> Vec<NodeId> {
        self.reachable_targets_by_source
            .try_cloned_targets(source, "clone reachable connectivity targets")
            .unwrap_or_default()
    }

    /// Node ids `source` has a CURRENTLY-HEALTHY connectivity record to (over any endpoint),
    /// excluding `source`. Unlike [`reachable_targets`], `Degraded` does NOT count. This is used
    /// by probe scheduling when deciding whether an alternate path is reliable enough to defer
    /// refreshing a failed direct endpoint.
    pub fn healthy_targets(&self, source: &NodeId) -> Vec<NodeId> {
        self.healthy_targets_by_source
            .try_cloned_targets(source, "clone healthy connectivity targets")
            .unwrap_or_default()
    }

    pub fn reachable_target_refs<'a>(
        &'a self,
        source: &NodeId,
    ) -> impl Iterator<Item = &'a NodeId> + 'a {
        self.reachable_targets_by_source
            .get(source)
            .into_iter()
            .flat_map(|targets| targets.iter())
    }

    pub fn healthy_target_refs<'a>(
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
            .get(source, target)
            .into_iter()
            .flat_map(|records| records.iter())
            .filter_map(|(_, endpoint_id)| self.records.get(source, target, endpoint_id))
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
            .get(source, target)
            .into_iter()
            .flat_map(|records| records.iter())
            .filter_map(|(_, _, endpoint_id)| self.records.get(source, target, endpoint_id))
            .map(cluster_endpoint_record_for_connectivity_record)
            .find(|endpoint| predicate(endpoint))
    }

    pub(crate) fn reachable_endpoints_matching(
        &self,
        source: &NodeId,
        target: &NodeId,
        mut predicate: impl FnMut(&ClusterEndpointRecord) -> bool,
    ) -> Vec<ClusterEndpointRecord> {
        let mut endpoints: Vec<ClusterEndpointRecord> = Vec::new();
        for endpoint in self
            .reachable_records_by_pair
            .get(source, target)
            .into_iter()
            .flat_map(|records| records.iter())
            .filter_map(|(_, _, endpoint_id)| self.records.get(source, target, endpoint_id))
            .map(cluster_endpoint_record_for_connectivity_record)
        {
            if !predicate(&endpoint) {
                continue;
            }
            if endpoints.iter().any(|retained| {
                retained.endpoint_id == endpoint.endpoint_id && retained.address == endpoint.address
            }) {
                continue;
            }
            endpoints.push(endpoint);
        }
        endpoints
    }

    pub(crate) fn earliest_expires_at(&self) -> Option<OffsetDateTime> {
        self.records.iter().map(|record| record.expires_at).min()
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
    try_cluster_endpoint_address_lookup_keys(address).unwrap_or_default()
}

fn try_cluster_endpoint_address_lookup_keys(address: &str) -> Result<Vec<String>, NetworkError> {
    let mut keys = Vec::new();
    crate::object::try_reserve_vec(&mut keys, 1, "build endpoint address lookup keys")?;
    keys.push(try_prefixed_connectivity_string(
        "raw:",
        address,
        "build endpoint address lookup keys",
    )?);
    if let Ok(socket_addr) = address.parse::<SocketAddr>() {
        crate::object::try_reserve_vec(&mut keys, 1, "build endpoint address lookup keys")?;
        keys.push(try_socket_address_lookup_key(
            socket_addr,
            "build endpoint address lookup keys",
        )?);
    }
    Ok(keys)
}

fn try_prefixed_connectivity_string(
    prefix: &str,
    value: &str,
    operation: &str,
) -> Result<String, NetworkError> {
    let requested_bytes = prefix.len().saturating_add(value.len());
    let mut output = String::new();
    output
        .try_reserve(requested_bytes)
        .map_err(|_| NetworkError::ResourceExhausted {
            operation: operation.to_string(),
            requested_bytes,
        })?;
    output.push_str(prefix);
    output.push_str(value);
    Ok(output)
}

fn try_socket_address_lookup_key(
    socket_addr: SocketAddr,
    operation: &str,
) -> Result<String, NetworkError> {
    use std::fmt::Write as _;

    const SOCKET_ADDR_DISPLAY_MAX_BYTES: usize = 64;
    let requested_bytes = "socket:"
        .len()
        .saturating_add(SOCKET_ADDR_DISPLAY_MAX_BYTES);
    let mut output = String::new();
    output
        .try_reserve(requested_bytes)
        .map_err(|_| NetworkError::ResourceExhausted {
            operation: operation.to_string(),
            requested_bytes,
        })?;
    output.push_str("socket:");
    write!(&mut output, "{socket_addr}").map_err(|_| NetworkError::ResourceExhausted {
        operation: operation.to_string(),
        requested_bytes,
    })?;
    Ok(output)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_record(
        state: ClusterConnectivityState,
        now: OffsetDateTime,
    ) -> ClusterConnectivityRecord {
        ClusterConnectivityRecord {
            cluster_id: ClusterId::from_legacy_named("cluster_connectivity_convergence")
                .expect("cluster id"),
            source_node_id: NodeId::from_legacy_named("node_connectivity_convergence_source")
                .expect("source node id"),
            target_node_id: NodeId::from_legacy_named("node_connectivity_convergence_target")
                .expect("target node id"),
            endpoint_id: ClusterEndpointId::from_legacy_named("endpoint_connectivity_convergence")
                .expect("endpoint id"),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.10:21353".to_string(),
            state,
            latency_ms: None,
            consecutive_failures: 1,
            last_success_at: None,
            last_failure_at: Some("failed".to_string()),
            failure_reason: None,
            observed_at: now,
            expires_at: now + time::Duration::minutes(10),
        }
    }

    #[test]
    fn convergence_only_accelerates_degraded_records() {
        let now = OffsetDateTime::now_utc();
        let refresh_deadline = now + time::Duration::seconds(30);

        for state in [
            ClusterConnectivityState::Healthy,
            ClusterConnectivityState::Unreachable,
        ] {
            let record = probe_record(state, now);
            assert!(!cluster_connectivity_probe_due_with_policy(
                &record,
                now,
                refresh_deadline,
                &ClusterNodeRole::Workstation,
                false,
                true,
            ));
        }

        let degraded = probe_record(ClusterConnectivityState::Degraded, now);
        assert!(!cluster_connectivity_probe_due_with_policy(
            &degraded,
            now,
            refresh_deadline,
            &ClusterNodeRole::Workstation,
            false,
            false,
        ));
        assert!(cluster_connectivity_probe_due_with_policy(
            &degraded,
            now,
            refresh_deadline,
            &ClusterNodeRole::Workstation,
            false,
            true,
        ));
    }

    #[test]
    fn convergence_degraded_probe_bypasses_alternate_route_suppression() {
        let now = OffsetDateTime::now_utc();
        let observed_at = now - time::Duration::minutes(2);
        let refresh_deadline = now + time::Duration::seconds(30);
        let cluster_id =
            ClusterId::from_legacy_named("cluster_connectivity_alternate").expect("cluster id");
        let local =
            NodeId::from_legacy_named("node_connectivity_alternate_local").expect("local node id");
        let bridge = NodeId::from_legacy_named("node_connectivity_alternate_bridge")
            .expect("bridge node id");
        let target = NodeId::from_legacy_named("node_connectivity_alternate_target")
            .expect("target node id");

        let healthy_edge =
            |source: &NodeId, target: &NodeId, suffix: &str| ClusterConnectivityRecord {
                cluster_id: cluster_id.clone(),
                source_node_id: source.clone(),
                target_node_id: target.clone(),
                endpoint_id: ClusterEndpointId::from_legacy_named(format!(
                    "endpoint_connectivity_alternate_{suffix}"
                ))
                .expect("endpoint id"),
                transport: ClusterTransportKind::LanTcp,
                address: format!("192.0.2.{}:21353", suffix.len()),
                state: ClusterConnectivityState::Healthy,
                latency_ms: Some(1),
                consecutive_failures: 0,
                last_success_at: Some("ok".to_string()),
                last_failure_at: None,
                failure_reason: None,
                observed_at: now,
                expires_at: now + time::Duration::minutes(10),
            };
        let graph = ClusterConnectivityGraph::from_records(
            local.clone(),
            [
                healthy_edge(&local, &bridge, "local_bridge"),
                healthy_edge(&bridge, &target, "bridge_target"),
            ],
        );
        let endpoint = ClusterEndpointRecord {
            endpoint_id: ClusterEndpointId::from_legacy_named(
                "endpoint_connectivity_alternate_direct",
            )
            .expect("endpoint id"),
            node_id: target.clone(),
            transport: ClusterTransportKind::LanTcp,
            address: "192.0.2.20:21353".to_string(),
            source: ClusterEndpointSource::Observed,
            privacy: ClusterEndpointPrivacy::Lan,
            priority: 0,
            last_observed_latency_ms: None,
        };
        let mut degraded = probe_record(ClusterConnectivityState::Degraded, observed_at);
        degraded.cluster_id = cluster_id;
        degraded.source_node_id = local.clone();
        degraded.target_node_id = target;
        degraded.endpoint_id = endpoint.endpoint_id.clone();
        degraded.address.clone_from(&endpoint.address);

        assert!(!cluster_connectivity_probe_due_with_graph_and_policy(
            &degraded,
            &endpoint,
            now,
            refresh_deadline,
            &ClusterNodeRole::Workstation,
            &graph,
            &local,
            false,
            false,
        ));
        assert!(cluster_connectivity_probe_due_with_graph_and_policy(
            &degraded,
            &endpoint,
            now,
            refresh_deadline,
            &ClusterNodeRole::Workstation,
            &graph,
            &local,
            false,
            true,
        ));
    }
}
