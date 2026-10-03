//! Cached storage and network evidence used by DFS placement and source ranking.
//!
//! The contract deliberately keeps physical content locations below node
//! identity. A node with several writable disks still contributes exactly one
//! DFS replica and one candidate to a placement plan.

use crate::{
    DfsDistributedError, DfsNodeAdvertisement, DfsReplicaCandidate, DfsReplicaPlacementDecision,
    DfsReplicaPlacementExplanation, DfsReplicaPlacementPlan, DfsReplicaPlacementRequest,
};
use serde::{Deserialize, Serialize};
use sinomo_core::node::NodeId;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use time::OffsetDateTime;

pub use sinomo_api::dfs::placement_environment::DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION;
pub const DFS_NODE_HEALTH_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
/// A bounded safety audit for capacity changes made outside Sinomo. Normal
/// placement updates are driven by committed control-plane generations; idle
/// nodes must not rescan their filesystem every few seconds.
pub const DEFAULT_DFS_STORAGE_SAMPLE_INTERVAL: Duration = Duration::from_secs(5 * 60);
pub const DEFAULT_DFS_STORAGE_OBSERVATION_TTL: Duration = Duration::from_secs(15 * 60);
pub const DEFAULT_DFS_STORAGE_RESERVE_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const DFS_STORAGE_RECOVERY_MARGIN_BYTES: u64 = 1024 * 1024 * 1024;
const DFS_ADVERTISEMENT_TRANSITION_SAMPLES: u8 = 2;

pub use sinomo_api::dfs::placement_environment::DfsHealthState;

pub use sinomo_api::dfs::placement_environment::DfsObservationFreshness;

pub use sinomo_api::dfs::placement_environment::DfsNetworkMetering;

pub use sinomo_api::dfs::placement_environment::DfsResourcePressure;

pub use sinomo_api::dfs::placement_environment::DfsPlacementPathState;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "kebab-case")]
pub enum DfsContentLocationLifecycle {
    #[default]
    Active,
    Draining,
    Detached,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentLocationConfig {
    pub location_id: String,
    pub label: String,
    pub root: PathBuf,
    #[serde(default)]
    pub lifecycle: DfsContentLocationLifecycle,
    #[serde(default = "default_storage_reserve_bytes")]
    pub reserved_free_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_dfs_bytes: Option<u64>,
    #[serde(default = "default_allocation_weight")]
    pub allocation_weight: u32,
    #[serde(default)]
    pub configuration_generation: u64,
}

fn default_storage_reserve_bytes() -> u64 {
    DEFAULT_DFS_STORAGE_RESERVE_BYTES
}

fn default_allocation_weight() -> u32 {
    1
}

impl DfsContentLocationConfig {
    pub fn legacy(root: impl Into<PathBuf>) -> Self {
        Self {
            location_id: "legacy-default".to_string(),
            label: "Default content store".to_string(),
            root: root.into(),
            lifecycle: DfsContentLocationLifecycle::Active,
            reserved_free_bytes: DEFAULT_DFS_STORAGE_RESERVE_BYTES,
            max_dfs_bytes: None,
            allocation_weight: 1,
            configuration_generation: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsContentLocationObservation {
    pub location_id: String,
    pub lifecycle: DfsContentLocationLifecycle,
    pub health: DfsHealthState,
    pub freshness: DfsObservationFreshness,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reason_codes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_bytes: Option<u64>,
    pub reserved_free_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usable_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_dfs_bytes: Option<u64>,
    pub allocation_weight: u32,
    pub writable: bool,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_success_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DfsNodeHealthSnapshot {
    pub schema_version: u32,
    pub node_id: NodeId,
    pub sequence: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub observed_at: OffsetDateTime,
    pub summary: DfsHealthState,
    pub freshness: DfsObservationFreshness,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_locations: Vec<DfsContentLocationObservation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issue_codes: Vec<String>,
    #[serde(default)]
    pub network_metering: DfsNetworkMetering,
    #[serde(default)]
    pub resource_pressure: DfsResourcePressure,
}

pub use sinomo_api::dfs::placement_environment::DfsPlacementNodeObservation;

pub use sinomo_api::dfs::placement_environment::DfsPlacementPathObservation;

pub use sinomo_api::dfs::placement_environment::DfsPlacementSnapshot;

#[derive(Clone, Debug, Default)]
pub struct DfsPlacementSnapshotCache {
    inner: Arc<RwLock<DfsPlacementSnapshotCacheState>>,
}

#[derive(Debug, Default)]
struct DfsPlacementSnapshotCacheState {
    snapshot: Option<Arc<DfsPlacementSnapshot>>,
    semantic_fingerprint: Option<blake3::Hash>,
    next_generation: u64,
    passive_throughput: BTreeMap<(NodeId, NodeId), u64>,
}

impl DfsPlacementSnapshotCache {
    pub fn snapshot(&self) -> Option<Arc<DfsPlacementSnapshot>> {
        self.inner
            .read()
            .ok()
            .and_then(|state| state.snapshot.clone())
    }

    /// Withdraw eligibility after an observation discontinuity. Existing
    /// consumers sharing this cache see unknown until a real projection rebuild.
    pub(crate) fn withdraw(&self) {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(|error| error.into_inner());
        state.snapshot = None;
        state.semantic_fingerprint = None;
    }

    pub fn publish(&self, mut snapshot: DfsPlacementSnapshot) -> Arc<DfsPlacementSnapshot> {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for path in &mut snapshot.paths {
            if let Some(observed) = state
                .passive_throughput
                .get(&(path.source_node_id.clone(), path.target_node_id.clone()))
            {
                path.delivered_bytes_per_second = Some(*observed);
            }
        }
        let now = OffsetDateTime::now_utc();
        for ((source_node_id, target_node_id), observed) in &state.passive_throughput {
            if snapshot.paths.iter().any(|path| {
                &path.source_node_id == source_node_id && &path.target_node_id == target_node_id
            }) {
                continue;
            }
            snapshot.paths.push(DfsPlacementPathObservation {
                source_node_id: source_node_id.clone(),
                target_node_id: target_node_id.clone(),
                state: DfsPlacementPathState::Unknown,
                latency_micros: None,
                delivered_bytes_per_second: Some(*observed),
                metering: DfsNetworkMetering::Unknown,
                observed_at: now,
                expires_at: now + time::Duration::minutes(15),
            });
        }
        let fingerprint = snapshot.semantic_fingerprint();
        if state.semantic_fingerprint.as_ref() == Some(&fingerprint)
            && let Some(current) = state.snapshot.as_ref()
        {
            snapshot.generation = current.generation;
        } else {
            state.next_generation = state.next_generation.saturating_add(1).max(1);
            snapshot.generation = state.next_generation;
            state.semantic_fingerprint = Some(fingerprint);
        }
        let snapshot = Arc::new(snapshot);
        state.snapshot = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Record passive delivery throughput with a bounded EWMA. The placement
    /// generation advances only when the power-of-two bandwidth bucket changes,
    /// preventing normal transfer noise from continuously reshuffling objects.
    pub fn observe_transfer(
        &self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
        bytes: u64,
        elapsed: Duration,
    ) {
        if bytes == 0 || elapsed.is_zero() {
            return;
        }
        let nanos = elapsed.as_nanos().max(1);
        let measured = u64::try_from((u128::from(bytes) * 1_000_000_000) / nanos)
            .unwrap_or(u64::MAX)
            .max(1);
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (source_node_id.clone(), target_node_id.clone());
        let previous = state.passive_throughput.get(&key).copied();
        let smoothed = previous
            .map(|previous| previous.saturating_mul(3).saturating_add(measured) / 4)
            .unwrap_or(measured);
        state.passive_throughput.insert(key.clone(), smoothed);
        if previous.map(throughput_bucket) == Some(throughput_bucket(smoothed)) {
            return;
        }
        let Some(current) = state.snapshot.as_ref().map(|snapshot| (**snapshot).clone()) else {
            return;
        };
        let mut updated = current;
        let now = OffsetDateTime::now_utc();
        if let Some(path) = updated
            .paths
            .iter_mut()
            .find(|path| path.source_node_id == key.0 && path.target_node_id == key.1)
        {
            path.delivered_bytes_per_second = Some(smoothed);
            path.observed_at = now;
        } else {
            updated.paths.push(DfsPlacementPathObservation {
                source_node_id: key.0,
                target_node_id: key.1,
                state: DfsPlacementPathState::Unknown,
                latency_micros: None,
                delivered_bytes_per_second: Some(smoothed),
                metering: DfsNetworkMetering::Unknown,
                observed_at: now,
                expires_at: now + time::Duration::minutes(15),
            });
        }
        state.next_generation = state.next_generation.saturating_add(1).max(1);
        updated.generation = state.next_generation;
        state.semantic_fingerprint = Some(updated.semantic_fingerprint());
        state.snapshot = Some(Arc::new(updated));
    }

    pub fn rank_sources(
        &self,
        local_node_id: &NodeId,
        source_node_ids: impl IntoIterator<Item = NodeId>,
    ) -> Vec<NodeId> {
        let mut sources = source_node_ids.into_iter().collect::<Vec<_>>();
        let Some(snapshot) = self.snapshot() else {
            sources.sort();
            sources.dedup();
            return sources;
        };
        sources.sort_by(|left, right| {
            let now = OffsetDateTime::now_utc();
            let left_local = left == local_node_id;
            let right_local = right == local_node_id;
            right_local
                .cmp(&left_local)
                .then_with(|| {
                    health_rank(snapshot.node(left), now)
                        .cmp(&health_rank(snapshot.node(right), now))
                })
                .then_with(|| {
                    compare_path_safety(
                        snapshot.path(left, local_node_id),
                        snapshot.path(right, local_node_id),
                        now,
                    )
                })
                .then_with(|| {
                    resource_pressure_rank(snapshot.node(left), now)
                        .cmp(&resource_pressure_rank(snapshot.node(right), now))
                })
                .then_with(|| {
                    compare_path_performance(
                        snapshot.path(left, local_node_id),
                        snapshot.path(right, local_node_id),
                        now,
                    )
                })
                .then_with(|| left.cmp(right))
        });
        sources.dedup();
        sources
    }
}

use sinomo_api::dfs::placement_environment::throughput_bucket;

#[derive(Debug)]
pub struct DfsDeviceHealthMonitor {
    node_id: NodeId,
    locations: Vec<DfsContentLocationConfig>,
    sequence: u64,
    last_sampled_at: Option<OffsetDateTime>,
    snapshot: Arc<RwLock<Arc<DfsNodeHealthSnapshot>>>,
    advertised: Option<DfsNodeAdvertisement>,
    pending_advertisement: Option<(DfsNodeAdvertisement, u8)>,
    last_advertisement_evaluated_sequence: Option<u64>,
}

impl DfsDeviceHealthMonitor {
    pub fn new(node_id: NodeId, locations: Vec<DfsContentLocationConfig>) -> Self {
        let now = OffsetDateTime::now_utc();
        let initial = Arc::new(DfsNodeHealthSnapshot {
            schema_version: DFS_NODE_HEALTH_SNAPSHOT_SCHEMA_VERSION,
            node_id: node_id.clone(),
            sequence: 0,
            observed_at: now,
            summary: DfsHealthState::Unknown,
            freshness: DfsObservationFreshness::NeverSampled,
            content_locations: Vec::new(),
            issue_codes: vec!["never-sampled".to_string()],
            network_metering: DfsNetworkMetering::Unknown,
            resource_pressure: DfsResourcePressure::Normal,
        });
        Self {
            node_id,
            locations,
            sequence: 0,
            last_sampled_at: None,
            snapshot: Arc::new(RwLock::new(initial)),
            advertised: None,
            pending_advertisement: None,
            last_advertisement_evaluated_sequence: None,
        }
    }

    pub fn cached_snapshot(&self) -> Arc<DfsNodeHealthSnapshot> {
        self.snapshot
            .read()
            .map(|snapshot| Arc::clone(&snapshot))
            .unwrap_or_else(|poisoned| Arc::clone(&poisoned.into_inner()))
    }

    pub fn sample_if_due(
        &mut self,
        now: OffsetDateTime,
        metering: DfsNetworkMetering,
        resource_pressure: DfsResourcePressure,
    ) -> Arc<DfsNodeHealthSnapshot> {
        if self.last_sampled_at.is_some_and(|last| {
            now - last
                < time::Duration::try_from(DEFAULT_DFS_STORAGE_SAMPLE_INTERVAL)
                    .unwrap_or(time::Duration::minutes(5))
        }) {
            return self.update_advisory_evidence(metering, resource_pressure);
        }
        self.sample_with_resources(now, metering, resource_pressure)
    }

    /// Update callback-owned placement evidence without touching storage.
    /// Physical capacity has a separate explicit dirty edge in daemon hosts;
    /// lease renewal and resource transitions consume this cached projection.
    pub fn update_advisory_evidence(
        &mut self,
        metering: DfsNetworkMetering,
        resource_pressure: DfsResourcePressure,
    ) -> Arc<DfsNodeHealthSnapshot> {
        let current = self.cached_snapshot();
        if current.network_metering == metering && current.resource_pressure == resource_pressure {
            return current;
        }
        self.sequence = self.sequence.max(current.sequence).saturating_add(1);
        let mut updated = (*current).clone();
        updated.sequence = self.sequence;
        updated.network_metering = metering;
        updated.resource_pressure = resource_pressure;
        let updated = Arc::new(updated);
        *self
            .snapshot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::clone(&updated);
        updated
    }

    pub fn sample(
        &mut self,
        now: OffsetDateTime,
        metering: DfsNetworkMetering,
    ) -> Arc<DfsNodeHealthSnapshot> {
        self.sample_with_resources(now, metering, DfsResourcePressure::Normal)
    }

    pub fn sample_with_resources(
        &mut self,
        now: OffsetDateTime,
        metering: DfsNetworkMetering,
        resource_pressure: DfsResourcePressure,
    ) -> Arc<DfsNodeHealthSnapshot> {
        let previous = self.cached_snapshot();
        let previous_by_id = previous
            .content_locations
            .iter()
            .map(|location| (location.location_id.as_str(), location))
            .collect::<BTreeMap<_, _>>();
        let observations = self
            .locations
            .iter()
            .map(|location| {
                sample_content_location(
                    location,
                    previous_by_id.get(location.location_id.as_str()).copied(),
                    now,
                )
            })
            .collect::<Vec<_>>();
        let summary = aggregate_health(&observations);
        let issue_codes = observations
            .iter()
            .flat_map(|location| location.reason_codes.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        self.sequence = self.sequence.saturating_add(1);
        self.last_sampled_at = Some(now);
        let snapshot = Arc::new(DfsNodeHealthSnapshot {
            schema_version: DFS_NODE_HEALTH_SNAPSHOT_SCHEMA_VERSION,
            node_id: self.node_id.clone(),
            sequence: self.sequence,
            observed_at: now,
            summary,
            freshness: DfsObservationFreshness::Fresh,
            content_locations: observations,
            issue_codes,
            network_metering: metering,
            resource_pressure,
        });
        *self
            .snapshot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::clone(&snapshot);
        snapshot
    }

    pub fn aggregate_advertisement(
        &mut self,
        zone: Option<String>,
        now: OffsetDateTime,
    ) -> DfsNodeAdvertisement {
        let snapshot = self.cached_snapshot();
        let writable = snapshot
            .content_locations
            .iter()
            .filter(|location| location.lifecycle == DfsContentLocationLifecycle::Active)
            .filter(|location| location.writable && location.health != DfsHealthState::Critical)
            .collect::<Vec<_>>();
        let usable_capacity_bytes = writable
            .iter()
            .filter_map(|location| location.usable_bytes)
            .fold(0_u64, u64::saturating_add);
        let reserved_free_bytes = snapshot
            .content_locations
            .iter()
            .map(|location| location.reserved_free_bytes)
            .fold(0_u64, u64::saturating_add);
        let configured_weight = writable
            .iter()
            .map(|location| location.allocation_weight.max(1))
            .fold(0_u32, u32::saturating_add)
            .max(1);
        let capacity_bucket = ((usable_capacity_bytes / (64 * 1024 * 1024 * 1024)) + 1)
            .min(u64::from(u32::MAX)) as u32;
        let mut desired = DfsNodeAdvertisement::content(self.node_id.clone());
        desired.accepts_content = !writable.is_empty();
        desired.capacity_weight = capacity_bucket
            .min(configured_weight.saturating_mul(16))
            .max(1);
        desired.zone = zone;
        desired.storage_health = snapshot.summary;
        desired.storage_freshness = snapshot.freshness;
        desired.usable_capacity_bytes = Some(usable_capacity_bytes);
        desired.reserved_free_bytes = Some(reserved_free_bytes);
        desired.content_location_count = snapshot
            .content_locations
            .len()
            .try_into()
            .unwrap_or(u32::MAX);
        desired.network_metering = snapshot.network_metering;
        desired.resource_pressure = snapshot.resource_pressure;
        desired.advertisement_generation = snapshot.sequence;
        desired.updated_at = now;
        desired.expires_at = Some(
            snapshot.observed_at
                + time::Duration::try_from(DEFAULT_DFS_STORAGE_OBSERVATION_TTL)
                    .unwrap_or(time::Duration::minutes(15)),
        );

        if self.last_advertisement_evaluated_sequence == Some(snapshot.sequence)
            && let Some(current) = self.advertised.clone()
        {
            let refreshed = refresh_advertisement_lease(current, &desired);
            self.advertised = Some(refreshed.clone());
            return refreshed;
        }
        self.last_advertisement_evaluated_sequence = Some(snapshot.sequence);

        let Some(current) = self.advertised.clone() else {
            self.advertised = Some(desired.clone());
            return desired;
        };
        if advertisement_semantics_equal(&current, &desired) {
            self.pending_advertisement = None;
            let refreshed = refresh_advertisement_lease(current, &desired);
            self.advertised = Some(refreshed.clone());
            return refreshed;
        }
        // Unsafe transitions stop admission immediately. Recovery and weight
        // changes require two consecutive samples with the same quantized state.
        if current.accepts_content && !desired.accepts_content {
            self.pending_advertisement = None;
            self.advertised = Some(desired.clone());
            return desired;
        }
        let consecutive = self
            .pending_advertisement
            .as_ref()
            .filter(|(pending, _)| advertisement_semantics_equal(pending, &desired))
            .map(|(_, count)| count.saturating_add(1))
            .unwrap_or(1);
        if consecutive >= DFS_ADVERTISEMENT_TRANSITION_SAMPLES {
            self.pending_advertisement = None;
            self.advertised = Some(desired.clone());
            desired
        } else {
            self.pending_advertisement = Some((desired, consecutive));
            let refreshed = refresh_advertisement_lease(
                current,
                &self
                    .pending_advertisement
                    .as_ref()
                    .expect("pending advertisement was just installed")
                    .0,
            );
            self.advertised = Some(refreshed.clone());
            refreshed
        }
    }
}

fn refresh_advertisement_lease(
    mut current: DfsNodeAdvertisement,
    desired: &DfsNodeAdvertisement,
) -> DfsNodeAdvertisement {
    // Keep decision-affecting state behind the transition hysteresis while
    // still publishing the newest bounded measurements and provenance.
    // Metering and resource pressure come from exact host callbacks and are
    // advisory ranking evidence, not storage-health samples. Applying them
    // immediately lets a restricted mobile host park its sampler without
    // requiring a synthetic second observation.
    current.usable_capacity_bytes = desired.usable_capacity_bytes;
    current.reserved_free_bytes = desired.reserved_free_bytes;
    current.network_metering = desired.network_metering;
    current.resource_pressure = desired.resource_pressure;
    current.advertisement_generation = desired.advertisement_generation;
    current.updated_at = desired.updated_at;
    current.expires_at = desired.expires_at;
    current
}

fn sample_content_location(
    config: &DfsContentLocationConfig,
    previous: Option<&DfsContentLocationObservation>,
    now: OffsetDateTime,
) -> DfsContentLocationObservation {
    let mut observation = DfsContentLocationObservation {
        location_id: config.location_id.clone(),
        lifecycle: config.lifecycle,
        health: DfsHealthState::Unknown,
        freshness: DfsObservationFreshness::Fresh,
        reason_codes: Vec::new(),
        total_bytes: None,
        available_bytes: None,
        reserved_free_bytes: config.reserved_free_bytes,
        usable_bytes: None,
        max_dfs_bytes: config.max_dfs_bytes,
        allocation_weight: config.allocation_weight.max(1),
        writable: false,
        last_success_at: None,
        observed_at: now,
    };
    if config.lifecycle == DfsContentLocationLifecycle::Detached {
        observation.health = DfsHealthState::Unknown;
        observation.freshness = DfsObservationFreshness::Unsupported;
        observation
            .reason_codes
            .push("location-detached".to_string());
        return observation;
    }
    let metadata = match std::fs::metadata(&config.root) {
        Ok(metadata) if metadata.is_dir() => metadata,
        Ok(_) => {
            observation.health = DfsHealthState::Critical;
            observation
                .reason_codes
                .push("location-not-directory".to_string());
            return observation;
        }
        Err(_) => {
            observation.health = DfsHealthState::Critical;
            observation
                .reason_codes
                .push("location-missing".to_string());
            return observation;
        }
    };
    let (total, available) = match (
        fs2::total_space(&config.root),
        fs2::available_space(&config.root),
    ) {
        (Ok(total), Ok(available)) => (total, available),
        _ => {
            observation
                .reason_codes
                .push("capacity-collector-error".to_string());
            return observation;
        }
    };
    observation.total_bytes = Some(total);
    observation.available_bytes = Some(available);
    observation.usable_bytes = Some(available.saturating_sub(config.reserved_free_bytes));
    observation.last_success_at = Some(now);
    if metadata.permissions().readonly() {
        observation.health = DfsHealthState::Critical;
        observation
            .reason_codes
            .push("location-read-only".to_string());
        return observation;
    }
    if config.lifecycle == DfsContentLocationLifecycle::Draining {
        observation.health = DfsHealthState::Degraded;
        observation
            .reason_codes
            .push("location-draining".to_string());
        return observation;
    }
    let recovery_floor = config
        .reserved_free_bytes
        .saturating_add(DFS_STORAGE_RECOVERY_MARGIN_BYTES);
    let was_reserve_exhausted = previous.is_some_and(|previous| {
        previous.health == DfsHealthState::Critical
            && previous
                .reason_codes
                .iter()
                .any(|code| code == "reserve-exhausted")
    });
    if available <= config.reserved_free_bytes
        || (was_reserve_exhausted && available < recovery_floor)
    {
        observation.health = DfsHealthState::Critical;
        observation
            .reason_codes
            .push("reserve-exhausted".to_string());
    } else if available < recovery_floor {
        observation.health = DfsHealthState::Degraded;
        observation.reason_codes.push("reserve-near".to_string());
        observation.writable = true;
    } else {
        observation.health = DfsHealthState::Healthy;
        observation.writable = true;
    }
    observation
}

fn aggregate_health(locations: &[DfsContentLocationObservation]) -> DfsHealthState {
    if locations.is_empty() {
        return DfsHealthState::Unknown;
    }
    if locations
        .iter()
        .any(|location| location.writable && location.health == DfsHealthState::Healthy)
    {
        DfsHealthState::Healthy
    } else if locations.iter().any(|location| location.writable) {
        DfsHealthState::Degraded
    } else if locations
        .iter()
        .any(|location| location.health == DfsHealthState::Critical)
    {
        DfsHealthState::Critical
    } else {
        DfsHealthState::Unknown
    }
}

fn advertisement_semantics_equal(
    left: &DfsNodeAdvertisement,
    right: &DfsNodeAdvertisement,
) -> bool {
    left.node_id == right.node_id
        && left.capacity_weight == right.capacity_weight
        && left.accepts_content == right.accepts_content
        && left.zone == right.zone
        && left.storage_health == right.storage_health
        && left.storage_freshness == right.storage_freshness
        && left.network_metering == right.network_metering
        && left.resource_pressure == right.resource_pressure
        && left.content_location_count == right.content_location_count
}

#[derive(Clone, Copy)]
struct CandidateEvidence<'a> {
    candidate: &'a DfsReplicaCandidate,
    node: Option<&'a DfsPlacementNodeObservation>,
    path: Option<&'a DfsPlacementPathObservation>,
    current: bool,
    pinned: bool,
}

pub fn explain_replicas_with_placement_snapshot(
    request: &DfsReplicaPlacementRequest,
    snapshot: &DfsPlacementSnapshot,
    current_replica_node_ids: &[NodeId],
    pinned_replica_node_ids: &[NodeId],
) -> Result<DfsReplicaPlacementExplanation, DfsDistributedError> {
    request.policy.validate()?;
    crate::distributed::validate_candidate_set(&request.candidates, &request.policy)?;
    let now = OffsetDateTime::now_utc();
    let current = current_replica_node_ids.iter().collect::<BTreeSet<_>>();
    let pinned = pinned_replica_node_ids.iter().collect::<BTreeSet<_>>();
    let mut eligible = request
        .candidates
        .iter()
        .filter(|candidate| {
            !request
                .policy
                .forbidden_node_ids
                .contains(&candidate.node_id)
        })
        .map(|candidate| CandidateEvidence {
            candidate,
            node: snapshot.node(&candidate.node_id),
            path: placement_candidate_path(request, snapshot, candidate),
            current: current.contains(&candidate.node_id),
            pinned: pinned.contains(&candidate.node_id),
        })
        .filter(|evidence| candidate_is_eligible(*evidence, now))
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| compare_candidate_evidence(request, *left, *right, now));

    for required in request
        .policy
        .required_node_ids
        .iter()
        .chain(pinned_replica_node_ids)
    {
        if !eligible
            .iter()
            .any(|candidate| &candidate.candidate.node_id == required)
        {
            return Err(DfsDistributedError::InvalidReplicaSet {
                reason: format!(
                    "required or pinned node {required} is not eligible in placement snapshot {}",
                    snapshot.generation
                ),
            });
        }
    }
    if eligible.len() < request.policy.desired_replicas {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: format!(
                "placement snapshot {} has {} eligible nodes but N={} is required",
                snapshot.generation,
                eligible.len(),
                request.policy.desired_replicas
            ),
        });
    }

    let mut selected = Vec::<CandidateEvidence<'_>>::new();
    for candidate in &eligible {
        if request
            .policy
            .required_node_ids
            .contains(&candidate.candidate.node_id)
            || candidate.pinned
        {
            selected.push(*candidate);
        }
    }
    if selected.len() > request.policy.desired_replicas {
        return Err(DfsDistributedError::InvalidReplicaSet {
            reason: format!(
                "required and pinned node count {} exceeds N={}",
                selected.len(),
                request.policy.desired_replicas
            ),
        });
    }
    while selected.len() < request.policy.desired_replicas {
        let represented = selected
            .iter()
            .filter_map(|candidate| candidate_zone(request, candidate.candidate))
            .collect::<BTreeSet<_>>();
        let need_zone = represented.len() < request.policy.min_distinct_zones;
        let next = eligible
            .iter()
            .find(|candidate| {
                !selected
                    .iter()
                    .any(|selected| selected.candidate.node_id == candidate.candidate.node_id)
                    && (!need_zone
                        || candidate_zone(request, candidate.candidate)
                            .is_some_and(|zone| !represented.contains(zone)))
            })
            .or_else(|| {
                eligible.iter().find(|candidate| {
                    !selected
                        .iter()
                        .any(|selected| selected.candidate.node_id == candidate.candidate.node_id)
                })
            });
        let Some(next) = next else { break };
        selected.push(*next);
    }
    let desired_replica_nodes = selected
        .iter()
        .map(|candidate| candidate.candidate.node_id.clone())
        .collect::<Vec<_>>();
    crate::validate_replication_plan(&desired_replica_nodes, &request.policy)?;
    let preferred_commit_nodes = desired_replica_nodes
        .iter()
        .take(request.policy.min_commit_replicas)
        .cloned()
        .collect::<Vec<_>>();
    let plan = DfsReplicaPlacementPlan {
        desired_replica_nodes,
        preferred_commit_nodes,
    };
    let represented_zone_count = selected
        .iter()
        .filter_map(|candidate| candidate_zone(request, candidate.candidate))
        .collect::<BTreeSet<_>>()
        .len();
    let decisions = request
        .candidates
        .iter()
        .map(|candidate| {
            let node = snapshot.node(&candidate.node_id);
            let path = placement_candidate_path(request, snapshot, candidate);
            let selected = plan.desired_replica_nodes.contains(&candidate.node_id);
            let preferred_for_commit = plan.preferred_commit_nodes.contains(&candidate.node_id);
            let required = request
                .policy
                .required_node_ids
                .contains(&candidate.node_id);
            let pinned = pinned.contains(&candidate.node_id);
            let forbidden = request
                .policy
                .forbidden_node_ids
                .contains(&candidate.node_id);
            let evidence = CandidateEvidence {
                candidate,
                node,
                path,
                current: current.contains(&candidate.node_id),
                pinned,
            };
            let eligible = !forbidden && candidate_is_eligible(evidence, now);
            let reason = if forbidden {
                "rejected: forbidden by policy".to_string()
            } else if !eligible {
                "rejected: current storage or path health is ineligible for new content".to_string()
            } else if required {
                "selected: required by policy".to_string()
            } else if pinned {
                "selected: manual content pin".to_string()
            } else if preferred_for_commit {
                "selected: preferred commit replica by current path and storage evidence"
                    .to_string()
            } else if selected && evidence.current {
                "selected: retained current replica".to_string()
            } else if selected {
                "selected: snapshot-ranked deterministic placement".to_string()
            } else {
                "eligible but outside the desired replica count".to_string()
            };
            DfsReplicaPlacementDecision {
                node_id: candidate.node_id.clone(),
                eligible,
                zone: candidate_zone(request, candidate).map(str::to_string),
                selected,
                preferred_for_commit,
                reason,
                storage_health: node.map(|node| node.storage_health),
                storage_freshness: node.map(|node| node.freshness_at(now)),
                usable_capacity_bytes: node.and_then(|node| node.usable_capacity_bytes),
                path_state: path.map(|path| {
                    if path.freshness_at(now) == DfsObservationFreshness::Fresh {
                        path.state
                    } else {
                        DfsPlacementPathState::Unknown
                    }
                }),
                path_latency_micros: path
                    .filter(|path| path.freshness_at(now) == DfsObservationFreshness::Fresh)
                    .and_then(|path| path.latency_micros),
                path_throughput_bytes_per_second: path
                    .filter(|path| path.freshness_at(now) == DfsObservationFreshness::Fresh)
                    .and_then(|path| path.delivered_bytes_per_second),
                path_metering: path
                    .map(|path| {
                        if path.freshness_at(now) == DfsObservationFreshness::Fresh {
                            path.metering
                        } else {
                            DfsNetworkMetering::Unknown
                        }
                    })
                    .or_else(|| node.map(|node| node.network_metering)),
                resource_pressure: node.map(|node| node.resource_pressure),
                observation_freshness: path
                    .map(|path| path.freshness_at(now))
                    .or_else(|| node.map(|node| node.freshness_at(now))),
            }
        })
        .collect();
    Ok(DfsReplicaPlacementExplanation {
        plan,
        represented_zone_count,
        decisions,
        snapshot_generation: Some(snapshot.generation),
        snapshot_observed_at: Some(snapshot.observed_at),
    })
}

fn placement_candidate_path<'a>(
    request: &DfsReplicaPlacementRequest,
    snapshot: &'a DfsPlacementSnapshot,
    candidate: &DfsReplicaCandidate,
) -> Option<&'a DfsPlacementPathObservation> {
    // The coordinator reaches its own storage without a network path. A stale
    // or synthesized self-edge must never make healthy local storage
    // ineligible or less preferred than a remote replica.
    (candidate.node_id != request.coordinator_node_id)
        .then(|| snapshot.path(&request.coordinator_node_id, &candidate.node_id))
        .flatten()
}

fn candidate_zone<'a>(
    request: &'a DfsReplicaPlacementRequest,
    candidate: &'a DfsReplicaCandidate,
) -> Option<&'a str> {
    candidate.zone.as_deref().or_else(|| {
        request
            .policy
            .node_zones
            .get(&candidate.node_id)
            .map(String::as_str)
    })
}

fn candidate_is_eligible(evidence: CandidateEvidence<'_>, now: OffsetDateTime) -> bool {
    let node_eligible = evidence.node.is_none_or(|node| {
        node.freshness_at(now) != DfsObservationFreshness::Fresh
            || (node.accepts_new_content && node.storage_health != DfsHealthState::Critical)
    });
    let path_eligible = evidence.path.is_none_or(|path| {
        path.freshness_at(now) != DfsObservationFreshness::Fresh
            || path.state != DfsPlacementPathState::Unreachable
    });
    node_eligible && path_eligible
}

fn compare_candidate_evidence(
    request: &DfsReplicaPlacementRequest,
    left: CandidateEvidence<'_>,
    right: CandidateEvidence<'_>,
    now: OffsetDateTime,
) -> Ordering {
    let left_coordinator = left.candidate.node_id == request.coordinator_node_id;
    let right_coordinator = right.candidate.node_id == request.coordinator_node_id;
    right_coordinator
        .cmp(&left_coordinator)
        .then_with(|| health_rank(left.node, now).cmp(&health_rank(right.node, now)))
        .then_with(|| compare_path_safety(left.path, right.path, now))
        .then_with(|| right.current.cmp(&left.current))
        .then_with(|| {
            resource_pressure_rank(left.node, now).cmp(&resource_pressure_rank(right.node, now))
        })
        .then_with(|| compare_path_performance(left.path, right.path, now))
        .then_with(|| right.candidate.weight.cmp(&left.candidate.weight))
        .then_with(|| {
            stable_candidate_score(request, right.candidate)
                .cmp(&stable_candidate_score(request, left.candidate))
        })
        .then_with(|| left.candidate.node_id.cmp(&right.candidate.node_id))
}

fn health_rank(node: Option<&DfsPlacementNodeObservation>, now: OffsetDateTime) -> u8 {
    let Some(node) = node else { return 1 };
    if node.freshness_at(now) != DfsObservationFreshness::Fresh {
        return 1;
    }
    match node.storage_health {
        DfsHealthState::Healthy => 0,
        DfsHealthState::Unknown => 1,
        DfsHealthState::Degraded => 2,
        DfsHealthState::Critical => 3,
    }
}

fn resource_pressure_rank(node: Option<&DfsPlacementNodeObservation>, now: OffsetDateTime) -> u8 {
    let Some(node) = node else { return 0 };
    if node.freshness_at(now) != DfsObservationFreshness::Fresh {
        return 0;
    }
    match node.resource_pressure {
        DfsResourcePressure::Normal => 0,
        DfsResourcePressure::Constrained => 1,
        DfsResourcePressure::Restricted => 2,
    }
}

#[cfg(test)]
fn compare_path_evidence(
    left: Option<&DfsPlacementPathObservation>,
    right: Option<&DfsPlacementPathObservation>,
    now: OffsetDateTime,
) -> Ordering {
    compare_path_safety(left, right, now).then_with(|| compare_path_performance(left, right, now))
}

fn compare_path_safety(
    left: Option<&DfsPlacementPathObservation>,
    right: Option<&DfsPlacementPathObservation>,
    now: OffsetDateTime,
) -> Ordering {
    let left = left.filter(|path| path.freshness_at(now) == DfsObservationFreshness::Fresh);
    let right = right.filter(|path| path.freshness_at(now) == DfsObservationFreshness::Fresh);
    path_state_rank(left)
        .cmp(&path_state_rank(right))
        .then_with(|| metering_rank(left).cmp(&metering_rank(right)))
}

fn compare_path_performance(
    left: Option<&DfsPlacementPathObservation>,
    right: Option<&DfsPlacementPathObservation>,
    now: OffsetDateTime,
) -> Ordering {
    let left = left.filter(|path| path.freshness_at(now) == DfsObservationFreshness::Fresh);
    let right = right.filter(|path| path.freshness_at(now) == DfsObservationFreshness::Fresh);
    // Unknown latency/bandwidth is not converted to a pessimistic number:
    // compare measurements only when both candidates have the evidence.
    match (
        left.and_then(|path| path.latency_micros),
        right.and_then(|path| path.latency_micros),
    ) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => Ordering::Equal,
    }
    .then_with(|| {
        match (
            left.and_then(|path| path.delivered_bytes_per_second),
            right.and_then(|path| path.delivered_bytes_per_second),
        ) {
            (Some(left), Some(right)) => right.cmp(&left),
            _ => Ordering::Equal,
        }
    })
}

fn path_state_rank(path: Option<&DfsPlacementPathObservation>) -> u8 {
    match path.map(|path| path.state).unwrap_or_default() {
        DfsPlacementPathState::Healthy => 0,
        DfsPlacementPathState::Unknown => 1,
        DfsPlacementPathState::Degraded => 2,
        DfsPlacementPathState::Unreachable => 3,
    }
}

fn metering_rank(path: Option<&DfsPlacementPathObservation>) -> u8 {
    match path.map(|path| path.metering).unwrap_or_default() {
        DfsNetworkMetering::Unmetered => 0,
        DfsNetworkMetering::Unknown => 1,
        DfsNetworkMetering::Metered => 2,
    }
}

fn stable_candidate_score(
    request: &DfsReplicaPlacementRequest,
    candidate: &DfsReplicaCandidate,
) -> u128 {
    // Deliberately excludes target_version: a new version should not relocate
    // an otherwise healthy object just because its version id changed.
    let hash_input = format!(
        "{}\0{}\0{}",
        request.file_system_id, request.object_id, candidate.node_id
    );
    let hash = blake3::hash(hash_input.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hash.as_bytes()[..16]);
    u128::from_be_bytes(bytes).saturating_mul(candidate.weight.max(1) as u128)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DfsFileSystemId, DfsObjectId, DfsReplicationPolicy, DfsVersion};

    fn node(value: &str) -> NodeId {
        NodeId::from_legacy_named(value).expect("node")
    }

    #[test]
    fn unknown_throughput_does_not_rank_as_zero_bandwidth() {
        let now = OffsetDateTime::now_utc();
        let local = node("local");
        let unknown = node("unknown");
        let measured = node("measured");
        let unknown_path = DfsPlacementPathObservation {
            source_node_id: unknown,
            target_node_id: local.clone(),
            state: DfsPlacementPathState::Healthy,
            latency_micros: None,
            delivered_bytes_per_second: None,
            metering: DfsNetworkMetering::Unknown,
            observed_at: now,
            expires_at: now + time::Duration::minutes(1),
        };
        let measured_path = DfsPlacementPathObservation {
            source_node_id: measured,
            target_node_id: local,
            state: DfsPlacementPathState::Healthy,
            latency_micros: None,
            delivered_bytes_per_second: Some(1),
            metering: DfsNetworkMetering::Unknown,
            observed_at: now,
            expires_at: now + time::Duration::minutes(1),
        };
        assert_eq!(
            compare_path_evidence(Some(&unknown_path), Some(&measured_path), now),
            Ordering::Equal
        );
    }

    #[test]
    fn read_sources_rank_health_before_current_latency() {
        let now = OffsetDateTime::now_utc();
        let local = node("local");
        let healthy_fast = node("healthy-fast");
        let healthy_slow = node("healthy-slow");
        let degraded = node("degraded");
        let node_observation = |node_id: NodeId, storage_health| DfsPlacementNodeObservation {
            node_id,
            zone: None,
            storage_health,
            storage_freshness: DfsObservationFreshness::Fresh,
            accepts_new_content: true,
            capacity_weight: 1,
            usable_capacity_bytes: Some(1),
            reserved_free_bytes: Some(0),
            content_location_count: 1,
            network_metering: DfsNetworkMetering::Unknown,
            resource_pressure: DfsResourcePressure::Normal,
            observed_at: now,
            expires_at: now + time::Duration::minutes(1),
        };
        let path_observation =
            |source_node_id: NodeId, latency_micros| DfsPlacementPathObservation {
                source_node_id,
                target_node_id: local.clone(),
                state: DfsPlacementPathState::Healthy,
                latency_micros: Some(latency_micros),
                delivered_bytes_per_second: None,
                metering: DfsNetworkMetering::Unknown,
                observed_at: now,
                expires_at: now + time::Duration::minutes(1),
            };
        let cache = DfsPlacementSnapshotCache::default();
        cache.publish(DfsPlacementSnapshot {
            schema_version: DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION,
            generation: 0,
            primary_node_id: local.clone(),
            observed_at: now,
            expires_at: now + time::Duration::minutes(1),
            nodes: vec![
                node_observation(healthy_fast.clone(), DfsHealthState::Healthy),
                node_observation(healthy_slow.clone(), DfsHealthState::Healthy),
                node_observation(degraded.clone(), DfsHealthState::Degraded),
            ],
            paths: vec![
                path_observation(healthy_fast.clone(), 1_000),
                path_observation(healthy_slow.clone(), 50_000),
                path_observation(degraded.clone(), 10),
            ],
        });
        assert_eq!(
            cache.rank_sources(
                &local,
                vec![degraded.clone(), healthy_slow.clone(), healthy_fast.clone()],
            ),
            vec![healthy_fast, healthy_slow, degraded]
        );
    }

    #[test]
    fn snapshot_planning_keeps_one_replica_per_node_and_preserves_w_n() {
        let primary = node("primary");
        let fast = node("fast");
        let slow = node("slow");
        let request = DfsReplicaPlacementRequest {
            file_system_id: DfsFileSystemId::from_legacy_named("fs").expect("fs"),
            object_id: DfsObjectId::from_legacy_named("object").expect("object"),
            target_version: DfsVersion::from_legacy_named("version").expect("version"),
            coordinator_node_id: primary.clone(),
            policy: DfsReplicationPolicy::new(1, 2).expect("policy"),
            candidates: vec![
                DfsReplicaCandidate::new(primary.clone()),
                DfsReplicaCandidate::new(slow.clone()),
                DfsReplicaCandidate::new(fast.clone()),
            ],
        };
        let now = OffsetDateTime::now_utc();
        let snapshot = DfsPlacementSnapshot {
            schema_version: DFS_PLACEMENT_SNAPSHOT_SCHEMA_VERSION,
            generation: 7,
            primary_node_id: primary.clone(),
            observed_at: now,
            expires_at: now + time::Duration::minutes(1),
            nodes: request
                .candidates
                .iter()
                .map(|candidate| DfsPlacementNodeObservation {
                    node_id: candidate.node_id.clone(),
                    zone: None,
                    storage_health: DfsHealthState::Healthy,
                    storage_freshness: DfsObservationFreshness::Fresh,
                    accepts_new_content: true,
                    capacity_weight: 1,
                    usable_capacity_bytes: Some(100),
                    reserved_free_bytes: Some(10),
                    content_location_count: if candidate.node_id == fast { 3 } else { 1 },
                    network_metering: DfsNetworkMetering::Unknown,
                    resource_pressure: DfsResourcePressure::Normal,
                    observed_at: now,
                    expires_at: now + time::Duration::minutes(1),
                })
                .collect(),
            paths: vec![
                DfsPlacementPathObservation {
                    source_node_id: primary.clone(),
                    target_node_id: primary.clone(),
                    state: DfsPlacementPathState::Unreachable,
                    latency_micros: None,
                    delivered_bytes_per_second: None,
                    metering: DfsNetworkMetering::Unknown,
                    observed_at: now,
                    expires_at: now + time::Duration::minutes(1),
                },
                DfsPlacementPathObservation {
                    source_node_id: primary.clone(),
                    target_node_id: fast.clone(),
                    state: DfsPlacementPathState::Healthy,
                    latency_micros: Some(1_000),
                    delivered_bytes_per_second: Some(100_000),
                    metering: DfsNetworkMetering::Unmetered,
                    observed_at: now,
                    expires_at: now + time::Duration::minutes(1),
                },
                DfsPlacementPathObservation {
                    source_node_id: primary.clone(),
                    target_node_id: slow,
                    state: DfsPlacementPathState::Healthy,
                    latency_micros: Some(50_000),
                    delivered_bytes_per_second: Some(10_000),
                    metering: DfsNetworkMetering::Metered,
                    observed_at: now,
                    expires_at: now + time::Duration::minutes(1),
                },
            ],
        };
        let explanation = explain_replicas_with_placement_snapshot(&request, &snapshot, &[], &[])
            .expect("placement");
        assert_eq!(explanation.plan.desired_replica_nodes, vec![primary, fast]);
        assert_eq!(explanation.plan.preferred_commit_nodes.len(), 1);
        assert_eq!(explanation.snapshot_generation, Some(7));

        let pinned =
            explain_replicas_with_placement_snapshot(&request, &snapshot, &[], &[node("slow")])
                .expect("pinned placement");
        assert_eq!(pinned.plan.desired_replica_nodes.len(), 2);
        assert!(pinned.plan.desired_replica_nodes.contains(&node("slow")));
        assert_eq!(pinned.plan.preferred_commit_nodes.len(), 1);

        let mut new_version_request = request;
        new_version_request.target_version =
            DfsVersion::from_legacy_named("new-version").expect("version");
        let new_version =
            explain_replicas_with_placement_snapshot(&new_version_request, &snapshot, &[], &[])
                .expect("new-version placement");
        assert_eq!(
            explanation.plan.desired_replica_nodes,
            new_version.plan.desired_replica_nodes
        );
    }

    #[test]
    fn resource_pressure_avoids_new_replicas_without_evacuating_current_ones() {
        let now = OffsetDateTime::now_utc();
        let normal_candidate = DfsReplicaCandidate::new(node("normal-resource-node"));
        let restricted_candidate = DfsReplicaCandidate::new(node("restricted-resource-node"));
        let observation =
            |candidate: &DfsReplicaCandidate, resource_pressure| DfsPlacementNodeObservation {
                node_id: candidate.node_id.clone(),
                zone: None,
                storage_health: DfsHealthState::Healthy,
                storage_freshness: DfsObservationFreshness::Fresh,
                accepts_new_content: true,
                capacity_weight: 1,
                usable_capacity_bytes: Some(1024),
                reserved_free_bytes: Some(0),
                content_location_count: 1,
                network_metering: DfsNetworkMetering::Unmetered,
                resource_pressure,
                observed_at: now,
                expires_at: now + time::Duration::minutes(1),
            };
        let normal = observation(&normal_candidate, DfsResourcePressure::Normal);
        let restricted = observation(&restricted_candidate, DfsResourcePressure::Restricted);
        let request = DfsReplicaPlacementRequest {
            file_system_id: DfsFileSystemId::from_legacy_named("resource-pressure-fs")
                .expect("filesystem"),
            object_id: DfsObjectId::from_legacy_named("resource-pressure-object").expect("object"),
            target_version: DfsVersion::from_legacy_named("resource-pressure-version")
                .expect("version"),
            coordinator_node_id: node("resource-pressure-coordinator"),
            policy: DfsReplicationPolicy::new(1, 1).expect("policy"),
            candidates: vec![normal_candidate.clone(), restricted_candidate.clone()],
        };
        let normal_evidence = CandidateEvidence {
            candidate: &normal_candidate,
            node: Some(&normal),
            path: None,
            current: false,
            pinned: false,
        };
        let restricted_evidence = CandidateEvidence {
            candidate: &restricted_candidate,
            node: Some(&restricted),
            path: None,
            current: false,
            pinned: false,
        };

        assert_eq!(
            compare_candidate_evidence(&request, normal_evidence, restricted_evidence, now),
            Ordering::Less,
            "new placement should prefer the normal node"
        );
        assert_eq!(
            compare_candidate_evidence(
                &request,
                normal_evidence,
                CandidateEvidence {
                    current: true,
                    ..restricted_evidence
                },
                now,
            ),
            Ordering::Greater,
            "resource pressure alone must not evacuate an existing replica"
        );
    }

    #[test]
    fn aggregate_advertisement_counts_locations_but_keeps_one_node_identity() {
        let root = sinomo_testdir::TestDir::new("dfs-health-multiple-locations");
        let first = root.join("first");
        let second = root.join("second");
        std::fs::create_dir_all(&first).expect("first location");
        std::fs::create_dir_all(&second).expect("second location");
        let location = |location_id: &str, root: PathBuf| DfsContentLocationConfig {
            location_id: location_id.to_string(),
            label: location_id.to_string(),
            root,
            lifecycle: DfsContentLocationLifecycle::Active,
            reserved_free_bytes: 0,
            max_dfs_bytes: None,
            allocation_weight: 1,
            configuration_generation: 1,
        };
        let local = node("local");
        let mut monitor = DfsDeviceHealthMonitor::new(
            local.clone(),
            vec![location("first", first), location("second", second)],
        );
        let now = OffsetDateTime::now_utc();
        monitor.sample(now, DfsNetworkMetering::Unknown);
        let advertisement = monitor.aggregate_advertisement(None, now);
        assert_eq!(advertisement.node_id, local);
        assert_eq!(advertisement.content_location_count, 2);
        assert!(advertisement.accepts_content);
    }

    #[test]
    fn metering_transition_updates_cached_health_without_resampling_storage() {
        let root = sinomo_testdir::TestDir::new("dfs-health-metering-transition");
        let content = root.join("content");
        std::fs::create_dir_all(&content).expect("content location");
        let mut monitor = DfsDeviceHealthMonitor::new(
            node("metering-transition"),
            vec![DfsContentLocationConfig::legacy(content.clone())],
        );
        let sampled_at = OffsetDateTime::now_utc();
        let initial = monitor.sample(sampled_at, DfsNetworkMetering::Unmetered);
        std::fs::remove_dir_all(&content).expect("remove after initial sample");

        let updated = monitor.sample_if_due(
            sampled_at + time::Duration::seconds(1),
            DfsNetworkMetering::Metered,
            DfsResourcePressure::Restricted,
        );

        assert_eq!(updated.sequence, initial.sequence + 1);
        assert_eq!(updated.observed_at, initial.observed_at);
        assert_eq!(updated.summary, initial.summary);
        assert_eq!(updated.content_locations, initial.content_locations);
        assert_eq!(updated.network_metering, DfsNetworkMetering::Metered);
        assert_eq!(updated.resource_pressure, DfsResourcePressure::Restricted);
        assert_eq!(monitor.last_sampled_at, Some(sampled_at));
    }

    #[test]
    fn advisory_resource_transition_bypasses_storage_health_hysteresis() {
        let root = sinomo_testdir::TestDir::new("dfs-health-advisory-transition");
        let content = root.join("content");
        std::fs::create_dir_all(&content).expect("content location");
        let local = node("advisory-transition");
        let mut monitor =
            DfsDeviceHealthMonitor::new(local, vec![DfsContentLocationConfig::legacy(content)]);
        let now = OffsetDateTime::now_utc();
        monitor.sample(now, DfsNetworkMetering::Unmetered);
        let initial = monitor.aggregate_advertisement(None, now);
        assert_eq!(initial.resource_pressure, DfsResourcePressure::Normal);

        monitor
            .update_advisory_evidence(DfsNetworkMetering::Metered, DfsResourcePressure::Restricted);
        let restricted = monitor.aggregate_advertisement(None, now + time::Duration::seconds(1));

        assert_eq!(restricted.network_metering, DfsNetworkMetering::Metered);
        assert_eq!(
            restricted.resource_pressure,
            DfsResourcePressure::Restricted
        );
        assert_eq!(restricted.storage_health, initial.storage_health);
        assert_eq!(
            restricted.expires_at, initial.expires_at,
            "an advisory callback must not renew unsampled capacity evidence"
        );
    }

    #[test]
    fn aggregate_advertisement_stops_immediately_and_recovers_hysteretically() {
        let root = sinomo_testdir::TestDir::new("dfs-health-advertisement-hysteresis");
        let content = root.join("content");
        std::fs::create_dir_all(&content).expect("content location");
        let local = node("local-hysteresis");
        let mut monitor = DfsDeviceHealthMonitor::new(
            local,
            vec![DfsContentLocationConfig {
                location_id: "content".to_string(),
                label: "content".to_string(),
                root: content,
                lifecycle: DfsContentLocationLifecycle::Active,
                reserved_free_bytes: 0,
                max_dfs_bytes: None,
                allocation_weight: 1,
                configuration_generation: 1,
            }],
        );
        let now = OffsetDateTime::now_utc();
        monitor.sample(now, DfsNetworkMetering::Unknown);
        assert!(monitor.aggregate_advertisement(None, now).accepts_content);

        monitor.locations[0].reserved_free_bytes = u64::MAX;
        monitor.sample(
            now + time::Duration::seconds(1),
            DfsNetworkMetering::Unknown,
        );
        assert!(
            !monitor
                .aggregate_advertisement(None, now + time::Duration::seconds(1))
                .accepts_content
        );

        monitor.locations[0].reserved_free_bytes = 0;
        monitor.sample(
            now + time::Duration::seconds(2),
            DfsNetworkMetering::Unknown,
        );
        assert!(
            !monitor
                .aggregate_advertisement(None, now + time::Duration::seconds(2))
                .accepts_content
        );
        monitor.sample(
            now + time::Duration::seconds(3),
            DfsNetworkMetering::Unknown,
        );
        assert!(
            monitor
                .aggregate_advertisement(None, now + time::Duration::seconds(3))
                .accepts_content
        );
    }
}
