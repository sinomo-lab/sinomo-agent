//! Immutable direct-connectivity evidence consumed by route planning.
//!
//! This module records only directed adjacent-peer observations. It does not
//! derive transitive reachability, establish sessions, or interpret service
//! methods.

use crate::direct_link::DirectStreamCapabilities;
use crate::error::NetworkError;
use crate::{ClusterConnectivityRecord, ClusterConnectivityState, DirectorySnapshot};
use arc_swap::ArcSwap;
use sinomo_base::{ClusterEndpointRecord, Id};
use sinomo_node::NodeId;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::sync::watch;

pub const MAX_CONNECTIVITY_SNAPSHOT_EDGES: usize = 4096;
pub const MAX_CONNECTIVITY_FAILURE_REASON_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectivityGeneration(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectConnectivityState {
    Reachable,
    Unreachable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectConnectivityStatus {
    Reachable,
    Unreachable,
    Unknown,
}

/// Source-owned evidence for one directed, directly reachable endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectConnectivityObservation {
    /// Node that submitted the observation. It must equal `source_node_id`.
    pub observer_node_id: NodeId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub endpoint: ClusterEndpointRecord,
    pub capabilities: DirectStreamCapabilities,
    pub state: DirectConnectivityState,
    pub failure_reason: Option<String>,
    pub latency_micros: Option<u64>,
    pub delivered_bytes_per_second: Option<u64>,
    pub provenance: String,
    pub source_generation: u64,
    pub directory_generation: u64,
    pub observed_at: OffsetDateTime,
    pub expires_at: OffsetDateTime,
}

impl DirectConnectivityObservation {
    fn key(&self) -> (NodeId, NodeId, Id) {
        (
            self.source_node_id.clone(),
            self.target_node_id.clone(),
            self.endpoint.endpoint_id.clone(),
        )
    }

    fn validate(&self) -> Result<(), NetworkError> {
        if self.observer_node_id != self.source_node_id {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "connectivity observer {} cannot publish source-owned generation {} for {}",
                    self.observer_node_id, self.source_generation, self.source_node_id
                ),
            });
        }
        if self.source_node_id == self.target_node_id {
            return Err(NetworkError::Serialization {
                detail: "direct connectivity evidence must connect distinct nodes".to_string(),
            });
        }
        if self.endpoint.node_id != self.target_node_id {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "connectivity endpoint {} belongs to {}, not target {}",
                    self.endpoint.endpoint_id, self.endpoint.node_id, self.target_node_id
                ),
            });
        }
        if self.provenance.trim().is_empty() {
            return Err(NetworkError::Serialization {
                detail: "connectivity evidence requires provenance".to_string(),
            });
        }
        if self.expires_at <= self.observed_at {
            return Err(NetworkError::Serialization {
                detail: "connectivity evidence must expire after it was observed".to_string(),
            });
        }
        if self.state == DirectConnectivityState::Reachable
            && (!self.capabilities.full_duplex || self.capabilities.max_concurrent_streams == 0)
        {
            return Err(NetworkError::Serialization {
                detail: "reachable connectivity evidence lacks a usable duplex-stream capability"
                    .to_string(),
            });
        }
        Ok(())
    }
}

/// Structured, source-owned evidence emitted when route realization rejects
/// or loses one specific direct edge.
///
/// The tracker may only update an edge already present in the current direct
/// snapshot. Routing therefore cannot create synthetic connectivity or alter
/// cluster membership while closing the plan/execution feedback loop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectConnectivityFailureEvidence {
    pub observer_node_id: NodeId,
    pub source_node_id: NodeId,
    pub target_node_id: NodeId,
    pub endpoint_id: Id,
    pub reason: String,
    pub observed_at: OffsetDateTime,
}

impl DirectConnectivityFailureEvidence {
    fn validate(&self) -> Result<(), NetworkError> {
        if self.observer_node_id != self.source_node_id
            || self.source_node_id == self.target_node_id
            || self.reason.trim().is_empty()
            || self.reason.len() > MAX_CONNECTIVITY_FAILURE_REASON_BYTES
        {
            return Err(NetworkError::Serialization {
                detail: "invalid source-owned direct-connectivity failure evidence".to_string(),
            });
        }
        Ok(())
    }
}

/// Generation-stamped replacement snapshot of current direct-edge evidence.
#[derive(Clone, Debug)]
pub struct ConnectivitySnapshot {
    generation: ConnectivityGeneration,
    directory_generation: u64,
    published_at: OffsetDateTime,
    edges: Arc<[DirectConnectivityObservation]>,
}

impl ConnectivitySnapshot {
    pub fn empty(
        generation: ConnectivityGeneration,
        directory_generation: u64,
        published_at: OffsetDateTime,
    ) -> Self {
        Self {
            generation,
            directory_generation,
            published_at,
            edges: Arc::from([]),
        }
    }

    /// Builds a current snapshot from source-owned observations.
    ///
    /// Only the newest generation submitted by each source is eligible. An
    /// expired or directory-stale newest generation does not fall back to an
    /// older generation, because that would let superseded evidence authorize
    /// a route again.
    pub fn try_new(
        generation: ConnectivityGeneration,
        directory_generation: u64,
        published_at: OffsetDateTime,
        observations: impl IntoIterator<Item = DirectConnectivityObservation>,
    ) -> Result<Self, NetworkError> {
        let observations = observations.into_iter().collect::<Vec<_>>();
        if observations.len() > MAX_CONNECTIVITY_SNAPSHOT_EDGES {
            return Err(NetworkError::ClusterConnectivitySnapshotRecordLimit {
                observed_records: observations.len(),
                max_records: MAX_CONNECTIVITY_SNAPSHOT_EDGES,
            });
        }

        let mut newest_generation_by_source = BTreeMap::<NodeId, u64>::new();
        for observation in &observations {
            observation.validate()?;
            newest_generation_by_source
                .entry(observation.source_node_id.clone())
                .and_modify(|generation| {
                    *generation = (*generation).max(observation.source_generation)
                })
                .or_insert(observation.source_generation);
        }

        let mut current = BTreeMap::<(NodeId, NodeId, Id), DirectConnectivityObservation>::new();
        for observation in observations {
            let is_newest_source_generation = newest_generation_by_source
                .get(&observation.source_node_id)
                .is_some_and(|generation| *generation == observation.source_generation);
            if !is_newest_source_generation
                || observation.directory_generation != directory_generation
                || observation.expires_at <= published_at
                || observation.observed_at > published_at
            {
                continue;
            }

            let key = observation.key();
            match current.get(&key) {
                Some(existing) if existing.observed_at == observation.observed_at => {
                    if existing != &observation {
                        return Err(NetworkError::Serialization {
                            detail: format!(
                                "conflicting connectivity evidence for {} -> {} endpoint {} at {}",
                                key.0, key.1, key.2, observation.observed_at
                            ),
                        });
                    }
                }
                Some(existing) if existing.observed_at > observation.observed_at => {}
                _ => {
                    current.insert(key, observation);
                }
            }
        }

        Ok(Self {
            generation,
            directory_generation,
            published_at,
            edges: Arc::from(current.into_values().collect::<Vec<_>>()),
        })
    }

    pub fn generation(&self) -> ConnectivityGeneration {
        self.generation
    }

    pub fn directory_generation(&self) -> u64 {
        self.directory_generation
    }

    pub fn published_at(&self) -> OffsetDateTime {
        self.published_at
    }

    pub fn edges(&self) -> &[DirectConnectivityObservation] {
        &self.edges
    }

    pub fn direct_status(
        &self,
        source_node_id: &NodeId,
        target_node_id: &NodeId,
    ) -> DirectConnectivityStatus {
        let mut saw_unreachable = false;
        for edge in self.edges.iter().filter(|edge| {
            &edge.source_node_id == source_node_id && &edge.target_node_id == target_node_id
        }) {
            match edge.state {
                DirectConnectivityState::Reachable => {
                    return DirectConnectivityStatus::Reachable;
                }
                DirectConnectivityState::Unreachable => saw_unreachable = true,
            }
        }
        if saw_unreachable {
            DirectConnectivityStatus::Unreachable
        } else {
            DirectConnectivityStatus::Unknown
        }
    }
}

/// Projects the durable directory connectivity contract into the immutable
/// direct-edge view consumed by Stage-D route planning. Records that no
/// longer name an authorized endpoint are omitted; no transitive edges are
/// synthesized.
pub fn connectivity_snapshot_from_directory_records(
    directory: &DirectorySnapshot,
    records: impl IntoIterator<Item = ClusterConnectivityRecord>,
    published_at: OffsetDateTime,
) -> Result<ConnectivitySnapshot, NetworkError> {
    let directory_generation = directory.version().0;
    let mut observations = Vec::new();
    for record in records {
        let Some(source) = directory.node(&record.source_node_id) else {
            continue;
        };
        let Some(target) = directory.node(&record.target_node_id) else {
            continue;
        };
        let Some(endpoint) = target
            .endpoints
            .iter()
            .find(|endpoint| endpoint.endpoint_id == record.endpoint_id)
            .cloned()
        else {
            continue;
        };
        if source.node_id == target.node_id
            || endpoint.transport != record.transport
            || endpoint.address != record.address
        {
            continue;
        }
        let confidential = matches!(
            endpoint.transport,
            sinomo_base::ClusterTransportKind::LanQuic
                | sinomo_base::ClusterTransportKind::Iroh
                | sinomo_base::ClusterTransportKind::WebRtc
                | sinomo_base::ClusterTransportKind::TurnRelay
                | sinomo_base::ClusterTransportKind::HttpsRelay
        );
        observations.push(DirectConnectivityObservation {
            observer_node_id: record.source_node_id.clone(),
            source_node_id: record.source_node_id,
            target_node_id: record.target_node_id,
            endpoint,
            capabilities: DirectStreamCapabilities {
                full_duplex: true,
                relay_forwarding: true,
                confidential,
                max_concurrent_streams: 1,
            },
            state: match record.state {
                ClusterConnectivityState::Healthy | ClusterConnectivityState::Degraded => {
                    DirectConnectivityState::Reachable
                }
                ClusterConnectivityState::Unreachable => DirectConnectivityState::Unreachable,
            },
            failure_reason: record.failure_reason.map(|reason| format!("{reason:?}")),
            latency_micros: record
                .latency_ms
                .and_then(|millis| millis.checked_mul(1_000)),
            delivered_bytes_per_second: None,
            provenance: "cluster-directory-connectivity".to_string(),
            source_generation: directory_generation,
            directory_generation,
            observed_at: record.observed_at,
            expires_at: record.expires_at,
        });
    }
    ConnectivitySnapshot::try_new(
        ConnectivityGeneration(directory_generation),
        directory_generation,
        published_at,
        observations,
    )
}

/// Side-effect-free read boundary for immutable connectivity snapshots.
pub trait ConnectivityView: Send + Sync {
    fn snapshot(&self) -> Arc<ConnectivitySnapshot>;
    fn subscribe(&self) -> watch::Receiver<Arc<ConnectivitySnapshot>>;
}

/// Narrow write boundary used by route realization to reject one stale edge.
pub trait ConnectivityFailureSink: Send + Sync {
    /// Marks one current direct edge unreachable while preserving every other
    /// edge in the source's latest complete set.
    fn submit_edge_failure(
        &self,
        evidence: DirectConnectivityFailureEvidence,
    ) -> Result<(), NetworkError>;
}

/// Explicit source-set write boundary used by connectivity probe owners.
pub trait ConnectivityObservationSink: ConnectivityFailureSink {
    /// Atomically replaces one source node's complete observation set.
    ///
    /// Empty sets are meaningful and still advance the source watermark, so a
    /// delayed older set cannot restore omitted direct edges.
    fn submit_source_set(
        &self,
        source_node_id: NodeId,
        source_generation: u64,
        observations: Vec<DirectConnectivityObservation>,
        published_at: OffsetDateTime,
    ) -> Result<(), NetworkError>;
}

struct ConnectivityTrackerState {
    source_generations: BTreeMap<NodeId, u64>,
}

/// In-memory publication owner for the immutable connectivity read model.
///
/// Mutations are serialized and replace the active `Arc` atomically. Snapshot
/// reads and subscriptions never probe, persist, expire, or otherwise mutate
/// network state.
pub struct ConnectivityTracker {
    active: ArcSwap<ConnectivitySnapshot>,
    publisher: watch::Sender<Arc<ConnectivitySnapshot>>,
    state: Mutex<ConnectivityTrackerState>,
}

impl ConnectivityTracker {
    pub fn new(initial: ConnectivitySnapshot) -> Self {
        let initial = Arc::new(initial);
        let source_generations = initial
            .edges()
            .iter()
            .map(|edge| (edge.source_node_id.clone(), edge.source_generation))
            .collect();
        let (publisher, _) = watch::channel(Arc::clone(&initial));
        Self {
            active: ArcSwap::from(initial),
            publisher,
            state: Mutex::new(ConnectivityTrackerState { source_generations }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ConnectivityTrackerState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn publish(&self, snapshot: ConnectivitySnapshot) {
        let snapshot = Arc::new(snapshot);
        self.active.store(Arc::clone(&snapshot));
        self.publisher.send_replace(snapshot);
    }

    /// Explicitly advances the directory generation and invalidates every
    /// edge captured against the older authorization/endpoint snapshot.
    pub fn advance_directory_generation(
        &self,
        directory_generation: u64,
        published_at: OffsetDateTime,
    ) -> Result<bool, NetworkError> {
        let mut state = self.state();
        let current = self.active.load_full();
        if published_at < current.published_at() {
            return Err(NetworkError::Serialization {
                detail: "connectivity publication time cannot move backwards".to_string(),
            });
        }
        if directory_generation < current.directory_generation() {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "connectivity directory generation regressed from {} to {directory_generation}",
                    current.directory_generation()
                ),
            });
        }
        if directory_generation == current.directory_generation() {
            return Ok(false);
        }
        state.source_generations.clear();
        let generation = ConnectivityGeneration(current.generation().0.saturating_add(1));
        self.publish(ConnectivitySnapshot::empty(
            generation,
            directory_generation,
            published_at,
        ));
        Ok(true)
    }

    /// Explicitly removes evidence expired at `published_at`.
    pub fn expire(&self, published_at: OffsetDateTime) -> Result<bool, NetworkError> {
        let _state = self.state();
        let current = self.active.load_full();
        if published_at < current.published_at() {
            return Err(NetworkError::Serialization {
                detail: "connectivity expiry time cannot move backwards".to_string(),
            });
        }
        if !current
            .edges()
            .iter()
            .any(|edge| edge.expires_at <= published_at)
        {
            return Ok(false);
        }
        let snapshot = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(current.generation().0.saturating_add(1)),
            current.directory_generation(),
            published_at,
            current.edges().iter().cloned(),
        )?;
        self.publish(snapshot);
        Ok(true)
    }
}

impl ConnectivityView for ConnectivityTracker {
    fn snapshot(&self) -> Arc<ConnectivitySnapshot> {
        self.active.load_full()
    }

    fn subscribe(&self) -> watch::Receiver<Arc<ConnectivitySnapshot>> {
        self.publisher.subscribe()
    }
}

impl ConnectivityObservationSink for ConnectivityTracker {
    fn submit_source_set(
        &self,
        source_node_id: NodeId,
        source_generation: u64,
        observations: Vec<DirectConnectivityObservation>,
        published_at: OffsetDateTime,
    ) -> Result<(), NetworkError> {
        let mut state = self.state();
        let current = self.active.load_full();
        if published_at < current.published_at() {
            return Err(NetworkError::Serialization {
                detail: "connectivity publication time cannot move backwards".to_string(),
            });
        }
        if state
            .source_generations
            .get(&source_node_id)
            .is_some_and(|current_generation| source_generation <= *current_generation)
        {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "connectivity source generation for {source_node_id} did not advance to {source_generation}"
                ),
            });
        }
        for observation in &observations {
            if observation.source_node_id != source_node_id
                || observation.observer_node_id != source_node_id
                || observation.source_generation != source_generation
            {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "connectivity source set {source_node_id}@{source_generation} contains foreign or mismatched evidence"
                    ),
                });
            }
            if observation.directory_generation != current.directory_generation() {
                return Err(NetworkError::Serialization {
                    detail: format!(
                        "connectivity source set uses directory generation {}, current is {}",
                        observation.directory_generation,
                        current.directory_generation()
                    ),
                });
            }
        }

        let retained = current
            .edges()
            .iter()
            .filter(|edge| edge.source_node_id != source_node_id)
            .cloned();
        let snapshot = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(current.generation().0.saturating_add(1)),
            current.directory_generation(),
            published_at,
            retained.chain(observations),
        )?;
        state
            .source_generations
            .insert(source_node_id, source_generation);
        self.publish(snapshot);
        Ok(())
    }
}

impl ConnectivityFailureSink for ConnectivityTracker {
    fn submit_edge_failure(
        &self,
        evidence: DirectConnectivityFailureEvidence,
    ) -> Result<(), NetworkError> {
        evidence.validate()?;
        let mut state = self.state();
        let current = self.active.load_full();
        if evidence.observed_at < current.published_at() {
            return Err(NetworkError::Serialization {
                detail: "connectivity failure evidence predates the current snapshot".to_string(),
            });
        }
        let source_generation = state
            .source_generations
            .get(&evidence.source_node_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let mut matched = false;
        let mut observations = Vec::with_capacity(current.edges().len());
        for observation in current.edges() {
            let mut observation = observation.clone();
            if observation.source_node_id == evidence.source_node_id {
                observation.source_generation = source_generation;
                if observation.target_node_id == evidence.target_node_id
                    && observation.endpoint.endpoint_id == evidence.endpoint_id
                {
                    matched = true;
                    observation.observed_at = evidence.observed_at;
                    observation.expires_at = evidence.observed_at + time::Duration::seconds(60);
                    observation.state = DirectConnectivityState::Unreachable;
                    observation.failure_reason = Some(evidence.reason.clone());
                    observation.latency_micros = None;
                    observation.delivered_bytes_per_second = None;
                    observation.provenance = "routed-session-edge-failure".to_string();
                }
            }
            observations.push(observation);
        }
        if !matched {
            return Err(NetworkError::Serialization {
                detail: format!(
                    "route failure referenced unknown direct edge {} -> {} endpoint {}",
                    evidence.source_node_id, evidence.target_node_id, evidence.endpoint_id
                ),
            });
        }
        let snapshot = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(current.generation().0.saturating_add(1)),
            current.directory_generation(),
            evidence.observed_at,
            observations,
        )?;
        state
            .source_generations
            .insert(evidence.source_node_id, source_generation);
        self.publish(snapshot);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_base::{ClusterEndpointPrivacy, ClusterEndpointSource, ClusterTransportKind};
    use time::Duration;

    fn node(name: &str) -> NodeId {
        NodeId::from_legacy_named(name).expect("node id")
    }

    fn observation(
        source: &NodeId,
        target: &NodeId,
        endpoint_name: &str,
        state: DirectConnectivityState,
        source_generation: u64,
        directory_generation: u64,
        observed_at: OffsetDateTime,
    ) -> DirectConnectivityObservation {
        DirectConnectivityObservation {
            observer_node_id: source.clone(),
            source_node_id: source.clone(),
            target_node_id: target.clone(),
            endpoint: ClusterEndpointRecord {
                endpoint_id: Id::from_legacy_named(endpoint_name).expect("endpoint id"),
                node_id: target.clone(),
                transport: ClusterTransportKind::LanQuic,
                address: format!("quic://{endpoint_name}"),
                source: ClusterEndpointSource::Observed,
                privacy: ClusterEndpointPrivacy::Lan,
                priority: 10,
                last_observed_latency_ms: Some(1),
            },
            capabilities: DirectStreamCapabilities::multiplexed_full_duplex(64),
            state,
            failure_reason: None,
            latency_micros: Some(1_000),
            delivered_bytes_per_second: None,
            provenance: "test-probe".to_string(),
            source_generation,
            directory_generation,
            observed_at,
            expires_at: observed_at + Duration::minutes(1),
        }
    }

    #[test]
    fn snapshot_keeps_only_current_source_owned_direct_evidence() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let a = node("node_connectivity_snapshot_a");
        let b = node("node_connectivity_snapshot_b");
        let c = node("node_connectivity_snapshot_c");
        let old = observation(
            &a,
            &b,
            "endpoint_connectivity_old",
            DirectConnectivityState::Reachable,
            1,
            7,
            now - Duration::seconds(10),
        );
        let current = observation(
            &a,
            &c,
            "endpoint_connectivity_current",
            DirectConnectivityState::Reachable,
            2,
            7,
            now - Duration::seconds(5),
        );
        let reverse = observation(
            &b,
            &a,
            "endpoint_connectivity_reverse",
            DirectConnectivityState::Unreachable,
            1,
            7,
            now - Duration::seconds(5),
        );

        let snapshot = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(3),
            7,
            now,
            [old, current, reverse],
        )
        .expect("snapshot");

        assert_eq!(snapshot.edges().len(), 2);
        assert_eq!(
            snapshot.direct_status(&a, &b),
            DirectConnectivityStatus::Unknown
        );
        assert_eq!(
            snapshot.direct_status(&a, &c),
            DirectConnectivityStatus::Reachable
        );
        assert_eq!(
            snapshot.direct_status(&b, &a),
            DirectConnectivityStatus::Unreachable
        );
        assert_eq!(
            snapshot.direct_status(&a, &b),
            DirectConnectivityStatus::Unknown,
            "reverse evidence must not create a synthetic symmetric edge"
        );
    }

    #[test]
    fn observer_cannot_advance_another_sources_generation() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let a = node("node_connectivity_owner_a");
        let b = node("node_connectivity_owner_b");
        let c = node("node_connectivity_owner_c");
        let mut forged = observation(
            &a,
            &b,
            "endpoint_connectivity_forged",
            DirectConnectivityState::Reachable,
            9,
            2,
            now - Duration::seconds(1),
        );
        forged.observer_node_id = c;

        let error = ConnectivitySnapshot::try_new(ConnectivityGeneration(1), 2, now, [forged])
            .expect_err("forged source generation must fail");
        assert!(
            error
                .to_string()
                .contains("cannot publish source-owned generation")
        );
    }

    #[test]
    fn tracker_publishes_atomic_source_sets_and_reads_are_side_effect_free() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let a = node("node_connectivity_tracker_a");
        let b = node("node_connectivity_tracker_b");
        let tracker = ConnectivityTracker::new(ConnectivitySnapshot::empty(
            ConnectivityGeneration(1),
            4,
            now,
        ));
        let mut changes = tracker.subscribe();
        let before = tracker.snapshot();
        assert_eq!(before.generation(), ConnectivityGeneration(1));
        assert_eq!(tracker.snapshot().generation(), before.generation());

        tracker
            .submit_source_set(
                a.clone(),
                1,
                vec![observation(
                    &a,
                    &b,
                    "endpoint_connectivity_tracker",
                    DirectConnectivityState::Reachable,
                    1,
                    4,
                    now - Duration::seconds(1),
                )],
                now,
            )
            .expect("publish source set");

        assert!(changes.has_changed().expect("watch state"));
        let published = changes.borrow_and_update().clone();
        assert_eq!(published.generation(), ConnectivityGeneration(2));
        assert_eq!(published.edges().len(), 1);
        assert_eq!(
            tracker.snapshot().direct_status(&a, &b),
            DirectConnectivityStatus::Reachable
        );
    }

    #[test]
    fn empty_source_set_fences_delayed_evidence_and_expiry_is_explicit() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let a = node("node_connectivity_fence_a");
        let b = node("node_connectivity_fence_b");
        let tracker = ConnectivityTracker::new(ConnectivitySnapshot::empty(
            ConnectivityGeneration(1),
            9,
            now,
        ));
        let edge = observation(
            &a,
            &b,
            "endpoint_connectivity_fence",
            DirectConnectivityState::Reachable,
            1,
            9,
            now - Duration::seconds(1),
        );
        tracker
            .submit_source_set(a.clone(), 1, vec![edge.clone()], now)
            .expect("initial source set");
        tracker
            .submit_source_set(a.clone(), 2, Vec::new(), now + Duration::seconds(1))
            .expect("empty replacement set");
        assert!(tracker.snapshot().edges().is_empty());
        assert!(
            tracker
                .submit_source_set(a.clone(), 1, vec![edge], now + Duration::seconds(2))
                .is_err(),
            "delayed generation must not restore an omitted edge"
        );

        let expiring = observation(
            &a,
            &b,
            "endpoint_connectivity_expiry",
            DirectConnectivityState::Reachable,
            3,
            9,
            now + Duration::seconds(3),
        );
        let expires_at = expiring.expires_at;
        tracker
            .submit_source_set(a, 3, vec![expiring], now + Duration::seconds(4))
            .expect("new source set");
        assert_eq!(tracker.snapshot().edges().len(), 1);
        assert!(tracker.expire(expires_at).expect("expire snapshot"));
        assert!(tracker.snapshot().edges().is_empty());
    }

    #[test]
    fn edge_failure_cannot_create_synthetic_connectivity() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let a = node("node_connectivity_failure_a");
        let b = node("node_connectivity_failure_b");
        let c = node("node_connectivity_failure_c");
        let initial = ConnectivitySnapshot::try_new(
            ConnectivityGeneration(7),
            11,
            now,
            [observation(
                &a,
                &b,
                "endpoint_connectivity_failure_known",
                DirectConnectivityState::Reachable,
                1,
                11,
                now - Duration::seconds(1),
            )],
        )
        .expect("initial connectivity");
        let tracker = ConnectivityTracker::new(initial);
        let before = tracker.snapshot();

        let error = tracker
            .submit_edge_failure(DirectConnectivityFailureEvidence {
                observer_node_id: a.clone(),
                source_node_id: a,
                target_node_id: c,
                endpoint_id: Id::generate(),
                reason: "route realization rejected unknown edge".to_string(),
                observed_at: now + Duration::seconds(1),
            })
            .expect_err("unknown edge rejection");

        assert!(error.to_string().contains("unknown direct edge"));
        let after = tracker.snapshot();
        assert_eq!(after.generation(), before.generation());
        assert_eq!(after.edges(), before.edges());
    }
}
