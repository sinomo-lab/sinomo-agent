use sinomo_base::ClusterBootstrapState;
use sinomo_base::ClusterEndpointRecord;
use sinomo_network::{
    ClusterConnectivityRecord, ClusterConnectivityState, ClusterObjectRpcObservation, ClusterPaths,
    NetworkError,
};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

pub const CLUSTER_COMMAND_SUCCESS_OBSERVATION_INTERVAL: Duration = Duration::from_secs(1);
pub const CLUSTER_COMMAND_OBSERVATION_MAX_ENDPOINTS: usize = 256;
pub const CLUSTER_COMMAND_OBSERVATION_MAX_IN_FLIGHT: usize = 8;

/// Cluster-owned projection of one command RPC result into connectivity
/// evidence. The runtime host decides how to persist the projected evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterCommandConnectivityObservation {
    pub connectivity_state: ClusterConnectivityState,
    pub latency_ms: Option<u64>,
    pub bypass_failure_grace: bool,
    pub command_auth_denied: bool,
}

impl ClusterCommandConnectivityObservation {
    pub fn from_rpc(observation: ClusterObjectRpcObservation) -> Self {
        if observation.is_success() {
            Self {
                connectivity_state: ClusterConnectivityState::Healthy,
                latency_ms: observation.latency_ms(),
                bypass_failure_grace: false,
                command_auth_denied: false,
            }
        } else if observation.is_io_failure() {
            Self {
                connectivity_state: ClusterConnectivityState::Degraded,
                latency_ms: None,
                bypass_failure_grace: observation.is_hard_io_failure(),
                command_auth_denied: false,
            }
        } else if observation.is_auth_denied() {
            Self {
                connectivity_state: ClusterConnectivityState::Unreachable,
                latency_ms: None,
                bypass_failure_grace: false,
                command_auth_denied: true,
            }
        } else {
            Self {
                connectivity_state: ClusterConnectivityState::Degraded,
                latency_ms: None,
                bypass_failure_grace: false,
                command_auth_denied: false,
            }
        }
    }

    /// Commits this projected RPC observation through the daemon-owned local
    /// connectivity-store boundary.
    ///
    /// Ordering, throttling, and cancellation are owned by
    /// [`ClusterCommandObservationCoordinator`]. The runtime host invokes this
    /// method inside the winning reservation's publication callback, then may
    /// schedule any host-specific convergence work.
    pub fn record_daemon_owned(
        &self,
        paths: &ClusterPaths,
        state: &ClusterBootstrapState,
        endpoint: &ClusterEndpointRecord,
    ) -> Result<ClusterConnectivityRecord, NetworkError> {
        if self.command_auth_denied {
            sinomo_network::record_local_cluster_endpoint_command_auth_denied(
                paths, state, endpoint,
            )
        } else {
            sinomo_network::__connectivity_store::record_daemon_owned_local_cluster_endpoint_command_connectivity(
                paths,
                state,
                endpoint,
                self.connectivity_state.clone(),
                self.latency_ms,
                !self.bypass_failure_grace,
            )
        }
    }
}

/// Stable key for ordering connectivity evidence for one endpoint in one
/// cluster-state directory.
pub fn cluster_command_observation_key(
    paths: Option<&ClusterPaths>,
    endpoint: &ClusterEndpointRecord,
) -> String {
    format!(
        "{}:{}:{}",
        paths
            .map(|paths| paths.root.display().to_string())
            .unwrap_or_else(|| "<default>".to_string()),
        endpoint.node_id,
        endpoint.endpoint_id
    )
}

/// Orders and bounds durable command-connectivity observations.
///
/// A runtime host reserves an observation generation, schedules its concrete
/// durable effect, and invokes [`ClusterCommandObservationReservation::publish`]
/// inside that effect. The reservation owns its global task permit, so dropping
/// a cancelled task releases both per-key and global capacity.
pub struct ClusterCommandObservationCoordinator {
    slots: Mutex<HashMap<String, Arc<ClusterCommandObservationSlot>>>,
    task_permits: Arc<tokio::sync::Semaphore>,
    sequence: AtomicU64,
    max_slots: usize,
    success_interval: Duration,
}

struct ClusterCommandObservationSlot {
    // Acquired only by the host's blocking durable-effect worker. Retaining
    // this gate across the callback keeps cancellation from reordering writes.
    write_gate: Mutex<()>,
    state: Mutex<ClusterCommandObservationState>,
}

struct ClusterCommandObservationState {
    active_operations: usize,
    success_in_flight: Option<u64>,
    failure_in_flight: Option<u64>,
    latest_sequence: u64,
    durable_sequence: u64,
    last_started: Option<Instant>,
    last_used: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClusterCommandObservationKind {
    Success,
    Failure,
}

pub struct ClusterCommandObservationReservation {
    slot: Arc<ClusterCommandObservationSlot>,
    sequence: u64,
    kind: ClusterCommandObservationKind,
    _task_permit: tokio::sync::OwnedSemaphorePermit,
}

impl ClusterCommandObservationCoordinator {
    pub fn production() -> Self {
        Self::new(
            CLUSTER_COMMAND_OBSERVATION_MAX_ENDPOINTS,
            CLUSTER_COMMAND_OBSERVATION_MAX_IN_FLIGHT,
            CLUSTER_COMMAND_SUCCESS_OBSERVATION_INTERVAL,
        )
    }

    pub fn new(max_slots: usize, max_tasks_in_flight: usize, success_interval: Duration) -> Self {
        assert!(
            max_slots > 0,
            "command observation slot capacity must be positive"
        );
        assert!(
            max_tasks_in_flight > 0,
            "command observation task capacity must be positive"
        );
        Self {
            slots: Mutex::new(HashMap::new()),
            task_permits: Arc::new(tokio::sync::Semaphore::new(max_tasks_in_flight)),
            sequence: AtomicU64::new(0),
            max_slots,
            success_interval,
        }
    }

    pub fn next_sequence(&self) -> u64 {
        let previous = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .expect("command observation sequence exhausted");
        previous + 1
    }

    pub fn try_reserve_success(
        self: &Arc<Self>,
        key: String,
        sequence: u64,
    ) -> Option<ClusterCommandObservationReservation> {
        let permit = Arc::clone(&self.task_permits).try_acquire_owned().ok()?;
        let now = Instant::now();
        let mut slots = lock_observation(&self.slots);
        let slot = self.slot_for_key(&mut slots, key, now)?;
        let mut state = lock_observation(&slot.state);
        state.last_used = now;
        if state.success_in_flight.is_some()
            || sequence <= state.latest_sequence
            || state
                .last_started
                .is_some_and(|last| now.saturating_duration_since(last) < self.success_interval)
        {
            return None;
        }
        state.active_operations = state
            .active_operations
            .checked_add(1)
            .expect("command observation active-operation count overflowed");
        state.success_in_flight = Some(sequence);
        state.latest_sequence = sequence;
        state.last_started = Some(now);
        drop(state);
        drop(slots);
        Some(ClusterCommandObservationReservation {
            slot,
            sequence,
            kind: ClusterCommandObservationKind::Success,
            _task_permit: permit,
        })
    }

    pub fn try_reserve_failure(
        self: &Arc<Self>,
        key: String,
        sequence: u64,
    ) -> Option<ClusterCommandObservationReservation> {
        let permit = Arc::clone(&self.task_permits).try_acquire_owned().ok()?;
        let now = Instant::now();
        let mut slots = lock_observation(&self.slots);
        let slot = self.slot_for_key(&mut slots, key, now)?;
        let mut state = lock_observation(&slot.state);
        state.latest_sequence = state.latest_sequence.max(sequence);
        state.last_used = now;
        if state.failure_in_flight.is_some() {
            return None;
        }
        state.active_operations = state
            .active_operations
            .checked_add(1)
            .expect("command observation active-operation count overflowed");
        state.failure_in_flight = Some(sequence);
        drop(state);
        drop(slots);
        Some(ClusterCommandObservationReservation {
            slot,
            sequence,
            kind: ClusterCommandObservationKind::Failure,
            _task_permit: permit,
        })
    }

    fn slot_for_key(
        &self,
        slots: &mut HashMap<String, Arc<ClusterCommandObservationSlot>>,
        key: String,
        now: Instant,
    ) -> Option<Arc<ClusterCommandObservationSlot>> {
        if let Some(slot) = slots.get(&key) {
            return Some(Arc::clone(slot));
        }
        if slots.len() >= self.max_slots && !Self::evict_oldest_inactive(slots) {
            return None;
        }
        let slot = Arc::new(ClusterCommandObservationSlot {
            write_gate: Mutex::new(()),
            state: Mutex::new(ClusterCommandObservationState {
                active_operations: 0,
                success_in_flight: None,
                failure_in_flight: None,
                latest_sequence: 0,
                durable_sequence: 0,
                last_started: None,
                last_used: now,
            }),
        });
        slots.insert(key, Arc::clone(&slot));
        Some(slot)
    }

    fn evict_oldest_inactive(
        slots: &mut HashMap<String, Arc<ClusterCommandObservationSlot>>,
    ) -> bool {
        let candidate = slots
            .iter()
            .filter_map(|(key, slot)| {
                let state = lock_observation(&slot.state);
                (state.active_operations == 0).then(|| (key.clone(), state.last_used))
            })
            .min_by_key(|(_, last_used)| *last_used)
            .map(|(key, _)| key);
        candidate.is_some_and(|key| slots.remove(&key).is_some())
    }
}

impl ClusterCommandObservationReservation {
    /// Runs the host's concrete durable publication only if this generation is
    /// still current, then advances the durable fence.
    pub fn publish<Publish>(self, publish: Publish)
    where
        Publish: FnOnce(),
    {
        let slot = Arc::clone(&self.slot);
        {
            let _write_guard = lock_observation(&slot.write_gate);
            let should_publish = {
                let state = lock_observation(&slot.state);
                match self.kind {
                    ClusterCommandObservationKind::Success => {
                        state.latest_sequence == self.sequence
                            && state.durable_sequence < self.sequence
                    }
                    ClusterCommandObservationKind::Failure => {
                        state.durable_sequence < self.sequence
                    }
                }
            };
            if should_publish {
                publish();
                let mut state = lock_observation(&slot.state);
                state.durable_sequence = state.durable_sequence.max(self.sequence);
                if self.kind == ClusterCommandObservationKind::Failure
                    && state.latest_sequence == self.sequence
                {
                    state.last_started = None;
                }
            }
        }
    }
}

impl Drop for ClusterCommandObservationReservation {
    fn drop(&mut self) {
        let mut state = lock_observation(&self.slot.state);
        if self.kind == ClusterCommandObservationKind::Success
            && state.success_in_flight == Some(self.sequence)
        {
            state.success_in_flight = None;
        }
        if self.kind == ClusterCommandObservationKind::Failure
            && state.failure_in_flight == Some(self.sequence)
        {
            state.failure_in_flight = None;
        }
        debug_assert!(state.active_operations > 0);
        state.active_operations = state.active_operations.saturating_sub(1);
        state.last_used = Instant::now();
    }
}

fn lock_observation<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_results_project_to_cluster_connectivity_evidence() {
        assert_eq!(
            ClusterCommandConnectivityObservation::from_rpc(ClusterObjectRpcObservation::success(
                Some(5)
            ),),
            ClusterCommandConnectivityObservation {
                connectivity_state: ClusterConnectivityState::Healthy,
                latency_ms: Some(5),
                bypass_failure_grace: false,
                command_auth_denied: false,
            }
        );
        assert!(
            ClusterCommandConnectivityObservation::from_rpc(
                ClusterObjectRpcObservation::hard_io_failure(),
            )
            .bypass_failure_grace
        );
        assert!(
            ClusterCommandConnectivityObservation::from_rpc(
                ClusterObjectRpcObservation::auth_denied(),
            )
            .command_auth_denied
        );
    }

    #[test]
    fn earlier_success_cannot_publish_after_failure_registration() {
        let coordinator = Arc::new(ClusterCommandObservationCoordinator::new(
            4,
            4,
            Duration::ZERO,
        ));
        let success_sequence = coordinator.next_sequence();
        let failure_sequence = coordinator.next_sequence();
        let failure = coordinator
            .try_reserve_failure("endpoint".to_string(), failure_sequence)
            .expect("failure reservation");

        assert!(
            coordinator
                .try_reserve_success("endpoint".to_string(), success_sequence)
                .is_none()
        );
        let mut writes = Vec::new();
        failure.publish(|| writes.push("failure"));

        let reserved_success = coordinator
            .try_reserve_success("reserved".to_string(), coordinator.next_sequence())
            .expect("reserved success");
        let later_failure = coordinator
            .try_reserve_failure("reserved".to_string(), coordinator.next_sequence())
            .expect("later failure reservation");
        reserved_success.publish(|| writes.push("stale success"));
        later_failure.publish(|| writes.push("later failure"));
        assert_eq!(writes, ["failure", "later failure"]);
    }

    #[test]
    fn cancelled_reservation_releases_slot_and_task_permit() {
        let coordinator = Arc::new(ClusterCommandObservationCoordinator::new(
            1,
            1,
            Duration::ZERO,
        ));
        let reservation = coordinator
            .try_reserve_success("endpoint".to_string(), coordinator.next_sequence())
            .expect("success reservation");
        assert_eq!(coordinator.task_permits.available_permits(), 0);

        drop(reservation);

        assert_eq!(coordinator.task_permits.available_permits(), 1);
        let recovered = coordinator
            .try_reserve_success("endpoint".to_string(), coordinator.next_sequence())
            .expect("reservation after cancellation cleanup");
        drop(recovered);
    }

    #[test]
    fn active_observation_slots_are_never_evicted_at_capacity() {
        let coordinator = Arc::new(ClusterCommandObservationCoordinator::new(
            2,
            3,
            Duration::ZERO,
        ));
        let first = coordinator
            .try_reserve_success("first".to_string(), coordinator.next_sequence())
            .expect("first reservation");
        let second = coordinator
            .try_reserve_success("second".to_string(), coordinator.next_sequence())
            .expect("second reservation");
        assert!(
            coordinator
                .try_reserve_success("third".to_string(), coordinator.next_sequence())
                .is_none()
        );

        drop(first);
        let third = coordinator
            .try_reserve_success("third".to_string(), coordinator.next_sequence())
            .expect("inactive slot should be evictable");
        let slots = lock_observation(&coordinator.slots);
        assert_eq!(slots.len(), 2);
        assert!(!slots.contains_key("first"));
        assert!(slots.contains_key("second"));
        assert!(slots.contains_key("third"));
        drop(slots);
        drop(second);
        drop(third);
    }

    #[test]
    fn success_observation_tasks_and_per_key_work_are_strictly_bounded() {
        let coordinator = Arc::new(ClusterCommandObservationCoordinator::new(
            8,
            2,
            Duration::ZERO,
        ));
        let first = coordinator
            .try_reserve_success("first".to_string(), coordinator.next_sequence())
            .expect("first reservation");
        assert!(
            coordinator
                .try_reserve_success("first".to_string(), coordinator.next_sequence())
                .is_none()
        );
        let second = coordinator
            .try_reserve_success("second".to_string(), coordinator.next_sequence())
            .expect("second reservation");
        assert_eq!(coordinator.task_permits.available_permits(), 0);
        assert!(
            coordinator
                .try_reserve_success("third".to_string(), coordinator.next_sequence())
                .is_none()
        );

        drop(first);
        let third = coordinator
            .try_reserve_success("third".to_string(), coordinator.next_sequence())
            .expect("released task permit");
        drop(second);
        drop(third);
    }

    #[test]
    fn failure_observation_storm_is_globally_and_per_key_bounded() {
        let coordinator = Arc::new(ClusterCommandObservationCoordinator::new(
            8,
            2,
            Duration::ZERO,
        ));
        let first = coordinator
            .try_reserve_failure("first".to_string(), coordinator.next_sequence())
            .expect("first failure");
        assert!(
            coordinator
                .try_reserve_failure("first".to_string(), coordinator.next_sequence())
                .is_none()
        );
        let second = coordinator
            .try_reserve_failure("second".to_string(), coordinator.next_sequence())
            .expect("second failure");
        for index in 0..1_024 {
            assert!(
                coordinator
                    .try_reserve_failure(
                        format!("failure-storm-{index}"),
                        coordinator.next_sequence(),
                    )
                    .is_none()
            );
        }
        assert_eq!(lock_observation(&coordinator.slots).len(), 2);

        drop(first);
        let replacement = coordinator
            .try_reserve_failure("first".to_string(), coordinator.next_sequence())
            .expect("released reservation");
        drop(second);
        drop(replacement);
        assert_eq!(coordinator.task_permits.available_permits(), 2);
    }
}
