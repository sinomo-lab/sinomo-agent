use sinomo_base::ClusterNodeRole;
use sinomo_network::{
    CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL, ClusterNetworkMaintenanceSchedule,
    cluster_network_discovery_interval_for_role, cluster_network_maintenance_interval_remaining,
    cluster_network_maintenance_last_timestamp_for_next_due,
    cluster_network_probe_interval_for_role, cluster_network_publish_interval_for_role,
};
use std::time::{Duration, Instant};

/// Longest retry delay for a failed cluster-network maintenance due class.
pub const CLUSTER_NETWORK_MAINTENANCE_FAILURE_BACKOFF: Duration = Duration::from_secs(600);
/// First retry delay for a failed cluster-network maintenance due class.
pub const CLUSTER_NETWORK_MAINTENANCE_FAILURE_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Bounded window in which a changed local network environment is allowed to
/// drive the fast convergence cadence.
pub const CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION: Duration = Duration::from_secs(8);

/// Work classes claimed by one cluster-network maintenance generation.
///
/// Claims advance their class timestamp before host effects begin, ensuring a
/// cancelled or failed host turn cannot spin an already-overdue generation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClusterNetworkMaintenanceDueClaims {
    pub publish: bool,
    pub probe: bool,
    pub discovery: bool,
    pub connectivity_publish: bool,
}

/// Host action selected for one due connectivity-probe generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterConnectivityProbePlanAction {
    Probe,
    PublishRetained,
    None,
}

/// Chooses whether a due generation observes endpoints, republishes the
/// retained durable set, or does no work. The host supplies only the bounded
/// plan facts produced by its concrete probe adapter.
pub fn cluster_connectivity_probe_plan_action(
    force_probe: bool,
    planned_entries: usize,
    remaining_due: usize,
    convergence_active: bool,
    retained_publish_due: bool,
) -> ClusterConnectivityProbePlanAction {
    if force_probe || planned_entries > 0 || remaining_due > 0 {
        ClusterConnectivityProbePlanAction::Probe
    } else if convergence_active || retained_publish_due {
        ClusterConnectivityProbePlanAction::PublishRetained
    } else {
        ClusterConnectivityProbePlanAction::None
    }
}

/// Returns whether bounded work remains or a degraded observation still needs
/// one accelerated convergence turn.
pub fn cluster_connectivity_probe_result_needs_followup(
    remaining_due: usize,
    convergence_active: bool,
    probed_degraded: bool,
) -> bool {
    remaining_due > 0 || (convergence_active && probed_degraded)
}

impl ClusterNetworkMaintenanceDueClaims {
    pub fn is_empty(self) -> bool {
        !(self.publish || self.probe || self.discovery || self.connectivity_publish)
    }
}

/// Owner-side state machine for cluster-network maintenance scheduling.
///
/// The cluster crate owns cadence, retry/backoff, convergence, and reactive
/// input generations. Runtime hosts retain only concrete effects such as mDNS
/// sockets, storage fences, probes, and durable publication payloads.
#[derive(Clone, Debug)]
pub struct ClusterNetworkMaintenanceCadence {
    pub connectivity_converged: bool,
    pub local_node_role: ClusterNodeRole,
    pub startup_settle_until: Option<Instant>,
    pub publish_failure_streak: u32,
    pub probe_failure_streak: u32,
    pub discovery_failure_streak: u32,
    pub probe_retry_at: Option<Instant>,
    pub discovery_retry_at: Option<Instant>,
    pub connectivity_publish_failure_streak: u32,
    pub connectivity_publish_retry_at: Option<Instant>,
    pub connectivity_input_generation: u64,
    pub connectivity_force_probe: bool,
    pub last_publish: Instant,
    pub last_discovery: Instant,
    pub last_probe: Instant,
    pub active_due_claims: ClusterNetworkMaintenanceDueClaims,
}

impl ClusterNetworkMaintenanceCadence {
    /// Starts with every ordinary maintenance class immediately due.
    pub fn immediate(now: Instant, local_node_role: ClusterNodeRole) -> Self {
        let last_publish = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_publish_interval_for_role(&local_node_role),
            Duration::ZERO,
        );
        let last_discovery = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_discovery_interval_for_role(&local_node_role),
            Duration::ZERO,
        );
        let last_probe = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_probe_interval_for_role(&local_node_role),
            Duration::ZERO,
        );
        Self {
            connectivity_converged: false,
            local_node_role,
            startup_settle_until: None,
            publish_failure_streak: 0,
            probe_failure_streak: 0,
            discovery_failure_streak: 0,
            probe_retry_at: None,
            discovery_retry_at: None,
            connectivity_publish_failure_streak: 0,
            connectivity_publish_retry_at: None,
            connectivity_input_generation: 0,
            connectivity_force_probe: false,
            last_publish,
            last_discovery,
            last_probe,
            active_due_claims: ClusterNetworkMaintenanceDueClaims::default(),
        }
    }

    pub fn schedule(&self, has_discovery: bool) -> ClusterNetworkMaintenanceSchedule {
        ClusterNetworkMaintenanceSchedule {
            local_node_role: self.local_node_role.clone(),
            has_discovery,
            connectivity_converged: self.connectivity_converged,
            startup_settle_until: self.startup_settle_until,
            last_publish: self.last_publish,
            last_probe: self.last_probe,
            last_discovery: self.last_discovery,
        }
    }

    pub fn schedule_with_connectivity_publish_backoff(
        &self,
        now: Instant,
        has_discovery: bool,
    ) -> ClusterNetworkMaintenanceSchedule {
        let mut schedule = self.schedule(has_discovery);
        if self.connectivity_publish_retry_waiting(now) {
            // The retained publication owns its retry deadline. Suppress the
            // fast convergence cadence without delaying ordinary publication.
            schedule.connectivity_converged = true;
            schedule.startup_settle_until = None;
        }
        schedule
    }

    pub fn next_due_duration(&self, now: Instant, has_discovery: bool) -> Duration {
        let publish_due = cluster_network_maintenance_interval_remaining(
            now,
            self.last_publish,
            cluster_network_publish_interval_for_role(&self.local_node_role),
        );
        let convergence_active = !self.connectivity_converged
            || self
                .startup_settle_until
                .is_some_and(|deadline| now < deadline);
        let connectivity_retry = self
            .connectivity_publish_retry_at
            .map(|retry_at| retry_at.saturating_duration_since(now));
        let probe_due = if let Some(connectivity_retry) = connectivity_retry {
            connectivity_retry
        } else if let Some(retry_at) = self.probe_retry_at {
            retry_at.saturating_duration_since(now)
        } else if convergence_active {
            CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL
        } else {
            cluster_network_maintenance_interval_remaining(
                now,
                self.last_probe,
                cluster_network_probe_interval_for_role(&self.local_node_role),
            )
        };
        let discovery_due = if !has_discovery {
            Duration::MAX
        } else if let Some(retry_at) = self.discovery_retry_at {
            retry_at.saturating_duration_since(now)
        } else if convergence_active && connectivity_retry.is_none() {
            CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL
        } else {
            cluster_network_maintenance_interval_remaining(
                now,
                self.last_discovery,
                cluster_network_discovery_interval_for_role(&self.local_node_role),
            )
        };
        publish_due.min(probe_due).min(discovery_due)
    }

    pub fn has_active_due_claims(&self) -> bool {
        !self.active_due_claims.is_empty()
    }

    pub fn discovery_claimed(&self) -> bool {
        self.active_due_claims.discovery
    }

    pub fn claim_due_classes(
        &mut self,
        now: Instant,
        publish: bool,
        probe: bool,
        discovery: bool,
        connectivity_publish: bool,
    ) {
        self.active_due_claims = ClusterNetworkMaintenanceDueClaims {
            publish,
            probe,
            discovery,
            connectivity_publish,
        };
        if publish {
            self.last_publish = now;
        }
        if probe {
            self.last_probe = now;
        }
        if discovery {
            self.last_discovery = now;
        }
    }

    pub fn take_publish_claim(&mut self) -> bool {
        std::mem::take(&mut self.active_due_claims.publish)
    }

    pub fn take_probe_claim(&mut self) -> bool {
        std::mem::take(&mut self.active_due_claims.probe)
    }

    pub fn take_discovery_claim(&mut self) -> bool {
        std::mem::take(&mut self.active_due_claims.discovery)
    }

    pub fn take_connectivity_publish_claim(&mut self) -> bool {
        std::mem::take(&mut self.active_due_claims.connectivity_publish)
    }

    pub fn complete_discovery_claim(&mut self) {
        self.take_discovery_claim();
        self.record_discovery_success();
    }

    pub fn record_active_tick_plan_failure(&mut self, now: Instant) {
        let claims = std::mem::take(&mut self.active_due_claims);
        if claims.publish {
            self.record_publish_failure_at(now);
        }
        if claims.probe {
            self.record_probe_failure_at(now);
        }
        if claims.discovery {
            self.record_discovery_failure_at(now);
        }
        if claims.connectivity_publish {
            self.record_connectivity_publish_failure_at(now);
        }
    }

    pub fn request_network_environment_refresh(&mut self, now: Instant) {
        self.request_connectivity_refresh(now);
        self.startup_settle_until =
            now.checked_add(CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION);
    }

    pub fn ensure_connectivity_settle_deadline(&mut self, now: Instant, settled_solo: bool) {
        if !self.connectivity_converged && self.startup_settle_until.is_none() && !settled_solo {
            self.startup_settle_until =
                now.checked_add(CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION);
        }
    }

    pub fn request_publish_refresh(&mut self, now: Instant) {
        self.last_publish = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_publish_interval_for_role(&self.local_node_role),
            Duration::ZERO,
        );
    }

    pub fn request_connectivity_refresh(&mut self, now: Instant) {
        self.connectivity_input_generation = self.connectivity_input_generation.wrapping_add(1);
        self.connectivity_converged = false;
        self.connectivity_force_probe = true;
        if self.connectivity_publish_retry_at.is_some() {
            self.connectivity_publish_retry_at = Some(now);
        }
        if self.probe_retry_at.is_some() {
            self.probe_retry_at = Some(now);
        }
        self.last_probe = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_probe_interval_for_role(&self.local_node_role),
            Duration::ZERO,
        );
    }

    pub fn request_discovery_refresh(&mut self, now: Instant) {
        if self.discovery_retry_at.is_some() {
            self.discovery_retry_at = Some(now);
        }
        self.last_discovery = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_discovery_interval_for_role(&self.local_node_role),
            Duration::ZERO,
        );
    }

    pub fn record_publish_failure(&mut self, now: Instant) {
        self.record_publish_failure_at(now);
    }

    pub fn record_publish_failure_at(&mut self, now: Instant) {
        let delay = self.record_publish_failure_delay();
        self.defer_publish_retry(now, delay);
    }

    pub fn record_probe_failure_at(&mut self, now: Instant) {
        self.probe_failure_streak = self.probe_failure_streak.saturating_add(1);
        let delay = cluster_network_due_class_failure_backoff(self.probe_failure_streak);
        self.probe_retry_at = now.checked_add(delay);
        self.last_probe = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_probe_interval_for_role(&self.local_node_role),
            delay,
        );
    }

    pub fn record_discovery_failure_at(&mut self, now: Instant) {
        self.discovery_failure_streak = self.discovery_failure_streak.saturating_add(1);
        let delay = cluster_network_due_class_failure_backoff(self.discovery_failure_streak);
        self.discovery_retry_at = now.checked_add(delay);
        self.last_discovery = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_discovery_interval_for_role(&self.local_node_role),
            delay,
        );
    }

    pub fn record_probe_success(&mut self) {
        self.probe_failure_streak = 0;
        self.probe_retry_at = None;
    }

    pub fn record_discovery_success(&mut self) {
        self.discovery_failure_streak = 0;
        self.discovery_retry_at = None;
    }

    pub fn probe_retry_waiting(&self, now: Instant) -> bool {
        self.probe_retry_at.is_some_and(|retry_at| retry_at > now)
    }

    pub fn probe_retry_due(&self, now: Instant) -> bool {
        self.probe_retry_at.is_some_and(|retry_at| retry_at <= now)
    }

    pub fn discovery_retry_waiting(&self, now: Instant) -> bool {
        self.discovery_retry_at
            .is_some_and(|retry_at| retry_at > now)
    }

    pub fn discovery_retry_due(&self, now: Instant) -> bool {
        self.discovery_retry_at
            .is_some_and(|retry_at| retry_at <= now)
    }

    pub fn record_connectivity_publish_failure(&mut self, now: Instant) {
        self.record_connectivity_publish_failure_at(now);
    }

    pub fn record_connectivity_publish_failure_at(&mut self, now: Instant) {
        self.connectivity_converged = false;
        self.connectivity_publish_failure_streak =
            self.connectivity_publish_failure_streak.saturating_add(1);
        let delay = self.connectivity_publish_failure_backoff();
        self.connectivity_publish_retry_at = now.checked_add(delay);
    }

    pub fn clear_connectivity_publish_retry(&mut self) {
        self.connectivity_publish_failure_streak = 0;
        self.connectivity_publish_retry_at = None;
    }

    pub fn record_connectivity_topology_superseded(&mut self) {
        self.clear_connectivity_publish_retry();
        self.connectivity_converged = false;
        self.connectivity_force_probe = true;
    }

    pub fn retain_superseding_connectivity_source_generation(&mut self, now: Instant) {
        self.clear_connectivity_publish_retry();
        self.connectivity_converged = false;
        self.connectivity_force_probe = false;
        self.connectivity_publish_retry_at = Some(now);
    }

    pub fn record_connectivity_publish_success(&mut self, superseded: bool) {
        self.clear_connectivity_publish_retry();
        self.connectivity_force_probe = superseded;
        if superseded {
            self.connectivity_converged = false;
        }
    }

    pub fn finish_connectivity_convergence(&mut self, superseded: bool) {
        self.connectivity_converged = true;
        self.startup_settle_until = None;
        self.record_connectivity_publish_success(superseded);
    }

    pub fn connectivity_publish_retry_waiting(&self, now: Instant) -> bool {
        self.connectivity_publish_retry_at
            .is_some_and(|retry_at| retry_at > now)
    }

    pub fn connectivity_publish_retry_due_duration(&self, now: Instant) -> Option<Duration> {
        self.connectivity_publish_retry_at
            .map(|retry_at| retry_at.saturating_duration_since(now))
    }

    pub fn record_publish_failure_delay(&mut self) -> Duration {
        self.publish_failure_streak = self.publish_failure_streak.saturating_add(1);
        self.publish_failure_backoff()
    }

    pub fn defer_publish_retry(&mut self, now: Instant, delay: Duration) {
        self.last_publish = cluster_network_maintenance_last_timestamp_for_next_due(
            now,
            cluster_network_publish_interval_for_role(&self.local_node_role),
            delay,
        );
    }

    pub fn publish_failure_backoff(&self) -> Duration {
        cluster_network_due_class_failure_backoff(self.publish_failure_streak)
    }

    pub fn connectivity_publish_failure_backoff(&self) -> Duration {
        cluster_network_due_class_failure_backoff(self.connectivity_publish_failure_streak)
    }

    pub fn record_publish_success(&mut self) {
        self.publish_failure_streak = 0;
    }
}

pub fn cluster_network_due_class_failure_backoff(failure_streak: u32) -> Duration {
    let exponent = failure_streak.saturating_sub(1).min(10);
    CLUSTER_NETWORK_MAINTENANCE_FAILURE_INITIAL_BACKOFF
        .saturating_mul(1_u32 << exponent)
        .min(CLUSTER_NETWORK_MAINTENANCE_FAILURE_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reactive_refresh_preserves_failure_streak_and_accelerates_waiting_retries() {
        let now = Instant::now();
        let mut cadence =
            ClusterNetworkMaintenanceCadence::immediate(now, ClusterNodeRole::Workstation);
        cadence.connectivity_publish_failure_streak = 3;
        cadence.connectivity_publish_retry_at = now.checked_add(Duration::from_secs(4));
        cadence.probe_failure_streak = 2;
        cadence.probe_retry_at = now.checked_add(Duration::from_secs(2));

        cadence.request_connectivity_refresh(now);

        assert_eq!(cadence.connectivity_input_generation, 1);
        assert_eq!(cadence.connectivity_publish_failure_streak, 3);
        assert_eq!(cadence.probe_failure_streak, 2);
        assert_eq!(cadence.connectivity_publish_retry_at, Some(now));
        assert_eq!(cadence.probe_retry_at, Some(now));
        assert!(cadence.connectivity_force_probe);
        assert!(!cadence.connectivity_converged);
    }

    #[test]
    fn failed_claims_advance_each_due_class_once() {
        let now = Instant::now();
        let mut cadence =
            ClusterNetworkMaintenanceCadence::immediate(now, ClusterNodeRole::Workstation);
        cadence.claim_due_classes(now, true, true, true, true);

        cadence.record_active_tick_plan_failure(now);

        assert_eq!(cadence.publish_failure_streak, 1);
        assert_eq!(cadence.probe_failure_streak, 1);
        assert_eq!(cadence.discovery_failure_streak, 1);
        assert_eq!(cadence.connectivity_publish_failure_streak, 1);
        assert!(!cadence.has_active_due_claims());
    }

    #[test]
    fn retained_publication_backoff_masks_fast_convergence_only() {
        let now = Instant::now();
        let mut cadence =
            ClusterNetworkMaintenanceCadence::immediate(now, ClusterNodeRole::Workstation);
        cadence.connectivity_converged = false;
        cadence.startup_settle_until = now.checked_add(Duration::from_secs(8));
        cadence.connectivity_publish_retry_at = now.checked_add(Duration::from_secs(5));

        let schedule = cadence.schedule_with_connectivity_publish_backoff(now, true);

        assert!(schedule.connectivity_converged);
        assert_eq!(schedule.startup_settle_until, None);
        assert_eq!(cadence.next_due_duration(now, true), Duration::ZERO);
    }

    #[test]
    fn probe_plan_action_preserves_retained_publish_semantics() {
        assert_eq!(
            cluster_connectivity_probe_plan_action(false, 0, 0, false, false),
            ClusterConnectivityProbePlanAction::None,
        );
        assert_eq!(
            cluster_connectivity_probe_plan_action(false, 0, 0, false, true),
            ClusterConnectivityProbePlanAction::PublishRetained,
        );
        assert_eq!(
            cluster_connectivity_probe_plan_action(false, 0, 0, true, false),
            ClusterConnectivityProbePlanAction::PublishRetained,
        );
        assert_eq!(
            cluster_connectivity_probe_plan_action(true, 0, 0, true, false),
            ClusterConnectivityProbePlanAction::Probe,
        );
    }

    #[test]
    fn probe_followup_tracks_bounded_work_and_degraded_convergence() {
        assert!(cluster_connectivity_probe_result_needs_followup(
            1, false, false,
        ));
        assert!(cluster_connectivity_probe_result_needs_followup(
            0, true, true,
        ));
        assert!(!cluster_connectivity_probe_result_needs_followup(
            0, false, true,
        ));
        assert!(!cluster_connectivity_probe_result_needs_followup(
            0, true, false,
        ));
    }
}
