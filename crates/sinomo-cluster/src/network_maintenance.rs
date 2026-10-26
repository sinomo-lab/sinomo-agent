use sinomo_base::ClusterNodeRole;
use sinomo_network::{
    CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL, ClusterNetworkMaintenanceSchedule,
    cluster_network_discovery_interval_for_role, cluster_network_maintenance_interval_remaining,
    cluster_network_maintenance_last_timestamp_for_next_due,
    cluster_network_probe_interval_for_role, cluster_network_publish_interval_for_role,
};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

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
    /// Earliest expiry in the accepted source observation set. Main callers
    /// bind live freshness to confirmed semantics; passive observers retain
    /// their own accepted local view. Speculative probes cannot advance it.
    pub accepted_connectivity_expiry: Option<OffsetDateTime>,
    /// One expiry-derived wake, consumed when its existing due classes claim
    /// work. Retries and bounded convergence retain their existing owners.
    pub connectivity_renewal_at: Option<Instant>,
    /// Keeps an unchanged overdue minimum from masking later expiries or
    /// repeatedly scheduling a source set that has no further eligible work.
    pub connectivity_overdue_claimed: bool,
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
            accepted_connectivity_expiry: None,
            connectivity_renewal_at: None,
            connectivity_overdue_claimed: false,
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
        } else {
            let ordinary = if convergence_active {
                CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL
            } else {
                cluster_network_maintenance_interval_remaining(
                    now,
                    self.last_probe,
                    cluster_network_probe_interval_for_role(&self.local_node_role),
                )
            };
            ordinary.min(
                self.connectivity_renewal_at
                    .map_or(Duration::MAX, |at| at.saturating_duration_since(now)),
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

    pub fn observe_accepted_connectivity_expiries(
        &mut self,
        expiries: impl IntoIterator<Item = OffsetDateTime>,
        wall_now: OffsetDateTime,
        now: Instant,
    ) {
        let margin =
            time::Duration::seconds(sinomo_network::CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS);
        let mut expiry = None;
        let mut future = None;
        for value in expiries {
            expiry = Some(expiry.map_or(value, |old: OffsetDateTime| old.min(value)));
            if value > wall_now + margin {
                future = Some(future.map_or(value, |old: OffsetDateTime| old.min(value)));
            }
        }
        if self.accepted_connectivity_expiry != expiry
            || expiry.is_some_and(|value| value > wall_now + margin)
        {
            self.connectivity_overdue_claimed = false;
        }
        // An inactive overdue row must neither spin nor hide a later healthy
        // renewal. Its claimed work is owned by retry/convergence; the next
        // future expiry still gets an independent wake from this confirmed set.
        let selected = if self.connectivity_overdue_claimed {
            future
        } else {
            expiry
        };
        let renewal_at = selected.and_then(|expiry| {
            let remaining = (expiry
                - wall_now
                - time::Duration::seconds(
                    sinomo_network::CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS,
                ))
            .max(time::Duration::ZERO);
            Duration::try_from(remaining)
                .ok()
                .and_then(|delay| now.checked_add(delay))
        });
        if self.accepted_connectivity_expiry != expiry || selected.is_none() {
            self.connectivity_renewal_at = renewal_at;
        } else if let Some(at) = renewal_at {
            self.connectivity_renewal_at = match self.connectivity_renewal_at {
                // A forward wall-clock adjustment may accelerate this same
                // confirmed expiry, but a repeated read cannot postpone it.
                Some(existing) => Some(existing.min(at)),
                // A consumed overdue generation must not rearm a zero-delay
                // loop on unchanged data. A backwards clock adjustment can
                // make that same confirmed expiry genuinely future again.
                None if at > now => Some(at),
                None => None,
            };
        }
        self.accepted_connectivity_expiry = expiry;
    }

    pub fn connectivity_renewal_due(&self, now: Instant) -> bool {
        self.connectivity_renewal_at.is_some_and(|at| at <= now)
            && !self.connectivity_publish_retry_waiting(now)
            && !self.probe_retry_waiting(now)
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
        if (probe || connectivity_publish)
            && self.connectivity_renewal_at.is_some_and(|at| at <= now)
        {
            // Consume only the wake, never the accepted expiry. Failed or
            // cancelled work keeps the existing due-class retry ownership;
            // bounded unfinished probe batches keep convergence ownership.
            self.connectivity_renewal_at = None;
            self.connectivity_overdue_claimed = true;
        }
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

    pub fn finish_connectivity_convergence(&mut self, superseded: bool, now: Instant) {
        self.connectivity_converged = true;
        // Close an initialized window without making it look uninitialized.
        // Ordinary retries/reconvergence must not soften established failures
        // again. A real environment refresh explicitly installs a new window.
        self.startup_settle_until = self
            .startup_settle_until
            .map(|deadline| deadline.min(now));
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
    fn completed_settle_does_not_restart_for_ordinary_reconvergence() {
        for reason in 0..3 {
            let start = Instant::now();
            let mut cadence =
                ClusterNetworkMaintenanceCadence::immediate(start, ClusterNodeRole::Workstation);
            cadence.ensure_connectivity_settle_deadline(start, false);
            assert!(cadence.startup_settle_until.is_some_and(|at| at > start));
            cadence.finish_connectivity_convergence(false, start);
            let later = Instant::now() + Duration::from_secs(1);
            match reason {
                0 => cadence.request_connectivity_refresh(later),
                1 => cadence.record_connectivity_topology_superseded(),
                _ => cadence.retain_superseding_connectivity_source_generation(later),
            }
            cadence.ensure_connectivity_settle_deadline(later, false);
            assert!(
                cadence.startup_settle_until.is_none_or(|at| at <= later),
                "ordinary follow-up {reason} must not create a new startup failure-grace window"
            );
            assert!(!cadence.connectivity_converged);
        }
    }

    #[test]
    fn completed_settle_still_allows_initial_and_explicit_environment_grace() {
        let start = Instant::now();
        let mut cadence =
            ClusterNetworkMaintenanceCadence::immediate(start, ClusterNodeRole::Sensor);
        cadence.ensure_connectivity_settle_deadline(start, true);
        assert!(cadence.startup_settle_until.is_none());
        cadence.ensure_connectivity_settle_deadline(start, false);
        assert_eq!(
            cadence.startup_settle_until,
            Some(start + CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION)
        );
        cadence.finish_connectivity_convergence(false, start);
        let changed = Instant::now() + Duration::from_secs(1);
        cadence.request_network_environment_refresh(changed);
        let expected = changed + CLUSTER_NETWORK_MAINTENANCE_STARTUP_SETTLE_DURATION;
        assert_eq!(cadence.startup_settle_until, Some(expected));
        cadence.ensure_connectivity_settle_deadline(expected + Duration::from_secs(1), false);
        assert_eq!(
            cadence.startup_settle_until,
            Some(expected),
            "an expired active window remains a consumed marker"
        );
    }

    #[test]
    fn confirmed_expiry_wakes_at_existing_margin_across_roles_and_clock_phases() {
        let origin = Instant::now();
        let wall = OffsetDateTime::UNIX_EPOCH + time::Duration::days(1);
        for role in [
            ClusterNodeRole::Workstation,
            ClusterNodeRole::Mobile,
            ClusterNodeRole::Sensor,
        ] {
            for ttl in [900_i64, 7200] {
                for phase in [0, ttl - 121, ttl - 120, ttl - 60, ttl + 1] {
                    let now = origin + Duration::from_secs(phase as u64);
                    let mut cadence =
                        ClusterNetworkMaintenanceCadence::immediate(now, role.clone());
                    cadence.connectivity_converged = true;
                    cadence.last_publish = now;
                    cadence.last_probe = now;
                    cadence.last_discovery = now;
                    cadence.observe_accepted_connectivity_expiries(
                        Some(wall + time::Duration::seconds(ttl)),
                        wall + time::Duration::seconds(phase),
                        now,
                    );
                    let renewal = Duration::from_secs((ttl - 120 - phase).max(0) as u64);
                    let ordinary = cluster_network_probe_interval_for_role(&role)
                        .min(cluster_network_publish_interval_for_role(&role));
                    assert_eq!(
                        cadence.next_due_duration(now, false),
                        ordinary.min(renewal),
                        "confirmed expiry must wake before the next role audit: {role:?}, ttl={ttl}, phase={phase}"
                    );
                }
            }
        }
    }

    fn renewal_cadence(now: Instant) -> ClusterNetworkMaintenanceCadence {
        let mut cadence = ClusterNetworkMaintenanceCadence::immediate(now, ClusterNodeRole::Sensor);
        cadence.connectivity_converged = true;
        cadence.last_publish = now;
        cadence.last_probe = now;
        cadence.last_discovery = now;
        cadence
    }

    #[test]
    fn confirmed_expiry_claim_preserves_backoff_and_cancelled_plan_ownership() {
        let now = Instant::now();
        let wall = OffsetDateTime::UNIX_EPOCH + time::Duration::days(1);
        let expiry = wall + time::Duration::seconds(120);
        let mut cadence = renewal_cadence(now);
        cadence.observe_accepted_connectivity_expiries(Some(expiry), wall, now);
        assert!(cadence.connectivity_renewal_due(now));
        cadence.claim_due_classes(now, false, true, false, true);
        assert_eq!(cadence.accepted_connectivity_expiry, Some(expiry));
        assert!(cadence.connectivity_renewal_at.is_none());
        // The reactor reports cancellation/timeout of a claimed planning turn
        // through this existing owner boundary, once for each due class.
        cadence.record_active_tick_plan_failure(now);
        assert_eq!(cadence.probe_failure_streak, 1);
        assert_eq!(cadence.connectivity_publish_failure_streak, 1);
        assert_eq!(
            cadence.next_due_duration(now, false),
            Duration::from_secs(1)
        );
        assert!(!cadence.connectivity_renewal_due(now));
        let retry = now + Duration::from_secs(1);
        assert_eq!(
            cadence.connectivity_publish_retry_due_duration(retry),
            Some(Duration::ZERO)
        );
        cadence.claim_due_classes(retry, false, true, false, true);
        cadence.record_active_tick_plan_failure(retry);
        assert_eq!(
            cadence.next_due_duration(retry, false),
            Duration::from_secs(2)
        );
        assert_eq!(cadence.accepted_connectivity_expiry, Some(expiry));
        let completed_retry = retry + Duration::from_secs(2);
        cadence.claim_due_classes(completed_retry, false, true, false, true);
        cadence.take_probe_claim();
        cadence.take_connectivity_publish_claim();
        cadence.record_probe_success();
        cadence.record_connectivity_publish_success(false);
        cadence.connectivity_converged = true;
        cadence.observe_accepted_connectivity_expiries(
            Some(wall + time::Duration::seconds(900)),
            wall + time::Duration::seconds(3),
            completed_retry,
        );
        assert_eq!(
            cadence.next_due_duration(completed_retry, false),
            Duration::from_secs(777)
        );
    }

    #[test]
    fn confirmed_expiry_does_not_bypass_an_existing_probe_or_publication_backoff() {
        let now = Instant::now();
        let wall = OffsetDateTime::UNIX_EPOCH;
        let mut cadence = renewal_cadence(now);
        cadence.observe_accepted_connectivity_expiries(Some(wall), wall, now);
        cadence.probe_retry_at = Some(now + Duration::from_secs(8));
        assert!(!cadence.connectivity_renewal_due(now));
        assert_eq!(
            cadence.next_due_duration(now, false),
            Duration::from_secs(8)
        );
        cadence.connectivity_publish_retry_at = Some(now + Duration::from_secs(4));
        assert_eq!(
            cadence.next_due_duration(now, false),
            Duration::from_secs(4)
        );
        assert!(!cadence.connectivity_renewal_due(now + Duration::from_secs(4)));
        // The existing independent publication retry can run at four seconds;
        // it must not force an unrelated backed-off probe before eight seconds.
        assert_eq!(
            cadence.connectivity_publish_retry_due_duration(now + Duration::from_secs(4)),
            Some(Duration::ZERO)
        );
        assert!(cadence.connectivity_renewal_due(now + Duration::from_secs(8)));
    }

    #[test]
    fn confirmed_expiry_unchanged_overdue_set_cannot_create_a_busy_loop() {
        let now = Instant::now();
        let wall = OffsetDateTime::UNIX_EPOCH + time::Duration::days(1);
        let expiry = wall - time::Duration::seconds(1);
        let mut cadence = renewal_cadence(now);
        cadence.observe_accepted_connectivity_expiries(Some(expiry), wall, now);
        assert_eq!(cadence.next_due_duration(now, false), Duration::ZERO);
        cadence.claim_due_classes(now, false, true, false, true);
        cadence.take_probe_claim();
        cadence.take_connectivity_publish_claim();
        cadence.finish_connectivity_convergence(false, now);
        for elapsed in [0, 1, 5, 30] {
            let at = now + Duration::from_secs(elapsed);
            cadence.observe_accepted_connectivity_expiries(
                Some(expiry),
                wall + time::Duration::seconds(elapsed as i64),
                at,
            );
            assert!(!cadence.connectivity_renewal_due(at));
            assert_eq!(
                cadence.next_due_duration(at, false),
                Duration::from_secs(900 - elapsed)
            );
        }
        // Inactive retained records keep their actual expiry; consuming the
        // scheduler edge never makes them live or invents a renewed value.
        assert!(!sinomo_network::cluster_network_record_live(expiry, wall));
        cadence.observe_accepted_connectivity_expiries(None, wall, now);
        assert!(cadence.accepted_connectivity_expiry.is_none());
        assert!(cadence.connectivity_renewal_at.is_none());
    }

    #[test]
    fn confirmed_expiry_clock_adjustments_do_not_postpone_or_spin_a_consumed_deadline() {
        let now = Instant::now();
        let wall = OffsetDateTime::UNIX_EPOCH + time::Duration::days(1);
        let expiry = wall + time::Duration::seconds(180);
        let mut cadence = renewal_cadence(now);
        cadence.observe_accepted_connectivity_expiries(Some(expiry), wall, now);
        let deadline = cadence.connectivity_renewal_at;
        cadence.observe_accepted_connectivity_expiries(
            Some(expiry),
            wall - time::Duration::seconds(30),
            now,
        );
        assert_eq!(cadence.connectivity_renewal_at, deadline);
        cadence.observe_accepted_connectivity_expiries(
            Some(expiry),
            wall + time::Duration::seconds(60),
            now,
        );
        assert!(cadence.connectivity_renewal_due(now));
        cadence.claim_due_classes(now, false, true, false, true);
        cadence.observe_accepted_connectivity_expiries(
            Some(expiry),
            wall + time::Duration::seconds(61),
            now,
        );
        assert!(cadence.connectivity_renewal_at.is_none());
        cadence.observe_accepted_connectivity_expiries(Some(expiry), wall, now);
        assert_eq!(
            cadence.connectivity_renewal_at,
            Some(now + Duration::from_secs(60))
        );
    }

    #[test]
    fn confirmed_expiry_bounded_followup_keeps_existing_convergence_cadence() {
        let now = Instant::now();
        let wall = OffsetDateTime::UNIX_EPOCH;
        let mut cadence = renewal_cadence(now);
        cadence.observe_accepted_connectivity_expiries(Some(wall), wall, now);
        cadence.claim_due_classes(now, false, true, false, true);
        cadence.take_probe_claim();
        cadence.take_connectivity_publish_claim();
        cadence.connectivity_converged =
            !cluster_connectivity_probe_result_needs_followup(31, false, false);
        cadence.record_connectivity_publish_success(false);
        cadence.observe_accepted_connectivity_expiries(Some(wall), wall, now);
        assert_eq!(
            cadence.next_due_duration(now, false),
            CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL
        );
        assert_eq!(
            cluster_connectivity_probe_plan_action(false, 0, 0, false, true),
            ClusterConnectivityProbePlanAction::PublishRetained
        );
    }

    #[test]
    fn confirmed_expiry_overdue_retained_row_does_not_hide_later_renewals() {
        let now = Instant::now();
        let wall = OffsetDateTime::UNIX_EPOCH + time::Duration::days(1);
        let overdue = wall - time::Duration::seconds(10);
        let later = wall + time::Duration::seconds(180);
        let mut cadence = renewal_cadence(now);
        cadence.observe_accepted_connectivity_expiries([overdue, later], wall, now);
        assert!(cadence.connectivity_renewal_due(now));
        cadence.claim_due_classes(now, false, true, false, true);
        cadence.take_probe_claim();
        cadence.take_connectivity_publish_claim();
        cadence.finish_connectivity_convergence(false, now);
        cadence.observe_accepted_connectivity_expiries([overdue, later], wall, now);
        assert_eq!(
            cadence.next_due_duration(now, false),
            Duration::from_secs(60)
        );
        let due = now + Duration::from_secs(60);
        assert!(cadence.connectivity_renewal_due(due));
        cadence.claim_due_classes(due, false, true, false, true);
        cadence.observe_accepted_connectivity_expiries(
            [overdue, later],
            wall + time::Duration::seconds(60),
            due,
        );
        assert!(
            cadence.connectivity_renewal_at.is_none(),
            "unchanged overdue rows do not rearm a loop"
        );
        cadence.observe_accepted_connectivity_expiries(
            [overdue, wall + time::Duration::seconds(960)],
            wall + time::Duration::seconds(60),
            due,
        );
        assert_eq!(
            cadence.next_due_duration(due, false),
            Duration::from_secs(780)
        );
    }

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
