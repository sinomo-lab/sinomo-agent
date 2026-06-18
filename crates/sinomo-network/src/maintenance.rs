use sinomo_core::{ClusterBootstrapState, ClusterNodeRole};
use std::time::{Duration, Instant};

pub const CLUSTER_NETWORK_STABLE_REFRESH_MARGIN_SECONDS: i64 = 60;
pub const CLUSTER_NETWORK_PUBLISH_INTERVAL: Duration = Duration::from_secs(60);
pub const CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL: Duration = Duration::from_secs(300);
pub const CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL: Duration = Duration::from_secs(900);
pub const CLUSTER_NETWORK_DISCOVERY_INTERVAL: Duration = CLUSTER_NETWORK_PUBLISH_INTERVAL;
pub const CLUSTER_NETWORK_PROBE_INTERVAL: Duration = Duration::from_secs(60);
pub const CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL: Duration = Duration::from_secs(300);
pub const CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL: Duration = Duration::from_secs(900);
pub const CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL: Duration = Duration::from_millis(250);
pub const CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE: u64 = 8;
pub const CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK: usize = 32;
pub const CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK: usize = 4;
pub const CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK: usize = 1;
pub const CLUSTER_CONNECTIVITY_REFRESH_MARGIN_SECONDS: i64 = 120;

pub fn cluster_local_node_role(state: &ClusterBootstrapState) -> ClusterNodeRole {
    state
        .members
        .iter()
        .find(|member| member.node_id == state.local_node_id)
        .map(|member| member.node_role.clone())
        .unwrap_or_default()
}

pub fn cluster_network_probe_interval_for_role(role: &ClusterNodeRole) -> Duration {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_NETWORK_MOBILE_PROBE_INTERVAL,
        ClusterNodeRole::Sensor => CLUSTER_NETWORK_SENSOR_PROBE_INTERVAL,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_NETWORK_PROBE_INTERVAL
        }
    }
}

pub fn cluster_network_publish_interval_for_role(role: &ClusterNodeRole) -> Duration {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL,
        ClusterNodeRole::Sensor => CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_NETWORK_PUBLISH_INTERVAL
        }
    }
}

pub fn cluster_iroh_proxy_republish_sleep_duration(
    now: Instant,
    next_republish: Instant,
) -> Duration {
    let remaining = next_republish.saturating_duration_since(now);
    if remaining.is_zero() {
        Duration::from_millis(1)
    } else {
        remaining
    }
}

pub fn cluster_iroh_proxy_start_retry_sleep_duration(
    consecutive_failures: u32,
    max_delay: Duration,
) -> Duration {
    let min_delay = Duration::from_secs(5);
    let max_delay = max_delay.max(min_delay);
    let shift = consecutive_failures.min(6);
    let multiplier = 1_u32.checked_shl(shift).unwrap_or(64);
    min_delay.saturating_mul(multiplier).min(max_delay)
}

pub fn cluster_network_discovery_interval_for_role(role: &ClusterNodeRole) -> Duration {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_NETWORK_MOBILE_PUBLISH_INTERVAL,
        ClusterNodeRole::Sensor => CLUSTER_NETWORK_SENSOR_PUBLISH_INTERVAL,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_NETWORK_DISCOVERY_INTERVAL
        }
    }
}

pub fn cluster_connectivity_probe_max_endpoints_per_tick_for_role(role: &ClusterNodeRole) -> usize {
    match role {
        ClusterNodeRole::Mobile => CLUSTER_CONNECTIVITY_MOBILE_PROBE_MAX_ENDPOINTS_PER_TICK,
        ClusterNodeRole::Sensor => CLUSTER_CONNECTIVITY_SENSOR_PROBE_MAX_ENDPOINTS_PER_TICK,
        ClusterNodeRole::Workstation | ClusterNodeRole::Server | ClusterNodeRole::Test => {
            CLUSTER_CONNECTIVITY_PROBE_MAX_ENDPOINTS_PER_TICK
        }
    }
}

pub fn cluster_network_stable_discovery_announcement_due(tick: u64, state_changed: bool) -> bool {
    state_changed
        || tick
            .saturating_sub(1)
            .is_multiple_of(CLUSTER_NETWORK_STABLE_DISCOVERY_ANNOUNCE_TICK_STRIDE)
}

pub fn cluster_network_maintenance_interval_due(
    now: Instant,
    last: Instant,
    interval: Duration,
) -> bool {
    now.saturating_duration_since(last) >= interval
}

pub fn cluster_network_maintenance_interval_remaining(
    now: Instant,
    last: Instant,
    interval: Duration,
) -> Duration {
    interval.saturating_sub(now.saturating_duration_since(last))
}

#[derive(Clone, Debug)]
pub struct ClusterNetworkMaintenanceSchedule {
    pub local_node_role: ClusterNodeRole,
    pub has_discovery: bool,
    pub connectivity_converged: bool,
    pub startup_settle_until: Option<Instant>,
    pub last_publish: Instant,
    pub last_probe: Instant,
    pub last_discovery: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterNetworkMaintenanceDue {
    pub convergence_due: bool,
    pub publish_due: bool,
    pub probe_due: bool,
    pub discovery_due: bool,
}

pub fn cluster_network_maintenance_due(
    now: Instant,
    schedule: &ClusterNetworkMaintenanceSchedule,
) -> ClusterNetworkMaintenanceDue {
    let startup_settle_due = schedule
        .startup_settle_until
        .is_some_and(|deadline| now < deadline);
    let convergence_due = !schedule.connectivity_converged || startup_settle_due;
    let publish_due = convergence_due
        || cluster_network_maintenance_interval_due(
            now,
            schedule.last_publish,
            cluster_network_publish_interval_for_role(&schedule.local_node_role),
        );
    let probe_due = convergence_due
        || cluster_network_maintenance_interval_due(
            now,
            schedule.last_probe,
            cluster_network_probe_interval_for_role(&schedule.local_node_role),
        );
    let discovery_due = schedule.has_discovery
        && (convergence_due
            || cluster_network_maintenance_interval_due(
                now,
                schedule.last_discovery,
                cluster_network_discovery_interval_for_role(&schedule.local_node_role),
            ));

    ClusterNetworkMaintenanceDue {
        convergence_due,
        publish_due,
        probe_due,
        discovery_due,
    }
}

pub fn cluster_network_maintenance_next_due_duration(
    now: Instant,
    schedule: &ClusterNetworkMaintenanceSchedule,
) -> Duration {
    let mut next_due = cluster_network_maintenance_interval_remaining(
        now,
        schedule.last_publish,
        cluster_network_publish_interval_for_role(&schedule.local_node_role),
    )
    .min(cluster_network_maintenance_interval_remaining(
        now,
        schedule.last_probe,
        cluster_network_probe_interval_for_role(&schedule.local_node_role),
    ));
    if schedule.has_discovery {
        next_due = next_due.min(cluster_network_maintenance_interval_remaining(
            now,
            schedule.last_discovery,
            cluster_network_discovery_interval_for_role(&schedule.local_node_role),
        ));
    }
    if schedule
        .startup_settle_until
        .is_some_and(|deadline| now < deadline)
    {
        next_due = next_due.min(CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL);
    }
    if !schedule.connectivity_converged {
        next_due = next_due.min(CLUSTER_NETWORK_CONVERGENCE_TICK_INTERVAL);
    }
    next_due
}

pub fn cluster_network_maintenance_last_timestamp_for_next_due(
    now: Instant,
    interval: Duration,
    delay: Duration,
) -> Instant {
    now.checked_add(delay.saturating_sub(interval))
        .unwrap_or(now)
}
