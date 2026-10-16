use crate::DfsPendingMaintenanceWork;
use sinomo_core::ClusterBootstrapState;
use std::time::{Duration, Instant};

pub const DFS_DEFERRED_WORK_REPROBE_INTERVAL: Duration = Duration::from_secs(300);
pub const DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL: Duration = Duration::from_secs(30);
pub const DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL: Duration = Duration::from_secs(30);
pub const DFS_MAINTENANCE_LEASE_RETRY_DELAY: Duration = Duration::from_secs(5);
pub const DFS_MAINTENANCE_MEMBER_PHASE_SLOT: Duration = Duration::from_secs(20);
pub const DFS_MAINTENANCE_MEMBER_PHASE_WINDOW: Duration = Duration::from_secs(300);
pub const DFS_MAINTENANCE_NETWORK_STARTUP_GRACE: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsMaintenanceTickOutcome {
    Completed {
        reached_limit: bool,
    },
    /// Actual retained phase progress; the pass is still incomplete.
    Continued,
    Deferred,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsMaintenanceProbeDecision {
    pub trigger_maintenance: bool,
    pub immediate_continuation: bool,
    pub next_probe_at: Option<Instant>,
}

pub fn dfs_maintenance_member_phase(state: &ClusterBootstrapState) -> Duration {
    let mut member_node_ids = state
        .members
        .iter()
        .filter(|member| member.state.is_authorized())
        .map(|member| member.node_id.clone())
        .collect::<Vec<_>>();
    if !member_node_ids.contains(&state.local_node_id) {
        member_node_ids.push(state.local_node_id.clone());
    }
    member_node_ids.sort();
    member_node_ids.dedup();
    let rank = member_node_ids
        .iter()
        .position(|node_id| node_id == &state.local_node_id)
        .unwrap_or_default();
    let member_count = u128::try_from(member_node_ids.len())
        .unwrap_or(u128::MAX)
        .max(1);
    let rank = u128::try_from(rank).unwrap_or(u128::MAX);
    let slot_nanos = DFS_MAINTENANCE_MEMBER_PHASE_SLOT
        .as_nanos()
        .min(DFS_MAINTENANCE_MEMBER_PHASE_WINDOW.as_nanos() / member_count);
    let phase_nanos = slot_nanos.saturating_mul(rank);
    Duration::from_nanos(u64::try_from(phase_nanos).unwrap_or(u64::MAX))
}

pub fn dfs_initial_maintenance_delay(
    member_phase: Duration,
    maintain_cluster_runtime: bool,
) -> Duration {
    if maintain_cluster_runtime {
        member_phase.saturating_add(DFS_MAINTENANCE_NETWORK_STARTUP_GRACE)
    } else {
        member_phase
    }
}

pub fn dfs_maintenance_probe_decision(
    observed_at: Instant,
    observed_at_unix_nanos: u64,
    work: Option<DfsPendingMaintenanceWork>,
) -> DfsMaintenanceProbeDecision {
    let Some(work) = work else {
        return DfsMaintenanceProbeDecision {
            trigger_maintenance: false,
            immediate_continuation: false,
            next_probe_at: Some(
                observed_at
                    .checked_add(DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL)
                    .unwrap_or(observed_at),
            ),
        };
    };
    let trigger_maintenance = work.should_trigger_background_maintenance();
    let next_probe_delay = if trigger_maintenance {
        Some(DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL)
    } else {
        work.next_maintenance_deadline_unix_nanos()
            .map(|retry_at| Duration::from_nanos(retry_at.saturating_sub(observed_at_unix_nanos)))
            .into_iter()
            .chain(
                work.has_deferred_work()
                    .then_some(DFS_DEFERRED_WORK_REPROBE_INTERVAL),
            )
            .min()
    };
    DfsMaintenanceProbeDecision {
        trigger_maintenance,
        immediate_continuation: false,
        next_probe_at: next_probe_delay
            .map(|delay| observed_at.checked_add(delay).unwrap_or(observed_at)),
    }
}

pub fn dfs_maintenance_completion_decision(
    outcome: DfsMaintenanceTickOutcome,
    observed_at: Instant,
    observed_at_unix_nanos: u64,
    work: Option<DfsPendingMaintenanceWork>,
    committed_edge_latched: bool,
) -> DfsMaintenanceProbeDecision {
    let mut decision = dfs_maintenance_probe_decision(observed_at, observed_at_unix_nanos, work);
    match outcome {
        DfsMaintenanceTickOutcome::Completed { reached_limit } => {
            decision.immediate_continuation = reached_limit && decision.trigger_maintenance;
        }
        DfsMaintenanceTickOutcome::Continued => {
            // A transient work probe can be empty while an owned placement
            // continuation remains. Resume that actual checkpoint, not a new
            // retry budget or a fabricated completed pass.
            decision.trigger_maintenance = true;
            decision.immediate_continuation = true;
        }
        DfsMaintenanceTickOutcome::Deferred => {
            decision.immediate_continuation = false;
        }
    }
    if committed_edge_latched {
        decision.trigger_maintenance = true;
        decision.immediate_continuation = true;
    }
    decision
}

pub fn dfs_maintenance_revalidation_decision(
    observed_at: Instant,
    observed_at_unix_nanos: u64,
    work: Option<DfsPendingMaintenanceWork>,
    immediate_continuation_requested: bool,
) -> DfsMaintenanceProbeDecision {
    let mut decision = dfs_maintenance_probe_decision(observed_at, observed_at_unix_nanos, work);
    decision.immediate_continuation =
        immediate_continuation_requested && decision.trigger_maintenance;
    decision
}

pub fn dfs_failed_maintenance_decision(
    observed_at: Instant,
    next_probe_at: Option<Instant>,
) -> DfsMaintenanceProbeDecision {
    let error_retry_at = observed_at
        .checked_add(DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL)
        .unwrap_or(observed_at);
    DfsMaintenanceProbeDecision {
        trigger_maintenance: true,
        immediate_continuation: false,
        next_probe_at: Some(
            next_probe_at
                .map(|next_probe_at| next_probe_at.min(error_retry_at))
                .unwrap_or(error_retry_at),
        ),
    }
}

pub fn dfs_maintenance_notification_decision(
    _observed_at: Instant,
    _triggered_maintenance_requested: bool,
    _immediate_continuation_requested: bool,
    next_probe_at: Option<Instant>,
) -> DfsMaintenanceProbeDecision {
    DfsMaintenanceProbeDecision {
        trigger_maintenance: true,
        immediate_continuation: true,
        next_probe_at,
    }
}

pub fn dfs_maintenance_due_in(
    now: Instant,
    initial_not_before: Instant,
    last_maintenance: Instant,
    triggered_maintenance_requested: bool,
    immediate_continuation_requested: bool,
) -> Option<Duration> {
    triggered_maintenance_requested.then(|| {
        if immediate_continuation_requested {
            return Duration::ZERO;
        }
        let startup_remaining = initial_not_before.saturating_duration_since(now);
        let interval_remaining = DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL
            .saturating_sub(now.saturating_duration_since(last_maintenance));
        startup_remaining.max(interval_remaining)
    })
}

pub fn dfs_maintenance_probe_is_due(now: Instant, next_probe_at: Option<Instant>) -> bool {
    next_probe_at.is_some_and(|next_probe_at| now >= next_probe_at)
}

pub fn dfs_maintenance_should_launch(triggered_due: bool, work_revalidated: bool) -> bool {
    triggered_due && work_revalidated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_incomplete_progress_resumes_even_when_the_queue_probe_is_empty() {
        let now = Instant::now();
        for work in [None, Some(DfsPendingMaintenanceWork::default())] {
            let continuation = dfs_maintenance_completion_decision(
                DfsMaintenanceTickOutcome::Continued,
                now,
                1_000_000_000,
                work.clone(),
                false,
            );
            assert!(continuation.trigger_maintenance);
            assert!(continuation.immediate_continuation);
            assert_eq!(
                dfs_maintenance_due_in(
                    now,
                    now + DFS_MAINTENANCE_NETWORK_STARTUP_GRACE,
                    now,
                    continuation.trigger_maintenance,
                    continuation.immediate_continuation,
                ),
                Some(Duration::ZERO),
            );
            let deferred = dfs_maintenance_completion_decision(
                DfsMaintenanceTickOutcome::Deferred,
                now,
                1_000_000_000,
                work,
                false,
            );
            assert!(!deferred.immediate_continuation);
        }
    }

    #[test]
    fn idle_work_parks_and_actionable_work_is_interval_gated() {
        let now = Instant::now();
        let idle = dfs_maintenance_probe_decision(
            now,
            1_000_000_000,
            Some(DfsPendingMaintenanceWork::default()),
        );
        assert_eq!(
            idle,
            DfsMaintenanceProbeDecision {
                trigger_maintenance: false,
                immediate_continuation: false,
                next_probe_at: None,
            }
        );

        let actionable = dfs_maintenance_probe_decision(
            now,
            1_000_000_000,
            Some(DfsPendingMaintenanceWork {
                pending_write_count: 1,
                ..DfsPendingMaintenanceWork::default()
            }),
        );
        assert!(actionable.trigger_maintenance);
        assert!(!actionable.immediate_continuation);
        assert_eq!(
            dfs_maintenance_due_in(now, now, now, true, false),
            Some(DFS_TRIGGERED_MAINTENANCE_MIN_INTERVAL)
        );
    }

    #[test]
    fn bounded_completion_and_committed_edge_own_immediate_continuations() {
        let now = Instant::now();
        let work = Some(DfsPendingMaintenanceWork {
            pending_namespace_job_count: 1,
            ..DfsPendingMaintenanceWork::default()
        });
        let bounded = dfs_maintenance_completion_decision(
            DfsMaintenanceTickOutcome::Completed {
                reached_limit: true,
            },
            now,
            1_000_000_000,
            work,
            false,
        );
        assert!(bounded.trigger_maintenance);
        assert!(bounded.immediate_continuation);

        let committed = dfs_maintenance_completion_decision(
            DfsMaintenanceTickOutcome::Deferred,
            now,
            1_000_000_000,
            Some(DfsPendingMaintenanceWork::default()),
            true,
        );
        assert!(committed.trigger_maintenance);
        assert!(committed.immediate_continuation);
    }

    #[test]
    fn failures_retain_request_with_bounded_retry() {
        let now = Instant::now();
        let decision = dfs_failed_maintenance_decision(
            now,
            now.checked_add(DFS_DEFERRED_WORK_REPROBE_INTERVAL),
        );
        assert!(decision.trigger_maintenance);
        assert!(!decision.immediate_continuation);
        assert_eq!(
            decision.next_probe_at,
            now.checked_add(DFS_MAINTENANCE_PROBE_ERROR_RETRY_INTERVAL)
        );
    }
}
