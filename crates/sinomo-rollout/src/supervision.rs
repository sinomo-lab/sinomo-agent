use crate::{MainExitKind, MainExitObservation};
use std::time::{Duration, Instant};

pub const SHADOW_MAIN_RESTART_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
pub const SHADOW_MAIN_RESTART_MAX_BACKOFF: Duration = Duration::from_secs(30);
pub const SHADOW_MAIN_RESTART_STABLE_RESET_AFTER: Duration = Duration::from_secs(60);

/// Rollout-owned restart cadence for a main daemon supervised by the shadow.
///
/// The host owns process creation and observation. This state only decides when
/// another start attempt is permitted after a failed start or crash.
#[derive(Clone, Debug, Default)]
pub struct ShadowMainRestartBackoff {
    consecutive_failures: u32,
    next_restart_at: Option<Instant>,
}

impl ShadowMainRestartBackoff {
    pub fn can_restart(&self, now: Instant) -> bool {
        self.next_restart_at
            .is_none_or(|next_restart_at| now >= next_restart_at)
    }

    pub fn schedule_after_failure(&mut self, now: Instant) -> Duration {
        let shift = self.consecutive_failures.min(7);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
        let delay = SHADOW_MAIN_RESTART_INITIAL_BACKOFF
            .saturating_mul(multiplier)
            .min(SHADOW_MAIN_RESTART_MAX_BACKOFF);
        self.next_restart_at = Some(now + delay);
        delay
    }

    pub fn clear_schedule(&mut self) {
        self.next_restart_at = None;
    }

    pub fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.next_restart_at = None;
    }

    pub fn reset_after_stable_start(&mut self, started_at: Option<Instant>, now: Instant) {
        let Some(started_at) = started_at else {
            return;
        };
        if now.duration_since(started_at) >= SHADOW_MAIN_RESTART_STABLE_RESET_AFTER {
            self.reset();
        }
    }
}

pub fn shadow_main_should_start(
    main_restart_suppressed: bool,
    pending_main_activation: bool,
) -> bool {
    pending_main_activation || !main_restart_suppressed
}

pub fn pending_rollout_probe_matches_candidate(
    running_main_pid: Option<u32>,
    candidate_pid: Option<u32>,
) -> bool {
    matches!((running_main_pid, candidate_pid), (Some(running), Some(candidate)) if running == candidate)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingShadowCandidateExitAction {
    Promote,
    Retry,
    Recover,
}

pub fn pending_shadow_candidate_exit_action(
    candidate_is_running: bool,
    observation: MainExitObservation,
    now: Instant,
    deadline: Instant,
) -> PendingShadowCandidateExitAction {
    let clean_exit = observation.exit_code == Some(0) && observation.signal.is_none();
    if clean_exit && candidate_is_running {
        return PendingShadowCandidateExitAction::Promote;
    }
    if clean_exit && now < deadline {
        return PendingShadowCandidateExitAction::Retry;
    }
    PendingShadowCandidateExitAction::Recover
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupervisedMainExitDecision {
    RestartImmediately,
    RestartWithBackoff,
    SuppressRestart,
}

pub fn classify_supervised_main_exit(
    supervisor_restart_requested: bool,
    kind: MainExitKind,
) -> SupervisedMainExitDecision {
    if supervisor_restart_requested
        && matches!(
            kind,
            MainExitKind::CleanExit | MainExitKind::RequestedShutdown
        )
    {
        SupervisedMainExitDecision::RestartImmediately
    } else if supervisor_restart_requested || kind == MainExitKind::Crash {
        SupervisedMainExitDecision::RestartWithBackoff
    } else {
        SupervisedMainExitDecision::SuppressRestart
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_backoff_caps_and_resets_after_stable_runtime() {
        let mut backoff = ShadowMainRestartBackoff::default();
        let start = Instant::now();

        let first = backoff.schedule_after_failure(start);
        assert_eq!(first, SHADOW_MAIN_RESTART_INITIAL_BACKOFF);
        assert!(!backoff.can_restart(start));
        assert!(backoff.can_restart(start + first));

        let mut delay = first;
        let mut now = start;
        for _ in 0..16 {
            now += delay;
            delay = backoff.schedule_after_failure(now);
        }
        assert_eq!(delay, SHADOW_MAIN_RESTART_MAX_BACKOFF);

        backoff.reset_after_stable_start(Some(now), now + SHADOW_MAIN_RESTART_STABLE_RESET_AFTER);
        assert_eq!(backoff.consecutive_failures, 0);
        assert!(backoff.next_restart_at.is_none());
    }

    #[test]
    fn pending_activation_overrides_suppressed_restart() {
        assert!(shadow_main_should_start(true, true));
        assert!(!shadow_main_should_start(true, false));
        assert!(shadow_main_should_start(false, false));
    }

    #[test]
    fn candidate_match_requires_same_running_pid() {
        assert!(pending_rollout_probe_matches_candidate(Some(42), Some(42)));
        assert!(!pending_rollout_probe_matches_candidate(Some(41), Some(42)));
        assert!(!pending_rollout_probe_matches_candidate(None, Some(42)));
        assert!(!pending_rollout_probe_matches_candidate(Some(42), None));
    }

    #[test]
    fn clean_candidate_exit_retries_until_deadline_or_promotes_running_candidate() {
        let now = Instant::now();
        let observation = MainExitObservation::exit_code(0);

        assert_eq!(
            pending_shadow_candidate_exit_action(
                false,
                observation,
                now,
                now + Duration::from_secs(30),
            ),
            PendingShadowCandidateExitAction::Retry
        );
        assert_eq!(
            pending_shadow_candidate_exit_action(false, observation, now, now),
            PendingShadowCandidateExitAction::Recover
        );
        assert_eq!(
            pending_shadow_candidate_exit_action(true, observation, now, now),
            PendingShadowCandidateExitAction::Promote
        );
    }

    #[test]
    fn explicit_clean_restart_is_distinct_from_a_crash() {
        assert_eq!(
            classify_supervised_main_exit(true, MainExitKind::CleanExit),
            SupervisedMainExitDecision::RestartImmediately
        );
        assert_eq!(
            classify_supervised_main_exit(true, MainExitKind::Crash),
            SupervisedMainExitDecision::RestartWithBackoff
        );
        assert_eq!(
            classify_supervised_main_exit(false, MainExitKind::Crash),
            SupervisedMainExitDecision::RestartWithBackoff
        );
        assert_eq!(
            classify_supervised_main_exit(false, MainExitKind::CleanExit),
            SupervisedMainExitDecision::SuppressRestart
        );
    }

    #[test]
    fn explicit_clean_restart_clears_existing_backoff() {
        let mut backoff = ShadowMainRestartBackoff::default();
        let now = Instant::now();
        backoff.schedule_after_failure(now);
        backoff.schedule_after_failure(now);
        assert!(!backoff.can_restart(now));

        backoff.reset();

        assert_eq!(backoff.consecutive_failures, 0);
        assert!(backoff.next_restart_at.is_none());
        assert!(backoff.can_restart(now));
    }
}
