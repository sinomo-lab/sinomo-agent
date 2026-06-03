use sinomo_core::ClusterNodeRolloutStatus;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const CLUSTER_ROLLOUT_PROGRESS_TOTAL: u32 = 5;
pub const CLUSTER_ROLLOUT_BUILD_HEARTBEAT_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRolloutProgressUpdate {
    pub status: String,
    pub mode: String,
    pub detail: Option<String>,
    pub source: String,
    pub requested_at: String,
    pub phase: String,
    pub progress_current: u32,
    pub progress_message: String,
}

impl ClusterRolloutProgressUpdate {
    pub fn new(
        status: impl Into<String>,
        mode: impl Into<String>,
        detail: Option<String>,
        source: impl Into<String>,
        requested_at: impl Into<String>,
        phase: impl Into<String>,
        progress_current: u32,
        progress_message: impl Into<String>,
    ) -> Self {
        Self {
            status: status.into(),
            mode: mode.into(),
            detail,
            source: source.into(),
            requested_at: requested_at.into(),
            phase: phase.into(),
            progress_current,
            progress_message: progress_message.into(),
        }
    }
}

pub fn rollout_timestamp_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string())
}

pub fn cluster_rollout_progress_status(
    update: ClusterRolloutProgressUpdate,
) -> ClusterNodeRolloutStatus {
    let now = rollout_timestamp_now();
    let is_terminal_failure = matches!(update.status.as_str(), "failed" | "rejected");
    ClusterNodeRolloutStatus {
        status: update.status.clone(),
        mode: Some(update.mode),
        detail: update.detail,
        phase: Some(update.phase),
        progress_current: Some(update.progress_current.min(CLUSTER_ROLLOUT_PROGRESS_TOTAL)),
        progress_total: Some(CLUSTER_ROLLOUT_PROGRESS_TOTAL),
        progress_message: Some(update.progress_message),
        source: Some(update.source),
        requested_at: Some(update.requested_at),
        staged_at: (update.status == "main-staged").then_some(now.clone()),
        updated_at: Some(now.clone()),
        failed_at: is_terminal_failure.then_some(now),
        diagnostics: None,
        stdout_log: None,
        stderr_log: None,
        stdout_tail: None,
        stderr_tail: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_rollout_progress_status_clamps_progress_and_marks_failure() {
        let status = cluster_rollout_progress_status(ClusterRolloutProgressUpdate::new(
            "rejected",
            "cluster-remote-build",
            Some("bad platform".to_string()),
            "source",
            "2026-06-03T00:00:00Z",
            "rejected",
            CLUSTER_ROLLOUT_PROGRESS_TOTAL + 10,
            "platform rejected",
        ));

        assert_eq!(status.status, "rejected");
        assert_eq!(
            status.progress_current,
            Some(CLUSTER_ROLLOUT_PROGRESS_TOTAL)
        );
        assert_eq!(status.progress_total, Some(CLUSTER_ROLLOUT_PROGRESS_TOTAL));
        assert!(status.failed_at.is_some());
        assert!(status.updated_at.is_some());
        assert!(status.staged_at.is_none());
    }

    #[test]
    fn cluster_rollout_progress_status_marks_main_staged() {
        let status = cluster_rollout_progress_status(ClusterRolloutProgressUpdate::new(
            "main-staged",
            "cluster-binary-push",
            None,
            "source",
            "2026-06-03T00:00:00Z",
            "main-staged",
            4,
            "candidate main staged",
        ));

        assert!(status.staged_at.is_some());
        assert!(status.failed_at.is_none());
    }
}
