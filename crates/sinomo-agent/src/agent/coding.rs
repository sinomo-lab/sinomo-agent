//! Coding evidence reduction, validation classification, and stop decisions.

use super::{
    AGENT_CODING_EVIDENCE_KIND, AgentCandidateEvidence, AgentCandidateEvidenceBundle,
    AgentCodingToolEvidence, AgentContextAttachment, AgentItem, AgentLoopDecision,
    AgentToolOutcome, AgentToolStatus, Deserialize, Serialize, TaskId, compact_multiline,
    push_unique,
};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum AgentVerificationState {
    #[default]
    NotRun,
    Passed,
    Failed,
    Blocked,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum CodingPatchStatus {
    #[default]
    NotStarted,
    Applied,
    Failed,
    Blocked,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum CodingValidationStatus {
    #[default]
    NotRun,
    Passed,
    FailedPatchRelevant,
    FailedEnvironment,
    FailedCommandSetup,
    TimedOut,
    Inconclusive,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CodingBlockedReason {
    PolicyDenied,
    ToolUnavailable,
    Environment,
    CommandSetup,
    NeedsUserInput,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CodingCommandFailureKind {
    TimedOut,
    CommandSetup,
    MissingCommand,
    MissingDependency,
    NetworkUnavailable,
    PermissionDenied,
    EnvironmentBlocked,
    PatchRelevant,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum CodingStopDecision {
    #[default]
    Continue,
    FinishWithPatch,
    FinishBlocked,
    RequestUserInput,
    Abort,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodingValidationAttempt {
    pub step_index: usize,
    pub call_id: TaskId,
    pub command: String,
    pub status: CodingValidationStatus,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_kind: Option<CodingCommandFailureKind>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct CodingRunState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changed_files: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_diff_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_diff_summary: Option<String>,
    #[serde(default)]
    pub patch_status: CodingPatchStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation_attempts: Vec<CodingValidationAttempt>,
    #[serde(default)]
    pub validation_status: CodingValidationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<CodingBlockedReason>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outstanding_evidence: Vec<String>,
    #[serde(default)]
    pub stop_recommendation: CodingStopDecision,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_risks: Vec<String>,
}

impl CodingRunState {
    pub fn is_empty(&self) -> bool {
        self.changed_files.is_empty()
            && self.current_diff_summary.is_none()
            && self.patch_status == CodingPatchStatus::NotStarted
            && self.validation_attempts.is_empty()
            && self.validation_status == CodingValidationStatus::NotRun
            && self.blocked_reason.is_none()
            && self.open_risks.is_empty()
    }
}

pub(super) fn coding_state_from_items(items: &[AgentItem]) -> CodingRunState {
    let mut state = CodingRunState::default();
    for item in items {
        match item {
            AgentItem::UserMessage { content, .. } => {
                if !content.contains("\"kind\": \"agent_queued_turn_context\"")
                    && state.objective.is_none()
                {
                    state.objective = Some(compact_multiline(content, 1000));
                }
            }
            AgentItem::PatchSnapshot {
                status,
                summary,
                changed_files,
                ..
            } => {
                match status {
                    AgentToolStatus::Succeeded => {
                        state.patch_status = CodingPatchStatus::Applied;
                        if !changed_files.is_empty() {
                            // A successful command before the edit can prove
                            // toolchain availability, but it cannot validate
                            // bytes that did not exist yet. Keep that evidence
                            // in the durable transcript while resetting the
                            // current-patch validation projection.
                            state.validation_attempts.clear();
                            state.validation_status = CodingValidationStatus::NotRun;
                        }
                    }
                    AgentToolStatus::Blocked => state.patch_status = CodingPatchStatus::Blocked,
                    AgentToolStatus::Failed | AgentToolStatus::Cancelled => {
                        state.patch_status = CodingPatchStatus::Failed
                    }
                }
                for changed_file in changed_files {
                    push_unique(&mut state.changed_files, changed_file.clone());
                }
                if !changed_files.is_empty() {
                    state.current_diff_summary = Some(compact_multiline(summary, 1000));
                }
                if matches!(status, AgentToolStatus::Failed | AgentToolStatus::Blocked) {
                    push_unique(&mut state.open_risks, compact_multiline(summary, 500));
                }
            }
            AgentItem::ToolResult {
                step_index,
                call_id,
                status,
                summary,
                output,
                evidence,
                ..
            } => {
                if let Some(reason) = coding_blocked_reason_from_tool_result(status, output) {
                    state.blocked_reason = Some(reason);
                    push_unique(&mut state.open_risks, compact_multiline(summary, 500));
                }
                let coding = evidence
                    .record::<AgentCodingToolEvidence>(AGENT_CODING_EVIDENCE_KIND)
                    .unwrap_or_default();
                for changed_file in coding.changed_files {
                    push_unique(&mut state.changed_files, changed_file);
                }
                if let Some(attempt) = coding_validation_attempt_from_tool_result(
                    *step_index,
                    call_id,
                    status,
                    summary,
                    output,
                ) {
                    state.validation_status =
                        merge_coding_validation_status(&state.validation_status, &attempt.status);
                    state.validation_attempts.push(attempt);
                }
            }
            AgentItem::VerificationResult {
                step_index,
                call_id,
                command,
                status,
                summary,
                ..
            } => {
                let status = coding_validation_status_from_agent_verification(status);
                state.validation_status =
                    merge_coding_validation_status(&state.validation_status, &status);
                state.validation_attempts.push(CodingValidationAttempt {
                    step_index: *step_index,
                    call_id: call_id.clone(),
                    command: command.clone(),
                    status,
                    summary: summary.clone(),
                    exit_code: None,
                    timed_out: false,
                    failure_kind: None,
                });
            }
            AgentItem::CodingRunState { .. }
            | AgentItem::AssistantText { .. }
            | AgentItem::AssistantReasoningSummary { .. }
            | AgentItem::ToolCall { .. }
            | AgentItem::AsyncTaskUpdate { .. }
            | AgentItem::ContextInjection { .. }
            | AgentItem::CompactionSummary { .. }
            | AgentItem::ModelUsage { .. }
            | AgentItem::Error { .. } => {}
        }
    }
    finalize_coding_state(state)
}

pub(super) fn finalize_coding_state(mut state: CodingRunState) -> CodingRunState {
    if state.patch_status == CodingPatchStatus::Applied
        && state.validation_status == CodingValidationStatus::NotRun
    {
        push_unique(
            &mut state.outstanding_evidence,
            "validation has not run for the current patch".to_string(),
        );
    }
    if state.patch_status == CodingPatchStatus::NotStarted && state.blocked_reason.is_none() {
        push_unique(
            &mut state.outstanding_evidence,
            "no patch has been produced yet".to_string(),
        );
    }
    state.stop_recommendation = match (&state.patch_status, &state.validation_status) {
        _ if state.blocked_reason == Some(CodingBlockedReason::NeedsUserInput) => {
            CodingStopDecision::RequestUserInput
        }
        _ if state.blocked_reason.is_some() && state.patch_status != CodingPatchStatus::Applied => {
            CodingStopDecision::FinishBlocked
        }
        _ if has_terminal_coding_blocker(&state) => CodingStopDecision::FinishBlocked,
        (CodingPatchStatus::Applied, CodingValidationStatus::Passed)
            if is_narrow_coding_patch(&state) =>
        {
            CodingStopDecision::FinishWithPatch
        }
        (CodingPatchStatus::Applied, CodingValidationStatus::FailedEnvironment)
            if is_narrow_coding_patch(&state)
                && environment_validation_can_finish_with_patch(&state) =>
        {
            CodingStopDecision::FinishWithPatch
        }
        _ => CodingStopDecision::Continue,
    };
    state
}

pub(super) fn is_narrow_coding_patch(state: &CodingRunState) -> bool {
    !state.changed_files.is_empty() && state.changed_files.len() <= 3
}

pub(super) fn coding_state_should_stop_loop(state: &CodingRunState) -> bool {
    match state.stop_recommendation {
        CodingStopDecision::FinishWithPatch
        | CodingStopDecision::RequestUserInput
        | CodingStopDecision::Abort => true,
        CodingStopDecision::FinishBlocked => {
            state.patch_status != CodingPatchStatus::NotStarted
                || !state.changed_files.is_empty()
                || !state.validation_attempts.is_empty()
        }
        CodingStopDecision::Continue => false,
    }
}

pub(super) fn has_terminal_coding_blocker(state: &CodingRunState) -> bool {
    matches!(
        state.blocked_reason,
        Some(
            CodingBlockedReason::PolicyDenied
                | CodingBlockedReason::ToolUnavailable
                | CodingBlockedReason::Unknown
        )
    ) || state.validation_attempts.iter().any(|attempt| {
        matches!(
            attempt.failure_kind,
            Some(
                CodingCommandFailureKind::NetworkUnavailable
                    | CodingCommandFailureKind::PermissionDenied
            )
        )
    })
}

pub(super) fn environment_validation_can_finish_with_patch(state: &CodingRunState) -> bool {
    state
        .validation_attempts
        .iter()
        .filter(|attempt| attempt.status == CodingValidationStatus::FailedEnvironment)
        .all(|attempt| {
            matches!(
                attempt.failure_kind,
                Some(
                    CodingCommandFailureKind::CommandSetup
                        | CodingCommandFailureKind::MissingCommand
                        | CodingCommandFailureKind::MissingDependency
                        | CodingCommandFailureKind::EnvironmentBlocked
                )
            )
        })
        && state
            .validation_attempts
            .iter()
            .any(|attempt| attempt.status == CodingValidationStatus::FailedEnvironment)
}

pub(super) fn coding_final_decision_from_state(state: &CodingRunState) -> AgentLoopDecision {
    let files = if state.changed_files.is_empty() {
        "no files recorded".to_string()
    } else {
        state.changed_files.join(", ")
    };
    let validation = coding_validation_summary_from_state(state);
    let summary = state
        .current_diff_summary
        .as_deref()
        .map(|summary| format!(" Summary: {summary}"))
        .unwrap_or_default();
    match state.stop_recommendation {
        CodingStopDecision::FinishWithPatch => AgentLoopDecision {
            response_mode: crate::AgentLoopResponseMode::Answer,
            text: format!("Patch is ready. Changed files: {files}. {validation}.{summary}"),
            tool_calls: Vec::new(),
            background_handoff: None,
            next_state: crate::AgentInteractionState::Listening,
        },
        CodingStopDecision::RequestUserInput => AgentLoopDecision {
            response_mode: crate::AgentLoopResponseMode::Clarify,
            text: format!(
                "I need user input before continuing. Changed files: {files}. {validation}.{summary}"
            ),
            tool_calls: Vec::new(),
            background_handoff: None,
            next_state: crate::AgentInteractionState::Waiting,
        },
        CodingStopDecision::FinishBlocked => AgentLoopDecision {
            response_mode: crate::AgentLoopResponseMode::Answer,
            text: format!("Coding run is blocked. Changed files: {files}. {validation}.{summary}"),
            tool_calls: Vec::new(),
            background_handoff: None,
            next_state: crate::AgentInteractionState::Listening,
        },
        CodingStopDecision::Abort | CodingStopDecision::Continue => AgentLoopDecision {
            response_mode: crate::AgentLoopResponseMode::Answer,
            text: format!(
                "Coding run state: {:?}. Changed files: {files}. {validation}.{summary}",
                state.stop_recommendation
            ),
            tool_calls: Vec::new(),
            background_handoff: None,
            next_state: crate::AgentInteractionState::Listening,
        },
    }
}

pub(super) fn coding_validation_summary_from_state(state: &CodingRunState) -> String {
    let failure_kind = state
        .validation_attempts
        .iter()
        .rev()
        .find_map(|attempt| attempt.failure_kind.as_ref())
        .map(coding_command_failure_kind_label);
    match state.validation_status {
        CodingValidationStatus::NotRun => "validation has not run".to_string(),
        CodingValidationStatus::Passed => "validation passed".to_string(),
        CodingValidationStatus::FailedPatchRelevant => {
            "validation failed against the patch".to_string()
        }
        CodingValidationStatus::FailedEnvironment => match failure_kind {
            Some(label) => {
                format!("validation was blocked by the local environment ({label})")
            }
            None => "validation was blocked by the local environment".to_string(),
        },
        CodingValidationStatus::FailedCommandSetup => match failure_kind {
            Some(label) => format!("validation command setup failed ({label})"),
            None => "validation command setup failed".to_string(),
        },
        CodingValidationStatus::TimedOut => "validation timed out".to_string(),
        CodingValidationStatus::Inconclusive => "validation was inconclusive".to_string(),
    }
}

pub(super) fn coding_command_failure_kind_label(kind: &CodingCommandFailureKind) -> &'static str {
    match kind {
        CodingCommandFailureKind::TimedOut => "timed out",
        CodingCommandFailureKind::CommandSetup => "command setup",
        CodingCommandFailureKind::MissingCommand => "missing command",
        CodingCommandFailureKind::MissingDependency => "missing dependency",
        CodingCommandFailureKind::NetworkUnavailable => "network unavailable",
        CodingCommandFailureKind::PermissionDenied => "permission denied",
        CodingCommandFailureKind::EnvironmentBlocked => "environment blocked",
        CodingCommandFailureKind::PatchRelevant => "patch relevant",
        CodingCommandFailureKind::Unknown => "unknown",
    }
}

pub(super) fn merge_coding_validation_status(
    current: &CodingValidationStatus,
    next: &CodingValidationStatus,
) -> CodingValidationStatus {
    use CodingValidationStatus::{
        FailedCommandSetup, FailedEnvironment, FailedPatchRelevant, Inconclusive, NotRun, Passed,
        TimedOut,
    };
    match (current, next) {
        (FailedPatchRelevant, _) | (_, FailedPatchRelevant) => FailedPatchRelevant,
        (TimedOut, _) | (_, TimedOut) => TimedOut,
        (FailedCommandSetup, _) | (_, FailedCommandSetup) => FailedCommandSetup,
        (FailedEnvironment, _) | (_, FailedEnvironment) => FailedEnvironment,
        (Inconclusive, _) | (_, Inconclusive) => Inconclusive,
        (Passed, _) | (_, Passed) => Passed,
        _ => NotRun,
    }
}

pub(super) fn coding_validation_status_from_agent_verification(
    status: &AgentVerificationState,
) -> CodingValidationStatus {
    match status {
        AgentVerificationState::NotRun => CodingValidationStatus::NotRun,
        AgentVerificationState::Passed => CodingValidationStatus::Passed,
        AgentVerificationState::Failed => CodingValidationStatus::FailedPatchRelevant,
        AgentVerificationState::Blocked => CodingValidationStatus::FailedEnvironment,
    }
}

pub(super) fn coding_validation_attempt_from_tool_result(
    step_index: usize,
    call_id: &TaskId,
    status: &AgentToolStatus,
    summary: &str,
    output: &serde_json::Value,
) -> Option<CodingValidationAttempt> {
    if !looks_like_command_result(output) {
        return None;
    }
    let command = verification_command_from_tool_output(output).unwrap_or_else(|| "command".into());
    let timed_out = output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let exit_code = output
        .get("status")
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let classification = classify_command_failure(status, summary, output);
    Some(CodingValidationAttempt {
        step_index,
        call_id: call_id.clone(),
        command,
        status: classification.status,
        summary: compact_multiline(summary, 1000),
        exit_code,
        timed_out,
        failure_kind: classification.failure_kind,
    })
}

pub(super) fn looks_like_command_result(output: &serde_json::Value) -> bool {
    output.get("program").is_some()
        && output.get("args").is_some()
        && output.get("status").is_some()
        && output.get("stdout").is_some()
        && output.get("stderr").is_some()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CodingCommandFailureClassification {
    pub(super) status: CodingValidationStatus,
    pub(super) failure_kind: Option<CodingCommandFailureKind>,
}

pub(super) fn classify_command_failure(
    status: &AgentToolStatus,
    summary: &str,
    output: &serde_json::Value,
) -> CodingCommandFailureClassification {
    if output
        .get("success")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::Passed,
            failure_kind: None,
        };
    }
    if output
        .get("timed_out")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::TimedOut,
            failure_kind: Some(CodingCommandFailureKind::TimedOut),
        };
    }
    if matches!(status, AgentToolStatus::Blocked) {
        let failure_kind = if output
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|error| error == "missing_command")
        {
            CodingCommandFailureKind::CommandSetup
        } else {
            CodingCommandFailureKind::EnvironmentBlocked
        };
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::FailedEnvironment,
            failure_kind: Some(failure_kind),
        };
    }
    let combined = command_failure_text(summary, output);
    if command_setup_failure(&combined) {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::FailedCommandSetup,
            failure_kind: Some(CodingCommandFailureKind::CommandSetup),
        };
    }
    if missing_command_failure(&combined) {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::FailedEnvironment,
            failure_kind: Some(CodingCommandFailureKind::MissingCommand),
        };
    }
    if missing_dependency_failure(&combined) {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::FailedEnvironment,
            failure_kind: Some(CodingCommandFailureKind::MissingDependency),
        };
    }
    if network_failure(&combined) {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::FailedEnvironment,
            failure_kind: Some(CodingCommandFailureKind::NetworkUnavailable),
        };
    }
    if permission_failure(&combined) {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::FailedEnvironment,
            failure_kind: Some(CodingCommandFailureKind::PermissionDenied),
        };
    }
    if environment_failure(&combined) {
        return CodingCommandFailureClassification {
            status: CodingValidationStatus::FailedEnvironment,
            failure_kind: Some(CodingCommandFailureKind::EnvironmentBlocked),
        };
    }
    CodingCommandFailureClassification {
        status: CodingValidationStatus::FailedPatchRelevant,
        failure_kind: Some(CodingCommandFailureKind::PatchRelevant),
    }
}

pub(super) fn command_failure_text(summary: &str, output: &serde_json::Value) -> String {
    let mut parts = vec![summary.to_string()];
    for key in ["stderr", "stdout", "aggregated_output", "error", "detail"] {
        if let Some(value) = output.get(key).and_then(serde_json::Value::as_str) {
            parts.push(value.to_string());
        }
    }
    if let Some(command_output) = output.get("command_output") {
        for key in ["stderr", "stdout", "aggregated_output"] {
            if let Some(value) = command_output.get(key).and_then(serde_json::Value::as_str) {
                parts.push(value.to_string());
            }
        }
    }
    parts.join("\n").to_ascii_lowercase()
}

pub(super) fn command_setup_failure(text: &str) -> bool {
    [
        "missing_command",
        "requires a string command",
        "invalid option",
        "usage:",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

pub(super) fn missing_command_failure(text: &str) -> bool {
    [
        "command not found",
        "no such file or directory",
        "not recognized as an internal or external command",
        "the system cannot find the file specified",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

pub(super) fn missing_dependency_failure(text: &str) -> bool {
    [
        "missing dependency",
        "module not found",
        "modulenotfounderror",
        "no module named",
        "cannot find module",
        "unable to locate package",
        "package not found",
        "could not find package",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

pub(super) fn network_failure(text: &str) -> bool {
    [
        "failed to download",
        "network is unreachable",
        "temporary failure in name resolution",
        "connection refused",
        "connection timed out",
        "operation timed out",
        "could not resolve",
        "could not resolve host",
        "dns error",
        "tls handshake",
        "certificate verify failed",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

pub(super) fn permission_failure(text: &str) -> bool {
    [
        "permission denied",
        "access is denied",
        "operation not permitted",
        "read-only file system",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

pub(super) fn environment_failure(text: &str) -> bool {
    [
        "no space left on device",
        "disk quota exceeded",
        "resource temporarily unavailable",
        "too many open files",
        "cannot allocate memory",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

pub(super) fn coding_blocked_reason_from_tool_result(
    status: &AgentToolStatus,
    output: &serde_json::Value,
) -> Option<CodingBlockedReason> {
    if !matches!(status, AgentToolStatus::Blocked) {
        return None;
    }
    let error = output
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match error {
        "policy_denied" | "tool_unavailable" => Some(CodingBlockedReason::PolicyDenied),
        "tool_not_visible" | "missing_executor" | "unknown_tool" => {
            Some(CodingBlockedReason::ToolUnavailable)
        }
        "missing_command" => Some(CodingBlockedReason::CommandSetup),
        "request_user_input_unavailable" => Some(CodingBlockedReason::NeedsUserInput),
        "command_execution_disabled" | "workspace_root_not_allowed" => {
            Some(CodingBlockedReason::Environment)
        }
        _ => Some(CodingBlockedReason::Unknown),
    }
}

pub(super) const MODEL_VISIBLE_CODING_CHANGED_FILE_LIMIT: usize = 25;
pub(super) const MODEL_VISIBLE_CODING_ALLOWED_PATH_LIMIT: usize = 25;
pub(super) const MODEL_VISIBLE_CODING_VALIDATION_ATTEMPT_LIMIT: usize = 3;
pub(super) const MODEL_VISIBLE_CODING_EVIDENCE_LIMIT: usize = 5;
pub(super) const MODEL_VISIBLE_CODING_RISK_LIMIT: usize = 5;
pub(super) const MODEL_VISIBLE_COMMAND_STDOUT_BYTES: usize = 4096;
pub(super) const MODEL_VISIBLE_COMMAND_STDERR_BYTES: usize = 4096;
pub(super) const MODEL_VISIBLE_COMMAND_AGGREGATED_BYTES: usize = 8192;
pub(super) const MODEL_VISIBLE_READ_FILE_CONTENT_BYTES: usize = 16 * 1024;
pub(super) const MODEL_VISIBLE_TOOL_RESULTS_AGGREGATE_BYTES: usize = 64 * 1024;
pub(super) const MODEL_VISIBLE_TOOL_INPUT_STRING_BYTES: usize = 8 * 1024;

pub(super) fn model_visible_coding_run_state(state: &CodingRunState) -> serde_json::Value {
    let validation_attempt_start = state
        .validation_attempts
        .len()
        .saturating_sub(MODEL_VISIBLE_CODING_VALIDATION_ATTEMPT_LIMIT);
    let outstanding_evidence_start = state
        .outstanding_evidence
        .len()
        .saturating_sub(MODEL_VISIBLE_CODING_EVIDENCE_LIMIT);
    let open_risk_start = state
        .open_risks
        .len()
        .saturating_sub(MODEL_VISIBLE_CODING_RISK_LIMIT);
    let changed_file_count = state.changed_files.len();
    let allowed_path_count = state.allowed_paths.len();

    serde_json::json!({
        "projection": "model_visible_bounded",
        "objective": &state.objective,
        "allowed_paths": &state.allowed_paths[..allowed_path_count.min(MODEL_VISIBLE_CODING_ALLOWED_PATH_LIMIT)],
        "allowed_path_count": allowed_path_count,
        "omitted_allowed_path_count": allowed_path_count.saturating_sub(MODEL_VISIBLE_CODING_ALLOWED_PATH_LIMIT),
        "changed_files": &state.changed_files[..changed_file_count.min(MODEL_VISIBLE_CODING_CHANGED_FILE_LIMIT)],
        "changed_file_count": changed_file_count,
        "omitted_changed_file_count": changed_file_count.saturating_sub(MODEL_VISIBLE_CODING_CHANGED_FILE_LIMIT),
        "current_diff_hash": &state.current_diff_hash,
        "current_diff_summary": &state.current_diff_summary,
        "patch_status": &state.patch_status,
        "validation_attempts": &state.validation_attempts[validation_attempt_start..],
        "validation_attempt_count": state.validation_attempts.len(),
        "omitted_validation_attempt_count": validation_attempt_start,
        "validation_status": &state.validation_status,
        "blocked_reason": &state.blocked_reason,
        "outstanding_evidence": &state.outstanding_evidence[outstanding_evidence_start..],
        "outstanding_evidence_count": state.outstanding_evidence.len(),
        "omitted_outstanding_evidence_count": outstanding_evidence_start,
        "stop_recommendation": &state.stop_recommendation,
        "open_risks": &state.open_risks[open_risk_start..],
        "open_risk_count": state.open_risks.len(),
        "omitted_open_risk_count": open_risk_start,
    })
}

pub(super) fn patch_snapshot_item_from_tool_outcome(
    outcome: &AgentToolOutcome,
) -> Option<AgentItem> {
    let coding = coding_evidence_from_tool_outcome(outcome);
    if !matches!(
        outcome.call.tool.as_str(),
        "apply_patch" | "edit_file" | "write_file"
    ) || (coding.patch_ref.is_none() && coding.changed_files.is_empty())
    {
        return None;
    }
    Some(AgentItem::PatchSnapshot {
        item_id: TaskId::generate(),
        step_index: outcome.step_index,
        call_id: outcome.call_id.clone(),
        status: outcome.status.clone(),
        summary: coding
            .diff_summary
            .clone()
            .unwrap_or_else(|| outcome.summary.clone()),
        patch_ref: coding.patch_ref.clone(),
        changed_files: coding.changed_files.clone(),
    })
}

pub(super) fn verification_item_from_tool_outcome(outcome: &AgentToolOutcome) -> Option<AgentItem> {
    let coding = coding_evidence_from_tool_outcome(outcome);
    if coding.verification_state == AgentVerificationState::NotRun {
        return None;
    }
    Some(AgentItem::VerificationResult {
        item_id: TaskId::generate(),
        step_index: outcome.step_index,
        call_id: outcome.call_id.clone(),
        command: verification_command_from_tool_outcome(outcome),
        status: coding.verification_state,
        summary: outcome.summary.clone(),
    })
}

pub(super) fn coding_evidence_from_tool_outcome(
    outcome: &AgentToolOutcome,
) -> AgentCodingToolEvidence {
    outcome
        .evidence
        .record(AGENT_CODING_EVIDENCE_KIND)
        .unwrap_or_default()
}

pub(super) fn verification_command_from_tool_outcome(outcome: &AgentToolOutcome) -> String {
    if let Some(command) = outcome
        .call
        .input
        .get("command")
        .and_then(serde_json::Value::as_str)
    {
        return command.to_string();
    }
    verification_command_from_tool_output(&outcome.output)
        .unwrap_or_else(|| outcome.call.tool.clone())
}

pub(super) fn verification_command_from_tool_output(output: &serde_json::Value) -> Option<String> {
    let program = output.get("program").and_then(serde_json::Value::as_str)?;
    let args = output
        .get("args")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if matches!(program, "bash" | "sh" | "cmd") {
        for (index, arg) in args.iter().enumerate() {
            if matches!(*arg, "-lc" | "-c" | "/C")
                && let Some(command) = args.get(index + 1)
            {
                return Some(command.to_string());
            }
        }
    }
    if args.is_empty() {
        Some(program.to_string())
    } else {
        Some(format!("{program} {}", args.join(" ")))
    }
}

pub(super) fn build_candidate_evidence_bundle(
    context_attachments: &[AgentContextAttachment],
    final_decision: &AgentLoopDecision,
    items: &[AgentItem],
) -> Option<AgentCandidateEvidenceBundle> {
    if context_attachments.is_empty() {
        return None;
    }
    let mut evidence = Vec::new();
    for item in items {
        match item {
            AgentItem::PatchSnapshot {
                status,
                summary,
                patch_ref,
                changed_files,
                ..
            } => evidence.push(AgentCandidateEvidence::Patch {
                summary: format!("{status:?}: {summary}"),
                patch_ref: patch_ref.clone(),
                changed_files: changed_files.clone(),
            }),
            AgentItem::VerificationResult {
                command,
                status,
                summary,
                ..
            } => evidence.push(AgentCandidateEvidence::Verification {
                command: command.clone(),
                status: status.clone(),
                summary: summary.clone(),
            }),
            _ => {}
        }
    }

    Some(AgentCandidateEvidenceBundle {
        bundle_id: TaskId::generate(),
        context_kind: context_attachments
            .first()
            .map(|context| context.kind.clone()),
        context_summary: context_attachments
            .first()
            .map(|context| context.summary.clone()),
        final_response: final_decision.text.clone(),
        evidence,
    })
}
