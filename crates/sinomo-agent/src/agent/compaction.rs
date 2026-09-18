//! Compaction projections, working-state summaries, and transcript retention.

use super::{
    AGENT_CODING_EVIDENCE_KIND, AgentCodingToolEvidence, AgentError, AgentFollowUpMessage,
    AgentItem, AgentSteeringMessage, AgentToolStatus, AgentTranscript, AgentTranscriptItem,
    AgentVerificationState, CodingValidationStatus, ContextBudgetEstimate, Deserialize,
    LanguageModelRequestItem, Serialize, TaskId, agent_item_id, model_visible_assistant_content,
    model_visible_coding_run_state, model_visible_tool_evidence, model_visible_tool_input,
    model_visible_tool_output, native_tool_call_item_from_call,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentSessionProjection {
    pub projection_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_projection_id: Option<TaskId>,
    #[serde(with = "crate::time_serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub reason: String,
    pub summary: String,
    pub source_item_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_item_ids: Vec<TaskId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retained_item_ids: Vec<TaskId>,
    pub working_state: AgentWorkingState,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentWorkingState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_objective: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_user_request: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending_questions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_tool_calls: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_modified: Vec<String>,
    #[serde(default)]
    pub verification_state: AgentVerificationState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blockers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assumptions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decisions: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_risks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_steering: Vec<AgentSteeringMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_follow_ups: Vec<AgentFollowUpMessage>,
}

impl Default for AgentWorkingState {
    fn default() -> Self {
        Self {
            active_objective: None,
            current_user_request: None,
            next_action: None,
            pending_questions: Vec::new(),
            open_tool_calls: Vec::new(),
            files_read: Vec::new(),
            files_modified: Vec::new(),
            verification_state: AgentVerificationState::NotRun,
            blockers: Vec::new(),
            assumptions: Vec::new(),
            decisions: Vec::new(),
            open_risks: Vec::new(),
            queued_steering: Vec::new(),
            queued_follow_ups: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentCompactionSummary {
    pub compaction_id: TaskId,
    pub reason: String,
    #[serde(default = "default_agent_compaction_summary_strategy_id")]
    pub strategy_id: String,
    #[serde(default)]
    pub trigger: AgentCompactionTrigger,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_model: Option<AgentCompactionModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_retained_item: Option<TaskId>,
    pub token_estimate_before: usize,
    pub retained_tail_count: usize,
    pub projection: AgentSessionProjection,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_read: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files_modified: Vec<String>,
    pub verification_state: AgentVerificationState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open_risks: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub policy_constraints: Vec<String>,
}

pub(super) fn default_agent_compaction_summary_strategy_id() -> String {
    crate::DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AgentCompactionTrigger {
    #[default]
    Manual,
    PreSamplingBudget,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct AgentCompactionModelRef {
    pub provider: String,
    pub model: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentCompactionBuildOptions {
    pub strategy_id: String,
    pub trigger: AgentCompactionTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_override: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_model: Option<AgentCompactionModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
}

impl Default for AgentCompactionBuildOptions {
    fn default() -> Self {
        Self {
            strategy_id: crate::DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string(),
            trigger: AgentCompactionTrigger::Manual,
            summary_override: None,
            summary_model: None,
            fallback_reason: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct AgentCompactor {
    pub retained_tail_count: Option<usize>,
}

impl AgentCompactor {
    pub fn compact(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
    ) -> AgentCompactionSummary {
        let retained_tail_count = self.retained_tail_count.unwrap_or(transcript.items.len());
        self.compact_retaining_tail_count(
            transcript,
            reason,
            token_estimate_before,
            retained_tail_count,
        )
    }

    pub fn compact_retaining_tail_count(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
        retained_tail_count: usize,
    ) -> AgentCompactionSummary {
        self.compact_retaining_tail_count_with_session_state(
            transcript,
            reason,
            token_estimate_before,
            retained_tail_count,
            None,
            &[],
            &[],
        )
    }

    pub fn compact_retaining_tail_count_with_session_state(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
        retained_tail_count: usize,
        previous_projection: Option<&AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
    ) -> AgentCompactionSummary {
        self.compact_retaining_tail_count_with_options(
            transcript,
            reason,
            token_estimate_before,
            retained_tail_count,
            previous_projection,
            steering,
            follow_ups,
            AgentCompactionBuildOptions::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compact_retaining_tail_count_with_options(
        &self,
        transcript: &AgentTranscript,
        reason: impl Into<String>,
        token_estimate_before: usize,
        retained_tail_count: usize,
        previous_projection: Option<&AgentSessionProjection>,
        steering: &[AgentSteeringMessage],
        follow_ups: &[AgentFollowUpMessage],
        options: AgentCompactionBuildOptions,
    ) -> AgentCompactionSummary {
        let reason = reason.into();
        let retained = transcript.retained_tail(retained_tail_count);
        let working_state = working_state_from_items(
            &transcript.items,
            steering,
            follow_ups,
            previous_projection.map(|projection| &projection.working_state),
        );
        let projection_summary = format!(
            "Compacted {} item(s), retaining {} most recent item(s). Current work: {}",
            transcript.items.len(),
            retained.len(),
            working_state
                .current_user_request
                .as_deref()
                .or(working_state.active_objective.as_deref())
                .unwrap_or("no active user request recorded")
        );
        let summary = options
            .summary_override
            .as_ref()
            .map(|summary| summary.trim())
            .filter(|summary| !summary.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| projection_summary.clone());
        let projection = AgentSessionProjection {
            projection_id: TaskId::generate(),
            previous_projection_id: previous_projection
                .map(|projection| projection.projection_id.clone()),
            created_at: time::OffsetDateTime::now_utc(),
            reason: reason.clone(),
            summary,
            source_item_count: transcript.items.len(),
            source_item_ids: transcript.items.iter().map(agent_item_id).collect(),
            retained_item_ids: retained.iter().map(agent_item_id).collect(),
            working_state: working_state.clone(),
        };
        AgentCompactionSummary {
            compaction_id: TaskId::generate(),
            reason,
            strategy_id: options.strategy_id,
            trigger: options.trigger,
            summary: projection.summary.clone(),
            summary_model: options.summary_model,
            fallback_reason: options.fallback_reason,
            first_retained_item: retained.first().map(agent_item_id),
            token_estimate_before,
            retained_tail_count: retained.len(),
            files_read: working_state.files_read.clone(),
            files_modified: working_state.files_modified.clone(),
            verification_state: working_state.verification_state.clone(),
            open_risks: working_state.open_risks.clone(),
            projection,
            policy_constraints: vec![
                "runtime tool surface remains authoritative after compaction".to_string(),
                "verification evidence is structured state, not summary text".to_string(),
            ],
        }
    }
}

pub(super) fn working_state_from_items(
    items: &[AgentItem],
    steering: &[AgentSteeringMessage],
    follow_ups: &[AgentFollowUpMessage],
    previous: Option<&AgentWorkingState>,
) -> AgentWorkingState {
    let mut state = previous.cloned().unwrap_or_default();
    state.queued_steering = steering.to_vec();
    state.queued_follow_ups = follow_ups.to_vec();
    let mut open_tool_calls: Vec<String> = Vec::new();
    for item in items {
        match item {
            AgentItem::UserMessage { content, .. } => {
                if !content.contains("\"kind\": \"agent_queued_turn_context\"") {
                    state.current_user_request = Some(compact_multiline(content, 2000));
                    if state.active_objective.is_none() {
                        state.active_objective = state.current_user_request.clone();
                    }
                }
            }
            AgentItem::AssistantText { content, .. } => {
                let text = compact_multiline(content, 1000);
                if !text.is_empty() {
                    state.decisions.push(text);
                    if state.decisions.len() > 5 {
                        state.decisions.remove(0);
                    }
                }
            }
            AgentItem::ToolCall { call, .. } => {
                let description = format!("{}: {}", call.tool, call.summary);
                open_tool_calls.push(description);
                if call.tool == "read_file"
                    && let Some(path) = call.input.get("path").and_then(|value| value.as_str())
                {
                    push_unique(&mut state.files_read, path.to_string());
                }
            }
            AgentItem::ToolResult {
                status,
                summary,
                evidence,
                ..
            } => {
                if !open_tool_calls.is_empty() {
                    open_tool_calls.remove(0);
                }
                if matches!(status, AgentToolStatus::Failed | AgentToolStatus::Blocked) {
                    push_unique(&mut state.blockers, compact_multiline(summary, 500));
                    state.verification_state = merge_verification_state(
                        &state.verification_state,
                        &AgentVerificationState::Blocked,
                    );
                }
                let coding = evidence
                    .record::<AgentCodingToolEvidence>(AGENT_CODING_EVIDENCE_KIND)
                    .unwrap_or_default();
                for changed_file in coding.changed_files {
                    push_unique(&mut state.files_modified, changed_file);
                }
                state.verification_state =
                    merge_verification_state(&state.verification_state, &coding.verification_state);
            }
            AgentItem::PatchSnapshot {
                status,
                summary,
                changed_files,
                ..
            } => {
                for changed_file in changed_files {
                    push_unique(&mut state.files_modified, changed_file.clone());
                }
                if matches!(status, AgentToolStatus::Failed | AgentToolStatus::Blocked) {
                    push_unique(&mut state.open_risks, compact_multiline(summary, 500));
                }
            }
            AgentItem::VerificationResult {
                command,
                status,
                summary,
                ..
            } => {
                state.verification_state =
                    merge_verification_state(&state.verification_state, status);
                if matches!(
                    status,
                    AgentVerificationState::Failed | AgentVerificationState::Blocked
                ) {
                    push_unique(
                        &mut state.open_risks,
                        format!("{}: {}", command, compact_multiline(summary, 500)),
                    );
                }
            }
            AgentItem::CodingRunState {
                state: coding_state,
                ..
            } => {
                // CodingRunState is the reducer's point-in-time view. A newer state supersedes
                // validation failures and blockers from earlier attempts (for example after an
                // environment repair and successful rerun) instead of making them permanent.
                state.verification_state = agent_verification_state_from_coding_validation(
                    &coding_state.validation_status,
                );
                state.blockers.clear();
                state.open_risks.clear();
                for changed_file in &coding_state.changed_files {
                    push_unique(&mut state.files_modified, changed_file.clone());
                }
                if let Some(blocked_reason) = &coding_state.blocked_reason {
                    push_unique(
                        &mut state.blockers,
                        format!("coding run blocked: {blocked_reason:?}"),
                    );
                }
                for risk in coding_state
                    .open_risks
                    .iter()
                    .chain(&coding_state.outstanding_evidence)
                {
                    if risk == "no patch has been produced yet" && !state.files_modified.is_empty()
                    {
                        continue;
                    }
                    push_unique(&mut state.open_risks, risk.clone());
                }
            }
            AgentItem::Error { detail, .. } => {
                push_unique(&mut state.blockers, compact_multiline(detail, 500));
            }
            AgentItem::CompactionSummary { summary, .. } => {
                if state.active_objective.is_none() {
                    state.active_objective =
                        summary.projection.working_state.active_objective.clone();
                }
                if state.current_user_request.is_none() {
                    state.current_user_request = summary
                        .projection
                        .working_state
                        .current_user_request
                        .clone();
                }
            }
            AgentItem::AssistantReasoningSummary { .. }
            | AgentItem::AsyncTaskUpdate { .. }
            | AgentItem::ContextInjection { .. }
            | AgentItem::ModelUsage { .. } => {}
        }
    }
    state.open_tool_calls = open_tool_calls;
    state.next_action = if !state.open_tool_calls.is_empty() {
        Some("wait for pending tool results".to_string())
    } else if state.verification_state == AgentVerificationState::Failed {
        Some("address failed verification before finalizing".to_string())
    } else if state.verification_state == AgentVerificationState::NotRun
        && !state.files_modified.is_empty()
    {
        Some("run verification for modified files".to_string())
    } else {
        None
    };
    state
}

pub(super) fn push_unique(values: &mut Vec<String>, value: String) {
    if !value.is_empty() && !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

pub(super) fn compact_multiline(value: &str, max_chars: usize) -> String {
    let compacted = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compacted.chars().count() <= max_chars {
        return compacted;
    }
    compacted.chars().take(max_chars).collect::<String>()
}

pub(super) fn merge_verification_state(
    current: &AgentVerificationState,
    next: &AgentVerificationState,
) -> AgentVerificationState {
    match (current, next) {
        (AgentVerificationState::Failed, _) | (_, AgentVerificationState::Failed) => {
            AgentVerificationState::Failed
        }
        (AgentVerificationState::Blocked, _) | (_, AgentVerificationState::Blocked) => {
            AgentVerificationState::Blocked
        }
        (AgentVerificationState::Passed, _) | (_, AgentVerificationState::Passed) => {
            AgentVerificationState::Passed
        }
        _ => AgentVerificationState::NotRun,
    }
}

pub(super) fn agent_verification_state_from_coding_validation(
    status: &CodingValidationStatus,
) -> AgentVerificationState {
    match status {
        CodingValidationStatus::NotRun => AgentVerificationState::NotRun,
        CodingValidationStatus::Passed => AgentVerificationState::Passed,
        CodingValidationStatus::FailedEnvironment | CodingValidationStatus::FailedCommandSetup => {
            AgentVerificationState::Blocked
        }
        CodingValidationStatus::FailedPatchRelevant
        | CodingValidationStatus::TimedOut
        | CodingValidationStatus::Inconclusive => AgentVerificationState::Failed,
    }
}

pub(super) fn project_session_transcript(
    transcript: &AgentTranscript,
    retained_tail_items: Option<usize>,
) -> Result<Vec<AgentTranscriptItem>, AgentError> {
    let items = compacted_projection_items(&transcript.items, retained_tail_items);
    items
        .into_iter()
        .filter_map(project_agent_item_to_transcript_item)
        .collect()
}

pub(super) fn compacted_session_items(
    transcript: &AgentTranscript,
    compaction_item: AgentItem,
    retained_tail_count: usize,
) -> Vec<AgentItem> {
    let mut items = vec![compaction_item];
    items.extend(transcript.retained_tail(retained_tail_count));
    items
}

pub(super) fn compacted_projection_items(
    items: &[AgentItem],
    retained_tail_items: Option<usize>,
) -> Vec<AgentItem> {
    let mut items = latest_compacted_projection_items(items);
    if let Some(retained_tail_items) = retained_tail_items {
        items = if items
            .first()
            .is_some_and(|item| matches!(item, AgentItem::CompactionSummary { .. }))
        {
            let mut retained = vec![items[0].clone()];
            retained.extend(agent_item_tail(&items[1..], retained_tail_items));
            retained
        } else {
            agent_item_tail(&items, retained_tail_items)
        };
    }
    items
}

pub(super) fn latest_compacted_projection_items(items: &[AgentItem]) -> Vec<AgentItem> {
    let Some(compaction_index) = items
        .iter()
        .rposition(|item| matches!(item, AgentItem::CompactionSummary { .. }))
    else {
        return items.to_vec();
    };
    let AgentItem::CompactionSummary { summary, .. } = &items[compaction_index] else {
        return items.to_vec();
    };

    let mut compacted = vec![items[compaction_index].clone()];
    if let Some(first_retained_item) = &summary.first_retained_item
        && let Some(retained_start) = items
            .iter()
            .position(|item| agent_item_id(item) == *first_retained_item)
        && retained_start < compaction_index
    {
        compacted.extend(
            items[retained_start..compaction_index]
                .iter()
                .filter(|item| !matches!(item, AgentItem::CompactionSummary { .. }))
                .cloned(),
        );
    }
    compacted.extend(items[compaction_index + 1..].iter().cloned());
    compacted
}

pub(super) fn agent_item_tail(items: &[AgentItem], retained_tail_items: usize) -> Vec<AgentItem> {
    if items.len() <= retained_tail_items {
        return items.to_vec();
    }
    items[items.len().saturating_sub(retained_tail_items)..].to_vec()
}

pub(super) fn compaction_target_transcript_bytes(
    estimate: &ContextBudgetEstimate,
) -> Option<usize> {
    let threshold_bytes = estimate.compaction_threshold_bytes?;
    let non_transcript_bytes = estimate
        .total_bytes
        .saturating_sub(estimate.transcript_bytes);
    Some(threshold_bytes.saturating_sub(non_transcript_bytes))
}

pub(super) fn retained_transcript_tail_for_compaction(
    transcript: &[AgentTranscriptItem],
    retained_tail_count: Option<usize>,
    target_transcript_bytes: Option<usize>,
) -> Vec<AgentTranscriptItem> {
    let candidates = retained_tail_count
        .map(|count| transcript_item_tail(transcript, count))
        .unwrap_or_else(|| transcript.to_vec());
    let Some(target_transcript_bytes) = target_transcript_bytes else {
        return candidates;
    };
    if transcript_items_bytes(&candidates) <= target_transcript_bytes {
        return candidates;
    }

    let mut retained = Vec::new();
    let mut retained_bytes = 0usize;
    let keep_latest_even_when_large = retained_tail_count != Some(0);
    for item in candidates.iter().rev() {
        let item_bytes = item.content.len();
        let would_fit = retained_bytes.saturating_add(item_bytes) <= target_transcript_bytes;
        if would_fit || (retained.is_empty() && keep_latest_even_when_large) {
            retained_bytes = retained_bytes.saturating_add(item_bytes);
            retained.push(item.clone());
        } else {
            break;
        }
    }
    retained.reverse();
    retained
}

pub(super) fn transcript_item_tail(
    items: &[AgentTranscriptItem],
    retained_tail_items: usize,
) -> Vec<AgentTranscriptItem> {
    if items.len() <= retained_tail_items {
        return items.to_vec();
    }
    items[items.len().saturating_sub(retained_tail_items)..].to_vec()
}

pub(super) fn transcript_items_bytes(items: &[AgentTranscriptItem]) -> usize {
    items.iter().map(|item| item.content.len()).sum()
}

pub(super) fn compacted_transcript_view(
    summary: &AgentCompactionSummary,
    retained_tail: Vec<AgentTranscriptItem>,
) -> Result<Vec<AgentTranscriptItem>, AgentError> {
    let mut transcript = vec![AgentTranscriptItem::user(
        0,
        serde_json::to_string_pretty(&serde_json::json!({
            "kind": "prior_compaction_summary",
            "summary": summary,
        }))
        .map_err(|source| AgentError::ModelRequest {
            detail: source.to_string(),
        })?,
    )];
    transcript.extend(retained_tail);
    Ok(transcript)
}

pub(super) fn project_agent_item_to_transcript_item(
    item: AgentItem,
) -> Option<Result<AgentTranscriptItem, AgentError>> {
    match item {
        AgentItem::UserMessage {
            step_index,
            content,
            ..
        } => Some(Ok(AgentTranscriptItem::user(step_index, content))),
        AgentItem::AssistantText {
            step_index,
            content,
            native_items,
            ..
        } => Some(Ok(AgentTranscriptItem::assistant(
            step_index,
            model_visible_assistant_content(&content),
        )
        .with_native_items(native_items))),
        AgentItem::ToolResult {
            item_id: _,
            step_index,
            call_id,
            native_call_id,
            tool_name,
            status,
            summary,
            output,
            evidence,
            ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_tool_result",
                "status": &status,
                "summary": &summary,
                "output": model_visible_tool_output(None, &output),
                "evidence": model_visible_tool_evidence(&evidence),
            }))
            .map(|content| {
                let native_items = native_call_id
                    .zip(tool_name)
                    .map(|(native_call_id, tool_name)| {
                        vec![LanguageModelRequestItem::tool_call_output(
                            native_call_id,
                            tool_name,
                            serde_json::json!({
                                "status": &status,
                                "summary": &summary,
                                "output": model_visible_tool_output(None, &output),
                                "evidence": model_visible_tool_evidence(&evidence),
                            }),
                        )]
                    })
                    .unwrap_or_default();
                AgentTranscriptItem::tool(step_index, call_id, content)
                    .with_native_items(native_items)
            })
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::ToolCall {
            step_index, call, ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_tool_call",
                "tool": &call.tool,
                "input": model_visible_tool_input(&call.input),
                "summary": &call.summary,
                "call_id": &call.call_id,
            }))
            .map(|content| {
                AgentTranscriptItem::tool_call(step_index, content)
                    .with_native_items(native_tool_call_item_from_call(&call).into_iter().collect())
            })
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::CompactionSummary { summary, .. } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_compaction_summary",
                "summary": summary,
            }))
            .map(|content| AgentTranscriptItem::user(0, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::Error {
            step_index, detail, ..
        } => Some(Ok(AgentTranscriptItem::user(
            step_index,
            serde_json::json!({
                "kind": "prior_agent_error",
                "detail": detail,
            })
            .to_string(),
        ))),
        AgentItem::PatchSnapshot {
            step_index,
            call_id,
            status,
            summary,
            patch_ref,
            changed_files,
            ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_patch_snapshot",
                "call_id": call_id,
                "status": status,
                "summary": summary,
                "patch_ref": patch_ref,
                "changed_files": changed_files,
            }))
            .map(|content| AgentTranscriptItem::user(step_index, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::VerificationResult {
            step_index,
            call_id,
            command,
            status,
            summary,
            ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "prior_verification_result",
                "call_id": call_id,
                "command": command,
                "status": status,
                "summary": summary,
            }))
            .map(|content| AgentTranscriptItem::user(step_index, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::CodingRunState {
            step_index, state, ..
        } => Some(
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "coding_run_state",
                "state": model_visible_coding_run_state(&state),
            }))
            .map(|content| AgentTranscriptItem::user(step_index, content))
            .map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            }),
        ),
        AgentItem::AssistantReasoningSummary { .. }
        | AgentItem::AsyncTaskUpdate { .. }
        | AgentItem::ContextInjection { .. }
        | AgentItem::ModelUsage { .. } => None,
    }
}
