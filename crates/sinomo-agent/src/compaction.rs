use crate::agent::{
    AgentCompactionBuildOptions, AgentCompactionModelRef, AgentCompactionSummary,
    AgentCompactionTrigger, AgentCompactor, AgentConfig, AgentFollowUpMessage,
    AgentSessionProjection, AgentSteeringMessage, AgentTranscript,
};
use crate::{
    AgentCancellationToken, AgentError, DEFAULT_AGENT_COMPACTION_STRATEGY_ID, InteractionModel,
};
use sinomo_lm::{
    LanguageModelMessage, LanguageModelRequest, LanguageModelRequestItem,
    LanguageModelResponseFormat, LanguageModelRole,
};

const CODEX_STYLE_COMPACTION_SYSTEM_PROMPT: &str = r#"You are Sinomo's context checkpoint compaction engine.

Read the prior agent transcript and write a concise handoff summary for the next model turn.
Do not continue the conversation and do not answer any user request.
Only preserve information that is needed to continue the work accurately."#;

const CODEX_STYLE_COMPACTION_USER_PROMPT: &str = r#"CONTEXT CHECKPOINT COMPACTION

Create a handoff summary with these sections:

## Current Progress
- What has been completed.
- What is currently in progress.

## Key Decisions
- Decisions made and the rationale when it matters.

## Constraints And Preferences
- User requirements, policy constraints, workspace constraints, and verification expectations.

## Next Steps
- The concrete next actions the next model should take.

## Critical Data
- File paths, IDs, commands, errors, tool results, and other exact values needed to continue.

Preserve opaque identifiers exactly. Prefer recent context when older context conflicts."#;

pub struct AgentCompactionRequest<'a> {
    pub config: &'a AgentConfig,
    pub projection_builder: &'a AgentCompactor,
    pub source_transcript: &'a AgentTranscript,
    pub reason: String,
    pub trigger: AgentCompactionTrigger,
    pub token_estimate_before: usize,
    pub retained_tail_count: usize,
    pub previous_projection: Option<&'a AgentSessionProjection>,
    pub steering: &'a [AgentSteeringMessage],
    pub follow_ups: &'a [AgentFollowUpMessage],
    pub cancellation_token: &'a AgentCancellationToken,
}

pub trait AgentCompactionEngine<M: InteractionModel>: Send + Sync {
    fn compact(
        &self,
        model: &M,
        request: AgentCompactionRequest<'_>,
    ) -> Result<AgentCompactionSummary, AgentError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexStyleAgentCompactionEngine {
    pub strategy_id: String,
    pub max_summary_tokens: Option<u32>,
    pub fallback_on_model_error: bool,
}

impl Default for CodexStyleAgentCompactionEngine {
    fn default() -> Self {
        Self {
            strategy_id: DEFAULT_AGENT_COMPACTION_STRATEGY_ID.to_string(),
            max_summary_tokens: Some(1200),
            fallback_on_model_error: true,
        }
    }
}

impl<M: InteractionModel> AgentCompactionEngine<M> for CodexStyleAgentCompactionEngine {
    fn compact(
        &self,
        model: &M,
        request: AgentCompactionRequest<'_>,
    ) -> Result<AgentCompactionSummary, AgentError> {
        let model_request = self.model_request(&request)?;
        match model.complete_with_cancellation(&model_request, request.cancellation_token) {
            Ok(response) => {
                let summary = normalized_summary_text(&response.content);
                let fallback_reason = summary.is_empty().then(|| {
                    "compaction model returned an empty summary; used deterministic working-state summary"
                        .to_string()
                });
                Ok(request
                    .projection_builder
                    .compact_retaining_tail_count_with_options(
                        request.source_transcript,
                        request.reason,
                        request.token_estimate_before,
                        request.retained_tail_count,
                        request.previous_projection,
                        request.steering,
                        request.follow_ups,
                        AgentCompactionBuildOptions {
                            strategy_id: self.strategy_id.clone(),
                            trigger: request.trigger,
                            summary_override: (!summary.is_empty()).then_some(summary),
                            summary_model: (!response.content.trim().is_empty()).then_some(
                                AgentCompactionModelRef {
                                    provider: response.provider,
                                    model: response.model,
                                },
                            ),
                            fallback_reason,
                        },
                    ))
            }
            Err(error) if self.fallback_on_model_error => {
                let fallback_reason = format!("compaction model failed: {error}");
                Ok(request
                    .projection_builder
                    .compact_retaining_tail_count_with_options(
                        request.source_transcript,
                        request.reason,
                        request.token_estimate_before,
                        request.retained_tail_count,
                        request.previous_projection,
                        request.steering,
                        request.follow_ups,
                        AgentCompactionBuildOptions {
                            strategy_id: self.strategy_id.clone(),
                            trigger: request.trigger,
                            summary_override: None,
                            summary_model: None,
                            fallback_reason: Some(fallback_reason),
                        },
                    ))
            }
            Err(error) => Err(AgentError::Model(error)),
        }
    }
}

impl CodexStyleAgentCompactionEngine {
    fn model_request(
        &self,
        request: &AgentCompactionRequest<'_>,
    ) -> Result<LanguageModelRequest, AgentError> {
        // Opaque protocol state is replayed only through provider-native items.
        // It has no value as summarizer prose and may belong to a different model.
        let mut source_transcript = request.source_transcript.clone();
        for item in &mut source_transcript.items {
            if let crate::AgentItem::AssistantText { native_items, .. } = item {
                native_items.retain(|item| {
                    !matches!(item, LanguageModelRequestItem::ProviderContinuation { .. })
                });
            }
        }
        let payload = serde_json::json!({
            "reason": &request.reason,
            "trigger": &request.trigger,
            "previous_projection": request.previous_projection,
            "steering_queue": request.steering,
            "follow_up_queue": request.follow_ups,
            "source_transcript": source_transcript,
            "retained_tail_count": request.retained_tail_count,
            "token_estimate_before": request.token_estimate_before,
            "instructions": CODEX_STYLE_COMPACTION_USER_PROMPT,
        });
        let user_content =
            serde_json::to_string_pretty(&payload).map_err(|source| AgentError::ModelRequest {
                detail: source.to_string(),
            })?;
        let messages = vec![
            LanguageModelMessage {
                role: LanguageModelRole::System,
                content: CODEX_STYLE_COMPACTION_SYSTEM_PROMPT.to_string(),
            },
            LanguageModelMessage {
                role: LanguageModelRole::User,
                content: user_content,
            },
        ];
        let items = messages
            .iter()
            .map(|message| {
                LanguageModelRequestItem::message(message.role.clone(), message.content.clone())
            })
            .collect();
        Ok(LanguageModelRequest {
            provider: request.config.provider.clone(),
            model: request.config.model.clone(),
            messages,
            items,
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: Some(false),
            temperature: Some(0.0),
            max_output_tokens: self.max_summary_tokens.or(request.config.max_output_tokens),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::json!({
                "loop": "agent_compaction",
                "strategy_id": &self.strategy_id,
                "trigger": &request.trigger,
            }),
        })
    }
}

fn normalized_summary_text(value: &str) -> String {
    value.trim().to_string()
}
