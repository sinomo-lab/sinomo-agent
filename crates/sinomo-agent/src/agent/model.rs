//! Provider-neutral model events, decision parsing, and malformed-output repair.

use super::{
    AgentError, AgentLoopDecision, AgentModelDecisionSource, AgentOutputFormat, AgentToolCall,
    AgentToolDescriptor, ContextAssemblyReport, Deserialize, LanguageModelErrorCategory,
    LanguageModelMessage, LanguageModelRequest, LanguageModelRequestItem, LanguageModelResponse,
    LanguageModelRole, LanguageModelStreamEvent, LanguageModelToolAdapterSource,
    LanguageModelToolCall, Serialize, TaskId, classify_language_model_error_detail,
    collect_language_model_tool_calls, language_model_tool_spec_from_agent_tool,
    parse_agent_loop_response, truncate_utf8_lossy,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentModelEvent {
    Started {
        step_index: usize,
        provider: String,
        model: String,
    },
    ContextAssembled {
        step_index: usize,
        report: ContextAssemblyReport,
    },
    AssistantTextDelta {
        step_index: usize,
        delta: String,
    },
    AssistantReasoningDelta {
        step_index: usize,
        delta: String,
    },
    AssistantReasoningSummary {
        step_index: usize,
        summary: String,
    },
    ToolCallStarted {
        step_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_index: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        tool: String,
    },
    ToolCallDelta {
        step_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool: Option<String>,
        arguments_delta: String,
    },
    ToolCallCompleted {
        step_index: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_index: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
        call: AgentToolCall,
    },
    Usage {
        step_index: usize,
        provider: String,
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<sinomo_lm::LanguageModelUsage>,
    },
    Finished {
        step_index: usize,
        response_mode: crate::AgentLoopResponseMode,
        tool_call_count: usize,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finish_reason: Option<String>,
    },
    Error {
        step_index: usize,
        detail: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        category: Option<LanguageModelErrorCategory>,
    },
    Aborted {
        reason: String,
    },
    /// The loop stopped before its next model request because the session
    /// was paused.
    Paused {
        reason: String,
    },
}

impl AgentModelEvent {
    pub(super) fn from_stream_event(
        step_index: usize,
        provider: &str,
        model: &str,
        stream_event: &LanguageModelStreamEvent,
    ) -> Option<Self> {
        match stream_event {
            LanguageModelStreamEvent::Started { provider, model } => Some(Self::Started {
                step_index,
                provider: provider.clone(),
                model: model.clone(),
            }),
            LanguageModelStreamEvent::TextDelta { delta } => {
                (!delta.is_empty()).then(|| Self::AssistantTextDelta {
                    step_index,
                    delta: delta.clone(),
                })
            }
            LanguageModelStreamEvent::ReasoningDelta { delta } => {
                Some(Self::AssistantReasoningDelta {
                    step_index,
                    delta: delta.clone(),
                })
            }
            LanguageModelStreamEvent::ReasoningSummary { summary } => {
                Some(Self::AssistantReasoningSummary {
                    step_index,
                    summary: summary.clone(),
                })
            }
            LanguageModelStreamEvent::ToolCallStarted { call_id, name } => {
                Some(Self::ToolCallStarted {
                    step_index,
                    call_index: None,
                    call_id: call_id.clone(),
                    tool: name.clone(),
                })
            }
            LanguageModelStreamEvent::ToolCallDelta {
                call_id,
                name,
                arguments_delta,
            } => Some(Self::ToolCallDelta {
                step_index,
                call_id: call_id.clone(),
                tool: name.clone(),
                arguments_delta: arguments_delta.clone(),
            }),
            LanguageModelStreamEvent::ToolCallCompleted {
                call_id,
                name,
                input,
            } => Some(Self::ToolCallCompleted {
                step_index,
                call_index: None,
                call_id: call_id.clone(),
                call: AgentToolCall {
                    call_id: call_id.clone(),
                    tool: name.clone(),
                    input: input.clone(),
                    r#async: false,
                    summary: String::new(),
                },
            }),
            LanguageModelStreamEvent::Usage { usage } => Some(Self::Usage {
                step_index,
                provider: provider.to_string(),
                model: model.to_string(),
                usage: Some(usage.clone()),
            }),
            LanguageModelStreamEvent::Finished { .. } => None,
            LanguageModelStreamEvent::Error { detail } => Some(Self::Error {
                step_index,
                category: classify_language_model_error_detail(detail),
                detail: detail.clone(),
            }),
            LanguageModelStreamEvent::Aborted { reason } => Some(Self::Aborted {
                reason: reason.clone(),
            }),
        }
    }

    pub(super) fn from_model_response(
        step_index: usize,
        response: &LanguageModelResponse,
        decision: &AgentLoopDecision,
        stream_events: Vec<LanguageModelStreamEvent>,
    ) -> Vec<Self> {
        let mut events = Vec::new();
        let mut saw_started = false;
        let mut saw_text_delta = false;
        let mut saw_usage = false;
        let mut saw_finished = false;
        let mut saw_tool_completed = false;
        for stream_event in stream_events {
            match stream_event {
                LanguageModelStreamEvent::Started { provider, model } => {
                    saw_started = true;
                    events.push(AgentModelEvent::Started {
                        step_index,
                        provider,
                        model,
                    });
                }
                LanguageModelStreamEvent::TextDelta { delta } => {
                    if !delta.is_empty() {
                        saw_text_delta = true;
                        events.push(AgentModelEvent::AssistantTextDelta { step_index, delta });
                    }
                }
                LanguageModelStreamEvent::ReasoningDelta { delta } => {
                    events.push(AgentModelEvent::AssistantReasoningDelta { step_index, delta });
                }
                LanguageModelStreamEvent::ReasoningSummary { summary } => {
                    events.push(AgentModelEvent::AssistantReasoningSummary {
                        step_index,
                        summary,
                    });
                }
                LanguageModelStreamEvent::ToolCallStarted { call_id, name } => {
                    events.push(AgentModelEvent::ToolCallStarted {
                        step_index,
                        call_index: decision
                            .tool_calls
                            .iter()
                            .position(|call| call.tool == name),
                        call_id,
                        tool: name,
                    });
                }
                LanguageModelStreamEvent::ToolCallDelta {
                    call_id,
                    name,
                    arguments_delta,
                } => {
                    events.push(AgentModelEvent::ToolCallDelta {
                        step_index,
                        call_id,
                        tool: name,
                        arguments_delta,
                    });
                }
                LanguageModelStreamEvent::ToolCallCompleted {
                    call_id,
                    name,
                    input,
                } => {
                    saw_tool_completed = true;
                    let call = AgentToolCall {
                        call_id: call_id.clone(),
                        tool: name.clone(),
                        input,
                        r#async: false,
                        summary: String::new(),
                    };
                    events.push(AgentModelEvent::ToolCallCompleted {
                        step_index,
                        call_index: decision
                            .tool_calls
                            .iter()
                            .position(|candidate| candidate.tool == name),
                        call_id,
                        call,
                    });
                }
                LanguageModelStreamEvent::Usage { usage } => {
                    saw_usage = true;
                    events.push(AgentModelEvent::Usage {
                        step_index,
                        provider: response.provider.clone(),
                        model: response.model.clone(),
                        usage: Some(usage),
                    });
                }
                LanguageModelStreamEvent::Finished { finish_reason } => {
                    saw_finished = true;
                    events.push(AgentModelEvent::Finished {
                        step_index,
                        response_mode: decision.response_mode.clone(),
                        tool_call_count: decision.tool_calls.len(),
                        finish_reason,
                    });
                }
                LanguageModelStreamEvent::Error { detail } => {
                    events.push(AgentModelEvent::Error {
                        step_index,
                        category: classify_language_model_error_detail(&detail),
                        detail,
                    });
                }
                LanguageModelStreamEvent::Aborted { reason } => {
                    events.push(AgentModelEvent::Aborted { reason });
                }
            }
        }
        if !saw_started {
            events.insert(
                0,
                AgentModelEvent::Started {
                    step_index,
                    provider: response.provider.clone(),
                    model: response.model.clone(),
                },
            );
        }
        if !saw_text_delta && !response.content.is_empty() {
            events.push(AgentModelEvent::AssistantTextDelta {
                step_index,
                delta: response.content.clone(),
            });
        }
        if !saw_tool_completed {
            for (call_index, call) in decision.tool_calls.iter().enumerate() {
                events.push(AgentModelEvent::ToolCallStarted {
                    step_index,
                    call_index: Some(call_index),
                    call_id: None,
                    tool: call.tool.clone(),
                });
                events.push(AgentModelEvent::ToolCallCompleted {
                    step_index,
                    call_index: Some(call_index),
                    call_id: None,
                    call: call.clone(),
                });
            }
        }
        if !saw_usage && response.usage.is_some() {
            events.push(AgentModelEvent::Usage {
                step_index,
                provider: response.provider.clone(),
                model: response.model.clone(),
                usage: response.usage.clone(),
            });
        }
        if !saw_finished {
            events.push(AgentModelEvent::Finished {
                step_index,
                response_mode: decision.response_mode.clone(),
                tool_call_count: decision.tool_calls.len(),
                finish_reason: None,
            });
        }
        events
    }

    pub(super) fn from_unparsed_model_response(
        step_index: usize,
        response: &LanguageModelResponse,
        stream_events: Vec<LanguageModelStreamEvent>,
        detail: String,
    ) -> Vec<Self> {
        let mut events = Vec::new();
        let mut saw_started = false;
        let mut saw_text_delta = false;
        let mut saw_usage = false;
        for stream_event in stream_events {
            match stream_event {
                LanguageModelStreamEvent::Started { provider, model } => {
                    saw_started = true;
                    events.push(AgentModelEvent::Started {
                        step_index,
                        provider,
                        model,
                    });
                }
                LanguageModelStreamEvent::TextDelta { delta } => {
                    if !delta.is_empty() {
                        saw_text_delta = true;
                        events.push(AgentModelEvent::AssistantTextDelta { step_index, delta });
                    }
                }
                LanguageModelStreamEvent::ReasoningDelta { delta } => {
                    events.push(AgentModelEvent::AssistantReasoningDelta { step_index, delta });
                }
                LanguageModelStreamEvent::ReasoningSummary { summary } => {
                    events.push(AgentModelEvent::AssistantReasoningSummary {
                        step_index,
                        summary,
                    });
                }
                LanguageModelStreamEvent::ToolCallStarted { call_id, name } => {
                    events.push(AgentModelEvent::ToolCallStarted {
                        step_index,
                        call_index: None,
                        call_id,
                        tool: name,
                    });
                }
                LanguageModelStreamEvent::ToolCallDelta {
                    call_id,
                    name,
                    arguments_delta,
                } => {
                    events.push(AgentModelEvent::ToolCallDelta {
                        step_index,
                        call_id,
                        tool: name,
                        arguments_delta,
                    });
                }
                LanguageModelStreamEvent::ToolCallCompleted {
                    call_id,
                    name,
                    input,
                } => {
                    events.push(AgentModelEvent::ToolCallCompleted {
                        step_index,
                        call_index: None,
                        call_id: call_id.clone(),
                        call: AgentToolCall {
                            call_id,
                            tool: name,
                            input,
                            r#async: false,
                            summary: String::new(),
                        },
                    });
                }
                LanguageModelStreamEvent::Usage { usage } => {
                    saw_usage = true;
                    events.push(AgentModelEvent::Usage {
                        step_index,
                        provider: response.provider.clone(),
                        model: response.model.clone(),
                        usage: Some(usage),
                    });
                }
                LanguageModelStreamEvent::Finished { .. } => {}
                LanguageModelStreamEvent::Error { detail } => {
                    events.push(AgentModelEvent::Error {
                        step_index,
                        category: classify_language_model_error_detail(&detail),
                        detail,
                    });
                }
                LanguageModelStreamEvent::Aborted { reason } => {
                    events.push(AgentModelEvent::Aborted { reason });
                }
            }
        }
        if !saw_started {
            events.insert(
                0,
                AgentModelEvent::Started {
                    step_index,
                    provider: response.provider.clone(),
                    model: response.model.clone(),
                },
            );
        }
        if !saw_text_delta && !response.content.is_empty() {
            events.push(AgentModelEvent::AssistantTextDelta {
                step_index,
                delta: response.content.clone(),
            });
        }
        if !saw_usage && response.usage.is_some() {
            events.push(AgentModelEvent::Usage {
                step_index,
                provider: response.provider.clone(),
                model: response.model.clone(),
                usage: response.usage.clone(),
            });
        }
        events.push(AgentModelEvent::Error {
            step_index,
            category: classify_language_model_error_detail(&detail)
                .or(Some(LanguageModelErrorCategory::RepairNeeded)),
            detail,
        });
        events
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentModelTurn {
    pub step_index: usize,
    pub request: LanguageModelRequest,
    pub response: LanguageModelResponse,
    pub decision: AgentLoopDecision,
    #[serde(default)]
    pub decision_source: AgentModelDecisionSource,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<AgentModelEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentTranscriptItem {
    pub step_index: usize,
    pub role: AgentTranscriptRole,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<TaskId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_items: Vec<LanguageModelRequestItem>,
}

impl AgentTranscriptItem {
    pub fn user(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::User,
            content: content.into(),
            tool_call_id: None,
            native_items: Vec::new(),
        }
    }

    pub fn assistant(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::Assistant,
            content: content.into(),
            tool_call_id: None,
            native_items: Vec::new(),
        }
    }

    pub fn tool(step_index: usize, tool_call_id: TaskId, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::Tool,
            content: content.into(),
            tool_call_id: Some(tool_call_id),
            native_items: Vec::new(),
        }
    }

    pub fn tool_call(step_index: usize, content: impl Into<String>) -> Self {
        Self {
            step_index,
            role: AgentTranscriptRole::ToolCall,
            content: content.into(),
            tool_call_id: None,
            native_items: Vec::new(),
        }
    }

    pub fn with_native_items(mut self, native_items: Vec<LanguageModelRequestItem>) -> Self {
        self.native_items = native_items;
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTranscriptRole {
    User,
    Assistant,
    ToolCall,
    Tool,
}

pub(super) fn parse_agent_model_decision(
    response: &LanguageModelResponse,
    stream_events: &[LanguageModelStreamEvent],
    available_tool_specs: &[AgentToolDescriptor],
    output_format: &AgentOutputFormat,
) -> Result<ParsedAgentModelDecision, AgentError> {
    let language_model_tools: Vec<_> = available_tool_specs
        .iter()
        .map(language_model_tool_spec_from_agent_tool)
        .collect();
    let tool_output = collect_language_model_tool_calls(
        &response.provider,
        &response.model,
        response,
        stream_events,
        &language_model_tools,
    );
    let tool_calls = agent_tool_calls_from_language_model(&tool_output.tool_calls);
    if !tool_calls.is_empty() {
        let text = tool_output
            .text
            .as_deref()
            .unwrap_or(&response.content)
            .trim();
        let source = match tool_output.source {
            LanguageModelToolAdapterSource::VendorFallback => {
                AgentModelDecisionSource::VendorFallbackToolCalls
            }
            LanguageModelToolAdapterSource::Native | LanguageModelToolAdapterSource::None => {
                AgentModelDecisionSource::NativeToolCalls
            }
        };
        return Ok(ParsedAgentModelDecision {
            decision: AgentLoopDecision {
                response_mode: crate::AgentLoopResponseMode::Defer,
                text: if text.is_empty() {
                    "The model requested tool execution.".to_string()
                } else {
                    text.to_string()
                },
                tool_calls,
                background_handoff: None,
                next_state: crate::AgentInteractionState::Waiting,
            },
            source,
        });
    }
    match output_format {
        AgentOutputFormat::JsonDecision => {
            parse_agent_loop_decision_strict(&response.content).map(|decision| {
                ParsedAgentModelDecision {
                    decision,
                    source: AgentModelDecisionSource::StrictJsonDecision,
                }
            })
        }
        AgentOutputFormat::Text => {
            parse_agent_loop_response(output_format, &response.content).map(|decision| {
                ParsedAgentModelDecision {
                    decision,
                    source: AgentModelDecisionSource::TextFallback,
                }
            })
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ParsedAgentModelDecision {
    pub(super) decision: AgentLoopDecision,
    pub(super) source: AgentModelDecisionSource,
}

pub(super) fn agent_tool_calls_from_language_model(
    calls: &[LanguageModelToolCall],
) -> Vec<AgentToolCall> {
    calls
        .iter()
        .map(|call| AgentToolCall {
            call_id: call.call_id.clone(),
            tool: call.name.clone(),
            input: call.input.clone(),
            r#async: false,
            summary: call
                .summary
                .clone()
                .unwrap_or_else(|| format!("Native model tool call `{}`.", call.name)),
        })
        .collect()
}

pub(super) fn parse_agent_loop_decision_strict(raw: &str) -> Result<AgentLoopDecision, AgentError> {
    let trimmed = raw.trim();
    let json = trimmed
        .strip_prefix("```json")
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    let value = serde_json::from_str::<serde_json::Value>(json).map_err(|source| {
        AgentError::ModelResponse {
            detail: format!("expected JSON object decision, got invalid JSON: {source}: {raw}"),
        }
    })?;
    if let Ok(decision) = serde_json::from_value::<AgentLoopDecision>(value.clone()) {
        return Ok(decision);
    }
    let Some(text) = value.get("text").and_then(serde_json::Value::as_str) else {
        return Err(AgentError::ModelResponse {
            detail: format!("model decision JSON is missing required string field `text`: {raw}"),
        });
    };
    Ok(AgentLoopDecision {
        response_mode: value
            .get("response_mode")
            .and_then(serde_json::Value::as_str)
            .and_then(crate::parse_agent_loop_response_mode)
            .unwrap_or(crate::AgentLoopResponseMode::Answer),
        text: text.to_string(),
        tool_calls: value
            .get("tool_calls")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .unwrap_or_default(),
        background_handoff: value
            .get("background_handoff")
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok()),
        next_state: value
            .get("next_state")
            .and_then(serde_json::Value::as_str)
            .and_then(crate::parse_agent_interaction_state)
            .unwrap_or(crate::AgentInteractionState::Listening),
    })
}

pub(super) fn model_repair_parse_error_detail(
    repair_attempt: usize,
    max_model_repair_attempts: Option<usize>,
    error: &AgentError,
) -> String {
    if let Some(max_model_repair_attempts) = max_model_repair_attempts {
        format!(
            "model response parse failed on attempt {} of {}: {error}",
            repair_attempt + 1,
            max_model_repair_attempts + 1,
        )
    } else {
        format!(
            "model response parse failed on attempt {} with no repair attempt limit: {error}",
            repair_attempt + 1,
        )
    }
}

pub(super) fn repair_request_for_model_response(
    request: &LanguageModelRequest,
    previous_response: &str,
    parse_error: &str,
    repair_attempt: usize,
) -> Result<LanguageModelRequest, AgentError> {
    let mut request = request.clone();
    let previous_response = truncate_utf8_lossy(previous_response.as_bytes(), 8 * 1024);
    let content = serde_json::to_string_pretty(&serde_json::json!({
        "kind": "agent_model_response_repair",
        "repair_attempt": repair_attempt,
        "parse_error": parse_error,
        "previous_response": previous_response.text,
        "previous_response_truncated": previous_response.truncated,
        "required_response_shape": {
            "response_mode": "answer|acknowledge|clarify|defer|silent",
            "text": "short user-facing response",
            "tool_calls": [],
            "background_handoff": null,
            "next_state": "speaking|listening|waiting|delegating|recovering|idle"
        },
        "instruction": "Return only a valid JSON object matching required_response_shape. Do not include markdown fences or explanatory text."
    }))
    .map_err(|source| AgentError::ModelRequest {
        detail: source.to_string(),
    })?;
    request.messages.push(LanguageModelMessage {
        role: LanguageModelRole::User,
        content,
    });
    if let Some(metadata) = request.metadata.as_object_mut() {
        metadata.insert(
            "model_response_repair_attempt".to_string(),
            serde_json::json!(repair_attempt),
        );
        metadata.insert(
            "model_response_parse_error".to_string(),
            serde_json::json!(parse_error),
        );
    } else {
        request.metadata = serde_json::json!({
            "model_response_repair_attempt": repair_attempt,
            "model_response_parse_error": parse_error,
        });
    }
    Ok(request)
}
