use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelProviderOptions, LanguageModelRequest,
    LanguageModelResponse, LanguageModelResponseFormat, LanguageModelResponseItem,
    LanguageModelRole, LanguageModelStreamEvent, LanguageModelUsage, PROVIDER_DEFAULT_MODEL,
    apply_provider_vendor_options_to_payload, read_server_sent_events, render_plain_prompt,
    selected_model,
};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnthropicConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub api_key: Option<String>,
    pub credentials_path: Option<PathBuf>,
    pub options: LanguageModelProviderOptions,
}

impl Default for AnthropicConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://api.anthropic.com".to_string(),
            default_model: sinomo_runtime::api::lm::ANTHROPIC_DEFAULT_MODEL.to_string(),
            api_key: None,
            credentials_path: default_api_key_credentials_path(
                "SINOMO_ANTHROPIC_CREDENTIALS_FILE",
                AnthropicProvider::PROVIDER_ID,
            ),
            options: LanguageModelProviderOptions::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AnthropicProvider {
    config: AnthropicConfig,
    http: Client,
}

impl AnthropicProvider {
    pub const PROVIDER_ID: &'static str = sinomo_runtime::api::lm::ANTHROPIC_PROVIDER_ID;

    pub fn new(config: AnthropicConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.config.credentials_path.clone()
    }

    pub fn api_key(&self) -> Result<String, LanguageModelError> {
        resolve_api_key(
            Self::PROVIDER_ID,
            &self.config.api_key,
            &self.config.credentials_path,
            &[
                "SINOMO_ANTHROPIC_API_KEY",
                "ANTHROPIC_API_KEY",
                "CLAUDE_API_KEY",
                "SINOMO_CLAUDE_API_KEY",
            ],
            "no Anthropic API key found; set ANTHROPIC_API_KEY or run `sinomo providers auth anthropic`",
        )
    }

    pub fn save_api_key(&self, api_key: &str, source: &str) -> Result<(), LanguageModelError> {
        save_api_key_credential(
            Self::PROVIDER_ID,
            &self.config.credentials_path,
            api_key,
            source,
        )
    }

    pub fn test_connection(&self) -> Result<(), LanguageModelError> {
        let request = LanguageModelRequest {
            provider: Self::PROVIDER_ID.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            messages: vec![LanguageModelMessage {
                role: LanguageModelRole::User,
                content: "Reply with ok.".to_string(),
            }],
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: None,
            max_output_tokens: Some(4),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        };
        self.complete(&request).map(|_| ())
    }
}

impl Default for AnthropicProvider {
    fn default() -> Self {
        Self::new(AnthropicConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::parse_declared_context_window_tokens(model).or_else(|| {
        let model = model.to_ascii_lowercase();
        if model.contains("claude") {
            Some(200_000)
        } else {
            None
        }
    })
}

impl LanguageModelProvider for AnthropicProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: crate::LanguageModelHostAccess::NetworkOnly,
            capabilities: crate::LanguageModelProviderCapabilities::default(),
            supports_json_object: false,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let api_key = self.api_key()?;
        let url = format!(
            "{}/v1/messages",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(anthropic_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&anthropic_messages_payload(
                request,
                &model,
                &self.config.options,
            ))
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail: format!("Anthropic Messages API failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        let content = extract_anthropic_text(&value);
        let usage = value.get("usage").map(|usage| LanguageModelUsage {
            input_tokens: usage
                .get("input_tokens")
                .and_then(serde_json::Value::as_u64),
            output_tokens: usage
                .get("output_tokens")
                .and_then(serde_json::Value::as_u64),
        });

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content,
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage,
        })
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let api_key = self.api_key()?;
        let url = format!(
            "{}/v1/messages",
            self.config.api_base_url.trim_end_matches('/')
        );
        let mut payload = anthropic_messages_payload(request, &model, &self.config.options);
        payload["stream"] = serde_json::json!(true);
        let mut headers = anthropic_headers(&api_key, Self::PROVIDER_ID, &model)?;
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        let response = self
            .http
            .post(url)
            .headers(headers)
            .json(&payload)
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail: format!("Anthropic Messages API failed: HTTP {status}: {body}"),
            });
        }

        on_event(LanguageModelStreamEvent::Started {
            provider: Self::PROVIDER_ID.to_string(),
            model: model.clone(),
        });
        stream_anthropic_messages_response(response, &model, on_event)
    }
}

fn anthropic_headers(
    api_key: &str,
    provider: &str,
    model: &str,
) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    headers.insert(
        "x-api-key",
        HeaderValue::from_str(api_key).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

pub(crate) fn anthropic_messages_payload(
    request: &LanguageModelRequest,
    model: &str,
    options: &LanguageModelProviderOptions,
) -> serde_json::Value {
    let system = request
        .messages
        .iter()
        .filter(|message| message.role == LanguageModelRole::System)
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let mut messages: Vec<serde_json::Value> = request
        .messages
        .iter()
        .filter(|message| message.role != LanguageModelRole::System)
        .map(|message| {
            serde_json::json!({
                "role": match message.role {
                    LanguageModelRole::Assistant => "assistant",
                    _ => "user",
                },
                "content": message.content,
            })
        })
        .collect();
    if messages.is_empty() {
        messages.push(serde_json::json!({
            "role": "user",
            "content": render_plain_prompt(request),
        }));
    }

    let thinking_budget = options.reasoning_budget_tokens;
    let adaptive_effort = options
        .reasoning_effort
        .and_then(|effort| effort.as_anthropic_effort_value());
    let mut max_tokens = request.max_output_tokens.unwrap_or(1024);
    if let Some(budget) = thinking_budget
        && max_tokens <= budget
    {
        max_tokens = budget.saturating_add(1024);
    }

    let mut payload = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
    });
    if !system.is_empty() {
        payload["system"] = serde_json::json!(system);
    }
    if let Some(temperature) = request.temperature {
        payload["temperature"] = serde_json::json!(temperature);
    }
    if let Some(budget) = thinking_budget
        && budget > 0
    {
        payload["thinking"] = serde_json::json!({
            "type": "enabled",
            "budget_tokens": budget,
        });
    }
    if thinking_budget.is_none() && adaptive_effort.is_some() {
        payload["thinking"] = serde_json::json!({ "type": "adaptive" });
    }
    if let Some(effort) = adaptive_effort {
        payload["output_config"] = serde_json::json!({ "effort": effort });
    }
    apply_provider_vendor_options_to_payload(&mut payload, options);
    payload
}

pub(crate) fn extract_anthropic_text(value: &serde_json::Value) -> String {
    value
        .get("content")
        .and_then(serde_json::Value::as_array)
        .map(|content| {
            content
                .iter()
                .filter_map(|block| block.get("text").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn stream_anthropic_messages_response(
    response: reqwest::blocking::Response,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError> {
    stream_anthropic_messages_response_with_provider(
        response,
        AnthropicProvider::PROVIDER_ID,
        model,
        on_event,
    )
}

pub(crate) fn stream_anthropic_messages_response_with_provider(
    response: reqwest::blocking::Response,
    provider: &str,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError> {
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut usage = LanguageModelUsage {
        input_tokens: None,
        output_tokens: None,
    };
    let mut finish_reason = None;
    let mut saw_usage = false;
    let mut emitted_finished = false;

    read_server_sent_events(response, provider, model, |event| {
        let mut value =
            serde_json::from_str::<serde_json::Value>(&event.data).map_err(|source| {
                LanguageModelError::ProviderInvocationFailed {
                    provider: provider.to_string(),
                    model: model.to_string(),
                    detail: format!(
                        "failed to parse Anthropic stream event: {source}: {}",
                        event.data
                    ),
                }
            })?;
        if value.get("type").is_none()
            && let (Some(event_name), Some(object)) = (event.event, value.as_object_mut())
        {
            object.insert("type".to_string(), serde_json::json!(event_name));
        }
        match value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
        {
            "message_start" => {
                if let Some(input_tokens) = value
                    .pointer("/message/usage/input_tokens")
                    .and_then(serde_json::Value::as_u64)
                {
                    usage.input_tokens = Some(input_tokens);
                    saw_usage = true;
                }
            }
            "content_block_delta" => {
                let delta_type = value
                    .pointer("/delta/type")
                    .and_then(serde_json::Value::as_str);
                if delta_type == Some("text_delta")
                    && let Some(delta) = value
                        .pointer("/delta/text")
                        .and_then(serde_json::Value::as_str)
                    && !delta.is_empty()
                {
                    content.push_str(delta);
                    on_event(LanguageModelStreamEvent::TextDelta {
                        delta: delta.to_string(),
                    });
                }
                if delta_type == Some("thinking_delta")
                    && let Some(delta) = anthropic_thinking_delta(&value)
                    && !delta.is_empty()
                {
                    reasoning.push_str(delta);
                    on_event(LanguageModelStreamEvent::ReasoningDelta {
                        delta: delta.to_string(),
                    });
                }
            }
            "message_delta" => {
                if let Some(output_tokens) = value
                    .pointer("/usage/output_tokens")
                    .and_then(serde_json::Value::as_u64)
                {
                    usage.output_tokens = Some(output_tokens);
                    saw_usage = true;
                }
                if let Some(reason) = value
                    .pointer("/delta/stop_reason")
                    .and_then(serde_json::Value::as_str)
                {
                    finish_reason = Some(reason.to_string());
                }
            }
            "message_stop" => {
                if saw_usage {
                    on_event(LanguageModelStreamEvent::Usage {
                        usage: usage.clone(),
                    });
                }
                on_event(LanguageModelStreamEvent::Finished {
                    finish_reason: finish_reason.clone(),
                });
                emitted_finished = true;
            }
            "error" => {
                let detail = value
                    .pointer("/error/message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Anthropic stream failed")
                    .to_string();
                on_event(LanguageModelStreamEvent::Error {
                    detail: detail.clone(),
                });
                return Err(LanguageModelError::ProviderInvocationFailed {
                    provider: provider.to_string(),
                    model: model.to_string(),
                    detail,
                });
            }
            _ => {}
        }
        Ok(())
    })?;

    let usage = saw_usage.then_some(usage);
    if !emitted_finished {
        if let Some(usage) = &usage {
            on_event(LanguageModelStreamEvent::Usage {
                usage: usage.clone(),
            });
        }
        on_event(LanguageModelStreamEvent::Finished {
            finish_reason: finish_reason.clone(),
        });
    }

    let items = (!reasoning.trim().is_empty())
        .then_some(LanguageModelResponseItem::ReasoningSummary { summary: reasoning })
        .into_iter()
        .collect();
    Ok(LanguageModelResponse {
        provider: provider.to_string(),
        model: model.to_string(),
        content,
        items,
        tool_calls: Vec::new(),
        usage,
    })
}

fn anthropic_thinking_delta(value: &serde_json::Value) -> Option<&str> {
    value
        .pointer("/delta/thinking")
        .and_then(serde_json::Value::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProviderReasoningEffort;

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: "anthropic".to_string(),
            model: "provider-default".to_string(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: "Return JSON only.".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "Summarize evt_1.".to_string(),
                },
            ],
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: Some(0.2),
            max_output_tokens: Some(200),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::json!({ "job_id": "task_test" }),
        }
    }

    #[test]
    fn explicit_thinking_delta_is_available_as_reasoning_output() {
        let value = serde_json::json!({
            "type": "content_block_delta",
            "delta": { "type": "thinking_delta", "thinking": "Check the constraints." }
        });
        assert_eq!(
            anthropic_thinking_delta(&value),
            Some("Check the constraints.")
        );
    }

    #[test]
    fn payload_uses_messages_shape() {
        let payload = anthropic_messages_payload(
            &request(),
            "claude-sonnet-4-20250514",
            &LanguageModelProviderOptions::default(),
        );

        assert_eq!(payload["model"], "claude-sonnet-4-20250514");
        assert_eq!(payload["system"], "Return JSON only.");
        assert_eq!(payload["messages"][0]["role"], "user");
        assert_eq!(payload["max_tokens"], 200);
    }

    #[test]
    fn payload_applies_adaptive_effort_options() {
        let mut options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::High),
            ..LanguageModelProviderOptions::default()
        };
        options
            .vendor_options
            .insert("metadata.trace".to_string(), "true".to_string());

        let payload = anthropic_messages_payload(&request(), "claude-sonnet-4-20250514", &options);

        assert_eq!(payload["thinking"]["type"], "adaptive");
        assert_eq!(payload["output_config"]["effort"], "high");
        assert_eq!(payload["max_tokens"], 200);
        assert_eq!(payload["metadata"]["trace"], true);
    }

    #[test]
    fn payload_applies_explicit_thinking_budget_options() {
        let options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::Max),
            reasoning_budget_tokens: Some(10_000),
            ..LanguageModelProviderOptions::default()
        };

        let payload = anthropic_messages_payload(&request(), "claude-sonnet-4-20250514", &options);

        assert_eq!(payload["thinking"]["type"], "enabled");
        assert_eq!(payload["thinking"]["budget_tokens"], 10_000);
        assert_eq!(payload["output_config"]["effort"], "max");
        assert_eq!(payload["max_tokens"], 11_024);
    }

    #[test]
    fn provider_reports_http_capabilities() {
        let info = AnthropicProvider::default().info();

        assert_eq!(info.provider, AnthropicProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(!info.supports_json_object);
    }
}
