//! DeepSeek Messages wire contract, compared with deepseek-harness 477b4f4205.
use super::*;
use crate::{LanguageModelRequestItem, LanguageModelToolChoice, ProviderReasoningEffort};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(super) fn uses_messages(base: &str) -> bool {
    let base = base.trim_end_matches('/');
    base.ends_with("/anthropic") || base.ends_with("/anthropic/v1") || base.ends_with("/messages")
}

fn endpoint(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/messages") {
        base.into()
    } else if base.ends_with("/v1") {
        format!("{base}/messages")
    } else {
        format!("{base}/v1/messages")
    }
}

fn error(model: &str, detail: impl ToString) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: DeepSeekProvider::PROVIDER_ID.into(),
        model: model.into(),
        detail: detail.to_string(),
    }
}

impl DeepSeekProvider {
    pub(super) fn complete_messages(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let payload = payload(request, &model, &self.config.options)?;
        let key = self.api_key()?;
        let response = self
            .http
            .post(endpoint(&self.config.api_base_url))
            .header(
                "x-api-key",
                HeaderValue::from_str(&key).map_err(|e| error(&model, e))?,
            )
            .header("anthropic-version", "2023-06-01")
            .header(ACCEPT, "text/event-stream")
            .header(USER_AGENT, "Sinomo/0.1")
            .json(&payload)
            .send()
            .map_err(|e| error(&model, e))?;
        if !response.status().is_success() {
            let status = response.status();
            return Err(error(
                &model,
                format!(
                    "DeepSeek Messages API failed: HTTP {status}: {}",
                    response.text().unwrap_or_default()
                ),
            ));
        }
        crate::anthropic::stream_anthropic_messages_response_with_provider(
            response,
            Self::PROVIDER_ID,
            &model,
            on_event,
        )
    }
}

pub(super) fn payload(
    request: &LanguageModelRequest,
    model: &str,
    options: &crate::LanguageModelProviderOptions,
) -> Result<Value, LanguageModelError> {
    // Switching endpoints must not replay Chat Completions assistant envelopes
    // as Messages blocks. The ordinary transcript remains the fallback.
    let mut request = request.clone();
    request.items.retain(|item| {
        !matches!(item,
            LanguageModelRequestItem::ProviderContinuation { continuation }
            if continuation.items.iter().any(|item| !item["content"].is_array())
        )
    });
    let mut messages = crate::anthropic::wire::messages(&request, model);
    if messages.is_empty() {
        messages = request.messages.iter().filter(|m| m.role != LanguageModelRole::System)
            .map(|m| json!({"role":if m.role == LanguageModelRole::Assistant {"assistant"} else {"user"}, "content":[{"type":"text","text":m.content}]})).collect();
    }
    validate_history(&mut messages, model)?;
    if options
        .reasoning_budget_tokens
        .is_some_and(|budget| budget > 0)
    {
        return Err(error(
            model,
            "DeepSeek Messages uses reasoning effort, not a thinking token budget",
        ));
    }
    let selected_effort = if options.reasoning_budget_tokens == Some(0) {
        ProviderReasoningEffort::None
    } else {
        options
            .reasoning_effort
            .unwrap_or(ProviderReasoningEffort::High)
    };
    let effort = match selected_effort {
        ProviderReasoningEffort::None => "off",
        ProviderReasoningEffort::Minimal | ProviderReasoningEffort::Low => "low",
        ProviderReasoningEffort::Max | ProviderReasoningEffort::Ultra => "max",
        _ => "high",
    };
    let mut payload = json!({"model":model, "stream":true, "messages":messages,
        "max_tokens":request.max_output_tokens.unwrap_or(256_000),
        "thinking":{"type":if effort == "off" {"disabled"} else {"enabled"}}});
    if effort != "off" {
        payload["output_config"] = json!({"effort":effort});
    }
    let mut system = request
        .messages
        .iter()
        .filter(|m| m.role == LanguageModelRole::System)
        .map(|m| m.content.clone())
        .collect::<Vec<_>>()
        .join("\n\n");
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        system.push_str("\nReturn a valid JSON object as the final answer.");
    }
    if !system.is_empty() {
        payload["system"] = json!(system);
    }
    if let Some(temperature) = request.temperature {
        payload["temperature"] = json!(temperature);
    }
    if !matches!(request.tool_choice, Some(LanguageModelToolChoice::None))
        && !request.tools.is_empty()
    {
        payload["tools"] = json!(request.tools.iter().map(|tool| json!({"name":tool.name,"description":tool.description,
            "input_schema":if tool.input_schema.is_null(){json!({"type":"object","properties":{}})}else{tool.input_schema.clone()}})).collect::<Vec<_>>());
    }
    if matches!(
        request.tool_choice,
        Some(LanguageModelToolChoice::Required | LanguageModelToolChoice::Tool { .. })
    ) {
        return Err(error(
            model,
            "DeepSeek Messages does not support forced tool choice; use auto/none or a Chat Completions endpoint",
        ));
    }
    crate::apply_provider_vendor_options_to_payload(&mut payload, options);
    if !matches!(
        payload["thinking"]["type"].as_str(),
        Some("enabled" | "disabled")
    ) {
        return Err(error(
            model,
            "DeepSeek Messages thinking must be enabled or disabled",
        ));
    }
    if payload["thinking"]["type"] == "disabled" {
        if let Some(config) = payload
            .get_mut("output_config")
            .and_then(Value::as_object_mut)
        {
            config.remove("effort");
        }
    } else if !matches!(
        payload["output_config"]["effort"].as_str(),
        Some("low" | "high" | "max")
    ) {
        return Err(error(
            model,
            "DeepSeek Messages effort must be low, high, or max",
        ));
    }
    Ok(payload)
}

fn validate_history(messages: &mut [Value], model: &str) -> Result<(), LanguageModelError> {
    let mut pending = BTreeSet::new();
    for message in messages {
        let role = message["role"].as_str().unwrap_or_default().to_string();
        let blocks = message["content"]
            .as_array_mut()
            .ok_or_else(|| error(model, "DeepSeek Messages requires content blocks"))?;
        if role == "assistant" {
            if !pending.is_empty() {
                return Err(error(model, "tool calls need immediate results"));
            }
            for block in blocks.iter_mut().filter(|b| b["type"] == "tool_use") {
                let id = block["id"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| error(model, "tool call id is missing"))?
                    .to_string();
                if !pending.insert(id) {
                    return Err(error(model, "duplicate tool call id"));
                }
                if !block["input"].is_object() {
                    block["input"] = json!({});
                }
            }
        } else if role == "user" {
            for block in blocks.iter_mut().filter(|b| b["type"] == "tool_result") {
                if !block["tool_use_id"]
                    .as_str()
                    .is_some_and(|id| pending.remove(id))
                {
                    return Err(error(model, "tool result has no matching call"));
                }
                if let Some(text) = block["content"].as_str() {
                    block["content"] = json!([{"type":"text","text":text}]);
                }
            }
            if !pending.is_empty() {
                return Err(error(model, "tool calls need immediate results"));
            }
            blocks.sort_by_key(|block| block["type"] != "tool_result");
        }
    }
    if !pending.is_empty() {
        return Err(error(model, "history ends with unresolved tools"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_endpoints_do_not_duplicate_version_paths() {
        for (base, expected) in [
            (
                "https://api.deepseek.com/anthropic",
                "https://api.deepseek.com/anthropic/v1/messages",
            ),
            (
                "https://api.deepseek.com/anthropic/v1/",
                "https://api.deepseek.com/anthropic/v1/messages",
            ),
            (
                "http://localhost:8000/v1/messages",
                "http://localhost:8000/v1/messages",
            ),
        ] {
            assert!(uses_messages(base));
            assert_eq!(endpoint(base), expected);
        }
        assert!(!uses_messages("https://api.deepseek.com/v1"));
        assert!(!uses_messages("https://api.deepseek.com/chat/completions"));
    }

    #[test]
    fn messages_history_rejects_duplicate_unresolved_and_orphan_calls() {
        for mut messages in [
            vec![json!({"role":"assistant","content":[{"type":"tool_use","id":"a","input":{}}]})],
            vec![
                json!({"role":"assistant","content":[{"type":"tool_use","id":"a","input":{}},{"type":"tool_use","id":"a","input":{}}]}),
            ],
            vec![
                json!({"role":"user","content":[{"type":"tool_result","tool_use_id":"a","content":"no call"}]}),
            ],
        ] {
            assert!(validate_history(&mut messages, "deepseek-flash").is_err());
        }
        let mut messages = vec![
            json!({"role":"assistant","content":[{"type":"tool_use","id":"a","input":null}]}),
            json!({"role":"user","content":[{"type":"text","text":"continue"},{"type":"tool_result","tool_use_id":"a","content":"done"}]}),
        ];
        validate_history(&mut messages, "deepseek-flash").unwrap();
        assert_eq!(messages[0]["content"][0]["input"], json!({}));
        assert_eq!(messages[1]["content"][0]["type"], "tool_result");
    }
}
