use crate::*;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) fn contents(request: &LanguageModelRequest, model: &str) -> Vec<Value> {
    let mut contents = Vec::new();
    let requires_signatures = model.trim_start_matches("models/").starts_with("gemini-3");
    let mut signed_calls = std::collections::BTreeSet::new();
    for item in provider_request_items(request, "gemini", model) {
        match item {
            LanguageModelRequestItem::Image {
                media_type,
                data_base64,
            } => append(
                &mut contents,
                "user",
                json!({"inlineData":{"mimeType":media_type, "data":data_base64}}),
            ),
            LanguageModelRequestItem::ProviderContinuation { continuation } => {
                signed_calls.extend(continuation.tool_call_ids);
                contents.extend(continuation.items);
            }
            LanguageModelRequestItem::Message {
                role: LanguageModelRole::System,
                ..
            } => {}
            LanguageModelRequestItem::Message { role, content } => append(
                &mut contents,
                if role == LanguageModelRole::Assistant {
                    "model"
                } else {
                    "user"
                },
                json!({"text":content}),
            ),
            LanguageModelRequestItem::Context { label, content } => append(
                &mut contents,
                "user",
                json!({"text":format!("{label}:\n{content}")}),
            ),
            LanguageModelRequestItem::ToolCall {
                call_id,
                name,
                input,
            } => {
                // A legacy or switched-model call has no valid thought signature.
                // Retain its meaning as history instead of forging a signature.
                if requires_signatures {
                    append(
                        &mut contents,
                        "model",
                        json!({"text":format!("Previous tool call {call_id}: {name}({input})")}),
                    );
                    continue;
                }
                let mut call = json!({"name":name,"args":input});
                if !call_id.starts_with("sinomo-gemini-") {
                    call["id"] = json!(call_id);
                }
                append(&mut contents, "model", json!({"functionCall":call}));
            }
            LanguageModelRequestItem::ToolCallOutput {
                call_id,
                name,
                output,
            } => {
                if requires_signatures && !signed_calls.contains(&call_id) {
                    append(
                        &mut contents,
                        "user",
                        json!({"text":format!("Previous tool result {call_id} ({name}): {output}")}),
                    );
                    continue;
                }
                let mut call = json!({"name":name,"response":if output.is_object(){output}else{json!({"result":output})}});
                if !call_id.starts_with("sinomo-gemini-") {
                    call["id"] = json!(call_id);
                }
                append(&mut contents, "user", json!({"functionResponse":call}));
            }
        }
    }
    contents
}
fn append(contents: &mut Vec<Value>, role: &str, part: Value) {
    if let Some(last) = contents.last_mut()
        && last["role"] == role
        && let Some(parts) = last["parts"].as_array_mut()
    {
        parts.push(part);
    } else {
        contents.push(json!({"role":role,"parts":[part]}));
    }
}

pub(super) fn options(
    payload: &mut Value,
    request: &LanguageModelRequest,
    model: &str,
    options: &LanguageModelProviderOptions,
) -> Result<(), LanguageModelError> {
    if !request.tools.is_empty() {
        payload["tools"] = json!([{"functionDeclarations":request.tools.iter().map(|tool|json!({"name":tool.name,"description":tool.description,
            "parametersJsonSchema":if tool.input_schema.is_null(){json!({"type":"object","properties":{}})}else{tool.input_schema.clone()}})).collect::<Vec<_>>()}]);
    }
    if let Some(choice) = &request.tool_choice {
        let config = match choice {
            LanguageModelToolChoice::Auto => json!({"mode":"AUTO"}),
            LanguageModelToolChoice::None => json!({"mode":"NONE"}),
            LanguageModelToolChoice::Required => json!({"mode":"ANY"}),
            LanguageModelToolChoice::Tool { name } => {
                json!({"mode":"ANY","allowedFunctionNames":[name]})
            }
        };
        payload["toolConfig"]["functionCallingConfig"] = config;
    }
    let model_id = model.trim_start_matches("models/");
    let gemini3 = model_id.starts_with("gemini-3");
    if let Some(budget) = options.reasoning_budget_tokens {
        if model_id.starts_with("gemini-2.5-pro") && budget == 0 {
            return Err(protocol_error(
                model,
                "Gemini 2.5 Pro thinking cannot be disabled",
            ));
        }
        payload["generationConfig"]["thinkingConfig"]["thinkingBudget"] = json!(budget);
    } else if let Some(effort) = options.reasoning_effort {
        use ProviderReasoningEffort::*;
        if gemini3 {
            if effort == None {
                return Err(protocol_error(
                    model,
                    "Gemini 3 thinking cannot be disabled; select low or clear the override",
                ));
            }
            let minimal = model_id.starts_with("gemini-3.5")
                || model_id.starts_with("gemini-3.6")
                || model_id.starts_with("gemini-3-flash")
                || model_id.starts_with("gemini-3.1-flash-lite");
            let level = match effort {
                Minimal if minimal => "minimal",
                Minimal | Low if model_id.starts_with("gemini-3.1-flash-lite-image") => "minimal",
                Minimal | Low => "low",
                Medium
                    if model_id.starts_with("gemini-3-pro")
                        || model_id.starts_with("gemini-3.1-flash-lite-image") =>
                {
                    "high"
                }
                Medium => "medium",
                _ => "high",
            };
            payload["generationConfig"]["thinkingConfig"]["thinkingLevel"] = json!(level);
        } else {
            if effort == None && model_id.starts_with("gemini-2.5-pro") {
                return Err(protocol_error(
                    model,
                    "Gemini 2.5 Pro thinking cannot be disabled",
                ));
            }
            payload["generationConfig"]["thinkingConfig"]["thinkingBudget"] = json!(match effort {
                None => 0,
                Minimal | Low => 1024,
                Medium => 8192,
                _ => 24576,
            });
        }
    }
    apply_provider_vendor_options_to_payload(payload, options);
    let thinking = payload.pointer("/generationConfig/thinkingConfig");
    let level = thinking
        .and_then(|config| config.get("thinkingLevel"))
        .and_then(Value::as_str);
    let budget = thinking
        .and_then(|config| config.get("thinkingBudget"))
        .and_then(Value::as_i64);
    if level.is_some() && budget.is_some() {
        return Err(protocol_error(
            model,
            "choose thinkingLevel or thinkingBudget, not both",
        ));
    }
    if budget == Some(0) && (gemini3 || model_id.starts_with("gemini-2.5-pro")) {
        return Err(protocol_error(
            model,
            "thinking cannot be disabled for this Gemini model",
        ));
    }
    if let Some(level) = level {
        let allowed: &[&str] = if model_id.starts_with("gemini-3-pro") {
            &["low", "high"]
        } else if model_id.starts_with("gemini-3.1-flash-lite-image") {
            &["minimal", "high"]
        } else if model_id.starts_with("gemini-3.5")
            || model_id.starts_with("gemini-3.6")
            || model_id.starts_with("gemini-3-flash")
            || model_id.starts_with("gemini-3.1-flash-lite")
        {
            &["minimal", "low", "medium", "high"]
        } else if gemini3 {
            &["low", "medium", "high"]
        } else {
            &[]
        };
        if !allowed.contains(&level) {
            return Err(protocol_error(
                model,
                format!("thinking level {level} is not supported by {model}"),
            ));
        }
    }
    Ok(())
}

fn protocol_error(model: &str, detail: impl ToString) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: "gemini".into(),
        model: model.into(),
        detail: detail.to_string(),
    }
}

pub(super) fn validate_response(value: &Value, model: &str) -> Result<(), LanguageModelError> {
    if let Some(error) = value.get("error") {
        return Err(protocol_error(model, error));
    }
    if let Some(reason) = value
        .pointer("/promptFeedback/blockReason")
        .and_then(Value::as_str)
        && reason != "BLOCK_REASON_UNSPECIFIED"
    {
        return Err(protocol_error(
            model,
            format!("Gemini prompt blocked: {reason}"),
        ));
    }
    if let Some(reason) = value
        .pointer("/candidates/0/finishReason")
        .and_then(Value::as_str)
        && !matches!(reason, "STOP" | "MAX_TOKENS")
    {
        return Err(protocol_error(
            model,
            format!("Gemini generation failed: {reason}"),
        ));
    }
    Ok(())
}

pub(super) fn response(value: &Value, model: &str) -> LanguageModelResponse {
    let content = value
        .pointer("/candidates/0/content")
        .cloned()
        .unwrap_or_default();
    let mut calls = Vec::new();
    for part in content["parts"].as_array().into_iter().flatten() {
        if let Some(call) = part.get("functionCall")
            && let Some(name) = call["name"].as_str()
        {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let id = call["id"].as_str().map(str::to_string).unwrap_or_else(|| {
                format!(
                    "sinomo-gemini-{}-{}",
                    crate::unix_timestamp(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                )
            });
            calls.push(LanguageModelToolCall {
                call_id: Some(id),
                name: name.into(),
                input: call["args"].clone(),
                summary: None,
            });
        }
    }
    let mut items: Vec<_> = calls
        .iter()
        .cloned()
        .map(|call| LanguageModelResponseItem::ToolCall { call })
        .collect();
    if content.is_object() {
        items.push(response_continuation(
            "gemini",
            model,
            vec![content],
            &calls,
        ));
    }
    LanguageModelResponse {
        provider: "gemini".into(),
        model: model.into(),
        content: super::extract_gemini_response_text(value),
        items,
        tool_calls: calls,
        usage: super::extract_gemini_usage(value),
    }
}

pub(super) fn stream<R: std::io::Read>(
    reader: R,
    model: &str,
    emit: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError> {
    let mut parts = Vec::new();
    let mut usage = None;
    let mut finish = None;
    read_server_sent_events(reader, "gemini", model, |event| {
        let value: Value = serde_json::from_str(&event.data).map_err(|error| {
            LanguageModelError::ProviderInvocationFailed {
                provider: "gemini".into(),
                model: model.into(),
                detail: error.to_string(),
            }
        })?;
        validate_response(&value, model)?;
        if let Some(error) = value.pointer("/error/message").and_then(Value::as_str) {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: "gemini".into(),
                model: model.into(),
                detail: error.into(),
            });
        }
        for part in value
            .pointer("/candidates/0/content/parts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(text) = part["text"].as_str() {
                emit(if part["thought"] == true {
                    LanguageModelStreamEvent::ReasoningDelta { delta: text.into() }
                } else {
                    LanguageModelStreamEvent::TextDelta { delta: text.into() }
                });
            }
            parts.push(part.clone());
        }
        if let Some(u) = value.get("usageMetadata") {
            usage = Some(u.clone());
        }
        if let Some(reason) = value
            .pointer("/candidates/0/finishReason")
            .and_then(Value::as_str)
        {
            finish = Some(reason.to_string());
        }
        Ok(())
    })?;
    if finish.is_none() {
        return Err(crate::incomplete_provider_stream("gemini", model, emit));
    }
    let mut value = json!({"candidates":[{"content":{"role":"model","parts":parts}}]});
    if let Some(usage) = usage {
        value["usageMetadata"] = usage;
    }
    let response = response(&value, model);
    for call in &response.tool_calls {
        emit(LanguageModelStreamEvent::ToolCallStarted {
            call_id: call.call_id.clone(),
            name: call.name.clone(),
        });
        emit(LanguageModelStreamEvent::ToolCallCompleted {
            call_id: call.call_id.clone(),
            name: call.name.clone(),
            input: call.input.clone(),
        });
    }
    if let Some(usage) = &response.usage {
        emit(LanguageModelStreamEvent::Usage {
            usage: usage.clone(),
        });
    }
    emit(LanguageModelStreamEvent::Finished {
        finish_reason: finish,
    });
    Ok(response)
}
