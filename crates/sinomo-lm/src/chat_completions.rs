//! Shared OpenAI-compatible Chat Completions wire format.
use crate::{
    LanguageModelMessage, LanguageModelRequest, LanguageModelRequestItem, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelResponseItem, LanguageModelRole,
    LanguageModelToolCall, LanguageModelToolChoice, LanguageModelToolSpec, LanguageModelUsage,
};

pub(crate) fn payload(
    request: &LanguageModelRequest,
    model: &str,
    provider: &str,
) -> serde_json::Value {
    let mut messages = chat_messages(request, provider, model);
    if messages.is_empty() {
        messages.push(
            serde_json::json!({"role":"user", "content":crate::render_plain_prompt(request)}),
        );
    }
    let mut payload = serde_json::json!({"model":model, "messages":messages, "stream":false});
    if let Some(value) = request.temperature {
        payload["temperature"] = serde_json::json!(value);
    }
    if let Some(value) = request.max_output_tokens {
        payload["max_tokens"] = serde_json::json!(value);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["response_format"] = serde_json::json!({"type":"json_object"});
    }
    if !request.tools.is_empty() {
        payload["tools"] =
            serde_json::json!(request.tools.iter().map(chat_tool_spec).collect::<Vec<_>>());
    }
    if let Some(choice) = &request.tool_choice {
        payload["tool_choice"] = chat_tool_choice(choice);
    }
    if let Some(parallel) = request.parallel_tool_calls {
        payload["parallel_tool_calls"] = serde_json::json!(parallel);
    }
    payload
}

pub(crate) fn response(
    value: &serde_json::Value,
    provider: &str,
    model: &str,
) -> LanguageModelResponse {
    let calls = extract_tool_calls(value);
    let mut items: Vec<_> = calls
        .iter()
        .cloned()
        .map(|call| LanguageModelResponseItem::ToolCall { call })
        .collect();
    if let Some(message) = value
        .pointer("/choices/0/message")
        .filter(|message| message.is_object())
    {
        let mut replay = message.clone();
        // Provider output is always assistant history, never a new instruction role.
        replay["role"] = serde_json::json!("assistant");
        items.push(crate::response_continuation(
            provider,
            model,
            vec![replay],
            &calls,
        ));
    }
    LanguageModelResponse {
        provider: provider.into(),
        model: model.into(),
        content: extract_text(value),
        items,
        tool_calls: calls,
        usage: extract_usage(value),
    }
}
pub(crate) fn chat_messages(
    request: &LanguageModelRequest,
    provider: &str,
    model: &str,
) -> Vec<serde_json::Value> {
    let item_messages =
        chat_messages_from_items(&crate::provider_request_items(request, provider, model));
    if !item_messages.is_empty() {
        let mut messages = request
            .messages
            .iter()
            .filter(|message| message.role == LanguageModelRole::System)
            .map(chat_message)
            .collect::<Vec<_>>();
        messages.extend(item_messages);
        return messages;
    }
    request.messages.iter().map(chat_message).collect()
}

fn chat_message(message: &LanguageModelMessage) -> serde_json::Value {
    serde_json::json!({
        "role": chat_role(&message.role),
        "content": message.content,
    })
}

fn chat_messages_from_items(items: &[LanguageModelRequestItem]) -> Vec<serde_json::Value> {
    let mut messages = Vec::new();
    let mut pending_assistant_content: Option<String> = None;
    let mut pending_assistant_tool_calls = Vec::new();

    for item in items {
        match item {
            LanguageModelRequestItem::Image {
                media_type,
                data_base64,
            } => {
                flush_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                messages.push(serde_json::json!({"role":"user", "content":[{"type":"image_url", "image_url":{"url":format!("data:{media_type};base64,{data_base64}"), "detail":"high"}}]}));
            }
            LanguageModelRequestItem::ProviderContinuation { continuation } => {
                flush_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                messages.extend(continuation.items.clone());
            }
            LanguageModelRequestItem::Message {
                role: LanguageModelRole::Assistant,
                content,
            } => {
                flush_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                if !content.trim().is_empty() {
                    pending_assistant_content = Some(content.clone());
                }
            }
            LanguageModelRequestItem::ToolCall {
                call_id,
                name,
                input,
            } => {
                pending_assistant_tool_calls.push(serde_json::json!({
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": name,
                        "arguments": tool_argument_text(input),
                    },
                }));
            }
            LanguageModelRequestItem::Message { role, content } => {
                flush_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                if !content.trim().is_empty() {
                    messages.push(serde_json::json!({
                        "role": chat_role(role),
                        "content": content,
                    }));
                }
            }
            LanguageModelRequestItem::ToolCallOutput {
                call_id, output, ..
            } => {
                flush_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                messages.push(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": tool_output_text(output),
                }));
            }
            LanguageModelRequestItem::Context { label, content } => {
                flush_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                if !content.trim().is_empty() {
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": format!("{label}:\n{content}"),
                    }));
                }
            }
        }
    }

    flush_pending_assistant(
        &mut messages,
        &mut pending_assistant_content,
        &mut pending_assistant_tool_calls,
    );
    messages
}

fn flush_pending_assistant(
    messages: &mut Vec<serde_json::Value>,
    pending_content: &mut Option<String>,
    pending_tool_calls: &mut Vec<serde_json::Value>,
) {
    if pending_content.is_none() && pending_tool_calls.is_empty() {
        return;
    }
    let mut message = serde_json::json!({
        "role": "assistant",
        "content": pending_content.take(),
    });
    if !pending_tool_calls.is_empty() {
        message["tool_calls"] = serde_json::Value::Array(std::mem::take(pending_tool_calls));
    }
    messages.push(message);
}

fn chat_role(role: &LanguageModelRole) -> &'static str {
    match role {
        LanguageModelRole::System => "system",
        LanguageModelRole::User => "user",
        LanguageModelRole::Assistant => "assistant",
    }
}

fn tool_argument_text(input: &serde_json::Value) -> String {
    match input {
        serde_json::Value::String(value) => value.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn tool_output_text(output: &serde_json::Value) -> String {
    match output {
        serde_json::Value::String(value) => value.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

pub(crate) fn chat_tool_spec(tool: &LanguageModelToolSpec) -> serde_json::Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": normalized_tool_input_schema(tool),
        }
    })
}

pub(crate) fn chat_tool_choice(tool_choice: &LanguageModelToolChoice) -> serde_json::Value {
    match tool_choice {
        LanguageModelToolChoice::Auto => serde_json::json!("auto"),
        LanguageModelToolChoice::None => serde_json::json!("none"),
        LanguageModelToolChoice::Required => serde_json::json!("required"),
        LanguageModelToolChoice::Tool { name } => {
            serde_json::json!({ "type": "function", "function": { "name": name } })
        }
    }
}

fn normalized_tool_input_schema(tool: &LanguageModelToolSpec) -> serde_json::Value {
    if tool.input_schema.is_null() {
        serde_json::json!({
            "type": "object",
            "properties": {},
        })
    } else {
        tool.input_schema.clone()
    }
}

pub(crate) fn extract_text(value: &serde_json::Value) -> String {
    value
        .pointer("/choices/0/message/content")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            value
                .pointer("/choices/0/text")
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn extract_usage(value: &serde_json::Value) -> Option<LanguageModelUsage> {
    crate::usage::openai(value)
}

pub(crate) fn extract_tool_calls(value: &serde_json::Value) -> Vec<LanguageModelToolCall> {
    value
        .pointer("/choices/0/message/tool_calls")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(tool_call)
        .collect()
}

fn tool_call(value: &serde_json::Value) -> Option<LanguageModelToolCall> {
    let function = value.get("function")?;
    Some(LanguageModelToolCall {
        call_id: value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        name: function.get("name")?.as_str()?.to_string(),
        input: parse_tool_arguments(function.get("arguments")),
        summary: None,
    })
}

fn parse_tool_arguments(arguments: Option<&serde_json::Value>) -> serde_json::Value {
    match arguments {
        Some(serde_json::Value::String(arguments)) => {
            serde_json::from_str(arguments).unwrap_or_else(|_| serde_json::json!(arguments))
        }
        Some(value) => value.clone(),
        None => serde_json::Value::Null,
    }
}
