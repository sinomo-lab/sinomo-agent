use crate::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(super) fn messages(request: &LanguageModelRequest, model: &str) -> Vec<Value> {
    let items = provider_request_items(request, &request.provider, model);
    let mut messages = Vec::new();
    for item in items {
        match item {
            LanguageModelRequestItem::ProviderContinuation { continuation } => {
                messages.extend(continuation.items)
            }
            LanguageModelRequestItem::Message {
                role: LanguageModelRole::System,
                ..
            } => {}
            LanguageModelRequestItem::Message { role, content } => append(
                &mut messages,
                if role == LanguageModelRole::Assistant {
                    "assistant"
                } else {
                    "user"
                },
                json!({"type":"text","text":content}),
            ),
            LanguageModelRequestItem::Context { label, content } => append(
                &mut messages,
                "user",
                json!({"type":"text","text":format!("{label}:\n{content}")}),
            ),
            LanguageModelRequestItem::ToolCall {
                call_id,
                name,
                input,
            } => append(
                &mut messages,
                "assistant",
                json!({"type":"tool_use","id":call_id,"name":name,"input":input}),
            ),
            LanguageModelRequestItem::ToolCallOutput {
                call_id, output, ..
            } => append(
                &mut messages,
                "user",
                json!({"type":"tool_result","tool_use_id":call_id,"content":output.as_str().map(str::to_string).unwrap_or_else(|| output.to_string())}),
            ),
        }
    }
    messages
}

fn append(messages: &mut Vec<Value>, role: &str, block: Value) {
    if let Some(last) = messages.last_mut()
        && last["role"] == role
        && let Some(content) = last["content"].as_array_mut()
    {
        content.push(block);
    } else {
        messages.push(json!({"role":role,"content":[block]}));
    }
}

pub(super) fn add_tools(payload: &mut Value, request: &LanguageModelRequest) {
    if !request.tools.is_empty() {
        payload["tools"]=json!(request.tools.iter().map(|tool| json!({"name":tool.name,"description":tool.description,"input_schema":if tool.input_schema.is_null(){json!({"type":"object","properties":{}})}else{tool.input_schema.clone()}})).collect::<Vec<_>>());
        let mut choice = match request
            .tool_choice
            .as_ref()
            .unwrap_or(&LanguageModelToolChoice::Auto)
        {
            LanguageModelToolChoice::Auto => json!({"type":"auto"}),
            LanguageModelToolChoice::None => json!({"type":"none"}),
            LanguageModelToolChoice::Required => json!({"type":"any"}),
            LanguageModelToolChoice::Tool { name } => json!({"type":"tool","name":name}),
        };
        if let Some(parallel) = request.parallel_tool_calls
            && choice["type"] != "none"
        {
            choice["disable_parallel_tool_use"] = json!(!parallel);
        }
        payload["tool_choice"] = choice;
    }
}

pub(crate) fn response(value: &Value, provider: &str, model: &str) -> LanguageModelResponse {
    let blocks = value
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let calls: Vec<_> = blocks
        .iter()
        .filter(|b| b["type"] == "tool_use")
        .filter_map(|b| {
            Some(LanguageModelToolCall {
                call_id: Some(b["id"].as_str()?.into()),
                name: b["name"].as_str()?.into(),
                input: b["input"].clone(),
                summary: None,
            })
        })
        .collect();
    let mut items: Vec<_> = calls
        .iter()
        .cloned()
        .map(|call| LanguageModelResponseItem::ToolCall { call })
        .collect();
    if !blocks.is_empty() {
        items.push(response_continuation(
            provider,
            model,
            vec![json!({"role":"assistant","content":blocks})],
            &calls,
        ));
    }
    LanguageModelResponse {
        provider: provider.into(),
        model: model.into(),
        content: super::extract_anthropic_text(value),
        tool_calls: calls,
        items,
        usage: value.get("usage").map(|u| LanguageModelUsage {
            input_tokens: u["input_tokens"].as_u64(),
            output_tokens: u["output_tokens"].as_u64(),
        }),
    }
}

#[derive(Default)]
pub(super) struct StreamBlocks {
    blocks: BTreeMap<usize, Value>,
    arguments: BTreeMap<usize, String>,
}
impl StreamBlocks {
    pub(super) fn event(
        &mut self,
        value: &Value,
        provider: &str,
        model: &str,
        emit: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<(), LanguageModelError> {
        let index = value["index"].as_u64().unwrap_or_default() as usize;
        match value["type"].as_str().unwrap_or_default() {
            "content_block_start" => {
                let block = value["content_block"].clone();
                if block["type"] == "tool_use" {
                    emit(LanguageModelStreamEvent::ToolCallStarted {
                        call_id: block["id"].as_str().map(str::to_string),
                        name: block["name"].as_str().unwrap_or_default().into(),
                    });
                }
                self.blocks.insert(index, block);
            }
            "content_block_delta" => {
                let delta = &value["delta"];
                if delta["type"] == "input_json_delta" {
                    let text = delta["partial_json"].as_str().unwrap_or_default();
                    self.arguments.entry(index).or_default().push_str(text);
                    let block = self.blocks.get(&index);
                    emit(LanguageModelStreamEvent::ToolCallDelta {
                        call_id: block.and_then(|b| b["id"].as_str()).map(str::to_string),
                        name: block.and_then(|b| b["name"].as_str()).map(str::to_string),
                        arguments_delta: text.into(),
                    });
                } else if let Some(block) = self.blocks.get_mut(&index) {
                    for key in ["text", "thinking", "signature"] {
                        if let Some(text) = delta[key].as_str() {
                            let mut joined = block[key].as_str().unwrap_or_default().to_string();
                            joined.push_str(text);
                            block[key] = json!(joined);
                        }
                    }
                }
            }
            "content_block_stop" => {
                if let Some(block) = self.blocks.get_mut(&index)
                    && block["type"] == "tool_use"
                {
                    if let Some(text) = self.arguments.remove(&index).filter(|s| !s.is_empty()) {
                        block["input"] = serde_json::from_str(&text).map_err(|e| {
                            LanguageModelError::ProviderInvocationFailed {
                                provider: provider.into(),
                                model: model.into(),
                                detail: format!("invalid tool arguments: {e}"),
                            }
                        })?;
                    }
                    emit(LanguageModelStreamEvent::ToolCallCompleted {
                        call_id: block["id"].as_str().map(str::to_string),
                        name: block["name"].as_str().unwrap_or_default().into(),
                        input: block["input"].clone(),
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(super) fn response(self, provider: &str, model: &str) -> LanguageModelResponse {
        response(
            &json!({"content":self.blocks.into_values().collect::<Vec<_>>()}),
            provider,
            model,
        )
    }
}
