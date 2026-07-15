use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelRequest, LanguageModelRequestItem,
    LanguageModelResponse, LanguageModelResponseFormat, LanguageModelResponseItem,
    LanguageModelRole, LanguageModelStreamEvent, LanguageModelToolCall, LanguageModelToolChoice,
    LanguageModelToolSpec, LanguageModelUsage, PROVIDER_DEFAULT_MODEL, render_plain_prompt,
    selected_model, stream_openai_chat_completion_response,
};
use reqwest::blocking::Client;
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT,
};
use std::path::PathBuf;
use std::time::Duration;

pub const DEEPSEEK_V4_FLASH_MODEL: &str = "deepseek-v4-flash";
pub const DEEPSEEK_V4_PRO_MODEL: &str = "deepseek-v4-pro";
pub const DEEPSEEK_DEFAULT_MODEL: &str = DEEPSEEK_V4_FLASH_MODEL;

const DEEPSEEK_MODEL_IDS: &[&str] = &[DEEPSEEK_V4_FLASH_MODEL, DEEPSEEK_V4_PRO_MODEL];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepSeekConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub api_key: Option<String>,
    pub credentials_path: Option<PathBuf>,
}

impl Default for DeepSeekConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://api.deepseek.com".to_string(),
            default_model: DEEPSEEK_DEFAULT_MODEL.to_string(),
            api_key: None,
            credentials_path: default_api_key_credentials_path(
                "SINOMO_DEEPSEEK_CREDENTIALS_FILE",
                DeepSeekProvider::PROVIDER_ID,
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeepSeekProvider {
    config: DeepSeekConfig,
    http: Client,
}

impl DeepSeekProvider {
    pub const PROVIDER_ID: &'static str = "deepseek";

    pub fn known_model_ids() -> Vec<String> {
        DEEPSEEK_MODEL_IDS
            .iter()
            .map(|model| model.to_string())
            .collect()
    }

    pub fn new(config: DeepSeekConfig) -> Self {
        Self {
            config,
            http: deepseek_http_client(),
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
            &["SINOMO_DEEPSEEK_API_KEY", "DEEPSEEK_API_KEY"],
            "no DeepSeek API key found; set DEEPSEEK_API_KEY or run `sinomo providers auth deepseek`",
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

    pub fn list_models(&self) -> Result<Vec<String>, LanguageModelError> {
        self.api_key()?;
        Ok(Self::known_model_ids())
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
            max_output_tokens: Some(8),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        };
        self.complete(&request).map(|_| ())
    }
}

impl Default for DeepSeekProvider {
    fn default() -> Self {
        Self::new(DeepSeekConfig::default())
    }
}

fn deepseek_http_client() -> Client {
    Client::builder()
        .http1_only()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(600))
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .build()
        .unwrap_or_else(|_| Client::new())
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::parse_declared_context_window_tokens(model).or_else(|| {
        let model = model.to_ascii_lowercase();
        if DEEPSEEK_MODEL_IDS.contains(&model.as_str())
            || model == "deepseek-chat"
            || model == "deepseek-reasoner"
        {
            Some(1_000_000)
        } else {
            None
        }
    })
}

pub(crate) fn prefers_native_tool_protocol(model: &str) -> bool {
    model.to_ascii_lowercase().contains("deepseek")
}

impl LanguageModelProvider for DeepSeekProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: crate::LanguageModelHostAccess::NetworkOnly,
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let api_key = self.api_key()?;
        let url = deepseek_chat_completions_url(&self.config.api_base_url);
        let response = self
            .http
            .post(url)
            .headers(deepseek_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&deepseek_chat_completion_payload(request, &model))
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            })?;

        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("unknown")
            .to_string();
        let body =
            response
                .text()
                .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                    provider: Self::PROVIDER_ID.to_string(),
                    model: model.clone(),
                    detail: format!(
                        "failed to read DeepSeek response body: {source}; debug: {source:?}"
                    ),
                })?;

        if !status.is_success() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail: format!("DeepSeek Chat Completions API failed: HTTP {status}: {body}"),
            });
        }

        let value = serde_json::from_str::<serde_json::Value>(&body).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: format!(
                    "failed to parse DeepSeek Chat Completions API response as JSON \
                     (HTTP {status}, content-type {content_type}): {source}; body prefix: {}",
                    body.chars().take(2048).collect::<String>()
                ),
            }
        })?;
        let content = extract_deepseek_response_text(&value);
        let tool_calls = extract_deepseek_tool_calls(&value);
        let items = tool_calls
            .iter()
            .cloned()
            .map(|call| LanguageModelResponseItem::ToolCall { call })
            .collect();
        let usage = extract_deepseek_usage(&value);

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content,
            items,
            tool_calls,
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
        let url = deepseek_chat_completions_url(&self.config.api_base_url);
        let mut payload = deepseek_chat_completion_payload(request, &model);
        payload["stream"] = serde_json::json!(true);
        let mut headers = deepseek_headers(&api_key, Self::PROVIDER_ID, &model)?;
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
                detail: format!("DeepSeek Chat Completions API failed: HTTP {status}: {body}"),
            });
        }

        on_event(LanguageModelStreamEvent::Started {
            provider: Self::PROVIDER_ID.to_string(),
            model: model.clone(),
        });
        stream_openai_chat_completion_response(response, Self::PROVIDER_ID, &model, on_event)
    }
}

fn deepseek_chat_completions_url(api_base_url: &str) -> String {
    let base = api_base_url.trim().trim_end_matches('/');
    if base.ends_with("/chat/completions") {
        base.to_string()
    } else {
        format!("{base}/chat/completions")
    }
}

fn deepseek_headers(
    api_key: &str,
    provider: &str,
    model: &str,
) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

fn deepseek_chat_completion_payload(
    request: &LanguageModelRequest,
    model: &str,
) -> serde_json::Value {
    let mut messages = deepseek_chat_messages(request);
    if messages.is_empty() {
        messages.push(serde_json::json!({
            "role": "user",
            "content": render_plain_prompt(request),
        }));
    }

    let mut payload = serde_json::json!({
        "model": model,
        "messages": messages,
        "stream": false,
    });
    if let Some(temperature) = request.temperature {
        payload["temperature"] = serde_json::json!(temperature);
    }
    if let Some(max_tokens) = request.max_output_tokens {
        payload["max_tokens"] = serde_json::json!(max_tokens);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["response_format"] = serde_json::json!({ "type": "json_object" });
    }
    if !request.tools.is_empty() {
        payload["tools"] = serde_json::json!(
            request
                .tools
                .iter()
                .map(deepseek_chat_tool_spec)
                .collect::<Vec<_>>()
        );
    }
    if let Some(tool_choice) = &request.tool_choice {
        payload["tool_choice"] = deepseek_chat_tool_choice(tool_choice);
    }
    if let Some(parallel_tool_calls) = request.parallel_tool_calls {
        payload["parallel_tool_calls"] = serde_json::json!(parallel_tool_calls);
    }
    if deepseek_request_uses_tools_or_tool_transcript(request) {
        payload["thinking"] = serde_json::json!({ "type": "disabled" });
    }
    payload
}

fn deepseek_request_uses_tools_or_tool_transcript(request: &LanguageModelRequest) -> bool {
    !request.tools.is_empty()
        || request.items.iter().any(|item| {
            matches!(
                item,
                LanguageModelRequestItem::ToolCall { .. }
                    | LanguageModelRequestItem::ToolCallOutput { .. }
            )
        })
}

fn deepseek_chat_messages(request: &LanguageModelRequest) -> Vec<serde_json::Value> {
    let item_messages = deepseek_chat_messages_from_items(&request.items);
    if !item_messages.is_empty() {
        let mut messages = request
            .messages
            .iter()
            .filter(|message| message.role == LanguageModelRole::System)
            .map(deepseek_chat_message)
            .collect::<Vec<_>>();
        messages.extend(item_messages);
        return messages;
    }
    request.messages.iter().map(deepseek_chat_message).collect()
}

fn deepseek_chat_message(message: &LanguageModelMessage) -> serde_json::Value {
    serde_json::json!({
        "role": deepseek_chat_role(&message.role),
        "content": message.content,
    })
}

fn deepseek_chat_messages_from_items(items: &[LanguageModelRequestItem]) -> Vec<serde_json::Value> {
    let mut messages = Vec::new();
    let mut pending_assistant_content: Option<String> = None;
    let mut pending_assistant_tool_calls = Vec::new();

    for item in items {
        match item {
            LanguageModelRequestItem::Message {
                role: LanguageModelRole::Assistant,
                content,
            } => {
                flush_deepseek_pending_assistant(
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
                        "arguments": deepseek_tool_argument_text(input),
                    },
                }));
            }
            LanguageModelRequestItem::Message { role, content } => {
                flush_deepseek_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                if !content.trim().is_empty() {
                    messages.push(serde_json::json!({
                        "role": deepseek_chat_role(role),
                        "content": content,
                    }));
                }
            }
            LanguageModelRequestItem::ToolCallOutput {
                call_id, output, ..
            } => {
                flush_deepseek_pending_assistant(
                    &mut messages,
                    &mut pending_assistant_content,
                    &mut pending_assistant_tool_calls,
                );
                messages.push(serde_json::json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": deepseek_tool_output_text(output),
                }));
            }
            LanguageModelRequestItem::Context { label, content } => {
                flush_deepseek_pending_assistant(
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

    flush_deepseek_pending_assistant(
        &mut messages,
        &mut pending_assistant_content,
        &mut pending_assistant_tool_calls,
    );
    messages
}

fn flush_deepseek_pending_assistant(
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

fn deepseek_chat_role(role: &LanguageModelRole) -> &'static str {
    match role {
        LanguageModelRole::System => "system",
        LanguageModelRole::User => "user",
        LanguageModelRole::Assistant => "assistant",
    }
}

fn deepseek_tool_argument_text(input: &serde_json::Value) -> String {
    match input {
        serde_json::Value::String(value) => value.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn deepseek_tool_output_text(output: &serde_json::Value) -> String {
    match output {
        serde_json::Value::String(value) => value.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn deepseek_chat_tool_spec(tool: &LanguageModelToolSpec) -> serde_json::Value {
    serde_json::json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": normalized_tool_input_schema(tool),
        }
    })
}

fn deepseek_chat_tool_choice(tool_choice: &LanguageModelToolChoice) -> serde_json::Value {
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

fn extract_deepseek_response_text(value: &serde_json::Value) -> String {
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

fn extract_deepseek_usage(value: &serde_json::Value) -> Option<LanguageModelUsage> {
    value.get("usage").map(|usage| LanguageModelUsage {
        input_tokens: usage
            .get("prompt_tokens")
            .or_else(|| usage.get("input_tokens"))
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("completion_tokens")
            .or_else(|| usage.get("output_tokens"))
            .and_then(serde_json::Value::as_u64),
    })
}

fn extract_deepseek_tool_calls(value: &serde_json::Value) -> Vec<LanguageModelToolCall> {
    value
        .pointer("/choices/0/message/tool_calls")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(deepseek_tool_call)
        .collect()
}

fn deepseek_tool_call(value: &serde_json::Value) -> Option<LanguageModelToolCall> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: DeepSeekProvider::PROVIDER_ID.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
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
    fn deepseek_payload_uses_openai_chat_completions_shape() {
        let payload = deepseek_chat_completion_payload(&request(), DEEPSEEK_V4_PRO_MODEL);

        assert_eq!(payload["model"], DEEPSEEK_V4_PRO_MODEL);
        assert_eq!(payload["stream"], false);
        assert_eq!(payload["max_tokens"], 200);
        assert_eq!(payload["response_format"]["type"], "json_object");
        assert_eq!(payload["messages"][0]["role"], "system");
        assert_eq!(payload["messages"][1]["content"], "Summarize evt_1.");
    }

    #[test]
    fn deepseek_payload_renders_native_tool_specs() {
        let mut request = request();
        request.tools = vec![LanguageModelToolSpec {
            name: "read_file".to_string(),
            description: "Read a file.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"],
            }),
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: Vec::new(),
        }];
        request.tool_choice = Some(LanguageModelToolChoice::Tool {
            name: "read_file".to_string(),
        });
        request.parallel_tool_calls = Some(false);

        let payload = deepseek_chat_completion_payload(&request, DEEPSEEK_V4_PRO_MODEL);

        assert_eq!(payload["tools"][0]["type"], "function");
        assert_eq!(payload["tools"][0]["function"]["name"], "read_file");
        assert_eq!(
            payload["tools"][0]["function"]["parameters"]["properties"]["path"]["type"],
            "string"
        );
        assert_eq!(payload["tool_choice"]["function"]["name"], "read_file");
        assert_eq!(payload["parallel_tool_calls"], false);
        assert_eq!(payload["thinking"]["type"], "disabled");
    }

    #[test]
    fn deepseek_payload_prefers_native_tool_transcript_items() {
        let mut request = request();
        request.items = vec![
            LanguageModelRequestItem::message(LanguageModelRole::User, "Inspect Cargo.toml."),
            LanguageModelRequestItem::message(LanguageModelRole::Assistant, "I'll read it."),
            LanguageModelRequestItem::tool_call(
                "call_read",
                "read_file",
                serde_json::json!({ "path": "Cargo.toml" }),
            ),
            LanguageModelRequestItem::tool_call_output(
                "call_read",
                "read_file",
                serde_json::json!({ "ok": true, "content": "[package]" }),
            ),
        ];
        request.tools = vec![LanguageModelToolSpec {
            name: "read_file".to_string(),
            description: "Read a file.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"],
            }),
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: Vec::new(),
        }];

        let payload = deepseek_chat_completion_payload(&request, DEEPSEEK_V4_PRO_MODEL);
        let messages = payload["messages"].as_array().expect("messages array");

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "Return JSON only.");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"], "Inspect Cargo.toml.");
        assert_eq!(messages[2]["role"], "assistant");
        assert_eq!(messages[2]["content"], "I'll read it.");
        assert_eq!(messages[2]["tool_calls"][0]["id"], "call_read");
        assert_eq!(messages[2]["tool_calls"][0]["type"], "function");
        assert_eq!(
            messages[2]["tool_calls"][0]["function"]["name"],
            "read_file"
        );
        let arguments = serde_json::from_str::<serde_json::Value>(
            messages[2]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .expect("arguments string"),
        )
        .expect("json arguments");
        assert_eq!(arguments["path"], "Cargo.toml");
        assert_eq!(messages[3]["role"], "tool");
        assert_eq!(messages[3]["tool_call_id"], "call_read");
        assert_eq!(payload["thinking"]["type"], "disabled");
        let output = serde_json::from_str::<serde_json::Value>(
            messages[3]["content"].as_str().expect("tool output string"),
        )
        .expect("json tool output");
        assert_eq!(output["ok"], true);
        assert_eq!(output["content"], "[package]");
    }

    #[test]
    fn deepseek_payload_does_not_disable_thinking_without_tools() {
        let payload = deepseek_chat_completion_payload(&request(), DEEPSEEK_V4_PRO_MODEL);

        assert!(payload.get("thinking").is_none());
    }

    #[test]
    fn deepseek_response_text_reads_chat_message_and_usage() {
        let value = serde_json::json!({
            "choices": [{ "message": { "content": "hello" } }],
            "usage": { "prompt_tokens": 11, "completion_tokens": 7 }
        });

        assert_eq!(extract_deepseek_response_text(&value), "hello");
        assert_eq!(
            extract_deepseek_usage(&value),
            Some(LanguageModelUsage {
                input_tokens: Some(11),
                output_tokens: Some(7),
            })
        );
    }

    #[test]
    fn deepseek_response_parser_extracts_native_tool_calls() {
        let value = serde_json::json!({
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "read_file",
                            "arguments": "{\"path\":\"Cargo.toml\"}"
                        }
                    }]
                }
            }]
        });

        let calls = extract_deepseek_tool_calls(&value);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call_id.as_deref(), Some("call_1"));
        assert_eq!(calls[0].name, "read_file");
        assert_eq!(calls[0].input["path"], "Cargo.toml");
    }

    #[test]
    fn provider_reports_http_capabilities() {
        let info = DeepSeekProvider::default().info();

        assert_eq!(info.provider, DeepSeekProvider::PROVIDER_ID);
        assert_eq!(info.default_model, DEEPSEEK_DEFAULT_MODEL);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
        assert!(!info.supports_tool_permissions);
    }

    #[test]
    fn deepseek_chat_url_accepts_base_or_full_endpoint() {
        assert_eq!(
            deepseek_chat_completions_url("https://api.deepseek.com"),
            "https://api.deepseek.com/chat/completions"
        );
        assert_eq!(
            deepseek_chat_completions_url("https://api.deepseek.com/chat/completions"),
            "https://api.deepseek.com/chat/completions"
        );
    }

    #[test]
    fn deepseek_v4_models_infer_one_million_token_context() {
        assert_eq!(
            inferred_context_window_tokens(DEEPSEEK_V4_FLASH_MODEL),
            Some(1_000_000)
        );
        assert_eq!(
            inferred_context_window_tokens(DEEPSEEK_V4_PRO_MODEL),
            Some(1_000_000)
        );
    }

    #[test]
    fn native_tool_protocol_preference_uses_deepseek_model_family() {
        assert!(prefers_native_tool_protocol("deepseek-chat"));
        assert!(prefers_native_tool_protocol(DEEPSEEK_V4_PRO_MODEL));
        assert!(!prefers_native_tool_protocol("gpt-5.2-codex"));
    }
}
