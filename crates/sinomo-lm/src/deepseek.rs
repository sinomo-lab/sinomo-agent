mod messages;

use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
#[cfg(test)]
use crate::chat_completions::{
    extract_text as extract_deepseek_response_text,
    extract_tool_calls as extract_deepseek_tool_calls, extract_usage as extract_deepseek_usage,
};
use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelRequest, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelStreamEvent,
    PROVIDER_DEFAULT_MODEL, selected_model, stream_openai_chat_completion_response,
};
use reqwest::blocking::Client;
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT,
};
use std::path::PathBuf;
use std::time::Duration;

pub use sinomo_runtime::api::lm::DEEPSEEK_V4_FLASH_MODEL;
pub const DEEPSEEK_V4_PRO_MODEL: &str = "deepseek-v4-pro";
pub use sinomo_runtime::api::lm::DEEPSEEK_DEFAULT_MODEL;

const DEEPSEEK_MODEL_IDS: &[&str] = &[DEEPSEEK_DEFAULT_MODEL, DEEPSEEK_V4_PRO_MODEL];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeepSeekConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub api_key: Option<String>,
    pub credentials_path: Option<PathBuf>,
    pub options: crate::LanguageModelProviderOptions,
}

impl Default for DeepSeekConfig {
    fn default() -> Self {
        Self {
            options: crate::LanguageModelProviderOptions::default(),
            api_base_url: "https://api.deepseek.com/anthropic".to_string(),
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
    pub const PROVIDER_ID: &'static str = sinomo_runtime::api::lm::DEEPSEEK_PROVIDER_ID;

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
        crate::catalog::list_models(
            &self.http,
            &crate::catalog::endpoint(
                self.config
                    .api_base_url
                    .trim_end_matches('/')
                    .trim_end_matches("/chat/completions")
                    .trim_end_matches("/messages")
                    .trim_end_matches("/anthropic/v1")
                    .trim_end_matches("/anthropic"),
                "models",
            ),
            deepseek_headers(
                &self.api_key()?,
                Self::PROVIDER_ID,
                &self.config.default_model,
            )?,
            crate::catalog::CatalogProtocol::OpenAi,
            Self::PROVIDER_ID,
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
            max_output_tokens: Some(8),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::json!({"purpose":"provider_verification","loop":"provider_service"}),
        };
        crate::usage::complete_provider(None, self, &request).map(|_| ())
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
        .redirect(reqwest::redirect::Policy::none())
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
            || model.starts_with("deepseek-v4-")
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
    fn available_models(&self) -> Result<Vec<String>, LanguageModelError> {
        self.list_models()
    }
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: crate::LanguageModelHostAccess::NetworkOnly,
            capabilities: crate::LanguageModelProviderCapabilities::default(),
            supports_json_object: !messages::uses_messages(&self.config.api_base_url),
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if messages::uses_messages(&self.config.api_base_url) {
            return self.complete_messages(request, &mut |_| {});
        }
        let model = selected_model(&self.config.default_model, request);
        let api_key = self.api_key()?;
        let url = deepseek_chat_completions_url(&self.config.api_base_url);
        let response = self
            .http
            .post(url)
            .headers(deepseek_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&deepseek_payload_with_options(
                request,
                &model,
                &self.config.options,
            )?)
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
        Ok(crate::chat_completions::response(
            &value,
            Self::PROVIDER_ID,
            &model,
        ))
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if messages::uses_messages(&self.config.api_base_url) {
            return self.complete_messages(request, on_event);
        }
        let model = selected_model(&self.config.default_model, request);
        let api_key = self.api_key()?;
        let url = deepseek_chat_completions_url(&self.config.api_base_url);
        let mut payload = deepseek_payload_with_options(request, &model, &self.config.options)?;
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
            crate::usage::record_error_usage("deepseek", &body);
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

#[cfg(test)]
fn deepseek_chat_completion_payload(
    request: &LanguageModelRequest,
    model: &str,
) -> serde_json::Value {
    deepseek_payload_with_options(
        request,
        model,
        &crate::LanguageModelProviderOptions::default(),
    )
    .expect("valid DeepSeek request")
}

fn deepseek_payload_with_options(
    request: &LanguageModelRequest,
    model: &str,
    options: &crate::LanguageModelProviderOptions,
) -> Result<serde_json::Value, LanguageModelError> {
    use crate::ProviderReasoningEffort::*;
    let mut request = request.clone();
    request.items.retain(|item| {
        !matches!(item,
            crate::LanguageModelRequestItem::ProviderContinuation { continuation }
            if continuation.items.iter().any(|item| item["content"].is_array())
        )
    });
    let mut payload =
        crate::chat_completions::payload(&request, model, DeepSeekProvider::PROVIDER_ID);
    let forced = matches!(
        request.tool_choice,
        Some(
            crate::LanguageModelToolChoice::Required | crate::LanguageModelToolChoice::Tool { .. }
        )
    );
    let enabled = options.reasoning_effort != Some(None);
    payload["thinking"] = serde_json::json!({"type":if enabled {"enabled"}else{"disabled"}});
    if let Some(effort) = options.reasoning_effort.filter(|effort| *effort != None) {
        payload["reasoning_effort"] = serde_json::json!(match effort {
            Minimal | Low => "low",
            Max | Ultra => "max",
            _ => "high",
        });
    }
    if forced && options.reasoning_effort.is_none() {
        payload["thinking"]["type"] = serde_json::json!("disabled");
    }
    crate::apply_provider_vendor_options_to_payload(&mut payload, options);
    if forced
        && payload
            .pointer("/thinking/type")
            .and_then(serde_json::Value::as_str)
            != Some("disabled")
    {
        return Err(LanguageModelError::ProviderInvocationFailed {
            provider: "deepseek".into(),
            model: model.into(),
            detail: "DeepSeek required/named tool choice requires thinking disabled".into(),
        });
    }
    if payload["thinking"]["type"] == "enabled" {
        payload.as_object_mut().unwrap().remove("temperature");
        if let Some(messages) = payload["messages"].as_array_mut() {
            for message in messages {
                if message["role"] == "assistant" && message.get("reasoning_content").is_none() {
                    message["reasoning_content"] = serde_json::json!("");
                }
            }
        }
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LanguageModelRequestItem, LanguageModelToolChoice, LanguageModelToolSpec,
        LanguageModelUsage,
    };

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
        assert_eq!(payload["thinking"]["type"], "enabled");
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

        assert_eq!(payload["thinking"]["type"], "enabled");
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
                ..Default::default()
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
        assert!(!info.supports_json_object);
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
