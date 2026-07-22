use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelRequest, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelStreamEvent, LanguageModelUsage,
    PROVIDER_DEFAULT_MODEL, render_plain_prompt, selected_model,
    stream_openai_chat_completion_response,
};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use std::path::PathBuf;

pub const NVIDIA_DEFAULT_MODEL: &str = "nvidia/nemotron-3-super-120b-a12b";

const NVIDIA_MODEL_IDS: &[&str] = &[
    "nvidia/nemotron-3-super-120b-a12b",
    "nvidia/moonshotai/kimi-k2.5",
    "nvidia/minimaxai/minimax-m2.5",
    "nvidia/z-ai/glm5",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NvidiaConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub api_key: Option<String>,
    pub credentials_path: Option<PathBuf>,
}

impl Default for NvidiaConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://integrate.api.nvidia.com/v1".to_string(),
            default_model: NVIDIA_DEFAULT_MODEL.to_string(),
            api_key: None,
            credentials_path: default_api_key_credentials_path(
                "SINOMO_NVIDIA_CREDENTIALS_FILE",
                NvidiaProvider::PROVIDER_ID,
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NvidiaProvider {
    config: NvidiaConfig,
    http: Client,
}

impl NvidiaProvider {
    pub const PROVIDER_ID: &'static str = "nvidia";

    pub fn known_model_ids() -> Vec<String> {
        NVIDIA_MODEL_IDS
            .iter()
            .map(|model| model.to_string())
            .collect()
    }

    pub fn new(config: NvidiaConfig) -> Self {
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
            &["SINOMO_NVIDIA_API_KEY", "NVIDIA_API_KEY", "NIM_API_KEY"],
            "no NVIDIA API key found; set NVIDIA_API_KEY or run `sinomo providers auth nvidia`",
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

impl Default for NvidiaProvider {
    fn default() -> Self {
        Self::new(NvidiaConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::parse_declared_context_window_tokens(model)
}

impl LanguageModelProvider for NvidiaProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: crate::LanguageModelHostAccess::NetworkOnly,
            capabilities: crate::LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = normalize_nvidia_model_id(&selected_model(&self.config.default_model, request));
        let api_key = self.api_key()?;
        let url = nvidia_chat_completions_url(&self.config.api_base_url);
        let response = self
            .http
            .post(url)
            .headers(nvidia_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&nvidia_chat_completion_payload(request, &model))
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
                detail: format!("NVIDIA Chat Completions API failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        let content = extract_nvidia_response_text(&value);
        let usage = value.get("usage").map(|usage| LanguageModelUsage {
            input_tokens: usage
                .get("prompt_tokens")
                .or_else(|| usage.get("input_tokens"))
                .and_then(serde_json::Value::as_u64),
            output_tokens: usage
                .get("completion_tokens")
                .or_else(|| usage.get("output_tokens"))
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
        let model = normalize_nvidia_model_id(&selected_model(&self.config.default_model, request));
        let api_key = self.api_key()?;
        let url = nvidia_chat_completions_url(&self.config.api_base_url);
        let mut payload = nvidia_chat_completion_payload(request, &model);
        payload["stream"] = serde_json::json!(true);
        let mut headers = nvidia_headers(&api_key, Self::PROVIDER_ID, &model)?;
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
                detail: format!("NVIDIA Chat Completions API failed: HTTP {status}: {body}"),
            });
        }

        on_event(LanguageModelStreamEvent::Started {
            provider: Self::PROVIDER_ID.to_string(),
            model: model.clone(),
        });
        stream_openai_chat_completion_response(response, Self::PROVIDER_ID, &model, on_event)
    }
}

pub fn normalize_nvidia_model_id(model: &str) -> String {
    let mut normalized = model.trim().to_string();
    while let Some(rest) = normalized.strip_prefix("nvidia/nvidia/") {
        normalized = format!("nvidia/{rest}");
    }
    normalized
}

fn nvidia_chat_completions_url(api_base_url: &str) -> String {
    let base = api_base_url.trim().trim_end_matches('/');
    if base.ends_with("/chat/completions") {
        base.to_string()
    } else if base == "https://integrate.api.nvidia.com" {
        format!("{base}/v1/chat/completions")
    } else {
        format!("{base}/chat/completions")
    }
}

fn nvidia_headers(
    api_key: &str,
    provider: &str,
    model: &str,
) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
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

fn nvidia_chat_completion_payload(
    request: &LanguageModelRequest,
    model: &str,
) -> serde_json::Value {
    let mut messages = request
        .messages
        .iter()
        .map(|message| {
            serde_json::json!({
                "role": match message.role {
                    LanguageModelRole::System => "system",
                    LanguageModelRole::User => "user",
                    LanguageModelRole::Assistant => "assistant",
                },
                "content": message.content,
            })
        })
        .collect::<Vec<_>>();
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
    payload
}

fn extract_nvidia_response_text(value: &serde_json::Value) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: NvidiaProvider::PROVIDER_ID.to_string(),
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
    fn nvidia_payload_uses_openai_chat_completions_shape() {
        let payload = nvidia_chat_completion_payload(&request(), NVIDIA_DEFAULT_MODEL);

        assert_eq!(payload["model"], NVIDIA_DEFAULT_MODEL);
        assert_eq!(payload["stream"], false);
        assert_eq!(payload["max_tokens"], 200);
        assert_eq!(payload["response_format"]["type"], "json_object");
        assert_eq!(payload["messages"][0]["role"], "system");
        assert_eq!(payload["messages"][1]["content"], "Summarize evt_1.");
    }

    #[test]
    fn nvidia_response_text_reads_chat_message() {
        let value = serde_json::json!({
            "choices": [{ "message": { "content": "hello" } }],
            "usage": { "prompt_tokens": 1, "completion_tokens": 2 }
        });

        assert_eq!(extract_nvidia_response_text(&value), "hello");
    }

    #[test]
    fn provider_reports_http_capabilities() {
        let info = NvidiaProvider::default().info();

        assert_eq!(info.provider, NvidiaProvider::PROVIDER_ID);
        assert_eq!(info.default_model, NVIDIA_DEFAULT_MODEL);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
    }

    #[test]
    fn nvidia_model_ids_drop_duplicate_provider_prefixes() {
        assert_eq!(
            normalize_nvidia_model_id("nvidia/nvidia/nemotron-3-super-120b-a12b"),
            "nvidia/nemotron-3-super-120b-a12b"
        );
        assert_eq!(
            normalize_nvidia_model_id("nvidia/nvidia/nvidia/nemotron-3-super-120b-a12b"),
            "nvidia/nemotron-3-super-120b-a12b"
        );
        assert_eq!(
            normalize_nvidia_model_id("nvidia/moonshotai/kimi-k2.5"),
            "nvidia/moonshotai/kimi-k2.5"
        );
    }

    #[test]
    fn nvidia_chat_url_accepts_base_or_full_endpoint() {
        assert_eq!(
            nvidia_chat_completions_url("https://integrate.api.nvidia.com"),
            "https://integrate.api.nvidia.com/v1/chat/completions"
        );
        assert_eq!(
            nvidia_chat_completions_url("https://integrate.api.nvidia.com/v1"),
            "https://integrate.api.nvidia.com/v1/chat/completions"
        );
        assert_eq!(
            nvidia_chat_completions_url("https://integrate.api.nvidia.com/v1/chat/completions"),
            "https://integrate.api.nvidia.com/v1/chat/completions"
        );
    }
}
