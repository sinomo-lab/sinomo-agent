use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
use crate::{
    LanguageModelCustomToolFormat, LanguageModelCustomToolSpec, LanguageModelError,
    LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelProviderOptions, LanguageModelProviderToolSpec,
    LanguageModelRequest, LanguageModelRequestItem, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelResponseItem, LanguageModelRole,
    LanguageModelStreamEvent, LanguageModelToolCall, LanguageModelToolChoice,
    LanguageModelToolSpec, LanguageModelUsage, PROVIDER_DEFAULT_MODEL,
    apply_provider_vendor_options_to_payload, read_server_sent_events, selected_model,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub use sinomo_runtime::api::lm::OPENAI_DEFAULT_MODEL;
pub const OPENAI_LONG_CONTEXT_PRICE_THRESHOLD_TOKENS: usize = 272_000;
pub const OPENAI_GPT_5_6_MAX_CONTEXT_WINDOW_TOKENS: usize = 1_050_000;
pub const OPENAI_DEFAULT_WEB_SEARCH_MODEL: &str = "gpt-5.6-luna";
pub const OPENAI_DEFAULT_IMAGE_MODEL: &str = "gpt-image-2";
/// Reference images accepted by one `images/edits` request.
pub const OPENAI_MAX_IMAGE_REFERENCES: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub api_key: Option<String>,
    pub credentials_path: Option<PathBuf>,
    pub options: LanguageModelProviderOptions,
}

impl Default for OpenAiConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://api.openai.com/v1".to_string(),
            default_model: OPENAI_DEFAULT_MODEL.to_string(),
            api_key: None,
            credentials_path: default_api_key_credentials_path(
                "SINOMO_OPENAI_CREDENTIALS_FILE",
                OpenAiProvider::PROVIDER_ID,
            ),
            options: LanguageModelProviderOptions::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct OpenAiProvider {
    config: OpenAiConfig,
    http: Client,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OpenAiWebSearchContextSize {
    Low,
    #[default]
    Medium,
    High,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OpenAiWebSearchRequest {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub context_size: OpenAiWebSearchContextSize,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OpenAiWebSearchSource {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OpenAiWebSearchResponse {
    pub model: String,
    pub text: String,
    pub sources: Vec<OpenAiWebSearchSource>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OpenAiImageRequest {
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub referenced_images: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiImageResponse {
    pub model: String,
    pub bytes: Vec<u8>,
    pub media_type: String,
    pub output_format: String,
    pub size: Option<String>,
}

impl OpenAiProvider {
    pub const PROVIDER_ID: &'static str = sinomo_runtime::api::lm::OPENAI_PROVIDER_ID;

    pub fn new(config: OpenAiConfig) -> Self {
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
            &["SINOMO_OPENAI_API_KEY", "OPENAI_API_KEY"],
            "no OpenAI API key found; set OPENAI_API_KEY or run `sinomo providers auth openai`",
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
            &crate::catalog::endpoint(&self.config.api_base_url, "models"),
            openai_headers(
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
            metadata: serde_json::Value::Null,
        };
        self.complete(&request).map(|_| ())
    }

    pub fn web_search(
        &self,
        request: &OpenAiWebSearchRequest,
    ) -> Result<OpenAiWebSearchResponse, LanguageModelError> {
        let query = request.query.trim();
        if query.is_empty() {
            return Err(openai_hosted_tool_error(
                OPENAI_DEFAULT_WEB_SEARCH_MODEL,
                "web search query must not be empty",
            ));
        }
        let model = request
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .unwrap_or(OPENAI_DEFAULT_WEB_SEARCH_MODEL)
            .to_string();
        let api_key = self.api_key()?;
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(openai_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&openai_web_search_payload(request, &model))
            .send()
            .map_err(|source| openai_hosted_tool_error(&model, source))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(openai_hosted_tool_error(
                &model,
                format!("OpenAI web search failed: HTTP {status}: {body}"),
            ));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| openai_hosted_tool_error(&model, source))?;
        Ok(OpenAiWebSearchResponse {
            model,
            text: extract_openai_response_text(&value),
            sources: extract_openai_web_search_sources(&value),
        })
    }

    pub fn generate_or_edit_image(
        &self,
        request: &OpenAiImageRequest,
    ) -> Result<OpenAiImageResponse, LanguageModelError> {
        let operation =
            image_request_operation(request, Self::PROVIDER_ID, OPENAI_DEFAULT_IMAGE_MODEL)?;
        let api_key = self.api_key()?;
        let url = format!(
            "{}/images/{operation}",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(openai_headers(
                &api_key,
                Self::PROVIDER_ID,
                OPENAI_DEFAULT_IMAGE_MODEL,
            )?)
            .json(&image_payload(request, OPENAI_DEFAULT_IMAGE_MODEL))
            .send()
            .map_err(|source| openai_hosted_tool_error(OPENAI_DEFAULT_IMAGE_MODEL, source))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(openai_hosted_tool_error(
                OPENAI_DEFAULT_IMAGE_MODEL,
                format!("OpenAI image {operation} failed: HTTP {status}: {body}"),
            ));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| openai_hosted_tool_error(OPENAI_DEFAULT_IMAGE_MODEL, source))?;
        image_response(&value, Self::PROVIDER_ID, OPENAI_DEFAULT_IMAGE_MODEL)
    }
}

fn openai_hosted_tool_error(model: &str, detail: impl std::fmt::Display) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: OpenAiProvider::PROVIDER_ID.to_string(),
        model: model.to_string(),
        detail: detail.to_string(),
    }
}

impl Default for OpenAiProvider {
    fn default() -> Self {
        Self::new(OpenAiConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::parse_declared_context_window_tokens(model).or_else(|| {
        let model = model.to_ascii_lowercase();
        if model.contains("gpt-5.6")
            || model.starts_with("gpt-6")
            || matches!(
                model.as_str(),
                "gpt-5.4" | "gpt-5.4-pro" | "gpt-5.5" | "gpt-5.5-pro"
            )
        {
            Some(OPENAI_LONG_CONTEXT_PRICE_THRESHOLD_TOKENS)
        } else if model.contains("gpt-5")
            || model.contains("gpt-4.1")
            || model.contains("o3")
            || model.contains("o4")
        {
            Some(400_000)
        } else {
            None
        }
    })
}

pub(crate) fn inferred_max_context_window_tokens(model: &str) -> Option<usize> {
    let model = model.to_ascii_lowercase();
    if model.contains("gpt-5.6")
        || model.starts_with("gpt-6")
        || matches!(
            model.as_str(),
            "gpt-5.4" | "gpt-5.4-pro" | "gpt-5.5" | "gpt-5.5-pro"
        )
    {
        Some(OPENAI_GPT_5_6_MAX_CONTEXT_WINDOW_TOKENS)
    } else {
        inferred_context_window_tokens(&model)
    }
}

pub(crate) fn prefers_native_tool_protocol(model: &str) -> bool {
    let model = model.to_ascii_lowercase();
    model.contains("gpt") || model.starts_with('o')
}

impl LanguageModelProvider for OpenAiProvider {
    fn available_models(&self) -> Result<Vec<String>, LanguageModelError> {
        self.list_models()
    }
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: crate::LanguageModelHostAccess::NetworkOnly,
            capabilities: crate::LanguageModelProviderCapabilities::openai(),
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
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(openai_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&openai_responses_payload(
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
                detail: format!("OpenAI Responses API failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        let content = extract_openai_response_text(&value);
        let tool_calls = extract_openai_tool_calls(&value);
        let items = openai_response_items(&value, Self::PROVIDER_ID, &model, &tool_calls);
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
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let mut headers = openai_headers(&api_key, Self::PROVIDER_ID, &model)?;
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        let mut payload = openai_responses_payload(request, &model, &self.config.options);
        payload["stream"] = serde_json::json!(true);
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
                detail: format!("OpenAI Responses API failed: HTTP {status}: {body}"),
            });
        }

        on_event(LanguageModelStreamEvent::Started {
            provider: Self::PROVIDER_ID.to_string(),
            model: model.clone(),
        });
        stream_openai_responses_response(response, Self::PROVIDER_ID, &model, on_event)
    }
}

pub(crate) fn openai_headers(
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

pub(crate) fn openai_responses_payload(
    request: &LanguageModelRequest,
    model: &str,
    options: &LanguageModelProviderOptions,
) -> serde_json::Value {
    let instructions = request
        .messages
        .iter()
        .filter(|message| message.role == LanguageModelRole::System)
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let input = openai_responses_input(request, model);
    let mut payload = serde_json::json!({
        "model": model,
        "input": input,
        "store": false,
    });
    if !instructions.is_empty() {
        payload["instructions"] = serde_json::json!(instructions);
    }
    if let Some(temperature) = request.temperature {
        payload["temperature"] = serde_json::json!(temperature);
    }
    if let Some(max_tokens) = request.max_output_tokens {
        payload["max_output_tokens"] = serde_json::json!(max_tokens);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["text"] = serde_json::json!({
            "format": { "type": "json_object" }
        });
    }
    if !request.tools.is_empty() {
        payload["tools"] = serde_json::json!(
            request
                .tools
                .iter()
                .map(openai_responses_tool_spec)
                .collect::<Vec<_>>()
        );
    }
    if let Some(tool_choice) = &request.tool_choice {
        payload["tool_choice"] = openai_responses_tool_choice(tool_choice, &request.tools);
    }
    if let Some(parallel_tool_calls) = request.parallel_tool_calls {
        payload["parallel_tool_calls"] = serde_json::json!(parallel_tool_calls);
    }
    if let Some(effort) = options.reasoning_effort.and_then(|effort| {
        if model.starts_with("gpt-6") || model.starts_with("gpt-5.6") {
            Some(match effort {
                crate::ProviderReasoningEffort::Max | crate::ProviderReasoningEffort::Ultra => {
                    "max"
                }
                _ => effort.as_config_value(),
            })
        } else {
            effort.as_openai_responses_value()
        }
    }) {
        payload["reasoning"] = serde_json::json!({ "effort": effort });
    }
    if let Some(verbosity) = options.text_verbosity {
        if !payload
            .get("text")
            .is_some_and(serde_json::Value::is_object)
        {
            payload["text"] = serde_json::json!({});
        }
        payload["text"]["verbosity"] = serde_json::json!(verbosity.as_config_value());
    }
    if matches!(request.provider.as_str(), "openai" | "github-copilot") {
        payload["include"] = serde_json::json!(["reasoning.encrypted_content"]);
    }
    apply_provider_vendor_options_to_payload(&mut payload, options);
    if model.starts_with("gpt-6") {
        for key in ["temperature", "top_p", "top_logprobs"] {
            payload.as_object_mut().unwrap().remove(key);
        }
    }
    payload
}

fn openai_web_search_payload(request: &OpenAiWebSearchRequest, model: &str) -> serde_json::Value {
    let mut tool = serde_json::json!({
        "type": "web_search",
        "search_context_size": match request.context_size {
            OpenAiWebSearchContextSize::Low => "low",
            OpenAiWebSearchContextSize::Medium => "medium",
            OpenAiWebSearchContextSize::High => "high",
        },
    });
    let allowed_domains = request
        .allowed_domains
        .iter()
        .map(|domain| domain.trim())
        .filter(|domain| !domain.is_empty())
        .collect::<Vec<_>>();
    if !allowed_domains.is_empty() {
        tool["filters"] = serde_json::json!({ "allowed_domains": allowed_domains });
    }
    serde_json::json!({
        "model": model,
        "input": request.query.trim(),
        "store": false,
        "tools": [tool],
        "include": ["web_search_call.action.sources"],
    })
}

fn extract_openai_web_search_sources(value: &serde_json::Value) -> Vec<OpenAiWebSearchSource> {
    let mut sources = BTreeMap::<String, Option<String>>::new();
    for item in value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        for source in item
            .get("action")
            .and_then(|action| action.get("sources"))
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            insert_openai_web_source(&mut sources, source);
        }
        for content in item
            .get("content")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            for annotation in content
                .get("annotations")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                insert_openai_web_source(&mut sources, annotation);
            }
        }
    }
    web_search_sources(sources)
}

/// Collects citation objects that carry a `url` into deduplicated sources.
///
/// The Codex backend returns a flat result list rather than Responses API output items, so the
/// per-source shape is shared while the traversal that reaches it is not.
pub(crate) fn collect_web_search_sources<'a>(
    values: impl IntoIterator<Item = &'a serde_json::Value>,
) -> Vec<OpenAiWebSearchSource> {
    let mut sources = BTreeMap::<String, Option<String>>::new();
    for value in values {
        insert_openai_web_source(&mut sources, value);
    }
    web_search_sources(sources)
}

fn web_search_sources(sources: BTreeMap<String, Option<String>>) -> Vec<OpenAiWebSearchSource> {
    sources
        .into_iter()
        .map(|(url, title)| OpenAiWebSearchSource { url, title })
        .collect()
}

fn insert_openai_web_source(
    sources: &mut BTreeMap<String, Option<String>>,
    value: &serde_json::Value,
) {
    let Some(url) = value
        .get("url")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())
    else {
        return;
    };
    let title = value
        .get("title")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|title| !title.is_empty())
        .map(str::to_string);
    sources
        .entry(url.to_string())
        .and_modify(|existing| {
            if existing.is_none() {
                *existing = title.clone();
            }
        })
        .or_insert(title);
}

/// Builds the error every hosted image caller reports, tagged with the provider that ran it.
pub(crate) fn image_tool_error(
    provider: &str,
    model: &str,
    detail: impl std::fmt::Display,
) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: provider.to_string(),
        model: model.to_string(),
        detail: detail.to_string(),
    }
}

/// Validates a hosted image request and resolves the images endpoint operation it maps to.
///
/// Generation and editing share one tool surface: a request that carries reference images is an
/// edit, and one without them is a generation.
pub(crate) fn image_request_operation(
    request: &OpenAiImageRequest,
    provider: &str,
    model: &str,
) -> Result<&'static str, LanguageModelError> {
    if request.prompt.trim().is_empty() {
        return Err(image_tool_error(
            provider,
            model,
            "image prompt must not be empty",
        ));
    }
    if request.referenced_images.len() > OPENAI_MAX_IMAGE_REFERENCES {
        return Err(image_tool_error(
            provider,
            model,
            format!("image edits accept at most {OPENAI_MAX_IMAGE_REFERENCES} reference images"),
        ));
    }
    Ok(if request.referenced_images.is_empty() {
        "generations"
    } else {
        "edits"
    })
}

pub(crate) fn image_payload(request: &OpenAiImageRequest, model: &str) -> serde_json::Value {
    let mut payload = serde_json::json!({
        "model": model,
        "prompt": request.prompt.trim(),
    });
    if !request.referenced_images.is_empty() {
        payload["images"] = serde_json::json!(
            request
                .referenced_images
                .iter()
                .map(|image_url| serde_json::json!({ "image_url": image_url }))
                .collect::<Vec<_>>()
        );
    }
    if let Some(size) = request
        .size
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        payload["size"] = serde_json::json!(size);
    }
    if let Some(quality) = request
        .quality
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        payload["quality"] = serde_json::json!(quality);
    }
    if let Some(background) = request
        .background
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        payload["background"] = serde_json::json!(background);
    }
    payload
}

pub(crate) fn image_response(
    value: &serde_json::Value,
    provider: &str,
    model: &str,
) -> Result<OpenAiImageResponse, LanguageModelError> {
    let encoded = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .and_then(|data| data.first())
        .and_then(|data| data.get("b64_json"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            image_tool_error(provider, model, "image response did not include image data")
        })?;
    let bytes = BASE64_STANDARD.decode(encoded.trim()).map_err(|source| {
        image_tool_error(
            provider,
            model,
            format!("image response contained invalid base64 data: {source}"),
        )
    })?;
    let output_format = value
        .get("output_format")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("png")
        .trim()
        .to_ascii_lowercase();
    let media_type = match output_format.as_str() {
        "jpeg" | "jpg" => "image/jpeg",
        "webp" => "image/webp",
        _ => "image/png",
    }
    .to_string();
    Ok(OpenAiImageResponse {
        model: model.to_string(),
        bytes,
        media_type,
        output_format,
        size: value
            .get("size")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
    })
}

fn openai_responses_tool_spec(tool: &LanguageModelToolSpec) -> serde_json::Value {
    if let Some(custom) = openai_custom_tool_spec(tool) {
        return serde_json::json!({
            "type": "custom",
            "name": tool.name,
            "description": tool.description,
            "format": openai_custom_tool_format(&custom.format),
        });
    }

    serde_json::json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": normalized_tool_input_schema(tool),
    })
}

fn openai_responses_input(request: &LanguageModelRequest, model: &str) -> serde_json::Value {
    let items = crate::provider_request_items(request, &request.provider, model)
        .iter()
        .flat_map(|item| match item {
            LanguageModelRequestItem::ProviderContinuation { continuation } => {
                continuation.items.clone()
            }
            _ => openai_responses_input_item(item, &request.tools)
                .into_iter()
                .collect(),
        })
        .collect::<Vec<_>>();
    if items.is_empty() {
        serde_json::json!(request.messages.iter().filter(|message|message.role!=LanguageModelRole::System)
            .map(|message|serde_json::json!({"role":openai_message_role(&message.role),"content":message.content})).collect::<Vec<_>>())
    } else {
        serde_json::Value::Array(items)
    }
}

fn openai_responses_input_item(
    item: &LanguageModelRequestItem,
    tools: &[LanguageModelToolSpec],
) -> Option<serde_json::Value> {
    match item {
        LanguageModelRequestItem::ProviderContinuation { .. } => None,
        LanguageModelRequestItem::Message { role, content } => {
            if *role == LanguageModelRole::System || content.trim().is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "type": "message",
                "role": openai_message_role(role),
                "content": [{
                    "type": match role {
                        LanguageModelRole::Assistant => "output_text",
                        LanguageModelRole::System | LanguageModelRole::User => "input_text",
                    },
                    "text": content,
                }],
            }))
        }
        LanguageModelRequestItem::ToolCall {
            call_id,
            name,
            input,
        } => {
            if let Some(custom) = openai_custom_tool_spec_by_name(name, tools) {
                let input = custom
                    .raw_input_argument
                    .as_deref()
                    .and_then(|key| input.get(key))
                    .unwrap_or(input);
                Some(serde_json::json!({
                    "type": "custom_tool_call",
                    "call_id": call_id,
                    "name": name,
                    "input": openai_tool_input_text(input),
                }))
            } else {
                Some(serde_json::json!({
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": openai_tool_input_text(input),
                }))
            }
        }
        LanguageModelRequestItem::ToolCallOutput {
            call_id,
            name,
            output,
        } => {
            if openai_custom_tool_spec_by_name(name, tools).is_some() {
                Some(serde_json::json!({
                    "type": "custom_tool_call_output",
                    "call_id": call_id,
                    "name": name,
                    "output": openai_tool_output_text(output),
                }))
            } else {
                Some(serde_json::json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": openai_tool_output_text(output),
                }))
            }
        }
        LanguageModelRequestItem::Context { label, content } => {
            if content.trim().is_empty() {
                return None;
            }
            Some(serde_json::json!({
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": format!("{label}:\n{content}"),
                }],
            }))
        }
    }
}

fn openai_message_role(role: &LanguageModelRole) -> &'static str {
    match role {
        LanguageModelRole::System => "system",
        LanguageModelRole::User => "user",
        LanguageModelRole::Assistant => "assistant",
    }
}

fn openai_tool_input_text(input: &serde_json::Value) -> String {
    match input {
        serde_json::Value::String(value) => value.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn openai_tool_output_text(output: &serde_json::Value) -> String {
    match output {
        serde_json::Value::String(value) => value.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn openai_responses_tool_choice(
    tool_choice: &LanguageModelToolChoice,
    tools: &[LanguageModelToolSpec],
) -> serde_json::Value {
    match tool_choice {
        LanguageModelToolChoice::Auto => serde_json::json!("auto"),
        LanguageModelToolChoice::None => serde_json::json!("none"),
        LanguageModelToolChoice::Required => serde_json::json!("required"),
        LanguageModelToolChoice::Tool { name } => match tools
            .iter()
            .find(|tool| tool.name == *name)
            .and_then(openai_custom_tool_spec)
        {
            Some(_) => serde_json::json!({ "type": "custom", "name": name }),
            None => serde_json::json!({ "type": "function", "name": name }),
        },
    }
}

fn openai_custom_tool_spec(tool: &LanguageModelToolSpec) -> Option<&LanguageModelCustomToolSpec> {
    provider_tool_spec(tool, OpenAiProvider::PROVIDER_ID).and_then(|spec| spec.custom.as_ref())
}

fn openai_custom_tool_spec_by_name<'a>(
    name: &str,
    tools: &'a [LanguageModelToolSpec],
) -> Option<&'a LanguageModelCustomToolSpec> {
    tools
        .iter()
        .find(|tool| tool.name == name)
        .and_then(openai_custom_tool_spec)
}

fn provider_tool_spec<'a>(
    tool: &'a LanguageModelToolSpec,
    provider: &str,
) -> Option<&'a LanguageModelProviderToolSpec> {
    tool.provider_specs
        .iter()
        .find(|spec| spec.provider.eq_ignore_ascii_case(provider))
}

fn openai_custom_tool_format(format: &LanguageModelCustomToolFormat) -> serde_json::Value {
    match format {
        LanguageModelCustomToolFormat::Grammar { syntax, definition } => serde_json::json!({
            "type": "grammar",
            "syntax": syntax,
            "definition": definition,
        }),
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

pub(crate) fn extract_openai_response_text(value: &serde_json::Value) -> String {
    if let Some(output_text) = value.get("output_text").and_then(serde_json::Value::as_str) {
        return output_text.to_string();
    }

    value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .flat_map(|item| {
                    item.get("content")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                })
                .filter_map(|content| content.get("text").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

pub(crate) fn stream_openai_responses_response(
    response: reqwest::blocking::Response,
    provider: &str,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError> {
    let mut output = Vec::new();
    let mut output_text = String::new();
    let mut emitted_reasoning_delta = false;
    let mut completed_response = None;
    let mut emitted_text = false;
    let mut emitted_finished = false;

    read_server_sent_events(response, provider, model, |event| {
        let mut value =
            serde_json::from_str::<serde_json::Value>(&event.data).map_err(|source| {
                LanguageModelError::ProviderInvocationFailed {
                    provider: provider.to_string(),
                    model: model.to_string(),
                    detail: format!(
                        "failed to parse OpenAI stream event: {source}: {}",
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
            "response.output_text.delta" => {
                if let Some(delta) = value.get("delta").and_then(serde_json::Value::as_str) {
                    output_text.push_str(delta);
                    emitted_text = true;
                    on_event(LanguageModelStreamEvent::TextDelta {
                        delta: delta.to_string(),
                    });
                }
            }
            "response.reasoning_summary_text.delta"
            | "response.reasoning_summary.delta"
            | "response.reasoning_text.delta" => {
                if let Some(delta) = value.get("delta").and_then(serde_json::Value::as_str)
                    && !delta.is_empty()
                {
                    emitted_reasoning_delta = true;
                    on_event(LanguageModelStreamEvent::ReasoningDelta {
                        delta: delta.to_string(),
                    });
                }
            }
            "response.output_item.done" => {
                if let Some(item) = value.get("item") {
                    if let Some(call) = openai_output_item_tool_call(item) {
                        on_event(LanguageModelStreamEvent::ToolCallStarted {
                            call_id: call.call_id.clone(),
                            name: call.name.clone(),
                        });
                        on_event(LanguageModelStreamEvent::ToolCallCompleted {
                            call_id: call.call_id.clone(),
                            name: call.name,
                            input: call.input,
                        });
                    }
                    if !emitted_reasoning_delta {
                        for summary in openai_reasoning_summaries(item) {
                            on_event(LanguageModelStreamEvent::ReasoningSummary { summary });
                        }
                    }
                    output.push(item.clone());
                }
            }
            "response.completed" => {
                let mut final_response = value
                    .get("response")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let missing_or_empty_output = final_response
                    .get("output")
                    .and_then(serde_json::Value::as_array)
                    .is_none_or(Vec::is_empty);
                if missing_or_empty_output && !output.is_empty() {
                    final_response["output"] = serde_json::json!(output.clone());
                }
                if final_response.get("output_text").is_none() && !output_text.is_empty() {
                    final_response["output_text"] = serde_json::json!(output_text.clone());
                }
                let final_text = extract_openai_response_text(&final_response);
                if !emitted_text && !final_text.is_empty() {
                    emitted_text = true;
                    on_event(LanguageModelStreamEvent::TextDelta {
                        delta: final_text.clone(),
                    });
                }
                if let Some(usage) = extract_openai_usage(&final_response) {
                    on_event(LanguageModelStreamEvent::Usage { usage });
                }
                on_event(LanguageModelStreamEvent::Finished {
                    finish_reason: openai_responses_finish_reason(&final_response),
                });
                emitted_finished = true;
                completed_response = Some(final_response);
            }
            "response.failed" | "error" => {
                let detail = openai_responses_event_error_message(&value)
                    .unwrap_or_else(|| format!("OpenAI stream failed: {value}"));
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

    let mut final_response = completed_response
        .ok_or_else(|| crate::incomplete_provider_stream(provider, model, on_event))?;
    let missing_or_empty_output = final_response
        .get("output")
        .and_then(serde_json::Value::as_array)
        .is_none_or(Vec::is_empty);
    if missing_or_empty_output && !output.is_empty() {
        final_response["output"] = serde_json::json!(output);
    }
    if final_response.get("output_text").is_none() && !output_text.is_empty() {
        final_response["output_text"] = serde_json::json!(output_text);
    }

    let content = extract_openai_response_text(&final_response);
    if !emitted_text && !content.is_empty() {
        on_event(LanguageModelStreamEvent::TextDelta {
            delta: content.clone(),
        });
    }
    let tool_calls = extract_openai_tool_calls(&final_response);
    let mut items = extract_openai_reasoning_summaries(&final_response)
        .into_iter()
        .map(|summary| LanguageModelResponseItem::ReasoningSummary { summary })
        .collect::<Vec<_>>();
    items.extend(
        tool_calls
            .iter()
            .cloned()
            .map(|call| LanguageModelResponseItem::ToolCall { call }),
    );
    items.extend(
        openai_response_items(&final_response, provider, model, &tool_calls)
            .into_iter()
            .filter(|item| matches!(item, LanguageModelResponseItem::ProviderContinuation { .. })),
    );
    let usage = extract_openai_usage(&final_response);
    if !emitted_finished {
        if let Some(usage) = &usage {
            on_event(LanguageModelStreamEvent::Usage {
                usage: usage.clone(),
            });
        }
        on_event(LanguageModelStreamEvent::Finished {
            finish_reason: openai_responses_finish_reason(&final_response),
        });
    }

    Ok(LanguageModelResponse {
        provider: provider.to_string(),
        model: model.to_string(),
        content,
        items,
        tool_calls,
        usage,
    })
}

pub(crate) fn openai_response_items(
    value: &serde_json::Value,
    provider: &str,
    model: &str,
    calls: &[LanguageModelToolCall],
) -> Vec<LanguageModelResponseItem> {
    let mut items: Vec<_> = calls
        .iter()
        .cloned()
        .map(|call| LanguageModelResponseItem::ToolCall { call })
        .collect();
    let output = value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| {
            matches!(
                item["type"].as_str(),
                Some("message" | "reasoning" | "function_call" | "custom_tool_call")
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    if !output.is_empty() {
        items.push(crate::response_continuation(provider, model, output, calls));
    }
    items
}

pub(crate) fn extract_openai_usage(value: &serde_json::Value) -> Option<LanguageModelUsage> {
    value.get("usage").map(|usage| LanguageModelUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("output_tokens")
            .and_then(serde_json::Value::as_u64),
    })
}

fn openai_responses_finish_reason(value: &serde_json::Value) -> Option<String> {
    value
        .get("status")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            value
                .get("finish_reason")
                .and_then(serde_json::Value::as_str)
        })
        .or_else(|| {
            value
                .get("incomplete_details")
                .and_then(|details| details.get("reason"))
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_string)
}

fn openai_responses_event_error_message(value: &serde_json::Value) -> Option<String> {
    value
        .pointer("/response/error/message")
        .or_else(|| value.pointer("/error/message"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

pub(crate) fn extract_openai_tool_calls(value: &serde_json::Value) -> Vec<LanguageModelToolCall> {
    value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(openai_output_item_tool_call)
        .collect()
}

fn extract_openai_reasoning_summaries(value: &serde_json::Value) -> Vec<String> {
    value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(openai_reasoning_summaries)
        .collect()
}

fn openai_reasoning_summaries(item: &serde_json::Value) -> Vec<String> {
    if item.get("type").and_then(serde_json::Value::as_str) != Some("reasoning") {
        return Vec::new();
    }
    item.get("summary")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| {
            part.as_str()
                .or_else(|| part.get("text").and_then(serde_json::Value::as_str))
                .or_else(|| part.get("summary").and_then(serde_json::Value::as_str))
        })
        .map(str::to_string)
        .filter(|summary| !summary.trim().is_empty())
        .collect()
}

fn openai_output_item_tool_call(item: &serde_json::Value) -> Option<LanguageModelToolCall> {
    let item_type = item.get("type").and_then(serde_json::Value::as_str)?;
    match item_type {
        "function_call" => Some(LanguageModelToolCall {
            call_id: item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            name: item.get("name")?.as_str()?.to_string(),
            input: parse_tool_arguments(item.get("arguments")),
            summary: None,
        }),
        "custom_tool_call" => Some(LanguageModelToolCall {
            call_id: item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            name: item
                .get("name")
                .or_else(|| item.get("tool_name"))?
                .as_str()?
                .to_string(),
            input: item
                .get("input")
                .cloned()
                .unwrap_or_else(|| parse_tool_arguments(item.get("arguments"))),
            summary: None,
        }),
        _ => None,
    }
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
    use crate::{ProviderReasoningEffort, ProviderTextVerbosity};

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: "openai".to_string(),
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
    fn openai_and_codex_defaults_use_gpt_5_6_sol() {
        assert_eq!(OPENAI_DEFAULT_MODEL, "gpt-5.6-sol");
        assert_eq!(OpenAiConfig::default().default_model, OPENAI_DEFAULT_MODEL);
        assert_eq!(crate::CODEX_DEFAULT_MODEL, OPENAI_DEFAULT_MODEL);
        assert_eq!(
            crate::effective_model_name("openai", crate::PROVIDER_DEFAULT_MODEL),
            OPENAI_DEFAULT_MODEL
        );
        assert_eq!(
            crate::effective_model_name(crate::CODEX_PROVIDER_ID, crate::PROVIDER_DEFAULT_MODEL),
            OPENAI_DEFAULT_MODEL
        );
    }

    #[test]
    fn payload_uses_responses_shape() {
        let payload = openai_responses_payload(
            &request(),
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );

        assert_eq!(payload["model"], "gpt-5.2-codex");
        assert_eq!(payload["store"], false);
        assert_eq!(payload["instructions"], "Return JSON only.");
        assert_eq!(payload["max_output_tokens"], 200);
        assert_eq!(payload["text"]["format"]["type"], "json_object");
        assert!(
            payload["input"][0]["content"]
                .as_str()
                .unwrap()
                .contains("Summarize evt_1.")
        );
    }

    #[test]
    fn payload_renders_native_tool_specs() {
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

        let payload = openai_responses_payload(
            &request,
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );

        assert_eq!(payload["tools"][0]["type"], "function");
        assert_eq!(payload["tools"][0]["name"], "read_file");
        assert_eq!(
            payload["tools"][0]["parameters"]["properties"]["path"]["type"],
            "string"
        );
        assert_eq!(payload["tool_choice"]["name"], "read_file");
        assert_eq!(payload["parallel_tool_calls"], false);
    }

    #[test]
    fn payload_renders_openai_custom_tool_specs() {
        let mut request = request();
        request.tools = vec![LanguageModelToolSpec {
            name: "apply_patch".to_string(),
            description: "Apply a patch.".to_string(),
            input_schema: serde_json::Value::Null,
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: vec![LanguageModelProviderToolSpec {
                provider: OpenAiProvider::PROVIDER_ID.to_string(),
                custom: Some(LanguageModelCustomToolSpec {
                    format: LanguageModelCustomToolFormat::Grammar {
                        syntax: "lark".to_string(),
                        definition: "start: /.+/".to_string(),
                    },
                    raw_input_argument: Some("patch".to_string()),
                }),
            }],
        }];
        request.tool_choice = Some(LanguageModelToolChoice::Tool {
            name: "apply_patch".to_string(),
        });

        let payload = openai_responses_payload(
            &request,
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );

        assert_eq!(payload["tools"][0]["type"], "custom");
        assert_eq!(payload["tools"][0]["name"], "apply_patch");
        assert_eq!(payload["tools"][0]["format"]["type"], "grammar");
        assert_eq!(payload["tools"][0]["format"]["syntax"], "lark");
        assert!(payload["tools"][0].get("parameters").is_none());
        assert_eq!(payload["tool_choice"]["type"], "custom");
        assert_eq!(payload["tool_choice"]["name"], "apply_patch");
    }

    #[test]
    fn payload_prefers_typed_responses_input_items() {
        let mut request = request();
        request.items = vec![
            LanguageModelRequestItem::message(LanguageModelRole::User, "Inspect Cargo.toml."),
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

        let payload = openai_responses_payload(
            &request,
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );
        let input = payload["input"].as_array().expect("typed input array");

        assert_eq!(input[0]["type"], "message");
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"][0]["type"], "input_text");
        assert_eq!(input[1]["type"], "function_call");
        assert_eq!(input[1]["call_id"], "call_read");
        assert_eq!(input[1]["name"], "read_file");
        assert_eq!(input[1]["arguments"], r#"{"path":"Cargo.toml"}"#);
        assert_eq!(input[2]["type"], "function_call_output");
        assert_eq!(input[2]["call_id"], "call_read");
        assert_eq!(input[2]["output"], r#"{"content":"[package]","ok":true}"#);
    }

    #[test]
    fn payload_renders_custom_tool_transcript_items() {
        let mut request = request();
        request.items = vec![
            LanguageModelRequestItem::tool_call(
                "call_patch",
                "apply_patch",
                serde_json::json!("*** Begin Patch\n*** End Patch\n"),
            ),
            LanguageModelRequestItem::tool_call_output(
                "call_patch",
                "apply_patch",
                serde_json::json!("patch applied"),
            ),
        ];
        request.tools = vec![LanguageModelToolSpec {
            name: "apply_patch".to_string(),
            description: "Apply a patch.".to_string(),
            input_schema: serde_json::Value::Null,
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: vec![LanguageModelProviderToolSpec {
                provider: OpenAiProvider::PROVIDER_ID.to_string(),
                custom: Some(LanguageModelCustomToolSpec {
                    format: LanguageModelCustomToolFormat::Grammar {
                        syntax: "lark".to_string(),
                        definition: "start: /.+/".to_string(),
                    },
                    raw_input_argument: Some("patch".to_string()),
                }),
            }],
        }];

        let payload = openai_responses_payload(
            &request,
            "gpt-5.2-codex",
            &LanguageModelProviderOptions::default(),
        );
        let input = payload["input"].as_array().expect("typed input array");

        assert_eq!(input[0]["type"], "custom_tool_call");
        assert_eq!(input[0]["call_id"], "call_patch");
        assert_eq!(input[0]["name"], "apply_patch");
        assert_eq!(input[0]["input"], "*** Begin Patch\n*** End Patch\n");
        assert_eq!(input[1]["type"], "custom_tool_call_output");
        assert_eq!(input[1]["call_id"], "call_patch");
        assert_eq!(input[1]["name"], "apply_patch");
        assert_eq!(input[1]["output"], "patch applied");
    }

    #[test]
    fn payload_applies_provider_options() {
        let mut options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::High),
            text_verbosity: Some(ProviderTextVerbosity::Low),
            ..LanguageModelProviderOptions::default()
        };
        options
            .vendor_options
            .insert("reasoning.summary".to_string(), "auto".to_string());

        let payload = openai_responses_payload(&request(), "gpt-5.2-codex", &options);

        assert_eq!(payload["reasoning"]["effort"], "high");
        assert_eq!(payload["reasoning"]["summary"], "auto");
        assert_eq!(payload["text"]["verbosity"], "low");
    }

    #[test]
    fn response_parser_extracts_native_tool_calls() {
        let value = serde_json::json!({
            "output": [
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_file",
                    "arguments": "{\"path\":\"Cargo.toml\"}"
                }
            ]
        });

        let calls = extract_openai_tool_calls(&value);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call_id.as_deref(), Some("call_1"));
        assert_eq!(calls[0].name, "read_file");
        assert_eq!(calls[0].input["path"], "Cargo.toml");
    }

    #[test]
    fn response_parser_extracts_custom_tool_calls() {
        let value = serde_json::json!({
            "output": [
                {
                    "type": "custom_tool_call",
                    "call_id": "call_patch",
                    "name": "apply_patch",
                    "input": "*** Begin Patch\n*** End Patch\n"
                }
            ]
        });

        let calls = extract_openai_tool_calls(&value);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call_id.as_deref(), Some("call_patch"));
        assert_eq!(calls[0].name, "apply_patch");
        assert_eq!(calls[0].input, "*** Begin Patch\n*** End Patch\n");
    }

    #[test]
    fn provider_reports_http_capabilities() {
        let info = OpenAiProvider::default().info();

        assert_eq!(info.provider, OpenAiProvider::PROVIDER_ID);
        assert_eq!(info.invocation, LanguageModelInvocation::Http);
        assert!(info.supports_json_object);
        assert!(info.capabilities.responses_http);
        assert!(info.capabilities.realtime_websocket);
        assert!(info.capabilities.web_search);
        assert!(info.capabilities.image_generation);
        assert!(info.capabilities.image_editing);
    }

    #[test]
    fn web_search_payload_and_sources_preserve_public_controls_and_citations() {
        let request = OpenAiWebSearchRequest {
            query: " latest release ".to_string(),
            model: None,
            allowed_domains: vec!["example.com".to_string(), " ".to_string()],
            context_size: OpenAiWebSearchContextSize::High,
        };
        let payload = openai_web_search_payload(&request, OPENAI_DEFAULT_WEB_SEARCH_MODEL);
        assert_eq!(payload["tools"][0]["type"], "web_search");
        assert_eq!(payload["tools"][0]["search_context_size"], "high");
        assert_eq!(
            payload["tools"][0]["filters"]["allowed_domains"][0],
            "example.com"
        );
        assert_eq!(payload["input"], "latest release");

        let sources = extract_openai_web_search_sources(&serde_json::json!({
            "output": [
                {
                    "type": "web_search_call",
                    "action": { "sources": [
                        { "url": "https://example.com/release", "title": "Release" }
                    ] }
                },
                {
                    "type": "message",
                    "content": [{
                        "type": "output_text",
                        "annotations": [
                            { "type": "url_citation", "url": "https://example.com/release" },
                            { "type": "url_citation", "url": "https://example.org/note", "title": "Note" }
                        ]
                    }]
                }
            ]
        }));
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].url, "https://example.com/release");
        assert_eq!(sources[0].title.as_deref(), Some("Release"));
        assert_eq!(sources[1].url, "https://example.org/note");
    }

    #[test]
    fn image_payload_selects_generation_or_edit_shape_and_decodes_result() {
        let request = OpenAiImageRequest {
            prompt: " add a hat ".to_string(),
            referenced_images: vec!["data:image/png;base64,AA==".to_string()],
            size: Some("1024x1024".to_string()),
            quality: Some("medium".to_string()),
            background: Some("auto".to_string()),
        };
        assert_eq!(
            image_request_operation(
                &request,
                OpenAiProvider::PROVIDER_ID,
                OPENAI_DEFAULT_IMAGE_MODEL
            )
            .expect("a referenced image selects the edit endpoint"),
            "edits"
        );
        let payload = image_payload(&request, OPENAI_DEFAULT_IMAGE_MODEL);
        assert_eq!(payload["model"], OPENAI_DEFAULT_IMAGE_MODEL);
        assert_eq!(payload["prompt"], "add a hat");
        assert_eq!(
            payload["images"][0]["image_url"],
            "data:image/png;base64,AA=="
        );

        let generation = OpenAiImageRequest {
            referenced_images: Vec::new(),
            ..request
        };
        assert_eq!(
            image_request_operation(
                &generation,
                OpenAiProvider::PROVIDER_ID,
                OPENAI_DEFAULT_IMAGE_MODEL
            )
            .expect("no referenced images selects the generation endpoint"),
            "generations"
        );
        assert!(
            image_payload(&generation, OPENAI_DEFAULT_IMAGE_MODEL)
                .get("images")
                .is_none()
        );

        let response = image_response(
            &serde_json::json!({
                "data": [{ "b64_json": "aW1hZ2U=" }],
                "output_format": "webp",
                "size": "1024x1024"
            }),
            OpenAiProvider::PROVIDER_ID,
            OPENAI_DEFAULT_IMAGE_MODEL,
        )
        .expect("decode image response");
        assert_eq!(response.model, OPENAI_DEFAULT_IMAGE_MODEL);
        assert_eq!(response.bytes, b"image");
        assert_eq!(response.media_type, "image/webp");
        assert_eq!(response.size.as_deref(), Some("1024x1024"));
    }

    #[test]
    fn native_tool_protocol_preference_uses_openai_model_family() {
        assert!(prefers_native_tool_protocol("gpt-5.2-codex"));
        assert!(prefers_native_tool_protocol("o3"));
        assert!(!prefers_native_tool_protocol("gemini-2.5-pro"));
    }
}
