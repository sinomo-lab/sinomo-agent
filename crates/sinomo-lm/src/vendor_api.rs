//! Direct Grok Responses and Kimi Chat Completions providers.
use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
use crate::*;
use reqwest::blocking::Client;
use serde_json::{Value, json};
use std::path::PathBuf;

pub use sinomo_runtime::api::lm::{
    GROK_DEFAULT_MODEL, GROK_PROVIDER_ID, KIMI_DEFAULT_MODEL, KIMI_PROVIDER_ID,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VendorApi {
    Grok,
    Kimi,
}
impl VendorApi {
    pub fn from_provider(provider: &str) -> Option<Self> {
        match normalize_model_provider(provider).as_deref()? {
            GROK_PROVIDER_ID => Some(Self::Grok),
            KIMI_PROVIDER_ID => Some(Self::Kimi),
            _ => None,
        }
    }
    pub fn provider(self) -> &'static str {
        match self {
            Self::Grok => GROK_PROVIDER_ID,
            Self::Kimi => KIMI_PROVIDER_ID,
        }
    }
}

#[derive(Clone, Debug)]
pub struct VendorApiConfig {
    pub api: VendorApi,
    pub api_base_url: String,
    pub default_model: String,
    pub api_key: Option<String>,
    pub credentials_path: Option<PathBuf>,
    pub options: LanguageModelProviderOptions,
}
impl VendorApiConfig {
    pub fn new(api: VendorApi) -> Self {
        let (base, model, env) = match api {
            VendorApi::Grok => (
                "https://api.x.ai/v1",
                GROK_DEFAULT_MODEL,
                "SINOMO_GROK_CREDENTIALS_FILE",
            ),
            VendorApi::Kimi => (
                "https://api.moonshot.ai/v1",
                KIMI_DEFAULT_MODEL,
                "SINOMO_KIMI_CREDENTIALS_FILE",
            ),
        };
        Self {
            api,
            api_base_url: base.into(),
            default_model: model.into(),
            api_key: None,
            credentials_path: default_api_key_credentials_path(env, api.provider()),
            options: LanguageModelProviderOptions::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VendorApiProvider {
    config: VendorApiConfig,
    http: Client,
}
impl VendorApiProvider {
    pub fn new(config: VendorApiConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }
    pub fn api_key(&self) -> Result<String, LanguageModelError> {
        let provider = self.config.api.provider();
        resolve_api_key(
            provider,
            &self.config.api_key,
            &self.config.credentials_path,
            provider_api_key_env_vars(provider).unwrap_or_default(),
            "configure this provider's API key in Sinomo settings",
        )
    }
    pub fn save_api_key(&self, key: &str, source: &str) -> Result<(), LanguageModelError> {
        save_api_key_credential(
            self.config.api.provider(),
            &self.config.credentials_path,
            key,
            source,
        )
    }
    pub fn list_models(&self) -> Result<Vec<String>, LanguageModelError> {
        catalog::list_models(
            &self.http,
            &catalog::endpoint(self.base(), "models"),
            openai::openai_headers(
                &self.api_key()?,
                self.config.api.provider(),
                &self.config.default_model,
            )?,
            catalog::CatalogProtocol::OpenAi,
            self.config.api.provider(),
        )
    }
    fn base(&self) -> &str {
        self.config
            .api_base_url
            .trim_end_matches('/')
            .trim_end_matches("/chat/completions")
            .trim_end_matches("/responses")
    }
    pub fn test_connection(&self) -> Result<(), LanguageModelError> {
        self.list_models().map(|_| ())
    }
    fn error(&self, model: &str, detail: impl ToString) -> LanguageModelError {
        LanguageModelError::ProviderInvocationFailed {
            provider: self.config.api.provider().into(),
            model: model.into(),
            detail: detail.to_string(),
        }
    }
    fn payload(
        &self,
        request: &LanguageModelRequest,
        model: &str,
    ) -> Result<Value, LanguageModelError> {
        use ProviderReasoningEffort::*;
        let mut request = request.clone();
        request.provider = self.config.api.provider().into();
        // These APIs expose Sinomo tools as JSON functions, not OpenAI's
        // provider-specific custom/freeform declarations.
        for tool in &mut request.tools {
            tool.provider_specs.clear();
        }
        let options = &self.config.options;
        if options.reasoning_budget_tokens.is_some() {
            return Err(self.error(
                model,
                "this provider uses reasoning effort, not a token budget",
            ));
        }
        let mut payload = match self.config.api {
            VendorApi::Grok => {
                let mut payload = openai::openai_responses_payload(
                    &request,
                    model,
                    &LanguageModelProviderOptions::default(),
                );
                if let Some(effort) = options.reasoning_effort {
                    let effort = match effort {
                        None => return Err(self.error(model, "Grok reasoning cannot be disabled")),
                        Minimal | Low => "low",
                        Medium => "medium",
                        High => "high",
                        _ => "xhigh",
                    };
                    if !model.starts_with("grok-4.7")
                        && !model.starts_with("grok-4.6")
                        && !model.starts_with("grok-4.5")
                    {
                        return Err(self.error(model, "reasoning effort is not supported for this Grok model; clear the override"));
                    }
                    payload["reasoning"] = json!({"effort":if model.starts_with("grok-4.5") && effort == "xhigh" {"high"} else {effort}});
                }
                payload["include"] = json!(["reasoning.encrypted_content"]);
                if let Some(session) = request.metadata["session_id"].as_str() {
                    payload["prompt_cache_key"] = json!(session);
                }
                payload
            }
            VendorApi::Kimi => {
                let mut payload = chat_completions::payload(&request, model, KIMI_PROVIDER_ID);
                if model.starts_with("kimi-k3") {
                    if options.reasoning_effort == Some(None) {
                        return Err(self.error(model, "Kimi K3 thinking cannot be disabled"));
                    }
                    if let Some(effort) = options.reasoning_effort {
                        payload["reasoning_effort"] = json!(match effort {
                            Minimal | Low => "low",
                            Max | Ultra => "max",
                            _ => "high",
                        });
                    }
                    if let Some(limit) = payload.as_object_mut().unwrap().remove("max_tokens") {
                        payload["max_completion_tokens"] = limit;
                    }
                } else if model.starts_with("kimi-k2.") {
                    let enabled = options.reasoning_effort != Some(None);
                    payload["thinking"] = json!({"type":if enabled {"enabled"} else {"disabled"}});
                } else if options.reasoning_effort.is_some() {
                    return Err(self.error(
                        model,
                        "reasoning effort is unsupported for this Kimi model; clear the override",
                    ));
                }
                payload
            }
        };
        apply_provider_vendor_options_to_payload(&mut payload, options);
        if self.config.api == VendorApi::Kimi
            && (model.starts_with("kimi-k3") || model.starts_with("kimi-k2."))
        {
            if model.starts_with("kimi-k2.7")
                && payload.pointer("/thinking/type").and_then(Value::as_str) == Some("disabled")
            {
                return Err(self.error(model, "Kimi K2.7 Code thinking cannot be disabled"));
            }

            for key in [
                "temperature",
                "top_p",
                "n",
                "presence_penalty",
                "frequency_penalty",
            ] {
                payload.as_object_mut().unwrap().remove(key);
            }
            if model.starts_with("kimi-k3") {
                if payload.pointer("/thinking/type").and_then(Value::as_str) == Some("disabled") {
                    return Err(self.error(model, "Kimi K3 thinking cannot be disabled"));
                }
                if payload.get("reasoning_effort").is_some()
                    && !matches!(
                        payload["reasoning_effort"].as_str(),
                        Some("low" | "high" | "max")
                    )
                {
                    return Err(self.error(model, "Kimi K3 effort must be low, high, or max"));
                }
            } else {
                if !matches!(
                    payload["thinking"]["type"].as_str(),
                    Some("enabled" | "disabled")
                ) {
                    return Err(self.error(model, "Kimi thinking must be enabled or disabled"));
                }
                if payload["thinking"]["type"] == "enabled"
                    && payload.get("tool_choice").is_some()
                    && !matches!(payload["tool_choice"].as_str(), Some("auto" | "none"))
                {
                    return Err(
                        self.error(model, "Kimi K2 thinking requires auto or none tool choice")
                    );
                }
            }
        }
        if self.config.api == VendorApi::Grok {
            for key in ["presence_penalty", "frequency_penalty", "stop"] {
                payload.as_object_mut().unwrap().remove(key);
            }
            if payload.pointer("/reasoning/effort").is_some()
                && !matches!(
                    payload["reasoning"]["effort"].as_str(),
                    Some("low" | "medium" | "high" | "xhigh")
                )
            {
                return Err(self.error(model, "Grok effort must be low, medium, high, or xhigh"));
            }
        }
        Ok(payload)
    }
    fn send(
        &self,
        request: &LanguageModelRequest,
        stream: bool,
    ) -> Result<(reqwest::blocking::Response, String), LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let wire_request = request_with_wire_names(request);
        let mut payload = self.payload(&wire_request, &model)?;
        payload["stream"] = json!(stream);
        let path = match self.config.api {
            VendorApi::Grok => "responses",
            VendorApi::Kimi => "chat/completions",
        };
        let mut headers =
            openai::openai_headers(&self.api_key()?, self.config.api.provider(), &model)?;
        if stream {
            headers.insert(
                reqwest::header::ACCEPT,
                reqwest::header::HeaderValue::from_static("text/event-stream"),
            );
        }
        let response = self
            .http
            .post(catalog::endpoint(self.base(), path))
            .headers(headers)
            .json(&payload)
            .send()
            .map_err(|e| self.error(&model, e))?;
        if !response.status().is_success() {
            let status = response.status();
            return Err(self.error(
                &model,
                format!("HTTP {status}: {}", response.text().unwrap_or_default()),
            ));
        }
        Ok((response, model))
    }
}
impl LanguageModelProvider for VendorApiProvider {
    fn available_models(&self) -> Result<Vec<String>, LanguageModelError> {
        self.list_models()
    }
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: self.config.api.provider().into(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::NetworkOnly,
            capabilities: LanguageModelProviderCapabilities::default(),
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let (response, model) = self.send(request, false)?;
        let value: Value = response.json().map_err(|e| self.error(&model, e))?;
        if let Some(error) = value.get("error").filter(|e| !e.is_null()) {
            return Err(self.error(&model, error));
        }
        let provider = self.config.api.provider();
        if self.config.api == VendorApi::Kimi {
            if !value
                .pointer("/choices/0/message")
                .is_some_and(Value::is_object)
            {
                return Err(self.error(&model, "Kimi response is missing its assistant message"));
            }
            return Ok(restore_tool_names(
                chat_completions::response(&value, provider, &model),
                request,
            ));
        }
        if !value.get("output").is_some_and(Value::is_array) {
            return Err(self.error(&model, "Grok response is missing its output items"));
        }
        let tool_calls = openai::extract_openai_tool_calls(&value);
        Ok(restore_tool_names(
            LanguageModelResponse {
                provider: provider.into(),
                content: openai::extract_openai_response_text(&value),
                items: openai::openai_response_items(&value, provider, &model, &tool_calls),
                tool_calls,
                usage: openai::extract_openai_usage(&value),
                model,
            },
            request,
        ))
    }
    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let (response, model) = self.send(request, true)?;
        let provider = self.config.api.provider();
        on_event(LanguageModelStreamEvent::Started {
            provider: provider.into(),
            model: model.clone(),
        });
        let mut emit = |mut event: LanguageModelStreamEvent| {
            match &mut event {
                LanguageModelStreamEvent::ToolCallStarted { name, .. }
                | LanguageModelStreamEvent::ToolCallCompleted { name, .. } => {
                    restore_name(name, request)
                }
                LanguageModelStreamEvent::ToolCallDelta {
                    name: Some(name), ..
                } => restore_name(name, request),
                _ => {}
            }
            on_event(event);
        };
        let response = match self.config.api {
            VendorApi::Grok => {
                openai::stream_openai_responses_response(response, provider, &model, &mut emit)
            }
            VendorApi::Kimi => {
                stream_openai_chat_completion_response(response, provider, &model, &mut emit)
            }
        }?;
        Ok(restore_tool_names(response, request))
    }
}

// Both APIs use flat function names. Keep qualified/long runtime names out of
// the wire while retaining their identity for registry dispatch. Reserve the
// alias prefix so it cannot collide with an ordinary declared tool name.
fn wire_tool_name(name: &str) -> String {
    if !name.is_empty()
        && name.len() <= 64
        && !name.starts_with("sinomo_")
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return name.into();
    }
    use std::fmt::Write;
    let digest = ring::digest::digest(&ring::digest::SHA256, name.as_bytes());
    let mut alias = String::from("sinomo_");
    for byte in &digest.as_ref()[..28] {
        write!(&mut alias, "{byte:02x}").unwrap();
    }
    alias
}

fn request_with_wire_names(request: &LanguageModelRequest) -> LanguageModelRequest {
    let mut request = request.clone();
    for tool in &mut request.tools {
        tool.name = wire_tool_name(&tool.name);
    }
    if let Some(LanguageModelToolChoice::Tool { name }) = &mut request.tool_choice {
        *name = wire_tool_name(name);
    }
    for item in &mut request.items {
        match item {
            LanguageModelRequestItem::ToolCall { name, .. }
            | LanguageModelRequestItem::ToolCallOutput { name, .. } => *name = wire_tool_name(name),
            // Opaque continuation items already contain their original wire names.
            _ => {}
        }
    }
    request
}

fn restore_name(name: &mut String, request: &LanguageModelRequest) {
    if let Some(tool) = request
        .tools
        .iter()
        .find(|tool| wire_tool_name(&tool.name) == *name)
    {
        *name = tool.name.clone();
    }
}

fn restore_tool_names(
    mut response: LanguageModelResponse,
    request: &LanguageModelRequest,
) -> LanguageModelResponse {
    for call in &mut response.tool_calls {
        restore_name(&mut call.name, request);
    }
    for item in &mut response.items {
        if let LanguageModelResponseItem::ToolCall { call } = item {
            restore_name(&mut call.name, request);
        }
    }
    response
}
