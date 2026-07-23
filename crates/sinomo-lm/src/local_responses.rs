use crate::openai::{
    extract_openai_response_text, extract_openai_tool_calls, extract_openai_usage, openai_headers,
    openai_responses_payload, stream_openai_responses_response,
};
use crate::{
    LanguageModelError, LanguageModelHostAccess, LanguageModelInvocation, LanguageModelProvider,
    LanguageModelProviderCapabilities, LanguageModelProviderInfo, LanguageModelProviderOptions,
    LanguageModelRequest, LanguageModelResponse, LanguageModelResponseItem,
    LanguageModelStreamEvent, selected_model,
};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, HeaderValue};
use serde::{Deserialize, Serialize};

pub const OLLAMA_PROVIDER_ID: &str = "ollama";
pub const LM_STUDIO_PROVIDER_ID: &str = "lmstudio";
pub const OLLAMA_DEFAULT_API_BASE_URL: &str = "http://127.0.0.1:11434/v1";
pub const LM_STUDIO_DEFAULT_API_BASE_URL: &str = "http://127.0.0.1:1234/v1";
pub const OLLAMA_DEFAULT_MODEL: &str = "gpt-oss:20b";
pub const LM_STUDIO_DEFAULT_MODEL: &str = "openai/gpt-oss-20b";

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum LocalResponsesRuntime {
    Ollama,
    LmStudio,
}

impl LocalResponsesRuntime {
    pub const fn provider_id(self) -> &'static str {
        match self {
            Self::Ollama => OLLAMA_PROVIDER_ID,
            Self::LmStudio => LM_STUDIO_PROVIDER_ID,
        }
    }

    pub const fn default_api_base_url(self) -> &'static str {
        match self {
            Self::Ollama => OLLAMA_DEFAULT_API_BASE_URL,
            Self::LmStudio => LM_STUDIO_DEFAULT_API_BASE_URL,
        }
    }

    pub const fn default_model(self) -> &'static str {
        match self {
            Self::Ollama => OLLAMA_DEFAULT_MODEL,
            Self::LmStudio => LM_STUDIO_DEFAULT_MODEL,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalResponsesConfig {
    pub runtime: LocalResponsesRuntime,
    pub api_base_url: String,
    pub default_model: String,
    pub options: LanguageModelProviderOptions,
}

impl LocalResponsesConfig {
    pub fn new(runtime: LocalResponsesRuntime) -> Self {
        Self {
            runtime,
            api_base_url: runtime.default_api_base_url().to_string(),
            default_model: runtime.default_model().to_string(),
            options: LanguageModelProviderOptions::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalResponsesProvider {
    config: LocalResponsesConfig,
    http: Client,
}

impl LocalResponsesProvider {
    pub fn new(config: LocalResponsesConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn runtime(&self) -> LocalResponsesRuntime {
        self.config.runtime
    }

    pub fn list_models(&self) -> Result<Vec<String>, LanguageModelError> {
        let url = match self.config.runtime {
            LocalResponsesRuntime::Ollama => {
                format!("{}/api/tags", self.host_root().trim_end_matches('/'))
            }
            LocalResponsesRuntime::LmStudio => {
                format!("{}/models", self.config.api_base_url.trim_end_matches('/'))
            }
        };
        let response = self
            .http
            .get(url)
            .send()
            .map_err(|source| self.error(source))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(self.error(format!("model discovery failed: HTTP {status}: {body}")));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| self.error(source))?;
        let mut models = match self.config.runtime {
            LocalResponsesRuntime::Ollama => value
                .get("models")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|model| model.get("name").and_then(serde_json::Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>(),
            LocalResponsesRuntime::LmStudio => value
                .get("data")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|model| model.get("id").and_then(serde_json::Value::as_str))
                .map(str::to_string)
                .collect::<Vec<_>>(),
        };
        models.sort();
        models.dedup();
        Ok(models)
    }

    pub fn pull_model(&self, model: &str) -> Result<(), LanguageModelError> {
        if self.config.runtime != LocalResponsesRuntime::Ollama {
            return Err(self.error(
                "LM Studio model downloads require its `lms get` client; Sinomo will not spawn it implicitly",
            ));
        }
        let model = non_empty_model(model).map_err(|detail| self.error(detail))?;
        let url = format!("{}/api/pull", self.host_root().trim_end_matches('/'));
        let response = self
            .http
            .post(url)
            .json(&serde_json::json!({ "model": model, "stream": false }))
            .send()
            .map_err(|source| self.error(source))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(self.error(format!("model pull failed: HTTP {status}: {body}")));
        }
        Ok(())
    }

    pub fn load_model(&self, model: &str) -> Result<(), LanguageModelError> {
        let model = non_empty_model(model).map_err(|detail| self.error(detail))?;
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(openai_headers(
                "local-runtime",
                self.config.runtime.provider_id(),
                model,
            )?)
            .json(&serde_json::json!({
                "model": model,
                "input": "",
                "max_output_tokens": 1,
                "store": false,
            }))
            .send()
            .map_err(|source| self.error(source))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(self.error(format!("model load failed: HTTP {status}: {body}")));
        }
        Ok(())
    }

    fn host_root(&self) -> &str {
        self.config
            .api_base_url
            .strip_suffix("/v1")
            .unwrap_or(self.config.api_base_url.as_str())
    }

    fn error(&self, detail: impl std::fmt::Display) -> LanguageModelError {
        LanguageModelError::ProviderInvocationFailed {
            provider: self.config.runtime.provider_id().to_string(),
            model: self.config.default_model.clone(),
            detail: detail.to_string(),
        }
    }
}

impl LanguageModelProvider for LocalResponsesProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: self.config.runtime.provider_id().to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: LanguageModelHostAccess::Bounded,
            capabilities: LanguageModelProviderCapabilities::local_responses(),
            supports_json_object: true,
            supports_tool_permissions: false,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let provider = self.config.runtime.provider_id();
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(openai_headers("local-runtime", provider, &model)?)
            .json(&openai_responses_payload(
                request,
                &model,
                &self.config.options,
            ))
            .send()
            .map_err(|source| self.error(source))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(self.error(format!("Responses API failed: HTTP {status}: {body}")));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| self.error(source))?;
        let tool_calls = extract_openai_tool_calls(&value);
        Ok(LanguageModelResponse {
            provider: provider.to_string(),
            model,
            content: extract_openai_response_text(&value),
            items: tool_calls
                .iter()
                .cloned()
                .map(|call| LanguageModelResponseItem::ToolCall { call })
                .collect(),
            tool_calls,
            usage: extract_openai_usage(&value),
        })
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let provider = self.config.runtime.provider_id();
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let mut headers = openai_headers("local-runtime", provider, &model)?;
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        let mut payload = openai_responses_payload(request, &model, &self.config.options);
        payload["stream"] = serde_json::json!(true);
        let response = self
            .http
            .post(url)
            .headers(headers)
            .json(&payload)
            .send()
            .map_err(|source| self.error(source))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(self.error(format!("Responses API failed: HTTP {status}: {body}")));
        }
        on_event(LanguageModelStreamEvent::Started {
            provider: provider.to_string(),
            model: model.clone(),
        });
        stream_openai_responses_response(response, provider, &model, on_event)
    }
}

fn non_empty_model(model: &str) -> Result<&str, &'static str> {
    let model = model.trim();
    if model.is_empty() {
        Err("model name must not be empty")
    } else {
        Ok(model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_runtime_defaults_and_capabilities_are_provider_specific() {
        let ollama =
            LocalResponsesProvider::new(LocalResponsesConfig::new(LocalResponsesRuntime::Ollama));
        let lm_studio =
            LocalResponsesProvider::new(LocalResponsesConfig::new(LocalResponsesRuntime::LmStudio));
        assert_eq!(ollama.info().provider, OLLAMA_PROVIDER_ID);
        assert_eq!(ollama.info().default_model, OLLAMA_DEFAULT_MODEL);
        assert_eq!(lm_studio.info().provider, LM_STUDIO_PROVIDER_ID);
        assert_eq!(lm_studio.info().default_model, LM_STUDIO_DEFAULT_MODEL);
        assert_eq!(ollama.info().host_access, LanguageModelHostAccess::Bounded);
        assert!(ollama.info().capabilities.responses_http);
        assert!(ollama.info().capabilities.local_model_management);
        assert!(!ollama.info().capabilities.web_search);
    }

    #[test]
    fn host_root_removes_only_the_openai_v1_suffix() {
        let mut config = LocalResponsesConfig::new(LocalResponsesRuntime::Ollama);
        config.api_base_url = "http://127.0.0.1:11434/v1".to_string();
        assert_eq!(
            LocalResponsesProvider::new(config).host_root(),
            "http://127.0.0.1:11434"
        );
    }
}
