use crate::api_key::{default_api_key_credentials_path, resolve_api_key, save_api_key_credential};
use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelRequest, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelUsage, PROVIDER_DEFAULT_MODEL,
    render_plain_prompt, selected_model,
};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeminiConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub credentials_path: Option<PathBuf>,
}

impl Default for GeminiConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://generativelanguage.googleapis.com/v1beta".to_string(),
            default_model: "gemini-2.5-flash".to_string(),
            credentials_path: default_api_key_credentials_path(
                "SINOMO_GEMINI_CREDENTIALS_FILE",
                GeminiProvider::PROVIDER_ID,
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GeminiProvider {
    config: GeminiConfig,
    http: Client,
}

impl GeminiProvider {
    pub const PROVIDER_ID: &'static str = "gemini";

    pub fn new(config: GeminiConfig) -> Self {
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
            &self.config.credentials_path,
            &[
                "SINOMO_GEMINI_API_KEY",
                "GEMINI_API_KEY",
                "GOOGLE_API_KEY",
                "GOOGLE_AI_API_KEY",
            ],
            "no Gemini API key found; set GEMINI_API_KEY or run `sinomo providers auth gemini`",
        )
    }

    pub fn save_api_key(&self, api_key: &str, source: &str) -> Result<(), LanguageModelError> {
        save_api_key_credential(&self.config.credentials_path, api_key, source)
    }

    pub fn test_connection(&self) -> Result<(), LanguageModelError> {
        let request = LanguageModelRequest {
            provider: Self::PROVIDER_ID.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            messages: vec![LanguageModelMessage {
                role: LanguageModelRole::User,
                content: "Reply with ok.".to_string(),
            }],
            temperature: None,
            max_output_tokens: Some(8),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        };
        self.complete(&request).map(|_| ())
    }
}

impl Default for GeminiProvider {
    fn default() -> Self {
        Self::new(GeminiConfig::default())
    }
}

impl LanguageModelProvider for GeminiProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
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
            "{}/{}:generateContent",
            self.config.api_base_url.trim_end_matches('/'),
            gemini_model_path(&model),
        );
        let response = self
            .http
            .post(url)
            .headers(gemini_headers(&api_key, Self::PROVIDER_ID, &model)?)
            .json(&gemini_generate_content_payload(request, &model))
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
                detail: format!("Gemini GenerateContent API failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        let content = extract_gemini_response_text(&value);
        let usage = extract_gemini_usage(&value);

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content,
            usage,
        })
    }
}

fn gemini_headers(
    api_key: &str,
    provider: &str,
    model: &str,
) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert(
        "x-goog-api-key",
        HeaderValue::from_str(api_key).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

fn gemini_model_path(model: &str) -> String {
    if model.starts_with("models/") {
        model.to_string()
    } else {
        format!("models/{model}")
    }
}

pub(crate) fn gemini_generate_content_payload(
    request: &LanguageModelRequest,
    _model: &str,
) -> serde_json::Value {
    let system = request
        .messages
        .iter()
        .filter(|message| message.role == LanguageModelRole::System)
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let mut contents: Vec<serde_json::Value> = request
        .messages
        .iter()
        .filter(|message| message.role != LanguageModelRole::System)
        .map(|message| {
            serde_json::json!({
                "role": match message.role {
                    LanguageModelRole::Assistant => "model",
                    _ => "user",
                },
                "parts": [{ "text": message.content }],
            })
        })
        .collect();
    if contents.is_empty() {
        contents.push(serde_json::json!({
            "role": "user",
            "parts": [{ "text": render_plain_prompt(request) }],
        }));
    }

    let mut payload = serde_json::json!({
        "contents": contents,
    });
    if !system.is_empty() {
        payload["systemInstruction"] = serde_json::json!({
            "parts": [{ "text": system }],
        });
    }

    let mut generation_config = serde_json::Map::new();
    if let Some(temperature) = request.temperature {
        generation_config.insert("temperature".to_string(), serde_json::json!(temperature));
    }
    if let Some(max_tokens) = request.max_output_tokens {
        generation_config.insert("maxOutputTokens".to_string(), serde_json::json!(max_tokens));
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        generation_config.insert(
            "responseMimeType".to_string(),
            serde_json::json!("application/json"),
        );
    }
    if !generation_config.is_empty() {
        payload["generationConfig"] = serde_json::Value::Object(generation_config);
    }

    payload
}

pub(crate) fn extract_gemini_response_text(value: &serde_json::Value) -> String {
    value
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .map(|candidates| {
            candidates
                .iter()
                .flat_map(|candidate| {
                    candidate
                        .get("content")
                        .and_then(|content| content.get("parts"))
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                })
                .filter_map(|part| part.get("text").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

pub(crate) fn extract_gemini_usage(value: &serde_json::Value) -> Option<LanguageModelUsage> {
    value.get("usageMetadata").map(|usage| LanguageModelUsage {
        input_tokens: usage
            .get("promptTokenCount")
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("candidatesTokenCount")
            .and_then(serde_json::Value::as_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_path_accepts_prefixed_or_bare_model_names() {
        assert_eq!(
            gemini_model_path("gemini-2.5-flash"),
            "models/gemini-2.5-flash"
        );
        assert_eq!(
            gemini_model_path("models/gemini-2.5-pro"),
            "models/gemini-2.5-pro"
        );
    }
}
