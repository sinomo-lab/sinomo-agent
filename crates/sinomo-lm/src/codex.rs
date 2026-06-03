use crate::{
    LanguageModelCreditUsage, LanguageModelError, LanguageModelInvocation, LanguageModelMessage,
    LanguageModelProvider, LanguageModelProviderInfo, LanguageModelProviderOptions,
    LanguageModelProviderUsage, LanguageModelRequest, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelStreamEvent, LanguageModelUsage,
    LanguageModelUsageLimit, LanguageModelUsageProvider, PROVIDER_DEFAULT_MODEL,
    apply_provider_vendor_options_to_payload, read_provider_credentials_json_value, selected_model,
    unix_timestamp, write_provider_credentials_json_value,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub const CODEX_DEFAULT_MODEL: &str = "gpt-5.5";
pub const CODEX_PROVIDER_ID: &str = "codex";
const LEGACY_CODEX_CHATGPT_DEFAULT_MODEL: &str = "gpt-5.2-codex";
const CODEX_DEFAULT_INSTRUCTIONS: &str =
    "You are Sinomo's Codex model provider. Answer the user's request clearly and accurately.";
pub const SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR: &str = "SINOMO_CODEX_CREDENTIALS_FILE";
pub const CODEX_AUTH_ISSUER: &str = "https://auth.openai.com";
pub const CODEX_AUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const CODEX_CHATGPT_API_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
// Match the open-source Codex workspace version used by its models manager.
pub const CODEX_MODELS_CLIENT_VERSION: &str = "0.0.0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexConfig {
    pub provider_id: String,
    pub default_model: String,
    pub max_iterations: usize,
    pub options: LanguageModelProviderOptions,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            provider_id: CODEX_PROVIDER_ID.to_string(),
            default_model: CODEX_DEFAULT_MODEL.to_string(),
            max_iterations: 12,
            options: LanguageModelProviderOptions::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiResponsesCodexConfig {
    pub api_base_url: String,
    pub auth_issuer: String,
    pub client_id: String,
    pub client_version: String,
    pub default_model: String,
    pub credentials_path: Option<PathBuf>,
}

impl Default for OpenAiResponsesCodexConfig {
    fn default() -> Self {
        Self {
            api_base_url: CODEX_CHATGPT_API_BASE_URL.to_string(),
            auth_issuer: CODEX_AUTH_ISSUER.to_string(),
            client_id: CODEX_AUTH_CLIENT_ID.to_string(),
            client_version: CODEX_MODELS_CLIENT_VERSION.to_string(),
            default_model: CODEX_DEFAULT_MODEL.to_string(),
            credentials_path: default_codex_credentials_path(),
        }
    }
}

fn default_codex_credentials_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var(SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(PathBuf::from(path));
    }
    sinomo_runtime::local::SinomoDataDir::from_env_or_home()
        .ok()
        .map(|dir| dir.provider_credentials_file(CODEX_PROVIDER_ID))
}

fn missing_codex_chatgpt_auth() -> LanguageModelError {
    LanguageModelError::AuthenticationRequired {
        provider: CODEX_PROVIDER_ID.to_string(),
        detail: "no Sinomo Codex ChatGPT sign-in found; run `sinomo providers auth codex`"
            .to_string(),
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::openai::inferred_context_window_tokens(model)
}

fn read_codex_chatgpt_tokens(
    credentials_path: &PathBuf,
) -> Result<Option<CodexChatGptTokens>, LanguageModelError> {
    let Some(value) =
        read_provider_credentials_json_value(CODEX_PROVIDER_ID, &Some(credentials_path.clone()))?
    else {
        return Ok(None);
    };
    let auth = serde_json::from_value::<CodexAuthDotJson>(value).map_err(|source| {
        LanguageModelError::TokenStore {
            path: credentials_path.display().to_string(),
            detail: source.to_string(),
        }
    })?;
    if auth.auth_mode.as_deref() == Some("apikey") || auth.openai_api_key.is_some() {
        return Err(LanguageModelError::AuthenticationRequired {
            provider: CODEX_PROVIDER_ID.to_string(),
            detail: "Codex now uses Sign in with ChatGPT for subscription access; run `sinomo providers auth codex` to replace the old API-key credential".to_string(),
        });
    }
    Ok(auth.tokens.filter(CodexChatGptTokens::has_access_token))
}

fn save_codex_chatgpt_tokens(
    credentials_path: &Option<PathBuf>,
    tokens: CodexChatGptTokens,
) -> Result<(), LanguageModelError> {
    let Some(credentials_path) = credentials_path else {
        return Err(LanguageModelError::AuthenticationRequired {
            provider: CODEX_PROVIDER_ID.to_string(),
            detail: "could not resolve a Sinomo Codex credentials path; set SINOMO_DATA_DIR or SINOMO_CODEX_CREDENTIALS_FILE".to_string(),
        });
    };
    let last_refresh = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|source| LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        })?;
    let auth = CodexAuthDotJson {
        auth_mode: Some("chatgpt".to_string()),
        openai_api_key: None,
        tokens: Some(tokens),
        last_refresh: Some(last_refresh),
        agent_identity: None,
    };
    let value =
        serde_json::to_value(auth).map_err(|source| LanguageModelError::RequestSerialization {
            detail: source.to_string(),
        })?;
    write_provider_credentials_json_value(CODEX_PROVIDER_ID, &Some(credentials_path.clone()), value)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CodexAuthDotJson {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_mode: Option<String>,
    #[serde(
        default,
        rename = "OPENAI_API_KEY",
        skip_serializing_if = "Option::is_none"
    )]
    openai_api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tokens: Option<CodexChatGptTokens>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_refresh: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_identity: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexChatGptTokens {
    pub id_token: String,
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub chatgpt_account_is_fedramp: bool,
}

impl CodexChatGptTokens {
    fn has_access_token(&self) -> bool {
        !self.access_token.trim().is_empty()
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexDeviceCode {
    pub verification_uri: String,
    pub user_code: String,
    pub device_auth_id: String,
    pub interval: u64,
    pub expires_in: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexDevicePoll {
    pub tokens: Option<CodexChatGptTokens>,
    pub pending: bool,
}

#[derive(Debug, Deserialize)]
struct CodexTokenResponse {
    id_token: String,
    access_token: String,
    refresh_token: String,
}

#[derive(Debug, Deserialize)]
struct CodexRefreshResponse {
    id_token: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
}

fn codex_auth_invocation_error(detail: String) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: CODEX_PROVIDER_ID.to_string(),
        model: CODEX_DEFAULT_MODEL.to_string(),
        detail,
    }
}

fn json_u64_or_string(value: Option<&serde_json::Value>) -> Option<u64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_u64(),
        Some(serde_json::Value::String(text)) => text.trim().parse().ok(),
        _ => None,
    }
}

fn json_string(value: &serde_json::Value, key: &str) -> Result<String, LanguageModelError> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            codex_auth_invocation_error(format!(
                "Codex device authorization response missed `{key}`"
            ))
        })
}

fn tokens_from_response(
    id_token: String,
    access_token: String,
    refresh_token: String,
    previous_account_id: Option<String>,
) -> CodexChatGptTokens {
    let claims = jwt_auth_claims(&id_token);
    let auth_claims = claims
        .get("https://api.openai.com/auth")
        .and_then(serde_json::Value::as_object);
    let account_id = auth_claims
        .and_then(|claims| claims.get("chatgpt_account_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or(previous_account_id);
    let chatgpt_account_is_fedramp = auth_claims
        .and_then(|claims| claims.get("chatgpt_account_is_fedramp"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    CodexChatGptTokens {
        id_token,
        access_token,
        refresh_token,
        account_id,
        chatgpt_account_is_fedramp,
    }
}

fn jwt_auth_claims(jwt: &str) -> serde_json::Value {
    let Some(payload) = jwt.split('.').nth(1) else {
        return serde_json::Value::Null;
    };
    URL_SAFE_NO_PAD
        .decode(payload)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(serde_json::Value::Null)
}

#[derive(Clone, Debug)]
pub struct OpenAiResponsesCodexClient {
    config: OpenAiResponsesCodexConfig,
    http: Client,
}

impl OpenAiResponsesCodexClient {
    pub fn new(config: OpenAiResponsesCodexConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn chatgpt_tokens(&self) -> Result<CodexChatGptTokens, LanguageModelError> {
        let Some(credentials_path) = &self.config.credentials_path else {
            return Err(missing_codex_chatgpt_auth());
        };
        read_codex_chatgpt_tokens(credentials_path)?.ok_or_else(missing_codex_chatgpt_auth)
    }

    pub fn save_chatgpt_tokens(
        &self,
        tokens: CodexChatGptTokens,
    ) -> Result<(), LanguageModelError> {
        save_codex_chatgpt_tokens(&self.config.credentials_path, tokens)
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.config.credentials_path.clone()
    }

    pub fn list_models(&self) -> Result<Vec<String>, LanguageModelError> {
        let mut tokens = self.chatgpt_tokens()?;
        let mut response = self.get_models(&tokens)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            tokens = self.refresh_chatgpt_tokens()?;
            response = self.get_models(&tokens)?;
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: format!("Codex backend models API failed: HTTP {status}: {body}"),
            });
        }

        let body =
            response
                .text()
                .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                    provider: CODEX_PROVIDER_ID.to_string(),
                    model: CODEX_DEFAULT_MODEL.to_string(),
                    detail: source.to_string(),
                })?;
        let value = serde_json::from_str::<serde_json::Value>(&body).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: format!("failed to decode Codex models response: {source}; body: {body}"),
            }
        })?;
        let models = extract_codex_model_ids(&value);
        if models.is_empty() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: "Codex backend models API returned no picker-visible models".to_string(),
            });
        }
        Ok(models)
    }

    pub fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError> {
        let mut tokens = self.chatgpt_tokens()?;
        let mut response = self.get_usage(&tokens)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            tokens = self.refresh_chatgpt_tokens()?;
            response = self.get_usage(&tokens)?;
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: format!("Codex backend usage API failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: source.to_string(),
            }
        })?;
        Ok(codex_usage_from_value(&value, unix_timestamp()))
    }

    pub fn device_login_start(&self) -> Result<CodexDeviceCode, LanguageModelError> {
        let issuer = self.config.auth_issuer.trim_end_matches('/');
        let response = self
            .http
            .post(format!("{issuer}/api/accounts/deviceauth/usercode"))
            .json(&serde_json::json!({ "client_id": self.config.client_id.as_str() }))
            .send()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(codex_auth_invocation_error(format!(
                "Codex device code request failed: HTTP {status}: {body}"
            )));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        let device_auth_id = value
            .get("device_auth_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let user_code = value
            .get("user_code")
            .or_else(|| value.get("usercode"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        if device_auth_id.is_empty() || user_code.is_empty() {
            return Err(codex_auth_invocation_error(
                "Codex device code response was missing a device id or user code".to_string(),
            ));
        }
        Ok(CodexDeviceCode {
            verification_uri: format!("{issuer}/codex/device"),
            user_code,
            device_auth_id,
            interval: json_u64_or_string(value.get("interval")).unwrap_or(5),
            expires_in: 15 * 60,
        })
    }

    pub fn device_login_poll(
        &self,
        device: &CodexDeviceCode,
    ) -> Result<CodexDevicePoll, LanguageModelError> {
        let issuer = self.config.auth_issuer.trim_end_matches('/');
        let response = self
            .http
            .post(format!("{issuer}/api/accounts/deviceauth/token"))
            .json(&serde_json::json!({
                "device_auth_id": device.device_auth_id.as_str(),
                "user_code": device.user_code.as_str(),
            }))
            .send()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        let status = response.status();
        if status == reqwest::StatusCode::FORBIDDEN || status == reqwest::StatusCode::NOT_FOUND {
            return Ok(CodexDevicePoll {
                tokens: None,
                pending: true,
            });
        }
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(codex_auth_invocation_error(format!(
                "Codex device authorization failed: HTTP {status}: {body}"
            )));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        let authorization_code = json_string(&value, "authorization_code")?;
        let code_verifier = json_string(&value, "code_verifier")?;
        let tokens =
            self.exchange_device_authorization_code(&authorization_code, &code_verifier)?;
        self.save_chatgpt_tokens(tokens.clone())?;
        Ok(CodexDevicePoll {
            tokens: Some(tokens),
            pending: false,
        })
    }

    fn exchange_device_authorization_code(
        &self,
        authorization_code: &str,
        code_verifier: &str,
    ) -> Result<CodexChatGptTokens, LanguageModelError> {
        let issuer = self.config.auth_issuer.trim_end_matches('/');
        let redirect_uri = format!("{issuer}/deviceauth/callback");
        let response = self
            .http
            .post(format!("{issuer}/oauth/token"))
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", authorization_code),
                ("redirect_uri", redirect_uri.as_str()),
                ("client_id", self.config.client_id.as_str()),
                ("code_verifier", code_verifier),
            ])
            .send()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(codex_auth_invocation_error(format!(
                "Codex token exchange failed: HTTP {status}: {body}"
            )));
        }
        let exchanged = response
            .json::<CodexTokenResponse>()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        Ok(tokens_from_response(
            exchanged.id_token,
            exchanged.access_token,
            exchanged.refresh_token,
            None,
        ))
    }

    pub fn refresh_chatgpt_tokens(&self) -> Result<CodexChatGptTokens, LanguageModelError> {
        let current = self.chatgpt_tokens()?;
        if current.refresh_token.trim().is_empty() {
            return Err(LanguageModelError::AuthenticationRequired {
                provider: CODEX_PROVIDER_ID.to_string(),
                detail: "Codex ChatGPT auth is missing a refresh token; run `sinomo providers auth codex` again".to_string(),
            });
        }
        let issuer = self.config.auth_issuer.trim_end_matches('/');
        let response = self
            .http
            .post(format!("{issuer}/oauth/token"))
            .json(&serde_json::json!({
                "client_id": self.config.client_id.as_str(),
                "grant_type": "refresh_token",
                "refresh_token": current.refresh_token.clone(),
            }))
            .send()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::AuthenticationRequired {
                provider: CODEX_PROVIDER_ID.to_string(),
                detail: format!("Codex ChatGPT token refresh failed: HTTP {status}: {body}"),
            });
        }
        let refreshed = response
            .json::<CodexRefreshResponse>()
            .map_err(|source| codex_auth_invocation_error(source.to_string()))?;
        let tokens = tokens_from_response(
            refreshed.id_token.unwrap_or(current.id_token),
            refreshed.access_token.unwrap_or(current.access_token),
            refreshed.refresh_token.unwrap_or(current.refresh_token),
            current.account_id,
        );
        self.save_chatgpt_tokens(tokens.clone())?;
        Ok(tokens)
    }
}

impl Default for OpenAiResponsesCodexClient {
    fn default() -> Self {
        Self::new(OpenAiResponsesCodexConfig::default())
    }
}

impl CodexClient for OpenAiResponsesCodexClient {
    fn complete(
        &self,
        request: &CodexModelRequest,
    ) -> Result<CodexModelResponse, LanguageModelError> {
        self.complete_streaming(request, &mut |_| {})
    }

    fn complete_streaming(
        &self,
        request: &CodexModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<CodexModelResponse, LanguageModelError> {
        let mut tokens = self.chatgpt_tokens()?;
        let request = request.with_chatgpt_supported_model();
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let payload = openai_responses_codex_payload(&request);
        let mut response = self.post_responses(&url, &request, &tokens, &payload)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            tokens = self.refresh_chatgpt_tokens()?;
            response = self.post_responses(&url, &request, &tokens, &payload)?;
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: request.model.clone(),
                detail: format!("Codex backend Responses API failed: HTTP {status}: {body}"),
            });
        }

        let body =
            response
                .text()
                .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                    provider: CODEX_PROVIDER_ID.to_string(),
                    model: request.model.clone(),
                    detail: source.to_string(),
                })?;

        parse_openai_responses_codex_body_with_events(&body, &request.model, on_event)
    }
}

impl OpenAiResponsesCodexClient {
    fn get_models(
        &self,
        tokens: &CodexChatGptTokens,
    ) -> Result<reqwest::blocking::Response, LanguageModelError> {
        let url = format!(
            "{}/models?client_version={}",
            self.config.api_base_url.trim_end_matches('/'),
            self.config.client_version
        );
        let response = self
            .http
            .get(url)
            .headers(codex_json_headers(
                tokens,
                CODEX_PROVIDER_ID,
                CODEX_DEFAULT_MODEL,
            )?)
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: source.to_string(),
            })?;
        Ok(response)
    }

    fn get_usage(
        &self,
        tokens: &CodexChatGptTokens,
    ) -> Result<reqwest::blocking::Response, LanguageModelError> {
        let url = format!("{}/wham/usage", self.chatgpt_backend_base_url());
        let response = self
            .http
            .get(url)
            .headers(codex_json_headers(
                tokens,
                CODEX_PROVIDER_ID,
                CODEX_DEFAULT_MODEL,
            )?)
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: source.to_string(),
            })?;
        Ok(response)
    }

    fn chatgpt_backend_base_url(&self) -> String {
        self.config
            .api_base_url
            .trim_end_matches('/')
            .strip_suffix("/codex")
            .unwrap_or_else(|| self.config.api_base_url.trim_end_matches('/'))
            .to_string()
    }

    fn post_responses(
        &self,
        url: &str,
        request: &CodexModelRequest,
        tokens: &CodexChatGptTokens,
        payload: &serde_json::Value,
    ) -> Result<reqwest::blocking::Response, LanguageModelError> {
        let response = self
            .http
            .post(url)
            .headers(codex_headers(tokens, CODEX_PROVIDER_ID, &request.model)?)
            .json(payload)
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: request.model.clone(),
                detail: source.to_string(),
            })?;
        Ok(response)
    }
}

pub trait CodexClient {
    fn complete(
        &self,
        request: &CodexModelRequest,
    ) -> Result<CodexModelResponse, LanguageModelError>;

    fn complete_streaming(
        &self,
        request: &CodexModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<CodexModelResponse, LanguageModelError> {
        let response = self.complete(request)?;
        for event in &response.stream_events {
            on_event(event.clone());
        }
        Ok(response)
    }
}

pub trait CodexTool {
    fn descriptor(&self) -> CodexToolDescriptor;
    fn execute(&self, call: &CodexToolCall) -> CodexToolResult;
}

pub trait CodexPromptBuilder {
    fn build_system_prompt(&self, request: &LanguageModelRequest) -> Option<String>;
}

pub trait CodexContextManager {
    fn prepare_messages(&self, request: &LanguageModelRequest) -> Vec<CodexMessage>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DefaultCodexPromptBuilder {
    pub extra_instructions: Vec<String>,
}

impl CodexPromptBuilder for DefaultCodexPromptBuilder {
    fn build_system_prompt(&self, request: &LanguageModelRequest) -> Option<String> {
        let mut parts: Vec<String> = request
            .messages
            .iter()
            .filter(|message| message.role == LanguageModelRole::System)
            .map(|message| message.content.clone())
            .collect();
        parts.extend(self.extra_instructions.iter().cloned());
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("\n\n"))
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DefaultCodexContextManager {
    pub max_messages: Option<usize>,
}

impl CodexContextManager for DefaultCodexContextManager {
    fn prepare_messages(&self, request: &LanguageModelRequest) -> Vec<CodexMessage> {
        let mut messages: Vec<CodexMessage> = request
            .messages
            .iter()
            .filter(|message| message.role != LanguageModelRole::System)
            .map(CodexMessage::from_language_model_message)
            .collect();
        if let Some(max_messages) = self.max_messages
            && messages.len() > max_messages
        {
            messages = messages[messages.len() - max_messages..].to_vec();
        }
        messages
    }
}

#[derive(Default)]
pub struct CodexToolRegistry {
    tools: BTreeMap<String, Box<dyn CodexTool>>,
}

impl CodexToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<T: CodexTool + 'static>(&mut self, tool: T) {
        self.tools
            .insert(tool.descriptor().name.clone(), Box::new(tool));
    }

    pub fn register_boxed(&mut self, tool: Box<dyn CodexTool>) {
        self.tools.insert(tool.descriptor().name.clone(), tool);
    }

    pub fn descriptors(&self) -> Vec<CodexToolDescriptor> {
        self.tools.values().map(|tool| tool.descriptor()).collect()
    }

    pub fn execute(&self, call: &CodexToolCall) -> CodexToolResult {
        self.tools
            .get(&call.name)
            .map(|tool| tool.execute(call))
            .unwrap_or_else(|| CodexToolResult {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                content: format!("Tool not found: {}", call.name),
                is_error: true,
                metadata: serde_json::Value::Null,
            })
    }
}

pub struct CodexProvider<
    C = OpenAiResponsesCodexClient,
    P = DefaultCodexPromptBuilder,
    M = DefaultCodexContextManager,
> {
    config: CodexConfig,
    client: C,
    tools: CodexToolRegistry,
    prompt_builder: P,
    context_manager: M,
}

impl CodexProvider<OpenAiResponsesCodexClient> {
    pub fn openai_responses() -> Self {
        Self::new(
            CodexConfig::default(),
            OpenAiResponsesCodexClient::default(),
            CodexToolRegistry::new(),
            DefaultCodexPromptBuilder::default(),
            DefaultCodexContextManager::default(),
        )
    }

    pub fn chatgpt_tokens(&self) -> Result<CodexChatGptTokens, LanguageModelError> {
        self.client.chatgpt_tokens()
    }

    pub fn save_chatgpt_tokens(
        &self,
        tokens: CodexChatGptTokens,
    ) -> Result<(), LanguageModelError> {
        self.client.save_chatgpt_tokens(tokens)
    }

    pub fn device_login_start(&self) -> Result<CodexDeviceCode, LanguageModelError> {
        self.client.device_login_start()
    }

    pub fn device_login_poll(
        &self,
        device: &CodexDeviceCode,
    ) -> Result<CodexDevicePoll, LanguageModelError> {
        self.client.device_login_poll(device)
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.client.credentials_path()
    }

    pub fn list_models(&self) -> Result<Vec<String>, LanguageModelError> {
        self.client.list_models()
    }

    pub fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError> {
        self.client.usage()
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

impl<C, P, M> CodexProvider<C, P, M> {
    pub const PROVIDER_ID: &'static str = CODEX_PROVIDER_ID;

    pub fn new(
        config: CodexConfig,
        client: C,
        tools: CodexToolRegistry,
        prompt_builder: P,
        context_manager: M,
    ) -> Self {
        Self {
            config,
            client,
            tools,
            prompt_builder,
            context_manager,
        }
    }

    pub fn tools(&self) -> &CodexToolRegistry {
        &self.tools
    }

    pub fn tools_mut(&mut self) -> &mut CodexToolRegistry {
        &mut self.tools
    }
}

impl<C, P, M> LanguageModelProvider for CodexProvider<C, P, M>
where
    C: CodexClient,
    P: CodexPromptBuilder,
    M: CodexContextManager,
{
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: self.config.provider_id.clone(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            supports_json_object: true,
            supports_tool_permissions: true,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        self.complete_streaming_events(request, &mut |_| {})
    }

    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let mut messages = self.context_manager.prepare_messages(request);
        if let Some(system_prompt) = self.prompt_builder.build_system_prompt(request) {
            messages.insert(0, CodexMessage::system(system_prompt));
        }
        let tools = self.tools.descriptors();

        for _ in 0..self.config.max_iterations {
            on_event(LanguageModelStreamEvent::Started {
                provider: self.config.provider_id.clone(),
                model: model.clone(),
            });
            let codex_request = CodexModelRequest {
                model: model.clone(),
                messages: messages.clone(),
                tools: tools.clone(),
                temperature: request.temperature,
                max_output_tokens: request.max_output_tokens,
                response_format: request.response_format.clone(),
                metadata: request.metadata.clone(),
                options: self.config.options.clone(),
            };
            let response = self.client.complete_streaming(&codex_request, on_event)?;
            if response.tool_calls.is_empty() {
                return Ok(LanguageModelResponse {
                    provider: self.config.provider_id.clone(),
                    model,
                    content: response.content,
                    items: Vec::new(),
                    tool_calls: Vec::new(),
                    usage: response.usage,
                });
            }

            if response.continuation_items.is_empty() {
                messages.push(CodexMessage::assistant_tool_calls(
                    response.content.clone(),
                    response.tool_calls.clone(),
                ));
            } else {
                for item in response.continuation_items {
                    messages.push(CodexMessage::raw_item(item));
                }
            }

            for call in response.tool_calls {
                let result = self.tools.execute(&call);
                messages.push(CodexMessage::tool_result(result));
            }
        }

        Err(LanguageModelError::ProviderInvocationFailed {
            provider: self.config.provider_id.clone(),
            model,
            detail: format!(
                "codex provider exceeded {} tool iteration(s)",
                self.config.max_iterations
            ),
        })
    }
}

impl LanguageModelUsageProvider for CodexProvider<OpenAiResponsesCodexClient> {
    fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError> {
        CodexProvider::usage(self)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexModelRequest {
    pub model: String,
    pub messages: Vec<CodexMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<CodexToolDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    pub response_format: LanguageModelResponseFormat,
    #[serde(default)]
    pub metadata: serde_json::Value,
    #[serde(
        default,
        skip_serializing_if = "LanguageModelProviderOptions::is_default"
    )]
    pub options: LanguageModelProviderOptions,
}

impl CodexModelRequest {
    fn with_chatgpt_supported_model(&self) -> Self {
        let mut request = self.clone();
        if request.model == LEGACY_CODEX_CHATGPT_DEFAULT_MODEL {
            request.model = CODEX_DEFAULT_MODEL.to_string();
        }
        request
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexModelResponse {
    #[serde(default)]
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<CodexToolCall>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub continuation_items: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<LanguageModelUsage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stream_events: Vec<LanguageModelStreamEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct CodexToolDescriptor {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexToolCall {
    pub call_id: String,
    pub name: String,
    #[serde(default)]
    pub input: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexToolResult {
    pub call_id: String,
    pub name: String,
    pub content: String,
    pub is_error: bool,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CodexRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodexMessage {
    pub role: CodexRole,
    #[serde(default)]
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<CodexToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_item: Option<serde_json::Value>,
}

impl CodexMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self::new(CodexRole::System, content)
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self::new(CodexRole::User, content)
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self::new(CodexRole::Assistant, content)
    }

    pub fn assistant_tool_calls(
        content: impl Into<String>,
        tool_calls: Vec<CodexToolCall>,
    ) -> Self {
        Self {
            role: CodexRole::Assistant,
            content: content.into(),
            tool_calls,
            tool_call_id: None,
            raw_item: None,
        }
    }

    pub fn tool_result(result: CodexToolResult) -> Self {
        Self {
            role: CodexRole::Tool,
            content: result.content,
            tool_calls: Vec::new(),
            tool_call_id: Some(result.call_id),
            raw_item: None,
        }
    }

    pub fn raw_item(item: serde_json::Value) -> Self {
        Self {
            role: CodexRole::Assistant,
            content: String::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            raw_item: Some(item),
        }
    }

    fn new(role: CodexRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            raw_item: None,
        }
    }

    fn from_language_model_message(message: &LanguageModelMessage) -> Self {
        let role = match message.role {
            LanguageModelRole::System => CodexRole::System,
            LanguageModelRole::User => CodexRole::User,
            LanguageModelRole::Assistant => CodexRole::Assistant,
        };
        Self::new(role, message.content.clone())
    }
}

fn codex_headers(
    tokens: &CodexChatGptTokens,
    provider: &str,
    model: &str,
) -> Result<HeaderMap, LanguageModelError> {
    codex_headers_with_accept(
        tokens,
        provider,
        model,
        HeaderValue::from_static("text/event-stream"),
    )
}

fn codex_json_headers(
    tokens: &CodexChatGptTokens,
    provider: &str,
    model: &str,
) -> Result<HeaderMap, LanguageModelError> {
    codex_headers_with_accept(
        tokens,
        provider,
        model,
        HeaderValue::from_static("application/json"),
    )
}

fn codex_headers_with_accept(
    tokens: &CodexChatGptTokens,
    provider: &str,
    model: &str,
    accept: HeaderValue,
) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, accept);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", tokens.access_token)).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: provider.to_string(),
                model: model.to_string(),
                detail: source.to_string(),
            }
        })?,
    );
    if let Some(account_id) = tokens.account_id.as_deref() {
        headers.insert(
            "ChatGPT-Account-ID",
            HeaderValue::from_str(account_id).map_err(|source| {
                LanguageModelError::ProviderInvocationFailed {
                    provider: provider.to_string(),
                    model: model.to_string(),
                    detail: source.to_string(),
                }
            })?,
        );
    }
    if tokens.chatgpt_account_is_fedramp {
        headers.insert("X-OpenAI-Fedramp", HeaderValue::from_static("true"));
    }
    Ok(headers)
}

fn extract_codex_model_ids(value: &serde_json::Value) -> Vec<String> {
    let Some(models) = value
        .get("models")
        .or_else(|| value.get("data"))
        .and_then(serde_json::Value::as_array)
        .or_else(|| value.as_array())
    else {
        return Vec::new();
    };

    let mut entries = models
        .iter()
        .filter_map(codex_model_list_entry)
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut seen = Vec::new();
    let mut ids = Vec::new();
    for (_, id) in entries {
        if seen.iter().any(|existing| existing == &id) {
            continue;
        }
        seen.push(id.clone());
        ids.push(id);
    }
    ids
}

fn codex_model_list_entry(value: &serde_json::Value) -> Option<(i64, String)> {
    if let Some(id) = value.as_str().map(str::trim).filter(|id| !id.is_empty()) {
        return Some((i64::MAX, id.to_string()));
    }
    let object = value.as_object()?;
    if matches!(
        object.get("visibility").and_then(serde_json::Value::as_str),
        Some("hide" | "none")
    ) {
        return None;
    }
    let id = object
        .get("slug")
        .or_else(|| object.get("id"))
        .or_else(|| object.get("model"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|id| !id.is_empty())?
        .to_string();
    let priority = object
        .get("priority")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(i64::MAX);
    Some((priority, id))
}

fn codex_usage_from_value(
    value: &serde_json::Value,
    captured_at_unix: u64,
) -> LanguageModelProviderUsage {
    let plan = value
        .get("plan_type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let mut limits = Vec::new();
    if let Some(rate_limit) = value.get("rate_limit") {
        limits.extend(codex_rate_limit_entries("codex", None, rate_limit));
    }
    if let Some(additional) = value
        .get("additional_rate_limits")
        .and_then(serde_json::Value::as_array)
    {
        for entry in additional {
            let id = entry
                .get("metered_feature")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("codex.additional");
            let name = entry
                .get("limit_name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string);
            if let Some(rate_limit) = entry.get("rate_limit") {
                limits.extend(codex_rate_limit_entries(id, name, rate_limit));
            }
        }
    }

    LanguageModelProviderUsage {
        provider: CODEX_PROVIDER_ID.to_string(),
        plan,
        captured_at_unix,
        limits,
        credits: codex_credit_usage(value.get("credits")),
    }
}

fn codex_rate_limit_entries(
    id: &str,
    name: Option<String>,
    value: &serde_json::Value,
) -> Vec<LanguageModelUsageLimit> {
    let allowed = value.get("allowed").and_then(serde_json::Value::as_bool);
    let exhausted = value
        .get("limit_reached")
        .and_then(serde_json::Value::as_bool);
    [
        ("primary", "primary_window"),
        ("secondary", "secondary_window"),
    ]
    .into_iter()
    .filter_map(|(scope, key)| {
        let window = value.get(key)?;
        let used_percent = codex_json_f64(window.get("used_percent"));
        Some(LanguageModelUsageLimit {
            id: id.to_string(),
            name: name.clone(),
            scope: Some(scope.to_string()),
            unlimited: None,
            allowed,
            exhausted,
            remaining: None,
            entitlement: None,
            used_percent,
            remaining_percent: used_percent.map(|percent| (100.0 - percent).max(0.0)),
            window_seconds: window
                .get("limit_window_seconds")
                .and_then(serde_json::Value::as_u64),
            reset_at_unix: window.get("reset_at").and_then(serde_json::Value::as_i64),
        })
    })
    .collect()
}

fn codex_credit_usage(value: Option<&serde_json::Value>) -> Option<LanguageModelCreditUsage> {
    let value = value?;
    Some(LanguageModelCreditUsage {
        has_credits: value
            .get("has_credits")
            .and_then(serde_json::Value::as_bool),
        unlimited: value.get("unlimited").and_then(serde_json::Value::as_bool),
        balance: value
            .get("balance")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
    })
}

fn codex_json_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_f64(),
        Some(serde_json::Value::String(text)) => text.trim().parse().ok(),
        _ => None,
    }
}

fn openai_responses_codex_payload(request: &CodexModelRequest) -> serde_json::Value {
    let instructions = codex_request_instructions(request);
    let mut input: Vec<serde_json::Value> = request
        .messages
        .iter()
        .filter(|message| message.role != CodexRole::System)
        .map(openai_responses_input_item)
        .collect();
    let tools: Vec<serde_json::Value> = request
        .tools
        .iter()
        .map(|tool| {
            serde_json::json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            })
        })
        .collect();
    if request.response_format == LanguageModelResponseFormat::JsonObject
        && !openai_responses_input_mentions_json(request)
    {
        input.insert(
            0,
            serde_json::json!({
                "role": "user",
                "content": "Return a valid JSON object. Do not include non-JSON text.",
            }),
        );
    }
    let mut payload = serde_json::json!({
        "model": request.model,
        "input": input,
        "instructions": instructions,
        "stream": true,
        "store": false,
    });
    if !tools.is_empty() {
        payload["tools"] = serde_json::json!(tools);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["text"] = serde_json::json!({ "format": { "type": "json_object" } });
    }
    if let Some(effort) = request
        .options
        .reasoning_effort
        .and_then(|effort| effort.as_openai_responses_value())
    {
        payload["reasoning"] = serde_json::json!({ "effort": effort });
    }
    if let Some(verbosity) = request.options.text_verbosity {
        if !payload
            .get("text")
            .is_some_and(serde_json::Value::is_object)
        {
            payload["text"] = serde_json::json!({});
        }
        payload["text"]["verbosity"] = serde_json::json!(verbosity.as_config_value());
    }
    apply_provider_vendor_options_to_payload(&mut payload, &request.options);
    payload
}

fn openai_responses_input_mentions_json(request: &CodexModelRequest) -> bool {
    request
        .messages
        .iter()
        .filter(|message| message.role != CodexRole::System)
        .any(codex_message_mentions_json)
}

fn codex_message_mentions_json(message: &CodexMessage) -> bool {
    if !matches!(message.role, CodexRole::User | CodexRole::Assistant) {
        return false;
    }
    if message.content.to_ascii_lowercase().contains("json") {
        return true;
    }
    message
        .raw_item
        .as_ref()
        .is_some_and(json_value_mentions_json)
}

fn json_value_mentions_json(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => text.to_ascii_lowercase().contains("json"),
        serde_json::Value::Array(values) => values.iter().any(json_value_mentions_json),
        serde_json::Value::Object(values) => values.values().any(json_value_mentions_json),
        _ => false,
    }
}

fn codex_request_instructions(request: &CodexModelRequest) -> String {
    let instructions = request
        .messages
        .iter()
        .filter(|message| message.role == CodexRole::System)
        .map(|message| message.content.as_str())
        .filter(|content| !content.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    if instructions.trim().is_empty() {
        CODEX_DEFAULT_INSTRUCTIONS.to_string()
    } else {
        instructions
    }
}

fn openai_responses_input_item(message: &CodexMessage) -> serde_json::Value {
    if let Some(raw_item) = &message.raw_item {
        return raw_item.clone();
    }
    match message.role {
        CodexRole::User => serde_json::json!({
            "role": "user",
            "content": message.content,
        }),
        CodexRole::Assistant => serde_json::json!({
            "role": "assistant",
            "content": message.content,
        }),
        CodexRole::Tool => serde_json::json!({
            "type": "function_call_output",
            "call_id": message.tool_call_id,
            "output": message.content,
        }),
        CodexRole::System => serde_json::Value::Null,
    }
}

fn parse_openai_responses_codex_response(value: &serde_json::Value) -> CodexModelResponse {
    let output = value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let content = value
        .get("output_text")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| extract_output_text(&output));
    let tool_calls = output
        .iter()
        .filter_map(parse_function_call_item)
        .collect::<Vec<_>>();
    let mut stream_events = stream_events_from_openai_output(&output);
    let continuation_items = output
        .into_iter()
        .filter(|item| {
            matches!(
                item.get("type").and_then(serde_json::Value::as_str),
                Some("function_call" | "reasoning")
            )
        })
        .collect();
    let usage = value.get("usage").map(|usage| LanguageModelUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("output_tokens")
            .and_then(serde_json::Value::as_u64),
    });
    if let Some(usage) = &usage {
        stream_events.push(LanguageModelStreamEvent::Usage {
            usage: usage.clone(),
        });
    }
    stream_events.push(LanguageModelStreamEvent::Finished {
        finish_reason: openai_responses_finish_reason(value),
    });
    CodexModelResponse {
        content,
        tool_calls,
        continuation_items,
        usage,
        stream_events,
    }
}

fn parse_openai_responses_codex_body_with_events(
    body: &str,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<CodexModelResponse, LanguageModelError> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        let response =
            parse_openai_responses_codex_response(value.get("response").unwrap_or(&value));
        for event in &response.stream_events {
            on_event(event.clone());
        }
        return Ok(response);
    }

    parse_openai_responses_codex_sse_response_with_events(body, model, on_event)
}

#[cfg(test)]
fn parse_openai_responses_codex_sse_response(
    body: &str,
    model: &str,
) -> Result<CodexModelResponse, LanguageModelError> {
    parse_openai_responses_codex_sse_response_with_events(body, model, &mut |_| {})
}

fn parse_openai_responses_codex_sse_response_with_events(
    body: &str,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<CodexModelResponse, LanguageModelError> {
    let mut output = Vec::new();
    let mut output_text = String::new();
    let mut stream_events = Vec::new();
    let normalized = body.replace("\r\n", "\n");

    for block in normalized.split("\n\n") {
        let Some(event) = parse_sse_event(block, model)? else {
            continue;
        };
        let kind = event
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        match kind {
            "response.output_item.done" => {
                if let Some(item) = event.get("item") {
                    for stream_event in stream_events_from_openai_output_item(item) {
                        on_event(stream_event.clone());
                        stream_events.push(stream_event);
                    }
                    output.push(item.clone());
                }
            }
            "response.output_text.delta" => {
                if let Some(delta) = event.get("delta").and_then(serde_json::Value::as_str) {
                    output_text.push_str(delta);
                    let stream_event = LanguageModelStreamEvent::TextDelta {
                        delta: delta.to_string(),
                    };
                    on_event(stream_event.clone());
                    stream_events.push(stream_event);
                }
            }
            "response.completed" => {
                let mut response = event
                    .get("response")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let missing_or_empty_output = response
                    .get("output")
                    .and_then(serde_json::Value::as_array)
                    .is_none_or(Vec::is_empty);
                if missing_or_empty_output && !output.is_empty() {
                    response["output"] = serde_json::json!(output);
                }
                if response.get("output_text").is_none() && !output_text.is_empty() {
                    response["output_text"] = serde_json::json!(output_text);
                }
                let mut parsed = parse_openai_responses_codex_response(&response);
                if let Some(usage) = &parsed.usage {
                    let stream_event = LanguageModelStreamEvent::Usage {
                        usage: usage.clone(),
                    };
                    on_event(stream_event.clone());
                    stream_events.push(stream_event);
                }
                let stream_event = LanguageModelStreamEvent::Finished {
                    finish_reason: openai_responses_finish_reason(&response),
                };
                on_event(stream_event.clone());
                stream_events.push(stream_event);
                parsed.stream_events = stream_events;
                return Ok(parsed);
            }
            "response.failed" => {
                let detail = openai_responses_event_error_message(&event)
                    .unwrap_or_else(|| format!("Codex backend stream failed: {event}"));
                on_event(LanguageModelStreamEvent::Error {
                    detail: detail.clone(),
                });
                return Err(codex_provider_invocation_error(model, detail));
            }
            _ => {}
        }
    }

    if !output.is_empty() || !output_text.is_empty() {
        let mut response = serde_json::json!({ "output": output });
        if !output_text.is_empty() {
            response["output_text"] = serde_json::json!(output_text);
        }
        let mut parsed = parse_openai_responses_codex_response(&response);
        if stream_events
            .iter()
            .all(|event| !matches!(event, LanguageModelStreamEvent::Finished { .. }))
        {
            let stream_event = LanguageModelStreamEvent::Finished {
                finish_reason: None,
            };
            on_event(stream_event.clone());
            stream_events.push(stream_event);
        }
        parsed.stream_events = stream_events;
        return Ok(parsed);
    }

    Err(codex_provider_invocation_error(
        model,
        "Codex backend stream closed before response.completed".to_string(),
    ))
}

fn stream_events_from_openai_output(output: &[serde_json::Value]) -> Vec<LanguageModelStreamEvent> {
    output
        .iter()
        .flat_map(stream_events_from_openai_output_item)
        .collect()
}

fn stream_events_from_openai_output_item(
    item: &serde_json::Value,
) -> Vec<LanguageModelStreamEvent> {
    match item
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
    {
        "message" => message_text_parts(item)
            .into_iter()
            .map(|delta| LanguageModelStreamEvent::TextDelta { delta })
            .collect(),
        "reasoning" => reasoning_summaries(item)
            .into_iter()
            .map(|summary| LanguageModelStreamEvent::ReasoningSummary { summary })
            .collect(),
        "function_call" => parse_function_call_item(item)
            .map(|call| {
                vec![
                    LanguageModelStreamEvent::ToolCallStarted {
                        call_id: Some(call.call_id.clone()),
                        name: call.name.clone(),
                    },
                    LanguageModelStreamEvent::ToolCallCompleted {
                        call_id: Some(call.call_id),
                        name: call.name,
                        input: call.input,
                    },
                ]
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn message_text_parts(item: &serde_json::Value) -> Vec<String> {
    item.get("content")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(
            |content| match content.get("type").and_then(serde_json::Value::as_str) {
                Some("output_text" | "text") => content
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
                _ => None,
            },
        )
        .collect()
}

fn reasoning_summaries(item: &serde_json::Value) -> Vec<String> {
    let Some(summary) = item.get("summary").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    summary
        .iter()
        .filter_map(|part| {
            part.as_str()
                .map(str::to_string)
                .or_else(|| {
                    part.get("text")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .or_else(|| {
                    part.get("summary")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
        })
        .filter(|summary| !summary.trim().is_empty())
        .collect()
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

fn parse_sse_event(
    block: &str,
    model: &str,
) -> Result<Option<serde_json::Value>, LanguageModelError> {
    let mut event_name = None;
    let mut data_lines = Vec::new();

    for line in block.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event_name = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start());
        }
    }

    if data_lines.is_empty() {
        return Ok(None);
    }

    let data = data_lines.join("\n");
    if data.trim().is_empty() || data.trim() == "[DONE]" {
        return Ok(None);
    }

    let mut value = serde_json::from_str::<serde_json::Value>(&data).map_err(|source| {
        codex_provider_invocation_error(
            model,
            format!("failed to parse Codex stream event: {source}: {data}"),
        )
    })?;
    if value.get("type").is_none()
        && let (Some(kind), Some(object)) = (event_name, value.as_object_mut())
    {
        object.insert("type".to_string(), serde_json::json!(kind));
    }

    Ok(Some(value))
}

fn openai_responses_event_error_message(value: &serde_json::Value) -> Option<String> {
    value
        .pointer("/response/error/message")
        .or_else(|| value.pointer("/error/message"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn codex_provider_invocation_error(model: &str, detail: String) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: CODEX_PROVIDER_ID.to_string(),
        model: model.to_string(),
        detail,
    }
}

fn extract_output_text(output: &[serde_json::Value]) -> String {
    output
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
}

fn parse_function_call_item(item: &serde_json::Value) -> Option<CodexToolCall> {
    if item.get("type").and_then(serde_json::Value::as_str) != Some("function_call") {
        return None;
    }
    let arguments = item
        .get("arguments")
        .and_then(serde_json::Value::as_str)
        .and_then(|arguments| serde_json::from_str(arguments).ok())
        .unwrap_or(serde_json::Value::Null);
    Some(CodexToolCall {
        call_id: item
            .get("call_id")
            .or_else(|| item.get("id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        name: item
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        input: arguments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort, ProviderTextVerbosity};
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: CodexProvider::<FixtureCodexClient>::PROVIDER_ID.to_string(),
            model: PROVIDER_DEFAULT_MODEL.to_string(),
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::System,
                    content: "Use Sinomo tools.".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "Inspect the repo.".to_string(),
                },
            ],
            items: Vec::new(),
            tools: Vec::new(),
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: Some(0.2),
            max_output_tokens: Some(200),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        }
    }

    #[test]
    fn codex_runs_tool_loop_before_final_answer() {
        let requests = Rc::new(RefCell::new(Vec::new()));
        let client = FixtureCodexClient {
            requests: Rc::clone(&requests),
            responses: RefCell::new(vec![
                CodexModelResponse {
                    content: String::new(),
                    tool_calls: vec![CodexToolCall {
                        call_id: "call_1".to_string(),
                        name: "read_context".to_string(),
                        input: serde_json::json!({ "path": "README.md" }),
                    }],
                    continuation_items: vec![serde_json::json!({
                        "type": "function_call",
                        "call_id": "call_1",
                        "name": "read_context",
                        "arguments": "{\"path\":\"README.md\"}"
                    })],
                    usage: None,
                    stream_events: Vec::new(),
                },
                CodexModelResponse {
                    content: "Done with README context.".to_string(),
                    tool_calls: Vec::new(),
                    continuation_items: Vec::new(),
                    usage: None,
                    stream_events: Vec::new(),
                },
            ]),
        };
        let mut tools = CodexToolRegistry::new();
        tools.register(FixtureTool);
        let provider = CodexProvider::new(
            CodexConfig::default(),
            client,
            tools,
            DefaultCodexPromptBuilder {
                extra_instructions: vec!["Return concise answers.".to_string()],
            },
            DefaultCodexContextManager::default(),
        );

        let response = provider.complete(&request()).expect("codex response");

        assert_eq!(response.content, "Done with README context.");
        let requests = requests.borrow();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].tools[0].name, "read_context");
        assert_eq!(
            requests[0].messages[0].content,
            "Use Sinomo tools.\n\nReturn concise answers."
        );
        assert!(
            requests[1]
                .messages
                .iter()
                .any(|message| message.raw_item.is_some())
        );
        assert!(requests[1].messages.iter().any(|message| {
            message.role == CodexRole::Tool
                && message.tool_call_id.as_deref() == Some("call_1")
                && message.content.contains("README context")
        }));
    }

    #[test]
    fn context_manager_can_limit_history() {
        let manager = DefaultCodexContextManager {
            max_messages: Some(1),
        };
        let messages = manager.prepare_messages(&LanguageModelRequest {
            messages: vec![
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "old".to_string(),
                },
                LanguageModelMessage {
                    role: LanguageModelRole::User,
                    content: "new".to_string(),
                },
            ],
            ..request()
        });

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "new");
    }

    #[test]
    fn openai_responses_payload_preserves_tools_and_tool_outputs() {
        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: CODEX_DEFAULT_MODEL.to_string(),
            messages: vec![
                CodexMessage::system("system prompt"),
                CodexMessage::user("run tool"),
                CodexMessage::raw_item(serde_json::json!({
                    "type": "reasoning",
                    "id": "rs_1",
                    "summary": []
                })),
                CodexMessage::tool_result(CodexToolResult {
                    call_id: "call_1".to_string(),
                    name: "read_context".to_string(),
                    content: "{\"ok\":true}".to_string(),
                    is_error: false,
                    metadata: serde_json::Value::Null,
                }),
            ],
            tools: vec![FixtureTool.descriptor()],
            temperature: Some(0.2),
            max_output_tokens: Some(100),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::Value::Null,
            options: LanguageModelProviderOptions::default(),
        });

        assert_eq!(payload["instructions"], "system prompt");
        assert_eq!(payload["model"], CODEX_DEFAULT_MODEL);
        assert_eq!(payload["tools"][0]["name"], "read_context");
        assert!(
            payload["input"][0]["content"]
                .as_str()
                .expect("json reminder")
                .contains("JSON")
        );
        assert_eq!(payload["input"][2]["type"], "reasoning");
        assert_eq!(payload["input"][3]["type"], "function_call_output");
        assert!(payload.get("temperature").is_none());
        assert!(payload.get("max_output_tokens").is_none());
        assert_eq!(payload["text"]["format"]["type"], "json_object");
        assert_eq!(payload["stream"], true);
    }

    #[test]
    fn openai_responses_json_payload_does_not_duplicate_json_reminder() {
        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: CODEX_DEFAULT_MODEL.to_string(),
            messages: vec![
                CodexMessage::system("system prompt"),
                CodexMessage::user("Return JSON for this decision."),
            ],
            tools: Vec::new(),
            temperature: Some(0.2),
            max_output_tokens: Some(100),
            response_format: LanguageModelResponseFormat::JsonObject,
            metadata: serde_json::Value::Null,
            options: LanguageModelProviderOptions::default(),
        });

        assert_eq!(payload["input"].as_array().expect("input").len(), 1);
        assert_eq!(
            payload["input"][0]["content"],
            "Return JSON for this decision."
        );
        assert_eq!(payload["text"]["format"]["type"], "json_object");
    }

    #[test]
    fn legacy_chatgpt_codex_default_maps_to_supported_model() {
        let request = CodexModelRequest {
            model: LEGACY_CODEX_CHATGPT_DEFAULT_MODEL.to_string(),
            messages: vec![CodexMessage::user("hello")],
            tools: Vec::new(),
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
            options: LanguageModelProviderOptions::default(),
        };

        assert_eq!(
            request.with_chatgpt_supported_model().model,
            CODEX_DEFAULT_MODEL
        );
    }

    #[test]
    fn openai_responses_payload_includes_default_instructions_without_system_prompt() {
        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: CODEX_DEFAULT_MODEL.to_string(),
            messages: vec![CodexMessage::user("hello")],
            tools: Vec::new(),
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
            options: LanguageModelProviderOptions::default(),
        });

        assert_eq!(payload["instructions"], CODEX_DEFAULT_INSTRUCTIONS);
    }

    #[test]
    fn openai_responses_payload_applies_provider_options() {
        let mut options = LanguageModelProviderOptions {
            reasoning_effort: Some(ProviderReasoningEffort::XHigh),
            text_verbosity: Some(ProviderTextVerbosity::Medium),
            ..LanguageModelProviderOptions::default()
        };
        options
            .vendor_options
            .insert("metadata.reason".to_string(), "\"test\"".to_string());

        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: CODEX_DEFAULT_MODEL.to_string(),
            messages: vec![CodexMessage::user("hello")],
            tools: Vec::new(),
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
            options,
        });

        assert_eq!(payload["reasoning"]["effort"], "xhigh");
        assert_eq!(payload["text"]["verbosity"], "medium");
        assert_eq!(payload["metadata"]["reason"], "test");
    }

    #[test]
    fn codex_models_parser_uses_remote_catalog_slugs() {
        let models = extract_codex_model_ids(&serde_json::json!({
            "models": [
                { "slug": "gpt-5.4", "priority": 20, "visibility": "list" },
                { "slug": "hidden-model", "priority": 1, "visibility": "hide" },
                { "slug": "gpt-5.5", "priority": 10, "visibility": "list" },
                { "id": "fallback-id", "priority": 30 },
                "raw-model"
            ]
        }));

        assert_eq!(
            models,
            vec!["gpt-5.5", "gpt-5.4", "fallback-id", "raw-model"]
        );
    }

    #[test]
    fn codex_usage_parser_maps_rate_limit_windows() {
        let usage = codex_usage_from_value(
            &serde_json::json!({
                "plan_type": "prolite",
                "rate_limit": {
                    "allowed": true,
                    "limit_reached": false,
                    "primary_window": {
                        "used_percent": 39,
                        "limit_window_seconds": 18000,
                        "reset_at": 1777522528
                    },
                    "secondary_window": {
                        "used_percent": 14,
                        "limit_window_seconds": 604800,
                        "reset_at": 1777996221
                    }
                },
                "additional_rate_limits": [{
                    "limit_name": "GPT-5.3-Codex-Spark",
                    "metered_feature": "codex_bengalfox",
                    "rate_limit": {
                        "allowed": true,
                        "limit_reached": false,
                        "primary_window": {
                            "used_percent": 0,
                            "limit_window_seconds": 18000,
                            "reset_at": 1777539436
                        }
                    }
                }],
                "credits": {
                    "has_credits": false,
                    "unlimited": false,
                    "balance": "0"
                }
            }),
            10,
        );

        assert_eq!(usage.provider, CODEX_PROVIDER_ID);
        assert_eq!(usage.plan.as_deref(), Some("prolite"));
        assert_eq!(usage.limits.len(), 3);
        assert_eq!(usage.limits[0].id, "codex");
        assert_eq!(usage.limits[0].scope.as_deref(), Some("primary"));
        assert_eq!(usage.limits[0].used_percent, Some(39.0));
        assert_eq!(usage.limits[1].remaining_percent, Some(86.0));
        assert_eq!(usage.credits.unwrap().balance.as_deref(), Some("0"));
    }

    #[test]
    fn openai_responses_parser_extracts_tool_calls_and_reasoning_items() {
        let parsed = parse_openai_responses_codex_response(&serde_json::json!({
            "output": [
                { "type": "reasoning", "id": "rs_1", "summary": [{ "text": "checked context" }] },
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_context",
                    "arguments": "{\"path\":\"README.md\"}"
                },
                {
                    "type": "message",
                    "content": [{ "type": "output_text", "text": "partial" }]
                }
            ],
            "usage": { "input_tokens": 10, "output_tokens": 4 }
        }));

        assert_eq!(parsed.content, "partial");
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].input["path"], "README.md");
        assert_eq!(parsed.continuation_items.len(), 2);
        assert_eq!(parsed.usage.unwrap().input_tokens, Some(10));
        assert!(parsed.stream_events.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::ReasoningSummary { summary }
                    if summary == "checked context"
            )
        }));
        assert!(parsed.stream_events.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::ToolCallCompleted { name, input, .. }
                    if name == "read_context" && input["path"] == "README.md"
            )
        }));
    }

    #[test]
    fn openai_responses_sse_parser_extracts_completed_response() {
        let body = concat!(
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2}}}\n\n",
        );

        let parsed =
            parse_openai_responses_codex_sse_response(body, CODEX_DEFAULT_MODEL).expect("sse");

        assert_eq!(parsed.content, "hello");
        assert_eq!(parsed.usage.unwrap().output_tokens, Some(2));
    }

    #[test]
    fn openai_responses_sse_parser_uses_text_deltas_as_fallback() {
        let body = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hel\"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"lo\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\"}}\n\n",
        );

        let parsed =
            parse_openai_responses_codex_sse_response(body, CODEX_DEFAULT_MODEL).expect("sse");

        assert_eq!(parsed.content, "hello");
        assert_eq!(
            parsed
                .stream_events
                .iter()
                .filter(|event| matches!(event, LanguageModelStreamEvent::TextDelta { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn openai_responses_sse_parser_emits_stream_events() {
        let body = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"reasoning\",\"summary\":[{\"text\":\"thinking\"}]}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_context\",\"arguments\":\"{\\\"path\\\":\\\"README.md\\\"}\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2}}}\n\n",
        );
        let mut streamed = Vec::new();

        let parsed = parse_openai_responses_codex_sse_response_with_events(
            body,
            CODEX_DEFAULT_MODEL,
            &mut |event| streamed.push(event),
        )
        .expect("sse");

        assert_eq!(parsed.content, "hi");
        assert!(streamed.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::ReasoningSummary { summary } if summary == "thinking"
            )
        }));
        assert!(streamed.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::ToolCallStarted { name, .. } if name == "read_context"
            )
        }));
        assert!(streamed.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::Usage { usage }
                    if usage.input_tokens == Some(3)
            )
        }));
        assert!(streamed.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::Finished { finish_reason }
                    if finish_reason.as_deref() == Some("completed")
            )
        }));
        assert_eq!(parsed.stream_events, streamed);
    }

    #[test]
    fn openai_responses_sse_parser_reports_failed_response() {
        let body = concat!(
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"bad stream\"}}}\n\n",
        );

        let mut streamed = Vec::new();
        let error = parse_openai_responses_codex_sse_response_with_events(
            body,
            CODEX_DEFAULT_MODEL,
            &mut |event| streamed.push(event),
        )
        .expect_err("failed stream");

        assert!(error.to_string().contains("bad stream"));
        assert!(streamed.iter().any(|event| {
            matches!(event, LanguageModelStreamEvent::Error { detail } if detail == "bad stream")
        }));
    }

    #[test]
    fn codex_auth_json_round_trips_official_chatgpt_shape() {
        let dir = std::env::temp_dir().join(format!(
            "sinomo-codex-auth-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_nanos()
        ));
        let credentials_path = Some(dir.join("credentials.json"));
        let id_token = fake_chatgpt_id_token("account_123", true);

        save_codex_chatgpt_tokens(
            &credentials_path,
            tokens_from_response(
                id_token.clone(),
                "access-token".to_string(),
                "refresh-token".to_string(),
                None,
            ),
        )
        .expect("save codex auth");
        let auth_path = dir.join("credentials.json");
        let auth = serde_json::from_str::<serde_json::Value>(
            &fs::read_to_string(&auth_path).expect("auth json"),
        )
        .expect("valid auth json");

        assert_eq!(auth["auth_mode"], "chatgpt");
        assert!(auth.get("OPENAI_API_KEY").is_none());
        assert_eq!(auth["tokens"]["id_token"], id_token);
        assert_eq!(auth["tokens"]["access_token"], "access-token");
        assert_eq!(auth["tokens"]["refresh_token"], "refresh-token");
        assert_eq!(auth["tokens"]["account_id"], "account_123");
        assert_eq!(auth["tokens"]["chatgpt_account_is_fedramp"], true);
        assert!(auth.get("last_refresh").is_some());
        let tokens = read_codex_chatgpt_tokens(&auth_path)
            .expect("read auth")
            .expect("tokens");
        assert_eq!(tokens.access_token, "access-token");
        assert_eq!(tokens.account_id.as_deref(), Some("account_123"));

        fs::remove_dir_all(dir).ok();
    }

    fn fake_chatgpt_id_token(account_id: &str, fedramp: bool) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            serde_json::json!({
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": account_id,
                    "chatgpt_account_is_fedramp": fedramp
                }
            })
            .to_string(),
        );
        format!("{header}.{payload}.sig")
    }

    struct FixtureCodexClient {
        requests: Rc<RefCell<Vec<CodexModelRequest>>>,
        responses: RefCell<Vec<CodexModelResponse>>,
    }

    impl CodexClient for FixtureCodexClient {
        fn complete(
            &self,
            request: &CodexModelRequest,
        ) -> Result<CodexModelResponse, LanguageModelError> {
            self.requests.borrow_mut().push(request.clone());
            Ok(self.responses.borrow_mut().remove(0))
        }
    }

    struct FixtureTool;

    impl CodexTool for FixtureTool {
        fn descriptor(&self) -> CodexToolDescriptor {
            CodexToolDescriptor {
                name: "read_context".to_string(),
                description: "Read scoped context.".to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" }
                    },
                    "required": ["path"]
                }),
            }
        }

        fn execute(&self, call: &CodexToolCall) -> CodexToolResult {
            CodexToolResult {
                call_id: call.call_id.clone(),
                name: call.name.clone(),
                content: "README context".to_string(),
                is_error: false,
                metadata: serde_json::Value::Null,
            }
        }
    }
}
