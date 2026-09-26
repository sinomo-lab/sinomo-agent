use crate::openai::{image_payload, image_request_operation, image_response, image_tool_error};
use crate::{
    LanguageModelCancellation, LanguageModelCreditUsage, LanguageModelError,
    LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelProviderOptions, LanguageModelProviderUsage,
    LanguageModelRequest, LanguageModelRequestItem, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelResponseItem, LanguageModelRole,
    LanguageModelStreamEvent, LanguageModelToolCall, LanguageModelToolChoice,
    LanguageModelToolSpec, LanguageModelUsage, LanguageModelUsageLimit, LanguageModelUsageProvider,
    OpenAiImageRequest, OpenAiImageResponse, OpenAiWebSearchContextSize, OpenAiWebSearchRequest,
    OpenAiWebSearchResponse, PROVIDER_DEFAULT_MODEL, apply_provider_vendor_options_to_payload,
    read_provider_credentials_json_value, selected_model, unix_timestamp,
    write_provider_credentials_json_value,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::StreamExt;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

mod models;
mod wire;
use models::builtin_model_info;
pub use models::{
    CodexModelInfo, CodexReasoningLevel, CodexServiceTier, codex_builtin_model_catalog,
};

pub use sinomo_runtime::api::lm::CODEX_DEFAULT_MODEL;
pub use sinomo_runtime::api::lm::CODEX_PROVIDER_ID;
const LEGACY_CODEX_CHATGPT_DEFAULT_MODEL: &str = "gpt-5.2-codex";
const CODEX_DEFAULT_INSTRUCTIONS: &str =
    "You are Sinomo's Codex model provider. Answer the user's request clearly and accurately.";
pub const SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR: &str = "SINOMO_CODEX_CREDENTIALS_FILE";
pub const CODEX_AUTH_ISSUER: &str = "https://auth.openai.com";
pub const CODEX_AUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const CODEX_CHATGPT_API_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
// Catalog compatibility floor at upstream c775dd3c3 (including GPT-6 Astra).
// Upstream Cargo.toml uses a development placeholder, which hides version-gated models.
pub const CODEX_MODELS_CLIENT_VERSION: &str = "0.153.0";
/// The image model the Codex backend serves to ChatGPT subscription sign-ins.
pub const CODEX_DEFAULT_IMAGE_MODEL: &str = crate::openai::OPENAI_DEFAULT_IMAGE_MODEL;
/// The Codex backend's standalone web search endpoint, relative to the API base URL.
const CODEX_SEARCH_PATH: &str = "alpha/search";
const CODEX_STREAM_MAX_TRANSPORT_RETRIES: usize = 2;
const CODEX_STREAM_RETRY_BASE_DELAY: Duration = Duration::from_millis(50);
const CODEX_STREAM_CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(10);

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
    /// Connect and per-read health timeout. This is not a total RPC deadline:
    /// every successfully received response-body chunk resets the read timer.
    pub transport_health_timeout: Duration,
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
            transport_health_timeout: sinomo_runtime::network::DEFAULT_NETWORK_TIMEOUT,
        }
    }
}

fn default_codex_credentials_path() -> Option<PathBuf> {
    if let Some(path) = codex_credentials_path_override() {
        return Some(path);
    }
    sinomo_runtime::local::SinomoDataDir::from_env_or_home()
        .ok()
        .map(|dir| dir.provider_credentials_file(CODEX_PROVIDER_ID))
}

/// Resolves the Codex ChatGPT credentials file backing a specific data dir.
///
/// The credentials-file environment override still wins, so a caller that resolves credentials for
/// a known data dir reads the same sign-in the default configuration would.
pub fn codex_credentials_file_in_data_dir(
    data_dir: &sinomo_runtime::local::SinomoDataDir,
) -> PathBuf {
    codex_credentials_path_override()
        .unwrap_or_else(|| data_dir.provider_credentials_file(CODEX_PROVIDER_ID))
}

fn codex_credentials_path_override() -> Option<PathBuf> {
    std::env::var(SINOMO_CODEX_CREDENTIALS_FILE_ENV_VAR)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn codex_image_error(detail: impl std::fmt::Display) -> LanguageModelError {
    image_tool_error(CODEX_PROVIDER_ID, CODEX_DEFAULT_IMAGE_MODEL, detail)
}

fn codex_search_error(detail: impl std::fmt::Display) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: CODEX_PROVIDER_ID.to_string(),
        model: CODEX_DEFAULT_MODEL.to_string(),
        detail: detail.to_string(),
    }
}

/// Builds the standalone search request the Codex backend expects.
///
/// The query is sent twice by design: as the `search_query` command that runs the search, and as
/// the free-text `input` that gives the backend the intent behind it. `allowed_callers` marks this
/// as a direct tool call rather than one relayed through a shell or interpreter.
fn codex_search_payload(
    request: &OpenAiWebSearchRequest,
    query: &str,
    model: &str,
) -> serde_json::Value {
    let allowed_domains = request
        .allowed_domains
        .iter()
        .map(|domain| domain.trim())
        .filter(|domain| !domain.is_empty())
        .collect::<Vec<_>>();
    let context_size = match request.context_size {
        OpenAiWebSearchContextSize::Low => "low",
        OpenAiWebSearchContextSize::Medium => "medium",
        OpenAiWebSearchContextSize::High => "high",
    };
    let mut search_query = serde_json::json!({ "q": query });
    let mut settings = serde_json::json!({
        "search_context_size": context_size,
        "allowed_callers": ["direct"],
        "external_web_access": true,
    });
    if !allowed_domains.is_empty() {
        search_query["domains"] = serde_json::json!(allowed_domains);
        settings["filters"] = serde_json::json!({ "allowed_domains": allowed_domains });
    }
    serde_json::json!({
        "id": codex_search_request_id(),
        "model": model,
        "input": query,
        "commands": { "search_query": [search_query] },
        "settings": settings,
    })
}

/// Returns a unique id for one standalone search.
///
/// Codex scopes a search to its thread so follow-up `open`/`click` commands can resolve the result
/// references. Sinomo's hosted search is single-shot, so each request stands alone.
fn codex_search_request_id() -> String {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    format!(
        "sinomo-search-{}-{}",
        unix_timestamp(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    )
}

fn missing_codex_chatgpt_auth() -> LanguageModelError {
    LanguageModelError::AuthenticationRequired {
        provider: CODEX_PROVIDER_ID.to_string(),
        detail: "no Sinomo Codex ChatGPT sign-in found; run `sinomo providers auth codex`"
            .to_string(),
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    builtin_model_info(model)
        .and_then(|info| info.context_window)
        .or_else(|| crate::openai::inferred_context_window_tokens(model))
}

pub(crate) fn inferred_max_context_window_tokens(model: &str) -> Option<usize> {
    builtin_model_info(model)
        .and_then(|info| info.max_context_window.or(info.context_window))
        .or_else(|| crate::openai::inferred_max_context_window_tokens(model))
}

pub(crate) fn prefers_native_tool_protocol(model: &str) -> bool {
    let model = model.to_ascii_lowercase();
    model.contains("gpt") || model.contains("codex")
}

fn read_codex_chatgpt_tokens(
    credentials_path: &Path,
) -> Result<Option<CodexChatGptTokens>, LanguageModelError> {
    let Some(value) = read_provider_credentials_json_value(
        CODEX_PROVIDER_ID,
        &Some(credentials_path.to_path_buf()),
    )?
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

/// How much life an access token needs left on it to open a realtime session with.
///
/// A session outlives the request that opened it, so a token expiring inside this window is renewed
/// up front instead of expiring partway through a conversation.
const REALTIME_TOKEN_MIN_REMAINING_SECS: i64 = 30 * 60;

/// Returns whether an access token has enough life left to carry a realtime session.
///
/// A token whose expiry cannot be read is treated as spent, so an unreadable credential is renewed
/// rather than presented to the provider and refused.
fn realtime_token_outlives_a_session(access_token: &str) -> bool {
    if access_token.trim().is_empty() {
        return false;
    }
    let Some(expires_at) = jwt_auth_claims(access_token)
        .get("exp")
        .and_then(serde_json::Value::as_i64)
    else {
        return false;
    };
    expires_at - unix_timestamp() as i64 > REALTIME_TOKEN_MIN_REMAINING_SECS
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
    models: std::sync::Arc<std::sync::RwLock<Vec<CodexModelInfo>>>,
}

impl OpenAiResponsesCodexClient {
    pub fn new(config: OpenAiResponsesCodexConfig) -> Self {
        let http = Client::builder()
            .connect_timeout(config.transport_health_timeout)
            .build()
            .expect("Codex HTTP client configuration is valid");
        Self {
            config,
            http,
            models: Default::default(),
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
        Ok(self
            .list_model_catalog()?
            .into_iter()
            .map(|info| info.slug)
            .collect())
    }

    /// Discover account-visible models and their provider capabilities.
    /// A successful refresh also updates this client's request protocol defaults.
    pub fn list_model_catalog(&self) -> Result<Vec<CodexModelInfo>, LanguageModelError> {
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
        let models = models::extract_model_catalog(&value);
        if models.is_empty() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: CODEX_PROVIDER_ID.to_string(),
                model: CODEX_DEFAULT_MODEL.to_string(),
                detail: "Codex backend models API returned no picker-visible models".to_string(),
            });
        }
        if let Ok(mut cache) = self.models.write() {
            *cache = models.clone();
        }
        Ok(models)
    }

    fn model_info(&self, model: &str) -> Option<CodexModelInfo> {
        self.models
            .read()
            .ok()
            .and_then(|models| models.iter().find(|info| info.slug == model).cloned())
            .or_else(|| builtin_model_info(model).cloned())
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

    /// Applies this sign-in's credentials to a realtime session configuration.
    ///
    /// A realtime session is authorized once and then runs for as long as the conversation lasts,
    /// so it cannot retry a refusal mid-call the way a request can — a token near its expiry is
    /// renewed before the session opens rather than after it breaks. A token with life left in it
    /// is used as it stands: refreshing rotates the refresh token, so renewing on every call would
    /// invalidate the sign-in that other Codex requests share.
    pub fn authorize_realtime_session(
        &self,
        config: &mut crate::RealtimeAudioSessionConfig,
    ) -> Result<(), LanguageModelError> {
        let tokens = match self.chatgpt_tokens() {
            Ok(tokens) if realtime_token_outlives_a_session(&tokens.access_token) => tokens,
            _ => self.refresh_chatgpt_tokens()?,
        };
        config.api_key = Some(tokens.access_token);
        if let Some(account_id) = tokens
            .account_id
            .as_deref()
            .map(str::trim)
            .filter(|account_id| !account_id.is_empty())
        {
            config
                .headers
                .insert("ChatGPT-Account-ID".to_string(), account_id.to_string());
        }
        if tokens.chatgpt_account_is_fedramp {
            config
                .headers
                .insert("X-OpenAI-Fedramp".to_string(), "true".to_string());
        }
        Ok(())
    }

    /// Runs a standalone web search through the Codex backend.
    ///
    /// The Codex backend does not take the Responses API `web_search` tool. It exposes a dedicated
    /// search endpoint that answers with rendered text plus the structured results behind it, so
    /// the query becomes a `search_query` command rather than a tool definition on a turn.
    pub fn web_search(
        &self,
        request: &OpenAiWebSearchRequest,
    ) -> Result<OpenAiWebSearchResponse, LanguageModelError> {
        let query = request.query.trim();
        if query.is_empty() {
            return Err(codex_search_error("web search query must not be empty"));
        }
        let model = request
            .model
            .as_deref()
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .unwrap_or(self.config.default_model.as_str())
            .to_string();
        let payload = codex_search_payload(request, query, &model);
        let mut tokens = self.chatgpt_tokens()?;
        let mut response = self.post_search_request(&tokens, &payload)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            tokens = self.refresh_chatgpt_tokens()?;
            response = self.post_search_request(&tokens, &payload)?;
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(codex_search_error(format!(
                "Codex web search failed: HTTP {status}: {body}"
            )));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| codex_search_error(source.to_string()))?;
        let text = value
            .get("output")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        Ok(OpenAiWebSearchResponse {
            model,
            text,
            sources: crate::openai::collect_web_search_sources(
                value
                    .get("results")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten(),
            ),
        })
    }

    fn post_search_request(
        &self,
        tokens: &CodexChatGptTokens,
        payload: &serde_json::Value,
    ) -> Result<reqwest::blocking::Response, LanguageModelError> {
        let url = format!(
            "{}/{CODEX_SEARCH_PATH}",
            self.config.api_base_url.trim_end_matches('/')
        );
        self.http
            .post(url)
            .headers(codex_json_headers(
                tokens,
                CODEX_PROVIDER_ID,
                &self.config.default_model,
            )?)
            .json(payload)
            .send()
            .map_err(|source| codex_search_error(source.to_string()))
    }

    /// Generates an image, or edits the supplied reference images, through the Codex backend.
    ///
    /// The ChatGPT sign-in is the credential here, so an expired access token is refreshed and the
    /// request retried once — the same recovery `list_models` and `usage` perform.
    pub fn generate_or_edit_image(
        &self,
        request: &OpenAiImageRequest,
    ) -> Result<OpenAiImageResponse, LanguageModelError> {
        let operation =
            image_request_operation(request, CODEX_PROVIDER_ID, CODEX_DEFAULT_IMAGE_MODEL)?;
        let payload = image_payload(request, CODEX_DEFAULT_IMAGE_MODEL);
        let mut tokens = self.chatgpt_tokens()?;
        let mut response = self.post_image_request(&tokens, operation, &payload)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            tokens = self.refresh_chatgpt_tokens()?;
            response = self.post_image_request(&tokens, operation, &payload)?;
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(codex_image_error(format!(
                "Codex image {operation} failed: HTTP {status}: {body}"
            )));
        }
        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| codex_image_error(source.to_string()))?;
        image_response(&value, CODEX_PROVIDER_ID, CODEX_DEFAULT_IMAGE_MODEL)
    }

    fn post_image_request(
        &self,
        tokens: &CodexChatGptTokens,
        operation: &str,
        payload: &serde_json::Value,
    ) -> Result<reqwest::blocking::Response, LanguageModelError> {
        let url = format!(
            "{}/images/{operation}",
            self.config.api_base_url.trim_end_matches('/')
        );
        self.http
            .post(url)
            .headers(codex_json_headers(
                tokens,
                CODEX_PROVIDER_ID,
                CODEX_DEFAULT_IMAGE_MODEL,
            )?)
            .json(payload)
            .send()
            .map_err(|source| codex_image_error(source.to_string()))
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
        let cancellation = LanguageModelCancellation::default();
        self.complete_streaming_with_cancellation(request, &cancellation, on_event)
    }

    fn complete_streaming_with_cancellation(
        &self,
        request: &CodexModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<CodexModelResponse, LanguageModelError> {
        if cancellation.is_cancelled() {
            return Err(codex_cancelled_error(&request.model, "before request"));
        }
        let tokens = self.chatgpt_tokens()?;
        let request = request.with_chatgpt_supported_model();
        let url = format!(
            "{}/responses",
            self.config.api_base_url.trim_end_matches('/')
        );
        let info = self.model_info(&request.model);
        let payload = openai_responses_codex_payload_with_model(&request, info.as_ref())?;
        wire::validate_model_options(&payload, info.as_ref())?;
        let response_model = request.model.clone();
        let worker_client = self.clone();
        let worker_cancellation = cancellation.clone();
        let (sender, receiver) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("sinomo-codex-stream".to_string())
            .spawn(move || {
                let result = worker_client.complete_streaming_worker(
                    &url,
                    &request,
                    tokens,
                    &payload,
                    &worker_cancellation,
                    &sender,
                );
                let _ = sender.send(CodexStreamingWorkerMessage::Finished(result));
            })
            .map_err(|source| {
                codex_provider_invocation_error(
                    &response_model,
                    format!("failed to start Codex streaming transport worker: {source}"),
                )
            })?;

        let mut completed = None;
        let mut emitted_error = false;
        while let Ok(message) = receiver.recv() {
            match message {
                CodexStreamingWorkerMessage::Event(event) => {
                    emitted_error |= matches!(event, LanguageModelStreamEvent::Error { .. });
                    on_event(event);
                }
                CodexStreamingWorkerMessage::Finished(result) => {
                    completed = Some(result);
                    break;
                }
            }
        }

        if worker.join().is_err() {
            return Err(codex_provider_invocation_error(
                &response_model,
                "Codex streaming transport worker panicked".to_string(),
            ));
        }
        let result = completed.ok_or_else(|| {
            codex_provider_invocation_error(
                &response_model,
                "Codex streaming transport worker closed without a result".to_string(),
            )
        })?;
        if let Err(error) = &result
            && !emitted_error
        {
            on_event(LanguageModelStreamEvent::Error {
                detail: error.to_string(),
            });
        }
        result
    }
}

enum CodexStreamingWorkerMessage {
    Event(LanguageModelStreamEvent),
    Finished(Result<CodexModelResponse, LanguageModelError>),
}

enum CodexHttpAttempt {
    Unauthorized,
    Finished(CodexModelResponse),
}

enum CodexHttpAttemptError {
    /// The transport failed before any decoded model event reached the caller, so replaying the
    /// request cannot duplicate caller-visible progress or a locally executed tool call.
    RetryableBeforeProgress(LanguageModelError),
    Terminal(LanguageModelError),
}

impl OpenAiResponsesCodexClient {
    fn complete_streaming_worker(
        &self,
        url: &str,
        request: &CodexModelRequest,
        mut tokens: CodexChatGptTokens,
        payload: &serde_json::Value,
        cancellation: &LanguageModelCancellation,
        sender: &mpsc::Sender<CodexStreamingWorkerMessage>,
    ) -> Result<CodexModelResponse, LanguageModelError> {
        if cancellation.is_cancelled() {
            return Err(codex_cancelled_error(&request.model, "before transport"));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|source| {
                codex_provider_invocation_error(
                    &request.model,
                    format!("failed to start Codex async transport runtime: {source}"),
                )
            })?;
        let async_http = {
            let _runtime = runtime.enter();
            reqwest::Client::builder()
                .connect_timeout(self.config.transport_health_timeout)
                .read_timeout(self.config.transport_health_timeout)
                .build()
                .map_err(|source| {
                    codex_provider_invocation_error(
                        &request.model,
                        format!("failed to configure Codex streaming transport: {source}"),
                    )
                })?
        };

        let mut transport_retries = 0;
        let mut refreshed_credentials = false;
        loop {
            let attempt = runtime.block_on(self.post_responses_streaming_attempt(
                &async_http,
                url,
                request,
                &tokens,
                payload,
                cancellation,
                sender,
            ));
            match attempt {
                Ok(CodexHttpAttempt::Finished(response)) => return Ok(response),
                Ok(CodexHttpAttempt::Unauthorized) if !refreshed_credentials => {
                    if cancellation.is_cancelled() {
                        return Err(codex_cancelled_error(
                            &request.model,
                            "before credential refresh",
                        ));
                    }
                    tokens = runtime
                        .block_on(self.refresh_chatgpt_tokens_async(&async_http, request))?;
                    refreshed_credentials = true;
                    if cancellation.is_cancelled() {
                        return Err(codex_cancelled_error(
                            &request.model,
                            "after credential refresh",
                        ));
                    }
                }
                Ok(CodexHttpAttempt::Unauthorized) => {
                    return Err(LanguageModelError::AuthenticationRequired {
                        provider: CODEX_PROVIDER_ID.to_string(),
                        detail: "Codex backend rejected refreshed ChatGPT credentials; run `sinomo providers auth codex` again".to_string(),
                    });
                }
                Err(CodexHttpAttemptError::Terminal(error)) => return Err(error),
                Err(CodexHttpAttemptError::RetryableBeforeProgress(error))
                    if transport_retries >= CODEX_STREAM_MAX_TRANSPORT_RETRIES =>
                {
                    return Err(error);
                }
                Err(CodexHttpAttemptError::RetryableBeforeProgress(_)) => {
                    let delay = codex_stream_retry_backoff(transport_retries);
                    transport_retries += 1;
                    runtime.block_on(wait_for_codex_retry_backoff(request, cancellation, delay))?;
                }
            }
        }
    }

    async fn post_responses_streaming_attempt(
        &self,
        http: &reqwest::Client,
        url: &str,
        request: &CodexModelRequest,
        tokens: &CodexChatGptTokens,
        payload: &serde_json::Value,
        cancellation: &LanguageModelCancellation,
        sender: &mpsc::Sender<CodexStreamingWorkerMessage>,
    ) -> Result<CodexHttpAttempt, CodexHttpAttemptError> {
        if cancellation.is_cancelled() {
            return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                &request.model,
                "before HTTP request",
            )));
        }
        let mut headers = codex_headers(tokens, CODEX_PROVIDER_ID, &request.model)
            .map_err(CodexHttpAttemptError::Terminal)?;
        if payload
            .pointer("/input/0/type")
            .and_then(serde_json::Value::as_str)
            == Some("additional_tools")
        {
            headers.insert(
                wire::RESPONSES_LITE_HEADER,
                HeaderValue::from_static("true"),
            );
        }
        let send = http.post(url).headers(headers).json(payload).send();
        tokio::pin!(send);
        let response = tokio::select! {
            _ = wait_for_codex_cancellation(cancellation) => {
                return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                    &request.model,
                    "while connecting or waiting for response headers",
                )));
            }
            response = &mut send => response.map_err(|source| {
                codex_http_attempt_transport_error(
                    &request.model,
                    "connecting or waiting for response headers",
                    self.config.transport_health_timeout,
                    source,
                    false,
                )
            })?,
        };
        if cancellation.is_cancelled() {
            return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                &request.model,
                "while waiting for response headers",
            )));
        }
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Ok(CodexHttpAttempt::Unauthorized);
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.map_err(|source| {
                CodexHttpAttemptError::Terminal(codex_transport_error(
                    &request.model,
                    "reading an error response body",
                    self.config.transport_health_timeout,
                    source,
                ))
            })?;
            return Err(CodexHttpAttemptError::Terminal(
                codex_provider_invocation_error(
                    &request.model,
                    format!("Codex backend Responses API failed: HTTP {status}: {body}"),
                ),
            ));
        }

        let is_event_stream = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"));
        let mut chunks = response.bytes_stream();
        if is_event_stream {
            let mut decoder = OpenAiResponsesCodexSseDecoder::default();
            let mut meaningful_progress = false;
            loop {
                let next_chunk = tokio::select! {
                    _ = wait_for_codex_cancellation(cancellation) => {
                        return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                            &request.model,
                            "during streaming response",
                        )));
                    }
                    chunk = chunks.next() => chunk,
                };
                let Some(chunk) = next_chunk else {
                    break;
                };
                let chunk = chunk.map_err(|source| {
                    codex_http_attempt_transport_error(
                        &request.model,
                        "waiting for streaming response-body progress",
                        self.config.transport_health_timeout,
                        source,
                        meaningful_progress,
                    )
                })?;
                let mut receiver_open = true;
                let decoded = decoder
                    .push(&chunk, &request.model, &mut |event| {
                        meaningful_progress = true;
                        if !cancellation.is_cancelled() {
                            receiver_open &= sender
                                .send(CodexStreamingWorkerMessage::Event(event))
                                .is_ok();
                        }
                    })
                    .map_err(CodexHttpAttemptError::Terminal)?;
                if cancellation.is_cancelled() {
                    return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                        &request.model,
                        "during streaming response",
                    )));
                }
                if let Some(response) = decoded {
                    return Ok(CodexHttpAttempt::Finished(response));
                }
                if !receiver_open {
                    return Err(CodexHttpAttemptError::Terminal(
                        codex_provider_invocation_error(
                            &request.model,
                            "Codex stream consumer closed".to_string(),
                        ),
                    ));
                }
            }
            if cancellation.is_cancelled() {
                return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                    &request.model,
                    "during streaming response",
                )));
            }
            let response = decoder.finish(&request.model, &mut |event| {
                meaningful_progress = true;
                let _ = sender.send(CodexStreamingWorkerMessage::Event(event));
            });
            let response = match response {
                Ok(response) => response,
                Err(error) if !meaningful_progress => {
                    return Err(CodexHttpAttemptError::RetryableBeforeProgress(error));
                }
                Err(error) => return Err(CodexHttpAttemptError::Terminal(error)),
            };
            return Ok(CodexHttpAttempt::Finished(response));
        }

        let mut body = Vec::new();
        loop {
            let next_chunk = tokio::select! {
                _ = wait_for_codex_cancellation(cancellation) => {
                    return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                        &request.model,
                        "during response body",
                    )));
                }
                chunk = chunks.next() => chunk,
            };
            let Some(chunk) = next_chunk else {
                break;
            };
            let chunk = chunk.map_err(|source| {
                codex_http_attempt_transport_error(
                    &request.model,
                    "waiting for response-body progress",
                    self.config.transport_health_timeout,
                    source,
                    false,
                )
            })?;
            body.extend_from_slice(&chunk);
        }
        if cancellation.is_cancelled() {
            return Err(CodexHttpAttemptError::Terminal(codex_cancelled_error(
                &request.model,
                "during response body",
            )));
        }
        let body = String::from_utf8(body).map_err(|source| {
            CodexHttpAttemptError::Terminal(codex_provider_invocation_error(
                &request.model,
                format!("Codex backend returned a non-UTF-8 response body: {source}"),
            ))
        })?;
        let response =
            parse_openai_responses_codex_body_with_events(&body, &request.model, &mut |event| {
                let _ = sender.send(CodexStreamingWorkerMessage::Event(event));
            })
            .map_err(CodexHttpAttemptError::Terminal)?;
        Ok(CodexHttpAttempt::Finished(response))
    }

    async fn refresh_chatgpt_tokens_async(
        &self,
        http: &reqwest::Client,
        request: &CodexModelRequest,
    ) -> Result<CodexChatGptTokens, LanguageModelError> {
        let current = self.chatgpt_tokens()?;
        if current.refresh_token.trim().is_empty() {
            return Err(LanguageModelError::AuthenticationRequired {
                provider: CODEX_PROVIDER_ID.to_string(),
                detail: "Codex ChatGPT auth is missing a refresh token; run `sinomo providers auth codex` again".to_string(),
            });
        }
        let issuer = self.config.auth_issuer.trim_end_matches('/');
        let response = http
            .post(format!("{issuer}/oauth/token"))
            .json(&serde_json::json!({
                "client_id": self.config.client_id.as_str(),
                "grant_type": "refresh_token",
                "refresh_token": current.refresh_token.clone(),
            }))
            .send()
            .await
            .map_err(|source| {
                codex_transport_error(
                    &request.model,
                    "refreshing ChatGPT credentials",
                    self.config.transport_health_timeout,
                    source,
                )
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(LanguageModelError::AuthenticationRequired {
                provider: CODEX_PROVIDER_ID.to_string(),
                detail: format!("Codex ChatGPT token refresh failed: HTTP {status}: {body}"),
            });
        }
        let refreshed = response
            .json::<CodexRefreshResponse>()
            .await
            .map_err(|source| {
                codex_transport_error(
                    &request.model,
                    "reading refreshed ChatGPT credentials",
                    self.config.transport_health_timeout,
                    source,
                )
            })?;
        let tokens = tokens_from_response(
            refreshed.id_token.unwrap_or(current.id_token),
            refreshed.access_token.unwrap_or(current.access_token),
            refreshed.refresh_token.unwrap_or(current.refresh_token),
            current.account_id,
        );
        self.save_chatgpt_tokens(tokens.clone())?;
        Ok(tokens)
    }

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
}

fn codex_transport_error(
    model: &str,
    phase: &str,
    health_timeout: Duration,
    source: reqwest::Error,
) -> LanguageModelError {
    let detail = if source.is_timeout() {
        format!(
            "Codex transport idle/unhealthy while {phase}: no network progress for {health_timeout:?}; this is a transport health timeout, not a total RPC deadline"
        )
    } else {
        format!("Codex transport failed while {phase}: {source}")
    };
    codex_provider_invocation_error(model, detail)
}

fn codex_http_attempt_transport_error(
    model: &str,
    phase: &str,
    health_timeout: Duration,
    source: reqwest::Error,
    meaningful_progress: bool,
) -> CodexHttpAttemptError {
    let retryable = !meaningful_progress
        && (source.is_timeout()
            || source.is_connect()
            || (source.is_body() && !source.is_decode())
            || (source.is_request()
                && !source.is_builder()
                && !source.is_redirect()
                && !source.is_status()));
    let error = codex_transport_error(model, phase, health_timeout, source);
    if retryable {
        CodexHttpAttemptError::RetryableBeforeProgress(error)
    } else {
        CodexHttpAttemptError::Terminal(error)
    }
}

fn codex_stream_retry_backoff(retry_index: usize) -> Duration {
    CODEX_STREAM_RETRY_BASE_DELAY.saturating_mul(1_u32 << retry_index.min(10))
}

async fn wait_for_codex_retry_backoff(
    request: &CodexModelRequest,
    cancellation: &LanguageModelCancellation,
    delay: Duration,
) -> Result<(), LanguageModelError> {
    let deadline = tokio::time::Instant::now() + delay;
    loop {
        if cancellation.is_cancelled() {
            return Err(codex_cancelled_error(
                &request.model,
                "during transport retry backoff",
            ));
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(());
        }
        tokio::time::sleep(remaining.min(CODEX_STREAM_CANCELLATION_POLL_INTERVAL)).await;
    }
}

async fn wait_for_codex_cancellation(cancellation: &LanguageModelCancellation) {
    while !cancellation.is_cancelled() {
        tokio::time::sleep(CODEX_STREAM_CANCELLATION_POLL_INTERVAL).await;
    }
}

fn codex_cancelled_error(model: &str, phase: &'static str) -> LanguageModelError {
    codex_provider_invocation_error(model, format!("Codex model invocation cancelled {phase}"))
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

    fn complete_streaming_with_cancellation(
        &self,
        request: &CodexModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<CodexModelResponse, LanguageModelError> {
        if cancellation.is_cancelled() {
            return Err(codex_cancelled_error(&request.model, "before request"));
        }
        let response = self.complete_streaming(request, &mut |event| {
            if !cancellation.is_cancelled() {
                on_event(event);
            }
        })?;
        if cancellation.is_cancelled() {
            return Err(codex_cancelled_error(&request.model, "during response"));
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
        let mut messages: Vec<CodexMessage> = if request.items.is_empty() {
            request
                .messages
                .iter()
                .filter(|message| message.role != LanguageModelRole::System)
                .map(CodexMessage::from_language_model_message)
                .collect()
        } else {
            codex_messages_from_request_items(
                &crate::provider_request_items(request, CODEX_PROVIDER_ID, &request.model),
                &request.tools,
            )
        };
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

    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
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

    pub fn list_model_catalog(&self) -> Result<Vec<CodexModelInfo>, LanguageModelError> {
        self.client.list_model_catalog()
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
            host_access: crate::LanguageModelHostAccess::NetworkOnly,
            capabilities: crate::LanguageModelProviderCapabilities::codex(),
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
        let cancellation = LanguageModelCancellation::default();
        self.complete_streaming_events_with_cancellation(request, &cancellation, on_event)
    }

    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.default_model, request);
        let context_request = if request.model == model {
            std::borrow::Cow::Borrowed(request)
        } else {
            std::borrow::Cow::Owned(LanguageModelRequest {
                model: model.clone(),
                ..request.clone()
            })
        };
        let mut messages = self.context_manager.prepare_messages(&context_request);
        if let Some(system_prompt) = self.prompt_builder.build_system_prompt(request) {
            messages.insert(0, CodexMessage::system(system_prompt));
        }
        let tools = codex_tool_descriptors(self.tools.descriptors(), &request.tools);

        for _ in 0..self.config.max_iterations {
            if cancellation.is_cancelled() {
                return Err(codex_cancelled_error(&model, "before model iteration"));
            }
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
                tool_choice: request.tool_choice.clone(),
                parallel_tool_calls: request.parallel_tool_calls,
            };
            let response = self.client.complete_streaming_with_cancellation(
                &codex_request,
                cancellation,
                on_event,
            )?;
            if response.tool_calls.is_empty() {
                return Ok(LanguageModelResponse {
                    provider: self.config.provider_id.clone(),
                    items: codex_language_model_response_items(
                        &response,
                        &self.config.provider_id,
                        &model,
                    ),
                    model,
                    content: response.content,
                    tool_calls: Vec::new(),
                    usage: response.usage,
                });
            }

            if response
                .tool_calls
                .iter()
                .any(|call| !self.tools.contains(&call.name))
            {
                return Ok(LanguageModelResponse {
                    provider: self.config.provider_id.clone(),
                    items: codex_language_model_response_items(
                        &response,
                        &self.config.provider_id,
                        &model,
                    ),
                    model,
                    content: response.content,
                    tool_calls: codex_language_model_tool_calls(&response.tool_calls),
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
                if tools
                    .iter()
                    .any(|tool| tool.name == call.name && tool.custom.is_some())
                {
                    messages.push(CodexMessage::raw_item(serde_json::json!({
                        "type": "custom_tool_call_output", "call_id": result.call_id,
                        "name": result.name, "output": result.content,
                    })));
                } else {
                    messages.push(CodexMessage::tool_result(result));
                }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<LanguageModelToolChoice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom: Option<crate::LanguageModelCustomToolSpec>,
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

#[cfg(test)]
fn extract_codex_model_ids(value: &serde_json::Value) -> Vec<String> {
    models::extract_model_catalog(value)
        .into_iter()
        .map(|info| info.slug)
        .collect()
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

#[cfg(test)]
fn openai_responses_codex_payload(request: &CodexModelRequest) -> serde_json::Value {
    openai_responses_codex_payload_with_model(request, builtin_model_info(&request.model))
        .expect("valid Codex test request")
}

fn openai_responses_codex_payload_with_model(
    request: &CodexModelRequest,
    info: Option<&CodexModelInfo>,
) -> Result<serde_json::Value, LanguageModelError> {
    let instructions = codex_request_instructions(request);
    let mut input: Vec<serde_json::Value> = request
        .messages
        .iter()
        .filter(|message| message.role != CodexRole::System)
        .map(openai_responses_input_item)
        .collect();
    let tools =
        crate::responses_tools::group_tools(request.tools.iter().map(wire::tool_spec), false);
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
        "include": ["reasoning.encrypted_content"],
    });
    if !tools.is_empty() {
        payload["tools"] = serde_json::json!(tools);
    }
    if let Some(tool_choice) = &request.tool_choice {
        payload["tool_choice"] = codex_responses_tool_choice(tool_choice, &request.tools);
        crate::responses_tools::split_name(&mut payload["tool_choice"]);
    }
    if let Some(parallel_tool_calls) = request.parallel_tool_calls {
        payload["parallel_tool_calls"] = serde_json::json!(parallel_tool_calls);
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["text"] = serde_json::json!({ "format": { "type": "json_object" } });
    }
    if let Some(effort) = request
        .options
        .reasoning_effort
        .and_then(|effort| effort.as_codex_cli_value())
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
    wire::apply_model_profile(&mut payload, info)?;
    Ok(payload)
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

fn codex_messages_from_request_items(
    items: &[LanguageModelRequestItem],
    tools: &[LanguageModelToolSpec],
) -> Vec<CodexMessage> {
    items
        .iter()
        .flat_map(|item| match item {
            LanguageModelRequestItem::ProviderContinuation { continuation } => continuation
                .items
                .iter()
                .cloned()
                .map(CodexMessage::raw_item)
                .collect(),
            _ => codex_message_from_request_item(item, tools)
                .map(|mut message| {
                    if let Some(item) = &mut message.raw_item {
                        crate::responses_tools::split_name(item);
                    }
                    message
                })
                .into_iter()
                .collect::<Vec<_>>(),
        })
        .collect()
}

fn codex_message_from_request_item(
    item: &LanguageModelRequestItem,
    tools: &[LanguageModelToolSpec],
) -> Option<CodexMessage> {
    match item {
        LanguageModelRequestItem::ProviderContinuation { .. } => None,
        LanguageModelRequestItem::Message { role, content } => {
            if content.trim().is_empty() {
                return None;
            }
            Some(CodexMessage::new(
                codex_role_from_language_model_role(role),
                content.clone(),
            ))
        }
        LanguageModelRequestItem::ToolCall {
            call_id,
            name,
            input,
        } if wire::custom_tool(name, tools).is_some() => {
            Some(CodexMessage::raw_item(serde_json::json!({
                "type": "custom_tool_call", "call_id": call_id, "name": name,
                "input": wire::custom_input(input, wire::custom_tool(name, tools).unwrap()),
            })))
        }
        LanguageModelRequestItem::ToolCall {
            call_id,
            name,
            input,
        } => Some(CodexMessage::raw_item(serde_json::json!({
            "type": "function_call",
            "call_id": call_id,
            "name": name,
            "arguments": codex_tool_value_text(input),
        }))),
        LanguageModelRequestItem::ToolCallOutput {
            call_id,
            name,
            output,
        } if wire::custom_tool(name, tools).is_some() => {
            Some(CodexMessage::raw_item(serde_json::json!({
                "type": "custom_tool_call_output", "call_id": call_id, "name": name,
                "output": codex_tool_value_text(output),
            })))
        }
        LanguageModelRequestItem::ToolCallOutput {
            call_id,
            name,
            output,
        } => Some(CodexMessage::tool_result(CodexToolResult {
            call_id: call_id.clone(),
            name: name.clone(),
            content: codex_tool_value_text(output),
            is_error: false,
            metadata: serde_json::Value::Null,
        })),
        LanguageModelRequestItem::Context { label, content } => {
            if content.trim().is_empty() {
                return None;
            }
            Some(CodexMessage::user(format!("{label}:\n{content}")))
        }
    }
}

fn codex_role_from_language_model_role(role: &LanguageModelRole) -> CodexRole {
    match role {
        LanguageModelRole::System => CodexRole::System,
        LanguageModelRole::User => CodexRole::User,
        LanguageModelRole::Assistant => CodexRole::Assistant,
    }
}

fn codex_tool_descriptors(
    mut descriptors: Vec<CodexToolDescriptor>,
    tools: &[LanguageModelToolSpec],
) -> Vec<CodexToolDescriptor> {
    for tool in tools {
        if descriptors
            .iter()
            .any(|descriptor| descriptor.name == tool.name)
        {
            continue;
        }
        descriptors.push(CodexToolDescriptor {
            name: tool.name.clone(),
            description: tool.description.clone(),
            parameters: normalized_codex_tool_input_schema(tool),
            custom: wire::custom_tool(&tool.name, tools).cloned(),
        });
    }
    descriptors
}

fn normalized_codex_tool_input_schema(tool: &LanguageModelToolSpec) -> serde_json::Value {
    if tool.input_schema.is_null() {
        serde_json::json!({
            "type": "object",
            "properties": {},
        })
    } else {
        tool.input_schema.clone()
    }
}

fn codex_tool_value_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn codex_responses_tool_choice(
    tool_choice: &LanguageModelToolChoice,
    tools: &[CodexToolDescriptor],
) -> serde_json::Value {
    match tool_choice {
        LanguageModelToolChoice::Auto => serde_json::json!("auto"),
        LanguageModelToolChoice::None => serde_json::json!("none"),
        LanguageModelToolChoice::Required => serde_json::json!("required"),
        LanguageModelToolChoice::Tool { name } => {
            serde_json::json!({ "type": if tools.iter().any(|tool| tool.name == *name && tool.custom.is_some()) { "custom" } else { "function" }, "name": name })
        }
    }
}

fn codex_language_model_response_items(
    response: &CodexModelResponse,
    provider: &str,
    model: &str,
) -> Vec<LanguageModelResponseItem> {
    let mut items = response
        .tool_calls
        .iter()
        .map(|call| LanguageModelResponseItem::ToolCall {
            call: codex_language_model_tool_call(call),
        })
        .collect::<Vec<_>>();
    if !response.continuation_items.is_empty() {
        items.push(crate::response_continuation(
            provider,
            model,
            response.continuation_items.clone(),
            &codex_language_model_tool_calls(&response.tool_calls),
        ));
    }
    if !response.content.trim().is_empty() {
        items.push(LanguageModelResponseItem::Message {
            role: LanguageModelRole::Assistant,
            content: response.content.clone(),
        });
    }
    items
}

fn codex_language_model_tool_calls(calls: &[CodexToolCall]) -> Vec<LanguageModelToolCall> {
    calls.iter().map(codex_language_model_tool_call).collect()
}

fn codex_language_model_tool_call(call: &CodexToolCall) -> LanguageModelToolCall {
    LanguageModelToolCall {
        call_id: Some(call.call_id.clone()),
        name: call.name.clone(),
        input: call.input.clone(),
        summary: None,
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
                Some("function_call" | "custom_tool_call" | "reasoning" | "message" | "compaction")
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
    let mut decoder = OpenAiResponsesCodexSseDecoder::default();
    if let Some(response) = decoder.push(body.as_bytes(), model, on_event)? {
        return Ok(response);
    }
    decoder.finish(model, on_event)
}

#[derive(Default)]
struct OpenAiResponsesCodexSseDecoder {
    pending: Vec<u8>,
    output: Vec<serde_json::Value>,
    output_text: String,
    saw_output_text_delta: bool,
    saw_reasoning_delta: bool,
    stream_events: Vec<LanguageModelStreamEvent>,
}

impl OpenAiResponsesCodexSseDecoder {
    fn push(
        &mut self,
        bytes: &[u8],
        model: &str,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<Option<CodexModelResponse>, LanguageModelError> {
        self.pending.extend_from_slice(bytes);
        while let Some((block_end, delimiter_len)) = sse_block_boundary(&self.pending) {
            let block = self.pending[..block_end].to_vec();
            self.pending.drain(..block_end + delimiter_len);
            let block = String::from_utf8(block).map_err(|source| {
                codex_provider_invocation_error(
                    model,
                    format!("Codex stream contained a non-UTF-8 event: {source}"),
                )
            })?;
            if let Some(response) = self.process_block(&block, model, on_event)? {
                return Ok(Some(response));
            }
        }
        Ok(None)
    }

    fn finish(
        mut self,
        model: &str,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<CodexModelResponse, LanguageModelError> {
        if !self.pending.is_empty() {
            let pending = std::mem::take(&mut self.pending);
            let block = String::from_utf8(pending).map_err(|source| {
                codex_provider_invocation_error(
                    model,
                    format!("Codex stream contained a non-UTF-8 event: {source}"),
                )
            })?;
            if let Some(response) = self.process_block(&block, model, on_event)? {
                return Ok(response);
            }
        }

        if !self.output.is_empty() || !self.output_text.is_empty() {
            let mut response = serde_json::json!({ "output": self.output });
            if !self.output_text.is_empty() {
                response["output_text"] = serde_json::json!(self.output_text);
            }
            let mut parsed = parse_openai_responses_codex_response(&response);
            if self
                .stream_events
                .iter()
                .all(|event| !matches!(event, LanguageModelStreamEvent::Finished { .. }))
            {
                let stream_event = LanguageModelStreamEvent::Finished {
                    finish_reason: None,
                };
                on_event(stream_event.clone());
                self.stream_events.push(stream_event);
            }
            parsed.stream_events = self.stream_events;
            return Ok(parsed);
        }

        Err(codex_provider_invocation_error(
            model,
            "Codex backend stream closed before response.completed".to_string(),
        ))
    }

    fn process_block(
        &mut self,
        block: &str,
        model: &str,
        on_event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<Option<CodexModelResponse>, LanguageModelError> {
        let Some(event) = parse_sse_event(block, model)? else {
            return Ok(None);
        };
        let kind = event
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();

        match kind {
            "response.output_item.done" => {
                if let Some(item) = event.get("item") {
                    for stream_event in stream_events_from_openai_output_item(item) {
                        if self.saw_output_text_delta
                            && matches!(&stream_event, LanguageModelStreamEvent::TextDelta { .. })
                        {
                            continue;
                        }
                        if self.saw_reasoning_delta
                            && matches!(
                                &stream_event,
                                LanguageModelStreamEvent::ReasoningSummary { .. }
                            )
                        {
                            continue;
                        }
                        on_event(stream_event.clone());
                        self.stream_events.push(stream_event);
                    }
                    self.output.push(item.clone());
                }
            }
            "response.output_text.delta" => {
                if let Some(delta) = event.get("delta").and_then(serde_json::Value::as_str) {
                    self.saw_output_text_delta = true;
                    self.output_text.push_str(delta);
                    let stream_event = LanguageModelStreamEvent::TextDelta {
                        delta: delta.to_string(),
                    };
                    on_event(stream_event.clone());
                    self.stream_events.push(stream_event);
                }
            }
            "response.reasoning_summary_text.delta"
            | "response.reasoning_summary.delta"
            | "response.reasoning_text.delta" => {
                if let Some(delta) = event.get("delta").and_then(serde_json::Value::as_str)
                    && !delta.is_empty()
                {
                    self.saw_reasoning_delta = true;
                    let stream_event = LanguageModelStreamEvent::ReasoningDelta {
                        delta: delta.to_string(),
                    };
                    on_event(stream_event.clone());
                    self.stream_events.push(stream_event);
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
                if missing_or_empty_output && !self.output.is_empty() {
                    response["output"] = serde_json::json!(self.output);
                }
                if response.get("output_text").is_none() && !self.output_text.is_empty() {
                    response["output_text"] = serde_json::json!(self.output_text);
                }
                let mut parsed = parse_openai_responses_codex_response(&response);
                if let Some(usage) = &parsed.usage {
                    let stream_event = LanguageModelStreamEvent::Usage {
                        usage: usage.clone(),
                    };
                    on_event(stream_event.clone());
                    self.stream_events.push(stream_event);
                }
                let stream_event = LanguageModelStreamEvent::Finished {
                    finish_reason: openai_responses_finish_reason(&response),
                };
                on_event(stream_event.clone());
                self.stream_events.push(stream_event);
                parsed.stream_events = std::mem::take(&mut self.stream_events);
                return Ok(Some(parsed));
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
        Ok(None)
    }
}

fn sse_block_boundary(bytes: &[u8]) -> Option<(usize, usize)> {
    let lf = bytes.windows(2).position(|window| window == b"\n\n");
    let crlf = bytes.windows(4).position(|window| window == b"\r\n\r\n");
    let cr = bytes.windows(2).position(|window| window == b"\r\r");
    [(lf, 2), (crlf, 4), (cr, 2)]
        .into_iter()
        .filter_map(|(position, delimiter_len)| position.map(|position| (position, delimiter_len)))
        .min_by_key(|(position, _)| *position)
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
        "function_call" | "custom_tool_call" => parse_function_call_item(item)
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
    let kind = item.get("type").and_then(serde_json::Value::as_str);
    if !matches!(kind, Some("function_call" | "custom_tool_call")) {
        return None;
    }
    let arguments = if kind == Some("custom_tool_call") {
        item.get("input")
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    } else {
        item.get("arguments")
            .and_then(serde_json::Value::as_str)
            .and_then(|arguments| serde_json::from_str(arguments).ok())
            .unwrap_or(serde_json::Value::Null)
    };
    Some(CodexToolCall {
        call_id: item
            .get("call_id")
            .or_else(|| item.get("id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        name: crate::responses_tools::qualify_name(item)?,
        input: arguments,
    })
}

#[cfg(test)]
#[path = "codex/compatibility_tests.rs"]
mod compatibility_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        OpenAiWebSearchSource, PROVIDER_DEFAULT_MODEL, ProviderReasoningEffort,
        ProviderTextVerbosity,
    };
    use std::cell::RefCell;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Instant;

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

    fn transport_request() -> CodexModelRequest {
        CodexModelRequest {
            model: CODEX_DEFAULT_MODEL.to_string(),
            messages: vec![CodexMessage::user("hello")],
            tools: Vec::new(),
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
            options: LanguageModelProviderOptions::default(),
            tool_choice: None,
            parallel_tool_calls: None,
        }
    }

    pub(super) fn transport_client(
        base_url: &str,
        health_timeout: Duration,
        dir: &sinomo_testdir::TestDir,
    ) -> OpenAiResponsesCodexClient {
        let credentials_path = Some(dir.join("credentials.json"));
        save_codex_chatgpt_tokens(
            &credentials_path,
            tokens_from_response(
                fake_chatgpt_id_token("transport-account", false),
                "transport-access".to_string(),
                "transport-refresh".to_string(),
                None,
            ),
        )
        .expect("save transport test credentials");
        OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
            api_base_url: base_url.to_string(),
            auth_issuer: base_url.to_string(),
            credentials_path,
            transport_health_timeout: health_timeout,
            ..OpenAiResponsesCodexConfig::default()
        })
    }

    pub(super) fn test_server(
        handler: impl FnOnce(TcpListener) + Send + 'static,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let worker = thread::spawn(move || handler(listener));
        (format!("http://{address}"), worker)
    }

    /// An idle window long enough that scheduling noise cannot trip it.
    ///
    /// Most of these tests are about payload shape, retries or cancellation, not about the health
    /// timeout. Giving those a window measured in tens of seconds stops a server thread descheduled on
    /// a busy machine from looking like a stalled stream — a one or two second window was routinely
    /// beaten while this repository was compiling. The tests that *are* about the timeout set their
    /// own, deliberately.
    const UNTESTED_IDLE_WINDOW: Duration = Duration::from_secs(30);

    /// How long a test server waits for a connection that a test says should arrive.
    ///
    /// Generous, because these tests share a machine with compilation and can be descheduled for a
    /// long time; the point of the bound is to fail rather than to be prompt.
    const TEST_SERVER_ACCEPT_TIMEOUT: Duration = Duration::from_secs(20);

    /// Accepts the next connection, panicking if none arrives.
    ///
    /// `TcpListener::accept` blocks with no timeout, so a server expecting more requests than the
    /// client makes hangs its thread forever — and because the test then joins that thread, the whole
    /// test binary wedges instead of reporting a failure. Nothing times it out and nothing kills it,
    /// so a run can sit for hours. Polling to a deadline turns that into a legible assertion naming
    /// the request that never came.
    pub(super) fn accept_expected(listener: &TcpListener, expectation: &str) -> TcpStream {
        listener
            .set_nonblocking(true)
            .expect("nonblocking test listener");
        let deadline = Instant::now() + TEST_SERVER_ACCEPT_TIMEOUT;
        let stream = loop {
            match listener.accept() {
                // Linux does not pass O_NONBLOCK to the accepted socket, so the stream is blocking
                // and callers can set their own read timeouts as before.
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "no client connected within {TEST_SERVER_ACCEPT_TIMEOUT:?} while waiting to \
                         {expectation}; the client made fewer requests than this server expects"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("failed to {expectation}: {error}"),
            }
        };
        listener
            .set_nonblocking(false)
            .expect("blocking test listener");
        stream
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        read_http_request_with_body(stream).0
    }

    /// Reads a request and keeps its body so payload-shaping assertions can inspect it.
    fn read_http_request_with_body(stream: &mut TcpStream) -> (String, String) {
        let (headers, body) = read_http_request_parts(stream);
        (
            headers.lines().next().expect("request line").to_string(),
            body,
        )
    }

    pub(super) fn read_http_request_parts(stream: &mut TcpStream) -> (String, String) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("request read timeout");
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut buffer).expect("read request");
            assert!(read > 0, "request closed before headers");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]).to_string();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).expect("read request body");
            assert!(read > 0, "request closed before body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        (
            headers,
            String::from_utf8_lossy(&bytes[header_end..]).to_string(),
        )
    }

    fn write_chunked_headers(stream: &mut TcpStream, content_type: &str) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
        )
        .expect("write response headers");
        stream.flush().expect("flush response headers");
    }

    /// Message used when a write fails before the response is semantically complete.
    ///
    /// Up to that point a closed socket means the client abandoned the stream early, which is worth
    /// failing on. Naming it keeps the failure legible: otherwise the run shows an opaque IO panic
    /// from a detached thread beside the test's own assertion and the cause has to be guessed.
    const CLIENT_LEFT: &str = "the client stopped reading before the server finished responding";

    fn write_http_chunk(stream: &mut TcpStream, chunk: &str) {
        write!(stream, "{:x}\r\n{chunk}\r\n", chunk.len()).expect(CLIENT_LEFT);
        stream.flush().expect(CLIENT_LEFT);
    }

    /// Writes the terminating zero-length chunk, tolerating a client that has already finished.
    ///
    /// The transport returns as soon as it decodes `response.completed`, and dropping the body closes
    /// the socket. So this last write is inherently racing a client that already has everything it
    /// needs, and losing that race is correct client behaviour rather than a server error — treating
    /// it as one made every streaming test fail intermittently on a busy machine.
    fn finish_http_chunks(stream: &mut TcpStream) {
        match stream.write_all(b"0\r\n\r\n").and_then(|()| stream.flush()) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ) => {}
            Err(error) => panic!("failed to terminate the chunked response: {error}"),
        }
    }

    fn write_json_response(stream: &mut TcpStream, status: &str, body: &str) {
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .expect("write JSON response");
        stream.flush().expect("flush JSON response");
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
    fn codex_returns_request_scoped_tool_calls_to_caller() {
        let requests = Rc::new(RefCell::new(Vec::new()));
        let client = FixtureCodexClient {
            requests: Rc::clone(&requests),
            responses: RefCell::new(vec![CodexModelResponse {
                content: String::new(),
                tool_calls: vec![CodexToolCall {
                    call_id: "call_echo".to_string(),
                    name: "echo".to_string(),
                    input: serde_json::json!({ "text": "hello" }),
                }],
                continuation_items: vec![serde_json::json!({
                    "type": "function_call",
                    "call_id": "call_echo",
                    "name": "echo",
                    "arguments": "{\"text\":\"hello\"}"
                })],
                usage: None,
                stream_events: Vec::new(),
            }]),
        };
        let provider = CodexProvider::new(
            CodexConfig::default(),
            client,
            CodexToolRegistry::new(),
            DefaultCodexPromptBuilder::default(),
            DefaultCodexContextManager::default(),
        );
        let mut request = request();
        request.tools = vec![LanguageModelToolSpec {
            name: "echo".to_string(),
            description: "Echo text.".to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "text": { "type": "string" }
                },
                "required": ["text"]
            }),
            output_schema: serde_json::Value::Null,
            supports_parallel: false,
            provider_specs: Vec::new(),
        }];
        request.tool_choice = Some(LanguageModelToolChoice::Tool {
            name: "echo".to_string(),
        });
        request.parallel_tool_calls = Some(false);

        let response = provider.complete(&request).expect("codex response");

        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].call_id.as_deref(), Some("call_echo"));
        assert_eq!(response.tool_calls[0].name, "echo");
        assert!(response.items.iter().any(|item| matches!(
            item,
            LanguageModelResponseItem::ToolCall { call }
                if call.call_id.as_deref() == Some("call_echo") && call.name == "echo"
        )));
        let requests = requests.borrow();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].tools[0].name, "echo");
        assert_eq!(
            requests[0].tool_choice,
            Some(LanguageModelToolChoice::Tool {
                name: "echo".to_string()
            })
        );
        assert_eq!(requests[0].parallel_tool_calls, Some(false));
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
    fn context_manager_preserves_native_tool_transcript_items() {
        let mut request = request();
        request.items = vec![
            LanguageModelRequestItem::message(LanguageModelRole::User, "Call echo."),
            LanguageModelRequestItem::tool_call(
                "call_echo",
                "echo",
                serde_json::json!({ "text": "hello" }),
            ),
            LanguageModelRequestItem::tool_call_output(
                "call_echo",
                "echo",
                serde_json::json!({ "text": "hello" }),
            ),
        ];

        let messages = DefaultCodexContextManager::default().prepare_messages(&request);

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].role, CodexRole::User);
        assert_eq!(
            messages[1].raw_item.as_ref().unwrap()["type"],
            "function_call"
        );
        assert_eq!(
            messages[1].raw_item.as_ref().unwrap()["call_id"],
            "call_echo"
        );
        assert_eq!(messages[2].role, CodexRole::Tool);
        assert_eq!(messages[2].tool_call_id.as_deref(), Some("call_echo"));
        assert_eq!(messages[2].content, "{\"text\":\"hello\"}");
    }

    #[test]
    fn openai_responses_payload_preserves_tools_and_tool_outputs() {
        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: "gpt-5.5".to_string(),
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
            tool_choice: Some(LanguageModelToolChoice::Tool {
                name: "read_context".to_string(),
            }),
            parallel_tool_calls: Some(false),
        });

        assert_eq!(payload["instructions"], "system prompt");
        assert_eq!(payload["model"], "gpt-5.5");
        assert_eq!(payload["tools"][0]["name"], "read_context");
        assert!(
            payload["input"][0]["content"]
                .as_str()
                .expect("json reminder")
                .contains("JSON")
        );
        assert_eq!(payload["input"][2]["type"], "reasoning");
        assert_eq!(payload["input"][3]["type"], "function_call_output");
        assert_eq!(payload["tool_choice"]["type"], "function");
        assert_eq!(payload["tool_choice"]["name"], "read_context");
        assert_eq!(payload["parallel_tool_calls"], false);
        assert!(payload.get("temperature").is_none());
        assert!(payload.get("max_output_tokens").is_none());
        assert_eq!(payload["text"]["format"]["type"], "json_object");
        assert_eq!(payload["stream"], true);
    }

    #[test]
    fn openai_responses_json_payload_does_not_duplicate_json_reminder() {
        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: "gpt-5.5".to_string(),
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
            tool_choice: None,
            parallel_tool_calls: None,
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
            tool_choice: None,
            parallel_tool_calls: None,
        };

        assert_eq!(
            request.with_chatgpt_supported_model().model,
            CODEX_DEFAULT_MODEL
        );
    }

    #[test]
    fn openai_responses_payload_includes_default_instructions_without_system_prompt() {
        let payload = openai_responses_codex_payload(&CodexModelRequest {
            model: "gpt-5.5".to_string(),
            messages: vec![CodexMessage::user("hello")],
            tools: Vec::new(),
            temperature: None,
            max_output_tokens: None,
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
            options: LanguageModelProviderOptions::default(),
            tool_choice: None,
            parallel_tool_calls: None,
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
            tool_choice: None,
            parallel_tool_calls: None,
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
        assert_eq!(parsed.continuation_items.len(), 3);
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
    fn codex_transport_delivers_sse_events_before_response_completion() {
        let (release_sender, release_receiver) = mpsc::channel();
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept responses request");
            assert!(read_http_request(&mut stream).starts_with("POST /responses "));
            write_chunked_headers(&mut stream, "text/event-stream");
            write_http_chunk(
                &mut stream,
                "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"live\"}\n\n",
            );
            release_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("test observed incremental event");
            write_http_chunk(
                &mut stream,
                "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
            );
            finish_http_chunks(&mut stream);
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-stream-incremental");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);
        let (event_sender, event_receiver) = mpsc::channel();
        let invocation = thread::spawn(move || {
            client.complete_streaming(&transport_request(), &mut |event| {
                event_sender.send(event).expect("send observed event");
            })
        });

        let event = event_receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("receive event before completion is released");
        assert_eq!(
            event,
            LanguageModelStreamEvent::TextDelta {
                delta: "live".to_string()
            }
        );
        release_sender.send(()).expect("release completion");
        let response = invocation
            .join()
            .expect("invocation thread")
            .expect("streaming response");
        server.join().expect("server thread");
        assert_eq!(response.content, "live");
    }

    #[test]
    fn codex_transport_resets_idle_timeout_on_stream_progress() {
        // Two conditions have to hold at once: every gap must stay inside one idle window, and the
        // gaps together must exceed one, so that finishing proves the window was reset rather than
        // merely never reached. Satisfying both with few large gaps leaves little slack — the original
        // 40ms gaps against a 120ms window left 80ms, and a machine busy compiling overshoots a sleep
        // by more than that. More, smaller gaps satisfy the same two conditions while leaving most of
        // the window as slack on each one.
        let health_timeout = Duration::from_millis(600);
        let progress_gap = Duration::from_millis(50);
        const PROGRESS_STEPS: usize = 15;
        let expected = (0..PROGRESS_STEPS)
            .map(|step| char::from(b'0' + (step % 10) as u8))
            .collect::<String>();
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept responses request");
            read_http_request(&mut stream);
            write_chunked_headers(&mut stream, "text/event-stream");
            for step in 0..PROGRESS_STEPS {
                let delta = char::from(b'0' + (step % 10) as u8);
                write_http_chunk(
                    &mut stream,
                    &format!(
                        "event: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"delta\":\"{delta}\"}}\n\n"
                    ),
                );
                thread::sleep(progress_gap);
            }
            write_http_chunk(
                &mut stream,
                "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
            );
            finish_http_chunks(&mut stream);
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-stream-progress");
        let client = transport_client(&base_url, health_timeout, &dir);
        let started = Instant::now();

        let response = client
            .complete_streaming(&transport_request(), &mut |_| {})
            .expect("progressing stream outlives one idle window");

        server.join().expect("server thread");
        assert!(started.elapsed() > health_timeout);
        assert_eq!(response.content, expected);
    }

    #[test]
    fn codex_transport_cancels_a_continuously_progressing_stream_promptly() {
        /// What the server would stream if nobody stopped it.
        const OFFERED_CHUNKS: usize = 200;
        /// Cadence of those chunks, so the whole stream takes `OFFERED_CHUNKS` of these.
        const CHUNK_INTERVAL: Duration = Duration::from_millis(10);

        // The server streams until the client goes away and counts what it managed to send. That
        // count is the evidence of promptness: a client that stopped early is one the server could
        // not keep feeding.
        let written = Arc::new(AtomicUsize::new(0));
        let server_written = Arc::clone(&written);
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept responses request");
            read_http_request(&mut stream);
            write_chunked_headers(&mut stream, "text/event-stream");
            for _ in 0..OFFERED_CHUNKS {
                let chunk = "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n";
                if write!(stream, "{:x}\r\n{chunk}\r\n", chunk.len()).is_err()
                    || stream.flush().is_err()
                {
                    break;
                }
                server_written.fetch_add(1, Ordering::SeqCst);
                thread::sleep(CHUNK_INTERVAL);
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-stream-cancel");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = {
            let cancelled = Arc::clone(&cancelled);
            LanguageModelCancellation::new(move || cancelled.load(Ordering::SeqCst))
        };
        let started = Instant::now();

        // Cancelling from the first delivered event ties cancellation to the stream having started,
        // rather than to a stopwatch. A timer here raced the connection itself: on a loaded machine
        // its deadline can pass before the request is even sent, and the transport then reports
        // cancellation without ever connecting — which left the server's `accept` waiting forever.
        let error = client
            .complete_streaming_with_cancellation(&transport_request(), &cancellation, &mut |_| {
                cancelled.store(true, Ordering::SeqCst);
            })
            .expect_err("cancelled stream must stop");

        server.join().expect("server thread");
        assert!(error.to_string().contains("cancelled"), "{error}");
        let written = written.load(Ordering::SeqCst);
        assert!(
            written < OFFERED_CHUNKS,
            "the client took all {OFFERED_CHUNKS} chunks instead of cancelling partway through"
        );
        // A backstop rather than the assertion that carries the test: running to completion would
        // take the full cadence, so stopping inside it is prompt however loaded the machine is.
        assert!(
            started.elapsed() < CHUNK_INTERVAL * OFFERED_CHUNKS as u32,
            "cancellation took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn codex_transport_reports_stalled_response_body_as_idle_unhealthy() {
        let health_timeout = Duration::from_millis(80);
        let (base_url, server) = test_server(move |listener| {
            for _ in 0..=CODEX_STREAM_MAX_TRANSPORT_RETRIES {
                let mut stream = accept_expected(&listener, "accept responses request");
                read_http_request(&mut stream);
                write_chunked_headers(&mut stream, "text/event-stream");
                thread::sleep(Duration::from_millis(100));
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-stream-stall");
        let client = transport_client(&base_url, health_timeout, &dir);
        let mut events = Vec::new();

        let error = client
            .complete_streaming(&transport_request(), &mut |event| events.push(event))
            .expect_err("stalled stream must fail");

        server.join().expect("server thread");
        let detail = error.to_string();
        assert!(detail.contains("idle/unhealthy"), "{detail}");
        assert!(detail.contains("80ms"), "{detail}");
        assert!(detail.contains("not a total RPC deadline"), "{detail}");
        assert!(events.iter().any(|event| {
            matches!(event, LanguageModelStreamEvent::Error { detail } if detail.contains("idle/unhealthy"))
        }));
    }

    #[test]
    fn codex_transport_accepts_non_streaming_json_response() {
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept responses request");
            read_http_request(&mut stream);
            write_json_response(
                &mut stream,
                "200 OK",
                "{\"output_text\":\"json-ok\",\"output\":[],\"status\":\"completed\"}",
            );
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-json-response");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);
        let mut events = Vec::new();

        let response = client
            .complete_streaming(&transport_request(), &mut |event| events.push(event))
            .expect("JSON response");

        server.join().expect("server thread");
        assert_eq!(response.content, "json-ok");
        assert!(events.iter().any(|event| {
            matches!(event, LanguageModelStreamEvent::Finished { finish_reason } if finish_reason.as_deref() == Some("completed"))
        }));
    }

    #[test]
    fn codex_transport_preserves_http_error_status_and_body() {
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept responses request");
            read_http_request(&mut stream);
            write_json_response(
                &mut stream,
                "503 Service Unavailable",
                "{\"error\":\"maintenance\"}",
            );
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-http-error");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        let error = client
            .complete_streaming(&transport_request(), &mut |_| {})
            .expect_err("provider error response");

        server.join().expect("server thread");
        let detail = error.to_string();
        assert!(detail.contains("503 Service Unavailable"), "{detail}");
        assert!(detail.contains("maintenance"), "{detail}");
    }

    #[test]
    fn codex_transport_refreshes_once_after_unauthorized_response() {
        let paths = Arc::new(Mutex::new(Vec::new()));
        let server_paths = Arc::clone(&paths);
        let (base_url, server) = test_server(move |listener| {
            for index in 0..3 {
                let mut stream = accept_expected(&listener, "accept transport request");
                let request_line = read_http_request(&mut stream);
                server_paths
                    .lock()
                    .expect("paths lock")
                    .push(request_line.clone());
                match index {
                    0 => write_json_response(&mut stream, "401 Unauthorized", "{}"),
                    1 => write_json_response(
                        &mut stream,
                        "200 OK",
                        "{\"access_token\":\"refreshed-access\",\"refresh_token\":\"refreshed-refresh\"}",
                    ),
                    _ => write_json_response(
                        &mut stream,
                        "200 OK",
                        "{\"output_text\":\"retried-ok\",\"output\":[],\"status\":\"completed\"}",
                    ),
                }
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-unauthorized-retry");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        let response = client
            .complete_streaming(&transport_request(), &mut |_| {})
            .expect("response succeeds after refresh");

        server.join().expect("server thread");
        assert_eq!(response.content, "retried-ok");
        let paths = paths.lock().expect("paths lock");
        assert!(paths[0].starts_with("POST /responses "), "{paths:?}");
        assert!(paths[1].starts_with("POST /oauth/token "), "{paths:?}");
        assert!(paths[2].starts_with("POST /responses "), "{paths:?}");
        assert_eq!(
            client
                .chatgpt_tokens()
                .expect("refreshed credentials")
                .access_token,
            "refreshed-access"
        );
    }

    #[test]
    fn codex_web_search_posts_search_command_and_maps_results_to_sources() {
        let request_body = Arc::new(Mutex::new(String::new()));
        let server_body = Arc::clone(&request_body);
        let paths = Arc::new(Mutex::new(Vec::new()));
        let server_paths = Arc::clone(&paths);
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept search request");
            let (request_line, body) = read_http_request_with_body(&mut stream);
            server_paths.lock().expect("paths lock").push(request_line);
            *server_body.lock().expect("body lock") = body;
            write_json_response(
                &mut stream,
                "200 OK",
                "{\"encrypted_output\":\"ciphertext\",\"output\":\"rendered answer\",\"results\":[{\"type\":\"text_result\",\"ref_id\":\"turn0search0\",\"url\":\"https://example.com/a\",\"title\":\"Example A\"},{\"type\":\"text_result\",\"url\":\"https://example.com/b\"},{\"type\":\"text_result\",\"url\":\"https://example.com/a\"},{\"type\":\"image_result\"}]}",
            );
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-web-search");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        let response = client
            .web_search(&OpenAiWebSearchRequest {
                query: "  latest rust release  ".to_string(),
                model: None,
                allowed_domains: vec!["rust-lang.org".to_string(), "  ".to_string()],
                context_size: OpenAiWebSearchContextSize::High,
            })
            .expect("codex web search succeeds");

        server.join().expect("server thread");
        let paths = paths.lock().expect("paths lock");
        assert!(paths[0].starts_with("POST /alpha/search "), "{paths:?}");
        let payload = serde_json::from_str::<serde_json::Value>(
            &request_body.lock().expect("body lock").clone(),
        )
        .expect("search payload is JSON");
        assert_eq!(
            payload["commands"]["search_query"][0]["q"],
            "latest rust release"
        );
        assert_eq!(
            payload["commands"]["search_query"][0]["domains"],
            serde_json::json!(["rust-lang.org"])
        );
        assert_eq!(payload["input"], "latest rust release");
        assert_eq!(payload["model"], CODEX_DEFAULT_MODEL);
        assert_eq!(payload["settings"]["search_context_size"], "high");
        assert_eq!(
            payload["settings"]["allowed_callers"],
            serde_json::json!(["direct"])
        );
        assert_eq!(payload["settings"]["external_web_access"], true);
        assert_eq!(
            payload["settings"]["filters"]["allowed_domains"],
            serde_json::json!(["rust-lang.org"])
        );
        assert!(
            payload["id"].as_str().is_some_and(|id| !id.is_empty()),
            "{payload}"
        );

        assert_eq!(response.text, "rendered answer");
        assert_eq!(response.model, CODEX_DEFAULT_MODEL);
        // Results without a URL are dropped and repeats collapse into one source.
        assert_eq!(
            response.sources,
            vec![
                OpenAiWebSearchSource {
                    url: "https://example.com/a".to_string(),
                    title: Some("Example A".to_string()),
                },
                OpenAiWebSearchSource {
                    url: "https://example.com/b".to_string(),
                    title: None,
                },
            ]
        );
    }

    #[test]
    fn codex_web_search_omits_domain_filters_when_none_are_requested() {
        let request_body = Arc::new(Mutex::new(String::new()));
        let server_body = Arc::clone(&request_body);
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept search request");
            let (_, body) = read_http_request_with_body(&mut stream);
            *server_body.lock().expect("body lock") = body;
            write_json_response(&mut stream, "200 OK", "{\"output\":\"answer\"}");
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-web-search-unfiltered");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        let response = client
            .web_search(&OpenAiWebSearchRequest {
                query: "rust".to_string(),
                model: Some("gpt-5.5-codex".to_string()),
                allowed_domains: Vec::new(),
                context_size: OpenAiWebSearchContextSize::Medium,
            })
            .expect("codex web search succeeds");

        server.join().expect("server thread");
        let payload = serde_json::from_str::<serde_json::Value>(
            &request_body.lock().expect("body lock").clone(),
        )
        .expect("search payload is JSON");
        assert!(payload["settings"].get("filters").is_none(), "{payload}");
        assert!(
            payload["commands"]["search_query"][0]
                .get("domains")
                .is_none(),
            "{payload}"
        );
        assert_eq!(payload["model"], "gpt-5.5-codex");
        assert_eq!(response.model, "gpt-5.5-codex");
        assert!(response.sources.is_empty());
    }

    #[test]
    fn codex_web_search_refreshes_once_after_unauthorized_response() {
        let paths = Arc::new(Mutex::new(Vec::new()));
        let server_paths = Arc::clone(&paths);
        let (base_url, server) = test_server(move |listener| {
            for index in 0..3 {
                let mut stream = accept_expected(&listener, "accept search request");
                let request_line = read_http_request(&mut stream);
                server_paths.lock().expect("paths lock").push(request_line);
                match index {
                    0 => write_json_response(&mut stream, "401 Unauthorized", "{}"),
                    1 => write_json_response(
                        &mut stream,
                        "200 OK",
                        "{\"access_token\":\"refreshed-access\",\"refresh_token\":\"refreshed-refresh\"}",
                    ),
                    _ => write_json_response(&mut stream, "200 OK", "{\"output\":\"answer\"}"),
                }
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-search-unauthorized");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        let response = client
            .web_search(&OpenAiWebSearchRequest {
                query: "rust".to_string(),
                model: None,
                allowed_domains: Vec::new(),
                context_size: OpenAiWebSearchContextSize::Medium,
            })
            .expect("search succeeds after refresh");

        server.join().expect("server thread");
        assert_eq!(response.text, "answer");
        let paths = paths.lock().expect("paths lock");
        assert!(paths[0].starts_with("POST /alpha/search "), "{paths:?}");
        assert!(paths[1].starts_with("POST /oauth/token "), "{paths:?}");
        assert!(paths[2].starts_with("POST /alpha/search "), "{paths:?}");
    }

    #[test]
    fn codex_web_search_rejects_an_empty_query_before_reaching_the_backend() {
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-search-invalid");
        let client = transport_client("http://127.0.0.1:1", UNTESTED_IDLE_WINDOW, &dir);

        let error = client
            .web_search(&OpenAiWebSearchRequest {
                query: "   ".to_string(),
                model: None,
                allowed_domains: Vec::new(),
                context_size: OpenAiWebSearchContextSize::Medium,
            })
            .expect_err("an empty query is rejected");
        assert!(error.to_string().contains("must not be empty"), "{error}");
    }

    #[test]
    fn codex_search_request_ids_are_unique_per_request() {
        assert_ne!(codex_search_request_id(), codex_search_request_id());
    }

    fn image_response_body() -> String {
        // "png-bytes" base64-encoded, matching the images API `b64_json` contract.
        "{\"created\":1,\"data\":[{\"b64_json\":\"cG5nLWJ5dGVz\"}],\"output_format\":\"png\",\"size\":\"1024x1024\"}"
            .to_string()
    }

    #[test]
    fn codex_image_generation_posts_prompt_to_generations_endpoint() {
        let request_body = Arc::new(Mutex::new(String::new()));
        let server_body = Arc::clone(&request_body);
        let paths = Arc::new(Mutex::new(Vec::new()));
        let server_paths = Arc::clone(&paths);
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept image request");
            let (request_line, body) = read_http_request_with_body(&mut stream);
            server_paths.lock().expect("paths lock").push(request_line);
            *server_body.lock().expect("body lock") = body;
            write_json_response(&mut stream, "200 OK", &image_response_body());
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-image-generation");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        let response = client
            .generate_or_edit_image(&OpenAiImageRequest {
                prompt: "a red fox in a field".to_string(),
                referenced_images: Vec::new(),
                size: Some("1024x1024".to_string()),
                quality: Some("high".to_string()),
                background: None,
            })
            .expect("codex image generation succeeds");

        server.join().expect("server thread");
        let paths = paths.lock().expect("paths lock");
        assert!(
            paths[0].starts_with("POST /images/generations "),
            "{paths:?}"
        );
        let payload = serde_json::from_str::<serde_json::Value>(
            &request_body.lock().expect("body lock").clone(),
        )
        .expect("image payload is JSON");
        assert_eq!(payload["model"], CODEX_DEFAULT_IMAGE_MODEL);
        assert_eq!(payload["prompt"], "a red fox in a field");
        assert_eq!(payload["size"], "1024x1024");
        assert_eq!(payload["quality"], "high");
        assert!(payload.get("images").is_none(), "{payload}");
        assert_eq!(response.model, CODEX_DEFAULT_IMAGE_MODEL);
        assert_eq!(response.bytes, b"png-bytes");
        assert_eq!(response.media_type, "image/png");
        assert_eq!(response.size.as_deref(), Some("1024x1024"));
    }

    #[test]
    fn codex_image_edit_sends_reference_images_to_edits_endpoint() {
        let request_body = Arc::new(Mutex::new(String::new()));
        let server_body = Arc::clone(&request_body);
        let paths = Arc::new(Mutex::new(Vec::new()));
        let server_paths = Arc::clone(&paths);
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept image edit request");
            let (request_line, body) = read_http_request_with_body(&mut stream);
            server_paths.lock().expect("paths lock").push(request_line);
            *server_body.lock().expect("body lock") = body;
            write_json_response(&mut stream, "200 OK", &image_response_body());
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-image-edit");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        client
            .generate_or_edit_image(&OpenAiImageRequest {
                prompt: "add a red hat".to_string(),
                referenced_images: vec!["data:image/png;base64,Zm9v".to_string()],
                size: None,
                quality: None,
                background: Some("transparent".to_string()),
            })
            .expect("codex image edit succeeds");

        server.join().expect("server thread");
        let paths = paths.lock().expect("paths lock");
        assert!(paths[0].starts_with("POST /images/edits "), "{paths:?}");
        let payload = serde_json::from_str::<serde_json::Value>(
            &request_body.lock().expect("body lock").clone(),
        )
        .expect("image payload is JSON");
        assert_eq!(
            payload["images"],
            serde_json::json!([{ "image_url": "data:image/png;base64,Zm9v" }])
        );
        assert_eq!(payload["background"], "transparent");
    }

    #[test]
    fn codex_image_request_refreshes_once_after_unauthorized_response() {
        let paths = Arc::new(Mutex::new(Vec::new()));
        let server_paths = Arc::clone(&paths);
        let (base_url, server) = test_server(move |listener| {
            for index in 0..3 {
                let mut stream = accept_expected(&listener, "accept image request");
                let request_line = read_http_request(&mut stream);
                server_paths.lock().expect("paths lock").push(request_line);
                match index {
                    0 => write_json_response(&mut stream, "401 Unauthorized", "{}"),
                    1 => write_json_response(
                        &mut stream,
                        "200 OK",
                        "{\"access_token\":\"refreshed-access\",\"refresh_token\":\"refreshed-refresh\"}",
                    ),
                    _ => write_json_response(&mut stream, "200 OK", &image_response_body()),
                }
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-image-unauthorized");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);

        let response = client
            .generate_or_edit_image(&OpenAiImageRequest {
                prompt: "a red fox in a field".to_string(),
                referenced_images: Vec::new(),
                size: None,
                quality: None,
                background: None,
            })
            .expect("image request succeeds after refresh");

        server.join().expect("server thread");
        assert_eq!(response.bytes, b"png-bytes");
        let paths = paths.lock().expect("paths lock");
        assert!(
            paths[0].starts_with("POST /images/generations "),
            "{paths:?}"
        );
        assert!(paths[1].starts_with("POST /oauth/token "), "{paths:?}");
        assert!(
            paths[2].starts_with("POST /images/generations "),
            "{paths:?}"
        );
    }

    #[test]
    fn codex_image_requests_are_rejected_before_reaching_the_backend_when_malformed() {
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-image-invalid");
        let client = transport_client("http://127.0.0.1:1", UNTESTED_IDLE_WINDOW, &dir);

        let empty_prompt = client
            .generate_or_edit_image(&OpenAiImageRequest {
                prompt: "   ".to_string(),
                referenced_images: Vec::new(),
                size: None,
                quality: None,
                background: None,
            })
            .expect_err("an empty prompt is rejected");
        assert!(
            empty_prompt.to_string().contains("must not be empty"),
            "{empty_prompt}"
        );

        let too_many = client
            .generate_or_edit_image(&OpenAiImageRequest {
                prompt: "edit these".to_string(),
                referenced_images: vec!["data:image/png;base64,Zm9v".to_string(); 6],
                size: None,
                quality: None,
                background: None,
            })
            .expect_err("more than five reference images is rejected");
        assert!(
            too_many.to_string().contains("at most 5 reference images"),
            "{too_many}"
        );
    }

    #[test]
    fn a_realtime_session_reuses_a_token_that_outlives_it_and_renews_one_that_does_not() {
        fn access_token(expires_in_secs: i64) -> String {
            let claims = serde_json::json!({ "exp": unix_timestamp() as i64 + expires_in_secs });
            format!(
                "header.{}.signature",
                URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims"))
            )
        }

        // Refreshing rotates the refresh token, so a token with hours left on it must be used as it
        // stands rather than renewed out from under the requests sharing the same sign-in.
        assert!(realtime_token_outlives_a_session(&access_token(6 * 3600)));
        // A session outlives the request that opened it, so a token about to expire is renewed.
        assert!(!realtime_token_outlives_a_session(&access_token(60)));
        assert!(!realtime_token_outlives_a_session(&access_token(-60)));
        // An unreadable credential is renewed rather than presented and refused.
        assert!(!realtime_token_outlives_a_session(""));
        assert!(!realtime_token_outlives_a_session("not-a-jwt"));
    }

    #[test]
    fn codex_capabilities_cover_the_backend_surface_this_runtime_can_reach() {
        let capabilities = crate::provider_capabilities(CODEX_PROVIDER_ID);
        assert!(capabilities.responses_http);
        assert!(capabilities.image_generation);
        assert!(capabilities.image_editing);
        assert!(capabilities.web_search);
        // Realtime rides the OpenAI API rather than the Codex backend, so both of its transports
        // are reachable with a ChatGPT sign-in.
        assert!(capabilities.realtime_webrtc);
        assert!(capabilities.realtime_websocket);
        assert_eq!(
            CodexProvider::openai_responses().info().capabilities,
            capabilities
        );
    }

    #[test]
    fn codex_transport_retries_before_semantic_stream_progress() {
        // Only the second attempt is meant to trip the idle window, and it does so by sleeping well
        // past it — a direction a loaded machine can only make safer. The final attempt sends its
        // events in a single chunk rather than two, so there is no gap left between them for
        // scheduling latency to turn into a false stall, which would abort after semantic progress and
        // skip the retry this test is about.
        let health_timeout = Duration::from_millis(300);
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let (base_url, server) = test_server(move |listener| {
            for index in 0..3 {
                let mut stream = accept_expected(&listener, "accept responses request");
                server_attempts.fetch_add(1, Ordering::SeqCst);
                read_http_request(&mut stream);
                if index == 0 {
                    // A connection that dies before response headers is safe to retry as well.
                    continue;
                }
                write_chunked_headers(&mut stream, "text/event-stream");
                if index == 1 {
                    // Network bytes alone are not semantic model progress. This partial event may
                    // be replayed because nothing has escaped the decoder to the caller.
                    write_http_chunk(
                        &mut stream,
                        "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":",
                    );
                    // Comfortably past the idle window, so this attempt fails for the reason the test
                    // intends rather than by a narrow margin.
                    thread::sleep(Duration::from_millis(600));
                    continue;
                }
                write_http_chunk(
                    &mut stream,
                    "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"retried\"}\n\n\
                     event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
                );
                finish_http_chunks(&mut stream);
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-before-progress-retry");
        let client = transport_client(&base_url, health_timeout, &dir);
        let mut events = Vec::new();

        let response = client
            .complete_streaming(&transport_request(), &mut |event| events.push(event))
            .expect("transient pre-progress failure is retried");

        server.join().expect("server thread");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(response.content, "retried");
        assert!(events.iter().any(|event| {
            matches!(event, LanguageModelStreamEvent::TextDelta { delta } if delta == "retried")
        }));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, LanguageModelStreamEvent::Error { .. }))
        );
    }

    #[test]
    fn codex_transport_never_retries_after_semantic_stream_progress() {
        // The probe below has to outlast any retry the client might make, or its absence proves
        // nothing: a retry arriving after a short probe window would look like no retry at all.
        let health_timeout = Duration::from_millis(300);
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept responses request");
            server_attempts.fetch_add(1, Ordering::SeqCst);
            read_http_request(&mut stream);
            write_chunked_headers(&mut stream, "text/event-stream");
            write_http_chunk(
                &mut stream,
                "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"visible\"}\n\n",
            );
            thread::sleep(Duration::from_millis(700));
            drop(stream);

            listener
                .set_nonblocking(true)
                .expect("nonblocking retry probe");
            let deadline = Instant::now() + Duration::from_millis(600);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        server_attempts.fetch_add(1, Ordering::SeqCst);
                        read_http_request(&mut stream);
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            "{\"output_text\":\"unsafe-replay\",\"output\":[],\"status\":\"completed\"}",
                        );
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("retry probe failed: {error}"),
                }
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-after-progress-no-retry");
        let client = transport_client(&base_url, health_timeout, &dir);
        let mut events = Vec::new();

        let error = client
            .complete_streaming(&transport_request(), &mut |event| events.push(event))
            .expect_err("failure after visible progress must not be replayed");

        server.join().expect("server thread");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(error.to_string().contains("idle/unhealthy"), "{error}");
        assert!(events.iter().any(|event| {
            matches!(event, LanguageModelStreamEvent::TextDelta { delta } if delta == "visible")
        }));
    }

    #[test]
    fn codex_transport_cancellation_stops_retry_backoff() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let server_attempts = Arc::clone(&attempts);
        let (first_attempt_sender, first_attempt_receiver) = mpsc::channel();
        let (base_url, server) = test_server(move |listener| {
            let mut stream = accept_expected(&listener, "accept responses request");
            server_attempts.fetch_add(1, Ordering::SeqCst);
            read_http_request(&mut stream);
            write_chunked_headers(&mut stream, "text/event-stream");
            write_http_chunk(
                &mut stream,
                "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":",
            );
            finish_http_chunks(&mut stream);
            first_attempt_sender.send(()).expect("signal first attempt");
            drop(stream);

            listener
                .set_nonblocking(true)
                .expect("nonblocking retry probe");
            let deadline = Instant::now() + Duration::from_millis(150);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((_stream, _)) => {
                        server_attempts.fetch_add(1, Ordering::SeqCst);
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("retry probe failed: {error}"),
                }
            }
        });
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-retry-cancel");
        let client = transport_client(&base_url, UNTESTED_IDLE_WINDOW, &dir);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = {
            let cancelled = Arc::clone(&cancelled);
            LanguageModelCancellation::new(move || cancelled.load(Ordering::SeqCst))
        };
        let cancel_worker = {
            let cancelled = Arc::clone(&cancelled);
            thread::spawn(move || {
                first_attempt_receiver
                    .recv_timeout(Duration::from_secs(1))
                    .expect("first attempt completes");
                thread::sleep(Duration::from_millis(20));
                cancelled.store(true, Ordering::SeqCst);
            })
        };

        let error = client
            .complete_streaming_with_cancellation(&transport_request(), &cancellation, &mut |_| {})
            .expect_err("cancellation stops retry backoff");

        cancel_worker.join().expect("cancellation thread");
        server.join().expect("server thread");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(error.to_string().contains("cancelled"), "{error}");
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
    fn openai_responses_sse_parser_does_not_repeat_completed_message_after_text_deltas() {
        let body = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hel\"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"lo\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"reasoning\",\"summary\":[{\"text\":\"checked\"}]}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_context\",\"arguments\":\"{\\\"path\\\":\\\"README.md\\\"}\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
        );
        let mut streamed = Vec::new();

        let parsed = parse_openai_responses_codex_sse_response_with_events(
            body,
            CODEX_DEFAULT_MODEL,
            &mut |event| streamed.push(event),
        )
        .expect("sse");

        assert_eq!(parsed.content, "hello");
        assert_eq!(
            streamed
                .iter()
                .filter_map(|event| match event {
                    LanguageModelStreamEvent::TextDelta { delta } => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["hel", "lo"]
        );
        assert!(streamed.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::ReasoningSummary { summary } if summary == "checked"
            )
        }));
        assert!(streamed.iter().any(|event| {
            matches!(
                event,
                LanguageModelStreamEvent::ToolCallCompleted { name, input, .. }
                    if name == "read_context" && input["path"] == "README.md"
            )
        }));
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
    fn openai_responses_sse_parser_streams_reasoning_summary_deltas() {
        let body = concat!(
            "event: response.reasoning_summary_text.delta\n",
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"check \"}\n\n",
            "event: response.reasoning_summary_text.delta\n",
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"context\"}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
        );
        let mut streamed = Vec::new();

        let parsed = parse_openai_responses_codex_sse_response_with_events(
            body,
            CODEX_DEFAULT_MODEL,
            &mut |event| streamed.push(event),
        )
        .expect("sse");

        assert_eq!(parsed.content, "done");
        assert_eq!(
            streamed
                .iter()
                .filter_map(|event| match event {
                    LanguageModelStreamEvent::ReasoningDelta { delta } => Some(delta.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["check ", "context"]
        );
    }

    #[test]
    fn codex_auth_json_round_trips_official_chatgpt_shape() {
        let dir = sinomo_testdir::TestDir::new("sinomo-codex-auth");
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
    }

    #[test]
    fn native_tool_protocol_preference_uses_codex_model_family() {
        assert!(prefers_native_tool_protocol("gpt-5.2-codex"));
        assert!(prefers_native_tool_protocol("codex-mini"));
        assert!(!prefers_native_tool_protocol("claude-sonnet-4-6"));
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
                custom: None,
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
