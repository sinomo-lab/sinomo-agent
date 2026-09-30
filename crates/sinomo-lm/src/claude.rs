use crate::anthropic::{
    anthropic_messages_payload, stream_anthropic_messages_response_with_provider,
};
use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelMessage, LanguageModelProvider,
    LanguageModelProviderInfo, LanguageModelProviderOptions, LanguageModelProviderUsage,
    LanguageModelRequest, LanguageModelResponse, LanguageModelResponseFormat, LanguageModelRole,
    LanguageModelStreamEvent, LanguageModelUsageProvider, PROVIDER_DEFAULT_MODEL,
    read_provider_credentials_json_value, selected_model, unix_timestamp,
    write_provider_credentials_json_value,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::env;
use std::path::PathBuf;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub use sinomo_runtime::api::lm::CLAUDE_DEFAULT_MODEL;
pub use sinomo_runtime::api::lm::CLAUDE_PROVIDER_ID;
pub const SINOMO_CLAUDE_CREDENTIALS_FILE_ENV_VAR: &str = "SINOMO_CLAUDE_CREDENTIALS_FILE";
pub const CLAUDE_OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
pub const CLAUDE_OAUTH_AUTHORIZE_URL: &str = "https://claude.ai/oauth/authorize";
pub const CLAUDE_OAUTH_REDIRECT_URI: &str = "https://console.anthropic.com/oauth/code/callback";
pub const CLAUDE_OAUTH_SCOPE: &str = "org:create_api_key user:profile user:inference";
// Keep the HTTP provider process-free: probing a PATH-resolved Claude CLI here
// would turn ordinary request/header construction into unrestricted host code
// execution and invalidate its `NetworkOnly` host-access classification.
const CLAUDE_CODE_VERSION_FALLBACK: &str = "2.1.74";
const CLAUDE_OAUTH_TOKEN_URLS: &[&str] = &[
    "https://platform.claude.com/v1/oauth/token",
    "https://console.anthropic.com/v1/oauth/token",
];
const CLAUDE_OAUTH_BETAS: &[&str] = &[
    "interleaved-thinking-2025-05-14",
    "fine-grained-tool-streaming-2025-05-14",
    "claude-code-20250219",
    "oauth-2025-04-20",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeConfig {
    pub api_base_url: String,
    pub default_model: String,
    pub credentials_path: Option<PathBuf>,
    pub options: LanguageModelProviderOptions,
    pub claude_code_version: Option<String>,
}

impl Default for ClaudeConfig {
    fn default() -> Self {
        Self {
            api_base_url: "https://api.anthropic.com".to_string(),
            default_model: CLAUDE_DEFAULT_MODEL.to_string(),
            credentials_path: default_claude_credentials_path(),
            options: LanguageModelProviderOptions::default(),
            claude_code_version: None,
        }
    }
}

fn default_claude_credentials_path() -> Option<PathBuf> {
    if let Some(path) = env::var(SINOMO_CLAUDE_CREDENTIALS_FILE_ENV_VAR)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    {
        return Some(PathBuf::from(path));
    }
    sinomo_runtime::local::SinomoDataDir::from_env_or_home()
        .ok()
        .map(|dir| dir.provider_credentials_file(CLAUDE_PROVIDER_ID))
}

fn missing_claude_auth() -> LanguageModelError {
    LanguageModelError::AuthenticationRequired {
        provider: CLAUDE_PROVIDER_ID.to_string(),
        detail: "no Sinomo Claude subscription sign-in found; run `sinomo providers auth claude`"
            .to_string(),
    }
}

fn claude_invocation_error(model: &str, detail: impl Into<String>) -> LanguageModelError {
    LanguageModelError::ProviderInvocationFailed {
        provider: CLAUDE_PROVIDER_ID.to_string(),
        model: model.to_string(),
        detail: detail.into(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClaudeOAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
}

impl ClaudeOAuthTokens {
    fn has_access_token(&self) -> bool {
        !self.access_token.trim().is_empty()
    }

    fn expires_soon(&self) -> bool {
        let Some(expires_at_ms) = self.expires_at_ms else {
            return false;
        };
        let now_ms = unix_timestamp().saturating_mul(1000) as i64;
        now_ms >= expires_at_ms.saturating_sub(60_000)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ClaudeAuthJson {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tokens: Option<ClaudeOAuthTokens>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_refresh: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudePkceLogin {
    pub authorization_url: String,
    pub state: String,
    code_verifier: String,
}

#[derive(Clone, Debug)]
pub struct ClaudeProvider {
    config: ClaudeConfig,
    http: Client,
}

impl ClaudeProvider {
    pub const PROVIDER_ID: &'static str = sinomo_runtime::api::lm::CLAUDE_PROVIDER_ID;

    pub fn new(config: ClaudeConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.config.credentials_path.clone()
    }

    pub fn begin_pkce_login(&self) -> Result<ClaudePkceLogin, LanguageModelError> {
        let (code_verifier, code_challenge) = generate_pkce_pair()?;
        let state = random_url_secret(32)?;
        let mut url = reqwest::Url::parse(CLAUDE_OAUTH_AUTHORIZE_URL).map_err(|source| {
            claude_invocation_error(PROVIDER_DEFAULT_MODEL, source.to_string())
        })?;
        url.query_pairs_mut()
            .append_pair("code", "true")
            .append_pair("client_id", CLAUDE_OAUTH_CLIENT_ID)
            .append_pair("response_type", "code")
            .append_pair("redirect_uri", CLAUDE_OAUTH_REDIRECT_URI)
            .append_pair("scope", CLAUDE_OAUTH_SCOPE)
            .append_pair("code_challenge", &code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", &state);
        Ok(ClaudePkceLogin {
            authorization_url: url.to_string(),
            state,
            code_verifier,
        })
    }

    pub fn complete_pkce_login(
        &self,
        login: &ClaudePkceLogin,
        authorization_code: &str,
    ) -> Result<ClaudeOAuthTokens, LanguageModelError> {
        let (code, state) = split_authorization_code(authorization_code);
        if code.trim().is_empty() {
            return Err(LanguageModelError::AuthenticationRequired {
                provider: Self::PROVIDER_ID.to_string(),
                detail: "Claude authorization code was empty".to_string(),
            });
        }
        if state.as_deref() != Some(login.state.as_str()) {
            return Err(LanguageModelError::AuthenticationRequired {
                provider: Self::PROVIDER_ID.to_string(),
                detail: "Claude authorization state did not match; sign-in was not saved"
                    .to_string(),
            });
        }
        let tokens = self.exchange_authorization_code(&code, &login.code_verifier, &login.state)?;
        self.save_oauth_tokens(tokens.clone())?;
        Ok(tokens)
    }

    pub fn oauth_tokens(&self) -> Result<ClaudeOAuthTokens, LanguageModelError> {
        let mut tokens = self
            .stored_oauth_tokens()?
            .or_else(read_claude_env_token)
            .or_else(read_claude_code_credentials_file)
            .ok_or_else(missing_claude_auth)?;
        if !tokens.has_access_token() {
            return Err(missing_claude_auth());
        }
        if tokens.expires_soon() {
            tokens = self.refresh_oauth_tokens(tokens)?;
        }
        Ok(tokens)
    }

    pub fn has_oauth_tokens(&self) -> bool {
        self.stored_oauth_tokens()
            .ok()
            .flatten()
            .or_else(read_claude_env_token)
            .or_else(read_claude_code_credentials_file)
            .is_some_and(|tokens| tokens.has_access_token())
    }

    pub fn save_oauth_tokens(&self, tokens: ClaudeOAuthTokens) -> Result<(), LanguageModelError> {
        let Some(credentials_path) = &self.config.credentials_path else {
            return Err(LanguageModelError::AuthenticationRequired {
                provider: Self::PROVIDER_ID.to_string(),
                detail: "could not resolve a Sinomo Claude credentials path; set SINOMO_DATA_DIR or SINOMO_CLAUDE_CREDENTIALS_FILE".to_string(),
            });
        };
        let last_refresh = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|source| LanguageModelError::RequestSerialization {
                detail: source.to_string(),
            })?;
        let auth = ClaudeAuthJson {
            auth_mode: Some("oauth".to_string()),
            tokens: Some(tokens),
            last_refresh: Some(last_refresh),
        };
        let value = serde_json::to_value(auth).map_err(|source| {
            LanguageModelError::RequestSerialization {
                detail: source.to_string(),
            }
        })?;
        write_provider_credentials_json_value(
            Self::PROVIDER_ID,
            &Some(credentials_path.clone()),
            value,
        )
    }

    pub fn list_models(&self) -> Result<Vec<String>, LanguageModelError> {
        let mut tokens = self.oauth_tokens()?;
        let mut refreshed = false;
        crate::catalog::collect_models(
            crate::catalog::CatalogProtocol::Anthropic,
            Self::PROVIDER_ID,
            |cursor| {
                let mut response = self.get_models(&tokens, cursor)?;
                if response.status() == reqwest::StatusCode::UNAUTHORIZED
                    && !refreshed
                    && !tokens.refresh_token.is_empty()
                {
                    tokens = self.refresh_oauth_tokens(tokens.clone())?;
                    refreshed = true;
                    response = self.get_models(&tokens, cursor)?;
                }
                if !response.status().is_success() {
                    return Err(claude_invocation_error(
                        PROVIDER_DEFAULT_MODEL,
                        format!(
                            "Claude models API failed: HTTP {}: {}",
                            response.status(),
                            response.text().unwrap_or_default()
                        ),
                    ));
                }
                response
                    .json()
                    .map_err(|e| claude_invocation_error(PROVIDER_DEFAULT_MODEL, e.to_string()))
            },
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
            max_output_tokens: Some(4),
            response_format: LanguageModelResponseFormat::Text,
            metadata: serde_json::Value::Null,
        };
        self.complete(&request).map(|_| ())
    }

    fn stored_oauth_tokens(&self) -> Result<Option<ClaudeOAuthTokens>, LanguageModelError> {
        let Some(credentials_path) = &self.config.credentials_path else {
            return Ok(None);
        };
        let Some(value) = read_provider_credentials_json_value(
            Self::PROVIDER_ID,
            &Some(credentials_path.clone()),
        )?
        else {
            return Ok(None);
        };
        parse_claude_tokens_value(&value).map_err(|source| LanguageModelError::TokenStore {
            path: credentials_path.display().to_string(),
            detail: source,
        })
    }

    fn exchange_authorization_code(
        &self,
        code: &str,
        code_verifier: &str,
        state: &str,
    ) -> Result<ClaudeOAuthTokens, LanguageModelError> {
        let payload = serde_json::json!({
            "grant_type": "authorization_code",
            "client_id": CLAUDE_OAUTH_CLIENT_ID,
            "code": code,
            "state": state,
            "redirect_uri": CLAUDE_OAUTH_REDIRECT_URI,
            "code_verifier": code_verifier,
        });
        let value = self.post_token_json(&payload, "Claude token exchange")?;
        tokens_from_oauth_response(&value, None)
    }

    fn refresh_oauth_tokens(
        &self,
        current: ClaudeOAuthTokens,
    ) -> Result<ClaudeOAuthTokens, LanguageModelError> {
        if current.refresh_token.trim().is_empty() {
            return Err(LanguageModelError::AuthenticationRequired {
                provider: Self::PROVIDER_ID.to_string(),
                detail: "Claude auth is missing a refresh token; run `sinomo providers auth claude` again".to_string(),
            });
        }
        let mut last_error = None;
        for endpoint in CLAUDE_OAUTH_TOKEN_URLS {
            let response = self
                .http
                .post(*endpoint)
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(USER_AGENT, self.claude_code_user_agent())
                .form(&[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", current.refresh_token.as_str()),
                    ("client_id", CLAUDE_OAUTH_CLIENT_ID),
                ])
                .send();
            let response = match response {
                Ok(response) => response,
                Err(source) => {
                    last_error = Some(source.to_string());
                    continue;
                }
            };
            let status = response.status();
            let body = response.text().unwrap_or_default();
            if !status.is_success() {
                last_error = Some(format!("HTTP {status}: {body}"));
                continue;
            }
            let value = serde_json::from_str::<serde_json::Value>(&body).map_err(|source| {
                claude_invocation_error(PROVIDER_DEFAULT_MODEL, source.to_string())
            })?;
            let tokens = tokens_from_oauth_response(&value, Some(current.refresh_token.clone()))?;
            self.save_oauth_tokens(tokens.clone())?;
            return Ok(tokens);
        }
        Err(LanguageModelError::AuthenticationRequired {
            provider: Self::PROVIDER_ID.to_string(),
            detail: format!(
                "Claude token refresh failed: {}",
                last_error.unwrap_or_else(|| "no token endpoint accepted the refresh".to_string())
            ),
        })
    }

    fn post_token_json(
        &self,
        payload: &serde_json::Value,
        label: &str,
    ) -> Result<serde_json::Value, LanguageModelError> {
        let mut last_error = None;
        for endpoint in CLAUDE_OAUTH_TOKEN_URLS {
            let response = self
                .http
                .post(*endpoint)
                .header(CONTENT_TYPE, "application/json")
                .header(USER_AGENT, self.claude_code_user_agent())
                .json(payload)
                .send();
            let response = match response {
                Ok(response) => response,
                Err(source) => {
                    last_error = Some(source.to_string());
                    continue;
                }
            };
            let status = response.status();
            let body = response.text().unwrap_or_default();
            if !status.is_success() {
                last_error = Some(format!("HTTP {status}: {body}"));
                continue;
            }
            return serde_json::from_str::<serde_json::Value>(&body).map_err(|source| {
                claude_invocation_error(PROVIDER_DEFAULT_MODEL, source.to_string())
            });
        }
        Err(claude_invocation_error(
            PROVIDER_DEFAULT_MODEL,
            format!(
                "{label} failed: {}",
                last_error.unwrap_or_else(|| "no token endpoint accepted the request".to_string())
            ),
        ))
    }

    fn get_models(
        &self,
        tokens: &ClaudeOAuthTokens,
        cursor: Option<&str>,
    ) -> Result<reqwest::blocking::Response, LanguageModelError> {
        let mut request = self
            .http
            .get(crate::catalog::endpoint(
                &self.config.api_base_url,
                "v1/models",
            ))
            .headers(self.claude_headers(tokens, PROVIDER_DEFAULT_MODEL)?);
        if let Some(cursor) = cursor {
            request = request.query(&[("after_id", cursor)]);
        }
        request
            .send()
            .map_err(|e| claude_invocation_error(PROVIDER_DEFAULT_MODEL, e.to_string()))
    }

    fn claude_code_user_agent(&self) -> String {
        format!(
            "claude-cli/{} (external, cli)",
            self.config
                .claude_code_version
                .clone()
                .unwrap_or_else(|| CLAUDE_CODE_VERSION_FALLBACK.to_string())
        )
    }

    fn claude_headers(
        &self,
        tokens: &ClaudeOAuthTokens,
        model: &str,
    ) -> Result<HeaderMap, LanguageModelError> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            USER_AGENT,
            header_value(&self.claude_code_user_agent(), model)?,
        );
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        let mut betas = CLAUDE_OAUTH_BETAS.join(",");
        if crate::anthropic::uses_bound_thinking(model) {
            betas.push(',');
            betas.push_str(crate::anthropic::ANTHROPIC_THINKING_BINDING_BETA);
        }
        headers.insert("anthropic-beta", header_value(&betas, model)?);
        headers.insert("x-app", HeaderValue::from_static("cli"));
        headers.insert(
            AUTHORIZATION,
            header_value(&format!("Bearer {}", tokens.access_token), model)?,
        );
        Ok(headers)
    }

    fn post_messages(
        &self,
        model: &str,
        tokens: &ClaudeOAuthTokens,
        payload: &serde_json::Value,
    ) -> Result<reqwest::blocking::Response, LanguageModelError> {
        let url = crate::catalog::endpoint(&self.config.api_base_url, "v1/messages");
        let mut headers = self.claude_headers(tokens, model)?;
        if payload.get("stream").and_then(serde_json::Value::as_bool) == Some(true) {
            headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        }
        self.http
            .post(url)
            .headers(headers)
            .json(payload)
            .send()
            .map_err(|source| claude_invocation_error(model, source.to_string()))
    }
}

impl Default for ClaudeProvider {
    fn default() -> Self {
        Self::new(ClaudeConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::anthropic::inferred_context_window_tokens(model)
}

impl LanguageModelProvider for ClaudeProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.default_model.clone(),
            invocation: LanguageModelInvocation::Http,
            host_access: crate::LanguageModelHostAccess::NetworkOnly,
            capabilities: crate::LanguageModelProviderCapabilities::default(),
            supports_json_object: false,
            supports_tool_permissions: false,
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
        let mut tokens = self.oauth_tokens()?;
        let mut payload = anthropic_messages_payload(request, &model, &self.config.options);
        crate::anthropic::validate_anthropic_payload(&payload, Self::PROVIDER_ID, &model)?;
        payload["stream"] = serde_json::json!(true);
        let mut response = self.post_messages(&model, &tokens, &payload)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            && !tokens.refresh_token.is_empty()
        {
            tokens = self.refresh_oauth_tokens(tokens)?;
            response = self.post_messages(&model, &tokens, &payload)?;
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(claude_invocation_error(
                &model,
                format!("Claude Messages API failed: HTTP {status}: {body}"),
            ));
        }

        if payload.get("stream").and_then(serde_json::Value::as_bool) == Some(true) {
            on_event(LanguageModelStreamEvent::Started {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
            });
            return stream_anthropic_messages_response_with_provider(
                response,
                Self::PROVIDER_ID,
                &model,
                on_event,
            );
        }

        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| claude_invocation_error(&model, source.to_string()))?;
        Ok(crate::anthropic::wire::response(
            &value,
            Self::PROVIDER_ID,
            &model,
        ))
    }
}

impl LanguageModelUsageProvider for ClaudeProvider {
    fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError> {
        let tokens = self.oauth_tokens()?;
        let response = self
            .http
            .get("https://api.anthropic.com/api/oauth/usage")
            .headers(self.claude_headers(&tokens, PROVIDER_DEFAULT_MODEL)?)
            .send()
            .map_err(|source| {
                claude_invocation_error(PROVIDER_DEFAULT_MODEL, source.to_string())
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(claude_invocation_error(
                PROVIDER_DEFAULT_MODEL,
                format!("Claude OAuth usage API failed: HTTP {status}: {body}"),
            ));
        }
        let value = response.json::<serde_json::Value>().map_err(|source| {
            claude_invocation_error(PROVIDER_DEFAULT_MODEL, source.to_string())
        })?;
        Ok(LanguageModelProviderUsage {
            provider: Self::PROVIDER_ID.to_string(),
            plan: value
                .get("plan")
                .or_else(|| value.get("subscription"))
                .or_else(|| value.get("tier"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            captured_at_unix: unix_timestamp(),
            limits: Vec::new(),
            credits: None,
        })
    }
}

fn header_value(value: &str, model: &str) -> Result<HeaderValue, LanguageModelError> {
    HeaderValue::from_str(value)
        .map_err(|source| claude_invocation_error(model, source.to_string()))
}

fn random_url_secret(byte_len: usize) -> Result<String, LanguageModelError> {
    let rng = SystemRandom::new();
    let mut bytes = vec![0u8; byte_len];
    rng.fill(&mut bytes).map_err(|_| {
        claude_invocation_error(PROVIDER_DEFAULT_MODEL, "secure random unavailable")
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn generate_pkce_pair() -> Result<(String, String), LanguageModelError> {
    let verifier = random_url_secret(32)?;
    let digest = digest::digest(&digest::SHA256, verifier.as_bytes());
    let challenge = URL_SAFE_NO_PAD.encode(digest.as_ref());
    Ok((verifier, challenge))
}

fn split_authorization_code(input: &str) -> (String, Option<String>) {
    let trimmed = input.trim();
    let mut parts = trimmed.splitn(2, '#');
    let code = parts.next().unwrap_or_default().trim().to_string();
    let state = parts
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    (code, state)
}

fn tokens_from_oauth_response(
    value: &serde_json::Value,
    previous_refresh_token: Option<String>,
) -> Result<ClaudeOAuthTokens, LanguageModelError> {
    let access_token = value
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|token| !token.trim().is_empty())
        .ok_or_else(|| {
            claude_invocation_error(
                PROVIDER_DEFAULT_MODEL,
                "Claude OAuth response missed access_token",
            )
        })?;
    let refresh_token = value
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .filter(|token| !token.trim().is_empty())
        .or(previous_refresh_token)
        .unwrap_or_default();
    let expires_at_ms = value
        .get("expires_at_ms")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| {
            value
                .get("expires_in")
                .and_then(serde_json::Value::as_i64)
                .map(|expires_in| unix_timestamp() as i64 * 1000 + expires_in * 1000)
        });
    let scopes = value
        .get("scope")
        .and_then(serde_json::Value::as_str)
        .map(|scope| scope.split_whitespace().map(str::to_string).collect())
        .or_else(|| {
            value
                .get("scopes")
                .and_then(serde_json::Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
        })
        .unwrap_or_default();
    Ok(ClaudeOAuthTokens {
        access_token,
        refresh_token,
        expires_at_ms,
        scopes,
    })
}

fn parse_claude_tokens_value(
    value: &serde_json::Value,
) -> Result<Option<ClaudeOAuthTokens>, String> {
    if value.get("tokens").is_some() {
        let auth = serde_json::from_value::<ClaudeAuthJson>(value.clone())
            .map_err(|source| source.to_string())?;
        return Ok(auth.tokens.filter(ClaudeOAuthTokens::has_access_token));
    }
    if let Some(oauth) = value.get("claudeAiOauth") {
        return Ok(parse_claude_code_oauth_value(oauth));
    }
    if value.get("access_token").is_some() {
        let tokens = serde_json::from_value::<ClaudeOAuthTokens>(value.clone())
            .map_err(|source| source.to_string())?;
        return Ok(tokens.has_access_token().then_some(tokens));
    }
    Ok(None)
}

fn parse_claude_code_oauth_value(value: &serde_json::Value) -> Option<ClaudeOAuthTokens> {
    let access_token = value
        .get("accessToken")
        .or_else(|| value.get("access_token"))
        .and_then(serde_json::Value::as_str)?
        .to_string();
    if access_token.trim().is_empty() {
        return None;
    }
    let refresh_token = value
        .get("refreshToken")
        .or_else(|| value.get("refresh_token"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let expires_at_ms = value
        .get("expiresAt")
        .or_else(|| value.get("expires_at_ms"))
        .and_then(serde_json::Value::as_i64);
    let scopes = value
        .get("scopes")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Some(ClaudeOAuthTokens {
        access_token,
        refresh_token,
        expires_at_ms,
        scopes,
    })
}

fn read_claude_env_token() -> Option<ClaudeOAuthTokens> {
    for env_var in [
        "SINOMO_CLAUDE_OAUTH_TOKEN",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "ANTHROPIC_TOKEN",
    ] {
        if let Ok(token) = env::var(env_var) {
            let token = token.trim();
            if !token.is_empty() {
                return Some(ClaudeOAuthTokens {
                    access_token: token.to_string(),
                    refresh_token: String::new(),
                    expires_at_ms: None,
                    scopes: Vec::new(),
                });
            }
        }
    }
    None
}

fn read_claude_code_credentials_file() -> Option<ClaudeOAuthTokens> {
    let path = env::var("CLAUDE_CONFIG_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            env::var("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".claude")
        })
        .join(".credentials.json");
    let content = std::fs::read_to_string(path).ok()?;
    let value = serde_json::from_str::<serde_json::Value>(&content).ok()?;
    parse_claude_tokens_value(&value).ok().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_user_agent_uses_a_static_version_without_spawning_a_cli() {
        let provider = ClaudeProvider::default();

        assert_eq!(
            provider.claude_code_user_agent(),
            format!("claude-cli/{CLAUDE_CODE_VERSION_FALLBACK} (external, cli)")
        );
    }

    #[test]
    fn pkce_authorization_url_separates_state_from_verifier() {
        let provider = ClaudeProvider::new(ClaudeConfig {
            claude_code_version: Some("2.1.99".to_string()),
            ..ClaudeConfig::default()
        });
        let login = provider.begin_pkce_login().expect("pkce login");

        assert!(
            login
                .authorization_url
                .starts_with(CLAUDE_OAUTH_AUTHORIZE_URL)
        );
        assert!(login.authorization_url.contains("code_challenge="));
        assert!(
            login
                .authorization_url
                .contains("code_challenge_method=S256")
        );
        assert!(login.authorization_url.contains("state="));
        assert!(!login.authorization_url.contains(&login.code_verifier));
        assert_ne!(login.state, login.code_verifier);
    }

    #[test]
    fn parses_claude_code_credentials_shape() {
        let value = serde_json::json!({
            "claudeAiOauth": {
                "accessToken": "cc-access",
                "refreshToken": "cc-refresh",
                "expiresAt": 12345,
                "scopes": ["user:inference"]
            }
        });

        let tokens = parse_claude_tokens_value(&value)
            .expect("parse")
            .expect("tokens");

        assert_eq!(tokens.access_token, "cc-access");
        assert_eq!(tokens.refresh_token, "cc-refresh");
        assert_eq!(tokens.expires_at_ms, Some(12345));
        assert_eq!(tokens.scopes, vec!["user:inference"]);
    }

    #[test]
    fn splits_authorization_code_and_state() {
        assert_eq!(
            split_authorization_code("code-123#state-456"),
            ("code-123".to_string(), Some("state-456".to_string()))
        );
    }
}
