use crate::{
    LanguageModelError, LanguageModelInvocation, LanguageModelProvider, LanguageModelProviderInfo,
    LanguageModelProviderUsage, LanguageModelRequest, LanguageModelResponse,
    LanguageModelResponseFormat, LanguageModelRole, LanguageModelStreamEvent, LanguageModelUsage,
    LanguageModelUsageLimit, LanguageModelUsageProvider, read_provider_credentials_json_value,
    read_server_sent_events, render_plain_prompt, selected_model,
    stream_openai_chat_completion_response, unix_timestamp, write_provider_credentials_json_value,
};
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use sinomo_runtime::local::SinomoDataDir;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubCopilotConfig {
    pub client_id: String,
    pub github_host: String,
    pub api_base_url: String,
    pub default_model: String,
    pub token_store_path: Option<PathBuf>,
}

impl Default for GitHubCopilotConfig {
    fn default() -> Self {
        Self {
            client_id: GITHUB_COPILOT_CLIENT_ID.to_string(),
            github_host: "github.com".to_string(),
            api_base_url: "https://api.githubcopilot.com".to_string(),
            default_model: "gpt-5.4".to_string(),
            token_store_path: default_copilot_credentials_path(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GitHubCopilotProvider {
    config: GitHubCopilotConfig,
    http: Client,
}

impl GitHubCopilotProvider {
    pub const PROVIDER_ID: &'static str = "github-copilot";

    pub fn new(config: GitHubCopilotConfig) -> Self {
        Self {
            config,
            http: Client::new(),
        }
    }

    pub fn with_token_store_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.token_store_path = Some(path.into());
        self
    }

    pub fn credentials_path(&self) -> Option<PathBuf> {
        self.config.token_store_path.clone()
    }

    pub fn device_login_start(&self) -> Result<GitHubDeviceCode, LanguageModelError> {
        let url = format!(
            "https://{}/login/device/code",
            self.config.github_host.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(github_oauth_headers())
            .query(&[("client_id", self.config.client_id.as_str())])
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: format!("device login start failed: HTTP {status}: {body}"),
            });
        }

        response.json::<GitHubDeviceCode>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            }
        })
    }

    pub fn device_login_poll(
        &self,
        device_code: &str,
    ) -> Result<GitHubDevicePoll, LanguageModelError> {
        let url = format!(
            "https://{}/login/oauth/access_token",
            self.config.github_host.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(github_oauth_headers())
            .query(&[
                ("client_id", self.config.client_id.as_str()),
                ("device_code", device_code),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ])
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: format!("device login poll failed: HTTP {status}: {body}"),
            });
        }

        let payload = response.json::<GitHubDevicePoll>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            }
        })?;

        if let Some(token) = payload.access_token.as_ref() {
            self.save_github_token(token, "device-login")?;
        }

        Ok(payload)
    }

    pub fn github_token(&self) -> Result<String, LanguageModelError> {
        if let Some(stored) = self.load_stored_github_token()? {
            validate_github_copilot_token(&stored.github_token)?;
            return Ok(stored.github_token);
        }

        for env_var in ["COPILOT_GITHUB_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"] {
            if let Ok(token) = std::env::var(env_var) {
                let token = token.trim();
                if !token.is_empty() {
                    validate_github_copilot_token(token)?;
                    return Ok(token.to_string());
                }
            }
        }

        Err(LanguageModelError::AuthenticationRequired {
            provider: Self::PROVIDER_ID.to_string(),
            detail: "no GitHub token found; set COPILOT_GITHUB_TOKEN or run the device login flow"
                .to_string(),
        })
    }

    pub fn copilot_api_token(&self) -> Result<String, LanguageModelError> {
        self.github_token()
    }

    pub fn test_connection(&self) -> Result<(), LanguageModelError> {
        self.list_models().map(|_| ())
    }

    pub fn list_models(&self) -> Result<Vec<String>, LanguageModelError> {
        let token = self.github_token()?;
        let url = format!("{}/models", self.config.api_base_url.trim_end_matches('/'));
        let response = self
            .http
            .get(url)
            .headers(copilot_api_headers(&token)?)
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: format!("Copilot models check failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            }
        })?;
        Ok(extract_copilot_model_ids(&value))
    }

    pub fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError> {
        let token = self.github_token()?;
        let api_host = if self.config.github_host == "github.com" {
            "api.github.com".to_string()
        } else {
            format!("api.{}", self.config.github_host.trim_end_matches('/'))
        };
        let url = format!("https://{api_host}/copilot_internal/user");
        let response = self
            .http
            .get(&url)
            .headers(copilot_exchange_headers(&token)?)
            .send()
            .map_err(|source| LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: format!("Copilot usage check failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: self.config.default_model.clone(),
                detail: source.to_string(),
            }
        })?;
        Ok(copilot_usage_from_value(&value, unix_timestamp()))
    }

    #[allow(dead_code)]
    fn exchange_copilot_api_token(
        &self,
        github_token: &str,
    ) -> Result<CopilotApiToken, LanguageModelError> {
        let api_host = if self.config.github_host == "github.com" {
            "api.github.com".to_string()
        } else {
            format!("api.{}", self.config.github_host.trim_end_matches('/'))
        };
        let urls = [
            format!("https://{api_host}/copilot_internal/user"),
            format!("https://{api_host}/copilot_internal/v2/token"),
        ];
        let mut errors = Vec::new();
        for url in urls {
            match self.try_copilot_token_endpoint(&url, github_token) {
                Ok(token) => return Ok(token),
                Err(error) => errors.push(error),
            }
        }
        Err(LanguageModelError::ProviderInvocationFailed {
            provider: Self::PROVIDER_ID.to_string(),
            model: self.config.default_model.clone(),
            detail: format!(
                "Copilot token exchange failed across known endpoints: {}",
                errors.join("; ")
            ),
        })
    }

    #[allow(dead_code)]
    fn try_copilot_token_endpoint(
        &self,
        url: &str,
        github_token: &str,
    ) -> Result<CopilotApiToken, String> {
        let response = self
            .http
            .get(url)
            .headers(copilot_exchange_headers(github_token).map_err(|source| source.to_string())?)
            .send()
            .map_err(|source| source.to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().unwrap_or_default();
            return Err(format!(
                "{url}: {}",
                copilot_token_exchange_error_detail(status.as_u16(), &body)
            ));
        }

        let value = response
            .json::<serde_json::Value>()
            .map_err(|source| format!("{url}: {source}"))?;
        parse_copilot_api_token(&value)
            .ok_or_else(|| format!("{url}: response did not include a Copilot API token"))
    }

    fn load_stored_github_token(&self) -> Result<Option<StoredGitHubToken>, LanguageModelError> {
        let value =
            read_provider_credentials_json_value(Self::PROVIDER_ID, &self.config.token_store_path)?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(|source| LanguageModelError::TokenStore {
                path: self
                    .config
                    .token_store_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<github-copilot-credentials>".to_string()),
                detail: source.to_string(),
            })
    }

    fn save_github_token(&self, token: &str, source: &str) -> Result<(), LanguageModelError> {
        let payload = StoredGitHubToken {
            github_token: token.to_string(),
            source: source.to_string(),
            created_at_unix: unix_timestamp(),
        };
        let value = serde_json::to_value(payload).map_err(|source| {
            LanguageModelError::RequestSerialization {
                detail: source.to_string(),
            }
        })?;
        write_provider_credentials_json_value(
            Self::PROVIDER_ID,
            &self.config.token_store_path,
            value,
        )
    }
}

impl Default for GitHubCopilotProvider {
    fn default() -> Self {
        Self::new(GitHubCopilotConfig::default())
    }
}

pub(crate) fn inferred_context_window_tokens(model: &str) -> Option<usize> {
    crate::parse_declared_context_window_tokens(model).or_else(|| {
        let model = model.to_ascii_lowercase();
        if model.contains("claude") {
            crate::anthropic::inferred_context_window_tokens(&model)
        } else {
            crate::openai::inferred_context_window_tokens(&model)
        }
    })
}

impl LanguageModelProvider for GitHubCopilotProvider {
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
        let model = selected_model(&self.config.default_model, request);
        let token = self.copilot_api_token()?;
        let (endpoint, payload) = if copilot_model_uses_responses_api(&model) {
            ("responses", responses_payload(request, &model))
        } else {
            ("chat/completions", chat_completion_payload(request, &model))
        };
        let url = format!(
            "{}/{endpoint}",
            self.config.api_base_url.trim_end_matches('/')
        );
        let response = self
            .http
            .post(url)
            .headers(copilot_api_headers(&token)?)
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
                detail: format!("Copilot {endpoint} failed: HTTP {status}: {body}"),
            });
        }

        let value = response.json::<serde_json::Value>().map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model: model.clone(),
                detail: source.to_string(),
            }
        })?;
        let (content, usage) = if endpoint == "responses" {
            (
                extract_responses_text(&value),
                value.get("usage").map(|usage| LanguageModelUsage {
                    input_tokens: usage
                        .get("input_tokens")
                        .and_then(serde_json::Value::as_u64),
                    output_tokens: usage
                        .get("output_tokens")
                        .and_then(serde_json::Value::as_u64),
                }),
            )
        } else {
            (
                value
                    .pointer("/choices/0/message/content")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                value.get("usage").map(|usage| LanguageModelUsage {
                    input_tokens: usage
                        .get("prompt_tokens")
                        .and_then(serde_json::Value::as_u64),
                    output_tokens: usage
                        .get("completion_tokens")
                        .and_then(serde_json::Value::as_u64),
                }),
            )
        };

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
        let model = selected_model(&self.config.default_model, request);
        let token = self.copilot_api_token()?;
        let uses_responses_api = copilot_model_uses_responses_api(&model);
        let (endpoint, mut payload) = if uses_responses_api {
            ("responses", responses_payload(request, &model))
        } else {
            ("chat/completions", chat_completion_payload(request, &model))
        };
        payload["stream"] = serde_json::json!(true);
        let url = format!(
            "{}/{endpoint}",
            self.config.api_base_url.trim_end_matches('/')
        );
        let mut headers = copilot_api_headers(&token)?;
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
                detail: format!("Copilot {endpoint} failed: HTTP {status}: {body}"),
            });
        }

        on_event(LanguageModelStreamEvent::Started {
            provider: Self::PROVIDER_ID.to_string(),
            model: model.clone(),
        });
        if uses_responses_api {
            stream_copilot_responses_response(response, &model, on_event)
        } else {
            stream_openai_chat_completion_response(response, Self::PROVIDER_ID, &model, on_event)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct GitHubDeviceCode {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub expires_in: u64,
    #[serde(default)]
    pub interval: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct GitHubDevicePoll {
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub token_type: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub error_description: Option<String>,
    #[serde(default)]
    pub interval: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StoredGitHubToken {
    github_token: String,
    source: String,
    created_at_unix: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CopilotApiToken {
    token: String,
    expires_at: u64,
}

pub const GITHUB_COPILOT_CLIENT_ID: &str = "Ov23li8tweQw6odWQebz";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubCopilotCliConfig {
    pub program: String,
    pub model: String,
    pub extra_args: Vec<String>,
    pub silent: bool,
    pub no_ask_user: bool,
}

impl Default for GitHubCopilotCliConfig {
    fn default() -> Self {
        Self {
            program: "copilot".to_string(),
            model: "copilot-default".to_string(),
            extra_args: Vec::new(),
            silent: true,
            no_ask_user: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitHubCopilotCliProvider {
    config: GitHubCopilotCliConfig,
}

impl GitHubCopilotCliProvider {
    pub const PROVIDER_ID: &'static str = "github-copilot-cli";

    pub fn new(config: GitHubCopilotCliConfig) -> Self {
        Self { config }
    }

    pub fn with_model(model: impl Into<String>) -> Self {
        Self::new(GitHubCopilotCliConfig {
            model: model.into(),
            ..GitHubCopilotCliConfig::default()
        })
    }

    pub fn command_args(&self, request: &LanguageModelRequest) -> Vec<String> {
        let mut args = Vec::new();
        args.push("-p".to_string());
        args.push(render_plain_prompt(request));
        if self.config.silent {
            args.push("-s".to_string());
        }
        if self.config.no_ask_user {
            args.push("--no-ask-user".to_string());
        }
        let model = selected_model(&self.config.model, request);
        if model != "copilot-default" {
            args.push("--model".to_string());
            args.push(model);
        }
        args.extend(self.config.extra_args.iter().cloned());
        args
    }
}

impl Default for GitHubCopilotCliProvider {
    fn default() -> Self {
        Self::new(GitHubCopilotCliConfig::default())
    }
}

impl LanguageModelProvider for GitHubCopilotCliProvider {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProviderInfo {
            provider: Self::PROVIDER_ID.to_string(),
            default_model: self.config.model.clone(),
            invocation: LanguageModelInvocation::Process,
            host_access: crate::LanguageModelHostAccess::Unrestricted,
            capabilities: crate::LanguageModelProviderCapabilities::default(),
            supports_json_object: false,
            supports_tool_permissions: true,
        }
    }

    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        let model = selected_model(&self.config.model, request);
        let output = Command::new(&self.config.program)
            .args(self.command_args(request))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|source| LanguageModelError::ProviderSpawnFailed {
                provider: Self::PROVIDER_ID.to_string(),
                program: self.config.program.clone(),
                detail: source.to_string(),
            })?;

        if !output.status.success() {
            return Err(LanguageModelError::ProviderInvocationFailed {
                provider: Self::PROVIDER_ID.to_string(),
                model,
                detail: format!(
                    "exit status {:?}: {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }

        Ok(LanguageModelResponse {
            provider: Self::PROVIDER_ID.to_string(),
            model,
            content: String::from_utf8_lossy(&output.stdout).trim().to_string(),
            items: Vec::new(),
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

fn github_oauth_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-www-form-urlencoded"),
    );
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("GitHubCopilotChat/0.26.7"),
    );
    headers
}

fn copilot_exchange_headers(github_token: &str) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("token {github_token}")).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
                model: GitHubCopilotConfig::default().default_model,
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

fn copilot_api_headers(copilot_token: &str) -> Result<HeaderMap, LanguageModelError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("Sinomo/0.1"));
    headers.insert("Editor-Version", HeaderValue::from_static("vscode/1.104.1"));
    headers.insert(
        "Copilot-Integration-Id",
        HeaderValue::from_static("vscode-chat"),
    );
    headers.insert(
        "Openai-Intent",
        HeaderValue::from_static("conversation-edits"),
    );
    headers.insert("x-initiator", HeaderValue::from_static("user"));
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {copilot_token}")).map_err(|source| {
            LanguageModelError::ProviderInvocationFailed {
                provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
                model: GitHubCopilotConfig::default().default_model,
                detail: source.to_string(),
            }
        })?,
    );
    Ok(headers)
}

fn extract_copilot_model_ids(value: &serde_json::Value) -> Vec<String> {
    let mut models: Vec<String> = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| model.get("id").and_then(serde_json::Value::as_str))
        .map(str::to_string)
        .collect();
    models.sort();
    models.dedup();
    models
}

fn copilot_usage_from_value(
    value: &serde_json::Value,
    captured_at_unix: u64,
) -> LanguageModelProviderUsage {
    let plan = value
        .get("copilot_plan")
        .or_else(|| value.get("access_type_sku"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let reset_at = value
        .get("quota_reset_date_utc")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_rfc3339_unix);

    let mut limits = value
        .get("quota_snapshots")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|snapshots| snapshots.iter())
        .filter_map(|(key, snapshot)| copilot_usage_limit(key, snapshot, reset_at))
        .collect::<Vec<_>>();
    limits.sort_by(|left, right| left.id.cmp(&right.id));

    LanguageModelProviderUsage {
        provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
        plan,
        captured_at_unix,
        limits,
        credits: None,
    }
}

fn copilot_usage_limit(
    key: &str,
    value: &serde_json::Value,
    fallback_reset_at: Option<i64>,
) -> Option<LanguageModelUsageLimit> {
    let id = value
        .get("quota_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(key)
        .to_string();
    let unlimited = value.get("unlimited").and_then(serde_json::Value::as_bool);
    let remaining =
        json_f64(value.get("remaining")).or_else(|| json_f64(value.get("quota_remaining")));
    let entitlement = json_f64(value.get("entitlement"));
    let remaining_percent = json_f64(value.get("percent_remaining"));
    let used_percent = remaining_percent.map(|percent| (100.0 - percent).max(0.0));
    let reset_at_unix = json_i64(value.get("quota_reset_at"))
        .filter(|value| *value > 0)
        .or(fallback_reset_at);

    Some(LanguageModelUsageLimit {
        id,
        name: None,
        scope: Some("quota".to_string()),
        unlimited,
        allowed: unlimited.map(|value| value || remaining.unwrap_or_default() > 0.0),
        exhausted: Some(unlimited == Some(false) && remaining == Some(0.0)),
        remaining,
        entitlement,
        used_percent,
        remaining_percent,
        window_seconds: None,
        reset_at_unix,
    })
}

fn parse_rfc3339_unix(value: &str) -> Option<i64> {
    OffsetDateTime::parse(value, &Rfc3339)
        .ok()
        .map(|datetime| datetime.unix_timestamp())
}

fn json_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_f64(),
        Some(serde_json::Value::String(text)) => text.trim().parse().ok(),
        _ => None,
    }
}

fn json_i64(value: Option<&serde_json::Value>) -> Option<i64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_i64(),
        Some(serde_json::Value::String(text)) => text.trim().parse().ok(),
        _ => None,
    }
}

fn copilot_model_uses_responses_api(model: &str) -> bool {
    model == "gpt-5.5" || model.starts_with("gpt-5.5-")
}

fn responses_payload(request: &LanguageModelRequest, model: &str) -> serde_json::Value {
    let input: Vec<serde_json::Value> = request
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
        .collect();
    let mut payload = serde_json::json!({
        "model": model,
        "input": input,
        "stream": false,
        "reasoning": { "effort": "none" },
    });
    payload["max_output_tokens"] = serde_json::json!(request.max_output_tokens.unwrap_or(3_000));
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["text"] = serde_json::json!({ "format": { "type": "json_object" } });
    }
    payload
}

fn extract_responses_text(value: &serde_json::Value) -> String {
    value
        .get("output")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|output| output.get("content").and_then(serde_json::Value::as_array))
        .flatten()
        .filter_map(|content| {
            if content.get("type").and_then(serde_json::Value::as_str) == Some("output_text") {
                content.get("text").and_then(serde_json::Value::as_str)
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("")
}

fn stream_copilot_responses_response(
    response: reqwest::blocking::Response,
    model: &str,
    on_event: &mut dyn FnMut(LanguageModelStreamEvent),
) -> Result<LanguageModelResponse, LanguageModelError> {
    let mut content = String::new();
    let mut completed_response = None;
    let mut emitted_text = false;
    let mut emitted_finished = false;

    read_server_sent_events(
        response,
        GitHubCopilotProvider::PROVIDER_ID,
        model,
        |event| {
            let mut value =
                serde_json::from_str::<serde_json::Value>(&event.data).map_err(|source| {
                    LanguageModelError::ProviderInvocationFailed {
                        provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
                        model: model.to_string(),
                        detail: format!(
                            "failed to parse Copilot stream event: {source}: {}",
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
                        content.push_str(delta);
                        emitted_text = true;
                        on_event(LanguageModelStreamEvent::TextDelta {
                            delta: delta.to_string(),
                        });
                    }
                }
                "response.completed" => {
                    let final_response = value
                        .get("response")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    let final_text = extract_responses_text(&final_response);
                    if !emitted_text && !final_text.is_empty() {
                        emitted_text = true;
                        on_event(LanguageModelStreamEvent::TextDelta {
                            delta: final_text.clone(),
                        });
                    }
                    if let Some(usage) = copilot_responses_usage(&final_response) {
                        on_event(LanguageModelStreamEvent::Usage { usage });
                    }
                    on_event(LanguageModelStreamEvent::Finished {
                        finish_reason: final_response
                            .get("status")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string),
                    });
                    emitted_finished = true;
                    completed_response = Some(final_response);
                }
                "response.failed" | "error" => {
                    let detail = value
                        .pointer("/response/error/message")
                        .or_else(|| value.pointer("/error/message"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("Copilot stream failed")
                        .to_string();
                    on_event(LanguageModelStreamEvent::Error {
                        detail: detail.clone(),
                    });
                    return Err(LanguageModelError::ProviderInvocationFailed {
                        provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
                        model: model.to_string(),
                        detail,
                    });
                }
                _ => {}
            }
            Ok(())
        },
    )?;

    let final_response = completed_response.unwrap_or_else(|| serde_json::json!({}));
    let final_text = extract_responses_text(&final_response);
    if content.is_empty() {
        content = final_text;
    }
    let usage = copilot_responses_usage(&final_response);
    if !emitted_text && !content.is_empty() {
        on_event(LanguageModelStreamEvent::TextDelta {
            delta: content.clone(),
        });
    }
    if !emitted_finished {
        if let Some(usage) = &usage {
            on_event(LanguageModelStreamEvent::Usage {
                usage: usage.clone(),
            });
        }
        on_event(LanguageModelStreamEvent::Finished {
            finish_reason: final_response
                .get("status")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        });
    }

    Ok(LanguageModelResponse {
        provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
        model: model.to_string(),
        content,
        items: Vec::new(),
        tool_calls: Vec::new(),
        usage,
    })
}

fn copilot_responses_usage(value: &serde_json::Value) -> Option<LanguageModelUsage> {
    value.get("usage").map(|usage| LanguageModelUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(serde_json::Value::as_u64),
        output_tokens: usage
            .get("output_tokens")
            .and_then(serde_json::Value::as_u64),
    })
}

fn chat_completion_payload(request: &LanguageModelRequest, model: &str) -> serde_json::Value {
    let messages: Vec<serde_json::Value> = request
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
        .collect();
    let mut payload = serde_json::json!({
        "model": model,
        "messages": messages,
        "stream": false,
    });
    if let Some(temperature) = request.temperature {
        payload["temperature"] = serde_json::json!(temperature);
    }
    if let Some(max_tokens) = request.max_output_tokens {
        if copilot_chat_model_uses_max_completion_tokens(model) {
            payload["max_completion_tokens"] = serde_json::json!(max_tokens);
        } else {
            payload["max_tokens"] = serde_json::json!(max_tokens);
        }
    }
    if request.response_format == LanguageModelResponseFormat::JsonObject {
        payload["response_format"] = serde_json::json!({ "type": "json_object" });
    }
    payload
}

impl LanguageModelUsageProvider for GitHubCopilotProvider {
    fn usage(&self) -> Result<LanguageModelProviderUsage, LanguageModelError> {
        GitHubCopilotProvider::usage(self)
    }
}

fn copilot_chat_model_uses_max_completion_tokens(model: &str) -> bool {
    model == "gpt-5" || model.starts_with("gpt-5.")
}

fn validate_github_copilot_token(token: &str) -> Result<(), LanguageModelError> {
    if token.starts_with("ghp_") {
        return Err(LanguageModelError::UnsupportedToken {
            provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
            detail: "classic GitHub PATs (ghp_*) are not supported for Copilot API requests; use a device-login token, fine-grained PAT with Copilot Requests, or GitHub App user token".to_string(),
        });
    }
    Ok(())
}

fn copilot_token_exchange_error_detail(status: u16, body: &str) -> String {
    let base = format!("Copilot token exchange failed: HTTP {status}: {body}");
    if status == 403 && body.contains("Resource not accessible by integration") {
        format!(
            "{base}; this usually means the stored GitHub token came from a GitHub App integration \
             rather than a Copilot-compatible OAuth/device flow. Re-run `sinomo providers \
             github-copilot auth` after removing the existing credentials file, or use a \
             fine-grained PAT with Copilot Requests permission."
        )
    } else {
        base
    }
}

fn parse_copilot_api_token(value: &serde_json::Value) -> Option<CopilotApiToken> {
    let token = value
        .get("token")
        .or_else(|| value.get("access_token"))
        .or_else(|| value.pointer("/copilot/token"))
        .and_then(serde_json::Value::as_str)?;
    let expires_at = value
        .get("expires_at")
        .or_else(|| value.pointer("/copilot/expires_at"))
        .and_then(|expires| {
            expires
                .as_u64()
                .or_else(|| expires.as_str().and_then(|value| value.parse().ok()))
        })
        .or_else(|| {
            value
                .get("expires_in")
                .or_else(|| value.pointer("/copilot/expires_in"))
                .and_then(serde_json::Value::as_u64)
                .map(|expires_in| unix_timestamp() + expires_in)
        })
        .unwrap_or_else(|| unix_timestamp() + 25 * 60);

    Some(CopilotApiToken {
        token: token.to_string(),
        expires_at,
    })
}

fn default_copilot_credentials_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("SINOMO_COPILOT_TOKEN_FILE") {
        return Some(PathBuf::from(path));
    }
    SinomoDataDir::from_env_or_home()
        .ok()
        .map(|data| data.provider_credentials_file(GitHubCopilotProvider::PROVIDER_ID))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LanguageModelMessage, LanguageModelResponseFormat, LanguageModelRole};

    fn request() -> LanguageModelRequest {
        LanguageModelRequest {
            provider: GitHubCopilotProvider::PROVIDER_ID.to_string(),
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
    fn direct_copilot_payload_uses_chat_completion_shape() {
        let payload = chat_completion_payload(&request(), "gpt-5.4");

        assert_eq!(payload["model"], "gpt-5.4");
        assert_eq!(payload["stream"], false);
        assert_eq!(payload["max_completion_tokens"], 200);
        assert!(payload.get("max_tokens").is_none());
        assert_eq!(payload["response_format"]["type"], "json_object");
        assert_eq!(payload["messages"][0]["role"], "system");
    }

    #[test]
    fn direct_copilot_legacy_chat_payload_uses_max_tokens() {
        let payload = chat_completion_payload(&request(), "gpt-4.1");

        assert_eq!(payload["max_tokens"], 200);
        assert!(payload.get("max_completion_tokens").is_none());
    }

    #[test]
    fn copilot_model_list_parser_extracts_sorted_ids() {
        let value = serde_json::json!({
            "data": [
                { "id": "gpt-5.5", "name": "GPT-5.5" },
                { "id": "gpt-5.4" },
                { "id": "gpt-5.5" }
            ]
        });

        assert_eq!(
            extract_copilot_model_ids(&value),
            vec!["gpt-5.4", "gpt-5.5"]
        );
    }

    #[test]
    fn copilot_usage_parser_maps_quota_snapshots() {
        let usage = copilot_usage_from_value(
            &serde_json::json!({
                "copilot_plan": "individual_pro",
                "quota_reset_date_utc": "2026-05-01T00:00:00.000Z",
                "quota_snapshots": {
                    "chat": {
                        "quota_id": "chat",
                        "percent_remaining": 100,
                        "unlimited": true,
                        "remaining": 0,
                        "entitlement": 0
                    },
                    "premium_interactions": {
                        "quota_id": "premium_interactions",
                        "percent_remaining": 13.7,
                        "quota_remaining": 205.9,
                        "unlimited": false,
                        "remaining": 205,
                        "entitlement": 1500
                    }
                }
            }),
            42,
        );

        assert_eq!(usage.provider, GitHubCopilotProvider::PROVIDER_ID);
        assert_eq!(usage.plan.as_deref(), Some("individual_pro"));
        assert_eq!(usage.limits.len(), 2);
        assert_eq!(usage.limits[1].id, "premium_interactions");
        assert_eq!(usage.limits[1].remaining, Some(205.0));
        assert_eq!(usage.limits[1].entitlement, Some(1500.0));
        assert_eq!(usage.limits[1].used_percent, Some(86.3));
        assert_eq!(usage.limits[1].reset_at_unix, Some(1777593600));
    }

    #[test]
    fn direct_copilot_responses_payload_uses_responses_shape_for_gpt_5_5() {
        let payload = responses_payload(&request(), "gpt-5.5");

        assert_eq!(payload["model"], "gpt-5.5");
        assert_eq!(payload["stream"], false);
        assert_eq!(payload["text"]["format"]["type"], "json_object");
        assert_eq!(payload["reasoning"]["effort"], "none");
        assert_eq!(payload["max_output_tokens"], 200);
        assert_eq!(payload["input"][0]["role"], "system");
        assert!(copilot_model_uses_responses_api("gpt-5.5"));
    }

    #[test]
    fn copilot_responses_text_extractor_collects_output_text() {
        let value = serde_json::json!({
            "output": [
                { "type": "reasoning", "summary": [] },
                {
                    "type": "message",
                    "content": [
                        { "type": "output_text", "text": "{\"ok\":" },
                        { "type": "output_text", "text": "true}" }
                    ]
                }
            ],
            "usage": { "input_tokens": 1, "output_tokens": 2 }
        });

        assert_eq!(extract_responses_text(&value), "{\"ok\":true}");
    }

    #[test]
    fn copilot_token_parser_accepts_common_shapes() {
        for value in [
            serde_json::json!({ "token": "copilot-token", "expires_at": 1234 }),
            serde_json::json!({ "access_token": "copilot-token", "expires_in": 60 }),
            serde_json::json!({ "copilot": { "token": "copilot-token", "expires_at": "1234" } }),
        ] {
            let token = parse_copilot_api_token(&value).expect("token");
            assert_eq!(token.token, "copilot-token");
        }

        assert!(parse_copilot_api_token(&serde_json::json!({ "id": 1 })).is_none());
    }

    #[test]
    fn classic_pat_is_rejected_for_direct_copilot() {
        let error = validate_github_copilot_token("ghp_legacy").expect_err("classic PAT rejected");

        assert!(matches!(error, LanguageModelError::UnsupportedToken { .. }));
    }
}
